# app/rdb_utils.py
import sys

def decode_size(f): 
  first = f.read(1)
  if not first: 
    raise EOFError("Unexpected end of file while decoding size")

  b = first[0]
  # AND operation keeps the first two digits after 'b' (which is also stored in variable b) and it shifts left 6 times so get rid the of the other bits after 'bxx'
  prefix = (b & 0b11000000) >> 6
  
  if prefix == 0b00: # 6-bit size
    return b & 0b00111111
  elif prefix == 0b01: 
    second = f.read(1)
    if not second: 
      raise EOFError("Expected second byte for 14-bit size")
    return ((b & 0b00111111) << 8) | second[0]
  elif prefix == 0b10: 
    full = f.read(4)
    if len(full) < 4: 
      raise EOFError("Expected 4 bytes for 32-bit size")
    return int.from_bytes(full, byteorder="big")
  else: 
    print(prefix)
    # raise ValueError("Unsupported size encoding (starts with 0b11)")
    print(f"[WARN] Skipping unsupported special string encoding byte: {hex(b)}", file=sys.stderr)
    return None


def read_string(f): 
  size = decode_size(f)
  if size: 
    return f.read(size).decode("utf-8")
  else: 
    print(f"[WARN] from read_string")

def read_resp_command(sock):
    def read_line():
        line = b""
        while not line.endswith(b"\r\n"):
            chunk = sock.recv(1)
            if not chunk:
                raise ConnectionError("Socket closed during line read")
            line += chunk
        return line
    
    header = read_line()
    if not header: 
      raise ConnectionError("Socket closed while reading RESP array header")
    # Read the RESP array header: *<num>\r\n
    
    if not header.startswith(b"*"):
        raise ValueError("Invalid RESP array")

    try:
        num_args = int(header[1:-2])  # Skip * and \r\n
    except ValueError:
        raise ValueError("Invalid RESP array length")

    args = []
    for _ in range(num_args):
        # Read bulk string header: $<len>\r\n
        length_line = read_line()
        if not length_line.startswith(b"$"):
            raise ValueError("Invalid bulk string header")

        try:
            length = int(length_line[1:-2])
        except ValueError:
            raise ValueError("Invalid bulk string length")

        # Read the actual content (can be binary-safe)
        arg = b""
        while len(arg) < length:
            part = sock.recv(length - len(arg))
            if not part:
                raise ConnectionError("Socket closed during argument read")
            arg += part

        # Consume the trailing \r\n
        trailer = sock.recv(2)
        if trailer != b"\r\n":
            raise ValueError("Expected CRLF after argument")

        args.append(arg.decode())  # or `.decode("utf-8", errors="replace")`

    return args
  
def consume_full_psync_response(sock):
    # Step 1: Read PSYNC response line
    line = b""
    while not line.endswith(b"\r\n"):
        chunk = sock.recv(1)
        if not chunk:
            raise ConnectionError("Socket closed during PSYNC line read")
        line += chunk

    print(f"[Replica] Received PSYNC response {line}")
    
    if not line.startswith(b"+FULLRESYNC"):
        raise ValueError("Expected FULLRESYNC")

    # Step 2: Read the RDB bulk string header: $<len>\r\n
    header = b""
    while not header.endswith(b"\r\n"):
        chunk = sock.recv(1)
        if not chunk:
            raise ConnectionError("Socket closed while reading RDB header")
        header += chunk

    print(f"[Replica] RDB header: {header}")
    
    if not header.startswith(b"$"):
        raise ValueError("Expected RDB bulk string header")

    try:
        rdb_len = int(header[1:-2])  # remove $ and trailing \r\n
    except ValueError:
        raise ValueError(f"Invalid RDB length: {header}")

    # Step 3: Read exactly rdb_len bytes of RDB binary data
    rdb_data = b""
    while len(rdb_data) < rdb_len:
        chunk = sock.recv(min(4096, rdb_len - len(rdb_data)))
        if not chunk:
            raise ConnectionError("Socket closed while receiving RDB data")
        rdb_data += chunk

    print(f"[Replica] Received RDB data ({len(rdb_data)} bytes)")
    return line, header, rdb_data

  