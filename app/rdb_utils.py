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