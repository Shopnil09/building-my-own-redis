import os
import io

from .rdb_utils import decode_size, read_string

def load_keys_from_rdb(path: str): 
  keys = {}
  
  if not os.path.exists(path): 
    print(f"[RDB] File not found {path}")
    return keys
  
  with open(path, "rb") as raw: 
    f = io.BufferedReader(raw)
    
    # Step 1: Verify Redis header
    magic = f.read(9)
    if magic != b"REDIS0011":
      print("[RDB] Unsupported header")
      return keys
    
    # Step 2: Skip metadata
    while f.peek(1)[:1] == b'\xFA':
      print(f.peek(1)) 
      f.read(1) # skip FA
      _ = read_string(f) # skip key
      _ = read_string(f) # skip value
    
    # Step 3: Look for DB selector and hash table size
    while True: 
      b = f.read(1)
      if not b: 
        break
      
      if b[0] == 0xFE: # DB selector
        _ = decode_size(f)
      elif b[0] == 0xFB: # hash size info
        _ = decode_size(f)
        _ = decode_size(f)
        break
    
    # Step 4: parse key-val entries
    while True: 
      b = f.read(1)
      if not b or b[0] == 0xFF: 
        break # EOF marker
      
      if b[0] in (0xFC, 0xFD): # optional expiry
        skip = 8 if b[0] == 0xFC else 4
        f.read(skip)
        b = f.read(1)
      
      if b and b[0] == 0x00: 
        key = read_string(f)
        val = read_string(f)
        keys[key] = (val, None)
      else: 
        print("[RDB] Unknown or unsupported type")
  
  return keys
        
        
        
      