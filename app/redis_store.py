import time
from .rdb_loader import load_keys_from_rdb

class RedisStore: 
  def __init__(self, rdb_path=None, replica_config=None): 
    self.data = {}
    self.role = replica_config.get("role", "master")
    self.master_host = replica_config.get("master_host")
    self.master_port = replica_config.get("master_port")
    if rdb_path: # if rdb_path exists, load the data from the file 
      parsed_data = load_keys_from_rdb(rdb_path)
      self.data.update(parsed_data)
      print(f"Printing self.data {self.data}")
  
  def set(self, key, val, px=None): 
    expiry_time = None
    if px is not None: 
      expiry_time = self._curr_time_ms() + px
    # storing it as a tuple when the val and expiry_time
    self.data[key] = (val, expiry_time)
    return b"+OK\r\n"
  
  def get(self, key): 
    if key in self.data:
      val, expiry = self.data[key]
      if expiry is not None and self._curr_time_ms() >= expiry: 
        del self.data[key]
        return b"$-1\r\n"
      
      # no expiry, or data is within the timeframe
      return f"${len(val)}\r\n{val}\r\n".encode()
    
    # if key does not exist, send a null bulk string
    return b"$-1\r\n"
  
  def keys(self): 
    now = self._curr_time_ms()
    valid_keys = []
    expired_keys = []
    
    for key, (val, expiry) in self.data.items(): 
      if expiry is not None and now >= expiry: 
        expired_keys.append(key)
      else: 
        valid_keys.append(key)
    
    # cleaning up expired keys
    for key in expired_keys: 
      del self.data[key]
    
    return self._encode_resp_list(valid_keys)
  
  def replication_info(self): 
    payload = f"role:{self.role}"
    return f"${len(payload)}\r\n{payload}\r\n".encode()

  def _encode_resp_list(self, items): 
    resp = f"*{len(items)}\r\n"
    for item in items: 
      resp += f"${len(item)}\r\n{item}\r\n"
    return resp.encode()
    
  def _curr_time_ms(self): 
    return int(time.time() * 1000)