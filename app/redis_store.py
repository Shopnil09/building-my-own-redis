import time

class RedisStore: 
  def __init__(self): 
    self.data = {}
  
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
  
  def _curr_time_ms(self): 
    return int(time.time() * 1000)