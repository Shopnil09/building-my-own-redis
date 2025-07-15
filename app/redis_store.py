class RedisStore: 
  def __init__(self): 
    self.data = {}
  
  def set(self, key, val): 
    self.data[key] = val
    return b"+OK\r\n"
  
  def get(self, key): 
    if key in self.data:
      val = self.data[key]
      return f"${len(val)}\r\n{val}\r\n".encode()
    
    # if key does not exist, we will send a null bulk string
    return b"$-1\r\n"