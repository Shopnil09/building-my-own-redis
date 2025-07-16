class Config: 
  def __init__(self, dir_path="/tmp", db_file_name="dump.rdb"):
    self.config_map = {
      "dir": dir_path,
      "db_file_name": db_file_name
    }
    
  def get(self, key): 
    value = self.config_map.get(key)
    if value is not None:
        return f"*2\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n".encode()
    else:
        return b"*0\r\n"  # empty array if key not found