import time
from .rdb_loader import load_keys_from_rdb
import secrets
import threading
from collections import OrderedDict

class RedisStore:
  def __init__(self, rdb_path=None, replica_config=None):
    self.data = {
      "stream_key": {
        "type": "stream",
        "entries": OrderedDict()
      } 
    }
    self.role = replica_config.get("role", "master")
    self.master_host = replica_config.get("master_host")
    self.master_port = replica_config.get("master_port")
    self.replica_port = replica_config.get("replica_port", None)

    # replication fields (only relevant for master)
    if self.role == "master":
      self.master_repl_id = secrets.token_hex(20)
      self.master_repl_offset = 0
      # to store the replica sockets to propagate commands
      self.replica_sockets = []
      self.command_logs = []
    else:
      self.master_repl_id = None
      self.master_repl_offset = None
      self.repl_offset = 0
      self.repl_offset_lock = threading.Lock()

    if rdb_path: # if rdb_path exists, load the data from the file
      parsed_data = load_keys_from_rdb(rdb_path)
      self.data.update(parsed_data)
      print(f"Printing self.data {self.data}")

  def set(self, key, val, px=None):
    # expiry_time = None
    # if px is not None:
    #   expiry_time = self._curr_time_ms() + px
    # # storing it as a tuple when the val and expiry_time
    # self.data[key] = (val, expiry_time)
    # return b"+OK\r\n"
    expiry_time = None
    if px is not None: 
      expiry_time = self._curr_time_ms() + px
    
    self.data[key] = {
      "type": "string",
      "value": val,
      "expiry": expiry_time
    }
    
    return b"+OK\r\n"

  def get(self, key):
    if key in self.data: 
      entry = self.data[key]
      
      expiry = entry.get("expiry")
      if expiry is not None and self._curr_time_ms() >= expiry: 
        del self.data[key]
        return b"$-1\r\n"
      
      val = entry["value"] 
      return f"${len(val)}\r\n{val}\r\n".encode()
    
    return b"$-1\r\n"

  def keys(self):
    now = self._curr_time_ms()
    valid_keys = []
    expired_keys = []

    for key, entry in self.data.items(): 
      # skipping over non-string types
      if entry.get("type") != "string": 
        continue 
      
      expiry = entry.get("expiry")
      if expiry is not None and now >= expiry: 
        expired_keys.append(key)
      else: 
        valid_keys.append(key)
      
    for key in expired_keys:
      del self.data[key]
      
    return self._encode_resp_list(valid_keys)

  def type(self, key):
    if key in self.data: 
      entry = self.data[key]
      expiry = entry.get("expiry")
      if expiry is not None and self._curr_time_ms() >= expiry:
        del self.data[key]
        # return none if key is expired
        return b"+none\r\n"
      
      # return type
      return f"+{entry['type']}\r\n".encode()
    
    # return none for type if the key is not found
    return b"+none\r\n"
  
  def xadd(self, stream_key, entry_id, fields):
    try: 
      if stream_key not in self.data: 
          self.data[stream_key] = {
            "type": "stream",
            "entries": OrderedDict()
          }
        
      stream_obj = self.data[stream_key]
      if stream_obj["type"] != "stream":
        return b"-ERR key exists but is not a stream\r\n"
        
      entries = stream_obj["entries"]
      
      if entry_id == "*": 
        ms_part = self._curr_time_ms()
        seq_part = 0
        if entries: 
          last_id = next(reversed(entries))
          last_ms, last_seq = map(int, last_id.split("-"))
          if ms_part < last_ms: 
            ms_part = last_ms
            seq_part = last_seq + 1
          elif ms_part == last_ms: 
            seq_part = last_seq + 1
        
        final_id = f"{ms_part}-{seq_part}"
      else: 
        if "-" not in entry_id: 
          return b"-ERR Invalid entry ID format\r\n"
        
        ms_raw, seq_raw = entry_id.split("-")
        ms_part = int(ms_raw)
        
        if ms_part < 0: 
          return b"-ERR The ID specified in XADD must be greater than 0-0\r\n"
        
        if seq_raw == "*": 
          # setting the default if seq_part is not found in the existing_id
          if ms_part == 0: 
            seq_part = 1
          else: 
            seq_part = 0
          
          for existing_id in reversed(entries): 
            last_ms, last_seq = map(int, existing_id.split("-"))
            if last_ms == ms_part:
              seq_part = last_seq + 1
              break
        else:
          seq_part = int(seq_raw)
          if ms_part == 0 and seq_part == 0:
            return b"-ERR The ID specified in XADD must be greater than 0-0\r\n"
        
        final_id = f"{ms_part}-{seq_part}"
        
        # Validate: final_id must be strictly greater than last entry
        if entries: 
          last_id = next(reversed(entries))
          last_ms, last_seq = map(int, last_id.split("-"))
          
          if ms_part < last_ms or (ms_part == last_ms and seq_part <= last_seq):
            return b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
        
      entry_data = {}
      # for loop to increment by 2 since it has key,val
      for i in range(0, len(fields), 2): 
        field = fields[i]
        value = fields[i+1]
        entry_data[field] = value
      
      # insert entry into stream
      stream_obj["entries"][final_id] = entry_data
      # return the entry ID as bulk string
      print(stream_obj)
      return f"${len(final_id)}\r\n{final_id}\r\n".encode()
    except Exception as e:
      print(f"[Redis Store XADD] Error {e}")
      return b"-ERR Error with XADD"
  
  def replication_info(self):
    lines = [
        f"role:{self.role}",
        f"master_repl_offset:{self.master_repl_offset}",
        f"master_replid:{self.master_repl_id}",
    ]
    payload = "\r\n".join(lines)  # do NOT add a final \r\n manually
    full_payload = f"${len(payload)}\r\n{payload}\r\n"
    return full_payload.encode()

  def _encode_resp_list(self, items):
    resp = f"*{len(items)}\r\n"
    for item in items:
      resp += f"${len(item)}\r\n{item}\r\n"
    return resp.encode()

  def _curr_time_ms(self):
    return int(time.time() * 1000)