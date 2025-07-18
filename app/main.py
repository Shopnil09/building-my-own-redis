import socket
import threading
from app.redis_store import RedisStore
from app.config import Config
import argparse
import os

BUFF_SIZE = 4096

def parse_redis_command(data: bytes): 
    lines = data.decode().split("\r\n")
    args = []
    i = 0
    while i < len(lines): 
        if lines[i].startswith("*"):
            i += 1  # skip array header like *2
        elif lines[i].startswith("$"):
            i += 1  # skip to the value (right after $length)
            if i < len(lines) and lines[i] != "":
                args.append(lines[i])  # add the actual value
            i += 1  # advance past the value
        else:
            if lines[i] != "":
                args.append(lines[i])  # fallback in case it's a raw string
            i += 1
    return args
    

def handle_command(client: socket.socket, store: RedisStore, config: Config):
    try: 
        while True: 
            chunk = client.recv(BUFF_SIZE)
            print("Raw chunk received", chunk)
            if not chunk: 
                break
            
            args = parse_redis_command(chunk)
            print("Parsed command:", args)
            
            command = args[0].upper()
            
            if command == "PING": 
                response = f"+PONG\r\n"
                client.send(response.encode())
            elif command == "ECHO" and len(args) == 2: 
                response = f"${len(args[1])}\r\n{args[1]}\r\n"
                client.send(response.encode())
            elif command == "GET" and len(args) == 2: 
                data = store.get(args[1])
                client.send(data)
            elif command == "CONFIG" and len(args) == 3 and args[1].upper() == "GET":
                param = args[2]
                value = config.get(param)
                client.send(value)
            elif command == "KEYS" and len(args) == 2 and args[1] == "*":
                keys = store.keys()
                client.send(keys)
            elif command == "SET": 
                if len(args) >= 3:
                    k, v = args[1], args[2]
                    px = None
                    if len(args) >= 5 and args[3].upper() == "PX": 
                        # form validation for wrong input
                        try: 
                            px = int(args[4])
                        except ValueError: 
                            client.send(b"-ERR PX value must be an integer\r\n")
                            continue
                    
                    data = store.set(k, v, px)
                    client.send(data)
                else: 
                    client.send(b"-ERR wrong number of arguments for SET\r\n")
            elif command == "INFO" and len(args) == 2 and args[1].upper() == "REPLICATION": 
                info = store.replication_info()
                client.send(info)
            else: 
                client.send(b"-ERR unknown command\r\n")
    except Exception as e: 
        print(f"[Thread Error] Exception in client handler: {e}")
        client.close()

def main():
    print("Started....")
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", default="tmp")
    parser.add_argument("--dbfilename", default="dump.rdb")
    parser.add_argument("--port", type=int, default=6379) 
    parser_args = parser.parse_args()
    
    cwd = os.getcwd()
    print(f"Printing cwd {cwd}")
    rdb_path = os.path.join(cwd, parser_args.dir, parser_args.dbfilename)
    print(f"Loading RDB from: {rdb_path}")
    
    config = Config(parser_args.dir, parser_args.dbfilename)
    store = RedisStore(rdb_path=rdb_path)
    
    server_socket = socket.create_server(("localhost", parser_args.port), reuse_port=True)
    while True: 
        client_sock, client_addr = server_socket.accept()
        threading.Thread(target=handle_command, args=(client_sock, store, config)).start()


if __name__ == "__main__":
    main()
