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
    
def replicate_handshake(store: RedisStore): 
    try: 
        s = socket.create_connection((store.master_host, store.master_port))
        store.replica_socket = s
        
        # step 1: Send ping
        s.sendall(b"*1\r\n$4\r\nPING\r\n")
        response = s.recv(1024)
        print(f"[Replica] Received from master: {response}")
        
        # step 2: send replconf listening-port
        port_str = str(store.replica_port)
        replconf_1 = (
            "*3\r\n"
            "$8\r\nREPLCONF\r\n"
            "$14\r\nlistening-port\r\n"
            f"${len(port_str)}\r\n{port_str}\r\n"
        )
        s.sendall(replconf_1.encode())
        repl_conf1_res = s.recv(1024)
        print(f"[Replica] Received replconf1 response: {repl_conf1_res}")
        
        # step 3: send replconf with capa and psync2
        replconf_2 = (
            "*3\r\n"
            "$8\r\nREPLCONF\r\n"
            "$4\r\ncapa\r\n"
            "$6\r\npsync2\r\n"
        )
        s.sendall(replconf_2.encode())
        repl_conf2_res = s.recv(1024)
        print(f"[Replica] Received replconf2 response: {repl_conf2_res}")
        
        # step 4: send psync ? -1
        psync_command = (
            "*3\r\n"
            "$5\r\nPSYNC\r\n"
            "$1\r\n?\r\n"
            "$2\r\n-1\r\n"
        )
        s.sendall(psync_command.encode())
        psync_response = s.recv(1024)
        print(f"[Replica] Received PSYNC response {psync_response}")
        print("[Replica] Completed REPLCONF handshake")
    except Exception as e: 
        # even with error, using "with" still closes the connection
        print(f"[Replica] Connection to master failed: {e}")

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
                print("INFO payload:", repr(info))
                client.send(info)
            elif command == "REPLCONF":
                client.send(b"+OK\r\n")
            elif command == "PSYNC": 
                if len(args) == 3 and args[1] == "?" and args[2] == "-1": 
                    repl_id = store.master_repl_id
                    response = f"+FULLRESYNC {repl_id} 0\r\n"
                    client.send(response.encode())
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
    parser.add_argument("--replicaof", type=str, help="Specify master host and port for replica mode, e.g. 'localhost 6379'")
    parser_args = parser.parse_args()

    cwd = os.getcwd()
    print(f"Printing cwd {cwd}")
    rdb_path = os.path.join(cwd, parser_args.dir, parser_args.dbfilename)
    print(f"Loading RDB from: {rdb_path}")
    
    config = Config(parser_args.dir, parser_args.dbfilename)
    replica_config = None
    # replica storing master information inside RedisStore if parser_args.replicaof exists
    if parser_args.replicaof: 
        host_port = parser_args.replicaof.strip().split()
        if len(host_port) != 2:
            raise ValueError("--replicaof must be in format '<host> <port>'")
        replica_config = {"role": "slave", "master_host": host_port[0], "master_port": int(host_port[1]), "replica_port": parser_args.port}
    else: 
        print("[REPLICA MASTER]")
        replica_config = {"role": "master"}
    
    store = RedisStore(rdb_path=rdb_path, replica_config=replica_config)
    # send PING to master server from slave server
    if replica_config["role"] == "slave": 
        threading.Thread(
            target=replicate_handshake,
            args=(store,),
            daemon=True
        ).start()

    server_socket = socket.create_server(("localhost", parser_args.port), reuse_port=True)
    while True: 
        client_sock, client_addr = server_socket.accept()
        threading.Thread(target=handle_command, args=(client_sock, store, config)).start()


if __name__ == "__main__":
    main()
