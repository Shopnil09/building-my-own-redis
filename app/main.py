import socket
import threading
from app.redis_store import RedisStore
from app.config import Config
import argparse
import os
from app.rdb_utils import read_resp_command, consume_full_psync_response, try_read_resp_command
import time

BUFF_SIZE = 4096

EMPTY_RDB_HEX = (
    "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473"
    "c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365"
    "c000fff06e3bfec0ff5aa2"
)

EMPTY_RDB_BYTES = bytes.fromhex(EMPTY_RDB_HEX)

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

def send_empty_rdb(sock: socket): 
    rdb_len = len(EMPTY_RDB_BYTES)
    header = f"${rdb_len}\r\n".encode()
    sock.sendall(header + EMPTY_RDB_BYTES)
    print("[Master] Sent the EMPTY_RDB_HEX to replica")

def send_getack_to_replica(sock: socket): 
    payload = (
        "*3\r\n"
        "$8\r\nREPLCONF\r\n"
        "$6\r\nGETACK\r\n"
        "$1\r\n*\r\n"
    ).encode()
    
    try: 
        while True: 
            sock.sendall(payload)
            print("[Master] Sent REPLCONF GETACK * to replica socket")
    except Exception as e: 
        print(f"[Master] Stopped sending GETACK to replica due to error: {e}")

def propagate_commands_to_replicas(args, store: RedisStore):
    # if it is not the master server, do not run any logic and return
    if store.role != "master": 
        return
    
    # construct the RESP array manually
    resp = f"*{len(args)}\r\n"
    for arg in args: 
        resp += f"${len(arg)}\r\n{arg}\r\n"
    print("[Master] Printing resp:", resp)
    data = resp.encode()
    # store the commands in the command_logs
    store.command_logs.append(data)
    # to remove non-active sockets
    disconnected = []
    print("[Master] Printing the length of replica_sockets", len(store.replica_sockets))
    for s in store.replica_sockets: 
        try: 
            s.sendall(data)
        except Exception as e: 
            print("[Master] Failed to send to replica {e}")
            disconnected.append(s)
    
    # add logic to cleanup disconnected sockets

# this function is for the replica server to listen to commands from the master server and take specific actions
def replicate_command_listener(store: RedisStore): 
    # this is added into the RedisStore in line 72 in replicate_handshake
    repl_sock = store.replica_socket
    buffer = b""
    while True: 
        try: 
            chunk = repl_sock.recv(4096)
            if not chunk: 
                break
            buffer += chunk
            while True: 
                try: 
                    args, remaining = try_read_resp_command(buffer)
                    if args is None: 
                        break
                    
                    consumed = len(buffer) - len(remaining)
                    command = args[0].upper()
                    if command == "SET": 
                        key, val = args[1], args[2]
                        px = None
                        if len(args) >= 5 and args[3].upper() == "PX":
                            px = int(args[4])
                        store.set(key, val, px)
                        print("[Replica] Set data to the RedisStore sent by master")
                    elif command == "PING": 
                        print("[Replica] Received ping from master")
                    elif (command == "REPLCONF" and len(args) == 3 and args[1].upper() == "GETACK"): 
                        print("[Replica] received REPLCONF from master, sending payload...")
                        with store.repl_offset_lock: 
                            ack_offset = store.repl_offset
                        payload = (
                            "*3\r\n"
                            "$8\r\nREPLCONF\r\n"
                            "$3\r\nACK\r\n"
                            f"${len(str(ack_offset))}\r\n{ack_offset}\r\n"
                        )
                        repl_sock.sendall(payload.encode())
                        print(f"[Replica] Sent REPLCONF ACK {ack_offset}")
                    else: # any other commands, ignore and continue
                        print(f"[Replica] Ignored command {args}")
                    
                    with store.repl_offset_lock: 
                        store.repl_offset += consumed
                    buffer = remaining
                except Exception as e: 
                    print(f"[Replica] Error during parsing or handling command {e}")
                    break
        except Exception as e: 
            print(f"[Replica] Error reading command: {e}")
            break

def replicate_handshake(store: RedisStore): 
    try: 
        s = socket.create_connection((store.master_host, store.master_port))
        # store.replica_socket is only present for replica RedisStores
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
        try:
            psync_response, rdb_header, rdb_data = consume_full_psync_response(s)
            print("[Replica] Completed PSYNC and RDB sync, socket is now clean")
        except Exception as e: 
            print(f"[Replica] Error during PSYNC handling: {e}")
        
        print("[Replica] Completed REPLCONF handshake")
        # logic to setup a background listener to handle propagated commands from master
        threading.Thread(
            target=replicate_command_listener,
            args=(store,),
            daemon=True,
        ).start()
        print("[Replica] Started listener thread for command propagation")
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
            elif command == "XADD": # command for streaming data
                if len(args) >= 4: 
                    stream_key = args[1]
                    entry_id = args[2]
                    field_values = args[3:]
                    response = store.xadd(stream_key=stream_key, entry_id=entry_id, fields=field_values)
                    client.send(response)
                else: 
                    client.send(b"-ERR wrong number of arguments for XADD\r\n")
            elif command == "XRANGE": 
                if len(args) == 4: 
                    stream_key = args[1]
                    start_id = args[2]
                    end_id = args[3]
                    response = store.xrange(stream_key=stream_key, start_id=start_id, end_id=end_id)
                    client.send(response)
                else: 
                    client.send(b"-ERR Wrong number of arguments for XRANGE\r\n")
            elif command == "XREAD": 
                if len(args) >= 4 and args[1].upper() == "STREAMS":
                    stream_key = args[2]
                    last_id = args[3]
                    response = store.xread(stream_key, last_id)
                    client.send(response)
                else: 
                    client.send(b"--ERR wrong number of arguments for XREAD\r\n")
            elif command == "CONFIG" and len(args) == 3 and args[1].upper() == "GET":
                param = args[2]
                value = config.get(param)
                client.send(value)
            elif command == "KEYS" and len(args) == 2 and args[1] == "*":
                keys = store.keys()
                client.send(keys)
            elif command == "TYPE" and len(args) == 2: 
                resp = store.type(args[1])
                client.send(resp)
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
                    # propagate the command to the replicas of the master
                    # this command will not be executed by the replicas since there is a condition in the beginning of the func.
                    propagate_commands_to_replicas(args, store)
                else: 
                    client.send(b"-ERR wrong number of arguments for SET\r\n")
            elif command == "INFO" and len(args) == 2 and args[1].upper() == "REPLICATION": 
                info = store.replication_info()
                print("INFO payload:", repr(info))
                client.send(info)
            elif command == "REPLCONF":
                if args[1].upper() == "ACK": 
                    print("[Master] Received ACK from replica, registering socket")
                else: 
                    print("[Master/Replica] Received REPLCONF command")
                    client.send(b"+OK\r\n")
            elif command == "PSYNC":
                if len(args) == 3 and args[1] == "?" and args[2] == "-1": 
                    repl_id = store.master_repl_id
                    response = f"+FULLRESYNC {repl_id} 0\r\n"
                    client.send(response.encode())
                    time.sleep(0.05)
                    # calling send_empty_rdb because psync command tells the master that the replica doesn't have data. 
                    # send_empty_rdb is sending an empty file (for this exercise) to fully synchronize
                    send_empty_rdb(client)
                    # delay briefly to let replica read RDB before sending other commands
                    time.sleep(0.1)
                    # mark this socket as a ready replica
                    if store.role == "master": 
                        store.replica_sockets.append(client)
                        print("[Master] Registered a new replica socket")
                        print("[Master] Replica fully synced and registered")
                        
                        # sync all the previous commands with the current replica
                        for command in store.command_logs: 
                            try: 
                                client.sendall(command)
                            except Exception as e: 
                                print("[Master] Failed to replay command to replica: {e}")
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
        # client_sock are the client requests incoming to the server e.g. replica clients
        client_sock, client_addr = server_socket.accept()
        threading.Thread(target=handle_command, args=(client_sock, store, config)).start()


if __name__ == "__main__":
    main()
