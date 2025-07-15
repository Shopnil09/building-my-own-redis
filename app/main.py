import socket
import threading  

BUFF_SIZE = 4096

def parse_redis_command(data: bytes): 
    lines = data.decode().split("\r\n")
    args = []
    i = 0
    while i < len(lines): 
        if lines[i].startswith("*") or lines[i].startswith("$") or lines[i] == "": 
            i += 1
            continue 
        
        args.append(lines[i]) 
        i += 1
    
    return args

def handle_command(client: socket.socket): 
    while True: 
        chunk = client.recv(BUFF_SIZE)
        print("Raw chunk received", chunk)
        if not chunk: 
            break
        
        args = parse_redis_command(chunk)
        print("Parsed command:", args)
        
        if not args:
            client.send(b"-ERR unknown command\r\n")
            continue
        if args[0].upper() == "PING": 
            response = f"+PONG\r\n"
            client.send(response.encode())
        elif args[0].upper() == "ECHO" and len(args) == 2: 
            response = f"${len(args[1])}\r\n{args[1]}\r\n"
            client.send(response.encode())
        else: 
            client.send(b"-ERR unknown command\r\n")

def main():
    print("Started....")
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True: 
        client_sock, client_addr = server_socket.accept()
        threading.Thread(target=handle_command, args=(client_sock,)).start()


if __name__ == "__main__":
    main()
