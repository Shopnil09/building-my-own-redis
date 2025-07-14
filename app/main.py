import socket
import threading  

BUFF_SIZE = 4096

def handle_command(client: socket.socket): 
    while True: 
        chunk = client.recv(BUFF_SIZE)
        if not chunk: 
            break
        
        client.send(b"+PONG\r\n")

def main():
    print("Started....")
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True: 
        client_sock, client_addr = server_socket.accept()
        threading.Thread(target=handle_command, args=(client_sock,)).start()


if __name__ == "__main__":
    main()
