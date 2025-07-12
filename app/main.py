import socket  # noqa: F401


def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    connection, _ = server_socket.accept() 
    while True: 
        msg = connection.recv(512)
        data = msg.decode()
        
        if "ping" in data: 
            connection.send("+PONG\n\r\n".encode())


if __name__ == "__main__":
    main()
