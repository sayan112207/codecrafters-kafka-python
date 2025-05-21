import socket  # noqa: F401

def create_message(id):
    id_bytes = id.to_bytes(4, byteorder="big")
    return len(id_bytes).to_bytes(4, byteorder="big") + id_bytes

def handle_client(client):
    client.recv(1024)
    client.sendall(create_message(7))
    client.close()

def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    #server.accept() # wait for client

    while True:
        client, addr = server.accept()
        handle_client(client)


if __name__ == "__main__":
    main()
