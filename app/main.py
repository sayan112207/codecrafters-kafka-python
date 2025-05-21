import socket
import threading

NO_ERROR = 0


class RequestValidationException(Exception):
    code: int
    message: str

class ApiVersionInvalidException(RequestValidationException):
    code = 35
    message = "UNSUPPORTED_VERSION"

class Request:
    request_api_key: int
    request_api_version: int
    correlation_id: int
    client_id: str | None
    tagged_fields: list[str] | None

    def __init__(self,request_api_key: int,request_api_version: int,correlation_id: int,client_id: str,):
        self.request_api_key = request_api_key
        self.request_api_version = request_api_version
        self.correlation_id = correlation_id
        self.client_id = client_id

    def validate(self):
        if self.request_api_version not in [0, 1, 2, 3, 4]:
            raise ApiVersionInvalidException
        
def parse_request_length(header: bytes) -> int:
    return int.from_bytes(header, byteorder="big", signed=True)

def parse_request(request: bytes) -> Request:
    request_api_key = int.from_bytes(request[:2], byteorder="big", signed=True)
    request_api_version = int.from_bytes(request[2:4], byteorder="big", signed=True)
    correlation_id = int.from_bytes(request[4:8], byteorder="big", signed=True)
    client_id = bytes.decode(request[8:], "utf-8")
    print("request_api_key, request_api_version, correlation_id, client_id ====== ")
    print(request_api_key, request_api_version, correlation_id, client_id)
    return Request(request_api_key, request_api_version, correlation_id, client_id)

def create_response(request: Request) -> bytes:
    message_bytes = request.correlation_id.to_bytes(4, byteorder="big", signed=True)
    min_version, max_version = 0, 4
    throttle_time_ms = 0
    tag_buffer = b"\x00"
    try:
        request.validate()
        error_bytes = NO_ERROR.to_bytes(2, byteorder="big", signed=True)
    except RequestValidationException as ex:
        error_bytes = ex.code.to_bytes(2, byteorder="big", signed=True)
    message_bytes += error_bytes
    message_bytes += int(3).to_bytes(1, byteorder="big", signed=True)
    # if request.request_api_key == 18:
    message_bytes += request.request_api_key.to_bytes(2, byteorder="big", signed=True)
    message_bytes += min_version.to_bytes(2, byteorder="big", signed=True)
    message_bytes += max_version.to_bytes(2, byteorder="big", signed=True)
    message_bytes += tag_buffer
    # elif request.request_api_key == 75:
    message_bytes += int(75).to_bytes(2, byteorder="big", signed=True)
    message_bytes += int(0).to_bytes(2, byteorder="big", signed=True)
    message_bytes += int(0).to_bytes(2, byteorder="big", signed=True)
    message_bytes += tag_buffer
    message_bytes += throttle_time_ms.to_bytes(4, byteorder="big", signed=True)
    message_bytes += tag_buffer
    req_len = len(message_bytes).to_bytes(4, byteorder="big", signed=True)
    response = req_len + message_bytes
    return response

def handler(client_conn, addr):
    while True:
        message_len = parse_request_length(client_conn.recv(4))
        print(f"message_len {message_len}")
        request_bytes = client_conn.recv(message_len + 8)
        print(f"request_bytes are {request_bytes}")
        print(int.from_bytes(request_bytes, byteorder="big", signed=True))
        # create response
        response = create_response(parse_request(request_bytes))
        # send response
        client_conn.sendall(response)
        
def main():
    with socket.create_server(("localhost", 9092), reuse_port=True) as server:
        # client_conn, addr = server.accept()
        while True:
            client_conn, addr = server.accept()
            # receive
            thread = threading.Thread(
                target=handler, args=(client_conn, addr), daemon=True
            )
            thread.start()

if __name__ == "__main__":
    main()