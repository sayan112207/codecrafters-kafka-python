import socket  # noqa: F401
# import struct
# import sys

# def create_message(correlation_id: int, error_code: int | None = None) -> bytes:
#     message = correlation_id.to_bytes(4, byteorder="big", signed=True)
#     if error_code is not None:
#         message += error_code.to_bytes(2, byteorder="big", signed=True)
#     message_len = len(message).to_bytes(4, byteorder="big", signed=False)
#     return message_len + message

# def parse_request(request: bytes) -> dict[str, int | str]:
#     buff_size = struct.calcsize(">ihhi")
#     length, api_key, api_version, correlation_id = struct.unpack(
#         ">ihhi", request[0:buff_size]
#     )
#     return {
#         "length": length,
#         "api_key": api_key,
#         "api_version": api_version,
#         "correlation_id": correlation_id,
#     }

from dataclasses import dataclass
from enum import Enum, unique
@unique
class ErrorCode(Enum):
    NONE = 0
    UNSUPPORTED_VERSION = 35
@dataclass
class KafkaRequest:
    api_key: int
    api_version: int
    correlation_id: int
    @staticmethod
    def from_client(client: socket.socket):
        data = client.recv(2048)
        return KafkaRequest(
            api_key=int.from_bytes(data[4:6]),
            api_version=int.from_bytes(data[6:8]),
            correlation_id=int.from_bytes(data[8:12]),
        )
    
def make_response(request: KafkaRequest):
    response_header = request.correlation_id.to_bytes(4)
    valid_api_versions = [0, 1, 2, 3, 4]
    error_code = (
        ErrorCode.NONE
        if request.api_version in valid_api_versions
        else ErrorCode.UNSUPPORTED_VERSION
    )
    min_version, max_version = 0, 4
    throttle_time_ms = 0
    tag_buffer = b"\x00"
    response_body = (
        error_code.value.to_bytes(2)
        + int(2).to_bytes(1)
        + request.api_key.to_bytes(2)
        + min_version.to_bytes(2)
        + max_version.to_bytes(2)
        + tag_buffer
        + throttle_time_ms.to_bytes(4)
        + tag_buffer
    )
    response_length = len(response_header) + len(response_body)
    return int(response_length).to_bytes(4) + response_header + response_body

def main():
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    client, _ = server.accept()
    # request = client.recv(1024)
    # request_data = parse_request(request)
    # #server.accept() # wait for client

    # if 0 <= request_data["api_version"] <= 4:
    #     message = create_message(request_data["correlation_id"])
    # else:
    #     message = create_message(
    #         request_data["correlation_id"], 35
    #     )

    # client.sendall(message)
    # client.close()
    # server.close()

    while True:
        request = KafkaRequest.from_client(client)
        print(request)
        client.sendall(make_response(request))


if __name__ == "__main__":
    main()