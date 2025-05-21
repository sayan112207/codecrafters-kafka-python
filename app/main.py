import socket
import threading

def response_api_key_75(id, cursor, array_length, length, topic_name):
    tag_buffer = b"\x00"
    response_header = id.to_bytes(4, byteorder="big") + tag_buffer
    error_code = int(3).to_bytes(2, byteorder="big")
    throttle_time_ms = int(0).to_bytes(4, byteorder="big")
    is_internal = int(0).to_bytes(1, byteorder="big")
    topic_authorized_operations = b"\x00\x00\x0d\xf8"
    topic_id = int(0).to_bytes(16, byteorder="big")
    partition_array = b"\x01"

    response_body = (
        throttle_time_ms
        + int(array_length).to_bytes(1, byteorder="big")
        + error_code
        + int(length).to_bytes(1, byteorder="big")
        + topic_name
        + topic_id
        + is_internal
        + partition_array
        + topic_authorized_operations
        + tag_buffer
        + cursor
        + tag_buffer
    )

    total_len = len(response_header) + len(response_body)
    return int(total_len).to_bytes(4, byteorder="big") + response_header + response_body

def create_msg(id, api_key: int, error_code: int = 0):
    response_header = id.to_bytes(4, byteorder="big")
    err = error_code.to_bytes(2, byteorder="big")
    api_key_bytes = api_key.to_bytes(2, byteorder="big")
    min_version_api_18, max_version_api_18, min_version_api_75, max_version_api_75 = (
        0,
        4,
        0,
        0,
    )
    tag_buffer = b"\x00"
    num_api_keys = int(3).to_bytes(1, byteorder="big")
    describle_topic_partition_api = int(75).to_bytes(2, byteorder="big")
    throttle_time_ms = 0
    response_body = (
        err
        + num_api_keys
        + api_key_bytes
        + min_version_api_18.to_bytes(2)
        + max_version_api_18.to_bytes(2)
        + tag_buffer
        + describle_topic_partition_api
        + min_version_api_75.to_bytes(2)
        + max_version_api_75.to_bytes(2)
        + tag_buffer
        + throttle_time_ms.to_bytes(4, byteorder="big")
        + tag_buffer
    )
    total_len = len(response_header) + len(response_body)
    return int(total_len).to_bytes(4, byteorder="big") + response_header + response_body

def handle(client):
    try:
        while True:
            req = client.recv(1024)
            if not req:
                break
            api_key = int.from_bytes(req[4:6], byteorder="big")
            api_version = int.from_bytes(req[6:8], byteorder="big")
            Coreleation_ID = int.from_bytes(req[8:12], byteorder="big")
            if api_key == 75:
                client_id_len = int.from_bytes(req[12:14])
                print(client_id_len)
                if client_id_len > 0:
                    cliend_id = req[14 : 14 + client_id_len]
                    tagged = req[14 + client_id_len]
                else:
                    cliend_id = ""
                    tagged = [14]
                array_len_finder = 14 + client_id_len + 1
                array_length = req[array_len_finder]
                topic_name_length = req[array_len_finder + 1]
                topic_name_starter = array_len_finder + 2
                topic_name = bytes(
                    req[
                        topic_name_starter : topic_name_starter
                        + (topic_name_length - 1)
                    ]
                )
                cursor_length = topic_name_starter + topic_name_length + 4
                cursor = req[cursor_length]
                cursor_bytes = int(cursor).to_bytes(1, byteorder="big")
                response = response_api_key_75(
                    Coreleation_ID,
                    cursor_bytes,
                    array_length,
                    topic_name_length,
                    topic_name,
                )
                client.sendall(response)
            else:
                version = {0, 1, 2, 3, 4}
                error_code = 0 if api_version in version else 35
                response = create_msg(Coreleation_ID, api_key, error_code)
                client.sendall(response)
    except Exception as e:
        print(f"Except Error Handling Client: {e}")
    finally:
        client.close()

def main():
    print("Logs from your program will appear here!")
    
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    
    while True:
        client, addr = server.accept()
        client_Thread = threading.Thread(target=handle, args=(client,))
        client_Thread.start()

if __name__ == "__main__":
    main()