import socket
import threading
from app.util import int_to_var_int
from app.metadata import MetadataFile

def read_int_from_socket(s, num_bytes):
    return int.from_bytes(s.recv(num_bytes), "big")
def read_varint_from_socket(s):
    continue_varint = True
    total_binary_str = ""
    while continue_varint:
        varint_byte = s.recv(1)[0]
        if varint_byte >> 7 == 0:
            continue_varint = False
        binary_str = "{0:b}".format(varint_byte)
        binary_str = ("0" * (8 - len(binary_str))) + binary_str
        total_binary_str = binary_str[1:] + total_binary_str
    return int(total_binary_str, 2)
def read_tag_buffer(s):
    tag_size = read_varint_from_socket(s)
    assert tag_size == 0
def read_compact_str(s):
    size = read_varint_from_socket(s) - 1
    if size <= 1:
        return ""
    return s.recv(size).decode("utf-8")
def read_nullable_str(s):
    size = read_int_from_socket(s, 2)
    if size == 0:
        return ""
    return s.recv(size).decode("utf-8")
class ApiVersion:
    def __init__(self, api_key, min_api_ver, max_api_ver):
        self.api_key = api_key
        self.min_api_ver = min_api_ver
        self.max_api_ver = max_api_ver
class DescribeTopicPartitions:
    def __init__(self, correlation_id):
        self.correlation_id = correlation_id
    def create_message(self):
        message = KafkaResponseMessage(self.correlation_id, 0)
        return message.create_message()
class APIVersionsRequest:
    def __init__(self, correlation_id):
        self.correlation_id = correlation_id
        self.api_key = 18
    def create_message(self):
        message = KafkaResponseMessage(self.correlation_id, 0)
        api_versions = [ApiVersion(18, 0, 4), ApiVersion(75, 0, 0)]
        # add element size
        message.add_var_int_to_message(len(api_versions) + 1)
        for api_version in api_versions:
            # api key
            message.add_int_to_message(api_version.api_key, 2)
            # min api version
            message.add_int_to_message(api_version.min_api_ver, 2)
            # max api version
            message.add_int_to_message(api_version.max_api_ver, 2)
            # tag buff
            message.add_var_int_to_message(0)
        # throttle time ms
        message.add_int_to_message(0, 4)
        # no tag buffer
        message.add_var_int_to_message(0)
        return message.create_message()
class KafkaReceivedMessage:
    def __init__(self, correlation_id, api_key, request_api_version):
        self.correlation_id = correlation_id
        self.api_key = api_key
        self.request_api_version = request_api_version
class KafkaResponseMessage:
    def __init__(self, correlation_id, error_code=None):
        self.correlation_id = correlation_id
        self.error_code = error_code
        self.message_size = 0
        self.message = bytearray()
        self.init_message()
    def init_message(self):
        correlation_id_message = self.correlation_id.to_bytes(4, "big")
        self.message.extend(correlation_id_message)
        self.message_size += 4
        if self.error_code is not None:
            error_bytes = self.error_code.to_bytes(2, "big")
            self.message_size += 2
            self.message.extend(error_bytes)
    def add_int_to_message(self, int_message, num_bytes, endian="big"):
        self.message.extend(int_message.to_bytes(num_bytes, endian))
        self.message_size += num_bytes
    def add_bytearray(self, b):
        self.message.extend(b)
        self.message_size += len(b)
    def add_var_int_to_message(self, n):
        b = int_to_var_int(n)
        self.message.extend(b)
        self.message_size += len(b)
    def add_compact_nullable_str_to_message(self, s):
        varint_size = int_to_var_int(len(s) + 1)
        self.message_size += len(varint_size)
        self.message_size += len(s)
        self.message.extend(varint_size)
        self.message.extend(s.encode())
    def add_boolean_to_message(self, boolean_val):
        if boolean_val:
            self.message.extend([1])
        else:
            self.message.extend([0])
        self.message_size += 1
    def create_message(self):
        message_size_message = self.message_size.to_bytes(4, "big")
        res_message = message_size_message + self.message
        return res_message
class DescribeTopicPartitionsRequest:
    def __init__(
        self,
        correlation_id,
        api_key,
        request_api_version,
        topics,
        response_partition_limit,
        cursor_topic_name,
        cursor_partition_index,
    ):
        self.api_key = api_key
        self.correlation_id = correlation_id
        self.request_api_version = request_api_version
        self.topics = topics
        self.response_partition_limit = response_partition_limit
        self.cursor_topic_name = cursor_topic_name
        self.cursor_partition_index = cursor_partition_index
class DescribeTopicPartitionsResponse:
    def __init__(self, correlation_id, topics, kafka_metadata_file):
        self.correlation_id = correlation_id
        self.topics = topics
        self.kafka_metadata_file = kafka_metadata_file
    def create_message(self):
        message = KafkaResponseMessage(self.correlation_id, None)
        # tag buffer
        message.add_var_int_to_message(0)
        # throttle time
        message.add_int_to_message(0, 4)
        message.add_var_int_to_message(len(self.topics) + 1)
        metadata_topics = self.kafka_metadata_file.topic_map
        for topic_name in self.topics:
            if topic_name not in metadata_topics:
                # error code
                message.add_int_to_message(3, 2)
                message.add_compact_nullable_str_to_message(topic_name)
                # topic_id = "00000000000000000000000000000000"
                message.add_int_to_message(0, 16)
                message.add_boolean_to_message(True)
                # no partitions
                message.add_var_int_to_message(1)
                # topic_authorized_operations
                message.add_int_to_message(0, 4)
                # tag buffer
                message.add_var_int_to_message(0)
            else:
                topic_uuid = metadata_topics[topic_name]
                # error code
                message.add_int_to_message(0, 2)
                message.add_compact_nullable_str_to_message(topic_name)
                # topic_id = "00000000000000000000000000000000"
                message.add_int_to_message(topic_uuid, 16)
                message.add_boolean_to_message(True)
                message.add_var_int_to_message(
                    len(
                        self.kafka_metadata_file.topic_uuid_to_partition_map[topic_uuid]
                    )
                    + 1
                )
                # no partitions
                for partition in self.kafka_metadata_file.topic_uuid_to_partition_map[
                    topic_uuid
                ]:
                    message.add_bytearray(partition.get_partition_byte_representation())
                # topic_authorized_operations
                message.add_int_to_message(0, 4)
                # tag buffer
                message.add_var_int_to_message(0)
        # cursor
        message.add_int_to_message(255, 1)
        # tag buffer
        message.add_var_int_to_message(0)
        return message.create_message()
def process_describe_partitions_req(s, correlation_id, api_key, request_api_version):
    topics = []
    arr_size = read_varint_from_socket(s)
    for _ in range(arr_size - 1):
        topics.append(read_compact_str(s))
        read_tag_buffer(s)
    # response_partition_limit = read_int_from_socket(s, 4)
    # topic_name = read_compact_str(s)
    # partition_index = read_int_from_socket(s, 4)
    response_partition_limit = None
    topic_name = None
    partition_index = None
    return DescribeTopicPartitionsRequest(
        correlation_id,
        api_key,
        request_api_version,
        topics,
        response_partition_limit,
        topic_name,
        partition_index,
    )
def get_kafka_message(client_socket):
    msg_size = int.from_bytes(client_socket.recv(4), "big")
    request_api_key = int.from_bytes(client_socket.recv(2), "big")
    msg_size -= 2
    request_api_version = int.from_bytes(client_socket.recv(2), "big")
    msg_size -= 2
    correlation_id = int.from_bytes(client_socket.recv(4), "big")
    msg_size -= 4
    client_id = read_nullable_str(client_socket)
    msg_size -= 2 + len(client_id)
    read_tag_buffer(client_socket)
    msg_size -= 1
    if request_api_key == 75:
        return process_describe_partitions_req(
            client_socket, correlation_id, request_api_key, request_api_version
        )
    else:
        rest = client_socket.recv(msg_size)
    return KafkaReceivedMessage(correlation_id, request_api_key, request_api_version)
def get_resp_given_recv(kafka_rcv_msg, kafka_metadata_file):
    correlation_id = kafka_rcv_msg.correlation_id
    error_code = None
    if kafka_rcv_msg.request_api_version > 4:
        return KafkaResponseMessage(correlation_id, 35)
    if kafka_rcv_msg.api_key == 18:
        return APIVersionsRequest(correlation_id)
    if kafka_rcv_msg.api_key == 75:
        return DescribeTopicPartitionsResponse(
            correlation_id, kafka_rcv_msg.topics, kafka_metadata_file
        )
    return KafkaResponseMessage(correlation_id, error_code)
def handle_kafka_client(client_socket, kafka_metadata_file):
    while True:
        kafka_rcv_message = get_kafka_message(client_socket)
        kafka_message = get_resp_given_recv(kafka_rcv_message, kafka_metadata_file)
        client_socket.send(kafka_message.create_message())
def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")
    # Uncomment this to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    kafka_metadata_file = MetadataFile(
        "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
    )
    # kafka_metadata_file = None
    while True:
        client_socket, addr = server.accept()  # wait for client
        t = threading.Thread(
            target=handle_kafka_client, args=(client_socket, kafka_metadata_file)
        )
        t.start()
if __name__ == "__main__":
    main()