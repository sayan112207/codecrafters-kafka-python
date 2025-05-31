import asyncio
import json
import socket
import uuid
from abc import ABC, abstractmethod
from enum import Enum
from os import path
from struct import pack, unpack, calcsize
from typing import BinaryIO, Any
error_codes = {
    "NONE": 0, 
    "UNKNOWN_TOPIC_OR_PARTITION": 3, 
    "UNKNOWN_TOPIC": 100, 
    "UNSUPPORTED_VERSION": 35}
supported_api_version = list(range(5))
supported_API_keys = {
    1: {"name": "Fetch", "min": 0, "max": 16},  # Add this line
    18: {"name": "APIVersions", "min": 0, "max": 4},
    75: {"name": "DescribeTopicPartitions", "min": 0, "max": 0},
}
min_version, max_version = 0, 4
tag_buffer = 0
throttle_time_ms = 0
port = 9092
sizes = {}
path_to_logs = "/tmp/kraft-combined-logs/__cluster_metadata-0/"
log_file = "00000000000000000000.log"
DEBUG = False
class BaseBinaryHandler(ABC):
    """Abstract class for handling incoming data"""
    @abstractmethod
    async def prepare_response_body(parsed_request: bytes) -> dict:
        pass
class Utilities:
    @staticmethod
    def display(data_dict: dict, msg: str) -> None:
        print(
            f"===============================BEGINNING OF {msg} ====================================="
        )
        print(
            json.dumps(
                data_dict,
                indent=4,
            )
        )
        print(
            f"===============================END OF {msg} ====================================="
        )
    @staticmethod
    def check_api_version(request_api_version):
        return (
            error_codes["NONE"]
            if request_api_version in supported_api_version
            else error_codes["UNSUPPORTED_VERSION"]
        )
    @staticmethod
    def unpack_helper(format, data):
        size = calcsize(format)
        return unpack(format, data[:size]), data[size:]
    @staticmethod
    def stringify(list_of_ascii_codes: tuple) -> str:
        return "".join(chr(c) for c in list_of_ascii_codes)
    @staticmethod
    def decode_zigzag_to_signed(n: int) -> int:
        return (n >> 1) - (n & 1) * n
    @staticmethod
    def test_msb(b: bytes) -> bool:
        int_b = int.from_bytes(b)
        if int_b >= 64:
            return True
        else:
            return False
    @staticmethod
    def read_varint(f: BinaryIO, signed: bool = True) -> int:
        """VARINT processing
        Args:
            f (BinaryIO): file descriptor
            signed (bool, optional): whether the varint is signed or not. Defaults to True.
        Returns:
            int: the signed or unsigned varint
        """
        val = ""
        while True:
            b = f.read(MetadataLogFile.R_KEY_LENGTH.value)
            num = bin(int.from_bytes(b))[2:].zfill(8)
            val = num[1:] + val
            if Utilities.test_msb(b) is False:
                break
        # SIGNED -> zigzag processing / UNSIGNED -> just the int convertion
        val = Utilities.decode_zigzag_to_signed(int(val, 2)) if signed else int(val, 2)
        return val
class MetadataLogFile(Enum):
    BASE_OFFSET_SIZE = 8
    BATCH_LENGTH_SIZE = 4
    PARTITION_LEADER_EPOCH = 4
    MAGIC_BYTE = 1
    CRC = 4
    ATTRIBUTES = 2
    LAST_OFFSET_DELTA = 4
    BASE_TIMESTAMP = 8
    MAX_TIMESTAMP = 8
    PRODUCER_ID = 8
    PRODUCER_EPOCH = 2
    BASE_SEQUENCE = 4
    RECORDS_LENGTH = 4
    # indivudual record constants :
    R_LENGTH = 1  # VARINT
    R_ATTRIBUTES = 1
    R_TIMESTAMP_DELTA = 1
    R_OFFSET_DELTA = 1
    R_KEY_LENGTH = 1
    R_VALUE_LENGTH = 1
    # Value in record constants
    R_V_FRAME_VERSION = 1
    R_V_TYPE = 1
    R_V_VERSION = 1
    R_V_NAME_LENGTH = 1
    R_V_FEATURE_LEVEL = 2
    R_V_TAGGED_FIELDS_COUNTS = 1
    R_V_TOPIC_UUID = 16
    R_V_PARTITION_ID = 4
    R_V_LENGTH_OF_REPLICA_ARRAY = 1
    R_V_REPLICA_ARRAY = 4
    R_HEADERS_ARRAY_COUNT = 1
class MetaDataLog:
    def __init__(self, file_name):
        self.file_name = file_name
        self.log = {}
        self.parse_common_structure()
    def parse_common_structure(self) -> None:
        """
        Parse path_to_logs + self.file_name binary file and fill the self.log dictionnary with the info retrieved.
        """
        with open(path_to_logs + self.file_name, "rb") as f:
            if DEBUG:
                print(f"----file {self.file_name} content : {f.read().hex(':')}---")
            f.seek(0, 2)
            file_size = f.tell() - 1
            f.seek(0)
            if DEBUG:
                print(f"File size : {file_size}")
            Record_Batch = 1
            while f.tell() < file_size:
                if DEBUG:
                    print(f"File position begining of new batch : {f.tell()}")
                self.log[f"Record Batch #{Record_Batch}"] = {}
                self.log[f"Record Batch #{Record_Batch}"]["Base Offset"] = (
                    int.from_bytes(
                        f.read(MetadataLogFile.BASE_OFFSET_SIZE.value), byteorder="big"
                    )
                )
                self.log[f"Record Batch #{Record_Batch}"]["Batch Length"] = (
                    int.from_bytes(
                        f.read(MetadataLogFile.BATCH_LENGTH_SIZE.value), byteorder="big"
                    )
                )
                self.log[f"Record Batch #{Record_Batch}"]["Partition Leader Epoch"] = (
                    int.from_bytes(
                        f.read(MetadataLogFile.PARTITION_LEADER_EPOCH.value),
                        byteorder="big",
                    )
                )
                self.log[f"Record Batch #{Record_Batch}"]["Magic Byte"] = (
                    int.from_bytes(
                        f.read(MetadataLogFile.MAGIC_BYTE.value), byteorder="big"
                    )
                )
                self.log[f"Record Batch #{Record_Batch}"]["CRC"] = int.from_bytes(
                    f.read(MetadataLogFile.CRC.value), byteorder="big", signed=True
                )
                self.log[f"Record Batch #{Record_Batch}"]["Attributes"] = (
                    int.from_bytes(
                        f.read(MetadataLogFile.ATTRIBUTES.value), byteorder="big"
                    )
                )
                self.log[f"Record Batch #{Record_Batch}"]["Last Offset Delta"] = (
                    int.from_bytes(
                        f.read(MetadataLogFile.LAST_OFFSET_DELTA.value), byteorder="big"
                    )
                )
                self.log[f"Record Batch #{Record_Batch}"]["Base Timestamp"] = (
                    int.from_bytes(
                        f.read(MetadataLogFile.BASE_TIMESTAMP.value), byteorder="big"
                    )
                )
                self.log[f"Record Batch #{Record_Batch}"]["Max Timestamp"] = (
                    int.from_bytes(
                        f.read(MetadataLogFile.MAX_TIMESTAMP.value), byteorder="big"
                    )
                )
                self.log[f"Record Batch #{Record_Batch}"]["Producer ID"] = (
                    int.from_bytes(
                        f.read(MetadataLogFile.PRODUCER_ID.value),
                        byteorder="big",
                        signed=True,
                    )
                )
                self.log[f"Record Batch #{Record_Batch}"]["Producer Epoch"] = (
                    int.from_bytes(
                        f.read(MetadataLogFile.PRODUCER_EPOCH.value), byteorder="big"
                    )
                )
                self.log[f"Record Batch #{Record_Batch}"]["Base Sequence"] = (
                    int.from_bytes(
                        f.read(MetadataLogFile.BASE_SEQUENCE.value), byteorder="big"
                    )
                )
                self.log[f"Record Batch #{Record_Batch}"]["Records Length"] = (
                    int.from_bytes(
                        f.read(MetadataLogFile.RECORDS_LENGTH.value), byteorder="big"
                    )
                )
                # Records parsing
                for record in range(
                    self.log[f"Record Batch #{Record_Batch}"]["Records Length"]
                ):
                    if DEBUG:
                        print(
                            f"File position begining of new record in batch : {f.tell()}"
                        )
                    self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"] = {}
                    # next field is a varint ...
                    self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"][
                        "Length"
                    ] = Utilities.read_varint(f, signed=True)
                    if DEBUG:
                        print(
                            f' self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"]["Length"] {self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"]["Length"]}'
                        )
                    pos = f.tell()
                    self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"][
                        "Attributes"
                    ] = int.from_bytes(
                        f.read(MetadataLogFile.R_ATTRIBUTES.value), byteorder="big"
                    )
                    self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"][
                        "Timestamp Delta"
                    ] = int.from_bytes(
                        f.read(MetadataLogFile.R_TIMESTAMP_DELTA.value), byteorder="big"
                    )
                    self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"][
                        "Offset Delta"
                    ] = int.from_bytes(
                        f.read(MetadataLogFile.R_OFFSET_DELTA.value), byteorder="big"
                    )
                    # next field is a varint ...
                    self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"][
                        "Key Length"
                    ] = Utilities.read_varint(f, signed=True)
                    if DEBUG:
                        print(
                            f'self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"]["Key Length"] : {self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"]["Key Length"]}'
                        )
                    if (
                        self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"][
                            "Key Length"
                        ]
                        != -1
                    ):
                        self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"][
                            "Key"
                        ] = 0  # not implemented
                    else:
                        self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"][
                            "Key"
                        ] = 0
                    self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"][
                        "Value Length"
                    ] = Utilities.read_varint(f, signed=True)
                    # self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"]["Value Length"] = int.from_bytes(f.read(MetadataLogFile.R_VALUE_LENGTH.value), byteorder="big")
                    if DEBUG:
                        print(
                            f'self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"]["Value Length"] : {self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"]["Value Length"]}'
                        )
                    # Value in record parsing
                    self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"][
                        "Value"
                    ] = {}
                    self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"][
                        "Value"
                    ]["Frame Version"] = int.from_bytes(
                        f.read(MetadataLogFile.R_V_FRAME_VERSION.value), byteorder="big"
                    )
                    self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"][
                        "Value"
                    ]["Type"] = int.from_bytes(
                        f.read(MetadataLogFile.R_V_TYPE.value), byteorder="big"
                    )
                    if DEBUG:
                        print(
                            f'self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"]["Value"]["Type"]  == {self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"]["Value"]["Type"]}'
                        )
                    match self.log[f"Record Batch #{Record_Batch}"][
                        f"Record #{record}"
                    ]["Value"]["Type"]:
                        case 2:
                            # Topic Record
                            self.log[f"Record Batch #{Record_Batch}"][
                                f"Record #{record}"
                            ]["Value"]["Version"] = int.from_bytes(
                                f.read(MetadataLogFile.R_V_VERSION.value),
                                byteorder="big",
                            )
                            self.log[f"Record Batch #{Record_Batch}"][
                                f"Record #{record}"
                            ]["Value"]["Name_Length"] = Utilities.read_varint(
                                f, signed=False
                            )
                            if DEBUG:
                                print(
                                    f'self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"]["Value"]["Name_Length"] : {self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"]["Value"]["Name_Length"]}'
                                )
                            # self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"]["Value"]["Name_Length"] = int.from_bytes(f.read(MetadataLogFile.R_V_NAME_LENGTH.value), byteorder="big")
                            self.log[f"Record Batch #{Record_Batch}"][
                                f"Record #{record}"
                            ]["Value"]["Topic Name"] = int.from_bytes(
                                f.read(
                                    self.log[f"Record Batch #{Record_Batch}"][
                                        f"Record #{record}"
                                    ]["Value"]["Name_Length"]
                                    - 1
                                ),
                                byteorder="big",
                            )
                            self.log[f"Record Batch #{Record_Batch}"][
                                f"Record #{record}"
                            ]["Value"]["Topic UUID"] = int.from_bytes(
                                f.read(MetadataLogFile.R_V_TOPIC_UUID.value),
                                byteorder="big",
                            )
                            self.log[f"Record Batch #{Record_Batch}"][
                                f"Record #{record}"
                            ]["Value"]["Tagged Fields Counts"] = int.from_bytes(
                                f.read(MetadataLogFile.R_V_TAGGED_FIELDS_COUNTS.value),
                                byteorder="big",
                            )
                            # TODO : implement tagged fields
                        case 3:
                            # Partition Record
                            self.log[f"Record Batch #{Record_Batch}"][
                                f"Record #{record}"
                            ]["Value"]["Version"] = int.from_bytes(
                                f.read(MetadataLogFile.R_V_VERSION.value),
                                byteorder="big",
                            )
                            self.log[f"Record Batch #{Record_Batch}"][
                                f"Record #{record}"
                            ]["Value"]["Partition ID"] = int.from_bytes(
                                f.read(MetadataLogFile.R_V_PARTITION_ID.value),
                                byteorder="big",
                            )
                            self.log[f"Record Batch #{Record_Batch}"][
                                f"Record #{record}"
                            ]["Value"]["Topic UUID"] = int.from_bytes(
                                f.read(MetadataLogFile.R_V_TOPIC_UUID.value),
                                byteorder="big",
                            )
                            self.log[f"Record Batch #{Record_Batch}"][
                                f"Record #{record}"
                            ]["Value"]["Replica Array Length"] = int.from_bytes(
                                f.read(
                                    MetadataLogFile.R_V_LENGTH_OF_REPLICA_ARRAY.value
                                ),
                                byteorder="big",
                            )
                            self.log[f"Record Batch #{Record_Batch}"][
                                f"Record #{record}"
                            ]["Value"]["Replica Array"] = int.from_bytes(
                                f.read(MetadataLogFile.R_V_REPLICA_ARRAY.value),
                                byteorder="big",
                            )
                        case 12:
                            # Feature Level Record
                            self.log[f"Record Batch #{Record_Batch}"][
                                f"Record #{record}"
                            ]["Value"]["Version"] = int.from_bytes(
                                f.read(MetadataLogFile.R_V_VERSION.value),
                                byteorder="big",
                            )
                            val = ""
                            while True:
                                b = f.read(MetadataLogFile.R_V_NAME_LENGTH.value)
                                num = bin(int.from_bytes(b))[2:].zfill(8)
                                val = num[1:] + val
                                if Utilities.test_msb(b) is False:
                                    break
                            self.log[f"Record Batch #{Record_Batch}"][
                                f"Record #{record}"
                            ]["Value"]["Name_Length"] = int(val, 2)
                            # self.log[f"Record Batch #{Record_Batch}"][f"Record #{record}"]["Value"]["Name_Length"] = int.from_bytes(f.read(MetadataLogFile.R_V_NAME_LENGTH.value), byteorder="big")
                            self.log[f"Record Batch #{Record_Batch}"][
                                f"Record #{record}"
                            ]["Value"]["Name"] = int.from_bytes(
                                f.read(
                                    self.log[f"Record Batch #{Record_Batch}"][
                                        f"Record #{record}"
                                    ]["Value"]["Name_Length"]
                                    - 1
                                ),
                                byteorder="big",
                            )
                            self.log[f"Record Batch #{Record_Batch}"][
                                f"Record #{record}"
                            ]["Value"]["Feature Level"] = int.from_bytes(
                                f.read(MetadataLogFile.R_V_FEATURE_LEVEL.value),
                                byteorder="big",
                            )
                            self.log[f"Record Batch #{Record_Batch}"][
                                f"Record #{record}"
                            ]["Value"]["Tagged Fields Counts"] = int.from_bytes(
                                f.read(MetadataLogFile.R_V_TAGGED_FIELDS_COUNTS.value),
                                byteorder="big",
                            )
                            # TODO : implement tagged fields
                        case _:
                            if DEBUG:
                                print("UNSUPPORTED")
                            # unsupported
                            self.log[f"Record Batch #{Record_Batch}"][
                                f"Record #{record}"
                            ]["Value"]["unsupported"] = self.log[
                                f"Record Batch #{Record_Batch}"
                            ][
                                f"Record #{record}"
                            ][
                                "Value"
                            ][
                                "Type"
                            ]
                            # rewind until "Attributes"
                    f.seek(
                        pos
                        + self.log[f"Record Batch #{Record_Batch}"][
                            f"Record #{record}"
                        ]["Length"]
                    )
                Record_Batch += 1
    def __str__(self):
        return f"""===============================  BEGINNING OF parsed Metadata log file   =====================================  
        {json.dumps(self.log, indent=4)} \
        ===============================  END OF parsed Metadata log file  ====================================="""
    def find_partitions_details_for_topic(self, uuid_value: str) -> bool | list[Any]:
        """MetaDatalog method for returning partition details from a topic uuid
        Args:
            uuid_value (str): topic uuid
        Returns:
            dict | bool : dictionnary with partition ID, Replicas, etc - very partially implemented OR False if no partition found
        """
        partitions_found = []
        for record_batch, _content in self.log.items():
            # Record Batch level
            for i in range(self.log[record_batch]["Records Length"]):
                if self.log[record_batch][f"Record #{i}"]["Value"]["Type"] == 3:
                    if (
                        self.log[record_batch][f"Record #{i}"]["Value"]["Topic UUID"]
                        == uuid_value
                    ):
                        if DEBUG:
                            print("++++++++++PARTION RECORD FOUND++++++++++")
                        partitions_found.append(
                            (
                                self.log[record_batch][f"Record #{i}"]["Value"][
                                    "Partition ID"
                                ],
                                self.log[record_batch][f"Record #{i}"]["Value"][
                                    "Replica Array Length"
                                ],
                                self.log[record_batch][f"Record #{i}"]["Value"][
                                    "Replica Array"
                                ],
                            )
                        )
        if not partitions_found:
            if DEBUG:
                print(f"+++++++++++++TOPIC UUID {uuid_value} NOT FOUND ")
            return False
        return partitions_found
    def find_topic(self, topic_name: str) -> int | bool:
        """MetaDatalog method for returning a Topic UUID if a topic name
        Args:
            topic_name (str)
        Returns:
            Topic UUID in int format or False if not found
        """
        if DEBUG:
            print(f"SEARCHING {topic_name}")
        for record_batch, _content in self.log.items():
            # Record Batch level
            for i in range(self.log[record_batch]["Records Length"]):
                if self.log[record_batch][f"Record #{i}"]["Value"]["Type"] == 2:
                    if (
                        self.log[record_batch][f"Record #{i}"]["Value"]["Topic Name"]
                        == topic_name
                    ):
                        if DEBUG:
                            print("++++++++++TOPIC FOUND ++++++++++")
                        return self.log[record_batch][f"Record #{i}"]["Value"][
                            "Topic UUID"
                        ]
        if DEBUG:
            print(f"+++++++++++++TOPIC {topic_name} NOT FOUND ")
        return False
class DescribeTopicPartitions(BaseBinaryHandler):
    @staticmethod
    def parse_body(request_body: bytes) -> dict:
        parsed_body = {}
        (parsed_body["topics_array_length"],), _remaining = Utilities.unpack_helper(
            ">B", request_body
        )
        topic = []
        for _topic_number in range(parsed_body["topics_array_length"] - 1):
            # parse the topic name length first and then use this information to parse the topic name itself
            # the topic id is then created through uuid
            topic_name_length, _remaining = Utilities.unpack_helper(">B", _remaining)
            _topic, _remaining = Utilities.unpack_helper(
                f">{(topic_name_length[0]) - 1}B", _remaining
            )
            # print(f"topic name : { Utilities.stringify(_topic)}")
            topic.append(
                {
                    "topic_name": _topic,
                    "topic_name_length": topic_name_length[0],
                    "topic_name_id": tuple(
                        uuid.UUID(int=_topic_number).bytes
                    ),  # Fake value so that it can be iterated
                }
            )
            _tag_buffer, _remaining = Utilities.unpack_helper(">b", _remaining)
        parsed_body["topics"] = topic
        (
            (
                parsed_body["response_partition_limit"],
                parsed_body["cursor"],
                _tag_buffer,
            ),
            _remaining,
        ) = Utilities.unpack_helper(">IBB", _remaining)
        return parsed_body
    @staticmethod
    async def prepare_response_body(parsed_request):
        # for DescribeTopicPartitions, parsing the request body is required
        fields = DescribeTopicPartitions.parse_body(parsed_request.request_body)
        Utilities.display(fields, "DescribeTopicPartitions Parsed Request Body")
        _response = {"throttle_time": {"value": 0, "format": "I"}}
        # TODO correct this :
        _response["topic_array_length"] = {
            "value": fields["topics_array_length"],
            "format": "B",
        }
        fi = f"{path_to_logs}{log_file}"
        if path.isfile(fi) is False:
            if DEBUG:
                print("File Not Found \n\n")
            Found = False
            raise FileExistsError
        for topic in fields["topics"]:
            # common to all responses :
            log = MetaDataLog(log_file)
            ttopic = int.from_bytes(topic["topic_name"])
            Found = log.find_topic(ttopic)
            # for each topic, the response depends on whether the topic is found or not
            if Found:
                UUID_int = Found
                partitions = log.find_partitions_details_for_topic(UUID_int)
                _response[f"topic_{topic['topic_name_id']}_error_code"] = {
                    "value": int(error_codes["NONE"]),
                    "format": "H",
                }
                _response[f"topic_{topic['topic_name_id']}_name_length"] = {
                    "value": topic["topic_name_length"],
                    "format": "B",
                }
                _response[f"topic_{topic['topic_name_id']}_name"] = {
                    "value": topic["topic_name"],
                    "format": f"{topic['topic_name_length'] - 1}B",
                }
                _response[f"topic_{topic['topic_name_id']}_id"] = {
                    "value": tuple(uuid.UUID(int=UUID_int).bytes),
                    "format": "16B",
                }
                _response[f"topic_{topic['topic_name_id']}_is_internal"] = {
                    "value": 0,
                    "format": "B",
                }
                _response[f"topic_{topic['topic_name_id']}_partition_Array_Length"] = {
                    "value": len(partitions) + 1,
                    "format": "B",
                }
                for index, (
                    Partition_ID,
                    Replica_Array_Length,
                    Replica_Array,
                ) in enumerate(partitions):
                    _response[
                        f"topic_{topic['topic_name_id']}_partition_{Partition_ID} Error Code"
                    ] = {
                        "value": 0,
                        "format": "H",
                    }
                    _response[
                        f"topic_{topic['topic_name_id']}_partition_{Partition_ID} Partition Index"
                    ] = {"value": Partition_ID, "format": "I"}
                    _response[
                        f"topic_{topic['topic_name_id']}_partition_{Partition_ID} Leader ID"
                    ] = {
                        "value": 0,
                        "format": "I",
                    }
                    _response[
                        f"topic_{topic['topic_name_id']}_partition_{Partition_ID} Leader Epoch"
                    ] = {"value": 0, "format": "I"}
                    _response[
                        f"topic_{topic['topic_name_id']}_partition_{Partition_ID} Replica Nodes Length"
                    ] = {"value": Replica_Array_Length, "format": "B"}
                    _response[
                        f"topic_{topic['topic_name_id']}_partition_{Partition_ID} Replica Node_0"
                    ] = {"value": Replica_Array, "format": "I"}
                    _response[
                        f"topic_{topic['topic_name_id']}_partition_{Partition_ID} ISR Nodes Array Length"
                    ] = {"value": 2, "format": "B"}
                    _response[
                        f"topic_{topic['topic_name_id']}_partition_{Partition_ID} ISR Node_0"
                    ] = {
                        "value": 1,
                        "format": "I",
                    }
                    _response[
                        f"topic_{topic['topic_name_id']}_partition_{Partition_ID} Eligible Leader Replicas Array Length"
                    ] = {"value": 1, "format": "B"}
                    _response[
                        f"topic_{topic['topic_name_id']}_partition_{Partition_ID} Last Known ELR Array Length"
                    ] = {"value": 1, "format": "B"}
                    _response[
                        f"topic_{topic['topic_name_id']}_partition_{Partition_ID} Offline Replicas Array Length"
                    ] = {"value": 1, "format": "B"}
                    _response[
                        f"topic_{topic['topic_name_id']}_tag_buffer_partition_{Partition_ID}"
                    ] = {
                        "value": 0,
                        "format": "B",
                    }
                _response[
                    f"topic_{topic['topic_name_id']}_Topic_Authorized_Operations"
                ] = {"value": 0xDF8, "format": "I"}
                _response[f"topic_{topic['topic_name_id']}_tag_buffer_2"] = {
                    "value": 0,
                    "format": "B",
                }
            else:
                _response[f"topic_{topic['topic_name_id']}_error_code"] = {
                    "value": int(error_codes["UNKNOWN_TOPIC_OR_PARTITION"]),
                    "format": "H",
                }
                _response[f"topic_{topic['topic_name_id']}_name_length"] = {
                    "value": topic["topic_name_length"],
                    "format": "B",
                }
                _response[f"topic_{topic['topic_name_id']}_name"] = {
                    "value": topic["topic_name"],
                    "format": f"{topic['topic_name_length'] - 1}B",
                }
                _response[f"topic_{topic['topic_name_id']}_id"] = {
                    "value": topic["topic_name_id"],
                    "format": "16B",
                }
                _response[f"topic_{topic['topic_name_id']}_is_internal"] = {
                    "value": 0,
                    "format": "B",
                }
                _response[f"topic_{topic['topic_name_id']}_partition_array"] = {
                    "value": 1,
                    "format": "B",
                }
                _response[
                    f"topic_{topic['topic_name_id']}_Topic_Authorized_Operations"
                ] = {"value": 0xDF8, "format": "I"}
                _response[f"topic_{topic['topic_name_id']}_tag_buffer"] = {
                    "value": 0,
                    "format": "B",
                }
        _response["next_cursor"] = {"value": 0xFF, "format": "B"}
        _response["_tag_buffer"] = {"value": 0, "format": "B"}
        return _response
class APIVersions(BaseBinaryHandler):
    # APIVersions primitive does not require to parse the request body (?)
    @staticmethod
    async def prepare_response_body(parsed_request):
        api_version_error_code = Utilities.check_api_version(
            parsed_request.request_V2_header["request_api_version"]
        )
        if api_version_error_code != 0:
            return {
                "api_version_error_code": {
                    "value": api_version_error_code,
                    "format": "H",
                }
            }
        else:
            _response = {
                "api_version_error_code": {
                    "value": api_version_error_code,
                    "format": "H",
                },
                "API_version_array_length": {
                    "value": len(supported_API_keys) + 1,
                    "format": "B",
                },
            }
            for numerical_API_key, name_min_max_dic in supported_API_keys.items():
                _response[name_min_max_dic["name"]] = {
                    "value": numerical_API_key,
                    "format": "H",
                }
                _response[name_min_max_dic["name"] + "_min"] = {
                    "value": name_min_max_dic["min"],
                    "format": "H",
                }
                _response[name_min_max_dic["name"] + "_max"] = {
                    "value": name_min_max_dic["max"],
                    "format": "H",
                }
                _response[name_min_max_dic["name"] + "_tag_buffer"] = {
                    "value": 0,
                    "format": "B",
                }
            _response["throttle_time_ms"] = {"value": 0, "format": "I"}
            _response["throttle_time_ms_tag_buffer"] = {"value": 0, "format": "B"}
            if DEBUG:
                print(f" RESPONSE DIC : {_response}")
            return _response
class Fetch(BaseBinaryHandler):
    @staticmethod
    def parse_body(request_body: bytes) -> dict:
        parsed_body = {}
        offset = 0
        
        # Parse max_wait_ms
        parsed_body["max_wait_ms"] = int.from_bytes(request_body[offset:offset+4], byteorder="big")
        offset += 4
        
        # Parse min_bytes
        parsed_body["min_bytes"] = int.from_bytes(request_body[offset:offset+4], byteorder="big")
        offset += 4
        
        # Parse max_bytes
        parsed_body["max_bytes"] = int.from_bytes(request_body[offset:offset+4], byteorder="big")
        offset += 4
        
        # Parse isolation_level
        parsed_body["isolation_level"] = int.from_bytes(request_body[offset:offset+1], byteorder="big")
        offset += 1
        
        # Parse session_id
        parsed_body["session_id"] = int.from_bytes(request_body[offset:offset+4], byteorder="big")
        offset += 4
        
        # Parse session_epoch
        parsed_body["session_epoch"] = int.from_bytes(request_body[offset:offset+4], byteorder="big")
        offset += 4
        
        # Parse topics array length (compact array)
        topics_length = int.from_bytes(request_body[offset:offset+1], byteorder="big")
        offset += 1
        parsed_body["topics_length"] = topics_length - 1  # Compact array: length - 1
        
        topics = []
        for _ in range(parsed_body["topics_length"]):
            topic = {}
            
            # Parse topic_id (16 bytes UUID)
            topic["topic_id"] = request_body[offset:offset+16]
            offset += 16
            
            # Parse partitions array length (compact array)
            partitions_length = int.from_bytes(request_body[offset:offset+1], byteorder="big")
            offset += 1
            topic["partitions_length"] = partitions_length - 1
            
            partitions = []
            for _ in range(topic["partitions_length"]):
                partition = {}
                
                # Parse partition_index
                partition["partition_index"] = int.from_bytes(request_body[offset:offset+4], byteorder="big")
                offset += 4
                
                # Parse current_leader_epoch
                partition["current_leader_epoch"] = int.from_bytes(request_body[offset:offset+4], byteorder="big")
                offset += 4
                
                # Parse fetch_offset
                partition["fetch_offset"] = int.from_bytes(request_body[offset:offset+8], byteorder="big")
                offset += 8
                
                # Parse last_fetched_epoch
                partition["last_fetched_epoch"] = int.from_bytes(request_body[offset:offset+4], byteorder="big")
                offset += 4
                
                # Parse log_start_offset
                partition["log_start_offset"] = int.from_bytes(request_body[offset:offset+8], byteorder="big")
                offset += 8
                
                # Parse partition_max_bytes
                partition["partition_max_bytes"] = int.from_bytes(request_body[offset:offset+4], byteorder="big")
                offset += 4
                
                # Parse tagged_fields
                partition["tagged_fields"] = int.from_bytes(request_body[offset:offset+1], byteorder="big")
                offset += 1
                
                partitions.append(partition)
            
            topic["partitions"] = partitions
            
            # Parse topic tagged_fields
            topic["tagged_fields"] = int.from_bytes(request_body[offset:offset+1], byteorder="big")
            offset += 1
            
            topics.append(topic)
        
        parsed_body["topics"] = topics
        
        return parsed_body
    
    @staticmethod
    async def prepare_response_body(parsed_request):
        # Parse the request body to get topic information
        fields = Fetch.parse_body(parsed_request.request_body)

        if DEBUG:
            Utilities.display(fields, "Fetch Parsed Request Body")

        _response = {
            "throttle_time_ms": {"value": 0, "format": "I"},
            "error_code": {"value": 0, "format": "H"},
            "session_id": {"value": 0, "format": "I"},
        }

        if fields["topics_length"] == 0:
            _response["responses_array_length"] = {"value": 1, "format": "B"}
        else:
            _response["responses_array_length"] = {"value": fields["topics_length"] + 1, "format": "B"}

            for i, topic in enumerate(fields["topics"]):
                topic_id = topic["topic_id"]
                topic_id_int = int.from_bytes(topic_id, byteorder="big")

                # Find topic name from topic_id using MetaDataLog
                log = MetaDataLog(log_file)
                topic_name = None
                for record_batch, _content in log.log.items():
                    for j in range(log.log[record_batch]["Records Length"]):
                        record = log.log[record_batch][f"Record #{j}"]
                        if (
                            record["Value"]["Type"] == 2
                            and record["Value"]["Topic UUID"] == topic_id_int
                        ):
                            # Convert topic_name from int to string
                            topic_name_int = record["Value"]["Topic Name"]
                            if isinstance(topic_name_int, int):
                                # Convert int to bytes, then decode
                                length = (topic_name_int.bit_length() + 7) // 8
                                topic_name = topic_name_int.to_bytes(length, 'big').decode('utf-8', errors='ignore')
                            else:
                                # Already bytes
                                topic_name = topic_name_int.decode('utf-8', errors='ignore')
                            break
                    if topic_name is not None:
                        break

                # Use the actual partition_index from the request
                if topic["partitions"]:
                    partition_index = topic["partitions"][0]["partition_index"]
                else:
                    partition_index = 0

                # Use the correct log path as per tester's directory
                log_path = f"/kraft-combined-logs/{topic_name}-{partition_index}/00000000000000000000.log"

                # Read the entire RecordBatch from the log file
                try:
                    with open(log_path, "rb") as f:
                        record_batch_bytes = f.read()
                except Exception:
                    record_batch_bytes = b""

                _response[f"topic_{i}_id"] = {"value": tuple(topic_id), "format": "16B"}
                _response[f"topic_{i}_partitions_length"] = {"value": 2, "format": "B"}  # 1 element

                _response[f"topic_{i}_partition_0_index"] = {"value": partition_index, "format": "I"}
                _response[f"topic_{i}_partition_0_error_code"] = {"value": 0, "format": "H"}
                _response[f"topic_{i}_partition_0_high_watermark"] = {"value": 1, "format": "Q"}
                _response[f"topic_{i}_partition_0_last_stable_offset"] = {"value": 1, "format": "Q"}
                _response[f"topic_{i}_partition_0_log_start_offset"] = {"value": 0, "format": "Q"}
                _response[f"topic_{i}_partition_0_aborted_transactions_length"] = {"value": 1, "format": "B"}
                _response[f"topic_{i}_partition_0_preferred_read_replica"] = {"value": -1, "format": "i"}

                # Only include the RecordBatch if it exists and is non-empty
                if record_batch_bytes and len(record_batch_bytes) > 0:
                    _response[f"topic_{i}_partition_0_records_length"] = {"value": 2, "format": "B"}  # 1 element
                    _response[f"topic_{i}_partition_0_records"] = {
                        "value": record_batch_bytes,
                        "format": f"{len(record_batch_bytes)}s",
                    }
                else:
                    _response[f"topic_{i}_partition_0_records_length"] = {"value": 1, "format": "B"}  # 0 elements

                _response[f"topic_{i}_partition_0_tagged_fields"] = {"value": 0, "format": "B"}
                _response[f"topic_{i}_tagged_fields"] = {"value": 0, "format": "B"}

        _response["tagged_fields"] = {"value": 0, "format": "B"}

        return _response
class BaseRequestParser(ABC):
    """Abstract class for parsing data"""
    @abstractmethod
    def __init__(self, incoming_data):
        pass
    def Kafka_message_size(request: bytes) -> dict:
        pass
    def Kafka_request_header(request: bytes) -> dict:
        pass
class RequestParser_V2(BaseRequestParser):
    def __init__(self, incoming_data):
        self.header_length = 0
        self.request_V2_header_message_size = self.Kafka_message_size(incoming_data[:4])
        self.request_V2_header = self.Kafka_request_header(incoming_data[4:])
        self.request_body = incoming_data[4 + self.header_length :]
    def Kafka_message_size(self, request: bytes) -> dict:
        message_size, remaining = Utilities.unpack_helper(">I", request)
        return {"message_size": message_size[0]}
    def Kafka_request_header(self, request: bytes) -> dict:
        (
            (
                request_api_key,
                request_api_version,
                correlation_id,
                client_id_length,
            ),
            _remaining,
        ) = Utilities.unpack_helper(">HHIH", request)
        self.header_length += 10
        client_id_content, _remaining = Utilities.unpack_helper(
            f">{int(client_id_length)}B", _remaining
        )
        self.header_length += client_id_length + 1  # +1 for final tag buffer'''
        return {
            "request_api_key": request_api_key,
            "request_api_version": request_api_version,
            "correlation_id": correlation_id,
            "client_id_length": client_id_length,
            "client_id_content": client_id_content,
        }
class AsyncBinaryServer:
    def __init__(
        self,
        host: str,
        port: int,
    ):
        self.host = host
        self.port = port
        self.server: asyncio.Server = None
        self.loop = asyncio.get_event_loop()
    async def handle_new_connection(self, conn, addr):
        if DEBUG:
            print(f"connected by {addr}")
        while True:
            data_rcv = await self.loop.sock_recv(conn, 1024)
            if DEBUG:
                print(data_rcv.hex(" ", 1))
            # Parse request header
            parsed_request = RequestParser_V2(data_rcv)
            if DEBUG:
                Utilities.display(
                    parsed_request.request_V2_header, "Parsed Request header V2"
                )
            API_Key = parsed_request.request_V2_header["request_api_key"]
            if API_Key not in supported_API_keys.keys():
                raise KeyError(f"API Key : {API_Key} is not yet supported")
            else:
                # instance class responsible for that API key
                API_Key_class_name = supported_API_keys[API_Key]["name"]
                API_Key_class = globals()[API_Key_class_name]
                # Prepare header for response :
                correlation_id = {
                    "value": parsed_request.request_V2_header["correlation_id"],
                    "format": "I",
                }
                tag_buffer = {"value": 0, "format": "B"}
                if API_Key == 18:
                    # send request header V0
                    data_to_send = pack(
                        ">" + correlation_id["format"], correlation_id["value"]
                    )
                elif API_Key == 1:
                    # Fetch uses HeaderV1 - send request header V1 (correlation_id + tagged_fields)
                    data_to_send = pack(
                        ">" + correlation_id["format"] + tag_buffer["format"],
                        correlation_id["value"],
                        tag_buffer["value"],
                    )
                else:
                    # send request header V2
                    data_to_send = pack(
                        ">" + correlation_id["format"] + tag_buffer["format"],
                        correlation_id["value"],
                        tag_buffer["value"],
                    )
                if DEBUG:
                    print(
                        f"packed correlation id {correlation_id['value']} into {correlation_id['format']}"
                    )
                # Prepare response body
                response_body = await API_Key_class.prepare_response_body(
                    parsed_request
                )
                if DEBUG:
                    Utilities.display(response_body, "response body")
                for _field, value_format_pair in response_body.items():
                    if DEBUG:
                        print(
                            f"packed response body {_field} with value {value_format_pair['value']} into {value_format_pair['format']}"
                        )
                    if type(value_format_pair["value"]) is tuple:
                        data_to_send += pack(
                            ">" + value_format_pair["format"],
                            *value_format_pair["value"],
                        )
                    else:
                        data_to_send += pack(
                            ">" + value_format_pair["format"],
                            value_format_pair["value"],
                        )
                message_size = len(data_to_send)
                data_to_send = pack(">I", message_size) + data_to_send
                print(f"data sent ; {data_to_send.hex(':')}")
                await self.loop.sock_sendall(conn, data_to_send)
    async def start(self):
        try:
            if DEBUG:
                print("Starting server ")
            self.server = socket.create_server((self.host, self.port), reuse_port=True)
            self.server.setblocking(False)
            while True:
                conn, addr = await self.loop.sock_accept(self.server)  # wait for client
                try:
                    self.loop.create_task(self.handle_new_connection(conn, addr))
                except KeyError:
                    raise
        except Exception as e:
            if DEBUG:
                print(f"Server Error {e}")
        finally:
            await self.stop()
    async def stop(self):
        if DEBUG:
            print("Stopping server ")
        self.server.close
        if DEBUG:
            print("Server stopped")
async def main():
    ze_server = AsyncBinaryServer(host="localhost", port=9092)
    try:
        await ze_server.start()
    except Exception as e:
        if DEBUG:
            print(f"Caught other exception in high: {str(e)}")
        await ze_server.stop()
if __name__ == "__main__":
    asyncio.run(main())