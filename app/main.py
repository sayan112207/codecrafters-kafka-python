import socket  # noqa: F401
import struct
import threading
from app.functions import encode_unsigned_varint, api_key
from app.request import Request
class Broker:
    def __init__(self):
        self.server = socket.create_server(("localhost", 9092), reuse_port=True)
        self.api_versions = {
            18: (0, 4),
            75: (0, 0),
        }
        self.handlers = {
            m._api_key: getattr(self, m.__name__)
            for m in Broker.__dict__.values()
            if hasattr(m, "_api_key")
        }
    def serve(self):
        print("Broker listening on port 9092")
        while True:
            con, addr = self.server.accept()
            print(f"Accepted connection from {addr}")
            t = threading.Thread(
                target=self._serve_connection, args=(con,), daemon=True
            )
            t.start()
    def _serve_connection(self, con: socket.socket):
        with con:
            while True:
                try:
                    req = Request.from_socket(con)
                except Exception as e:
                    print("Error handling connection:", e)
                    break
                try:
                    self.handle(con, req)
                except Exception as e:
                    print("Handler error:", e)
                    break
    def handle(self, con: socket.socket, req: Request):
        print(f"Received request: {req}")
        if req.api_key not in self.api_versions:
            return self._send_error(con, req.correlation_id, error_code=3)
        min_v, max_v = self.api_versions[req.api_key]
        if not (min_v <= req.api_version <= max_v):
            return self._send_error(con, req.correlation_id, error_code=35)
        handler = self.handlers.get(req.api_key)
        if not handler:
            return self._send_error(con, req.correlation_id, error_code=3)
        payload = self.handlers[req.api_key](req)
        self._send_response(con, req.correlation_id, payload)
    def _send_response(self, conn: socket.socket, correlation_id: int, payload: bytes):
        """
        Send a Kafka response: [length:int32][correlation_id:uint32][payload bytefs]
        """
        total_length = 4 + len(payload)  # 4 bytes for correlation_id
        header = struct.pack(">I", total_length) + struct.pack(">I", correlation_id)
        message = header + payload
        conn.sendall(message)
    def _send_error(self, con: socket.socket, correlation_id: int, error_code: int):
        """
        Send a simple error response with only an int16 error_code in payload.
        """
        payload = struct.pack(">h", error_code)
        self._send_response(con, correlation_id, payload)
    @api_key(18)
    def handle_api_versions(self, req: Request) -> bytes:
        apis = list(self.api_versions.items())
        body = struct.pack(">h", 0)
        body += encode_unsigned_varint(len(apis) + 1)
        for key, (min_v, max_v) in apis:
            body += struct.pack(">hhh", key, min_v, max_v)
            # tagged_fields for this entry (empty)
            body += encode_unsigned_varint(0)
        # 4) throttle_time_ms (0)
        body += struct.pack(">i", 0)
        # 5) final tagged_fields (empty)
        body += encode_unsigned_varint(0)
        return body