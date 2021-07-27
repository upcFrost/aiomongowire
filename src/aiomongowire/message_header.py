import io
import random
from typing import SupportsBytes


class MessageHeader(SupportsBytes):
    """
    Standard message header
    https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#standard-message-header

    Message length is moved out of the header as it is not really involved in anything except encoding/decoding
    OpCode is moved out of the header as it is Op-related
    """
    __slots__ = ['request_id', 'response_to']

    def __init__(self, request_id: int = None, response_to: int = 0):
        self.response_to = response_to
        if request_id is not None:
            self.request_id = request_id
        else:
            self.request_id = random.randint(0, 1 << 31)

    @classmethod
    def from_data(cls, data: io.BytesIO) -> 'MessageHeader':
        """
        Constructs new message header instance from BytesIO
        """
        request_id = int.from_bytes(data.read(4), byteorder='little', signed=True)
        response_to = int.from_bytes(data.read(4), byteorder='little', signed=True)
        return cls(request_id=request_id, response_to=response_to)

    def __bytes__(self) -> bytes:
        request_id_bytes = self.request_id.to_bytes(length=4, byteorder='little', signed=True)
        response_to_bytes = self.response_to.to_bytes(length=4, byteorder='little', signed=True)
        return request_id_bytes + response_to_bytes
