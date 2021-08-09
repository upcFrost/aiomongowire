import io

from .base_op import BaseOp
from .message_header import MessageHeader
from .op_code import OpCode


class MongoWireMessage:
    """
    Base class for the mongodb wire protocol message
    """
    __slots__ = ['header', 'operation']

    def __init__(self, operation: BaseOp, header: MessageHeader = None):
        if header:
            self.header = header
        else:
            self.header = MessageHeader()
        self.operation = operation

    @classmethod
    def from_data(cls, data: io.BytesIO) -> 'MongoWireMessage':
        """
        Deserialize message from bytes

        :raises KeyError: If operation has unknown OpCode
        """
        message_length = int.from_bytes(data.read(4), byteorder='little', signed=True)
        header = MessageHeader.from_data(data)
        op_code = OpCode(int.from_bytes(data.read(4), byteorder='little', signed=True))
        operation = BaseOp.from_data(op_code, data)
        return cls(header=header, operation=operation)

    def __bytes__(self):
        operation_bytes = bytes(self.operation)
        message_len = len(operation_bytes) + 16
        message_len = message_len.to_bytes(length=4, byteorder='little', signed=False)
        return message_len + bytes(self.header) + bytes(self.operation.op_code()) + operation_bytes
