import io

from ._base_op import BaseOp, parse_op
from ._message_header import MessageHeader
from ._op_code import OpCode, UnknownOpcodeException


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

        :raises UnknownOpcodeException: If operation has unknown OpCode
        """
        message_length = int.from_bytes(data.read(4), byteorder='little', signed=True)
        header = MessageHeader.from_data(data)
        op_code_value = int.from_bytes(data.read(4), byteorder='little', signed=True)
        try:
            op_code = OpCode(op_code_value)
        except ValueError:
            raise UnknownOpcodeException(op_code_value)
        operation = parse_op(op_code, data)
        return cls(header=header, operation=operation)

    def __bytes__(self):
        operation_bytes = bytes(self.operation)
        message_len = len(operation_bytes) + 16
        message_len = message_len.to_bytes(length=4, byteorder='little', signed=False)
        return message_len + bytes(self.header) + bytes(self.operation.op_code) + operation_bytes
