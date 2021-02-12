import abc
import io
from typing import SupportsBytes, Dict, Type

from aiomongowire.message_header import MessageHeader
from aiomongowire.op_code import OpCode

_OP_CLASSES_BY_CODE: Dict[OpCode, Type['BaseOp']] = {}


class BaseOp(abc.ABC, SupportsBytes):
    """
    Generic operation. Children should define __bytes__ method
    """

    __slots__ = ['header']

    @classmethod
    @abc.abstractmethod
    def op_code(cls) -> OpCode:
        pass

    @classmethod
    @abc.abstractmethod
    def has_reply(cls) -> bool:
        pass

    @classmethod
    def from_data(cls, data: io.BytesIO) -> 'BaseOp':
        """
        Deserialize operation from bytes

        :raises KeyError: If operation has unknown OpCode
        """
        message_length = int.from_bytes(data.read(4), byteorder='little', signed=True)
        header = MessageHeader.from_data(data)
        op_code = OpCode(int.from_bytes(data.read(4), byteorder='little', signed=True))
        return _OP_CLASSES_BY_CODE[op_code]._from_data(message_length, header, data)

    @classmethod
    @abc.abstractmethod
    def _from_data(cls, message_length: int, header: MessageHeader, data: io.BytesIO):
        """
        Deserialize operation from bytes. Internal implementation.
        """
        pass

    def __init__(self, header: MessageHeader):
        self.header = header

    def __init_subclass__(cls, **kwargs):
        _OP_CLASSES_BY_CODE[cls.op_code()] = cls

    def __str__(self):
        return f"{self.__class__.__name__}: " + ', '.join([f'{x}: {self.__getattribute__(x)}'
                                                           for x in self.__slots__])

    @abc.abstractmethod
    def _as_bytes(self) -> bytes:
        """
        Convert operation to bytes w/o header. Used inside the base __bytes__ method.
        """
        pass

    def __bytes__(self):
        data = self._as_bytes()
        message_len = len(data) + 16
        message_len = message_len.to_bytes(length=4, byteorder='little', signed=False)
        return message_len + bytes(self.header) + bytes(self.op_code()) + data
