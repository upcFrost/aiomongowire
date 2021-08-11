import abc
import io
from typing import Dict, Type, SupportsBytes

from ._op_code import OpCode

_OP_CLASSES_BY_CODE: Dict[OpCode, Type['BaseOp']] = {}


class BaseOp(SupportsBytes):
    """
    Generic operation. Children should define __bytes__ method
    """

    @classmethod
    @abc.abstractmethod
    def op_code(cls) -> OpCode:
        pass

    @classmethod
    @abc.abstractmethod
    def has_reply(cls) -> bool:
        """
        True if operation is expected to return something
        """
        pass

    @classmethod
    def from_data(cls, op_code: OpCode, data: io.BytesIO) -> 'BaseOp':
        """
        Deserialize operation from bytes

        :raises KeyError: If operation has unknown OpCode
        """
        return _OP_CLASSES_BY_CODE[op_code]._from_data(data)

    @classmethod
    @abc.abstractmethod
    def _from_data(cls, data: io.BytesIO):
        """
        Deserialize operation from bytes. Internal implementation.
        """
        pass

    def __init_subclass__(cls, **kwargs):
        _OP_CLASSES_BY_CODE[cls.op_code()] = cls

    def __str__(self):
        return f"{self.__class__.__name__}: " + ', '.join([f'{x}: {self.__getattribute__(x)}'
                                                           for x in self.__slots__])
