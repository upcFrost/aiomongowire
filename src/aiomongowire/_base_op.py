import abc
import io
from typing import Dict, Type, SupportsBytes, ClassVar

from ._op_code import OpCode, UnknownOpcodeException

_OP_CLASSES_BY_CODE: Dict[OpCode, Type['BaseOp']] = {}


def parse_op(op_code: OpCode, data: io.BytesIO) -> 'BaseOp':
    """
    Deserialize operation from bytes
    """
    try:
        return _OP_CLASSES_BY_CODE[op_code].from_data(data)
    except KeyError:
        raise UnknownOpcodeException(op_code)


class BaseOp(SupportsBytes):
    """
    Generic operation. Children should define __bytes__ method
    """
    op_code: ClassVar[OpCode]

    @classmethod
    @abc.abstractmethod
    def from_data(cls, data: io.BytesIO):
        """
        Deserialize operation from bytes.
        """
        pass

    @property
    @abc.abstractmethod
    def has_reply(self) -> bool:
        """
        True if OP is expected to have a reply
        """
        pass

    def __init_subclass__(cls, **kwargs):
        _OP_CLASSES_BY_CODE[cls.op_code] = cls

    def __str__(self):
        return f"{self.__class__.__name__}: " + ', '.join([f'{x}: {self.__getattribute__(x)}'
                                                           for x in self.__slots__])
