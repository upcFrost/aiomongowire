import io
from enum import IntFlag

import bson

from .base_op import BaseOp
from .op_code import OpCode
from .utils import decode_cstring


class OpDelete(BaseOp):
    """
    OP_DELETE is used to remove one or more documents from a collection.
    """
    __slots__ = ['full_collection_name', 'flags', 'selector']

    class Flags(IntFlag):
        """OP_DELETE flag bits"""
        SINGLE_REMOVE = 1 << 0  # Remove only the first matching document in the collection

    def __init__(self, full_collection_name: str, selector: dict, flags: int = 0):
        self.full_collection_name = full_collection_name
        self.flags = flags
        self.selector = selector

    @classmethod
    def has_reply(cls) -> bool:
        return False

    @classmethod
    def op_code(cls) -> OpCode:
        return OpCode.OP_DELETE

    @classmethod
    def _from_data(cls, data: io.BytesIO):
        data.seek(4, io.SEEK_CUR)  # 0 - reserved for future use
        full_collection_name = decode_cstring(data)  # "dbname.collectionname"
        flags = int(data.read(4))  # bit vector
        selector = bson.decode_object(data)  # query object.
        return cls(full_collection_name=full_collection_name, flags=flags, selector=selector)

    def __bytes__(self):
        with io.BytesIO() as data:
            data.write(int.to_bytes(0, length=4, byteorder='little'))
            data.write(bson.encode_cstring(self.full_collection_name))
            data.write(int.to_bytes(self.flags, length=4, byteorder='little', signed=False))
            data.write(bson.dumps(self.selector))
            return data.getvalue()
