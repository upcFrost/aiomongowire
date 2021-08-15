import io
from enum import IntEnum
from typing import ClassVar

from ._base_op import BaseOp
from ._bson import get_bson_parser
from ._op_code import OpCode


class OpUpdate(BaseOp):
    """Update operation request"""

    __slots__ = ['full_collection_name', 'flags', 'selector', 'update']

    op_code: ClassVar[OpCode] = OpCode.OP_UPDATE

    class Flags(IntEnum):
        """Update operation flag bit positions"""
        UPSERT = 1 << 0
        MULTI_UPDATE = 1 << 1

    def __init__(self, full_collection_name: str, selector: dict, update: dict, flags: int = 0):
        self.full_collection_name = full_collection_name
        self.flags = flags
        self.selector = selector
        self.update = update

    @property
    def has_reply(self) -> bool:
        return False

    @classmethod
    def from_data(cls, data: io.BytesIO) -> 'OpUpdate':
        data.seek(4, io.SEEK_CUR)  # 0 - reserved for future use
        full_collection_name = get_bson_parser().decode_cstring(data)  # "dbname.collectionname"
        flags = int(data.read(4))  # bit vector
        selector = get_bson_parser().decode_object(data)  # the query to select the document
        update = get_bson_parser().decode_object(data)  # specification of the update to perform

        return cls(
            full_collection_name=full_collection_name,
            flags=flags,
            selector=selector,
            update=update
        )

    def __bytes__(self):
        with io.BytesIO() as data:
            data.write(int.to_bytes(0, length=4, byteorder='little'))
            data.write(get_bson_parser().encode_cstring(self.full_collection_name))
            data.write(int.to_bytes(self.flags, length=4, byteorder='little', signed=False))
            data.write(get_bson_parser().encode_object(self.selector))
            data.write(get_bson_parser().encode_object(self.update))
            return data.getvalue()
