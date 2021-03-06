import io
from enum import IntEnum

import bson

from aiomongowire.base_op import BaseOp
from aiomongowire.message_header import MessageHeader
from aiomongowire.op_code import OpCode
from aiomongowire.utils import decode_cstring


class OpUpdate(BaseOp):
    """Update operation request"""

    __slots__ = ['header', 'full_collection_name', 'flags', 'selector', 'update']

    class Flags(IntEnum):
        """Update operation flag bit positions"""
        UPSERT = 0
        MULTI_UPDATE = 1

    @classmethod
    def op_code(cls) -> OpCode:
        return OpCode.OP_UPDATE

    def __init__(self, header: MessageHeader, full_collection_name: str, flags: int, selector: dict, update: dict):
        super().__init__(header)
        self.full_collection_name = full_collection_name
        self.flags = flags
        self.selector = selector
        self.update = update

    @classmethod
    def has_reply(cls) -> bool:
        return False

    @classmethod
    def _from_data(cls, message_length: int, header: MessageHeader, data: io.BytesIO) -> 'OpUpdate':
        data.seek(4, io.SEEK_CUR)  # 0 - reserved for future use
        full_collection_name = decode_cstring(data)  # "dbname.collectionname"
        flags = int(data.read(4))  # bit vector
        selector = bson.decode_object(data)  # the query to select the document
        update = bson.decode_object(data)  # specification of the update to perform

        return cls(
            header=header,
            full_collection_name=full_collection_name,
            flags=flags,
            selector=selector,
            update=update
        )

    def _as_bytes(self) -> bytes:
        data = io.BytesIO()
        data.write(int.to_bytes(0, length=4, byteorder='little'))
        data.write(bson.encode_cstring(self.full_collection_name))
        data.write(int.to_bytes(self.flags, length=4, byteorder='little', signed=False))
        data.write(bson.dumps(self.selector))
        data.write(bson.dumps(self.update))
        return data.getvalue()
