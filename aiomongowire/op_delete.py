import io

import bson

from aiomongowire.base_op import BaseOp
from aiomongowire.message_header import MessageHeader
from aiomongowire.op_code import OpCode
from aiomongowire.utils import decode_cstring


class OpDelete(BaseOp):
    """
    OP_DELETE is used to remove one or more documents from a collection.
    """
    __slots__ = ['header', 'full_collection_name', 'flags', 'selector']

    def __init__(self, header: MessageHeader, full_collection_name: str, flags: int, selector: dict):
        super(OpDelete, self).__init__(header)
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
    def _from_data(cls, message_length: int, header: MessageHeader, data: io.BytesIO):
        data.seek(4, io.SEEK_CUR)  # 0 - reserved for future use
        full_collection_name = decode_cstring(data)  # "dbname.collectionname"
        flags = int(data.read(4))  # bit vector
        selector = bson.decode_object(data)  # query object.
        return cls(header=header, full_collection_name=full_collection_name, flags=flags, selector=selector)

    def _as_bytes(self) -> bytes:
        raise NotImplementedError()
