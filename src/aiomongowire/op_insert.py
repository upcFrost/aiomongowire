import io
from enum import IntFlag
from typing import Optional

import bson

from .base_op import BaseOp
from .message_header import MessageHeader
from .op_code import OpCode
from .utils import decode_cstring


class OpInsert(BaseOp):
    """
    OP_INSERT is used to insert documents.

    For insert with confirmation use OP_MSG
    """
    __slots__ = ['header', 'flags', 'full_collection_name', 'documents']

    class Flags(IntFlag):
        CONTINUE_ON_ERROR = 1 << 0

    def __init__(self, full_collection_name: str, documents: list, header: Optional[MessageHeader] = None,
                 flags: int = 0):
        super().__init__(header)
        self.flags = flags  # bit vector
        self.full_collection_name = full_collection_name  # "dbname.collectionname"
        self.documents = documents  # one or more documents to insert into the collection

    @classmethod
    def has_reply(cls) -> bool:
        return False

    @classmethod
    def op_code(cls) -> OpCode:
        return OpCode.OP_INSERT

    @classmethod
    def _from_data(cls, message_length: int, header: MessageHeader, data: io.BytesIO):
        flags = int.from_bytes(data.read(4), byteorder='little', signed=True)  # bit vector
        full_collection_name = decode_cstring(data)  # "dbname.collectionname"
        documents = bson.decode_object(data)  # one or more documents to insert into the collection
        return cls(header=header, flags=flags, full_collection_name=full_collection_name, documents=documents)

    def _as_bytes(self) -> bytes:
        with io.BytesIO() as data:
            data.write(self.flags.to_bytes(length=4, byteorder='little', signed=True))
            data.write(bson.encode_cstring(self.full_collection_name))
            for doc in self.documents:
                data.write(bson.dumps(doc))
            return data.getvalue()
