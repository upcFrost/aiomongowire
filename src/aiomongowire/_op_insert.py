import io
from enum import IntFlag
from typing import ClassVar

from ._base_op import BaseOp
from ._bson import get_bson_parser
from ._op_code import OpCode


class OpInsert(BaseOp):
    """
    OP_INSERT is used to insert documents.

    For insert with confirmation use OP_MSG
    """
    __slots__ = ['flags', 'full_collection_name', 'documents']

    op_code: ClassVar[OpCode] = OpCode.OP_INSERT

    class Flags(IntFlag):
        """OP_INSERT flag bits"""
        CONTINUE_ON_ERROR = 1 << 0

    def __init__(self, full_collection_name: str, documents: list, flags: int = 0):
        self.flags = flags  # bit vector
        self.full_collection_name = full_collection_name  # "dbname.collectionname"
        self.documents = documents  # one or more documents to insert into the collection

    @property
    def has_reply(self) -> bool:
        return False

    @classmethod
    def from_data(cls, data: io.BytesIO):
        flags = int.from_bytes(data.read(4), byteorder='little', signed=True)  # bit vector
        full_collection_name = get_bson_parser().decode_cstring(data)  # "dbname.collectionname"
        documents = get_bson_parser().decode_object(data)  # one or more documents to insert into the collection
        return cls(flags=flags, full_collection_name=full_collection_name, documents=documents)

    def __bytes__(self):
        with io.BytesIO() as data:
            data.write(self.flags.to_bytes(length=4, byteorder='little', signed=True))
            data.write(get_bson_parser().encode_cstring(self.full_collection_name))
            for doc in self.documents:
                data.write(get_bson_parser().encode_object(doc))
            return data.getvalue()
