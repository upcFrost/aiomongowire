import io
from enum import IntFlag

from ._base_op import BaseOp
from ._bson import get_bson_parser
from ._op_code import OpCode


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
        bson_parser = get_bson_parser()
        data.seek(4, io.SEEK_CUR)  # 0 - reserved for future use
        full_collection_name = bson_parser.decode_cstring(data)  # "dbname.collectionname"
        flags = int(data.read(4))  # bit vector
        selector = bson_parser.decode_object(data)  # query object.
        return cls(full_collection_name=full_collection_name, flags=flags, selector=selector)

    def __bytes__(self):
        bson_parser = get_bson_parser()
        with io.BytesIO() as data:
            data.write(int.to_bytes(0, length=4, byteorder='little'))
            data.write(bson_parser.encode_cstring(self.full_collection_name))
            data.write(int.to_bytes(self.flags, length=4, byteorder='little', signed=False))
            data.write(bson_parser.encode_object(self.selector))
            return data.getvalue()
