import io

import bson

from aiomongowire.base_op import BaseOp
from aiomongowire.message_header import MessageHeader
from aiomongowire.op_code import OpCode


class OpGetMore(BaseOp):
    """
    OP_GET_MORE is used to query the database for documents in a collection
    """
    __slots__ = ['header', 'full_collection_name', 'number_to_return', 'cursor_id']

    def __init__(self, header: MessageHeader, full_collection_name: str, number_to_return: int, cursor_id: int):
        super().__init__(header)
        self.full_collection_name = full_collection_name
        self.number_to_return = number_to_return
        self.cursor_id = cursor_id

    @classmethod
    def has_reply(cls) -> bool:
        return True

    @classmethod
    def op_code(cls) -> OpCode:
        return OpCode.OP_GET_MORE

    @classmethod
    def _from_data(cls, message_length: int, header: MessageHeader, data: io.BytesIO) -> 'OpGetMore':
        data.seek(4, io.SEEK_CUR)  # 0 - reserved for future use
        full_collection_name = bson.decode_object(data)  # "dbname.collectionname"
        number_to_return = int(data.read(4))  # number of documents to return
        cursor_id = int(data.read(8))  # cursorID from the OP_REPLY
        return cls(header=header, full_collection_name=full_collection_name, number_to_return=number_to_return,
                   cursor_id=cursor_id)

    def _as_bytes(self):
        pass
