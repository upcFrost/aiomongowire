import io
from enum import IntFlag
from typing import List, ClassVar

from ._base_op import BaseOp
from ._bson import get_bson_parser
from ._op_code import OpCode


class OpReply(BaseOp):
    """
    OP_REPLY contains server response to the query.

    For OP_MSG, another OP_MSG is used for the response
    """
    __slots__ = ['response_flags', 'cursor_id', 'starting_from', 'number_returned', 'documents']

    op_code: ClassVar[OpCode] = OpCode.OP_REPLY

    class Flags(IntFlag):
        CURSOR_NOT_FOUND = 1 << 0
        QUERY_FAILURE = 1 << 1
        SHARD_CONFIG_STATE = 1 << 2
        AWAIT_CAPABLE = 1 << 3

    def __init__(self, cursor_id: int, starting_from: int, number_returned: int, documents: List[dict],
                 response_flags: int = 0):
        self.response_flags = response_flags
        self.cursor_id = cursor_id
        self.starting_from = starting_from
        self.number_returned = number_returned
        self.documents = documents

    @property
    def has_reply(self) -> bool:
        return False

    @classmethod
    def from_data(cls, data: io.BytesIO):
        response_flags = OpReply.Flags(int.from_bytes(data.read(4), byteorder='little', signed=True))  # bit vector
        # cursor id if client needs to do get more's
        cursor_id = int.from_bytes(data.read(8), byteorder='little', signed=True)
        # where in the cursor this reply is starting
        starting_from = int.from_bytes(data.read(4), byteorder='little', signed=True)
        # number of documents in the reply
        number_returned = int.from_bytes(data.read(4), byteorder='little', signed=True)

        documents = []
        for _ in range(number_returned):
            len_bytes = data.read(4)
            doc_len = int.from_bytes(len_bytes, byteorder='little', signed=False)
            documents.append(get_bson_parser().decode_object(len_bytes + data.read(doc_len - 4)))
        return cls(
            response_flags=response_flags,
            cursor_id=cursor_id,
            starting_from=starting_from,
            number_returned=number_returned,
            documents=documents
        )

    def __str__(self):
        return f"OP_REPLY: flags: {self.response_flags}, cursor id: {self.cursor_id}, documents: {self.documents}"

    def __bytes__(self):
        raise NotImplementedError()
