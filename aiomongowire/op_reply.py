import io
from typing import List

import bson

from aiomongowire.base_op import BaseOp
from aiomongowire.message_header import MessageHeader
from aiomongowire.op_code import OpCode


class OpReply(BaseOp):
    """
    OP_REPLY contains server response to the query.

    For OP_MSG, another OP_MSG is used for the response
    """
    __slots__ = ['header', 'response_flags', 'cursor_id', 'starting_from', 'number_returned', 'documents']

    @classmethod
    def op_code(cls) -> OpCode:
        return OpCode.OP_REPLY

    @classmethod
    def has_reply(cls) -> bool:
        return False

    def __init__(self, header: MessageHeader, response_flags: int, cursor_id: int, starting_from: int,
                 number_returned: int, documents: List[dict]):
        super(OpReply, self).__init__(header)
        self.response_flags = response_flags
        self.cursor_id = cursor_id
        self.starting_from = starting_from
        self.number_returned = number_returned
        self.documents = documents

    @classmethod
    def _from_data(cls, message_length: int, header: MessageHeader, data: io.BytesIO):
        response_flags = int.from_bytes(data.read(4), byteorder='little', signed=True)  # bit vector
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
            documents.append(bson.loads(len_bytes + data.read(doc_len - 4)))
        return cls(
            header=header,
            response_flags=response_flags,
            cursor_id=cursor_id,
            starting_from=starting_from,
            number_returned=number_returned,
            documents=documents
        )

    def __str__(self):
        return f"OP_REPLY: flags: {self.response_flags}, cursor id: {self.cursor_id}, documents: {self.documents}"

    def _as_bytes(self):
        raise NotImplementedError()
