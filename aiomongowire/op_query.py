import io
from enum import IntFlag
from typing import Optional

import bson

from .base_op import BaseOp
from .message_header import MessageHeader
from .op_code import OpCode
from .utils import decode_cstring


class OpQuery(BaseOp):
    """
    The OP_QUERY message is used to query the database for documents in a collection
    """

    __slots__ = ['header', 'flags', 'full_collection_name', 'number_to_skip', 'number_to_return', 'query',
                 'return_fields_selector']

    class Flags(IntFlag):
        TAILABLE_CURSOR = 1 << 1  # Cursor is not closed when the last data is retrieved
        SLAVE_OK = 1 << 2  # Allow query of replica slave
        OPLOG_REPLAY = 1 << 3
        NO_CURSOR_TIMEOUT = 1 << 4
        AWAIT_DATA = 1 << 5  # Use with TailableCursor. If at the end of the data, block rather than returning no data
        EXHAUST = 1 << 6  # Stream the data down full blast in multiple "more" packages
        PARTIAL = 1 << 7  # Get partial results from a mongos if some shards are down

    @classmethod
    def op_code(cls) -> OpCode:
        return OpCode.OP_QUERY

    @classmethod
    def has_reply(cls) -> bool:
        return True

    def __init__(self, full_collection_name: str, query: dict, number_to_skip: int = 0, number_to_return: int = 0,
                 return_fields_selector: Optional[dict] = None, header: Optional[MessageHeader] = None, flags: int = 0):
        super().__init__(header)
        self.flags = flags
        self.full_collection_name = full_collection_name
        self.number_to_skip = number_to_skip
        self.number_to_return = number_to_return
        self.query = query
        self.return_fields_selector = return_fields_selector

    @classmethod
    def _from_data(cls, message_length: int, header: MessageHeader, data: io.BytesIO):
        # bit vector of query options
        flags = int.from_bytes(data.read(4), byteorder='little', signed=False)
        # "dbname.collection_name"
        full_collection_name = decode_cstring(data)
        # number of documents to skip
        number_to_skip = int.from_bytes(data.read(4), byteorder='little', signed=True)
        # number of documents to return in the first OP_REPLY batch
        number_to_return = int.from_bytes(data.read(4), byteorder='little', signed=True)
        # query object
        query = bson.decode_object(data)

        data_left = data.getvalue()
        if data_left:
            # Optional. Selector indicating the fields to return.
            return_fields_selector = bson.decode_object(data_left)
        else:
            return_fields_selector = None

        return cls(header=header, flags=flags, full_collection_name=full_collection_name,
                   number_to_skip=number_to_skip, number_to_return=number_to_return,
                   query=query, return_fields_selector=return_fields_selector)

    def _as_bytes(self) -> bytes:
        with io.BytesIO() as data:
            data.write(int.to_bytes(self.flags, length=4, byteorder='little', signed=False))
            data.write(bson.encode_cstring(self.full_collection_name))
            data.write(int.to_bytes(self.number_to_skip, length=4, byteorder='little', signed=True))
            data.write(int.to_bytes(self.number_to_return, length=4, byteorder='little', signed=True))
            data.write(bson.dumps(self.query))
            if self.return_fields_selector:
                data.write(bson.dumps(self.return_fields_selector))
            return data.getvalue()
