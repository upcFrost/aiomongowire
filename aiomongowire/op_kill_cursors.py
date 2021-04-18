import io
from typing import List

from aiomongowire.base_op import BaseOp
from aiomongowire.message_header import MessageHeader
from aiomongowire.op_code import OpCode


class OpKillCursors(BaseOp):
    """
    OP_KILL_CURSORS is used to close an active cursor in the database
    """
    __slots__ = ['header', 'number_of_cursor_ids', 'cursor_ids']

    @classmethod
    def op_code(cls) -> OpCode:
        return OpCode.OP_KILL_CURSORS

    @classmethod
    def has_reply(cls) -> bool:
        return False

    def __init__(self, header: MessageHeader, number_of_cursor_ids: int, cursor_ids: List[int]):
        super().__init__(header)
        self.number_of_cursor_ids = number_of_cursor_ids
        self.cursor_ids = cursor_ids

    @classmethod
    def _from_data(cls, message_length: int, header: MessageHeader, data: io.BytesIO):
        data.seek(4, io.SEEK_CUR)  # 0 - reserved for future use
        # number of cursorIDs in message
        number_of_cursor_ids = int.from_bytes(data.read(4), byteorder='little', signed=True)
        # sequence of cursorIDs to close
        cursor_ids = [int.from_bytes(data.read(8), byteorder='little', signed=True)
                      for _ in range(number_of_cursor_ids)]
        return cls(header=header, number_of_cursor_ids=number_of_cursor_ids, cursor_ids=cursor_ids)

    def _as_bytes(self) -> bytes:
        data = io.BytesIO()
        data.write(int.to_bytes(0, length=4, byteorder='little'))
        data.write(self.number_of_cursor_ids.to_bytes(length=4, byteorder='little', signed=True))
        for cursor in self.cursor_ids:
            data.write(cursor.to_bytes(length=8, byteorder='little', signed=True))
        return data.getvalue()
