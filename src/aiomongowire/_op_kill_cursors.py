import io
from typing import List, ClassVar

from ._base_op import BaseOp
from ._op_code import OpCode


class OpKillCursors(BaseOp):
    """
    OP_KILL_CURSORS is used to close an active cursor in the database
    """
    __slots__ = ['number_of_cursor_ids', 'cursor_ids']

    op_code: ClassVar[OpCode] = OpCode.OP_KILL_CURSORS

    def __init__(self, number_of_cursor_ids: int, cursor_ids: List[int]):
        self.number_of_cursor_ids = number_of_cursor_ids
        self.cursor_ids = cursor_ids

    @property
    def has_reply(self) -> bool:
        return False

    @classmethod
    def from_data(cls, data: io.BytesIO):
        data.seek(4, io.SEEK_CUR)  # 0 - reserved for future use
        # number of cursorIDs in message
        number_of_cursor_ids = int.from_bytes(data.read(4), byteorder='little', signed=True)
        # sequence of cursorIDs to close
        cursor_ids = [int.from_bytes(data.read(8), byteorder='little', signed=True)
                      for _ in range(number_of_cursor_ids)]
        return cls(number_of_cursor_ids=number_of_cursor_ids, cursor_ids=cursor_ids)

    def __bytes__(self):
        with io.BytesIO() as data:
            data.write(int.to_bytes(0, length=4, byteorder='little'))
            data.write(self.number_of_cursor_ids.to_bytes(length=4, byteorder='little', signed=True))
            for cursor in self.cursor_ids:
                data.write(cursor.to_bytes(length=8, byteorder='little', signed=True))
            return data.getvalue()
