import io
from typing import Type

from ._base_op import BaseOp
from ._compressor import Compressor
from ._op_code import OpCode


class OpCompressed(BaseOp):
    """
    OP_COMPRESSED wraps other opcodes to provide compression
    """
    __slots__ = ['compressor', 'original_msg']

    @classmethod
    def op_code(cls) -> OpCode:
        return OpCode.OP_COMPRESSED

    def has_reply(self) -> bool:
        # Depends on the original opcode
        return self.original_msg.has_reply()

    def __init__(self, compressor: Type[Compressor], original_msg: BaseOp):
        self.compressor = compressor
        self.original_msg = original_msg

    @classmethod
    def _from_data(cls, data: io.BytesIO):
        original_opcode = OpCode(int.from_bytes(data.read(4), byteorder='little', signed=False))
        original_len = data.read(4)
        compressor = Compressor.by_id(int.from_bytes(data.read(1), byteorder='little', signed=False))
        compressed = data.read()
        decompressed = compressor.decompress(compressed)
        return cls(compressor=compressor, original_msg=BaseOp.from_data(original_opcode, io.BytesIO(decompressed)))

    def __bytes__(self):
        original_bytes = bytes(self.original_msg)
        compressed = self.compressor.compress(original_bytes)
        with io.BytesIO() as data:
            data.write(int.to_bytes(self.original_msg.op_code(), length=4, byteorder='little'))
            data.write(int.to_bytes(len(original_bytes), length=4, byteorder='little'))
            data.write(int.to_bytes(int(self.compressor.id()), length=1, byteorder='little', signed=False))
            data.write(compressed)
            return data.getvalue()
