import io
import zlib
from enum import Enum
from typing import Callable

import snappy
import zstandard

from .base_op import BaseOp
from .op_code import OpCode


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

    class Compressor(Enum):
        def __new__(cls, value: int,
                    compress_method: Callable[[bytes], bytes],
                    decompress_method: Callable[[bytes], bytes]):
            obj = object.__new__(cls)
            obj._value_ = value
            obj.compress = compress_method
            obj.decompress = decompress_method
            return obj

        def __int__(self):
            return self._value_

        NO_COMPRESSION = 0, lambda x: x, lambda x: x
        SNAPPY = 1, snappy.compress, snappy.decompress
        ZLIB = 2, zlib.compress, zlib.decompress
        ZSTD = 3, zstandard.compress, zstandard.decompress

    def __init__(self, compressor: Compressor, original_msg: BaseOp):
        self.compressor = compressor
        self.original_msg = original_msg

    @classmethod
    def _from_data(cls, data: io.BytesIO):
        original_opcode = OpCode(int.from_bytes(data.read(4), byteorder='little', signed=False))
        original_len = data.read(4)
        compressor = OpCompressed.Compressor(int.from_bytes(data.read(1), byteorder='little', signed=False))
        compressed = data.read()
        decompressed = compressor.decompress(compressed)
        return cls(compressor=compressor, original_msg=BaseOp.from_data(original_opcode, io.BytesIO(decompressed)))

    def __bytes__(self):
        original_bytes = bytes(self.original_msg)
        compressed = self.compressor.compress(original_bytes)
        with io.BytesIO() as data:
            data.write(int.to_bytes(self.original_msg.op_code(), length=4, byteorder='little'))
            data.write(int.to_bytes(len(original_bytes), length=4, byteorder='little'))
            data.write(int.to_bytes(int(self.compressor), length=1, byteorder='little', signed=False))
            data.write(compressed)
            return data.getvalue()
