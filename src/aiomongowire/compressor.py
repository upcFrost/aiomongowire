import abc
import zlib
from typing import Dict, Type, Set

_COMPRESSORS_BY_NAME: Dict[str, Type['Compressor']] = {}
_COMPRESSORS_BY_ID: Dict[int, Type['Compressor']] = {}


class Compressor(abc.ABC):
    """
    Compressor base class, used with OP_COMPRESSED and the connection string or hello msg
    """

    @classmethod
    @abc.abstractmethod
    def name(cls) -> str:
        """Compressor name to be used with the connection string or hello msg"""
        pass

    @classmethod
    @abc.abstractmethod
    def id(cls) -> int:
        """Compressor ID to be used with OP_COMPRESSED"""
        pass

    @classmethod
    @abc.abstractmethod
    def compress(cls, data: bytes) -> bytes:
        """Compress method"""
        pass

    @classmethod
    @abc.abstractmethod
    def decompress(cls, data: bytes) -> bytes:
        """Decompress method"""
        pass

    @classmethod
    def compressor_names(cls) -> Set[str]:
        return set(_COMPRESSORS_BY_NAME.keys())

    @classmethod
    def by_id(cls, _id: int) -> Type['Compressor']:
        return _COMPRESSORS_BY_ID[_id]

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        _COMPRESSORS_BY_ID[cls.id()] = cls
        _COMPRESSORS_BY_NAME[cls.name()] = cls


try:
    import snappy


    class CompressorSnappy(Compressor):
        @classmethod
        def name(cls) -> str:
            return 'snappy'

        @classmethod
        def id(cls) -> int:
            return 1

        @classmethod
        def compress(cls, data: bytes) -> bytes:
            return snappy.compress(data)

        @classmethod
        def decompress(cls, data: bytes) -> bytes:
            return snappy.decompress(data)

except ImportError:
    pass


class CompressorZlib(Compressor):
    @classmethod
    def name(cls) -> str:
        return 'zlib'

    @classmethod
    def id(cls) -> int:
        return 2

    @classmethod
    def compress(cls, data: bytes) -> bytes:
        return zlib.compress(data)

    @classmethod
    def decompress(cls, data: bytes) -> bytes:
        return zlib.decompress(data)


try:
    import zstandard


    class CompressorZstd(Compressor):
        @classmethod
        def name(cls) -> str:
            return 'zstd'

        @classmethod
        def id(cls) -> int:
            return 3

        @classmethod
        def compress(cls, data: bytes) -> bytes:
            return zstandard.compress(data)

        @classmethod
        def decompress(cls, data: bytes) -> bytes:
            return zstandard.decompress(data)

except ImportError:
    pass
