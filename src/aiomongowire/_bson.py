import io
from typing import Union


class BsonTools:
    """
    Base class for BSON parser, actual implementation should be configured
    """

    def encode_cstring(self, s: str) -> bytes:
        raise NotImplementedError("Bson parser not installed/configured")

    def decode_cstring(self, b: Union[io.BytesIO, bytes]) -> str:
        raise NotImplementedError("Bson parser not installed/configured")

    def encode_object(self, d: Union[list, dict]) -> bytes:
        raise NotImplementedError("Bson parser not installed/configured")

    def decode_object(self, b: Union[io.BytesIO, bytes]) -> Union[list, dict]:
        raise NotImplementedError("Bson parser not installed/configured")


_BSON_PARSER = BsonTools()


def set_bson_parser(parser: BsonTools):
    """
    Set bson parser to a custom one
    """
    global _BSON_PARSER
    _BSON_PARSER = parser


def get_bson_parser() -> BsonTools:
    """
    Set bson parser to a custom one
    """
    return _BSON_PARSER


try:
    # Pymongo and bson have name conflict, trying to check which one is installed
    import pymongo
    import bson


    class PymongoBson(BsonTools):
        """
        BsonTool implementation for pymongo package
        """

        def encode_cstring(self, s: str) -> bytes:
            return bson._make_c_string(s)

        def decode_cstring(self, b: Union[io.BytesIO, bytes]) -> str:
            if isinstance(b, io.BytesIO):
                length = int.from_bytes(b.read(4), byteorder='little', signed=False)
                return b.read(length).decode()
            else:
                return b.decode()

        def encode_object(self, d: Union[list, dict]) -> bytes:
            return bson.encode(d)

        def decode_object(self, b: Union[io.BytesIO, bytes]) -> Union[list, dict]:
            return bson.decode(b)


    _BSON_PARSER = PymongoBson()
except ImportError:
    pass

try:
    if _BSON_PARSER:
        raise ImportError
    import bson


    class PyBson(BsonTools):
        """
        BsonTool implementation for bson package
        """

        def encode_cstring(self, s: str) -> bytes:
            return bson.encode_cstring(s)

        def decode_cstring(self, b: Union[io.BytesIO, bytes]) -> str:
            if isinstance(b, io.BytesIO):
                length = int.from_bytes(b.read(4), byteorder='little', signed=False)
                return b.read(length).decode()
            else:
                return b.decode()

        def encode_object(self, d: Union[list, dict]) -> bytes:
            return bson.dumps(d)

        def decode_object(self, b: Union[io.BytesIO, bytes]) -> Union[list, dict]:
            return bson.loads(b)


    _BSON_PARSER = PyBson()
except ImportError:
    pass
