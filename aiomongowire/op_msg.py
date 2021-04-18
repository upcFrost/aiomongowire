import abc
import io
from enum import IntEnum
from typing import SupportsBytes, List

import bson

from aiomongowire.base_op import BaseOp
from aiomongowire.message_header import MessageHeader
from aiomongowire.op_code import OpCode


class OpMsg(BaseOp):
    """
    Mongo 3.6+ OP_MSG
    """
    __slots__ = ['header', 'flag_bits', 'sections', 'checksum']

    class PayloadType(IntEnum):
        """
        Section payload type. Each message should have one body and one documents section
        """
        BODY = 0
        DOCUMENTS = 1

    class Section(abc.ABC, SupportsBytes):
        """
        Generic section
        """

        def __init__(self, payload_type: 'OpMsg.PayloadType'):
            self.payload_type = payload_type

    class Body(Section):
        """
        Body section. Should contain a single document with op name, db name and write concert
        """

        def __init__(self, data: dict):
            super().__init__(OpMsg.PayloadType.BODY)
            self.data = data

        def __bytes__(self) -> bytes:
            return int.to_bytes(self.payload_type, length=1, byteorder='little') + bson.dumps(self.data)

        @classmethod
        def identifier(cls) -> str:
            """
            Command argument to use in the documents section.
            https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst#id14
            """
            return ""

        @classmethod
        def from_data(cls, data: io.BytesIO) -> 'OpMsg.Body':
            len_bytes = data.read(4)
            data_len = int.from_bytes(len_bytes, byteorder='little')
            return cls(bson.loads(len_bytes + data.read(data_len - 4)))

        def __str__(self):
            return str(self.data)

    class Insert(Body):
        """
        Insert op body
        """

        def __init__(self, db: str, collection: str):
            super().__init__({"insert": collection, "$db": db})

        @classmethod
        def identifier(cls) -> str:
            return "documents"

    class Update(Body):
        """
        Update op body
        """

        def __init__(self, db: str, collection: str):
            super().__init__({"update": collection, "$db": db})

        @classmethod
        def identifier(cls) -> str:
            return "updates"

    class Delete(Body):
        """
        Delete op body
        """

        def __init__(self, db: str, collection: str):
            super().__init__({"delete": collection, "$db": db})

        @classmethod
        def identifier(cls) -> str:
            return "deletes"

    class Document(Section):
        """
        Documents section. Should include identifier (bound to the op type) and a list of documents
        """
        __slots__ = ['size', 'identifier', 'documents']

        def __init__(self, size: int, identifier: str, documents: List[dict] = None):
            super().__init__(OpMsg.PayloadType.DOCUMENTS)
            self.size = size
            self.identifier = identifier
            self.documents = documents or []

        def __bytes__(self):
            result = io.BytesIO()
            result.write(bson.encode_cstring(self.identifier))
            for doc in self.documents:
                result.write(bson.dumps(doc))
            data = result.getvalue()
            self.size = len(data) + 4

            payload_type = int.to_bytes(self.payload_type, length=1, byteorder='little')
            size = self.size.to_bytes(length=4, byteorder='little', signed=True)
            return payload_type + size + data

        def __str__(self):
            return f"Identifier: {self.identifier}, Data: {str(self.documents)}"

    def __init__(self, header: MessageHeader, flag_bits: int, sections: List[Section], checksum: int = None):
        super().__init__(header)
        self.flag_bits = flag_bits
        self.sections = sections
        self.checksum = checksum

    @classmethod
    def has_reply(cls) -> bool:
        return True

    @classmethod
    def op_code(cls) -> OpCode:
        return OpCode.OP_MSG

    @classmethod
    def _from_data(cls, message_length: int, header: MessageHeader, data: io.BytesIO):
        flag_bits = int.from_bytes(data.read(4), byteorder='little', signed=False)  # uint32, message flags

        sections = {}
        next_section = int.from_bytes(data.read(1), byteorder='little', signed=True)
        if next_section == OpMsg.PayloadType.BODY:
            sections[OpMsg.PayloadType.BODY] = OpMsg.Body.from_data(data)
        elif next_section == OpMsg.PayloadType.DOCUMENTS:
            sections[OpMsg.PayloadType.DOCUMENTS] = OpMsg.Document.from_data(data)
        else:
            raise ValueError(f"Unknown section type for OpMsg: {next_section}")
        checksum = int.from_bytes(data.read(4), byteorder='little', signed=False)  # optional CRC-32C checksum
        return cls(header, flag_bits, sections=list(sections.values()), checksum=checksum)

    def _as_bytes(self) -> bytes:
        result = io.BytesIO()
        result.write(self.flag_bits.to_bytes(length=4, byteorder='little', signed=False))
        for section in self.sections:
            result.write(bytes(section))
        if self.checksum:
            result.write(self.checksum.to_bytes(length=4, byteorder='little', signed=False))
        return result.getvalue()

    def __str__(self):
        return f"{[str(s) for s in self.sections]}"
