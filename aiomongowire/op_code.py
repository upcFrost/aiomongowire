from enum import IntEnum


class OpCode(IntEnum):
    """
    Request opcodes
    https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#request-opcodes
    """
    OP_REPLY = 1,  # Reply to a client request. responseTo is set.
    OP_UPDATE = 2001,  # Update document.
    OP_INSERT = 2002,  # Insert new document.
    RESERVED = 2003,  # Formerly used for OP_GET_BY_OID.
    OP_QUERY = 2004,  # Query a collection.
    OP_GET_MORE = 2005,  # Get more data from a query. See Cursors.
    OP_DELETE = 2006,  # Delete documents.
    OP_KILL_CURSORS = 2007,  # Notify database that the client has finished with the cursor.
    OP_MSG = 2013  # Send a message using the format introduced in MongoDB 3.6

    def __bytes__(self):
        return self.to_bytes(length=4, byteorder='little', signed=False)
