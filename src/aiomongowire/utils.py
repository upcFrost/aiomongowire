import io


def decode_cstring(data: io.BytesIO) -> str:
    """
    Decode BSON C-string as python string
    """
    length = int.from_bytes(data.read(4), byteorder='little', signed=False)
    return data.read(length).decode()
