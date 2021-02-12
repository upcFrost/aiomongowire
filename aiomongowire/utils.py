import io


def decode_cstring(data: io.BytesIO) -> str:
    length = int.from_bytes(data.read(4), byteorder='little', signed=False)
    return data.read(length).decode()
