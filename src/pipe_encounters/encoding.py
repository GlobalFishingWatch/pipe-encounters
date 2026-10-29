"""Conversions between str and bytes, which the encounter ids travel as."""


def to_bytes(value: str | bytes) -> bytes:
    """Returns value as bytes, encoding it in UTF-8 if it's a str."""
    if isinstance(value, bytes):
        return value

    return value.encode("utf-8")


def to_text(value: str | bytes) -> str:
    """Returns value as a str, decoding it from UTF-8 if it's bytes."""
    if isinstance(value, str):
        return value

    return value.decode("utf-8")
