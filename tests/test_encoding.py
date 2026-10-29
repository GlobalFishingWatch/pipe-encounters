"""Tests for the byte and text encoding helpers."""

from pipe_encounters.encoding import to_bytes, to_text


def test_to_bytes() -> None:
    """Checks that to_bytes encodes text as UTF-8 and keeps bytes as they are."""
    assert to_bytes("seg-ñ") == "seg-ñ".encode("utf-8")
    assert to_bytes(b"seg") == b"seg"


def test_to_text() -> None:
    """Checks that to_text decodes UTF-8 bytes and keeps text as it is."""
    assert to_text("seg-ñ".encode("utf-8")) == "seg-ñ"
    assert to_text("seg") == "seg"
