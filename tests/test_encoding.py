from pipe_encounters.encoding import to_bytes, to_text


def test_to_bytes():
    assert to_bytes("seg-ñ") == "seg-ñ".encode("utf-8")
    assert to_bytes(b"seg") == b"seg"


def test_to_text():
    assert to_text("seg-ñ".encode("utf-8")) == "seg-ñ"
    assert to_text("seg") == "seg"
