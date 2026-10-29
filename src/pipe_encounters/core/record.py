"""Record namedtuple and its coder."""

from collections import namedtuple
from typing import ClassVar

from .namedtuples import NamedtupleCoder


Record = namedtuple("Record", ["id", "timestamp", "lat", "lon", "speed", "course"])


class RecordCoder(NamedtupleCoder):
    """Coder for Record."""

    target = Record
    time_fields: ClassVar[list[str]] = ["timestamp"]


RecordCoder.register()
