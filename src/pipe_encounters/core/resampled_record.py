"""ResampledRecord namedtuple and its coder."""

from collections import namedtuple
from typing import ClassVar

from .namedtuples import NamedtupleCoder
from .record import Record as _Record


fields = tuple(x for x in _Record._fields if x != "course")
ResampledRecord = namedtuple("ResampledRecord", (*fields, "point_density"))


class ResampledRecordCoder(NamedtupleCoder):
    """Coder for ResampledRecord."""

    target = ResampledRecord
    time_fields: ClassVar[list[str]] = ["timestamp"]


ResampledRecordCoder.register()
