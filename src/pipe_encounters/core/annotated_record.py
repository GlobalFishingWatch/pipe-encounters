"""AnnotatedRecord namedtuple and its coder."""

from collections import namedtuple
from typing import ClassVar

from .namedtuples import NamedtupleCoder
from .resampled_record import ResampledRecord as _ResampledRecord


AnnotatedRecord = namedtuple(
    "AnnotatedRecord", (*_ResampledRecord._fields, "closest_neighbors", "closest_distances")
)


class AnnotatedRecordCoder(NamedtupleCoder):
    """Coder for AnnotatedRecord."""

    target = AnnotatedRecord
    time_fields: ClassVar[list[str]] = ["timestamp"]


AnnotatedRecordCoder.register()
