"""Base coder for serializing namedtuples to and from JSON."""

import datetime
import json as ujson

from typing import Any, ClassVar, NamedTuple

import apache_beam as beam


epoch = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)


def _datetime_to_s(x: datetime.datetime) -> float:
    return (x - epoch).total_seconds()


def _s_to_datetime(x: float) -> datetime.datetime:
    return epoch + datetime.timedelta(seconds=x)


class NamedtupleCoder(beam.coders.Coder):
    """A coder used for reading and writing nametuples to/from json."""

    # Overide target with actual target
    target = None
    # Overide time_fields with sequence of field names containing datetime instances
    time_fields: ClassVar[list[str]] = []

    @classmethod
    def _encode(cls, value: NamedTuple) -> NamedTuple:
        replacements = {x: _datetime_to_s(getattr(value, x)) for x in cls.time_fields}
        return value._replace(**replacements)

    @classmethod
    def encode(cls, value: NamedTuple) -> str:
        """Serialize a namedtuple to a JSON string."""
        return ujson.dumps(cls._encode(value))

    @classmethod
    def _decode(cls, value: NamedTuple) -> NamedTuple:
        replacements = {x: _s_to_datetime(getattr(value, x)) for x in cls.time_fields}
        return value._replace(**replacements)

    @classmethod
    def decode(cls, value: str | bytes) -> NamedTuple:
        """Deserialize a JSON string to a namedtuple."""
        return cls._decode(cls.target(*ujson.loads(value)))

    def is_deterministic(self) -> bool:
        """Return True, since the encoding is deterministic."""
        return True

    @classmethod
    def register(cls) -> None:
        """Register the coder and attach the FromDict and ToDict transforms to the target."""
        beam.coders.registry.register_coder(cls.target, cls)

        class FromDict(beam.PTransform):
            """converts a Dict to a namedtuple."""

            def from_dict(self, x: dict[str, Any]) -> NamedTuple:
                return cls._decode(cls.target(**x))

            def expand(self, p: beam.PCollection) -> beam.PCollection:
                return p | beam.Map(self.from_dict).with_output_types(Any)

        cls.target.FromDict = FromDict

        class ToDict(beam.PTransform):
            """converts namedtuple to a dict."""

            def to_dict(self, x: NamedTuple) -> dict[str, Any]:
                return cls._encode(x)._asdict()

            def expand(self, p: beam.PCollection) -> beam.PCollection:
                return p | beam.Map(self.to_dict).with_output_types(Any)

        cls.target.ToDict = ToDict
