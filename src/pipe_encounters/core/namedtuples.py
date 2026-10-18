import datetime
import json as ujson

import apache_beam as beam


epoch = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)


def _datetime_to_s(x):
    return (x - epoch).total_seconds()


def _s_to_datetime(x):
    return epoch + datetime.timedelta(seconds=x)


class NamedtupleCoder(beam.coders.Coder):
    """A coder used for reading and writing nametuples to/from json"""

    # Overide target with actual target
    target = None
    # Overide time_fields with sequence of field names containing datetime instances
    time_fields = []

    @classmethod
    def _encode(cls, value):
        replacements = {x: _datetime_to_s(getattr(value, x)) for x in cls.time_fields}
        return value._replace(**replacements)

    @classmethod
    def encode(cls, value):
        return ujson.dumps(cls._encode(value))

    @classmethod
    def _decode(cls, value):
        replacements = {x: _s_to_datetime(getattr(value, x)) for x in cls.time_fields}
        return value._replace(**replacements)

    @classmethod
    def decode(cls, value):
        return cls._decode(cls.target(*ujson.loads(value)))

    def is_deterministic(self):
        return True

    @classmethod
    def register(cls):
        beam.coders.registry.register_coder(cls.target, cls)

        class FromDict(beam.PTransform):
            """converts a Dict to a namedtuple"""

            def from_dict(self, x):
                return cls._decode(cls.target(**x))

            def expand(self, p):
                return p | beam.Map(self.from_dict)

        cls.target.FromDict = FromDict

        class ToDict(beam.PTransform):
            """converts namedtuple to a dict"""

            def to_dict(self, x):
                return cls._encode(x)._asdict()

            def expand(self, p):
                return p | beam.Map(self.to_dict)

        cls.target.ToDict = ToDict
