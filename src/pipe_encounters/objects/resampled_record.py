from collections import namedtuple
from .namedtuples import NamedtupleCoder
from .record import Record as _Record

fields = tuple(x for x in _Record._fields if x != 'course')
ResampledRecord = namedtuple("ResampledRecord", fields + ('point_density',))


class ResampledRecordCoder(NamedtupleCoder):
    target = ResampledRecord
    time_fields = ['timestamp']


ResampledRecordCoder.register()

  