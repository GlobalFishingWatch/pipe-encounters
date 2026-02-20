from __future__ import division
from apache_beam import PTransform
from apache_beam import Map
import apache_beam as beam
import six
import datetime as dt
import hashlib

from ..objects.record import Record


class AddRawEncounterId(PTransform):

    def __init__(self, prefix=''):
        self.prefix = prefix

    def add_id(self, x):
        # Ensure that timestamps are naive or in UTC
        start = dt.datetime.utcfromtimestamp(x['start_time'])
        text = (f"{self.prefix}encounter|{x['vessel_1_seg_id']}|{x['vessel_2_seg_id']}|{start:%Y-%m-%d %H:%M:%S}")
        x['encounter_id'] = hashlib.md5(text.encode('latin-1')).hexdigest()
        return x


    def expand(self, xs):
        return (
            xs
            | Map(self.add_id)
        )



class AddEncounterId(PTransform):

    def __init__(self, prefix=''):
        self.prefix = prefix

    def add_id(self, x):
        # Ensure that timestamps are naive or in UTC
        start = dt.datetime.utcfromtimestamp(x['start_time'])

        text = (f"{self.prefix}encounter|{x['vessel_1_seg_ids'][0]}|{x['vessel_2_seg_ids'][0]}|" +
                f"{start:%Y-%m-%d %H:%M:%S}")
        x['encounter_id'] = hashlib.md5(text.encode('latin-1')).hexdigest()
        return x


    def expand(self, xs):
        return (
            xs
            | Map(self.add_id)
        )