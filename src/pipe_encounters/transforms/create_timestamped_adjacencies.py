from apache_beam import io
from apache_beam import Filter
from apache_beam import Map
from apache_beam import PTransform
from apache_beam.transforms.window import TimestampedValue

from pipe_encounters.objects.namedtuples import _datetime_to_s


class CreateTimestampedAdjacencies(PTransform):

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date


    def extract_nbr_dict(self, item):
        return dict(
            vessel_id=item.id,
            timestamp=_datetime_to_s(item.timestamp),
            neighbor_count=item.neighbor_count
        )

    def expand(self, xs):
        return (xs
            | Filter(lambda x: self.start_date.date() <= x.timestamp.date() <= self.end_date.date())
            | Map(self.extract_nbr_dict)
            | Map(lambda x: TimestampedValue(x, x['timestamp']))
        )
