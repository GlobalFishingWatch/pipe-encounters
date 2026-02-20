from apache_beam import PTransform
from apache_beam import io
from apache_beam import Map
import datetime as dt
import pytz


# class Source(PTransform):

#     def __init__(self, query):
#         self.query = query

#     def decode_datetime(self, value):
#         return dt.datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f %Z").replace(tzinfo=pytz.utc)

#     def decode_datetime_fields(self, x):
#         x['timestamp'] = self.decode_datetime(x['timestamp'])
#         return x

#     def expand(self, xs):
#         return (
#             xs
#             | io.Read(io.gcp.bigquery.BigQuerySource(query=self.query))
#             | Map(self.decode_datetime_fields)
#         )


# class EncounterSource(PTransform):

#     def __init__(self, query):
#         self.query = query

#     def decode_datetime(self, value):
#         return dt.datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f %Z").replace(tzinfo=pytz.utc)

#     def decode_datetime_fields(self, x):
#         x['start_time'] = self.decode_datetime(x['start_time'])
#         x['end_time'] = self.decode_datetime(x['end_time'])
#         return x

#     def expand(self, xs):
#         return (
#             xs
#             | io.Read(io.gcp.bigquery.BigQuerySource(query=self.query))
#             | Map(self.decode_datetime_fields)
#         )