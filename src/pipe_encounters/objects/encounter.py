from collections import namedtuple
from .namedtuples import NamedtupleCoder


RawEncounter = namedtuple("RawEncounter", 
    ["vessel_1_seg_id", "vessel_2_seg_id", 
     "start_time", "end_time", 
     "mean_latitude", "mean_longitude", 
     "median_distance_km", "median_speed_knots", 
     "vessel_1_point_count", "vessel_2_point_count",
     "start_lat", "start_lon", "end_lat", "end_lon",
])


class RawEncounterCoder(NamedtupleCoder):
    target = RawEncounter
    time_fields = ['start_time', 'end_time']


RawEncounterCoder.register()


Encounter = namedtuple("Encounter", 
    ["vessel_1_id", "vessel_2_id", 
     "vessel_1_seg_ids", "vessel_2_seg_ids",
     "start_time", "end_time", 
     "mean_latitude", "mean_longitude", 
     "median_distance_km", "median_speed_knots", 
     "vessel_1_point_count", "vessel_2_point_count",
     "start_lat", "start_lon", "end_lat", "end_lon",
])


class EncounterCoder(NamedtupleCoder):
    target = Encounter
    time_fields = ['start_time', 'end_time']


EncounterCoder.register()
