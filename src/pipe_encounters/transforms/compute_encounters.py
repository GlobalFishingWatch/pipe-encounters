from ..objects.encounter import RawEncounter
from .compute_adjacency import compute_distance as compute_distance_km
from apache_beam import FlatMap
from apache_beam import GroupByKey
from apache_beam import Map
from apache_beam import PTransform
from collections import defaultdict
from statistics import mean
from statistics import median

import datetime
import itertools as it
import logging
import math
import six

MPS_TO_KNOTS = 1.94384

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = it.tee(iterable)
    next(b, None)
    return six.moves.zip(a, b)


def implied_speed_mps(rcd1, rcd2):
    distance = 1000 * compute_distance_km(rcd1, rcd2) 
    duration = (rcd2.timestamp - rcd1.timestamp).total_seconds() # s
    return distance / duration



class ComputeEncounters(PTransform):


    def __init__(self, max_km_for_encounter, min_minutes_for_encounter):
        self.min_minutes_for_encounter = min_minutes_for_encounter
        self.max_km_for_encounter = max_km_for_encounter

    def _try_to_create_encounter(self, adjacency_run):
        if len(adjacency_run) < 2:
            return

        start_time = adjacency_run[0][0].timestamp
        end_time = adjacency_run[-1][0].timestamp
        encounter_duration = end_time - start_time

        if encounter_duration < datetime.timedelta(minutes=self.min_minutes_for_encounter):
            return

        implied_speeds = [implied_speed_mps(rcd1a, rcd1b) for 
                        ((rcd1a, rcd2a, dista), (rcd1b, rcd2b, distb)) in pairwise(adjacency_run)]


        median_distance_km = median(dist for (rcd1, rcd2, dist) in adjacency_run)
        mean_lat = mean(rcd1.lat for (rcd1, rcd2, dist) in adjacency_run)
        cos_lon = mean(math.cos(math.radians(rcd1.lon)) for (rcd1, rcd2, dist) in adjacency_run)
        sin_lon = mean(math.sin(math.radians(rcd1.lon)) for (rcd1, rcd2, dist) in adjacency_run)
        mean_lon = math.degrees(math.atan2(sin_lon, cos_lon))
        median_speed_knots = median(implied_speeds) * MPS_TO_KNOTS

        vessel_1_points = int(round(sum(rcd1.point_density for (rcd1, rcd2, dist) in adjacency_run)))
        vessel_2_points = int(round(sum(rcd2.point_density for (rcd1, rcd2, dist) in adjacency_run)))

        rcd1, rcd2, _ = adjacency_run[0]

        yield RawEncounter(
                        vessel_1_seg_id = rcd1.id,
                        vessel_2_seg_id = rcd2.id,
                        start_time = start_time,
                        end_time = end_time,
                        mean_latitude = mean_lat,
                        mean_longitude = mean_lon,
                        median_distance_km = median_distance_km,
                        median_speed_knots = median_speed_knots,
                        vessel_1_point_count = vessel_1_points,
                        vessel_2_point_count = vessel_2_points,
                        start_lat = rcd1.lat, 
                        start_lon = rcd1.lon, 
                        end_lat = adjacency_run[-1][0].lat, 
                        end_lon = adjacency_run[-1][0].lon,
                        )

    def _create_valid_encounters(self, adjacency_runs, active_ids):
        removal_list = []
        for k, v in adjacency_runs.items():
            if k not in active_ids:
                removal_list.append(k)
                yield from self._try_to_create_encounter(v) 
        for k in removal_list:
            del adjacency_runs[k]

    def compute_encounters(self, item):
        """Create encounters for one vessel

        Parameters
        ==========
        item : (str, sequence of AnnotatedRecord)
            A tuple of the vessel id and the associated annotated records

        Yields
        ======
        Encounter
        """
        key, records = item
        adjacency_runs = defaultdict(list)

        for rcd1 in records:
            active_ids = set()
            for ndx, rcd2 in enumerate(rcd1.closest_neighbors):
                distance = rcd1.closest_distances[ndx]
                if distance <= self.max_km_for_encounter:
                    adjacency_runs[rcd2.id].append((rcd1, rcd2, distance))
                    active_ids.add(rcd2.id)
            yield from self._create_valid_encounters(adjacency_runs, active_ids)
        yield from self._create_valid_encounters(adjacency_runs, set())

    def tag_with_id(self, item):
        return (f'{item.id}_{item.timestamp.date()}', item)

    def sort_by_time(self, item):
        key, value = item
        value = list(value)
        value.sort(key=lambda x: x.timestamp)
        return key, value

    def expand(self, xs):
        return (
            xs
            | Map(self.tag_with_id)
            | "Group by id" >> GroupByKey()
            | Map(self.sort_by_time)
            | FlatMap(self.compute_encounters)
        )
 
