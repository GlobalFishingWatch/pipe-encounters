"""Core transform of the encounters pipeline."""

import six

from apache_beam import Filter, FlatMap, Map, PTransform

from pipe_encounters.core.encounter import Encounter, RawEncounter
from pipe_encounters.pipelines.encounters.transforms.merge_encounters import MergeEncounters
from pipe_encounters.transforms.add_id import AddEncounterId


MIN_DISTANCE_FROM_PORT_KM = 10


def combine_ids(obj):
    """Replaces each segment id with a (vessel id, segment id) pair, as MergeEncounters needs."""
    for v in [1, 2]:
        obj[f"vessel_{v}_seg_id"] = (
            six.ensure_binary(obj.pop(f"vessel_{v}_id")),
            six.ensure_binary(obj[f"vessel_{v}_seg_id"]),
        )
    return obj


def filter_by_distance(obj, min_distance_from_port_km):
    """Drops the rows close to a port, or without a positive distance from shore."""
    distance_from_shore_m = obj.pop("distance_from_shore_m")
    distance_from_port_m = obj.pop("distance_from_port_m")
    if distance_from_port_m < min_distance_from_port_km * 1000:
        return []
    elif distance_from_shore_m is None or distance_from_shore_m <= 0:
        return []
    return [obj]


class JoinPartialEncounters(PTransform):
    """Joins partial encounter rows into encounter rows, one per encounter between two vessels.

    Args:
        min_hours_between_encounters:
            Partial encounters less than these hours apart are joined into one.
        min_encounter_time_minutes:
            Minimum duration (minutes) of an encounter. Not applied if None.
    """

    def __init__(self, min_hours_between_encounters: float, min_encounter_time_minutes: float):
        super().__init__()
        self.min_hours_between_encounters = min_hours_between_encounters
        self.min_encounter_time_minutes = min_encounter_time_minutes

    def expand(self, partial_encounters):
        encounters = (
            partial_encounters
            | "CombineSegVesselIds" >> Map(combine_ids)
            | FlatMap(filter_by_distance, min_distance_from_port_km=MIN_DISTANCE_FROM_PORT_KM)
            | RawEncounter.FromDict()
            | MergeEncounters(min_hours_between_encounters=self.min_hours_between_encounters)
        )

        min_minutes = self.min_encounter_time_minutes
        if min_minutes is not None:
            encounters = encounters | Filter(
                lambda x: (x.end_time - x.start_time).total_seconds() / 60.0 >= min_minutes
            )

        return encounters | "FilteredToDicts" >> Encounter.ToDict() | AddEncounterId()
