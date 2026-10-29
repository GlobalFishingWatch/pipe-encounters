"""Core transform of the partial-encounters pipeline."""

import datetime

from apache_beam import Filter, Map, PTransform
from apache_beam.transforms.window import TimestampedValue

from pipe_encounters.core.encounter import RawEncounter
from pipe_encounters.core.record import Record
from pipe_encounters.pipelines.partial_encounters.transforms.compute_adjacency import (
    ComputeAdjacency,
)
from pipe_encounters.pipelines.partial_encounters.transforms.compute_encounters import (
    ComputeEncounters,
)
from pipe_encounters.pipelines.partial_encounters.transforms.resample import Resample
from pipe_encounters.transforms.add_id import AddRawEncounterId


RESAMPLE_INCREMENT_MINUTES = 10.0


def check_schema(x, schema):
    """Checks that a row has exactly the schema's fields, all required, with valid types."""
    assert set(x.keys()) == set([x["name"] for x in schema]), x.keys()
    for field in schema:
        assert field["mode"] == "REQUIRED"
        # TODO: support 'NULLABLE' and 'REPEATED'
        ftype = field["type"]
        val = x[field["name"]]
        allowed_types_map = {
            "STRING": (str,),
            "INTEGER": (int,),
            "FLOAT": (int, float),
            "TIMESTAMP": (int, float),
        }
        if ftype not in allowed_types_map:
            raise ValueError(f"unknown schema type {field}")
        allowed_types = allowed_types_map[ftype]
        assert isinstance(val, allowed_types), (field, val)
    return x


class DetectPartialEncounters(PTransform):
    """Detects the pieces of encounters in position rows, as partial encounter rows.

    Args:
        start_date:
            First date of the range, inclusive.
        end_date:
            Date the range ends at, exclusive. Pieces are kept if their end date is in range.
        max_encounter_dist_km:
            Maximum distance (km) between two vessels to be in an encounter.
        min_encounter_time_minutes:
            Minimum duration (minutes) of a piece of an encounter.
        max_gap_seconds:
            Gaps between positions longer than this (seconds) are not interpolated across.
        max_gap_seconds_inclusive:
            Also interpolate across gaps of exactly max_gap_seconds.
        schema:
            The output table's schema, to check the rows against.
    """

    def __init__(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        max_encounter_dist_km: float,
        min_encounter_time_minutes: float,
        max_gap_seconds: int,
        max_gap_seconds_inclusive: bool,
        schema: list[dict],
    ):
        super().__init__()
        self.start_date = start_date
        self.end_date = end_date
        self.max_encounter_dist_km = max_encounter_dist_km
        self.min_encounter_time_minutes = min_encounter_time_minutes
        self.max_gap_seconds = max_gap_seconds
        self.max_gap_seconds_inclusive = max_gap_seconds_inclusive
        self.schema = schema

    def expand(self, positions):
        start_date, end_date = self.start_date, self.end_date
        return (
            positions
            | Record.FromDict()
            | Resample(
                increment_s=60 * RESAMPLE_INCREMENT_MINUTES,
                max_gap_s=self.max_gap_seconds,
                inclusive=self.max_gap_seconds_inclusive,
            )
            | ComputeAdjacency(max_adjacency_distance_km=self.max_encounter_dist_km)
            | ComputeEncounters(
                max_km_for_encounter=self.max_encounter_dist_km,
                min_minutes_for_encounter=self.min_encounter_time_minutes,
            )
            | Filter(lambda x: start_date <= x.end_time.date() < end_date)
            | RawEncounter.ToDict()
            | AddRawEncounterId()
            | Map(lambda x: TimestampedValue(x, x["end_time"]))
            | Map(check_schema, schema=self.schema)
        )
