"""Core transform of the encounters pipeline."""

from typing import Any

from apache_beam import Filter, Map, PCollection, PTransform

from pipe_encounters.core.encounter import Encounter, RawEncounter
from pipe_encounters.encoding import to_bytes
from pipe_encounters.pipelines.encounters.transforms.merge_encounters import MergeEncounters
from pipe_encounters.transforms.add_id import AddEncounterId


def combine_ids(obj: dict[str, Any]) -> dict[str, Any]:
    """Replaces each segment id with a (vessel id, segment id) pair, as MergeEncounters needs."""
    for v in [1, 2]:
        obj[f"vessel_{v}_seg_id"] = (
            to_bytes(obj.pop(f"vessel_{v}_id")),
            to_bytes(obj[f"vessel_{v}_seg_id"]),
        )
    return obj


class JoinPartialEncounters(PTransform):
    """Joins partial encounter rows into encounter rows, one per encounter between two vessels.

    Args:
        min_hours_between_encounters:
            Partial encounters less than these hours apart are joined into one.
        min_encounter_time_minutes:
            Minimum duration (minutes) of an encounter. Not applied if None.
    """

    def __init__(
        self, min_hours_between_encounters: float, min_encounter_time_minutes: float | None
    ) -> None:
        """Initialize the transform.

        Args:
            min_hours_between_encounters: Partial encounters less than these hours apart are joined.
            min_encounter_time_minutes: Minimum duration of an encounter in minutes, or None.
        """
        super().__init__()
        self.min_hours_between_encounters = min_hours_between_encounters
        self.min_encounter_time_minutes = min_encounter_time_minutes

    def expand(self, partial_encounters: PCollection) -> PCollection:
        """Joins the partial encounters into encounters, filtered by minimum duration.

        Args:
            partial_encounters: Collection of partial encounter rows.

        Returns:
            Collection of encounter rows with encounter_id added.
        """
        encounters = (
            partial_encounters
            | "CombineSegVesselIds" >> Map(combine_ids).with_output_types(Any)
            | RawEncounter.FromDict()
            | MergeEncounters(min_hours_between_encounters=self.min_hours_between_encounters)
        )

        min_minutes = self.min_encounter_time_minutes
        if min_minutes is not None:
            encounters = encounters | Filter(
                lambda x: (x.end_time - x.start_time).total_seconds() / 60.0 >= min_minutes
            )

        return encounters | "FilteredToDicts" >> Encounter.ToDict() | AddEncounterId()
