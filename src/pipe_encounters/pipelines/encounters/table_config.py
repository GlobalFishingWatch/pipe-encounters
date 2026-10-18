"""Configuration of the encounters table."""

from dataclasses import dataclass

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription

from pipe_encounters.assets import schemas


SUMMARY = """\
Encounters between two vessels: one row per encounter, built by joining the partial
encounters of the same two vessels that are less than a minimum number of hours apart,
across days, track segments and both vessels' sides.
Encounters within 10 km of a port, or without a positive distance from shore, are dropped.
The published encounter events are built from this table by pipe-events.
"""

CAVEATS = """\
⬖ The vessels' point counts are doubled: they add up both sides of each partial encounter.
⬖ Parts of an encounter shorter than the minimum time on their own day are missing, since
partial-encounters drops them.
"""


@dataclass
class EncountersTableDescription(TableDescription):
    """Description of the encounters table."""

    repo_name: str = "pipe-encounters"
    title: str = "ENCOUNTERS"
    subtitle: str = "Encounters between two vessels"
    summary: str = SUMMARY
    caveats: str = CAVEATS


@dataclass
class EncountersTableConfig(TableConfig):
    """Configuration of the encounters table."""

    schema_file: str = "encounters.json"
    # Same layout as the legacy pipeline's output table.
    partition_type: str = "MONTH"
    partition_field: str = "start_time"
    clustering_fields: tuple[str, ...] = ("start_time",)

    @property
    def schema(self) -> list[dict]:
        """The table's schema."""
        return schemas.get_schema(self.schema_file)
