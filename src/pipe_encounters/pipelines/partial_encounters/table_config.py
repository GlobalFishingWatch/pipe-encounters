"""Configuration of the partial encounters table."""

from dataclasses import dataclass
from datetime import date
from typing import Optional

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription

from pipe_encounters.assets import schemas


SUMMARY = """\
Pieces of vessel encounters: pairs of track segments that stayed within a maximum distance
of each other for at least a minimum time.
Each row is a piece of an encounter, not a whole one:
positions are processed one UTC day at a time, so an encounter that crosses midnight is
split into one piece per day; each piece appears twice, once from each segment's side;
and pieces are identified by segment, not by vessel.
The encounters command joins them into one row per encounter between two vessels.
"""

CAVEATS = """\
⬖ A piece shorter than the minimum time on its own day is dropped, so it can't be joined to
the rest of its encounter.
⬖ Positions are extrapolated up to the maximum gap before the first and after the last
position of each segment-day, so pieces can extend beyond the positions that support them.
"""


@dataclass
class PartialEncountersTableDescription(TableDescription):
    """Description of the partial encounters table."""

    repo_name: str = "pipe-encounters"
    title: str = "PARTIAL ENCOUNTERS"
    subtitle: str = "Pieces of vessel encounters, per UTC day and pair of track segments"
    summary: str = SUMMARY
    caveats: str = CAVEATS


@dataclass
class PartialEncountersTableConfig(TableConfig):
    """Configuration of the partial encounters table."""

    schema_file: str = "partial_encounters.json"
    # Same layout as the legacy pipeline's output table.
    partition_type: str = "MONTH"
    partition_field: str = "start_time"
    clustering_fields: tuple[str, ...] = ("start_time",)

    @property
    def schema(self) -> list[dict]:
        """The table's schema."""
        return schemas.get_schema(self.schema_file)

    def delete_query(self, start_date: date, end_date: Optional[date] = None) -> str:
        """Returns the query that deletes the rows of the processed range [start_date, end_date).

        The pipeline appends, so the range is cleared first to make reprocessing it idempotent.
        Rows are deleted by their start date, which is also their end date: pieces never cross
        midnight. Without end_date, it deletes everything from start_date on.
        """
        query = f"DELETE FROM `{self.table_id}` WHERE DATE(start_time) >= '{start_date}'"
        if end_date is not None:
            query += f" AND DATE(start_time) < '{end_date}'"

        return query
