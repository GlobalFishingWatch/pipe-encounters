"""Queries of the encounters pipeline."""

import dataclasses
import datetime

from dataclasses import dataclass

from gfw.common.query import Query


@dataclass
class PartialEncountersQuery(Query):
    """Partial encounters that start in [start_date, end_date), one row per partial encounter.

    Each row gets both segments' vessel ids, from the segment info tables, and the distances
    from shore and from port of its mean position, from the spatial measures table.
    Optionally skips the partial encounters of the bad segments ``bad_segs`` returns.
    Its fields are the template's variables (see template_vars).
    """

    source_partial_encounters: str
    source_segment_info: list[tuple[str, str]]
    """(seg_id prefix, table) of each segment info table."""
    source_spatial_measures: str
    start_date: datetime.date
    end_date: datetime.date
    bad_segs: str = None

    @property
    def template_filename(self) -> str:
        """The template's file, in assets/queries."""
        return "partial_encounters.sql.j2"

    @property
    def template_vars(self) -> dict:
        """The template's variables: the query's fields."""
        return dataclasses.asdict(self)
