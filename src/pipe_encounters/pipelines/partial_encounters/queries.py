"""Queries of the partial-encounters pipeline."""

import dataclasses
import datetime

from dataclasses import dataclass

from gfw.common.query import Query


@dataclass
class PositionsQuery(Query):
    """Positions of each segment in [start_date, end_date), one row per position.

    Each row's ``id`` is its seg_id, prefixed by ``id_prefix``.
    Optionally limited to the vessels ``ssvid_filter`` (a subquery or a list of ssvids) returns.
    Its fields are the template's variables (see template_vars).
    """

    source_positions: str
    start_date: datetime.date
    end_date: datetime.date
    id_prefix: str = ""
    ssvid_filter: str = None

    template_filename = "positions.sql.j2"

    @property
    def template_vars(self) -> dict:
        """The template's variables: the query's fields."""
        return dataclasses.asdict(self)
