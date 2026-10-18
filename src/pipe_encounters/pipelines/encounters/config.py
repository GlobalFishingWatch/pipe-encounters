"""Configuration of the encounters pipeline."""

from __future__ import annotations

from dataclasses import dataclass

from gfw.common.config import DatePipelineConfig


@dataclass(frozen=True, kw_only=True)
class EncountersConfig(DatePipelineConfig):
    """Configuration of the encounters pipeline.

    Processes the partial encounters that start in [start_date, end_date).
    """

    bq_in_partial_encounters: str
    """Table to read the partial encounters from."""

    bq_in_segment_info: list[str]
    """Tables mapping each seg_id to its vessel_id, each optionally prefixed by 'ID::'."""

    bq_in_spatial_measures: str
    """Table to read the distance from shore and from port from."""

    bq_out_encounters: str
    """Table in which to write the encounters. Its content is replaced."""

    bad_segs: str = None
    """Table or subquery with the seg_id of bad segments, whose partial encounters are skipped."""

    min_hours_between_encounters: float = 4
    """Partial encounters less than these hours apart are joined into one."""

    min_encounter_time_minutes: float = None
    """Minimum duration (minutes) of an encounter. Not applied if not set."""
