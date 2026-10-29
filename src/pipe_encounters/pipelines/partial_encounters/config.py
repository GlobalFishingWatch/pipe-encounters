"""Configuration of the partial-encounters pipeline."""

from __future__ import annotations

from dataclasses import dataclass

from gfw.common.config import DatePipelineConfig


@dataclass(frozen=True, kw_only=True)
class PartialEncountersConfig(DatePipelineConfig):
    """Configuration of the partial-encounters pipeline.

    Processes the dates in [start_date, end_date).
    """

    bq_in_positions: str
    """Table to read the positions from."""

    bq_out_partial_encounters: str
    """Table in which to append the partial encounters."""

    max_encounter_dist_km: float
    """Maximum distance (km) between two vessels to be in an encounter."""

    min_encounter_time_minutes: float
    """Minimum duration (minutes) of a piece of an encounter."""

    max_gap_seconds: int = 3600
    """Gaps between positions longer than this (seconds) are not interpolated across."""

    max_gap_seconds_inclusive: bool = False
    """Also interpolate across gaps of exactly max_gap_seconds."""

    ssvid_filter: str = None
    """Subquery or list of ssvid to limit processing to, or a path to read it from, after @."""
