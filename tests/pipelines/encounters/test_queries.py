"""Tests for the query that reads the partial encounters of the encounters pipeline."""

from datetime import date
from typing import Any

from pipe_encounters.pipelines.encounters.queries import PartialEncountersQuery


def render(**overrides: Any) -> str:
    """Renders the query with default sources and dates, with overrides applied."""
    fields = {
        "source_partial_encounters": "project.dataset.raw_encounters",
        "source_segment_info": "project.dataset.segment_info",
        "source_spatial_measures": "project.dataset.spatial_measures",
        "start_date": date(2017, 1, 1),
        "end_date": date(2024, 1, 2),
    }
    fields.update(overrides)
    return " ".join(PartialEncountersQuery(**fields).render().split())


def test_reads_the_partial_encounters_that_start_in_the_date_range() -> None:
    """Checks the query reads the partial encounters that start within the date range."""
    query = render()

    assert "FROM `project.dataset.raw_encounters`" in query
    assert "DATE(start_time) >= '2017-01-01' AND DATE(start_time) < '2024-01-02'" in query
    assert "JOIN `project.dataset.spatial_measures` USING (gridcode)" in query
    assert "NOT IN" not in query


def test_drops_the_partial_encounters_near_a_port_or_on_land() -> None:
    """Checks the query drops the partial encounters near a port or on land."""
    assert "WHERE distance_from_port_m >= 10 * 1000 AND distance_from_shore_m > 0" in render()
    assert "distance_from_port_m >= 5 * 1000" in render(min_distance_from_port_km=5)


def test_maps_the_segments_to_vessel_ids() -> None:
    """Checks the query maps the segment ids to vessel ids."""
    query = render()

    assert "SELECT seg_id, vessel_id FROM `project.dataset.segment_info`" in query


def test_skips_the_bad_segments() -> None:
    """Checks the query skips the partial encounters of the bad segments."""
    query = render(bad_segs="(SELECT seg_id FROM `project.dataset.segs` WHERE bad)")

    for v in [1, 2]:
        assert (
            f"vessel_{v}_seg_id NOT IN (SELECT seg_id FROM "
            "(SELECT seg_id FROM `project.dataset.segs` WHERE bad))"
        ) in query
