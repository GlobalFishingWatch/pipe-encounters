from datetime import date

from pipe_encounters.pipelines.encounters.queries import PartialEncountersQuery


def render(**overrides):
    fields = dict(
        source_partial_encounters="project.dataset.raw_encounters",
        source_segment_info="project.dataset.segment_info",
        source_spatial_measures="project.dataset.spatial_measures",
        start_date=date(2017, 1, 1),
        end_date=date(2024, 1, 2),
    )
    fields.update(overrides)
    return " ".join(PartialEncountersQuery(**fields).render().split())


def test_reads_the_partial_encounters_that_start_in_the_date_range():
    query = render()

    assert "FROM `project.dataset.raw_encounters`" in query
    assert "DATE(start_time) >= '2017-01-01' AND DATE(start_time) < '2024-01-02'" in query
    assert "JOIN `project.dataset.spatial_measures` USING (gridcode)" in query
    assert "NOT IN" not in query


def test_maps_the_segments_to_vessel_ids():
    query = render()

    assert "SELECT seg_id, vessel_id FROM `project.dataset.segment_info`" in query


def test_skips_the_bad_segments():
    query = render(bad_segs="(SELECT seg_id FROM `project.dataset.segs` WHERE bad)")

    for v in [1, 2]:
        assert (
            f"vessel_{v}_seg_id NOT IN (SELECT seg_id FROM "
            "(SELECT seg_id FROM `project.dataset.segs` WHERE bad))"
        ) in query
