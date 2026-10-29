from datetime import date

from pipe_encounters.pipelines.partial_encounters.queries import PositionsQuery


def render(**overrides):
    fields = dict(
        source_positions="project.dataset.positions",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 2),
    )
    fields.update(overrides)
    return " ".join(PositionsQuery(**fields).render().split())


def test_reads_the_positions_of_the_date_range():
    query = render()

    assert "FROM `project.dataset.positions`" in query
    assert "date(timestamp) >= '2024-01-01' AND date(timestamp) < '2024-01-02'" in query
    assert "UNIX_MILLIS(timestamp) / 1000.0 AS timestamp" in query
    assert "seg_id AS id" in query
    assert "ssvid" not in query


def test_limits_the_vessels_to_the_ssvid_filter():
    assert "AND ssvid IN ('123', '456')" in render(ssvid_filter="'123', '456'")
