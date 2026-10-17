from datetime import date, datetime, timezone

import apache_beam as beam

from apache_beam.testing.util import assert_that
from gfw.common.beam.transforms.bigquery import FakeReadFromBigQuery

from pipe_encounters.assets import schemas
from pipe_encounters.pipelines.partial_encounters.config import PartialEncountersConfig
from pipe_encounters.pipelines.partial_encounters.main import (
    positions_queries,
    read_ssvid_filter,
    run,
    split_id_prefix,
)


def config(**overrides):
    fields = dict(
        bq_in_positions=["project.dataset.positions"],
        bq_out_partial_encounters="project.dataset.raw_encounters",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 2),
        max_encounter_dist_km=0.5,
        min_encounter_time_minutes=120,
        labels={"environment": "test"},
    )
    fields.update(overrides)
    return PartialEncountersConfig(**fields)


def test_split_id_prefix():
    assert split_id_prefix("project.dataset.positions") == ("", "project.dataset.positions")
    assert split_id_prefix("VMS::project.dataset.vms") == ("VMS:", "project.dataset.vms")


def test_read_ssvid_filter(tmp_path):
    ssvids = tmp_path / "ssvids.txt"
    ssvids.write_text("'123', '456'")

    assert read_ssvid_filter(None) is None
    assert read_ssvid_filter("'123'") == "'123'"
    assert read_ssvid_filter(f"@{ssvids}") == "'123', '456'"


def test_positions_queries_reads_each_source_table():
    queries = positions_queries(
        config(bq_in_positions=["AIS::project.dataset.ais", "VMS::project.dataset.vms"])
    )

    assert [(q.source_positions, q.id_prefix) for q in queries] == [
        ("project.dataset.ais", "AIS:"),
        ("project.dataset.vms", "VMS:"),
    ]
    assert {(q.start_date, q.end_date) for q in queries} == {(date(2024, 1, 1), date(2024, 1, 2))}


# Beam's local runner, named explicitly: DirectRunner would fall back to other runners for
# pipelines Prism doesn't support.
RUNNER = "PrismRunner"

T0 = datetime(2024, 1, 1, 9, tzinfo=timezone.utc).timestamp()
EVERY_SECONDS = 10 * 60


def positions(seg_id, lat, hours=3):
    """Rows shaped like PositionsQuery's output: a segment drifting east, every 10 minutes."""
    return [
        dict(
            id=seg_id,
            timestamp=T0 + i * EVERY_SECONDS,
            lat=lat,
            lon=0.001 * i,
            speed=1.0,
            course=90.0,
        )
        for i in range(hours * 6 + 1)
    ]


class AssertWritten(beam.PTransform):
    """Stands in for WriteToBigQuery: checks the rows to be written, inside the pipeline."""

    def __init__(self, check, **kwargs):
        super().__init__()
        self._check = check

    def expand(self, pcoll):
        assert_that(pcoll, self._check)
        return pcoll


def check_one_encounter_seen_from_both_segments(rows):
    # Each piece appears once from each segment's side.
    assert sorted((r["vessel_1_seg_id"], r["vessel_2_seg_id"]) for r in rows) == [
        ("A", "B"),
        ("B", "A"),
    ]
    assert {f["name"] for f in schemas.get_schema("partial_encounters.json")} == set(rows[0])
    # Documents current behavior: positions are extrapolated up to max_gap_seconds (1 hour)
    # before the first and after the last position, so the piece starts at 08:00, not 09:00.
    assert {r["start_time"] for r in rows} == {T0 - 3600}
    assert {r["end_time"] for r in rows} == {T0 + 4 * 3600}


def test_run(tmp_path):
    reads, writes = [], []

    def read_factory(**kwargs):
        reads.append(kwargs)
        return FakeReadFromBigQuery(elements=positions("A", 0.0) + positions("B", 0.004), **kwargs)

    def write_factory(**kwargs):
        writes.append(kwargs)
        return AssertWritten(check_one_encounter_seen_from_both_segments, **kwargs)

    run(
        config(mock_bq_clients=True, unknown_parsed_args={"project": "test-project"}),
        read_from_bigquery_factory=read_factory,
        write_to_bigquery_factory=write_factory,
        runner=RUNNER,
    )

    assert len(reads) == 1
    assert "`project.dataset.positions`" in reads[0]["query"]
    assert reads[0]["bigquery_job_labels"] == {"environment": "test"}
    assert writes[0]["table"] == "project.dataset.raw_encounters"
    assert writes[0]["write_disposition"] == "WRITE_APPEND"
    assert writes[0]["create_disposition"] == "CREATE_NEVER"
