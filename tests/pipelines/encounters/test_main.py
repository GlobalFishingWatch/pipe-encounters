from datetime import date, datetime, timezone

import apache_beam as beam

from apache_beam.testing.util import assert_that
from gfw.common.beam.transforms.bigquery import FakeReadFromBigQuery

from pipe_encounters.assets import schemas
from pipe_encounters.pipelines.encounters.config import EncountersConfig
from pipe_encounters.pipelines.encounters.main import partial_encounters_query, run


def config(**overrides):
    fields = dict(
        bq_in_partial_encounters="project.dataset.raw_encounters",
        bq_in_segment_info=["project.dataset.segment_info"],
        bq_in_spatial_measures="project.dataset.spatial_measures",
        bq_out_encounters="project.dataset.encounters",
        start_date=date(2017, 1, 1),
        end_date=date(2024, 1, 2),
        labels={"environment": "test"},
    )
    fields.update(overrides)
    return EncountersConfig(**fields)


def test_partial_encounters_query():
    query = partial_encounters_query(
        config(bq_in_segment_info=["AIS::project.dataset.ais", "project.dataset.vms"])
    )

    assert query.source_partial_encounters == "project.dataset.raw_encounters"
    assert query.source_segment_info == [
        ("AIS:", "project.dataset.ais"),
        ("", "project.dataset.vms"),
    ]
    assert (query.start_date, query.end_date) == (date(2017, 1, 1), date(2024, 1, 2))


# Beam's local runner, named explicitly: DirectRunner would fall back to other runners for
# pipelines Prism doesn't support.
RUNNER = "PrismRunner"

DAY = datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp()
HOUR = 3600


def partial_encounter(seg_1, seg_2, start, end, points=6, distance_from_port_m=50_000):
    """A row shaped like PartialEncountersQuery's output. Segment 'A1' is of vessel 'vessel-A'."""
    return dict(
        vessel_1_seg_id=seg_1,
        vessel_2_seg_id=seg_2,
        vessel_1_id=f"vessel-{seg_1[0]}",
        vessel_2_id=f"vessel-{seg_2[0]}",
        start_time=start,
        end_time=end,
        mean_latitude=1.0,
        mean_longitude=2.0,
        median_distance_km=0.3,
        median_speed_knots=1.0,
        vessel_1_point_count=points,
        vessel_2_point_count=points,
        start_lat=1.0,
        start_lon=2.0,
        end_lat=1.0,
        end_lon=2.0,
        distance_from_shore_m=20_000,
        distance_from_port_m=distance_from_port_m,
    )


# An encounter from 22:00 to 02:00, split at midnight, each piece from both vessels' sides,
# and a piece of another encounter next to a port.
PARTIAL_ENCOUNTERS = [
    partial_encounter("A1", "B1", DAY + 22 * HOUR, DAY + 23 * HOUR + 50 * 60),
    partial_encounter("B1", "A1", DAY + 22 * HOUR, DAY + 23 * HOUR + 50 * 60),
    partial_encounter("A1", "B1", DAY + 24 * HOUR, DAY + 26 * HOUR),
    partial_encounter("B1", "A1", DAY + 24 * HOUR, DAY + 26 * HOUR),
    partial_encounter("C1", "D1", DAY, DAY + 3 * HOUR, distance_from_port_m=5_000),
]


class AssertWritten(beam.PTransform):
    """Stands in for WriteToBigQuery: checks the rows to be written, inside the pipeline."""

    def __init__(self, check, **kwargs):
        super().__init__()
        self._check = check

    def expand(self, pcoll):
        assert_that(pcoll, self._check)
        return pcoll


def check_one_encounter_between_vessels_a_and_b(rows):
    assert len(rows) == 1
    row = rows[0]
    assert {f["name"] for f in schemas.get_schema("encounters.json")} == set(row)
    # Documents current behavior: vessel ids are bytes until written; Beam decodes them.
    assert (row["vessel_1_id"], row["vessel_2_id"]) == (b"vessel-A", b"vessel-B")
    assert (row["vessel_1_seg_ids"], row["vessel_2_seg_ids"]) == (["A1"], ["B1"])
    assert (row["start_time"], row["end_time"]) == (DAY + 22 * HOUR, DAY + 26 * HOUR)
    # Documents current behavior: the point counts add up both sides of each piece.
    assert (row["vessel_1_point_count"], row["vessel_2_point_count"]) == (24, 24)


def test_run():
    reads, writes = [], []

    def read_factory(**kwargs):
        reads.append(kwargs)
        return FakeReadFromBigQuery(elements=PARTIAL_ENCOUNTERS, **kwargs)

    def write_factory(**kwargs):
        writes.append(kwargs)
        return AssertWritten(check_one_encounter_between_vessels_a_and_b, **kwargs)

    run(
        config(mock_bq_clients=True, unknown_parsed_args={"project": "test-project"}),
        read_from_bigquery_factory=read_factory,
        write_to_bigquery_factory=write_factory,
        runner=RUNNER,
    )

    assert len(reads) == 1
    assert "`project.dataset.raw_encounters`" in reads[0]["query"]
    assert reads[0]["bigquery_job_labels"] == {"environment": "test"}
    assert writes[0]["table"] == "project.dataset.encounters"
    assert writes[0]["write_disposition"] == "WRITE_TRUNCATE"
    assert writes[0]["create_disposition"] == "CREATE_NEVER"
