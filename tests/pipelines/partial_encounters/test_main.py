from datetime import date

from pipe_encounters.pipelines.partial_encounters.config import PartialEncountersConfig
from pipe_encounters.pipelines.partial_encounters.main import (
    positions_queries,
    read_ssvid_filter,
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
