from datetime import date

from pipe_encounters.pipelines.encounters.config import EncountersConfig
from pipe_encounters.pipelines.encounters.main import partial_encounters_query


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
