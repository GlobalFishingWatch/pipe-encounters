"""Tests for merging the encounters of the encounters pipeline."""

from typing import Any, NamedTuple

from pipe_encounters.core import encounter
from pipe_encounters.encoding import to_bytes
from tests.pipelines.encounters.transforms.steps import merge
from tests.pipelines.partial_encounters.transforms.steps import detect, records
from tests.series_data import dateline_series_data, real_series_data
from tests.utils import approx_equal_to


def add_fake_vessel_id(obj: NamedTuple) -> encounter.RawEncounter:
    """Replaces each vessel segment id with a (vessel id, segment id) pair of bytes."""
    obj = obj._asdict()
    for v in [1, 2]:
        obj[f"vessel_{v}_seg_id"] = (
            to_bytes(obj[f"vessel_{v}_seg_id"]),
            to_bytes(obj[f"vessel_{v}_seg_id"]),
        )
    return encounter.RawEncounter(**obj)


def test_merge_messages() -> None:
    """Checks that the encounters between two vessels are merged as expected."""
    raw = [add_fake_vessel_id(r) for r in detect(records(real_series_data))]
    results = [encounter.Encounter.ToDict().to_dict(e) for e in merge(raw)]

    approx_equal_to(merged_expected())(results)


def test_merge_dateline() -> None:
    """Checks that encounters across the antimeridian are merged as expected."""
    raw = [add_fake_vessel_id(r) for r in detect(records(dateline_series_data))]
    results = [encounter.Encounter.ToDict().to_dict(e) for e in merge(raw)]

    approx_equal_to(merged_dateline_expected())(results)


def merged_expected() -> list[dict[str, Any]]:
    """Returns the expected merged encounters of the real series data."""
    return [
        {
            "median_speed_knots": 0.18809378172003316,
            "start_time": 1426750800.0,
            "mean_longitude": 38.53406239539688,
            "vessel_1_seg_ids": ["441910000"],
            "vessel_2_seg_ids": ["563418000"],
            "vessel_2_point_count": 10,
            "mean_latitude": -27.479382615650064,
            "end_time": 1426795800.0,
            "median_distance_km": 0.02959787505046703,
            "vessel_1_point_count": 12,
            "vessel_2_id": b"563418000",
            "vessel_1_id": b"441910000",
            "start_lat": -27.484518051171428,
            "start_lon": 38.53651973177143,
            "end_lat": -27.464933395399996,
            "end_lon": 38.52257468481818,
        }
    ]


def merged_dateline_expected() -> list[dict[str, Any]]:
    """Returns the expected merged encounters of the dateline series data."""
    return [
        {
            "vessel_1_id": b"1",
            "vessel_2_id": b"2",
            "start_time": 1293898200.0,
            "vessel_1_seg_ids": ["1"],
            "vessel_2_seg_ids": ["2"],
            "end_time": 1293901800.0,
            "mean_latitude": -1.4716407285714286,
            "mean_longitude": 179.99984999999998,
            "median_distance_km": 0.21170220169329662,
            "median_speed_knots": 0.18393498586650497,
            "vessel_1_point_count": 14,
            "vessel_2_point_count": 12,
            "start_lat": -1.4719963,
            "start_lon": 179.999,
            "end_lat": -1.4730263,
            "end_lon": 179.999,
        }
    ]
