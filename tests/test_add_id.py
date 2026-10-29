"""Tests for the encounter id transforms."""

import datetime as dt

from typing import Any

import pytest

from pipe_encounters.transforms import add_id


epoch = dt.datetime(1970, 1, 1)


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (
            {
                "vessel_1_id": "3f2b89cd6-6b36-e89c-dfaa-5b9e32703c63",
                "vessel_2_id": "e5e1847ca-a61f-fbf7-1f79-7c7ed7912b5c",
                "vessel_1_seg_ids": ["a", "b", "c"],
                "vessel_2_seg_ids": ["d", "e", "f"],
                "start_time": (dt.datetime(2020, 3, 24, 5, 0, 0) - epoch).total_seconds(),
                "end_time": (dt.datetime(2020, 3, 24, 8, 30, 0) - epoch).total_seconds(),
            },
            "93b461788df324d3d63f81a2baeb15a8",
        ),
    ],
)
def test_add_id(test_input: dict[str, Any], expected: str) -> None:
    """Checks that AddEncounterId sets the expected encounter_id."""
    adder = add_id.AddEncounterId()
    encounter_id = adder.add_id(test_input)["encounter_id"]
    assert encounter_id == expected
