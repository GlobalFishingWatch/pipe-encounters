"""Tests for the partial encounters CLI command."""

import argparse

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pytest_mock import MockerFixture

from gfw.common.config import PipelineConfigError
from pipe_encounters.cli import main
from pipe_encounters.pipelines.partial_encounters.config import PartialEncountersConfig


BASE_ARGS = [
    "partial-encounters",
    "--bq-in-positions",
    "project.dataset.messages",
    "--bq-out-partial-encounters",
    "project.dataset.raw_encounters",
    "--start-date",
    "2024-01-01",
    "--end-date",
    "2024-01-08",
    "--max-encounter-dist-km",
    "0.5",
    "--min-encounter-time-minutes",
    "120",
    "--labels",
    "environment=test",
]


@pytest.fixture
def run(mocker: MockerFixture) -> MagicMock:
    """Mocks the pipeline run that the CLI calls."""
    return mocker.patch("pipe_encounters.cli.commands.partial_encounters.main.run")


def test_cli_executes_run_with_the_config(run: MagicMock) -> None:
    """Checks that the CLI builds the config and passes it to run."""
    main.run(BASE_ARGS)

    config = run.call_args.args[0]
    assert isinstance(config, PartialEncountersConfig)
    assert config.bq_in_positions == "project.dataset.messages"
    assert config.bq_out_partial_encounters == "project.dataset.raw_encounters"
    assert config.start_date == date(2024, 1, 1)
    assert config.end_date == date(2024, 1, 8)
    assert config.max_encounter_dist_km == 0.5
    assert config.min_encounter_time_minutes == 120.0
    assert config.max_gap_seconds == 3600
    assert config.max_gap_seconds_inclusive is False
    assert config.ssvid_filter is None
    assert config.mock_bq_clients is False
    assert config.labels == {"environment": "test"}


def test_cli_requires_the_output_table(run: MagicMock) -> None:
    """Checks that the CLI rejects a missing output table."""
    output = ("--bq-out-partial-encounters", "project.dataset.raw_encounters")
    args = [a for a in BASE_ARGS if a not in output]

    with pytest.raises(argparse.ArgumentTypeError, match="bq_out_partial_encounters"):
        main.run(args)


class StopAfterPipeline(Exception):
    """Stops the run once the Beam pipeline is built, before reading or writing anything."""


def test_config_file_beam_options_reach_the_pipeline(
    mocker: MockerFixture, tmp_path: Path
) -> None:
    """Checks that Beam options from the config file reach the pipeline."""
    pipeline = mocker.patch(
        "pipe_encounters.pipelines.partial_encounters.main.Pipeline", side_effect=StopAfterPipeline
    )
    config_file = tmp_path / "config.yaml"
    config_file.write_text("project: test-project\nmax_num_workers: 50\n")

    with pytest.raises(StopAfterPipeline):
        main.run([*BASE_ARGS, "-c", str(config_file)])

    kwargs = pipeline.call_args.kwargs
    assert kwargs["unparsed_args"] == []
    assert kwargs["labels"] == {"environment": "test"}
    assert kwargs["project"] == "test-project"
    assert kwargs["max_num_workers"] == 50


def test_cli_rejects_an_empty_date_range(run: MagicMock) -> None:
    """Checks that the CLI rejects an empty date range."""
    args = [a.replace("2024-01-08", "2024-01-01") for a in BASE_ARGS]

    with pytest.raises(PipelineConfigError, match="end_date"):
        main.run(args)
