"""Command-line interface of pipe-encounters, built on gfw-common's CLI."""

import sys

from typing import Any

from gfw.common.cli import CLI, labels_option
from gfw.common.cli.formatting import default_formatter
from gfw.common.logging import LoggerConfig

from pipe_encounters.cli.commands import Encounters, PartialEncounters
from pipe_encounters.utils.ver import __version__


NAME = "pipe-encounters"
DESCRIPTION = "Tools for detecting vessel encounters."
EXAMPLES = (
    "pipe-encounters partial-encounters "
    "--bq-in-positions project.dataset.messages "
    "--bq-out-partial-encounters project.dataset.raw_encounters "
    "--start-date 2024-01-01 --end-date 2024-01-02 "
    "--max-encounter-dist-km 0.5 --min-encounter-time-minutes 120 "
    "--labels environment=development resource_creator=tomas-link",
    "pipe-encounters partial-encounters -c config/partial-encounters/bq-1-day.yaml",
    "pipe-encounters encounters "
    "--bq-in-partial-encounters project.dataset.raw_encounters "
    "--bq-in-segment-info project.dataset.segment_info "
    "--bq-in-spatial-measures project.dataset.spatial_measures "
    "--bq-out-encounters project.dataset.encounters "
    "--start-date 2017-01-01 --end-date 2024-01-02 "
    "--labels environment=development resource_creator=tomas-link",
    "pipe-encounters encounters -c config/encounters/bq-full-history.yaml",
)


def run(args: list[str]) -> Any:
    """Builds the CLI and executes the command given in ``args``."""
    cli = CLI(
        name=NAME,
        description=DESCRIPTION,
        formatter=default_formatter(max_pos=120),
        subcommands=[PartialEncounters, Encounters],
        options=[labels_option()],  # Common options for all subcommands.
        version=__version__,
        examples=EXAMPLES,
        logger_config=LoggerConfig(
            warning_level=[
                "apache_beam.runners.portability",
                "apache_beam.runners.worker",
                "apache_beam.transforms.core",
                "apache_beam.io.filesystem",
                "apache_beam.io.gcp.bigquery_tools",
                "urllib3",
            ]
        ),
        allow_unknown=True,
    )

    result, _ = cli.execute(args)
    return result


def main() -> None:
    """Entry point of the installed package."""
    run(sys.argv[1:])


if __name__ == "__main__":
    main()
