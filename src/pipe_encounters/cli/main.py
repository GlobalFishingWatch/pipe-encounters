"""Command-line interface of pipe-encounters, built on gfw-common's CLI."""

import sys

from typing import Any

from gfw.common.cli import CLI, labels_option
from gfw.common.cli.formatting import default_formatter
from gfw.common.logging import LoggerConfig

from pipe_encounters.cli.commands import PartialEncounters
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
)


def run(args: list[str]) -> Any:
    """Builds the CLI and executes the command given in ``args``."""
    cli = CLI(
        name=NAME,
        description=DESCRIPTION,
        formatter=default_formatter(max_pos=120),
        subcommands=[PartialEncounters],
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


# FROM NOW ON: LEGACY ENTRY POINT.
# TODO: REMOVE AFTER MIGRATING merge_encounters.


def run_merge_encounters(args: list[str]) -> None:
    """Runs the legacy merge_encounters command, not migrated to the CLI yet."""
    # Imported here so its PipelineOptions don't collide with other commands' arguments.
    from pipe_encounters.merge_encounters import main as run_merge

    run_merge(args)


SUBCOMMANDS = {
    "merge_encounters": run_merge_encounters,
}


def main() -> None:
    """Entry point of the installed package."""
    args = sys.argv[1:]

    if args and args[0] in SUBCOMMANDS:
        SUBCOMMANDS[args[0]](args[1:])
        return

    run(args)


if __name__ == "__main__":
    main()
