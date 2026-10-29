"""CLI command that joins the partial encounters into vessel encounters."""

from types import SimpleNamespace
from typing import Any

from gfw.common.cli import Command, Option, date_range_options
from pipe_encounters.pipelines.encounters import main
from pipe_encounters.pipelines.encounters.config import EncountersConfig


DESCRIPTION = """\
Joins the partial encounters into vessel encounters: one row per encounter between
two vessels, as published later by the events pipeline.

The partial encounters' segments are mapped to vessel ids, and the pieces of the
same pair of vessels are chained into one encounter while they are less than
min-hours-between-encounters apart: across days, segments and both vessels' sides.
Encounters within 10 km of a port, or without a positive distance from shore, are
dropped.

The output table is replaced with the encounters of the processed range, so it is
usually run over the whole history. The range starts at start-date and ends before
end-date.

Besides the arguments defined here, you can also pass any pipeline option
defined for Apache Beam PipelineOptions class. For more information, see
    https://cloud.google.com/dataflow/docs/reference/pipeline-options#python.\n
"""

HELP_PARTIAL = "BigQuery table to pull the partial encounters from."
HELP_SEGMENTS = "BigQuery table mapping each seg_id to its vessel_id, e.g. segment_info."
HELP_SPATIAL = "BigQuery table to pull the distance from shore and from port from."
HELP_OUTPUT = "BigQuery table in which to write the encounters. Its content is replaced."
HELP_BAD_SEGS = (
    "Table or subquery with the seg_id of bad segments, whose partial encounters are skipped."
)
HELP_MIN_HOURS = "Partial encounters less than these hours apart are joined into one."
HELP_MIN_TIME = "Minimum duration (minutes) of an encounter. Not applied if not set."
HELP_MOCK_BQ = "If passed, mocks the BigQuery clients [Useful for development]."


class Encounters(Command):
    """Joins the partial encounters into vessel encounters."""

    @property
    def name(self) -> str:
        """The command's name."""
        return "encounters"

    @property
    def description(self) -> str:
        """The command's description."""
        return DESCRIPTION

    @property
    def options(self) -> list[Option]:
        """The command's options."""
        return [
            Option("--bq-in-partial-encounters", type=str, required=True, help=HELP_PARTIAL),
            Option("--bq-in-segment-info", type=str, required=True, help=HELP_SEGMENTS),
            Option("--bq-in-spatial-measures", type=str, required=True, help=HELP_SPATIAL),
            Option("--bq-out-encounters", type=str, required=True, help=HELP_OUTPUT),
            *date_range_options(),
            Option("--bad-segs", type=str, help=HELP_BAD_SEGS),
            Option("--min-hours-between-encounters", type=float, default=4, help=HELP_MIN_HOURS),
            Option("--min-encounter-time-minutes", type=float, help=HELP_MIN_TIME),
            Option("--mock-bq-clients", type=bool, default=False, help=HELP_MOCK_BQ),
        ]

    def run(self, config: SimpleNamespace, **kwargs: Any) -> Any:
        """Runs the pipeline with the resolved configuration."""
        return main.run(EncountersConfig.from_namespace(config), **kwargs)
