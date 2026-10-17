"""CLI command that detects the pieces of encounters, per day and pair of segments."""

from types import SimpleNamespace
from typing import Any

from gfw.common.cli import Command, Option, date_range_options

from pipe_encounters.pipelines.partial_encounters import main
from pipe_encounters.pipelines.partial_encounters.config import PartialEncountersConfig


DESCRIPTION = """\
Detects the pieces of vessel encounters: pairs of track segments that stay within
max-encounter-dist-km of each other for at least min-encounter-time-minutes.

Each output row is a piece of an encounter, not a whole encounter:
  * positions are processed one UTC day at a time, so an encounter that crosses
    midnight is split into one piece per day;
  * each piece appears twice, once from each segment's side;
  * pieces are identified by segment, not by vessel.
The encounters command joins the pieces into one row per encounter between two vessels.

Positions are resampled to a 10-minute grid, interpolating across gaps shorter than
max-gap-seconds and extrapolating up to max-gap-seconds before the first and after the
last position of each segment-day. Encounters whose end date falls in the processed range
are appended to the output table, after deleting the range's existing rows.
The range starts at start-date and ends before end-date.

Besides the arguments defined here, you can also pass any pipeline option
defined for Apache Beam PipelineOptions class. For more information, see
    https://cloud.google.com/dataflow/docs/reference/pipeline-options#python.\n
"""

HELP_POSITIONS = (
    "BigQuery tables to pull position messages from. Each may be prefixed by 'ID::' to prefix "
    "its segment ids with 'ID:', which keeps the ids of different sources (e.g. AIS and VMS) "
    "distinct."
)
HELP_OUTPUT = "BigQuery table in which to append the partial encounters."
HELP_MAX_DIST = "Maximum distance (km) between two vessels to be in an encounter."
HELP_MIN_TIME = "Minimum duration (minutes) of a piece of an encounter."
HELP_MAX_GAP = (
    "Gaps between positions longer than this (seconds) are not interpolated across, "
    "so no encounter can be detected there."
)
HELP_INCLUSIVE = (
    "Also interpolate across gaps of exactly max-gap-seconds. "
    "Off by default, which matches the legacy behavior."
)
HELP_SSVID_FILTER = (
    "Subquery or list of ssvid to limit processing to. If prefixed by @, load from given path."
)
HELP_WAIT_FOR_JOB = "Wait until the job finishes before returning."


class PartialEncounters(Command):
    """Detects the pieces of vessel encounters, per day and pair of segments."""

    @property
    def name(self) -> str:
        """The command's name."""
        return "partial-encounters"

    @property
    def description(self) -> str:
        """The command's description."""
        return DESCRIPTION

    @property
    def options(self) -> list[Option]:
        """The command's options."""
        return [
            Option("--bq-in-positions", type=str, nargs="+", required=True, help=HELP_POSITIONS),
            Option("--bq-out-partial-encounters", type=str, required=True, help=HELP_OUTPUT),
            *date_range_options(),
            Option("--max-encounter-dist-km", type=float, required=True, help=HELP_MAX_DIST),
            Option("--min-encounter-time-minutes", type=float, required=True, help=HELP_MIN_TIME),
            Option("--max-gap-seconds", type=int, default=3600, help=HELP_MAX_GAP),
            Option("--max-gap-seconds-inclusive", type=bool, default=False, help=HELP_INCLUSIVE),
            Option("--ssvid-filter", type=str, help=HELP_SSVID_FILTER),
            Option("--wait-for-job", type=bool, default=False, help=HELP_WAIT_FOR_JOB),
        ]

    def run(self, config: SimpleNamespace, **kwargs: Any) -> Any:
        """Runs the pipeline with the resolved configuration."""
        return main.run(PartialEncountersConfig.from_namespace(config), **kwargs)
