import datetime
import logging

import apache_beam as beam
import pytz

from typing import Any

from apache_beam import Filter, Flatten, Map
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import PipelineState
from apache_beam.transforms.window import TimestampedValue
from gfw.common.beam.pipeline.base import Pipeline

from pipe_encounters.objects.encounter import RawEncounter
from pipe_encounters.objects.record import Record
from pipe_encounters.pipelines.partial_encounters.config import PartialEncountersConfig
from pipe_encounters.schemas.output import build_raw_encounter
from pipe_encounters.schemas.utils import schema_to_obj
from pipe_encounters.transforms.add_id import AddRawEncounterId
from pipe_encounters.transforms.compute_adjacency import ComputeAdjacency
from pipe_encounters.transforms.compute_encounters import ComputeEncounters
from pipe_encounters.transforms.readers import ReadSources
from pipe_encounters.transforms.resample import Resample
from pipe_encounters.transforms.writers import WriteEncountersToBQ
from pipe_encounters.utils.ver import __version__


RESAMPLE_INCREMENT_MINUTES = 10.0
PRECURSOR_DAYS = 1


def get_description(config: PartialEncountersConfig):
    return f"""\
Created by the encounters_pipeline: {__version__}.
* Creates raw encounters, reads the data from source and computes encounters over windows between start_date and end_date.  # noqa
* https://github.com/GlobalFishingWatch/encounters_pipeline
* Sources: {config.bq_in_positions}
* Maximum distance for vessels to be elegible (km): {config.max_encounter_dist_km}
* Minimum minutes of vessel adjacency before we have an encounter: {config.min_encounter_time_minutes}
* Maximum gap between positions to interpolate across (seconds): {config.max_gap_seconds}
* Gaps of exactly max_gap_seconds are treated as interpolatable: {config.max_gap_seconds_inclusive}
* Last processing date range: [{config.start_date}, {config.end_date}).
"""


def create_queries(args, start_date, end_date):
    """Yields the queries that read the positions of [start_date, end_date], both inclusive."""
    template = """
    SELECT
      lat        AS lat,
      lon        AS lon,
      speed      AS speed,
      course     AS course,
      UNIX_MILLIS(timestamp) / 1000.0  AS timestamp,
      CONCAT("{id_prefix}", seg_id) AS id
    FROM
        `{position_table}`
    WHERE
        date(timestamp) BETWEEN '{start:%Y-%m-%d}' AND '{end:%Y-%m-%d}'
        {condition}
    """
    if args.ssvid_filter is None:
        condition = ""
    else:
        filter_core = args.ssvid_filter
        if filter_core.startswith("@"):
            with open(args.ssvid_filter[1:]) as f:
                filter_core = f.read()
        condition = f"AND ssvid in ({filter_core})"

    for table in args.bq_in_positions:
        if "::" in table:
            id_prefix, table = table.split("::", 1)
            id_prefix += ":"
        else:
            id_prefix = ""
        start_window = start_date
        while start_window <= end_date:
            end_window = min(start_window + datetime.timedelta(days=999), end_date)
            query = template.format(
                id_prefix=id_prefix,
                position_table=table,
                start=start_window,
                end=end_window,
                condition=condition,
            )
            yield query
            start_window = end_window + datetime.timedelta(days=1)


def check_schema(x, schema):
    assert set(x.keys()) == set([x["name"] for x in schema]), x.keys()
    for field in schema:
        assert field["mode"] == "REQUIRED"
        # TODO: support 'NULLABLE' and 'REPEATED'
        ftype = field["type"]
        val = x[field["name"]]
        allowed_types_map = {
            "STRING": (str,),
            "INTEGER": (int,),
            "FLOAT": (int, float),
            "TIMESTAMP": (int, float),
        }
        if ftype not in allowed_types_map:
            raise ValueError(f"unknown schema type {field}")
        allowed_types = allowed_types_map[ftype]
        assert isinstance(val, allowed_types), (field, val)
    return x


def run(config: PartialEncountersConfig, **kwargs: Any) -> int:
    pipeline = Pipeline(
        unparsed_args=config.unknown_unparsed_args,
        labels=config.labels,
        **config.unknown_parsed_args,
        **kwargs,
    )
    project = pipeline.cloud_options.project
    p = pipeline.pipeline

    # The end date is exclusive; the pipeline works on the last day it includes.
    last_date = config.end_date - datetime.timedelta(days=1)

    start_date = datetime.datetime.combine(config.start_date, datetime.time.min, tzinfo=pytz.utc)
    end_date = datetime.datetime.combine(last_date, datetime.time.min, tzinfo=pytz.utc)

    schema = build_raw_encounter()

    sources = [
        (p | f"Read_{i}" >> ReadSources(query=query, project=project, labels=config.labels))
        for (i, query) in enumerate(create_queries(config, config.start_date, last_date))
    ]

    writer = WriteEncountersToBQ(
        table_id=config.bq_out_partial_encounters,
        schema=schema,
        project=project,
        labels=config.labels,
        description=get_description(config),
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    )

    adjacencies = (  # noqa
        sources
        | Flatten()
        | Record.FromDict()
        | Resample(
            increment_s=60 * RESAMPLE_INCREMENT_MINUTES,
            max_gap_s=config.max_gap_seconds,
            inclusive=config.max_gap_seconds_inclusive,
        )
        | ComputeAdjacency(max_adjacency_distance_km=config.max_encounter_dist_km)
        | ComputeEncounters(
            max_km_for_encounter=config.max_encounter_dist_km,
            min_minutes_for_encounter=config.min_encounter_time_minutes,
        )
        | Filter(lambda x: start_date.date() <= x.end_time.date() <= end_date.date())
        | RawEncounter.ToDict()
        | AddRawEncounterId()
        | Map(lambda x: TimestampedValue(x, x["end_time"]))
        | Map(check_schema, schema=schema_to_obj(schema))
        | writer
    )

    # Ensure we delete any existing rows from the date to be processed.
    # Needed to maintain consistency we re-processing dates.
    writer.delete_rows(start_date=config.start_date, end_date=last_date)

    result = p.run()

    success_states = set([PipelineState.DONE])

    runner = pipeline.pipeline_options.view_as(StandardOptions).runner
    if config.wait_for_job or runner == "DirectRunner":
        result.wait_until_finish()
        if result.state in success_states:
            writer.update_table_metadata()
    else:
        success_states.add(PipelineState.RUNNING)
        success_states.add(PipelineState.UNKNOWN)
        success_states.add(PipelineState.PENDING)

    logging.info("returning with result.state=%s" % result.state)
    return 0 if result.state in success_states else 1
