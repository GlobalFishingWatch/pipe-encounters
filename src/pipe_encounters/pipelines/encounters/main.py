import logging

from datetime import timedelta
from typing import Any

import apache_beam as beam
import six

from apache_beam import Filter, FlatMap, Flatten, Map
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import PipelineState
from gfw.common.beam.pipeline.base import Pipeline

from pipe_encounters.core.encounter import Encounter, RawEncounter
from pipe_encounters.pipelines.encounters.config import EncountersConfig
from pipe_encounters.schemas.output import build as merge_schema
from pipe_encounters.transforms.add_id import AddEncounterId
from pipe_encounters.transforms.merge_encounters import MergeEncounters
from pipe_encounters.transforms.readers import ReadSources
from pipe_encounters.transforms.writers import WriteEncountersToBQ
from pipe_encounters.utils.ver import __version__


def get_description(config: EncountersConfig):
    return f"""\
Created by the encounters_pipeline: {__version__}
* Merges the encounters that are close in time into one long encounter.
* https://github.com/GlobalFishingWatch/encounters_pipeline
* Source partial encounters: {config.bq_in_partial_encounters}
* Source vessel id: {config.bq_in_segment_info}
* Source Spatial Measure: {config.bq_in_spatial_measures}
* Min hours before encounter: {config.min_hours_between_encounters}
* Skip bad segments table? {"Yes" if config.bad_segs else "No"}
* Date range: [{config.start_date}, {config.end_date})
"""


def combine_ids(obj):
    for v in [1, 2]:
        obj[f"vessel_{v}_seg_id"] = (
            six.ensure_binary(obj.pop(f"vessel_{v}_id")),
            six.ensure_binary(obj[f"vessel_{v}_seg_id"]),
        )
    return obj


def create_queries(args, start_date, end_date):
    """Yields the queries that read the partial encounters of [start_date, end_date], inclusive."""

    template = """
    WITH

    raw_encounters as (
        SELECT * except (start_time, end_time, encounter_id),
                CAST(UNIX_MICROS(start_time) AS FLOAT64) / 1000000 AS start_time,
                CAST(UNIX_MICROS(end_time) AS FLOAT64) / 1000000 AS end_time,
                format("lon:%+07.2f_lat:%+07.2f",
                  floor(cast(mean_longitude as numeric) / numeric '0.01') * numeric '0.01',
                  floor(cast(mean_latitude as numeric) / numeric '0.01') * numeric '0.01')
                as gridcode
        FROM `{raw_table}` events
        WHERE DATE(start_time) BETWEEN '{start}' AND '{end}'
          {condition}
    ),

    vessel_ids as (
        {vessel_id_query}
    )

    SELECT raw_encounters.* except(gridcode),
            vid1.vessel_id as vessel_1_id,
            vid2.vessel_id as vessel_2_id,
            distance_from_shore_m, distance_from_port_m
    FROM raw_encounters
    JOIN vessel_ids as vid1
    ON vessel_1_seg_id = vid1.seg_id
    JOIN vessel_ids as vid2
    ON vessel_2_seg_id = vid2.seg_id
    JOIN  `{spatial_measures_table}`
    USING (gridcode)
    """
    if args.bad_segs is None:
        condition = ""
    else:
        # TODO: need multiple bad segs tables to support multiple inputs.
        # Need optional 'prefix::' for them
        condition = f"""  AND vessel_1_seg_id NOT IN (SELECT seg_id FROM {args.bad_segs})
                          AND vessel_2_seg_id NOT IN (SELECT seg_id FROM {args.bad_segs})
        """

    subqueries = []
    for table in args.bq_in_segment_info:
        if "::" in table:
            id_prefix, table = table.split("::", 1)
            id_prefix += ":"
        else:
            id_prefix = ""
        subqueries.append(
            f'SELECT CONCAT("{id_prefix}", seg_id) AS seg_id, vessel_id FROM {table}'
        )
    vessel_id_query = "\nUNION ALL\n".join(subqueries)

    start_window = start_date
    shift = 1000
    while start_window <= end_date:
        end_window = min(start_window + timedelta(days=shift), end_date)
        query = template.format(
            raw_table=args.bq_in_partial_encounters,
            condition=condition,
            vessel_id_query=vessel_id_query,
            start=start_window.isoformat(),
            end=end_window.isoformat(),
            spatial_measures_table=args.bq_in_spatial_measures,
        )

        yield query
        start_window = end_window + timedelta(days=1)


def filter_by_distance(obj, min_distance_from_port_km):
    distance_from_shore_m = obj.pop("distance_from_shore_m")
    distance_from_port_m = obj.pop("distance_from_port_m")
    if distance_from_port_m < min_distance_from_port_km * 1000:
        return []
    elif distance_from_shore_m is None or distance_from_shore_m <= 0:
        return []
    return [obj]


def run(config: EncountersConfig, **kwargs: Any) -> int:
    pipeline = Pipeline(
        unparsed_args=config.unknown_unparsed_args,
        labels=config.labels,
        **config.unknown_parsed_args,
        **kwargs,
    )
    project = pipeline.cloud_options.project
    p = pipeline.pipeline

    # The end date is exclusive; the pipeline works on the last day it includes.
    last_date = config.end_date - timedelta(days=1)

    queries = create_queries(config, config.start_date, last_date)

    sources = [
        (p | f"Read_{i}" >> ReadSources(query=query, project=project, labels=config.labels))
        for (i, query) in enumerate(queries)
    ]

    merged = (
        sources
        | Flatten()
        | "CombineSegVesselIds" >> Map(combine_ids)
        | FlatMap(filter_by_distance, min_distance_from_port_km=10)
        | RawEncounter.FromDict()
        | MergeEncounters(min_hours_between_encounters=config.min_hours_between_encounters)
    )

    if config.min_encounter_time_minutes is not None:
        merged = merged | Filter(
            lambda x: (x.end_time - x.start_time).total_seconds() / 60.0
            >= config.min_encounter_time_minutes
        )

    writer = WriteEncountersToBQ(
        table_id=config.bq_out_encounters,
        schema=merge_schema(),
        project=project,
        labels=config.labels,
        description=get_description(config),
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    )

    merged = merged | "FilteredToDicts" >> Encounter.ToDict() | AddEncounterId() | writer

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
