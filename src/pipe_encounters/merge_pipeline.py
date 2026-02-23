import logging

from datetime import datetime, timedelta

import apache_beam as beam
import numpy as np
import pytz
import six

from apache_beam import Filter, FlatMap, Flatten, Map, Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions, StandardOptions
from apache_beam.runners import PipelineState

from pipe_encounters.objects.encounter import Encounter, RawEncounter
from pipe_encounters.options.merge_options import MergeOptions
from pipe_encounters.schemas.output import build as merge_schema
from pipe_encounters.transforms.add_id import AddEncounterId
from pipe_encounters.transforms.merge_encounters import MergeEncounters
from pipe_encounters.transforms.readers import ReadSources
from pipe_encounters.transforms.writers import WriteEncountersToBQ
from pipe_encounters.utils.ver import get_pipe_ver


def get_description(options: MergeOptions):
    return f"""\
Created by the encounters_pipeline: {get_pipe_ver()}
* Merges the encounters that are close in time into one long encounter.
* https://github.com/GlobalFishingWatch/encounters_pipeline
* Source raw encounters: {options.sink_table}
* Source vessel id: {options.vessel_id_tables}
* Source Spatial Measure: {options.spatial_measures_table}
* Min hours before encounter: {options.min_hours_between_encounters}
* Skip bad segments table? {"Yes" if options.bad_segs_table else "No"}
* Date range: {options.start_date}, {options.end_date}
"""


def combine_ids(obj):
    for v in [1, 2]:
        obj[f"vessel_{v}_seg_id"] = (
            six.ensure_binary(obj.pop(f"vessel_{v}_id")),
            six.ensure_binary(obj[f"vessel_{v}_seg_id"]),
        )
    return obj


def create_queries(args):
    start_time = datetime.fromisoformat(args.start_date).replace(tzinfo=pytz.utc)
    end_time = datetime.fromisoformat(args.end_date).replace(tzinfo=pytz.utc)

    start_date = start_time.date()
    end_date = end_time.date()

    template = """
    WITH

    raw_encounters as (
        SELECT * except (start_time, end_time, encounter_id),
                CAST(UNIX_MICROS(start_time) AS FLOAT64) / 1000000 AS start_time,
                CAST(UNIX_MICROS(end_time) AS FLOAT64) / 1000000 AS end_time,
                format("lon:%+07.2f_lat:%+07.2f", mean_longitude, mean_latitude) as gridcode,
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
    if args.bad_segs_table is None:
        condition = ""
    else:
        # TODO: need multiple bad segs tables to support multiple inputs.
        # Need optional 'prefix::' for them
        condition = f"""  AND vessel_1_seg_id NOT IN (SELECT seg_id FROM {args.bad_segs_table})
                          AND vessel_2_seg_id NOT IN (SELECT seg_id FROM {args.bad_segs_table})
        """

    subqueries = []
    for table in args.vessel_id_tables:
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
            raw_table=args.raw_table,
            condition=condition,
            vessel_id_query=vessel_id_query,
            start=start_window.isoformat(),
            end=end_window.isoformat(),
            spatial_measures_table=args.spatial_measures_table,
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


def tag_with_gridcode(x):
    lat = np.clip(x.mean_latitude, -89.99, 89.99)
    gc = f"lon:{round(x.mean_longitude, 2):+07.2f}_lat:{round(lat, 2):+07.2f}"
    gc = gc.replace("-000.00", "+000.00").replace("+180.00", "-180.00")
    return (gc, x)


def run(options):
    p = Pipeline(options=options)

    merge_options = options.view_as(MergeOptions)
    cloud_options = options.view_as(GoogleCloudOptions)

    queries = create_queries(merge_options)

    sources = [
        (p | f"Read_{i}" >> ReadSources(query=query, options=cloud_options))
        for (i, query) in enumerate(queries)
    ]

    merged = (
        sources
        | Flatten()
        | "CombineSegVesselIds" >> Map(combine_ids)
        | FlatMap(filter_by_distance, min_distance_from_port_km=10)
        | RawEncounter.FromDict()
        | MergeEncounters(min_hours_between_encounters=merge_options.min_hours_between_encounters)
    )

    if merge_options.min_encounter_time_minutes is not None:
        merged = merged | Filter(
            lambda x: (x.end_time - x.start_time).total_seconds() / 60.0
            >= merge_options.min_encounter_time_minutes
        )

    writer = WriteEncountersToBQ(
        table_id=merge_options.sink_table,
        schema=merge_schema(),
        cloud_opts=cloud_options,
        description=get_description(merge_options),
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    )

    merged = merged | "FilteredToDicts" >> Encounter.ToDict() | AddEncounterId() | writer

    result = p.run()

    success_states = set([PipelineState.DONE])

    if merge_options.wait_for_job or options.view_as(StandardOptions).runner == "DirectRunner":
        result.wait_until_finish()
        if result.state in success_states:
            writer.update_table_metadata()
    else:
        success_states.add(PipelineState.RUNNING)
        success_states.add(PipelineState.UNKNOWN)
        success_states.add(PipelineState.PENDING)

    logging.info("returning with result.state=%s" % result.state)
    return 0 if result.state in success_states else 1
