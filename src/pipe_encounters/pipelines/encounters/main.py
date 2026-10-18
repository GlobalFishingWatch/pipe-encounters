import logging

from typing import Any

import apache_beam as beam
import six

from apache_beam import Filter, FlatMap, Flatten, Map
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import PipelineState
from gfw.common.beam.pipeline.base import Pipeline

from pipe_encounters.core.encounter import Encounter, RawEncounter
from pipe_encounters.pipelines.encounters.config import EncountersConfig
from pipe_encounters.pipelines.encounters.queries import PartialEncountersQuery
from pipe_encounters.schemas.output import build as merge_schema
from pipe_encounters.transforms.add_id import AddEncounterId
from pipe_encounters.pipelines.encounters.transforms.merge_encounters import MergeEncounters
from pipe_encounters.transforms.readers import ReadSources
from pipe_encounters.transforms.writers import WriteEncountersToBQ
from pipe_encounters.sources import split_id_prefix
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


def partial_encounters_query(config: EncountersConfig) -> PartialEncountersQuery:
    """Returns the query that reads the partial encounters with their vessel ids and distances."""
    query = PartialEncountersQuery(
        source_partial_encounters=config.bq_in_partial_encounters,
        source_segment_info=[split_id_prefix(t) for t in config.bq_in_segment_info],
        source_spatial_measures=config.bq_in_spatial_measures,
        start_date=config.start_date,
        end_date=config.end_date,
        bad_segs=config.bad_segs,
    )
    return query.with_env(config.jinja_env)


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

    query = partial_encounters_query(config).render()
    sources = [p | "Read" >> ReadSources(query=query, project=project, labels=config.labels)]

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
