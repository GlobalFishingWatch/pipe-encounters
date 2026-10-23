"""Encounters pipeline: joins the partial encounters into encounters between two vessels."""

from typing import Any, Callable

import apache_beam as beam

from gfw.common.beam.pipeline.base import Pipeline
from gfw.common.beam.pipeline.dag import LinearDag
from gfw.common.beam.pipeline.hooks import create_table_hook, update_table_metadata_hook
from gfw.common.beam.transforms import ReadFromBigQuery, WriteToBigQueryWrapper
from gfw.common.bigquery.helper import BigQueryHelper

from pipe_encounters.pipelines.encounters.config import EncountersConfig
from pipe_encounters.pipelines.encounters.queries import PartialEncountersQuery
from pipe_encounters.pipelines.encounters.table_config import (
    EncountersTableConfig,
    EncountersTableDescription,
)
from pipe_encounters.pipelines.encounters.transforms.core import JoinPartialEncounters
from pipe_encounters.utils.ver import __version__


def partial_encounters_query(config: EncountersConfig) -> PartialEncountersQuery:
    """Returns the query that reads the partial encounters away from port, with their vessel ids."""
    query = PartialEncountersQuery(
        source_partial_encounters=config.bq_in_partial_encounters,
        source_segment_info=config.bq_in_segment_info,
        source_spatial_measures=config.bq_in_spatial_measures,
        start_date=config.start_date,
        end_date=config.end_date,
        bad_segs=config.bad_segs,
    )
    return query.with_env(config.jinja_env)


def run(
    config: EncountersConfig,
    read_from_bigquery_factory: Callable = None,
    write_to_bigquery_factory: Callable = None,
    bq_client_factory: Callable = None,
    **kwargs: Any,
) -> None:
    """Runs the encounters pipeline.

    Args:
        config:
            The pipeline's configuration.

        read_from_bigquery_factory:
            Builds the transform that reads from BigQuery. Defaults to gfw-common's, mocked if
            config.mock_bq_clients is set.

        write_to_bigquery_factory:
            Builds the transform that writes to BigQuery. Defaults to gfw-common's, mocked if
            config.mock_bq_clients is set.

        bq_client_factory:
            Builds the BigQuery client of the table-metadata hook. Defaults to gfw-common's,
            mocked if config.mock_bq_clients is set.

        **kwargs:
            Apache Beam pipeline options.
    """
    if read_from_bigquery_factory is None:
        read_from_bigquery_factory = ReadFromBigQuery.get_client_factory(
            mocked=config.mock_bq_clients
        )
    if write_to_bigquery_factory is None:
        write_to_bigquery_factory = WriteToBigQueryWrapper.get_client_factory(
            mocked=config.mock_bq_clients
        )
    if bq_client_factory is None:
        bq_client_factory = BigQueryHelper.get_client_factory(mocked=config.mock_bq_clients)

    table_config = EncountersTableConfig(
        table_id=config.bq_out_encounters,
        description=EncountersTableDescription(
            version=__version__,
            relevant_params={
                "bq_in_partial_encounters": config.bq_in_partial_encounters,
                "bq_in_segment_info": config.bq_in_segment_info,
                "bq_in_spatial_measures": config.bq_in_spatial_measures,
                "start_date": config.start_date,
                "end_date": config.end_date,
                "bad_segs": config.bad_segs,
                "min_hours_between_encounters": config.min_hours_between_encounters,
                "min_encounter_time_minutes": config.min_encounter_time_minutes,
            },
        ),
    )

    dag = LinearDag(
        sources=[
            ReadFromBigQuery.from_query(
                partial_encounters_query(config),
                label="ReadPartialEncounters",
                read_from_bigquery_factory=read_from_bigquery_factory,
                read_from_bigquery_kwargs={"bigquery_job_labels": config.labels},
            ),
        ],
        core=JoinPartialEncounters(
            min_hours_between_encounters=config.min_hours_between_encounters,
            min_encounter_time_minutes=config.min_encounter_time_minutes,
        ),
        sinks=(
            WriteToBigQueryWrapper(
                table=table_config.table_id,
                schema=table_config.schema,
                write_to_bigquery_factory=write_to_bigquery_factory,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            ),
        ),
    )

    pipeline = Pipeline(
        dag=dag,
        pre_hooks=[
            create_table_hook(
                table_config, labels=config.labels, bq_client_factory=bq_client_factory
            )
        ],
        post_hooks=[
            update_table_metadata_hook(
                table_config, labels=config.labels, bq_client_factory=bq_client_factory
            )
        ],
        unparsed_args=config.unknown_unparsed_args,
        labels=config.labels,
        **config.unknown_parsed_args,
        **kwargs,
    )

    pipeline.run()
