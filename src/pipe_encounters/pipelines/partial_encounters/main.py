"""Partial-encounters pipeline: detects the pieces of vessel encounters."""

from typing import Any, Callable

import apache_beam as beam

from gfw.common.beam.pipeline.base import Pipeline
from gfw.common.beam.pipeline.dag import LinearDag
from gfw.common.beam.pipeline.hooks import (
    create_table_hook,
    delete_events_hook,
    update_table_metadata_hook,
)
from gfw.common.beam.transforms import ReadFromBigQuery, WriteToBigQueryWrapper
from gfw.common.bigquery.helper import BigQueryHelper

from pipe_encounters.pipelines.partial_encounters.config import PartialEncountersConfig
from pipe_encounters.pipelines.partial_encounters.queries import PositionsQuery
from pipe_encounters.pipelines.partial_encounters.table_config import (
    PartialEncountersTableConfig,
    PartialEncountersTableDescription,
)
from pipe_encounters.pipelines.partial_encounters.transforms.core import DetectPartialEncounters
from pipe_encounters.utils.ver import __version__


def split_id_prefix(source: str) -> tuple[str, str]:
    """Splits a positions source, optionally "ID::table", into its seg_id prefix and table."""
    if "::" not in source:
        return "", source

    id_prefix, table = source.split("::", 1)
    return f"{id_prefix}:", table


def read_ssvid_filter(ssvid_filter: str) -> str:
    """Returns the ssvid filter, read from a file if it's a path prefixed by @."""
    if ssvid_filter is None or not ssvid_filter.startswith("@"):
        return ssvid_filter

    with open(ssvid_filter[1:]) as f:
        return f.read()


def positions_queries(config: PartialEncountersConfig) -> list[PositionsQuery]:
    """Returns the query that reads the positions of each source table."""
    ssvid_filter = read_ssvid_filter(config.ssvid_filter)

    queries = []
    for source in config.bq_in_positions:
        id_prefix, table = split_id_prefix(source)
        query = PositionsQuery(
            source_positions=table,
            start_date=config.start_date,
            end_date=config.end_date,
            id_prefix=id_prefix,
            ssvid_filter=ssvid_filter,
        )
        queries.append(query.with_env(config.jinja_env))

    return queries


def run(
    config: PartialEncountersConfig,
    read_from_bigquery_factory: Callable = None,
    write_to_bigquery_factory: Callable = None,
    bq_client_factory: Callable = None,
    **kwargs: Any,
) -> None:
    """Runs the partial-encounters pipeline.

    Args:
        config:
            The pipeline's configuration.

        read_from_bigquery_factory:
            Builds the transforms that read from BigQuery. Defaults to gfw-common's, mocked if
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

    table_config = PartialEncountersTableConfig(
        table_id=config.bq_out_partial_encounters,
        description=PartialEncountersTableDescription(
            version=__version__,
            relevant_params={
                "bq_in_positions": config.bq_in_positions,
                "start_date": config.start_date,
                "end_date": config.end_date,
                "max_encounter_dist_km": config.max_encounter_dist_km,
                "min_encounter_time_minutes": config.min_encounter_time_minutes,
                "max_gap_seconds": config.max_gap_seconds,
                "max_gap_seconds_inclusive": config.max_gap_seconds_inclusive,
                "ssvid_filter": config.ssvid_filter,
            },
        ),
    )

    dag = LinearDag(
        sources=[
            ReadFromBigQuery.from_query(
                query,
                label=f"ReadPositions_{i}",
                read_from_bigquery_factory=read_from_bigquery_factory,
                read_from_bigquery_kwargs={"bigquery_job_labels": config.labels},
            )
            for i, query in enumerate(positions_queries(config))
        ],
        core=DetectPartialEncounters(
            start_date=config.start_date,
            end_date=config.end_date,
            max_encounter_dist_km=config.max_encounter_dist_km,
            min_encounter_time_minutes=config.min_encounter_time_minutes,
            max_gap_seconds=config.max_gap_seconds,
            max_gap_seconds_inclusive=config.max_gap_seconds_inclusive,
            schema=table_config.schema,
        ),
        sinks=(
            WriteToBigQueryWrapper(
                table=table_config.table_id,
                schema=table_config.schema,
                write_to_bigquery_factory=write_to_bigquery_factory,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            ),
        ),
    )

    pipeline = Pipeline(
        dag=dag,
        pre_hooks=[
            create_table_hook(
                table_config, labels=config.labels, bq_client_factory=bq_client_factory
            ),
            delete_events_hook(
                table_config,
                start_date=config.start_date,
                end_date=config.end_date,
                labels=config.labels,
                bq_client_factory=bq_client_factory,
            ),
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
