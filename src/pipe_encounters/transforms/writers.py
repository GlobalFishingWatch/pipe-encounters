import logging

from typing import Any

import apache_beam as beam

from apache_beam.io.gcp.internal.clients.bigquery import TableSchema
from google.cloud import bigquery


DELETE_QUERY = """
    DELETE FROM `{table}`
    WHERE DATE({partitioning_field})
    BETWEEN '{start_date}' AND '{end_date}'
"""

logger = logging.getLogger(__name__)


class WriteEncountersToBQ(beam.PTransform):
    def __init__(
        self,
        table_id: str,
        schema: TableSchema,
        project: str,
        labels: dict,
        description: str = None,
        **kwargs: Any,
    ):
        self.bqclient = bigquery.Client(project=project)
        self.table_id = table_id
        self.schema = schema
        self.labels = labels
        self.description = description
        self.kwargs = kwargs

    def delete_rows(self, start_date: str, end_date: str) -> None:
        logger.info(
            "Deleting records in {} from date range [{},{}] (inclusive)".format(
                self.table_id, start_date, end_date
            )
        )

        self.bqclient.query(
            DELETE_QUERY.format(
                table=self.table_id,
                partitioning_field="start_time",
                start_date=start_date,
                end_date=end_date,
            )
        )

    def update_table_metadata(self):
        table = self.bqclient.get_table(self.table_id)  # API request
        if self.description is not None:
            table.description = self.description

        table.labels = self.labels
        self.bqclient.update_table(table, ["description", "labels"])  # API request
        logger.info(f"Update table metadata to output table <{self.table_id}>")

    def expand(self, pcoll):
        return pcoll | beam.io.WriteToBigQuery(
            self.table_id,
            schema=self.schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            additional_bq_parameters={
                "timePartitioning": {
                    "type": "MONTH",
                    "field": "start_time",
                    "requirePartitionFilter": False,
                },
                "clustering": {"fields": ["start_time"]},
            },
            **self.kwargs,
        )
