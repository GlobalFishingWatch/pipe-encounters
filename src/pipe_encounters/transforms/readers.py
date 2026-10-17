import apache_beam as beam


class ReadSources(beam.PTransform):
    def __init__(self, query: str, project: str, labels: dict):
        self.project = project
        self.use_legacy_sql = True
        self.labels = labels
        self.query = query

    def expand(self, pcoll):
        return pcoll | beam.io.ReadFromBigQuery(
            query=self.query,
            project=self.project,
            use_standard_sql=self.use_legacy_sql,
            bigquery_job_labels=self.labels,
        )
