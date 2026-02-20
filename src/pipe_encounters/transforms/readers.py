import apache_beam as beam

list_to_dict = lambda labels: {x.split('=')[0]:x.split('=')[1] for x in labels}

class ReadSources(beam.PTransform):
    def __init__(self, query, options: dict):
        self.project = options.project
        self.use_legacy_sql = True
        self.labels = list_to_dict(options.labels)
        self.query = query

    def expand(self, pcoll):
        return pcoll | beam.io.ReadFromBigQuery(
            query = self.query,
            project = self.project,
            use_standard_sql = self.use_legacy_sql,
            bigquery_job_labels = self.labels
        )


