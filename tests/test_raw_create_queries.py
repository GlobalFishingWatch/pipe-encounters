
from pipe_encounters.create_raw_pipeline import create_queries

class DummyOptions(object):
    def __init__(self, start_date, end_date, source_dataset="SOURCE_DATASET", position_messages_table="position_messages_", segments_table="segments_"):
        self.start_date = start_date
        self.end_date = end_date
        self.source_tables = [source_dataset]
        self.position_messages_table=position_messages_table
        self.segments_table=segments_table
        self.fast_test = False
        self.id_column = None
        self.ssvid_filter = None
    def view_as(self, x):
      return self


def test_create_queries_1():
    options=DummyOptions("2016-01-01", "2016-01-01")
    assert [x.strip() for x in create_queries(options)] == [x.strip() for x in ["""
    SELECT
      lat        AS lat,
      lon        AS lon,
      speed      AS speed,
      course     AS course,
      UNIX_MILLIS(timestamp) / 1000.0  AS timestamp,
      CONCAT("", seg_id) AS id
    FROM
        `SOURCE_DATASET`
    WHERE
        date(timestamp) BETWEEN '2016-01-01' AND '2016-01-01'
    """]]

def test_create_queries_2():
    options=DummyOptions("2012-5-01", "2017-05-15")
    assert [x.strip() for x in create_queries(options)] == [x.strip() for x in ["""
    SELECT
      lat        AS lat,
      lon        AS lon,
      speed      AS speed,
      course     AS course,
      UNIX_MILLIS(timestamp) / 1000.0  AS timestamp,
      CONCAT("", seg_id) AS id
    FROM
        `SOURCE_DATASET`
    WHERE
        date(timestamp) BETWEEN '2012-05-01' AND '2015-01-25'
    """,
    """
    SELECT
      lat        AS lat,
      lon        AS lon,
      speed      AS speed,
      course     AS course,
      UNIX_MILLIS(timestamp) / 1000.0  AS timestamp,
      CONCAT("", seg_id) AS id
    FROM
        `SOURCE_DATASET`
    WHERE
        date(timestamp) BETWEEN '2015-01-26' AND '2017-05-15'
    """]]
