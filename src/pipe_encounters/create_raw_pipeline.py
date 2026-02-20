import apache_beam as beam
from apache_beam import Filter
from apache_beam import Flatten
from apache_beam import Map
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import PipelineState
from apache_beam.transforms.window import TimestampedValue

from pipe_encounters.objects.encounter import RawEncounter
from pipe_encounters.objects.record import Record
from pipe_encounters.options.create_options import CreateOptions
from pipe_encounters.schemas.output import build_raw_encounter
from pipe_encounters.schemas.utils import schema_to_obj
from pipe_encounters.transforms.add_id import AddRawEncounterId
from pipe_encounters.transforms.compute_adjacency import ComputeAdjacency
from pipe_encounters.transforms.compute_encounters import ComputeEncounters
from pipe_encounters.transforms.resample import Resample
from pipe_encounters.transforms.writers import WriteEncountersToBQ
from pipe_encounters.transforms.readers import ReadSources
from pipe_encounters.utils.ver import __version__

import datetime
import logging
import pytz


RESAMPLE_INCREMENT_MINUTES = 10.0
MAX_GAP_HOURS = 1.0
PRECURSOR_DAYS = 1


def get_description(options: CreateOptions):
    return f"""\
Created by the encounters_pipeline: {__version__}.
* Creates raw encounters, reads the data from source and computes encounters over windows between start_date and end_date.
* https://github.com/GlobalFishingWatch/encounters_pipeline
* Sources: {options.source_tables}
* Maximum distance for vessels to be elegible (km): {options.max_encounter_dist_km}
* Minimum minutes of vessel adjacency before we have an encounter: {options.min_encounter_time_minutes}
* Last processing date range: [{options.start_date}, {options.end_date}].
"""


def create_queries(args):
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
        condition = ''
    else:
        filter_core = args.ssvid_filter
        if filter_core.startswith('@'):
            with open(args.ssvid_filter[1:]) as f:
                filter_core = f.read()
        condition = f'AND ssvid in ({filter_core})'

    start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date= datetime.datetime.strptime(args.end_date, '%Y-%m-%d')

    for table in args.source_tables:
        if '::' in table:
            id_prefix, table = table.split('::', 1)
            id_prefix += ':'
        else:
            id_prefix = ''
        start_window = start_date
        while start_window <= end_date:
            end_window = min(start_window + datetime.timedelta(days=999), end_date)
            query = template.format(id_prefix=id_prefix, position_table=table,
                                    start=start_window, end=end_window,
                                    condition=condition
                                    )
            yield query
            start_window = end_window + datetime.timedelta(days=1)


def check_schema(x, schema):
    assert set(x.keys()) == set([x['name'] for x in schema]), x.keys()
    for field in schema:
        assert field['mode'] == 'REQUIRED'
        # TODO: support 'NULLABLE' and 'REPEATED'
        ftype = field['type']
        val = x[field['name']]
        allowed_types_map = {
            'STRING' : (str,),
            'INTEGER' : (int,),
            'FLOAT' : (int, float),
            'TIMESTAMP' : (int, float)
        }
        if ftype not in allowed_types_map:
            raise ValueError(f'unknown schema type {field}')
        allowed_types = allowed_types_map[ftype]
        assert isinstance(val, allowed_types), (field, val)
    return x


def run(options):

    p = Pipeline(options=options)

    create_options = options.view_as(CreateOptions)
    cloud_options = options.view_as(GoogleCloudOptions)

    start_date = datetime.datetime.strptime(create_options.start_date, '%Y-%m-%d').replace(tzinfo=pytz.utc)
    end_date= datetime.datetime.strptime(create_options.end_date, '%Y-%m-%d').replace(tzinfo=pytz.utc)

    schema = build_raw_encounter()

    sources = [
        (p | f"Read_{i}" >> ReadSources(
            query=query,
            options=cloud_options
        )) for (i, query) in enumerate(create_queries(create_options))
    ]

    writer = WriteEncountersToBQ(
        table_id=create_options.raw_table,
        schema=schema,
        cloud_opts=cloud_options,
        description=get_description(create_options),
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    )

    adjacencies = (sources
        | Flatten()
        | Record.FromDict()
        | Resample(increment_s = 60 * RESAMPLE_INCREMENT_MINUTES,
                   max_gap_s = 60 * 60 * MAX_GAP_HOURS)
        | ComputeAdjacency(max_adjacency_distance_km=create_options.max_encounter_dist_km)
        | ComputeEncounters(max_km_for_encounter=create_options.max_encounter_dist_km,
                            min_minutes_for_encounter=create_options.min_encounter_time_minutes)
        | Filter(lambda x: start_date.date() <= x.end_time.date() <= end_date.date())
        | RawEncounter.ToDict()
        | AddRawEncounterId()
        | Map(lambda x: TimestampedValue(x, x['end_time'])) 
        | Map(check_schema, schema=schema_to_obj(schema))
        | writer
    )

    # Ensure we delete any existing rows from the date to be processed.
    # Needed to maintain consistency we re-processing dates.
    writer.delete_rows(start_date=create_options.start_date, end_date=create_options.end_date)

    result = p.run()

    success_states = set([PipelineState.DONE])

    if create_options.wait_for_job or options.view_as(StandardOptions).runner == 'DirectRunner':
        result.wait_until_finish()
        if result.state in success_states:
            writer.update_table_metadata()
    else:
        success_states.add(PipelineState.RUNNING)
        success_states.add(PipelineState.UNKNOWN)
        success_states.add(PipelineState.PENDING)

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1
