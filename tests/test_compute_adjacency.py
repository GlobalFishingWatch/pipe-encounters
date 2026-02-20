import pytest
import unittest
import logging
import numpy as np
from collections import namedtuple
import six

import apache_beam as beam
from apache_beam import Map
from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import assert_that

from .test_resample import Record
from .test_resample import ResampledRecord
from .series_data import simple_series_data
from pipe_encounters.options.create_options import CreateOptions
from pipe_encounters.transforms import compute_adjacency
from pipe_encounters.transforms import resample
from pipe_encounters.objects import record
from pipe_encounters.utils.test import approx_equal_to as equal_to


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def ensure_bytes_id(obj):
    return obj._replace(id=six.ensure_binary(obj.id))

inf = float('inf')

def TaggedAnnotatedRecord(vessel_id, record, neighbor_count, closest_neighbor):
    if closest_neighbor is None:
        nbr_dist = inf
    else:
        nbr_id, nbr_dist, nbr_args = closest_neighbor
        closest_neighbor = ResampledRecord(*nbr_args, id=nbr_id)
    record = record._replace(id=vessel_id)
    if np.isinf(nbr_dist):
        closest_neighbors = []
        closest_distances = []
    else:
        closest_neighbors = [closest_neighbor]
        closest_distances = [nbr_dist]
    return compute_adjacency.AnnotatedRecord(closest_distances=closest_distances,
            closest_neighbors=closest_neighbors, **record._asdict())


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestComputeAdjacency(unittest.TestCase):

    def test_with_resampling(self):

        tuple_data = [tuple(x) for x in simple_series_data]

        args = [
            f"--source_table={tuple_data}",
            "--raw_table=test",
            "--start_date=2011-01-01",
            "--end_date=2011-01-01",
            "--max_encounter_dist_km=0.5",
            "--min_encounter_time_minutes=120"
        ]
        opts = CreateOptions(args)
        with _TestPipeline(options=opts) as p:
            results = (
                p
                | beam.Create(tuple_data)
                | beam.Map(lambda x : record.Record(*x))
                | 'Ensure ID is bytes' >> Map(ensure_bytes_id)
                | resample.Resample(increment_s=60*10, max_gap_s=60*70, extrapolate=False)
                | compute_adjacency.ComputeAdjacency(max_adjacency_distance_km=1.0) 
            )
            assert_that(results, equal_to(self._get_expected(interpolated=True)))

    def _get_expected(self, interpolated=True):
        density = 0.5 if interpolated else 1.0
        return [
            TaggedAnnotatedRecord(b'1', ResampledRecord("2011-01-01 15:40:00 UTC", -1.5032387, 55.2340155),0,None),
            TaggedAnnotatedRecord(b'1', ResampledRecord("2011-01-01 15:50:00 UTC",-1.4869308,55.232743),0,None),
            TaggedAnnotatedRecord(b'1', ResampledRecord("2011-01-01 16:00:00 UTC",-1.4752579,55.2292189,id=b'1'),1,
                ((b'2',0.6322538449438863, ("2011-01-01 16:00:00 UTC",-1.469593,55.2287294)))),
            # Encounter start
            TaggedAnnotatedRecord(b'1', ResampledRecord("2011-01-01 16:10:00 UTC",-1.4719963,55.2251069),1,
                (b'2',0.20817755069585123, ("2011-01-01 16:10:00 UTC",-1.471138,55.2267713)))] + ([
                TaggedAnnotatedRecord(b'1', ResampledRecord("2011-01-01 16:20:00 UTC",-1.471653,55.2224633),1,
                    (b'2',0.16617515594685828, ("2011-01-01 16:20:00 UTC",-1.4707947,55.22368710000001, density)))
                    ] if interpolated else
                [TaggedAnnotatedRecord(b'1', ResampledRecord("2011-01-01 16:20:00 UTC",-1.471653,55.2224633),0,None)
            ]) + [
            TaggedAnnotatedRecord(b'1', ResampledRecord("2011-01-01 16:30:00 UTC",-1.4718246,55.2199175),1,
                (b'2',0.17064497317314595, ("2011-01-01 16:30:00 UTC",-1.4704514,55.2206029, density))),
            TaggedAnnotatedRecord(b'1', ResampledRecord("2011-01-01 16:40:00 UTC",-1.472168,55.2185466),1,
                (b'2',0.1984842113071651, ("2011-01-01 16:40:00 UTC",-1.4704514,55.218057))),
            TaggedAnnotatedRecord(b'1', ResampledRecord("2011-01-01 16:50:00 UTC",-1.4718246,55.2167839),1,
                (b'2',0.23162828282668915, ("2011-01-01 16:50:00 UTC",-1.4704514,55.215217))),
            TaggedAnnotatedRecord(b'1', ResampledRecord("2011-01-01 17:00:00 UTC",-1.4725113,55.2156088),1,
                (b'2',0.4371321971770557, ("2011-01-01 17:00:00 UTC",-1.4728546,55.2116913))),
            # Encounter end
            TaggedAnnotatedRecord(b'1', ResampledRecord("2011-01-01 17:10:00 UTC",-1.4730263,55.2139439),1,
                (b'2',0.5816845132999947, ("2011-01-01 17:10:00 UTC",-1.4718246,55.2088509))),
            # TaggedAnnotatedRecord(b'1', ResampledRecord("2011-01-01 17:10:00 UTC",-1.4730263,55.2139439),1,None),
            TaggedAnnotatedRecord(b'1', ResampledRecord("2011-01-01 17:20:00 UTC",-1.4859009,55.2089489),0,None),
            TaggedAnnotatedRecord(b'1', ResampledRecord("2011-01-01 17:30:00 UTC",-1.4974022,55.2078715),0,None),
            TaggedAnnotatedRecord(b'1', ResampledRecord("2011-01-01 17:40:00 UTC",-1.5140533,55.2069899),0,None),

            TaggedAnnotatedRecord(b'2', ResampledRecord("2011-01-01 15:20:00 UTC", -1.4065933, 55.2350923),0,None),
            TaggedAnnotatedRecord(b'2', ResampledRecord("2011-01-01 15:30:00 UTC",-1.4218712,55.2342113),0,None),
            TaggedAnnotatedRecord(b'2', ResampledRecord("2011-01-01 15:40:00 UTC",-1.4467621,55.2334282),0,None),
            TaggedAnnotatedRecord(b'2', ResampledRecord("2011-01-01 15:50:00 UTC",-1.4623833,55.2310789),0,None),
            TaggedAnnotatedRecord(b'2', ResampledRecord("2011-01-01 16:00:00 UTC",-1.469593,55.2287294),1,
                (b'1',0.6322538449438863, ("2011-01-01 16:00:00 UTC",-1.4752579,55.2292189))),
            # Encounter Start
            TaggedAnnotatedRecord(b'2', ResampledRecord("2011-01-01 16:10:00 UTC",-1.471138,55.2267713),1,
                (b'1',0.20817755069585123, ("2011-01-01 16:10:00 UTC",-1.4719963,55.2251069)))] + ([
                TaggedAnnotatedRecord(b'2', ResampledRecord("2011-01-01 16:20:00 UTC",-1.4707947,55.22368710000001, density),1,
                    (b'1',0.16617515594685828, ("2011-01-01 16:20:00 UTC",-1.471653,55.2224633)))
                    ] if interpolated else []) + [
            TaggedAnnotatedRecord(b'2', ResampledRecord("2011-01-01 16:30:00 UTC",-1.4704514,55.2206029, density),1,
                (b'1',0.17064497317314595, ("2011-01-01 16:30:00 UTC",-1.4718246,55.2199175))),
            TaggedAnnotatedRecord(b'2', ResampledRecord("2011-01-01 16:40:00 UTC",-1.4704514,55.218057),1,
                (b'1',0.1984842113071651, ("2011-01-01 16:40:00 UTC",-1.472168,55.2185466))),
            TaggedAnnotatedRecord(b'2', ResampledRecord("2011-01-01 16:50:00 UTC",-1.4704514,55.215217),1,
                (b'1',0.23162828282668915, ("2011-01-01 16:50:00 UTC",-1.4718246,55.2167839))),
            TaggedAnnotatedRecord(b'2', ResampledRecord("2011-01-01 17:00:00 UTC",-1.4728546,55.2116913),1,
                (b'1',0.4371321971770557, ("2011-01-01 17:00:00 UTC",-1.4725113,55.2156088))),
            TaggedAnnotatedRecord(b'2', ResampledRecord("2011-01-01 17:10:00 UTC",-1.4718246,55.2088509),1,
                (b'1',0.5816845132999947, ("2011-01-01 17:10:00 UTC",-1.4730263,55.2139439))),
            # Encounter End
            TaggedAnnotatedRecord(b'2', ResampledRecord("2011-01-01 17:20:00 UTC",-1.4474487,55.2057165),0,None),
            TaggedAnnotatedRecord(b'2', ResampledRecord("2011-01-01 17:30:00 UTC",-1.4278793,55.2040512),0,None),
            TaggedAnnotatedRecord(b'2', ResampledRecord("2011-01-01 17:40:00 UTC",-1.4084816,55.2036594),0,None),
            TaggedAnnotatedRecord(b'2', ResampledRecord("2011-01-01 17:50:00 UTC",-1.3998985,55.2037574),0,None)
    ]

