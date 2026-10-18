import datetime
import logging
import unittest

import apache_beam as beam
import numpy as np
import pytest
import pytz
import six
from apache_beam import Map
from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.util import assert_that
from pipe_encounters.core import encounter, record
from pipe_encounters.pipelines.partial_encounters.transforms import compute_adjacency
from pipe_encounters.pipelines.partial_encounters.transforms.compute_adjacency import (
    ComputeAdjacency,
)
from pipe_encounters.pipelines.partial_encounters.transforms.compute_encounters import (
    ComputeEncounters,
)
from pipe_encounters.pipelines.encounters.transforms.merge_encounters import MergeEncounters
from pipe_encounters.pipelines.partial_encounters.transforms.resample import Resample
from pipe_encounters.utils.test import approx_equal_to as equal_to

from .series_data import dateline_series_data, real_series_data
from tests.pipelines.partial_encounters.transforms.test_resample import ResampledRecord


def ensure_bytes_id(obj):
    return obj._replace(id=six.ensure_binary(obj.id))


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def add_fake_vessel_id(obj):
    obj = obj._asdict()
    for v in [1, 2]:
        obj[f"vessel_{v}_seg_id"] = (
            six.ensure_binary(obj[f"vessel_{v}_seg_id"]),
            six.ensure_binary(obj[f"vessel_{v}_seg_id"]),
        )
    return encounter.RawEncounter(**obj)


inf = float("inf")


def ts(x):
    return datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc)


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
    return compute_adjacency.AnnotatedRecord(
        closest_distances=closest_distances,
        closest_neighbors=closest_neighbors,
        **record._asdict(),
    )


def get_options(items: list):
    tuple_data = [tuple(x) for x in items]
    return tuple_data, PipelineOptions()


@pytest.mark.filterwarnings("ignore:Using fallback coder:UserWarning")
@pytest.mark.filterwarnings(
    "ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning"
)
class TestMergeEncounters(unittest.TestCase):

    def test_merge_messages(self):
        items, opts = get_options(real_series_data)
        with _TestPipeline(options=opts) as p:
            results = (
                p
                | beam.Create(items)
                | beam.Map(lambda x: record.Record(*x))
                | "Ensure ID is bytes" >> Map(ensure_bytes_id)
                | Resample(increment_s=60 * 10, max_gap_s=60 * 70, extrapolate=False)
                | ComputeAdjacency(max_adjacency_distance_km=1.0)
                | ComputeEncounters(max_km_for_encounter=0.5, min_minutes_for_encounter=30)
                | beam.Map(add_fake_vessel_id)
                | MergeEncounters(min_hours_between_encounters=24)
                | encounter.Encounter.ToDict()
            )
            assert_that(results, equal_to(self._get_merged_expected()))

    def test_merge_dateline(self):

        items, opts = get_options(dateline_series_data)
        with _TestPipeline(options=opts) as p:
            results = (
                p
                | beam.Create(items)
                | beam.Map(lambda x: record.Record(*x))
                | "Ensure ID is bytes" >> Map(ensure_bytes_id)
                | Resample(increment_s=60 * 10, max_gap_s=60 * 70, extrapolate=False)
                | ComputeAdjacency(max_adjacency_distance_km=1.0)
                | ComputeEncounters(max_km_for_encounter=0.5, min_minutes_for_encounter=30)
                | beam.Map(add_fake_vessel_id)
                | MergeEncounters(min_hours_between_encounters=24)
                | encounter.Encounter.ToDict()
            )
            assert_that(results, equal_to(self._get_merged_dateline_expected()))

    def _get_merged_expected(self):
        return [
            {
                "median_speed_knots": 0.18809378172003316,
                "start_time": 1426750800.0,
                "mean_longitude": 38.53406239539688,
                "vessel_1_seg_ids": ["441910000"],
                "vessel_2_seg_ids": ["563418000"],
                "vessel_2_point_count": 10,
                "mean_latitude": -27.479382615650064,
                "end_time": 1426795800.0,
                "median_distance_km": 0.02959787505046703,
                "vessel_1_point_count": 12,
                "vessel_2_id": b"563418000",
                "vessel_1_id": b"441910000",
                "start_lat": -27.484518051171428,
                "start_lon": 38.53651973177143,
                "end_lat": -27.464933395399996,
                "end_lon": 38.52257468481818,
            }
        ]

    def _get_merged_dateline_expected(self):
        return [
            {
                "vessel_1_id": b"1",
                "vessel_2_id": b"2",
                "start_time": 1293898200.0,
                "vessel_1_seg_ids": ["1"],
                "vessel_2_seg_ids": ["2"],
                "end_time": 1293901800.0,
                "mean_latitude": -1.4716407285714286,
                "mean_longitude": 179.99984999999998,
                "median_distance_km": 0.21170220169329662,
                "median_speed_knots": 0.18393498586650497,
                "vessel_1_point_count": 14,
                "vessel_2_point_count": 12,
                "start_lat": -1.4719963,
                "start_lon": 179.999,
                "end_lat": -1.4730263,
                "end_lon": 179.999,
            }
        ]
