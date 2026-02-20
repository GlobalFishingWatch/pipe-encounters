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
from apache_beam.testing.util import assert_that
from pipe_encounters.objects import encounter, record
from pipe_encounters.options.create_options import CreateOptions
from pipe_encounters.transforms import compute_adjacency
from pipe_encounters.transforms.compute_adjacency import ComputeAdjacency
from pipe_encounters.transforms.compute_encounters import ComputeEncounters
from pipe_encounters.transforms.merge_encounters import MergeEncounters
from pipe_encounters.transforms.resample import Resample
from pipe_encounters.utils.test import approx_equal_to as equal_to

from .series_data import (dateline_series_data, fastsep_series_data,
                          multi_series_data, real_series_data,
                          simple_series_data)
from .test_resample import ResampledRecord


def ensure_bytes_id(obj):
    return obj._replace(id=six.ensure_binary(obj.id))


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


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

def get_options(items:list):
    tuple_data = [tuple(x) for x in items]
    args = [
        f"--source_table={tuple_data}",
        "--raw_table=test",
        "--start_date=2011-01-01",
        "--end_date=2011-01-01",
        "--max_encounter_dist_km=0.5",
        "--min_encounter_time_minutes=120"
    ]
    return tuple_data, CreateOptions(args)




@pytest.mark.filterwarnings("ignore:Using fallback coder:UserWarning")
@pytest.mark.filterwarnings(
    "ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning"
)
class TestComputeEncounters(unittest.TestCase):
    def test_simple_encounters(self):
        items, opts = get_options(simple_series_data)
        with _TestPipeline(options=opts) as p:
            results = (
                p
                | beam.Create(items)
                | beam.Map(lambda x: record.Record(*x))
                | "Ensure ID is bytes" >> Map(ensure_bytes_id)
                | Resample(increment_s=60 * 10, max_gap_s=60 * 70, extrapolate=False)
                | ComputeAdjacency(max_adjacency_distance_km=1.0)
                | ComputeEncounters(
                    max_km_for_encounter=0.5, min_minutes_for_encounter=30
                )
            )
            assert_that(results, equal_to(self._get_simple_expected()))

    def test_multi_encounters(self):
        items, opts = get_options(multi_series_data)
        with _TestPipeline(options=opts) as p:
            results = (
                p
                | beam.Create(items)
                | beam.Map(lambda x: record.Record(*x))
                | "Ensure ID is bytes" >> Map(ensure_bytes_id)
                | Resample(increment_s=60 * 10, max_gap_s=60 * 70, extrapolate=False)
                | ComputeAdjacency(max_adjacency_distance_km=1.0)
                | ComputeEncounters(
                    max_km_for_encounter=0.5, min_minutes_for_encounter=30
                )
            )
            assert_that(results, equal_to(self._get_multi_expected()))

    def test_nerfed_multi_encounters(self):
        """If we reduce the number of tracked distances we shouldn't get all of the encounters"""
        items, opts = get_options(multi_series_data)
        with _TestPipeline(options=opts) as p:
            results = (
                p
                | beam.Create(items)
                | beam.Map(lambda x: record.Record(*x))
                | "Ensure ID is bytes" >> Map(ensure_bytes_id)
                | Resample(increment_s=60 * 10, max_gap_s=60 * 70, extrapolate=False)
                | ComputeAdjacency(
                    max_adjacency_distance_km=1.0, max_tracked_distances=1
                )
                | ComputeEncounters(
                    max_km_for_encounter=0.5, min_minutes_for_encounter=30
                )
            )
            assert_that(results, equal_to(self._get_nerfed_multi_expected()))

    def test_dateline_encounters(self):
        items, opts = get_options(dateline_series_data)
        with _TestPipeline(options=opts) as p:
            results = (
                p
                | beam.Create(items)
                | beam.Map(lambda x: record.Record(*x))
                | "Ensure ID is bytes" >> Map(ensure_bytes_id)
                | Resample(increment_s=60 * 10, max_gap_s=60 * 70, extrapolate=False)
                | ComputeAdjacency(max_adjacency_distance_km=1.0)
                | ComputeEncounters(
                    max_km_for_encounter=0.5, min_minutes_for_encounter=30
                )
            )
            assert_that(results, equal_to(self._get_dateline_expected()))

    def test_fastsep_encounters(self):
        """Make sure false encounters aren't generated when vessels move rapidly

        There's a possible corner case where vessels that move apart rapidly are invisible
        while they are far apart. If they then move back together quickly a false, or overly
        extended encounter could be generated. However the current algorithm is not vulnerable
        to this.
        """
        items, opts = get_options(fastsep_series_data)
        with _TestPipeline(options=opts) as p:
            results = (
                p
                | beam.Create(items)
                | beam.Map(lambda x: record.Record(*x))
                | "Ensure ID is bytes" >> Map(ensure_bytes_id)
                | Resample(increment_s=60 * 10, max_gap_s=60 * 70, extrapolate=False)
                | ComputeAdjacency(max_adjacency_distance_km=1.0)
                | ComputeEncounters(
                    max_km_for_encounter=0.5, min_minutes_for_encounter=30
                )
            )
            assert_that(results, equal_to([]))

    def test_real_data(self):
        items, opts = get_options(real_series_data)
        with _TestPipeline(options=opts) as p:
            results = (
                p
                | beam.Create(items)
                | beam.Map(lambda x: record.Record(*x))
                | "Ensure ID is bytes" >> Map(ensure_bytes_id)
                | Resample(increment_s=60 * 10, max_gap_s=60 * 70, extrapolate=False)
                | ComputeAdjacency(max_adjacency_distance_km=1.0)
                | ComputeEncounters(
                    max_km_for_encounter=0.5, min_minutes_for_encounter=30
                )
            )
            assert_that(results, equal_to(self._get_real_expected()))

    def test_message_generation(self):
        items, opts = get_options(real_series_data)
        with _TestPipeline(options=opts) as p:
            results = (
                p
                | beam.Create(items)
                | beam.Map(lambda x: record.Record(*x))
                | "Ensure ID is bytes" >> Map(ensure_bytes_id)
                | Resample(increment_s=60 * 10, max_gap_s=60 * 70, extrapolate=False)
                | ComputeAdjacency(max_adjacency_distance_km=1.0)
                | ComputeEncounters(
                    max_km_for_encounter=0.5, min_minutes_for_encounter=30
                )
                | encounter.Encounter.ToDict()
            )
            assert_that(results, equal_to(self._get_messages_expected()))

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
                | ComputeEncounters(
                    max_km_for_encounter=0.5, min_minutes_for_encounter=30
                )
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
                | ComputeEncounters(
                    max_km_for_encounter=0.5, min_minutes_for_encounter=30
                )
                | beam.Map(add_fake_vessel_id)
                | MergeEncounters(min_hours_between_encounters=24)
                | encounter.Encounter.ToDict()
            )
            assert_that(results, equal_to(self._get_merged_dateline_expected()))

    def _get_simple_expected(self):
        return [
            encounter.RawEncounter(
                b"1",
                b"2",
                ts("2011-01-01T16:10:00Z"),
                ts("2011-01-01T17:00:00Z"),
                -1.4719963,
                55.21973783333268,
                0.20333088100150815,
                0.6467305031568592,
                6,
                5,
                start_lat=-1.4719963,
                start_lon=55.2251069,
                end_lat=-1.4725113,
                end_lon=55.2156088,
            ),
            encounter.RawEncounter(
                b"2",
                b"1",
                ts("2011-01-01T16:10:00Z"),
                ts("2011-01-01T17:00:00Z"),
                -1.4710235833333334,
                55.219337766666875,
                0.20333088100150815,
                1.1175558891689739,
                5,
                6,
                start_lat=-1.471138,
                start_lon=55.2267713,
                end_lat=-1.4728546,
                end_lon=55.2116913,
            ),
        ]

    def _get_dateline_expected(self):
        return [
            encounter.RawEncounter(
                b"1",
                b"2",
                datetime.datetime(2011, 1, 1, 16, 10, tzinfo=pytz.UTC),
                datetime.datetime(2011, 1, 1, 17, 10, tzinfo=pytz.UTC),
                -1.472143442857143,
                179.999,
                0.21170220169329662,
                0.12370707201347748,
                7,
                6,
                start_lat=-1.4719963,
                start_lon=179.999,
                end_lat=-1.4730263,
                end_lon=179.999,
            ),
            encounter.RawEncounter(
                b"2",
                b"1",
                start_time=datetime.datetime(2011, 1, 1, 16, 10, tzinfo=pytz.UTC),
                end_time=datetime.datetime(2011, 1, 1, 17, 10, tzinfo=pytz.UTC),
                mean_latitude=-1.4711380142857142,
                mean_longitude=-179.9993,
                median_distance_km=0.21170220169329662,
                median_speed_knots=0.24416289971953248,
                vessel_1_point_count=6,
                vessel_2_point_count=7,
                start_lat=-1.471138,
                start_lon=-179.9993,
                end_lat=-1.4718246,
                end_lon=-179.999,
            ),
        ]

    def _get_multi_expected(self):
        return [
            encounter.RawEncounter(
                vessel_1_seg_id=b"1",
                vessel_2_seg_id=b"2",
                start_time=datetime.datetime(2011, 1, 1, 15, 40, tzinfo=pytz.UTC),
                end_time=datetime.datetime(2011, 1, 1, 17, 40, tzinfo=pytz.UTC),
                mean_latitude=0.0,
                mean_longitude=0.0,
                median_distance_km=0.0,
                median_speed_knots=0.0,
                vessel_1_point_count=13,
                vessel_2_point_count=12,
                start_lat=0.0,
                start_lon=0.0,
                end_lat=0.0,
                end_lon=0.0,
            ),
            encounter.RawEncounter(
                vessel_1_seg_id=b"1",
                vessel_2_seg_id=b"3",
                start_time=datetime.datetime(2011, 1, 1, 15, 40, tzinfo=pytz.UTC),
                end_time=datetime.datetime(2011, 1, 1, 17, 40, tzinfo=pytz.UTC),
                mean_latitude=0.0,
                mean_longitude=0.0,
                median_distance_km=0.11119492664455874,
                median_speed_knots=0.0,
                vessel_1_point_count=13,
                vessel_2_point_count=12,
                start_lat=0.0,
                start_lon=0.0,
                end_lat=0.0,
                end_lon=0.0,
            ),
            encounter.RawEncounter(
                vessel_1_seg_id=b"2",
                vessel_2_seg_id=b"1",
                start_time=datetime.datetime(2011, 1, 1, 15, 40, tzinfo=pytz.UTC),
                end_time=datetime.datetime(2011, 1, 1, 17, 40, tzinfo=pytz.UTC),
                mean_latitude=0.0,
                mean_longitude=0.00046153846153749106,
                median_distance_km=0.0,
                median_speed_knots=0.0,
                vessel_1_point_count=12,
                vessel_2_point_count=13,
                start_lat=0.0,
                start_lon=0.0,
                end_lat=0.0,
                end_lon=0.001,
            ),
            encounter.RawEncounter(
                vessel_1_seg_id=b"2",
                vessel_2_seg_id=b"3",
                start_time=datetime.datetime(2011, 1, 1, 15, 20, tzinfo=pytz.UTC),
                end_time=datetime.datetime(2011, 1, 1, 17, 50, tzinfo=pytz.UTC),
                mean_latitude=0.0,
                mean_longitude=0.0004374999999984383,
                median_distance_km=0.11119492664455874,
                median_speed_knots=0.0,
                vessel_1_point_count=15,
                vessel_2_point_count=15,
                start_lat=0.0,
                start_lon=0.0,
                end_lat=0.0,
                end_lon=0.001,
            ),
            encounter.RawEncounter(
                vessel_1_seg_id=b"3",
                vessel_2_seg_id=b"1",
                start_time=datetime.datetime(2011, 1, 1, 15, 40, tzinfo=pytz.UTC),
                end_time=datetime.datetime(2011, 1, 1, 17, 40, tzinfo=pytz.UTC),
                mean_latitude=0.0,
                mean_longitude=0.000538461538462509,
                median_distance_km=0.11119492664455874,
                median_speed_knots=0.0,
                vessel_1_point_count=12,
                vessel_2_point_count=13,
                start_lat=0.0,
                start_lon=0.001,
                end_lat=0.0,
                end_lon=0.0,
            ),
            encounter.RawEncounter(
                vessel_1_seg_id=b"3",
                vessel_2_seg_id=b"2",
                start_time=datetime.datetime(2011, 1, 1, 15, 20, tzinfo=pytz.UTC),
                end_time=datetime.datetime(2011, 1, 1, 17, 50, tzinfo=pytz.UTC),
                mean_latitude=0.0,
                mean_longitude=0.0005625000000015619,
                median_distance_km=0.11119492664455874,
                median_speed_knots=0.0,
                vessel_1_point_count=15,
                vessel_2_point_count=15,
                start_lat=0.0,
                start_lon=0.001,
                end_lat=0.0,
                end_lon=0.0,
            ),
        ]

    def _get_nerfed_multi_expected(self):
        return [
            encounter.RawEncounter(
                vessel_1_seg_id=b"1",
                vessel_2_seg_id=b"2",
                start_time=datetime.datetime(2011, 1, 1, 15, 40, tzinfo=pytz.UTC),
                end_time=datetime.datetime(2011, 1, 1, 16, 40, tzinfo=pytz.UTC),
                mean_latitude=0.0,
                mean_longitude=0.0,
                median_distance_km=0.0,
                median_speed_knots=0.0,
                vessel_1_point_count=7,
                vessel_2_point_count=6,
                start_lat=0.0,
                start_lon=0.0,
                end_lat=0.0,
                end_lon=0.0,
            ),
            encounter.RawEncounter(
                vessel_1_seg_id=b"1",
                vessel_2_seg_id=b"3",
                start_time=datetime.datetime(2011, 1, 1, 16, 50, tzinfo=pytz.UTC),
                end_time=datetime.datetime(2011, 1, 1, 17, 40, tzinfo=pytz.UTC),
                mean_latitude=0.0,
                mean_longitude=0.0,
                median_distance_km=0.0,
                median_speed_knots=0.0,
                vessel_1_point_count=6,
                vessel_2_point_count=6,
                start_lat=0.0,
                start_lon=0.0,
                end_lat=0.0,
                end_lon=0.0,
            ),
            encounter.RawEncounter(
                vessel_1_seg_id=b"2",
                vessel_2_seg_id=b"1",
                start_time=datetime.datetime(2011, 1, 1, 15, 40, tzinfo=pytz.UTC),
                end_time=datetime.datetime(2011, 1, 1, 17, 40, tzinfo=pytz.UTC),
                mean_latitude=0.0,
                mean_longitude=0.00046153846153749106,
                median_distance_km=0.0,
                median_speed_knots=0.0,
                vessel_1_point_count=12,
                vessel_2_point_count=13,
                start_lat=0.0,
                start_lon=0.0,
                end_lat=0.0,
                end_lon=0.001,
            ),
            encounter.RawEncounter(
                vessel_1_seg_id=b"3",
                vessel_2_seg_id=b"1",
                start_time=datetime.datetime(2011, 1, 1, 15, 40, tzinfo=pytz.UTC),
                end_time=datetime.datetime(2011, 1, 1, 17, 40, tzinfo=pytz.UTC),
                mean_latitude=0.0,
                mean_longitude=0.000538461538462509,
                median_distance_km=0.11119492664455874,
                median_speed_knots=0.0,
                vessel_1_point_count=12,
                vessel_2_point_count=13,
                start_lat=0.0,
                start_lon=0.001,
                end_lat=0.0,
                end_lon=0.0,
            ),
        ]

    def _get_real_expected(self):
        return [
            encounter.RawEncounter(
                b"441910000",
                b"563418000",
                ts("2015-03-19T07:40:00Z"),
                ts("2015-03-19T20:10:00Z"),
                -27.47909444042379,
                38.533749458969304,
                0.028845166034633843,
                0.20569554161530468,
                7,
                6,
                start_lat=-27.48458079902857,
                start_lon=38.5362468641449,
                end_lat=-27.464933395399996,
                end_lon=38.52257468481818,
            ),
            encounter.RawEncounter(
                b"563418000",
                b"441910000",
                ts("2015-03-19T07:40:00Z"),
                ts("2015-03-19T10:10:00Z"),
                -27.480823491781422,
                38.53562707753488,
                0.030350584066300215,
                0.17049202182476167,
                4,
                5,
                start_lat=-27.484518051171428,
                start_lon=38.53651973177143,
                end_lat=-27.475459163642533,
                end_lon=38.53207037113999,
            ),
        ]

    def _get_messages_expected(self):
        return [
            dict(
                [
                    ("start_time", 1426750800.0),
                    ("end_time", 1426795800.0),
                    ("mean_latitude", -27.47909444042379),
                    ("mean_longitude", 38.533749458969304),
                    ("median_distance_km", 0.028845166034633843),
                    ("median_speed_knots", 0.20569554161530468),
                    ("vessel_1_point_count", 7),
                    ("vessel_2_point_count", 6),
                    ("vessel_1_seg_id", b"441910000"),
                    ("vessel_2_seg_id", b"563418000"),
                    ("start_lat", -27.48458079902857),
                    ("start_lon", 38.5362468641449),
                    ("end_lat", -27.464933395399996),
                    ("end_lon", 38.52257468481818),
                ]
            ),
            dict(
                [
                    ("start_time", 1426750800.0),
                    ("end_time", 1426759800.0),
                    ("mean_latitude", -27.480823491781422),
                    ("mean_longitude", 38.53562707753488),
                    ("median_distance_km", 0.030350584066300215),
                    ("median_speed_knots", 0.17049202182476167),
                    ("vessel_1_point_count", 4),
                    ("vessel_2_point_count", 5),
                    ("vessel_1_seg_id", b"563418000"),
                    ("vessel_2_seg_id", b"441910000"),
                    ("start_lat", -27.484518051171428),
                    ("start_lon", 38.53651973177143),
                    ("end_lat", -27.475459163642533),
                    ("end_lon", 38.53207037113999),
                ]
            ),
        ]

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
