import datetime


from pipe_encounters.core import encounter
from pipe_encounters.utils.test import approx_equal_to
from tests.pipelines.partial_encounters.transforms.steps import detect, records
from tests.series_data import (
    dateline_series_data,
    fastsep_series_data,
    multi_series_data,
    real_series_data,
    simple_series_data,
)


inf = float("inf")


def ts(x):
    dt = datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ")
    return dt.replace(tzinfo=datetime.timezone.utc)


def test_simple_encounters():
    results = detect(records(simple_series_data))

    approx_equal_to(simple_expected())(results)


def test_multi_encounters():
    results = detect(records(multi_series_data))

    approx_equal_to(multi_expected())(results)


def test_nerfed_multi_encounters():
    """If we reduce the number of tracked distances we shouldn't get all of the encounters"""
    results = detect(records(multi_series_data), max_tracked_distances=1)

    approx_equal_to(nerfed_multi_expected())(results)


def test_dateline_encounters():
    results = detect(records(dateline_series_data))

    approx_equal_to(dateline_expected())(results)


def test_fastsep_encounters():
    """Make sure false encounters aren't generated when vessels move rapidly

    There's a possible corner case where vessels that move apart rapidly are invisible
    while they are far apart. If they then move back together quickly a false, or overly
    extended encounter could be generated. However the current algorithm is not vulnerable
    to this.
    """
    results = detect(records(fastsep_series_data))

    approx_equal_to([])(results)


def test_real_data():
    results = detect(records(real_series_data))

    approx_equal_to(real_expected())(results)


def test_message_generation():
    results = [encounter.Encounter.ToDict().to_dict(r) for r in detect(records(real_series_data))]

    approx_equal_to(messages_expected())(results)


def simple_expected():
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


def multi_expected():
    return [
        encounter.RawEncounter(
            vessel_1_seg_id=b"1",
            vessel_2_seg_id=b"2",
            start_time=datetime.datetime(2011, 1, 1, 15, 40, tzinfo=datetime.timezone.utc),
            end_time=datetime.datetime(2011, 1, 1, 17, 40, tzinfo=datetime.timezone.utc),
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
            start_time=datetime.datetime(2011, 1, 1, 15, 40, tzinfo=datetime.timezone.utc),
            end_time=datetime.datetime(2011, 1, 1, 17, 40, tzinfo=datetime.timezone.utc),
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
            start_time=datetime.datetime(2011, 1, 1, 15, 40, tzinfo=datetime.timezone.utc),
            end_time=datetime.datetime(2011, 1, 1, 17, 40, tzinfo=datetime.timezone.utc),
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
            start_time=datetime.datetime(2011, 1, 1, 15, 20, tzinfo=datetime.timezone.utc),
            end_time=datetime.datetime(2011, 1, 1, 17, 50, tzinfo=datetime.timezone.utc),
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
            start_time=datetime.datetime(2011, 1, 1, 15, 40, tzinfo=datetime.timezone.utc),
            end_time=datetime.datetime(2011, 1, 1, 17, 40, tzinfo=datetime.timezone.utc),
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
            start_time=datetime.datetime(2011, 1, 1, 15, 20, tzinfo=datetime.timezone.utc),
            end_time=datetime.datetime(2011, 1, 1, 17, 50, tzinfo=datetime.timezone.utc),
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


def nerfed_multi_expected():
    return [
        encounter.RawEncounter(
            vessel_1_seg_id=b"1",
            vessel_2_seg_id=b"2",
            start_time=datetime.datetime(2011, 1, 1, 15, 40, tzinfo=datetime.timezone.utc),
            end_time=datetime.datetime(2011, 1, 1, 16, 40, tzinfo=datetime.timezone.utc),
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
            start_time=datetime.datetime(2011, 1, 1, 16, 50, tzinfo=datetime.timezone.utc),
            end_time=datetime.datetime(2011, 1, 1, 17, 40, tzinfo=datetime.timezone.utc),
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
            start_time=datetime.datetime(2011, 1, 1, 15, 40, tzinfo=datetime.timezone.utc),
            end_time=datetime.datetime(2011, 1, 1, 17, 40, tzinfo=datetime.timezone.utc),
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
            start_time=datetime.datetime(2011, 1, 1, 15, 40, tzinfo=datetime.timezone.utc),
            end_time=datetime.datetime(2011, 1, 1, 17, 40, tzinfo=datetime.timezone.utc),
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


def dateline_expected():
    return [
        encounter.RawEncounter(
            b"1",
            b"2",
            datetime.datetime(2011, 1, 1, 16, 10, tzinfo=datetime.timezone.utc),
            datetime.datetime(2011, 1, 1, 17, 10, tzinfo=datetime.timezone.utc),
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
            start_time=datetime.datetime(2011, 1, 1, 16, 10, tzinfo=datetime.timezone.utc),
            end_time=datetime.datetime(2011, 1, 1, 17, 10, tzinfo=datetime.timezone.utc),
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


def real_expected():
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


def messages_expected():
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
