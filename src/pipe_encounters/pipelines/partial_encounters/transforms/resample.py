"""Resampling of records onto a constant time grid."""

import datetime as dtime
import math

from collections.abc import Callable, Iterator
from typing import Any

from apache_beam import FlatMap, PCollection, PTransform
from more_itertools import peekable

from pipe_encounters.core.record import Record
from pipe_encounters.core.resampled_record import ResampledRecord
from pipe_encounters.pipelines.partial_encounters.transforms.group_by_id import GroupByIdAndDate
from pipe_encounters.pipelines.partial_encounters.transforms.sort_by_time import SortByTime


def timestamp_as_datetime(ts: float) -> dtime.datetime:
    """Return the UTC datetime of a timestamp in seconds from the Unix epoch."""
    return dtime.datetime.fromtimestamp(ts, tz=dtime.timezone.utc)


class Resample(PTransform):
    """Resample Records onto a constant grid.

    Parameters
    ----------
    increment_s : float
        New records are interpolated every increment_s seconds aligned width midnight.
    max_gap_s : float
        Gaps longer than max_gap_s are not fully interpolated across.
    extrapolate : bool, optional
        If true, extrapolate past the end of the first/last record up to
        max_gap_s or the day boundary, whichever is closer.
    inclusive : bool, optional
        If true, a gap of exactly max_gap_s is still interpolated across.
        If false (default), only gaps strictly less than max_gap_s qualify.
    """

    epoch = dtime.datetime.fromtimestamp(0, tz=dtime.timezone.utc)

    def __init__(
        self,
        increment_s: float,
        max_gap_s: float,
        extrapolate: bool = True,
        inclusive: bool = False,
    ) -> None:
        """Initialize the transform."""
        self.increment_s = increment_s
        self.max_gap_s = max_gap_s
        self.extrapolate = extrapolate
        self.inclusive = inclusive

    def dt_to_s(self, timestamp: dtime.datetime) -> float:
        """Return the seconds from the Unix epoch of a datetime."""
        return (timestamp - self.epoch).total_seconds()

    def round_to_increment(
        self, timestamp: dtime.datetime, rounder: Callable[[float], int]
    ) -> float:
        """Round the timestamp to a multiple of the increment, in seconds."""
        return rounder(self.dt_to_s(timestamp) / self.increment_s) * self.increment_s

    def _interpolate(
        self, records: list[Record], begin_time: float, end_time: float
    ) -> Iterator[ResampledRecord]:
        """Interpolate records onto a grid between beg and end times inclusive.

        Parameters
        ----------
        records : list of Records
        begin_time, end_time : float
            Timestamp in seconds measured from Unix Epoch

        Yields:
        ------
        ResampledRecord
        """
        if len(records) >= 2:
            interp_time = begin_time
            record_iter = peekable(records)
            last_record = next(record_iter)
            current_record = next(record_iter)

            while interp_time <= end_time:
                while record_iter and (self.dt_to_s(current_record.timestamp) < interp_time):
                    # Advance record_iter till last_record and current_record bracket current time.
                    last_record = current_record
                    current_record = next(record_iter)
                    assert last_record.id == current_record.id

                t0 = self.dt_to_s(last_record.timestamp)
                t1 = self.dt_to_s(current_record.timestamp)
                DT = t1 - t0
                dt = interp_time - t0

                assert t0 <= interp_time
                assert t1 >= interp_time
                assert DT > 0

                if self.inclusive:
                    gap_within_limit = DT <= self.max_gap_s
                else:
                    gap_within_limit = DT < self.max_gap_s

                if gap_within_limit:
                    mix = dt / DT
                    yield ResampledRecord(
                        id=last_record.id,
                        timestamp=timestamp_as_datetime(interp_time),
                        lat=current_record.lat * mix + last_record.lat * (1 - mix),
                        lon=current_record.lon * mix + last_record.lon * (1 - mix),
                        speed=current_record.speed * mix + last_record.speed * (1 - mix),
                        point_density=round(
                            self.increment_s / max(float(DT), self.increment_s), 2
                        ),
                    )

                interp_time += self.increment_s

    def _extrapolate(
        self, record: Record, beg_time: float, end_time: float
    ) -> Iterator[ResampledRecord]:
        """Extrapolate a single record onto a grid between beg and end times inclusive.

        Parameters
        ----------
        record : Record
        beg_time, end_time : float
            Timestamp in seconds measured from Unix Epoch

        Yields:
        ------
        ResampledRecord
        """
        t0 = self.dt_to_s(record.timestamp)
        rads = math.radians(record.course)
        nm_s = record.speed / (60 * 60)
        deg_lat_s = nm_s / 60
        dlat_dt = math.cos(rads) * deg_lat_s
        EPSILON = 0.1
        dlon_dt = math.sin(rads) * deg_lat_s / max(math.cos(math.radians(record.lat)), EPSILON)

        interp_time = beg_time
        while interp_time <= end_time:
            dt = interp_time - t0
            lat = record.lat + dt * dlat_dt
            lon = record.lon + dt * dlon_dt
            lon = (lon + 180) % 360 - 180
            yield ResampledRecord(
                id=record.id,
                timestamp=timestamp_as_datetime(interp_time),
                lat=lat,
                lon=lon,
                speed=record.speed,
                point_density=round(self.increment_s / max(float(dt), self.increment_s), 2),
            )

            interp_time += self.increment_s

    def resample_records(self, records: list[Record]) -> Iterator[ResampledRecord]:
        """Resample the records of one vessel and day onto the grid.

        Parameters
        ----------
        records: list of records that has been sorted by time and uniquified

        Yields:
        ------
        ResampledRecord
        """
        date = records[0].timestamp.date()
        assert records[-1].timestamp.date() == date, (records[0].timestamp, records[1].timestamp)
        beg_dt = max(
            dtime.datetime.combine(date, dtime.time.min, tzinfo=dtime.timezone.utc),
            records[0].timestamp - dtime.timedelta(seconds=self.max_gap_s),
        )
        end_dt = min(
            dtime.datetime.combine(date, dtime.time.max, tzinfo=dtime.timezone.utc),
            records[-1].timestamp + dtime.timedelta(seconds=self.max_gap_s),
        )

        beg_interp = self.round_to_increment(records[0].timestamp, rounder=math.ceil)
        end_interp = self.round_to_increment(records[-1].timestamp, rounder=math.floor)
        beg_extrap = self.round_to_increment(beg_dt, rounder=math.ceil)
        end_extrap = self.round_to_increment(end_dt, rounder=math.floor)

        if self.extrapolate:
            for rcd in self._extrapolate(records[0], beg_extrap, beg_interp - self.increment_s):
                yield rcd

        for rcd in self._interpolate(records, beg_interp, end_interp):
            yield rcd

        if self.extrapolate:
            for rcd in self._extrapolate(records[-1], end_interp + self.increment_s, end_extrap):
                yield rcd

    def resample(self, item: tuple[str, list[Record]]) -> Iterator[ResampledRecord]:
        """Resample the records of one vessel and day onto the grid."""
        _key, records = item
        yield from self.resample_records(records)

    def expand(self, xs: PCollection) -> PCollection:
        """Resample the records of each vessel and day onto the grid."""
        return (
            xs
            # This is slightly less accurate than just by id,
            # but makes but makes behavior consistent
            # across daily and longer runs and protects against hot keys on long runs.
            | GroupByIdAndDate()
            | SortByTime()
            | FlatMap(self.resample).with_output_types(Any)
        )
