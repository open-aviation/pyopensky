from datetime import datetime, timedelta
from numbers import Real
from typing import Any, Iterator, Tuple, Union

import pandas as pd

timelike = Union[str, Real, datetime, pd.Timestamp]
deltalike = Union[None, str, Real, timedelta, pd.Timedelta]

time_or_delta = Union[timelike, timedelta]
timetuple = Tuple[datetime, datetime, datetime, datetime]


def to_timedelta(delta: deltalike, **kwargs: Any) -> pd.Timedelta:
    if isinstance(delta, Real):
        delta = pd.Timedelta(seconds=float(delta))
    elif isinstance(delta, (str, timedelta)):
        delta = pd.Timedelta(delta)
    elif delta is None:
        delta = pd.Timedelta(**kwargs)
    return delta


def to_datetime(time: timelike) -> pd.Timestamp:
    if isinstance(time, str):
        time = pd.Timestamp(time, tz="utc")
    elif isinstance(time, datetime):
        time = pd.to_datetime(time, utc=True)
    elif isinstance(time, Real):
        time = pd.Timestamp(float(time), unit="s", tz="utc")
    return time


def split_times(
    before: datetime,
    after: datetime,
    by: timedelta = timedelta(hours=1),
) -> Iterator[timetuple]:
    seq = pd.date_range(
        to_datetime(before).floor(by),
        to_datetime(after).ceil(by),
        freq=by,
    )

    for bh, ah in zip(seq[:-1], seq[1:]):
        yield (before, after, bh, ah)
