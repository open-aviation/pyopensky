from datetime import datetime, timedelta
from numbers import Real
from typing import Any, Iterator, Tuple, Union, cast

import pandas as pd

timelike = Union[str, Real, datetime, pd.Timestamp]
deltalike = Union[None, str, Real, timedelta, pd.Timedelta]

time_or_delta = Union[timelike, timedelta]
timetuple = Tuple[datetime, datetime, datetime, datetime]


def to_timedelta(delta: deltalike, **kwargs: Any) -> pd.Timedelta:
    if isinstance(delta, Real):
        return cast(pd.Timedelta, pd.Timedelta(seconds=float(delta)))
    if isinstance(delta, (str, timedelta)):
        return cast(pd.Timedelta, pd.Timedelta(delta))
    if delta is None:
        return cast(pd.Timedelta, pd.Timedelta(**kwargs))
    return delta


def to_datetime(time: timelike) -> pd.Timestamp:
    if isinstance(time, str):
        return cast(pd.Timestamp, pd.Timestamp(time, tz="utc"))
    if isinstance(time, datetime):
        return pd.to_datetime(time, utc=True)
    if isinstance(time, Real):
        return cast(pd.Timestamp, pd.Timestamp(float(time), unit="s", tz="utc"))
    return time


def split_times(
    before: datetime,
    after: datetime,
    by: timedelta = timedelta(hours=1),
) -> Iterator[timetuple]:
    seq = pd.date_range(
        to_datetime(before).floor(by),  # ty: ignore[invalid-argument-type]
        to_datetime(after).ceil(by),  # ty: ignore[invalid-argument-type]
        freq=by,
    )

    for bh, ah in zip(seq[:-1], seq[1:]):
        yield (before, after, bh, ah)
