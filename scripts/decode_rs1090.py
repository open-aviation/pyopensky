# ruff: noqa: E741

import sys

import rs1090

import pandas as pd

if sys.version_info < (3, 12):
    from typing import Iterator, Tuple, TypeVar

    T = TypeVar("T", covariant=True)

    def batched(
        iterable: Iterator[Tuple[T, ...]], n: int = 1
    ) -> Iterator[Tuple[T, ...]]:
        l = len(iterable)  # type: ignore
        for ndx in range(0, l, n):
            yield iterable[ndx : min(ndx + n, l)]  # type: ignore

else:
    from itertools import batched


def decode(df: pd.DataFrame) -> pd.DataFrame:
    decoded = rs1090.decode(df["rawmsg"], df["mintime"])

    # 5000 is (empirically) the fastest batch size I found
    df = pd.concat(pd.DataFrame.from_records(d) for d in batched(decoded, 5000))
    df = df.assign(timestamp=pd.to_datetime(df.timestamp, unit="s", utc=True))
    return df
