"""rs1090 decoder for ADS-B messages.

This module provides a decoder implementation using the rs1090 library
(Rust-based).
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import pandas as pd

from .base import Decoder

if TYPE_CHECKING:
    import pandas as pd


if sys.version_info < (3, 12):
    from typing import Iterator, Tuple, TypeVar

    T = TypeVar("T", covariant=True)

    def batched(
        iterable: Iterator[Tuple[T, ...]], n: int = 1
    ) -> Iterator[Tuple[T, ...]]:
        length = len(iterable)  # type: ignore
        for ndx in range(0, length, n):
            yield iterable[ndx : min(ndx + n, length)]  # type: ignore

else:
    from itertools import batched


class Rs1090Decoder(Decoder):
    """Decoder for ADS-B messages using rs1090 library.

    This decoder implements a Rust-based decoder for improved performance.
    rs1090 is a fast, Rust-based ADS-B message decoder.

    **Example usage**::

        from pyopensky.decoders import Rs1090Decoder
        from pyopensky.trino import Trino
        from pyopensky.rebuild import Rebuild

        trino = Trino()
        rebuild = Rebuild(trino)
        decoder = Rs1090Decoder()

        # Redecode position data
        pos = rebuild.redecode_position(
            start="2023-01-03 16:00",
            stop="2023-01-03 20:00",
            icao24="400A0E",
            decoder=decoder
        )

        # Or rebuild complete state vectors
        data = rebuild.rebuild(
            start="2023-01-03 16:00",
            stop="2023-01-03 20:00",
            icao24="400A0E",
            decoder=decoder
        )
    """

    def __init__(self) -> None:
        """Initialize Rs1090Decoder and import rs1090 library."""
        try:
            import rs1090

            self.rs1090 = rs1090
        except ImportError as e:
            raise ImportError(
                "rs1090 is required for Rs1090Decoder. "
                "Install it with: pip install 'pyopensky[rs1090]'"
            ) from e

    def decode(self, df: pd.DataFrame) -> pd.DataFrame:
        decoded = self.rs1090.decode(df["rawmsg"], df["mintime"])

        # 5000 is (empirically) the fastest batch size I found
        df = pd.concat(
            pd.DataFrame.from_records(d) for d in batched(decoded, 5000)
        )
        df = df.assign(
            timestamp=pd.to_datetime(df.timestamp, unit="s", utc=True)
        )
        return df

    def decode_position(self, df: pd.DataFrame) -> pd.DataFrame:
        """Decode position data from raw ADS-B messages.

        :param df: DataFrame with raw position data
        :return: DataFrame with decoded positions
        """

        return self.decode(df)

    def decode_velocity(self, df: pd.DataFrame) -> pd.DataFrame:
        """Decode velocity data from raw ADS-B messages.

        Decoding is not necessary as there are no easy mistakes to fix.

        :param df: DataFrame with raw velocity data
        :return: DataFrame with decoded velocity
        """
        return df

    def decode_identification(self, df: pd.DataFrame) -> pd.DataFrame:
        """Decode identification (callsign) data from raw ADS-B messages.

        Decoding is not necessary as there are no easy mistakes to fix.

        :param df: DataFrame with raw identification data
        :return: DataFrame with decoded callsign
        """
        return df

    def decode_rollcall(self, df: pd.DataFrame) -> pd.DataFrame:
        """Decode rollcall data from raw Mode S messages.

        Implement rollcall decoding using rs1090.

        :param df: DataFrame with raw rollcall data
        :return: DataFrame with decoded rollcall data
        """

        # TODO implement smarter discarding of wrong BDS messages
        return self.decode(df)
