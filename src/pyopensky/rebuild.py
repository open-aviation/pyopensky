"""State vector reconstruction from raw ADS-B messages.

This module provides the Rebuild class for downloading raw ADS-B data from
multiple tables and reconstructing complete state vectors.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    import pandas as pd

    from .api import HasBounds
    from .trino import Trino


import numpy as np
import pandas as pd

from .decoders.base import Decoder
from .schema import (
    IdentificationData4,
    PositionData4,
    RollcallRepliesData4,
    VelocityData4,
)
from .time import timelike, to_datetime


def _normalize_icao24_dtype(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize icao24 column to object dtype for consistent merging.

    :param df: DataFrame with icao24 column
    :return: DataFrame with icao24 as object dtype
    """
    if df is not None and "icao24" in df.columns:
        df = df.copy()
        df["icao24"] = df["icao24"].astype(str)
    return df


class Rebuild:
    """Class for rebuilding state vectors from raw ADS-B messages.

    This class provides methods to download and decode raw ADS-B messages
    from different data tables and merge them into complete state vectors.

    **Example usage**::

        from pyopensky.trino import Trino
        from pyopensky.rebuild import Rebuild

        trino = Trino()
        rebuild = Rebuild(trino)

        # Without decoding
        data = rebuild.rebuild(
            start="2023-01-03 16:00",
            stop="2023-01-03 20:00",
            icao24="400A0E"
        )

        # With pymodes or rs1090 decoding
        data = rebuild.rebuild(
            start="2023-01-03 16:00",
            stop="2023-01-03 20:00",
            icao24="400A0E",
            decoder="pymodes"  # or "rs1090"
        )

    """

    def __init__(self, trino: Trino) -> None:
        """Initialize Rebuild with a Trino instance.

        :param trino: Trino instance for database queries
        """
        self.trino = trino

    def redecode_position(
        self,
        start: timelike,
        stop: timelike,
        *,
        icao24: None | str | list[str] = None,
        bounds: None | HasBounds | tuple[float, float, float, float] = None,
        cached: bool = True,
        compress: bool = False,
        decoder: None | Decoder = None,
        **kwargs: Any,
    ) -> None | pd.DataFrame:
        """Download and optionally redecode position data from raw messages.

        Downloads position data with raw ADS-B messages, pairs odd/even CPR
        frames, and optionally decodes them using a visitor pattern.

        :param start: start of time range
        :param stop: end of time range
        :param icao24: aircraft transponder code(s)
        :param bounds: geographical footprint
        :param cached: use cached data
        :param compress: compress cache files
        :param decoder: decoder object implementing decode_position(df) method
            that decodes raw messages. If None, returns raw data.

        :return: DataFrame with position data (decoded if visitor provided)

        **Example usage**::

            # Create a pymodes decoder
            from pyopensky.decoders import PyModesDecoder
            decoder = PyModesDecoder()

            # Redecode positions
            pos = rebuild.redecode_position(
                start="2023-01-03 16:00",
                stop="2023-01-03 20:00",
                icao24="400A0E",
                decoder=decoder
            )
        """
        start_ts = to_datetime(start)
        stop_ts = to_datetime(stop)

        if icao24 is None and bounds is None:
            raise ValueError("Either 'icao24' or 'bounds' must be specified")

        # Get icao24s from bounds if needed
        query_icao24 = icao24
        if bounds is not None and icao24 is None:
            pos_sample = self.trino.rawdata(
                start=start_ts,
                stop=stop_ts,
                bounds=bounds,
                Table=PositionData4,
                cached=cached,
                compress=compress,
            )
            if pos_sample is not None and pos_sample.shape[0] > 0:
                query_icao24 = pos_sample["icao24"].unique().tolist()
            else:
                return None

        # Download position data with rawmsg
        pos_data = self.trino.rawdata(
            start=start_ts,
            stop=stop_ts,
            icao24=query_icao24,
            bounds=bounds if icao24 is not None else None,
            Table=PositionData4,
            cached=cached,
            compress=compress,
            extra_columns=(
                PositionData4.lat,
                PositionData4.lon,
                PositionData4.alt,
                PositionData4.groundspeed,
                PositionData4.heading,
                PositionData4.surface,
                PositionData4.nic,
                PositionData4.odd,
            ),
        )

        if pos_data is None or pos_data.shape[0] == 0:
            return None

        # Prepare position data
        pos_data = (
            pos_data.assign(
                timestamp=lambda x: pd.to_datetime(
                    x.mintime, unit="s", utc=True
                )
            )
            .drop_duplicates("rawmsg")
            .rename(
                columns={
                    "surface": "onground",
                    "alt": "altitude",
                    "heading": "track",
                }
            )
            .sort_values("timestamp")
        )

        # Apply decoder if provided
        if decoder is not None:
            pos_data = decoder.decode_position(pos_data)

        if "rawmsg" in pos_data.columns:
            pos_data = pos_data.drop(columns=["rawmsg"])

        return pos_data

    def redecode_velocity(
        self,
        start: timelike,
        stop: timelike,
        *,
        icao24: None | str | list[str] = None,
        cached: bool = True,
        compress: bool = False,
        decoder: None | Decoder = None,
        **kwargs: Any,
    ) -> None | pd.DataFrame:
        """Download and optionally redecode velocity data from raw messages.

        :param decoder: decoder implementing decode_velocity(df) method
        """
        start_ts = to_datetime(start)
        stop_ts = to_datetime(stop)

        if icao24 is None:
            raise ValueError("'icao24' must be specified")

        vel_data = self.trino.rawdata(
            start=start_ts,
            stop=stop_ts,
            icao24=icao24,
            bounds=None,
            Table=VelocityData4,
            cached=cached,
            compress=compress,
            extra_columns=(
                VelocityData4.velocity,
                VelocityData4.heading,
                VelocityData4.vertrate,
                VelocityData4.geominurbaro,
            ),
        )

        if vel_data is None or vel_data.shape[0] == 0:
            return None

        vel_data = (
            vel_data.assign(
                timestamp=lambda x: pd.to_datetime(
                    x.mintime, unit="s", utc=True
                )
            )
            .drop_duplicates("rawmsg")
            .rename(columns={"heading": "track", "vertrate": "vertical_rate"})
            .sort_values("timestamp")
        )

        if decoder is not None:
            vel_data = decoder.decode_velocity(vel_data)

        if "rawmsg" in vel_data.columns:
            vel_data = vel_data.drop(columns=["rawmsg"])

        return vel_data

    def redecode_identification(
        self,
        start: timelike,
        stop: timelike,
        *,
        icao24: None | str | list[str] = None,
        cached: bool = True,
        compress: bool = False,
        decoder: None | Decoder = None,
        **kwargs: Any,
    ) -> None | pd.DataFrame:
        """Download and optionally redecode identification (callsign) data.

        :param decoder: decoder implementing decode_identification(df) method
        """
        start_ts = to_datetime(start)
        stop_ts = to_datetime(stop)

        if icao24 is None:
            raise ValueError("'icao24' must be specified")

        ident_data = self.trino.rawdata(
            start=start_ts,
            stop=stop_ts,
            icao24=icao24,
            bounds=None,
            Table=IdentificationData4,
            cached=cached,
            compress=compress,
            extra_columns=(IdentificationData4.identity,),
        )

        if ident_data is None or ident_data.shape[0] == 0:
            return None

        ident_data = (
            ident_data.assign(
                timestamp=lambda x: pd.to_datetime(
                    x.mintime, unit="s", utc=True
                )
            )
            .rename(columns={"identity": "callsign"})
            .assign(callsign=lambda x: x.callsign.str.replace(" ", ""))
            .sort_values("timestamp")
        )

        if decoder is not None:
            ident_data = decoder.decode_identification(ident_data)

        if "rawmsg" in ident_data.columns:
            ident_data = ident_data.drop(columns=["rawmsg"])

        return ident_data

    def redecode_rollcall(
        self,
        start: timelike,
        stop: timelike,
        *,
        icao24: None | str | list[str] = None,
        cached: bool = True,
        compress: bool = False,
        decoder: None | Decoder = None,
        **kwargs: Any,
    ) -> None | pd.DataFrame:
        """Download and optionally redecode rollcall (squawk) data.

        :param decoder: decoder implementing decode_rollcall(df) method
        """
        start_ts = to_datetime(start)
        stop_ts = to_datetime(stop)

        if icao24 is None:
            raise ValueError("'icao24' must be specified")

        rollcall_data = self.trino.rawdata(
            start=start_ts,
            stop=stop_ts,
            icao24=icao24,
            bounds=None,
            Table=RollcallRepliesData4,
            cached=cached,
            compress=compress,
        )

        if rollcall_data is None or rollcall_data.shape[0] == 0:
            return None

        rollcall_data = rollcall_data.assign(
            timestamp=lambda x: pd.to_datetime(x.mintime, unit="s", utc=True)
        ).sort_values("timestamp")

        if decoder is not None:
            rollcall_data = decoder.decode_rollcall(rollcall_data)

        return rollcall_data

    def rebuild(
        self,
        start: timelike,
        stop: timelike,
        *,
        icao24: None | str | list[str] = None,
        bounds: None | HasBounds | tuple[float, float, float, float] = None,
        cached: bool = True,
        compress: bool = False,
        decoder: None | Decoder | Literal["pymodes", "rs1090"] = None,
        include_rollcall: bool = False,
        **kwargs: Any,
    ) -> None | pd.DataFrame:
        """Rebuild state vectors by downloading and merging all data tables.

        This method downloads data from position, velocity, identification,
        and optionally rollcall tables, then merges them using time-based
        joins to reconstruct complete state vectors.

        :param start: start of time range
        :param stop: end of time range
        :param icao24: aircraft transponder code(s)
        :param bounds: geographical footprint
        :param cached: use cached data
        :param compress: compress cache files
        :param decoder: decoder for decoding raw messages (e.g. PyModesDecoder)
        :param include_rollcall: include rollcall (squawk) data in merge

        :return: DataFrame with merged state vectors

        **Example usage**::

            # Without decoding (use database decoded values)
            data = rebuild.rebuild(
                start="2023-01-03 16:00",
                stop="2023-01-03 20:00",
                icao24="400A0E"
            )

            # With pymodes or rs1090 decoding
            data = rebuild.rebuild(
                start="2023-01-03 16:00",
                stop="2023-01-03 20:00",
                icao24="400A0E",
                decoder="pymodes"  # or "rs1090"
            )
        """
        if isinstance(decoder, str):
            if decoder == "pymodes":
                from .decoders.pymodes import PyModesDecoder

                decoder = PyModesDecoder()
            elif decoder == "rs1090":
                from .decoders.rs1090 import Rs1090Decoder

                decoder = Rs1090Decoder()
            else:
                raise ValueError(
                    f"Unknown decoder string: {decoder}. "
                    "Use 'pymodes' or 'rs1090' or your own instance."
                )

        # Get position data
        pos_data = self.redecode_position(
            start=start,
            stop=stop,
            icao24=icao24,
            bounds=bounds,
            cached=cached,
            compress=compress,
            decoder=decoder,
        )

        if pos_data is None or pos_data.shape[0] == 0:
            return None

        # Normalize icao24 dtype for consistent merging
        pos_data = _normalize_icao24_dtype(pos_data)

        # Get icao24s from position data for other queries
        query_icao24 = pos_data["icao24"].unique().tolist()

        # Get velocity data
        vel_data = self.redecode_velocity(
            start=start,
            stop=stop,
            icao24=query_icao24,
            cached=cached,
            compress=compress,
            decoder=decoder,
        )

        # Merge velocity data
        if vel_data is not None and vel_data.shape[0] > 0:
            vel_data = _normalize_icao24_dtype(vel_data)
            pos_data = pd.merge_asof(
                pos_data,
                vel_data[
                    [
                        "timestamp",
                        "icao24",
                        "velocity",
                        "track",
                        "vertical_rate",
                        "geominurbaro",
                    ]
                ],
                on="timestamp",
                by="icao24",
                direction="nearest",
                tolerance=pd.Timedelta(seconds=5),
                suffixes=("_pos", "_vel"),
            )

            # Combine overlapping columns
            if (
                "track_pos" in pos_data.columns
                and "track_vel" in pos_data.columns
            ):
                track_pos = pos_data["track_pos"]
                track_vel = pos_data["track_vel"]
                pos_data["track"] = np.where(
                    pd.isna(track_pos), track_vel, track_pos
                )
                pos_data = pos_data.drop(
                    columns=["track_pos", "track_vel"], errors="ignore"
                )
            elif "track_pos" in pos_data.columns:
                pos_data["track"] = pos_data["track_pos"]
                pos_data = pos_data.drop(columns=["track_pos"], errors="ignore")

            # Calculate geoaltitude
            if (
                "geominurbaro" in pos_data.columns
                and "altitude" in pos_data.columns
            ):
                pos_data["geoaltitude"] = (
                    pos_data["altitude"] + pos_data["geominurbaro"]
                )

        # Get identification data
        ident_data = self.redecode_identification(
            start=start,
            stop=stop,
            icao24=query_icao24,
            cached=cached,
            compress=compress,
            decoder=decoder,
        )

        # Merge identification data
        if ident_data is not None and ident_data.shape[0] > 0:
            ident_data = _normalize_icao24_dtype(ident_data)
            pos_data = pd.merge_asof(
                pos_data,
                ident_data[["timestamp", "icao24", "callsign"]],
                on="timestamp",
                by="icao24",
                direction="nearest",
                tolerance=pd.Timedelta(seconds=5),
            )

        # Optionally get and merge rollcall data
        rollcall_data = self.redecode_rollcall(
            start=start,
            stop=stop,
            icao24=query_icao24,
            cached=cached,
            compress=compress,
            decoder=decoder,
        )

        if rollcall_data is not None and rollcall_data.shape[0] > 0:
            rollcall_data = _normalize_icao24_dtype(rollcall_data)
            if include_rollcall:
                pos_data = pd.merge_asof(
                    pos_data,
                    rollcall_data,
                    on="timestamp",
                    by="icao24",
                    direction="nearest",
                    tolerance=pd.Timedelta(seconds=5),
                )
            else:
                pos_data = pd.concat([pos_data, rollcall_data], axis=0)

        return pos_data.sort_values(["icao24", "timestamp"])
