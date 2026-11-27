"""pyModeS decoder for ADS-B messages.

This module provides a decoder implementation using the pyModeS library.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .base import Decoder

if TYPE_CHECKING:
    import pandas as pd


# Match the pipe's state lifetime and finalize short encounters before expiry.
_ENCOUNTER_GAP_SECONDS = 300.0


class PyModesDecoder(Decoder):
    """Decoder for ADS-B messages using pyModeS library.

    This decoder implements CPR position decoding with odd/even frame pairing
    and validation using reference positions.

    **Example usage**::

        from pyopensky.decoders import PyModesDecoder
        from pyopensky.trino import Trino
        from pyopensky.rebuild import Rebuild

        trino = Trino()
        rebuild = Rebuild(trino)
        decoder = PyModesDecoder()

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
        """Initialize PyModesDecoder and import pyModeS library."""
        try:
            from pyModeS import PipeDecoder
        except ImportError as e:
            raise ImportError(
                "PyModesDecoder requires pyModeS v3. "
                "Install it with: pip install 'pyopensky[pymodes]'"
            ) from e
        self._pipe_decoder = PipeDecoder

    def decode_position(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return positions accepted by the v3 pipe in capture-time order."""
        from pyModeS import DecodeError, Message

        frames = df.dropna(
            subset=["rawmsg", "timestamp", "icao24"]
        ).sort_values(["icao24", "timestamp"], kind="stable")
        pipe = self._pipe_decoder(eviction_ttl=_ENCOUNTER_GAP_SECONDS)
        previous_icao = None
        previous_time = 0.0
        indices = []
        results = []
        for index, (rawmsg, timestamp, icao24) in enumerate(
            frames[["rawmsg", "timestamp", "icao24"]].itertuples(
                index=False, name=None
            )
        ):
            try:
                message = Message(rawmsg)
            except DecodeError:
                continue
            # A corrupt frame can spoil the CPR pair for a valid neighbour.
            if not message.crc_valid or message.icao.lower() != icao24.lower():
                continue
            capture_time = timestamp.timestamp()
            if previous_icao is not None and (
                message.icao != previous_icao
                or capture_time - previous_time > _ENCOUNTER_GAP_SECONDS
            ):
                pipe.flush()
                pipe.reset()
            indices.append(index)
            results.append(pipe.decode(rawmsg, timestamp=capture_time))
            previous_icao = message.icao
            previous_time = capture_time

        # Pairing and the final bootstrap pass update earlier results in place.
        pipe.flush()
        return (
            frames.iloc[indices]
            .assign(
                lat=[result.get("latitude") for result in results],
                lon=[result.get("longitude") for result in results],
                altitude=[result.get("altitude") for result in results],
            )
            .dropna(subset=["lat", "lon"])
            .sort_values("timestamp", kind="stable")
        )

    def decode_velocity(self, df: pd.DataFrame) -> pd.DataFrame:
        """Decode velocity data from raw messages.

        Decoding is not necessary as there are no easy mistakes to fix.

        :param df: DataFrame with raw velocity data
        :return: DataFrame with decoded velocity (currently returns as-is)
        """
        # Velocity data is already decoded in the database
        # Could add additional validation or decoding here
        return df

    def decode_identification(self, df: pd.DataFrame) -> pd.DataFrame:
        """Decode identification (callsign) data from raw messages.

        Decoding is not necessary as there are no easy mistakes to fix.

        :param df: DataFrame with raw identification data
        :return: DataFrame with decoded callsign (currently returns as-is)
        """
        # Identification data is already decoded in the database
        return df

    def decode_rollcall(self, df: pd.DataFrame) -> pd.DataFrame:
        """Decode rollcall data from raw messages.

        TODO

        This should decode Mode S rollcall replies including:
        - Mode A/C: squawk codes and altitude
        - BDS 5,0 (Track and Turn Report): track angle, roll angle, track rate
        - BDS 6,0 (Heading and Speed Report): magnetic heading, IAS, Mach, TAS
        - more BDS registers can be added as needed

        :param df: DataFrame with raw rollcall data
        :return: DataFrame with decoded rollcall data including BDS fields
        """
        return df
