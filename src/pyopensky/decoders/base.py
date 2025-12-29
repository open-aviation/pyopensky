"""Abstract base class for ADS-B message decoders.

This module defines the interface that all decoder implementations must follow.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


class Decoder(ABC):
    """Abstract base class for ADS-B message decoders.

    All decoder implementations must inherit from this class and implement
    the four decoding methods for different message types.

    **Example usage**::

        from pyopensky.decoders.base import Decoder
        import pandas as pd

        class MyDecoder(Decoder):
            def decode_position(self, df: pd.DataFrame) -> pd.DataFrame:
                # Custom position decoding logic
                return df

            def decode_velocity(self, df: pd.DataFrame) -> pd.DataFrame:
                # Custom velocity decoding logic
                return df

            def decode_identification(self, df: pd.DataFrame) -> pd.DataFrame:
                # Custom identification decoding logic
                return df

            def decode_rollcall(self, df: pd.DataFrame) -> pd.DataFrame:
                # Custom rollcall decoding logic
                return df
    """

    @abstractmethod
    def decode_position(self, df: pd.DataFrame) -> pd.DataFrame:
        """Decode position data from raw ADS-B messages.

        This method should decode position messages (TC 9-18, 20-22) which
        contain CPR-encoded latitude/longitude, altitude, and onground status.

        :param df: DataFrame with raw position data including 'rawmsg' column
        :return: DataFrame with decoded position data (lat, lon, altitude, etc.)
        """
        ...

    @abstractmethod
    def decode_velocity(self, df: pd.DataFrame) -> pd.DataFrame:
        """Decode velocity data from raw ADS-B messages.

        This method should decode velocity messages (TC 19) which contain
        ground speed, track angle, and vertical rate.

        :param df: DataFrame with raw velocity data including 'rawmsg' column
        :return: DataFrame with decoded velocity data (velocity, track,
            vertical_rate, etc.)
        """
        ...

    @abstractmethod
    def decode_identification(self, df: pd.DataFrame) -> pd.DataFrame:
        """Decode identification (callsign) data from raw ADS-B messages.

        This method should decode identification messages (TC 1-4) which
        contain the aircraft callsign.

        :param df: DataFrame with raw identification data including 'rawmsg'
            column
        :return: DataFrame with decoded identification data (callsign)
        """
        ...

    @abstractmethod
    def decode_rollcall(self, df: pd.DataFrame) -> pd.DataFrame:
        """Decode rollcall data from raw Mode S messages.

        This method should decode Mode S Comm-B rollcall replies including
        BDS registers (e.g., BDS 5,0 for track/turn, BDS 6,0 for heading/speed)
        and Mode A/C replies (squawk codes, altitude).

        :param df: DataFrame with raw rollcall data including 'rawmsg' column
        :return: DataFrame with decoded rollcall data (squawk, BDS fields, etc.)
        """
        ...
