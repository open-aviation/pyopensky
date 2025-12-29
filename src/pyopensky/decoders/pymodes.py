"""pyModeS decoder for ADS-B messages.

This module provides a decoder implementation using the pyModeS library.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .base import Decoder

if TYPE_CHECKING:
    import pandas as pd


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
            import pyModeS as pms

            self.pms = pms
        except ImportError as e:
            raise ImportError(
                "pyModeS is required for PyModesDecoder. "
                "Install it with: pip install 'pyopensky[pymodes]'"
            ) from e

    def decode_position(self, df: pd.DataFrame) -> pd.DataFrame:
        """Decode position data by pairing odd/even CPR frames.

        This method:
        1. Pairs odd and even CPR frames using merge_asof
        2. Decodes positions using pyModeS
        3. Validates decoded positions using reference positions
        4. Filters outliers

        :param df: DataFrame with raw position data
        :return: DataFrame with decoded positions
        """
        import numpy as np
        import pandas as pd

        # Pair odd and even frames
        pos_odd = df.query("odd")[
            ["timestamp", "icao24", "mintime", "rawmsg", "altitude"]
        ]
        pos_even = df.query("not odd")[
            ["timestamp", "icao24", "mintime", "rawmsg", "altitude"]
        ]

        # Create pairs in both directions
        pos_pairs = (
            pd.concat(
                [
                    pd.merge_asof(
                        pos_odd,
                        pos_even,
                        on="timestamp",
                        by="icao24",
                        tolerance=pd.Timedelta(seconds=10),
                        suffixes=("_odd", "_even"),
                    ),
                    pd.merge_asof(
                        pos_even,
                        pos_odd,
                        on="timestamp",
                        by="icao24",
                        tolerance=pd.Timedelta(seconds=10),
                        suffixes=("_even", "_odd"),
                    ),
                ]
            )
            .sort_values("timestamp")
            .dropna()
        )

        if pos_pairs.shape[0] == 0:
            return df

        # Decode positions
        lats = []
        lons = []
        alts = []

        for _, row in pos_pairs.iterrows():
            try:
                # Determine odd/even messages
                if "rawmsg_odd" in pos_pairs.columns:
                    msg_odd = row.rawmsg_odd
                    msg_even = row.rawmsg_even
                    t_odd = row.mintime_odd
                    t_even = row.mintime_even
                else:
                    msg_even = row.rawmsg_odd
                    msg_odd = row.rawmsg_even
                    t_even = row.mintime_odd
                    t_odd = row.mintime_even

                # Decode position
                latlon = self.pms.adsb.position(
                    msg_odd, msg_even, t_odd, t_even
                )

                if latlon is not None:
                    lats.append(latlon[0])
                    lons.append(latlon[1])
                else:
                    lats.append(None)
                    lons.append(None)

                # Decode altitude
                alt = self.pms.adsb.altitude(msg_odd)
                alts.append(alt)

            except Exception:
                lats.append(None)
                lons.append(None)
                alts.append(None)

        pos_pairs = pos_pairs.assign(lat=lats, lon=lons, altitude=alts)

        # Validate using reference positions
        pos_pairs = pos_pairs.assign(
            lat_ref_1=lambda x: x.lat.shift(5),
            lon_ref_1=lambda x: x.lon.shift(5),
            lat_ref_2=lambda x: x.lat.shift(10),
            lon_ref_2=lambda x: x.lon.shift(10),
        ).dropna(subset=["lat", "lon"])

        # Decode with reference positions for validation
        latlon_1 = []
        latlon_2 = []

        for _, row in pos_pairs.iterrows():
            try:
                if "rawmsg_odd" in pos_pairs.columns:
                    msg = row.rawmsg_odd
                else:
                    msg = row.rawmsg_even

                ll1 = self.pms.adsb.position_with_ref(
                    msg, lat_ref=row.lat_ref_1, lon_ref=row.lon_ref_1
                )
                ll2 = self.pms.adsb.position_with_ref(
                    msg, lat_ref=row.lat_ref_2, lon_ref=row.lon_ref_2
                )
                latlon_1.append(ll1)
                latlon_2.append(ll2)
            except Exception:
                latlon_1.append((None, None))
                latlon_2.append((None, None))

        latlon_1_arr = np.array(latlon_1)
        latlon_2_arr = np.array(latlon_2)

        # Filter outliers based on consistency
        pos_pairs = (
            pos_pairs.assign(
                lat_1=latlon_1_arr[:, 0],
                lon_1=latlon_1_arr[:, 1],
                lat_2=latlon_2_arr[:, 0],
                lon_2=latlon_2_arr[:, 1],
            )
            .dropna()
            .eval("dlat_1=abs(lat-lat_1)")
            .eval("dlon_1=abs(lon-lon_1)")
            .eval("dlat_2=abs(lat-lat_2)")
            .eval("dlon_2=abs(lon-lon_2)")
            .query("dlat_1<0.1 and dlon_1<0.1 and dlat_2<0.1 and dlon_2<0.1")
        )

        return pos_pairs

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
