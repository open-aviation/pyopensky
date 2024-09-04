from __future__ import annotations

import logging
import time
from datetime import timedelta
from json import JSONDecodeError
from typing import Any, cast

import httpx

import pandas as pd

from .api import HasBounds
from .config import password, username
from .time import timelike, to_datetime

_log = logging.getLogger(__name__)


class REST:
    """Wrapper to OpenSky REST API.

    Credentials are fetched from the configuration file.
    REST API is documented here: https://opensky-network.org/apidoc/rest.html

    All methods return standard structures. When calls are made from the traffic
    library, they return advanced structures.

    """

    # All Impala specific functions are implemented in opensky_impala.py

    _json_columns = (
        "icao24",
        "callsign",
        "origin_country",
        "last_position",
        "timestamp",
        "longitude",
        "latitude",
        "altitude",
        "onground",
        "groundspeed",
        "track",
        "vertical_rate",
        "sensors",
        "geoaltitude",
        "squawk",
        "spi",
        "position_source",
    )

    def __init__(self) -> None:
        self.username = username
        self.password = password
        self.auth = cast(tuple[str, str], (username, password))

        self.client = httpx.Client()

    def get(self, query: str, retry: int = 5) -> Any:
        c = self.client.get(query, auth=self.auth)
        try:
            if limit := c.headers.get("X-Rate-Limit-Remaining", None):
                limit = int(limit)
                if limit < 100:
                    _log.warning(f"Rate limiting: Only {limit} calls remaining")
            c.raise_for_status()
            return c.json()
        except httpx.HTTPStatusError:
            if c.status_code == 503 and retry > 0:
                retry = retry - 1
                _log.warning(
                    "Error 503 (Service unavailable): "
                    f"Retrying in 1 second... {retry} more time before failing"
                )
                time.sleep(1)
                return self.get(query, retry=retry)
            if c.status_code == 429:
                retry_after = c.headers.get(
                    "X-Rate-Limit-Retry-After-Seconds", None
                )
                if retry_after is not None:
                    s = int(retry_after)
                    _log.warning(
                        "Error 429 (Too many requests): "
                        f"Retry after {s} seconds"
                    )
                    time.sleep(s)
                    return self.get(query, retry=retry)
            raise
        except JSONDecodeError:
            _log.warning(c.content)
            raise

    async def async_get(
        self, client: httpx.AsyncClient, query: str, retry: int = 5
    ) -> pd.DataFrame:
        c = await client.get(query, auth=self.auth)
        try:
            if limit := c.headers.get("X-Rate-Limit-Remaining", None):
                limit = int(limit)
                if limit < 100:
                    _log.warning(f"Rate limiting: Only {limit} calls remaining")
            c.raise_for_status()
            return c.json()
        except httpx.HTTPStatusError:
            if c.status_code == 503 and retry > 0:
                retry = retry - 1
                _log.warning(
                    "Error 503 (Service unavailable): "
                    f"Retrying in 1 second... {retry} more time before failing"
                )
                time.sleep(1)
                return await self.async_get(client, query, retry=retry)
            if c.status_code == 429:
                retry_after = c.headers.get(
                    "X-Rate-Limit-Retry-After-Seconds", None
                )
                if retry_after is not None:
                    s = int(retry_after)
                    _log.warning(
                        "Error 429 (Too many requests): "
                        f"Retry after {s} seconds"
                    )
                    time.sleep(s)
                    return await self.async_get(client, query, retry=retry)
            raise

    def states(
        self,
        own: bool = False,
        bounds: None
        | str
        | HasBounds
        | tuple[float, float, float, float] = None,
        retry: int = 5,
    ) -> pd.DataFrame:
        """Returns the current state vectors from OpenSky REST API.

        If own parameter is set to True, returns only the state vectors
        associated to own sensors (requires authentication)

        bounds parameter can be a shape or a tuple of float.

        Official documentation
        ----------------------

        Limitations for anonymous (unauthenticated) users

        Anonymous are those users who access the API without using credentials.
        The limitations for anonymous users are:

        Anonymous users can only get the most recent state vectors, i.e. the
        time parameter will be ignored.  Anonymous users can only retrieve data
        with a time resolution of 10 seconds. That means, the API will return
        state vectors for time now - (now mod 10)

        Limitations for OpenSky users

        An OpenSky user is anybody who uses a valid OpenSky account (see below)
        to access the API. The rate limitations for OpenSky users are:

        - OpenSky users can retrieve data of up to 1 hour in the past. If the
        time parameter has a value t < now-3600 the API will return
        400 Bad Request.

        - OpenSky users can retrieve data with a time resolution of 5 seconds.
        That means, if the time parameter was set to t , the API will return
        state vectors for time t-(t mod 5).

        """

        what = "own" if (own and self.auth is not None) else "all"

        if bounds is not None:
            if isinstance(bounds, str):
                from cartes.osm import Nominatim

                bounds = cast(HasBounds, Nominatim.search(bounds))
                if bounds is None:
                    raise RuntimeError(f"'{bounds}' not found on Nominatim")

            if hasattr(bounds, "bounds"):
                # thinking of shapely bounds attribute (in this order)
                # I just don't want to add the shapely dependency here
                west, south, east, north = getattr(bounds, "bounds")
            else:
                assert isinstance(bounds, tuple)
                west, south, east, north = bounds

            what += f"?lamin={south}&lamax={north}&lomin={west}&lomax={east}"

        json = self.get(f"https://opensky-network.org/api/states/{what}")
        columns: list[str] = list(self._json_columns)

        # For some reason, OpenSky may return 18 fields instead of 17
        if len(json["states"]) > 0:
            if len(json["states"][0]) > len(self._json_columns):
                columns.append("_")
        r = pd.DataFrame.from_records(
            json["states"], columns=columns
        ).convert_dtypes(dtype_backend="pyarrow")

        return r.assign(
            timestamp=lambda df: pd.to_datetime(
                df.timestamp, utc=True, unit="s"
            ),
            last_position=lambda df: pd.to_datetime(
                df.last_position, utc=True, unit="s"
            ),
            callsign=lambda df: df.callsign.str.strip(),
        )

    def tracks(self, icao24: str, ts: None | timelike = None) -> pd.DataFrame:
        """Returns a Flight corresponding to a given aircraft.

        Official documentation
        ----------------------

        Retrieve the trajectory for a certain aircraft at a given time. The
        trajectory is a list of waypoints containing position, barometric
        altitude, true track and an on-ground flag.

        In contrast to state vectors, trajectories do not contain all
        information we have about the flight, but rather show the aircraft`s
        general movement pattern. For this reason, waypoints are selected among
        available state vectors given the following set of rules:

        - The first point is set immediately after the the aircraft`s expected
        departure, or after the network received the first position when the
        aircraft entered its reception range.
        - The last point is set right before the aircraft`s expected arrival, or
        the aircraft left the networks reception range.
        - There is a waypoint at least every 15 minutes when the aircraft is
        in-flight.
        - A waypoint is added if the aircraft changes its track more than 2.5°.
        - A waypoint is added if the aircraft changes altitude by more than 100m
        (~330ft).
        - A waypoint is added if the on-ground state changes.

        Tracks are strongly related to flights. Internally, we compute flights
        and tracks within the same processing step. As such, it may be
        beneficial to retrieve a list of flights with the API methods from
        above, and use these results with the given time stamps to retrieve
        detailed track information.

        """
        ts_int = int(to_datetime(ts).timestamp()) if ts is not None else 0
        json = self.get(
            f"https://opensky-network.org/api/tracks/"
            f"?icao24={icao24}&time={ts_int}"
        )

        df = (
            pd.DataFrame.from_records(
                json["path"],
                columns=[
                    "timestamp",
                    "latitude",
                    "longitude",
                    "altitude",
                    "track",
                    "onground",
                ],
            )
            .assign(
                timestamp=lambda df: pd.to_datetime(
                    df.timestamp, utc=True, unit="s"
                ),
                icao24=json["icao24"],
                callsign=json["callsign"],
            )
            .convert_dtypes(dtype_backend="pyarrow")
        )

        return df

    def routes(self, callsign: str) -> tuple[str, str]:
        """Returns the route associated to a callsign."""
        json = self.get(
            f"https://opensky-network.org/api/routes?callsign={callsign}"
        )

        return tuple(json["route"])

    def aircraft(
        self,
        icao24: str,
        begin: None | timelike = None,
        end: None | timelike = None,
    ) -> pd.DataFrame:
        """Returns a flight table associated to an aircraft.

        Official documentation
        ----------------------

        This API call retrieves flights for a particular aircraft within a
        certain time interval. Resulting flights departed and arrived within
        [begin, end]. If no flights are found for the given period, HTTP stats
        404 - Not found is returned with an empty response body.

        """

        if begin is None:
            begin_ts = pd.Timestamp("now", tz="utc").floor("1d")
        else:
            begin_ts = to_datetime(begin)

        if end is None:
            end_ts = begin_ts + pd.Timedelta("1d")
        else:
            end_ts = to_datetime(end)

        json = self.get(
            f"https://opensky-network.org/api/flights/aircraft"
            f"?icao24={icao24}&begin={begin_ts.timestamp():.0f}&"
            f"end={end_ts.timestamp():.0f}"
        )
        return (
            pd.DataFrame.from_records(json)
            .convert_dtypes(dtype_backend="pyarrow")[
                [
                    "firstSeen",
                    "lastSeen",
                    "icao24",
                    "callsign",
                    "estDepartureAirport",
                    "estArrivalAirport",
                ]
            ]
            .assign(
                firstSeen=lambda df: pd.to_datetime(
                    df.firstSeen * 1e9
                ).dt.tz_localize("utc"),
                lastSeen=lambda df: pd.to_datetime(
                    df.lastSeen * 1e9
                ).dt.tz_localize("utc"),
            )
            .sort_values("lastSeen")
        )

    def sensors(self, day: None | timelike = None) -> set[str]:
        """The set of sensors serials you own (require authentication)."""
        today = pd.Timestamp("now", tz="utc").floor("1d")
        if day is not None:
            today = to_datetime(day)
        json = self.get(
            f"https://opensky-network.org/api/sensor/myStats"
            f"?days={today.timestamp():.0f}",
        )
        try:
            return set(json[0]["stats"].keys())
        except JSONDecodeError:
            return set()

    def range(self, serial: str, day: None | timelike = None) -> Any:
        """Wraps a polygon representing a sensor's range.

        By default, returns the current range. Otherwise, you may enter a
        specific day (as a string, as an epoch or as a datetime)
        """

        if day is None:
            day = pd.Timestamp("now", tz="utc").floor("1d")
        day_ts = to_datetime(day)

        return self.get(
            f"https://opensky-network.org/api/range/days?"
            f"days={day_ts.timestamp():.0f}&serials={serial}"
        )

    def global_coverage(self, day: None | timelike = None) -> Any:
        if day is None:
            day = pd.Timestamp("now", tz="utc").floor("1d")
        day_ts = to_datetime(day)

        return self.client.get(
            f"https://opensky-network.org/api/range/coverage?"
            f"day={day_ts.timestamp():.0f}"
        )

    def arrival(
        self,
        airport: str,
        begin: None | timelike = None,
        end: None | timelike = None,
    ) -> pd.DataFrame:
        """Returns a flight table associated to an airport.

        By default, returns the current table. Otherwise, you may enter a
        specific date (as a string, as an epoch or as a datetime)

        Official documentation
        ----------------------

        Retrieve flights for a certain airport which arrived within a given time
        interval [begin, end]. If no flights are found for the given period,
        HTTP stats 404 - Not found is returned with an empty response body.

        """

        if begin is None:
            begin = pd.Timestamp("now", tz="utc").floor("1d")
        begin_ts = to_datetime(begin)
        if end is None:
            end_ts = begin_ts + pd.Timedelta("1d")
        else:
            end_ts = to_datetime(end)

        json = self.get(
            f"https://opensky-network.org/api/flights/arrival"
            f"?begin={begin_ts.timestamp():.0f}&airport={airport}&"
            f"end={end_ts.timestamp():.0f}"
        )

        return (
            pd.DataFrame.from_records(json)
            .convert_dtypes(dtype_backend="pyarrow")[
                [
                    "firstSeen",
                    "lastSeen",
                    "icao24",
                    "callsign",
                    "estDepartureAirport",
                    "estArrivalAirport",
                ]
            ]
            .query("callsign == callsign")
            .assign(
                firstSeen=lambda df: pd.to_datetime(
                    df.firstSeen, utc=True, unit="s"
                ),
                lastSeen=lambda df: pd.to_datetime(
                    df.lastSeen, utc=True, unit="s"
                ),
                callsign=lambda df: df.callsign.str.strip(),
            )
            .sort_values("lastSeen")
        )

    def departure(
        self,
        airport: str,
        begin: None | timelike = None,
        end: None | timelike = None,
    ) -> pd.DataFrame:
        """Returns a flight table associated to an airport.

        By default, returns the current table. Otherwise, you may enter a
        specific date (as a string, as an epoch or as a datetime)

        Official documentation
        ----------------------

        Retrieve flights for a certain airport which departed within a given
        time interval [begin, end]. If no flights are found for the given
        period, HTTP stats 404 - Not found is returned with an empty response
        body.

        """

        if begin is None:
            begin = pd.Timestamp("now", tz="utc").floor("1d")
        begin_ts = to_datetime(begin)
        if end is None:
            end_ts = begin_ts + timedelta(days=1)
        else:
            end_ts = to_datetime(end)

        json = self.get(
            f"https://opensky-network.org/api/flights/departure"
            f"?begin={begin_ts.timestamp():.0f}&airport={airport}&"
            f"end={end_ts.timestamp():.0f}"
        )

        return (
            pd.DataFrame.from_records(json)
            .convert_dtypes(dtype_backend="pyarrow")[
                [
                    "firstSeen",
                    "lastSeen",
                    "icao24",
                    "callsign",
                    "estDepartureAirport",
                    "estArrivalAirport",
                ]
            ]
            .query("callsign == callsign")
            .assign(
                firstSeen=lambda df: pd.to_datetime(
                    df.firstSeen, utc=True, unit="s"
                ),
                lastSeen=lambda df: pd.to_datetime(
                    df.lastSeen, utc=True, unit="s"
                ),
                callsign=lambda df: df.callsign.str.strip(),
            )
            .sort_values("firstSeen")
        )
