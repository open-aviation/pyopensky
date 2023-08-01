from __future__ import annotations

import functools
import hashlib
import logging
import time
from multiprocessing.pool import ThreadPool
from operator import or_
from typing import Any, Iterable, Type, TypedDict, cast

import jwt
import requests
from sqlalchemy import (
    Connection,
    CursorResult,
    Engine,
    Select,
    TextClause,
    create_engine,
    func,
    select,
)
from sqlalchemy.orm import aliased, join
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.expression import text
from tqdm import tqdm
from trino.auth import JWTAuthentication, OAuth2Authentication
from trino.sqlalchemy import URL

import pandas as pd

from .api import HasBounds, OpenSkyDBAPI
from .config import cache_path, password, username
from .schema import (
    FlightsData4,
    FlightsData5,
    RawTable,
    RollcallRepliesData4,
    StateVectorsData4,
)
from .time import timelike, to_datetime

_log = logging.getLogger(__name__)


class Token(TypedDict):
    access_token: str
    iat: int
    exp: int


class Trino(OpenSkyDBAPI):
    _token: None | Token = None

    def token(self, **kwargs: Any) -> None | str:
        if username is None or password is None:
            _log.warn(
                "No credentials provided, "
                "falling back to browser authentication"
            )
            self._token = None
            return None

        # take a little margin (one minute)
        now = pd.Timestamp("now", tz="utc") - pd.Timedelta("1 min")
        if self._token is not None and self._token["exp"] < now.timestamp():
            _log.info(f"Token still valid until {self._token['exp']}")
            return self._token["access_token"]

        _log.info("Requesting authentication token")
        result = requests.post(
            "https://auth.opensky-network.org/auth/realms/"
            "opensky-network/protocol/openid-connect/token",
            data={
                "client_id": "trino-client",
                "grant_type": "password",
                "username": username,
                "password": password,
            },
            **kwargs,
        )
        result.raise_for_status()
        payload = result.json()
        self._token = {  # type: ignore
            **payload,  # type: ignore
            **jwt.decode(
                payload["access_token"],
                options={"verify_signature": False},
            ),
        }
        return payload["access_token"]  # type: ignore

    def engine(self) -> Engine:
        token = self.token()
        engine = create_engine(
            URL(
                "trino.opensky-network.org",
                port=443,
                user=username,
                catalog="minio",
                schema="osky",
            ),
            connect_args=dict(
                auth=JWTAuthentication(token)
                if token is not None
                else OAuth2Authentication(),
                http_scheme="https",
            ),
        )
        return engine

    def connect(self) -> Connection:
        return self.engine().connect()

    def query(
        self,
        query: str | TextClause | Select[Any],
        cached: bool = True,
        compress: bool = False,
    ) -> pd.DataFrame:
        exec_kw = dict(stream_results=True)
        if isinstance(query, str):
            query = text(query)

        query_str = f"{(s := query.compile())}\n{s.params}"
        digest = hashlib.md5(query_str.encode("utf8")).hexdigest()
        suffix = ".parquet.gz" if compress else ".parquet"
        if (cache_file := (cache_path / digest).with_suffix(suffix)).exists():
            if cached:
                _log.info(f"Reading results from {cache_file}")
                return pd.read_parquet(cache_file)
            else:
                cache_file.unlink(missing_ok=True)

        with self.connect().execution_options(**exec_kw) as connect:
            # There are steps here, that will not appear in the progress bar
            # but that you can check on https://trino.opensky-network.org/ui/
            res = pd.concat(self.process_result(connect.execute(query)))

        if cached:
            _log.info(f"Saving results to {cache_file}")
            res.to_parquet(cache_file)

        return res

    def process_result(
        self,
        res: CursorResult[Any],
        batch_size: int = 50_000,
    ) -> Iterable[pd.DataFrame]:
        pool = ThreadPool(processes=1)
        async_result = pool.apply_async(res.fetchmany, (batch_size,))
        percentage = 0

        with tqdm(unit="%", unit_scale=True) as processing_bar:
            while not async_result.ready():
                if res.cursor is not None:
                    processing_bar.set_description(res.cursor.stats["state"])
                    new_percentage = res.cursor.stats["progressPercentage"]
                    increment = new_percentage - percentage
                    percentage = new_percentage
                    processing_bar.update(increment)

                    time.sleep(0.1)

            if res.cursor is not None:
                new_percentage = res.cursor.stats["progressPercentage"]
                increment = new_percentage - percentage
                percentage = new_percentage
                processing_bar.set_description(res.cursor.stats["state"])

        with tqdm(
            unit="lines", unit_scale=True, desc="DOWNLOAD"
        ) as download_bar:
            sequence_rows = async_result.get()
            download_bar.update(len(sequence_rows))
            yield pd.DataFrame.from_records(sequence_rows, columns=res.keys())

            while len(sequence_rows) == batch_size:
                sequence_rows = res.fetchmany(batch_size)
                download_bar.update(len(sequence_rows))
                yield pd.DataFrame.from_records(
                    sequence_rows, columns=res.keys()
                )

    ## Specific queries

    def stmt_where_str(
        self,
        stmt: Select[Any],
        value: None | str | list[str],
        *attr: InstrumentedAttribute[str],
    ) -> Select[Any]:
        if len(attr) == 0:
            return stmt
        if isinstance(value, str):
            if value.find("%") >= 0 or value.find("_") >= 0:
                like = functools.reduce(or_, (a.like(value) for a in attr))
                stmt = stmt.where(like)
            else:
                equal = functools.reduce(or_, (a == value for a in attr))
                stmt = stmt.where(equal)
        elif isinstance(value, Iterable):
            is_in = functools.reduce(or_, (a.in_(list(value)) for a in attr))
            stmt = stmt.where(is_in)
        return stmt

    def flightlist(
        self,
        start: timelike,
        stop: None | timelike = None,
        *args: Any,  # more reasonable to be explicit about arguments
        departure_airport: None | str | list[str] = None,
        arrival_airport: None | str | list[str] = None,
        airport: None | str | list[str] = None,
        callsign: None | str | list[str] = None,
        icao24: None | str | list[str] = None,
        cached: bool = True,
        compress: bool = False,
        limit: None | int = None,
        extra_columns: None | list[Any] = None,
        Table: Type[FlightsData4] | Type[FlightsData5] = FlightsData4,
        **kwargs: Any,
    ) -> None | pd.DataFrame:
        """Lists flights departing or arriving at a given airport.

        You may pass requests based on time ranges, callsigns, aircraft, areas,
        serial numbers for receivers, or airports of departure or arrival.

        The method builds appropriate SQL requests, caches results and formats
        data into a proper pandas DataFrame. Requests are split by hour (by
        default) in case the connection fails.

        :param start: a string (default to UTC), epoch or datetime (native
            Python or pandas)
        :param stop: a string (default to UTC), epoch or datetime (native Python
            or pandas), *by default, one day after start*

        More arguments to filter resulting data:

        :param departure_airport: a string for the ICAO identifier of the
            airport. Selects flights departing from the airport between the two
            timestamps;
        :param arrival_airport: a string for the ICAO identifier of the airport.
            Selects flights arriving at the airport between the two timestamps;
        :param airport: a string for the ICAO identifier of the airport. Selects
            flights departing from or arriving at the airport between the two
            timestamps;
        :param callsign: a string or a list of strings (wildcards
            accepted, _ for any character, % for any sequence of characters);
        :param icao24: a string or a list of strings identifying the transponder
            code of the aircraft;

        .. warning::

            - If both departure_airport and arrival_airport are set, requested
              timestamps match the arrival time;
            - If airport is set, ``departure_airport`` and ``arrival_airport``
              cannot be specified (a RuntimeException is raised).

        **Useful options for debug**

        :param cached: (default: True) switch to False to force a new request to
            the database regardless of the cached files. This option also
            deletes previous cache files;
        :param compress: (default: False) compress cache files. Reduces disk
            space occupied at the expense of slightly increased time
            to load.
        :param limit: maximum number of records requested, LIMIT keyword in SQL.

        """

        start_ts = to_datetime(start)
        stop_ts = (
            to_datetime(stop)
            if stop is not None
            else start_ts + pd.Timedelta("1d")
        )

        stmt = select(Table).with_only_columns(  # type: ignore
            Table.icao24,
            Table.firstseen,
            Table.estdepartureairport,
            Table.lastseen,
            Table.estarrivalairport,
            Table.callsign,
            Table.day,
            *(extra_columns if extra_columns is not None else []),
        )

        stmt = self.stmt_where_str(stmt, icao24, Table.icao24)  # type: ignore
        stmt = self.stmt_where_str(
            stmt,
            callsign,
            Table.callsign,  # type: ignore
        )
        stmt = self.stmt_where_str(
            stmt,
            departure_airport,
            Table.estdepartureairport,  # type: ignore
        )
        stmt = self.stmt_where_str(
            stmt,
            arrival_airport,
            Table.estarrivalairport,  # type: ignore
        )
        if airport is not None and arrival_airport is not None:
            raise RuntimeError("airport may not be set if arrival_airport is")
        if airport is not None and departure_airport is not None:
            raise RuntimeError("airport may not be set if departure_airport is")
        stmt = self.stmt_where_str(
            stmt,
            airport,
            Table.estdepartureairport,  # type: ignore
            Table.estarrivalairport,  # type: ignore
        )

        if departure_airport is not None:
            stmt = stmt.where(
                Table.firstseen >= start_ts,
                Table.firstseen <= stop_ts,
                Table.day >= start_ts.floor("1d"),
                Table.day < stop_ts.ceil("1d"),
            )
        else:
            stmt = stmt.where(
                Table.lastseen >= start_ts,
                Table.lastseen <= stop_ts,
                Table.day >= start_ts.floor("1d"),
                Table.day < stop_ts.ceil("1d"),
            )

        if limit is not None:
            stmt = stmt.limit(limit)

        res = self.query(stmt, cached=cached, compress=compress)

        if res.shape[0] == 0:
            return None

        return res.rename(
            columns=dict(
                estarrivalairport="arrival",
                estdepartureairport="departure",
            )
        )

    def history(
        self,
        start: timelike,
        stop: None | timelike = None,
        *args: Any,
        # date_delta: timedelta = timedelta(hours=1),
        callsign: None | str | list[str] = None,
        icao24: None | str | list[str] = None,
        serials: None | int | Iterable[int] = None,
        bounds: None
        | str
        | HasBounds
        | tuple[float, float, float, float] = None,
        departure_airport: None | str = None,
        arrival_airport: None | str = None,
        airport: None | str = None,
        cached: bool = True,
        compress: bool = False,
        limit: None | int = None,
        **kwargs: Any,
    ) -> None | pd.DataFrame:
        """Get Traffic from the OpenSky Trino database.

        You may pass requests based on time ranges, callsigns, aircraft, areas,
        serial numbers for receivers, or airports of departure or arrival.

        The method builds appropriate SQL requests, caches results and formats
        data into a proper pandas DataFrame. Requests are split by hour (by
        default) in case the connection fails.

        :param start: a string (default to UTC), epoch or datetime (native
            Python or pandas)
        :param stop: a string (default to UTC), epoch or datetime (native Python
            or pandas), *by default, one day after start*
        :param date_delta: a timedelta representing how to split the requests,
            *by default: per hour*

        More arguments to filter resulting data:

        :param callsign: a string or a list of strings (wildcards
            accepted, _ for any character, % for any sequence of characters);
        :param icao24: a string or a list of strings identifying the transponder
            code of the aircraft;
        :param serials: an integer or a list of integers identifying the sensors
            receiving the data;
        :param bounds: sets a geographical footprint. Either an **airspace or
            shapely shape** (requires the bounds attribute); or a **tuple of
            float** (west, south, east, north);

        **Airports**

        The following options build more complicated requests by merging
        information from two tables in the Trino database, resp.
        ``state_vectors_data4`` and ``flights_data4``.

        :param departure_airport: a string for the ICAO identifier of the
            airport. Selects flights departing from the airport between the two
            timestamps;
        :param arrival_airport: a string for the ICAO identifier of the airport.
            Selects flights arriving at the airport between the two timestamps;
        :param airport: a string for the ICAO identifier of the airport. Selects
            flights departing from or arriving at the airport between the two
            timestamps;

        .. warning::

            - See :meth:`pyopensky.trino.flightlist` if you do not need any
              trajectory information.
            - If both departure_airport and arrival_airport are set, requested
              timestamps match the arrival time;
            - If airport is set, departure_airport and arrival_airport cannot be
              specified (a RuntimeException is raised).

        **Useful options for debug**

        :param cached: (default: True) switch to False to force a new request to
            the database regardless of the cached files. This option also
            deletes previous cache files;
        :param compress: (default: False) compress cache files. Reduces disk
            space occupied at the expense of slightly increased time
            to load.
        :param limit: maximum number of records requested, LIMIT keyword in SQL.

        """

        start_ts = to_datetime(start)
        stop_ts = (
            to_datetime(stop)
            if stop is not None
            else start_ts + pd.Timedelta("1d")
        )

        airports_params = [airport, departure_airport, arrival_airport]
        count_airports_params = sum(x is not None for x in airports_params)

        if count_airports_params == 0:
            stmt = select(StateVectorsData4)
        else:
            flight_table = (
                select(FlightsData4)
                .with_only_columns(
                    FlightsData4.icao24,
                    FlightsData4.callsign,
                    FlightsData4.firstseen,
                    FlightsData4.lastseen,
                    FlightsData4.estdepartureairport,
                    FlightsData4.estarrivalairport,
                )
                .where(
                    FlightsData4.day >= start_ts.floor("1d"),
                    FlightsData4.day <= start_ts.ceil("1d"),
                )
            )

            flight_table = self.stmt_where_str(
                flight_table, icao24, FlightsData4.icao24
            )
            flight_table = self.stmt_where_str(
                flight_table, callsign, FlightsData4.callsign
            )
            flight_table = self.stmt_where_str(
                flight_table,
                departure_airport,
                FlightsData4.estdepartureairport,
            )
            flight_table = self.stmt_where_str(
                flight_table,
                arrival_airport,
                FlightsData4.estarrivalairport,
            )
            if airport is not None and arrival_airport is not None:
                raise RuntimeError(
                    "airport may not be set if arrival_airport is"
                )
            if airport is not None and departure_airport is not None:
                raise RuntimeError(
                    "airport may not be set if departure_airport is"
                )
            flight_table = self.stmt_where_str(
                flight_table,
                airport,
                FlightsData4.estdepartureairport,
                FlightsData4.estarrivalairport,
            )

            flight_query = flight_table.subquery()
            fd4 = aliased(FlightsData4, alias=flight_query, adapt_on_names=True)
            stmt = select(
                join(
                    StateVectorsData4,
                    flight_query,
                    (fd4.icao24 == StateVectorsData4.icao24)
                    & (fd4.callsign == StateVectorsData4.callsign),
                )
            ).where(
                StateVectorsData4.time >= fd4.firstseen,
                StateVectorsData4.time <= fd4.lastseen,
            )

        stmt = self.stmt_where_str(stmt, icao24, StateVectorsData4.icao24)
        stmt = self.stmt_where_str(stmt, callsign, StateVectorsData4.callsign)

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
                west, south, east, north = bounds

            stmt = stmt.where(
                StateVectorsData4.lon >= west,
                StateVectorsData4.lon <= east,
                StateVectorsData4.lat >= south,
                StateVectorsData4.lat <= north,
            )

        if serials is not None:
            if isinstance(serials, int):
                stmt = stmt.where(
                    func.contains(StateVectorsData4.serials, serials)
                )
            else:
                stmt = stmt.where(
                    func.arrays_overlap(
                        StateVectorsData4.serials, list(serials)
                    )
                )

        for condition in args:
            stmt = stmt.where(condition)

        stmt = stmt.where(
            StateVectorsData4.time >= start_ts,
            StateVectorsData4.time <= stop_ts,
            StateVectorsData4.hour >= start_ts.floor("1H"),
            StateVectorsData4.hour < stop_ts.ceil("1H"),
        )

        if limit is not None:
            stmt = stmt.limit(limit)

        res = self.query(stmt, cached=cached, compress=compress)

        if res.shape[0] == 0:
            return None

        return res

    def rawdata(
        self,
        start: timelike,
        stop: None | timelike = None,
        *args: Any,  # more reasonable to be explicit about arguments
        icao24: None | str | list[str] = None,
        serials: None | int | Iterable[int] = None,
        bounds: None | HasBounds | tuple[float, float, float, float] = None,
        callsign: None | str | list[str] = None,
        departure_airport: None | str = None,
        arrival_airport: None | str = None,
        airport: None | str = None,
        cached: bool = True,
        compress: bool = False,
        limit: None | int = None,
        Table: Type[RawTable] = RollcallRepliesData4,
        **kwargs: Any,
    ) -> None | pd.DataFrame:
        """Get raw message from the OpenSky Trino database.

        You may pass requests based on time ranges, callsigns, aircraft, areas,
        serial numbers for receivers, or airports of departure or arrival.

        The method builds appropriate SQL requests, caches results and formats
        data into a proper pandas DataFrame. Requests are split by hour (by
        default) in case the connection fails.


        :param start: a string (default to UTC), epoch or datetime (native
            Python or pandas)
        :param stop: a string (default to UTC), epoch or datetime (native Python
            or pandas), *by default, one day after start*
        :param date_delta: a timedelta representing how to split the requests,
            *by default: per hour*

        More arguments to filter resulting data:

        :param callsign: a string or a list of strings (wildcards
            accepted, _ for any character, % for any sequence of characters);
        :param icao24: a string or a list of strings identifying the transponder
            code of the aircraft;
        :param serials: an integer or a list of integers identifying the sensors
            receiving the data;
        :param bounds: sets a geographical footprint. Either an **airspace or
            shapely shape** (requires the bounds attribute); or a **tuple of
            float** (west, south, east, north);

        **Airports**

        The following options build more complicated requests by merging
        information from two tables in the Trino database, resp.
        ``rollcall_replies_data4`` and ``flights_data4``.

        :param departure_airport: a string for the ICAO identifier of the
            airport. Selects flights departing from the airport between the two
            timestamps;
        :param arrival_airport: a string for the ICAO identifier of the airport.
            Selects flights arriving at the airport between the two timestamps;
        :param airport: a string for the ICAO identifier of the airport. Selects
            flights departing from or arriving at the airport between the two
            timestamps;

        .. warning::

            - If both departure_airport and arrival_airport are set, requested
              timestamps match the arrival time;
            - If airport is set, departure_airport and arrival_airport cannot be
              specified (a RuntimeException is raised).
            - It is not possible at the moment to filter both on airports and on
              geographical bounds (help welcome!).

        **Useful options for debug**

        :param cached: (default: True) switch to False to force a new request to
            the database regardless of the cached files. This option also
            deletes previous cache files;
        :param compress: (default: False) compress cache files. Reduces disk
            space occupied at the expense of slightly increased time
            to load.
        :param limit: maximum number of records requested, LIMIT keyword in SQL.

        """

        start_ts = to_datetime(start)
        stop_ts = (
            to_datetime(stop)
            if stop is not None
            else start_ts + pd.Timedelta("1d")
        )

        airports_params = [airport, departure_airport, arrival_airport]
        count_airports_params = sum(x is not None for x in airports_params)
        only_one_valid = [
            count_airports_params > 0,
            bounds is not None,
            callsign is not None,
            # serials is not None,
        ]

        if sum(only_one_valid) > 1:
            raise RuntimeError(
                "Filter on only one among: airports, bounds, callsign"
            )

        stmt = None
        if sum(only_one_valid) == 0:
            stmt = (
                select(Table)
                .with_only_columns(
                    Table.mintime,
                    Table.maxtime,
                    Table.rawmsg,
                    Table.msgcount,
                    Table.icao24,
                    Table.sensors,
                )
                .where(Table.rawmsg.is_not(None))
            )
        elif count_airports_params > 0:
            flight_table = (
                select(FlightsData4)
                .with_only_columns(
                    FlightsData4.icao24,
                    FlightsData4.callsign,
                    FlightsData4.firstseen,
                    FlightsData4.lastseen,
                    FlightsData4.estdepartureairport,
                    FlightsData4.estarrivalairport,
                )
                .where(
                    FlightsData4.day >= start_ts.floor("1d"),
                    FlightsData4.day <= start_ts.ceil("1d"),
                )
            )

            flight_table = self.stmt_where_str(
                flight_table, icao24, FlightsData4.icao24
            )
            flight_table = self.stmt_where_str(
                flight_table, callsign, FlightsData4.callsign
            )
            flight_table = self.stmt_where_str(
                flight_table,
                departure_airport,
                FlightsData4.estdepartureairport,
            )
            flight_table = self.stmt_where_str(
                flight_table,
                arrival_airport,
                FlightsData4.estarrivalairport,
            )
            if airport is not None and arrival_airport is not None:
                raise RuntimeError(
                    "airport may not be set if arrival_airport is"
                )
            if airport is not None and departure_airport is not None:
                raise RuntimeError(
                    "airport may not be set if departure_airport is"
                )
            flight_table = self.stmt_where_str(
                flight_table,
                airport,
                FlightsData4.estdepartureairport,
                FlightsData4.estarrivalairport,
            )
            flight_query = flight_table.subquery()
            fd4 = aliased(FlightsData4, alias=flight_query, adapt_on_names=True)
            stmt = select(
                join(Table, flight_query, (Table.icao24 == fd4.icao24))
            ).where(
                Table.mintime >= fd4.firstseen,
                Table.mintime <= fd4.lastseen,
                Table.rawmsg.is_not(None),
            )
        else:
            flight_table = (
                select(
                    func.min(StateVectorsData4.time).label("firstseen"),
                    func.max(StateVectorsData4.time).label("lastseen"),
                    StateVectorsData4.icao24,
                )
                .group_by(StateVectorsData4.icao24)
                .where(
                    StateVectorsData4.hour >= start_ts.floor("1H"),
                    StateVectorsData4.hour <= stop_ts.ceil("1H"),
                    StateVectorsData4.time >= start_ts,
                    StateVectorsData4.time <= stop_ts,
                )
            )
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
                    west, south, east, north = bounds

                flight_table = flight_table.where(
                    StateVectorsData4.lon >= west,
                    StateVectorsData4.lon <= east,
                    StateVectorsData4.lat >= south,
                    StateVectorsData4.lat <= north,
                )

            if callsign is not None:
                flight_table = self.stmt_where_str(
                    flight_table, callsign, StateVectorsData4.callsign
                )

            flight_query = flight_table.subquery()
            fd4 = aliased(FlightsData4, alias=flight_query, adapt_on_names=True)
            stmt = select(
                join(Table, flight_query, (Table.icao24 == fd4.icao24))
            ).where(
                Table.mintime >= fd4.firstseen,
                Table.mintime <= fd4.lastseen,
                Table.rawmsg.is_not(None),
            )

        stmt = self.stmt_where_str(stmt, icao24, Table.icao24)

        stmt = stmt.where(
            Table.mintime >= start_ts,
            Table.mintime <= stop_ts,
            Table.hour >= start_ts.floor("1H"),
            Table.hour < stop_ts.ceil("1H"),
        )

        if limit is not None:
            stmt = stmt.limit(limit)

        res = self.query(stmt, cached=cached, compress=compress)

        if res.shape[0] == 0:
            return None

        return res