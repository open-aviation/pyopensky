from __future__ import annotations

import functools
import hashlib
import logging
import time
from multiprocessing.pool import ThreadPool
from operator import or_
from typing import Any, Iterable, Mapping, Type, TypedDict, cast

import httpcore
import httpx
import jwt
import stamina
from sqlalchemy import (
    Connection,
    CursorResult,
    Engine,
    Select,
    TextClause,
    and_,
    create_engine,
    func,
    select,
    tuple_,
)
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import aliased
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql import (
    ClauseElement,
    ColumnElement,
    ColumnExpressionArgument,
)
from sqlalchemy.sql.expression import text
from sqlalchemy.sql.selectable import SelectBase, Subquery
from sqlalchemy.sql.util import ClauseAdapter
from sqlalchemy.sql.visitors import iterate, replacement_traverse
from tqdm import tqdm
from trino.auth import JWTAuthentication, OAuth2Authentication
from trino.exceptions import TrinoQueryError
from trino.sqlalchemy import URL

import pandas as pd

from .api import HasBounds, OpenSkyDBAPI
from .config import cache_path, trino_password, trino_username
from .schema import (
    AcasData4,
    AllcallRepliesData4,
    FlarmRaw,
    FlightsData4,
    FlightsData5,
    IdentificationData4,
    OperationalStatusData4,
    PositionData4,
    RawTable,
    RollcallRepliesData4,
    StateVectorsData4,
    VelocityData4,
)
from .time import timelike, to_datetime

_log = logging.getLogger(__name__)

limit_resources_msg = """
*********************************************************************

You have hit the limit of your available queries.
(2 concurrent queries + 2 queued queries)

If you want to monitor your running queries, connect to
https://trino.opensky-network.org/ui/

- filter the queries based on your user id
- kill the irrelevant queries (click on the query id then press kill)
- try running your code again

*********************************************************************
"""


class Token(TypedDict):
    access_token: str
    preferred_username: str
    iat: int
    exp: int


def _adapt_flight_conditions(
    conditions: ColumnElement[bool], flight_query: Subquery
) -> ColumnElement[bool]:
    # An independent SELECT keeps its own flight table and time window.
    subqueries = [
        element
        for element in iterate(conditions)
        if isinstance(element, SelectBase)
    ]
    adapter = ClauseAdapter(flight_query)

    def replace(element: ClauseElement, **kwargs: Any) -> ClauseElement | None:
        return adapter.replace(element)

    return replacement_traverse(
        conditions,
        {"stop_on": subqueries},
        replace,
    )


class Trino(OpenSkyDBAPI):
    """Wrapper to OpenSky Trino database

    Credentials are fetched from the configuration file.

    All methods return standard structures. When calls are made from the traffic
    library, they return advanced structures.

    :param session_properties: optional Trino session property names and values,
        applied to every connection opened by this client. For example,
        ``{"join_distribution_type": "BROADCAST"}``. By default, use the
        server's session settings.
    """

    _token: Token | None = None

    def __init__(
        self, *, session_properties: Mapping[str, str] | None = None
    ) -> None:
        self.session_properties = dict(session_properties or {})

    def token(self, **kwargs: Any) -> str | None:
        if trino_username is None or trino_password is None:
            _log.warning(
                "No credentials provided, "
                "falling back to browser authentication"
            )
            self._token = None
            return None

        # take a little margin (one minute)
        now = pd.Timestamp("now", tz="utc") - pd.Timedelta("1 min")
        if self._token is not None and self._token["exp"] > now.timestamp():
            _log.info(f"Token still valid until {self._token['exp']}")
            return self._token["access_token"]

        _log.info("Requesting authentication token")
        result = httpx.post(
            "https://auth.opensky-network.org/auth/realms/"
            "opensky-network/protocol/openid-connect/token",
            data={
                "client_id": "trino-client",
                "grant_type": "password",
                "username": trino_username,
                "password": trino_password,
            },
            **kwargs,
        )

        if result.status_code in [400, 401]:
            msg = "Authentication failing on trino"
            _log.warning(msg)
            return None

        result.raise_for_status()
        payload = result.json()
        self._token = {
            **payload,
            **jwt.decode(
                payload["access_token"],
                options={"verify_signature": False},
            ),
        }
        return payload["access_token"]

    def engine(self) -> Engine:
        token = self.token()
        engine = create_engine(
            URL(
                "trino.opensky-network.org",
                port=443,
                user=trino_username,
                catalog="minio",
                schema="osky",
                source="pyopensky",
            ),
            connect_args=dict(
                auth=JWTAuthentication(token)
                if token is not None
                else OAuth2Authentication(),
                http_scheme="https",
                user=self._token["preferred_username"]
                if self._token is not None
                else None,
                # https://github.com/trinodb/trino-python-client/discussions/456
                legacy_prepared_statements=True,
                session_properties=self.session_properties.copy(),
            ),
        )
        return engine

    @stamina.retry(
        on=(httpx.TransportError, httpcore.TimeoutException, OSError),
        attempts=5,
        wait_initial=1.0,
        wait_max=10.0,
    )
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

        s = query.compile()
        sql_raw = str(s)
        params_raw = str(s.params)
        _log.debug("cache-key sql   repr: %r", sql_raw)
        _log.debug("cache-key params repr: %r", params_raw)
        query_str = f"{sql_raw}\n{params_raw}"
        query_str = query_str.replace("\r\n", "\n").replace("\r", "\n")
        if self.session_properties:
            properties = sorted(self.session_properties.items())
            query_str += f"\nsession_properties={properties}"
        _log.debug("cache-key final repr: %r", query_str)
        _log.info(f"Processing query {query_str}")

        digest = hashlib.md5(query_str.encode("utf8")).hexdigest()
        _log.debug("cache-key digest: %s", digest)
        suffix = ".parquet.gz" if compress else ".parquet"
        if (cache_file := (cache_path / digest).with_suffix(suffix)).exists():
            if cached:
                _log.info(f"Reading results from {cache_file}")
                df = pd.read_parquet(cache_file)
                df = df.convert_dtypes(dtype_backend="pyarrow")

                for column in df.select_dtypes(include=["datetime"]):
                    if df[column].dtype.pyarrow_dtype.tz is None:
                        df = df.assign(
                            **{column: df[column].dt.tz_localize("UTC")}
                        )
                return df
            else:
                cache_file.unlink(missing_ok=True)

        with self.connect().execution_options(**exec_kw) as connect:
            # There are steps here, that will not appear in the progress bar
            # but that you can check on https://trino.opensky-network.org/ui/
            try:
                res = connect.execute(query)
            except DBAPIError as exc:
                if isinstance(exc.orig, TrinoQueryError):
                    if exc.orig.error_name == "QUERY_QUEUE_FULL":
                        _log.error(limit_resources_msg)
                        res = exc
                    else:
                        raise exc
                else:
                    raise exc

            # The point of this error is to limit the traceback to a minimum
            # If we raise the exception inside the try/except, we get a really
            # long traceback that could discourage users to read the errors.

            if isinstance(res, DBAPIError):
                raise RuntimeError(str(res.orig))

            chunks = list(self.process_result(res))
            df = pd.concat(chunks) if chunks else pd.DataFrame()

        df = df.convert_dtypes(dtype_backend="pyarrow")
        for column in df.select_dtypes(include=["datetime"]):
            if df[column].dtype.pyarrow_dtype.tz is None:
                df = df.assign(**{column: df[column].dt.tz_localize("UTC")})

        if cached:
            _log.info(f"Saving results to {cache_file}")
            df.to_parquet(cache_file)

        return df

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
        value: str | list[str] | None,
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
        stop: timelike | None = None,
        *args: ColumnExpressionArgument[bool],
        departure_airport: str | list[str] | None = None,
        arrival_airport: str | list[str] | None = None,
        airport: str | list[str] | None = None,
        routes: list[tuple[str, str]] | None = None,
        callsign: str | list[str] | None = None,
        icao24: str | list[str] | None = None,
        cached: bool = True,
        compress: bool = False,
        limit: int | None = None,
        extra_columns: list[Any] | None = None,
        Table: Type[FlightsData4] | Type[FlightsData5] = FlightsData4,
        **kwargs: Any,
    ) -> pd.DataFrame | None:
        """Lists flights departing or arriving at a given airport.

        You may pass requests based on time ranges, callsigns, aircraft, areas,
        serial numbers for receivers, or airports of departure or arrival.

        The method builds appropriate SQL requests, caches results and formats
        data into a proper pandas DataFrame. Each call submits one query for
        the requested time window.

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
        :param routes: exact directed (departure, arrival) ICAO airport pairs.
            Selects any of these pairs, combined with other filters using AND.
            An empty list selects no flights. Wildcards are not expanded;
        :param callsign: a string or a list of strings (wildcards
            accepted, _ for any character, % for any sequence of characters);
        :param icao24: a string or a list of strings identifying the transponder
            code of the aircraft;

        .. warning::

            - If departure_airport is set, timestamps constrain firstseen.
              Otherwise, including routes alone, they constrain lastseen;
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
            else start_ts + pd.Timedelta("1D")
        )

        stmt = select(Table).with_only_columns(
            Table.icao24,
            Table.firstseen,
            Table.estdepartureairport,
            Table.lastseen,
            Table.estarrivalairport,
            Table.callsign,
            Table.day,
            *(extra_columns if extra_columns is not None else []),
        )

        stmt = self.stmt_where_str(stmt, icao24, Table.icao24)
        stmt = self.stmt_where_str(stmt, callsign, Table.callsign)
        stmt = self.stmt_where_str(
            stmt,
            departure_airport,
            Table.estdepartureairport,
        )
        stmt = self.stmt_where_str(
            stmt,
            arrival_airport,
            Table.estarrivalairport,
        )
        if airport is not None and arrival_airport is not None:
            raise RuntimeError("airport may not be set if arrival_airport is")
        if airport is not None and departure_airport is not None:
            raise RuntimeError("airport may not be set if departure_airport is")
        stmt = self.stmt_where_str(
            stmt,
            airport,
            Table.estdepartureairport,
            Table.estarrivalairport,
        )
        if routes is not None:
            stmt = stmt.where(
                tuple_(Table.estdepartureairport, Table.estarrivalairport).in_(
                    routes
                )
            )

        if departure_airport is not None:
            stmt = stmt.where(
                Table.firstseen >= start_ts,
                Table.firstseen <= stop_ts,
                Table.day >= start_ts.floor("1D"),
                Table.day < stop_ts.ceil("1D"),
            )
        else:
            stmt = stmt.where(
                Table.lastseen >= start_ts,
                Table.lastseen <= stop_ts,
                Table.day >= start_ts.floor("1D"),
                Table.day < stop_ts.ceil("1D"),
            )

        for condition in args:
            stmt = stmt.where(condition)

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
        stop: timelike | None = None,
        *args: ColumnExpressionArgument[bool],
        callsign: str | list[str] | None = None,
        icao24: str | list[str] | None = None,
        serials: int | Iterable[int] | None = None,
        bounds: (
            str | HasBounds | tuple[float, float, float, float] | None
        ) = None,
        departure_airport: str | list[str] | None = None,
        arrival_airport: str | list[str] | None = None,
        airport: str | list[str] | None = None,
        routes: list[tuple[str, str]] | None = None,
        time_buffer: str | pd.Timedelta | None = None,
        cached: bool = True,
        compress: bool = False,
        limit: int | None = None,
        selected_columns: tuple[InstrumentedAttribute[Any] | str, ...] = (),
        **kwargs: Any,
    ) -> pd.DataFrame | None:
        """Get Traffic from the OpenSky Trino database.

        You may pass requests based on time ranges, callsigns, aircraft, areas,
        serial numbers for receivers, or airports of departure or arrival.

        The method builds appropriate SQL requests, caches results and formats
        data into a proper pandas DataFrame. Each call submits one query for
        the requested time window.

        :param start: a string (default to UTC), epoch or datetime (native
            Python or pandas)
        :param stop: a string (default to UTC), epoch or datetime (native Python
            or pandas), *by default, one day after start*

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
        :param selected_columns: specify the columns you want to retrieve. When
            empty, use all columns of the StateVectorsData4 table. You may
            specify column names as strings or schema attributes, including
            columns from the FlightsData4 table when a flight join is used.

        **Airports**

        The following options build more complicated requests by merging
        information from two tables in the Trino database, resp.
        ``state_vectors_data4`` and ``flights_data4``.

        :param departure_airport: an ICAO airport identifier or a list of
            identifiers. Filters the estimated departure airport;
        :param arrival_airport: an ICAO airport identifier or a list of
            identifiers. Filters the estimated arrival airport;
        :param airport: an ICAO airport identifier or a list of identifiers.
            Filters either estimated departure or arrival airport;
        :param routes: exact directed (departure, arrival) ICAO airport pairs.
            Selects any of these pairs, combined with other filters using AND.
            An empty list selects no flights. Wildcards are not expanded.
            Unlike separate airport lists, only the listed pairs are selected;
        :param time_buffer: (default: None) time buffer used to extend time
            bounds around each flight's firstseen and lastseen. State vectors
            are still clipped to start and stop. Ignored without a flight join;

        .. warning::

            - See :meth:`pyopensky.trino.flightlist` if you do not need any
              trajectory information.
            - start and stop constrain state-vector timestamps, not flight
              departure or arrival times. Overnight flights can be partial;
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
            else start_ts + pd.Timedelta("1D")
        )

        airports_params = [airport, departure_airport, arrival_airport]
        count_airports_params = sum(x is not None for x in airports_params)
        conditions = and_(*args) if args else None
        flight_conditions = conditions is not None and (
            FlightsData4.__table__
            in select(1).where(conditions).get_final_froms()
        )
        flight_columns = any(
            (
                column.startswith("FlightsData4.")
                or (
                    column in FlightsData4.__table__.columns
                    and column not in StateVectorsData4.__table__.columns
                )
            )
            if isinstance(column, str)
            else column.class_ is FlightsData4
            for column in selected_columns
        )
        flight_query = None
        fd4 = None

        if (
            count_airports_params == 0
            and routes is None
            and not flight_conditions
            and not flight_columns
        ):
            stmt = select(StateVectorsData4)
        else:
            # Keep every flight column available for alias adaptation. The
            # database can prune columns unused by the outer query.
            flight_table = select(FlightsData4).where(
                FlightsData4.day >= start_ts.floor("1D"),
                FlightsData4.day <= stop_ts.ceil("1D"),
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
            if routes is not None:
                flight_table = flight_table.where(
                    tuple_(
                        FlightsData4.estdepartureairport,
                        FlightsData4.estarrivalairport,
                    ).in_(routes)
                )

            flight_query = flight_table.subquery()
            fd4 = aliased(FlightsData4, alias=flight_query, adapt_on_names=True)

            if isinstance(time_buffer, str):
                time_buffer = cast(pd.Timedelta, pd.Timedelta(time_buffer))

            stmt = select(StateVectorsData4).join(
                flight_query,
                (fd4.icao24 == StateVectorsData4.icao24)
                & (fd4.callsign == StateVectorsData4.callsign),
            )

            if time_buffer is None:
                stmt = stmt.where(
                    StateVectorsData4.time >= fd4.firstseen,
                    StateVectorsData4.time <= fd4.lastseen,
                )
            else:
                stmt = stmt.where(
                    StateVectorsData4.time >= (fd4.firstseen - time_buffer),
                    StateVectorsData4.time <= (fd4.lastseen + time_buffer),
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
                assert isinstance(bounds, tuple)
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

        if conditions is not None:
            if flight_query is not None:
                conditions = _adapt_flight_conditions(conditions, flight_query)
            stmt = stmt.where(conditions)

        stmt = stmt.where(
            StateVectorsData4.time >= start_ts,
            StateVectorsData4.time <= stop_ts,
            StateVectorsData4.hour >= start_ts.floor("1h"),
            StateVectorsData4.hour < stop_ts.ceil("1h"),
        )

        def transform_column(col: str) -> InstrumentedAttribute[Any]:
            if "." in col:
                table, column = col.split(".")
                MATCH = {
                    "StateVectorsData4": StateVectorsData4,
                    "FlightsData4": fd4,
                }
                return getattr(MATCH[table], column)
            if candidate := getattr(StateVectorsData4, col, None):
                return candidate
            if fd4 is not None and (candidate := getattr(fd4, col, None)):
                return candidate
            raise ValueError(f"Unknown column: {col}")

        if len(selected_columns) > 0:
            columns = [
                transform_column(col) if isinstance(col, str) else col
                for col in selected_columns
            ]
            if flight_query is not None:
                columns = [
                    ClauseAdapter(flight_query).traverse(
                        col.__clause_element__()
                    )
                    for col in columns
                ]
            stmt = stmt.with_only_columns(*columns)

        if limit is not None:
            stmt = stmt.limit(limit)

        res = self.query(stmt, cached=cached, compress=compress)

        if res.shape[0] == 0:
            return None

        return res

    def flarm(
        self,
        start: timelike,
        stop: timelike | None = None,
        *args: ColumnExpressionArgument[bool],
        sensor_name: str | list[str] | None = None,
        cached: bool = True,
        compress: bool = False,
        limit: int | None = None,
        correct_only: bool = True,
        extra_columns: tuple[InstrumentedAttribute[Any], ...] = (),
        **kwargs: Any,
    ) -> pd.DataFrame | None:
        start_ts = to_datetime(start)
        stop_ts = (
            to_datetime(stop)
            if stop is not None
            else start_ts + pd.Timedelta("1D")
        )
        stmt = select(FlarmRaw).with_only_columns(
            FlarmRaw.sensoraltitude,
            FlarmRaw.sensorlatitude,
            FlarmRaw.sensorlongitude,
            FlarmRaw.sensorname,
            FlarmRaw.timestamp,
            FlarmRaw.timeatserver,
            FlarmRaw.timeatsensor,
            FlarmRaw.rawmessage,
            *extra_columns,
        )
        if correct_only:
            stmt = stmt.where(FlarmRaw.crccorrect)
            stmt = stmt.where(FlarmRaw.rawmessage.is_not(None))

        if sensor_name is not None:
            stmt = self.stmt_where_str(stmt, sensor_name, FlarmRaw.sensorname)

        for condition in args:
            stmt = stmt.where(condition)

        stmt = stmt.where(
            FlarmRaw.timestamp >= start_ts,
            FlarmRaw.timestamp <= stop_ts,
            FlarmRaw.hour >= start_ts.floor("1h"),
            FlarmRaw.hour < stop_ts.ceil("1h"),
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
        stop: timelike | None = None,
        *args: ColumnExpressionArgument[bool],
        icao24: str | list[str] | None = None,
        serials: int | Iterable[int] | None = None,
        bounds: HasBounds | tuple[float, float, float, float] | None = None,
        callsign: str | list[str] | None = None,
        departure_airport: str | list[str] | None = None,
        arrival_airport: str | list[str] | None = None,
        airport: str | list[str] | None = None,
        routes: list[tuple[str, str]] | None = None,
        cached: bool = True,
        compress: bool = False,
        limit: int | None = None,
        Table: Type[RawTable]
        | Type[AcasData4]
        | Type[AllcallRepliesData4]
        | Type[IdentificationData4]
        | Type[OperationalStatusData4]
        | Type[PositionData4]
        | Type[RollcallRepliesData4]
        | Type[VelocityData4] = RollcallRepliesData4,
        extra_columns: tuple[InstrumentedAttribute[Any], ...] = (),
        **kwargs: Any,
    ) -> pd.DataFrame | None:
        """Get raw message from the OpenSky Trino database.

        You may pass requests based on time ranges, callsigns, aircraft, areas,
        serial numbers for receivers, or airports of departure or arrival.

        The method builds appropriate SQL requests, caches results and formats
        data into a proper pandas DataFrame. Each call submits one query for
        the requested time window.


        :param start: a string (default to UTC), epoch or datetime (native
            Python or pandas)
        :param stop: a string (default to UTC), epoch or datetime (native Python
            or pandas), *by default, one day after start*

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

        :param departure_airport: an ICAO airport identifier or a list of
            identifiers. Filters the estimated departure airport;
        :param arrival_airport: an ICAO airport identifier or a list of
            identifiers. Filters the estimated arrival airport;
        :param airport: an ICAO airport identifier or a list of identifiers.
            Filters either estimated departure or arrival airport;
        :param routes: exact directed (departure, arrival) ICAO airport pairs.
            Selects any of these pairs, combined with other airport and icao24
            filters using AND. An empty list selects no flights. Wildcards
            are not expanded;
        :param extra_columns: additional raw table columns, or FlightsData4
            columns from the joined flight table;

        .. warning::

            - start and stop constrain raw-message timestamps, not flight
              departure or arrival times;
            - If airport is set, departure_airport and arrival_airport cannot be
              specified (a RuntimeException is raised).
            - Select only one of flight metadata (airports, routes or flight
              conditions/columns), geographical bounds, or callsign.

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
            else start_ts + pd.Timedelta("1D")
        )

        airports_params = [airport, departure_airport, arrival_airport]
        count_airports_params = sum(x is not None for x in airports_params)
        conditions = and_(*args) if args else None
        flight_conditions = conditions is not None and (
            FlightsData4.__table__
            in select(1).where(conditions).get_final_froms()
        )
        has_flights = (
            count_airports_params > 0
            or routes is not None
            or flight_conditions
            or any(column.class_ is FlightsData4 for column in extra_columns)
        )
        only_one_valid = [
            has_flights,
            bounds is not None,
            callsign is not None,
            # serials is not None,
        ]

        if sum(only_one_valid) > 1:
            raise RuntimeError(
                "Filter on only one among: airports/routes/flight metadata, "
                "bounds, callsign"
            )

        stmt = None
        if sum(only_one_valid) == 0:
            stmt = (
                select(Table)
                .with_only_columns(
                    Table.mintime,
                    Table.rawmsg,
                    Table.icao24,
                    *extra_columns,
                )
                .where(Table.rawmsg.is_not(None))
            )
        elif has_flights:
            flight_table = select(FlightsData4).where(
                FlightsData4.day >= start_ts.floor("1D"),
                FlightsData4.day <= stop_ts.ceil("1D"),
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
            if routes is not None:
                flight_table = flight_table.where(
                    tuple_(
                        FlightsData4.estdepartureairport,
                        FlightsData4.estarrivalairport,
                    ).in_(routes)
                )
            flight_query = flight_table.subquery()
            fd4 = aliased(FlightsData4, alias=flight_query, adapt_on_names=True)
            stmt = (
                select(Table)
                .join(flight_query, (Table.icao24 == fd4.icao24))
                .where(
                    Table.mintime >= fd4.firstseen,
                    Table.mintime <= fd4.lastseen,
                    Table.rawmsg.is_not(None),
                )
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
                    StateVectorsData4.hour >= start_ts.floor("1h"),
                    StateVectorsData4.hour <= stop_ts.ceil("1h"),
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
                    assert isinstance(bounds, tuple)
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
            stmt = (
                select(Table)
                .join(flight_query, (Table.icao24 == fd4.icao24))
                .where(
                    Table.mintime >= fd4.firstseen,
                    Table.mintime <= fd4.lastseen,
                    Table.rawmsg.is_not(None),
                )
            )

        if has_flights:
            adapter = ClauseAdapter(flight_query)
            stmt = stmt.add_columns(
                *(
                    adapter.traverse(column.__clause_element__())
                    for column in extra_columns
                    if column.class_ is FlightsData4
                )
            )
            if conditions is not None:
                conditions = _adapt_flight_conditions(conditions, flight_query)
        if conditions is not None:
            stmt = stmt.where(conditions)

        stmt = self.stmt_where_str(stmt, icao24, Table.icao24)

        stmt = stmt.where(
            Table.mintime >= start_ts,
            Table.mintime <= stop_ts,
            Table.hour >= start_ts.floor("1h"),
            Table.hour < stop_ts.ceil("1h"),
        )

        if limit is not None:
            stmt = stmt.limit(limit)

        res = self.query(stmt, cached=cached, compress=compress)

        if res.shape[0] == 0:
            return None

        return res
