from __future__ import annotations

import operator
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, List

from sqlalchemy import ARRAY, Integer, String, TypeDecorator
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy.sql.operators import OperatorType
from typing_extensions import Annotated

import pandas as pd


class UTCTimestampInteger(TypeDecorator[pd.Timestamp]):
    """Automatic coercing of UTC timestamps into and from integers.

    In OpenSky databases, timestamps are all encoded as a number of seconds
    since the epoch.
    """

    impl = Integer

    def process_bind_param(
        self, value: str | datetime | pd.Timestamp | None, dialect: Dialect
    ) -> Any:
        if isinstance(value, (str, datetime)):
            value = pd.to_datetime(value, utc=True)
        if isinstance(value, pd.Timestamp):
            return int(value.timestamp())
        return super().process_bind_param(value, dialect)

    def process_result_value(
        self, value: Any | None, dialect: Dialect
    ) -> pd.Timestamp | None:
        if value is None:
            return pd.NaT
        if isinstance(value, (int, float)):
            return pd.to_datetime(value, unit="s", utc=True)
        return super().process_result_value(value, dialect)


class CallsignString8(TypeDecorator[str]):
    """Automatic coercing of callsigns into 8-char left padded strings.

    Callsigns are decoded as 8 character strings, filled with spaces if needed.
    In practice, we trim the remaining spaces at the end of a callsign.

    When binding parameters, callsigns should be fit into 8 characters only when
    tested for equality.
    """

    impl = String

    def coerce_compared_value(self, op: OperatorType | None, value: Any) -> Any:
        if op in (
            operator.eq,  # ==
            operator.ne,  # !=
            sql_operators.eq,
            sql_operators.ne,
            sql_operators.contains,  # .in()
        ):
            return self
        return String()

    def process_bind_param(self, value: str | None, dialect: Dialect) -> Any:
        if isinstance(value, str):
            return value.ljust(8)
        return super().process_bind_param(value, dialect)

    def process_result_value(
        self, value: Any | None, dialect: Dialect
    ) -> Any | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value.rstrip()
        return super().process_result_value(value, dialect)


@dataclass
class TrackRow:
    time: pd.Timestamp
    latitude: float
    longitude: float
    altitude: float
    heading: float
    onground: bool

    def __post_init__(self) -> None:
        if isinstance(self.time, int):
            self.time = pd.Timestamp(self.time, unit="s", tz="utc")


@dataclass
class AirportCandidate:
    icao24: str
    horizdistance: int
    vertdistance: int


class TrackType(TypeDecorator[List[Dict[str, Any]]]):
    impl = ARRAY(String)

    def process_result_value(
        self, value: Any | None, dialect: Dialect
    ) -> List[Dict[str, Any]] | None:
        if isinstance(value, list):
            return list(asdict(TrackRow(*row)) for row in value)[::-1]
        return super().process_result_value(value, dialect)


class AirportCandidateType(TypeDecorator[List[Dict[str, Any]]]):
    impl = ARRAY(String)

    def process_result_value(
        self, value: Any | None, dialect: Dialect
    ) -> List[Dict[str, Any]] | None:
        if value is None:
            return []
        if isinstance(value, list):
            return list(asdict(AirportCandidate(*row)) for row in value)
        return super().process_result_value(value, dialect)


AirportCandidates = List[AirportCandidate]
Track = List[TrackRow]

Callsign = Annotated[str, mapped_column(CallsignString8)]


class Base(DeclarativeBase):
    type_annotation_map = {
        pd.Timestamp: UTCTimestampInteger,
        AirportCandidates: AirportCandidateType,
        Track: TrackType,
        Callsign: CallsignString8,
    }


class StateVectorsData4(Base):
    __tablename__ = "state_vectors_data4"

    time: Mapped[pd.Timestamp]
    icao24: Mapped[str]
    lat: Mapped[float]
    lon: Mapped[float]
    velocity: Mapped[float]
    heading: Mapped[float]
    vertrate: Mapped[float]
    callsign: Mapped[Callsign]
    onground: Mapped[bool]
    alert: Mapped[bool]
    spi: Mapped[bool]
    squawk: Mapped[str]
    baroaltitude: Mapped[float]
    geoaltitude: Mapped[float]
    lastposupdate: Mapped[float]
    lastcontact: Mapped[float]
    serials: Mapped[List[int]] = mapped_column(ARRAY(Integer))  # TODO
    hour: Mapped[pd.Timestamp] = mapped_column(primary_key=True)


class FlightsData4(Base):
    __tablename__ = "flights_data4"

    icao24: Mapped[str]
    firstseen: Mapped[pd.Timestamp]
    estdepartureairport: Mapped[str]
    lastseen: Mapped[pd.Timestamp]
    estarrivalairport: Mapped[str]
    callsign: Mapped[Callsign]
    track: Mapped[Track]
    estdepartureairporthorizdistance: Mapped[int]
    estdepartureairportvertdistance: Mapped[int]
    estarrivalairporthorizdistance: Mapped[int]
    estarrivalairportvertdistance: Mapped[int]
    departureairportcandidatescount: Mapped[int]
    arrivalairportcandidatescount: Mapped[int]
    otherdepartureairportcandidates: Mapped[AirportCandidates]
    otherarrivalairportcandidates: Mapped[AirportCandidates]

    # Whatever, we pick this one as primary key BUT
    #   - this is not true
    #   - we still need one column with a primary key
    day: Mapped[pd.Timestamp] = mapped_column(primary_key=True)


class FlightsData5(Base):
    __tablename__ = "flights_data5"

    icao24: Mapped[str]
    firstseen: Mapped[pd.Timestamp]
    estdepartureairport: Mapped[str]
    lastseen: Mapped[pd.Timestamp]
    estarrivalairport: Mapped[str]
    callsign: Mapped[Callsign]
    track: Mapped[Track]
    estdepartureairporthorizdistance: Mapped[int]
    estdepartureairportvertdistance: Mapped[int]
    estarrivalairporthorizdistance: Mapped[int]
    estarrivalairportvertdistance: Mapped[int]
    departureairportcandidatescount: Mapped[int]
    arrivalairportcandidatescount: Mapped[int]
    otherdepartureairportcandidates: Mapped[AirportCandidates]
    otherarrivalairportcandidates: Mapped[AirportCandidates]

    airportofdeparture: Mapped[str]
    airportofdestination: Mapped[str]
    takeofftime: Mapped[pd.Timestamp]
    takeofflatitude: Mapped[float]
    takeofflongitude: Mapped[float]
    landingtime: Mapped[pd.Timestamp]
    landinglatitude: Mapped[float]
    landinglongitude: Mapped[float]

    # Whatever, we pick this one as primary key BUT
    #   - this is not true
    #   - we still need one column with a primary key
    day: Mapped[pd.Timestamp] = mapped_column(primary_key=True)
