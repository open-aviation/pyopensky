from __future__ import annotations

import operator
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, ClassVar, Dict, List, Protocol

from sqlalchemy import ARRAY, Float, Integer, String, TypeDecorator
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
        if isinstance(value, pd.Timedelta):
            return int(value.total_seconds())
        return super().process_bind_param(value, dialect)

    def process_result_value(
        self, value: Any | None, dialect: Dialect
    ) -> pd.Timestamp | None:
        if value is None:
            return pd.NaT
        if isinstance(value, (int, float)):
            return pd.to_datetime(value, unit="s", utc=True)
        return super().process_result_value(value, dialect)


class UTCTimestampFloat(TypeDecorator[pd.Timestamp]):
    """Automatic coercing of UTC timestamps into and from float.

    In OpenSky databases, timestamps are all encoded as a number of seconds
    since the epoch.

    The data here is not converted back to pd.Timestamp as it conflicts with
    saving to parquet files.
    """

    impl = Float

    def process_bind_param(
        self, value: str | datetime | pd.Timestamp | None, dialect: Dialect
    ) -> Any:
        if isinstance(value, (str, datetime)):
            value = pd.to_datetime(value, utc=True)
        if isinstance(value, pd.Timestamp):
            return float(value.timestamp())
        return super().process_bind_param(value, dialect)


class AddressString(TypeDecorator[str]):
    """Automatic coercing of icao24 addresses into low characters."""

    impl = String

    def process_bind_param(self, value: str | None, dialect: Dialect) -> Any:
        if isinstance(value, str):
            return value.lower()
        return super().process_bind_param(value, dialect)


class CallsignString(TypeDecorator[str]):
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
            sql_operators.in_op,
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
class AirportCandidateRow:
    icao24: Address
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
            return list(asdict(AirportCandidateRow(*row)) for row in value)
        return super().process_result_value(value, dialect)


@dataclass
class SensorRow:
    serial: int
    mintime: float
    maxtime: float


class SensorsType(TypeDecorator[List[Dict[str, Any]]]):
    impl = ARRAY(String)

    def process_result_value(
        self, value: Any | None, dialect: Dialect
    ) -> List[Dict[str, Any]] | None:
        if value is None:
            return []
        if isinstance(value, list):
            return list(asdict(SensorRow(*row)) for row in value)
        return super().process_result_value(value, dialect)


## Descriptions of tables are below
## For each table, indicate its type in Python and on the SQL side
## For some types, the conversion is automatic (see `type_annotation_map`)

AirportCandidates = List[AirportCandidateRow]
Track = List[TrackRow]
Sensors = List[SensorRow]

Callsign = Annotated[str, mapped_column(CallsignString)]
Address = Annotated[str, mapped_column(AddressString)]


class Base(DeclarativeBase):
    type_annotation_map: ClassVar[dict[Any, Any]] = {
        pd.Timestamp: UTCTimestampInteger,
        AirportCandidates: AirportCandidateType,
        Track: TrackType,
        Callsign: CallsignString,
        Address: AddressString,
        Sensors: SensorsType,
    }


class StateVectorsData4(Base):
    __tablename__ = "state_vectors_data4"

    time: Mapped[pd.Timestamp]
    icao24: Mapped[Address]
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

    icao24: Mapped[Address]
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

    icao24: Mapped[Address]
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


class RawTable(Protocol):
    sensors: Mapped[Sensors]
    rawmsg: Mapped[str]
    mintime: Mapped[pd.Timestamp]
    maxtime: Mapped[pd.Timestamp]
    msgcount: Mapped[int]
    icao24: Mapped[Address]

    # Whatever, we pick this one as primary key BUT
    #   - this is not true
    #   - we still need one column with a primary key
    hour: Mapped[pd.Timestamp] = mapped_column(primary_key=True)


class AcasData4(Base):
    __tablename__ = "acas_data4"

    sensors: Mapped[Sensors]
    rawmsg: Mapped[str]
    mintime: Mapped[pd.Timestamp] = mapped_column(UTCTimestampFloat)
    maxtime: Mapped[pd.Timestamp] = mapped_column(UTCTimestampFloat)
    msgcount: Mapped[int]
    icao24: Mapped[Address]
    message: Mapped[str]
    isid: Mapped[bool]
    flightstatus: Mapped[int]
    downlinkrequest: Mapped[int]
    utilitymsg: Mapped[int]
    interrogatorid: Mapped[int]
    identifierdesignator: Mapped[int]
    valuecode: Mapped[int]
    altitude: Mapped[float]
    identity: Mapped[str]

    # Whatever, we pick this one as primary key BUT
    #   - this is not true
    #   - we still need one column with a primary key
    hour: Mapped[pd.Timestamp] = mapped_column(primary_key=True)


class AllcallRepliesData4(Base):
    __tablename__ = "allcall_replies_data4"

    sensors: Mapped[Sensors]
    rawmsg: Mapped[str]
    mintime: Mapped[pd.Timestamp] = mapped_column(UTCTimestampFloat)
    maxtime: Mapped[pd.Timestamp] = mapped_column(UTCTimestampFloat)
    msgcount: Mapped[int]
    icao24: Mapped[Address]
    message: Mapped[str]
    isid: Mapped[bool]
    flightstatus: Mapped[int]
    downlinkrequest: Mapped[int]
    utilitymsg: Mapped[int]
    interrogatorid: Mapped[int]
    identifierdesignator: Mapped[int]
    valuecode: Mapped[int]
    altitude: Mapped[float]
    identity: Mapped[str]

    # Whatever, we pick this one as primary key BUT
    #   - this is not true
    #   - we still need one column with a primary key
    hour: Mapped[pd.Timestamp] = mapped_column(primary_key=True)


class IdentificationData4(Base):
    __tablename__ = "identification_data4"

    sensors: Mapped[Sensors]
    rawmsg: Mapped[str]
    mintime: Mapped[pd.Timestamp] = mapped_column(UTCTimestampFloat)
    maxtime: Mapped[pd.Timestamp] = mapped_column(UTCTimestampFloat)
    msgcount: Mapped[int]
    icao24: Mapped[Address]
    emittercategory: Mapped[int]
    ftc: Mapped[int]
    identity: Mapped[str]

    # Whatever, we pick this one as primary key BUT
    #   - this is not true
    #   - we still need one column with a primary key
    hour: Mapped[pd.Timestamp] = mapped_column(primary_key=True)


class OperationalStatusData4(Base):
    __tablename__ = "operational_status_data4"

    sensors: Mapped[Sensors]
    rawmsg: Mapped[str]
    mintime: Mapped[pd.Timestamp] = mapped_column(UTCTimestampFloat)
    maxtime: Mapped[pd.Timestamp] = mapped_column(UTCTimestampFloat)
    msgcount: Mapped[int]
    icao24: Mapped[str]
    subtypecode: Mapped[int]
    unknowncapcode: Mapped[bool]
    unknownopcode: Mapped[bool]
    hasoperationaltcas: Mapped[int]
    has1090esin: Mapped[bool]
    supportsairreferencedvelocity: Mapped[int]
    haslowtxpower: Mapped[int]
    supportstargetstatereport: Mapped[int]
    supportstargetchangereport: Mapped[int]
    hasuatin: Mapped[bool]
    nacv: Mapped[int]
    nicsupplementc: Mapped[int]
    hastcasresolutionadvisory: Mapped[bool]
    hasactiveidentswitch: Mapped[bool]
    usessingleantenna: Mapped[bool]
    systemdesignassurance: Mapped[int]
    gpsantennaoffset: Mapped[int]
    airplanelength: Mapped[int]
    airplanewidth: Mapped[float]
    version: Mapped[int]
    nicsupplementa: Mapped[bool]
    positionnac: Mapped[float]
    geometricverticalaccuracy: Mapped[int]
    sourceintegritylevel: Mapped[int]
    barometricaltitudeintegritycode: Mapped[int]
    trackheadinginfo: Mapped[int]
    horizontalreferencedirection: Mapped[bool]

    # Whatever, we pick this one as primary key BUT
    #   - this is not true
    #   - we still need one column with a primary key
    hour: Mapped[pd.Timestamp] = mapped_column(primary_key=True)


class PositionData4(Base):
    __tablename__ = "position_data4"

    sensors: Mapped[Sensors]
    rawmsg: Mapped[str]
    mintime: Mapped[pd.Timestamp] = mapped_column(UTCTimestampFloat)
    maxtime: Mapped[pd.Timestamp] = mapped_column(UTCTimestampFloat)
    msgcount: Mapped[int]
    icao24: Mapped[Address]
    nicsuppla: Mapped[bool]
    hcr: Mapped[float]
    nic: Mapped[int]
    survstatus: Mapped[int]
    nicsupplb: Mapped[bool]
    odd: Mapped[bool]
    baroalt: Mapped[bool]
    lat: Mapped[float]
    lon: Mapped[float]
    alt: Mapped[float]
    nicsupplc: Mapped[bool]
    groundspeed: Mapped[float]
    gsresolution: Mapped[float]
    heading: Mapped[float]
    timeflag: Mapped[bool]
    surface: Mapped[bool]

    # Whatever, we pick this one as primary key BUT
    #   - this is not true
    #   - we still need one column with a primary key
    hour: Mapped[pd.Timestamp] = mapped_column(primary_key=True)


class RollcallRepliesData4(Base):
    __tablename__ = "rollcall_replies_data4"

    sensors: Mapped[Sensors]
    rawmsg: Mapped[str]
    mintime: Mapped[pd.Timestamp] = mapped_column(UTCTimestampFloat)
    maxtime: Mapped[pd.Timestamp] = mapped_column(UTCTimestampFloat)
    msgcount: Mapped[int]
    icao24: Mapped[Address]
    message: Mapped[str]
    isid: Mapped[bool]
    flightstatus: Mapped[int]
    downlinkrequest: Mapped[int]
    utilitymsg: Mapped[int]
    interrogatorid: Mapped[int]
    identifierdesignator: Mapped[int]
    valuecode: Mapped[int]
    altitude: Mapped[float]
    identity: Mapped[str]

    # Whatever, we pick this one as primary key BUT
    #   - this is not true
    #   - we still need one column with a primary key
    hour: Mapped[pd.Timestamp] = mapped_column(primary_key=True)


class VelocityData4(Base):
    __tablename__ = "velocity_data4"

    sensors: Mapped[Sensors]
    rawmsg: Mapped[str]
    mintime: Mapped[pd.Timestamp] = mapped_column(UTCTimestampFloat)
    maxtime: Mapped[pd.Timestamp] = mapped_column(UTCTimestampFloat)
    msgcount: Mapped[int]
    icao24: Mapped[Address]
    supersonic: Mapped[bool]
    intentchange: Mapped[bool]
    ifrcapability: Mapped[bool]
    nac: Mapped[int]
    ewvelocity: Mapped[float]
    nsvelocity: Mapped[float]
    baro: Mapped[bool]
    vertrate: Mapped[float]
    geominurbaro: Mapped[float]  # typo confirmed
    heading: Mapped[float]
    velocity: Mapped[float]

    # Whatever, we pick this one as primary key BUT
    #   - this is not true
    #   - we still need one column with a primary key
    hour: Mapped[pd.Timestamp] = mapped_column(primary_key=True)


class FlarmRaw(Base):
    __tablename__ = "flarm_raw"

    sensortype: Mapped[str]
    sensorlatitude: Mapped[float]
    sensorlongitude: Mapped[float]
    sensoraltitude: Mapped[int]
    timeatserver: Mapped[pd.Timestamp] = mapped_column(UTCTimestampFloat)
    timeatsensor: Mapped[pd.Timestamp] = mapped_column(UTCTimestampFloat)
    timestamp: Mapped[pd.Timestamp] = mapped_column(UTCTimestampFloat)
    rawmessage: Mapped[str]
    crc: Mapped[str]
    rawsoftmessage: Mapped[str]
    sensorname: Mapped[str]
    ntperror: Mapped[float]
    userfreqcorrection: Mapped[float]
    autofreqcorrection: Mapped[float]
    frequency: Mapped[float]
    channel: Mapped[int]
    snrdetector: Mapped[float]
    snrdemodulator: Mapped[float]
    typeogn: Mapped[bool]
    crccorrect: Mapped[bool]

    # Whatever, we pick this one as primary key BUT
    #   - this is not true
    #   - we still need one column with a primary key
    hour: Mapped[pd.Timestamp] = mapped_column(primary_key=True)
