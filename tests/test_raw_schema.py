"""Raw-model regressions against an independent production-schema snapshot.

The expected schemas come from minio.information_schema.columns, not from
SQLAlchemy models. Building fixtures from those same models would conceal
missing columns in a live table.
"""

from typing import Any, Iterator

import pytest
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Float,
    Integer,
    MetaData,
    Select,
    SmallInteger,
    String,
    Table,
    TextClause,
    create_engine,
    select,
)
from trino.sqlalchemy.dialect import TrinoDialect

import pandas as pd
from pyopensky.schema import (
    AcasData4,
    AddressString,
    AllcallRepliesData4,
    FlightsData4,
    SensorsType,
    UTCTimestampFloat,
    UTCTimestampInteger,
)
from pyopensky.time import to_datetime
from pyopensky.trino import Trino

SENSORS = "array(row(serial integer, mintime double, maxtime double))"
COMMON = {
    "sensors": SENSORS,
    "rawmsg": "varchar",
    "mintime": "double",
    "maxtime": "double",
    "msgcount": "bigint",
    "icao24": "varchar",
}
SCHEMAS = {
    "acas_data4": {
        **COMMON,
        "islongformat": "boolean",
        "isairborne": "boolean",
        "hascrosslinkcapability": "boolean",
        "sensitivitylevel": "smallint",
        "replyinformation": "smallint",
        "altitude": "double",
        "hasvalidrac": "boolean",
        "activeresolutionadvisories": "smallint",
        "resolutionadvisorycomplement": "smallint",
        "nopassbelow": "boolean",
        "nopassabove": "boolean",
        "noturnleft": "boolean",
        "noturnright": "boolean",
        "hasterminated": "boolean",
        "hasmultiplethreats": "boolean",
        "hour": "integer",
    },
    "allcall_replies_data4": {
        **COMMON,
        "capabilities": "smallint",
        "interrogatorid": "smallint",
        "hour": "integer",
    },
}
TYPES = {
    "bigint": BigInteger,
    "boolean": Boolean,
    "double": Float,
    "integer": Integer,
    "smallint": SmallInteger,
    "varchar": String,
}
MODELS = [AcasData4, AllcallRepliesData4]
START = "2026-11-01"
STOP = "2026-11-02"


@pytest.mark.parametrize("model", MODELS)
def test_raw_model_matches_production_snapshot(model: Any) -> None:
    expected = SCHEMAS[model.__tablename__]
    assert list(model.__table__.columns.keys()) == list(expected)
    for column in model.__table__.columns:
        special = {
            "sensors": SensorsType,
            "icao24": AddressString,
            "mintime": UTCTimestampFloat,
            "maxtime": UTCTimestampFloat,
            "hour": UTCTimestampInteger,
        }
        kind = special.get(column.name, TYPES.get(expected[column.name]))
        assert kind is not None
        assert isinstance(column.type, kind), column.name


class OfflineTrino(Trino):
    def __init__(self) -> None:
        super().__init__()
        self.database = create_engine("sqlite://").execution_options(
            compiled_cache=None
        )
        self.statements: list[Select[Any]] = []

    def query(
        self,
        query: str | TextClause | Select[Any],
        cached: bool = True,
        compress: bool = False,
    ) -> pd.DataFrame:
        assert isinstance(query, Select)
        self.statements.append(query)
        with self.database.connect() as connection:
            result = connection.execute(query)
            return pd.DataFrame(result.fetchall(), columns=list(result.keys()))


@pytest.fixture(params=MODELS)
def raw_table(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[OfflineTrino, Any]]:
    def no_network(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Schema regressions must not connect to OpenSky")

    monkeypatch.setattr(Trino, "connect", no_network)
    model = request.param
    schema = SCHEMAS[model.__tablename__]
    client = OfflineTrino()
    metadata = MetaData()
    # Independent DDL deliberately does not use model.__table__.columns.
    raw = Table(
        model.__tablename__,
        metadata,
        *(
            Column(name, TYPES.get(kind, String))
            for name, kind in schema.items()
        ),
    )
    flights = Table(
        FlightsData4.__tablename__,
        metadata,
        *(
            Column(
                column.name,
                Integer
                if isinstance(column.type, (Integer, UTCTimestampInteger))
                else String,
            )
            for column in FlightsData4.__table__.columns
        ),
    )
    metadata.create_all(client.database)
    epoch = int(to_datetime(START).timestamp())
    rows = []
    for index in range(2):
        row: dict[str, Any] = {
            name: True if kind == "boolean" else 3
            for name, kind in schema.items()
            if name not in COMMON and name != "hour"
        }
        row.update(
            sensors=None,
            rawmsg=f"{index:028x}",
            mintime=epoch + 200.125 + index,
            maxtime=epoch + 200.5 + index,
            msgcount=2**35,
            icao24="abcdef",
            hour=epoch,
        )
        if model is AcasData4:
            row.update(
                hasvalidrac=True if index == 0 else None,
                sensitivitylevel=5,
                altitude=3048.0 if index == 0 else None,
            )
        else:
            row.update(capabilities=5 - index, interrogatorid=17)
        rows.append(row)
    with client.database.begin() as connection:
        connection.execute(raw.insert(), rows)
        connection.execute(
            flights.insert(),
            {
                "icao24": "abcdef",
                "callsign": "TEST    ",
                "firstseen": epoch + 100,
                "lastseen": epoch + 900,
                "day": epoch,
                "estdepartureairport": "LFPG",
                "estarrivalairport": "EHAM",
            },
        )
    try:
        yield client, model
    finally:
        client.database.dispose()


def test_full_projection_uses_real_columns(raw_table: Any) -> None:
    client, model = raw_table
    frame = client.query(select(model))
    assert list(frame.columns) == list(SCHEMAS[model.__tablename__])
    assert len(frame) == 2
    assert frame.msgcount.tolist() == [2**35, 2**35]
    assert frame.mintime.iloc[0] % 1 == 0.125
    assert frame.sensors.tolist() == [[], []]
    if model is AcasData4:
        assert frame.hasvalidrac.iloc[0] is True
        assert pd.isna(frame.hasvalidrac.iloc[1])
        assert frame.altitude.iloc[0] == 3048.0
        assert pd.isna(frame.altitude.iloc[1])
    else:
        assert frame.capabilities.tolist() == [5, 4]
        assert frame.interrogatorid.tolist() == [17, 17]


@pytest.mark.parametrize("airport", [None, "LFPG"])
def test_rawdata_filters_payload_with_or_without_flight_join(
    raw_table: Any, airport: str | None
) -> None:
    client, model = raw_table
    if model is AcasData4:
        condition = model.hasvalidrac.is_(True)
        extra = (model.sensitivitylevel, model.activeresolutionadvisories)
    else:
        condition = model.capabilities == 5
        extra = (model.capabilities, model.interrogatorid)
    result = client.rawdata(
        START,
        STOP,
        condition,
        airport=airport,
        icao24="ABCDEF",
        Table=model,
        extra_columns=extra,
        cached=False,
    )
    assert result is not None
    assert len(result) == 1
    assert result.icao24.tolist() == ["abcdef"]
    assert all(column.key in result.columns for column in extra)
    if airport is not None:
        assert list(result.columns) == list(SCHEMAS[model.__tablename__])
    statement = client.statements[-1]
    sql = str(statement.compile(dialect=TrinoDialect()))
    assert f"{model.__tablename__}.hour >=" in sql
    assert f"{model.__tablename__}.hour <" in sql
    assert len(statement.get_final_froms()) == 1
