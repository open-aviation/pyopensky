"""ADS-C/MLAT models checked against independent production metadata.

The JSON snapshot records minio.information_schema.columns for the four
readsb tables/views. SQLite fixtures use that snapshot, not the model DDL.
"""

import json
import re
from pathlib import Path
from typing import Any, Iterator

import pytest
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Double,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    select,
)
from trino.sqlalchemy.dialect import TrinoDialect

import pandas as pd
from pyopensky.schema import (
    ReadsbAdscState,
    ReadsbAdscStateVectors,
    ReadsbMlatState,
    ReadsbMlatStateVectors,
    ReadsbPositionType,
    UTCTimestampBigInteger,
)
from pyopensky.time import to_datetime

SCHEMAS = json.loads(
    (Path(__file__).parent / "data" / "readsb_schema.json").read_text()
)
MODELS = [
    ReadsbAdscState,
    ReadsbAdscStateVectors,
    ReadsbMlatState,
    ReadsbMlatStateVectors,
]
TYPES = {
    "bigint": BigInteger,
    "boolean": Boolean,
    "double": Double,
    "integer": Integer,
    "varchar": String,
}
START = "2026-11-01"
STOP = "2026-11-02"


@pytest.mark.parametrize("model", MODELS)
def test_readsb_schema_matches_production_snapshot(model: Any) -> None:
    expected = SCHEMAS[model.__tablename__]
    assert list(model.__table__.columns.keys()) == list(expected)
    for column in model.__table__.columns:
        actual = column.type.compile(dialect=TrinoDialect())
        assert re.sub(r"\s", "", actual.lower()) == re.sub(
            r"\s", "", expected[column.name]
        ), column.name


@pytest.mark.parametrize("model", MODELS)
def test_source_payload_fields_remain_nullable(model: Any) -> None:
    required = {"time", "hex", "icao24", "hour"}
    for column in model.__table__.columns:
        if column.name not in required:
            assert column.nullable, column.name


@pytest.fixture(params=MODELS)
def readsb_table(request: pytest.FixtureRequest) -> Iterator[Any]:
    model = request.param
    expected = SCHEMAS[model.__tablename__]
    engine = create_engine("sqlite://").execution_options(compiled_cache=None)
    table = Table(
        model.__tablename__,
        MetaData(),
        *(
            Column(name, TYPES.get(kind, String))
            for name, kind in expected.items()
        ),
    )
    table.metadata.create_all(engine)
    epoch = int(to_datetime(START).timestamp())
    row = dict.fromkeys(expected)
    row.update(time=epoch + 0.125, lat=50.125, lon=8.5, hour=epoch)
    row["hex" if "hex" in expected else "icao24"] = "abcdef"
    if model is ReadsbAdscState:
        row.update(
            flight="AFR1  ",
            alt_baro_ft=35000,
            gs=450.5,
            baro_rate=-640,
            seen=2.5,
            seen_pos=1.25,
            gps_ok_before=epoch - 100,
            messages=2**40,
            db_flags=2**35,
        )
    elif model is ReadsbAdscStateVectors:
        row.update(
            callsign="AFR1  ",
            velocity=450.5,
            vertrate=-640.0,
            baroaltitude=35000.0,
        )
    elif model is ReadsbMlatState:
        row["messages"] = 2**40
    with engine.begin() as connection:
        connection.execute(table.insert(), row)
    try:
        yield engine, model
    finally:
        engine.dispose()


def test_select_preserves_units_nulls_and_fractional_time(
    readsb_table: Any,
) -> None:
    engine, model = readsb_table
    statement = select(model).where(
        model.time >= START,
        model.time < STOP,
        model.hour >= START,
        model.hour < STOP,
    )
    with engine.connect() as connection:
        result = connection.execute(statement)
        frame = pd.DataFrame(result.fetchall(), columns=list(result.keys()))
    assert list(frame.columns) == list(SCHEMAS[model.__tablename__])
    assert len(frame) == 1
    assert frame.time.iloc[0] == to_datetime(START).timestamp() + 0.125
    assert frame.hour.iloc[0] == to_datetime(START)
    if model is ReadsbAdscState:
        assert frame.alt_baro_ft.iloc[0] == 35000
        assert frame.gs.iloc[0] == 450.5
        assert frame.baro_rate.iloc[0] == -640
        assert frame.flight.iloc[0] == "AFR1  "
        assert frame.seen.iloc[0] == 2.5
        assert frame.seen_pos.iloc[0] == 1.25
        assert frame.last_position.iloc[0] is None
        assert frame.nav_modes.iloc[0] is None
        assert frame.db_flags.iloc[0] == 2**35
    elif model is ReadsbAdscStateVectors:
        assert frame.velocity.iloc[0] == 450.5
        assert frame.vertrate.iloc[0] == -640.0
        assert frame.baroaltitude.iloc[0] == 35000.0
        assert frame.callsign.iloc[0] == "AFR1  "
    else:
        assert "altitude" not in frame.columns
        assert "velocity" not in frame.columns
        assert "callsign" not in frame.columns
    if "messages" in frame.columns:
        assert frame.messages.iloc[0] == 2**40


@pytest.mark.parametrize(
    "model,column",
    [(ReadsbAdscState, "flight"), (ReadsbAdscStateVectors, "callsign")],
)
def test_callsign_filter_does_not_add_padding(model: Any, column: str) -> None:
    statement = select(model).where(getattr(model, column) == "AFR1")
    sql = str(
        statement.compile(
            dialect=TrinoDialect(), compile_kwargs={"literal_binds": True}
        )
    )
    assert f"{column} = 'AFR1'" in sql
    assert "'AFR1    '" not in sql


def test_bigint_partition_timestamp_after_2038() -> None:
    timestamp = to_datetime("2040-01-01")
    kind = UTCTimestampBigInteger()
    dialect = TrinoDialect()
    bound = kind.process_bind_param(timestamp, dialect)
    assert bound > 2**31
    assert kind.process_result_value(bound, dialect) == timestamp


@pytest.mark.parametrize(
    "values", [[50.125, 8.5, 7, 185, 1.25], [None, None, None, None, None]]
)
def test_nested_position_preserves_named_fields(values: list[Any]) -> None:
    names = ["lat", "lon", "nic", "rc", "seen_pos"]
    row = tuple(values)
    assert ReadsbPositionType().process_result_value(
        row, TrinoDialect()
    ) == dict(zip(names, values))


def test_missing_nested_position_stays_none() -> None:
    assert (
        ReadsbPositionType().process_result_value(None, TrinoDialect()) is None
    )


def test_nested_position_survives_parquet_cache(tmp_path: Path) -> None:
    value = ReadsbPositionType().process_result_value(
        (50.125, 8.5, 7, 185, 1.25), TrinoDialect()
    )
    path = tmp_path / "positions.parquet"
    pd.DataFrame({"last_position": [value, None]}).to_parquet(path)
    result = pd.read_parquet(path)
    assert result.last_position.iloc[0] == value
    assert result.last_position.iloc[1] is None
