import pytest
from sqlalchemy import func, not_, select

import pandas as pd
from pyopensky.schema import FlightsData4, StateVectorsData4
from pyopensky.trino import Trino

trino = Trino()


commercial_callsign = (
    "^([A-Z]{3})[0-9](([0-9]{0,3})|([0-9]{0,2})([A-Z])|([0-9]?)([A-Z]{2}))\\s*$"
)


def test_alive() -> None:
    df = trino.query(
        "select * from state_vectors_data4 limit 5",
        cached=False,
    )
    assert df.shape[0] == 5


def test_query() -> None:
    res = trino.query(
        select(FlightsData4)
        .with_only_columns(
            FlightsData4.icao24,
            FlightsData4.callsign,
            FlightsData4.firstseen,
            FlightsData4.lastseen,
            FlightsData4.estdepartureairport,
            FlightsData4.estarrivalairport,
            FlightsData4.track,
        )
        .where(FlightsData4.callsign.startswith("AFR"))
        .where(FlightsData4.callsign == "AFR291")
        .where(FlightsData4.icao24.like("39%"))
        .where(FlightsData4.day >= "2023-01-01")
        .where(FlightsData4.day < "2023-02-01")
        .where(FlightsData4.estarrivalairport != None)  # noqa: E711
        .order_by(FlightsData4.firstseen)
        .limit(50)
    )
    assert res.shape[0] >= 10
    assert res.callsign.max() == "AFR291"
    assert res.estdepartureairport.max() == "RJBB"
    assert res.firstseen.diff().min() > pd.Timedelta("1 day 12 hours")
    assert res.track.str.len().max() > 600


def test_flightlist() -> None:
    df = trino.flightlist(
        "2023-01-01",
        "2023-01-10",
        callsign="AFR%",
        arrival_airport="RJBB",
    )
    assert df is not None
    assert df.callsign.max() == "AFR292"

    df = trino.flightlist(
        "2023-01-01",
        "2023-01-10",
        callsign="AFR292",
        departure_airport="LFPG",
    )
    assert df is not None
    assert df.arrival.max() == "RJBB"

    df = trino.flightlist(
        "2023-01-01",
        "2023-01-10",
        departure_airport="LF%",
        arrival_airport="RJ%",
    )
    assert df is not None

    expected = {"AFR", "ANA", "FDX", "JAL"}
    assert expected.intersection(df.callsign.str[:3].unique()) == expected

    df = trino.flightlist(
        "2019-11-01",
        departure_airport="LFBO",
        arrival_airport="LFBO",
        callsign="AIB%",
    )
    assert df is not None
    assert df.shape[0] == 2
    assert all(df.callsign.str.startswith("AIB"))

    df = trino.flightlist("2019-11-01", icao24="44017c")
    assert df is not None
    assert all(df.callsign.str.startswith("EJU"))


def test_history() -> None:
    df = trino.history(
        "2019-11-01 09:00",
        "2019-11-01 12:00",
        departure_airport="LFBO",
        arrival_airport="LFBO",
        callsign="AIB04%",
        compress=True,
    )
    assert df is not None
    assert df.icao24.max() == "388dfb"
    assert df.callsign.max() == "AIB04FI"

    df = trino.history(
        "2019-11-11 10:00",
        "2019-11-11 10:10",
        bounds=(0.11, 42.3, 2.82, 44.6),
        serials=1433801924,
        compress=False,
    )

    assert df is not None
    assert len(df.groupby(["icao24", "callsign"])) == 34


def test_complex_queries() -> None:
    error_msg = "airport may not be set if arrival_airport is"
    with pytest.raises(RuntimeError, match=error_msg):
        _ = trino.history(
            start="2021-08-24 00:00",
            stop="2021-08-24 01:00",
            airport="ESSA",
            arrival_airport="EGLL",
        )
    # test that `limit` generate correct query
    df = trino.history(
        start="2021-08-24 00:00",
        stop="2021-08-24 01:00",
        airport="ESSA",
        limit=3,
    )
    assert df is not None
    assert df.shape[0] == 3

    df = trino.history(
        start="2021-08-24 09:00",
        stop="2021-08-24 09:10",
        airport="ESSA",
    )
    assert df is not None
    assert len(df.groupby(["icao24", "callsign"])) == 23

    df = trino.history(
        start="2021-08-24 09:00",
        stop="2021-08-24 09:10",
        arrival_airport="ESSA",
    )
    assert df is not None
    assert len(df.groupby(["icao24", "callsign"])) == 13

    df = trino.history(
        start="2021-08-24 11:32",
        stop="2021-08-24 11:42",
        departure_airport="ESSA",
        arrival_airport="EGLL",
    )
    assert df is not None
    assert len(df.groupby(["icao24", "callsign"])) == 1
    s = df.groupby(["icao24", "callsign"]).agg(dict(time="min")).reset_index()
    assert s.callsign.iloc[0] == "BAW777C"
    assert s.icao24.iloc[0] == "400936"

    with pytest.raises(RuntimeError, match=error_msg):
        _ = trino.history(
            start="2021-08-24 00:00",
            stop="2021-08-24 01:00",
            airport="ESSA",
            arrival_airport="EGLL",
            limit=3,
        )

    df = trino.history(
        start="2021-08-24 00:00",
        stop="2021-08-24 00:10",
        arrival_airport="ESSA",
        serials=-1408232560,
    )
    assert df is not None
    assert len(df.groupby(["icao24", "callsign"])) == 1
    s = df.groupby(["icao24", "callsign"]).agg(dict(time="min")).reset_index()
    assert s.callsign.iloc[0] == "SAS6906"
    assert s.icao24.iloc[0] == "4ca863"

    df = trino.history(
        start="2021-08-24 00:00",
        stop="2021-08-24 00:10",
        serials=(-1408232560, -1408232534),
    )
    assert df is not None
    assert len(df.groupby(["icao24", "callsign"])) == 12

    df = trino.history(
        start="2021-08-24 09:00",
        stop="2021-08-24 09:10",
        departure_airport="ESSA",
        serials=(-1408232560, -1408232534),
        callsign="LOT%",
    )
    assert df is not None
    assert len(df.groupby(["icao24", "callsign"])) == 1
    s = df.groupby(["icao24", "callsign"]).agg(dict(time="min")).reset_index()
    assert s.callsign.iloc[0] == "LOT454"
    assert s.icao24.iloc[0] == "489789"

    df = trino.history(
        start="2021-08-24 09:00",
        stop="2021-08-24 09:10",
        bounds=(17.8936, 59.6118, 17.9894, 59.6716),
        serials=(-1408232560, -1408232534),
    )
    assert df is not None
    assert len(df.groupby(["icao24", "callsign"])) == 11
    s = df.groupby(["icao24", "callsign"]).agg(dict(time="min")).reset_index()
    assert s.query('callsign == "SAS1136"').icao24.iloc[0] == "51110b"

    df = trino.history(
        start="2021-08-24 09:30",
        stop="2021-08-24 09:40",
        departure_airport="ESSA",
        bounds=(17.8936, 59.6118, 17.9894, 59.6716),
        serials=(-1408232560, -1408232534),
    )
    assert df is not None
    assert len(df.groupby(["icao24", "callsign"])) == 1
    assert df.callsign.iloc[0] == "THY5HT"
    assert df.icao24.iloc[0] == "4bb1c5"


def test_time_buffer() -> None:
    df = trino.history(
        start="2024-03-16 09:00",
        stop="2024-03-16 11:00",
        time_buffer="25m",
        airport="UGTB",
        bounds=(44.958636, 41.665760, 44.965417, 41.670505),
    )
    assert df is not None
    assert len(df.groupby(["icao24", "callsign"])) == 4


def test_specific_columns() -> None:
    df = trino.history(
        "2023-03-01 12:00",
        "2023-03-01 13:00",
        arrival_airport="EHAM",
        selected_columns=(
            StateVectorsData4.time,
            "lat",
            "lon",
            "StateVectorsData4.icao24",
            # FlightsData4.estdepartureairport does not work without quotes
            "FlightsData4.estdepartureairport",
            "lastseen",  # in FlightData4
        ),
        limit=10,
    )
    assert df is not None
    for col in [
        "time",
        "lat",
        "lon",
        "icao24",
        "estdepartureairport",
        "lastseen",
    ]:
        assert col in df.columns


def test_func() -> None:
    airport = "EHAM"
    lat, lon = (52.308601, 4.76389)
    df = trino.history(
        "2023-03-01 12:00",
        "2023-03-01 13:00",
        func.ST_Distance(
            func.to_spherical_geography(func.ST_Point(lon, lat)),
            func.to_spherical_geography(
                func.ST_Point(StateVectorsData4.lon, StateVectorsData4.lat)
            ),
        )
        <= 10 * 1858,  # within 10 nautical miles
        arrival_airport=airport,
        limit=10,
    )
    assert df is not None

    df = trino.history(
        "2023-07-13",
        "2023-07-14",
        not_(func.regexp_like(StateVectorsData4.callsign, commercial_callsign)),
        limit=10,
    )
    assert df is not None


def test_icao24_lowcase() -> None:
    df = trino.flightlist(
        start="2023-01-03",
        stop="2023-01-04",
        icao24="400A0E",
    )
    assert df is not None

    df = trino.history(
        "2023-01-01 16:19:00",
        "2023-01-01 18:50:45",
        icao24="485A35",
        limit=30,
    )
    assert df is not None


def test_flarm() -> None:
    df = trino.flarm(
        "2018-09-11 07:00",
        "2018-09-11 08:00",
        limit=10,
        sensor_name="LS%",
    )
    assert df is not None
