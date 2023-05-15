from pyopensky.schema import FlightsData4
from pyopensky.trino import Trino
from sqlalchemy import select

import pandas as pd

opensky = Trino()


def test_alive() -> None:
    df = opensky.query("select * from state_vectors_data4 limit 5")
    assert df.shape[0] == 5


def test_query() -> None:
    res = opensky.query(
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
        .where(FlightsData4.estarrivalairport != None)
        .order_by(FlightsData4.firstseen)
        .limit(50)
    )
    assert res.shape[0] == 10
    assert res.callsign.max() == "AFR291"
    assert res.estdepartureairport.max() == "RJBB"
    assert res.firstseen.diff().min() > pd.Timedelta("1 day 12 hours")
    assert res.track.str.len().max() > 600


def test_flightlist() -> None:
    df = opensky.flightlist(
        "2023-01-01",
        "2023-01-10",
        callsign="AFR%",
        arrival_airport="RJBB",
    )
    assert df is not None
    assert df.callsign.max() == "AFR292"

    df = opensky.flightlist(
        "2023-01-01",
        "2023-01-10",
        callsign="AFR292",
        departure_airport="LFPG",
    )
    assert df is not None
    assert df.arrival.max() == "RJBB"

    df = opensky.flightlist(
        "2023-01-01",
        "2023-01-10",
        departure_airport="LF%",
        arrival_airport="RJ%",
    )
    assert df is not None

    expected = {"AFR", "ANA", "FDX", "JAL"}
    assert expected.intersection(df.callsign.str[:3].unique()) == expected

    df = opensky.flightlist(
        "2019-11-01",
        departure_airport="LFBO",
        arrival_airport="LFBO",
        callsign="AIB%",
    )
    assert df is not None
    assert df.shape[0] == 2
    assert all(df.callsign.str.startswith("AIB"))

    df = opensky.flightlist("2019-11-01", icao24="44017c")
    assert df is not None
    assert all(df.callsign.str.startswith("EJU"))
