import pytest
from sqlalchemy import func, not_, select

import pandas as pd
from pyopensky.schema import FlightsData4, StateVectorsData4
from pyopensky.trino import Trino

trino = Trino()


commercial_callsign = (
    "^([A-Z]{3})[0-9](([0-9]{0,3})|([0-9]{0,2})([A-Z])|([0-9]?)([A-Z]{2}))\\s*$"
)


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
    # The archive can resample tracks. Check that it returns timed positions.
    track = res.track.iloc[res.track.str.len().argmax()]
    assert len(track) > 1
    assert {"time", "latitude", "longitude"} <= track[0].keys()
    assert track[0]["time"] != track[-1]["time"]


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
    assert (
        expected.intersection(df.callsign.str.slice(0, 3).unique()) == expected
    )

    df = trino.flightlist(
        "2025-04-07",
        departure_airport="LFBO",
        arrival_airport="LFBO",
        callsign="AIB%",
    )
    assert df is not None
    assert "AIB07TN" in df.callsign.values
    assert all(df.callsign.str.startswith("AIB"))

    df = trino.flightlist("2025-04-07", icao24="38ed1b")
    assert df is not None
    assert set(df.icao24) == {"38ed1b"}
    assert "AIB07TN" in df.callsign.values


def test_history() -> None:
    df = trino.history(
        "2025-04-07 07:40",
        "2025-04-07 07:43",
        departure_airport="LFBO",
        arrival_airport="LFBO",
        callsign="AIB07%",
        compress=True,
    )
    assert df is not None
    assert set(df.icao24) == {"38ed1b"}
    assert set(df.callsign) == {"AIB07TN"}

    df = trino.history(
        "2025-04-07 07:40",
        "2025-04-07 07:43",
        bounds=(0.11, 42.3, 2.82, 44.6),
        serials=1433801924,
        compress=False,
    )

    assert df is not None
    assert "AIB07TN" in df.callsign.values
    assert df.lon.between(0.11, 2.82).all()
    assert df.lat.between(42.3, 44.6).all()
    assert all(1433801924 in receivers for receivers in df.serials)


def test_complex_queries() -> None:
    start = "2025-04-13 08:46"
    stop = "2025-04-13 08:49"
    stockholm = (17.8936, 59.6118, 17.9894, 59.6716)
    receivers = (-1408232560, -1408232534)
    error_msg = "airport may not be set if arrival_airport is"
    for limit in (None, 3):
        with pytest.raises(RuntimeError, match=error_msg):
            trino.history(
                start, stop, airport="ESSA", arrival_airport="EGLL", limit=limit
            )

    df = trino.history(start, stop, airport="ESSA", limit=3)
    assert df is not None
    assert len(df) == 3

    df = trino.history(start, stop, airport="ESSA")
    assert df is not None
    stockholm_flights = set(df.icao24)
    assert "48ac81" in stockholm_flights  # LOT454 departing Stockholm

    df = trino.history(start, stop, arrival_airport="ESSA")
    assert df is not None
    assert set(df.icao24) < stockholm_flights

    df = trino.history(
        "2025-04-13 06:03",
        "2025-04-13 06:06",
        departure_airport="ESSA",
        arrival_airport="EGLL",
        selected_columns=(
            "icao24",
            "callsign",
            "estdepartureairport",
            "estarrivalairport",
        ),
    )
    assert df is not None
    assert set(df.estdepartureairport) == {"ESSA"}
    assert set(df.estarrivalairport) == {"EGLL"}
    assert set(df.loc[df.callsign == "BAW771C", "icao24"]) == {"40097e"}

    df = trino.history(
        start, stop, arrival_airport="ESSA", serials=receivers[0]
    )
    assert df is not None
    assert all(receivers[0] in heard_by for heard_by in df.serials)

    df = trino.history(start, stop, serials=receivers)
    assert df is not None
    assert all(set(receivers).intersection(heard_by) for heard_by in df.serials)

    df = trino.history(
        start,
        stop,
        departure_airport="ESSA",
        serials=receivers,
        callsign="LOT%",
    )
    assert df is not None
    assert set(df.callsign) == {"LOT454"}
    assert set(df.icao24) == {"48ac81"}

    df = trino.history(start, stop, bounds=stockholm, serials=receivers)
    assert df is not None
    assert "LOT454" in df.callsign.values
    assert df.lon.between(stockholm[0], stockholm[2]).all()
    assert df.lat.between(stockholm[1], stockholm[3]).all()

    df = trino.history(
        start,
        stop,
        departure_airport="ESSA",
        bounds=stockholm,
        serials=receivers,
    )
    assert df is not None
    assert "LOT454" in df.callsign.values
    assert df.lon.between(stockholm[0], stockholm[2]).all()
    assert df.lat.between(stockholm[1], stockholm[3]).all()


def test_time_buffer() -> None:
    df = trino.history(
        start="2024-03-16 09:25",
        stop="2024-03-16 10:50",
        time_buffer="25m",
        airport="UGTB",
        bounds=(44.958636, 41.665760, 44.965417, 41.670505),
    )
    assert df is not None

    before = df.query('time <= "2024-03-16 09:25Z"')
    assert before is not None
    print(before)
    assert "QTR91M" in before.callsign.values

    after = df.query('time >= "2024-03-16 10:50Z"')
    assert after is not None
    assert "QTR256" in after.callsign.values


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
