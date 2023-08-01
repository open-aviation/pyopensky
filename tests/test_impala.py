import pytest
from pyopensky.impala import Impala

opensky = Impala()


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

    # df = opensky.flightlist(
    #     "2023-01-01",
    #     "2023-01-10",
    #     departure_airport="LF%",
    #     arrival_airport="RJ%",
    # )
    # assert df is not None

    # expected = {"AFR", "ANA", "FDX", "JAL"}
    # assert expected.intersection(df.callsign.str[:3].unique()) == expected

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


def test_history() -> None:
    df = opensky.history(
        "2019-11-01 09:00",
        "2019-11-01 12:00",
        departure_airport="LFBO",
        arrival_airport="LFBO",
        callsign="AIB%",
        compress=True,
    )
    assert df is not None
    assert df.icao24.max() == "388dfb"
    assert df.callsign.max() == "AIB04FI"

    df = opensky.history(
        "2019-11-11 10:00",
        "2019-11-11 10:10",
        bounds=(0.11, 42.3, 2.82, 44.6),
        serials=1433801924,
        compress=False,
    )

    assert df is not None
    assert len(df.groupby(["icao24", "callsign"])) == 34


# @pytest.mark.timeout(300)
def test_complex_queries() -> None:
    error_msg = "airport may not be set if arrival_airport is"
    with pytest.raises(RuntimeError, match=error_msg):
        _ = opensky.history(
            start="2021-08-24 00:00",
            stop="2021-08-24 01:00",
            airport="ESSA",
            arrival_airport="EGLL",
        )
    # test that `limit` generate correct query
    df = opensky.history(
        start="2021-08-24 00:00",
        stop="2021-08-24 01:00",
        airport="ESSA",
        limit=3,
    )
    assert df is not None
    assert df.shape[0] == 3

    df = opensky.history(
        start="2021-08-24 09:00",
        stop="2021-08-24 09:10",
        airport="ESSA",
    )
    assert df is not None
    assert len(df.groupby(["icao24", "callsign"])) == 23

    df = opensky.history(
        start="2021-08-24 09:00",
        stop="2021-08-24 09:10",
        arrival_airport="ESSA",
    )
    assert df is not None
    assert len(df.groupby(["icao24", "callsign"])) == 13

    df = opensky.history(
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
        _ = opensky.history(
            start="2021-08-24 00:00",
            stop="2021-08-24 01:00",
            airport="ESSA",
            arrival_airport="EGLL",
            limit=3,
        )

    df = opensky.history(
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

    df = opensky.history(
        start="2021-08-24 00:00",
        stop="2021-08-24 00:10",
        serials=(-1408232560, -1408232534),
    )
    assert df is not None
    assert len(df.groupby(["icao24", "callsign"])) == 12

    df = opensky.history(
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

    df = opensky.history(
        start="2021-08-24 09:00",
        stop="2021-08-24 09:10",
        bounds=(17.8936, 59.6118, 17.9894, 59.6716),
        serials=(-1408232560, -1408232534),
    )
    assert df is not None
    assert len(df.groupby(["icao24", "callsign"])) == 11
    s = df.groupby(["icao24", "callsign"]).agg(dict(time="min")).reset_index()
    assert s.query('callsign == "SAS1136"').icao24.iloc[0] == "51110b"

    df = opensky.history(
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
