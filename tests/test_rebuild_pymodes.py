"""Offline rebuild regressions using recorded ADS-B position messages."""

from unittest.mock import Mock

import pytest

import pandas as pd
from pyopensky.rebuild import Rebuild
from pyopensky.trino import Trino

pytest.importorskip("pyModeS", minversion="3.6.1")

EVEN = "8D484164602B33D825DF33A49786"
ODD = "8D484164602B275037D32FB795B8"
# A CRC-valid 4,075 ft report received while this aircraft was at 7,450 ft.
ALTITUDE_OUTLIER = "8D4841646019B736C5D12199D7CA"


def rebuild_positions(
    frames: list[tuple[float, str]], rollcall: pd.DataFrame | None = None
) -> pd.DataFrame | None:
    positions = pd.DataFrame(frames, columns=["mintime", "rawmsg"]).assign(
        icao24=lambda df: df.rawmsg.str[2:8].str.lower(),
        lat=0.0,
        lon=0.0,
        alt=0.0,
    )
    trino = Mock(spec=Trino)
    trino.rawdata.side_effect = [positions, None, None, rollcall]
    return Rebuild(trino).rebuild(
        "2025-04-13",
        "2025-04-14",
        icao24=positions.icao24.unique().tolist(),
        decoder="pymodes",
        include_rollcall=rollcall is not None,
    )


def test_rebuild_validates_positions_and_preserves_capture_times() -> None:
    result = rebuild_positions(
        [
            (1001.0, ODD),
            (1000.0, EVEN),
            (1001.2, ALTITUDE_OUTLIER),
            (1001.4, "8D484164602B23D7F9DF1AFDD978"),
            (1001.5, "8D484164602B275029D328D75A02"),
            (1000.5, EVEN),  # Same payload broadcast again.
            (1000.5, EVEN),  # Duplicate observation.
        ]
    )
    assert result is not None
    assert result.mintime.tolist() == [1000.0, 1000.5, 1001.0, 1001.4, 1001.5]
    assert result.altitude.tolist() == [7475, 7475, 7450, 7450, 7450]
    assert all(result.lat.between(47.76, 47.77))
    assert all(result.lon.between(8.42, 8.43))


@pytest.mark.parametrize(
    "frames",
    [
        [(1000.0, EVEN), (1011.0, ODD)],  # Outside the CPR pairing window.
        [(1000.0, EVEN), (1001.0, ODD[:-1] + "0")],  # Corrupt parity.
    ],
)
def test_rebuild_without_a_valid_pair(frames: list[tuple[float, str]]) -> None:
    assert rebuild_positions(frames) is None


@pytest.mark.parametrize(
    "later_pair",
    [
        (EVEN, ODD),
        ("8D40058B58C901375147EFD09357", "8D40058B58C904A87F402D3B8C59"),
    ],
)
def test_sparse_encounters_keep_both_pairs(later_pair: tuple[str, str]) -> None:
    result = rebuild_positions(
        [(1, EVEN), (2, ODD), (305, later_pair[0]), (306, later_pair[1])]
    )
    assert result is not None
    assert sorted(result.mintime) == [1, 2, 305, 306]


def test_rollcall_merge_keeps_both_capture_times() -> None:
    rollcall = pd.DataFrame({"mintime": [1.5], "icao24": ["484164"]})
    result = rebuild_positions([(1.0, EVEN), (2.0, ODD)], rollcall)
    assert result is not None
    assert result.mintime.tolist() == [1.5, 1.5]
    assert result.mintime_pos.tolist() == [1, 2]
