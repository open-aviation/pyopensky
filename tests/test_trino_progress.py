"""Deterministic progress and query-link checks without Trino connections."""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
from sqlalchemy import CursorResult
from tqdm import tqdm

import pandas as pd
import pyopensky.trino as trino_module
from pyopensky.trino import Trino


@pytest.mark.parametrize(
    "stats,expected",
    [
        ({}, ""),
        ({"progressPercentage": 0}, "server 0.0%"),
        ({"totalSplits": 12}, "splits 0/12"),
        (
            {
                "progressPercentage": 25.5,
                "totalSplits": 1000,
                "completedSplits": 250,
            },
            "server 25.5% | splits 250/1,000",
        ),
        (
            {"processedRows": 1000, "processedBytes": 2048},
            f"scanned {tqdm.format_sizeof(1000)} rows, "
            f"{tqdm.format_sizeof(2048, 'B', 1024)}",
        ),
        (
            {"processedRows": 1},
            f"scanned {tqdm.format_sizeof(1)} rows, "
            f"{tqdm.format_sizeof(0, 'B', 1024)}",
        ),
    ],
)
def test_server_status(stats: dict[str, Any], expected: str) -> None:
    assert Trino.server_status(stats) == expected


@pytest.mark.parametrize("terminal", [False, True])
@pytest.mark.parametrize("stats", [None, {}, {"state": "RUNNING"}])
def test_progress_tolerates_partial_stats_and_preserves_rows(
    terminal: bool, stats: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = Mock()
    result.cursor = Mock(stats=stats)
    result.keys.return_value = ["value"]
    result.fetchmany.side_effect = [[(3,)], []]
    future = Mock()
    future.ready.side_effect = [False, False, True]
    future.get.return_value = [(1,), (2,)]
    pool = Mock()
    pool.apply_async.return_value = future
    monkeypatch.setattr(trino_module, "ThreadPool", Mock(return_value=pool))
    sleep = Mock()
    monkeypatch.setattr(trino_module.time, "sleep", sleep)
    bars = [MagicMock(), MagicMock()]
    for bar in bars:
        bar.__enter__.return_value = bar
    factory = Mock(side_effect=bars)
    factory.format_sizeof = tqdm.format_sizeof
    monkeypatch.setattr(trino_module, "tqdm", factory)
    monkeypatch.setattr(trino_module.sys.stderr, "isatty", lambda: terminal)
    frames = list(Trino().process_result(result, batch_size=2))
    assert pd.concat(frames).value.tolist() == [1, 2, 3]
    assert len(frames) == 2
    pool.apply_async.assert_called_once_with(result.fetchmany, (2,))
    pool.close.assert_called_once()
    assert sleep.call_count == 2
    for call in factory.call_args_list:
        assert call.kwargs["mininterval"] == (0.1 if terminal else 60.0)
    assert factory.call_args_list[0].kwargs["total"] == 100
    # Polling must not bypass the configured refresh interval.
    for call in bars[0].set_description.call_args_list[:2]:
        assert call.kwargs["refresh"] is False
    for call in bars[0].set_postfix_str.call_args_list[:2]:
        assert call.kwargs["refresh"] is False


def test_waiting_without_cursor_does_not_spin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = Mock(cursor=None)
    result.keys.return_value = ["value"]
    future = Mock()
    future.ready.side_effect = [False, True]
    future.get.return_value = []
    pool = Mock()
    pool.apply_async.return_value = future
    monkeypatch.setattr(trino_module, "ThreadPool", Mock(return_value=pool))
    sleep = Mock()
    monkeypatch.setattr(trino_module.time, "sleep", sleep)
    frames = list(Trino().process_result(result))
    assert len(frames) == 1
    assert frames[0].empty
    sleep.assert_called_once_with(0.1)
    pool.close.assert_called_once()


@pytest.mark.parametrize("first_batch", [False, True])
def test_fetch_failure_closes_worker_pool(
    first_batch: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = Mock(cursor=None)
    result.keys.return_value = ["value"]
    future = Mock()
    future.ready.return_value = True
    if first_batch:
        future.get.side_effect = RuntimeError("fetch failed")
    else:
        future.get.return_value = [(1,)]
        result.fetchmany.side_effect = RuntimeError("fetch failed")
    pool = Mock()
    pool.apply_async.return_value = future
    monkeypatch.setattr(trino_module, "ThreadPool", Mock(return_value=pool))
    with pytest.raises(RuntimeError, match="fetch failed"):
        list(Trino().process_result(result, batch_size=1))
    pool.close.assert_called_once()


@pytest.mark.parametrize("query_id", [None, "test-query"])
def test_query_link_uses_configured_host(
    query_id: str | None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(trino_module, "cache_path", tmp_path)
    client = Trino(host="trino.dev.opensky-network.org")
    result = Mock(spec=CursorResult)
    result.cursor = Mock(query_id=query_id)
    conn = MagicMock()
    conn.execution_options.return_value = conn
    conn.__enter__.return_value = conn
    conn.execute.return_value = result
    monkeypatch.setattr(client, "connect", lambda: conn)
    monkeypatch.setattr(
        client, "process_result", lambda _: [pd.DataFrame({"value": [1]})]
    )
    assert client.query("SELECT 1", cached=False).value.tolist() == [1]
    output = capsys.readouterr()
    assert output.out == ""
    if query_id is None:
        assert "ui/query.html" not in output.err
    else:
        assert (
            "https://trino.dev.opensky-network.org/ui/query.html?test-query"
            in output.err
        )
        assert "https://trino.opensky-network.org" not in output.err
