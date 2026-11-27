"""Offline cancellation and signal handling regressions; no signals are sent."""

import signal
import threading
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
from sqlalchemy import CursorResult

import pandas as pd
import pyopensky.trino as trino_module
from pyopensky.trino import Trino, _terminate_as_exit


@pytest.fixture
def running_query(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> tuple[Trino, MagicMock, MagicMock]:
    monkeypatch.setattr(trino_module, "cache_path", tmp_path)
    client = Trino()
    conn = MagicMock()
    conn.execution_options.return_value = conn
    conn.__enter__.return_value = conn
    result = MagicMock(spec=CursorResult)
    result.cursor = Mock(query_id="test-query")
    conn.execute.return_value = result
    monkeypatch.setattr(client, "connect", lambda: conn)
    return client, conn, result


@pytest.mark.parametrize(
    "error",
    [KeyboardInterrupt(), SystemExit(143), RuntimeError("fetch failed")],
)
def test_interrupted_query_cancels_and_preserves_exception(
    running_query: Any,
    error: BaseException,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client, conn, result = running_query
    monkeypatch.setattr(client, "process_result", Mock(side_effect=error))
    with pytest.raises(type(error)) as caught:
        client.query("SELECT 1")
    assert caught.value is error
    result.cursor.cancel.assert_called_once()
    conn.__exit__.assert_called_once()
    assert not list(tmp_path.iterdir())


def test_cancellation_failure_does_not_mask_interruption(
    running_query: Any, monkeypatch: pytest.MonkeyPatch, caplog: Any
) -> None:
    client, conn, result = running_query
    result.cursor.cancel.side_effect = RuntimeError("server unavailable")
    error = KeyboardInterrupt()
    monkeypatch.setattr(client, "process_result", Mock(side_effect=error))
    with pytest.raises(KeyboardInterrupt) as caught:
        client.query("SELECT 1", cached=False)
    assert caught.value is error
    assert "Could not cancel Trino query test-query" in caplog.text
    conn.__exit__.assert_called_once()


def test_completed_query_is_not_cancelled(
    running_query: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, _, result = running_query
    monkeypatch.setattr(
        client, "process_result", lambda _: [pd.DataFrame({"value": [1]})]
    )
    assert client.query("SELECT 1", cached=False).value.tolist() == [1]
    result.cursor.cancel.assert_not_called()


def test_cancellation_without_cursor_is_harmless() -> None:
    result = Mock(cursor=None)
    Trino.cancel(result)


def test_interrupt_waiting_for_first_batch_closes_pool_and_cancels(
    running_query: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, conn, result = running_query
    result.cursor.stats = {"state": "RUNNING", "progressPercentage": 0}
    pool = Mock()
    pool.apply_async.return_value.ready.return_value = False
    monkeypatch.setattr(trino_module, "ThreadPool", Mock(return_value=pool))
    monkeypatch.setattr(
        trino_module.time, "sleep", Mock(side_effect=KeyboardInterrupt)
    )
    with pytest.raises(KeyboardInterrupt):
        client.query("SELECT 1", cached=False)
    pool.close.assert_called_once()
    pool.join.assert_not_called()  # Must not wait before cancelling the fetch.
    result.cursor.cancel.assert_called_once()
    conn.__exit__.assert_called_once()


@pytest.mark.parametrize("interrupt", [False, True])
def test_sigterm_handler_restoration(
    interrupt: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Exercise the handler directly rather than sending a real process signal.
    state: dict[str, Any] = {"handler": signal.SIG_DFL}
    history = []

    def install(signum: int, handler: Any) -> None:
        assert signum == signal.SIGTERM
        state["handler"] = handler
        history.append(handler)

    monkeypatch.setattr(signal, "getsignal", lambda _: state["handler"])
    monkeypatch.setattr(signal, "signal", install)
    if interrupt:
        with pytest.raises(SystemExit) as caught:
            with _terminate_as_exit():
                handler = state["handler"]
                assert callable(handler)
                handler(signal.SIGTERM, None)
        assert caught.value.code == 128 + signal.SIGTERM
        assert history[-2] == signal.SIG_IGN
    else:
        with _terminate_as_exit():
            assert callable(state["handler"])
    assert state["handler"] == signal.SIG_DFL


def test_existing_sigterm_handler_is_preserved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing = Mock()
    install = Mock()
    monkeypatch.setattr(signal, "getsignal", lambda _: existing)
    monkeypatch.setattr(signal, "signal", install)
    with _terminate_as_exit():
        pass
    install.assert_not_called()


def test_worker_thread_does_not_install_signal_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install = Mock()
    monkeypatch.setattr(signal, "signal", install)
    completed = []

    def worker() -> None:
        with _terminate_as_exit():
            completed.append(True)

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert completed == [True]
    install.assert_not_called()
