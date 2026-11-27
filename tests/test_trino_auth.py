"""Offline endpoint, authentication and cache isolation regressions."""

import hashlib
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import httpx
import pytest
from sqlalchemy import CursorResult, create_engine, event
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from trino.auth import JWTAuthentication, OAuth2Authentication
from trino.exceptions import HttpError, TrinoQueryError

import pandas as pd
import pyopensky.trino as trino_module
from pyopensky.trino import Token, Trino


def token(seconds: int = 3600) -> Token:
    now = int(pd.Timestamp.now(tz="UTC").timestamp())
    return {
        "access_token": "test-token",
        "preferred_username": "token-user",
        "iat": now,
        "exp": now + seconds,
    }


@pytest.fixture(autouse=True)
def no_auth_network(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(trino_module, "trino_username", "test-user")
    monkeypatch.setattr(trino_module, "trino_password", "test-password")
    monkeypatch.setattr(
        trino_module.httpx,
        "post",
        Mock(side_effect=AssertionError("Unexpected authentication request")),
    )


@pytest.mark.parametrize(
    "host,auth,jwt",
    [
        ("trino.opensky-network.org", "auto", True),
        ("trino.dev.opensky-network.org", "auto", False),
        ("trino.opensky-network.org", "oauth", False),
        ("trino.dev.opensky-network.org", "jwt", True),
    ],
)
def test_endpoint_authentication(
    host: str, auth: Any, jwt: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = Trino(host=host, catalog="catalog", schema="schema", auth=auth)
    client._token = token()
    request = Mock(return_value="test-token")
    monkeypatch.setattr(client, "token", request)
    with patch.object(trino_module, "create_engine") as factory:
        client.engine()
    url = make_url(factory.call_args.args[0])
    options = factory.call_args.kwargs["connect_args"]
    assert url.host == host
    assert url.database == "catalog/schema"
    assert url.query["source"] == "pyopensky"
    assert isinstance(
        options["auth"], JWTAuthentication if jwt else OAuth2Authentication
    )
    assert options["user"] == ("token-user" if jwt else "test-user")
    assert options["legacy_prepared_statements"] is True
    assert request.call_count == int(jwt)


def test_default_endpoint() -> None:
    client = Trino()
    assert (client.host, client.catalog, client.schema, client.auth) == (
        "trino.opensky-network.org",
        "minio",
        "osky",
        "auto",
    )


def test_oauth_fallback_does_not_use_stale_token_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = Trino()
    client._token = token(-1)
    monkeypatch.setattr(client, "token", lambda: None)
    with patch.object(trino_module, "create_engine") as factory:
        client.engine()
    options = factory.call_args.kwargs["connect_args"]
    assert isinstance(options["auth"], OAuth2Authentication)
    assert options["user"] == "test-user"


@pytest.mark.parametrize("remaining", [-30, 30, 120])
def test_token_refresh_margin(
    remaining: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = Trino()
    client._token = token(remaining)
    refreshed = token()
    refreshed["access_token"] = "refreshed"
    response = httpx.Response(
        200,
        json={"access_token": "refreshed"},
        request=httpx.Request("POST", "https://auth.example.test/token"),
    )
    request = Mock(return_value=response)
    monkeypatch.setattr(trino_module.httpx, "post", request)
    monkeypatch.setattr(trino_module.jwt, "decode", lambda *a, **k: refreshed)
    assert client.token() == ("test-token" if remaining > 60 else "refreshed")
    assert request.call_count == int(remaining <= 60)


@pytest.mark.parametrize("status", [400, 401])
def test_failed_refresh_discards_token(
    status: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = Trino()
    client._token = token(-1)
    monkeypatch.setattr(
        trino_module.httpx, "post", Mock(return_value=httpx.Response(status))
    )
    assert client.token() is None
    assert client._token is None


def test_missing_credentials_discards_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = Trino()
    client._token = token()
    monkeypatch.setattr(trino_module, "trino_password", None)
    assert client.token() is None
    assert client._token is None


def connection() -> MagicMock:
    result = MagicMock()
    result.execution_options.return_value = result
    result.__enter__.return_value = result
    return result


@pytest.mark.parametrize("wrapped", [False, True])
def test_unauthorized_query_retries_once(
    wrapped: bool, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    monkeypatch.setattr(trino_module, "cache_path", tmp_path)
    client = Trino()
    client._token = token()
    first, second = connection(), connection()
    error = HttpError("error 401: Unauthorized")
    first.execute.side_effect = (
        DBAPIError("SELECT 1", {}, error) if wrapped else error
    )
    result = MagicMock(spec=CursorResult)
    result.cursor = None
    second.execute.return_value = result
    states = []

    def connect() -> MagicMock:
        states.append(client._token)
        return first if len(states) == 1 else second

    monkeypatch.setattr(client, "connect", connect)
    monkeypatch.setattr(
        client, "process_result", lambda _: [pd.DataFrame({"value": [1]})]
    )
    frame = client.query("SELECT 1", cached=False)
    assert frame.value.tolist() == [1]
    assert states[0] is not None
    assert states[1] is None
    first.__exit__.assert_called_once()
    second.__exit__.assert_called_once()


def test_second_unauthorized_response_is_not_retried(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    monkeypatch.setattr(trino_module, "cache_path", tmp_path)
    client = Trino()
    first, second = connection(), connection()
    first.execute.side_effect = HttpError("error 401: Unauthorized")
    second.execute.side_effect = HttpError("error 401: Unauthorized")
    connect = Mock(side_effect=[first, second])
    monkeypatch.setattr(client, "connect", connect)
    with pytest.raises(HttpError, match="401"):
        client.query("SELECT 1", cached=False)
    assert connect.call_count == 2
    first.__exit__.assert_called_once()
    second.__exit__.assert_called_once()


@pytest.mark.parametrize(
    "error",
    [HttpError("error 403: Forbidden"), ValueError("invalid query")],
)
def test_other_errors_are_not_retried(
    error: Exception, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    monkeypatch.setattr(trino_module, "cache_path", tmp_path)
    client = Trino()
    conn = connection()
    conn.execute.side_effect = error
    connect = Mock(return_value=conn)
    monkeypatch.setattr(client, "connect", connect)
    with pytest.raises(type(error)):
        client.query("SELECT 1", cached=False)
    connect.assert_called_once()


def test_query_queue_error_keeps_friendly_message(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    monkeypatch.setattr(trino_module, "cache_path", tmp_path)
    client = Trino()
    conn = connection()
    conn.execute.side_effect = DBAPIError(
        "SELECT 1", {}, TrinoQueryError({"errorName": "QUERY_QUEUE_FULL"})
    )
    monkeypatch.setattr(client, "connect", lambda: conn)
    with pytest.raises(RuntimeError, match="QUERY_QUEUE_FULL"):
        client.query("SELECT 1", cached=False)
    conn.__exit__.assert_called_once()


def test_endpoint_cache_isolation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    monkeypatch.setattr(trino_module, "cache_path", tmp_path)
    sql = "SELECT source() AS value"
    # Pre-namespace entries have unknown provenance and must not be reused.
    legacy = hashlib.md5(f"{sql}\n{{}}".encode()).hexdigest()
    pd.DataFrame({"value": ["legacy"]}).to_parquet(
        tmp_path / f"{legacy}.parquet"
    )
    endpoints = [
        ("trino.opensky-network.org", "minio", "osky"),
        ("trino.dev.opensky-network.org", "minio", "osky"),
        ("trino.opensky-network.org", "other", "osky"),
        ("trino.opensky-network.org", "minio", "other"),
    ]
    opened = []
    for endpoint in [*endpoints, endpoints[0]]:
        client = Trino(*endpoint)
        engine = create_engine("sqlite://")
        label = "/".join(endpoint)

        @event.listens_for(engine, "connect")
        def register(db: Any, record: Any) -> None:
            opened.append(label)
            db.create_function("source", 0, lambda: label)

        monkeypatch.setattr(client, "connect", engine.connect)
        monkeypatch.setattr(
            client,
            "process_result",
            lambda result: [
                pd.DataFrame(result.fetchall(), columns=list(result.keys()))
            ],
        )
        try:
            assert client.query(sql).value.tolist() == [label]
        finally:
            engine.dispose()
    assert len(opened) == 4
    assert len(list(tmp_path.glob("*.parquet"))) == 5
