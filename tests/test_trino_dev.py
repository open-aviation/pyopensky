from typing import Any, Iterable

import pytest
from sqlalchemy import Select
from trino.sqlalchemy.dialect import TrinoDialect

import pandas as pd
from pyopensky.schema_dev import Df18Data4
from pyopensky.time import to_datetime
from pyopensky.trino import Trino


def test_development_host_is_configurable() -> None:
    trino = Trino(host="trino.dev.opensky-network.org")

    assert trino.engine().url.host == "trino.dev.opensky-network.org"
    assert trino.engine().url.database == "minio/osky"


def test_df18_messages_builds_partition_bounded_flat_query(
    monkeypatch: Any,
) -> None:
    trino = Trino(host="trino.dev.opensky-network.org")
    captured: dict[str, Any] = {}

    def fake_query(
        query: Select[Any], cached: bool = True, compress: bool = False
    ) -> pd.DataFrame:
        captured.update(query=query, cached=cached, compress=compress)
        return pd.DataFrame({"rawmsg": ["90" + "0" * 26]})

    monkeypatch.setattr(trino, "query", fake_query)
    result = trino.df18_messages(
        "2026-04-29 15:05Z",
        "2026-04-29 15:10Z",
        address="ABCDEF",
        control_field=[0, 2, 6],
        message_kind="tisb_%",
        cached=False,
        compress=True,
        limit=10,
    )

    assert result is not None
    statement = captured["query"]
    sql = str(statement)
    assert statement.get_final_froms()[0].name == Df18Data4.__tablename__
    assert "SELECT *" in sql
    assert "df18_data4.mintime >=" in sql
    assert "df18_data4.mintime <" in sql
    assert "df18_data4.hour >=" in sql
    assert "df18_data4.address =" in sql
    assert "df18_data4.cf IN" in sql
    assert "df18_data4.msgkind LIKE" in sql
    assert captured["cached"] is False
    assert captured["compress"] is True


@pytest.mark.parametrize(
    "host", ["trino.opensky-network.org", "trino.dev.opensky-network.org"]
)
@pytest.mark.parametrize("control_field", [None, 0, [0, 2], (0, 6)])
def test_df18_messages_defaults_and_filters(
    host: str,
    control_field: int | Iterable[int] | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = Trino(host=host)
    captured = []

    def query(statement: Select[Any], **kwargs: Any) -> pd.DataFrame:
        captured.append(statement)
        return pd.DataFrame()

    monkeypatch.setattr(client, "query", query)
    assert (
        client.df18_messages(
            "2026-04-29 15:05Z",
            None,
            Df18Data4.surface.is_(True),
            control_field=control_field,
            address=["ABCDEF", "123456"],
        )
        is None
    )
    sql = str(
        captured[0].compile(
            dialect=TrinoDialect(), compile_kwargs={"literal_binds": True}
        )
    )
    start = int(to_datetime("2026-04-29 15:05Z").timestamp())
    assert f"df18_data4.mintime >= {start}" in sql
    assert f"df18_data4.mintime < {start + 86400}" in sql
    assert f"df18_data4.hour >= {start - 300}" in sql
    assert f"df18_data4.hour < {start + 86400 + 3300}" in sql
    assert "df18_data4.surface IS true" in sql
    assert "'abcdef'" in sql
    assert "'123456'" in sql
    assert ("df18_data4.cf IN" in sql) == (control_field is not None)
