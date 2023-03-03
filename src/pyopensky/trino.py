from __future__ import annotations

from typing import Any, TypedDict, cast

import requests
from sqlalchemy import Connection, create_engine
from sqlalchemy.sql.expression import text
from tqdm import tqdm
from trino.auth import JWTAuthentication  # , OAuth2Authentication
from trino.sqlalchemy import URL

import pandas as pd

from .config import password, username


class Token(TypedDict):
    access_token: str


class Trino:
    def token(self, **kwargs: Any) -> Token:
        result = requests.post(
            "https://auth.opensky-network.org/auth/realms/"
            "opensky-network/protocol/openid-connect/token",
            data={
                "client_id": "trino-client",
                "grant_type": "password",
                "username": username,
                "password": password,
            },
            **kwargs,
        )
        result.raise_for_status()
        return cast(Token, result.json())

    def connection(self) -> Connection:
        token = self.token()
        engine = create_engine(
            URL(
                "trino.opensky-network.org",
                port=443,
                user=username,
                catalog="minio",
                schema="osky",
            ),
            connect_args=dict(
                auth=JWTAuthentication(token["access_token"]),
                # auth=OAuth2Authentication(),
                http_scheme="https",
            ),
        )
        return engine.connect().execution_options(stream_results=True)

    def query(self, query: str) -> pd.DataFrame:
        results = []
        for chunk in tqdm(
            pd.read_sql_query(
                text(query),
                self.connection(),
                chunksize=2**20,
                parse_dates={
                    "time": {"utc": True, "unit": "s"},
                    "lastposupdate": {"utc": True, "unit": "s"},
                    "lastcontact": {"utc": True, "unit": "s"},
                    "hour": {"utc": True, "unit": "s"},
                },
            )
        ):
            results.append(chunk)
        return pd.concat(results)
