from __future__ import annotations

import time
from multiprocessing.pool import ThreadPool
from typing import Any, Iterable, TypedDict, cast

import requests
from sqlalchemy import (
    Connection,
    CursorResult,
    Engine,
    TextClause,
    create_engine,
)
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

    def engine(self) -> Engine:
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
        return engine

    def connect(self) -> Connection:
        return self.engine().connect()

    def query(self, query: str | TextClause) -> pd.DataFrame:
        exec_kw = dict(stream_results=True)  # not sure this option is necessary
        if isinstance(query, str):
            query = text(query)
        with self.connect().execution_options(**exec_kw) as connect:
            # There are steps here, that will not appear in the progress bar
            # but that you can check on https://trino.opensky-network.org/ui/
            return pd.concat(self.process_result(connect.execute(query)))

    def process_result(
        self,
        res: CursorResult[Any],
        batch_size: int = 50_000,
    ) -> Iterable[pd.DataFrame]:
        pool = ThreadPool(processes=1)
        async_result = pool.apply_async(res.fetchmany, (batch_size,))
        percentage = 0

        with tqdm(unit="%", unit_scale=True) as processing_bar:
            while not async_result.ready():
                processing_bar.set_description(res.cursor.stats["state"])
                increment = res.cursor.stats["progressPercentage"] - percentage
                percentage = res.cursor.stats["progressPercentage"]
                processing_bar.update(increment)

                time.sleep(0.1)

            increment = res.cursor.stats["progressPercentage"] - percentage
            percentage = res.cursor.stats["progressPercentage"]
            processing_bar.set_description(res.cursor.stats["state"])

        with tqdm(
            unit="lines", unit_scale=True, desc="DOWNLOAD"
        ) as download_bar:
            sequence_rows = async_result.get()
            download_bar.update(len(sequence_rows))
            yield pd.DataFrame.from_records(sequence_rows, columns=res.keys())

            while len(sequence_rows) == batch_size:
                sequence_rows = res.fetchmany(batch_size)
                download_bar.update(len(sequence_rows))
                yield pd.DataFrame.from_records(
                    sequence_rows, columns=res.keys()
                )
