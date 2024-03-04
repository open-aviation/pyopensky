from __future__ import annotations

import logging
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Iterator, Literal, cast, overload

import urllib3
from minio import Minio, datatypes
from tqdm import tqdm

_log = logging.getLogger(__name__)


class S3Client:
    """Wrapper to OpenSky S3 repository

    Credentials are fetched from the configuration file.

    All methods return standard structures. When calls are made from the traffic
    library, they return advanced structures."""

    def __init__(self, **kwargs: Any) -> None:
        from .config import access_key, http_proxy, secret_key

        super().__init__()

        if http_proxy is not None:
            _log.info(f"Using http_proxy: {http_proxy}")
            kwargs["http_client"] = urllib3.ProxyManager(http_proxy)

        self.s3client = Minio(
            "s3.opensky-network.org:443",
            access_key=access_key,
            secret_key=secret_key,
            **kwargs,
        )

    def list_objects(
        self,
        hour: datetime,
        table: str = "state_vectors",
        folder: str = "tables_v4",
        **kwargs: Any,
    ) -> Iterator[datatypes.Object]:
        """
        Iterates over all files in the specified folder in the s3 object store.

        :param hour: will be converted to a timestamp, should be a UTC day for
            the flights table
        :param table: one of `flights`, `identification`, `operational_status`,
            `position`, `rollcall_replies`, `state_vectors`, `velocity`
        :param folder: one of `tables_v4`, `tables_v5`

        With `tables_v4`, the tables follow the pattern below:
            - `flights/day=`
            - `identification/hour=`
            - `operational_status/hour=`
            - `position/hour=`
            - `rollcall_replies/hour=`
            - `state_vectors/hour=`
            - `velocity/hour=`

        With `tables_v5`, the tables follow the pattern below:
            - `flights/day=`

        With `raw`, the tables follow the pattern below:
            - ads-b.mode-s-v2/year/month/day/hour
            - flarm-v1.avro/year/month/day/hour

        """

        if folder == "raw":
            yield from self.s3client.list_objects(
                "opensky-hdfs-backup",
                prefix=f"{folder}/{table}/{hour:%Y/%m/%d/%H}",
                **kwargs,
            )

        granularity = "day" if table == "flights" else "hour"
        yield from self.s3client.list_objects(
            "opensky-hdfs-backup",
            prefix=f"{folder}/{table}/{granularity}={hour.timestamp():.0f}",
            **kwargs,
        )

    @overload
    def download_object(
        self,
        obj: datatypes.Object,
        filename: None | Path,
    ) -> Path: ...

    @overload
    def download_object(
        self,
        obj: datatypes.Object,
        filename: None | Path,
        return_buffer: Literal[False],
    ) -> Path: ...

    @overload
    def download_object(
        self,
        obj: datatypes.Object,
        filename: None | Path,
        return_buffer: Literal[True],
    ) -> BytesIO: ...

    def download_object(
        self,
        obj: datatypes.Object,
        filename: None | Path = None,
        return_buffer: bool = False,
    ) -> BytesIO | Path:
        """
        Download files from the s3 object store.

        :param obj: object returned by returned by :meth S3Client.list_objects:
        :param filename: If `None`, writes the file in current folder.
            Otherwise, if a folder, writes the file in the given folder.
        """
        total_size: int = cast(int, obj.size)
        buffer = BytesIO()

        if obj.object_name is None:
            raise ValueError("Object {obj} has no object_name attribute")

        for idx in tqdm(range(0, total_size, 2**20), unit="Mb"):
            t = self.s3client.get_object(
                obj.bucket_name,
                obj.object_name,
                offset=idx,
                length=2**20,
            )
            buffer.write(t.data)

        buffer.seek(0)

        if return_buffer:
            return buffer

        dirname = None
        if filename is not None and filename.is_dir():
            dirname = filename
            filename = None

        if filename is None:
            filename = Path(obj.object_name)
            filename = Path(filename.name) if filename.name else filename
            if dirname is not None:
                filename = dirname / filename

        filename.write_bytes(buffer.getvalue())

        return filename
