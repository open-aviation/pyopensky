from __future__ import annotations

from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any

from minio import Minio, datatypes
from tqdm import tqdm


class S3Client:
    def __init__(self, **kwargs: Any) -> None:
        from .config import access_key, secret_key

        super().__init__(**kwargs)

        self.s3client = Minio(
            "s3.opensky-network.org:443",
            access_key=access_key,
            secret_key=secret_key,
            **kwargs,
        )

    def table_v4(
        self,
        hour: datetime,
        table_v4: str = "state_vectors",
        **kwargs: Any,
    ) -> datatypes.Object:
        yield from self.s3client.list_objects(
            "opensky-hdfs-backup",
            prefix=f"tables_v4/{table_v4}/hour={hour.timestamp():.0f}",
            **kwargs,
        )

    def download_object(
        self, obj: datatypes.Object, filename: None | Path
    ) -> Path:
        total_size = obj.size
        buffer = BytesIO()

        for idx in tqdm(range(0, total_size, 2**20), unit="Mb"):
            t = self.s3client.get_object(
                obj.bucket_name,
                obj.object_name,
                offset=idx,
                length=2**20,
            )
            buffer.write(t.data)

        buffer.seek(0)

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
