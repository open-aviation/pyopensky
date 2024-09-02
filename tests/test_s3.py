import logging
from pathlib import Path

import pytest

import pandas as pd
from pyopensky.s3 import S3Client

logging.getLogger().setLevel(logging.INFO)
opensky = S3Client()

timestamp = pd.Timestamp("2022-07-07 18:00Z")
stem = "part-r-00169-1cb1ad83-4e0f-4ae8-8a3a-9ca53b4e0f3e.snappy"


@pytest.mark.skipif(True, reason="only for local debug")
def test_list_objects() -> None:
    for obj in opensky.list_objects(hour=timestamp, recursive=True):
        print(f"{obj.bucket_name=}, {obj.object_name=}")
    assert obj.object_name is not None
    assert Path(obj.object_name).stem == stem


@pytest.mark.skipif(True, reason="only for local debug")
def test_download_objects() -> None:
    for obj in opensky.list_objects(hour=timestamp, recursive=True):
        output_file = opensky.download_object(obj, Path("/tmp"))
        assert len(output_file.read_bytes()) == obj.size
