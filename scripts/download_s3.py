# %%

from pathlib import Path

import urllib3
from pyopensky.s3 import S3Client

import pandas as pd

opensky = S3Client(
    http_client=urllib3.ProxyManager("http://localhost:8123"),
)

for obj in opensky.table_v4(pd.Timestamp("2022-06-01 17:00Z"), recursive=True):
    print(f"{obj.bucket_name=}, {obj.object_name=}")
    output_file = opensky.download_object(obj, Path("/tmp"))
    print(f"{output_file=}")


# %%

# from traffic.data import opensky
#
# df = df.assign(time=lambda df: df.time.astype("int64"))
# df = opensky._format_dataframe(df.rename(columns=str.lower))
# df = opensky._format_history(df, nautical_units=True)
# df
