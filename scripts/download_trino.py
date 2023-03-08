# %%

from pyopensky.trino import Trino

import pandas as pd

opensky = Trino()
df = opensky.query(
    "select * from state_vectors_data4 "
    "where 43 < lat and lat < 44 and 1 < lon and lon < 2 "
    f"and hour = {pd.Timestamp('2023-02-12 12:00Z').timestamp():.0f}"
)
df
