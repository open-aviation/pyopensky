from pymodes_opensky import OpenskyImpalaWrapper

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


opensky = OpenskyImpalaWrapper()

# test a simple and massive query, bounded on EU
df = opensky.query(
    type="raw",
    start="2018-07-01 13:00:00",
    end="2018-07-01 13:00:01",
    bound=[30, -30, 90, 35],
)
print("**Print first 10 rows:")
print(df.head(10))

# test query with single icao address
df = opensky.query(
    type="raw", start="2018-07-01 13:00:00", end="2018-07-01 13:00:10", icao24="40097e"
)
print("**Print first 10 rows:")
print(df.head(10))

# test query with multiple icao address
df = opensky.query(
    type="raw",
    start="2018-07-01 13:00:00",
    end="2018-07-01 13:00:10",
    icao24=["40097e", "3c6487"],
)
print("**Print first 10 rows:")
print(df.head(10))
