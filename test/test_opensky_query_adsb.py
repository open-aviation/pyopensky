from pymodes_opensky import OpenskyImpalaWrapper

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


opensky = OpenskyImpalaWrapper()

# test a simple and massive query
df = opensky.query(
    type="adsb",
    start="2018-08-01 13:00:00",
    end="2018-08-01 13:00:05",
    bound=[30, -30, 90, 35],
)
print("**Print first 10 rows:")
print(df.head(10))

# test query with single icao address
df = opensky.query(
    type="adsb", start="2018-07-01 13:00:00", end="2018-07-01 13:00:10", icao24="424588"
)
print("**Print first 10 rows:")
print(df.head(10))

# test query with multiple icao address
df = opensky.query(
    type="adsb",
    start="2018-07-01 13:00:00",
    end="2018-07-01 13:00:10",
    icao24=["424588", "3c66a9"],
)

print("**Print first 10 rows:")
print(df.head(10))
