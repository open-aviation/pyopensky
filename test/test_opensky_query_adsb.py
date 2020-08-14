from pyopensky import OpenskyImpalaWrapper

# import logging
#
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)


opensky = OpenskyImpalaWrapper()

# test a simple and large query, over France
df = opensky.query(
    type="adsb",
    start="2020-02-01 13:00:00",
    end="2020-02-01 13:00:05",
    # icao24=[],
    bound=[40, -5, 50, 10],
)

print("**Print first 10 rows:")
print(df.head(10))
