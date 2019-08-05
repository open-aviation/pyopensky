from pyms4os import EHSHelper

ehs = EHSHelper()
df = ehs.get(
    icao24="49d304", start="2018-07-19 15:00:00", end="2018-07-19 15:10:00"
)
print("**Print first 10 rows:")
print(df.head(10))
