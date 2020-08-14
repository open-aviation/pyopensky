from pyopensky import EHSHelper

ehs = EHSHelper()

df = ehs.get(icao24="4844C6", start="2019-10-01 08:00:00", end="2019-10-01 08:10:00")

print("Print first 10 rows:")
print(df.head(10))
