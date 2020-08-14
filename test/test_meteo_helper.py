from pyopensky import MeteoHelper

meteo = MeteoHelper()
df = meteo.get(
    icao24=["341395"],
    start="2020-03-15 19:20:00",
    end="2020-03-15 20:20:00",
    include45=False,
)
print("Print first 10 rows:")
print(df.head(10))
