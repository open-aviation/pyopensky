from pymodes_opensky import MeteoHelper

meteo = MeteoHelper()
df = meteo.get(
    icao24=["49d304", "4007f9"],
    start="2018-07-19 15:00:00",
    end="2018-07-19 15:10:00",
    include45=True,
)
print("**Print first 10 rows:")
print(df.head(10))
