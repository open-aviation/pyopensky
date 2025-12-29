# pyopensky

The `pyopensky` Python library provides functions to download data from the OpenSky Network live API and historical databases. It aims at making ADS-B and Mode S data from OpenSky easily accessible in the Python programming environment.

**Key features:**

- Access to **live** aircraft state vectors and flight information via REST API
- Query **historical** ADS-B and Mode S data from the Trino database
- Decode and rebuild flight trajectories from raw messages
- Support for spatial and temporal filtering
- Built-in caching for efficient data retrieval
- Integration with pandas DataFrames for easy data analysis

Full documentation on <https://mode-s.org/pyopensky/>

## Installation

```sh
pip install pyopensky
```

The library is also available on conda-forge:

```sh
conda install -c conda-forge pyopensky
```

Development mode (with uv):

```sh
curl -LsSf https://astral.sh/uv/install.sh | sh  # Linux and MacOS
irm https://astral.sh/uv/install.ps1 | iex  # Windows
uv sync --dev
```

## Credentials

Access to the OpenSky Network historical database requires authentication. See details in the [documentation](https://open-aviation.github.io/pyopensky/credentials.html) on how to:

- Apply an OpenSky Network account
- Configure your credentials for API access
- Set up authentication for the Trino database

## Usage

### 1. REST API

> [!IMPORTANT]
> Do NOT use REST API for historical data, use trino instead!

Access **live** and recent flight data.

#### Functions

```python
from pyopensky.rest import REST

rest = REST()

rest.states()
rest.tracks(icao24)
rest.routes(callsign)
rest.aircraft(icao24, begin, end)
rest.arrival(airport, begin, end)
rest.departure(airport, begin, end)
```

#### Examples

```python
from pyopensky.rest import REST

rest = REST()

# Get current state vectors for all aircraft
rest.states()

# Get trajectory for a specific aircraft
rest.tracks(icao24="3c6444")

# Get route information for a callsign
rest.routes(callsign="AFR292")

# Get flights for a specific aircraft in a time range
# NOTE: do NOT use REST for historical data, use trino instead!!
rest.aircraft(icao24="3c6444", begin="2024-01-01", end="2024-01-02")

# Get arrivals at an airport
# NOTE: do NOT use REST for historical data, use trino instead!!
rest.arrival(airport="EHAM", begin="2024-01-01 12:00", end="2024-01-01 13:00")

# Get departures from an airport
# NOTE: do NOT use REST for historical data, use trino instead!!
rest.departure(airport="LFPG", begin="2024-01-01 12:00", end="2024-01-01 13:00")
```

### 2. Trino Interface

Query **historical** ADS-B and Mode S data with advanced selection and processing.

Functions

```python
from pyopensky.trino import Trino

trino = Trino()
# full description of the whole set of parameters in the documentation
trino.flightlist(start, stop, *, airport, callsign, icao24)
trino.rawdata(start, stop, *, callsign, icao24, bounds)
trino.history(start, stop, *, callsign, icao24, bounds)
trino.rebuild(start, stop, *, icao24, bounds)
```


#### Key Parameters

- **Time ranges**: Accept strings (ISO format), timestamps, or datetime objects
- **Wildcards**: Use `%` for any sequence and `_` for any character (e.g., `"KLM%"`)
- **Bounds**: Specify geographic area as `(west, south, east, north)` tuple
- **Caching**: Results are cached by default for faster subsequent queries
- **Filtering**: Combine multiple filters (airport, callsign, icao24, bounds, etc.)

#### Examples

```python
from pyopensky.trino import Trino

trino = Trino()

# Get list of flights with flexible selection
trino.flightlist(
    start="2023-01-01",
    stop="2023-01-10",
    callsign="AFR%",
    arrival_airport="RJBB",
)

# Get raw ADS-B messages for advanced analysis
trino.rawdata(
    start="2023-01-03 16:00:00",
    stop="2023-01-03 20:00:00",
    icao24="400A0E",
    bounds=(-1, 40.0, 15, 53.0)
)

# Get detailed trajectory data (state vectors)
trino.history(
    start="2019-11-01 09:00",
    stop="2019-11-01 12:00",
    departure_airport="LFBO",
    arrival_airport="LFBO",
    callsign="AIB04%",
)

# Query with geographical bounds and sensor filters
trino.history(
    start="2021-08-24 09:00",
    stop="2021-08-24 09:10",
    bounds=(17.8936, 59.6118, 17.9894, 59.6716),  # (W, S, E, N)
    serials=(-1408232560, -1408232534),
)
```

#### Downloading Raw ADS-B Data

The `rebuild()` method downloads raw ADS-B messages from multiple tables (position, velocity, identification, and rollcall) and merges them using time-based joins to create a comprehensive dataset for trajectory reconstruction.

This approach separates data download from decoding, giving you flexibility to choose your preferred decoder (pymodes or rs1090) or to implement custom decoding logic.

```python
# Download and merge raw data from all relevant tables
data = trino.rebuild(
    start="2023-01-03 16:00:00",
    stop="2023-01-03 20:00:00",
    icao24="400A0E",
)

# The result is a merged DataFrame with columns from all tables:
# - timestamp, icao24, rawmsg (raw message)
# - lat, lon, altitude, groundspeed, track (from position)
# - velocity, vertical_rate, geominurbaro (from velocity)
# - callsign (from identification)
# - squawk (from rollcall)

# You can also filter by geographic bounds
data = trino.rebuild(
    start="2023-01-03 16:00:00",
    stop="2023-01-03 20:00:00",
    bounds=(west, south, east, north),
)
```

**Decoding raw messages:**

The downloaded raw messages can be decoded using external libraries. Install the optional decoding dependencies:

```sh
pip install pyopensky[decoding]
```

This includes both `pymodes` (pure Python) and `rs1090` (Rust-based, faster) decoders. You can then use the decode functions from the scripts directory or implement your own decoding logic.