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

or add it in your project with

```sh
uv add pyopensky
```

The library is also available on conda-forge, although this approach is no longer recommended:

```sh
conda install -c conda-forge pyopensky
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

For several routes in the same time window, use one query with exact directed
airport pairs:

```python
df = trino.history(
    "2025-01-01",
    "2025-01-02",
    routes=[("KJFK", "EGLL"), ("KMIA", "EIDW"), ("LFPG", "KJFK")],
    selected_columns=("time", "icao24", "callsign", "lat", "lon"),
)
```

The same `routes` filter is available in `flightlist()` for metadata queries and
`rawdata()` for raw messages.
`history()` clips trajectories to the requested time window, including flights
that cross midnight.

Optional Trino session settings apply to all queries on a client. For selective
flight joins whose filtered flight table fits in each worker's memory:

```python
trino = Trino(session_properties={"join_distribution_type": "BROADCAST"})
```

`Trino()` uses the server's default settings. Use `cached=False` on each call when
benchmarking query performance.

> [!IMPORTANT]
> Refer to the [documentation](https://mode-s.org/pyopensky) for advanced usage and parameter details.
