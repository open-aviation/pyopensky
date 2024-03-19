# pyopensky

The `pyopensky` Python library provides functions to download data from the OpenSky Network live API and historical databases. It aims at making ADS-B and Mode S data from OpenSky easily accessible in the Python programming environment.

Full documentation on [https://open-aviation.github.io/pyopensky](https://open-aviation.github.io/pyopensky)

## Installation

```sh
pip install pyopensky
```

The library is also available on conda-forge:

```sh
conda install -c conda-forge pyopensky
```

Development mode (with poetry):

```sh
git clone https://github.com/open-aviation/pyopensky
cd pyopensky
poetry install
```

## Credentials

See details in the [documentation](https://open-aviation.github.io/pyopensky/credentials.html)

## Usage

> [!IMPORTANT]
> The Impala shell is now deprecated. Please upgrade to Trino.

- from the [REST API](https://open-aviation.github.io/pyopensky/rest.html):

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

- from the [Trino](https://open-aviation.github.io/pyopensky/trino.html) database (requires authentication):

  ```python
  from pyopensky.trino import Trino

  trino = Trino()
  # full description of the whole set of parameters in the documentation
  trino.flightlist(start, stop, *, airport, callsign, icao24)
  trino.history(start, stop, *, callsign, icao24, bounds)
  trino.rawdata(start, stop, *, callsign, icao24, bounds)
  ```
