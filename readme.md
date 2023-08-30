# pyopensky

The `pyopensky` Python library provides functions to download data from the OpenSky Network live API and historical databases. It aims at making ADS-B and Mode S data from OpenSky easily accessible in the Python programming environment.

Full documentation on https://open-aviation.github.io/pyopensky

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

|        | Apply for access                                               |
| ------ | -------------------------------------------------------------- |
| Impala | https://opensky-network.org/data/impala                        |
| Trino  | contact@opensky-network.org (or specify it in the Impala form) |

The first time you use the library, a configuration file named `settings.conf` is created, including the following content:

```text
[default]
username =
password =
```

You will identify the folder where the `settings.conf` is located:

```python
from pyopensky.config import opensky_config_dir

print(opensky_config_dir)
```

## Usage

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

- from the [Impala](https://open-aviation.github.io/pyopensky/impala.html) shell (requires authentication):

  ```python
  from pyopensky.impala import Impala

  impala = Impala()
  # full description of the whole set of parameters in the documentation
  impala.flightlist(start, stop, *, airport, callsign, icao24)
  impala.history(start, stop, *, callsign, icao24, bounds)
  impala.rawdata(start, stop, *, callsign, icao24, bounds)
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
