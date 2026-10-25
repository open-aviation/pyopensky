Advanced Trino queries
======================

This section demonstrates advanced query patterns for working with the OpenSky Trino database.

Using schema objects with SQLAlchemy
------------------------------------

PyOpenSky provides schema classes for all Trino tables. You can use SQLAlchemy's query builder for complex queries:

.. code-block:: python

    from sqlalchemy import select
    from pyopensky.schema import StateVectorsData4, FlightsData4
    from pyopensky.trino import Trino
    
    trino = Trino()
    
    # Select specific columns
    query = (
        select(StateVectorsData4)
        .with_only_columns(
            StateVectorsData4.time,
            StateVectorsData4.icao24,
            StateVectorsData4.lat,
            StateVectorsData4.lon,
            StateVectorsData4.baroaltitude,
            StateVectorsData4.callsign,
        )
        .where(StateVectorsData4.callsign.startswith("AFR"))
        .where(StateVectorsData4.hour >= "2023-01-01")
        .where(StateVectorsData4.hour < "2023-01-02")
        .limit(1000)
    )
    
    df = trino.query(query)

Selecting specific columns
--------------------------

Use ``with_only_columns()`` to reduce data transfer and improve query performance:

.. code-block:: python

    from pyopensky.schema import StateVectorsData4
    from pyopensky.trino import Trino
    
    trino = Trino()
    
    # Minimal trajectory data
    df = trino.history(
        start="2023-01-03 16:00",
        stop="2023-01-03 20:00",
        icao24="400a0e",
        selected_columns=(
            "time",
            "icao24",
            "lat",
            "lon",
            "baroaltitude",
            "velocity",
            "heading",
        ),
    )

.. note::
   Column names can be specified as:
   
   - String: ``"lat"`` (automatically prefixed with table name)
   - Schema attribute: ``StateVectorsData4.lat``
   - Qualified string: ``"StateVectorsData4.lat"`` or ``"FlightsData4.estdepartureairport"``

Configuring Trino sessions
---------------------------

Pass ``session_properties`` to ``Trino`` to configure each connection opened by
that client. Names and values are strings. For example, a broadcast join can
reduce CPU and worker network traffic for selective flight queries:

.. code-block:: python

    from pyopensky.trino import Trino

    trino = Trino(
        session_properties={"join_distribution_type": "BROADCAST"}
    )
    df = trino.history(
        "2025-06-01 12:00",
        "2025-06-01 13:00",
        routes=[("EGLL", "KJFK"), ("LFPG", "KJFK"), ("EHAM", "KJFK")],
        selected_columns=("time", "icao24", "callsign", "lat", "lon"),
    )

``Trino()`` uses the server's default session settings. Broadcast joins are
optional: the filtered build table must fit in each worker's memory. For large
flight selections, let the server choose the join strategy or choose another
session configuration. See `Trino join distribution selection
<https://trino.io/docs/current/optimizer/cost-based-optimizations.html#join-distribution-selection>`_.

Session properties apply to ``query()``, ``history()``, ``flightlist()``, and
``rawdata()`` on that client. The constructor copies the supplied mapping.
Result-cache keys include configured properties, so different configurations do
not reuse each other's results. To measure query performance, pass
``cached=False`` to every measured call.

Batching directed routes
------------------------

Use ``routes`` to retrieve several directed airport pairs in one query for the
same time window. This avoids submitting one state-vector query per route.

.. code-block:: python

    from pyopensky.trino import Trino

    trino = Trino()
    routes = [
        ("KJFK", "EGLL"),
        ("KMIA", "EIDW"),
        ("LFPG", "KJFK"),
    ]
    df = trino.history(
        "2025-01-01",
        "2025-01-02",
        routes=routes,
        selected_columns=(
            "time", "icao24", "callsign", "lat", "lon", "baroaltitude",
            "FlightsData4.firstseen", "FlightsData4.lastseen",
            "FlightsData4.estdepartureairport",
            "FlightsData4.estarrivalairport",
        ),
    )

Each pair contains the estimated departure and arrival ICAO identifiers, in that
order. Only the listed pairs match. For example, ``[("KJFK", "EGLL"),
("KMIA", "EIDW")]`` does not include KJFK to EIDW or the reverse directions.
Separate ``departure_airport`` and ``arrival_airport`` lists select every
combination between those lists.

Route identifiers match exactly; SQL wildcards are not expanded. An empty route
list selects no flights. Repeating a pair does not duplicate its results. Other
filters, such as ``icao24``, ``callsign``, bounds and airport filters, further
restrict the selected routes.

For long periods, loop over time windows and include all required routes in each
call. Select the columns needed for the analysis. Actual scan savings depend on
the Trino query plan and table layout.

If only flight metadata is needed, use the same route list with
``trino.flightlist(start, stop, routes=routes)``. With routes alone,
``flightlist()`` filters by ``lastseen``. Setting ``departure_airport`` also
selects its existing departure-time behavior.

.. note::

   ``history()`` returns state vectors within ``start`` and ``stop``. These
   timestamps do not select flights by their departure or arrival time. A flight
   that crosses midnight can be partial in a daily result. ``time_buffer``
   extends the bounds around each flight's ``firstseen`` and ``lastseen`` but
   does not extend the requested state-vector window. Each call submits one
   query; pyopensky does not split that window automatically.

Filtering flight metadata in history queries
--------------------------------------------

``history()`` accepts ``FlightsData4`` conditions alongside state-vector
conditions. Flight columns refer to the flight table joined to the state vectors.
A flight condition also enables this join when no airport or route is specified.
Selecting flight columns also enables this join. Flight schema attributes can
be used directly in ``selected_columns``.

.. code-block:: python

    from pyopensky.schema import FlightsData4, StateVectorsData4
    from pyopensky.trino import Trino

    df = Trino().history(
        "2025-01-01",
        "2025-01-02",
        FlightsData4.estarrivalairporthorizdistance < 5000,
        routes=[("KJFK", "EGLL"), ("LFPG", "KJFK")],
        selected_columns=(
            StateVectorsData4.time,
            StateVectorsData4.icao24,
            FlightsData4.estdepartureairport,
            FlightsData4.estarrivalairport,
        ),
    )

Raw messages for directed routes
--------------------------------

``rawdata()`` also accepts exact ``routes``. It joins raw messages to matching
flights by aircraft address and the flight's ``firstseen``/``lastseen`` interval,
then clips messages to the requested time window. FlightsData4 conditions and
extra columns use this joined flight table.

.. code-block:: python

    from pyopensky.schema import FlightsData4, PositionData4
    from pyopensky.trino import Trino

    df = Trino().rawdata(
        "2025-01-01",
        "2025-01-02",
        FlightsData4.estarrivalairporthorizdistance < 5000,
        routes=[("KJFK", "EGLL"), ("LFPG", "KJFK")],
        Table=PositionData4,
        extra_columns=(FlightsData4.estdepartureairport,),
    )

An empty route list selects no messages. Repeated pairs do not duplicate results.
Airport filters and ``icao24`` further restrict the selected routes. Flight
conditions or flight extra columns enable a flight join even without an airport
or route argument.

``rawdata()`` supports one selection mode per call: flight metadata (airports,
routes, flight conditions or flight extra columns), geographic bounds, or
callsign. These modes cannot be combined. Conditions on the raw table can
further restrict any mode. Its existing raw-column output is preserved.


Spatial queries with bounds
---------------------------

Filter data by geographic bounding box:

.. code-block:: python

    from pyopensky.trino import Trino
    
    trino = Trino()
    
    # Download data within geographic bounds (west, south, east, north)
    df = trino.history(
        start="2023-07-01",
        stop="2023-07-02",
        bounds=(1.06, 43.38, 1.74, 43.85),  # Toulouse area
        StateVectorsData4.baroaltitude < 3048,  # Below 10,000 ft
    )

Spatial functions with SQLAlchemy
---------------------------------

Use Trino's geospatial functions for advanced filtering:

.. code-block:: python

    from sqlalchemy import func
    from pyopensky.schema import StateVectorsData4
    from pyopensky.trino import Trino
    
    trino = Trino()
    
    # Aircraft within 50 nautical miles of an airport
    airport_lat, airport_lon = 52.308601, 4.76389  # Amsterdam Schiphol
    
    df = trino.history(
        "2023-03-01 12:00",
        "2023-03-01 13:00",
        func.ST_Distance(
            func.to_spherical_geography(func.ST_Point(airport_lon, airport_lat)),
            func.to_spherical_geography(
                func.ST_Point(StateVectorsData4.lon, StateVectorsData4.lat)
            ),
        ) <= 50 * 1852,  # 50 nautical miles in meters
        arrival_airport="EHAM",
    )

.. tip::
   The example above finds aircraft within a circular area. This is more accurate than
   rectangular bounds for airport vicinity queries.

Ring-shaped area queries
------------------------

Query aircraft in a specific distance range (e.g., for approach patterns):

.. code-block:: python

    from sqlalchemy import func
    from pyopensky.schema import StateVectorsData4
    from pyopensky.trino import Trino
    
    trino = Trino()
    
    airport_lat, airport_lon = 47.464722, 8.549167  # Zurich Airport
    
    # Aircraft between 49 and 50 nautical miles from airport
    df = trino.history(
        start="2023-01-01",
        stop="2023-02-01",
        func.ST_Distance(
            func.to_spherical_geography(func.ST_Point(airport_lon, airport_lat)),
            func.to_spherical_geography(
                func.ST_Point(StateVectorsData4.lon, StateVectorsData4.lat)
            ),
        ) <= 50 * 1852,
        func.ST_Distance(
            func.to_spherical_geography(func.ST_Point(airport_lon, airport_lat)),
            func.to_spherical_geography(
                func.ST_Point(StateVectorsData4.lon, StateVectorsData4.lat)
            ),
        ) > 49 * 1852,
        arrival_airport="LSZH",
    )

Query data from specific ADS-B receivers
----------------------------------------

Filter by sensor serial numbers to get data from specific ground stations:

.. code-block:: python

    from pyopensky.trino import Trino
    
    trino = Trino()
    
    # Data from specific sensors
    df = trino.history(
        start="2021-08-24 09:00",
        stop="2021-08-24 09:10",
        bounds=(17.8936, 59.6118, 17.9894, 59.6716),
        serials=(-1408232560, -1408232534),  # Specific receiver IDs
    )

.. note::
   Serial numbers can be positive or negative integers. Use a single integer or tuple of integers.

Extending temporal range for airport queries
--------------------------------------------

Use ``time_buffer`` to include state vectors just before a flight's ``firstseen``
or after its ``lastseen``, within the requested time window:

.. code-block:: python

    from pyopensky.trino import Trino
    
    trino = Trino()
    
    # Allow samples up to 30 minutes before/after each flight's time bounds
    df = trino.history(
        start="2024-03-16 09:00",
        stop="2024-03-16 11:00",
        airport="UGTB",
        time_buffer="30m",  # Also accepts "1h", "45min", etc.
    )

The result remains clipped to 09:00 through 11:00. To retrieve samples outside
that window, extend ``start`` or ``stop`` explicitly.

Filtering with boolean expressions
----------------------------------

Use SQLAlchemy expressions for complex filtering:

.. code-block:: python

    from sqlalchemy import not_, func
    from pyopensky.schema import StateVectorsData4
    from pyopensky.trino import Trino
    
    trino = Trino()
    
    # Non-commercial aircraft (negative lookahead for commercial pattern)
    commercial_pattern = "^([A-Z]{3})[0-9](([0-9]{0,3})|([0-9]{0,2})([A-Z])|([0-9]?)([A-Z]{2}))\\s*$"
    
    df = trino.history(
        "2023-07-13",
        "2023-07-14",
        not_(func.regexp_like(StateVectorsData4.callsign, commercial_pattern)),
        limit=1000,
    )

Pattern matching with like()
-----------------------------

Use ``.like()`` for SQL pattern matching with wildcards:

.. code-block:: python

    from sqlalchemy import select
    from pyopensky.schema import StateVectorsData4, FlightsData4
    from pyopensky.trino import Trino
    
    trino = Trino()
    
    # French registered aircraft (icao24 starting with 39)
    query = (
        select(StateVectorsData4)
        .where(StateVectorsData4.icao24.like("39%"))
        .where(StateVectorsData4.hour >= "2023-01-01")
        .where(StateVectorsData4.hour < "2023-01-02")
        .limit(1000)
    )
    df = trino.query(query)
    
    # Multiple character wildcard patterns
    query = (
        select(FlightsData4)
        .where(FlightsData4.callsign.like("AFR%"))  # Air France flights
        .where(FlightsData4.day >= "2023-01-01")
        .where(FlightsData4.day < "2023-02-01")
    )
    df = trino.query(query)

.. tip::
   SQL wildcards: ``%`` matches any sequence of characters, ``_`` matches a single character.

Filtering for non-null values
------------------------------

Use ``!= None`` to filter out rows with missing data:

.. code-block:: python

    from sqlalchemy import select
    from pyopensky.schema import FlightsData4
    from pyopensky.trino import Trino
    
    trino = Trino()
    
    # Only flights with known arrival airport
    query = (
        select(FlightsData4)
        .with_only_columns(
            FlightsData4.icao24,
            FlightsData4.callsign,
            FlightsData4.firstseen,
            FlightsData4.lastseen,
            FlightsData4.estdepartureairport,
            FlightsData4.estarrivalairport,
        )
        .where(FlightsData4.callsign.like("AFR%"))
        .where(FlightsData4.day >= "2023-01-01")
        .where(FlightsData4.day < "2023-02-01")
        .where(FlightsData4.estarrivalairport != None)
        .limit(100)
    )
    df = trino.query(query)

.. note::
   This is useful for filtering flights with complete metadata, especially when working with estimated airport fields that may be null.

Inspecting database schema
--------------------------

Discover available tables and columns:

.. code-block:: python

    from sqlalchemy import MetaData
    from pyopensky.trino import Trino
    
    trino = Trino()
    connection = trino.connect()
    
    # Reflect database schema
    metadata = MetaData()
    metadata.reflect(connection)
    
    # List all tables
    print("Available tables:")
    for table_name in metadata.tables.keys():
        print(f"  - {table_name}")
    
    # List columns in a specific table
    print("\nColumns in state_vectors_data4:")
    for column in metadata.tables["state_vectors_data4"].columns:
        print(f"  - {column.name}: {column.type}")

Available tables
----------------

Common tables in the OpenSky Trino database:

- ``state_vectors_data4``: Historical ADS-B state vectors (position, velocity, altitude)
- ``flights_data4``: Flight metadata (departure/arrival airports, times)
- ``flights_data5``: Enhanced flight data with takeoff/landing times
- ``position_data4``: Raw position messages
- ``velocity_data4``: Raw velocity messages
- ``identification_data4``: Raw identification/callsign messages
- ``rollcall_replies_data4``: Mode S rollcall replies (squawk, BDS registers)
- ``adsc``: ADS-C messages (for oceanic/remote areas)

See also
--------

- :doc:`trino` - Complete API reference for the Trino class
- :doc:`rebuild` - Reconstructing trajectories from raw messages
- `SQLAlchemy Query API <https://docs.sqlalchemy.org/en/latest/core/tutorial.html>`_
- `Trino SQL Functions <https://trino.io/docs/current/functions.html>`_
