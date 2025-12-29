Rebuilding trajectories
=======================

The :class:`~pyopensky.rebuild.Rebuild` class provides functionality for reconstructing aircraft trajectories from raw ADS-B and Mode S messages.

When to use the rebuild functionality
--------------------------------------

There are two main use cases for the ``Rebuild`` class:

1. **Merge different data sources to rebuild a state vector table**: Combine position, velocity, identification, and rollcall data that are stored in separate database tables into a single trajectory dataframe.

2. **Redecode raw messages**: Apply custom decoding logic to raw messages for more precise position calculations, BDS register extraction, or custom validation.

Rebuilding without custom decoding
-----------------------------------

If you only need to merge data from different sources and trust the database-decoded values, use the ``rebuild()`` method without specifying a decoder:

.. code-block:: python

    from pyopensky.trino import Trino
    from pyopensky.rebuild import Rebuild

    trino = Trino()
    rebuild = Rebuild(trino)

    # Fetch and merge position, velocity, identification data
    df = rebuild.rebuild(
        start="2023-01-03 16:00",
        stop="2023-01-03 20:00",
        icao24="400A0E"
    )

This returns a dataframe with columns from all three sources merged on ``icao24`` and ``mintime`` with a 5-second tolerance.

Rebuilding with custom decoding
--------------------------------

When you need more control over position decoding or want to extract additional data from raw messages, specify a decoder:

.. code-block:: python

    from pyopensky.trino import Trino
    from pyopensky.rebuild import Rebuild

    trino = Trino()
    rebuild = Rebuild(trino)

    # Redecode position data with PyModesDecoder
    df = rebuild.rebuild(
        start="2023-01-03 16:00",
        stop="2023-01-03 20:00",
        icao24="400A0E",
        decoder="pymodes"  # Use string-based decoder selection
    )

The decoder will process the raw messages and apply custom logic for CPR position decoding, validation, and filtering.

Available decoders
------------------

PyModes decoder
~~~~~~~~~~~~~~~

The :class:`~pyopensky.decoders.PyModesDecoder` uses the `pyModeS <https://github.com/junzis/pyModeS>`_ library for decoding.

**Features:**

- CPR position decoding with odd/even frame pairing
- Reference position validation to filter outliers
- BDS register decoding (BDS 4,0, BDS 5,0, BDS 6,0)
- Pure Python implementation

**Installation:**

.. code-block:: bash

    pip install 'pyopensky[pymodes]'

**Usage:**

.. code-block:: python

    from pyopensky.decoders import PyModesDecoder
    from pyopensky.rebuild import Rebuild
    from pyopensky.trino import Trino

    trino = Trino()
    rebuild = Rebuild(trino)

    # Using string-based selection
    df = rebuild.redecode_position(
        start="2023-01-03 16:00",
        stop="2023-01-03 20:00",
        icao24="400A0E",
        decoder="pymodes"
    )

    # Or instantiate the decoder directly
    decoder = PyModesDecoder()
    df = rebuild.redecode_position(
        start="2023-01-03 16:00",
        stop="2023-01-03 20:00",
        icao24="400A0E",
        decoder=decoder
    )

Rs1090 decoder
~~~~~~~~~~~~~~

The :class:`~pyopensky.decoders.Rs1090Decoder` uses the Rust-based `rs1090 <https://github.com/xoolive/rs1090>`_ library for faster decoding.

**Features:**

- High-performance Rust implementation
- Same decoding logic as PyModesDecoder
- Significantly faster for large datasets

**Installation:**

.. code-block:: bash

    pip install 'pyopensky[rs1090]'

**Usage:**

.. code-block:: python

    from pyopensky.rebuild import Rebuild
    from pyopensky.trino import Trino

    trino = Trino()
    rebuild = Rebuild(trino)

    # Using string-based selection
    df = rebuild.redecode_position(
        start="2023-01-03 16:00",
        stop="2023-01-03 20:00",
        icao24="400A0E",
        decoder="rs1090"
    )

Custom decoder
~~~~~~~~~~~~~~

You can implement your own decoder by subclassing :class:`~pyopensky.decoders.Decoder`:

.. code-block:: python

    from pyopensky.decoders import Decoder
    import pandas as pd

    class MyCustomDecoder(Decoder):
        def decode_position(self, df: pd.DataFrame) -> pd.DataFrame:
            # Custom position decoding logic
            return df

        def decode_velocity(self, df: pd.DataFrame) -> pd.DataFrame:
            # Custom velocity decoding logic
            return df

        def decode_identification(self, df: pd.DataFrame) -> pd.DataFrame:
            # Custom identification decoding logic
            return df

        def decode_rollcall(self, df: pd.DataFrame) -> pd.DataFrame:
            # Custom rollcall decoding logic
            return df

    # Use your custom decoder
    decoder = MyCustomDecoder()
    df = rebuild.rebuild(
        start="2023-01-03 16:00",
        stop="2023-01-03 20:00",
        icao24="400A0E",
        decoder=decoder
    )

Performance considerations
---------------------------

**When to use PyModesDecoder vs Rs1090Decoder:**

- Use ``PyModesDecoder`` for small to medium datasets (< 1M messages) or when you need to inspect the decoding logic
- Use ``Rs1090Decoder`` for large datasets (> 1M messages) where performance is critical
- Both decoders produce equivalent results, Rs1090Decoder is simply faster

**Database vs custom decoding:**

- Database-decoded positions are already validated and filtered by OpenSky
- Custom decoding gives you full control over CPR frame pairing and outlier filtering
- Use custom decoding when you need BDS register data or have specific validation requirements

**Merge tolerance:**

All merges use a 5-second tolerance by default. This means:

- Position and velocity data must be within 5 seconds to be merged
- Identification data must be within 5 seconds of position data
- Rollcall data must be within 5 seconds of position data

This tolerance balances data completeness with temporal accuracy.


.. autoclass:: pyopensky.rebuild.Rebuild
    :members:
    :inherited-members:
    :no-undoc-members:
    :show-inheritance:
