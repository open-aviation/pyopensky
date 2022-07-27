Python interface for OpenSky historical database
================================================

Introduction
------------

The ``pyopensky`` library provides the Python interface to OpenSky-network Historical data. It aims at making ADS-B and Mode S data from OpenSky easily accessible in the Python programming environment. 

This Python library provides interfaces to:

1. Query and download OpenSky ADS-B data (state vectors) using `Impala shell <https://opensky-network.org/data/impala>`_. 
2. Query and raw Mode S message (rollcall replies) using `Impala shell <https://opensky-network.org/data/impala>`_. 
3. Decode Mode S messages automatically using `pyModeS <https://github.com/junzis/pyModeS>`_. 


Decoding capabilities
---------------------

In addition to the ability to download ADS-B data (state vectors), it can also retrieve raw Mode S messages in ``rollcall_replies_data4`` table, and then decodes common Mode S Comm-B message types. Currently, automatic decoding of the following Mode S messages are supported:

Mode S Enhanced Surveillance:

- BDS40: Selected vertical intention report
- BDS50: Track and turn report
- BDS60: Heading and speed report

Mode S meteorological reports:

- BDS44: Meteorological routine air report
- BDS45: Meteorological hazard report


Install
-------

1. Install pyopensky
********************

**[Option 1]**: stable release:

.. code-block:: sh

  pip install pyopensky --upgrade

**[Option 2]**: development version:

.. code-block:: sh

  pip install git+https://github.com/junzis/pyopensky --upgrade



2. Obtain access to OpenSky Impala database
*******************************************

Apply access at: https://opensky-network.org/data/impala. The user name and password will be used for the following configuration.


3. Configure OpenSky Impala login
*********************************


The first time you use this library, the following configuration file will be created:

.. code-block::

  ~/.config/pyopensky/secret.conf

with the following content:

.. code-block::

  [default]
  server = data.opensky-network.org
  port = 2230
  username =
  password =

Fill in the empty ``username`` and ``password`` field with your OpenSky login.


Use the library
----------------

1. OpenskyImpalaWrapper
***********************

The ``OpenskyImpalaWrapper`` class can be used to download raw messages, and ADS-B data (aka. OpenSky state vectors).

**Be aware!** The number of records can be massive without the ICAO filter. Thus the query can take a long time. To increase the query efficiency, please consider using an ICAO filter when possible.


Perform Mode S roll-call queries
++++++++++++++++++++++++++++++++

By defined the query type as ``source="rollcall"``, the rollcall Mode S message can be obtained. For example:

.. code-block:: python

  from pyopensky import OpenskyImpalaWrapper

  opensky = OpenskyImpalaWrapper()

  # Perform a simple and massive query (~20k records for 1 second here!)
  df = opensky.query(
      source="rollcall", start="2018-07-01 13:00:00", end="2018-07-01 13:00:01"
  )

  # Perform a query with ICAO filter
  df = opensky.query(
      source="rollcall",
      start="2018-07-01 13:00:00",
      end="2018-07-01 13:00:10",
      icao24=["424588", "3c66a9"],
  )


Perform ADS-B (state vector) queries
++++++++++++++++++++++++++++++++++++

By switching the query type from ``source="rollcall"`` to ``source="adsb"``, you can obtained the history ADS-B information (state vectors) in a similar way. You can also add a boundary (with the format of ``[lat1, lon1, lat2, lon2]``) to the queries. For example:

.. code-block:: python

  from pyopensky import OpenskyImpalaWrapper

  opensky = OpenskyImpalaWrapper()

  # Perform a simple and massive query (~25k records for 5 second here!)
  df = opensky.query(
      source="adsb", start="2018-08-01 13:00:00", end="2018-08-01 13:00:10"
  )

  # Perform a query with ICAO address filter
  df = opensky.query(
      source="adsb",
      start="2018-07-01 13:00:00",
      end="2018-07-01 13:00:10",
      icao24=["424588", "3c66a9"],
      bound=[30, -20, 65, 20],
  )


Perform SQL-like queries
++++++++++++++++++++++++

You can use `rawquery` function to execuate SQL-like query directly. Following lines of code will perform a raw SQL query that counts aircraft which arrived or departed or crossed Frankfurt airport during a certain hour (an example from https://opensky-network.org/data/impala).

.. code-block:: python

  from pyopensky import OpenskyImpalaWrapper

  opensky = OpenskyImpalaWrapper()

  df = opensky.rawquery(
      "SELECT COUNT(DISTINCT icao24) FROM state_vectors_data4 WHERE lat<=50.07 AND lat>=49.98 AND lon<=8.62 AND lon>=8.48 AND hour=1493892000;",
      verbose=True
  )



2. EHSHelper
************

The ``EHSHelper`` class allows the users to download and decode Enhanced Mode S messages automatically.

To get the messages, the query requires an ICAO address (or a list of ICAO addresses), the start time, and the end time for the messages. By default, all BDS40, BDS50, and BDS60 messages are decoded. The results is represented in a pandas ``DataFrame``.

An example is shown as follows:

.. code-block:: python

  from pyopensky import EHSHelper

  ehs = EHSHelper()

  df = ehs.get(
      icao24="4844C6",
      start="2019-10-01 08:00:00",
      end="2019-10-01 08:10:00",
  )

It is also possible to decode a subset of EHS message types, by specify the BDS codes using ``require_bds()`` function. For example:

.. code-block:: python

  ehs.require_bds(["BDS50", "BDS60"])

  df = ehs.get(
      icao24="4844C6",
      start="2019-10-01 08:00:00",
      end="2019-10-01 08:10:00",
  )



3. MeteoHelper
**************

The ``MeteoHelper`` class allows the users to download and decoded meteorological messages automatically. By default it provides information from BDS44 messages. Information from BDS45 messages can also be enable with ``include45=True`` switch.

The interface is similar to ``EHSHelper``, for example:

.. code-block:: python

  from pyopensky import MeteoHelper

  meteo = MeteoHelper()
  df = meteo.get(
      icao24=["341395"],
      start="2020-03-15 19:20:00",
      end="2020-03-15 20:20:00",
      include45=False,
  )



More examples
-------------

More complete examples can be found in the ``test`` directory of this library.


Other information
-----------------

If you find this project useful for your research, please consider citing the following works:

.. code-block:: bibtex

  @inproceedings{sun2019integrating,
    title={Integrating pyModeS and OpenSky Historical Database},
    author={Sun, Junzi and Hoekstra, Jacco M},
    booktitle={Proceedings of the 7th OpenSky Workshop},
    volume={67},
    pages={63--72},
    year={2019}
  }

  @article{sun2019pymodes,
      title={pyModeS: Decoding Mode S Surveillance Data for Open Air Transportation Research},
      author={J. {Sun} and H. {V\^u} and J. {Ellerbroek} and J. M. {Hoekstra}},
      journal={IEEE Transactions on Intelligent Transportation Systems},
      year={2019},
      doi={10.1109/TITS.2019.2914770},
      ISSN={1524-9050},
  }

  @inproceedings{schafer2014opensky,
    title={Bringing up OpenSky: A large-scale ADS-B sensor network for research},
    author={Sch{\"a}fer, Matthias and Strohmeier, Martin and Lenders, Vincent and Martinovic, Ivan and Wilhelm, Matthias},
    booktitle={Proceedings of the 13th international symposium on Information processing in sensor networks},
    pages={83--94},
    year={2014},
    organization={IEEE Press}
  }
