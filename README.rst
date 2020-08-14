Python interface for OpenSky database with pyModeS decoder
===========================================================

Introduction
---------------------

This Python library provides interfaces to:

1. Query raw and ADS-B messages from OpenSky Impala database.
2. Decode OpenSky Comm-B information automatically using pyModeS.


The ``pyopensky`` connects the `pyModeS <https://github.com/junzis/pyModeS>`_ decoder and OpenSky-network raw Mode-S data. It aims at making the Enhance Mode-S information form OpenSky network more accessible for researchers. 

It can automatically retrieve and download data in ``rollcall_replies_data4`` table from the `OpenSky Impala database <https://opensky-network.org/data/impala>`_, and then decodes several common Mode-S Comm-B message types. Currently, follows Mode-S downlink reports are supported:

**Enhanced Mode-S:**

- BDS40: Selected vertical intention report
- BDS50: Track and turn report
- BDS60: Heading and speed report

**Mode-S meteorological information:**

- BDS44: Meteorological routine air report
- BDS45: Meteorological hazard report


Install
-----------------------

In order to successfully use this library, you need:

**1. Get the ``pyModeS`` library**

Install the up-to-date pyModeS version from PyPI:

.. code-block:: sh

  $ pip install pyModeS --upgrade

Install this library:

.. code-block:: sh

  $ pip install pyopensky
  or
  $ pip install git+https://github.com/junzis/pyopensky



**2. Obtain access to OpenSky Impala database**

Apply access at: https://opensky-network.org/data/impala. The user name and password will be used for the following configuration.


**3. Configure OpenSky Impala login**

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
-----------------

EHSHelper
**********

The ``EHSHelper`` class allows the users to download and decode Enhanced Mode-S messages automatically.

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


MeteoHelper
************

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

OpenskyImpalaWrapper
**********************

All previous queries are based on the ``OpenskyImpalaWrapper`` class from the library. The wrapper class can also be used independently to query OpenSky Impala database. It can be used for raw messages, as wells as decoded ADS-B data by OpenSky.

**Be aware!** The number of records can be massive without the ICAO filter. Thus the query can take a long time. To increase the query efficiency, please consider using a ICAO filter when possible.

By defined the query type as ``type="raw"``, the raw Mode-S message can be obtained. For example:

.. code-block:: python

  from pyopensky import OpenskyImpalaWrapper

  opensky = OpenskyImpalaWrapper()

  # Perform a simple and massive query (~20k records for 1 second here!)
  df = opensky.query(
      type="raw", start="2018-07-01 13:00:00", end="2018-07-01 13:00:01"
  )

  # Perform a query with ICAO filter
  df = opensky.query(
      type="raw",
      start="2018-07-01 13:00:00",
      end="2018-07-01 13:00:10",
      icao24=["424588", "3c66a9"],
  )

By switching the query type from ``type="raw"`` to ``type="adsb"``, you can obtained the history ADS-B information in a similar way. You can also add a boundary (with format of ``[lat1, lon1, lat2, lon2]``) to the queries. For example:

.. code-block:: python

  from pyopensky import OpenskyImpalaWrapper

  opensky = OpenskyImpalaWrapper()

  # Perform a simple and massive query (~25k records for 5 second here!)
  df = opensky.query(
      type="adsb", start="2018-08-01 13:00:00", end="2018-08-01 13:00:10"
  )

  # Perform a query with ICAO address filter
  df = opensky.query(
      type="adsb",
      start="2018-07-01 13:00:00",
      end="2018-07-01 13:00:10",
      icao24=["424588", "3c66a9"],
      bound=[30, -20, 65, 20],
  )


More examples
--------------

More complete examples can be found in the ``test`` directory of this library.


Other information
-------------------
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
      title={pyModeS: Decoding Mode-S Surveillance Data for Open Air Transportation Research},
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
