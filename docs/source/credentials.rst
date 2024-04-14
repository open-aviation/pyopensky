Credentials
===========

Apply for access
----------------

Access to the Trino database will be granted provided you meet the conditions
defined by The OpenSky Network. Please create an account on
https://trino.opensky-network.org, and then send an email to
contact@opensky-network.org with your Trino username to request for activation.

Configuration
-------------

The first time you use the library, a configuration file named ``settings.conf``
is created, including the following content:

.. code::

    [default]
    username =
    password =

If you use the same login for the OpenSky website (therefore the REST API) and 
for Trino (it will be unified one day), just fill it here.

You will identify the folder where the ``settings.conf`` file is located:

.. code:: python

    from pyopensky.config import opensky_config_dir

    print(opensky_config_dir)


Different credentials per service
---------------------------------

If you want to specify different credentials for the REST API and for the Trino 
database, use the following sections in the configuration file.

.. code::

    [opensky]  # REST API
    username =
    password =

    [trino]  # Trino database
    username =
    password =

If these sections are commented, the library will use the credentials from the
[default] section.

Fallback configuration
----------------------

If no username and password are specified in the `pyopensky` configuration file, the
following steps are performed in order:

- try to get the credentials from the `traffic <https://traffic-viz.github.io>`_
  configuration file;
- get the credentials from ``$OPENSKY_USERNAME`` and ``$OPENSKY_PASSWORD``
  environment variables;
- open a browser and log in on a dedicated webpage.
