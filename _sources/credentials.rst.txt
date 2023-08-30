Credentials
===========

Apply for access
----------------

Access to the Impala database can be granted provide you meet the conditions
defined by The OpenSky Network. Please apply here:
https://opensky-network.org/data/impala 

For access to the Trino database, please create an account on
https://trino.opensky-network.org and specify you want access in the Impala
form. If you already have access to Impala, send an email to
contact@opensky-network.org.

Configuration
-------------

The first time you use the library, a configuration file named ``settings.conf``
is created, including the following content:

.. code::

    [default]
    username =
    password =

You will identify the folder where the ``settings.conf`` file is located:

.. code:: python

    from pyopensky.config import opensky_config_dir

    print(opensky_config_dir)

Different credentials for all services
--------------------------------------

If you use different credentials for one of the services, you may specify
different credentials in the configuration file.

.. code::

    [impala]
    username =
    password =

or 

.. code::

    [trino]
    username =
    password =

If those sections are commented, the library will get the credentials from the
[default] section.

If no username and password are specified in the configuration file, the
following steps are performed in order:

- try to get the credentials from the `traffic <https://traffic-viz.github.io>`_
  configuration file;
- get the credentials from ``$OPENSKY_USERNAME`` and ``$OPENSKY_PASSWORD``
  environment variables;
- open a browser and log in on a dedicated webpage.
