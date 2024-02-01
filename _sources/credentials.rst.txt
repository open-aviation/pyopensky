Credentials
===========

Apply for access
----------------

Access to the Impala database can be granted provide you meet the conditions
defined by The OpenSky Network. Please apply here:
https://opensky-network.org/data/impala 

Access to the Trino database requires separate a login, please create an account on
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

This is your login to OpenSky website, REST API, and Impala service, which shares the same login.

You will identify the folder where the ``settings.conf`` file is located:

.. code:: python

    from pyopensky.config import opensky_config_dir

    print(opensky_config_dir)


Credentials for Impala and Trino
--------------------------------

If you use Trino services in addition to Impala, you should specify
different credentials in the configuration file.

.. code::

    [impala]
    username =
    password =

    [trino]
    username =
    password =

If these sections are commented, the library will use the credentials from the
[default] section. For Trino users, not specify Trino credentials will likely fail.

Fallback configuration
----------------------

If no username and password are specified in the `pyopensky` configuration file, the
following steps are performed in order:

- try to get the credentials from the `traffic <https://traffic-viz.github.io>`_
  configuration file;
- get the credentials from ``$OPENSKY_USERNAME`` and ``$OPENSKY_PASSWORD``
  environment variables;
- open a browser and log in on a dedicated webpage.
