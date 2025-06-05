Credentials
===========

Apply for access
----------------

Access to the Trino database will be granted provided you meet the `conditions
defined by The OpenSky Network <https://opensky-network.org/about/faq#q5>`_. Please create an account on
https://trino.opensky-network.org, and then apply at https://opensky-network.org/data/apply
with your Trino username to request for activation.

Configuration
-------------

The first time you use the library, a configuration file named ``settings.conf``
is created, including the following content:

.. code::

    [default]
    username =
    password =
    client_id =
    client_secret =


The username and password is used to access the Trino database, while the
client ID and client secret are used to access the OpenSky REST API. 
Log in to your OpenSky account and visit the `Account <https://opensky-network.org/my-opensky/account>`_ page in order to create a new API client and retrieve your ``client_id`` and ``client_secret``.

You will identify the folder where the ``settings.conf`` file is located:

.. code:: python

    from pyopensky.config import opensky_config_dir

    print(opensky_config_dir)

Fallback configuration
----------------------

If no username and password are specified in the `pyopensky` configuration file, the
following steps are performed in order:

- try to get the credentials from the `traffic <https://traffic-viz.github.io>`_
  configuration file;
- get the credentials from ``$OPENSKY_USERNAME`` and ``$OPENSKY_PASSWORD``
  environment variables (``$OPENSKY_CLIENT_ID`` and ``$OPENSKY_CLIENT_SECRET`` for the API client);
- open a browser and log in on a dedicated webpage (for Trino).
