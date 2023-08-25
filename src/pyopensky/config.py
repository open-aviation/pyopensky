from __future__ import annotations

import configparser
import logging
import os
from pathlib import Path

from appdirs import user_cache_dir, user_config_dir

import pandas as pd

_log = logging.getLogger(__name__)

__all__ = [  # noqa: F822
    "access_key",
    "secret_key",
    "impala_username",
    "impala_password",
    "trino_username",
    "trino_password",
    "http_proxy",
    "cache_path",
    "ssh_proxycommand",
    "ssh.proxycommand",
]

DEFAULT_CONFIG = """
[default]
username =
password =

[trino]
username =
password =

[cache]
## You can use a different cache folder if data is large
## default to: $HOME/.cache/pyopensky on Linux
# path = 

## The purge cache folder after certain days
## purge happens when the library is imported.
purge = 90 days

"""

opensky_config_dir = Path(user_config_dir("pyopensky"))
opensky_config = configparser.ConfigParser()

if (opensky_config_file := opensky_config_dir / "settings.conf").exists():
    opensky_config.read(opensky_config_file.as_posix())
else:
    opensky_config_dir.mkdir(parents=True)
    opensky_config_file.write_text(DEFAULT_CONFIG)

cache_dir = user_cache_dir("opensky")
cache_path = Path(cache_dir)
if not cache_path.exists():
    cache_path.mkdir(parents=True)


cache_purge = opensky_config.get("cache", "purge", fallback="90 days")
cache_no_expire = bool(os.environ.get("OPENSKY_CACHE_NO_EXPIRE"))

if cache_purge != "" and not cache_no_expire:  # coverage: ignore
    expiration = pd.Timestamp("now") - pd.Timedelta(cache_purge)

    for cache_file in cache_path.glob("*"):
        ctime = cache_file.stat().st_ctime
        if ctime < expiration.timestamp():
            _log.warn(f"Removing {cache_file} created on {ctime}")
            cache_file.unlink()


def __getattr__(name: str) -> None | str:
    # Pick in order:
    # 1. pyopensky -> settings.conf
    # 2. environment variables

    traffic_section = "opensky"

    if name == "http_proxy":
        traffic_section = "network"

    if name == "ssh_proxycommand":
        name = "ssh.proxycommand"
        traffic_section = "network"

    if name == "username":
        return opensky_config.get("default", "username", fallback=None)

    if name == "password":
        return opensky_config.get("default", "password", fallback=None)

    if name == "trino_username":
        return opensky_config.get("trino", "username", fallback=None)

    if name == "trino_password":
        return opensky_config.get("trino", "password", fallback=None)

    if name in ["http_proxy"]:
        return os.environ.get(name)

    if name in __all__:
        return None

    raise AttributeError
