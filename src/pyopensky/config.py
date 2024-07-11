from __future__ import annotations

import configparser
import logging
import os
from pathlib import Path
from typing import TypedDict

from appdirs import user_cache_dir, user_config_dir

import pandas as pd

_log = logging.getLogger(__name__)


DEFAULT_CONFIG = """
[default]
username =
password =

# Uncomment the following if you have access to the s3 server
# secret_key =
# access_key =

# If you need a different account on Trino, uncomment the following
# [trino]
# username =
# password =

[cache]
## You may set a different cache folder here if you have a preferred space
## for storing possibly large files.
## Default settings:
## - $HOME/.cache/opensky (Linux)
## - $HOME/Library/Caches/opensky (MacOS)
## - C:\\Users\\<username>\\AppData\\Local\\opensky\\Cache (Windows)
##
# path =

## The purge parameter refers to the cache folder containing data from the
## Opensky databases. Cache files tend to take space so you may want to let
## the library delete older files.
## Files are removed when the library is imported.
purge = 90 days
"""

opensky_config_dir = Path(user_config_dir("pyopensky"))
opensky_config = configparser.ConfigParser()

if (opensky_config_file := opensky_config_dir / "settings.conf").exists():
    opensky_config.read(opensky_config_file.as_posix())
elif (previous_config_file := opensky_config_dir / "secret.conf").exists():
    previous_config_file.rename(opensky_config_file)
    opensky_config.read(opensky_config_file.as_posix())
else:
    if not opensky_config_dir.exists():
        opensky_config_dir.mkdir(parents=True)
    opensky_config_file.write_text(DEFAULT_CONFIG)

traffic_config_dir = Path(user_config_dir("traffic"))
traffic_config = configparser.ConfigParser()

if (traffic_config_file := traffic_config_dir / "traffic.conf").exists():
    traffic_config.read(traffic_config_file.as_posix())


def purge_cache(cache_path: Path) -> None:
    cache_no_expire = False
    cache_no_expire |= bool(os.environ.get("TRAFFIC_CACHE_NO_EXPIRE"))
    cache_no_expire |= bool(os.environ.get("OPENSKY_CACHE_NO_EXPIRE"))

    cache_purge = "90 days"
    if purge := traffic_config.get("cache", "purge", fallback=None):
        if purge.strip() != "":
            cache_purge = purge
    if purge := opensky_config.get("cache", "purge", fallback=None):
        if purge.strip() != "":
            cache_purge = purge

    if not cache_no_expire:  # coverage: ignore
        expiration = pd.Timestamp("now") - pd.Timedelta(cache_purge)

        for cache_file in cache_path.glob("*"):
            ctime = cache_file.stat().st_ctime
            if ctime < expiration.timestamp():
                _log.warn(f"Removing {cache_file} created on {ctime}")
                cache_file.unlink()


class Resolution(TypedDict, total=False):
    opensky_category: str
    opensky_name: str
    traffic_category: str
    traffic_name: str
    environment_variable: str


NAME_RESOLUTION: dict[str, Resolution] = {
    # Cache configuration
    "cache_dir": dict(
        environment_variable="OPENSKY_CACHE",
        opensky_category="cache",
        opensky_name="path",
        traffic_category="cache",
        traffic_name="path",
    ),
    # Network configuration
    "http_proxy": dict(
        environment_variable="http_proxy",
        opensky_category="default",
        opensky_name="http_proxy",
        traffic_category="network",
        traffic_name="http_proxy",
    ),
    # Credentials configuration
    "access_key": dict(
        opensky_category="s3",
        opensky_name="access_key",
        traffic_category="opensky",
        traffic_name="access_key",
    ),
    "secret_key": dict(
        opensky_category="s3",
        opensky_name="secret_key",
        traffic_category="opensky",
        traffic_name="secret_key",
    ),
    "username": dict(
        environment_variable="OPENSKY_USERNAME",
        opensky_category="default",
        opensky_name="username",
        traffic_category="opensky",
        traffic_name="username",
    ),
    "password": dict(
        environment_variable="OPENSKY_PASSWORD",
        opensky_category="default",
        opensky_name="password",
        traffic_category="opensky",
        traffic_name="password",
    ),
    "trino_username": dict(
        environment_variable="OPENSKY_TRINO_USERNAME",
        opensky_category="trino",
        opensky_name="username",
    ),
    "trino_password": dict(
        environment_variable="OPENSKY_TRINO_PASSWORD",
        opensky_category="trino",
        opensky_name="password",
    ),
}

__all__ = list(NAME_RESOLUTION.keys())


def get_config(
    opensky_category: str,
    opensky_name: str,
    traffic_category: None | str = None,
    traffic_name: None | str = None,
    environment_variable: None | str = None,
) -> None | str:
    if opensky_value := opensky_config.get(
        opensky_category, opensky_name, fallback=None
    ):
        return opensky_value

    if traffic_category is not None and traffic_name is not None:
        if traffic_value := traffic_config.get(
            traffic_category, traffic_name, fallback=None
        ):
            return traffic_value

    if environment_variable is not None:
        return os.environ.get(environment_variable)

    return None


cache_dir = get_config(**NAME_RESOLUTION["cache_dir"])
if cache_dir is None:
    cache_dir = user_cache_dir("opensky")
cache_path = Path(cache_dir)
if not cache_path.exists():
    cache_path.mkdir(parents=True)

purge_cache(cache_path)


def __getattr__(name: str) -> None | str:
    # Pick in order:
    # 1. pyopensky -> settings.conf
    # 2. traffic -> traffic.conf
    # 3. environment variables

    if name == "trino_username":
        if value := get_config(**NAME_RESOLUTION[name]):
            return value
        return get_config(**NAME_RESOLUTION["username"])

    if name == "trino_password":
        if value := get_config(**NAME_RESOLUTION[name]):
            return value
        return get_config(**NAME_RESOLUTION["password"])

    if name in __all__:
        return get_config(**NAME_RESOLUTION[name])

    raise AttributeError
