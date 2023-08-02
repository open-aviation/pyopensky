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
    "username",
    "password",
    "http_proxy",
    "cache_path",
    "ssh_proxycommand",
    "ssh.proxycommand",
]

traffic_config_dir = Path(user_config_dir("traffic"))
opensky_config_dir = Path(user_config_dir("opensky"))

traffic_config = configparser.ConfigParser()
if (traffic_config_file := (traffic_config_dir / "traffic.conf")).exists():
    traffic_config.read(traffic_config_file.as_posix())

opensky_config = configparser.ConfigParser()
if (opensky_config_file := opensky_config_dir / "opensky.conf").exists():
    opensky_config.read(opensky_config_file.as_posix())

cache_dir = user_cache_dir("opensky")
cache_path = Path(cache_dir)
if not cache_path.exists():
    cache_path.mkdir(parents=True)

expiration = pd.Timestamp("now", tz="utc") - pd.Timedelta("90 days")
for cache_file in cache_path.glob("*"):
    ctime = cache_file.stat().st_ctime
    if ctime < expiration.timestamp():
        _log.warn(f"Removing {cache_file} created on {ctime}")
        cache_file.unlink()


def __getattr__(name: str) -> None | str:
    # in order: traffic.conf, opensky.conf, environment variables
    section = "opensky"

    if name == "http_proxy":
        section = "network"

    if name == "ssh_proxycommand":
        name = "ssh.proxycommand"
        section = "network"

    if option := traffic_config.get(section, name, fallback=None):
        return option

    if option := opensky_config.get(section, name, fallback=None):
        return option

    if name in ["username", "password"]:
        return os.environ.get(f"opensky_{name}", None)

    if name in ["http_proxy"]:
        return os.environ.get(name)

    if name in __all__:
        return None

    raise AttributeError
