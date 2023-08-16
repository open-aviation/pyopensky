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

DEFAULT_CONFIG = """
[default]
username =
password =
"""

traffic_config_dir = Path(user_config_dir("traffic"))
opensky_config_dir = Path(user_config_dir("pyopensky"))

traffic_config = configparser.ConfigParser()
if (traffic_config_file := (traffic_config_dir / "traffic.conf")).exists():
    traffic_config.read(traffic_config_file.as_posix())

opensky_config = configparser.ConfigParser()
if (opensky_config_file := opensky_config_dir / "secret.conf").exists():
    opensky_config.read(opensky_config_file.as_posix())

cache_dir = user_cache_dir("opensky")
cache_path = Path(cache_dir)
if not cache_path.exists():
    cache_path.mkdir(parents=True)


cache_purge = traffic_config.get("cache", "purge", fallback="90 days")
cache_no_expire = bool(os.environ.get("TRAFFIC_CACHE_NO_EXPIRE"))

if cache_purge != "" and not cache_no_expire:  # coverage: ignore
    expiration = pd.Timestamp("now").timestamp() - pd.Timedelta(cache_purge)

    for cache_file in cache_path.glob("*"):
        ctime = cache_file.stat().st_ctime
        if ctime < expiration.timestamp():
            _log.warn(f"Removing {cache_file} created on {ctime}")
            cache_file.unlink()


def __getattr__(name: str) -> None | str:
    # in order: traffic.conf, opensky.conf, environment variables
    traffic_section = "opensky"

    if name == "http_proxy":
        traffic_section = "network"

    if name == "ssh_proxycommand":
        name = "ssh.proxycommand"
        traffic_section = "network"

    if option := traffic_config.get(traffic_section, name, fallback=None):
        return option

    if option := opensky_config.get("default", name, fallback=None):
        return option

    if name in ["username", "password"]:
        return os.environ.get(f"OPENSKY_{name.upper()}", None)

    if name in ["http_proxy"]:
        return os.environ.get(name)

    if not opensky_config_file.exists():
        opensky_config_file.write_text(DEFAULT_CONFIG)

    if name in __all__:
        return None

    raise AttributeError
