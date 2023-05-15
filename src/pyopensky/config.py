from __future__ import annotations

import configparser
import os
from pathlib import Path

from appdirs import user_config_dir

__all__ = [  # noqa: F822
    "access_key",
    "secret_key",
    "username",
    "password",
    "http_proxy",
]

traffic_config_dir = Path(user_config_dir("traffic"))
opensky_config_dir = Path(user_config_dir("opensky"))

traffic_config = configparser.ConfigParser()
if (traffic_config_file := (traffic_config_dir / "traffic.conf")).exists():
    traffic_config.read(traffic_config_file.as_posix())

opensky_config = configparser.ConfigParser()
if (opensky_config_file := opensky_config_dir / "opensky.conf").exists():
    opensky_config.read(opensky_config_file.as_posix())


def __getattr__(name: str) -> None | str:
    # in order: traffic.conf, opensky.conf, environment variables
    section = "network" if name == "http_proxy" else "opensky"

    if option := traffic_config.get(section, name, fallback=None):
        return option

    if option := opensky_config.get(section, name, fallback=None):
        return option

    if name in ["username", "password"]:
        return os.environ.get(f"opensky_{name}", None)

    if name in ["http_proxy"]:
        return os.environ.get(name)

    raise AttributeError
