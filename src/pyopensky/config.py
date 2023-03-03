import configparser
from pathlib import Path

from appdirs import user_config_dir

__all__ = ["access_key", "secret_key", "username", "password"]  # noqa: F822

config_dir = Path(user_config_dir("traffic"))
config_file = config_dir / "traffic.conf"

config = configparser.ConfigParser()
config.read(config_file.as_posix())


def __getattr__(name: str) -> str:
    if option := config.get("opensky", name, fallback=None):
        return option
    raise AttributeError
