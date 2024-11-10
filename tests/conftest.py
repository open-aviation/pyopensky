import sys

from pyopensky.tzdata import download_tzdata_windows


def pytest_configure(config) -> None:  # type: ignore
    if sys.platform == "win32":
        download_tzdata_windows(year=2022)
