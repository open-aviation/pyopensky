# While Arrow uses the OS-provided timezone database on Linux and macOS, it
# requires a user-provided database on Windows. You must download and extract
# the text version of the IANA timezone database and add the Windows timezone
# mapping XML.
# https://arrow.apache.org/docs/cpp/build_system.html#runtime-dependencies

from __future__ import annotations

import os
import tarfile
from pathlib import Path

import httpx

rk = "Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\Shell Folders"


def download_tzdata_windows(
    year: int = 2022,
    *,
    name: str = "tzdata",
    base_dir: None | Path = None,
) -> None:
    # This module only exists in Windows
    from winreg import HKEY_CURRENT_USER, OpenKey, QueryValueEx  # type: ignore

    if base_dir is None:
        conda_env_path = os.getenv("CONDA_PREFIX", None)
        if conda_env_path:
            base_dir = Path(conda_env_path) / "Lib" / "site-packages" / "tzdata"
        else:
            # This \o/ line finds the Downloads directory for the current user
            with OpenKey(HKEY_CURRENT_USER, rk) as key:
                download_path, _ = QueryValueEx(
                    key,
                    # don't ask what this uuid stands for...
                    "{374DE290-123F-4565-9164-39C4925E467B}",
                )
                base_dir = Path(download_path)

    folder = base_dir

    if (folder / name).is_dir():
        return

    tz_path = folder / "tzdata.tar.gz"

    c = httpx.get(
        f"https://data.iana.org/time-zones/releases/tzdata{year}f.tar.gz"
    )
    c.raise_for_status()
    tz_path.write_bytes(c.content)

    extracted_folder = folder / name

    if not extracted_folder.exists():
        extracted_folder.mkdir(parents=True)

    tarfile.open(tz_path).extractall(extracted_folder)

    c = httpx.get(
        "https://raw.githubusercontent.com/unicode-org/cldr/master/common/supplemental/windowsZones.xml"
    )
    c.raise_for_status()
    (extracted_folder / "windowsZones.xml").write_bytes(c.content)
