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


def download_tzdata_windows(
    year: int = 2022,
    *,
    name: str = "tzdata",
    base_dir: None | Path = None,
) -> None:
    folder = (
        base_dir if base_dir else Path(os.path.expanduser("~")) / "Downloads"
    )

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
