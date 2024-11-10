import os
import sys
from pathlib import Path

import pytest

from pyopensky.trino import Trino  # noqa: F401


@pytest.mark.skipif(sys.platform != "win32", reason="Only relevant for Windows")
def test_tzdata() -> None:
    folder = Path(os.path.expanduser("~")) / "Downloads" / "tzdata"
    assert (folder / "windowsZones.xml").exists()
