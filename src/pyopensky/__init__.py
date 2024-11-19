import os
import sys
from importlib.metadata import version
from pathlib import Path

from .tzdata import download_tzdata_windows

__version__ = version("pyopensky")

# Despite the 'win32' string, this value is returned on all versions of Windows,
# including both 32-bit and 64-bit. The 'win32' identifier is a historical
# artifact that has been retained for compatibility with older versions of
# Python.
if sys.platform == "win32":
    conda_env_path = os.getenv('CONDA_PREFIX', None)
    if conda_env_path:
        base_dir = Path(conda_env_path) / "Lib" / "site-packages" / "tzdata"
    else:
        base_dir = Path(os.path.expanduser("~")) / "Downloads"

    download_tzdata_windows(year=2022, base_dir=base_dir)
