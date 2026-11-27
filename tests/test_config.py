"""Cache expiration checks use mocked files, never the user's cache."""

from configparser import ConfigParser
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import pandas as pd
from pyopensky import config


def test_purge_only_removes_expired_files(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENSKY_CACHE_NO_EXPIRE", "")
    monkeypatch.setenv("TRAFFIC_CACHE_NO_EXPIRE", "")
    settings = ConfigParser()
    settings.read_dict({"cache": {"purge": "1 day"}})
    monkeypatch.setattr(config, "opensky_config", settings)
    monkeypatch.setattr(config, "traffic_config", ConfigParser())
    now = pd.Timestamp.now().timestamp()
    old = Mock()
    old.stat.return_value = SimpleNamespace(st_ctime=now - 2 * 86400)
    recent = Mock()
    recent.stat.return_value = SimpleNamespace(st_ctime=now - 60)
    folder = Mock(spec=Path)
    folder.glob.return_value = [old, recent]
    config.purge_cache(folder)
    old.unlink.assert_called_once()
    recent.unlink.assert_not_called()


def test_no_expire_preserves_files(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENSKY_CACHE_NO_EXPIRE", "1")
    folder = Mock(spec=Path)
    config.purge_cache(folder)
    folder.glob.assert_not_called()
