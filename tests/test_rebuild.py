"""Tests for the Rebuild class and decoder functionality."""

import pytest

import pandas as pd
from pyopensky.rebuild import Rebuild
from pyopensky.trino import Trino


class TestRebuildClass:
    """Test the Rebuild class functionality."""

    @pytest.fixture
    def trino(self) -> Trino:
        """Create a Trino instance for testing."""
        return Trino()

    @pytest.fixture
    def rebuild(self, trino: Trino) -> Rebuild:
        """Create a Rebuild instance for testing."""
        return Rebuild(trino)

    def test_rebuild_initialization(self, trino: Trino) -> None:
        """Test Rebuild can be initialized with Trino instance."""
        rebuild = Rebuild(trino)
        assert rebuild.trino is trino

    def test_rebuild_basic_query(self, rebuild: Rebuild) -> None:
        """Test basic rebuild without decoder."""
        df = rebuild.rebuild(
            start="2023-01-03 16:00:00",
            stop="2023-01-03 20:00:00",
            icao24="400a0e",
        )

        # Should return a DataFrame
        assert df is not None
        assert isinstance(df, pd.DataFrame)

        # Should have records
        assert df.shape[0] > 0

        # Should have key columns
        assert "icao24" in df.columns
        assert "timestamp" in df.columns
        assert "mintime" in df.columns

    def test_rebuild_with_string_decoder_rs1090(self, rebuild: Rebuild) -> None:
        """Test rebuild with 'rs1090' string decoder."""
        df = rebuild.rebuild(
            start="2023-01-03 16:00:00",
            stop="2023-01-03 17:00:00",
            icao24="400a0e",
            decoder="rs1090",
        )

        # If data is returned, it should be a DataFrame
        if df is not None:
            assert isinstance(df, pd.DataFrame)
