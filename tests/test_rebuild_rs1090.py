"""Offline checks for decoding iterators with the standard-library batched."""

import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import pandas as pd
from pyopensky.decoders.rs1090 import Rs1090Decoder


@pytest.mark.parametrize("count", [1, 5001])
def test_rs1090_batches_decoded_iterator(
    count: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    frames = pd.DataFrame({"rawmsg": ["example"], "mintime": [1000.0]})
    decode = Mock(
        return_value=iter(
            {"timestamp": 1000.0 + i, "latitude": 50.0} for i in range(count)
        )
    )
    monkeypatch.setitem(sys.modules, "rs1090", SimpleNamespace(decode=decode))
    result = Rs1090Decoder().decode_position(frames)
    assert len(result) == count
    assert result.latitude.eq(50).all()
    assert (
        result.timestamp.tolist()
        == pd.date_range(
            "1970-01-01 00:16:40Z", periods=count, freq="s"
        ).tolist()
    )
    decode.assert_called_once()
