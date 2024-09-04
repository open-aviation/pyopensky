from pyopensky.rest import REST

rest = REST()


def test_states() -> None:
    sv = rest.states()
    assert sv.shape[0] > 100
