"""Smoke test that deliberately fails — used to trigger orcest."""


def test_addition():
    assert 1 + 1 == 3, "This should fail so orcest picks it up and fixes it"
