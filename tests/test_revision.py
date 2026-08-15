"""Tests for source-revision attestation."""

import json

import pytest
from click.testing import CliRunner

from orcest.cli import main
from orcest.revision import (
    UNKNOWN_REVISION,
    get_build_revision,
    normalize_revision,
    revision_is_attested,
)

pytestmark = pytest.mark.unit


def test_normalize_revision_rejects_untrusted_values():
    assert normalize_revision("abc1234") == "abc1234"
    assert normalize_revision("ABCDEF012345") == "abcdef012345"
    assert normalize_revision("abc1234-dirty") == "abc1234-dirty"
    assert normalize_revision("$(touch /tmp/nope)") is None
    assert normalize_revision("unknown") is None
    assert revision_is_attested("ABCDEF0-DIRTY") is False


def test_environment_revision_has_priority(monkeypatch):
    monkeypatch.setenv("ORCEST_BUILD_REVISION", "a" * 40)

    assert get_build_revision() == "a" * 40


def test_unknown_environment_does_not_mask_generated_or_system_sources(monkeypatch, mocker):
    monkeypatch.setenv("ORCEST_BUILD_REVISION", UNKNOWN_REVISION)
    mocker.patch("orcest.revision._checkout_revision", return_value=None)
    mocker.patch("orcest.revision._generated_revision", return_value="b" * 40)

    assert get_build_revision() == "b" * 40


def test_checkout_revision_has_priority_over_stale_system_marker(monkeypatch, mocker):
    monkeypatch.delenv("ORCEST_BUILD_REVISION", raising=False)
    mocker.patch("orcest.revision._generated_revision", return_value="b" * 40)
    mocker.patch("orcest.revision._checkout_revision", return_value="c" * 40)
    mocker.patch("orcest.revision._system_revision", return_value="d" * 40)

    assert get_build_revision() == "c" * 40


def test_revision_cli_json(monkeypatch):
    monkeypatch.setenv("ORCEST_BUILD_REVISION", "c" * 40)

    result = CliRunner().invoke(main, ["revision", "--json"])

    assert result.exit_code == 0
    assert json.loads(result.output) == {"attested": True, "revision": "c" * 40}


def test_revision_cli_marks_dirty_checkout_unattested(monkeypatch):
    monkeypatch.setenv("ORCEST_BUILD_REVISION", f"{'d' * 40}-dirty")

    result = CliRunner().invoke(main, ["revision", "--json"])

    assert result.exit_code == 0
    assert json.loads(result.output) == {
        "attested": False,
        "revision": f"{'d' * 40}-dirty",
    }
