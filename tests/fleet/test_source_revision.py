"""Tests for orcest.fleet.source_revision."""

from __future__ import annotations

import subprocess

import pytest

from orcest.fleet.config import DesiredSourceConfig
from orcest.fleet.source_revision import (
    DesiredRevision,
    RuntimeRevision,
    evaluate_source_revision,
    resolve_desired_revision,
)

pytestmark = pytest.mark.unit

SHA_A = "a" * 40
SHA_B = "b" * 40


def _completed(returncode=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(
        ["git", "ls-remote"], returncode, stdout=stdout, stderr=stderr
    )


# ── resolve_desired_revision ─────────────────────────────────


class TestResolveDesiredRevision:
    def test_unconfigured(self):
        resolved = resolve_desired_revision(DesiredSourceConfig())
        assert resolved.sha is None
        assert resolved.resolved is False
        assert resolved.error == "desired revision unconfigured"

    def test_sha_pin_resolves_without_network_call(self, mocker):
        run = mocker.patch("orcest.fleet.source_revision._run_ls_remote")
        desired = DesiredSourceConfig(repo="org/orcest", sha=SHA_A)
        resolved = resolve_desired_revision(desired)
        assert resolved.sha == SHA_A
        assert resolved.error is None
        run.assert_not_called()

    def test_ref_resolves_exact_sha(self, mocker):
        mocker.patch(
            "orcest.fleet.source_revision._run_ls_remote",
            return_value=_completed(stdout=f"{SHA_A}\trefs/heads/master\n"),
        )
        desired = DesiredSourceConfig(repo="org/orcest", ref="refs/heads/master")
        resolved = resolve_desired_revision(desired)
        assert resolved.sha == SHA_A
        assert resolved.error is None

    def test_ref_resolution_is_case_insensitive(self, mocker):
        mocker.patch(
            "orcest.fleet.source_revision._run_ls_remote",
            return_value=_completed(stdout=f"{SHA_A.upper()}\trefs/heads/master\n"),
        )
        desired = DesiredSourceConfig(repo="org/orcest", ref="refs/heads/master")
        resolved = resolve_desired_revision(desired)
        assert resolved.sha == SHA_A

    def test_timeout_is_unknown_not_current(self, mocker):
        mocker.patch(
            "orcest.fleet.source_revision._run_ls_remote",
            side_effect=subprocess.TimeoutExpired(cmd="git", timeout=10),
        )
        desired = DesiredSourceConfig(repo="org/orcest", ref="refs/heads/master")
        resolved = resolve_desired_revision(desired)
        assert resolved.sha is None
        assert "timed out" in resolved.error

    def test_ref_not_found(self, mocker):
        mocker.patch(
            "orcest.fleet.source_revision._run_ls_remote",
            return_value=_completed(returncode=2, stdout="", stderr="fatal: no such ref"),
        )
        desired = DesiredSourceConfig(repo="org/orcest", ref="refs/heads/nope")
        resolved = resolve_desired_revision(desired)
        assert resolved.sha is None
        assert resolved.error == "desired ref not found"

    def test_authentication_failure(self, mocker):
        mocker.patch(
            "orcest.fleet.source_revision._run_ls_remote",
            return_value=_completed(
                returncode=128,
                stderr=(
                    "fatal: could not read Username for "
                    "'https://x-access-token:s3cr3t@github.com': terminal prompts disabled"
                ),
            ),
        )
        desired = DesiredSourceConfig(repo="org/orcest", ref="refs/heads/master")
        resolved = resolve_desired_revision(desired)
        assert resolved.sha is None
        assert resolved.error == "desired ref authentication failed"
        # Secret-redaction: the embedded token must never leak into the report.
        assert "s3cr3t" not in resolved.error
        assert "x-access-token" not in resolved.error

    def test_malformed_response_not_hex(self, mocker):
        mocker.patch(
            "orcest.fleet.source_revision._run_ls_remote",
            return_value=_completed(stdout="not-a-sha\trefs/heads/master\n"),
        )
        desired = DesiredSourceConfig(repo="org/orcest", ref="refs/heads/master")
        resolved = resolve_desired_revision(desired)
        assert resolved.sha is None
        assert resolved.error == "desired ref response was malformed"

    def test_malformed_response_short_hash(self, mocker):
        mocker.patch(
            "orcest.fleet.source_revision._run_ls_remote",
            return_value=_completed(stdout="abc123\trefs/heads/master\n"),
        )
        desired = DesiredSourceConfig(repo="org/orcest", ref="refs/heads/master")
        resolved = resolve_desired_revision(desired)
        assert resolved.sha is None

    def test_empty_response_is_malformed(self, mocker):
        mocker.patch(
            "orcest.fleet.source_revision._run_ls_remote",
            return_value=_completed(stdout=""),
        )
        desired = DesiredSourceConfig(repo="org/orcest", ref="refs/heads/master")
        resolved = resolve_desired_revision(desired)
        assert resolved.sha is None
        assert resolved.error == "desired ref response was malformed"

    def test_oversized_response_is_rejected(self, mocker):
        mocker.patch(
            "orcest.fleet.source_revision._run_ls_remote",
            return_value=_completed(stdout=(f"{SHA_A}\trefs/heads/master\n" + "x" * 5000)),
        )
        desired = DesiredSourceConfig(repo="org/orcest", ref="refs/heads/master")
        resolved = resolve_desired_revision(desired)
        assert resolved.sha is None
        assert resolved.error == "desired ref response was oversized"

    def test_generic_git_failure_never_echoes_stderr(self, mocker):
        mocker.patch(
            "orcest.fleet.source_revision._run_ls_remote",
            return_value=_completed(returncode=1, stderr="some unclassified git internal detail"),
        )
        desired = DesiredSourceConfig(repo="org/orcest", ref="refs/heads/master")
        resolved = resolve_desired_revision(desired)
        assert resolved.sha is None
        assert "unclassified git internal detail" not in resolved.error

    def test_repository_unreachable(self, mocker):
        mocker.patch(
            "orcest.fleet.source_revision._run_ls_remote",
            return_value=_completed(
                returncode=128, stderr="fatal: unable to access: Could not resolve host: github.com"
            ),
        )
        desired = DesiredSourceConfig(repo="org/orcest", ref="refs/heads/master")
        resolved = resolve_desired_revision(desired)
        assert resolved.error == "desired ref repository unreachable"

    def test_valid_sha_pin_accepted(self):
        desired = DesiredSourceConfig(repo="org/orcest", sha="a" * 40)
        resolved = resolve_desired_revision(desired)
        assert resolved.sha == "a" * 40


# ── evaluate_source_revision ──────────────────────────────────


def _desired(sha=SHA_A, error=None):
    return DesiredRevision(repo="org/orcest", ref="refs/heads/master", sha=sha, error=error)


class TestEvaluateSourceRevision:
    def test_exact_match_is_healthy(self):
        surfaces = [
            RuntimeRevision(surface="orchestrator:proj", revision=SHA_A),
            RuntimeRevision(surface="pool-manager", revision=SHA_A),
            RuntimeRevision(surface="template", revision=SHA_A),
            RuntimeRevision(surface="worker:w1", revision=SHA_A),
        ]
        report = evaluate_source_revision(_desired(), surfaces)
        assert report.healthy is True
        assert report.mismatches == ()

    def test_coherent_old_revision_is_unhealthy(self):
        """The pve-test incident: everything agrees, but on the wrong SHA."""
        surfaces = [RuntimeRevision(surface=f"worker:w{i}", revision=SHA_B) for i in range(4)]
        report = evaluate_source_revision(_desired(sha=SHA_A), surfaces)
        assert report.healthy is False
        assert any(SHA_B[:12] in m and SHA_A[:12] in m for m in report.mismatches)

    def test_orchestrator_current_worker_stale(self):
        surfaces = [
            RuntimeRevision(surface="orchestrator:proj", revision=SHA_A),
            RuntimeRevision(surface="worker:w1", revision=SHA_B),
        ]
        report = evaluate_source_revision(_desired(), surfaces)
        assert report.healthy is False
        assert any(m.startswith("worker:w1:") for m in report.mismatches)
        assert not any(m.startswith("orchestrator:proj:") for m in report.mismatches)

    def test_worker_current_orchestrator_stale(self):
        surfaces = [
            RuntimeRevision(surface="orchestrator:proj", revision=SHA_B),
            RuntimeRevision(surface="worker:w1", revision=SHA_A),
        ]
        report = evaluate_source_revision(_desired(), surfaces)
        assert report.healthy is False
        assert any(m.startswith("orchestrator:proj:") for m in report.mismatches)
        assert not any(m.startswith("worker:w1:") for m in report.mismatches)

    def test_stale_active_template_with_no_workers(self):
        surfaces = [RuntimeRevision(surface="template", revision=SHA_B)]
        report = evaluate_source_revision(_desired(), surfaces)
        assert report.healthy is False
        assert any(m.startswith("template:") for m in report.mismatches)

    def test_mixed_rolling_generation_is_non_green(self):
        surfaces = [
            RuntimeRevision(surface="worker:new1", revision=SHA_A),
            RuntimeRevision(surface="worker:old1", revision=SHA_B, degraded=True),
        ]
        report = evaluate_source_revision(_desired(), surfaces)
        assert report.healthy is False
        assert any("degraded" in m for m in report.mismatches)

    def test_surviving_busy_old_worker_still_flagged(self):
        """Drain grace keeps the worker visible, but never makes the fleet green."""
        surfaces = [RuntimeRevision(surface="worker:busy1", revision=SHA_B, degraded=True)]
        report = evaluate_source_revision(_desired(), surfaces)
        assert report.healthy is False
        assert len(report.surfaces) == 1
        assert report.surfaces[0].degraded is True

    def test_unresolved_desired_cannot_be_healthy(self):
        surfaces = [RuntimeRevision(surface="worker:w1", revision=SHA_A)]
        report = evaluate_source_revision(
            DesiredRevision(
                repo="org/orcest", ref="refs/heads/master", sha=None, error="desired ref not found"
            ),
            surfaces,
        )
        assert report.healthy is False
        assert "desired ref not found" in report.mismatches

    def test_unconfigured_is_explicit_not_silently_current(self):
        report = evaluate_source_revision(
            DesiredRevision(repo="", ref="", sha=None, error="desired revision unconfigured"),
            [],
        )
        assert report.healthy is False
        assert report.mismatches == ("desired revision unconfigured",)

    def test_missing_surface_revision_is_a_mismatch(self):
        surfaces = [RuntimeRevision(surface="pool-manager", revision=None)]
        report = evaluate_source_revision(_desired(), surfaces)
        assert report.healthy is False
        assert "pool-manager: no revision reported" in report.mismatches

    def test_unattested_dirty_revision_is_a_mismatch(self):
        surfaces = [RuntimeRevision(surface="orchestrator:proj", revision=f"{SHA_A}-dirty")]
        report = evaluate_source_revision(_desired(), surfaces)
        assert report.healthy is False
        assert any("unattested" in m for m in report.mismatches)

    def test_no_surfaces_at_all_is_healthy_when_desired_resolved(self):
        """An empty surface set (e.g. host unset) is not itself a drift signal."""
        report = evaluate_source_revision(_desired(), [])
        assert report.healthy is True

    def test_diagnostic_text_is_bounded(self):
        surfaces = [RuntimeRevision(surface="worker:w1", revision="a" * 500)]
        report = evaluate_source_revision(_desired(), surfaces)
        assert all(len(m) < 300 for m in report.mismatches)
