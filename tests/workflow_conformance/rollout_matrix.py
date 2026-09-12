"""Canonical registry of the version-skew and rollback guarantees required by
operations-and-rollout.md's "Versioned rolling upgrades" section (accepted spec
commit f9e719aec850674dca8e7ea2f1697395fd0b3a04).

This section is prose, not a table, so each entry paraphrases one required
guarantee rather than transcribing a row verbatim; the ``spec_excerpt`` field
carries the closest verbatim wording for traceability. See coverage.py for
which automated test proves each requirement today, and
test_matrix_completeness.py for the release gate that every requirement is
accounted for.
"""

from __future__ import annotations

from tests.workflow_conformance.matrix import ConformanceCase

ROLLOUT_COMPATIBILITY_MATRIX: tuple[ConformanceCase, ...] = (
    ConformanceCase(
        case_id="vsr.controller_publishes_compatibility_matrix_in_readiness",
        injection="every deployed component reports build revision, schema range, "
        "queue protocol range, controller API range, reducer version, registered "
        "capabilities, and worker profile identity",
        expected="the controller publishes a compatibility matrix in its readiness output",
    ),
    ConformanceCase(
        case_id="vsr.new_major_protocol_uses_new_stream_names",
        injection="a new Activity protocol major version is introduced",
        expected="it uses new physical stream names including the major protocol version; "
        "a consumer group is never reused across incompatible major versions, so an old "
        "worker cannot receive a new envelope accidentally",
    ),
    ConformanceCase(
        case_id="vsr.controller_dual_publish_only_with_single_winner_claim",
        injection="a logical Activity is dual-published across an old and new protocol",
        expected="the controller MAY dual-publish only when one logical Activity has an "
        "explicit, single-winner claim across both versions; otherwise it publishes to "
        "exactly one compatible stream",
    ),
    ConformanceCase(
        case_id="vsr.rolling_worker_upgrade_canary_procedure",
        injection="a rolling worker upgrade is rehearsed",
        expected="idle canary workers on the new image/protocol pass synthetic claim, "
        "heartbeat, Candidate, receipt, loss, and credential-isolation tests before the "
        "revision is enabled for a bounded share of compatible Activities, old workers "
        "finish their claimed Attempts, old-protocol Activities stop publishing, and old "
        "idle workers are destroyed only after their stream/claim inventory is empty",
    ),
    ConformanceCase(
        case_id="vsr.controller_api_accepts_preceding_compatible_worker_minor_version",
        injection="a controller API change ships while workers on the immediately "
        "preceding compatible minor version are still live",
        expected="the controller accepts that preceding compatible worker minor version "
        "for the published overlap window; a major break uses a drain and new streams "
        "instead",
    ),
    ConformanceCase(
        case_id="vsr.database_migrations_forward_only_transactional",
        injection="a database migration runs",
        expected="migrations are forward-only and transactional where SQLite permits",
    ),
    ConformanceCase(
        case_id="vsr.filesystem_rewrite_migration_runs_in_maintenance_with_journal",
        injection="a migration requires filesystem rewrites or table rebuilds",
        expected="it runs in MAINTENANCE from a verified backup and records a durable "
        "migration journal",
    ),
    ConformanceCase(
        case_id="vsr.stored_protocol_object_unknown_literal_rejected",
        injection="a stored or wire protocol object names an unregistered protocol "
        "literal (an unsupported version combination)",
        expected="validation rejects it before the object can reach dispatch or mutation authority",
    ),
    ConformanceCase(
        case_id="vsr.schema_reducer_version_skew_fails_closed",
        injection="a workflow.db carries an unsupported schema version, or any run/"
        "transition row carries an unsupported reducer version",
        expected="startup fails closed (raises, or degrades to MAINTENANCE when "
        "configured) before ordinary endpoints open, rather than mutating state under an "
        "unrecognized version",
    ),
)

ROLLOUT_CASE_BY_ID: dict[str, ConformanceCase] = {
    c.case_id: c for c in ROLLOUT_COMPATIBILITY_MATRIX
}

assert len(ROLLOUT_CASE_BY_ID) == len(ROLLOUT_COMPATIBILITY_MATRIX), "duplicate case_id"
