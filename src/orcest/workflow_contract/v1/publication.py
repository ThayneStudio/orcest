"""Pure Publication domain logic (docs/wiki/domain-model.md "Publication" /
"Publication Effect" / "Publication Effect Checkpoint" / "Forge Observation"
``CHANGE_REQUEST_SEARCH_RESULT``, docs/wiki/forge-integration.md).

This module owns deterministic decisions that need no I/O and no durable
state beyond their explicit inputs: the deterministic ref/marker literal
format, the Change Request Search Member ownership tagged-union classifier,
the complete-search precedence/cardinality router, the ``REF_READ`` CAS
decision, and the base-read policy dispatch. ``workflow_store.store.RunStore``
calls these functions and persists their outcomes; it never re-implements
the decision logic inline, mirroring how ``verification.py`` separates
Verification Profile/Receipt logic from the durable Attempt Result path.

Everything here is intentionally ignorant of SQL, Activities, and Outboxes.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

__all__ = [
    "DETERMINISTIC_REF_PREFIX",
    "MARKER_PREFIX",
    "MARKER_SUFFIX",
    "SearchMember",
    "SearchPrecedenceOutcome",
    "RefCasDecision",
    "base_read_outcome",
    "classify_member_ownership",
    "decide_ref_cas",
    "deterministic_publication_ref",
    "is_legacy_marker_reserved",
    "parse_run_marker",
    "render_run_marker",
    "search_precedence",
]

DETERMINISTIC_REF_PREFIX = "refs/heads/orcest/run/"
MARKER_PREFIX = "<!-- orcest:run="
MARKER_SUFFIX = " -->"

# forge-integration.md:398 -- "<!-- orcest:run=<run-uuid>;publication=<publication-uuid> -->"
_MARKER_RE = re.compile(
    r"<!--\s*orcest:run=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
    r";publication=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\s*-->"
)

_OWNERSHIP_DEFECT_CODES_FOR_MISMATCH = (
    "CREATOR_AUTHORITY_MISMATCH",
    "REF_MISMATCH",
    "MARKER_MISMATCH",
    "DESIRED_COMMIT_MISMATCH",
    "HEAD_UNPROVEN",
    "EFFECT_GENERATION_MISMATCH",
)


def deterministic_publication_ref(run_id: str) -> str:
    """The one deterministic ref for a Run (forge-integration.md:390)."""
    return f"{DETERMINISTIC_REF_PREFIX}{run_id.lower()}"


def render_run_marker(*, run_id: str, publication_id: str) -> str:
    return f"{MARKER_PREFIX}{run_id.lower()};publication={publication_id.lower()}{MARKER_SUFFIX}"


def parse_run_marker(body_text: str) -> tuple[str, str] | None:
    """Parse the first syntactically valid Orcest v1 marker in ``body_text``.

    Returns ``(run_id, publication_id)`` or ``None`` when no syntactically
    valid marker is present. A syntactically invalid/partial marker-like
    string is not a marker (forge-integration.md:398-405) -- callers must
    not infer ownership from it.

    When ``body_text`` contains more than one syntactically valid marker
    (e.g. a forged marker prepended ahead of the legitimate one), this is
    an intentional first-match-wins choice, not incidental regex behavior:
    a second marker never displaces the first found. It is not this
    function's sole defense against a forged/duplicated marker -- that is
    ``classify_member_ownership``'s other bindings (ref/desired-commit/head
    evidence), which independently corroborate ownership.
    """
    match = _MARKER_RE.search(body_text)
    if match is None:
        return None
    return match.group(1), match.group(2)


def is_legacy_marker_reserved(body_text: str) -> bool:
    """Whether ``body_text`` carries any syntactically valid Orcest v1
    marker, which alone reserves the Change Request from the legacy engine
    even when its Run/Publication is temporarily unknown
    (forge-integration.md:403-405)."""
    return parse_run_marker(body_text) is not None


def classify_member_ownership(
    *,
    proof_kind: str | None,
    evidence_complete: bool,
    creator_authority_ok: bool = True,
    ref_ok: bool = True,
    marker_ok: bool = True,
    desired_commit_ok: bool = True,
    head_evidence_ok: bool = True,
    effect_generation_ok: bool = True,
) -> tuple[str, tuple[str, ...]]:
    """Classify one Change Request Search Member's ownership.

    Domain-model.md 2900-2907/2893-2898: a *proved contradiction* is
    ``INCOMPATIBLE``; merely missing/stale/rate-limited/ambiguous proof is
    ``INCOMPLETE`` and "authorizes neither association, terminalization,
    cleanup mutation, nor a Human Boundary." ``POSITIVE`` requires
    ``proof_kind`` be one of ``EXACT_CREATE_RESPONSE``,
    ``AMBIGUOUS_CREATE_RECONCILED``, or ``LIVE_ASSOCIATION`` *and* every
    binding equal (DM:2900-2907).

    Callers supply the pre-computed comparison booleans (creator authority,
    ref, marker, desired commit, head evidence, Effect generation) and an
    ``evidence_complete`` flag distinguishing "all evidence was read and at
    least one binding definitively disagrees" (-> ``INCOMPATIBLE``) from
    "some binding could not yet be read/confirmed" (-> ``INCOMPLETE``). When
    ``proof_kind`` is ``None`` (no create checkpoint and no durable
    association at all), the member is always ``INCOMPLETE`` with
    ``CREATE_PROVENANCE_MISSING`` -- a proven absence of provenance is
    insufficient basis, never a contradiction.
    """
    if proof_kind is None:
        return "INCOMPLETE", ("CREATE_PROVENANCE_MISSING",)
    valid_proof_kinds = ("EXACT_CREATE_RESPONSE", "AMBIGUOUS_CREATE_RECONCILED", "LIVE_ASSOCIATION")
    if proof_kind not in valid_proof_kinds:
        raise ValueError(f"unknown proof_kind {proof_kind!r}")
    checks = {
        "CREATOR_AUTHORITY_MISMATCH": not creator_authority_ok,
        "REF_MISMATCH": not ref_ok,
        "MARKER_MISMATCH": not marker_ok,
        "DESIRED_COMMIT_MISMATCH": not desired_commit_ok,
        "HEAD_UNPROVEN": not head_evidence_ok,
        "EFFECT_GENERATION_MISMATCH": not effect_generation_ok,
    }
    defects = tuple(code for code in _OWNERSHIP_DEFECT_CODES_FOR_MISMATCH if checks[code])
    if not defects:
        return "POSITIVE", ()
    if evidence_complete:
        return "INCOMPATIBLE", defects
    return "INCOMPLETE", defects


@dataclass(frozen=True, slots=True)
class SearchMember:
    """One already-classified Change Request Search Member, as needed by
    ``search_precedence``. Mirrors the closed field set of domain-model.md
    "Change Request Search Member" that the router actually consumes."""

    member_class: str  # LIVE | TERMINAL
    member_ordinal: int
    change_request_external_id: str
    ownership_status: str  # POSITIVE | INCOMPATIBLE | INCOMPLETE
    terminal_state: str | None = None  # MERGED | CLOSED, TERMINAL only


@dataclass(frozen=True, slots=True)
class SearchPrecedenceOutcome:
    """The deterministic outcome of applying domain-model.md 2142-2173 /
    forge-integration.md 477-499 to one complete search result."""

    outcome: str
    # One of: "MERGED_TERMINAL", "OWNERSHIP_CONFLICT", "INCOMPLETE_BACKOFF",
    # "ZERO_LIVE_NO_TERMINAL", "ZERO_LIVE_CLOSED_TERMINAL", "ONE_LIVE",
    # "MULTIPLE_LIVE".
    selected_external_id: str | None = None
    selected_member_ordinal: int | None = None
    incompatible_member_ordinals: tuple[int, ...] = ()
    incomplete_member_ordinals: tuple[int, ...] = ()
    live_member_ordinals: tuple[int, ...] = ()
    retained_live_external_id: str | None = None


def search_precedence(members: Sequence[SearchMember]) -> SearchPrecedenceOutcome:
    """Apply the fixed precedence order to one complete search result
    (domain-model.md 2142-2173, restated forge-integration.md 477-499):

    1. Any ``TERMINAL``/``MERGED`` member with ``ownership_status=POSITIVE``
       -> select the bytewise-lowest such id, regardless of live cardinality.
    2. Else any ``INCOMPATIBLE`` member -> exceptional ownership-conflict.
    3. Else any ``INCOMPLETE`` member -> autonomous backoff, no association.
    4. Else (every member ``POSITIVE`` and none ``MERGED``) -> live-cardinality
       routing (ZERO/ONE/MULTIPLE).
    """
    live = [m for m in members if m.member_class == "LIVE"]
    terminal = [m for m in members if m.member_class == "TERMINAL"]

    positive_merged = sorted(
        (m for m in terminal if m.terminal_state == "MERGED" and m.ownership_status == "POSITIVE"),
        key=lambda m: m.change_request_external_id,
    )
    if positive_merged:
        winner = positive_merged[0]
        return SearchPrecedenceOutcome(
            outcome="MERGED_TERMINAL",
            selected_external_id=winner.change_request_external_id,
            selected_member_ordinal=winner.member_ordinal,
            live_member_ordinals=tuple(m.member_ordinal for m in live),
        )

    incompatible = tuple(
        m.member_ordinal for m in (*live, *terminal) if m.ownership_status == "INCOMPATIBLE"
    )
    if incompatible:
        return SearchPrecedenceOutcome(
            outcome="OWNERSHIP_CONFLICT", incompatible_member_ordinals=incompatible
        )

    incomplete = tuple(
        m.member_ordinal for m in (*live, *terminal) if m.ownership_status == "INCOMPLETE"
    )
    if incomplete:
        return SearchPrecedenceOutcome(
            outcome="INCOMPLETE_BACKOFF", incomplete_member_ordinals=incomplete
        )

    # Every member is POSITIVE and none is MERGED: live-cardinality routing.
    if not live:
        positive_closed = sorted(
            (m for m in terminal if m.terminal_state == "CLOSED"),
            key=lambda m: m.change_request_external_id,
        )
        if positive_closed:
            winner = positive_closed[0]
            return SearchPrecedenceOutcome(
                outcome="ZERO_LIVE_CLOSED_TERMINAL",
                selected_external_id=winner.change_request_external_id,
                selected_member_ordinal=winner.member_ordinal,
            )
        return SearchPrecedenceOutcome(outcome="ZERO_LIVE_NO_TERMINAL")

    if len(live) == 1:
        only = live[0]
        return SearchPrecedenceOutcome(
            outcome="ONE_LIVE",
            selected_external_id=only.change_request_external_id,
            selected_member_ordinal=only.member_ordinal,
            live_member_ordinals=(only.member_ordinal,),
        )

    retained = min(live, key=lambda m: m.change_request_external_id)
    return SearchPrecedenceOutcome(
        outcome="MULTIPLE_LIVE",
        live_member_ordinals=tuple(m.member_ordinal for m in live),
        retained_live_external_id=retained.change_request_external_id,
    )


@dataclass(frozen=True, slots=True)
class RefCasDecision:
    """The deterministic ``REF_READ`` decision (forge-integration.md 458-468)."""

    action: str  # "IDEMPOTENT_REPLAY" | "MUTATE" | "FOREIGN_SHA"
    mutation_suboperation: str | None = None  # "REF_CREATE" | "REF_UPDATE", only for MUTATE


_EXPLICIT_ABSENCE = None


def decide_ref_cas(
    *,
    observed_ref_commit: str | None,
    expected_remote_commit: str | None,
    desired_commit: str,
) -> RefCasDecision:
    """Deterministic ref CAS decision (forge-integration.md 458-468).

    ``expected_remote_commit is None`` means "explicit nonexistence" (initial
    ref creation). Never returns a decision that overwrites a value other
    than the exact expected one -- any other observed value is ``FOREIGN_SHA``,
    which the caller MUST route to ownership reconciliation, never a blind
    overwrite (forge-integration.md 548-564, domain-model.md I14).
    """
    if observed_ref_commit == desired_commit:
        return RefCasDecision(action="IDEMPOTENT_REPLAY")
    if observed_ref_commit == expected_remote_commit:
        suboperation = "REF_CREATE" if expected_remote_commit is _EXPLICIT_ABSENCE else "REF_UPDATE"
        return RefCasDecision(action="MUTATE", mutation_suboperation=suboperation)
    return RefCasDecision(action="FOREIGN_SHA")


def base_read_outcome(*, base_movement_policy: str, base_commit: str, observed_commit: str) -> str:
    """Base-read policy dispatch (domain-model.md 2350-2364), identical for
    ``BASE_READ_PRE`` and ``BASE_READ_POST``. Returns ``OBSERVED_SATISFIED``
    or ``BASE_MISMATCH``."""
    if base_movement_policy == "PIN":
        return "OBSERVED_SATISFIED"
    if base_movement_policy not in ("REBASE_BEFORE_PUBLICATION", "SUPERSEDE_AT_BOUNDARY"):
        raise ValueError(f"unknown base_movement_policy {base_movement_policy!r}")
    return "OBSERVED_SATISFIED" if base_commit == observed_commit else "BASE_MISMATCH"
