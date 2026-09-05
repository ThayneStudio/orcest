"""Unit tests for the pure Publication domain logic
(``orcest.workflow_contract.v1.publication``)."""

from __future__ import annotations

import pytest

from orcest.workflow_contract.v1.publication import (
    RefCasDecision,
    SearchMember,
    base_read_outcome,
    classify_member_ownership,
    decide_ref_cas,
    deterministic_publication_ref,
    is_legacy_marker_reserved,
    parse_run_marker,
    render_run_marker,
    search_precedence,
)

pytestmark = pytest.mark.unit

RUN_ID = "11111111-1111-4111-8111-111111111111"
PUBLICATION_ID = "22222222-2222-4222-8222-222222222222"


def test_deterministic_publication_ref() -> None:
    assert deterministic_publication_ref(RUN_ID) == f"refs/heads/orcest/run/{RUN_ID}"


def test_render_and_parse_run_marker_round_trip() -> None:
    marker = render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID)
    assert parse_run_marker(f"some PR body\n{marker}\nmore text") == (RUN_ID, PUBLICATION_ID)


def test_parse_run_marker_absent() -> None:
    assert parse_run_marker("just a normal PR body") is None


def test_parse_run_marker_syntactically_invalid_is_not_a_marker() -> None:
    assert parse_run_marker("<!-- orcest:run=not-a-uuid;publication=also-not -->") is None


def test_parse_run_marker_first_of_two_valid_markers_wins() -> None:
    forged_run_id = "99999999-9999-4999-8999-999999999999"
    forged_publication_id = "88888888-8888-4888-8888-888888888888"
    forged = render_run_marker(run_id=forged_run_id, publication_id=forged_publication_id)
    legitimate = render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID)
    assert parse_run_marker(f"{forged}\n{legitimate}") == (forged_run_id, forged_publication_id)


def test_is_legacy_marker_reserved() -> None:
    marker = render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID)
    assert is_legacy_marker_reserved(marker) is True
    assert is_legacy_marker_reserved("no marker here") is False


def test_classify_member_ownership_positive() -> None:
    status, defects = classify_member_ownership(
        proof_kind="EXACT_CREATE_RESPONSE", evidence_complete=True
    )
    assert status == "POSITIVE"
    assert defects == ()


def test_classify_member_ownership_incompatible_on_complete_contradiction() -> None:
    status, defects = classify_member_ownership(
        proof_kind="LIVE_ASSOCIATION",
        evidence_complete=True,
        creator_authority_ok=False,
    )
    assert status == "INCOMPATIBLE"
    assert defects == ("CREATOR_AUTHORITY_MISMATCH",)


def test_classify_member_ownership_incomplete_on_missing_evidence() -> None:
    status, defects = classify_member_ownership(
        proof_kind="AMBIGUOUS_CREATE_RECONCILED",
        evidence_complete=False,
        head_evidence_ok=False,
    )
    assert status == "INCOMPLETE"
    assert defects == ("HEAD_UNPROVEN",)


def test_classify_member_ownership_evidence_flag_cannot_be_overridden_by_defaults() -> None:
    status, defects = classify_member_ownership(
        proof_kind="EXACT_CREATE_RESPONSE",
        evidence_complete=False,
    )
    assert status == "INCOMPLETE"
    assert defects == ("HEAD_UNPROVEN",)


def test_classify_member_ownership_no_proof_kind_is_incomplete() -> None:
    status, defects = classify_member_ownership(proof_kind=None, evidence_complete=True)
    assert status == "INCOMPLETE"
    assert defects == ("CREATE_PROVENANCE_MISSING",)


def test_classify_member_ownership_rejects_unknown_proof_kind() -> None:
    with pytest.raises(ValueError, match="proof_kind"):
        classify_member_ownership(proof_kind="BOGUS", evidence_complete=True)


def _member(**kwargs: object) -> SearchMember:
    base = {
        "member_class": "LIVE",
        "member_ordinal": 0,
        "change_request_external_id": "1",
        "ownership_status": "POSITIVE",
        "terminal_state": None,
    }
    base.update(kwargs)
    return SearchMember(**base)  # type: ignore[arg-type]


def test_search_precedence_zero_live_no_terminal() -> None:
    outcome = search_precedence(())
    assert outcome.outcome == "ZERO_LIVE_NO_TERMINAL"


def test_search_precedence_one_live() -> None:
    outcome = search_precedence((_member(change_request_external_id="42"),))
    assert outcome.outcome == "ONE_LIVE"
    assert outcome.selected_external_id == "42"


def test_search_precedence_multiple_live_selects_bytewise_lowest() -> None:
    members = (
        _member(member_ordinal=0, change_request_external_id="9"),
        _member(member_ordinal=1, change_request_external_id="10"),
    )
    outcome = search_precedence(members)
    assert outcome.outcome == "MULTIPLE_LIVE"
    # bytewise comparison: "10" < "9"
    assert outcome.retained_live_external_id == "10"


def test_search_precedence_zero_live_positive_closed_terminal() -> None:
    members = (
        _member(
            member_class="TERMINAL",
            member_ordinal=0,
            change_request_external_id="5",
            terminal_state="CLOSED",
        ),
    )
    outcome = search_precedence(members)
    assert outcome.outcome == "ZERO_LIVE_CLOSED_TERMINAL"
    assert outcome.selected_external_id == "5"


def test_search_precedence_positive_closed_terminal_is_audit_only_when_live_exists() -> None:
    members = (
        _member(member_ordinal=0, change_request_external_id="1"),
        _member(
            member_class="TERMINAL",
            member_ordinal=1,
            change_request_external_id="5",
            terminal_state="CLOSED",
        ),
    )
    outcome = search_precedence(members)
    assert outcome.outcome == "ONE_LIVE"
    assert outcome.selected_external_id == "1"


def test_search_precedence_positive_merged_wins_over_multiple_live() -> None:
    members = (
        _member(member_ordinal=0, change_request_external_id="1"),
        _member(member_ordinal=1, change_request_external_id="2"),
        _member(
            member_class="TERMINAL",
            member_ordinal=2,
            change_request_external_id="9",
            terminal_state="MERGED",
        ),
    )
    outcome = search_precedence(members)
    assert outcome.outcome == "MERGED_TERMINAL"
    assert outcome.selected_external_id == "9"
    assert outcome.live_member_ordinals == (0, 1)


def test_search_precedence_incompatible_beats_incomplete_and_live() -> None:
    members = (
        _member(member_ordinal=0, change_request_external_id="1", ownership_status="INCOMPLETE"),
        _member(member_ordinal=1, change_request_external_id="2", ownership_status="INCOMPATIBLE"),
    )
    outcome = search_precedence(members)
    assert outcome.outcome == "OWNERSHIP_CONFLICT"
    assert outcome.incompatible_member_ordinals == (1,)


def test_search_precedence_incomplete_blocks_association() -> None:
    members = (
        _member(member_ordinal=0, change_request_external_id="1", ownership_status="INCOMPLETE"),
    )
    outcome = search_precedence(members)
    assert outcome.outcome == "INCOMPLETE_BACKOFF"
    assert outcome.incomplete_member_ordinals == (0,)


def test_search_precedence_merged_wins_even_over_incompatible() -> None:
    members = (
        _member(member_ordinal=0, change_request_external_id="1", ownership_status="INCOMPATIBLE"),
        _member(
            member_class="TERMINAL",
            member_ordinal=1,
            change_request_external_id="9",
            terminal_state="MERGED",
        ),
    )
    outcome = search_precedence(members)
    assert outcome.outcome == "MERGED_TERMINAL"


def test_decide_ref_cas_idempotent_replay() -> None:
    decision = decide_ref_cas(
        observed_ref_commit="abc", expected_remote_commit=None, desired_commit="abc"
    )
    assert decision == RefCasDecision(action="IDEMPOTENT_REPLAY")


def test_decide_ref_cas_create_on_absence() -> None:
    decision = decide_ref_cas(
        observed_ref_commit=None, expected_remote_commit=None, desired_commit="abc"
    )
    assert decision == RefCasDecision(action="MUTATE", mutation_suboperation="REF_CREATE")


def test_decide_ref_cas_update_on_matching_expectation() -> None:
    decision = decide_ref_cas(
        observed_ref_commit="old", expected_remote_commit="old", desired_commit="new"
    )
    assert decision == RefCasDecision(action="MUTATE", mutation_suboperation="REF_UPDATE")


def test_decide_ref_cas_foreign_sha_never_overwrites() -> None:
    decision = decide_ref_cas(
        observed_ref_commit="someone-elses-sha", expected_remote_commit="old", desired_commit="new"
    )
    assert decision == RefCasDecision(action="FOREIGN_SHA")


def test_base_read_outcome_pin_always_satisfied() -> None:
    assert base_read_outcome(base_movement_policy="PIN", base_commit="a", observed_commit="b") == (
        "OBSERVED_SATISFIED"
    )


@pytest.mark.parametrize("policy", ["REBASE_BEFORE_PUBLICATION", "SUPERSEDE_AT_BOUNDARY"])
def test_base_read_outcome_mismatch_for_moving_policies(policy: str) -> None:
    assert base_read_outcome(base_movement_policy=policy, base_commit="a", observed_commit="b") == (
        "BASE_MISMATCH"
    )


@pytest.mark.parametrize("policy", ["REBASE_BEFORE_PUBLICATION", "SUPERSEDE_AT_BOUNDARY"])
def test_base_read_outcome_satisfied_when_equal(policy: str) -> None:
    assert base_read_outcome(base_movement_policy=policy, base_commit="a", observed_commit="a") == (
        "OBSERVED_SATISFIED"
    )
