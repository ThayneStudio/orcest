"""Unit tests for CI failure classification heuristics."""

from orcest.orchestrator.ci_triage import (
    CIFailureType,
    classify_ci_failure,
)

# -- Transient patterns ------------------------------------------------------


def test_transient_timeout():
    result = classify_ci_failure("build", logs="Step 3 failed: timeout")
    assert result is CIFailureType.TRANSIENT


def test_transient_rate_limit():
    result = classify_ci_failure(
        "api-tests",
        logs="GitHub API returned rate limit exceeded",
    )
    assert result is CIFailureType.TRANSIENT


# -- Code patterns -----------------------------------------------------------


def test_code_syntax_error():
    result = classify_ci_failure(
        "lint",
        logs="  File 'app.py', line 12\n    SyntaxError: invalid syntax",
    )
    assert result is CIFailureType.CODE


def test_code_pytest_failed():
    # Pattern is r"pytest.*FAILED", so both words must appear in order.
    result = classify_ci_failure(
        "unit-tests",
        logs="pytest: 2 passed, 1 FAILED in 4.32s",
    )
    assert result is CIFailureType.CODE


# -- Dependency patterns -----------------------------------------------------


def test_dependency_no_matching():
    result = classify_ci_failure(
        "install",
        logs="No matching distribution found for foobar==99.0",
    )
    assert result is CIFailureType.DEPENDENCY


# -- Unknown / edge cases ----------------------------------------------------


def test_unknown_no_match():
    result = classify_ci_failure(
        "deploy",
        logs="Something completely unrecognizable happened.",
    )
    assert result is CIFailureType.UNKNOWN


def test_empty_logs():
    result = classify_ci_failure("", logs="")
    assert result is CIFailureType.UNKNOWN


# -- Definitive code signal beats incidental transient keyword ---------------


def test_assertion_error_beats_incidental_timeout_keyword():
    """A real test failure must not be misread as transient because the long
    CI log happens to contain the word "timeout" somewhere unrelated."""
    logs = (
        "Running edge function tests...\n"
        "request timeout is configured to 30s\n"  # incidental transient keyword
        "error: AssertionError: Values are not equal.\n"
        "    -   200\n    +   400\n"
        "FAILED | 59 passed | 3 failed (63ms)\n"
        "##[error]Process completed with exit code 1.\n"
    )
    assert (
        classify_ci_failure("Deno tests (edge functions)", logs)
        is CIFailureType.CODE
    )


def test_test_summary_counts_beat_incidental_transient_keyword():
    """A test-summary line with passed+failed counts is a definitive code
    failure even when a transient keyword appears elsewhere."""
    logs = "connection reset noted in one flaky step\nTests: 3 failed, 10 passed\n"
    assert classify_ci_failure("jest", logs) is CIFailureType.CODE


def test_genuine_transient_without_code_signal_stays_transient():
    """No definitive code signal -> a transient keyword still wins."""
    logs = "##[error]The operation was canceled.\nECONNREFUSED 10.0.0.1:443\n"
    assert classify_ci_failure("build", logs) is CIFailureType.TRANSIENT


def test_step_number_failed_is_not_a_test_summary():
    """'Step 3 failed' must not be mistaken for a '3 failed' test summary."""
    assert (
        classify_ci_failure("build", logs="Step 3 failed: timeout")
        is CIFailureType.TRANSIENT
    )


# -- Case insensitivity ------------------------------------------------------


def test_case_insensitive():
    # The implementation lower-cases the combined text AND uses
    # re.IGNORECASE, so mixed-case input must still match.
    assert classify_ci_failure("build", logs="TIMEOUT after 30s") is CIFailureType.TRANSIENT

    assert classify_ci_failure("build", logs="syntaxerror: unexpected EOF") is CIFailureType.CODE

    assert (
        classify_ci_failure("install", logs="NO MATCHING DISTRIBUTION FOUND for pkg")
        is CIFailureType.DEPENDENCY
    )
