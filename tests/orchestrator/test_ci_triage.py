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
