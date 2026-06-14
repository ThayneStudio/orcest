"""CI failure classification using heuristic pattern matching.

Classifies CI failures without Claude by matching check run names and log
snippets against known patterns.

Matching order: definitive code-failure signals first (a build/test tool
actually ran and reported a real defect — never produced by flaky infra),
then transient, then dependency, then the broader code patterns. Unknown
is the fallback for cases needing Claude classification in Phase 2.

The definitive-code precedence exists because transient patterns are matched
against the whole log: a bare keyword like "timeout" appearing incidentally
in a long log would otherwise mask a genuine ``AssertionError`` and send a
deterministically-failing PR into an endless transient-rerun loop.
"""

import re
from enum import Enum


class CIFailureType(str, Enum):
    """Classification of a CI failure."""

    TRANSIENT = "transient"  # Network timeout, flaky test
    CODE = "code"  # Lint error, test failure, type error
    DEPENDENCY = "dependency"  # Pip/npm install failure
    UNKNOWN = "unknown"  # Needs Claude classification (Phase 2)


# Pattern -> classification mapping
# Patterns are matched against check run names and log snippets
TRANSIENT_PATTERNS: list[str] = [
    r"timeout",
    r"ETIMEDOUT",
    r"connection reset",
    r"502 bad gateway",
    r"503 service unavailable",
    r"rate limit",
    r"socket hang up",
    r"ECONNREFUSED",
]

# Definitive code-failure signals. When any of these is present the build or
# test tooling ran and reported a real defect (a parse error, a failed
# assertion, a test-summary line with a non-zero failed count). Flaky infra
# — network timeouts, runner crashes, 5xx — never produces these, so they win
# over an incidental transient keyword elsewhere in the log.
DEFINITIVE_CODE_PATTERNS: list[str] = [
    r"\bAssertionError\b",
    r"\bSyntaxError\b",
    r"\bIndentationError\b",
    # Test-summary lines: a failed count next to a passed count, either order
    # ("59 passed | 3 failed", "Tests: 3 failed, 10 passed"). Requiring both
    # counts on one line avoids matching a step number like "Step 3 failed".
    r"\d+\s+passed\b.{0,40}\b\d+\s+failed\b",
    r"\d+\s+failed\b.{0,40}\b\d+\s+passed\b",
    # SQL / pgTAP / TAP failures. Database tooling produces these only when a
    # migration or test actually ran and reported a real defect; flaky infra
    # never emits them. Without these, a pgTAP failure in a log that also
    # mentions "timeout" elsewhere is misclassified TRANSIENT and loops.
    r"(?m)^not ok \d+",  # TAP failed-test line
    r"# Bad plan\b",  # pgTAP plan mismatch
    r"Looks like you (planned|failed)",  # pgTAP/Test::More summary
    r"Failed \d+ of \d+\b",  # pg_prove run summary
    r"ERROR:\s+syntax error at or near",  # Postgres parse error
    r"duplicate key value violates unique constraint",
    r"violates (foreign key|not-null|check) constraint",
    r"(?m)^psql:.*\bERROR:",  # psql-reported SQL error
]

CODE_PATTERNS: list[str] = [
    r"ruff.*error",
    r"lint.*fail",
    r"mypy.*error",
    r"pytest.*FAILED",
    r"test.*fail",
    r"AssertionError",
    r"SyntaxError",
    r"TypeError",
    r"NameError",
    r"ImportError",
    r"ModuleNotFoundError",
    r"IndentationError",
    r"AttributeError",
    r"compilation failed",
    r"type.?check.*fail",
    # mypy / tsc type errors: "error: Incompatible types ...". These are a CODE
    # defect, not a dependency problem -- pin them here so the narrowed
    # DEPENDENCY `incompatible` patterns below cannot swallow them.
    r"Incompatible types",
    r"Incompatible return value type",
]

DEPENDENCY_PATTERNS: list[str] = [
    r"Could not find a version that satisfies",
    r"No matching distribution found",
    r"npm ERR!.*404",
    r"ERESOLVE",
    r"dependency resolution failed",
    r"version conflict",
    # Only treat 'incompatible' as a dependency signal when it is clearly about
    # package/version resolution -- NOT a mypy/tsc 'Incompatible types' error,
    # which must classify as CODE via the Incompatible-types CODE_PATTERNS.
    r"incompatible (?:version|dependenc(?:y|ies)|requirement|package)",
    r"(?:version|dependenc(?:y|ies)|requirement|package)[^\n]*incompatible",
    # Resolver phrasing: "X is incompatible with Y" / "X and Y are incompatible".
    # mypy/tsc say "Incompatible types in assignment" (no "with"), so anchoring
    # on "incompatible with" / "are incompatible" avoids swallowing type errors.
    r"incompatible with",
    r"are incompatible",
]


def classify_ci_failure(
    check_name: str,
    logs: str = "",
) -> CIFailureType:
    """Classify a CI failure using heuristic pattern matching.

    Args:
        check_name: Name of the failed check run.
        logs: Log output from the check run (may be empty).

    Returns:
        CIFailureType classification.
    """
    text = f"{check_name}\n{logs}"

    # Definitive code-failure signals win over an incidental transient
    # keyword: flaky infra never produces an assertion error or a test
    # summary with failure counts.
    for pattern in DEFINITIVE_CODE_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return CIFailureType.CODE

    for pattern in TRANSIENT_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return CIFailureType.TRANSIENT

    for pattern in DEPENDENCY_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return CIFailureType.DEPENDENCY

    for pattern in CODE_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return CIFailureType.CODE

    return CIFailureType.UNKNOWN
