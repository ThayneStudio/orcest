"""CI failure classification using heuristic pattern matching.

Classifies CI failures without Claude by matching check run names and log
snippets against known patterns. Pattern matching order: transient first
(cheapest to handle), then dependency, then code (most common). Unknown
is the fallback for cases needing Claude classification in Phase 2.
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
]

DEPENDENCY_PATTERNS: list[str] = [
    r"Could not find a version that satisfies",
    r"No matching distribution found",
    r"npm ERR!.*404",
    r"ERESOLVE",
    r"dependency resolution failed",
    r"version conflict",
    r"incompatible",
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
