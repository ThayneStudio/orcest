"""Canonical registry of the operations-and-rollout.md "Required failure-injection
matrix" (accepted spec commit f9e719aec850674dca8e7ea2f1697395fd0b3a04).

Row text lives in ``golden/failure_injection_matrix.json`` (transcribed
verbatim from the wiki table, one JSON object per row, in table order) rather
than inline Python string literals, matching this repo's existing golden-
fixture convention (see tests/workflow_contract/golden/) and keeping the
89-column line-length lint happy without wrapping normative prose. See
coverage.py for which automated test proves each case today, and
test_matrix_completeness.py for the release gate that every case is
accounted for.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

_GOLDEN_PATH = Path(__file__).parent / "golden" / "failure_injection_matrix.json"


@dataclass(frozen=True)
class ConformanceCase:
    """One row of the required failure-injection matrix."""

    case_id: str
    injection: str
    expected: str


def _load_matrix() -> tuple[ConformanceCase, ...]:
    rows = json.loads(_GOLDEN_PATH.read_text(encoding="utf-8"))
    return tuple(
        ConformanceCase(
            case_id=row["case_id"], injection=row["injection"], expected=row["expected"]
        )
        for row in rows
    )


FAILURE_INJECTION_MATRIX: tuple[ConformanceCase, ...] = _load_matrix()

CASE_BY_ID: dict[str, ConformanceCase] = {c.case_id: c for c in FAILURE_INJECTION_MATRIX}

assert len(CASE_BY_ID) == len(FAILURE_INJECTION_MATRIX), "duplicate case_id in matrix"
