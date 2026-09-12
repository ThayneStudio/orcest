"""Hand-curated coverage ledger for the two conformance registries in this
package (matrix.py's 149-row failure-injection matrix and
rollout_matrix.py's 9 version-skew/rollback requirements).

Ledger content lives in ``golden/failure_injection_coverage.json`` and
``golden/rollout_coverage.json`` (same convention as matrix.py's golden
fixture) rather than inline Python literals, so long prose notes and file
paths never fight the 100-column lint limit.

Every case_id from both registries MUST appear here exactly once (enforced by
test_matrix_completeness.py). Each entry is one of:

- ``status="automated"``: a real, existing (or newly added in this package)
  test proves the case. ``confidence="exact"`` means the test body was read
  and matches the row's injection/expected text; ``confidence="thematic"``
  means the test targets the same mechanism (by name and file placement) but
  was not individually re-verified line-by-line against this exact row's
  wording.
- ``status="deferred"``: the production mechanism the row requires does not
  exist yet (confirmed by reading the relevant source, not by absence of a
  matching test name). Deferred cases still cite ``refs`` when a
  partial/adjacent slice of the row IS covered. The exact deferred set is
  pinned in test_matrix_completeness.py so new gaps cannot silently join it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

_GOLDEN_DIR = Path(__file__).parent / "golden"


@dataclass(frozen=True)
class TestRef:
    """One pytest module::function this coverage entry relies on."""

    module: str
    function: str


@dataclass(frozen=True)
class CaseStatus:
    """The release gate's verdict for one ConformanceCase."""

    case_id: str
    status: Literal["automated", "deferred"]
    confidence: Literal["exact", "thematic"] | None
    refs: tuple[TestRef, ...]
    note: str


def _load_coverage(filename: str) -> dict[str, CaseStatus]:
    rows = json.loads((_GOLDEN_DIR / filename).read_text(encoding="utf-8"))
    result: dict[str, CaseStatus] = {}
    for row in rows:
        result[row["case_id"]] = CaseStatus(
            case_id=row["case_id"],
            status=row["status"],
            confidence=row["confidence"],
            refs=tuple(TestRef(r["module"], r["function"]) for r in row["refs"]),
            note=row["note"],
        )
    return result


FAILURE_INJECTION_COVERAGE: dict[str, CaseStatus] = _load_coverage(
    "failure_injection_coverage.json"
)

ROLLOUT_COVERAGE: dict[str, CaseStatus] = _load_coverage("rollout_coverage.json")
