from __future__ import annotations

import json
from typing import Any

from orcest.orchestrator import usage_check


class _Response:
    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(
            {
                "five_hour": {
                    "utilization": 91.25,
                    "resets_at": "2026-08-20T01:00:00Z",
                    "unsafe_extra": "not public",
                },
                "seven_day": {
                    "utilization": 42,
                    "resets_at": "2026-08-24T01:00:00Z",
                    "unsafe_extra": "not public",
                },
                "other": "not public",
            }
        ).encode()


def test_get_token_usage_state_extracts_only_public_fields(monkeypatch) -> None:
    requests = []

    def fake_urlopen(request: Any, *, timeout: int) -> _Response:
        requests.append((request, timeout))
        return _Response()

    monkeypatch.setattr(usage_check, "urlopen", fake_urlopen)

    assert usage_check.get_token_usage_state("secret-token") == {
        "five_hour": {
            "utilization": 91.25,
            "resets_at": "2026-08-20T01:00:00Z",
        },
        "seven_day": {
            "utilization": 42,
            "resets_at": "2026-08-24T01:00:00Z",
        },
    }

    request, timeout = requests[0]
    assert timeout == 10
    assert request.get_header("Authorization") == "Bearer secret-token"
    assert request.get_header("Anthropic-beta") == "oauth-2025-04-20"
