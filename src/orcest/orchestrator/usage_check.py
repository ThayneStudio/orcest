"""Query Anthropic's OAuth usage endpoint for token reset times.

Called reactively when a token hits its usage limit to determine
when the token will become available again.

The endpoint is undocumented and may change without notice.
All errors are handled gracefully — callers should fall back
to a default cooldown when this returns None.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

_USAGE_URL = "https://api.anthropic.com/api/oauth/usage"
_TIMEOUT = 10  # seconds
# Utilization percentage threshold (0-100 scale, as returned by the API).
_UTILIZATION_THRESHOLD = 95


def get_token_usage_state(token: str) -> dict[str, dict[str, object]] | None:
    """Query the Anthropic OAuth usage endpoint for the two quota windows.

    Returns only the fields that are safe for public CI summaries:
    ``five_hour.utilization``, ``five_hour.resets_at``,
    ``seven_day.utilization``, and ``seven_day.resets_at``.
    """
    data = _query_usage_endpoint(token)
    if data is None:
        return None

    try:
        return {
            "five_hour": _extract_usage_window(data["five_hour"]),
            "seven_day": _extract_usage_window(data["seven_day"]),
        }
    except (TypeError, KeyError, AttributeError) as exc:
        logger.warning("Failed to parse usage response: %s", exc)
        return None


def get_token_reset_time(
    token: str,
    *,
    observe: Callable[[dict[str, object]], None] | None = None,
) -> datetime | None:
    """Query the Anthropic OAuth usage endpoint for a token's reset time.

    Returns the ``resets_at`` timestamp from whichever usage window
    (five-hour or seven-day) has utilization >= 95%.  If both windows
    are near the limit, returns the *sooner* reset time (the five-hour
    window) so the token is retried at the earliest opportunity.

    Returns ``None`` on any error (HTTP 429, network failure, unexpected
    response format) so the caller can fall back to a default cooldown.
    """
    data = _query_usage_endpoint(token)
    if data is None:
        return None

    if observe is not None:
        try:
            observe(
                {
                    key: _extract_usage_window(value)
                    for key, value in data.items()
                    if key in {"five_hour", "seven_day"}
                }
            )
        except Exception:
            logger.warning("Unable to retain dashboard quota observation")

    try:
        # Find the window(s) that are near their limit
        candidates: list[datetime] = []
        for window in (
            _usage_window_or_empty(data.get("five_hour")),
            _usage_window_or_empty(data.get("seven_day")),
        ):
            utilization_raw = window.get("utilization", 0)
            resets_at = window.get("resets_at", "")
            if not isinstance(utilization_raw, int | float | str) or not isinstance(resets_at, str):
                continue
            utilization = float(utilization_raw)
            if utilization >= _UTILIZATION_THRESHOLD and resets_at:
                parsed = datetime.fromisoformat(resets_at.replace("Z", "+00:00"))
                candidates.append(parsed)

        if not candidates:
            logger.info("Usage endpoint returned no high-utilization windows")
            return None

        # Return the soonest reset time
        return min(candidates)
    except (TypeError, ValueError, KeyError, AttributeError) as exc:
        logger.warning("Failed to parse usage response: %s", exc)
        return None


def _query_usage_endpoint(token: str) -> dict[str, object] | None:
    req = Request(
        _USAGE_URL,
        headers={
            "Authorization": f"Bearer {token}",
            "anthropic-beta": "oauth-2025-04-20",
        },
    )

    try:
        with urlopen(req, timeout=_TIMEOUT) as resp:
            data = json.loads(resp.read())
    except (HTTPError, URLError, OSError, json.JSONDecodeError) as exc:
        logger.warning("Failed to query usage endpoint: %s", exc)
        return None

    if not isinstance(data, dict):
        logger.warning("Usage endpoint returned non-object JSON: %s", type(data).__name__)
        return None

    return data


def _usage_window_or_empty(window: object) -> dict[str, object]:
    if isinstance(window, dict):
        return window
    return {}


def _extract_usage_window(window: object) -> dict[str, object]:
    if not isinstance(window, dict):
        raise TypeError(f"usage window must be an object, got {type(window).__name__}")

    return {
        "utilization": window["utilization"],
        "resets_at": window["resets_at"],
    }
