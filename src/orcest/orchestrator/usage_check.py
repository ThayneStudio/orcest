"""Query Anthropic's OAuth usage endpoint for token reset times.

Called reactively when a token hits its usage limit to determine
when the token will become available again.

The endpoint is undocumented and may change without notice.
All errors are handled gracefully — callers should fall back
to a default cooldown when this returns None.
"""

from __future__ import annotations

import logging
from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

_USAGE_URL = "https://api.anthropic.com/api/oauth/usage"
_TIMEOUT = 10  # seconds


def get_token_reset_time(token: str) -> datetime | None:
    """Query the Anthropic OAuth usage endpoint for a token's reset time.

    Returns the ``resets_at`` timestamp from whichever usage window
    (five-hour or seven-day) has utilization >= 95%.  If both windows
    are near the limit, returns the *sooner* reset time (the five-hour
    window) so the token is retried at the earliest opportunity.

    Returns ``None`` on any error (HTTP 429, network failure, unexpected
    response format) so the caller can fall back to a default cooldown.
    """
    import json

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

    try:
        five_hour = data.get("five_hour", {})
        seven_day = data.get("seven_day", {})

        # Find the window(s) that are near their limit
        candidates: list[datetime] = []
        for window in (five_hour, seven_day):
            utilization = window.get("utilization", 0)
            resets_at = window.get("resets_at", "")
            if utilization >= 95 and resets_at:
                parsed = datetime.fromisoformat(resets_at.replace("Z", "+00:00"))
                candidates.append(parsed)

        if not candidates:
            logger.info("Usage endpoint returned no high-utilization windows")
            return None

        # Return the soonest reset time
        return min(candidates)
    except (TypeError, ValueError, KeyError) as exc:
        logger.warning("Failed to parse usage response: %s", exc)
        return None
