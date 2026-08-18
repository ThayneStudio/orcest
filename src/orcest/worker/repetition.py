"""Repetition (livelock) detector for the activity watchdog.

Catches an agent that is repeating itself while still *looking* active to
the other liveness signals (CPU ticking, stdout flowing, workspace files
touched). Three independent hash streams are tracked from the tool
calls/errors an agent emits (see ``stream_liveness.StreamSignal``):

- **exact**: the same (tool name, normalized args) tuple called back to
  back -- the agent is stuck retrying the identical action.
- **error_class**: the same (tool name, error class) failing back to back,
  regardless of what args produced it -- the agent is stuck retrying a
  fundamentally broken action with cosmetic variations.
- **ping_pong**: the last ``2 * pingpong_threshold`` exact call hashes
  strictly alternate between exactly two distinct values -- the agent is
  oscillating between two actions (e.g. edit file, revert file) without
  making progress.

Per spec §8's redaction rule, this module only ever produces/retains tool
**names**, 16-hex-char **hashes**, and error **classes** -- never raw args
or output. It performs no I/O and reads no clock; it is a pure, in-memory
accumulator driven entirely by ``observe_*`` calls from the caller.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import deque
from dataclasses import dataclass

# --- Volatile-substring normalization (spec §4 S4) --------------------------
#
# Applied (in this order) to the sort_keys=True JSON serialization of a tool
# call's args before hashing, so that two calls that differ only in a
# request id / timestamp / freshly-generated token still hash identically.
_UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
_ISO_TIMESTAMP_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?"
)
# Unix-epoch-looking numbers: 10+ consecutive digits, optionally with a
# fractional part (epoch-with-millis/micros as a float).
_EPOCH_RE = re.compile(r"(?<!\d)\d{10,}(?:\.\d+)?(?!\d)")
# Any other long run of hex characters (ids, digests, tokens, ...). Applied
# last so it doesn't fight with the more specific patterns above. Requires at
# least one a-f letter in the run -- otherwise a pure-decimal run (e.g. two
# distinct byte offsets like 100000000 vs 200000000, digits being a subset
# of the hex alphabet) would be stripped and collapse genuinely-different
# args into the same hash. Epoch-length pure-decimal runs (>=10 digits) are
# still normalized by _EPOCH_RE above.
_HEX_RUN_RE = re.compile(
    r"(?<![0-9a-fA-F])(?=[0-9a-fA-F]*[a-fA-F])[0-9a-fA-F]{8,}(?![0-9a-fA-F])"
)

_HASH_LEN = 16


def _normalize_args(args: dict | None) -> str:
    serialized = json.dumps(args, sort_keys=True, default=str)
    serialized = _UUID_RE.sub("<uuid>", serialized)
    serialized = _ISO_TIMESTAMP_RE.sub("<ts>", serialized)
    # Hex-run stripping before epoch: _HEX_RUN_RE only matches runs
    # containing an a-f letter, so it never touches pure-decimal epoch
    # numbers. Running it first keeps a genuinely-hex identifier (e.g. a
    # git SHA) that happens to contain an embedded >=10-digit decimal
    # substring as one atomic match, instead of _EPOCH_RE fragmenting it
    # into a hex part + a decimal part first (which produced
    # position/length-dependent, non-deterministic normalization).
    serialized = _HEX_RUN_RE.sub("<hex>", serialized)
    serialized = _EPOCH_RE.sub("<epoch>", serialized)
    return serialized


def _exact_hash(name: str, args: dict | None) -> str:
    normalized = _normalize_args(args)
    return hashlib.sha256(f"{name}:{normalized}".encode()).hexdigest()[:_HASH_LEN]


def _error_hash(name: str, error_class: str) -> str:
    # Deliberately ignores args entirely -- two calls to the same tool that
    # fail with the same error class count as the same failure, no matter
    # what arguments produced them.
    return hashlib.sha256(f"{name}:{error_class}".encode()).hexdigest()[:_HASH_LEN]


@dataclass(frozen=True)
class RepetitionVerdict:
    """A tripped repetition stream, safe to emit (hashes only, no raw args)."""

    stream: str  # "exact" | "error_class" | "ping_pong"
    count: int
    hashes: tuple[str, ...]  # the offending normalized hashes


class RepetitionDetector:
    """Accumulates tool-call/error hashes across a task and detects livelock.

    Pure and stateless w.r.t. the outside world: no I/O, no Redis, no clock
    reads. The caller (the liveness ladder / tracker) feeds it every parsed
    tool call and tool error observed on the stream, in order.
    """

    def __init__(
        self,
        exact_threshold: int = 4,
        error_threshold: int = 3,
        pingpong_threshold: int = 6,
    ) -> None:
        self.exact_threshold = exact_threshold
        self.error_threshold = error_threshold
        self.pingpong_threshold = pingpong_threshold

        self._exact_streak_hash: str | None = None
        self._exact_streak_count = 0

        self._error_streak_hash: str | None = None
        self._error_streak_count = 0

        # Only exact-call hashes feed the ping-pong window; bounded to
        # exactly the window size the ping-pong check needs.
        self._exact_window: deque[str] = deque(maxlen=2 * pingpong_threshold)

        # Unified, bounded history for task.activity emission -- both calls
        # and errors, newest-last. Bounded deque: no unbounded history.
        self._recent: deque[tuple[str, str]] = deque(maxlen=20)

    def observe_tool_call(self, name: str, args: dict | None) -> None:
        h = _exact_hash(name, args)
        self._recent.append((name, h))
        self._exact_window.append(h)

        if h == self._exact_streak_hash:
            self._exact_streak_count += 1
        else:
            # Ladder-reset rule: a novel exact hash means the agent did
            # something genuinely different, so it clears *every* stream's
            # consecutive count, not just this one -- a prior run of
            # identical errors is stale once real new activity happens.
            self._exact_streak_hash = h
            self._exact_streak_count = 1
            self._error_streak_hash = None
            self._error_streak_count = 0

    def observe_tool_error(self, name: str, error_class: str) -> None:
        h = _error_hash(name, error_class)
        self._recent.append((name, h))

        if h == self._error_streak_hash:
            self._error_streak_count += 1
        else:
            self._error_streak_hash = h
            self._error_streak_count = 1

    def verdict(self) -> RepetitionVerdict | None:
        # Priority: exact > error_class > ping_pong. Exact repeats are the
        # cheapest and strongest signal (identical action, no ambiguity);
        # error-class loops are the next-strongest (still deterministic,
        # just args-blind); ping-pong is the weakest/most expensive (needs
        # a full window and a shape check), so it's only consulted once the
        # two consecutive-streak streams have had their say.
        if self._exact_streak_count >= self.exact_threshold:
            return RepetitionVerdict(
                stream="exact",
                count=self._exact_streak_count,
                hashes=(self._exact_streak_hash,),
            )
        if self._error_streak_count >= self.error_threshold:
            return RepetitionVerdict(
                stream="error_class",
                count=self._error_streak_count,
                hashes=(self._error_streak_hash,),
            )
        return self._pingpong_verdict()

    def _pingpong_verdict(self) -> RepetitionVerdict | None:
        window_size = 2 * self.pingpong_threshold
        if len(self._exact_window) < window_size:
            return None
        window = list(self._exact_window)
        distinct = set(window)
        if len(distinct) != 2:
            return None
        if any(window[i] == window[i - 1] for i in range(1, len(window))):
            return None
        return RepetitionVerdict(
            stream="ping_pong",
            count=window_size,
            hashes=(window[0], window[1]),
        )

    def recent_hashes(self, n: int = 20) -> list[dict]:
        items = list(self._recent)[-n:]
        return [{"tool": name, "hash": h} for name, h in items]
