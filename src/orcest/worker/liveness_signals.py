"""Low-level activity samplers for the worker liveness watchdog.

Two independent, stateless-ish samplers feed the liveness ladder (see the
activity-watchdog spec):

- ``ProcessTreeSampler`` reports cumulative CPU seconds consumed by a
  process-tree rooted at a given pid, walking ``/proc`` directly.
- ``WorkspaceSampler`` reports whether any file under a workspace directory
  has been modified since a given timestamp.

Both samplers return raw facts only. Idle-detection policy (e.g. "N
consecutive zero CPU deltas means stuck") lives in the ladder, not here.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# /proc/[pid]/stat field indices (1-indexed, per proc(5)) that we care about.
_STATE_FIELD = 3
_PPID_FIELD = 4
_UTIME_FIELD = 14
_STIME_FIELD = 15
_CUTIME_FIELD = 16
_CSTIME_FIELD = 17

# Errors that mean "this pid vanished or became unreadable between listing
# and reading" -- always non-fatal for individual /proc reads.
_VANISHED_ERRORS = (FileNotFoundError, ProcessLookupError, PermissionError)


class _ProcStat:
    """Parsed fields of interest from /proc/[pid]/stat."""

    __slots__ = ("ppid", "state", "utime", "stime", "cutime", "cstime")

    def __init__(
        self, ppid: int, state: str, utime: int, stime: int, cutime: int, cstime: int
    ) -> None:
        self.ppid = ppid
        self.state = state
        self.utime = utime
        self.stime = stime
        self.cutime = cutime
        self.cstime = cstime


def _parse_stat_line(content: str) -> _ProcStat:
    """Parse a /proc/[pid]/stat line.

    The comm field (2nd field) is parenthesized and may itself contain
    spaces or parens (e.g. a process renamed to "my proc)"), so we split
    after the *last* ')' to reliably locate the remaining space-separated
    fields, which start at field 3 (state).
    """
    idx = content.rfind(")")
    rest = content[idx + 2 :].split()
    # rest[0] is field 3 (state), rest[1] is field 4 (ppid), etc.
    return _ProcStat(
        ppid=int(rest[_PPID_FIELD - _STATE_FIELD]),
        state=rest[0],
        utime=int(rest[_UTIME_FIELD - _STATE_FIELD]),
        stime=int(rest[_STIME_FIELD - _STATE_FIELD]),
        cutime=int(rest[_CUTIME_FIELD - _STATE_FIELD]),
        cstime=int(rest[_CSTIME_FIELD - _STATE_FIELD]),
    )


def _read_stat(pid: int) -> _ProcStat:
    with open(f"/proc/{pid}/stat") as f:
        return _parse_stat_line(f.read())


def _snapshot_all_procs() -> dict[int, _ProcStat]:
    """One pass over /proc/[0-9]*/stat, building a pid -> parsed-stat map.

    Individual unreadable/vanished pids are silently skipped -- processes
    routinely disappear between directory listing and file read.
    """
    procs: dict[int, _ProcStat] = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        try:
            procs[pid] = _read_stat(pid)
        except _VANISHED_ERRORS:
            continue
    return procs


def _descendant_closure(procs: dict[int, _ProcStat], root_pid: int) -> list[int]:
    """All pids in `procs` reachable from root_pid via ppid links, plus root itself."""
    children: dict[int, list[int]] = {}
    for pid, stat in procs.items():
        children.setdefault(stat.ppid, []).append(pid)

    result = [root_pid]
    seen = {root_pid}
    stack = [root_pid]
    while stack:
        pid = stack.pop()
        for child in children.get(pid, ()):
            if child not in seen:
                seen.add(child)
                result.append(child)
                stack.append(child)
    return result


class ProcessTreeSampler:
    """Samples cumulative CPU seconds for a process tree rooted at `root_pid`."""

    def __init__(self, root_pid: int) -> None:
        self.root_pid = root_pid
        self._clk_tck = os.sysconf("SC_CLK_TCK")

    def sample(self) -> float | None:
        """Cumulative CPU seconds (utime+stime) of every live descendant,
        plus root's cutime+cstime (which accumulates CPU from reaped
        children). Returns None if root is gone.
        """
        procs = _snapshot_all_procs()
        root_stat = procs.get(self.root_pid)
        if root_stat is None:
            return None

        total_ticks = 0
        for pid in _descendant_closure(procs, self.root_pid):
            stat = procs.get(pid)
            if stat is None:
                continue
            total_ticks += stat.utime + stat.stime
        total_ticks += root_stat.cutime + root_stat.cstime

        return total_ticks / self._clk_tck

    def state_of_tree(self) -> list[str]:
        """/proc/[pid]/stat state chars of all live tree members (root included)."""
        procs = _snapshot_all_procs()
        if self.root_pid not in procs:
            return []
        return [
            procs[pid].state for pid in _descendant_closure(procs, self.root_pid) if pid in procs
        ]


class WorkspaceSampler:
    """Samples whether any file under `root` has changed since a timestamp."""

    def __init__(self, root: Path, max_entries: int = 20000) -> None:
        self.root = Path(root)
        self.max_entries = max_entries
        self._cap_logged = False

    def changed_since(self, ts: float) -> bool:
        """True on the first file whose mtime is newer than `ts`.

        Traversal is bounded at `max_entries` files to keep this cheap on
        huge workspaces; if the cap is hit before finding anything newer,
        returns False (the cap event is logged once per sampler instance).
        """
        count = 0
        for dirpath, _dirnames, filenames in os.walk(self.root):
            for name in filenames:
                if count >= self.max_entries:
                    if not self._cap_logged:
                        logger.warning(
                            "WorkspaceSampler hit max_entries cap (%d) scanning %s; "
                            "traversal truncated",
                            self.max_entries,
                            self.root,
                        )
                        self._cap_logged = True
                    return False
                count += 1
                path = os.path.join(dirpath, name)
                try:
                    mtime = os.stat(path).st_mtime
                except OSError:
                    # File vanished or became unreadable mid-walk.
                    continue
                if mtime > ts:
                    return True
        return False
