"""Generic CLI runner base.

Hosts the shared worker execution contract: the ``NEEDS_HUMAN:`` convention,
env construction, credential preparation, and a provider-agnostic subprocess
driver (``_run_cli_agent``) with watchdog timeout, abort-on-lock-loss, and
retry-with-backoff. Per-provider Runners subclass ``_BaseCliRunner`` and
implement a small set of hooks; ``_BaseCliRunner.run`` drives them.

Hooks each per-provider Runner overrides:
    build_argv(binary, prompt, model, work_dir) -> list[str]
    extract_summary(stdout) -> str
    extract_agent_text(stdout) -> str   # feeds the shared _check_needs_human
    detect_exhaustion(stdout, stderr) -> tuple[bool, int]
    detect_overload(stdout, stderr) -> bool
    prepare_credential(credential, work_dir, home_dir, env_var_name) -> CredentialContext
    extract_credential_update(watch_path, original) -> str | None
    classify_timeout(stdout_lines, stderr_lines, timeout) -> RunnerResult | None
    postprocess_result(result, stdout_lines, stderr_lines) -> RunnerResult

NEEDS_HUMAN contract: ``extract_agent_text`` returns only agent-authored text
that the shared ``NEEDS_HUMAN:`` regex may scan. CLIs whose output echoes the
prompt (Claude's stream-json ``user`` messages) MUST strip that echo;
CLIs that take the prompt on stdin / never echo it (Codex, Grok) need not.

Exhaustion-first dispatch invariant: the driver checks ``detect_exhaustion``
then ``detect_overload`` BEFORE the exit code, because some CLIs exit
non-zero on a silent rate-limit while others exit 0 with an error envelope in
stdout.

Credential write-back (Path B / OAuth-blob providers): ``prepare_credential``
may write a credential blob to a CLI cache file (Codex ``~/.codex/auth.json``,
Grok ``~/.grok/auth.json``) under ``home_dir`` and return its path as
``watch_path``. After the run, the driver calls ``extract_credential_update``
on that path; if the CLI refreshed the token in place, the driver surfaces the
new blob on ``RunnerResult.credential_update`` so the orchestrator can persist
it (orchestrator-side persistence is a separate change).

Activity watchdog integration (task B8): ``_run_cli_agent`` accepts an
optional ``tracker_factory``. The task-B8 brief's original interface sketch
was ``tracker: LivenessTracker | None``, but ``LivenessTracker`` takes the
child's ``root_pid`` at construction time (it binds a ``ProcessTreeSampler``
to it immediately), and the pid does not exist until *after* ``Popen`` --
and a fresh pid exists for every retry attempt, since each attempt spawns a
new process tree. A single pre-built tracker instance can't express that.
So the caller (``worker/loop.py``) passes a ``Callable[[int], LivenessTracker]``
instead; this driver calls it with ``proc.pid`` immediately after each
successful ``Popen`` and gets a tracker scoped to that one attempt, closed
(``tracker.close()``) when the attempt ends regardless of outcome. When
``tracker_factory`` is ``None`` (the caller's ``watchdog.enabled: False``
rollback lever), the fixed wall-clock watchdog runs byte-for-byte as before
-- this is the load-bearing rollback path, see ``global-constraints.md``.

Import note: this module must NOT import ``liveness_tracker`` (or its
transitive dependency ``stream_liveness``) at module scope --
``stream_liveness`` imports ``claude_runner``, which imports this module,
which would deadlock the import cycle. ``LivenessTracker`` is only ever
referenced here as a ``TYPE_CHECKING``-guarded type hint; the runtime code
never constructs one directly, only calls the factory it's handed.
"""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import subprocess
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from orcest.worker.runner import RunnerResult

if TYPE_CHECKING:
    from orcest.worker.liveness_tracker import LivenessTracker

# Environment variables safe to forward to provider subprocesses.
# Whitelist (not os.environ.copy()) to avoid leaking secrets for other services.
# Credential env vars themselves are NOT whitelisted: they would cross-leak
# (a grok subprocess inheriting ANTHROPIC_API_KEY, etc.). The credential for
# the current task's provider is injected explicitly via prepare_credential.
_ENV_WHITELIST: set[str] = {
    "PATH",
    "HOME",
    "USER",
    "SHELL",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TERM",
    "TMPDIR",
    "TZ",
    "NODE_PATH",
    "NODE_OPTIONS",
    "XDG_CONFIG_HOME",
    "XDG_DATA_HOME",
    "XDG_CACHE_HOME",
    "GIT_AUTHOR_NAME",
    "GIT_AUTHOR_EMAIL",
    "GIT_COMMITTER_NAME",
    "GIT_COMMITTER_EMAIL",
    # Claude provider routing flags. Harmless to forward to other CLIs.
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_USE_VERTEX",
}


# NEEDS_HUMAN: <reason> — orcest worker convention, emitted by the fix prompt.
# Anchored to start-of-line so a mid-sentence mention does not trip it.
_NEEDS_HUMAN_RE = re.compile(r"(?m)^[ \t>]*NEEDS_HUMAN:[ \t]*([^\n]{1,300})")


def _build_base_env(token: str, extra_env_keys: set[str] | None = None) -> dict[str, str]:
    """Whitelisted env + GITHUB_TOKEN/GH_TOKEN, with NO provider credential.

    Credential injection is left to ``prepare_credential`` so env-var and
    blob-file providers can be handled uniformly.
    """
    keys = _ENV_WHITELIST | (extra_env_keys or set())
    env: dict[str, str] = {}
    for key in keys:
        val = os.environ.get(key)
        if val is not None:
            env[key] = val
    env["GITHUB_TOKEN"] = token
    env["GH_TOKEN"] = token
    return env


def _build_env(
    token: str,
    credential: str = "",
    env_var_name: str = "",
    extra_env_keys: set[str] | None = None,
) -> dict[str, str]:
    """Minimal environment for a provider subprocess (env-var credential).

    Whitelisted parent env vars + GITHUB_TOKEN/GH_TOKEN, plus the provider
    credential under ``env_var_name``. If ``credential`` is empty, falls back
    to the parent process's value for that var. The credential is never on
    argv. Retained for ``claude_interactive_runner`` (its own driver).
    """
    env = _build_base_env(token, extra_env_keys)
    if env_var_name:
        if credential:
            env[env_var_name] = credential
        else:
            parent_val = os.environ.get(env_var_name)
            if parent_val:
                env[env_var_name] = parent_val
    return env


def _check_needs_human(agent_text: str) -> tuple[bool, str]:
    """Detect a worker-reported human-decision blocker in already-filtered
    agent text. Returns (flag, reason); a ``<placeholder>`` reason is ignored.

    Named distinctly from ``claude_runner._parse_needs_human`` (which takes raw
    stream-json and extracts agent text first) to avoid a same-name,
    different-signature footgun.
    """
    if not agent_text:
        return False, ""
    m = _NEEDS_HUMAN_RE.search(agent_text)
    if not m:
        return False, ""
    reason = m.group(1).strip()
    if reason.startswith("<"):
        return False, ""
    return True, reason


@dataclass
class CredentialContext:
    """What ``prepare_credential`` tells the driver about credential delivery.

    - ``extra_env``: env vars to inject (env-var-credential providers put the
      credential here, e.g. ``{"XAI_API_KEY": ...}`` / ``{"CODEX_API_KEY": ...}``).
    - ``watch_path``: a credential file the CLI may refresh in place
      (OAuth-blob providers). After the run, the driver reads it back via
      ``extract_credential_update`` to capture a rotated token.
    """

    extra_env: dict[str, str] = field(default_factory=dict)
    watch_path: Path | None = None


def _kill_process_tree(proc: subprocess.Popen[str], sigterm_timeout: float = 2.0) -> None:
    """Kill a subprocess and all children via process-group signal.

    The child leads its own process group (start_new_session=True), so
    signalling the group reaps grandchildren too. SIGTERM, then SIGKILL after
    ``sigterm_timeout``.
    """
    # Pre-existing theoretical hazard (parity with the legacy watchdog): if
    # the child was already reaped, proc.pid could have been recycled and
    # getpgid/killpg would target an unrelated process group.
    try:
        pgid = os.getpgid(proc.pid)
    except (ProcessLookupError, PermissionError):
        return
    try:
        os.killpg(pgid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        return
    try:
        proc.wait(timeout=sigterm_timeout)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(pgid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def _close_pipes(proc: subprocess.Popen[str]) -> None:
    """Close stdout/stderr pipes; idempotent."""
    for pipe in (proc.stdout, proc.stderr):
        if pipe is not None and hasattr(pipe, "close"):
            try:
                pipe.close()
            except OSError:
                pass


def _drain_stderr(
    proc: subprocess.Popen[str],
    on_stderr: Callable[[str], None] | None = None,
) -> tuple[list[str], threading.Thread]:
    """Read stderr in a background thread to avoid pipe deadlock.

    Returns (lines, thread); caller ``join``s before reading. ``on_stderr``
    receives each line; callback raises are swallowed.
    """
    lines: list[str] = []

    def _reader() -> None:
        if proc.stderr is None:  # pragma: no cover
            return
        try:
            for line in proc.stderr:
                lines.append(line)
                if on_stderr is not None:
                    try:
                        on_stderr(line)
                    except Exception:
                        pass
        except (OSError, ValueError):
            pass

    thread = threading.Thread(target=_reader, daemon=True)
    thread.start()
    return lines, thread


def _run_cli_agent(
    runner: _BaseCliRunner,
    prompt: str,
    work_dir: Path,
    token: str,
    timeout: int,
    *,
    binary: str,
    env_var_name: str,
    credential: str,
    model: str,
    home_dir: Path,
    logger: logging.Logger | None,
    on_output: Callable[[str], None] | None,
    on_stderr: Callable[[str], None] | None,
    abort_event: threading.Event,
    tracker_factory: Callable[[int], LivenessTracker] | None = None,
) -> RunnerResult:
    """Provider-agnostic subprocess driver. Spawns the CLI, streams stdout,
    drains stderr, enforces a watchdog timeout, retries transient crashes with
    backoff, and dispatches parsing to the runner's hooks.

    Credential delivery goes through ``runner.prepare_credential`` (env var or
    blob file). After a completed run, ``runner.extract_credential_update`` is
    consulted on any ``watch_path`` to capture a refreshed OAuth token.

    ``tracker_factory``, when not ``None``, replaces the fixed wall-clock
    watchdog with the activity-ladder-driven one (see the module docstring's
    "Activity watchdog integration" note for why this is a factory and not a
    pre-built tracker).
    """
    env = _build_base_env(token, runner.extra_env_keys)
    cred_ctx = runner.prepare_credential(credential, work_dir, home_dir, env_var_name)
    env.update(cred_ctx.extra_env)
    env["HOME"] = str(home_dir)

    cmd = runner.build_argv(binary, "" if runner.prompt_via_stdin else prompt, model, work_dir)
    stdin_input = prompt if runner.prompt_via_stdin else None

    if logger:
        logger.info(
            "Launching %s: cwd=%s, home=%s, timeout=%ds, env_vars=%s",
            binary,
            work_dir,
            home_dir,
            timeout,
            sorted(env.keys()),
        )

    stdout_lines: list[str] = []
    stderr_lines: list[str] = []

    def _finish(result: RunnerResult) -> RunnerResult:
        # Provider-specific post-processing of the outgoing result (e.g.
        # Claude's NEEDS_HUMAN scan over the run's raw output, which must
        # apply to failed results too — the base loop only checks it on
        # rc==0). ``stdout_lines``/``stderr_lines`` are rebound per attempt;
        # the closure always sees the current attempt's output.
        result = runner.postprocess_result(result, stdout_lines, stderr_lines)
        # Capture a refreshed credential blob (if the CLI rotated it in place).
        if cred_ctx.watch_path is not None:
            try:
                update = runner.extract_credential_update(cred_ctx.watch_path, credential)
            except Exception:  # pragma: no cover - defensive
                update = None
            if update:
                result.credential_update = update
                # Ordering is assigned by shared Redis immediately before
                # publication. Filesystem mtime comes from independent worker
                # VM clocks and is not a safe cross-process version.
                result.credential_update_minted_at = 0.0
        return result

    for attempt in range(1, runner.max_retries + 1):
        attempt_start = time.monotonic()
        if logger:
            logger.info("%s attempt %d/%d", binary, attempt, runner.max_retries)

        try:
            proc = subprocess.Popen(
                cmd,
                cwd=work_dir,
                env=env,
                stdin=subprocess.PIPE if stdin_input is not None else subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
        except (OSError, ValueError) as e:
            if logger:
                logger.error("Failed to start %s: %s", binary, e)
            return _finish(RunnerResult(success=False, summary=f"Failed to start: {e}"))

        # A fresh tracker per attempt: LivenessTracker binds a
        # ProcessTreeSampler to a root_pid at construction, and each retry
        # attempt spawns a brand-new process tree (see module docstring's
        # "Activity watchdog integration" note). Closed in the `finally`
        # below regardless of how this attempt ends.
        tracker: LivenessTracker | None = None
        if tracker_factory is not None:
            try:
                tracker = tracker_factory(proc.pid)
            except Exception:
                if logger:
                    logger.warning(
                        "tracker_factory raised; falling back to fixed watchdog",
                        exc_info=True,
                    )

        try:
            if stdin_input is not None and proc.stdin is not None:
                try:
                    proc.stdin.write(stdin_input)
                    proc.stdin.close()
                except (BrokenPipeError, OSError):
                    pass

            auth_required = threading.Event()

            def _handle_stderr(line: str) -> None:
                if on_stderr is not None:
                    try:
                        on_stderr(line)
                    except Exception:
                        pass
                if runner.detect_auth_prompt(line):
                    auth_required.set()
                    if logger:
                        logger.warning("%s requested interactive authentication", binary)
                    _kill_process_tree(proc)

            try:
                stderr_lines, stderr_thread = _drain_stderr(proc, on_stderr=_handle_stderr)
            except RuntimeError:
                _kill_process_tree(proc)
                _close_pipes(proc)
                if attempt < runner.max_retries:
                    abort_event.wait(timeout=runner.retry_backoff)
                    if abort_event.is_set():
                        break
                continue

            if proc.stdout is None:  # pragma: no cover
                _kill_process_tree(proc)
                stderr_thread.join(timeout=5)
                _close_pipes(proc)
                raise RuntimeError("Popen stdout pipe is None despite PIPE flag")

            watchdog_cancelled = threading.Event()
            watchdog_killed = threading.Event()
            # Only meaningful when `tracker` is not None: which ladder
            # trigger ("stuck" | "looping" | "ceiling") fired the kill.
            killed_trigger: str | None = None

            if tracker is None:
                watchdog_remaining = max(0.0, timeout - (time.monotonic() - attempt_start))

                def _watchdog(
                    _proc: subprocess.Popen[str] = proc,
                    _remaining: float = watchdog_remaining,
                    _cancelled: threading.Event = watchdog_cancelled,
                    _killed: threading.Event = watchdog_killed,
                ) -> None:
                    if _remaining > 0:
                        _cancelled.wait(timeout=_remaining)
                    if not _cancelled.is_set():
                        _killed.set()
                        _kill_process_tree(_proc)
            else:

                def _watchdog(
                    _proc: subprocess.Popen[str] = proc,
                    _tracker: LivenessTracker = tracker,
                    _interval: float = tracker.sample_interval,
                    _cancelled: threading.Event = watchdog_cancelled,
                    _killed: threading.Event = watchdog_killed,
                    _timeout: float = timeout,
                    _attempt_start: float = attempt_start,
                ) -> None:
                    nonlocal killed_trigger
                    # I2: tick() must never be allowed to silently disable
                    # kill protection for the rest of this attempt. If it
                    # raises (a bug in the ladder/tracker glue, an
                    # unexpected Redis client exception shape, etc.), log it
                    # once and fall back to an inline wall-clock ceiling
                    # check for every remaining iteration -- the task still
                    # gets killed at `timeout`, just without ladder-driven
                    # early detection.
                    tick_failure_logged = False
                    while not _cancelled.wait(timeout=_interval):
                        try:
                            trigger = _tracker.tick()
                        except Exception:
                            if not tick_failure_logged:
                                if logger:
                                    logger.error(
                                        "tracker.tick() raised; falling back to inline "
                                        "wall-clock ceiling kill protection for the rest "
                                        "of this attempt",
                                        exc_info=True,
                                    )
                                tick_failure_logged = True
                            elapsed = time.monotonic() - _attempt_start
                            trigger = "ceiling" if elapsed >= _timeout else None
                        # Re-check cancellation right before latching: tick()
                        # can stall (e.g. a wedged Redis call) past this
                        # attempt's watchdog_thread.join(timeout=5), in which
                        # case the main thread has already moved on to a new
                        # attempt with its own `killed_trigger`. A late-firing
                        # stale-attempt trigger must not overwrite it (review
                        # round 1 hardening).
                        if trigger and not _cancelled.is_set():
                            killed_trigger = trigger
                            _killed.set()
                            _kill_process_tree(_proc)
                            break

            watchdog_thread = threading.Thread(target=_watchdog, daemon=True)
            try:
                watchdog_thread.start()
            except RuntimeError:
                _kill_process_tree(proc)
                stderr_thread.join(timeout=5)
                _close_pipes(proc)
                if attempt < runner.max_retries:
                    abort_event.wait(timeout=runner.retry_backoff)
                    if abort_event.is_set():
                        break
                continue

            stdout_lines = []
            timed_out = False
            streaming_on_output = on_output
            try:
                for line in proc.stdout:
                    stdout_lines.append(line)
                    if tracker is not None:
                        try:
                            tracker.observe_line(line)
                        except Exception:
                            if logger:
                                logger.warning(
                                    "tracker.observe_line failed", exc_info=True
                                )
                    if streaming_on_output is not None:
                        try:
                            streaming_on_output(line)
                        except Exception:
                            if logger:
                                logger.warning(
                                    "on_output callback raised; disabling streaming",
                                    exc_info=True,
                                )
                            streaming_on_output = None
                    if abort_event.is_set():
                        watchdog_cancelled.set()
                        watchdog_thread.join(timeout=5)
                        _kill_process_tree(proc)
                        stderr_thread.join(timeout=5)
                        _close_pipes(proc)
                        if logger:
                            logger.warning("%s subprocess killed: lock lost", binary)
                        # A ladder kill may have latched (consuming kill
                        # budget) just before the abort won: account for it
                        # in the event stream even though the lock-lost
                        # result below supersedes it. No post-kill death
                        # verification ran on this path, so no "verified"
                        # field is emitted.
                        if tracker is not None and killed_trigger is not None:
                            tracker.emit_killed(killed_trigger, superseded_by="abort")
                        return _finish(
                            RunnerResult(
                                success=False, summary="Aborted: lock lost", transient=True
                            )
                        )
                    # With a tracker, CEILING is the ladder's job (evaluated
                    # in the watchdog thread via tick()); the inline
                    # wall-clock check below is only for the no-tracker
                    # (disabled-watchdog) fixed-timeout path.
                    if tracker is None and time.monotonic() - attempt_start >= timeout:
                        timed_out = True
                        break
            except Exception as stdout_exc:
                watchdog_cancelled.set()
                watchdog_thread.join(timeout=5)
                _kill_process_tree(proc)
                stderr_thread.join(timeout=5)
                _close_pipes(proc)
                if watchdog_killed.is_set():
                    timed_out = True
                else:
                    if logger:
                        logger.warning("stdout read failed: %s", stdout_exc, exc_info=True)
                    if attempt < runner.max_retries:
                        abort_event.wait(timeout=runner.retry_backoff)
                        if abort_event.is_set():
                            break
                    continue

            watchdog_cancelled.set()
            watchdog_thread.join(timeout=5)
            if not timed_out and watchdog_killed.is_set():
                timed_out = True

            if not timed_out:
                try:
                    proc.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    _kill_process_tree(proc)
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        pass
            else:
                _kill_process_tree(proc)
            stderr_thread.join(timeout=5)
            _close_pipes(proc)

            # Verified kill (spec: D-state escalation). Only applies to a
            # ladder-driven kill (tracker present, a trigger latched); the
            # legacy fixed-watchdog timeout never verifies death.
            if tracker is not None and timed_out and killed_trigger is not None:
                time.sleep(2)
                # Known limitation (deliberate, not worth a heavy fix):
                # verification only sees the surviving IN-CLOSURE tree --
                # the root pid plus descendants reachable via the ppid
                # walk. A D-state grandchild whose intermediate parents
                # already died is reparented to init, leaves the closure,
                # and is invisible here: state_of_tree() returns [] and
                # the kill reads as "verified" even though a D-state
                # orphan survives. Cheaply tracking reparented orphans is
                # not possible from this vantage point; such a VM falls
                # back to the pool's max_task_duration ceiling reap.
                try:
                    states = tracker.tree_states()
                    verified = "D" not in states
                except Exception:
                    # Fail closed: a tree_states() failure must never
                    # yield an affirmative "verified" kill. Treat exactly
                    # like an observed D state -- mark for the pool
                    # reaper and report the kill as unverified.
                    verified = False
                    if logger:
                        logger.warning("tracker.tree_states() failed", exc_info=True)
                if not verified:
                    tracker.mark_needs_reap()
                tracker.emit_killed(killed_trigger, verified)

            stdout = "".join(stdout_lines)
            stderr = "".join(stderr_lines)

            if auth_required.is_set():
                return _finish(
                    RunnerResult(
                        success=False,
                        summary=runner.auth_required_summary(),
                    )
                )

            # A corroborated stall (activity-ladder stuck/looping kill) is
            # returned before the exhaustion/overload scans and is never
            # handed to ``classify_timeout``: rate-limit noise in a stalled
            # run's partial output must not reclassify it — the stall itself
            # is the diagnosis (task B9 controller ruling).
            if timed_out and tracker is not None and killed_trigger in ("stuck", "looping"):
                if logger:
                    logger.error(
                        "%s killed by activity watchdog: trigger=%s", binary, killed_trigger
                    )
                snapshot_head = json.dumps(tracker.last_snapshot(), default=str)[:500]
                return _finish(
                    RunnerResult(
                        success=False,
                        summary=f"STALLED({killed_trigger}): {snapshot_head}",
                        transient=False,
                    )
                )

            # Exhaustion / overload first (some CLIs exit 0 with an error envelope).
            exhausted, resets_at = runner.detect_exhaustion(stdout, stderr)
            if exhausted:
                return _finish(
                    RunnerResult(
                        success=False,
                        summary="Usage limit reached",
                        usage_exhausted=True,
                        rate_limit_resets_at=resets_at,
                    )
                )
            if runner.detect_overload(stdout, stderr):
                return _finish(
                    RunnerResult(success=False, summary="Provider overloaded", transient=True)
                )

            if timed_out:
                # Only ceiling / fixed wall-clock timeouts reach this point
                # (STALLED stuck/looping kills returned above). Give the
                # provider a chance to reclassify before the generic timeout
                # result is built: Claude, for example, can stall at the wall
                # clock because it silently hit a usage/rate limit and the
                # CLI never exited — exactly what the legacy
                # ``_timeout_claude_result`` reclassified. STALLED results
                # are deliberately NOT offered to this hook (see above).
                reclassified = runner.classify_timeout(stdout_lines, stderr_lines, timeout)
                if reclassified is not None:
                    return _finish(reclassified)
                if logger:
                    stderr_snippet = stderr.strip()[:1000]
                    logger.error(
                        "%s timed out after %ds (stdout_lines=%d, stderr=%s)",
                        binary,
                        timeout,
                        len(stdout_lines),
                        stderr_snippet if stderr_snippet else "(empty)",
                    )
                return _finish(
                    RunnerResult(
                        success=False, summary=f"Timed out after {timeout}s", transient=True
                    )
                )

            rc = proc.returncode
            if rc is None:
                return _finish(
                    RunnerResult(
                        success=False,
                        summary="Process did not exit (stuck in D-state)",
                        transient=True,
                    )
                )
            if rc == 0:
                summary = runner.extract_summary(stdout)
                needs_human, reason = _check_needs_human(runner.extract_agent_text(stdout))
                return _finish(
                    RunnerResult(
                        success=True,
                        summary=summary,
                        needs_human=needs_human,
                        needs_human_reason=reason,
                    )
                )

            if logger:
                logger.warning("%s exited with code %d: %s", binary, rc, stderr[:500])
            if attempt < runner.max_retries:
                abort_event.wait(timeout=runner.retry_backoff)
                if abort_event.is_set():
                    break
        finally:
            if tracker is not None:
                tracker.close()

    # Repeated non-zero exits with no explicit transient signal: treat as
    # transient infrastructure failure (retried by the orchestrator), never
    # escalated to needs-human.
    return _finish(
        RunnerResult(
            success=False,
            summary=f"Failed after {runner.max_retries} attempts",
            transient=True,
        )
    )


class _BaseCliRunner(ABC):
    """Base for CLI-driven provider runners.

    Provides a concrete ``run`` (via ``_run_cli_agent``). Subclasses implement
    the parsing/argv hooks; OAuth-blob providers also override the credential
    hooks. Providers with bespoke timeout/result semantics (Claude) override
    ``classify_timeout`` / ``postprocess_result``.
    """

    # Extra safe env keys to forward (CLI-specific routing flags). ClassVar so
    # type-checkers flag accidental ``self.extra_env_keys.add(...)`` mutation
    # (which would silently corrupt the shared base-class set for all runners).
    extra_env_keys: ClassVar[set[str]] = set()

    # When True, the prompt is piped on stdin instead of placed in argv
    # (Codex's ``codex exec ... -``). Claude/Grok use ``-p <prompt>`` on argv.
    prompt_via_stdin: ClassVar[bool] = False

    def __init__(
        self,
        max_retries: int = 3,
        retry_backoff: int = 10,
        model: str = "",
    ) -> None:
        if max_retries < 1:
            raise ValueError(f"max_retries must be >= 1, got {max_retries}")
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.model = model

    # --- Parsing / argv hooks (required) -----------------------------------

    @abstractmethod
    def build_argv(self, binary: str, prompt: str, model: str, work_dir: Path) -> list[str]:
        """Return the argv list. Credential MUST NOT appear here — env/blob only.

        ``work_dir`` is passed so providers with a workspace flag (Codex
        ``--cd``, Grok ``--cwd``) can include it. ``prompt`` is empty when
        ``prompt_via_stdin`` is True (the driver pipes it on stdin)."""

    @abstractmethod
    def extract_summary(self, stdout: str) -> str:
        """Human-readable summary from the CLI's stdout."""

    @abstractmethod
    def extract_agent_text(self, stdout: str) -> str:
        """Agent-authored text only (strip any prompt echo); feeds NEEDS_HUMAN."""

    @abstractmethod
    def detect_exhaustion(self, stdout: str, stderr: str) -> tuple[bool, int]:
        """(exhausted?, resets_at_unix). resets_at == 0 means unknown."""

    @abstractmethod
    def detect_overload(self, stdout: str, stderr: str) -> bool:
        """Transient 5xx / overload — orchestrator retries with backoff."""

    def detect_auth_prompt(self, text: str) -> bool:
        """Return True when a CLI requested interactive browser authentication."""
        return False

    def auth_required_summary(self) -> str:
        """Summary used when ``detect_auth_prompt`` aborts the subprocess."""
        return "Provider authentication required"

    # --- Result-shaping hooks (default: no-op) -----------------------------

    def classify_timeout(
        self, stdout_lines: list[str], stderr_lines: list[str], timeout: int
    ) -> RunnerResult | None:
        """Reclassify a ceiling / fixed wall-clock timeout, or return None.

        Called at the timeout-result site BEFORE the generic
        ``Timed out after {timeout}s`` result is constructed, and ONLY for
        wall-clock/ceiling timeouts — activity-ladder STALLED(stuck/looping)
        kills are never offered for reclassification (a corroborated stall
        with rate-limit noise in its partial output must stay STALLED).
        Claude uses this to convert a stall-at-the-wall caused by a silent
        usage/rate limit into a usage-exhausted result.
        """
        return None

    def postprocess_result(
        self, result: RunnerResult, stdout_lines: list[str], stderr_lines: list[str]
    ) -> RunnerResult:
        """Final provider-specific pass over every outgoing result.

        Called in the driver's ``_finish`` for all outcomes (success, timeout,
        exhaustion, retries exhausted, abort). Claude uses it to scan the
        run's raw output for the ``NEEDS_HUMAN:`` signal on non-success
        results too. Default: return ``result`` unchanged.
        """
        return result

    # --- Credential hooks (default: env-var injection) ---------------------

    def prepare_credential(
        self, credential: str, work_dir: Path, home_dir: Path, env_var_name: str
    ) -> CredentialContext:
        """Map ``credential`` to subprocess delivery.

        Default: inject it as the ``env_var_name`` env var (API-key providers).
        OAuth-blob providers (Grok, Codex) override this to write the blob to a
        CLI cache file under ``home_dir`` and return its ``watch_path``.
        """
        if credential:
            return CredentialContext(extra_env={env_var_name: credential})
        # Empty credential: fall back to the parent env value if present
        # (worker /opt/orcest/.env). Mirrors _build_env's fallback.
        parent_val = os.environ.get(env_var_name)
        if parent_val:
            return CredentialContext(extra_env={env_var_name: parent_val})
        return CredentialContext()

    def extract_credential_update(self, watch_path: Path, original: str) -> str | None:
        """Return a refreshed credential blob if the CLI rotated it, else None.
        Default: no write-back (env-var providers don't rotate in place)."""
        return None

    # --- Driver ------------------------------------------------------------

    def run(
        self,
        prompt: str,
        work_dir: Path,
        token: str,
        timeout: int,
        logger: logging.Logger | None = None,
        on_output: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        abort_event: threading.Event | None = None,
        claude_token: str = "",
        provider: str = "",
        credential: str = "",
        model: str = "",
        home_dir: Path | None = None,
        tracker_factory: Callable[[int], LivenessTracker] | None = None,
    ) -> RunnerResult:
        from orcest.worker.runner import get_provider_recipe

        if not credential and claude_token:
            credential = claude_token
        effective_provider = provider or self._default_provider()
        recipe = get_provider_recipe(effective_provider)
        binary = recipe.binary if recipe is not None else self._default_binary()
        env_var_name = recipe.env_var if recipe is not None else self._default_env_var()

        abort = abort_event if abort_event is not None else threading.Event()
        resolved_home = home_dir if home_dir is not None else Path(os.environ.get("HOME", "/root"))

        return _run_cli_agent(
            self,
            prompt,
            work_dir,
            token,
            timeout,
            binary=binary,
            env_var_name=env_var_name,
            credential=credential,
            model=model or self.model,
            home_dir=resolved_home,
            logger=logger,
            on_output=on_output,
            on_stderr=on_stderr,
            tracker_factory=tracker_factory,
            abort_event=abort,
        )

    # --- Defaults a subclass may override ----------------------------------

    def _default_provider(self) -> str:
        return ""

    def _default_binary(self) -> str:
        return ""

    def _default_env_var(self) -> str:
        return ""
