"""Interactive Claude Code runner.

Runs the installed ``claude`` CLI in its default interactive mode through a
PTY. This deliberately avoids ``-p`` / ``--print`` so pool workers use Claude
Code's interactive billing path while preserving Orcest's Runner contract.
"""

from __future__ import annotations

import codecs
import errno
import fcntl
import logging
import os
import pty
import re
import select
import struct
import subprocess
import termios
import threading
import time
import uuid
from collections.abc import Callable
from enum import Enum
from pathlib import Path

from orcest.worker._runner_base import _build_env, _check_needs_human, _kill_process_tree
from orcest.worker.claude_runner import _is_usage_exhausted
from orcest.worker.runner import RunnerResult, get_provider_recipe

_ANSI_RE = re.compile(r"\x1b(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\))")
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
# Consecutive idle select() ticks (0.25s each) the terminal must stay quiet
# before a composer match is trusted.  A dialog emits its caret marker before
# its option numbers, so a buffer sampled mid-render holds a bare "❯" line that
# is indistinguishable from the composer; waiting for the render to settle is
# what tells them apart.
_COMPOSER_SETTLE_TICKS = 3
# Fall back to sending on a composer match without full quiet after this many
# seconds without a new setup dialog.  A TUI that repaints continuously would
# otherwise never satisfy the quiet rule and the task would stall outright; by
# this point any dialog has long since been classified and answered.
_COMPOSER_SETTLE_FALLBACK_SECONDS = 20.0
# A numbered selection row ("❯ 1. …"); the composer glyph is never numbered.
_MENU_OPTION_RE = re.compile(r"❯\s*\d+\s*\.")
# The composer's bracketed-paste placeholder ("[Pasted text #1 +42 lines]").
# Its presence after a submission keystroke means the Enter did not land.
_PASTED_PLACEHOLDER_RE = re.compile(r"\[Pasted text #\d+")
# Static rows Claude Code 2.1.235 paints below the composer. Ink often places
# words with absolute-column CSI sequences instead of literal spaces; those
# sequences disappear from our append-only transcript, so match a compacted
# representation of only the captured inert rows. Unknown trailing rows
# (especially dialog instructions) remain ambiguous and never authorize Enter.
_COMPOSER_FOOTER_COMPACT_RES = (
    re.compile(r"^[\u2500-\u257f]+$"),
    re.compile(r"^\?forshortcuts(?:[·|]?\d{1,3}%(?:untilauto-compact|contextused))?$"),
    re.compile(r"^\d{1,3}%(?:untilauto-compact|contextused)$"),
    # Multiline bracketed paste help. The second spelling is the exact raw-PTY
    # 2.1.235 delta after absolute-column moves are removed (unchanged letters
    # already present on the terminal grid are absent from the transcript).
    re.compile(r"^(?:pasteagaintoexpand|pastegaintoexpad)$"),
    re.compile(
        r"^⏵⏵bypasspermissionson\(shift\+tabtocycle\)"
        r"(?:·←foragents)?(?:●(?:low|medium|high|xhigh|max)·/effort)?$"
    ),
    re.compile(r"^●(?:low|medium|high|xhigh|max)·/effort$"),
)
# Total number of submission keystrokes, INCLUDING the initial Enter. A value
# of 3 permits the initial submission plus two evidence-backed retries. Keep
# the name and retry log denominator aligned with this total-attempt meaning.
_SUBMISSION_ATTEMPT_LIMIT = 3
# Minimum quiet time between submission keystrokes so a still-rendering TUI
# frame is never mistaken for a stuck composer.
_SUBMISSION_RETRY_SETTLE_SECONDS = 2.0
# Hard cap on confirming Claude accepted the pasted prompt, independent of
# (and far shorter than) the overall task timeout. A composer that never
# clears and never shows activity fails fast here instead of consuming the
# full runner timeout.
_PRE_EXECUTION_CONFIRM_DEADLINE_SECONDS = 60.0
_USAGE_ERROR_LINE_RE = re.compile(
    r"^\s*(?:error:|api error:|claude(?: code)? error:|rate limit|usage limit|quota|"
    r"billing limit|token limit)",
    re.IGNORECASE,
)


class _PostSubmitState(str, Enum):
    """What settled terminal evidence says about the submitted prompt."""

    EXECUTING = "executing"
    EXPLICITLY_STUCK = "explicitly_stuck"
    AMBIGUOUS = "ambiguous"


class _PtyOutputDecoder:
    """Stateful UTF-8 decoder for PTY reads that may split code points."""

    def __init__(self) -> None:
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self._finished = False

    def decode(self, chunk: bytes) -> str:
        if self._finished:
            return ""
        return self._decoder.decode(chunk, final=False)

    def finish(self) -> str:
        if self._finished:
            return ""
        self._finished = True
        return self._decoder.decode(b"", final=True)


def _is_interactive_usage_exhausted(
    terminal_output: str,
    submitted_prompt: str = "",
) -> bool:
    """Detect usage exhaustion in Claude's PTY stream without scanning prose.

    The PTY combines stdout, stderr, prompt echo, and assistant text. Reusing
    the stderr-oriented Claude detector on the full transcript can falsely
    bench credentials when normal text happens to contain words like "usage"
    and "limit". Only CLI-looking error/status lines are eligible here.
    """
    text = _CONTROL_RE.sub("", _ANSI_RE.sub("", terminal_output)).replace("\r", "\n")
    prompt_lines = {
        re.sub(r"\s+", " ", line).strip() for line in submitted_prompt.splitlines() if line.strip()
    }

    def is_prompt_echo(line: str) -> bool:
        normalized = re.sub(r"\s+", " ", line).strip()
        if normalized in prompt_lines:
            return True
        # Common TUI input prefixes are presentation, not trusted CLI output.
        without_prefix = normalized.lstrip(">❯│┃ ")
        return without_prefix in prompt_lines

    return any(
        not is_prompt_echo(line) and _USAGE_ERROR_LINE_RE.search(line) and _is_usage_exhausted(line)
        for line in text.splitlines()
    )


class ClaudeInteractiveRunner:
    """Runner that controls Claude Code interactively through a PTY."""

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

    def build_argv(self, binary: str, model: str) -> list[str]:
        """Return interactive Claude argv. Prompt and credential never appear here."""
        cmd = [binary, "--dangerously-skip-permissions"]
        if model:
            cmd += ["--model", model]
        return cmd

    def _prompt_with_result_contract(self, prompt: str, result_path: Path) -> str:
        return (
            prompt.rstrip()
            + "\n\n"
            + "ORCEST_WORKER_RESULT_CONTRACT:\n"
            + f"- Before stopping, write your final one-line summary to {result_path}.\n"
            + "- If a genuine human decision is required, include a standalone "
            + "`NEEDS_HUMAN: <reason>` line in that file.\n"
            + "- Do not write that file until all requested code/test/review work is complete.\n"
        )

    def _child_preexec(self) -> None:
        os.setsid()
        fcntl.ioctl(0, termios.TIOCSCTTY, 0)

    def _set_window_size(self, fd: int, rows: int = 40, cols: int = 120) -> None:
        fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))

    def _read_available(
        self,
        master_fd: int,
        terminal_output: list[str],
        on_output: Callable[[str], None] | None,
        logger: logging.Logger | None,
        decoder: _PtyOutputDecoder,
    ) -> bool:
        readable, _, _ = select.select([master_fd], [], [], 0)
        if not readable:
            return False
        try:
            chunk = os.read(master_fd, 4096)
        except OSError as exc:
            if exc.errno == errno.EIO:
                return False
            raise
        if not chunk:
            return False
        text = decoder.decode(chunk)
        if not text:
            return True
        self._record_output(text, terminal_output, on_output, logger)
        return True

    def _record_output(
        self,
        text: str,
        terminal_output: list[str],
        on_output: Callable[[str], None] | None,
        logger: logging.Logger | None,
    ) -> None:
        terminal_output.append(text)
        if on_output is not None:
            try:
                on_output(text)
            except Exception:
                if logger:
                    logger.warning(
                        "on_output callback raised; continuing",
                        exc_info=True,
                    )

    def _finish_output_decoder(
        self,
        decoder: _PtyOutputDecoder,
        terminal_output: list[str],
        on_output: Callable[[str], None] | None,
        logger: logging.Logger | None,
    ) -> None:
        text = decoder.finish()
        if text:
            self._record_output(text, terminal_output, on_output, logger)

    def _drain_available_output(
        self,
        master_fd: int,
        terminal_output: list[str],
        on_output: Callable[[str], None] | None,
        logger: logging.Logger | None,
        decoder: _PtyOutputDecoder,
    ) -> None:
        while self._read_available(master_fd, terminal_output, on_output, logger, decoder):
            pass

    def _drain_startup_output(
        self,
        master_fd: int,
        proc: subprocess.Popen[bytes],
        terminal_output: list[str],
        on_output: Callable[[str], None] | None,
        logger: logging.Logger | None,
        decoder: _PtyOutputDecoder,
        startup_delay: float = 3.0,
    ) -> None:
        deadline = time.monotonic() + startup_delay
        while proc.poll() is None and time.monotonic() < deadline:
            timeout = max(0.0, min(0.1, deadline - time.monotonic()))
            readable, _, _ = select.select([master_fd], [], [], timeout)
            if readable:
                self._read_available(master_fd, terminal_output, on_output, logger, decoder)

    def _read_result(self, result_path: Path) -> str | None:
        try:
            text = result_path.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        return text or None

    def _write_all(
        self,
        fd: int,
        data: bytes,
        abort_event: threading.Event | None = None,
        timeout: float = 10.0,
    ) -> None:
        old_flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, old_flags | os.O_NONBLOCK)
        written = 0
        deadline = time.monotonic() + timeout
        try:
            while written < len(data):
                if abort_event is not None and abort_event.is_set():
                    raise OSError("aborted while writing prompt")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("timed out writing prompt to Claude PTY")
                _, writable, _ = select.select([], [fd], [], min(0.1, remaining))
                if not writable:
                    continue
                try:
                    chunk = os.write(fd, data[written : written + 65536])
                except (BlockingIOError, InterruptedError):
                    continue
                if chunk <= 0:
                    raise OSError("Claude PTY accepted no prompt bytes")
                written += chunk
        finally:
            try:
                fcntl.fcntl(fd, fcntl.F_SETFL, old_flags)
            except OSError:
                pass

    def _send_prompt(
        self,
        master_fd: int,
        prompt: str,
        logger: logging.Logger | None,
        abort_event: threading.Event | None = None,
        timeout: float = 10.0,
    ) -> None:
        # Bracketed paste keeps multi-line prompts intact in terminal UIs.
        # Submission is a separate keystroke (see _send_submit_keystroke) so
        # a stuck composer can be recovered by retrying only the Enter, never
        # by re-pasting the prompt.
        payload = f"\x1b[200~{prompt}\x1b[201~".encode("utf-8")
        self._write_all(
            master_fd,
            payload,
            abort_event=abort_event,
            timeout=timeout,
        )
        if logger:
            logger.info("Pasted prompt to interactive Claude over PTY")

    def _send_submit_keystroke(
        self,
        master_fd: int,
        logger: logging.Logger | None,
        abort_event: threading.Event | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._write_all(
            master_fd,
            b"\r",
            abort_event=abort_event,
            timeout=timeout,
        )
        if logger:
            logger.info("Sent submission keystroke to interactive Claude")

    def _submit_full_prompt(
        self,
        master_fd: int,
        full_prompt: str,
        logger: logging.Logger | None,
        abort_event: threading.Event | None,
        timeout: float,
    ) -> None:
        self._send_prompt(master_fd, full_prompt, logger, abort_event=abort_event, timeout=timeout)
        self._send_submit_keystroke(master_fd, logger, abort_event=abort_event, timeout=timeout)

    def _looks_like_activity_state(self, text: str) -> bool:
        """True once Claude has visibly started working on the submitted prompt.

        Claude Code's TUI shows an "esc to interrupt" status line while a
        turn is in flight (spinner + tool calls). It is a single, unambiguous
        marker unlike the composer's caret, so no render-settle debounce is
        needed before trusting it.
        """
        stripped = _CONTROL_RE.sub("", _ANSI_RE.sub("", text))
        normalized = re.sub(r"\s+", "", stripped).lower()
        return "esctointerrupt" in normalized

    def _looks_like_pending_paste_composer(self, text: str) -> bool:
        """True when the latest settled composer frame still holds the paste."""
        return self._classify_post_submit_state(text) is _PostSubmitState.EXPLICITLY_STUCK

    @staticmethod
    def _is_composer_footer_line(line: str) -> bool:
        """Return whether *line* is known inert chrome below the composer."""
        unframed = line.strip(" \t│┃║|")
        compact = re.sub(r"[ \t\u00a0]+", "", unframed).lower()
        return not compact or any(
            pattern.fullmatch(compact) for pattern in _COMPOSER_FOOTER_COMPACT_RES
        )

    def _classify_post_submit_state(self, text: str) -> _PostSubmitState:
        """Classify only the latest settled post-submit terminal evidence.

        The interactive runner cannot infer execution from silence. Recovery is
        therefore allowed only for an explicit pending-paste placeholder in the
        latest composer frame. A numbered selection row is a menu, and all
        unrecognized output remains ambiguous so a bare Enter cannot accept an
        unknown dialog. Claude Code 2.1.235's accepted activity marker is
        documented by the redacted transcript fixture in the worker tests.
        """
        if self._looks_like_activity_state(text):
            return _PostSubmitState.EXECUTING

        stripped = _CONTROL_RE.sub("", _ANSI_RE.sub("", text)).replace("\r", "\n")
        lines = stripped.splitlines()
        caret_lines = [
            (index, unframed)
            for index, line in enumerate(lines)
            if (unframed := line.lstrip(" \t│┃║|")).startswith("❯")
        ]
        if not caret_lines:
            return _PostSubmitState.AMBIGUOUS

        latest_index, latest_caret = caret_lines[-1]
        if _MENU_OPTION_RE.match(latest_caret):
            return _PostSubmitState.AMBIGUOUS

        # The caret is explicit current evidence only when newer rows are known
        # inert composer chrome. Unknown trailing output (including dialog
        # instructions) makes the state ambiguous instead of letting a
        # historical placeholder authorize Enter.
        if any(not self._is_composer_footer_line(line) for line in lines[latest_index + 1 :]):
            return _PostSubmitState.AMBIGUOUS

        if _PASTED_PLACEHOLDER_RE.search(latest_caret):
            return _PostSubmitState.EXPLICITLY_STUCK

        # A latest, settled, non-menu composer without the paste placeholder
        # means the submitted text was consumed. Treat that as accepted without
        # sending another keystroke.
        return _PostSubmitState.EXECUTING

    def _looks_like_workspace_trust_prompt(self, text: str) -> bool:
        stripped = _CONTROL_RE.sub("", _ANSI_RE.sub("", text))
        normalized = re.sub(r"\s+", "", stripped).lower()
        return (
            "quicksafetycheck" in normalized
            and "itrustthisfolder" in normalized
            and "entertoconfirm" in normalized
        )

    def _looks_like_bypass_permissions_prompt(self, text: str) -> bool:
        stripped = _CONTROL_RE.sub("", _ANSI_RE.sub("", text))
        normalized = re.sub(r"\s+", "", stripped).lower()
        return (
            "bypasspermissionsmode" in normalized
            and "no,exit" in normalized
            and "yes,iaccept" in normalized
            and "entertoconfirm" in normalized
        )

    def _looks_like_mcp_server_prompt(self, text: str) -> bool:
        stripped = _CONTROL_RE.sub("", _ANSI_RE.sub("", text))
        normalized = re.sub(r"\s+", "", stripped).lower()
        has_new_mcp_prompt = (
            "newmcpserverfoundinthisproject" in normalized
            or "newmcpserversfoundinthisproject" in normalized
            or "newmcpserverfound" in normalized
            or "newmcpserversfound" in normalized
        )
        has_use_option = "usethismcpserver" in normalized or "usethesemcpservers" in normalized
        # Real TUI output repositions the cursor mid-word, so a literal option
        # string can arrive with a character missing (observed in production:
        # "Continue without using this MCP servr").  Match on a stem that
        # survives that instead of the full option text.
        has_decline_option = "continuewithoutusing" in normalized and "mcpserv" in normalized
        return (
            has_new_mcp_prompt
            and has_use_option
            and has_decline_option
            and "entertoconfirm" in normalized
        )

    def _looks_like_main_input_prompt(self, text: str) -> bool:
        """Return True only for Claude's actual interactive input marker."""
        if (
            self._looks_like_workspace_trust_prompt(text)
            or self._looks_like_bypass_permissions_prompt(text)
            or self._looks_like_mcp_server_prompt(text)
        ):
            return False
        stripped = _CONTROL_RE.sub("", _ANSI_RE.sub("", text)).replace("\r", "\n")
        # Claude Code renders its main composer with the distinctive ❯ glyph.
        # Setup menus use numbered selections and must never satisfy this gate.
        # The composer line may be framed inside a box border (e.g. "│ ❯ …"),
        # so tolerate leading box-drawing characters before the glyph.
        caret_lines = [
            unframed
            for line in stripped.splitlines()
            if (unframed := line.lstrip(" \t│┃║|")).startswith("❯")
        ]
        # A selection dialog marks its highlighted row with the same glyph as
        # the composer, so the glyph alone cannot tell them apart -- but only a
        # dialog numbers that row.  Rejecting on that shape also covers menus
        # that cannot be enumerated ahead of time, which the three explicit
        # exclusions above cannot (the CLI is installed unpinned).
        if any(_MENU_OPTION_RE.match(line) for line in caret_lines):
            return False
        return bool(caret_lines)

    def _confirm_workspace_trust_if_needed(
        self,
        master_fd: int,
        terminal_output: list[str],
        already_confirmed: bool,
        logger: logging.Logger | None,
    ) -> bool:
        if already_confirmed:
            return True
        recent_output = "".join(terminal_output[-8:])
        if not self._looks_like_workspace_trust_prompt(recent_output):
            return False
        os.write(master_fd, b"\r")
        if logger:
            logger.info("Confirmed Claude workspace trust prompt")
        return True

    def _confirm_bypass_permissions_if_needed(
        self,
        master_fd: int,
        terminal_output: list[str],
        already_confirmed: bool,
        logger: logging.Logger | None,
    ) -> bool:
        if already_confirmed:
            return True
        recent_output = "".join(terminal_output[-8:])
        if not self._looks_like_bypass_permissions_prompt(recent_output):
            return False
        # The warning defaults to "No, exit"; select numbered option 2 directly.
        os.write(master_fd, b"2\r")
        if logger:
            logger.info("Confirmed Claude bypass-permissions prompt")
        return True

    def _confirm_mcp_server_if_needed(
        self,
        master_fd: int,
        terminal_output: list[str],
        already_confirmed: bool,
        logger: logging.Logger | None,
    ) -> bool:
        if already_confirmed:
            return True
        recent_output = "".join(terminal_output[-8:])
        if not self._looks_like_mcp_server_prompt(recent_output):
            return False
        # Option 3 continues without enabling project MCP servers in unattended runs.
        os.write(master_fd, b"3\r")
        if logger:
            logger.info("Declined Claude project MCP server prompt")
        return True

    def _result_from_summary(self, summary: str) -> RunnerResult:
        needs_human, reason = _check_needs_human(summary)
        return RunnerResult(
            success=True,
            summary=summary[:500] if summary else "No summary available",
            needs_human=needs_human,
            needs_human_reason=reason,
        )

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
        provider: str = "claude",
        credential: str = "",
        model: str = "",
        home_dir: Path | None = None,
    ) -> RunnerResult:
        del on_stderr  # PTY combines stdout/stderr into one terminal stream.

        if not credential and claude_token:
            credential = claude_token
        recipe = get_provider_recipe(provider or "claude")
        binary = recipe.binary if recipe is not None else "claude"
        env_var = recipe.env_var if recipe is not None else "CLAUDE_CODE_OAUTH_TOKEN"
        env = _build_env(token, credential=credential, env_var_name=env_var)
        env.setdefault("TERM", "xterm-256color")
        if home_dir is not None:
            env["HOME"] = str(home_dir)

        result_dir = work_dir / ".orcest"
        result_dir.mkdir(parents=True, exist_ok=True)
        result_path = result_dir / f"claude-interactive-result-{uuid.uuid4().hex}.txt"
        full_prompt = self._prompt_with_result_contract(prompt, result_path)
        effective_model = model or self.model
        cmd = self.build_argv(binary, effective_model)
        abort = abort_event if abort_event is not None else threading.Event()

        for attempt in range(1, self.max_retries + 1):
            attempt_start = time.monotonic()
            attempt_deadline = attempt_start + max(0.0, float(timeout))
            proc: subprocess.Popen[bytes] | None = None
            master_fd = -1
            slave_fd = -1
            terminal_output: list[str] = []
            decoder = _PtyOutputDecoder()
            try:
                master_fd, slave_fd = pty.openpty()
                self._set_window_size(slave_fd)
                proc = subprocess.Popen(
                    cmd,
                    cwd=work_dir,
                    env=env,
                    stdin=slave_fd,
                    stdout=slave_fd,
                    stderr=slave_fd,
                    preexec_fn=self._child_preexec,
                )
                os.close(slave_fd)
                slave_fd = -1
                if logger:
                    logger.info(
                        "Launching interactive Claude: cwd=%s, timeout=%ds, attempt=%d/%d",
                        work_dir,
                        timeout,
                        attempt,
                        self.max_retries,
                    )
                workspace_trust_confirmed = False
                bypass_permissions_confirmed = False
                mcp_server_confirmed = False
                # prompt_pasted: bracketed-paste bytes were written.
                # submission_confirmed: Claude positively started acting on the
                # prompt (composer cleared or an activity signal was seen).
                # These are tracked separately because writing the paste bytes
                # only proves the write syscall succeeded, not that Claude's
                # TUI accepted the submission.
                prompt_pasted = False
                submission_confirmed = False
                submission_attempts = 0
                last_submission_at = 0.0
                paste_output_index = 0
                post_submit_state = _PostSubmitState.AMBIGUOUS
                explicit_stuck_deadline: float | None = None
                setup_output_index = 0
                quiet_ticks = 0
                prompt_resends = 0
                last_setup_at = time.monotonic()

                while True:
                    summary = self._read_result(result_path)
                    if summary is not None:
                        _kill_process_tree(proc)  # type: ignore[arg-type]
                        return self._result_from_summary(summary)

                    if abort.is_set():
                        _kill_process_tree(proc)  # type: ignore[arg-type]
                        return RunnerResult(
                            success=False,
                            summary="Aborted: lock lost",
                            transient=True,
                        )

                    now = time.monotonic()
                    if now >= attempt_deadline:
                        _kill_process_tree(proc)  # type: ignore[arg-type]
                        self._drain_available_output(
                            master_fd,
                            terminal_output,
                            on_output,
                            logger,
                            decoder,
                        )
                        self._finish_output_decoder(
                            decoder,
                            terminal_output,
                            on_output,
                            logger,
                        )
                        combined = "".join(terminal_output)
                        if _is_interactive_usage_exhausted(combined, full_prompt):
                            return RunnerResult(
                                success=False,
                                summary="Claude usage limit reached",
                                usage_exhausted=True,
                            )
                        if not prompt_pasted:
                            return RunnerResult(
                                success=False,
                                summary="Timed out waiting for interactive Claude input prompt",
                                transient=True,
                            )
                        return RunnerResult(
                            success=False,
                            summary=f"Timed out after {timeout}s",
                            transient=True,
                        )

                    if (
                        explicit_stuck_deadline is not None
                        and not submission_confirmed
                        and post_submit_state is _PostSubmitState.EXPLICITLY_STUCK
                        and now >= explicit_stuck_deadline
                    ):
                        # Give a just-arrived activity/clear frame one final
                        # non-blocking read before killing the process. The
                        # short deadline is valid only while the latest evidence
                        # still explicitly shows the pending paste.
                        self._drain_available_output(
                            master_fd,
                            terminal_output,
                            on_output,
                            logger,
                            decoder,
                        )
                        post_submit_state = self._classify_post_submit_state(
                            "".join(terminal_output[paste_output_index:])
                        )
                        if post_submit_state is _PostSubmitState.EXECUTING:
                            submission_confirmed = True
                            explicit_stuck_deadline = None
                            if logger:
                                logger.info(
                                    "Prompt submission confirmed at explicit-stuck deadline"
                                )
                            continue
                        if post_submit_state is not _PostSubmitState.EXPLICITLY_STUCK:
                            # Silence or unrecognized output is ambiguous. It
                            # neither proves the prompt is stuck nor authorizes
                            # a fail-fast retry of work that may have started.
                            explicit_stuck_deadline = None
                            continue

                        _kill_process_tree(proc)  # type: ignore[arg-type]
                        self._drain_available_output(
                            master_fd,
                            terminal_output,
                            on_output,
                            logger,
                            decoder,
                        )
                        self._finish_output_decoder(
                            decoder,
                            terminal_output,
                            on_output,
                            logger,
                        )
                        combined = "".join(terminal_output)
                        if _is_interactive_usage_exhausted(combined, full_prompt):
                            return RunnerResult(
                                success=False,
                                summary="Claude usage limit reached",
                                usage_exhausted=True,
                            )
                        return RunnerResult(
                            success=False,
                            summary=(
                                "Timed out confirming interactive Claude accepted the "
                                "prompt submission"
                            ),
                            transient=True,
                        )

                    if proc.poll() is not None:
                        self._drain_available_output(
                            master_fd,
                            terminal_output,
                            on_output,
                            logger,
                            decoder,
                        )
                        self._finish_output_decoder(
                            decoder,
                            terminal_output,
                            on_output,
                            logger,
                        )
                        summary = self._read_result(result_path)
                        if summary is not None:
                            return self._result_from_summary(summary)
                        break

                    readable, _, _ = select.select([master_fd], [], [], 0.25)
                    if not readable:
                        # The terminal has gone quiet, so any dialog that was
                        # mid-render has finished painting and been classified
                        # above.  Only now is a composer match trustworthy.
                        quiet_ticks += 1
                        if (
                            not prompt_pasted
                            and quiet_ticks >= _COMPOSER_SETTLE_TICKS
                            and self._looks_like_main_input_prompt(
                                "".join(terminal_output[setup_output_index:])
                            )
                        ):
                            remaining = max(0.0, attempt_deadline - time.monotonic())
                            self._submit_full_prompt(
                                master_fd,
                                full_prompt,
                                logger,
                                abort,
                                remaining,
                            )
                            prompt_pasted = True
                            paste_output_index = len(terminal_output)
                            submission_attempts = 1
                            last_submission_at = time.monotonic()
                            post_submit_state = _PostSubmitState.AMBIGUOUS
                            explicit_stuck_deadline = None
                        elif (
                            prompt_pasted
                            and not submission_confirmed
                            and quiet_ticks >= _COMPOSER_SETTLE_TICKS
                        ):
                            since_paste = "".join(terminal_output[paste_output_index:])
                            post_submit_state = self._classify_post_submit_state(since_paste)
                            if post_submit_state is _PostSubmitState.EXECUTING:
                                submission_confirmed = True
                                explicit_stuck_deadline = None
                                if logger:
                                    logger.info(
                                        "Claude activity or cleared composer confirmed submission"
                                    )
                            elif post_submit_state is _PostSubmitState.EXPLICITLY_STUCK:
                                if explicit_stuck_deadline is None:
                                    explicit_stuck_deadline = min(
                                        time.monotonic() + _PRE_EXECUTION_CONFIRM_DEADLINE_SECONDS,
                                        attempt_deadline,
                                    )
                                if (
                                    submission_attempts < _SUBMISSION_ATTEMPT_LIMIT
                                    and time.monotonic() < explicit_stuck_deadline
                                    and time.monotonic() - last_submission_at
                                    >= _SUBMISSION_RETRY_SETTLE_SECONDS
                                ):
                                    self._send_submit_keystroke(
                                        master_fd,
                                        logger,
                                        abort_event=abort,
                                        timeout=max(0.0, attempt_deadline - time.monotonic()),
                                    )
                                    submission_attempts += 1
                                    last_submission_at = time.monotonic()
                                    # Evidence predating this input cannot prove
                                    # the new submission attempt also failed.
                                    # Require a newly rendered placeholder before
                                    # sending another Enter.
                                    paste_output_index = len(terminal_output)
                                    post_submit_state = _PostSubmitState.AMBIGUOUS
                                    explicit_stuck_deadline = None
                                    if logger:
                                        logger.info(
                                            "Composer explicitly retains pasted placeholder; "
                                            "retrying submission keystroke "
                                            "(total attempt %d/%d)",
                                            submission_attempts,
                                            _SUBMISSION_ATTEMPT_LIMIT,
                                        )
                            else:
                                # A short fail-fast window belongs to one
                                # continuous explicit-stuck epoch. Pause it as
                                # soon as newer evidence becomes ambiguous; a
                                # later fresh placeholder starts a fresh window.
                                explicit_stuck_deadline = None
                        continue
                    quiet_ticks = 0
                    if not self._read_available(
                        master_fd,
                        terminal_output,
                        on_output,
                        logger,
                        decoder,
                    ):
                        break
                    if not prompt_pasted:
                        if _is_interactive_usage_exhausted(
                            "".join(terminal_output),
                            full_prompt,
                        ):
                            _kill_process_tree(proc)  # type: ignore[arg-type]
                            return RunnerResult(
                                success=False,
                                summary="Claude usage limit reached",
                                usage_exhausted=True,
                            )
                    elif not submission_confirmed and self._looks_like_activity_state(
                        "".join(terminal_output[paste_output_index:])
                    ):
                        # Activity can appear while the terminal is still
                        # actively repainting (a spinner ticking), which never
                        # satisfies the quiet-tick settle check above, so
                        # activity is also checked here on every new read.
                        submission_confirmed = True
                        post_submit_state = _PostSubmitState.EXECUTING
                        explicit_stuck_deadline = None
                        if logger:
                            logger.info("Detected Claude activity; prompt submission confirmed")

                    # Setup dialogs are answered whenever they appear, not only
                    # before the task prompt goes out: Claude can raise one (the
                    # bypass-permissions warning in particular) after the prompt
                    # was submitted, and an unanswered dialog blocks the session
                    # until the wall clock kills it.  Each confirmation latches,
                    # so every dialog is answered at most once.
                    confirmed_setup = False
                    previous = workspace_trust_confirmed
                    workspace_trust_confirmed = self._confirm_workspace_trust_if_needed(
                        master_fd,
                        terminal_output,
                        workspace_trust_confirmed,
                        logger,
                    )
                    if workspace_trust_confirmed and not previous:
                        confirmed_setup = True
                    previous = bypass_permissions_confirmed
                    bypass_permissions_confirmed = self._confirm_bypass_permissions_if_needed(
                        master_fd,
                        terminal_output,
                        bypass_permissions_confirmed,
                        logger,
                    )
                    if bypass_permissions_confirmed and not previous:
                        confirmed_setup = True
                    previous = mcp_server_confirmed
                    mcp_server_confirmed = self._confirm_mcp_server_if_needed(
                        master_fd,
                        terminal_output,
                        mcp_server_confirmed,
                        logger,
                    )
                    if mcp_server_confirmed and not previous:
                        confirmed_setup = True
                    if (
                        not confirmed_setup
                        and not prompt_pasted
                        and time.monotonic() - last_setup_at >= _COMPOSER_SETTLE_FALLBACK_SECONDS
                        and self._looks_like_main_input_prompt(
                            "".join(terminal_output[setup_output_index:])
                        )
                    ):
                        remaining = max(0.0, attempt_deadline - time.monotonic())
                        self._submit_full_prompt(
                            master_fd,
                            full_prompt,
                            logger,
                            abort,
                            remaining,
                        )
                        prompt_pasted = True
                        paste_output_index = len(terminal_output)
                        submission_attempts = 1
                        last_submission_at = time.monotonic()
                        post_submit_state = _PostSubmitState.AMBIGUOUS
                        explicit_stuck_deadline = None
                    if confirmed_setup:
                        # Do not let a setup menu's rendering count as the
                        # later main prompt. Wait for new output after the
                        # confirmation response.
                        setup_output_index = len(terminal_output)
                        quiet_ticks = 0
                        last_setup_at = time.monotonic()
                        if prompt_pasted and not submission_confirmed and prompt_resends < 1:
                            # A dialog we had to answer was on screen after the
                            # prompt went out, so the paste landed in that menu
                            # and its trailing carriage return picked the menu
                            # default -- Claude never received the task. Re-arm
                            # so the prompt is resent once the composer settles.
                            # This can only happen before any activity signal:
                            # once submission is confirmed, dialogs are still
                            # answered above but never trigger a repaste.
                            prompt_pasted = False
                            submission_attempts = 0
                            post_submit_state = _PostSubmitState.AMBIGUOUS
                            explicit_stuck_deadline = None
                            prompt_resends += 1
                            if logger:
                                logger.info("Setup dialog consumed the task prompt; will resend it")

                self._finish_output_decoder(
                    decoder,
                    terminal_output,
                    on_output,
                    logger,
                )
                combined = "".join(terminal_output)
                if _is_interactive_usage_exhausted(combined, full_prompt):
                    return RunnerResult(
                        success=False,
                        summary="Claude usage limit reached",
                        usage_exhausted=True,
                    )
                if attempt < self.max_retries:
                    abort.wait(timeout=self.retry_backoff)
                    if abort.is_set():
                        return RunnerResult(
                            success=False,
                            summary="Aborted: lock lost",
                            transient=True,
                        )
                    continue
                return RunnerResult(
                    success=False,
                    summary=f"Interactive Claude exited before writing {result_path}",
                    transient=True,
                )
            except TimeoutError as exc:
                if proc is not None:
                    _kill_process_tree(proc)  # type: ignore[arg-type]
                if attempt < self.max_retries:
                    abort.wait(timeout=self.retry_backoff)
                    if abort.is_set():
                        return RunnerResult(
                            success=False,
                            summary="Aborted: lock lost",
                            transient=True,
                        )
                    continue
                return RunnerResult(
                    success=False,
                    summary=f"Failed to write prompt: {exc}",
                    transient=True,
                )
            except (OSError, ValueError) as exc:
                if proc is not None:
                    _kill_process_tree(proc)  # type: ignore[arg-type]
                if attempt < self.max_retries:
                    abort.wait(timeout=self.retry_backoff)
                    if abort.is_set():
                        return RunnerResult(
                            success=False,
                            summary="Aborted: lock lost",
                            transient=True,
                        )
                    continue
                return RunnerResult(success=False, summary=f"Failed to start: {exc}")
            finally:
                for fd in (master_fd, slave_fd):
                    if fd >= 0:
                        try:
                            os.close(fd)
                        except OSError:
                            pass

        return RunnerResult(
            success=False,
            summary="Interactive Claude exhausted retries without a result",
            transient=True,
        )
