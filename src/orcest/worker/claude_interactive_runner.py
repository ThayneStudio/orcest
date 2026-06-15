"""Interactive Claude Code runner.

Runs the installed ``claude`` CLI in its default interactive mode through a
PTY. This deliberately avoids ``-p`` / ``--print`` so pool workers use Claude
Code's interactive billing path while preserving Orcest's Runner contract.
"""

from __future__ import annotations

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
from pathlib import Path

from orcest.worker._runner_base import _build_env, _check_needs_human, _kill_process_tree
from orcest.worker.claude_runner import _is_usage_exhausted
from orcest.worker.runner import RunnerResult, get_provider_recipe

_ANSI_RE = re.compile(
    r"\x1b(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\))"
)
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


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
        text = chunk.decode("utf-8", errors="replace")
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
        return True

    def _drain_startup_output(
        self,
        master_fd: int,
        proc: subprocess.Popen[bytes],
        terminal_output: list[str],
        on_output: Callable[[str], None] | None,
        logger: logging.Logger | None,
        startup_delay: float = 2.0,
    ) -> None:
        deadline = time.monotonic() + startup_delay
        while proc.poll() is None and time.monotonic() < deadline:
            timeout = max(0.0, min(0.1, deadline - time.monotonic()))
            readable, _, _ = select.select([master_fd], [], [], timeout)
            if readable:
                self._read_available(master_fd, terminal_output, on_output, logger)

    def _read_result(self, result_path: Path) -> str | None:
        try:
            text = result_path.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        return text or None

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
        cmd = self.build_argv(binary, effective_model) + [full_prompt]
        abort = abort_event if abort_event is not None else threading.Event()

        for attempt in range(1, self.max_retries + 1):
            proc: subprocess.Popen[bytes] | None = None
            master_fd = -1
            slave_fd = -1
            start = time.monotonic()
            terminal_output: list[str] = []
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
                self._drain_startup_output(
                    master_fd,
                    proc,
                    terminal_output,
                    on_output,
                    logger,
                )
                workspace_trust_confirmed = self._confirm_workspace_trust_if_needed(
                    master_fd,
                    terminal_output,
                    workspace_trust_confirmed,
                    logger,
                )
                bypass_permissions_confirmed = self._confirm_bypass_permissions_if_needed(
                    master_fd,
                    terminal_output,
                    bypass_permissions_confirmed,
                    logger,
                )

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

                    if time.monotonic() - start >= timeout:
                        combined = "".join(terminal_output)
                        _kill_process_tree(proc)  # type: ignore[arg-type]
                        if _is_usage_exhausted(combined):
                            return RunnerResult(
                                success=False,
                                summary="Claude usage limit reached",
                                usage_exhausted=True,
                            )
                        return RunnerResult(
                            success=False,
                            summary=f"Timed out after {timeout}s",
                            transient=True,
                        )

                    if proc.poll() is not None:
                        summary = self._read_result(result_path)
                        if summary is not None:
                            return self._result_from_summary(summary)
                        break

                    readable, _, _ = select.select([master_fd], [], [], 0.25)
                    if not readable:
                        continue
                    if not self._read_available(master_fd, terminal_output, on_output, logger):
                        break
                    workspace_trust_confirmed = self._confirm_workspace_trust_if_needed(
                        master_fd,
                        terminal_output,
                        workspace_trust_confirmed,
                        logger,
                    )
                    bypass_permissions_confirmed = self._confirm_bypass_permissions_if_needed(
                        master_fd,
                        terminal_output,
                        bypass_permissions_confirmed,
                        logger,
                    )

                combined = "".join(terminal_output)
                if _is_usage_exhausted(combined):
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
