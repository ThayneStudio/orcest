"""Worker package.

Seeds the per-provider runner registry on first import so any caller of
``runner.PROVIDER_REGISTRY`` / ``get_provider_recipe`` / the per-task dispatch
in ``loop.py`` sees the built-in providers. Doing the seeding here (rather
than at the bottom of ``runner.py``) avoids a top-of-module circular import:
``claude_runner`` imports ``ProviderRecipe`` from ``runner``.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from pathlib import Path

from orcest.worker._runner_base import _BaseCliRunner
from orcest.worker.claude_runner import ClaudeRunner
from orcest.worker.runner import PROVIDER_REGISTRY, ProviderRecipe, RunnerResult


class _GrokPlaceholderRunner(_BaseCliRunner):
    """Permanent-FAILED stub for Grok tasks until GrokRunner ships in PR 3.

    Registered as runner_cls so dispatch never falls back to ClaudeRunner
    (which would invoke the grok binary with Claude-specific flags, producing
    confusing transient failures). Returns an immediate, non-transient FAILED
    regardless of whether the grok binary is installed, so operators get the
    documented clean "rebake worker image" message rather than retried noise.
    """

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
    ) -> RunnerResult:
        return RunnerResult(
            success=False,
            summary=(
                "Grok runner not yet implemented on this worker image; "
                "rebake worker image after GrokRunner ships (PR 3)."
            ),
            transient=False,
        )

    def build_argv(self, binary: str, prompt: str, model: str, work_dir: Path) -> list[str]:
        raise NotImplementedError  # never called; run() short-circuits

    def extract_summary(self, stdout: str) -> str:
        raise NotImplementedError

    def extract_agent_text(self, stdout: str) -> str:
        raise NotImplementedError

    def detect_exhaustion(self, stdout: str, stderr: str) -> tuple[bool, int]:
        raise NotImplementedError

    def detect_overload(self, stdout: str, stderr: str) -> bool:
        raise NotImplementedError


PROVIDER_REGISTRY["claude"] = ProviderRecipe(
    binary="claude",
    env_var="CLAUDE_CODE_OAUTH_TOKEN",
    runner_cls=ClaudeRunner,
)

# Grok placeholder: dedicated GrokRunner ships in PR 3 of the multi-provider
# plan. _GrokPlaceholderRunner returns an immediate permanent FAILED so that
# installing the grok binary before PR 3 never produces confusing transient
# failures from ClaudeRunner invoking grok with Claude-specific flags.
# When GrokRunner lands, this entry's runner_cls flips.
PROVIDER_REGISTRY["grok"] = ProviderRecipe(
    binary="grok",
    env_var="XAI_API_KEY",
    runner_cls=_GrokPlaceholderRunner,
)

# Noop: registered so integration / stress tests that publish noop-provider
# tasks pass the early-reject's "known provider" check. The dispatch in
# loop.py uses the worker's pre-instantiated NoopRunner fallback whenever
# task.provider matches config.runner.type ("noop"), so runner_cls=None
# here intentionally; no fresh-from-class instantiation is ever needed.
# Empty binary tells get_unsupported_reason "no baked CLI required".
PROVIDER_REGISTRY["noop"] = ProviderRecipe(
    binary="",
    env_var="",
    runner_cls=None,
)
