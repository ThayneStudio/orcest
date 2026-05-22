"""Worker package.

Seeds the per-provider runner registry on first import so any caller of
``runner.PROVIDER_REGISTRY`` / ``get_provider_recipe`` / the per-task dispatch
in ``loop.py`` sees the built-in providers. Doing the seeding here (rather
than at the bottom of ``runner.py``) avoids a top-of-module circular import:
``claude_runner`` imports ``ProviderRecipe`` from ``runner``.
"""

from __future__ import annotations

from orcest.worker.claude_runner import ClaudeRunner
from orcest.worker.grok_runner import GrokRunner
from orcest.worker.runner import PROVIDER_REGISTRY, ProviderRecipe

PROVIDER_REGISTRY["claude"] = ProviderRecipe(
    binary="claude",
    env_var="CLAUDE_CODE_OAUTH_TOKEN",
    runner_cls=ClaudeRunner,
)

# Grok (xAI Grok Build). Primary auth is the SuperGrok OAuth blob written to
# ~/.grok/auth.json by GrokRunner.prepare_credential (Path B); env_var is the
# API-key fallback path. Supersedes the earlier _GrokPlaceholderRunner stub.
PROVIDER_REGISTRY["grok"] = ProviderRecipe(
    binary="grok",
    env_var="XAI_API_KEY",
    runner_cls=GrokRunner,
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
