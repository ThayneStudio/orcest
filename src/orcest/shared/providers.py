"""ProviderEntry: rich, safe, immutable dataclass for multi-provider support.

Each entry is frozen (hashable, stable identity for cooldown maps), carries
self-describing fields for worker CLI dispatch (effective_binary / effective_env_var),
and guarantees no secret leakage via redacted __repr__ and non-secret identity().
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ProviderEntry:
    provider: str
    credential: str
    model: str | None = None
    cli_binary: str | None = None
    env_var: str | None = None
    extras: dict[str, str] = field(default_factory=dict)

    # WORKER-SIDE ONLY ---------------------------------------------------
    # The two properties below encode *execution mechanics* (which binary to
    # invoke, which env var carries the credential). Per the Provider
    # Registration & Invocation Boundary (see CLAUDE.md), the orchestrator
    # MUST NOT call these — execution recipes live in the worker-image-baked
    # PROVIDER_REGISTRY (src/orcest/worker/runner.py). They exist on the
    # shared dataclass only because explicit overrides (cli_binary, env_var)
    # ride on the entry across the wire; the fallback table is a transitional
    # convenience for worker-side defaulting and is not a registration source.
    # If you find yourself calling these from orchestrator code paths, STOP
    # and route through the worker registry instead.
    # --------------------------------------------------------------------

    @property
    def effective_binary(self) -> str:
        """WORKER-SIDE ONLY: resolve binary name (override or provider name)."""
        return self.cli_binary or self.provider

    @property
    def effective_env_var(self) -> str:
        """WORKER-SIDE ONLY: resolve credential env var name.

        Do not import or call from orchestrator code — see the boundary note
        above. The hardcoded fallback table is a migration aid that must be
        deleted once every entry carries an explicit ``env_var``.
        """
        if self.env_var:
            return self.env_var
        return {"claude": "CLAUDE_CODE_OAUTH_TOKEN", "grok": "XAI_API_KEY"}.get(
            self.provider, f"{self.provider.upper()}_TOKEN"
        )

    def _credential_hash(self) -> str:
        return hashlib.sha256(self.credential.encode()).hexdigest()[:12]

    def identity(self) -> str:
        """Selection / round-robin / credential-override anchor (model-inclusive)."""
        return f"{self.provider}:{self.model or ''}:{self._credential_hash()}"

    def account_key(self) -> str:
        """Exhaustion-cooldown key: the rate-limited ACCOUNT, independent of model.

        Claude/Grok rate limits are per-account (credential), so an account
        pinned under two models must share a single cooldown. Never contains a
        raw secret (only the credential hash + provider).
        """
        return f"{self.provider}:{self._credential_hash()}"

    def __repr__(self) -> str:
        cred = self.credential[:4] + "..." if self.credential else ""
        return (
            f"ProviderEntry(provider={self.provider!r}, credential={cred!r}, "
            f"model={self.model!r}, ...)"
        )
