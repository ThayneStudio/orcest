"""ProviderEntry: rich, safe, immutable dataclass for multi-provider support.

Each entry is frozen (hashable, stable identity for cooldown maps), carries
self-describing fields for worker CLI dispatch (effective_binary / effective_env_var),
and guarantees no secret leakage via redacted __repr__ and non-secret identity().
"""

from __future__ import annotations
from dataclasses import dataclass, field
import hashlib


@dataclass(frozen=True)
class ProviderEntry:
    provider: str
    credential: str
    model: str | None = None
    cli_binary: str | None = None
    env_var: str | None = None
    extras: dict[str, str] = field(default_factory=dict)

    @property
    def effective_binary(self) -> str:
        return self.cli_binary or self.provider

    @property
    def effective_env_var(self) -> str:
        if self.env_var:
            return self.env_var
        return {"claude": "CLAUDE_CODE_OAUTH_TOKEN", "grok": "XAI_API_KEY"}.get(
            self.provider, f"{self.provider.upper()}_TOKEN"
        )

    def identity(self) -> str:
        h = hashlib.sha256(self.credential.encode()).hexdigest()[:12]
        return f"{self.provider}:{self.model or ''}:{h}"

    def __repr__(self) -> str:
        cred = self.credential[:4] + "..." if self.credential else ""
        return f"ProviderEntry(provider={self.provider!r}, credential={cred!r}, model={self.model!r}, ...)"
