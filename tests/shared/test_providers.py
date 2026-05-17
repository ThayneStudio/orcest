"""Unit tests for ProviderEntry dataclass (rich, safe, immutable)."""

from orcest.shared.providers import ProviderEntry


def test_provider_entry_rich_fields_and_redaction():
    e = ProviderEntry(
        provider="grok",
        credential="xai-secret-1234567890",
        model="grok-3-latest",
        cli_binary="grok",
        env_var="XAI_API_KEY",
        extras={"temperature": "0.2"}
    )
    assert e.effective_binary == "grok"
    assert e.effective_env_var == "XAI_API_KEY"
    key = e.identity()
    assert "xai-secret" not in key and "1234567890" not in key
    assert "grok" in key
    assert "secret" not in repr(e) and "xai-secret" not in repr(e)
