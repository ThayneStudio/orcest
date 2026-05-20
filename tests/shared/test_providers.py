"""Unit tests for ProviderEntry dataclass (rich, safe, immutable).

Also contains broad redaction invariants test for Task 9 hardening that
exercises the full redaction surface (Task + ProviderEntry + ProviderPool +
dead-letter simulation + logging safety) to ensure no raw secrets ever escape
via str/repr/logs/DL/exception paths.
"""

import logging

from orcest.orchestrator.provider_pool import ProviderPool
from orcest.shared.models import Task, TaskType
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


def test_redaction_invariants_across_task_providerentry_pool_and_deadletter_paths(
    caplog,
):
    """Comprehensive redaction property-style test (Task 9).

    Verifies the systematic redaction layer (REDACTED_FIELDS + to_safe_dict +
    masked identity/repr on ProviderEntry + safe __repr__ on ProviderPool)
    protects every surface: Task, ProviderEntry, ProviderPool ops, simulated
    dead-letter payloads (as written in worker/loop.py and read in cli.py),
    log output, exception strings, and repr/str of mixed objects.

    Uses mixed claude + grok entries to cover the multi-provider case.
    Must never leak raw credentials even under error conditions or exhaustion.
    """
    secret_ct = "sk-ant-redact-test-001122334455"
    secret_g = "xai-redact-grok-9988776655"
    gh_token = "ghp_redactgh_667788"

    # Task with mixed secrets (legacy + new fields)
    t = Task.create(
        task_type=TaskType.FIX_PR,
        repo="acme/redact",
        token=gh_token,
        claude_token=secret_ct,
        provider="claude",
        credential=secret_ct,
        resource_type="pr",
        resource_id=1,
        prompt="redact test",
        branch="fix/redact",
    )
    grok_task = Task.create(
        task_type=TaskType.IMPLEMENT_ISSUE,
        repo="acme/redact",
        token="",  # github token (lean path still accepts)
        provider="grok",
        credential=secret_g,
        model="grok-3",
        resource_type="issue",
        resource_id=99,
        prompt="grok redact",
        branch="",
    )

    # ProviderEntry mixed
    e_claude = ProviderEntry("claude", secret_ct)
    e_grok = ProviderEntry("grok", secret_g, model="grok-3-latest")

    # Pool with mixed
    pool = ProviderPool([e_claude, e_grok])

    # 1. Safe dicts on Task never leak
    for task in (t, grok_task):
        safe = task.to_safe_dict()
        for f in ("token", "claude_token", "credential"):
            assert safe.get(f) == "[REDACTED]", f"Task safe dict failed for {f}"
        s = str(safe)
        assert secret_ct not in s and secret_g not in s and gh_token not in s

    # 2. ProviderEntry identity + repr safe
    for e in (e_claude, e_grok):
        assert secret_ct not in e.identity() and secret_g not in e.identity()
        assert "secret" not in e.identity().lower()
        r = repr(e)
        assert secret_ct not in r and secret_g not in r

    # 3. ProviderPool ops + repr use only identity()
    pool_repr = repr(pool)
    assert secret_ct not in pool_repr and secret_g not in pool_repr
    e = pool.next_entry()
    if e:
        pool.register_task("redact-t1", e)
        pool.mark_exhausted("redact-t1")
    pool_repr2 = repr(pool)
    assert secret_ct not in pool_repr2 and secret_g not in pool_repr2

    # 4. Simulated dead-letter payload (exact shape used by _dead_letter_task and result DL)
    # must be clean (as asserted in cli replay refusal and models test)
    dl_payload = {
        **t.to_safe_dict(),
        **grok_task.to_safe_dict(),
        "dead_letter_reason": "Exceeded max delivery count (3)",
        "tasks_stream": "tasks:grok",
        "original_entry_id": "1234-0",
        "delivery_count": "4",
    }
    dl_str = str(dl_payload)
    assert secret_ct not in dl_str and secret_g not in dl_str and gh_token not in dl_str
    assert dl_payload["credential"] == "[REDACTED]"

    # 5. Logging safety: operations under caplog must not emit raw secrets
    caplog.set_level(logging.DEBUG)
    logger = logging.getLogger("redaction-test")
    logger.info("pool state: %r", pool)
    logger.debug("entry: %r task: %s", e_grok, t)
    log_text = caplog.text
    assert secret_ct not in log_text
    assert secret_g not in log_text
    assert gh_token not in log_text

    # 6. Even if exceptions occur, str(exc) must not be seeded with secrets
    # (our code never does, but defensive check on constructed error)
    try:
        # simulate a failure path that might mention an identity
        ident = e_grok.identity()
        raise RuntimeError(f"simulated publish failure for ident {ident}")
    except RuntimeError as exc:
        err_text = str(exc)
        assert secret_g not in err_text
        assert secret_ct not in err_text

    # 7. CLI dead-letter redaction refusal path (already has dedicated test)
    # here we just assert the marker is present so replay would catch it
    assert "[REDACTED]" in dl_payload["credential"]

    # Final: the lean boundary objects themselves expose credential only to
    # the execution side (worker), never to logs/DL/orch display
    assert e_grok.credential == secret_g  # internal for dispatch, but never serialized raw
