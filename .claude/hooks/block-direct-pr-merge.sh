#!/usr/bin/env bash
# PreToolUse hook: deny a Claude Code Bash tool call that would merge a PR.
# Fail-closed: a missed block is worse than an inconvenient one.
set -eu

DENY_MERGE='{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"Blocked by policy: PR merges go through orcest (CI green + one approval), never a direct gh merge. If a hand-merge is genuinely required, ask the user to run it."}}'
DENY_PARSE='{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"Blocked by policy: merge-policy hook could not parse the tool payload (python3/jq missing or invalid JSON). Fail-closed: ask the user to run any intended merge."}}'

# Require a JSON object payload. Empty stdin, invalid JSON, or a non-object
# must not look like "no merge command found". Prefer python3 (baked into
# every orcest worker) so a missing jq cannot deny every Bash tool call;
# jq remains a fallback.
payload=$(cat)
c=""
parsed=0
if command -v python3 >/dev/null 2>&1; then
  if c=$(printf '%s' "$payload" | python3 -c '
import json, sys
data = json.load(sys.stdin)
if not isinstance(data, dict):
    raise SystemExit(1)
tool_input = data.get("tool_input")
if tool_input is None:
    sys.stdout.write("")
    raise SystemExit(0)
if not isinstance(tool_input, dict):
    raise SystemExit(1)
command = tool_input.get("command")
if command is None:
    command = ""
if not isinstance(command, str):
    raise SystemExit(1)
sys.stdout.write(command)
' 2>/dev/null); then
    parsed=1
  fi
fi
if [ "$parsed" -eq 0 ] && command -v jq >/dev/null 2>&1; then
  if c=$(printf '%s' "$payload" | jq -e -r '. | if type != "object" then error("not an object") else .tool_input.command // "" end' 2>/dev/null); then
    parsed=1
  fi
fi
if [ "$parsed" -eq 0 ]; then
  printf '%s' "$DENY_PARSE"
  exit 0
fi

# grep matches per line, so a command split across lines (backslash
# continuation, heredoc, pretty-printed gh api, ...) would otherwise slip
# through. Collapse whitespace and leftover continuation backslashes first.
flat=$(printf '%s' "$c" | tr '\n\r\t' '   ')
flat=${flat//\\/ }

# CLI subcommand (optional global flags between gh and pr), REST PUT
# .../pulls/N/merge, and the GraphQL mutation.
if printf '%s' "$flat" | grep -qiE \
  '(^|[^[:alnum:]_.-])gh[[:space:]]+([^|;&]*[[:space:]]+)?pr[[:space:]]+merge|(^|[^[:alnum:]_.-])gh[[:space:]]+([^|;&]*[[:space:]]+)?api[^|;&]*pulls/[0-9]+/merge|mergePullRequest'; then
  printf '%s' "$DENY_MERGE"
fi

exit 0
