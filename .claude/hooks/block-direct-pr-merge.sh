#!/usr/bin/env bash
# PreToolUse hook: deny a Claude Code Bash tool call that would merge a PR.
# Fail-closed: a missed block is worse than an inconvenient one.
set -eu

DENY_MERGE='{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"Blocked by policy: PR merges go through orcest (CI green + one approval), never a direct gh merge. If a hand-merge is genuinely required, ask the user to run it."}}'
DENY_PARSE='{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"Blocked by policy: merge-policy hook could not parse the tool payload (jq missing or invalid JSON). Fail-closed: ask the user to run any intended merge."}}'

# Require a JSON object payload. Empty stdin, invalid JSON, or a non-object
# must not look like "no merge command found".
if ! c=$(jq -e -r '. | if type != "object" then error("not an object") else .tool_input.command // "" end' 2>/dev/null); then
  printf '%s' "$DENY_PARSE"
  exit 0
fi

# grep matches per line, so a command split across lines (backslash
# continuation, heredoc, pretty-printed gh api, ...) would otherwise slip
# through. Collapse whitespace and leftover continuation backslashes first.
flat=$(printf '%s' "$c" | tr '\n\r\t' '   ')
flat=${flat//\\/ }

# CLI subcommand, REST PUT .../pulls/N/merge, and the GraphQL mutation.
if printf '%s' "$flat" | grep -qiE \
  '(^|[^[:alnum:]_.-])gh[[:space:]]+pr[[:space:]]+merge|(^|[^[:alnum:]_.-])gh[[:space:]]+api[^|;&]*pulls/[0-9]+/merge|mergePullRequest'; then
  printf '%s' "$DENY_MERGE"
fi

exit 0
