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

# CLI subcommand (optional global flags between gh and pr), REST
# .../pulls/N/merge unless it is a single explicit GET, and the GraphQL
# mutation.
if printf '%s' "$flat" | grep -qiE \
  '(^|[^[:alnum:]_.-])gh[[:space:]]+([^|;&]*[[:space:]]+)?pr[[:space:]]+merge|mergePullRequest'; then
  printf '%s' "$DENY_MERGE"
elif printf '%s' "$flat" | grep -qiE \
  '(^|[^[:alnum:]_.-])gh[[:space:]]+([^|;&]*[[:space:]]+)?api[^|;&]*pulls/[0-9]+/merge'; then
  has_compound=$(printf '%s' "$flat" | grep -qE '(;|&&|\|\||\||`|\$\()' && printf '1' || printf '0')
  rest_allow=""
  if [ "$has_compound" -eq 0 ] && command -v python3 >/dev/null 2>&1; then
    rest_allow=$(COMMAND="$flat" python3 -c '
import os
import re
import shlex

try:
    tokens = shlex.split(os.environ["COMMAND"])
except ValueError:
    raise SystemExit(0)

if len(tokens) < 2 or tokens[0] != "gh" or "api" not in tokens[1:]:
    raise SystemExit(0)

api_index = tokens.index("api", 1)
args = tokens[api_index + 1 :]
has_merge_path = any(re.search(r"(^|/)pulls/[0-9]+/merge$", token) for token in args)
methods = []
for index, token in enumerate(args):
    upper = token.upper()
    if upper == "-X" or upper == "--METHOD":
        if index + 1 < len(args):
            methods.append(args[index + 1].upper())
    elif upper.startswith("-X") and len(token) > 2:
        methods.append(token[2:].upper())
    elif upper.startswith("--METHOD="):
        methods.append(token.split("=", 1)[1].upper())

if has_merge_path and methods and all(method == "GET" for method in methods):
    print("1")
' 2>/dev/null || true)
  fi
  if [ "$rest_allow" != "1" ]; then
    printf '%s' "$DENY_MERGE"
  fi
fi

exit 0
