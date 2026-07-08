#!/usr/bin/env sh
set -eu

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

CHECKED_PATHS="
dashboard
docker-compose.dashboard.yml
Makefile
.github/workflows/ci.yml
tests/test_dashboard.py
"

untracked="$(git ls-files --others --exclude-standard -- $CHECKED_PATHS || true)"
ignored_untracked_copied="$(
  git ls-files --others --ignored --exclude-standard -- dashboard 2>/dev/null |
    awk '
      function copy_excluded(path, rel) {
        rel = path
        sub(/^dashboard\//, "", rel)
        return rel ~ /^(node_modules|dist|build)(\/|$)/ ||
          rel ~ /^\.git(\/|$)/ ||
          rel == ".env" ||
          rel ~ /^\.env\./ ||
          rel ~ /^[^\/]*\.env$/ ||
          rel ~ /^\.npmrc/ ||
          rel ~ /^npm-debug\.log/ ||
          rel ~ /^vite\.config\.ts\.timestamp-.*\.mjs$/
      }
      !copy_excluded($0) { print }
    '
)"
intent_to_add="$(git status --porcelain=v1 -- $CHECKED_PATHS | awk 'substr($0,1,2) == " A" { print substr($0,4) }')"
unstaged="$(git diff --name-only -- $CHECKED_PATHS || true)"
if [ -z "$untracked" ] && [ -z "$ignored_untracked_copied" ] && [ -z "$intent_to_add" ] && [ -z "$unstaged" ]; then
  exit 0
fi

if [ -n "$untracked" ]; then
  cat >&2 <<'EOF'
Dashboard verification has untracked files.

Local dashboard test/deploy targets copy the working tree and deploy compose
files from the repo, so untracked files can make local verification pass while
a clean checkout fails. Add intentional files to git, or add generated/local-only
files to the appropriate ignore list.

Untracked dashboard verification files:
EOF
  printf '%s\n' "$untracked" >&2
fi

if [ -n "$ignored_untracked_copied" ]; then
  cat >&2 <<'EOF'
Dashboard verification has ignored untracked files that are not excluded from
dashboard copy/deploy.

Local dashboard test/deploy targets copy the working tree with explicit tar and
rsync exclude lists, not Git's full ignore rules. Ignored files outside those
explicit excludes can make local verification pass while a clean checkout fails.
Add intentional files to git, or add generated/local-only files to the dashboard
copy/deploy excludes.

Ignored untracked dashboard files that would be copied:
EOF
  printf '%s\n' "$ignored_untracked_copied" >&2
fi

if [ -n "$intent_to_add" ]; then
  cat >&2 <<'EOF'
Dashboard verification has intent-to-add files whose contents are not staged.

Intent-to-add entries are visible in diffs, but their index content is still
empty. Stage the file contents so local verification matches what a clean
checkout or CI job can actually build.

Intent-to-add dashboard verification files:
EOF
  printf '%s\n' "$intent_to_add" >&2
fi

if [ -n "$unstaged" ]; then
  cat >&2 <<'EOF'
Dashboard verification has unstaged tracked file changes.

Local dashboard test/deploy targets copy the working tree and deploy compose
files from the repo, so unstaged edits can make local verification pass while a
clean checkout or commit built from the index fails. Stage intentional edits
before running the dashboard verification/deploy targets.

Unstaged dashboard verification files:
EOF
  printf '%s\n' "$unstaged" >&2
fi

exit 1
