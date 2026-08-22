# Packaging

The skill is this directory (`spec/`), not a global install. Copy the folder into each repo that should use it.

## Install

Copy `spec/` so the harness you use can see `SKILL.md`:

| Harness | Path |
|---------|------|
| Codex / portable | `<repo>/.agents/skills/spec/` |
| Claude Code | `<repo>/.claude/skills/spec/` |
| Grok | `<repo>/.grok/skills/spec/` |

Same files in all three. Prefer one canonical copy plus symlinks if a repo uses multiple harnesses:

```bash
mkdir -p .agents/skills
cp -R spec .agents/skills/spec
# optional
mkdir -p .claude/skills .grok/skills
ln -s ../../.agents/skills/spec .claude/skills/spec
ln -s ../../.agents/skills/spec .grok/skills/spec
```

Do not add harness-only frontmatter (`allowed-tools`, Claude `argument-hint`, etc.). Portable fields are `name` and `description` only.

## Symlinks and Windows checkouts

The per-harness directories are symlinks, not copies:

```
.claude/skills/spec
.codex/skills/spec
.cursor/skills/spec
.gemini/skills/spec
.grok/skills/spec
.opencode/skills/spec
```

each point at `../../.agents/skills/spec`. That only survives a checkout where
Git can create symlinks. On Windows, creating one requires Developer Mode or an
elevated shell, so `core.symlinks` is commonly off — and Git then materializes
each entry as a **plain text file containing the relative path** instead of a
link. The harness finds no `SKILL.md` under that path and simply never offers
`/spec`, with no error saying why.

Check for it: if `.claude/skills/spec` is a small regular file whose contents
are `../../.agents/skills/spec`, this is what happened.

Fix it, then re-materialize the working tree:

```bash
git config --global core.symlinks true   # plus Developer Mode, or an elevated shell
rm .claude/skills/spec .codex/skills/spec .cursor/skills/spec \
   .gemini/skills/spec .grok/skills/spec .opencode/skills/spec
git checkout -- .
```

The `rm` matters: with the placeholder files present, Git sees no difference
between index and working tree (the blob *is* that path text), so `git checkout`
alone is a no-op. Deleting them first makes Git restore each one — as a real
symlink this time. A fresh clone after setting `core.symlinks` works too.

Failing all that, copy `.agents/skills/spec/` into whichever provider directory
your harness reads — but keep editing the canonical copy under `.agents/skills/`.

## What travels with it

- `SKILL.md` — discipline and gates
- `references/issue-graph.md` — templates and `gh` commands
- `references/packaging.md` — this file
- `agents/openai.yaml` — optional Codex display; safe to keep or drop

Repo-specific conventions (spec directory, extra labels) are **detected at run time**, not baked into the skill. Do not fork the skill per project unless the fleet contract itself differs.

## Distributed systems

A system that spans repos gets this folder in **each** repo you plan in. Run `/spec` in the repo that owns that slice. Cross-repo blockers use native `blocked-by` (URLs / `owner/repo#N`), not body `#N`.
