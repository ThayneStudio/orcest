# Agent instructions

See [`.claude/CLAUDE.md`](.claude/CLAUDE.md) for architecture, issue-dependency rules, and development commands.

## Skills

Project skills live in **`.agents/skills/<name>/SKILL.md`**. Provider directories (`.claude/skills`, `.grok/skills`, `.codex/skills`, `.gemini/skills`, `.opencode/skills`, `.cursor/skills`) symlink to that canonical copy so Claude, Grok, Codex, Gemini, OpenCode, and Cursor load the same files.

- **`/spec`** — Fully specify a system/feature, then file a GitHub issue graph with dependency links for the worker fleet. Do **not** implement in that session.

Edit skills only under `.agents/skills/`.

**Windows checkouts:** those provider directories are real symlinks, so they need `core.symlinks`. With it off (the common Windows default, since creating a symlink requires Developer Mode or an elevated shell) Git writes each one as a plain text file holding the relative path, the harness finds no `SKILL.md`, and `/spec` silently never appears. Fix and repair recipe: [`.agents/skills/spec/references/packaging.md`](.agents/skills/spec/references/packaging.md#symlinks-and-windows-checkouts).
