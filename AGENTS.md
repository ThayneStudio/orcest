# Agent instructions

See [`.claude/CLAUDE.md`](.claude/CLAUDE.md) for architecture, issue-dependency rules, and development commands.

## Skills

Project skills live in **`.agents/skills/<name>/SKILL.md`**. Provider directories (`.claude/skills`, `.grok/skills`, `.codex/skills`, `.gemini/skills`, `.opencode/skills`, `.cursor/skills`) symlink to that canonical copy so Claude, Grok, Codex, Gemini, OpenCode, and Cursor load the same files.

- **`/spec`** — Fully specify a system/feature, then file a GitHub issue graph with dependency links for the worker fleet. Do **not** implement in that session.

Edit skills only under `.agents/skills/`.
