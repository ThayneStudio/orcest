---
name: review
description: Use when the user wants to review staged or unstaged git changes before committing, or asks to review local modifications
argument-hint: "[number-of-agents]"
allowed-tools: Bash, Read, Grep, Glob, Task
---

# Parallel Code Review of Local Changes

Review all staged and unstaged git changes by dispatching parallel review agents — one per logical file group.

**Requested agent count:** $ARGUMENTS

## Current Changes

**Git status:**
```
!`git status --short 2>/dev/null || echo "Not a git repository"`
```

**Unstaged changes:**
```
!`git diff --stat 2>/dev/null`
```

**Staged changes:**
```
!`git diff --cached --stat 2>/dev/null`
```

## Instructions

Follow these steps exactly:

### 1. Identify changed files

From the git status and diff stats above, collect every file with staged or unstaged modifications. If there are no changes, tell the user and stop.

### 2. Group files for parallel review

If the user provided a number in "Requested agent count" above, use exactly that many agents (split files evenly across that many groups). Otherwise, determine the count automatically:
- If 1-3 files total: one agent per file
- If 4+ files: group by nearest shared directory, targeting 2-6 groups
- Cap at 10 agents unless the user explicitly requested more

Keep closely related files together (e.g. a module and its tests).

### 3. Dispatch parallel review agents

Launch one Task agent per group using `subagent_type: "general-purpose"`. All agents run in parallel in a single tool-call message.

Each agent prompt MUST include:
- The exact file paths to review
- Whether to review staged changes, unstaged changes, or both
- The specific review checklist below

**Agent prompt template:**

```
Review the following git changes for code quality issues.

Files to review: {file_list}

For each file, run these commands to get the diff:
- `git diff -- {file}` for unstaged changes
- `git diff --cached -- {file}` for staged changes

Then read the full current version of each file for context.

Review checklist:
1. **Bugs**: Logic errors, off-by-one, null/None handling, race conditions
2. **Security**: Injection, secrets exposure, unsafe input handling
3. **Error handling**: Missing try/except, swallowed errors, unclear error messages
4. **API contract**: Function signatures match callers, return types consistent
5. **Edge cases**: Empty inputs, boundary values, concurrent access
6. **Naming/clarity**: Misleading names, confusing logic that needs comments

For each issue found, report:
- Severity: critical / important / minor
- File and line number
- What the issue is
- Suggested fix (brief)

If a file looks correct, say so — don't invent issues. Be precise, not verbose.
Return your findings as a structured list.
```

### 4. Present consolidated results

After all agents return, present a single summary:

**Format:**

```
## Review Summary

**Files reviewed:** N files across M groups
**Issues found:** X critical, Y important, Z minor

### Critical Issues
- `file:line` — description (suggested fix)

### Important Issues
- `file:line` — description (suggested fix)

### Minor Issues
- `file:line` — description (suggested fix)

### Files with no issues
- file1, file2, ...
```

Sort issues by severity (critical first), then by file path. Deduplicate if multiple agents flagged the same issue. If no issues found in any category, say "None" instead of omitting the section.
