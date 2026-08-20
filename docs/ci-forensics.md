# CI Forensics Gotchas

This page records CI investigation traps that look like valid evidence but
are not. Use it when reconstructing a GitHub Actions failure, especially
when the PR page and `gh` output disagree.

## `gh run view --log` can return empty output for existing logs

Wrong conclusion prevented: **do not infer that logs expired just because
`gh run view <run_id> --log` prints nothing.** The command can return no
lines, no error, and exit code 0 even when the logs still exist and are
available through the REST API.

This is dangerous because it is inconsistent: one run may print hundreds of
lines while another prints zero lines under the same command shape.

```bash
gh run view 32306180491 --repo ThayneStudio/orcest --log | wc -l
gh run view 32295583120 --repo ThayneStudio/orcest --log | wc -l
```

Use job-level REST logs instead:

```bash
run_id=32295583120
gh api "repos/ThayneStudio/orcest/actions/runs/${run_id}/jobs" --jq '.jobs[].id'

job_id=<job_id>
gh api "repos/ThayneStudio/orcest/actions/jobs/${job_id}/logs"
```

For a re-run, jobs are scoped to the run attempt. Check the current attempt
before trusting a remembered verdict, because a re-run replaces the visible
result on the same run:

```bash
run_id=<run_id>
gh api "repos/ThayneStudio/orcest/actions/runs/${run_id}" --jq .run_attempt

attempt=<attempt>
gh api "repos/ThayneStudio/orcest/actions/runs/${run_id}/attempts/${attempt}/jobs" \
  --jq '.jobs[].id'
```

## A run's `conclusion` can look green while the agent failed

Wrong conclusion prevented: **do not count a run as correct from
`conclusion` alone when the result matters.** Run `32306180491` is labelled
`success` at both run and job level, but the job log contained an agent
result with `is_error:true` and no review was posted:

```text
##[error]Claude result reported subtype success with is_error:true
```

Fetch the job log and inspect the agent result JSON rather than stopping at
the GitHub Actions conclusion:

```bash
run_id=32306180491
gh api "repos/ThayneStudio/orcest/actions/runs/${run_id}/jobs" --jq '.jobs[].id'

job_id=<job_id>
gh api "repos/ThayneStudio/orcest/actions/jobs/${job_id}/logs" \
  | rg 'Claude result|is_error|total_cost_usd|num_turns'
```

Cheap triage: if the logged result has `total_cost_usd` of `0` and
`num_turns` of `1`, the agent did not review anything. Re-run it instead of
hunting for a code problem.

## Zero checks does not mean passing

Wrong conclusion prevented: **do not treat a PR page with no checks as a
passing PR.** "No checks" can mean GitHub never created the workflow run.

Two cases have shown up:

- A conflicting base: GitHub cannot build `refs/pull/N/merge`, so no
  `pull_request` workflow run is created. This is
  [documented GitHub Actions behavior](https://docs.github.com/actions/using-workflows/events-that-trigger-workflows#pull_request):
  workflows do not run on `pull_request` activity while the pull request has
  a merge conflict.
- A dropped event: on 2026-08-19, a `master` push at `0f5d3dd` and a PR head
  at `715e4f1` both returned `total_count: 0`, meaning GitHub had not
  created a run.

Verify by SHA instead of trusting the PR check summary:

```bash
sha=<head_sha>
gh api "repos/ThayneStudio/orcest/actions/runs?head_sha=${sha}" --jq .total_count
```

For the durable dropped-event reproducer:

```bash
gh api "repos/ThayneStudio/orcest/actions/runs?head_sha=715e4f1" --jq .total_count
```

Do not use `0f5d3dd` as the reproducer anymore: it now returns `1` because a
later manual `workflow_dispatch` run carries the same head SHA.

If a required run is missing, dispatch CI manually:

```bash
gh workflow run ci.yml --repo ThayneStudio/orcest --ref master
```

## `gh issue view` can fail while exiting 0

Wrong conclusion prevented: **do not let scripts assume `gh issue view`
succeeded just because `$?` is 0.** The default form requests the deprecated
`repository.issue.projectCards` GraphQL field, GitHub rejects it, and `gh`
still exits 0 while printing the GraphQL error.

```bash
gh issue view 563 --repo ThayneStudio/orcest
echo "$?"
```

Use `--json` to select fields explicitly; that avoids the rejected field and
gives machine-readable output:

```bash
gh issue view 563 --repo ThayneStudio/orcest --json body --jq .body
```

This belongs in the same mental bucket as a green `conclusion` with an
errored agent result: the command surface does not present the failure as a
failure, so read the payload.

## Native issue dependencies use numeric issue IDs

Wrong conclusion prevented: **do not assume a 422 means native dependencies
are unavailable or that the issue number is the identifier.** The
`blocked_by` REST endpoint takes the blocker's numeric `id`, not the issue
number and not the GraphQL node id.

Get the blocker numeric id from the REST issue payload:

```bash
blocker_number=<blocker_issue_number>
gh api "repos/ThayneStudio/orcest/issues/${blocker_number}" --jq '{number, id}'
```

Use that numeric REST `id` as the `blocked_by` id:

```bash
blocked_issue=<dependent_issue_number>
blocker_id=<blocker_numeric_id>

gh api \
  --method POST \
  "repos/ThayneStudio/orcest/issues/${blocked_issue}/dependencies/blocked_by" \
  -f blocked_by="${blocker_id}"
```

Read native blockers back:

```bash
gh api "repos/ThayneStudio/orcest/issues/${blocked_issue}/dependencies/blocked_by"
```

Orcest reads dependencies from two sources: GitHub-native `blocked_by`
relationships and body text matching patterns such as `blocked by #N`. See
[`docs/issue-dependencies.md`](issue-dependencies.md). Declaring both is the
safe move when the dependency is operationally important:

```markdown
Blocked by #<blocker_issue_number>
```
