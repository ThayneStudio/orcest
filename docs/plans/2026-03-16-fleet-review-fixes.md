# Fleet Review Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix all 6 issues identified during code review of the fleet management refactor.

**Architecture:** Direct fixes to existing modules — remove dead code, consolidate validation, add Docker image pre-check, make worker specs configurable, and add missing test coverage.

**Tech Stack:** Python 3.12+, Click, pytest, pytest-mock

---

### Task 1: Remove orphaned `provision-orchestrator` command

The `provision-orchestrator` command in `src/orcest/cli.py` references deleted `provision/setup-orchestrator.sh`. It's fully replaced by `orcest fleet create-orchestrator`. Remove it.

**Files:**
- Modify: `src/orcest/cli.py:595-770` (remove the entire `provision_orchestrator` function)

**Step 1: Remove the command**

Delete lines 595-770 from `src/orcest/cli.py` — the entire `@main.command("provision-orchestrator")` block including the function body. The `main.add_command(fleet)` on line 592 must remain.

**Step 2: Verify**

Run: `python -c "from orcest.cli import main; print([c.name for c in main.commands.values()])"`
Expected: `provision-orchestrator` should NOT appear in the list.

Run: `pytest tests/ -x -q --timeout=10 -m unit`
Expected: All tests pass.

**Step 3: Commit**

```bash
git add src/orcest/cli.py
git commit -m "Remove orphaned provision-orchestrator command

Replaced by 'orcest fleet create-orchestrator'. The old command
referenced deleted setup-orchestrator.sh and would always fail."
```

---

### Task 2: Clean up `add-runner` command's removed dependency imports

The `add-runner` command imports `ProxmoxClient` from `orcest.fleet.proxmox` which depends on the removed `proxmoxer` package. Since `proxmoxer` was removed from pyproject.toml, this code path always falls back. Simplify by removing the `proxmoxer` code path entirely — just write the cloud-init file.

**Files:**
- Modify: `src/orcest/fleet/cli.py:657-748`

**Step 1: Simplify `add_runner` to always write cloud-init file**

Replace the `add_runner` function body (lines 677-738). Remove the `ProxmoxClient`/`OldProxmoxConfig` import block and the API call branch. Keep only the cloud-init file write path:

```python
def add_runner(
    org_url: str,
    runner_token: str,
    runner_name: str,
    labels: str,
    config: str,
    vm_id: int | None,
) -> None:
    """Create a self-hosted GitHub Actions runner VM via Proxmox."""
    from orcest.fleet.config import load_config
    from orcest.fleet.runner_cloud_init import render_runner_userdata

    console = Console()
    cfg = load_config(config)

    if vm_id is None:
        vm_id = 300

    runner_vm_name = runner_name or f"orcest-runner-{vm_id}"

    console.print(f"\n[bold]Creating runner VM {vm_id}[/bold]")
    console.print(f"  Name: {runner_vm_name}")
    console.print(f"  Labels: {labels}")

    userdata = render_runner_userdata(
        org_url=org_url,
        runner_token=runner_token,
        runner_name=runner_vm_name,
        runner_labels=labels,
    )

    _write_userdata_file(userdata, vm_id, "runner", console)
    console.print(f"\n[bold]Runner VM {vm_id} created.[/bold]")
```

**Step 2: Verify**

Run: `pytest tests/ -x -q --timeout=10 -m unit`
Expected: All tests pass.

**Step 3: Commit**

```bash
git add src/orcest/fleet/cli.py
git commit -m "Remove proxmoxer fallback from add-runner command

proxmoxer was removed from dependencies, so the API path always
fell through to the file-write fallback. Simplify to just write
the cloud-init file directly."
```

---

### Task 3: Consolidate `_validate_project_name` duplication

Both `cli.py` and `orchestrator.py` define their own `_validate_project_name` wrappers. Make `config.py` export a raising validator, and have CLI wrap it.

**Files:**
- Modify: `src/orcest/fleet/config.py:24-26`
- Modify: `src/orcest/fleet/orchestrator.py:17-28` (remove local wrapper, import from config)
- Modify: `src/orcest/fleet/cli.py:20-33` (simplify wrapper)
- Modify: `tests/fleet/test_orchestrator.py:6-8` (update import)
- Modify: `tests/fleet/test_config.py` (add tests for raising validator)

**Step 1: Add raising validator to config.py**

Add after `validate_project_name` in `config.py`:

```python
def require_valid_project_name(name: str) -> None:
    """Raise ValueError if *name* is not a valid project name."""
    if not validate_project_name(name):
        raise ValueError(
            f"Invalid project name {name!r}: must be 1-64 chars, "
            "alphanumeric/dot/hyphen/underscore, starting with alphanumeric."
        )
```

**Step 2: Update orchestrator.py to import from config**

Replace lines 17-28 in `orchestrator.py`:

```python
from orcest.fleet.config import require_valid_project_name as _validate_project_name
```

Remove the local `_validate_project_name` function definition (lines 22-28).

**Step 3: Simplify CLI wrapper**

Replace `_validate_project_name` in `cli.py` (lines 25-33):

```python
def _validate_project_name(name: str) -> None:
    """Validate project name, exit on failure."""
    from orcest.fleet.config import require_valid_project_name
    try:
        require_valid_project_name(name)
    except ValueError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)
```

**Step 4: Update test imports**

In `tests/fleet/test_orchestrator.py`, change the import:
```python
from orcest.fleet.config import require_valid_project_name as _validate_project_name
```

Remove the import of `_validate_project_name` from `orcest.fleet.orchestrator`.

**Step 5: Add test for `require_valid_project_name` in `test_config.py`**

```python
class TestRequireValidProjectName:
    def test_valid_name_does_not_raise(self):
        require_valid_project_name("my-project")  # should not raise

    def test_invalid_name_raises(self):
        with pytest.raises(ValueError, match="Invalid project name"):
            require_valid_project_name("")

    def test_shell_injection_raises(self):
        with pytest.raises(ValueError):
            require_valid_project_name("; rm -rf /")
```

**Step 6: Verify**

Run: `pytest tests/ -x -q --timeout=10 -m unit`
Expected: All tests pass.

**Step 7: Commit**

```bash
git add src/orcest/fleet/config.py src/orcest/fleet/orchestrator.py src/orcest/fleet/cli.py tests/fleet/test_orchestrator.py tests/fleet/test_config.py
git commit -m "Consolidate _validate_project_name into config module

Add require_valid_project_name() that raises ValueError. orchestrator.py
now imports it instead of defining its own copy. CLI wraps it with
sys.exit(1) for user-facing output."
```

---

### Task 4: Add Docker image pre-check in `onboard`

When `onboard` runs on a fresh orchestrator without `create-orchestrator`, the Docker image won't exist. Add a check before `deploy_stack`.

**Files:**
- Modify: `src/orcest/fleet/orchestrator.py` (add `image_exists` function)
- Modify: `src/orcest/fleet/cli.py:337-352` (check image before deploy, build if missing)
- Modify: `tests/fleet/test_orchestrator.py` (add test for `image_exists`)

**Step 1: Add `image_exists` function to orchestrator.py**

Add after `build_image`:

```python
def image_exists(ssh_target: str, image: str = "orcest:latest") -> bool:
    """Check whether a Docker image exists on the orchestrator VM."""
    result = _ssh(ssh_target, f"docker image inspect {image} >/dev/null 2>&1")
    return result.returncode == 0
```

**Step 2: Update `onboard` in cli.py**

Replace the "Ensure Docker image exists" block (lines 337-352):

```python
    # Step 3: Ensure Docker image exists, then deploy stack
    try:
        from orcest.fleet.orchestrator import deploy_stack, image_exists

        if not image_exists(ssh_target):
            from orcest.fleet.orchestrator import build_image

            console.print("  Docker image not found, building...")
            build_image(ssh_target)
            console.print("  Docker build [green]ok[/green]")

        deploy_stack(ssh_target, project_name)
        console.print("  Stack deployed [green]ok[/green]")
    except Exception as exc:
        console.print(f"  Deploy stack [red]failed[/red]: {exc}")
        console.print("  [yellow]Config saved. Re-run onboard to retry stack deployment.[/yellow]")
        sys.exit(1)
```

**Step 3: Add test for `image_exists`**

In `tests/fleet/test_orchestrator.py`:

```python
from orcest.fleet.orchestrator import image_exists

class TestImageExists:
    def test_returns_true_when_image_found(self, mocker):
        mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            return_value=subprocess.CompletedProcess(args=[], returncode=0),
        )
        assert image_exists("user@host") is True

    def test_returns_false_when_image_missing(self, mocker):
        mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            return_value=subprocess.CompletedProcess(args=[], returncode=1),
        )
        assert image_exists("user@host") is False
```

Add `import subprocess` to the test file imports.

**Step 4: Update `test_onboard_creates_project` in test_cli.py**

Add mock for `image_exists`:
```python
mocker.patch("orcest.fleet.orchestrator.image_exists", return_value=True)
```

Do the same for `test_onboard_custom_name`.

**Step 5: Verify**

Run: `pytest tests/ -x -q --timeout=10 -m unit`
Expected: All tests pass.

**Step 6: Commit**

```bash
git add src/orcest/fleet/orchestrator.py src/orcest/fleet/cli.py tests/fleet/test_orchestrator.py tests/fleet/test_cli.py
git commit -m "Add Docker image pre-check in onboard command

Instead of only building on first project, check if the image exists
and build if missing. Prevents silent failures when onboarding on
an orchestrator that wasn't set up via create-orchestrator."
```

---

### Task 5: Make worker VM specs configurable

Add `worker_memory`, `worker_cores`, `worker_disk_size` fields to `ProjectEntry` with defaults matching current hardcoded values.

**Files:**
- Modify: `src/orcest/fleet/config.py:66-72` (add fields to `ProjectEntry`)
- Modify: `src/orcest/fleet/config.py:173-180` (parse new fields in `load_config`)
- Modify: `src/orcest/fleet/config.py:219-226` (serialize new fields in `save_config`)
- Modify: `src/orcest/fleet/provisioner.py:101-108` (use config values instead of hardcoded)
- Modify: `tests/fleet/test_provisioner.py` (add test for custom worker specs)
- Modify: `tests/fleet/test_config.py` (add round-trip test for new fields)

**Step 1: Write failing test for custom worker specs**

In `tests/fleet/test_provisioner.py`, add:

```python
    def test_custom_worker_specs(self):
        cfg = _cfg(
            projects=[
                ProjectEntry(
                    name="heavy",
                    repo="Org/heavy",
                    workers=1,
                    worker_memory=32768,
                    worker_cores=16,
                    worker_disk_size=100,
                ),
            ],
        )
        tfvars = generate_tfvars(cfg)
        w = tfvars["workers"]["heavy-0"]
        assert w["memory"] == 32768
        assert w["cores"] == 16
        assert w["disk_size"] == 100
```

Run: `pytest tests/fleet/test_provisioner.py::TestGenerateTfvars::test_custom_worker_specs -v`
Expected: FAIL (no `worker_memory` field on `ProjectEntry`)

**Step 2: Add fields to `ProjectEntry`**

```python
@dataclass
class ProjectEntry:
    """A project managed by orcest."""

    name: str = ""
    repo: str = ""  # "org/repo" format
    redis_port: int = 6379
    workers: int = 1
    worker_memory: int = 16384  # MB
    worker_cores: int = 8
    worker_disk_size: int = 30  # GB
```

**Step 3: Update `load_config` to parse new fields**

In the projects loop, add:
```python
worker_memory=proj.get("worker_memory", 16384),
worker_cores=proj.get("worker_cores", 8),
worker_disk_size=proj.get("worker_disk_size", 30),
```

**Step 4: Update `save_config` to serialize new fields**

In the projects list comprehension, add:
```python
"worker_memory": p.worker_memory,
"worker_cores": p.worker_cores,
"worker_disk_size": p.worker_disk_size,
```

**Step 5: Update `generate_tfvars` to use config values**

In `provisioner.py`, replace the hardcoded worker dict (lines 101-108):
```python
workers[key] = {
    "vm_id": next_vm_id,
    "project_name": project.name,
    "memory": project.worker_memory,
    "cores": project.worker_cores,
    "disk_size": project.worker_disk_size,
    "cloud_init_content": worker_userdata,
}
```

**Step 6: Add round-trip test in test_config.py**

```python
    def test_round_trip_worker_specs(self, tmp_path):
        path = tmp_path / "config.yaml"
        original = FleetConfig(
            projects=[
                ProjectEntry(
                    name="p",
                    repo="O/p",
                    worker_memory=32768,
                    worker_cores=16,
                    worker_disk_size=100,
                ),
            ],
        )
        save_config(original, path)
        loaded = load_config(path)
        assert loaded.projects[0].worker_memory == 32768
        assert loaded.projects[0].worker_cores == 16
        assert loaded.projects[0].worker_disk_size == 100
```

**Step 7: Verify**

Run: `pytest tests/ -x -q --timeout=10 -m unit`
Expected: All tests pass including new tests.

**Step 8: Commit**

```bash
git add src/orcest/fleet/config.py src/orcest/fleet/provisioner.py tests/fleet/test_provisioner.py tests/fleet/test_config.py
git commit -m "Make worker VM specs configurable per-project

Add worker_memory, worker_cores, worker_disk_size fields to
ProjectEntry with defaults matching previous hardcoded values
(16384 MB, 8 cores, 30 GB)."
```

---

### Task 6: Add tests for `create-orchestrator` and `update` commands

These are the most complex multi-step CLI commands with no test coverage.

**Files:**
- Modify: `tests/fleet/test_cli.py`

**Step 1: Add test for `create-orchestrator` happy path**

```python
def test_create_orchestrator(runner, cfg_path, mocker):
    """fleet create-orchestrator creates VM and deploys Docker stack."""
    cfg = FleetConfig(
        proxmox=ProxmoxConfig(
            api_token_id="root@pam!orcest",
            api_token_secret="secret",
        ),
        orchestrator=OrchestratorConfig(ssh_key="ssh-ed25519 AAAA..."),
    )
    _save(cfg, cfg_path)
    mocker.patch("orcest.fleet.provisioner.generate_tfvars", return_value={})
    mocker.patch("orcest.fleet.provisioner.write_tfvars")
    mocker.patch("orcest.fleet.provisioner.apply")
    mocker.patch("orcest.fleet.provisioner.get_output", return_value="10.20.0.99")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=True)
    mocker.patch("orcest.fleet.orchestrator.upload_source")
    mocker.patch("orcest.fleet.orchestrator.build_image")

    result = runner.invoke(fleet, ["create-orchestrator", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "10.20.0.99" in result.output

    # Verify config was updated with orchestrator host
    with open(cfg_path) as f:
        data = yaml.safe_load(f)
    assert data["orchestrator"]["host"] == "10.20.0.99"
```

**Step 2: Add test for `create-orchestrator` SSH timeout**

```python
def test_create_orchestrator_ssh_timeout(runner, cfg_path, mocker):
    """fleet create-orchestrator saves config and exits if SSH times out."""
    cfg = FleetConfig(
        proxmox=ProxmoxConfig(
            api_token_id="root@pam!orcest",
            api_token_secret="secret",
        ),
    )
    _save(cfg, cfg_path)
    mocker.patch("orcest.fleet.provisioner.generate_tfvars", return_value={})
    mocker.patch("orcest.fleet.provisioner.write_tfvars")
    mocker.patch("orcest.fleet.provisioner.apply")
    mocker.patch("orcest.fleet.provisioner.get_output", return_value="10.20.0.99")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=False)

    result = runner.invoke(fleet, ["create-orchestrator", "--config", cfg_path])
    assert result.exit_code != 0

    # Config should still be saved with the IP
    with open(cfg_path) as f:
        data = yaml.safe_load(f)
    assert data["orchestrator"]["host"] == "10.20.0.99"
```

**Step 3: Add test for `update` happy path**

```python
def test_update_rebuilds_and_restarts(runner, cfg_path, mocker):
    """fleet update uploads source, rebuilds image, restarts stacks, and applies terraform."""
    cfg = FleetConfig(
        orchestrator=OrchestratorConfig(host="10.20.0.23"),
        projects=[
            ProjectEntry(name="alpha", repo="Org/alpha"),
            ProjectEntry(name="beta", repo="Org/beta"),
        ],
    )
    _save(cfg, cfg_path)
    mocker.patch("orcest.fleet.orchestrator.upload_source")
    mocker.patch("orcest.fleet.orchestrator.build_image")
    mock_restart = mocker.patch("orcest.fleet.orchestrator.restart_stack")
    mocker.patch("orcest.fleet.provisioner.generate_tfvars", return_value={})
    mocker.patch("orcest.fleet.provisioner.write_tfvars")
    mocker.patch("orcest.fleet.provisioner.apply")

    result = runner.invoke(fleet, ["update", "--config", cfg_path])
    assert result.exit_code == 0, result.output

    # Should restart both project stacks
    assert mock_restart.call_count == 2
```

**Step 4: Add test for `update` without orchestrator host**

```python
def test_update_requires_orchestrator_host(runner, cfg_path):
    """fleet update fails if orchestrator host is not set."""
    _save(FleetConfig(), cfg_path)
    result = runner.invoke(fleet, ["update", "--config", cfg_path])
    assert result.exit_code != 0
    assert "Orchestrator host not set" in result.output
```

**Step 5: Verify**

Run: `pytest tests/ -x -q --timeout=10 -m unit`
Expected: All tests pass.

**Step 6: Commit**

```bash
git add tests/fleet/test_cli.py
git commit -m "Add tests for create-orchestrator and update commands

Cover happy paths, SSH timeout handling, and missing orchestrator
host validation. These were the most complex commands without
test coverage."
```

---

## Execution Notes

- Tasks 1-3 are independent and can be parallelized.
- Task 4 depends on Task 3 (imports `require_valid_project_name`... actually no, it doesn't. It's independent).
- Task 5 is independent.
- Task 6 depends on Task 4 (needs `image_exists` mock in onboard tests).
- All tasks: run `pytest tests/ -x -q --timeout=10 -m unit` after each to verify.
