"""Tests for orcest.fleet.inventory."""

import pytest
import yaml

from orcest.fleet.inventory import (
    FleetInventory,
    ProjectEntry,
    ProxmoxConfig,
    WorkerEntry,
    load_inventory,
    save_inventory,
)

pytestmark = pytest.mark.unit


SAMPLE_YAML = {
    "orchestrator_host": "orch.example.com",
    "orchestrator_user": "deploy",
    "proxmox": {
        "host": "proxmox.example.com",
        "node": "pve",
        "storage": "local-lvm",
        "token_id": "root@pam!orcest",
        "token_secret": "secret-value",
    },
    "projects": [
        {
            "name": "alpha",
            "repo": "Org/alpha",
            "redis_port": 6379,
            "workers": [{"vm_id": 200}, {"vm_id": 201}],
        },
        {
            "name": "beta",
            "repo": "Org/beta",
            "redis_port": 6380,
            "workers": [{"vm_id": 210}],
        },
    ],
}


def test_load_inventory(tmp_path):
    """load_inventory parses a YAML file into a FleetInventory."""
    path = tmp_path / "fleet.yaml"
    path.write_text(yaml.dump(SAMPLE_YAML))

    inv = load_inventory(path)

    assert inv.orchestrator_host == "orch.example.com"
    assert inv.orchestrator_user == "deploy"
    assert inv.proxmox.host == "proxmox.example.com"
    assert inv.proxmox.token_id == "root@pam!orcest"
    assert len(inv.projects) == 2
    assert inv.projects[0].name == "alpha"
    assert inv.projects[0].redis_port == 6379
    assert len(inv.projects[0].workers) == 2
    assert inv.projects[0].workers[0].vm_id == 200
    assert inv.projects[1].name == "beta"
    assert inv.projects[1].redis_port == 6380


def test_load_inventory_missing_file(tmp_path):
    """load_inventory returns empty FleetInventory for missing file."""
    inv = load_inventory(tmp_path / "nonexistent.yaml")
    assert inv.orchestrator_host == ""
    assert inv.projects == []


def test_load_inventory_empty_file(tmp_path):
    """load_inventory handles an empty YAML file."""
    path = tmp_path / "fleet.yaml"
    path.write_text("")
    inv = load_inventory(path)
    assert inv.projects == []


def test_save_inventory_roundtrip(tmp_path):
    """save_inventory followed by load_inventory preserves data."""
    inv = FleetInventory(
        orchestrator_host="host.example.com",
        orchestrator_user="admin",
        proxmox=ProxmoxConfig(host="px.example.com", token_id="user!tok", token_secret="s3cret"),
        projects=[
            ProjectEntry(
                name="proj1",
                repo="Org/proj1",
                redis_port=6379,
                workers=[WorkerEntry(vm_id=100), WorkerEntry(vm_id=101)],
            ),
        ],
    )

    path = tmp_path / "subdir" / "fleet.yaml"
    save_inventory(inv, path)

    loaded = load_inventory(path)
    assert loaded.orchestrator_host == "host.example.com"
    assert loaded.orchestrator_user == "admin"
    assert loaded.proxmox.host == "px.example.com"
    assert loaded.proxmox.token_secret == "s3cret"
    assert len(loaded.projects) == 1
    assert loaded.projects[0].name == "proj1"
    assert len(loaded.projects[0].workers) == 2


def test_get_project():
    """get_project returns the matching project or None."""
    inv = FleetInventory(
        projects=[
            ProjectEntry(name="a", repo="Org/a"),
            ProjectEntry(name="b", repo="Org/b"),
        ]
    )
    assert inv.get_project("a") is not None
    assert inv.get_project("a").repo == "Org/a"
    assert inv.get_project("missing") is None


def test_next_redis_port():
    """next_redis_port returns the lowest available port in [6379, 6399], reusing gaps."""
    empty = FleetInventory()
    assert empty.next_redis_port() == 6379

    # Gap at 6380 should be reused rather than returning max+1 (6382)
    inv = FleetInventory(
        projects=[
            ProjectEntry(name="a", repo="Org/a", redis_port=6379),
            ProjectEntry(name="b", repo="Org/b", redis_port=6381),
        ]
    )
    assert inv.next_redis_port() == 6380

    # Contiguous block: returns first port after the block
    inv2 = FleetInventory(
        projects=[
            ProjectEntry(name="a", repo="Org/a", redis_port=6379),
            ProjectEntry(name="b", repo="Org/b", redis_port=6380),
        ]
    )
    assert inv2.next_redis_port() == 6381

    # Full range exhausted: returns 6400 (caller enforces the cap)
    all_ports = [
        ProjectEntry(name=f"p{i}", repo=f"Org/p{i}", redis_port=6379 + i)
        for i in range(21)
    ]
    full = FleetInventory(projects=all_ports)
    assert full.next_redis_port() == 6400


def test_next_vm_id():
    """next_vm_id returns max existing + 1, or 200 if empty."""
    empty = FleetInventory()
    assert empty.next_vm_id() == 200

    inv = FleetInventory(
        projects=[
            ProjectEntry(name="a", repo="Org/a", workers=[WorkerEntry(vm_id=200)]),
            ProjectEntry(name="b", repo="Org/b", workers=[WorkerEntry(vm_id=210)]),
        ]
    )
    assert inv.next_vm_id() == 211


def test_all_vm_ids():
    """all_vm_ids returns a set of all VM IDs across all projects."""
    inv = FleetInventory(
        projects=[
            ProjectEntry(
                name="a", repo="Org/a", workers=[WorkerEntry(vm_id=200), WorkerEntry(vm_id=201)]
            ),
            ProjectEntry(name="b", repo="Org/b", workers=[WorkerEntry(vm_id=210)]),
        ]
    )
    assert inv.all_vm_ids() == {200, 201, 210}


def test_all_vm_ids_empty():
    """all_vm_ids returns empty set when no workers exist."""
    inv = FleetInventory(projects=[ProjectEntry(name="a", repo="Org/a")])
    assert inv.all_vm_ids() == set()
