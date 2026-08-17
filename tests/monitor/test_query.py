from fastapi.testclient import TestClient

from orcest.monitor import db
from orcest.monitor.config import MonitorConfig, Reader
from orcest.monitor.query_app import create_query_app
from orcest.shared.events import make_event


def _seed(tmp_path):
    path = str(tmp_path / "m.db")
    conn = db.open_rw(path)
    envs = []
    for attempt, task_id in [(0, "t-a"), (1, "t-b")]:
        for suffix in ["task.enqueued", "task.started", "task.failed"]:
            envs.append(make_event(
                f"net.orcest.{suffix}", source_project="p", task_id=task_id,
                repo="o/r", resource_type="pr", resource_id=9, attempt=attempt,
            ))
    envs.append(make_event(
        "net.orcest.task.started", source_project="p", task_id="t-live",
        repo="o/r", resource_type="pr", resource_id=10, attempt=0,
    ))
    db.insert_events(conn, envs)
    return path


def _client(tmp_path, scopes=frozenset({"events:read"})):
    cfg = MonitorConfig(
        db_path=_seed(tmp_path), trace_archive_path=None, write_token="w",
        readers=[Reader(name="r", token="read-secret", scopes=scopes)],
    )
    return TestClient(create_query_app(cfg))


H = {"Authorization": "Bearer read-secret"}


def test_health_unauthenticated(tmp_path):
    assert _client(tmp_path).get("/api/v1/health").json() == {"ok": True}


def test_auth_required_and_method_gate(tmp_path):
    c = _client(tmp_path)
    assert c.get("/api/v1/events").status_code == 401
    assert c.post("/api/v1/events", headers=H).status_code == 405
    assert c.delete("/api/v1/events", headers=H).status_code == 405


def test_events_filtering(tmp_path):
    c = _client(tmp_path)
    r = c.get("/api/v1/events?type=net.orcest.task.failed", headers=H)
    assert r.status_code == 200
    assert {e["type"] for e in r.json()["events"]} == {"net.orcest.task.failed"}


def test_task_timeline_ascending(tmp_path):
    c = _client(tmp_path)
    evs = c.get("/api/v1/tasks/t-a/timeline", headers=H).json()["events"]
    assert [e["subject"] for e in evs] == ["t-a"] * 3
    assert evs[0]["type"] == "net.orcest.task.enqueued"


def test_work_grouping_by_attempt(tmp_path):
    c = _client(tmp_path)
    r = c.get("/api/v1/work/o/r/pr/9", headers=H).json()
    assert [a["attempt"] for a in r["attempts"]] == [0, 1]
    assert r["attempts"][0]["task_ids"] == ["t-a"]
    assert r["attempts"][1]["last_type"] == "net.orcest.task.failed"


def test_fleet_active_tasks(tmp_path):
    c = _client(tmp_path)
    r = c.get("/api/v1/fleet", headers=H).json()
    assert [t["task_id"] for t in r["active_tasks"]] == ["t-live"]
    assert r["pressure"] is None
