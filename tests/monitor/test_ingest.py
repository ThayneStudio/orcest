from fastapi.testclient import TestClient

from orcest.monitor.config import MonitorConfig
from orcest.monitor.ingest_app import create_ingest_app
from orcest.shared.events import make_event


def _cfg(tmp_path):
    return MonitorConfig(
        db_path=str(tmp_path / "m.db"), trace_archive_path=None,
        write_token="write-secret", readers=[],
    )


def _env():
    return make_event(
        "net.orcest.task.started", source_project="p", task_id="t",
        repo="o/r", resource_type="pr", resource_id=1, attempt=0,
    )


def test_ingest_requires_write_token(tmp_path):
    client = TestClient(create_ingest_app(_cfg(tmp_path)))
    r = client.post("/ingest/v1/events", json={"events": [_env()]})
    assert r.status_code == 401
    r = client.post(
        "/ingest/v1/events", json={"events": [_env()]},
        headers={"Authorization": "Bearer wrong"},
    )
    assert r.status_code == 401


def test_ingest_accepts_and_dedupes(tmp_path):
    client = TestClient(create_ingest_app(_cfg(tmp_path)))
    env = _env()
    h = {"Authorization": "Bearer write-secret"}
    r = client.post("/ingest/v1/events", json={"events": [env]}, headers=h)
    assert r.json() == {"accepted": 1, "skipped": 0}
    r = client.post("/ingest/v1/events", json={"events": [env]}, headers=h)
    assert r.json() == {"accepted": 0, "skipped": 1}
