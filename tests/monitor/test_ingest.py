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


def test_ingest_skips_poison_envelopes_without_500(tmp_path):
    """Poison envelopes (shape-valid but insert-time-invalid, e.g. a NULL
    subject or a non-numeric resource_id) must be skipped, not crash the
    endpoint -- otherwise the relay would retry the same poison batch
    forever. The rest of the batch is still accepted.
    """
    client = TestClient(create_ingest_app(_cfg(tmp_path)))
    h = {"Authorization": "Bearer write-secret"}

    poison_null_subject = _env()
    poison_null_subject["subject"] = None

    poison_bad_resource_id = _env()
    poison_bad_resource_id["id"] = "different-id-1"
    poison_bad_resource_id["data"]["work"]["resource_id"] = "abc"

    valid = _env()
    valid["id"] = "different-id-2"

    r = client.post(
        "/ingest/v1/events",
        json={"events": [poison_null_subject, poison_bad_resource_id, valid]},
        headers=h,
    )
    assert r.status_code == 200
    assert r.json() == {"accepted": 1, "skipped": 2}


def test_ingest_malformed_json_returns_400(tmp_path):
    client = TestClient(create_ingest_app(_cfg(tmp_path)))
    h = {"Authorization": "Bearer write-secret", "Content-Type": "application/json"}
    r = client.post("/ingest/v1/events", content=b"{not valid json", headers=h)
    assert r.status_code == 400
    assert "detail" in r.json()


def test_ingest_non_dict_body_returns_400(tmp_path):
    client = TestClient(create_ingest_app(_cfg(tmp_path)))
    h = {"Authorization": "Bearer write-secret"}
    r = client.post("/ingest/v1/events", json=[_env()], headers=h)
    assert r.status_code == 400
    assert "detail" in r.json()


def test_ingest_non_list_events_returns_400(tmp_path):
    client = TestClient(create_ingest_app(_cfg(tmp_path)))
    h = {"Authorization": "Bearer write-secret"}
    r = client.post("/ingest/v1/events", json={"events": "not-a-list"}, headers=h)
    assert r.status_code == 400
    assert "detail" in r.json()


def test_ingest_missing_events_key_treated_as_empty_batch(tmp_path):
    """No 'events' key at all is not malformed -- it's an empty batch."""
    client = TestClient(create_ingest_app(_cfg(tmp_path)))
    h = {"Authorization": "Bearer write-secret"}
    r = client.post("/ingest/v1/events", json={}, headers=h)
    assert r.status_code == 200
    assert r.json() == {"accepted": 0, "skipped": 0}
