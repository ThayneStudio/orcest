from fastapi.testclient import TestClient

from orcest.monitor import db
from orcest.monitor.config import MonitorConfig
from orcest.monitor.query_app import create_query_app
from orcest.shared.events import EVENT_TYPES


def test_openapi_type_enum_matches_taxonomy(tmp_path):
    dbp = str(tmp_path / "m.db")
    db.open_rw(dbp).close()
    cfg = MonitorConfig(db_path=dbp, trace_archive_path=None, write_token="w", readers=[])
    spec = TestClient(create_query_app(cfg)).get("/api/v1/openapi.json").json()
    params = spec["paths"]["/api/v1/events"]["get"]["parameters"]
    type_param = next(p for p in params if p["name"] == "type")
    assert set(type_param["schema"]["enum"]) == EVENT_TYPES


def test_openapi_json_reachable_without_auth(tmp_path):
    dbp = str(tmp_path / "m.db")
    db.open_rw(dbp).close()
    cfg = MonitorConfig(db_path=dbp, trace_archive_path=None, write_token="w", readers=[])
    resp = TestClient(create_query_app(cfg)).get("/api/v1/openapi.json")
    assert resp.status_code == 200
