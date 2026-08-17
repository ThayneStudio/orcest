from fastapi.testclient import TestClient

from orcest.monitor import db
from orcest.monitor.config import MonitorConfig, Reader
from orcest.monitor.query_app import create_query_app


def _client(tmp_path, scopes, ptr_content="proj/2026/08/17\n"):
    # Default ptr_content matches the real on-disk contract written by
    # trace_archiver._write_index_pointer: the trace file's parent directory
    # relative to the archive root, plus a trailing newline -- not the file
    # path itself (see src/orcest/orchestrator/trace_archiver.py:302-310).
    archive = tmp_path / "traces"
    trace = archive / "proj" / "2026" / "08" / "17" / "task1.jsonl"
    trace.parent.mkdir(parents=True)
    trace.write_text("".join(f'{{"line": {i}}}\n' for i in range(10)))
    ptr = archive / "index" / "by-task-id" / "ta" / "task1"
    ptr.parent.mkdir(parents=True)
    ptr.write_text(ptr_content)
    dbp = str(tmp_path / "m.db"); db.open_rw(dbp).close()  # noqa: E702
    cfg = MonitorConfig(
        db_path=dbp, trace_archive_path=str(archive), write_token="w",
        readers=[Reader(name="r", token="tok", scopes=scopes)],
    )
    return TestClient(create_query_app(cfg))


H = {"Authorization": "Bearer tok"}


def test_scope_enforced(tmp_path):
    c = _client(tmp_path, frozenset({"events:read"}))
    assert c.get("/api/v1/tasks/task1/trace", headers=H).status_code == 403


def test_tail_returns_last_lines(tmp_path):
    c = _client(tmp_path, frozenset({"events:read", "traces:read"}))
    r = c.get("/api/v1/tasks/task1/trace?tail=3", headers=H)
    assert r.status_code == 200
    assert r.json()["lines"] == ['{"line": 7}', '{"line": 8}', '{"line": 9}']


def test_bad_task_id_rejected(tmp_path):
    c = _client(tmp_path, frozenset({"events:read", "traces:read"}))
    assert c.get("/api/v1/tasks/..%2F..%2Fetc/trace", headers=H).status_code == 400


def test_unknown_task_404(tmp_path):
    c = _client(tmp_path, frozenset({"events:read", "traces:read"}))
    assert c.get("/api/v1/tasks/nosuchtask/trace", headers=H).status_code == 404


def test_pointer_as_file_path_tolerated(tmp_path):
    # Tolerate a pointer whose content is already the full file path
    # (ends with ".jsonl"), in addition to the real directory-only contract.
    c = _client(
        tmp_path,
        frozenset({"events:read", "traces:read"}),
        ptr_content="proj/2026/08/17/task1.jsonl",
    )
    r = c.get("/api/v1/tasks/task1/trace?tail=3", headers=H)
    assert r.status_code == 200
    assert r.json()["lines"] == ['{"line": 7}', '{"line": 8}', '{"line": 9}']
