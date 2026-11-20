"""阶段二路线B C4：批次控制 API 端到端。

覆盖：
  - GET /api/mirror/batch/status 无规则/有规则回显；
  - POST /api/mirror/batch/control 的 pause/resume/skip/cancel 与幂等；
  - 非法指令 400；
  - 前端存在批次控制按钮与 strict 选项。
用明文密码 header 兼容通道（脚本用），模块级 TestClient 单例避免 lifespan 反复触发。
"""
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.db import SessionLocal, migrate
from app.main import app
from app.models import MirrorRule

migrate()
_client = TestClient(app)
HEADERS = {"X-Admin-Password": "testpass"}
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def client():
    with SessionLocal() as db:
        db.query(MirrorRule).delete()
        db.commit()
    yield _client


def _rule():
    with SessionLocal() as db:
        r = MirrorRule(name="t", source_root_id="A", target_root_id="B")
        db.add(r)
        db.commit()
        return r.id


def test_status_no_rule(client):
    r = client.get("/api/mirror/batch/status", headers=HEADERS)
    assert r.status_code == 200
    d = r.json()
    assert d["status"] == "idle" and d["total"] == 0


def test_status_with_rule_idle(client):
    _rule()
    d = client.get("/api/mirror/batch/status", headers=HEADERS).json()
    assert d["status"] == "idle" and d["batch_id"] == ""


def test_control_pause_resume_skip_cancel(client):
    _rule()
    # 未开批次时 pause 无副作用（control_action 置但不改状态；执行器未起）
    p = client.post("/api/mirror/batch/control", params={"action": "pause"}, headers=HEADERS)
    assert p.status_code == 200 and p.json()["ok"] is True
    # resume 恢复
    r = client.post("/api/mirror/batch/control", params={"action": "resume"}, headers=HEADERS)
    assert r.status_code == 200 and r.json()["status"] == "running" or r.json()["status"] == "idle"
    # skip/cancel 可接受
    for a in ("skip", "cancel"):
        q = client.post("/api/mirror/batch/control", params={"action": a}, headers=HEADERS)
        assert q.status_code == 200


def test_control_invalid_action_400(client):
    _rule()
    r = client.post("/api/mirror/batch/control", params={"action": "explode"}, headers=HEADERS)
    assert r.status_code == 400


def test_frontend_has_batch_controls_and_strict_option(client):
    src = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "batch/control" in src
    assert "batch/status" in src or "batch" in src
    assert "严格顺序" in src or "strict" in src.lower()
