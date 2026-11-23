"""阶段二路线B C4：批次控制 API 端到端。

覆盖：
  - GET /api/mirror/batch/status 无规则/有规则回显；
  - POST /api/mirror/batch/control 的 pause/resume/skip/cancel 与幂等；
  - 非法指令 400；
  - 前端批次控制按钮常态可用（v2.1.13：严格顺序引擎已移除，按钮不再挂在开关上）。
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
    assert r.status_code == 200
    assert r.json()["status"] in ("running", "idle"), r.json()
    # skip/cancel 可接受
    for a in ("skip", "cancel"):
        q = client.post("/api/mirror/batch/control", params={"action": a}, headers=HEADERS)
        assert q.status_code == 200


def test_control_invalid_action_400(client):
    _rule()
    r = client.post("/api/mirror/batch/control", params={"action": "explode"}, headers=HEADERS)
    assert r.status_code == 400


def test_frontend_has_batch_controls_always_available(client):
    """v2.1.13：批次控制按钮**常态可用**，不再挂在「严格顺序引擎」开关上。

    旧版 5 个按钮全部 `:disabled="!mirrorStrict || ..."`，而 strict 默认关闭
    → 生产上这批按钮一直是灰的。引擎移除后按钮改由 `batchCan(...)` 按真实批次
    状态决定可用性。
    """
    src = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "batch/control" in src
    assert "batch/status" in src
    for action in ["pause", "resume", "skip", "cancel", "reopen"]:
        assert f"batchControl('{action}')" in src, f"批次控制按钮丢失：{action}"
    assert "batchCan(" in src, "按钮需按批次真实状态禁用，而不是统一禁用"
    assert "mirrorStrict" not in src, "严格顺序引擎开关应已移除"
