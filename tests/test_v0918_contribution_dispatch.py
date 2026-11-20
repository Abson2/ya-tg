"""v0.9.18：投稿链接强校验 + 审核/分发状态分离 + 分发任务（中转群/OpenAPI）+ 重试 + 防重复。

覆盖需求 10 条验收点（纯函数/库层，真机网络通道另标 🟡）：
  1 投稿正常提交（光鸭链接入库）        2 审核拒绝不触发分发
  3 审核通过成功提交                    4 分发失败投稿仍保持 approved
  5 分发失败可重试                      6 重复点通过不重复建任务
  7 重启后未完成任务可续处理（pick_pending）
  8 光鸭分享链接识别（强校验）          9 分发成功≠光鸭处理成功（独立记录）
  10 原有投稿/审核功能不受影响（删除连带清理）
"""
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from app import bot_listener as bl
from app import contribution_dispatch as cd
from app import contribution_links as cl
from app import main as main_mod
from app import runtime_config
from app.db import SessionLocal, migrate
from app.models import AppConfig, ContributionDispatch, UserContribution

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")

GY = "https://www.guangyapan.com/s/7kqj"
CFG_KEYS = ("contribution_dispatch_mode", "contribution_relay_chat_id",
            "contribution_relay_template", "tgto_base_url", "tgto_api_key")

_orig_get = runtime_config.get


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as db:
        for k in CFG_KEYS:
            row = db.get(AppConfig, k)
            if row:
                db.delete(row)
        db.query(ContributionDispatch).delete()
        db.query(UserContribution).delete()
        db.commit()
    yield
    with SessionLocal() as db:
        db.query(ContributionDispatch).delete()
        db.query(UserContribution).delete()
        db.commit()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as s:
        yield s


def _seed(db, *, status="pending", url=GY, title="测试剧"):
    c = UserContribution(title=title, tg_user_id="u1", tg_username="tester",
                         share_url=url, status=status, note="")
    db.add(c); db.commit()
    return c


def _cfg(db, **kv):
    runtime_config.set_many(db, {k: str(v) for k, v in kv.items()})
    db.commit()


# ---------- 强校验（验收 8） ----------
def test_guangya_link_valid():
    assert cl.is_guangya_share_url(GY)
    assert cl.is_guangya_share_url("http://guangyapan.com/s/abcd")
    assert cl.is_guangya_share_url("https://m.guangyapan.com/s/xyz?foo=1")
    assert not cl.is_guangya_share_url("https://www.guangyapan.com/folder/123")
    assert not cl.is_guangya_share_url("https://www.baidu.com/s/abc")


def test_extract_link_rules():
    url, err = cl.extract_guangya_share_url(f"凡人修仙传 {GY}")
    assert url == GY and err == ""
    url, err = cl.extract_guangya_share_url("凡人修仙传")
    assert url == "" and err == ""  # 只给剧名不判错（走候选流程）
    url, err = cl.extract_guangya_share_url("https://t.me/foo?start=yg_x 加群")
    assert url == "" and "非光鸭" in err


def test_contribution_rejects_non_guangya_link(db):
    """验收 8：非光鸭链接不进入投稿后台（不落库）。"""
    action, reply = asyncio.run(bl.handle_text(
        db, "/投稿 盗版资源 https://www.baidu.com/s/abc", tg_user_id="u9", tg_username="bad"))
    assert action == "contribute_bad_url"
    assert "非光鸭" in reply
    assert db.query(UserContribution).count() == 0


def test_candidate_await_rejects_non_guangya_link(db):
    """验收 8：TMDB 候选补链接路径同样强校验。"""
    bl._pick_set("u10", bl.FLOW_CONTRIB, "某剧", [
        {"title": "某剧", "tmdb_id": 123, "media_type": "tv", "year": "2026"}])
    # 模拟已点选候选、进入「等链接」态
    bl._pick_get("u10")["picked"] = {"title": "某剧", "tmdb_id": 123,
                                     "media_type": "tv", "year": "2026"}
    action, reply = asyncio.run(bl.handle_text(
        db, "https://pan.baidu.com/s/x1", tg_user_id="u10", tg_username="bad"))
    assert action == "contribute_bad_url"
    assert db.query(UserContribution).count() == 0
    bl._pick_clear("u10")


# ---------- 审核通过 → 分发（验收 1/3/4/6/9） ----------
def test_approve_keeps_status_on_dispatch_failure(db):
    """验收 4/9：分发失败不改审核状态；失败原因独立落分发任务。"""
    c = _seed(db)
    _cfg(db, contribution_dispatch_mode="relay")  # 未配 chat_id → 必失败
    out = asyncio.run(main_mod.handle_contribution(
        c.id, main_mod.ContributionHandleInput(status="approved"), db))
    assert out["ok"] is True
    assert out["forwarded"] is False
    db.refresh(c)
    assert c.status == "approved"
    row = cd.dispatch_for(db, c.id)
    assert row is not None and row.status == "failed"
    assert "未配置" in row.error or "chat_id" in row.error


def test_approve_idempotent_no_duplicate_dispatch(db):
    """验收 6：重复点通过不重复建任务；已提交不自动重跑。"""
    c = _seed(db)
    _cfg(db, contribution_dispatch_mode="relay", contribution_relay_chat_id="-100123")
    with patch.object(bl, "_api", new=AsyncMock(return_value={"ok": True, "result": {}})), \
         patch.object(bl, "effective_token", return_value="fake:token"):
        asyncio.run(main_mod.handle_contribution(
            c.id, main_mod.ContributionHandleInput(status="approved"), db))
    row1 = cd.dispatch_for(db, c.id)
    assert row1.status == "submitted" and row1.attempts == 1
    # 第二次点通过
    asyncio.run(main_mod.handle_contribution(
        c.id, main_mod.ContributionHandleInput(status="approved"), db))
    all_rows = db.scalars(select(ContributionDispatch).where(
        ContributionDispatch.contribution_id == c.id)).all()
    assert len(all_rows) == 1
    db.refresh(row1)
    assert row1.attempts == 1


def test_relay_success_records_ref_and_no_privacy(db):
    """验收 3/9：中转群发送成功 → 已提交 + 唯一编号；消息不含隐私。"""
    c = _seed(db)
    _cfg(db, contribution_dispatch_mode="relay", contribution_relay_chat_id="-100999")
    captured = {}

    async def fake_api(dbi, token, method, payload):
        captured.update(payload)
        return {"ok": True, "result": {"message_id": 1}}

    with patch.object(bl, "_api", new=fake_api), \
         patch.object(bl, "effective_token", return_value="fake:token"):
        ok, info, row = asyncio.run(cd.run_dispatch_for_contribution(db, c.id))
    assert ok is True
    assert row.status == "submitted"
    assert row.ref.startswith("YATG-C-")
    assert str(c.id) in captured["text"]
    assert GY in captured["text"]
    assert "tester" not in captured["text"] and "u1" not in captured["text"]


def test_reject_does_not_dispatch(db):
    """验收 2：审核拒绝不触发分发。"""
    c = _seed(db)
    _cfg(db, contribution_dispatch_mode="relay", contribution_relay_chat_id="-100123")
    asyncio.run(main_mod.handle_contribution(
        c.id, main_mod.ContributionHandleInput(status="rejected", note="版权"), db))
    assert cd.dispatch_for(db, c.id) is None
    db.refresh(c)
    assert c.status == "rejected"


def test_off_mode_no_dispatch(db):
    """验收：off 模式审核通过不建任务、不影响审核。"""
    c = _seed(db)
    _cfg(db, contribution_dispatch_mode="off")
    out = asyncio.run(main_mod.handle_contribution(
        c.id, main_mod.ContributionHandleInput(status="approved"), db))
    assert out["dispatch_status"] == "off"
    assert cd.dispatch_for(db, c.id) is None
    db.refresh(c)
    assert c.status == "approved"


# ---------- 重试（验收 5） ----------
def test_retry_failed_dispatch(db):
    """验收 5：失败任务可手动重试。"""
    c = _seed(db)
    _cfg(db, contribution_dispatch_mode="relay")  # 无 chat → 失败
    asyncio.run(main_mod.handle_contribution(
        c.id, main_mod.ContributionHandleInput(status="approved"), db))
    row = cd.dispatch_for(db, c.id)
    assert row.status == "failed"
    # 补齐 chat 配置后重试
    with patch.object(bl, "_api", new=AsyncMock(return_value={"ok": True})), \
         patch.object(bl, "effective_token", return_value="fake:token"):
        _cfg(db, contribution_relay_chat_id="-100123")
        ok, info, r2 = asyncio.run(cd.retry_dispatch(db, c.id))
    assert ok is True and r2.status == "submitted" and r2.attempts == 2


# ---------- 重启续处理（验收 7） ----------
def test_pick_pending_for_restart(db):
    """验收 7：未完成任务可被后台 job 扫描续处理。"""
    c = _seed(db)
    row, created = cd.ensure_dispatch(db, c.id)
    assert created is True and row.status == "pending"
    assert any(r.contribution_id == c.id for r in cd.pick_pending(db))
    row.status = "submitted"; db.commit()
    assert not any(r.contribution_id == c.id for r in cd.pick_pending(db))


# ---------- 删除连带清理（验收 10） ----------
def test_delete_cleans_dispatch(db):
    c = _seed(db)
    cd.ensure_dispatch(db, c.id); db.commit()
    main_mod.delete_contribution(c.id, db=db)
    assert db.get(UserContribution, c.id) is None
    assert cd.dispatch_for(db, c.id) is None


# ---------- 配置端点（模式校验） ----------
def test_interaction_config_save_validates_mode(db):
    p = main_mod.InteractionConfigInput(contribution_dispatch_mode="bad-mode")
    with pytest.raises(HTTPException) as e:
        main_mod.interaction_config_save(p, db=db)
    assert e.value.status_code == 400


def test_openapi_payload_uses_share_url_only(db):
    """OpenAPI 通道：share_url + source，不传 candidate_item_id/slug。"""
    c = _seed(db)
    _cfg(db, tgto_base_url="http://tgto.local:8090", tgto_api_key="secret-token")
    captured = {}

    class FakeResp:
        status_code = 200
        text = '{"success":true,"task_id":"t-1"}'
        def json(self):
            return {"success": True, "task_id": "t-1"}

    async def fake_post(self, url, **kw):
        captured["url"] = url
        captured["json"] = kw.get("json")
        return FakeResp()

    with patch("httpx.AsyncClient.post", new=fake_post):
        ok, info, task_id, raw = asyncio.run(cd._send_openapi(db, c, None))
    assert ok is True and task_id == "t-1"
    assert captured["url"].endswith("/api/media/resources/transfer")
    assert captured["json"]["share_url"] == GY
    assert "candidate_item_id" not in captured["json"]
    assert "slug" not in captured["json"]


# ---------- 前端静态 ----------
def test_frontend_dispatch_ui():
    seg = APP.split("<h3>用户投稿</h3>", 1)[1].split("</el-table>", 1)[0]
    assert "分发状态" in seg
    assert "dispatchStatusTag(s.row.dispatch_status)" in seg
    assert "retryContributionDispatch(s.row)" in seg
    assert "showContributionDispatch(s.row)" in seg
    assert "function retryContributionDispatch" in APP
    assert "contribution_dispatch_mode" in APP
    assert "contribution_relay_chat_id" in APP
    assert "USER_BOT_TO_BOT_DISABLED" in APP
