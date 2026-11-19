"""v0.9.8 止血组 B：求片用户反馈边界 + 后台审核 + 用户账号记录。"""
import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from app import bot_listener, main
from app.db import SessionLocal, migrate
from app.models import MediaRequest, UserContribution
from app.security import encrypt_secret

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def session():
    migrate()
    with SessionLocal() as s:
        s.query(MediaRequest).delete()
        s.query(UserContribution).delete()
        s.commit()
        try:
            yield s
        finally:
            s.query(MediaRequest).delete()
            s.query(UserContribution).delete()
            s.commit()


# ---- 1. 求片用户回复不暴露 TgtoDrive 管理员错误 ----
def test_want_reply_hides_admin_tgto_error(session):
    async def boom(*a, **k):
        raise RuntimeError("尚未配置 TgtoDrive 地址，请先在互动设置里填写")

    with patch("app.bot_listener.tgto.upsert_keyword", new=AsyncMock(side_effect=boom)):
        action, reply = _run(bot_listener.handle_text(
            session, "/求片 伟大的长征", tg_user_id="123", tg_username="alice"))
    assert action == "want"
    assert "TgtoDrive" not in reply and "配置" not in reply and "互动设置" not in reply
    assert "已登记" in reply
    rows = session.query(MediaRequest).all()
    assert len(rows) == 1
    assert rows[0].status == "failed"
    assert "TgtoDrive" in rows[0].note  # 管理员错误只进后台 note
    assert rows[0].tg_user_id == "123" and rows[0].tg_username == "alice"


def test_want_reply_hides_keyword_sync_fail_message(session):
    async def fail(*a, **k):
        return {"ok": False, "message": "TgtoDrive API 不可达"}

    with patch("app.bot_listener.tgto.upsert_keyword", new=AsyncMock(side_effect=fail)):
        action, reply = _run(bot_listener.handle_text(session, "/求片 测试剧", tg_user_id="9"))
    assert "TgtoDrive" not in reply and "不可达" not in reply
    assert rows_ok(session)


def rows_ok(session):
    r = session.query(MediaRequest).first()
    return r is not None and r.review_status == "pending" and r.status == "failed"


def test_contribution_records_user_account(session):
    action, reply = _run(bot_listener.handle_text(
        session, "/投稿 我的片子 https://pan.example/s/abc", tg_user_id="77", tg_username="bob"))
    assert action == "contribute"
    session.flush()  # handle_text 只 add 不 commit
    c = session.query(UserContribution).first()
    assert c.tg_user_id == "77" and c.tg_username == "bob"
    assert c.share_url == "https://pan.example/s/abc"


# ---- 2. 求片后台审核：采纳 / 拒绝（原因反馈）/ 删除 ----
def test_media_requests_list_exposes_user_and_review_state(session):
    r = MediaRequest(title="测试", status="failed", review_status="pending",
                     note="TgtoDrive 未配置", tg_user_id="55", tg_username="carol")
    session.add(r)
    session.commit()
    out = main.media_requests(db=session)
    assert out[0]["tg_user_id"] == "55" and out[0]["tg_username"] == "carol"
    assert out[0]["review_status"] == "pending"
    assert out[0]["sync_note"] == "TgtoDrive 未配置"
    assert out[0]["status"] == "failed"  # 内部同步态仍保留给管理员排查


def test_handle_media_request_adopt_notifies(session):
    r = MediaRequest(title="剧集A", status="failed", review_status="pending", tg_user_id="55")
    session.add(r)
    session.commit()
    with patch("app.main.bot_listener.notify_user", new=AsyncMock(return_value=True)) as m:
        out = _run(main.handle_media_request(
            r.id, main.MediaRequestHandleInput(review_status="adopted"), db=session))
    assert out["ok"] is True and out["notified"] is True
    assert r.review_status == "adopted" and r.handled_at is not None
    assert "采纳" in m.await_args.args[2]


def test_handle_media_request_rejected_reason_sent_to_user(session):
    r = MediaRequest(title="剧集B", status="synced", review_status="pending", tg_user_id="56")
    session.add(r)
    session.commit()
    with patch("app.main.bot_listener.notify_user", new=AsyncMock(return_value=True)) as m:
        out = _run(main.handle_media_request(
            r.id, main.MediaRequestHandleInput(review_status="rejected", note="版权原因"), db=session))
    assert out["ok"] is True
    assert r.review_status == "rejected" and r.review_note == "版权原因"
    assert "版权原因" in m.await_args.args[2]


def test_delete_media_request(session):
    r = MediaRequest(title="待删", status="failed", review_status="pending")
    session.add(r)
    session.commit()
    assert main.delete_media_request(r.id, db=session)["ok"] is True
    assert session.get(MediaRequest, r.id) is None


# ---- 3. 前端契约 ----
def test_frontend_want_admin_controls():
    assert "async function handleMediaRequest" in APP
    assert "async function deleteMediaRequest" in APP
    assert "function wantReviewLabel" in APP
    assert "/media-requests/${row.id}/handle" in APP
    assert ">采纳</el-button>" in APP and ">拒绝</el-button>" in APP and ">删除</el-button>" in APP
    assert "s.row.tg_username || s.row.tg_user_id" in APP
    assert "TgtoDrive 同步失败原因只在后台备注可见" in APP
