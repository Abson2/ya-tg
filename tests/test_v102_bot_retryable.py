"""阶段四 A-10：Bot 消息瞬时故障不推进 offset、下轮重投（不丢投稿/求片）。

审计场景：mock handle_text 首次抛 Timeout、二次成功，断言该 update 被处理两次。
业务拒答（非法链接/配置缺失）仍按旧语义记账放行，避免毒消息永久卡 offset。
"""
import asyncio
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from sqlalchemy import select

from app import bot_listener as bl
from app import runtime_config
from app.db import SessionLocal, migrate
from app.models import AppConfig, Bot, ProcessedUpdate, UserContribution
from app.security import encrypt_secret

migrate()


def _run(coro):
    return asyncio.run(coro)


def _set(db, key, value):
    row = db.get(AppConfig, key)
    if row is None:
        db.add(AppConfig(key=key, value_enc=encrypt_secret(str(value))))
    else:
        row.value_enc = encrypt_secret(str(value))
    db.flush()


class FakeResp:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code

    def json(self):
        return self._data


def _poll_with(updates):
    class FakeClient:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url, params=None):
            return FakeResp({"ok": True, "result": updates})

        async def post(self, url, json=None):
            return FakeResp({"ok": True, "result": {}})

    return patch("app.bot_listener.httpx.AsyncClient", FakeClient)


@pytest.fixture
def db():
    bl._poll_retry_counts.clear()
    with SessionLocal() as s:
        for k in ("interaction_bot_id", "contribution_enabled", "contribution_bot_token",
                  "contribution_update_offset", "push_routes_json", "bot_gate_mode"):
            row = s.get(AppConfig, k)
            if row:
                s.delete(row)
        s.query(ProcessedUpdate).delete()
        s.query(UserContribution).delete()
        s.query(Bot).delete()
        s.commit()
        b = Bot(name="交互Bot", token=encrypt_secret("123456789:AA_FAKE_TOKEN_XYZ"), enabled=True)
        s.add(b)
        s.flush()
        _set(s, "interaction_bot_id", b.id)
        _set(s, "contribution_enabled", "true")
        _set(s, "contribution_update_offset", "5")
        s.commit()
        yield s
    bl._poll_retry_counts.clear()


def _upd(uid=6):
    return {"update_id": uid, "message": {
        "message_id": 1, "chat": {"id": -1001, "type": "private"},
        "from": {"id": 9}, "text": "投稿 测试剧"}}


def test_classifier_retryable_vs_business():
    assert bl.is_retryable_poll_error(httpx.ReadTimeout("t")) is True
    assert bl.is_retryable_poll_error(asyncio.TimeoutError()) is True
    assert bl.is_retryable_poll_error(RuntimeError("database is locked")) is True
    err = httpx.HTTPStatusError("x", request=httpx.Request("GET", "http://x"),
                                response=httpx.Response(503))
    assert bl.is_retryable_poll_error(err) is True
    # 业务拒答：非法链接 / 配置缺失不重试
    assert bl.is_retryable_poll_error(ValueError("非光鸭分享链接")) is False
    assert bl.is_retryable_poll_error(RuntimeError("AI 未配置")) is False


def test_retryable_first_then_success_reprocessed(db):
    """首次 Timeout → 不推进 offset、不记账；二次成功 → 同一 update 被处理两次。"""
    calls = {"n": 0}

    async def flaky(*a, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ReadTimeout("TMDB 超时")
        return "ignored", ""

    with _poll_with([_upd(6)]), \
         patch.object(bl, "auto_clear_stale_commands", new=AsyncMock(return_value=False)), \
         patch.object(bl, "check_restriction", new=AsyncMock(return_value=(True, ""))), \
         patch.object(bl, "handle_text", new=AsyncMock(side_effect=flaky)):
        st1 = _run(bl.poll_once(db))
    assert calls["n"] == 1
    assert st1["offset"] == 6, "可重试失败时 offset 不得推进到 7"
    assert db.get(ProcessedUpdate, 6) is None, "失败 update 不得标记已处理"

    with _poll_with([_upd(6)]), \
         patch.object(bl, "auto_clear_stale_commands", new=AsyncMock(return_value=False)), \
         patch.object(bl, "check_restriction", new=AsyncMock(return_value=(True, ""))), \
         patch.object(bl, "handle_text", new=AsyncMock(side_effect=flaky)):
        st2 = _run(bl.poll_once(db))
    assert calls["n"] == 2, "同一 update 被处理两次（首次失败未记账）"
    assert st2["offset"] == 7
    assert db.get(ProcessedUpdate, 6) is not None


def test_business_error_still_marks_processed(db):
    """业务拒答不重试：仍记账 + 推进 offset，避免毒消息卡死。"""
    async def reject(*a, **kw):
        raise ValueError("非光鸭分享链接")

    with _poll_with([_upd(6)]), \
         patch.object(bl, "auto_clear_stale_commands", new=AsyncMock(return_value=False)), \
         patch.object(bl, "check_restriction", new=AsyncMock(return_value=(True, ""))), \
         patch.object(bl, "handle_text", new=AsyncMock(side_effect=reject)):
        st = _run(bl.poll_once(db))
    assert st["offset"] == 7
    assert db.get(ProcessedUpdate, 6) is not None


def test_retry_cap_poison_message_not_blocking(db):
    """连续失败达上限后按毒消息放行，offset 仍能推进（不永久卡队列）。"""
    bot_id = bl.effective_bot_id(db)
    bl._poll_retry_counts[bl._poll_retry_key(bot_id, 6)] = bl._POLL_RETRY_LIMIT

    async def always_fail(*a, **kw):
        raise httpx.ReadTimeout("一直超时")

    with _poll_with([_upd(6)]), \
         patch.object(bl, "auto_clear_stale_commands", new=AsyncMock(return_value=False)), \
         patch.object(bl, "check_restriction", new=AsyncMock(return_value=(True, ""))), \
         patch.object(bl, "handle_text", new=AsyncMock(side_effect=always_fail)):
        st = _run(bl.poll_once(db))
    assert st["offset"] == 7, "重试耗尽后必须放行，避免卡死"
    assert db.get(ProcessedUpdate, 6) is not None
