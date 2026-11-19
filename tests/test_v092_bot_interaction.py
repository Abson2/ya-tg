"""v0.9.2 回归：Bot 真交互菜单 + 使用限制/推送白名单二选一 + 审核回复。

锁死以下红线：
  - gate_mode 三态：whitelist 只过滤白名单、membership 只校验关注/加群、off 全放行。
  - 二选一——membership 模式下推送不按白名单拦截；whitelist 模式下用户用指令不受限制。
  - inline 主菜单为两列按钮，callback_data 均 < 64 字节。
  - callback_query 被 poll_once 接收并路由；/start 附带主菜单键盘。
  - 审核投稿后主动回复用户一次，失败不影响审核主链路。
"""
import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from app import bot_listener
from app.db import SessionLocal, json_dumps, migrate
from app.models import AppConfig, UserContribution

TABLES = [UserContribution]


def _set(db, key, value):
    row = db.get(AppConfig, key)
    if row is None:
        db.add(AppConfig(key=key, value_enc=__import__("app.security", fromlist=["encrypt_secret"]).encrypt_secret(str(value))))
    else:
        from app.security import encrypt_secret
        row.value_enc = encrypt_secret(str(value))
    db.flush()


def _del(db, key):
    row = db.get(AppConfig, key)
    if row is not None:
        db.delete(row)
        db.flush()


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as db:
        from app.security import encrypt_secret
        # 清理互动相关配置，回到默认
        for k in ("bot_gate_mode", "bot_restrict_enabled", "bot_push_channels_json",
                  "bot_push_groups_json", "bot_required_channels_json",
                  "bot_required_groups_json", "contribution_bot_token",
                  "contribution_enabled", "donation_enabled", "donation_text"):
            row = db.get(AppConfig, k)
            if row:
                db.delete(row)
        for t in TABLES:
            db.query(t).delete()
        db.commit()
    bot_listener._restrict_cache.clear()
    yield
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        db.commit()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as s:
        try:
            yield s
        finally:
            s.rollback()


# ---------- 1. 二选一 gate_mode ----------
def test_gate_mode_default_is_whitelist(db):
    assert bot_listener.gate_mode(db) == "whitelist"


def test_gate_mode_legacy_restrict_true_maps_membership(db):
    _set(db, "bot_restrict_enabled", "true")
    assert bot_listener.gate_mode(db) == "membership"


def test_membership_mode_does_not_filter_push(db):
    _set(db, "bot_gate_mode", "membership")
    _set(db, "bot_push_channels_json", json_dumps(["-1001"]))
    allowed, _ = bot_listener.is_push_allowed(db, "-1002")
    assert allowed is True  # 白名单模式不生效


def test_whitelist_mode_filters_push(db):
    _set(db, "bot_gate_mode", "whitelist")
    _set(db, "bot_push_channels_json", json_dumps(["-1001"]))
    assert bot_listener.is_push_allowed(db, "-1001")[0] is True
    assert bot_listener.is_push_allowed(db, "-1002")[0] is False


def test_off_mode_allows_everything(db):
    _set(db, "bot_gate_mode", "off")
    _set(db, "bot_push_channels_json", json_dumps(["-1001"]))
    assert bot_listener.is_push_allowed(db, "-9999")[0] is True
    assert asyncio.run(bot_listener.check_restriction(db, "tk", "u1")) == (True, "")


def test_whitelist_mode_skips_membership_check(db):
    """二选一核心：whitelist 模式下用户用指令不受关注/加群限制。"""
    _set(db, "bot_gate_mode", "whitelist")
    _set(db, "bot_required_channels_json", json_dumps(["-1001"]))
    ok, reason = asyncio.run(bot_listener.check_restriction(db, "tk", "u1"))
    assert ok is True and reason == ""


# ---------- 2. inline 菜单 ----------
def test_main_keyboard_is_two_columns_and_short_callbacks():
    kb = bot_listener.main_keyboard()
    rows = kb["inline_keyboard"]
    assert all(1 <= len(r) <= 2 for r in rows)  # 两列布局（末行可单列）
    assert any(len(r) == 2 for r in rows)
    for r in rows:
        for b in r:
            assert len(b["callback_data"].encode()) < 64


@pytest.mark.parametrize("data", [
    bot_listener.CB_HOME, bot_listener.CB_CHANNELS, bot_listener.CB_GROUPS,
    bot_listener.CB_ACTIONS, bot_listener.CB_WANT, bot_listener.CB_CONTRIB,
    bot_listener.CB_SEARCH, bot_listener.CB_DONATE,
])
def test_menu_text_has_reply_for_every_callback(db, data):
    text, keyboard = bot_listener._menu_text(db, data)
    assert text and keyboard is not None


def test_menu_channels_shows_whitelist(db):
    _set(db, "bot_push_channels_json", json_dumps(["@official"]))
    text, _ = bot_listener._menu_text(db, bot_listener.CB_CHANNELS)
    assert "@official" in text and "https://t.me/official" in text


# ---------- 3. callback 路由 ----------
def test_handle_callback_answers_and_edits(db):
    calls = []

    async def fake_api(d, t, method, payload):
        calls.append(method)
        return {"ok": True}

    cb = {"id": "cb1", "data": bot_listener.CB_WANT,
          "message": {"message_id": 7, "chat": {"id": "555"}},
          "from": {"id": "555"}}
    with patch.object(bot_listener, "_api", new=fake_api):
        out = asyncio.run(bot_listener._handle_callback(db, "tk", cb))
    assert out == "menu"
    assert "answerCallbackQuery" in calls
    assert "editMessageText" in calls


# ---------- 4. 审核回复 ----------
def test_notify_user_sends_message(db):
    from app import runtime_config
    runtime_config.set_many(db, {"contribution_bot_token": "123:ABC"})
    captured = {}

    async def fake_api(d, t, method, payload):
        captured.update(method=method, payload=payload)
        return {"ok": True}

    with patch.object(bot_listener, "_api", new=fake_api):
        ok = asyncio.run(bot_listener.notify_user(db, "u42", "已通过"))
    assert ok is True
    assert captured["method"] == "sendMessage"
    assert captured["payload"]["chat_id"] == "u42"


def test_notify_user_no_token_returns_false(db):
    assert asyncio.run(bot_listener.notify_user(db, "u42", "hi")) is False
