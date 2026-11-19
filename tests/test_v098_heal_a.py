"""v0.9.8 止血组 A：模板删除 500 / 跳转按钮整体性 / 指令栏同步。"""
import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from app import bot_listener, main, runtime_config
from app.db import SessionLocal, migrate
from app.models import AppConfig, Bot, MessageTemplate
from app.publisher import _template
from app.security import encrypt_secret

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def session():
    migrate()
    with SessionLocal() as s:
        s.query(MessageTemplate).delete()
        s.query(Bot).delete()
        # v0.9.10：本文件会写 interaction_bot_id 指向临时 Bot；测试结束 Bot 表被清，
        # 若配置残留，后续 effective_token 会指向不存在的 Bot 返回空，污染其它测试
        # （如 notify_user）。这里连同配置键一起清，保证测试间隔离。
        for _k in ("interaction_bot_id", "contribution_bot_token", "bot_commands_cleared_fp"):
            _row = s.get(AppConfig, _k)
            if _row:
                s.delete(_row)
        s.commit()
        try:
            yield s
        finally:
            s.query(MessageTemplate).delete()
            s.query(Bot).delete()
            for _k in ("interaction_bot_id", "contribution_bot_token", "bot_commands_cleared_fp"):
                _row = s.get(AppConfig, _k)
                if _row:
                    s.delete(_row)
            s.commit()


# ---- 1. 默认图文模板删除不再 500，且自动补位 ----
def test_delete_default_template_auto_promotes_other(session):
    dflt = MessageTemplate(name="默认图文模板", content="d", enabled=True, is_default=True)
    other = MessageTemplate(name="自定义", content="o", enabled=True, is_default=False)
    session.add_all([dflt, other])
    session.commit()
    out = main.delete_template(dflt.id, db=session)
    assert out["ok"] is True
    remain = session.scalars(select_alive()).all()
    assert len(remain) == 1
    assert remain[0].is_default is True and remain[0].name == "自定义"
    assert _template(session).name == "自定义"


def test_delete_only_template_rebuilds_default(session):
    only = MessageTemplate(name="默认图文模板", content="x", enabled=True, is_default=True)
    session.add(only)
    session.commit()
    main.delete_template(only.id, db=session)
    remain = session.scalars(select_alive()).all()
    assert len(remain) == 1
    assert remain[0].name == "默认图文模板" and remain[0].is_default is True
    assert "{{title}}" in remain[0].content


def test_delete_last_template_with_multiple_defaults_no_500(session):
    # 历史库多条默认：删掉其中一条不应抛 MultipleResultsFound / 500
    session.add_all([
        MessageTemplate(name="A", content="a", enabled=True, is_default=True),
        MessageTemplate(name="B", content="b", enabled=True, is_default=True),
    ])
    session.commit()
    first = session.scalars(select_alive()).first()
    out = main.delete_template(first.id, db=session)
    assert out["ok"] is True
    remain = session.scalars(select_alive()).all()
    assert len(remain) == 1 and remain[0].is_default is True
    assert _template(session) is not None  # first() 不抛异常


def test_reset_default_template_with_dupes_no_500(session):
    for _ in range(3):
        session.add(MessageTemplate(name="默认图文模板", content="x", enabled=True, is_default=True))
    session.commit()
    out = main.reset_default_template(db=session)
    assert out["ok"] is True
    assert _template(session) is not None


def select_alive():
    from sqlalchemy import select
    return select(MessageTemplate)


# ---- 2. 跳转按钮与卡片整体性 ----
def test_jump_link_button_inside_card():
    assert "推送消息 · 跳转链接" in APP
    # 保存按钮所在 panel-foot 与 three-col 卡片同处一个 panel
    seg = APP.split("推送消息 · 跳转链接", 1)[1].split("推送渲染选项", 1)[0]
    assert '<div class="panel-foot"><el-button type="primary" plain :loading="interactionSaving" @click="saveJumpLinks">保存消息跳转链接</el-button>' in seg
    assert "three-col" in seg


# ---- 3. Bot 指令栏同步 / 清空 ----
def test_sync_bot_command_menu_uses_current_commands():
    calls = {}

    async def fake_api(db, token, method, payload):
        calls.update({"method": method, "payload": payload})
        return {"ok": True}

    src = Path(ROOT / "backend/app/bot_listener.py").read_text(encoding="utf-8")
    with patch.object(bot_listener, "_api", new=fake_api):
        with SessionLocal() as db:
            out = _run(bot_listener.sync_bot_command_menu(db, "t", clear=False))
    assert out["ok"] is True and out["count"] == 6
    assert calls["method"] == "setMyCommands"
    cmds = [c["command"] for c in calls["payload"]["commands"]]
    assert cmds == ["start", "help", "want", "contribute", "search", "donate"]
    assert "start" in src and "setMyCommands" in src


def test_clear_bot_command_menu_sends_empty():
    calls = {}

    async def fake_api(db, token, method, payload):
        calls.update(payload)
        return {"ok": True}

    with patch.object(bot_listener, "_api", new=fake_api):
        with SessionLocal() as db:
            out = _run(bot_listener.sync_bot_command_menu(db, "t", clear=True))
    assert out["count"] == 0 and calls["commands"] == []


def test_bot_commands_menu_endpoint_syncs(session):
    bot = Bot(name="Ya-TG Bot", token=encrypt_secret("fake-token"), enabled=True)
    session.add(bot)
    session.flush()
    from app import runtime_config
    runtime_config.set_many(session, {"interaction_bot_id": str(bot.id)})
    session.commit()

    async def fake_sync(db, token, *, clear=False):
        return {"ok": True, "count": 0 if clear else 6, "description": ""}

    with patch("app.main.bot_listener.sync_bot_command_menu", new=AsyncMock(side_effect=fake_sync)):
        out = _run(main.bot_commands_menu(main.BotCommandsMenuInput(clear=False), db=session))
    assert out["ok"] is True and out["count"] == 6

    with patch("app.main.bot_listener.sync_bot_command_menu", new=AsyncMock(side_effect=fake_sync)):
        out = _run(main.bot_commands_menu(main.BotCommandsMenuInput(clear=True), db=session))
    assert out["cleared"] is True and out["count"] == 0


def test_frontend_has_bot_commands_menu_controls():
    assert "async function syncBotCommands" in APP
    assert "/bot/commands/menu" in APP
    assert "同步指令栏（覆盖旧项目残留）" in APP
    assert "清空指令栏" in APP
