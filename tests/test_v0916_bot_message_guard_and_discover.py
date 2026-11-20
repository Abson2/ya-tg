"""v0.9.16 #277-#279 三项真机反馈回归。

锁死以下红线：
  - #277：非指令/非会话态内容（含 startgroup 授权链接、任意裸链接、裸文字）不得登记投稿，
          统一回「未识别指令」纠正提示；仅 /投稿、Reply 会话态、TMDB 候选补链接三条路径可投稿。
  - #278：getMe 成功后自动回写 interaction_bot_username；前端初次加载同步 bot 下拉；
          copyText 带 execCommand fallback（HTTP 局域网可复制）。
  - #279：发现台账为空时，把白名单/目录路由里已配置的真实目标补进列表；刷新返回诊断字段，
          不再用「新增发现 0」误导用户反复点刷新。不虚构任何扫描数量。
"""
import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from app import bot_listener, runtime_config
from app.db import SessionLocal, migrate
from app.models import (AppConfig, Bot, Channel, DiscoveredChat, MediaRequest,
                        UserContribution)

TABLES = (UserContribution, MediaRequest, DiscoveredChat, Channel, Bot)
CFG_KEYS = ("contribution_enabled", "interaction_bot_id", "interaction_bot_username",
            "push_routes_json", "bot_push_channels_json", "bot_push_groups_json",
            "contribution_bot_token")

CANDIDATES = [{"tmdb_id": 261315, "title": "十万狂花入梦来", "year": "2026",
               "media_type": "tv", "vote_average": 9.0}]


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    bot_listener._flow_sessions.clear()
    bot_listener._pick_sessions.clear()
    with SessionLocal() as db:
        for k in CFG_KEYS:
            row = db.get(AppConfig, k)
            if row:
                db.delete(row)
        for t in TABLES:
            db.query(t).delete()
        db.commit()
    yield
    bot_listener._flow_sessions.clear()
    bot_listener._pick_sessions.clear()
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        db.commit()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as s:
        yield s


def _fake_search(cands):
    async def fake(*a, **kw):
        return list(cands)
    return fake


# ---------------- #277 非指令内容不投稿 ----------------
def test_bare_startgroup_link_not_registered(db):
    """用户误发的 startgroup 授权链接不得被登记为投稿。"""
    before = db.query(UserContribution).count()
    action, reply = asyncio.run(bot_listener.handle_text(
        db, "https://t.me/yyds_embty_bot?startgroup=yg_abc123_XyZ", tg_user_id="u1"))
    assert action == "unrecognized"
    assert "未识别为指令" in reply
    assert db.query(UserContribution).count() == before


def test_bare_share_link_not_registered(db):
    """裸光鸭分享链接在非会话态下也不得自动投稿。"""
    before = db.query(UserContribution).count()
    action, _ = asyncio.run(bot_listener.handle_text(
        db, "https://www.alipan.com/s/abc123", tg_user_id="u2"))
    assert action == "unrecognized"
    assert db.query(UserContribution).count() == before


def test_bare_text_not_registered(db):
    action, reply = asyncio.run(bot_listener.handle_text(db, "随便一句话", tg_user_id="u3"))
    assert action == "unrecognized"
    assert "未识别为指令" in reply


def test_unrecognized_hint_has_menu_guidance():
    h = bot_listener._UNRECOGNIZED_HINT
    assert "/求片" in h and "/投稿" in h and "/搜索" in h and "结束" in h


def test_session_contribute_still_works(db):
    """点「投稿」进入会话态后，输入剧名仍走候选，不被纠正提示拦截。"""
    bot_listener._flow_set("u4", bot_listener.FLOW_CONTRIB)
    with patch.object(bot_listener, "search_candidates", new=_fake_search(CANDIDATES)):
        action, _ = asyncio.run(bot_listener.handle_text(db, "十万狂花入梦来", tg_user_id="u4"))
    assert action == "contribute_pick"


def test_picked_then_link_still_registers(db):
    """TMDB 候选点选后补链接仍是合法投稿路径。"""
    sess = {"flow": bot_listener.FLOW_CONTRIB, "query": "x", "candidates": CANDIDATES,
            "secret": "", "created": time.time(), "picked": {
                "title": "十万狂花入梦来", "tmdb_id": 261315, "media_type": "tv", "year": "2026"}}
    bot_listener._pick_sessions["u5"] = sess
    action, reply = asyncio.run(bot_listener.handle_text(
        db, "https://www.alipan.com/s/xyz789", tg_user_id="u5"))
    assert action == "contribute" and "登记" in reply
    db.flush()
    row = db.query(UserContribution).order_by(UserContribution.id.desc()).first()
    assert row.tmdb_id == 261315


# ---------------- #278 Bot 用户名自动回写 ----------------
def test_invite_link_writes_back_username(db):
    """getMe 成功 → invite-link 端点把 username 写回配置。"""
    from app import main
    b = Bot(name="Ya-TG Bot", token="enc-fake", enabled=True)
    db.add(b)
    db.flush()
    runtime_config.set_many(db, {"interaction_bot_id": str(b.id)})

    async def fake_get_me(d, token):
        return "yyds_embty_bot", ""

    with patch.object(bot_listener, "get_bot_username", new=fake_get_me), \
         patch.object(bot_listener, "auto_clear_stale_commands", new=AsyncMock(return_value=False)):
        out = asyncio.run(main.discover_invite_link(main.DiscoverInviteInput(), db))

    assert out["ok"] is True and out["bot_username"] == "yyds_embty_bot"
    assert "startgroup=y" in out["startgroup_url"]
    assert (runtime_config.get(db, "interaction_bot_username") or "") == "yyds_embty_bot"


def test_get_bot_username_strips_at():
    async def fake_api(d, t, method, payload):
        return {"ok": True, "result": {"username": "@yyds_embty_bot"}}
    with patch.object(bot_listener, "_api", new=fake_api):
        name, err = asyncio.run(bot_listener.get_bot_username(None, "tok"))
    assert name == "yyds_embty_bot" and err == ""


def test_copy_fallback_source_present():
    """前端 copyText 必须带 execCommand fallback 与 clipboard 兼容分支。"""
    import pathlib
    root = pathlib.Path(__file__).resolve().parents[1]
    src = (root / "frontend" / "src" / "App.vue").read_text(encoding="utf-8")
    assert "document.execCommand('copy')" in src
    assert "navigator.clipboard.writeText" in src
    assert "syncDiscBots()" in src


# ---------------- #279 白名单目标补进发现列表 ----------------
def test_whitelist_configured_chats_from_channels(db):
    from app import main
    b = Bot(name="b", token="x", enabled=True)
    db.add(b); db.flush()
    db.add(Channel(bot_id=b.id, name="影视交流群", chat_id="-1001234567890",
                   enabled=True, kind="group"))
    db.add(Channel(bot_id=b.id, name="官方频道", chat_id="-1009999999999",
                   enabled=True, kind="channel"))
    db.commit()
    rows = main._whitelist_configured_chats(db)
    ids = {r[1] for r in rows}
    assert "-1001234567890" in ids and "-1009999999999" in ids


def test_discover_chats_merges_whitelist_when_pool_empty(db):
    """发现台账为空时，白名单里已配置的目标仍出现在列表。"""
    from app import main
    b = Bot(name="b", token="x", enabled=True)
    db.add(b); db.flush()
    runtime_config.set_many(db, {"interaction_bot_id": str(b.id),
                                 "push_routes_json": '[{"dir_id":"","targets":['
                                                     '{"kind":"channel","chat_id":"-1008888888","name":"我的频道"}]}]'})
    out = main.discover_chats(db)
    chats = out["chats"]
    assert any(c["chat_id"] == "-1008888888" for c in chats)
    merged = [c for c in chats if c["chat_id"] == "-1008888888"][0]
    assert merged["source"] == "whitelist" and merged["in_whitelist"] is True


def test_discover_refresh_returns_diagnosis(db):
    """无新发现时不虚构数量，返回诚实诊断。"""
    from app import main
    runtime_config.set_many(db, {"contribution_enabled": "false"})
    out = asyncio.run(main.discover_refresh(main.DiscoverRefreshInput(), db))
    assert out["ok"] is True
    assert out["diagnosis"], "应返回诊断说明，而非静默的'新增发现 0'"
