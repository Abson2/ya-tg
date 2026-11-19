"""v0.9.5 回归：目录路由白名单 + 使用限制独立 + 跳转按钮 + 扫码业务凭证。

锁死以下红线：
  - push_routes 读写最多 10 条、频道/群任填其一（空 chat_id 跳过）、kind 归一。
  - match_push_routes：dir_id 空=通配；精确匹配 parent_id；命中目录链。
  - check_restriction 为 OR 语义：多个候选满足任一即通过。
  - restriction_keyboard / _normalize_tme_link：@用户名 / URL 归一成 t.me 链接。
  - _build_message ctx 含 channel_link/group_link 模板变量。
  - 扫码登录（webqr）凭证补 did，可被 GuangYaWebFiles 用作业务凭证（另见 test_v092）。
  - 全部 mock / 本地库，不触网、不外传用户数据。
"""
import asyncio
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from app import bot_listener, runtime_config
from app.db import SessionLocal, migrate
from app.models import AppConfig, AuthToken, Bot, Channel, QueueItem, Resource, Rule
from app.security import encrypt_secret


def _set(db, key, value):
    row = db.get(AppConfig, key)
    if row is None:
        db.add(AppConfig(key=key, value_enc=encrypt_secret(str(value))))
    else:
        row.value_enc = encrypt_secret(str(value))
    db.flush()


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as db:
        db.query(QueueItem).delete()
        db.query(Rule).delete()
        db.query(Channel).delete()
        db.query(Bot).delete()
        db.query(Resource).delete()
        for k in ("push_routes_json", "bot_required_channels_json", "bot_required_groups_json",
                  "bot_restrict_enabled", "bot_gate_mode", "bot_required_channel_link",
                  "bot_required_group_link", "tg_channel_link", "tg_group_link",
                  "interaction_bot_id"):
            row = db.get(AppConfig, k)
            if row:
                db.delete(row)
        db.commit()
    bot_listener._restrict_cache.clear()
    yield
    bot_listener._restrict_cache.clear()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as s:
        try:
            yield s
        finally:
            s.rollback()


# ---------- 1. 目录路由表 ----------
def test_push_routes_normalizes_kind_and_skips_empty(db):
    _set(db, "push_routes_json", json.dumps([
        {"dir_id": "dir-a", "kind": "group", "chat_id": "-1001"},
        {"dir_id": "dir-b", "kind": "whatever", "chat_id": "-1002"},
        {"dir_id": "dir-c", "kind": "channel", "chat_id": "   "},  # 空 chat_id 跳过
    ]))
    routes = bot_listener.push_routes(db)
    assert len(routes) == 2
    assert routes[0]["kind"] == "group" and routes[0]["chat_id"] == "-1001"
    assert routes[1]["kind"] == "channel"  # 非 group 归一为 channel


def test_push_routes_max_10(db):
    _set(db, "push_routes_json", json.dumps(
        [{"dir_id": "", "kind": "channel", "chat_id": str(i)} for i in range(20)]))
    assert len(bot_listener.push_routes(db)) == 10


def test_match_push_routes_wildcard_parent_and_chain(db):
    _set(db, "push_routes_json", json.dumps([
        {"dir_id": "", "kind": "channel", "chat_id": "root"},
        {"dir_id": "parent-1", "kind": "channel", "chat_id": "c1"},
        {"dir_id": "grand-2", "kind": "group", "chat_id": "g2"},
        {"dir_id": "other", "kind": "group", "chat_id": "g3"},
    ]))
    res = SimpleNamespace(parent_id="parent-1", dir_chain_json=json.dumps(["root-x", "grand-2"]))
    matched = bot_listener.match_push_routes(db, res)
    chat_ids = {r["chat_id"] for r in matched}
    assert chat_ids == {"root", "c1", "g2"}, chat_ids  # 通配 + parent + 目录链命中，other 不命中


# ---------- 2. 使用限制 OR 语义 + 跳转 ----------
def test_check_restriction_or_semantics(db, monkeypatch):
    # 使用限制由独立开关控制，即便 gate_mode=whitelist 也应生效（不再二选一）
    _set(db, "bot_restrict_enabled", "true")
    _set(db, "bot_gate_mode", "whitelist")
    _set(db, "bot_required_channels_json", json.dumps(["@chA", "@chB"]))

    checked = []

    async def fake_api(d, token, method, payload):
        checked.append(payload["chat_id"])
        # 只有 @chB 是 member，其余 left
        status = "member" if payload["chat_id"] == "@chB" else "left"
        return {"ok": True, "result": {"status": status}}

    monkeypatch.setattr(bot_listener, "_api", fake_api)
    ok, reason = asyncio.run(bot_listener.check_restriction(db, "tk", "u1"))
    assert ok is True and reason == ""
    assert "@chB" in checked  # 命中第二个候选即通过（OR，不要求全部）


def test_normalize_tme_link_and_restriction_keyboard(db):
    assert bot_listener._normalize_tme_link("@official") == "https://t.me/official"
    assert bot_listener._normalize_tme_link("https://t.me/g") == "https://t.me/g"
    assert bot_listener._normalize_tme_link("") == ""
    _set(db, "bot_required_channel_link", "@official")
    _set(db, "bot_required_group_link", "https://t.me/chat")
    kb = bot_listener.restriction_keyboard(db)
    urls = [b["url"] for row in kb["inline_keyboard"] for b in row]
    assert "https://t.me/official" in urls and "https://t.me/chat" in urls


# ---------- 3. 推送消息模板变量 ----------
def test_build_message_exposes_channel_group_links(db):
    from app.publisher import _build_message
    from app.models import MessageTemplate

    _set(db, "tg_channel_link", "@mychannel")
    _set(db, "tg_group_link", "@mygroup")
    db.query(MessageTemplate).delete()  # 清理默认模板，确保用下面的测试模板
    tpl = MessageTemplate(name="t", content="CH={{channel_link}} GR={{group_link}} {{title}}",
                          enabled=True, is_default=True)
    db.add(tpl)
    db.flush()
    res = Resource(file_id="v095-1", name="Movie.2024.mkv", identify_status="identified",
                   media_type="movie", media_title="Movie", poster_path="/p.jpg", size=1)
    db.add(res)
    db.commit()
    text, _ = _build_message(db, res, "https://share/x")
    assert "CH=" in text and "@mychannel" in text
    assert "GR=" in text and "@mygroup" in text
    db.query(MessageTemplate).delete()
    db.commit()


# ---------- 4. 路由保存端点：同步维护 Channel + 全局单 Bot ----------
def test_push_routes_save_syncs_channels_and_bot(db):
    from app.main import PushRouteInput, PushRoutesInput, push_routes_save, push_routes_get

    d = push_routes_save(PushRoutesInput(
        bot_token="123456:NEWTOKEN", bot_name="主 Bot",
        routes=[
            PushRouteInput(dir_id="dir-a", kind="channel", chat_id="-100aaa"),
            PushRouteInput(dir_id="dir-b", kind="group", chat_id="-100bbb"),
        ]), db)
    assert d["count"] == 2
    # 路由表落库
    assert len(bot_listener.push_routes(db)) == 2
    # 同步创建两条启用 Channel，kind 正确
    chans = {c.chat_id: c for c in db.scalars(select(Channel)).all()}
    assert chans["-100aaa"].kind == "channel" and chans["-100aaa"].enabled
    assert chans["-100bbb"].kind == "group" and chans["-100bbb"].enabled
    # 全局单 Bot：token 加密写入，interaction_bot_id 指向它
    bot = db.scalar(select(Bot))
    assert bot is not None and bot.name == "主 Bot"
    from app.security import decrypt_secret
    assert decrypt_secret(bot.token) == "123456:NEWTOKEN"
    assert runtime_config.get(db, "interaction_bot_id") == str(bot.id)
    # 回显不含 token 明文
    got = push_routes_get(db)
    assert got["bot"]["id"] == bot.id and "NEWTOKEN" not in str(got)


def test_push_routes_save_caps_at_10_and_skips_empty(db):
    from app.main import PushRouteInput, PushRoutesInput, push_routes_save
    routes = [PushRouteInput(dir_id="", kind="channel", chat_id=str(i)) for i in range(12)]
    routes.append(PushRouteInput(dir_id="x", kind="group", chat_id="   "))  # 空目标跳过
    d = push_routes_save(PushRoutesInput(bot_token="123456:TOK", routes=routes), db)
    assert d["count"] == 10  # 前端/后端双重限制 10 条
