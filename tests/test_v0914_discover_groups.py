"""v0.9.14 #255-#258：自动发现群组 + 统一推送目标管理回归。

锁死以下红线：
  #256 record_discovered_chat 只收群/超级群/频道、bot+chat 幂等、累加消息数；
       poll_once 收到群消息即发现；getChatMember 权限判定（频道/群/离开）。
  #257 目录路由一个目录绑定多个频道/群（targets 列表），展开与回退兼容老双字段。
  #258 统一目标列表、手动添加、勾选加入/移出白名单、删除、测试推送端点；
       前端自动发现 UI 静态断言。
"""
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from app import bot_listener as bl
from app import main as main_mod
from app import runtime_config
from app.db import SessionLocal, json_dumps, migrate
from app.models import AppConfig, Bot, Channel, DiscoveredChat, ProcessedUpdate

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = ROOT / "frontend" / "src" / "App.vue"

CFG_KEYS = ("interaction_bot_id", "contribution_enabled", "contribution_bot_token",
            "push_routes_json", "bot_gate_mode", "contribution_update_offset")


def _run(coro):
    # 用 asyncio.run 每次新建并关闭 loop：全量套件里其它测试会关闭默认 loop，
    # get_event_loop() 会抛 RuntimeError；run() 也能避免 coroutine never awaited 警告。
    return asyncio.run(coro)


def _set(db, key, value):
    from app.security import encrypt_secret
    row = db.get(AppConfig, key)
    if row is None:
        db.add(AppConfig(key=key, value_enc=encrypt_secret(str(value))))
    else:
        row.value_enc = encrypt_secret(str(value))
    db.flush()


TABLES = [DiscoveredChat, Channel, Bot, ProcessedUpdate]


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as db:
        for k in CFG_KEYS:
            row = db.get(AppConfig, k)
            if row:
                db.delete(row)
        for t in TABLES:
            db.query(t).delete()
        db.commit()
    yield
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        db.commit()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as s:
        yield s


def _seed_bot(db):
    from app.security import encrypt_secret
    b = Bot(name="交互Bot", token=encrypt_secret("123456789:AA_FAKE_TOKEN_XYZ"), enabled=True)
    db.add(b)
    db.flush()
    _set(db, "interaction_bot_id", b.id)
    _set(db, "contribution_enabled", "true")
    db.flush()
    return b


# ---------- #256：发现记录 ----------
def test_record_discovered_only_group_type(db):
    _seed_bot(db)
    assert bl.record_discovered_chat(db, {"id": -1001, "type": "supergroup", "title": "资源群"}) is not None
    assert bl.record_discovered_chat(db, {"id": -1002, "type": "channel", "title": "频道"}) is not None
    assert bl.record_discovered_chat(db, {"id": 123, "type": "private"}) is None  # 私聊不发现
    assert bl.record_discovered_chat(db, {"id": -1003}) is None  # 无 type 不发现
    rows = db.scalars(select(DiscoveredChat)).all()
    assert len(rows) == 2
    types = {r.chat_type for r in rows}
    assert types == {"supergroup", "channel"}


def test_record_discovered_is_idempotent_and_counts(db):
    _seed_bot(db)
    chat = {"id": -1001, "type": "group", "title": "测试群", "username": "testgroup"}
    bl.record_discovered_chat(db, chat)
    bl.record_discovered_chat(db, chat)
    bl.record_discovered_chat(db, {"id": -1001, "type": "group", "title": "测试群改名"})
    rows = db.scalars(select(DiscoveredChat)).all()
    assert len(rows) == 1  # 幂等，不重复插
    assert rows[0].message_count == 3
    assert rows[0].title == "测试群改名"  # 名称刷新
    assert rows[0].username == "testgroup"


def test_record_discovered_manual_source_preserved(db):
    _seed_bot(db)
    bl.record_discovered_chat(db, {"id": -1001, "type": "group", "source": "manual"})
    # 手动来源直接绕开 record（它默认 auto），这里验证 source 字段可保留
    row = db.scalar(select(DiscoveredChat).where(DiscoveredChat.chat_id == "-1001"))
    row.source = "manual"
    db.flush()
    bl.record_discovered_chat(db, {"id": -1001, "type": "group", "title": "x"})
    assert row.source == "manual"


def test_bot_user_id_from_token():
    assert bl.bot_user_id_from_token("123456789:AAxx") == "123456789"
    assert bl.bot_user_id_from_token("garbage") == ""


def test_check_permission_group_member_cannot_send(db):
    _seed_bot(db)
    async def fake_api(d, token, method, payload):
        return {"ok": True, "result": {"status": "member", "can_send_messages": False}}
    with patch.object(bl, "_api", new=fake_api):
        res = _run(bl.check_chat_permission(db, "123:AA", "-1001", "group"))
    assert res["status"] == "no_permission" and res["can_send"] is False


def test_check_permission_group_admin_can_send(db):
    _seed_bot(db)
    async def fake_api(d, token, method, payload):
        return {"ok": True, "result": {"status": "administrator"}}
    with patch.object(bl, "_api", new=fake_api):
        res = _run(bl.check_chat_permission(db, "123:AA", "-1001", "supergroup"))
    assert res["status"] == "ok" and res["can_send"] is True


def test_check_permission_group_member_default_can_send(db):
    """普通群 member 不带 can_send_messages 字段时应默认可发（Telegram ChatMemberMember 无该字段）。"""
    _seed_bot(db)
    async def fake_api(d, token, method, payload):
        return {"ok": True, "result": {"status": "member"}}
    with patch.object(bl, "_api", new=fake_api):
        res = _run(bl.check_chat_permission(db, "123:AA", "-1001", "group"))
    assert res["status"] == "ok" and res["can_send"] is True


def test_check_permission_channel_needs_post_rights(db):
    _seed_bot(db)
    async def fake_no(d, token, method, payload):
        return {"ok": True, "result": {"status": "administrator", "can_post_messages": False}}
    with patch.object(bl, "_api", new=fake_no):
        res = _run(bl.check_chat_permission(db, "123:AA", "-1001", "channel"))
    assert res["status"] == "no_permission"  # 频道管理员未开发布权限也不能发

    async def fake_yes(d, token, method, payload):
        return {"ok": True, "result": {"status": "administrator", "can_post_messages": True}}
    with patch.object(bl, "_api", new=fake_yes):
        res = _run(bl.check_chat_permission(db, "123:AA", "-1001", "channel"))
    assert res["status"] == "ok" and res["can_send"] is True


def test_check_permission_not_member(db):
    _seed_bot(db)
    async def fake_left(d, token, method, payload):
        return {"ok": True, "result": {"status": "left"}}
    with patch.object(bl, "_api", new=fake_left):
        res = _run(bl.check_chat_permission(db, "123:AA", "-1001", "group"))
    assert res["status"] == "not_member" and res["can_send"] is False


def test_poll_once_discovers_group_from_message(db):
    """poll_once 收到群消息即发现（mock getUpdates + auto_clear + 限制检查 + 回复）。"""
    _seed_bot(db)
    upd = {"update_id": 1, "message": {"message_id": 5,
           "chat": {"id": -1001, "type": "supergroup", "title": "真机群"},
           "from": {"id": 999, "username": "u"}, "text": "你好"}}

    class FakeResp:
        def __init__(self, data):
            self._data = data
        def json(self):
            return self._data

    class FakeClient:
        def __init__(self, *a, **kw):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *a):
            return False
        async def get(self, url, params=None):
            return FakeResp({"ok": True, "result": [upd]})
        async def post(self, url, json=None):
            return FakeResp({"ok": True, "result": {}})

    with patch("app.bot_listener.httpx.AsyncClient", FakeClient), \
         patch.object(bl, "auto_clear_stale_commands", new=AsyncMock(return_value=False)), \
         patch.object(bl, "check_restriction", new=AsyncMock(return_value=(True, ""))), \
         patch.object(bl, "handle_text", new=AsyncMock(return_value=("ignored", ""))):
        stats = _run(bl.poll_once(db))
    assert stats.get("enabled") is True, stats
    assert not stats.get("error"), stats
    row = db.scalar(select(DiscoveredChat).where(DiscoveredChat.chat_id == "-1001"))
    assert row is not None and row.title == "真机群" and row.chat_type == "supergroup"


# ---------- #257：一目录多目标 ----------
def test_push_routes_expands_multiple_targets(db):
    _set(db, "push_routes_json", json_dumps([
        {"dir_id": "dir001", "targets": [
            {"kind": "channel", "chat_id": "-1001", "name": "频道A"},
            {"kind": "group", "chat_id": "-1002", "name": "群B"},
            {"kind": "group", "chat_id": "-1003", "name": "群C"},
        ], "note": "电视剧目录"}
    ]))
    routes = bl.push_routes(db)
    assert len(routes) == 3
    assert {r["chat_id"] for r in routes} == {"-1001", "-1002", "-1003"}
    raw = bl.push_routes_raw(db)
    assert raw[0]["targets"][0]["name"] == "频道A"


def test_push_routes_backward_compatible(db):
    _set(db, "push_routes_json", json_dumps([
        {"dir_id": "dir001", "kind": "group", "chat_id": "-1002"}
    ]))
    routes = bl.push_routes(db)
    assert len(routes) == 1 and routes[0]["kind"] == "group" and routes[0]["chat_id"] == "-1002"


def test_save_routes_preserves_targets(db):
    b = _seed_bot(db)
    p = main_mod.PushRoutesInput(routes=[main_mod.PushRouteInput(
        dir_id="dir001", note="多目标",
        targets=[main_mod.PushTargetInput(kind="channel", chat_id="-1001", name="频道A"),
                 main_mod.PushTargetInput(kind="group", chat_id="-1002", name="群B")])])
    res = main_mod.push_routes_save(p, db)
    assert res["count"] == 1
    raw = bl.push_routes_raw(db)
    assert len(raw[0]["targets"]) == 2
    # Channel 落点为两条
    chats = {c.chat_id for c in db.scalars(select(Channel)).all()}
    assert {"-1001", "-1002"}.issubset(chats)


# ---------- #258：API 端点 ----------
def test_discover_chats_reflects_whitelist(db):
    _seed_bot(db)
    bl.record_discovered_chat(db, {"id": -1001, "type": "group", "title": "群A"})
    _set(db, "push_routes_json", json_dumps([
        {"dir_id": "", "targets": [{"kind": "group", "chat_id": "-1001", "name": "群A"}]}
    ]))
    res = main_mod.discover_chats(db)
    assert res["total"] == 1 and res["chats"][0]["in_whitelist"] is True
    assert res["chats"][0]["type_label"] == "普通群"


def test_manual_add_via_refresh(db):
    _seed_bot(db)
    p = main_mod.DiscoverRefreshInput(manual_chat_id="@publicchannel",
                                      manual_name="公开频道", manual_kind="channel")

    async def fake_resolve(db_, token, ref):
        return {"ok": True, "chat": {"id": "-100777", "title": "公开频道",
                                     "type": "channel", "username": "publicchannel"}}

    with patch.object(bl, "resolve_chat_via_getchat", fake_resolve):
        res = _run(main_mod.discover_refresh(p, db))
    assert res["added"]["chat_id"] == "-100777"
    assert res["added"]["source"] == "manual"
    assert res["added"]["chat_type"] == "channel"
    assert res["added"]["username"] == "publicchannel"


def test_set_whitelist_add_and_remove(db):
    _seed_bot(db)
    bl.record_discovered_chat(db, {"id": -1001, "type": "supergroup", "title": "超级群"})
    row = db.scalar(select(DiscoveredChat))
    # 加入
    main_mod.discover_set_whitelist(row.id, main_mod.DiscoverWhitelistInput(in_whitelist=True), db)
    assert row.in_whitelist is True
    assert db.scalar(select(Channel).where(Channel.chat_id == "-1001")) is not None
    raw = bl.push_routes_raw(db)
    assert any(t["chat_id"] == "-1001" for r in raw for t in r["targets"])
    # supergroup 必须归为 group，不能被误判成 channel
    assert any(t["chat_id"] == "-1001" and t["kind"] == "group" for r in raw for t in r["targets"])
    ch = db.scalar(select(Channel).where(Channel.chat_id == "-1001"))
    assert ch.kind == "group"
    # 移出
    main_mod.discover_set_whitelist(row.id, main_mod.DiscoverWhitelistInput(in_whitelist=False), db)
    assert row.in_whitelist is False
    ch = db.scalar(select(Channel).where(Channel.chat_id == "-1001"))
    assert ch is None or ch.enabled is False


def test_delete_chat_removes_from_routes(db):
    _seed_bot(db)
    bl.record_discovered_chat(db, {"id": -1001, "type": "group", "title": "群"})
    row = db.scalar(select(DiscoveredChat))
    main_mod.discover_set_whitelist(row.id, main_mod.DiscoverWhitelistInput(in_whitelist=True), db)
    main_mod.discover_delete_chat(row.id, db)
    assert db.scalar(select(DiscoveredChat).where(DiscoveredChat.chat_id == "-1001")) is None
    raw = bl.push_routes_raw(db)
    assert not any(t["chat_id"] == "-1001" for r in raw for t in r["targets"])


# ---------- 前端静态断言 ----------
def test_frontend_discover_ui_present():
    app = APP_VUE.read_text(encoding="utf-8")
    assert "discoverChats" in app
    assert "/discover/chats" in app and "/discover/refresh" in app
    assert "自动发现" in app and "检查权限" in app and "测试推送" in app
