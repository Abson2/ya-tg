"""v0.9.15 #271-#276：自动发现增强（事件驱动 + /id + 智能添加 + startgroup）。

锁死以下红线（15 项场景）：
  1-3) my_chat_member 入群登记（普通群/超级群/频道），带成员状态与 bot 归属；
  4-5) 被移出只标记 left/kicked、不删历史、发送权限失效；
  6) 私有群 /id 命令登记（含 /id@BotUsername），回复信息且不进指令分发；
  7-9) 智能添加：@username / t.me 公开链接 / 数字 Chat ID；
  10) 私有邀请链接（t.me/+ / joinchat）明确提示、不当 ID；
  11) update_id 幂等，重复事件不重复记录；
  12-13) 多 Bot 同群分别记录、一个退出不影响其它；
  14) offset 处理成功后才推进（重启不丢、失败不冒进）；
  15) allowed_updates 含 my_chat_member/message/channel_post，原有推送不受影响。
  startgroup：URL 合法、payload 不含 token、随机短期、多 Bot 独立、频道引导文案。
"""
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from app import bot_listener as bl
from app import main as main_mod
from app import runtime_config
from app.db import SessionLocal, migrate
from app.models import AppConfig, Bot, DiscoveredChat, ProcessedUpdate

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = ROOT / "frontend" / "src" / "App.vue"

CFG_KEYS = ("interaction_bot_id", "interaction_bot_username", "contribution_enabled",
            "contribution_bot_token", "contribution_update_offset", "push_routes_json",
            "bot_gate_mode")


def _run(coro):
    return asyncio.run(coro)


def _set(db, key, value):
    from app.security import encrypt_secret
    row = db.get(AppConfig, key)
    if row is None:
        db.add(AppConfig(key=key, value_enc=encrypt_secret(str(value))))
    else:
        row.value_enc = encrypt_secret(str(value))
    db.flush()


TABLES = [DiscoveredChat, ProcessedUpdate, Bot]


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as db:
        for k in CFG_KEYS:
            row = db.get(AppConfig, k)
            if row:
                db.delete(row)
        db.query(ProcessedUpdate).delete()
        for t in TABLES:
            db.query(t).delete()
        db.commit()
    yield
    with SessionLocal() as db:
        db.query(ProcessedUpdate).delete()
        for t in TABLES:
            db.query(t).delete()
        db.commit()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as s:
        yield s


def _seed_bot(db, token="123456789:AA_FAKE_TOKEN_XYZ", name="交互Bot", selected=True):
    from app.security import encrypt_secret
    b = Bot(name=name, token=encrypt_secret(token), enabled=True)
    db.add(b)
    db.flush()
    if selected:
        _set(db, "interaction_bot_id", b.id)
        _set(db, "contribution_enabled", "true")
    db.flush()
    return b


class FakeResp:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code

    def json(self):
        return self._data


def _poll_with(updates, *, extra_post=None):
    """构造 poll_once 的 httpx mock，返回可复用的 patch 上下文构造器。"""
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
            if extra_post:
                return FakeResp(extra_post(url, json))
            return FakeResp({"ok": True, "result": {}})

    return patch("app.bot_listener.httpx.AsyncClient", FakeClient)


# ---------- 1-3) my_chat_member 入群登记 ----------
@pytest.mark.parametrize("ctype,title,status", [
    ("group", "普通群", "member"),
    ("supergroup", "超级群", "administrator"),
    ("channel", "频道", "creator"),
])
def test_member_join_registers_with_status(db, ctype, title, status):
    _seed_bot(db)
    upd = {"update_id": 10, "my_chat_member": {
        "chat": {"id": -1001, "type": ctype, "title": title},
        "new_chat_member": {"status": status, "user": {"id": 123456789, "username": "mybot"}}}}
    with _poll_with([upd]), \
         patch.object(bl, "auto_clear_stale_commands", new=AsyncMock(return_value=False)):
        stats = _run(bl.poll_once(db))
    row = db.scalar(select(DiscoveredChat).where(DiscoveredChat.chat_id == "-1001"))
    assert row is not None and row.chat_type == ctype and row.title == title
    assert row.member_status == status
    assert row.bot_username == "mybot"
    assert row.message_count == 0  # 成员事件不计消息
    assert stats["member_events"] == 1


# ---------- 4-5) 被移出只标记不删 ----------
def test_member_left_marks_not_delete(db):
    _seed_bot(db)
    bl.record_discovered_chat(db, {"id": -1001, "type": "channel", "title": "频道"})
    db.flush()
    upd = {"update_id": 11, "my_chat_member": {
        "chat": {"id": -1001, "type": "channel", "title": "频道"},
        "new_chat_member": {"status": "left", "user": {"id": 123456789}}}}
    with _poll_with([upd]), \
         patch.object(bl, "auto_clear_stale_commands", new=AsyncMock(return_value=False)):
        _run(bl.poll_once(db))
    row = db.scalar(select(DiscoveredChat).where(DiscoveredChat.chat_id == "-1001"))
    assert row is not None and row.member_status == "left"
    assert row.perm_status == "not_member" and row.perm_can_send is False


def test_member_kicked_marks_only(db):
    _seed_bot(db)
    upd = {"update_id": 12, "my_chat_member": {
        "chat": {"id": -1002, "type": "supergroup", "title": "被踢群"},
        "new_chat_member": {"status": "kicked", "user": {"id": 123456789}}}}
    with _poll_with([upd]), \
         patch.object(bl, "auto_clear_stale_commands", new=AsyncMock(return_value=False)):
        _run(bl.poll_once(db))
    row = db.scalar(select(DiscoveredChat).where(DiscoveredChat.chat_id == "-1002"))
    assert row is not None and row.member_status == "kicked"


# ---------- 6) 私有群 /id 登记 ----------
def test_id_command_registers_private_group(db):
    _seed_bot(db)
    chat = {"id": -1999999, "type": "supergroup", "title": "私有群", "username": ""}
    reply = bl._handle_id_command(db, "123:AA", chat, bot_id=1, update_id=20)
    assert reply and "群组 ID" in reply and "-1999999" in reply
    db.flush()  # autoflush=False：显式 flush 后 writes 才可见
    row = db.scalar(select(DiscoveredChat).where(DiscoveredChat.chat_id == "-1999999"))
    assert row is not None and row.source == "auto"


def test_id_command_ignored_in_channel_and_private(db):
    _seed_bot(db)
    assert bl._handle_id_command(db, "t", {"id": 5, "type": "channel"}, bot_id=1) is None
    assert bl._handle_id_command(db, "t", {"id": 5, "type": "private"}, bot_id=1) is None


def test_id_command_via_poll_intercepted(db):
    """群内 /id 走 poll_once 拦截，不进 handle_text。"""
    _seed_bot(db)
    upd = {"update_id": 21, "message": {
        "message_id": 1, "chat": {"id": -1009, "type": "group", "title": "ID群"},
        "from": {"id": 999}, "text": "/id"}}
    with _poll_with([upd]), \
         patch.object(bl, "auto_clear_stale_commands", new=AsyncMock(return_value=False)), \
         patch.object(bl, "check_restriction", new=AsyncMock(return_value=(True, ""))), \
         patch.object(bl, "handle_text", new=AsyncMock(return_value=("ignored", ""))) as htext:
        stats = _run(bl.poll_once(db))
    htext.assert_not_called()
    assert "id" in stats["actions"]
    row = db.scalar(select(DiscoveredChat).where(DiscoveredChat.chat_id == "-1009"))
    assert row is not None


# ---------- 7-9) 智能添加解析 ----------
def test_parse_chat_input_username_link_id():
    assert bl.parse_chat_input("@publicgroup") == {"kind": "username", "chat_id": "@publicgroup", "error": ""}
    r = bl.parse_chat_input("https://t.me/publicchannel")
    assert r["kind"] == "username" and r["chat_id"] == "@publicchannel"
    r = bl.parse_chat_input("https://t.me/c/1234567890")  # 含斜杠的私有频道链接 → invalid
    assert r["kind"] == "invalid"
    assert bl.parse_chat_input("-1001234567890")["kind"] == "id"
    assert bl.parse_chat_input("123456789")["kind"] == "id"


def test_resolve_chat_via_getchat_ok_and_error(db):
    _seed_bot(db)

    async def ok(d, token, method, payload):
        return {"ok": True, "result": {"id": -100777, "title": "公开群", "type": "supergroup", "username": "pub"}}

    with patch.object(bl, "_api", ok):
        res = _run(bl.resolve_chat_via_getchat(db, "t", "@pub"))
    assert res["ok"] and res["chat"]["id"] == "-100777" and res["chat"]["username"] == "pub"

    async def bad(d, token, method, payload):
        return {"ok": False, "description": "Chat not found"}

    with patch.object(bl, "_api", bad):
        res = _run(bl.resolve_chat_via_getchat(db, "t", "@notexist"))
    assert not res["ok"] and res["error_kind"] == "auth"


# ---------- 10) 私有邀请链接 ----------
def test_parse_invite_link_rejected():
    for raw in ("https://t.me/+abcdef", "https://t.me/joinchat/AAAA", "telegram.me/+xyz"):
        r = bl.parse_chat_input(raw)
        assert r["kind"] == "invite" and "私有" in r["error"] and "/id" in r["error"]
        assert r["chat_id"] == ""


# ---------- 11) update_id 幂等 ----------
def test_duplicate_update_id_not_recorded_twice(db):
    _seed_bot(db)
    upd = {"update_id": 30, "message": {
        "message_id": 1, "chat": {"id": -1001, "type": "group", "title": "幂等群"},
        "from": {"id": 9}, "text": "hi"}}
    with _poll_with([upd]), \
         patch.object(bl, "auto_clear_stale_commands", new=AsyncMock(return_value=False)), \
         patch.object(bl, "check_restriction", new=AsyncMock(return_value=(True, ""))), \
         patch.object(bl, "handle_text", new=AsyncMock(return_value=("ignored", ""))):
        _run(bl.poll_once(db))
        # 同一 update 再投一次
        _run(bl.poll_once(db))
    row = db.scalar(select(DiscoveredChat).where(DiscoveredChat.chat_id == "-1001"))
    assert row.message_count == 1  # 只记一次
    assert db.scalar(select(ProcessedUpdate).where(ProcessedUpdate.update_id == 30)) is not None


# ---------- 12-13) 多 Bot 隔离 ----------
def test_two_bots_same_chat_separate_records(db):
    b1 = _seed_bot(db, token="111:AA", name="Bot1")
    b2 = _seed_bot(db, token="222:AA", name="Bot2", selected=False)
    chat = {"id": -1001, "type": "supergroup", "title": "共享群"}
    bl.record_discovered_chat(db, chat, bot_id_override=b1.id)
    bl.record_discovered_chat(db, chat, bot_id_override=b2.id)
    rows = db.scalars(select(DiscoveredChat).where(DiscoveredChat.chat_id == "-1001")).all()
    assert len(rows) == 2 and {r.bot_id for r in rows} == {b1.id, b2.id}


def test_one_bot_leaves_not_affect_other(db):
    b1 = _seed_bot(db, token="111:AA", name="Bot1")
    b2 = _seed_bot(db, token="222:AA", name="Bot2", selected=False)
    chat = {"id": -1001, "type": "group", "title": "群"}
    bl.record_discovered_chat(db, chat, bot_id_override=b1.id, member_status="member")
    bl.record_discovered_chat(db, chat, bot_id_override=b2.id, member_status="member")
    bl.record_discovered_chat(db, chat, bot_id_override=b1.id, member_status="left")
    r1 = db.scalar(select(DiscoveredChat).where(DiscoveredChat.bot_id == b1.id))
    r2 = db.scalar(select(DiscoveredChat).where(DiscoveredChat.bot_id == b2.id))
    assert r1.member_status == "left" and r2.member_status == "member"


# ---------- 14) offset 成功后才推进 ----------
def test_offset_advances_after_success(db):
    _seed_bot(db)
    _set(db, "contribution_update_offset", "5")
    upd = {"update_id": 6, "message": {
        "message_id": 1, "chat": {"id": -1001, "type": "group", "title": "G"},
        "from": {"id": 9}, "text": "hi"}}
    with _poll_with([upd]), \
         patch.object(bl, "auto_clear_stale_commands", new=AsyncMock(return_value=False)), \
         patch.object(bl, "check_restriction", new=AsyncMock(return_value=(True, ""))), \
         patch.object(bl, "handle_text", new=AsyncMock(return_value=("ignored", ""))):
        _run(bl.poll_once(db))
    assert runtime_config.get(db, "contribution_update_offset") == "7"


def test_error_409_does_not_advance_offset(db):
    _seed_bot(db)
    _set(db, "contribution_update_offset", "5")
    class Client409:
        def __init__(self, *a, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def get(self, url, params=None): return FakeResp({"ok": False, "description": "Conflict"}, 409)
    with patch("app.bot_listener.httpx.AsyncClient", Client409), \
         patch.object(bl, "auto_clear_stale_commands", new=AsyncMock(return_value=False)):
        stats = _run(bl.poll_once(db))
    assert stats.get("error_kind") == "conflict"
    assert runtime_config.get(db, "contribution_update_offset") == "5"


# ---------- 15) allowed_updates + channel_post ----------
def test_allowed_updates_complete():
    assert "my_chat_member" in bl.ALLOWED_UPDATES
    assert "message" in bl.ALLOWED_UPDATES
    assert "channel_post" in bl.ALLOWED_UPDATES
    assert "callback_query" in bl.ALLOWED_UPDATES


def test_channel_post_discovers_channel(db):
    _seed_bot(db)
    upd = {"update_id": 40, "channel_post": {
        "message_id": 1, "chat": {"id": -100123, "type": "channel", "title": "影视频道"},
        "text": "新片"}}
    with _poll_with([upd]), \
         patch.object(bl, "auto_clear_stale_commands", new=AsyncMock(return_value=False)):
        stats = _run(bl.poll_once(db))
    row = db.scalar(select(DiscoveredChat).where(DiscoveredChat.chat_id == "-100123"))
    assert row is not None and row.chat_type == "channel"
    assert stats["discovered"] >= 1


# ---------- startgroup ----------
def test_build_startgroup_url_valid_and_no_token():
    url = bl.build_startgroup_url("@MyBot", "yg_1f_abcd1234")
    assert url == "https://t.me/MyBot?startgroup=yg_1f_abcd1234"
    assert "AA_FAKE" not in url and "123456789:" not in url  # 不含 token 指纹
    assert url.count(":") == 1  # 只有 https 的冒号，没有 token 的冒号
    assert bl.build_startgroup_url("", "x") == ""
    assert bl.build_startgroup_url("a", "x") == ""  # 用户名至少 5 位


def test_payload_random_short_and_no_token():
    p1, p2 = bl.new_startgroup_payload(), bl.new_startgroup_payload()
    assert p1.startswith("yg_") and p1 != p2
    assert len(p1) <= 64 and "AA_FAKE" not in p1 and ":" not in p1


def test_get_bot_username_from_getme(db):
    _seed_bot(db)

    async def ok(d, token, method, payload):
        assert method == "getMe"
        return {"ok": True, "result": {"id": 123456789, "username": "MyRealBot"}}

    with patch.object(bl, "_api", ok):
        name, err = _run(bl.get_bot_username(db, "123:AA"))
    assert name == "MyRealBot" and err == ""

    async def no_username(d, token, method, payload):
        return {"ok": True, "result": {"id": 123456789}}

    with patch.object(bl, "_api", no_username):
        name, err = _run(bl.get_bot_username(db, "123:AA"))
    assert name == "" and "username" in err


def test_invite_link_endpoint_main_bot(db):
    _seed_bot(db)

    async def ok(d, token, method, payload):
        return {"ok": True, "result": {"username": "MainBot"}}

    with patch.object(bl, "_api", ok):
        res = _run(main_mod.discover_invite_link(main_mod.DiscoverInviteInput(), db))
    assert res["ok"] and res["bot_username"] == "MainBot"
    assert res["startgroup_url"].startswith("https://t.me/MainBot?startgroup=")
    assert "AA_FAKE" not in res["startgroup_url"]
    assert res["expires_at"] and isinstance(res["channel_guide"], list) and res["channel_guide"]


def test_invite_link_endpoint_fallback_username(db):
    _seed_bot(db)
    _set(db, "interaction_bot_username", "FallbackBot")

    async def bad(d, token, method, payload):
        return {"ok": False, "description": "network down"}

    with patch.object(bl, "_api", bad):
        res = _run(main_mod.discover_invite_link(main_mod.DiscoverInviteInput(), db))
    assert res["ok"] and res["bot_username"] == "FallbackBot"


def test_invite_link_multi_bot_independent(db):
    b1 = _seed_bot(db, token="111:AA", name="Bot1")
    b2 = _seed_bot(db, token="222:AA", name="Bot2", selected=False)

    async def ok1(d, token, method, payload):
        return {"ok": True, "result": {"username": "BotOne"}}

    async def ok2(d, token, method, payload):
        return {"ok": True, "result": {"username": "BotTwo"}}

    with patch.object(bl, "_api", ok1):
        r1 = _run(main_mod.discover_invite_link(main_mod.DiscoverInviteInput(bot_id=b1.id), db))
    with patch.object(bl, "_api", ok2):
        r2 = _run(main_mod.discover_invite_link(main_mod.DiscoverInviteInput(bot_id=b2.id), db))
    assert r1["bot_username"] == "BotOne" and r2["bot_username"] == "BotTwo"
    assert r1["startgroup_url"] != r2["startgroup_url"]
    assert r1["bot_id"] == b1.id and r2["bot_id"] == b2.id


def test_invite_link_no_token_returns_error(db):
    res = _run(main_mod.discover_invite_link(main_mod.DiscoverInviteInput(), db))
    assert not res["ok"] and "Token" in res["error"]


def test_frontend_startgroup_ui_present():
    app = APP_VUE.read_text(encoding="utf-8")
    for token in ("startgroup", "加入群组", "添加频道", "生成授权链接",
                  "复制用户名", "复制授权链接", "discFilterType", "batchWhitelist",
                  "channelGuideVisible", "/discover/invite-link",
                  "暂无发现记录。请确保 Bot 已加入群组"):
        assert token in app, token
