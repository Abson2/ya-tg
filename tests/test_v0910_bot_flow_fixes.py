"""v0.9.9 #207-#209 Bot 三项真机反馈回归。

锁死以下红线：
  - #207：每个 token 首次轮询自动清空一次旧项目残留的 "/" 指令栏，指纹去重只清一次。
  - #208：投稿不再产出「未命名投稿」；用户回执不含「转存接口/暂不自动入库/管理员审核」等后台状态
          （这些只写后台 note）。
  - #209：点「求片/投稿」裸命令进入文字会话态，后续直接输入文字即可，无需 / 命令；
          结束词与 inline「结束交互」按钮都能清会话；poll_once 在会话态附结束键盘。
"""
import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from app import bot_listener, runtime_config
from app.db import SessionLocal, migrate
from app.models import AppConfig, MediaRequest, UserContribution

TABLES = (UserContribution, MediaRequest)
CFG_KEYS = ("bot_commands_cleared_fp", "contribution_bot_token", "contribution_enabled",
            "bot_gate_mode", "tmdb_api_key", "tmdb_v4_token")
CANDIDATES = [{"tmdb_id": 261315, "title": "十万狂花入梦来", "year": "2026",
               "media_type": "tv", "vote_average": 9.0}]
BANNED_USER_TEXT = ("转存接口", "暂不自动入库", "管理员审核", "管理员处理", "未命名投稿")


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    bot_listener._flow_sessions.clear()
    bot_listener._pick_sessions.clear()
    bot_listener._AUTO_CLEARED_TOKENS.clear()
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
    bot_listener._AUTO_CLEARED_TOKENS.clear()
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        db.commit()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as s:
        yield s


def _fake_api_recorder(calls):
    async def fake_api(d, t, method, payload):
        calls.append((method, payload))
        return {"ok": True}
    return fake_api


def _fake_search(candidates):
    async def fake(d, q, media_type=""):
        return candidates
    return fake


# ---------- #207 指令栏自动清空 ----------
def test_auto_clear_commands_once_per_token(db):
    calls = []
    with patch.object(bot_listener, "_api", new=_fake_api_recorder(calls)):
        assert asyncio.run(bot_listener.auto_clear_stale_commands(db, "123:AAA")) is True
        # 同一 token 第二次不再打 TG 接口
        assert asyncio.run(bot_listener.auto_clear_stale_commands(db, "123:AAA")) is False
        # 不同 token 再清一次
        assert asyncio.run(bot_listener.auto_clear_stale_commands(db, "123:BBB")) is True
    calls = [c for c in calls if c[0] == "setMyCommands"]
    assert len(calls) == 2
    assert all(c[1]["commands"] == [] for c in calls)  # 置空，非注册旧指令


def test_auto_clear_persists_fingerprint(db):
    with patch.object(bot_listener, "_api", new=_fake_api_recorder([])):
        assert asyncio.run(bot_listener.auto_clear_stale_commands(db, "123:CCC")) is True
    # 清掉内存标记后，DB 指纹仍能让下一进程跳过
    bot_listener._AUTO_CLEARED_TOKENS.clear()
    calls = []
    with patch.object(bot_listener, "_api", new=_fake_api_recorder(calls)):
        assert asyncio.run(bot_listener.auto_clear_stale_commands(db, "123:CCC")) is False
    assert not [c for c in calls if c[0] == "setMyCommands"]


# ---------- #209 会话态：裸命令进入 ----------
def test_bare_want_enters_flow(db):
    action, reply = asyncio.run(bot_listener.handle_text(db, "求片", tg_user_id="u1"))
    assert action == "flow_want"
    assert "直接输入剧名" in reply
    assert bot_listener._flow_get("u1") == bot_listener.FLOW_WANT


def test_bare_contribute_enters_flow(db):
    action, reply = asyncio.run(bot_listener.handle_text(db, "投稿", tg_user_id="u2"))
    assert action == "flow_contribute"
    assert "直接输入剧名" in reply
    assert bot_listener._flow_get("u2") == bot_listener.FLOW_CONTRIB


def test_flow_text_without_slash_routes_to_candidates(db):
    bot_listener._flow_set("u3", bot_listener.FLOW_WANT)
    with patch.object(bot_listener, "search_candidates", new=_fake_search(CANDIDATES)):
        action, reply = asyncio.run(
            bot_listener.handle_text(db, "十万狂花入梦来", tg_user_id="u3"))
    assert action == "want_pick"
    assert "261315" in reply
    assert bot_listener._pick_get("u3") is not None


# ---------- #209 结束交互 ----------
def test_end_word_clears_flow_and_pick(db):
    bot_listener._flow_set("u4", bot_listener.FLOW_CONTRIB)
    bot_listener._pick_set("u4", bot_listener.FLOW_CONTRIB, "x", CANDIDATES)
    action, reply = asyncio.run(bot_listener.handle_text(db, "结束", tg_user_id="u4"))
    assert action == "flow_end"
    assert "已结束本次交互" in reply
    assert bot_listener._flow_get("u4") == ""
    assert bot_listener._pick_get("u4") is None


def test_cb_end_clears_flow_and_pick(db):
    bot_listener._flow_set("u5", bot_listener.FLOW_WANT)
    bot_listener._pick_set("u5", bot_listener.FLOW_WANT, "x", CANDIDATES)
    calls = []
    cb = {"id": "cb1", "data": bot_listener.CB_END,
          "message": {"message_id": 1, "chat": {"id": "555"}},
          "from": {"id": "u5"}}
    with patch.object(bot_listener, "_api", new=_fake_api_recorder(calls)):
        asyncio.run(bot_listener._handle_callback(db, "tk", cb))
    assert "answerCallbackQuery" in [c[0] for c in calls]
    assert bot_listener._flow_get("u5") == ""
    assert bot_listener._pick_get("u5") is None


def test_end_flow_keyboard_carries_callback():
    kb = bot_listener.end_flow_keyboard()
    data = [b["callback_data"] for row in kb["inline_keyboard"] for b in row]
    assert bot_listener.CB_END in data
    assert all(len(d.encode()) < 64 for d in data)


# ---------- #208 投稿后台信息不外泄 ----------
def test_contribute_link_never_unnamed(db):
    with patch.object(bot_listener, "search_candidates", new=_fake_search([])):
        action, reply = asyncio.run(bot_listener.handle_text(
            db, "投稿 https://www.guangyapan.com/s/abc123", tg_user_id="u6"))
    assert action == "contribute"
    assert "未命名投稿" not in reply
    db.flush()  # autoflush=False：写完即查必须先 flush
    row = db.query(UserContribution).order_by(UserContribution.id.desc()).first()
    assert row is not None and "未命名" not in (row.title or "")


def test_user_reply_has_no_internal_state(db):
    with patch.object(bot_listener, "search_candidates", new=_fake_search([])), \
         patch.object(bot_listener.tgto, "upsert_keyword",
                      new=AsyncMock(return_value={"ok": True})):
        _, r1 = asyncio.run(bot_listener.handle_text(db, "投稿 某剧", tg_user_id="u7"))
        _, r2 = asyncio.run(bot_listener.handle_text(db, "求片 另一剧", tg_user_id="u7"))
    for r in (r1, r2):
        for bad in BANNED_USER_TEXT:
            assert bad not in r, (bad, r)
    # 后台 note 仍保留内部状态，供管理员排查
    db.flush()
    row = db.query(UserContribution).order_by(UserContribution.id.desc()).first()
    assert row is not None and "转存接口" in (row.note or "")


def test_contribute_picked_then_link_reply_is_clean(db):
    sess = {"flow": bot_listener.FLOW_CONTRIB, "query": "十万狂花入梦来",
            "candidates": CANDIDATES, "secret": "", "created": time.time(),
            "picked": {"title": "十万狂花入梦来", "tmdb_id": 261315,
                       "media_type": "tv", "year": "2026"}}
    bot_listener._pick_sessions["u8"] = sess
    action, reply = asyncio.run(bot_listener.handle_text(
        db, "https://www.guangyapan.com/s/xyz789", tg_user_id="u8"))
    assert action == "contribute"
    for bad in BANNED_USER_TEXT:
        assert bad not in reply, (bad, reply)
    db.flush()
    row = db.query(UserContribution).order_by(UserContribution.id.desc()).first()
    assert row.tmdb_id == 261315 and row.year == "2026"


# ---------- #209 poll_once 会话态附结束键盘 ----------
def test_poll_once_attaches_end_keyboard_in_flow(db):
    runtime_config.set_many(db, {"contribution_enabled": "true"})
    upd = {"update_id": 1,
           "message": {"text": "求片", "chat": {"id": "555"},
                       "from": {"id": "u9", "username": "nine"}}}

    class FakeResp:
        def json(self):
            return {"ok": True, "result": [upd]}

    class FakeClient:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, *a, **kw):
            return FakeResp()

    replies = []

    async def fake_reply(d, t, chat_id, text, **kw):
        replies.append((text, kw.get("reply_markup")))

    with patch.object(bot_listener, "effective_token", return_value="123:TOK"), \
         patch.object(bot_listener, "auto_clear_stale_commands",
                      new=AsyncMock(return_value=False)), \
         patch.object(bot_listener, "check_restriction",
                      new=AsyncMock(return_value=(True, ""))), \
         patch.object(bot_listener.httpx, "AsyncClient", FakeClient), \
         patch.object(bot_listener, "_reply", new=fake_reply):
        stats = asyncio.run(bot_listener.poll_once(db))
    assert stats["handled"] == 1, stats
    assert replies and replies[0][1] is not None, (stats, replies)
    keyboard = replies[0][1]
    data = [b["callback_data"] for row in keyboard["inline_keyboard"] for b in row]
    assert bot_listener.CB_END in data
