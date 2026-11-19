"""v0.9.10 群聊推送 + 消息模板统一跳转链接回归。

真机根因与修复：
  - 测试推送走 sendMessage（无图无分享）能收；正常推送入队强制带海报->sendPhoto，
    且 sendPhoto 失败无回退、建分享失败直接跳过发送 -> 群/频道一条都收不到。
    修复：sendPhoto 失败回退 sendMessage；建分享失败不再阻断发送。
  - 路由 Channel 换 Bot 后 bot_id 不更新（route: 前缀被排除），导致测试用新 Bot、
    正常用旧 bot_id -> 错配收不到。修复：路由 Channel 统一同步 bot_id。
  - 所有模板统一在正文末尾追加频道/群/机器人三行文字链接（取自配置），
    {{buttons_text}} 置空；inline 按钮只保留「📥 获取资源」。
"""
import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from app import main, publisher, runtime_config
from app.db import SessionLocal, migrate
from app.models import Bot, Channel, MessageTemplate, QueueItem, Resource
from app.security import encrypt_secret

TABLES = (QueueItem, Resource, Channel, Bot, MessageTemplate)


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as s:
        for t in TABLES:
            s.query(t).delete()
        for k in ("tg_channel_link", "tg_group_link", "tg_bot_link", "tg_link_style",
                  "interaction_bot_id", "contribution_bot_token"):
            row = s.get(__import__("app.models", fromlist=["AppConfig"]).AppConfig, k)
            if row:
                s.delete(row)
        s.commit()
    yield
    with SessionLocal() as s:
        for t in TABLES:
            s.query(t).delete()
        s.commit()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as s:
        yield s


def _seed(db, chat_id="-987654321", poster="/p.jpg"):
    b = Bot(name="b", token=encrypt_secret("1:TOK"), enabled=True)
    db.add(b); db.flush()
    c = Channel(bot_id=b.id, name="群", chat_id=chat_id, kind="group", enabled=True)
    db.add(c); db.flush()
    r = Resource(file_id="g-push", name="Movie.2024.mkv", ext=".mkv", size=1, utime=1,
                 identify_status="identified", media_type="movie", media_title="Movie",
                 poster_path=poster, share_url="https://share/x")
    db.add(r); db.flush()
    q = QueueItem(resource_id=r.id, channel_id=c.id, status="pending")
    db.add(q); db.commit()
    return b, c, r, q


class _Resp:
    def __init__(self, ok=True, status=200, desc="", mid="7"):
        self.status_code = status
        self.text = ""
        self._data = {"ok": ok, "result": {"message_id": mid}}
        if desc:
            self._data["description"] = desc

    def json(self):
        return self._data


# ---------- sendPhoto 失败回退 sendMessage ----------
def test_sendphoto_failure_falls_back_to_sendmessage(db):
    b, c, r, q = _seed(db)
    calls = []

    async def fake_post(self, url, json=None, **kw):
        calls.append((url.split("/")[-1], json))
        if calls[-1][0] == "sendPhoto":
            return _Resp(ok=False, status=400, desc="can't get photo")
        return _Resp(ok=True, mid="9")

    with patch("httpx.AsyncClient.post", new=fake_post):
        ok, mid, err, _ = asyncio.run(publisher._send_telegram(db, q, r, c, b))
    assert ok is True and mid == "9" and err == ""
    methods = [m for m, _ in calls]
    assert methods == ["sendPhoto", "sendMessage"], calls
    # 回退消息仍发到同一个群 chat_id
    assert all(p["chat_id"] == "-987654321" for _, p in calls)


def test_sendphoto_429_does_not_fallback(db):
    """429 限流走上层退避，不回退重发（避免加剧限流）。"""
    b, c, r, q = _seed(db)
    calls = []

    async def fake_post(self, url, json=None, **kw):
        calls.append(url.split("/")[-1])
        return _Resp(ok=False, status=429, desc="Too Many Requests")

    with patch("httpx.AsyncClient.post", new=fake_post):
        ok, _, err, retry_after = asyncio.run(publisher._send_telegram(db, q, r, c, b))
    assert ok is False and "429" in err
    assert calls == ["sendPhoto"], calls


# ---------- 建分享失败不再阻断发送 ----------
def test_share_failure_does_not_block_send(db):
    b, c, r, q = _seed(db)
    r.share_url = ""  # 尚无分享，触发建分享
    db.commit()
    sent = []

    async def fail_share(*a, **k):
        raise RuntimeError("光鸭未登录，建分享失败")

    async def fake_send(*a, **k):
        sent.append(True)
        return True, "msg", "", None

    with patch.object(publisher, "ensure_share", new=fail_share), \
         patch.object(publisher, "_send_telegram", new=AsyncMock(side_effect=fake_send)):
        result = asyncio.run(publisher.process_queue(limit=10))
    assert result["sent"] == 1, result
    assert sent == [True]
    q2 = db.get(QueueItem, q.id)
    assert q2.status == "sent"
    assert "建分享失败" in (q2.error or "")


# ---------- 路由 Channel 换 Bot 后 bot_id 同步 ----------
def test_route_channel_bot_id_synced_on_bot_change(db):
    old = Bot(name="old", token=encrypt_secret("1:OLD"), enabled=True)
    db.add(old); db.flush()
    ch = Channel(bot_id=old.id, name="route:group:-100G", chat_id="-100G",
                 kind="group", enabled=True)
    db.add(ch); db.flush()
    new = Bot(name="new", token=encrypt_secret("2:NEW"), enabled=True)
    db.add(new); db.commit()
    runtime_config.set_many(db, {"interaction_bot_id": str(new.id)})

    class _P:
        routes = [type("R", (), {"dir_id": "", "channel_id": "", "group_id": "-100G",
                                 "note": "g"})()]
        bot_token = ""
        bot_name = ""

    main.push_routes_save(_P(), db)
    db.refresh(ch)
    assert ch.bot_id == new.id, "路由 Channel 的 bot_id 必须同步到新全局 Bot"


# ---------- 模板三行文字链接（渲染层统一） ----------
def test_build_message_appends_three_jump_links(db):
    _, _, r, _ = _seed(db)
    runtime_config.set_many(db, {
        "tg_channel_link": "@official",
        "tg_group_link": "https://t.me/chatgroup",
        "tg_bot_link": "@push_bot",
    })
    text, _ = publisher._build_message(db, r, "https://share/x")
    assert "📢 官方频道" in text and 'href="https://t.me/official"' in text
    assert "👥 交流群" in text and 'href="https://t.me/chatgroup"' in text
    assert "🤖 机器人" in text and 'href="https://t.me/push_bot"' in text
    # buttons_text 置空：旧的纯文字标签不再出现
    assert "🔍 投稿/搜索" not in text
    # 三行链接在 share_url 之前
    assert text.rstrip().endswith("https://share/x")


def test_build_message_skips_empty_jump_link(db):
    _, _, r, _ = _seed(db)
    runtime_config.set_many(db, {
        "tg_channel_link": "@official",
        "tg_group_link": "",
        "tg_bot_link": "",
    })
    text, _ = publisher._build_message(db, r, "https://share/x")
    assert "📢 官方频道" in text
    assert "👥 交流群" not in text and "🤖 机器人" not in text


def test_inline_keyboard_only_get_resource(db):
    b, c, r, q = _seed(db)
    runtime_config.set_many(db, {"tg_link_style": "button", "tg_channel_link": "@official",
                                 "tg_group_link": "@grp", "tg_bot_link": "@bot"})
    captured = {}

    async def fake_post(self, url, json=None, **kw):
        captured["payload"] = json
        return _Resp()

    with patch("httpx.AsyncClient.post", new=fake_post):
        asyncio.run(publisher._send_telegram(db, q, r, c, b))
    kb = captured["payload"]["reply_markup"]["inline_keyboard"]
    assert len(kb) == 1 and len(kb[0]) == 1
    assert kb[0][0]["text"] == "📥 获取资源"
