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


# ---------- v0.9.10.1：正文三个标签式超链接；inline 按钮只保留「获取资源」 ----------
def test_build_message_has_jump_tag_links_in_body(db):
    _, _, r, _ = _seed(db)
    runtime_config.set_many(db, {
        "tg_channel_link": "@official",
        "tg_group_link": "https://t.me/chatgroup",
        "tg_bot_link": "@push_bot",
    })
    text, _ = publisher._build_message(db, r, "https://share/x")
    # 三个入口改为正文 <a href> 标签式超链接，同一行用 · 分隔
    assert '<a href="https://t.me/official">频道</a>' in text
    assert '<a href="https://t.me/chatgroup">群聊</a>' in text
    assert '<a href="https://t.me/push_bot">投稿·求片</a>' in text
    assert " · " in text
    # 旧 inline 按钮文案不再出现在正文
    assert "📢 官方频道" not in text and "🤖 机器人" not in text
    # v2.1.23（用户口径 6）：这一行行首必须带一个符号，与上方「emoji + 空格 + 字段」
    # 的行格式对齐；此前它从「频道」直接开头，视觉上是断层的。
    line = next(l for l in text.split("\n") if "投稿·求片</a>" in l)
    assert line.startswith("⚡ "), line
    assert line.count("⚡") == 1, line      # 只加行首一个，不逐个标签加


def test_inline_keyboard_only_resource_button(db):
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
    # inline 键盘只剩一行「获取资源」，三个跳转不再占按钮位
    assert len(kb) == 1
    assert kb[0][0]["text"] == "📥 获取资源"
    # 三个跳转以正文标签形式出现，不在 reply_markup 里
    assert all("频道" not in btn["text"] and "群聊" not in btn["text"]
               and "投稿" not in btn["text"] for row in kb for btn in row)
    assert '<a href="https://t.me/official">频道</a>' in captured["payload"]["caption"]


def test_jump_tag_links_skip_empty_link(db):
    b, c, r, q = _seed(db)
    runtime_config.set_many(db, {"tg_link_style": "button", "tg_channel_link": "@official",
                                 "tg_group_link": "", "tg_bot_link": ""})
    captured = {}

    async def fake_post(self, url, json=None, **kw):
        captured["payload"] = json
        return _Resp()

    with patch("httpx.AsyncClient.post", new=fake_post):
        asyncio.run(publisher._send_telegram(db, q, r, c, b))
    caption = captured["payload"].get("caption") or captured["payload"].get("text")
    # 只出现配了的频道标签，留空的两项不生成
    assert '<a href="https://t.me/official">频道</a>' in caption
    assert ">群聊</a>" not in caption and ">投稿·求片</a>" not in caption
    kb = captured["payload"]["reply_markup"]["inline_keyboard"]
    assert len(kb) == 1 and kb[0][0]["text"] == "📥 获取资源"
