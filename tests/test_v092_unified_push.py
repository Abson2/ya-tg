"""v0.9.2 回归：频道与群聊推送统一——都由 Bot 发到 Channel.chat_id。

锁死红线：
  - _send_telegram 只用该 Channel 所属 Bot 的 token，目标恒为 channel.chat_id；
    不再有 custom_bot_token / custom_chat_id 全局覆盖分支。
  - 频道(chat_id=-100xxx)与群聊(chat_id=-100yyy)走完全相同的发送路径。
  - 白名单闸门对频道和群聊一视同仁（都按 channel.chat_id 过滤）。
"""
import asyncio
from unittest.mock import patch

import pytest

from app import publisher, runtime_config
from app.db import SessionLocal, migrate
from app.models import Bot, Channel, QueueItem, Resource
from app.security import encrypt_secret
from app.utils import utcnow

TABLES = [QueueItem, Resource, Channel, Bot]


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        for k in ("tg_push_mode", "tg_custom_bot_token", "tg_custom_chat_id",
                  "bot_gate_mode", "bot_push_channels_json", "bot_push_groups_json"):
            row = db.get(__import__("app.models", fromlist=["AppConfig"]).AppConfig, k)
            if row:
                db.delete(row)
        db.commit()
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


class _Resp:
    status_code = 200
    text = ""
    def json(self):
        return {"ok": True, "result": {"message_id": "1"}}


@pytest.mark.parametrize("chat_id,label", [("-1001234567890", "频道"), ("-987654321", "群聊")])
def test_send_uses_bot_token_and_channel_chat_id(db, chat_id, label):
    """群聊与频道发送路径完全一致：URL 用 Bot token，payload.chat_id=该 Channel。"""
    b = Bot(name="b", token=encrypt_secret("111:TOKEN_A"), enabled=True)
    db.add(b); db.flush()
    c = Channel(name=label, bot_id=b.id, chat_id=chat_id, enabled=True)
    db.add(c); db.flush()
    r = Resource(file_id="v092-x", name="Movie.2024.mkv", ext=".mkv", size=1, utime=1,
                 identify_status="identified", media_type="movie", media_title="Movie")
    db.add(r); db.flush()
    q = QueueItem(resource_id=r.id, channel_id=c.id, status="pending")
    db.add(q); db.commit()

    captured = {}

    async def fake_post(self, url, json=None, **kw):
        captured["url"] = url
        captured["payload"] = json
        return _Resp()

    with patch("httpx.AsyncClient.post", new=fake_post):
        ok, _, err, _ = asyncio.run(publisher._send_telegram(db, q, r, c, b))
    assert ok is True and err == ""
    assert captured["url"] == f"{runtime_config.telegram_base(db)}/bot111:TOKEN_A/sendMessage"
    assert captured["payload"]["chat_id"] == chat_id


def test_custom_bot_config_is_ignored_even_if_set(db):
    """即便存量仍留着 custom_bot_*，也不得覆盖 Channel 的 bot/chat。"""
    runtime_config.set_many(db, {"tg_push_mode": "custom_bot_group",
                                 "tg_custom_bot_token": "999:OVERRIDE",
                                 "tg_custom_chat_id": "-000000000"})
    b = Bot(name="b", token=encrypt_secret("111:TOKEN_A"), enabled=True)
    db.add(b); db.flush()
    c = Channel(name="群", bot_id=b.id, chat_id="-555", enabled=True)
    db.add(c); db.flush()
    r = Resource(file_id="v092-y", name="M.mkv", ext=".mkv", size=1, utime=1)
    db.add(r); db.flush()
    q = QueueItem(resource_id=r.id, channel_id=c.id, status="pending")
    db.add(q); db.commit()

    captured = {}

    async def fake_post(self, url, json=None, **kw):
        captured["url"] = url
        captured["chat"] = json["chat_id"]
        return _Resp()

    with patch("httpx.AsyncClient.post", new=fake_post):
        asyncio.run(publisher._send_telegram(db, q, r, c, b))
    assert "111:TOKEN_A" in captured["url"]      # 用 Channel 所属 Bot
    assert "999:OVERRIDE" not in captured["url"]  # 不用全局覆盖
    assert captured["chat"] == "-555"            # 发到 Channel.chat_id


def test_process_queue_has_no_custom_override(db):
    """process_queue 源码级保证：不再读取 tg_custom_* / 不再传 override 参数。"""
    import inspect
    src = inspect.getsource(publisher.process_queue)
    assert "tg_custom_bot_token" not in src
    assert "tg_custom_chat_id" not in src
    assert "override_token" not in src

    src2 = inspect.getsource(publisher._send_telegram)
    assert "override_token" not in src2
    assert "override_chat" not in src2
