"""阶段五 #6：Bot 429 退避重试 / 403-401 永久错误直接失败。

永久错误（Token 失效、Bot 被移出群、无权限）重试 MAX_ATTEMPTS 次只会浪费配额、
让队列看似「在重试」实则永不成功。分级后状态只落在本条 QueueItem（目标独立），
不影响其它目标。发送历史已有 AuditLog push_sent + QueueItem.telegram_message_id。
"""
import asyncio
from unittest.mock import AsyncMock, patch

from sqlalchemy import select

from app import publisher as pub
from app.db import SessionLocal, migrate
from app.models import Bot, Channel, QueueItem, Resource
from app.security import encrypt_secret

migrate()


def _res(name):
    return Resource(file_id=name, name=name, path_hint="/x", size=1,
                    media_type="tv", media_title="剧", share_url="https://share/x")


def _setup():
    with SessionLocal() as db:
        b = Bot(name="c-bot", token=encrypt_secret("1:FAKE"), enabled=True)
        db.add(b); db.flush()
        c = Channel(name="c-ch", bot_id=b.id, chat_id="-1009", enabled=True)
        db.add(c); db.flush()
        r = _res("c-res")
        db.add(r); db.flush()
        q = QueueItem(resource_id=r.id, channel_id=c.id, status="pending")
        db.add(q); db.commit()
        return q.id, c.id, r.id


def _cleanup(cid, rid):
    with SessionLocal() as db:
        for q in db.scalars(select(QueueItem).where(QueueItem.channel_id == cid)).all():
            db.delete(q)
        for r in db.scalars(select(Resource).where(Resource.id == rid)).all():
            db.delete(r)
        for c in db.scalars(select(Channel).where(Channel.id == cid)).all():
            db.delete(c)
        for b in db.scalars(select(Bot).where(Bot.name == "c-bot")).all():
            db.delete(b)
        db.commit()


def test_403_permament_error_fails_without_retry():
    qid, cid, rid = _setup()
    async def fail403(*a, **k):
        return False, "", "Telegram sendMessage 失败 HTTP 403：Forbidden: bot was kicked", 401

    with patch.object(pub, "ensure_share", new=AsyncMock(return_value="https://share/x")), \
         patch.object(pub, "_send_telegram", new=AsyncMock(side_effect=fail403)):
        asyncio.run(pub.process_queue(limit=20))
    with SessionLocal() as db:
        q = db.get(QueueItem, qid)
        assert q.status == "failed", q.status
        assert q.next_retry_at is None, "永久错误不应设重试时间"
        assert "永久错误" in (q.error or "")
    _cleanup(cid, rid)


def test_429_rate_limit_still_retries():
    qid, cid, rid = _setup()
    async def fail429(*a, **k):
        return False, "", "Telegram 429 Too Many Requests", 30

    with patch.object(pub, "ensure_share", new=AsyncMock(return_value="https://share/x")), \
         patch.object(pub, "_send_telegram", new=AsyncMock(side_effect=fail429)):
        asyncio.run(pub.process_queue(limit=20))
    with SessionLocal() as db:
        q = db.get(QueueItem, qid)
        assert q.status == "retry", q.status
        assert q.next_retry_at is not None, "限流错误应退避重试"
    _cleanup(cid, rid)
