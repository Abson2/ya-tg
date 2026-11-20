import asyncio
from unittest.mock import patch

from sqlalchemy import func, select

from app.db import SessionLocal, migrate
from app.main import _enqueue_after_identify
from app.models import AuthToken, Bot, Channel, QueueItem, Resource, Rule
from app.publisher import process_queue
from app.scanner import run_scan
from app.security import encrypt_secret


class FakeResponse:
    status_code = 200

    def json(self):
        return {"ok": True, "result": {"message_id": 99}}


def test_scan_to_share_to_push_flow():
    migrate()
    db = SessionLocal()
    db.add(AuthToken(id=1, client_id="test-client",
                     access_token_enc=encrypt_secret("fake-access"),
                     refresh_token_enc=encrypt_secret("fake-refresh"),
                     expires_at=None, flow="test"))
    # v1.1：建分享走网页端 client，给它一个占位授权让 __init__ 通过；
    # create 方法由测试 patch 提供假响应。扫描仍走 OpenAPI（flow=test → WebFiles 未启用）。
    from app import runtime_config
    runtime_config.set_many(db, {"guangya_share_authorization": "Bearer test-share-token"})
    db.flush()
    bot = Bot(name="test-bot", token=encrypt_secret("123456:AAFakeToken"))
    db.add(bot)
    db.flush()
    channel = Channel(bot_id=bot.id, name="test-channel", chat_id="-100123")
    db.add(channel)
    db.flush()
    db.add(Rule(name="all", channel_id=channel.id, auto_publish=True))
    db.commit()
    db.close()

    async def fake_list(self, parent_id="", page=0, page_size=100):
        if parent_id == "":
            return {"list": [
                {"fileId": "dir1", "fileName": "电影", "resType": 2},
                {"fileId": "f1", "fileName": "Test.Movie.2024.1080p.mkv", "resType": 1,
                 "fileSize": 100, "ext": ".mkv", "utime": 1},
            ], "total": 2}
        if parent_id == "dir1":
            return {"list": [
                {"fileId": "f2", "fileName": "Test.Show.S01E02.mkv", "resType": 1,
                 "fileSize": 200, "ext": ".mkv", "utime": 2},
            ], "total": 1}
        return {"list": [], "total": 0}

    async def fake_create(self, file_id, title, duration):
        return {"data": {"shareId": "s" + file_id, "shareUrl": "https://share.test/" + file_id}}

    async def fake_post(self, url, **kwargs):
        return FakeResponse()

    with patch("app.guangya.GuangYaOpenAPI.list_files", fake_list), \
         patch("app.guangya.GuangYaShareClient.create", fake_create), \
         patch("httpx.AsyncClient.post", fake_post):
        scan = asyncio.run(run_scan(""))
        assert scan["files"] == 2
        assert scan["directories"] == 2
        # v0.6.5：扫描只入库，不产生队列项
        db = SessionLocal()
        assert db.query(QueueItem).count() == 0
        db.close()
        # 模拟识别成功（带 TMDB 海报）后才入队
        db = SessionLocal()
        for r in db.scalars(select(Resource)).all():
            r.identify_status = "identified"
            r.media_type = "movie"
            r.media_title = r.name.split(".")[0]
            r.poster_path = "/poster.jpg"
        db.commit()
        queued = 0
        for r in db.scalars(select(Resource)).all():
            queued += _enqueue_after_identify(db, r)
        db.commit()
        db.close()
        assert queued == 2
        result = asyncio.run(process_queue())
        assert result["sent"] == 2
        # v1.1 新链：建分享在分享中心 run_cycle 完成（process_queue 内部已驱动一轮），
        # publisher 的 shares_created 只统计旧链兜底路径，新链下可为 0。验证真相源即可。
        from app.models import ShareTask, ShareRecord
        db2 = SessionLocal()
        try:
            assert db2.scalar(select(func.count(ShareTask.id))) == 2
            assert db2.scalar(select(func.count(ShareRecord.id))) == 2
            assert all((t.share_url or "").startswith("https://share.test/")
                       for t in db2.scalars(select(ShareTask)).all())
        finally:
            db2.close()

    db = SessionLocal()
    try:
        assert db.scalar(select(func.count(Resource.id))) == 2
        assert all(r.share_url for r in db.scalars(select(Resource)).all())
        items = db.scalars(select(QueueItem)).all()
        assert len(items) == 2
        assert all(q.status == "sent" and q.telegram_message_id == "99" for q in items)
    finally:
        db.close()


def test_retry_backoff_on_telegram_failure():
    db = SessionLocal()
    try:
        q = db.scalar(select(QueueItem).limit(1))
        assert q is not None and q.status == "sent"
        q.status, q.attempts, q.next_retry_at = "retry", 1, None
        db.commit()
    finally:
        db.close()

    class FailResponse:
        status_code = 429

        def json(self):
            return {"ok": False, "description": "Too Many Requests",
                    "parameters": {"retry_after": 7}}

    async def fake_post(self, url, **kwargs):
        return FailResponse()

    with patch("httpx.AsyncClient.post", fake_post):
        result = asyncio.run(process_queue())
    assert result["sent"] == 0
    assert result["failed"] >= 1

    db = SessionLocal()
    try:
        q = db.scalar(select(QueueItem).order_by(QueueItem.id).limit(1))
        assert q.status == "retry"
        assert q.next_retry_at is not None
        assert "Too Many Requests" in q.error
    finally:
        db.close()
