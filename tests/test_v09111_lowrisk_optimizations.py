"""v0.9.11.1：两项纯内部性能优化的回归锁。

1. 扫描 scanned_at 写放大：未变文件不再写库（scanned_at 仅新建/指纹变化时更新），
   变化文件仍更新 scanned_at + revision + 变更事件，thumbnail 等字段照常刷新。
2. 队列去重 O(N·M)→预构建映射：语义必须与逐条查库完全等价——
   a) 历史已 sent 的同作品项被跳过（老测试已覆盖，这里再锁）；
   b) 同一轮内刚发出的项对后续同作品单集可见，不得重复推送（防回归关键）。
"""
import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from app import publisher as pub
from app.db import SessionLocal, migrate
from app.models import Bot, Channel, QueueItem, Resource, AppConfig
from app.scanner import _upsert_resource
from app.security import encrypt_secret

TABLES = (QueueItem, Resource, Channel, Bot)


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as s:
        _clean(s)
        for k in ("tg_channel_link", "tg_group_link", "tg_bot_link"):
            row = s.get(AppConfig, k)
            if row:
                s.delete(row)
        s.commit()


def _clean(db):
    for m in TABLES:
        db.query(m).delete(synchronize_session=False)
    db.commit()


def _item(fid="f-1", size=100, utime=1000, thumb="http://t/1.jpg"):
    return {"fileId": fid, "fileName": "Show.S01E01.mkv", "ext": ".mkv",
            "fileSize": size, "utime": utime, "ctime": 900,
            "fileType": 1, "thumbnail": thumb, "parentId": "dir-1"}


def test_scanned_at_unchanged_when_fingerprint_same():
    """未变文件二次扫描：scanned_at / revision 不变，且无字段被标记 dirty。"""
    migrate()
    with SessionLocal() as db:
        _clean(db)
        row, created, change = _upsert_resource(db, _item(), "dir-1")
        db.commit()
        assert created is True
        first_at = row.scanned_at
        first_rev = int(row.revision or 0)

        # 完全相同的条目再扫一次
        row2, created2, change2 = _upsert_resource(db, _item(), "dir-1")
        db.commit()
        assert created2 is False
        assert change2 is None
        assert row2.scanned_at == first_at, "未变文件不应重写 scanned_at"
        assert int(row2.revision or 0) == first_rev
        _clean(db)


def test_scanned_at_updates_when_fingerprint_changes():
    """指纹变化：scanned_at 前进、revision+1、thumbnail 照常刷新、返回变更事件。"""
    migrate()
    with SessionLocal() as db:
        _clean(db)
        row, _, _ = _upsert_resource(db, _item(), "dir-1")
        db.commit()
        first_at = row.scanned_at
        first_rev = int(row.revision or 0)

        changed = _item(size=200, utime=2000, thumb="http://t/2.jpg")
        row2, created2, change2 = _upsert_resource(db, changed, "dir-1")
        db.commit()
        assert created2 is False
        # v2.1.25：变化指纹只看 size（utime 单变不算更新，见下方反证用例）。
        assert change2 is not None and change2["new"] == "200"
        assert row2.scanned_at >= first_at
        assert int(row2.revision or 0) == first_rev + 1
        assert row2.thumbnail == "http://t/2.jpg"
        _clean(db)


def _setup_bot_channel(db):
    b = Bot(name="opt-bot", token=encrypt_secret("1:FAKE"), enabled=True)
    db.add(b); db.flush()
    c = Channel(name="opt-ch", bot_id=b.id, chat_id="-100OPT", enabled=True)
    db.add(c); db.flush()
    return c.id


def _queue_res(fid, share_dir_id):
    return Resource(file_id=fid, name=fid + ".mkv", ext=".mkv", size=1, utime=1,
                    media_type="tv", media_title="剧", share_dir_id=share_dir_id,
                    share_url="https://share/x", poster_path="/p.jpg")


def test_dedup_skips_when_history_sent():
    """历史已 sent 的同作品项：预构建映射命中，pending 被标 sent 且不发送。"""
    with SessionLocal() as db:
        cid = _setup_bot_channel(db)
        r1 = _queue_res("opt-h1", "dir-h")
        r2 = _queue_res("opt-h2", "dir-h")
        db.add_all([r1, r2]); db.flush()
        db.add(QueueItem(resource_id=r1.id, channel_id=cid, status="sent",
                         telegram_message_id="old"))
        db.add(QueueItem(resource_id=r2.id, channel_id=cid, status="pending"))
        db.commit()
        rid2 = r2.id

    with patch.object(pub, "ensure_share", new=AsyncMock(return_value="https://share/x")), \
         patch.object(pub, "_send_telegram", new=AsyncMock(return_value=(True, "m", "", None))):
        result = asyncio.run(pub.process_queue(limit=20))
    assert result["sent"] == 0
    with SessionLocal() as db:
        q2 = db.scalar(select(QueueItem).where(QueueItem.resource_id == rid2,
                                              QueueItem.channel_id == cid))
        assert q2.status == "sent" and "去重" in (q2.error or "")


def test_dedup_sees_item_sent_in_same_round():
    """关键防回归：同一轮内先发出的项，必须让后续同作品单集命中去重。

    若映射不含本轮写回，两条同作品 pending 会在同一轮各发一次（重复推送）。
    """
    with SessionLocal() as db:
        cid = _setup_bot_channel(db)
        r1 = _queue_res("opt-r1", "dir-same")
        r2 = _queue_res("opt-r2", "dir-same")
        db.add_all([r1, r2]); db.flush()
        db.add(QueueItem(resource_id=r1.id, channel_id=cid, status="pending"))
        db.add(QueueItem(resource_id=r2.id, channel_id=cid, status="pending"))
        db.commit()
        id1, id2 = r1.id, r2.id

    async def fake_send(*a, **k):
        return True, "msg", "", None

    with patch.object(pub, "ensure_share", new=AsyncMock(return_value="https://share/x")), \
         patch.object(pub, "_send_telegram", new=AsyncMock(side_effect=fake_send)) as send_mock:
        result = asyncio.run(pub.process_queue(limit=20))

    # 两条同作品：只应真正发出一条，另一条被本轮去重跳过
    assert send_mock.await_count == 1, f"同作品同轮被发送 {send_mock.await_count} 次（重复推送）"
    assert result["sent"] == 1
    with SessionLocal() as db:
        q1 = db.scalar(select(QueueItem).where(QueueItem.resource_id == id1,
                                              QueueItem.channel_id == cid))
        q2 = db.scalar(select(QueueItem).where(QueueItem.resource_id == id2,
                                              QueueItem.channel_id == cid))
        sent_errors = [q.error or "" for q in (q1, q2) if q.status == "sent"]
        assert any("去重" in e for e in sent_errors), (q1.status, q1.error, q2.status, q2.error)
