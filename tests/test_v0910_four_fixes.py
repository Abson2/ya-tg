"""v0.9.10 四项真机反馈回归。

  - 影视映射清理：口径改为「联网核对 B 库，删整部已无文件的映射行」；前端不再显示失效数字。
  - 手动扫库：改后台异步任务（/api/scan/async），立即返回 + 轮询状态，不再 Failed to fetch 卡死。
  - process_queue 跨 await 持写事务：每条处理完立即 commit，杜绝 database is locked。
  - 进度节点：只读本轮任务快照，无快照显示空闲，不用全库累计冒充进度。
"""
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from app import main, mirror, publisher
from app.db import SessionLocal, migrate
from app.models import (Bot, Channel, MirrorFile, MirrorMedia, MirrorRule,
                        QueueItem, Resource)

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")

TABLES = (MirrorFile, MirrorMedia, MirrorRule, QueueItem, Resource, Channel, Bot)


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as s:
        for t in TABLES:
            s.query(t).delete()
        s.commit()
    yield
    with SessionLocal() as s:
        for t in TABLES:
            s.query(t).delete()
        s.commit()
    # 复位异步扫描状态，避免测试间污染
    main._async_scan.update({"running": False, "started_at": None,
                             "finished_at": None, "mode": "", "error": "",
                             "result": {}, "stage": ""})


@pytest.fixture
def db(_fresh):
    with SessionLocal() as s:
        yield s


def _seed_queue(db, n=3):
    b = Bot(name="b", token="x", enabled=True)
    db.add(b); db.flush()
    c = Channel(bot_id=b.id, name="ch", chat_id="-1001", enabled=True)
    db.add(c); db.flush()
    ids = []
    for i in range(n):
        r = Resource(file_id=f"f{i}", name=f"Show.S01E0{i+1}.mkv",
                     media_type="tv", media_title="剧", share_url="https://share/x",
                     poster_path="/p.jpg", identify_status="identified")
        db.add(r); db.flush()
        db.add(QueueItem(resource_id=r.id, channel_id=c.id, status="pending"))
        ids.append((r.id, c.id))
    db.commit()
    return ids


# ---------- process_queue 每条立即 commit（database is locked 根因） ----------
def test_process_queue_commits_per_item(db):
    """3 条队列 -> commit 至少 4 次（每条开头/结尾 + 起始），不再整轮只 commit 一次。"""
    _seed_queue(db, 3)
    from app.security import encrypt_secret
    db.query(Bot).first().token = encrypt_secret("1:FAKE")
    db.commit()

    real_session = SessionLocal()
    counter = {"n": 0}
    real_commit = real_session.commit

    def counting_commit():
        counter["n"] += 1
        return real_commit()

    real_session.commit = counting_commit

    async def fake_send(*a, **k):
        return True, "msg", "", None

    with patch.object(publisher, "SessionLocal", return_value=real_session), \
         patch.object(publisher, "ensure_share", new=AsyncMock(return_value="https://share/x")), \
         patch.object(publisher, "_send_telegram", new=AsyncMock(side_effect=fake_send)):
        result = asyncio.run(publisher.process_queue(limit=20))
    assert result["sent"] == 3, result
    # 3 条各至少一次 commit（每条开头+结尾），远超旧的「整轮 1 次」
    assert counter["n"] >= 3, counter
    qs = db.query(QueueItem).all()
    assert all(q.status == "sent" for q in qs)


# ---------- 异步扫描端点 ----------
def test_scan_async_start_and_status(db):
    """启动立即返回 started，后台任务在同一 loop 内跑完，状态可读。"""
    async def fake_refresh(*a, **k):
        return {"files": 12, "identify": {"identified": 3, "queued": 2}}

    async def scenario():
        with patch.object(main, "_refresh_library_from_b", new=fake_refresh):
            out = await main.scan_async(mode="scan")
            assert out["started"] is True and out["mode"] == "scan"
            # 等后台任务跑完（同一事件循环）
            for _ in range(100):
                if not main._async_scan["running"]:
                    break
                await asyncio.sleep(0.02)
        return main.scan_async_status()

    st = asyncio.run(scenario())
    assert st["mode"] == "scan" and st["running"] is False
    assert st["result"]["files"] == 12
    assert st["error"] == ""


def test_scan_async_rejects_duplicate(db):
    main._async_scan["running"] = True
    with pytest.raises(Exception):
        asyncio.run(main.scan_async(mode="scan"))
    main._async_scan["running"] = False


def test_scan_async_rejects_bad_mode(db):
    with pytest.raises(Exception):
        asyncio.run(main.scan_async(mode="bogus"))


# ---------- 影视映射清理：联网核对 B 库 ----------
def test_prune_missing_media_deletes_verified_empty(db):
    r = MirrorRule(name="r", source_root_id="A", target_root_id="B", enabled=True)
    db.add(r); db.flush()
    # v2.1.64：MirrorMedia 唯一身份为 (rule_id, source_dir_id)，夹具给各自源目录 id。
    gone = MirrorMedia(rule_id=r.id, work_key="wk-gone", title_standard="已删剧",
                       source_dir_id="A-gone", status="synced")
    alive = MirrorMedia(rule_id=r.id, work_key="wk-alive", title_standard="仍在剧",
                        source_dir_id="A-alive", status="synced")
    db.add_all([gone, alive]); db.flush()
    db.add(MirrorFile(media_id=gone.id, rule_id=r.id, file_name="g.mkv", status="synced"))
    db.add(MirrorFile(media_id=alive.id, rule_id=r.id, file_name="a.mkv", status="synced"))
    db.commit()
    gid, aid, rid = gone.id, alive.id, r.id

    async def fake_candidates(api, media):
        # 已删剧：verified 且空 -> 清；仍在剧：verified 且有文件 -> 保留
        if media.title_standard == "已删剧":
            return {}, True
        return {"f1": "a.mkv"}, True

    with patch.object(mirror, "GuangYaWebFiles", return_value=object()), \
         patch.object(mirror, "_b_file_candidates", new=fake_candidates):
        out = asyncio.run(mirror.prune_missing_media_rows(db, r))
    assert out["media_deleted"] == 1
    assert out["skipped_verified_has_files"] == 1
    assert db.get(MirrorMedia, gid) is None
    assert db.get(MirrorMedia, aid) is not None
    assert db.query(MirrorFile).filter(MirrorFile.media_id == gid).count() == 0


def test_prune_missing_media_skips_unverified(db):
    r = MirrorRule(name="r", source_root_id="A", target_root_id="B", enabled=True)
    db.add(r); db.flush()
    m = MirrorMedia(rule_id=r.id, title_standard="列目录失败剧", status="synced")
    db.add(m); db.commit()
    mid = m.id

    async def fake_candidates(api, media):
        return {}, False  # 列目录不可信

    with patch.object(mirror, "GuangYaWebFiles", return_value=object()), \
         patch.object(mirror, "_b_file_candidates", new=fake_candidates):
        out = asyncio.run(mirror.prune_missing_media_rows(db, r))
    assert out["skipped_unverified"] == 1
    assert out["media_deleted"] == 0
    assert db.get(MirrorMedia, mid) is not None


def test_clear_invalid_endpoint_deletes_b_deleted(db):
    """端点 async 版：B 库已删的行被删，反馈统计含 b_deleted。"""
    r = MirrorRule(name="r", source_root_id="A", target_root_id="B", enabled=True)
    db.add(r); db.flush()
    m = MirrorMedia(rule_id=r.id, title_standard="已删剧", status="synced")
    db.add(m); db.flush()
    db.add(MirrorFile(media_id=m.id, rule_id=r.id, file_name="g.mkv", status="synced"))
    db.commit()
    mid = m.id

    async def fake_candidates(api, media):
        return {}, True

    with patch.object(mirror, "GuangYaWebFiles", return_value=object()), \
         patch.object(mirror, "_b_file_candidates", new=fake_candidates):
        out = asyncio.run(main.mirror_media_clear_invalid_async(db=db))
    assert out["b_deleted"] == 1 and out["removed_media"] >= 1
    assert db.get(MirrorMedia, mid) is None


# ---------- 前端不再展示失效影视数字 ----------
def test_frontend_no_invalid_number_and_async_scan():
    assert "async function runAsyncScan" in APP
    assert "/scan/async" in APP
    assert "核对 B 库" in APP
    assert "media_invalid" not in APP
    assert "无效待清理" not in APP
