"""v1.1 四模块重构主链验收：新机制真正接管，不再是影子模式。

覆盖真机三大反馈的修复 + 设计文档强制规则：
  P0-1 写锁：_persist_english_title 用独立 Session，主 Session 持读快照+网络 await 也不锁；
  P0-2 主链：share_center 默认启用；force/first/update 三类任务都经 建分享→提交→领取→回执；
  P0-3 边界：ShareRecord 真相源（一部剧一个 current）、三队列分类、发布门禁、配额；
  规则1：追更复用原链接并独立重推；
  规则2：B库删除作废分享+事件，A库补齐后 cancelled 任务复活重建；
  事件：mirror_events / resource_events 生产与幂等消费。
"""
import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v110_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'b.db')}")
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng, d
    shutil.rmtree(d, ignore_errors=True)


def _seed_res(db, *, file_id="f1", title="Show.S01E01.mkv", tmdb=1,
              identified=True, poster="/p.jpg", share_url=""):
    from app.models import Resource
    r = Resource(file_id=file_id, name=title, ext=".mkv", size=1, utime=1,
                 identify_status="identified" if identified else "none",
                 media_type="tv", tmdb_id=tmdb, media_title="Show",
                 poster_path=poster, share_url=share_url,
                 share_dir_id=f"dir_{tmdb}", share_scope="series_dir",
                 share_dir_name="Show")
    db.add(r)
    db.commit()
    return r


# ---------- P0-1 写锁 ----------
def test_persist_english_title_independent_session(env, monkeypatch):
    """主 Session 持有过期读快照 + 独立 Session 仍能把英文名写库。"""
    from app import mirror
    S, _, _ = env
    with S() as db:
        from app.models import MirrorMedia
        m = MirrorMedia(rule_id=1, title_standard="新济公活佛 (2014)", media_type="tv")
        db.add(m)
        db.commit()
        mid = m.id
        # 主 Session 制造一个读快照（SELECT 后不 commit）
        db.scalar(select(MirrorMedia).where(MirrorMedia.id == mid))
        # 把 _persist 使用的 SessionLocal 指向本测试库
        monkeypatch.setattr(mirror, "SessionLocal", S)
        mirror._persist_english_title(db, mid, "West Out of Yumen", "ai")
        # 独立 Session 读取确认已落库
    with S() as check:
        got = check.get(MirrorMedia, mid)
        assert got.english_title == "West Out of Yumen"
        assert got.english_source == "ai"


def test_persist_english_title_no_media_id_noop(env):
    from app import mirror
    S, _, _ = env
    with S() as db:
        mirror._persist_english_title(db, 0, "x", "ai")  # 不抛异常


# ---------- P0-2 默认启用 + ShareRecord 真相源 ----------
def test_share_center_default_enabled(env):
    from app import share_center as sc
    S, _, _ = env
    with S() as db:
        assert sc.is_enabled(db) is True


def test_share_record_current_singleton():
    """部分唯一索引：同一 work_key 只允许一条 current=1，历史 current=0 可多条。"""
    from app.db import Base
    from app import models, share_center  # noqa
    eng = create_engine("sqlite:///:memory:")
    # 内存库跨连接不可见，用临时文件
    d = tempfile.mkdtemp(prefix="v110sr_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'r.db')}")
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    try:
        with S() as db:
            r = _seed_res(db)
            rec1 = share_center.upsert_share_record(db, r, share_id="s1", share_url="u1")
            assert rec1.current_flag == 1
            rec2 = share_center.upsert_share_record(db, r, share_id="s2", share_url="u2")
            assert rec2.current_flag == 1
            db.flush()
            assert db.scalar(select(func.count(models.ShareRecord.id)).where(
                models.ShareRecord.current_flag == 1)) == 1  # 旧记录被作废
            assert share_center.current_share(db, share_center.task_work_key(r)).share_url == "u2"
    finally:
        shutil.rmtree(d, ignore_errors=True)


# ---------- P0-3 三队列 + 门禁 + 配额 ----------
def test_queue_classification_and_gate(env):
    from app import share_center as sc
    from app import runtime_config
    S, _, _ = env
    with S() as db:
        r = _seed_res(db)
        wk = sc.task_work_key(r)
        assert sc.classify_queue(db, wk, "first") == "fresh"
        sc.upsert_share_record(db, r, share_id="s", share_url="u")
        assert sc.classify_queue(db, wk, "update") == "followup"
        # 门禁：无海报阻塞 first，force 放行
        t1, _ = sc.ensure_task(db, r, kind="first", channel_targets=[{"channel_id": 1}])
        r.poster_path = ""
        db.commit()
        ok, reason = sc.can_publish(db, t1)
        assert not ok and "海报" in reason
        tf, _ = sc.ensure_task(db, r, kind="force", channel_targets=[{"channel_id": 1}])
        assert sc.can_publish(db, tf)[0] is True


def test_publish_quota_gate(env):
    """配额门禁：fresh 队列当日上限 1，已有 1 条 submitted → 后续阻塞。"""
    from app import share_center as sc, runtime_config
    S, _, _ = env
    with S() as db:
        r = _seed_res(db)
        runtime_config.set_many(db, {"fresh_daily_quota": "1"})
        db.commit()
        t1, _ = sc.ensure_task(db, r, kind="first", channel_targets=[{"channel_id": 1}])
        t1.status = sc.ST_SUBMITTED
        t1.submitted_at = sc.utcnow()
        db.commit()
        ok, reason = sc.can_publish(db, t1)
        assert not ok and "配额" in reason


# ---------- 规则1 追更复用链接 + 独立事件 ----------
def test_update_reuses_share_url_independent_task(env):
    from app import share_center as sc
    S, _, _ = env
    with S() as db:
        r = _seed_res(db, share_url="https://share.test/original")
        first, _ = sc.ensure_task(db, r, kind="first", channel_targets=[{"channel_id": 1}])
        first.share_url = "https://share.test/original"
        first.status = sc.ST_PUBLISHED
        db.commit()
        sc.upsert_share_record(db, r, share_id="sid", share_url="https://share.test/original")
        upd, created = sc.ensure_task(db, r, kind="update", event_id=42,
                                      channel_targets=[{"channel_id": 1}])
        assert created and upd.id != first.id
        assert upd.share_url == first.share_url  # 复用原链接
        assert upd.parent_task_id == first.id
        assert upd.parent_event_id == 42


# ---------- 规则2 B库删除闭环 ----------
def test_reconcile_delete_retires_share_and_emit_event(env):
    from app import main, share_center as sc, events
    from app.models import DistributionEvent
    S, _, _ = env
    with S() as db:
        r = _seed_res(db, share_url="https://share.test/x")
        sc.upsert_share_record(db, r, share_id="sid", share_url="https://share.test/x")
        first, _ = sc.ensure_task(db, r, kind="first", channel_targets=[{"channel_id": 1}])
        first.status = sc.ST_PUBLISHED
        db.commit()
        wk = sc.task_work_key(r)
        # 模拟 B 库对账：seen 为空在该文件（file_id 不在 seen）→ stale，触发删除
        with patch("app.guangya.GuangYaShareClient.delete", new=AsyncMock()):
            # seen 含其它文件但不含本资源 → 本资源成为 stale（且满足非空门槛）
            out = asyncio.run(main._reconcile_library(db, ["other_fid"], cancel_shares=False))
        assert out["removed"] == 1
        assert sc.current_share(db, wk) is None  # 当前分享作废
        ev = db.scalar(select(events.ResourceEvent).where(
            events.ResourceEvent.work_key == wk,
            events.ResourceEvent.type == events.RESOURCE_REMOVED))
        assert ev is not None  # 出库事件已落库


def test_restore_revives_cancelled_first(env):
    """A库补齐后：cancelled 的 first 任务被复活重建，不再被 dedup_key 卡死。"""
    from app import share_center as sc
    S, _, _ = env
    with S() as db:
        r = _seed_res(db)
        wk = sc.task_work_key(r)
        first, _ = sc.ensure_task(db, r, kind="first", channel_targets=[{"channel_id": 1}])
        sc.cancel_task(db, first)  # 模拟 B库删除取消
        assert first.status == sc.ST_CANCELLED
        revived, created = sc.ensure_task(db, r, kind="first", channel_targets=[{"channel_id": 1}])
        assert created and revived.id == first.id  # 复用同一行复活
        assert revived.status == sc.ST_PLANNED
        assert not revived.share_url


# ---------- 事件幂等 ----------
def test_resource_event_emit_idempotent(env):
    from app import events
    S, _, _ = env
    with S() as db:
        e1 = events.emit_resource_event(db, event_type=events.RESOURCE_NEW,
                                        work_key="wk", resource_id=1)
        e2 = events.emit_resource_event(db, event_type=events.RESOURCE_NEW,
                                        work_key="wk", resource_id=1)
        assert e1 is not None and e2 is not None and e1.id == e2.id  # 幂等，不重复落
        assert len(events.pending_resource_events(db)) == 1
        events.mark_consumed(db, e1)
        assert len(events.pending_resource_events(db)) == 0
