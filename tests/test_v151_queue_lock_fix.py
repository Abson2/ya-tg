"""P0 第六次死锁根治验收：queue_worker 的 run_cycle 漏 to_thread + PendingRollbackError。

真机 2026-11-21：process_queue 每 60s 在主协程 `await run_cycle(db)`，内部
build_share/submit_to_dispatch 的同步 commit 撞写锁→主线程等 30s→冻结 event loop→
_queue_lock 永不释放→队列停滞但容器 healthy。日志伴随 PendingRollbackError
（异常后未 rollback 就再次 commit）。

覆盖：
  1. publisher 的 process_queue 路径把事件消费+run_cycle 走 to_thread+独立 Session；
  2. run_cycle 内建分享抛异常（Session 进入 failed 态）后先 rollback，
     不拖垮整批、后续任务仍被处理。
"""
import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import PendingRollbackError

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v151_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'c.db')}")
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _make_res(db, fid, name, share_dir_id, media_title):
    from app.models import Resource
    r = Resource(file_id=fid, name=name, ext=".mkv", size=1, utime=1,
                 identify_status="identified", media_type="tv", tmdb_id=1,
                 media_title=media_title, path_hint=media_title,
                 share_dir_id=share_dir_id, share_scope="series_dir",
                 share_dir_name=media_title)
    db.add(r)
    db.commit()
    return r


# ---------- 1. 静态守卫：queue_worker 路径不再在主协程跑 run_cycle ----------

def test_publisher_run_cycle_is_offloaded_to_thread():
    src = (ROOT / "backend/app/publisher.py").read_text(encoding="utf-8")
    # 主协程里不允许再出现直接 await _sc.run_cycle(db, ...)（第六次死锁根因）
    assert "await _sc.run_cycle(db" not in src
    assert "await _sc.consume_resource_events(db" not in src
    # 必须走 to_thread + 独立 Session 的专用 helper
    assert "asyncio.to_thread(_run_share_center_in_thread" in src
    assert "def _run_share_center_in_thread" in src
    # helper 内线程自建 Session、asyncio.run 驱动、异常 rollback
    helper = src[src.index("def _run_share_center_in_thread"):
                 src.index("async def _process_queue_impl")]
    assert "SessionLocal()" in helper
    assert "asyncio.run(_sc.run_cycle" in helper
    assert "consume_resource_events" in helper
    assert "rollback()" in helper


def test_main_job_share_center_still_offloaded():
    """回归：main.py 的 _job_share_center 仍保留 v1.4.2 的 to_thread 保护。"""
    src = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
    assert "await asyncio.to_thread(_run_share_cycle_in_thread)" in src
    assert "await asyncio.to_thread(_consume_resource_events_in_thread)" in src


# ---------- 2. 行为：建分享异常后先 rollback，不拖垮整批 ----------

def test_run_cycle_survives_build_share_db_error(env, monkeypatch):
    """build_share 抛 DB 异常→Session failed→必须先 rollback 才能继续处理下一条。"""
    from app import share_center as sc
    S, _ = env
    with S() as db:
        r1 = _make_res(db, "f1", "Show1", "dir1", "Show1")
        r2 = _make_res(db, "f2", "Show2", "dir2", "Show2")
        t1, _ = sc.ensure_task(db, r1, kind="first")
        t2, _ = sc.ensure_task(db, r2, kind="first")
        db.commit()

        calls = {"n": 0}

        async def fake_build_share(sess, task):
            calls["n"] += 1
            if task.id == t1.id:
                # 模拟 build_share 内 commit 撞锁：Session 进入 failed 态
                sess.expire_all()
                raise PendingRollbackError("模拟 database is locked / PendingRollbackError",
                                           params=None, orig=Exception("locked"))
            task.share_url = "https://www.guangyapan.com/s/ok"
            task.status = sc.ST_READY

        monkeypatch.setattr(sc, "build_share", fake_build_share)
        # 门禁/提交打桩，避免真实网络；让 t2 能走完 submitted
        monkeypatch.setattr(sc, "can_publish", lambda d, t: (True, ""))
        monkeypatch.setattr(sc, "submit_to_dispatch", lambda d, t: 1)

        stats = asyncio.run(sc.run_cycle(db, limit=20))

        # 两条任务都被尝试建分享（第一条失败没拖垮第二条）
        assert calls["n"] == 2, stats
        assert stats["degraded"] >= 1
        # t2 成功提交；t1 降级为 ready（无链接）
        assert stats["submitted"] >= 1, stats
        db.expire_all()
        t1_db = db.get(sc.ShareTask, t1.id)
        assert t1_db.status == sc.ST_READY, "降级任务标 ready，不阻塞"
        assert "[建分享失败" in (t1_db.error or "")


def test_run_cycle_survives_submit_commit_error(env, monkeypatch):
    """submit_to_dispatch 撞锁抛异常→外层 rollback→后续任务仍处理。"""
    from app import share_center as sc
    S, _ = env
    with S() as db:
        r1 = _make_res(db, "f1", "Show1", "dir1", "Show1")
        r2 = _make_res(db, "f2", "Show2", "dir2", "Show2")
        sc.ensure_task(db, r1, kind="first")
        sc.ensure_task(db, r2, kind="first")
        db.commit()

        async def fake_build_share(sess, task):
            task.share_url = "https://www.guangyapan.com/s/ok"
            task.status = sc.ST_READY

        def fake_submit(d, t):
            if "show1" in (t.work_key or "").lower():
                raise PendingRollbackError("模拟 submit commit 撞锁", params=None,
                                           orig=Exception("locked"))
            return 1

        monkeypatch.setattr(sc, "build_share", fake_build_share)
        monkeypatch.setattr(sc, "can_publish", lambda d, t: (True, ""))
        monkeypatch.setattr(sc, "submit_to_dispatch", fake_submit)

        stats = asyncio.run(sc.run_cycle(db, limit=20))
        # 即使第一条 submit 抛异常，整批不崩、返回 stats
        assert isinstance(stats, dict)
        assert "built" in stats
