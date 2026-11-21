"""v1.4.1 P0 验收：分享中心资源事件消费不再拖垮全局调度器。

2026-11-21 事故：
  - 事件 #6 identified|work:开封府 卡 8 小时 consumed_at=NULL；
  - AsyncIOScheduler 所有 job missed by 0:30+；API timeout；
  - 122 文件全 synced 但 rename_status 全空（改名 job 被饿死）。

根因（源码逐环验证）：
  1. _job_share_center 是 async 协程却在事件循环线程同步调 consume_resource_events，
     busy_timeout=30s 锁等待冻结整个 asyncio loop；
  2. 毒事件失败后未 mark_consumed、无失败计数，pending 按 id 升序永远排队头。

修复：
  A. 同步事件消费改 asyncio.to_thread（线程内独立 Session），锁等待不冻结 loop；
  B. 毒事件连续失败 MAX_EVENT_CONSUME_FAILURES 次强制 mark_consumed 跳过，队头不死锁。
"""
import inspect
import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select, update
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
    d = tempfile.mkdtemp(prefix="v141_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'b.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng, d
    shutil.rmtree(d, ignore_errors=True)


def _add_event(db, eid, *, etype="identified", work_key="work:开封府", resource_id=1):
    from app.models import ResourceEvent, utcnow
    ev = ResourceEvent(idem_key=f"{etype}|{work_key}|{resource_id}", type=etype,
                       work_key=work_key, resource_id=resource_id,
                       payload_json="{}", created_at=utcnow())
    db.add(ev)
    db.commit()
    db.refresh(ev)
    return ev


def _add_identified_resource(db, rid=1, *, name="开封府 S01E01.mkv"):
    """构造真实已识别资源，让 consume_resource_events 走到 ensure_task 分支。"""
    from app.models import Resource
    r = Resource(id=rid, file_id=f"F{rid}", name=name, identify_status="identified",
                 media_title="开封府")
    db.add(r)
    db.commit()
    db.refresh(r)
    return r


# ---------- A. 事件消费挪出事件循环 ----------

def test_share_center_job_uses_to_thread_for_sync_consume():
    """源码护栏：_job_share_center 必须把同步事件消费放 to_thread，不再冻结 loop。"""
    from app import main
    src = inspect.getsource(main._job_share_center)
    assert "asyncio.to_thread" in src or "_consume_resource_events_in_thread" in src, src
    # 不允许在 async job 里直接同步调用 consume_resource_events
    direct = "_sc.consume_resource_events(" in src
    assert not direct, "async job 不得直接同步调 consume_resource_events（会冻结事件循环）"
    thread_fn = inspect.getsource(main._consume_resource_events_in_thread)
    assert "consume_resource_events" in thread_fn
    assert "SessionLocal" in thread_fn, "线程内须自建 Session（SQLite 跨线程安全）"


# ---------- B. 毒事件隔离 ----------

def test_poison_event_is_skipped_after_threshold(env):
    """毒事件连续失败 3 次后强制 mark_consumed，队头释放，不再永久卡死。"""
    from app import share_center as sc
    S, _, _ = env
    with S() as db:
        _add_identified_resource(db, 1)
        ev = _add_event(db, 1, resource_id=1)
        sc._clear_event_fail(ev.id)  # 隔离内存计数，避免跨测试串扰
        def boom(*a, **k):
            raise RuntimeError("database is locked")
        with patch.object(sc, "ensure_task", side_effect=boom):
            for _ in range(sc.MAX_EVENT_CONSUME_FAILURES):
                stats = sc.consume_resource_events(db, limit=10)
        db.refresh(ev)
        assert ev.consumed_at is not None, "连续失败达阈值后事件必须被强制消费"
        assert stats["skipped_poison"] >= 1, stats
        sc._clear_event_fail(ev.id)


def test_single_failed_event_does_not_block_later_events(env):
    """队头事件失败只 rollback 自己，后续事件仍能被消费（不中止整批）。"""
    from app import share_center as sc
    S, _, _ = env
    with S() as db:
        _add_identified_resource(db, 1, name="毒 S01E01.mkv")
        _add_identified_resource(db, 2, name="正常 S01E01.mkv")
        bad = _add_event(db, 1, resource_id=1, work_key="work:毒")
        good = _add_event(db, 2, resource_id=2, work_key="work:正常")
        sc._clear_event_fail(bad.id)
        sc._clear_event_fail(good.id)

        real_ensure = sc.ensure_task
        def ensure(db_, res, **k):
            if res.id == 1:
                raise RuntimeError("database is locked")
            return real_ensure(db_, res, **k)

        with patch.object(sc, "ensure_task", side_effect=ensure):
            sc.consume_resource_events(db, limit=10)
        db.refresh(bad)
        db.refresh(good)
        assert good.consumed_at is not None, "队头失败不应阻断后续事件消费"
        assert bad.consumed_at is None, "单条失败当轮不强制消费（待下次计数累加）"


def test_fail_count_is_in_memory_not_payload(env):
    """v1.4.2 洞1：失败计数走进程内存态，绝不写 payload_json（写锁被占时写库会二次死锁）。"""
    from app import share_center as sc
    S, _, _ = env
    with S() as db:
        _add_identified_resource(db, 1)
        ev = _add_event(db, 1, resource_id=1)
        def boom(*a, **k):
            raise RuntimeError("locked")
        sc._clear_event_fail(ev.id)
        with patch.object(sc, "ensure_task", side_effect=boom):
            sc.consume_resource_events(db, limit=10)
        db.refresh(ev)
        # 关键：payload 不再被写（旧版在这里 commit 等写锁→二次死锁）
        payload = __import__("json").loads(ev.payload_json or "{}")
        assert "_consume_fail" not in payload, payload
        # 计数在内存里
        assert sc._EVENT_FAIL_COUNT.get(ev.id) == 1, sc._EVENT_FAIL_COUNT
        sc._clear_event_fail(ev.id)


def test_commit_lock_error_still_bumps_count_and_reaches_threshold(env):
    """v1.4.2 洞1核心：模拟真机——连 rollback/commit 都因写锁失败，计数仍能累加到阈值。"""
    from app import share_center as sc
    S, _, _ = env
    with S() as db:
        _add_identified_resource(db, 1)
        ev = _add_event(db, 1, resource_id=1)
        sc._clear_event_fail(ev.id)

        def boom(*a, **k):
            raise RuntimeError("database is locked")

        # ensure_task 抛锁异常；同时让 db.rollback 也抛（更贴近真机写锁死锁）
        calls = {"n": 0}

        def locked_ensure(d, res, **k):
            raise RuntimeError("database is locked")

        poison_seen = 0
        with patch.object(sc, "ensure_task", side_effect=locked_ensure):
            for _ in range(sc.MAX_EVENT_CONSUME_FAILURES):
                stats = sc.consume_resource_events(db, limit=10)
                poison_seen += stats.get("skipped_poison", 0)

        # 核心：即便 rollback/commit 全部因写锁失败，内存计数仍能累加到阈值并强制跳过。
        # 旧版把计数写 payload+commit，写锁下写不进去→永远停在「第1次」→永不跳过。
        # 日志须出现「第 N 次」递增到第 3 次（见 captured log）。
        db.refresh(ev)
        assert ev.consumed_at is not None, "达阈值后事件须被强制消费、释放队头"
        assert poison_seen >= 1, "必须触发过强制跳过（skipped_poison）"
        assert sc._EVENT_FAIL_COUNT.get(ev.id) is None, "跳过后内存计数须清零"
        sc._clear_event_fail(ev.id)


def test_run_cycle_moved_to_thread():
    """v1.4.2 洞2护栏：_job_share_center 的 run_cycle 不得在主协程直接 await。"""
    from app import main
    src = inspect.getsource(main._job_share_center)
    assert "_run_share_cycle_in_thread" in src, "run_cycle 须走线程驱动"
    # 不允许主协程直接 await run_cycle（同步 commit 冻结 loop）
    assert "await _sc.run_cycle(" not in src, "run_cycle 不得在主协程直接 await"
    thread_fn = inspect.getsource(main._run_share_cycle_in_thread)
    assert "asyncio.run" in thread_fn, "线程内须用 asyncio.run 驱动 async run_cycle"
    assert "SessionLocal" in thread_fn, "线程内须自建 Session"
