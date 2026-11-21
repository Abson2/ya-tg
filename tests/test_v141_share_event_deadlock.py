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
        def boom(*a, **k):
            raise RuntimeError("database is locked")
        with patch.object(sc, "ensure_task", side_effect=boom):
            for _ in range(sc.MAX_EVENT_CONSUME_FAILURES):
                stats = sc.consume_resource_events(db, limit=10)
        db.refresh(ev)
        assert ev.consumed_at is not None, "连续失败达阈值后事件必须被强制消费"
        assert stats["skipped_poison"] >= 1, stats


def test_single_failed_event_does_not_block_later_events(env):
    """队头事件失败只 rollback 自己，后续事件仍能被消费（不中止整批）。"""
    from app import share_center as sc
    S, _, _ = env
    with S() as db:
        _add_identified_resource(db, 1, name="毒 S01E01.mkv")
        _add_identified_resource(db, 2, name="正常 S01E01.mkv")
        bad = _add_event(db, 1, resource_id=1, work_key="work:毒")
        good = _add_event(db, 2, resource_id=2, work_key="work:正常")

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


def test_fail_count_persists_in_payload(env):
    """失败计数写进 payload_json._consume_fail，可跨周期累加。"""
    from app import share_center as sc
    S, _, _ = env
    with S() as db:
        _add_identified_resource(db, 1)
        ev = _add_event(db, 1, resource_id=1)
        def boom(*a, **k):
            raise RuntimeError("locked")
        import json
        with patch.object(sc, "ensure_task", side_effect=boom):
            sc.consume_resource_events(db, limit=10)
        db.refresh(ev)
        payload = json.loads(ev.payload_json or "{}")
        assert payload.get("_consume_fail") == 1, payload
        assert "locked" in payload.get("_consume_last_error", ""), payload
