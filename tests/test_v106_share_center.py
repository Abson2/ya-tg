"""四模块重构 阶段A：分享中心核心逻辑护栏。

覆盖：
  - first 任务整剧唯一（dedup_key=work_key），重复 ensure 复用不新建；
  - update/restore 复用首次分享链接，但生成独立 ShareTask（独立事件 ID，强制项3）；
  - force 不去重、优先级最高，允许同剧多次强推；
  - submit_to_dispatch 写交接队列项（submitted），溯源事件；
  - claim_tasks 原子领取 submitted→accepted（强制项4），不重复领取；
  - cancel_task 撤销未发送队列项。
用自建临时库（沙箱下不用 pytest tmp_path），mock 光鸭客户端避免真实网络。
"""
import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
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
    d = tempfile.mkdtemp(prefix="v106c_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'c.db')}")
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _make_res(db, fid="f1", name="Show (2024) {tmdb-1}/Season 1/Show.S01E01.mkv",
              share_dir_id="dir_show", scope="series_dir", share_url=""):
    from app.models import Resource
    r = Resource(file_id=fid, name=name, ext=".mkv", size=1, utime=1,
                 identify_status="identified", media_type="tv", tmdb_id=1,
                 media_title="Show", path_hint="Show (2024)/Season 1",
                 share_dir_id=share_dir_id, share_scope=scope,
                 share_dir_name="Show", share_url=share_url)
    db.add(r)
    db.commit()
    return r


def test_first_task_dedup_and_reuse(env):
    from app import share_center as sc
    S, _ = env
    with S() as db:
        r = _make_res(db)
        t1, c1 = sc.ensure_task(db, r, kind="first")
        t2, c2 = sc.ensure_task(db, r, kind="first")
        assert c1 is True and c2 is False
        assert t1.id == t2.id
        assert t1.dedup_key == sc.task_work_key(r)
        assert t1.status == sc.ST_PLANNED


def test_update_reuses_share_url_but_own_task_and_event_id(env):
    from app import share_center as sc
    S, _ = env
    with S() as db:
        r = _make_res(db)
        first, _ = sc.ensure_task(db, r, kind="first")
        first.share_url = "https://www.guangyapan.com/s/abc"
        first.status = sc.ST_READY
        db.commit()
        upd, created = sc.ensure_task(db, r, kind="update", event_id=42)
        assert created is True
        assert upd.id != first.id                    # 独立任务（独立事件 ID）
        assert upd.parent_task_id == first.id         # 追更链
        assert upd.parent_event_id == 42
        assert upd.share_url == first.share_url       # 复用原链接，不重建
        assert upd.status == sc.ST_READY
        # 同一事件重复 ensure 不新建
        upd2, created2 = sc.ensure_task(db, r, kind="update", event_id=42)
        assert created2 is False and upd2.id == upd.id


def test_force_always_new_and_high_priority(env):
    from app import share_center as sc
    S, _ = env
    with S() as db:
        r = _make_res(db)
        f1, _ = sc.ensure_task(db, r, kind="force")
        f2, _ = sc.ensure_task(db, r, kind="force")
        assert f1.id != f2.id                         # 允许重复强推
        assert f1.dedup_key is None and f2.dedup_key is None
        assert f1.priority > sc.KIND_PRIORITY["update"]


def test_submit_and_claim_handoff(env):
    from app import share_center as sc
    S, _ = env
    with S() as db:
        r = _make_res(db)
        t, _ = sc.ensure_task(db, r, kind="first")
        t.share_url = "https://www.guangyapan.com/s/abc"
        t.status = sc.ST_READY
        targets = [{"channel_id": 7}, {"channel_id": 8}]
        n = sc.submit_to_dispatch(db, t)
        assert n == 0  # 未提供 targets 时 0
        t.channel_targets = __import__("json").dumps(targets)
        db.flush()
        n = sc.submit_to_dispatch(db, t)
        assert n == 2
        assert t.status == sc.ST_SUBMITTED
        qs = db.scalars(select(sc.QueueItem).where(sc.QueueItem.share_task_id == t.id)).all()
        assert len(qs) == 2
        assert all(q.status_ext == sc.ST_SUBMITTED for q in qs)
    # 领取：同一 worker 连领两次不重复
    with S() as db:
        claimed1 = sc.claim_tasks(db, "worker-A", limit=10)
        assert claimed1
        qs = db.scalars(select(sc.QueueItem).where(sc.QueueItem.status_ext == sc.ST_ACCEPTED)).all()
        assert len(qs) == 2
        assert all(q.accept_worker == "worker-A" for q in qs)
        task = db.get(sc.ShareTask, qs[0].share_task_id)
        assert task.status == sc.ST_ACCEPTED
    with S() as db:
        claimed2 = sc.claim_tasks(db, "worker-B", limit=10)
        assert claimed2 == []  # 已全部 accepted，无可领


def test_cancel_task_revokes_pending_queue(env):
    from app import share_center as sc
    S, _ = env
    with S() as db:
        r = _make_res(db)
        t, _ = sc.ensure_task(db, r, kind="first")
        t.share_url = "https://www.guangyapan.com/s/abc"
        t.status = sc.ST_READY
        t.channel_targets = __import__("json").dumps([{"channel_id": 7}])
        db.flush()
        sc.submit_to_dispatch(db, t)
        before = db.scalars(select(sc.QueueItem).where(sc.QueueItem.share_task_id == t.id)).all()
        assert before and all(q.status == "pending" for q in before)
    with S() as db:
        t = db.get(sc.ShareTask, 1)
        n = sc.cancel_task(db, t)
        assert n == 1
        assert t.status == sc.ST_CANCELLED
        q = db.scalar(select(sc.QueueItem).where(sc.QueueItem.share_task_id == t.id))
        assert q.status == "failed" and q.status_ext == sc.ST_CANCELLED
