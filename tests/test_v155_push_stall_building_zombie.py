"""v1.5.1 P0 验收：全局推送停摆 + building 僵尸任务根治。

真机 2026-11-21（v1.5.0 试跑）：药神 q50 群聊项 pending 从未被领取，全局群聊
40条pending只1条发出。根因两层：
  1. building 僵尸：build_share 先 commit building 落盘(328)，后续建分享异常
     rollback 救不回已提交状态，run_cycle 候选只扫 planned/ready/failed → 永久
     卡死（真机 task4 attempts=1354，error「文件不存在」）。
  2. 队头阻塞：publisher 每轮取前20条 pending，status_ext 空的僵尸项占满名额，
     后面的新项排不到；retry 只重置 status 不重置 status_ext，故 retry 无效。

覆盖：
  - building 超时自愈：超时→planned、未超时不动、清 building_at；
  - build_share 设 building 时写 building_at；
  - 配置默认 5 分钟；
  - ShareTask.building_at 字段 + db.py 迁移；
  - publisher 取样扩大 + 可消费优先排序（静态守卫 + 排序函数行为）；
  - retry 端点重置 status_ext=submitted。
"""
import asyncio
import os
import shutil
import sys
import tempfile
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

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
    d = tempfile.mkdtemp(prefix="v155_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'c.db')}")
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _task(db, status="planned", **kw):
    from app.models import ShareTask
    from app.utils import utcnow
    t = ShareTask(work_key=kw.pop("work_key", "work:测试"), kind="update",
                  status=status, resource_id=1, attempts=kw.pop("attempts", 0), **kw)
    db.add(t)
    db.commit()
    return t


# ---------- 配置默认 ----------
def test_building_timeout_default():
    from app.runtime_config import DEFAULTS
    assert DEFAULTS["share_building_timeout_minutes"][1] == "5"


# ---------- 字段 + 迁移 ----------
def test_building_at_column_exists(env):
    from app.models import ShareTask
    S, _ = env
    with S() as db:
        t = _task(db, status="building")
        assert hasattr(t, "building_at")
        assert t.building_at is None


def test_db_migration_has_building_at():
    db_py = (ROOT / "backend/app/db.py").read_text(encoding="utf-8")
    assert '"building_at": "DATETIME"' in db_py


# ---------- building 超时自愈（核心）----------
def test_run_cycle_recovers_stale_building(env, monkeypatch):
    from app import share_center as sc
    from app import runtime_config
    from app.utils import utcnow
    S, _ = env
    with S() as db:
        runtime_config.set_many(db, {"share_building_timeout_minutes": "5"})
        db.commit()
        stale = _task(db, status="building", work_key="work:僵尸剧", attempts=1354)
        stale.building_at = utcnow() - timedelta(minutes=10)
        fresh = _task(db, status="building", work_key="work:新剧")
        fresh.building_at = utcnow() - timedelta(minutes=1)
        db.commit()
        # 让 run_cycle 的建分享/提交阶段不真的调网络：核心只看自愈是否发生
        monkeypatch.setattr(sc, "build_share", AsyncMock(return_value=""))
        monkeypatch.setattr(sc, "can_publish", lambda *a, **k: (False, "test"))
        stats = asyncio.run(sc.run_cycle(db, limit=20))
        assert stats.get("building_recovered") == 1
        db.refresh(stale)
        db.refresh(fresh)
        assert stale.status == "planned" and stale.building_at is None
        assert "building超时自愈" in stale.error
        # 未超时的 building 不动
        assert fresh.status == "building" and fresh.building_at is not None


def test_building_at_is_set_on_build_share():
    # 守住源码：build_share 进入 building 时必须写 building_at（超时自愈的依据）
    src = (ROOT / "backend/app/share_center.py").read_text(encoding="utf-8")
    seg = src[src.index("task.status = ST_BUILDING", src.index("async def build_share")):][:220]
    assert "task.building_at = utcnow()" in seg


# ---------- publisher 队头解阻塞（静态守卫）----------
def test_publisher_enlarged_sample_and_consumable_first():
    src = (ROOT / "backend/app/publisher.py").read_text(encoding="utf-8")
    assert "sample = max(limit * 10, 200)" in src
    assert "case((consumable, 0), else_=1)" in src
    assert "_sc.claim_tasks(db, worker=\"publisher\", limit=sample)" in src
    # 发送上限仍受 limit 截断，不一次发 200 条
    assert "or not q.share_task_id][:limit]" in src


# ---------- retry 端点重置 status_ext ----------
def test_retry_resets_status_ext():
    src = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
    i = src.index("def retry(qid: int")
    seg = src[i:i + 500]
    assert "q.status_ext = \"submitted\"" in seg
    assert "q.accept_worker = \"\"" in seg
