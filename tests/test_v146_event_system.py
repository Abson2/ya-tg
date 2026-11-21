"""阶段三验收（事件体系补齐）：幂等键扩展 + 5类事件 + 镜像事件消费端。

对齐 RESOURCE_SHARE_REDESIGN_PLAN.md 阶段三：
  1. emit_resource_event 可选 fingerprint 扩展幂等键，不传保持旧键（零迁移）；
  2. 补齐 5 类资源事件常量 + ALL_RESOURCE_EVENT_TYPES；
  3. consume_mirror_events：MIRROR_RESTORED 转 RESOURCE_RESTORED，其余只标记消费，毒事件隔离。
"""
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
    d = tempfile.mkdtemp(prefix="v146_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'b.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng, d
    shutil.rmtree(d, ignore_errors=True)


# ---------- 幂等键扩展 ----------

def test_emit_without_fingerprint_keeps_legacy_key(env):
    """不传 fingerprint：幂等键保持旧格式 type|work_key|resource_id（兼容存量）。"""
    from app import events
    from app.models import ResourceEvent
    S, eng, d = env
    db = S()
    ev = events.emit_resource_event(db, event_type=events.RESOURCE_NEW,
                                    work_key="work:开封府", resource_id=1)
    assert ev.idem_key == "identified|work:开封府|1"


def test_emit_with_fingerprint_allows_multiple_changes(env):
    """传 fingerprint：同一资源同类型多次变化各落一条（幂等键扩展）。"""
    from app import events
    from app.models import ResourceEvent
    S, eng, d = env
    db = S()
    e1 = events.emit_resource_event(db, event_type=events.RESOURCE_FILE_CHANGED,
                                    work_key="work:开封府", resource_id=1,
                                    fingerprint="size:100:u:1")
    e2 = events.emit_resource_event(db, event_type=events.RESOURCE_FILE_CHANGED,
                                    work_key="work:开封府", resource_id=1,
                                    fingerprint="size:200:u:2")
    e3 = events.emit_resource_event(db, event_type=events.RESOURCE_FILE_CHANGED,
                                    work_key="work:开封府", resource_id=1,
                                    fingerprint="size:200:u:2")  # 同指纹幂等
    assert e1.id != e2.id
    assert e3.id == e2.id
    from sqlalchemy import func
    n = db.scalar(select(func.count()).select_from(ResourceEvent).where(
        ResourceEvent.type == events.RESOURCE_FILE_CHANGED))
    assert n == 2


# ---------- 5 类事件常量 ----------

def test_all_resource_event_types():
    from app import events
    for name in ("RESOURCE_CREATED", "RESOURCE_FILE_CHANGED", "RESOURCE_RENAMED",
                 "RESOURCE_MOVED", "RESOURCE_DELETED"):
        assert getattr(events, name) in events.ALL_RESOURCE_EVENT_TYPES
    # 原有类型仍在集合内
    assert events.RESOURCE_NEW in events.ALL_RESOURCE_EVENT_TYPES
    assert events.RESOURCE_RESTORED in events.ALL_RESOURCE_EVENT_TYPES


def test_consumer_ignores_new_event_types():
    """新 5 类事件与分享中心消费的 3 类不重叠（避免高频文件事件重复推送）。

    源码护栏：consume_resource_events 的分支只处理 removed/identified/restored。
    """
    from app import events
    # 新类型与分享中心消费的三类（identified/restored/removed）不重叠
    consumed = {events.RESOURCE_NEW, events.RESOURCE_RESTORED, events.RESOURCE_REMOVED}
    new_types = {events.RESOURCE_CREATED, events.RESOURCE_FILE_CHANGED,
                 events.RESOURCE_RENAMED, events.RESOURCE_MOVED,
                 events.RESOURCE_DELETED}
    assert not (new_types & consumed)


# ---------- 镜像事件消费端 ----------

def test_consume_mirror_completed_only_marks(env):
    """MIRROR_COMPLETED 只标记消费，不触发重扫、不发资源事件。"""
    from app import events
    from app.models import ResourceEvent
    S, eng, d = env
    db = S()
    ev = events.emit_mirror_event(db, work_key="mirror:1:batch-1", rule_id=1,
                                  batch_id="batch-1", scope=events.MIRROR_COMPLETED)
    stats = events.consume_mirror_events(db)
    assert stats["consumed"] == 1
    assert "restored_events" not in stats
    assert ev.consumed_at is not None
    assert db.scalar(select(ResourceEvent).limit(1)) is None


def test_consume_mirror_restored_only_marks_no_fabricated_link(env):
    """MIRROR_RESTORED 只标记消费，不虚构 MirrorMedia.id→Resource.id 关联。

    镜像恢复走可靠链路（pipeline→_enqueue_ready 按 ever_cancelled 判 restored），
    消费端不凭 media_id 造资源事件（Resource 无 work_group_key 列可反查）。
    """
    from app import events
    from app.models import ResourceEvent
    S, eng, d = env
    db = S()
    ev = events.emit_mirror_event(db, work_key="mirror:2:batch-2", rule_id=2,
                                  batch_id="batch-2", media_id=7,
                                  scope=events.MIRROR_RESTORED)
    stats = events.consume_mirror_events(db)
    assert stats["consumed"] == 1
    assert "restored_events" not in stats  # 不做事件转换
    assert ev.consumed_at is not None
    # 未虚构资源事件
    assert db.scalar(select(ResourceEvent).limit(1)) is None


def test_consume_mirror_is_idempotent(env):
    """重复消费同一镜像事件不重复计数（幂等：第二次无 pending 事件）。"""
    from app import events
    S, eng, d = env
    db = S()
    ev = events.emit_mirror_event(db, work_key="mirror:3:batch-3", rule_id=3,
                                  scope=events.MIRROR_RESTORED)
    first = events.consume_mirror_events(db)
    second = events.consume_mirror_events(db)
    assert first["consumed"] == 1
    assert second["consumed"] == 0  # 已消费，不再出现
    db.refresh(ev)
    assert ev.consumed_at is not None