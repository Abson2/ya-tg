"""阶段五验收：链接校验与清理（本地元数据+删除事件驱动，不虚构光鸭接口）。

覆盖：
  - ShareRecord 新列默认（link_state=active/fail_count=0）；
  - mark_suspicious 幂等+累加；confirm_inactive_if_due 次数/间隔双门槛；
  - 清理只置 cleaned+归档，不删历史/B 库；
  - can_publish 对失效链接拒绝复用；
  - consume_resource_events 删除事件→suspicious；
  - 配置默认（auto_cleanup_enabled=false）。
"""
import asyncio
import os
import shutil
import sys
import tempfile
from datetime import timedelta
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
    d = tempfile.mkdtemp(prefix="v152_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'c.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _res(db, fid="f1", wk_name="Show"):
    from app.models import Resource
    r = Resource(file_id=fid, name=f"{wk_name} (2024)/S01E01.mkv", ext=".mkv", size=1,
                 utime=1, identify_status="identified", media_type="tv", tmdb_id=1,
                 media_title=wk_name, path_hint=wk_name, share_dir_id=f"dir_{fid}",
                 share_scope="series_dir", share_dir_name=wk_name,
                 poster_path="/x.jpg")
    db.add(r)
    db.commit()
    return r


def _record(db, wk, url="https://www.guangyapan.com/s/abc"):
    from app.models import ShareRecord
    rec = ShareRecord(work_key=wk, resource_id=1, share_id="sid", share_url=url,
                      share_status="active", current_flag=1)
    db.add(rec)
    db.commit()
    db.refresh(rec)
    return rec


# ---------- 默认值 ----------
def test_defaults(env):
    from app.runtime_config import DEFAULTS
    assert DEFAULTS["auto_cleanup_enabled"][1] == "false"
    assert DEFAULTS["link_inactive_confirm_count"][1] == "2"
    assert DEFAULTS["link_inactive_min_minutes"][1] == "30"


def test_share_record_new_columns_default(env):
    from app import share_link as sl
    S, _ = env
    with S() as db:
        rec = _record(db, "work:show")
        assert rec.link_state == sl.LINK_ACTIVE
        assert rec.verify_fail_count == 0
        assert rec.cleaned_at is None


# ---------- suspicious 状态机 ----------
def test_mark_suspicious_increments_and_is_idempotent_per_state(env):
    from app import share_link as sl
    S, _ = env
    with S() as db:
        rec = _record(db, "work:show")
        r1 = sl.mark_suspicious(db, "work:show", reason="resource_removed_event")
        assert r1["changed"] and r1["fail_count"] == 1
        db.commit()
        # 同一状态再次收到信号 → 继续累加（事件重放会各算一次；幂等靠事件 consumed_at）
        r2 = sl.mark_suspicious(db, "work:show")
        assert r2["fail_count"] == 2
        db.commit()
        db.expire_all()
        assert db.get(type(rec), rec.id).link_state == sl.LINK_SUSPICIOUS
        assert sl._load_verified(db.get(type(rec), rec.id).verified_result)["source"] == "local_event"


def test_no_inactive_until_count_and_gap_met(env):
    from app import share_link as sl
    from app.models import ShareRecord
    S, _ = env
    with S() as db:
        rec = _record(db, "work:show")
        sl.mark_suspicious(db, "work:show")
        db.commit()
        # 次数不足（默认需2）
        assert sl.confirm_inactive_if_due(db, "work:show")["changed"] is False
        # 第二次信号→次数够但间隔不足30min
        sl.mark_suspicious(db, "work:show")
        db.commit()
        assert sl.confirm_inactive_if_due(db, "work:show")["changed"] is False
        # 把 last_signal_at 回拨到 31 分钟前→满足间隔
        db.expire_all()
        rec2 = db.get(ShareRecord, rec.id)
        info = sl._load_verified(rec2.verified_result)
        from app.utils import utcnow
        info["last_signal_at"] = (utcnow() - timedelta(minutes=31)).isoformat()
        rec2.verified_result = sl._dump_verified(info)
        db.commit()
        r = sl.confirm_inactive_if_due(db, "work:show")
        assert r["changed"] and r["state"] == sl.LINK_INACTIVE
        db.expire_all()
        assert db.get(ShareRecord, rec.id).link_state == sl.LINK_INACTIVE


def test_inactive_threshold_configurable(env):
    from app import share_link as sl, runtime_config
    from app.models import ShareRecord
    S, _ = env
    with S() as db:
        rec = _record(db, "work:show")
        runtime_config.set_many(db, {"link_inactive_confirm_count": "1",
                                     "link_inactive_min_minutes": "0"})
        sl.mark_suspicious(db, "work:show")
        db.commit()
        db.expire_all()
        rec2 = db.get(ShareRecord, rec.id)
        info = sl._load_verified(rec2.verified_result)
        from app.utils import utcnow
        info["last_signal_at"] = (utcnow() - timedelta(seconds=1)).isoformat()
        rec2.verified_result = sl._dump_verified(info)
        db.commit()
        assert sl.confirm_inactive_if_due(db, "work:show")["changed"] is True


# ---------- 清理 ----------
def test_cleanup_archives_not_deletes(env):
    from app import share_link as sl
    from app.models import ShareRecord
    S, _ = env
    with S() as db:
        rec = _record(db, "work:show")
        r = sl.cleanup_link(db, "work:show", operator="tester")
        assert r["changed"]
        db.commit()
        db.expire_all()
        cleaned = db.get(ShareRecord, rec.id)
        assert cleaned.link_state == sl.LINK_CLEANED
        assert cleaned.current_flag == 0
        assert cleaned.cleaned_at is not None
        # 历史行还在（逻辑归档，不物理删）
        assert db.scalar(select(__import__("sqlalchemy").func.count()).select_from(
            ShareRecord).where(ShareRecord.work_key == "work:show")) == 1
        # 清完后无可复用 current
        assert sl.link_summary(db, "work:show")["state"] == sl.LINK_UNKNOWN


def test_cleanup_is_manual_only(env):
    """模块内无自动清理入口：mark_suspicious/confirm 都不会置 cleaned。"""
    from app import share_link as sl
    S, _ = env
    with S() as db:
        _record(db, "work:show")
        sl.mark_suspicious(db, "work:show")
        sl.mark_suspicious(db, "work:show")
        db.commit()
        db.expire_all()
        assert sl.link_summary(db, "work:show")["state"] == sl.LINK_SUSPICIOUS


# ---------- can_publish 接 link_state ----------
def test_can_publish_rejects_non_reusable_link(env):
    from app import share_link as sl, share_center as sc
    from app.models import ShareTask
    S, _ = env
    with S() as db:
        r = _res(db)
        # 建一个 update 任务 + 当前失效链接
        task, _ = sc.ensure_task(db, r, kind="update", event_id=1)
        rec = _record(db, sc.task_work_key(r))
        rec.share_status = "active"
        db.commit()
        ok, reason = sc.can_publish(db, task)
        assert ok is True
        # 置 inactive → 拒绝复用
        rec.link_state = sl.LINK_INACTIVE
        db.commit()
        ok, reason = sc.can_publish(db, task)
        assert ok is False and "失效" in reason


# ---------- 事件消费接入 ----------
def test_resource_removed_event_marks_suspicious(env):
    from app import share_center as sc, events as ev, share_link as sl
    from app.models import ShareRecord
    S, _ = env
    with S() as db:
        _record(db, "work:show")
        e = ev.emit_resource_event(db, event_type=ev.RESOURCE_REMOVED,
                                   work_key="work:show", resource_id=1,
                                   payload={"reason": "b_deleted"})
        assert e is not None
        sc.consume_resource_events(db, limit=10)
        db.expire_all()
        assert db.scalar(select(ShareRecord).where(
            ShareRecord.work_key == "work:show")).link_state == sl.LINK_SUSPICIOUS
