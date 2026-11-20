"""四模块重构 阶段A：分享中心数据层迁移护栏。

覆盖两条路径：
  1) 新库：Base.metadata.create_all 建出 share_tasks + 交接列；
  2) 老库：只有旧列的表，migrate() 的 _COLUMNS 幂等 ALTER 补齐交接列。
不用 pytest tmp_path（沙箱下 pytest-of-unknown 目录 EEXIST），自建 /tmp 子目录。
"""
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))


@pytest.fixture
def dbdir():
    d = tempfile.mkdtemp(prefix="v106_", dir="/tmp")
    yield d
    shutil.rmtree(d, ignore_errors=True)


def _fresh_engine(dbdir):
    from app.db import Base
    from app import models  # noqa: F401  注册所有表
    eng = create_engine(f"sqlite:///{os.path.join(dbdir, 'fresh.db')}")
    Base.metadata.create_all(bind=eng)
    return eng


def test_share_tasks_table_and_columns_on_fresh_db(dbdir):
    """新库：create_all 建出 share_tasks 表，含全部生命周期字段。"""
    eng = _fresh_engine(dbdir)
    cols = {c["name"] for c in inspect(eng).get_columns("share_tasks")}
    required = {
        "id", "work_key", "kind", "resource_id", "share_dir_id", "share_scope",
        "share_id", "share_url", "share_status", "parent_task_id", "parent_event_id",
        "status", "priority", "scheduled_at", "channel_targets", "dedup_key",
        "attempts", "next_retry_at", "error", "created_at", "published_at",
    }
    assert required.issubset(cols), f"缺列：{required - cols}"


def test_queue_and_event_handoff_columns_on_fresh_db(dbdir):
    """新库：push_queue / distribution_events 含交接列。"""
    eng = _fresh_engine(dbdir)
    q = {c["name"] for c in inspect(eng).get_columns("push_queue")}
    assert {"share_task_id", "push_reason", "event_id", "status_ext",
            "accept_worker", "accepted_at"}.issubset(q)
    e = {c["name"] for c in inspect(eng).get_columns("distribution_events")}
    assert {"share_task_id", "push_task_ids"}.issubset(e)


def test_alter_path_adds_columns_to_legacy_tables(dbdir):
    """老库：只有旧列的表，经 _COLUMNS 幂等 ALTER 补齐。"""
    from app.db import _COLUMNS
    eng = create_engine(f"sqlite:///{os.path.join(dbdir, 'legacy.db')}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE push_queue (id INTEGER PRIMARY KEY, resource_id INTEGER, channel_id INTEGER, status TEXT DEFAULT 'pending', attempts INTEGER DEFAULT 0, error TEXT DEFAULT '', next_retry_at DATETIME, telegram_message_id TEXT DEFAULT '', created_at DATETIME, sent_at DATETIME)"))
        conn.execute(text("CREATE TABLE distribution_events (id INTEGER PRIMARY KEY, work_key TEXT DEFAULT '', fingerprint TEXT DEFAULT '', event_type TEXT DEFAULT 'updated', changed_files INTEGER DEFAULT 0, queue_id INTEGER DEFAULT 0, share_reused BOOLEAN DEFAULT 1, created_at DATETIME)"))
    insp = inspect(eng)
    tables = set(insp.get_table_names())
    for table in ("push_queue", "distribution_events"):
        assert table in tables
        existing = {c["name"] for c in insp.get_columns(table)}
        for name, ddl in _COLUMNS[table].items():
            if name not in existing:
                with eng.begin() as conn:
                    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}"))
    q = {c["name"] for c in inspect(eng).get_columns("push_queue")}
    assert {"share_task_id", "push_reason", "event_id", "status_ext",
            "accept_worker", "accepted_at"}.issubset(q)
    e = {c["name"] for c in inspect(eng).get_columns("distribution_events")}
    assert {"share_task_id", "push_task_ids"}.issubset(e)


def test_dedup_key_unique_allows_null(dbdir):
    """dedup_key 唯一约束：NULL 可多行（force），非空重复被挡。"""
    from app.db import Base
    from app import models
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.exc import IntegrityError
    eng = create_engine(f"sqlite:///{os.path.join(dbdir, 'dedup.db')}")
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng)
    with S() as db:
        db.add(models.ShareTask(work_key="w1", kind="force", dedup_key=None))
        db.add(models.ShareTask(work_key="w1", kind="force", dedup_key=None))
        db.commit()  # 两条 NULL 不冲突
        assert db.query(models.ShareTask).count() == 2
    with S() as db:
        db.add(models.ShareTask(work_key="w1", kind="first", dedup_key="w1"))
        db.commit()
    with S() as db:
        db.add(models.ShareTask(work_key="w1", kind="first", dedup_key="w1"))
        with pytest.raises(IntegrityError):
            db.commit()
