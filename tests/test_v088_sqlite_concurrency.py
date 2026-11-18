"""v0.8.8 回归：SQLite `database is locked` 写冲突（用户「失败一直重试中」）。

真实环境报错：

    sqlite3.OperationalError: (sqlite3.OperationalError) database is locked
    [SQL: UPDATE resources SET thumbnail=?, scanned_at=? WHERE resources.id = ?]

两层根因：
  ① `db.py` 的 SQLite 连接既没开 WAL，也没放宽写等待（默认约 5 秒）。
     后台 job（3 秒 Token 轮询 / 10 秒 Bot 轮询 / 30 秒队列 worker）与手动同步、
     定时扫描并发写时，短写事务撞上扫描的长事务就立刻报错。
  ② `scanner.run_scan()` 在 `db.flush()` 插入批次记录后，直接 `await api.list_files()`
     发网络请求——**写事务跨 await 持有**，把持锁时间从毫秒级放大成一次网络往返。

v0.8.8：连接层 WAL + busy_timeout=30000 + timeout=30；扫描层批次插入后立即 commit；
锁错误落可操作中文提示（批次 / 断点 / 502），避免用户盲目「失败→重试→又失败」。
"""
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.exc import OperationalError

from app import main, scanner
from app.db import SessionLocal, engine, migrate
from app.models import (AuditLog, AuthToken, Resource, ResourceScanBatch,
                        ScanCursor, ScanTask)

ROOT = Path(__file__).resolve().parents[1]
TABLES = [ResourceScanBatch, ScanCursor, ScanTask, AuditLog, Resource, AuthToken]


def _clean():
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        db.commit()


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    _clean()
    ProbeAPI.seen_committed = []
    yield
    _clean()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as session:
        try:
            yield session
        finally:
            session.rollback()


def _run(coro):
    return asyncio.run(coro)


def _locked_error():
    return OperationalError(
        "UPDATE resources SET thumbnail=?, scanned_at=? WHERE resources.id = ?",
        {}, Exception("database is locked"))


class ProbeAPI:
    """第一次列目录时，用**独立 Session** 检查批次记录是否已经提交。"""

    seen_committed = []

    def __init__(self, db):
        self.db = db

    async def list_files(self, parent_id="", page=0, page_size=100):
        with SessionLocal() as other:
            b = other.scalar(select(ResourceScanBatch).order_by(
                ResourceScanBatch.id.desc()).limit(1))
            ProbeAPI.seen_committed.append(bool(b and b.status == "running"))
        return {"list": [], "total": 0}


class LockedAPI:
    def __init__(self, db):
        self.db = db

    async def list_files(self, parent_id="", page=0, page_size=100):
        raise _locked_error()


def _api_patches(cls):
    return (patch("app.scanner.GuangYaWebFiles", cls),
            patch("app.scanner.GuangYaOpenAPI", cls))


def _seed_rule(db):
    from app.models import MirrorRule
    r = MirrorRule(name="v088-db", source_root_id="A_ROOT", target_root_id="B_ROOT",
                   enabled=False, auto_identify=False, rename_files=False)
    db.add(r)
    db.commit()
    return r


# ---------- 1. 连接层：WAL 与 30 秒写等待 ----------
def test_sqlite_wal_and_busy_timeout_are_enabled():
    with engine.connect() as conn:
        mode = str(conn.execute(text("PRAGMA journal_mode")).scalar() or "").lower()
        timeout = int(conn.execute(text("PRAGMA busy_timeout")).scalar() or 0)
    assert mode == "wal", mode
    assert timeout >= 30000, timeout


# ---------- 2. 扫描层：绝不跨网络 await 持有写事务 ----------
def test_scan_commits_batch_before_first_network_await():
    patches = _api_patches(ProbeAPI)
    with patches[0], patches[1]:
        out = _run(scanner.run_scan("B_ROOT", resume=False))
    assert out["files"] == 0
    # 若仍只 flush 不 commit，独立 Session 看不到这条 running 批次
    assert ProbeAPI.seen_committed and all(ProbeAPI.seen_committed), ProbeAPI.seen_committed


# ---------- 3. 锁错误必须给可操作提示，而不是一串 SQL ----------
def test_db_locked_scan_failure_records_actionable_hint():
    patches = _api_patches(LockedAPI)
    with patches[0], patches[1], pytest.raises(OperationalError):
        _run(scanner.run_scan("B_ROOT", resume=False))
    with SessionLocal() as other:
        b = other.scalar(select(ResourceScanBatch).order_by(
            ResourceScanBatch.id.desc()).limit(1))
        task = other.scalar(select(ScanTask).limit(1))
        assert b is not None and b.status == "failed"
        assert "只运行一个容器实例" in (b.error or "")
        assert "WAL" in (b.error or "")
        if task:
            assert "只运行一个容器实例" in (task.last_message or "")


def test_mirror_run_reports_actionable_db_lock_hint(db):
    _seed_rule(db)
    with patch("app.main.mirror.sync_rule", new=AsyncMock(side_effect=_locked_error())):
        with pytest.raises(HTTPException) as ei:
            _run(main.mirror_run(db=db))
    assert ei.value.status_code == 502
    assert "只运行一个容器实例" in str(ei.value.detail)
    assert "30 秒写等待" in str(ei.value.detail)


# ---------- 4. 加固：单项 PRAGMA 失败不能吞掉 30 秒写等待 ----------
def test_sqlite_pragmas_are_isolated():
    """journal_mode=WAL 在别的连接持写锁时自己就可能抛 database is locked。

    若把三条 PRAGMA 放进同一 try，WAL 失败会连带吞掉 busy_timeout——恰好是
    最需要放宽写等待的时刻。此处锁死「每条独立执行、互不牵连」。
    """
    from app import db as db_module
    fn = getattr(db_module, "_sqlite_pragmas", None)
    if fn is None:
        pytest.skip("非 SQLite 环境")
    executed: list[str] = []

    class _Cur:
        def execute(self, sql):
            if "journal_mode" in sql:
                raise Exception("database is locked")
            executed.append(sql)

        def close(self):
            pass

    class _Conn:
        def cursor(self):
            return _Cur()

    fn(_Conn(), None)
    assert any("busy_timeout=30000" in s for s in executed), executed
    assert any("synchronous=NORMAL" in s for s in executed), executed
