"""v1.3 验收：借鉴 TgtoDrive「已入库名称沿用不重复解析」修复改名性能/卡死。

① 已完成作品整段跳过（不再重复解析英文名 + 列目录）否则 force 仍全量重解析；
② async 写回用 await asyncio.sleep 退避，不阻塞事件循环（系统卡死主因）；
③ 写回不回刷调用方 Session ORM 对象（避免重新引入 SQLITE_BUSY_SNAPSHOT）。
"""
import asyncio
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

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
    d = tempfile.mkdtemp(prefix="v130_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'b.db')}")
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng, d
    shutil.rmtree(d, ignore_errors=True)


def _seed_done_work(db, *, eng_title="West Out of Yumen", n=3, status="done"):
    from app.models import MirrorMedia, MirrorFile
    m = MirrorMedia(rule_id=1, title_standard="玉门关外 (2020) {tmdb-999}",
                    media_type="tv", target_dir_id="B_DIR", english_title=eng_title,
                    english_source="tmdb")
    db.add(m)
    db.flush()
    for i in range(n):
        db.add(MirrorFile(media_id=m.id, rule_id=1, source_file_id=f"sf{i}",
                          target_file_id=f"bf{i}", file_name=f"玉门关外 S01E0{i+1}.mkv",
                          size=1, utime=1, fingerprint=f"1:1", status="synced",
                          rename_status=status, target_name=f"West.Out.of.Yumen.S01E0{i+1}"))
    db.commit()
    return m


# ---------- ① 名称缓存沿用（借鉴 TgtoDrive「已入库名称沿用不重复解析」） ----------
def test_resolve_reuses_cached_english_no_network(env, monkeypatch):
    """缓存命中即零网络返回；force 才重新解析。名称沿用是本项目已有的提速机制。"""
    from app import mirror
    S, _, _ = env
    with S() as db:
        m = _seed_done_work(db, eng_title="West Out of Yumen")
        calls = {"tmdb": 0}

        async def fake_tmdb(*a, **k):
            calls["tmdb"] += 1
            return "Should Not Be Called"

        monkeypatch.setattr(mirror.tmdb, "english_title", fake_tmdb)
        eng, src = asyncio.run(mirror._resolve_english_title(db, m))
        assert eng == "West Out of Yumen" and calls["tmdb"] == 0, (eng, calls)


def test_resolve_force_reparses_even_if_cached(env, monkeypatch):
    """force=True（用户点「重新解析英文名」）绕过缓存重新查 TMDB。"""
    from app import mirror
    S, _, _ = env
    with S() as db:
        m = _seed_done_work(db, eng_title="Old Cache")

        async def fake_tmdb(*a, **k):
            return "New English Title"

        monkeypatch.setattr(mirror.tmdb, "english_title", fake_tmdb)
        eng, _src = asyncio.run(mirror._resolve_english_title(db, m, force=True))
        assert eng == "New English Title", eng


# ---------- ② async 退避不阻塞事件循环 ----------
def test_async_persist_uses_asyncio_sleep_not_blocking(env, monkeypatch):
    """async 写回连续锁冲突时，退避用 await asyncio.sleep，其它协程能同时跑。"""
    import inspect
    from app import mirror
    from sqlalchemy.exc import OperationalError
    S, _, _ = env

    # 静态护栏：async 写回函数体（剔除 docstring）必须出现 `await asyncio.sleep`、
    # 且不出现可执行的 `time.sleep(`（docstring 里描述历史问题不算）。
    import ast
    src = inspect.getsource(mirror._persist_english_title_async)
    tree = ast.parse(src)
    # 静态检查可执行文件体：不得有 time.sleep(...) 调用；必须有 asyncio.sleep 调用
    calls = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            f = node.func
            if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name):
                calls.append(f"{f.value.id}.{f.attr}")
    assert "asyncio.sleep" in calls, f"async 写回退避必须用 asyncio.sleep: {calls}"
    assert "time.sleep" not in calls, f"async 写回不得用同步 time.sleep: {calls}"

    ticks = {"other": 0}

    async def concurrent_ticker():
        for _ in range(20000):
            ticks["other"] += 1
            await asyncio.sleep(0)

    attempts = {"n": 0}

    def fake_once(mid, eng, src):
        attempts["n"] += 1
        if attempts["n"] <= 3:
            return OperationalError("stmt", {}, Exception("database is locked"))
        return None

    monkeypatch.setattr(mirror, "_write_english_title_once", fake_once)

    async def scenario():
        await asyncio.gather(
            mirror._persist_english_title_async(None, 1, "X", "tmdb"),
            concurrent_ticker())

    asyncio.run(scenario())
    assert attempts["n"] == 4, attempts
    assert ticks["other"] > 0, "退避期间其它协程应能运行（事件循环未冻结）"


def test_write_once_does_not_refresh_caller_session(env):
    """写回不污染调用方 Session：调用方对象不因此变脏。"""
    from app import mirror
    from app.models import MirrorMedia
    S, _, _ = env
    with S() as db:
        m = MirrorMedia(rule_id=1, title_standard="X", media_type="tv")
        db.add(m)
        db.commit()
        mid = m.id
    with S() as db:
        db.scalar(select(MirrorMedia).where(MirrorMedia.id == mid))  # 建读快照
        mirror._write_english_title_once(mid, "English Name", "tmdb")
        obj = db.get(MirrorMedia, mid)
        assert obj.english_title in (None, ""), "写回不应回刷调用方 ORM 对象"
        dirty = obj in db.dirty
        assert not dirty, "调用方 Session 不应被标脏（否则下次 commit 重新引入写锁）"
