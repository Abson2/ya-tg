"""v1.4.3 验收：Bot 轮询不再每 10 秒写 app_config 与改名抢 SQLite 写锁。

2026-11-21 第五次死锁复现（真机完整堆栈取证）：
  _job_bot_poll (main.py) → bot_listener.poll_once → runtime_config.set_many (db.flush)
  → UPDATE app_config SET value_enc=? → database is locked
每 10 秒无条件写 contribution_update_offset + 清空 err_key，与改名 job 的
58 次 rename_status 更新抢同一把写锁，双方互相饿死；翻译正常但落盘全失败。

修复：
  1. offset/error 改进程内存态，轮询热路径零 DB 写；
  2. 只在有新消息（ProcessedUpdate 新增）时才 commit；
  3. 优雅退出 / 低频 checkpoint 落库（flush_poll_state）；
  4. _job_bot_poll 整体走 to_thread，主线程零 SQLite 锁等待。
"""
import asyncio
import inspect
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
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
    d = tempfile.mkdtemp(prefix="v143_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'b.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng, d
    eng.dispose()
    shutil.rmtree(d, ignore_errors=True)


# ---------- A. 内存态 offset/error ----------

def test_poll_state_in_memory_roundtrip():
    from app import bot_listener as bl
    key = "test_bot_v143"
    bl._poll_state.pop(key, None)
    st = bl.get_poll_state(key)
    assert st == {"offset": 0, "error": ""}
    bl.set_poll_state(key, offset=42, error="429: 限流")
    st = bl.get_poll_state(key)
    assert st["offset"] == 42 and st["error"] == "429: 限流"
    bl.set_poll_state(key, error="")
    st = bl.get_poll_state(key)
    assert st["offset"] == 42 and st["error"] == "", "只更新传入字段"
    bl._poll_state.pop(key, None)


def test_flush_poll_state_persists_to_db(env):
    from app import bot_listener as bl, runtime_config as rc
    S, _, _ = env
    key = "test_flush_v143"
    bl.set_poll_state(key, offset=77, error="network: boom")
    with S() as db:
        out = bl.flush_poll_state(db)
        assert out["flushed"] >= 1
        assert rc.get(db, "contribution_update_offset") == "77"
        assert rc.get(db, f"bot_poll_error_{key}") == "network: boom"
    bl._poll_state.pop(key, None)


# ---------- B. 热路径零 DB 写（核心死锁修复）----------

def test_poll_once_empty_updates_no_config_write(env):
    """空轮询（无新消息）不得写 app_config——第五次死锁的直接持锁源。"""
    from app import bot_listener as bl, runtime_config as rc
    S, _, _ = env
    with S() as db:
        rc.set_many(db, {"contribution_enabled": "true",
                         "contribution_update_offset": "10"})
        db.commit()

        class FakeResp:
            status_code = 200
            def json(self):
                return {"ok": True, "result": []}

        class FakeClient:
            async def __aenter__(self): return self
            async def __aexit__(self, *a): return False
            async def get(self, *a, **k): return FakeResp()

        # 用 patch 让任何 set_many 写 app_config 都失败（模拟持锁），
        # 若热路径仍写 DB，poll_once 会抛异常；修复后纯内存写，路径应正常返回。
        bl.set_poll_state(1, offset=10, error="")
        bl.get_poll_state(1)["_seeded"] = True

        def forbid_config_write(*a, **k):
            raise RuntimeError("database is locked (app_config)")

        with patch.object(rc, "set_many", side_effect=forbid_config_write), \
             patch.object(bl.httpx, "AsyncClient", lambda *a, **k: FakeClient()), \
             patch.object(bl, "effective_token", lambda d: "faketoken"), \
             patch.object(bl, "effective_bot_id", lambda d: 1), \
             patch.object(bl, "auto_clear_stale_commands", new=_async_false):
            out = asyncio.run(bl.poll_once(db))

        assert out.get("enabled") is True
        assert out.get("messages") == 0
        assert bl.get_poll_state(1)["offset"] == 11, "offset 内存推进"
    bl._poll_state.pop("1", None)


async def _async_false(*a, **k):
    return False


def test_poll_once_error_branch_no_db_write(env):
    """409/429 错误分支也不写 app_config（旧版 4 处 set_many+db.commit 持锁）。"""
    from app import bot_listener as bl, runtime_config as rc
    S, _, _ = env
    with S() as db:
        rc.set_many(db, {"contribution_enabled": "true"})
        db.commit()
        bl.set_poll_state(2, offset=0, error="")
        bl.get_poll_state(2)["_seeded"] = True

        class FakeResp:
            status_code = 429
            def json(self):
                return {"ok": False, "description": "Too Many Requests"}

        class FakeClient:
            async def __aenter__(self): return self
            async def __aexit__(self, *a): return False
            async def get(self, *a, **k): return FakeResp()

        def forbid(*a, **k):
            raise RuntimeError("database is locked")

        with patch.object(rc, "set_many", side_effect=forbid), \
             patch.object(bl.httpx, "AsyncClient", lambda *a, **k: FakeClient()), \
             patch.object(bl, "effective_token", lambda d: "faketoken"), \
             patch.object(bl, "effective_bot_id", lambda d: 2), \
             patch.object(bl, "auto_clear_stale_commands", new=_async_false):
            out = asyncio.run(bl.poll_once(db))

        assert out.get("error_kind"), out
        assert bl.get_poll_state(2)["error"], "错误信息写内存"
    bl._poll_state.pop("2", None)


# ---------- C. to_thread + 退出 flush 护栏 ----------

def test_bot_poll_job_uses_to_thread():
    from app import main
    src = inspect.getsource(main._job_bot_poll)
    assert "asyncio.to_thread" in src or "_bot_poll_in_thread" in src
    assert "await bot_listener.poll_once(" not in src, "主协程不得直接 await poll_once"
    fn = inspect.getsource(main._bot_poll_in_thread)
    assert "asyncio.run" in fn and "SessionLocal" in fn


def test_shutdown_flushes_poll_state():
    from app import main
    src = inspect.getsource(main.lifespan)
    assert "flush_poll_state" in src, "优雅退出须 flush 内存态 offset/error"


def test_version_is_144():
    import json
    root = Path(__file__).resolve().parents[1]
    main_src = (root / "backend/app/main.py").read_text(encoding="utf-8")
    assert 'version="2.1.45"' in main_src
    assert "v2.1.45 · Asia/Shanghai" in (root / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert json.loads((root / "frontend/package.json").read_text(encoding="utf-8"))["version"] == "2.1.45"
