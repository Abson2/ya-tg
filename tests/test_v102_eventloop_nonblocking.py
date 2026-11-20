"""阶段四 A-07：DB 密集后台 job 不阻塞事件循环（health 等请求可并发）。

约束：不降低 busy_timeout——v0.8.8 的 30s + WAL 是真实 `database is locked` 修复，
审计建议的 5s 会回退该修复。仅把纯同步 DB job 挪到线程池；完整 async session 迁移
属架构债、本批不做（功能一致优先）。
"""
import asyncio
import time
from pathlib import Path

from app import main

ROOT = Path(__file__).resolve().parents[1]


def test_heavy_db_jobs_use_to_thread():
    """僵尸自愈扫描与日志清理的 DB 主体必须放线程池。"""
    import inspect
    src = inspect.getsource(main._job_mirror_task_reap)
    assert "asyncio.to_thread" in src
    assert "asyncio.to_thread" in inspect.getsource(main._job_log_prune)


def test_busy_timeout_not_regressed_to_5s():
    """守住 v0.8.8 修复：busy_timeout 30s + WAL 不得被审计建议的 5s 回退。"""
    src = (ROOT / "backend/app/db.py").read_text(encoding="utf-8")
    assert "busy_timeout=30000" in src
    assert "PRAGMA journal_mode=WAL" in src
    assert "timeout=30" in src


def test_to_thread_actually_frees_event_loop():
    """真实并发：阻塞型 job 在 to_thread 里 sleep 时，事件循环仍能推进计时器。"""
    ticks = {"n": 0}

    async def ticker():
        for _ in range(6):
            await asyncio.sleep(0.01)
            ticks["n"] += 1

    def blocking_db_job():
        # 模拟线程池里的同步阻塞，不应卡住事件循环的 ticker
        time.sleep(0.03)

    async def scenario():
        await asyncio.gather(asyncio.to_thread(blocking_db_job), ticker())

    asyncio.run(scenario())
    assert ticks["n"] >= 4, f"事件循环被阻塞，只推进了 {ticks['n']} 次"


def test_health_route_is_sync_def():
    """/api/health 必须是同步 def（FastAPI 走线程池），不被 async DB 阻塞。"""
    import inspect
    assert not inspect.iscoroutinefunction(main.health)
