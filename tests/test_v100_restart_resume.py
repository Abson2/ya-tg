"""阶段二路线B C5：重启续跑定时 job。

覆盖：
  - _job_mirror_task_poll 存在且在 lifespan 注册；
  - 镜像锁被持有时跳过（不 poll）；
  - 无规则时跳过；
  - 有规则时调用 poll_pending_tasks 确认远端。
"""
import asyncio
from pathlib import Path
from unittest.mock import patch

from app import main

ROOT = Path(__file__).resolve().parents[1]


def test_poll_job_registered_in_lifespan():
    src = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
    assert "def _job_mirror_task_poll" in src
    assert 'add_job(_job_mirror_task_poll, "interval", seconds=60' in src


def test_poll_job_skips_when_lock_busy(monkeypatch):
    """v2.1.10：复制任务轮询自己**也持闸门**（非阻塞）——忙就跳过本轮。

    此前它只看 `info()["busy"]` 却不自己持锁：它正在轮询时，
    定时镜像轮次可能同时拿到闸门开跑，两条链路并发碰同一批文件。
    故闸门语义从「查忙不查」改为「自己也要拿」。
    """
    calls = {"n": 0}

    async def fake_poll(db, r, **kw):
        calls["n"] += 1
        return {"done": 0, "failed": 0}

    class BusyLock:
        """拿不到闸门（忙）→ 返回 False，本轮跳过。"""

        def info(self):
            return {"busy": True, "owner": "x"}

        def acquire(self, blocking=True, owner=""):
            return False

        def release(self):
            pass

    monkeypatch.setattr(main, "_mirror_lock", BusyLock())
    with patch.object(main.mirror, "poll_pending_tasks", fake_poll):
        asyncio.run(main._job_mirror_task_poll())
    assert calls["n"] == 0  # 持锁跳过


def test_poll_job_runs_when_idle(monkeypatch):
    """v2.1.10：闸门空闲时轮询照常执行（并负责释放闸门）。"""
    calls = {"n": 0}

    async def fake_poll(db, r, **kw):
        calls["n"] += 1
        return {"done": 1, "failed": 0}

    class FakeLock:
        def info(self):
            return {"busy": False}

        def acquire(self, blocking=True, owner=""):
            return True

        def release(self):
            pass

    r = main.MirrorRule(name="t", source_root_id="A", target_root_id="B")
    monkeypatch.setattr(main, "_mirror_lock", FakeLock())
    monkeypatch.setattr(main, "_mirror_rule", lambda db: r)
    with patch.object(main.mirror, "poll_pending_tasks", fake_poll):
        asyncio.run(main._job_mirror_task_poll())
    assert calls["n"] == 1
