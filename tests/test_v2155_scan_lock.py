"""v2.1.55 验收：手动扫描必须参与流水线锁（否则与定时扫描并发）。

真机背景（2026-11-27）：用户报容器内存 1GB。
只读实测：uvicorn 单进程 RSS 1019MiB、峰值 1169MiB，CPU 100%+；
`resource_scan_batches` 里 **266 号（15:52:09 手动）与 267 号（16:00:00 整点定时）
同时 running**，更早的 264 号也是同样情形——它至今还挂在 running 永不结束。

根因：定时扫描 `_scheduled_scan` 会拿 `_mirror_lock`（v2.1.11 加的流水线闸门），
而**手动扫描三个入口一个都没拿**，于是「两个 run_scan 同时各持一份全量目录树
+ 2.4 万资源行」成为可能。并发还会互相覆盖 `scan_cursors`，让批次判不完整、
对账读到错的 seen。
"""
import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from app import main
from app.db import SessionLocal, migrate
from app.models import MirrorRule

ROOT = Path(__file__).resolve().parents[1]
MAIN_SRC = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")


@pytest.fixture(autouse=True)
def _state():
    migrate()
    try:
        yield
    finally:
        main._async_scan.update({"running": False, "started_at": None,
                                 "finished_at": None, "mode": "", "error": "",
                                 "result": {}, "stage": ""})
        main._async_scan_task = None
        # 任何用例留下的锁占用都必须释放，否则会污染后续用例
        if main._mirror_lock.info().get("held"):
            try:
                main._mirror_lock.release()
            except Exception:
                pass


def _db():
    return SessionLocal()


def test_async_scan_acquires_pipeline_lock_before_scanning():
    """后台扫描先拿锁再动手，且跑完一定 release。"""
    events = []

    async def fake_refresh(db, **kw):
        events.append("scan")
        return {"files": 0}

    async def fake_acquire(owner="", timeout=3600.0):
        events.append(("acquire", owner))
        return True

    def fake_release():
        events.append("release")

    with patch.object(main, "_refresh_library_from_b", new=fake_refresh), \
         patch.object(main, "_write_pipeline_snapshot", new=lambda *a, **k: None), \
         patch.object(main._mirror_lock, "acquire_async", new=fake_acquire), \
         patch.object(main._mirror_lock, "release", new=fake_release):
        asyncio.run(main._run_async_scan("scan"))

    assert events[0][0] == "acquire", "必须先拿锁"
    assert events[0][1].startswith("手动扫描")
    assert events.index("scan") > 0, "拿到锁之后才允许扫描"
    assert events[-1] == "release", "无论成败都要释放"


def test_async_scan_gives_up_when_lock_wait_times_out():
    """等锁超时 → 一次都不扫，并如实写状态（不许假装在跑）。"""
    called = []

    async def fake_refresh(db, **kw):
        called.append(1)
        return {}

    async def never_acquire(owner="", timeout=3600.0):
        return False

    with patch.object(main, "_refresh_library_from_b", new=fake_refresh), \
         patch.object(main, "_write_pipeline_snapshot", new=lambda *a, **k: None), \
         patch.object(main._mirror_lock, "acquire_async", new=never_acquire):
        asyncio.run(main._run_async_scan("scan"))

    assert called == [], "没拿到锁就不该开始扫描"
    assert main._async_scan["running"] is False
    assert main._async_scan["stage"] == "超时放弃"
    assert "超时" in main._async_scan["error"]


@pytest.mark.parametrize("fn_name,owner", [
    ("scan_now", "立即扫描"),
    ("rescan_library", "重新扫描库"),
    ("refresh_library", "刷新资源库"),
])
def test_sync_entrypoints_refuse_when_lock_held(fn_name, owner):
    """三个同步入口在锁被占时立刻 409，而不是插进去并发扫。"""
    fn = getattr(main, fn_name)
    assert main._mirror_lock.acquire(blocking=False, owner="定时扫描识别")
    try:
        with _db() as db:
            with pytest.raises(HTTPException) as ei:
                asyncio.run(fn(db=db))
        assert ei.value.status_code == 409
        assert "已有扫描或镜像任务在运行" in str(ei.value.detail)
    finally:
        main._mirror_lock.release()


def test_scan_pipeline_source_has_lock_on_all_manual_paths():
    """源码级兜底：四个手动入口都必须出现锁操作（防以后再漏一条）。"""
    for marker in ('async def _run_async_scan',
                   'async def scan_now',
                   'async def rescan_library',
                   'async def refresh_library'):
        i = MAIN_SRC.index(marker)
        seg = MAIN_SRC[i:i + 3200]
        assert "_mirror_lock" in seg, f"{marker} 没有参与流水线锁"


def test_scheduled_scan_still_uses_same_lock():
    """定时扫描那条路径的锁不能被改掉（两条链路必须同一把锁才有意义）。"""
    i = MAIN_SRC.index('async def _scheduled_scan')
    seg = MAIN_SRC[i:i + 1200]
    assert 'acquire_async("定时扫描识别"' in seg
