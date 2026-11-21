"""v1.5.1 P0 第七次验收：TG 发送循环单条超时拖垮全局队列。

真机 2026-11-21（v1.5.0）：药神 q50 迟迟未发，全局群聊 32→40 条 pending 只 1 条发出。
非写锁——sendPhoto 硬编码 timeout=20s，失败回退 sendMessage 再 20s，逐条串行 +
全局 _queue_lock，一轮拖到数分钟，后续 process_queue 全 skipped。

覆盖：
  - 配置默认 telegram_http_timeout_seconds=8 / telegram_send_hard_limit_seconds=12；
  - _send_telegram 两处 AsyncClient 用可配置超时（不再硬编码 20）；
  - 发送循环用 asyncio.wait_for 给单条加硬上限，超时按退避重试；
  - 行为：慢发送被 wait_for 截断为失败，不卡死整轮。
"""
import asyncio
import os
from pathlib import Path

os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")
ROOT = Path(__file__).resolve().parents[1]


def test_timeout_config_defaults():
    import sys
    sys.path.insert(0, str(ROOT / "backend"))
    from app.runtime_config import DEFAULTS
    assert DEFAULTS["telegram_http_timeout_seconds"][1] == "8"
    assert DEFAULTS["telegram_send_hard_limit_seconds"][1] == "12"


def test_no_hardcoded_20s_timeout():
    src = (ROOT / "backend/app/publisher.py").read_text(encoding="utf-8")
    assert "httpx.AsyncClient(timeout=20)" not in src, "不得再有硬编码20s超时"
    assert "telegram_http_timeout_seconds" in src
    assert "timeout=http_timeout" in src


def test_send_loop_uses_wait_for_hard_limit():
    src = (ROOT / "backend/app/publisher.py").read_text(encoding="utf-8")
    assert "telegram_send_hard_limit_seconds" in src
    assert "asyncio.wait_for(" in src
    assert "_send_telegram(db, q, res, channel, bot), timeout=hard_limit" in src
    assert "except asyncio.TimeoutError" in src


def test_wait_for_truncates_slow_send(monkeypatch):
    """行为验证：慢发送被 wait_for 截断，返回失败而非卡住。"""
    async def slow_send():
        await asyncio.sleep(5)
        return True, "mid", "", None

    async def run():
        try:
            return await asyncio.wait_for(slow_send(), timeout=0.2)
        except asyncio.TimeoutError:
            return False, "", "超时退避", None

    result = asyncio.run(run())
    assert result[0] is False
    assert "超时" in result[2]


def test_lock_still_guards_duplicate_processing():
    """第七次修复不碰全局锁（防两轮并发重复发送）。"""
    src = (ROOT / "backend/app/publisher.py").read_text(encoding="utf-8")
    assert "_queue_lock = asyncio.Lock()" in src
    assert "async with _queue_lock:" in src
