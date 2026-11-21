"""阶段四 B-01：队列处理进程内互斥。

定时 worker 与 HTTP「立即处理一次」共享 pending 队列，原实现无锁可并发，
导致重复建分享 / 重复发 TG。验证：
  - 正有一轮在跑时，第二次调用非阻塞跳过（skipped=True），等待第一次完成；
  - 第一轮结束后锁释放，可再次正常处理。
"""
import asyncio

import pytest

from app import publisher


def test_process_queue_is_mutex():
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = [0]

    async def slow_impl(limit=20):
        calls[0] += 1
        entered.set()
        await release.wait()
        return {"sent": 1, "failed": 0, "shares_created": 0}

    orig = publisher._process_queue_impl
    publisher._process_queue_impl = slow_impl
    # 确保初始未持锁
    assert not publisher._queue_lock.locked()

    async def scenario():
        first = asyncio.create_task(publisher.process_queue())
        await entered.wait()  # 第一轮已进入并持锁
        second = await publisher.process_queue()  # 第二轮应非阻塞跳过
        release.set()
        result1 = await first
        return second, result1

    try:
        second, first = asyncio.run(scenario())
    finally:
        publisher._process_queue_impl = orig

    # v1.5.2：锁冲突标志从 bool skipped 改为 queue_busy——skipped 必须留给
    # 手动推送端点的字符串数组契约，否则前端展开 bool 抛 TypeError。
    assert second.get("queue_busy") is True
    assert "skipped" not in second
    assert second["sent"] == 0
    assert first["sent"] == 1
    assert calls[0] == 1  # 慢实现只被调用一次
    assert not publisher._queue_lock.locked()  # 锁已释放
