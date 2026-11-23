"""v2.1.10 验收：镜像流水线**串行**闸门（复制 → 改名 → 翻译 → 识别 → 入库资源库）。

用户口径（2026-11-23）：「复制、改名、翻译、识别直到入库到资源库才算第一轮任务完结」，
「如果上一轮任务还没跑完，下一轮定时任务不启动，直到上一轮完全结束才按新的定时
启动新一轮，避免任务冲突」。

本文件锁死的行为：
  1. 同一时刻只允许一轮流水线在跑（绝不并发）；
  2. 下一轮**排队等**上一轮彻底结束再跑（不丢轮次，也不像 v2.1.3 之前那样被饿死）；
  3. 等待**不冻结事件循环**（await 轮询，不占住 loop 线程）；
  4. 等待者先来先得；
  5. 等待超时只放弃本轮（下一轮定时会重来），绝不无限堆积；
  6. 四个定时入口（定时镜像 / 后台改名复查 / 定时扫描识别 / 复制任务轮询）
     与手动端点共用同一把闸门；
  7. 手动端点仍是 409 明确拒绝（不让长任务在 HTTP 请求里排队）。
"""
import asyncio
import os
import sys
import time
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

ROOT = Path(__file__).resolve().parents[1]


def _slice_func(src: str, header: str) -> str:
    """截出某个顶层函数的源码（到下一个顶层语句为止）。

    注意：函数头可能跨行（如 `_do_mirror_run(post_identify..., db: Session,`），
    必须从**头部所在行的下一行**开始扫描，否则第一段签名会被当成顶层语句而截断。
    """
    i = src.index(header)
    nl = src.index("\n", i)
    rest = src[nl + 1:]
    out = []
    for ln in rest.split("\n"):
        if ln and not ln[0].isspace() and ln.strip():
            break
        out.append(ln)
    return "\n".join(out)


def _strip_docstring(body: str) -> str:
    """去掉函数体开头的三引号文档串。

    文档串里常引用历史实现片段（例如「旧版是 acquire(blocking=False) 直接 return」），
    断言「函数体里不得出现某写法」时必须先把文档串摘掉，否则会被历史叙述误判。
    """
    lines = body.split("\n")
    if not lines or not lines[0].strip().startswith('"""'):
        return body
    if lines[0].strip().count('"""') >= 2:
        return "\n".join(lines[1:])
    for j in range(1, len(lines)):
        if '"""' in lines[j]:
            return "\n".join(lines[j + 1:])
    return body


@pytest.fixture
def src():
    return (ROOT / "backend/app/main.py").read_text(encoding="utf-8")


# ---------- 1. 互斥：同一时刻只允许一轮 ----------

def test_gate_is_mutually_exclusive():
    from app import main
    gate = main._TimedMirrorLock()
    assert gate.acquire(blocking=False, owner="第一轮") is True
    assert gate.acquire(blocking=False, owner="第二轮") is False, "闸门必须互斥"
    gate.release()
    assert gate.acquire(blocking=False, owner="第二轮") is True
    gate.release()
    assert gate.info()["busy"] is False


def test_next_round_never_overlaps_previous_round():
    """核心口径：上一轮没结束，下一轮不启动；上一轮结束后下一轮才跑。"""
    from app import main
    gate = main._TimedMirrorLock()
    timeline = []

    async def scenario():
        async def round_(name, work):
            await gate.acquire_async(name, timeout=10)
            timeline.append((name, "start"))
            try:
                await work()
            finally:
                timeline.append((name, "end"))
                gate.release()

        async def first_work():
            await asyncio.sleep(0.3)

        async def second_work():
            await asyncio.sleep(0.05)

        t1 = asyncio.create_task(round_("第一轮", first_work))
        await asyncio.sleep(0.05)          # 第一轮已开跑
        t2 = asyncio.create_task(round_("第二轮", second_work))
        await asyncio.gather(t1, t2)

    asyncio.run(scenario())
    assert timeline == [("第一轮", "start"), ("第一轮", "end"),
                        ("第二轮", "start"), ("第二轮", "end")], timeline
    assert timeline.index(("第一轮", "end")) < timeline.index(("第二轮", "start")), \
        "第二轮绝不能与第一轮并发"


def test_waiting_round_is_not_dropped():
    """等待中的那一轮必须真的跑完（不丢轮次）——v2.1.3 的病灶是「抢不到就跳过」。"""
    from app import main
    gate = main._TimedMirrorLock()
    ran = []

    async def scenario():
        assert gate.acquire(blocking=False, owner="占位")
        t = asyncio.create_task(gate.acquire_async("等待者", timeout=10))
        await asyncio.sleep(0.05)
        gate.release()
        ok = await t
        assert ok is True
        if ok:
            ran.append("等待者")
            gate.release()

    asyncio.run(scenario())
    assert ran == ["等待者"], "等待中的轮次不能被丢掉"


def test_acquire_async_does_not_block_event_loop():
    """等待期间事件循环必须还能跑别的任务（否则整个 loop 被冻住）。"""
    from app import main
    gate = main._TimedMirrorLock()
    ticks = []

    async def scenario():
        assert gate.acquire(blocking=False, owner="占位")

        async def ticker():
            for _ in range(5):
                await asyncio.sleep(0.02)
                ticks.append(1)

        t = asyncio.create_task(ticker())
        w = asyncio.create_task(gate.acquire_async("等待者", timeout=10))
        await asyncio.sleep(0.15)
        gate.release()
        await w
        await t
        gate.release()

    asyncio.run(scenario())
    assert len(ticks) >= 3, f"等待期间事件循环被冻住了：{ticks}"


def test_waiters_are_first_in_first_out():
    from app import main
    gate = main._TimedMirrorLock()
    order = []

    async def scenario():
        assert gate.acquire(blocking=False, owner="占位")

        async def waiter(name):
            ok = await gate.acquire_async(name, timeout=10)
            order.append(name)
            if ok:
                gate.release()

        t1 = asyncio.create_task(waiter("先来的"))
        await asyncio.sleep(0.05)
        t2 = asyncio.create_task(waiter("后到的"))
        await asyncio.sleep(0.05)
        gate.release()
        await asyncio.gather(t1, t2)

    asyncio.run(scenario())
    assert order == ["先来的", "后到的"], order


def test_acquire_async_gives_up_after_timeout():
    """等待超时只放弃本轮（下一轮定时会重来），不无限堆积。"""
    from app import main
    gate = main._TimedMirrorLock()

    async def scenario():
        assert gate.acquire(blocking=False, owner="占位")
        t0 = time.monotonic()
        ok = await gate.acquire_async("等不到的人", timeout=0.2)
        return ok, time.monotonic() - t0

    ok, elapsed = asyncio.run(scenario())
    assert ok is False
    assert elapsed < 6, elapsed


def test_gate_reports_waiting_count():
    from app import main
    gate = main._TimedMirrorLock()
    assert gate.info()["waiting"] == 0

    async def scenario():
        assert gate.acquire(blocking=False, owner="占位")
        t = asyncio.create_task(gate.acquire_async("等待者", timeout=3))
        await asyncio.sleep(0.05)
        n = gate.info()["waiting"]
        gate.release()
        await t
        gate.release()
        return n

    assert asyncio.run(scenario()) >= 1, "等待中的轮次必须能被界面看到"


def test_stale_holder_is_recovered_not_deadlocked():
    """上一轮真卡死（长时间无心跳）时，等待方能接管，不把后续轮次永久堵死。"""
    from app import main
    gate = main._TimedMirrorLock()
    old = gate.STALE_SECONDS
    try:
        gate.STALE_SECONDS = 0.1
        assert gate.acquire(blocking=False, owner="卡死的持有者") is True
        time.sleep(0.2)   # 超过 stale 阈值且没有心跳
        assert gate.acquire(blocking=False, owner="接管者") is True
        gate.release()
    finally:
        gate.STALE_SECONDS = old


# ---------- 2. 四个定时入口 + 手动端点共用一把闸门 ----------

def test_scheduled_mirror_waits_instead_of_skipping(src):
    body = _strip_docstring(_slice_func(src, "async def _scheduled_mirror()"))
    assert "acquire_async(" in body, "定时镜像必须走等待式闸门"
    assert "PIPELINE_WAIT_SECONDS" in body
    assert "acquire(blocking=False)" not in body, \
        "定时镜像不能「抢不到就跳过」——上一轮没结束就得等"


def test_rename_pending_waits_instead_of_skipping(src):
    body = _strip_docstring(_slice_func(src, "async def _job_rename_pending():"))
    assert "acquire_async(" in body, "后台改名复查必须走等待式闸门"
    assert "PIPELINE_WAIT_SECONDS" in body
    assert "acquire(blocking=False)" not in body, \
        "改名复查不能「抢不到就跳过」——那正是 v2.1.3 饿死英文名解析的病灶"
    assert "_rename_lock" not in body, "改名必须与复制 / 翻译共用同一把闸门"


def test_scheduled_scan_takes_the_gate(src):
    """定时扫描（扫描入库 → 识别翻译 → 推送）以前完全没有锁，会与镜像轮次并发。"""
    body = _strip_docstring(_slice_func(src, "async def _scheduled_scan(root: str)"))
    assert "acquire_async(" in body, "定时扫描必须持闸门"
    assert "_mirror_lock.release()" in body, "必须释放，否则后续轮次永远排队"


def test_poll_job_participates_in_the_gate(src):
    body = _strip_docstring(_slice_func(src, "async def _job_mirror_task_poll():"))
    assert "_mirror_lock.acquire(blocking=False" in body
    assert "_mirror_lock.release()" in body


def test_rename_lock_is_alias_of_the_same_gate():
    """v2.1.3 拆出来的独立改名锁已回归同一把闸门（用户口径：绝不并发）。"""
    from app import main
    assert main._rename_lock is main._mirror_lock


def test_manual_guard_still_returns_409():
    """手动端点保持「明确拒绝」，不让长任务在 HTTP 请求里排队。"""
    from fastapi import HTTPException
    from app import main
    with main._mirror_guard():
        with pytest.raises(HTTPException) as ei:
            with main._mirror_guard():
                pass
        assert ei.value.status_code == 409
        assert "已有镜像同步" in str(ei.value.detail)
    with main._mirror_guard():
        pass


def test_wait_timeout_is_configurable_constant():
    from app import main
    assert main.PIPELINE_WAIT_SECONDS > 0
    assert isinstance(main.PIPELINE_WAIT_SECONDS, float)


# ---------- 3. 改名门禁收敛到作品级 ----------

def test_rename_gate_no_longer_blocks_whole_round_by_default(src):
    """整轮拦截默认关闭：一部剧改名失败不该把同轮其它作品一起拦下。

    v2.1.0 门禁的原意（英文名没改对就不推群/频道）由推送侧的作品级闸门兜底。
    """
    assert "RENAME_GATE_BLOCK_WHOLE_ROUND = False" in src
    body = _strip_docstring(_slice_func(src, "async def _do_mirror_run("))
    assert "gate_block_round = rename_failed and RENAME_GATE_BLOCK_WHOLE_ROUND" in body
    assert "if do_pipeline and not gate_block_round:" in body


def test_push_gate_still_blocks_untranslated_works():
    """作品级闸门必须仍然拦得住「英文名没改对」的作品（红线不被放松）。"""
    from app import push_gate
    src = (ROOT / "backend/app/push_gate.py").read_text(encoding="utf-8")
    assert '"file_english"' in src
    assert "file_english" in push_gate.GATE_ITEMS
