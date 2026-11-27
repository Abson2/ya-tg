"""v2.1.54 验收：连接池隔离 —— 让"整站假死"不可能再发生。

真机事故（2026-11-27）：
  容器 Up 19 小时但 unhealthy；`/api/health`、`/docs`、所有业务接口全部超时；
  CPU 0%、进程都在。日志报
  `QueuePool limit of size 5 overflow 10 reached, connection timed out, timeout 30.00`。
  时间线：21:44 起 share_center 一轮不结束持续 skipped → 22:21 rename_pending 也被拖下水
  → 07:58 镜像锁 5296s 无心跳被判残留 → 09:14 起调度器大面积 missed。

根因不是某个接口慢，而是**结构**：
  ① 全部 HTTP 请求与全部后台 job 共用同一个 15 连接池（SQLAlchemy 默认 5+10）；
  ② `pool_timeout` 默认 30 秒 —— 池一满，每个请求都卡 30 秒，页面表现为完全无响应；
  ③ 池满时只有一句 TimeoutError，看不出是 job 池还是 API 池、谁占的。

本版改法：拆成 job 池（8+8，等 15s）与 API 池（10+20，等 5s），`get_db()` 走 API 池。
这样后台 job 再卡，用户至少能打开页面、看到状态；API 请求池满也只会"快速失败"，
不会全员卡 30 秒。

本文件的断言在改前应当红：`api_engine` 不存在 / 两个池是同一对象 / 归档过滤漏装。
"""
import asyncio
import inspect

import pytest
from sqlalchemy import select
from sqlalchemy.exc import TimeoutError as PoolTimeoutErrorKey

from app import db as dbmod
from app import main as mainmod
from app.db import ApiSessionLocal, SessionLocal, api_engine, engine, migrate, pool_status
from app.models import QueueItem, Resource, include_archived


# ---------------------------------------------------------------------------
# A. 两个池确实是两个池（3 项）
# ---------------------------------------------------------------------------
def test_api_engine_is_a_separate_engine():
    """API 池必须是独立 engine —— 同一个 engine 就没有隔离可言。"""
    assert api_engine is not engine
    assert api_engine.pool is not engine.pool


def test_job_pool_is_small_and_patient_api_pool_is_big_and_impatient():
    """job 池小、等得久；API 池大、等得短。

    这两个数字是这次事故的直接修正：
      - job 是长任务，排队久一点无所谓（15s），但容量要小，别把连接都锁在自己手里；
      - API 必须快速失败（5s），因为用户盯着页面，卡 30 秒等于整站不可用。
    """
    job_pool, api_pool = engine.pool, api_engine.pool
    assert (job_pool.size(), job_pool._max_overflow, job_pool._timeout) == (8, 8, 15)
    assert (api_pool.size(), api_pool._max_overflow, api_pool._timeout) == (10, 20, 5)
    # job 池总容量不许比改动前（默认 5+10=15）更小，否则是"用降容量换隔离"
    assert (job_pool.size() + job_pool._max_overflow) >= 15
    # API 池的总容量必须严格大于 job 池，否则"页面能活"就是空话
    assert (api_pool.size() + api_pool._max_overflow) > (job_pool.size() + job_pool._max_overflow)


def test_get_db_uses_the_api_maker():
    """`get_db` 是全站 HTTP 请求的唯一入口，它必须走 API 池。"""
    gen = dbmod.get_db()
    session = next(gen)
    try:
        assert session.get_bind() is api_engine
        assert session.get_bind() is not engine
    finally:
        gen.close()


# ---------------------------------------------------------------------------
# B. 归档过滤必须装到新池上（红线，3 项）
#
# v2.1.11 的硬规则：已归档的行绝不能漏给页面，否则会被重新识别、重新入队推送。
# 归档过滤是挂在 sessionmaker 上的事件 —— **新建 maker 而忘记装，就等于开了一个后门**。
# 这三项就是那道后门的守门测试。
# ---------------------------------------------------------------------------
@pytest.fixture()
def archived_rows():
    migrate()
    with SessionLocal() as s:
        for t in (Resource, QueueItem):
            s.query(t).delete()
        s.commit()
        # 一条正常行 + 一条已归档行
        s.add(Resource(file_id="f1", name="normal", clean_state=None))
        s.add(Resource(file_id="f2", name="archived", clean_state="cleaned"))
        s.add(QueueItem(resource_id=1, channel_id=1, error="normal", clean_state=None))
        s.add(QueueItem(resource_id=2, channel_id=1, error="archived", clean_state="cleaned"))
        s.commit()
    yield
    with SessionLocal() as s:
        for t in (Resource, QueueItem):
            s.query(t).delete()
        s.commit()


def test_api_session_hides_archived_resources(archived_rows):
    with ApiSessionLocal() as s:
        names = [r.name for r in s.scalars(select(Resource)).all()]
    assert "normal" in names, "正常行被误过滤了"
    assert "archived" not in names, "API 池漏出了已归档资源 —— 归档过滤没装到 ApiSessionLocal"


def test_api_session_hides_archived_queue_items(archived_rows):
    with ApiSessionLocal() as s:
        keys = [q.error for q in s.scalars(select(QueueItem)).all()]
    assert "normal" in keys
    assert "archived" not in keys, "API 池漏出了已归档队列项"


def test_include_archived_still_works_on_api_session(archived_rows):
    """旁路开关也要在两个池上都有效，否则「恢复归档」这类功能在 API 侧用不了。"""
    with ApiSessionLocal() as s:
        with include_archived():
            names = [r.name for r in s.scalars(select(Resource)).all()]
    assert "archived" in names


# ---------------------------------------------------------------------------
# C. 池占用可观测（2 项）
# ---------------------------------------------------------------------------
def test_pool_status_reports_both_pools():
    st = pool_status()
    assert set(st) == {"job", "api"}
    for name in ("job", "api"):
        assert set(st[name]) == {"size", "checkedout", "overflow", "timeout"}
        assert st[name]["size"] > 0


def test_pool_timeout_handler_is_registered():
    """池满时必须收成 503，而不是让请求抛 500 或干等。

    事故当时的表现是"什么反应都没有"；有了这个处理器，用户会看到一句明确的
    "服务繁忙"，日志里还会带上两个池的占用数字。
    """
    handlers = mainmod.app.exception_handlers
    assert PoolTimeoutErrorKey in handlers, "没有注册连接池超时的异常处理器"
    fn = handlers[PoolTimeoutErrorKey]
    assert inspect.iscoroutinefunction(fn)


# ---------------------------------------------------------------------------
# D. 分享中心单轮硬上限（3 项）
# ---------------------------------------------------------------------------
def test_share_center_cycle_timeout_constant_is_sane():
    assert 30 <= mainmod.SHARE_CENTER_CYCLE_TIMEOUT_SECONDS <= 900


def test_run_cycle_is_wrapped_in_wait_for():
    """静态守门：整轮必须被 wait_for 包住。只包建分享是不够的 ——
    真实事故里卡住的正是"某一轮整体不返回"。"""
    src = inspect.getsource(mainmod._run_share_center_v2_cycle_in_thread)
    assert "asyncio.wait_for" in src
    assert "SHARE_CENTER_CYCLE_TIMEOUT_SECONDS" in src
    assert "asyncio.TimeoutError" in src


def test_stuck_cycle_returns_instead_of_hanging(monkeypatch):
    """行为反证：让 run_cycle 卡死，函数必须在超时后**返回**而不是挂住。

    这是本次事故的直接复现场景（一轮跑 10 小时不结束）。改前这里会真的挂住，
    所以改前跑这个用例会超时失败；改后应快速返回 `timeout: True`。
    """
    from app import share_center_v2

    monkeypatch.setattr(mainmod, "SHARE_CENTER_CYCLE_TIMEOUT_SECONDS", 0.2, raising=False)

    async def _never_returns(db, limit=20):
        await asyncio.sleep(30)          # 模拟卡住的一轮

    monkeypatch.setattr(share_center_v2, "run_cycle", _never_returns)
    monkeypatch.setattr(share_center_v2, "ensure_defaults", lambda db: None)

    out = mainmod._run_share_center_v2_cycle_in_thread(limit=1)
    assert out.get("timeout") is True
    assert out.get("error") is True
