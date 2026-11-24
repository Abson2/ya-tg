"""v2.1.24：镜像定时同步的「注册可见 + 错过补跑」。

真机问题（2026-11-24 排查，只读、未改线上数据）：
  同步间隔设 15 分钟，但 14:02 起 last_run 一直停在 13:47，后台进程在跑、
  镜像界面「同步执行中」卡住，30 分钟没有任何一轮定时同步触发。

根因（三条，都被绕过了）：
  ① 注册点只有启动与保存配置两处，而**启动那处是 `try: ... except Exception: pass`**
     —— 注册失败被静默吞掉，日志、审计、页面全无痕迹；
  ② 状态接口只有 `last_run`，没有「下次触发时间」，页面上「定时没注册」和
     「刚跑过」长得一模一样，用户无从判断；
  ③ 进程停机期间错过的窗口**不补**：IntervalTrigger 从启动时刻重新计时，
     「重启一次 = 往后顺延一个间隔」。

本文件锁死修复后的三条行为：
  ① `_schedule_mirror_job` 用统一 job id 注册、间隔下限 5 分钟、关掉就摘掉；
  ② `_mirror_next_run_at()` 取不到就返回 None（绝不编时间），取到就转上海时间串；
  ③ `_mirror_missed_window()` 只补**一轮**（镜像同步是幂等全量比对，补最新一轮够）；
  ④ 启动路径不再静默吞异常，失败必落日志 + 审计；
  ⑤ 两个接口都暴露 `schedule`（registered / next_run_at / interval_minutes / enabled）；
  ⑥ 前端三态如实展示，未注册时明确告警。
"""
import inspect
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app import main

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
MAIN_SRC = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# 假调度器：只实现被测代码真正用到的那几个方法
# ---------------------------------------------------------------------------
class _FakeJob:
    def __init__(self, next_run_time=None):
        self.next_run_time = next_run_time


class _FakeScheduler:
    def __init__(self, jobs=None):
        self.jobs = dict(jobs or {})
        self.added = []
        self.removed = []

    def get_job(self, job_id):
        return self.jobs.get(job_id)

    def remove_job(self, job_id):
        self.removed.append(job_id)
        self.jobs.pop(job_id, None)

    def add_job(self, func, trigger, **kwargs):
        self.added.append({"func": func, "trigger": trigger, **kwargs})
        self.jobs[kwargs.get("id")] = _FakeJob()


@pytest.fixture
def fake_sched(monkeypatch):
    sched = _FakeScheduler()
    monkeypatch.setattr(main, "scheduler", sched)
    return sched


# ---------------------------------------------------------------------------
# ① 注册：统一 id / 间隔下限 / 关掉就摘
# ---------------------------------------------------------------------------
def test_schedule_registers_with_stable_job_id(fake_sched):
    main._schedule_mirror_job(15, True)
    assert len(fake_sched.added) == 1
    job = fake_sched.added[0]
    assert job["id"] == main.MIRROR_JOB_ID == "mirror_sync"
    assert job["func"] is main._scheduled_mirror
    assert job["replace_existing"] is True
    assert job["max_instances"] == 1
    assert job["coalesce"] is True


def test_schedule_clamps_interval_floor(fake_sched):
    """下限 5 分钟：间隔填 1 分钟会打爆光鸭写接口，注册层就要挡住。"""
    main._schedule_mirror_job(1, True)
    trigger = fake_sched.added[0]["trigger"]
    assert trigger.interval == timedelta(minutes=main.MIRROR_MIN_INTERVAL_MINUTES)


def test_schedule_disabled_removes_job(fake_sched):
    fake_sched.jobs[main.MIRROR_JOB_ID] = _FakeJob()
    main._schedule_mirror_job(15, False)
    assert main.MIRROR_JOB_ID in fake_sched.removed
    assert not fake_sched.added, "关掉定时同步不该再注册 job"
    assert main.MIRROR_JOB_ID not in fake_sched.jobs


def test_schedule_replaces_previous_job(fake_sched):
    """改间隔要先把旧 job 摘掉，否则同一 id 两句 add_job 的语义容易想当然。"""
    fake_sched.jobs[main.MIRROR_JOB_ID] = _FakeJob()
    main._schedule_mirror_job(60, True)
    assert main.MIRROR_JOB_ID in fake_sched.removed
    assert fake_sched.added[0]["id"] == main.MIRROR_JOB_ID


# ---------------------------------------------------------------------------
# ①b 真正的根因：misfire_grace_time 默认只有 1 秒
#
# APScheduler 3.11 的 job_defaults 是 {'misfire_grace_time': 1, 'coalesce': True,
# 'max_instances': 1}。**晚到 1 秒以上就整轮丢掉**，只留一行 "was missed by"。
# 本容器镜像一轮跑几十分钟、事件循环被同步 DB 调用占着 → 定时镜像晚到几秒是常态
# → 「定时从来不跑，手动点立即同步完全正常」（手动路径不过调度器）。
# ---------------------------------------------------------------------------
def test_apscheduler_default_grace_is_one_second():
    """把前提钉住：这条一旦被上游改成别的值，下面的宽限断言就失去意义。"""
    import asyncio
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.interval import IntervalTrigger

    async def probe():
        s = AsyncIOScheduler()
        s.add_job(lambda: None, IntervalTrigger(minutes=15), id="probe")
        s.start()
        try:
            return s.get_job("probe").misfire_grace_time
        finally:
            s.shutdown(wait=False)

    assert asyncio.run(probe()) == 1


def test_schedule_sets_generous_misfire_grace(fake_sched):
    main._schedule_mirror_job(15, True)
    grace = fake_sched.added[0]["misfire_grace_time"]
    assert grace >= main.MIRROR_MISFIRE_GRACE_FLOOR_SECONDS, grace


def test_misfire_grace_scales_with_interval_but_is_capped():
    # 5 分钟档：按间隔 300 秒，正好等于下限
    assert main._mirror_misfire_grace(5) == 300
    # 15 分钟档：900 秒（整段间隔都允许晚）
    assert main._mirror_misfire_grace(15) == 900
    # 6 小时档：封顶 1 小时，不跟着无限放大
    assert main._mirror_misfire_grace(360) == main.MIRROR_MISFIRE_GRACE_CAP_SECONDS
    # 脏数据（0 / 负数 / None）落到下限，绝不返回 0 或负数
    for bad in (0, -5, None):
        assert main._mirror_misfire_grace(bad) == main.MIRROR_MISFIRE_GRACE_FLOOR_SECONDS


def test_real_scheduler_job_carries_grace(monkeypatch):
    """真起一个 AsyncIOScheduler，确认注册出来的 job 真的带上了宽限值。

    上面几条只验参数传进去了；这条验「参数确实落到了 job 上」—— 两回事。
    """
    import asyncio
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    async def probe():
        sched = AsyncIOScheduler()
        monkeypatch.setattr(main, "scheduler", sched)
        main._schedule_mirror_job(15, True)
        sched.start()
        try:
            job = sched.get_job(main.MIRROR_JOB_ID)
            assert job is not None, "定时镜像没注册上"
            assert job.misfire_grace_time == 900
            assert job.next_run_time is not None, "注册了却没有下次触发时间"
            return job.misfire_grace_time
        finally:
            sched.shutdown(wait=False)

    assert asyncio.run(probe()) == 900


def test_scan_job_also_gets_grace():
    """同一类 bug 在定时扫描上也要堵住（默认 1 秒同样会丢轮次）。"""
    assert MAIN_SRC.count("misfire_grace_time=300") >= 2


def test_mirror_lock_timeout_is_warning_not_info():
    """「本轮定时镜像放弃」是真正丢轮次，必须 WARNING 级并且带占用者。"""
    assert 'logger.warning("[mirror] 等待上一轮任务结束超时' in MAIN_SRC


# ---------------------------------------------------------------------------
# ② 下次触发时间：取不到就 None，绝不编
# ---------------------------------------------------------------------------
def test_next_run_at_none_when_not_registered(fake_sched):
    assert main._mirror_next_run_at() is None


def test_next_run_at_none_when_job_has_no_time(fake_sched):
    """job 在、但还没算出 next_run_time（scheduler 未 start）→ None，不是编一个。"""
    fake_sched.jobs[main.MIRROR_JOB_ID] = _FakeJob(next_run_time=None)
    assert main._mirror_next_run_at() is None


def test_next_run_at_formats_in_shanghai(fake_sched):
    utc = datetime(2026, 11, 24, 7, 2, 0, tzinfo=timezone.utc)
    fake_sched.jobs[main.MIRROR_JOB_ID] = _FakeJob(next_run_time=utc)
    assert main._mirror_next_run_at() == "2026-11-24 15:02:00"


def test_next_run_at_survives_broken_job(fake_sched):
    class _Broken:
        @property
        def next_run_time(self):
            raise RuntimeError("boom")

    fake_sched.jobs[main.MIRROR_JOB_ID] = _Broken()
    # 取不到就留空：状态接口不能因为一个时间字段整页 500
    assert main._mirror_next_run_at() is None


# ---------------------------------------------------------------------------
# ③ 错过窗口补跑
# ---------------------------------------------------------------------------
def test_missed_window_true_when_never_run():
    """启用但从未跑过 → 该立刻补一轮（否则新库永远等第一个间隔）。"""
    assert main._mirror_missed_window(None, 15) is True


def test_missed_window_false_within_interval():
    now = datetime(2026, 11, 24, 15, 0, tzinfo=timezone.utc)
    assert main._mirror_missed_window(now - timedelta(minutes=5), 15, now=now) is False


def test_missed_window_true_after_interval():
    now = datetime(2026, 11, 24, 15, 0, tzinfo=timezone.utc)
    assert main._mirror_missed_window(now - timedelta(minutes=20), 15, now=now) is True


def test_missed_window_handles_naive_datetime():
    """SQLite 读回的是 naive（不带 tz），必须按 UTC 处理而不是抛 TypeError。"""
    now = datetime(2026, 11, 24, 15, 0, tzinfo=timezone.utc)
    naive = (now - timedelta(minutes=20)).replace(tzinfo=None)
    assert main._mirror_missed_window(naive, 15, now=now) is True
    naive_fresh = (now - timedelta(minutes=2)).replace(tzinfo=None)
    assert main._mirror_missed_window(naive_fresh, 15, now=now) is False


def test_missed_window_zero_interval_is_never_due():
    assert main._mirror_missed_window(None, 0) is False


def test_missed_window_uses_interval_floor():
    """间隔被填成 1 分钟时按 5 分钟判，和注册层的下限保持同一口径。"""
    now = datetime(2026, 11, 24, 15, 0, tzinfo=timezone.utc)
    assert main._mirror_missed_window(now - timedelta(minutes=3), 1, now=now) is False
    assert main._mirror_missed_window(now - timedelta(minutes=6), 1, now=now) is True


# ---------------------------------------------------------------------------
# ④ 启动路径不再静默吞异常
# ---------------------------------------------------------------------------
def test_startup_logs_instead_of_swallowing():
    """反证：启动注册失败必须有日志与审计，不能再是 `except Exception: pass`。"""
    assert 'logger.error("[mirror] 启动注册定时同步失败' in MAIN_SRC
    assert '"mirror_schedule_failed"' in MAIN_SRC
    # 老写法（裸 pass 吞掉）不得残留
    assert not re.search(
        r"_schedule_mirror_job\(mirror_minutes, True\)\s*\n\s*except Exception:\s*\n\s*pass",
        MAIN_SRC), "启动注册仍是静默吞异常"


def test_startup_schedules_catchup_job():
    assert 'id=MIRROR_CATCHUP_JOB_ID' in MAIN_SRC
    assert "MIRROR_CATCHUP_DELAY_SECONDS" in MAIN_SRC
    # 补跑必须先判断「是否真的错过」，否则每次重启都无条件跑一轮
    assert "_mirror_missed_window(mirror_last_run, mirror_minutes)" in MAIN_SRC


def test_startup_reads_last_run_inside_session():
    """`mr.last_run` 必须在会话内取出来 —— 出了 with 块是 detached，读属性会炸。"""
    assert "mirror_last_run = mr.last_run if mr else None" in MAIN_SRC
    assert "_mirror_missed_window(mr.last_run" not in MAIN_SRC


# ---------------------------------------------------------------------------
# ⑤ 两个接口都暴露调度状态
# ---------------------------------------------------------------------------
def test_status_endpoint_exposes_schedule():
    src = MAIN_SRC[MAIN_SRC.index('@app.get("/api/mirror/status"'):]
    src = src[:src.index('@app.', 10)]
    assert '"schedule"' in src
    assert "MIRROR_JOB_ID" in src
    assert "next_run_at" in src
    assert "registered" in src


def test_config_endpoint_reports_schedule_error():
    """保存配置时注册失败不能假装成功：配置照存，但把失败原因回给前端。"""
    assert '"schedule_error"' in MAIN_SRC
    assert "next_run_at" in MAIN_SRC


# ---------------------------------------------------------------------------
# ⑥ 前端三态展示
# ---------------------------------------------------------------------------
def test_frontend_shows_next_run_three_states():
    assert "mirrorStatus.schedule" in APP_VUE
    # 有下次时间
    assert "下次自动同步" in APP_VUE
    # 已注册但还没算出时间
    assert "定时已注册，正在计算下次触发时间" in APP_VUE
    # 未注册必须**明确告警**，不能留空糊过去
    assert "定时任务未注册" in APP_VUE
    assert "mirror-sched-dot warn" in APP_VUE
    # 关闭态也要说清楚（否则用户以为还在定时跑）
    assert "定时同步已关闭" in APP_VUE


def test_frontend_surfaces_schedule_error_on_save():
    assert "r.schedule_error" in APP_VUE
    assert "定时任务注册失败" in APP_VUE


def test_frontend_does_not_show_raw_field_names():
    """页面不得体现后端代码名词：展示的是中文，不是 schedule_registered 这种键名。"""
    seg = APP_VUE[APP_VUE.index('class="mirror-sched"'):]
    seg = seg[:seg.index("\n")]
    for bad in ("schedule_registered", "MIRROR_JOB_ID", "next_run_time", "mirror_sync"):
        assert bad not in seg, bad
