"""v2.1.19：推送排期「匀速放行」+「今日排期预测」。

用户两句话定义了本版：

  ① 发布规则不只是分流规则，而是调动资源库存量、有序推送的**中枢**；
  ② 每时段的推送任务应当**均匀分配**，不能时间一到就一股脑全推完。

① 对应 `forecast_schedule`：回答「今天接下来推什么、几点推、还能推多少、卡在哪」。
② 对应 `_hourly_due`：把原来的「整点配额突发」改成「按分钟线性放行」。

本文件锁死：
  - cap=3 时第 0/20/40 分钟各 1 部（**反证**：整点不得一次放 3 部）；
  - 放行额度单调不减、永不超过 cap；
  - `_sched_allows_core` 与原闭包逐条等价（无排期行放行 / 窗口边界 / 跨午夜 / 上限）；
  - `forecast_schedule` **只读**（调用前后库内容一致）、如实报卡点、不编造。
"""
import json
from datetime import datetime, timedelta

import pytest
from sqlalchemy import delete, func, select

from app import share_center_v2 as sc2
from app import utils
from app.db import SessionLocal, migrate
from app.models import (
    Bot,
    Channel,
    Resource,
    SCCategory,
    SCCategoryCursor,
    SCCategorySchedule,
    SCLibraryVersion,
    SCPublishTask,
    SCPushList,
    SCPushListItem,
    SCSharePlan,
    SCSharePlanVersion,
    SCWorkCategory,
)

from conftest import ensure_full_day_hour_window

WIPED = (SCPublishTask, SCPushListItem, SCPushList, SCLibraryVersion,
         SCCategoryCursor, SCCategorySchedule, SCWorkCategory, SCCategory,
         SCSharePlanVersion, SCSharePlan, Resource, Channel, Bot)


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    for _ in range(2):
        with SessionLocal() as db:
            for model in WIPED:
                db.execute(delete(model))
            db.commit()
    yield


# --------------------------------------------------------------------------
# 造数据
# --------------------------------------------------------------------------
def _channel(db, chat_id="-1001234567890"):
    bot = Bot(name="b", token="x", enabled=True)
    db.add(bot)
    db.flush()
    ch = Channel(bot_id=bot.id, name="频道", chat_id=chat_id,
                 kind="channel", enabled=True)
    db.add(ch)
    db.flush()
    return ch


def _work(db, *, title, category_key="genre:剧情", file_id="f-1"):
    """造一部「在库且可排」的作品：Resource + 最新快照 + 分类映射。

    关键：`work_key` 必须用 `utils.work_group_key(res)` **实算**，
    否则和 `_live_work_keys` 算出来的键对不上，forecast 会当成「不在库」直接跳过。
    """
    res = Resource(file_id=file_id, name=f"{title}.S01E01.mkv", ext=".mkv",
                   size=100, utime=1, identify_status="identified", media_type="tv",
                   tmdb_id=1, media_title=title, share_dir_name=title,
                   share_status="not_created", share_url="")
    db.add(res)
    db.flush()
    work_key = utils.work_group_key(res)
    lv = SCLibraryVersion(work_key=work_key, version_no=1, fingerprint="fp",
                          media_type="tv", resource_ids_json=json.dumps([res.id]),
                          metadata_json=json.dumps({"title": title}),
                          change_type="first")
    db.add(lv)
    db.flush()
    db.add(SCWorkCategory(work_key=work_key, category_key=category_key))
    db.commit()
    return res, lv, work_key


def _plan(db, *, category_limit=20, work_limit=50, extra=None):
    plan = SCSharePlan(name="p", plan_type="daily", enabled=True)
    db.add(plan)
    db.flush()
    cfg = {"category_limit": category_limit, "work_limit": work_limit}
    if extra:
        cfg.update(extra)
    ver = SCSharePlanVersion(plan_id=plan.id, version_no=1, status="active",
                             config_json=json.dumps(cfg))
    db.add(ver)
    db.flush()
    ensure_full_day_hour_window(db, ver.id)
    db.commit()
    return ver


def _category(db, key, name, priority=0):
    db.add(SCCategory(category_key=key, name=name, enabled=True, priority=priority))
    db.commit()


def _forecast():
    with SessionLocal() as db:
        return sc2.forecast_schedule(db)


def _snapshot():
    """把库里的关键内容拍成快照，用于断言「只读」。"""
    with SessionLocal() as db:
        return {
            "tasks": db.scalar(select(func.count(SCPublishTask.id))),
            "items": db.scalar(select(func.count(SCPushListItem.id))),
            "lists": db.scalar(select(func.count(SCPushList.id))),
            "lv": db.scalar(select(func.count(SCLibraryVersion.id))),
            "cursors": db.scalar(select(func.count(SCCategoryCursor.id))),
            "task_status": [r[0] for r in db.execute(
                select(SCPublishTask.status).order_by(SCPublishTask.id)).all()],
        }


# --------------------------------------------------------------------------
# ① 匀速放行：核心是「不许整点突发」
# --------------------------------------------------------------------------
def test_hourly_due_does_not_burst_at_the_top_of_the_hour():
    """反证：cap=3 时整点只能放 1 部，绝不能一次放 3 部。

    这正是用户说的「时间一到就一股脑全推送」——旧实现是
    `submitted_this_hour >= hourly_cap`，整点瞬间就允许 cap 部。
    """
    cap = 3
    assert sc2._hourly_due(cap, 0) == 1, "整点只应放行 1 部"
    assert sc2._hourly_due(cap, 0) < cap, "整点放行数必须小于 cap（否则就是突发）"


def test_hourly_due_spreads_three_works_across_the_hour():
    """cap=3 → 第 0/20/40 分钟各 1 部，第 60 分钟满额。"""
    cap = 3
    assert sc2._hourly_due(cap, 0) == 1
    assert sc2._hourly_due(cap, 19) == 1
    assert sc2._hourly_due(cap, 20) == 2
    assert sc2._hourly_due(cap, 39) == 2
    assert sc2._hourly_due(cap, 40) == 3
    assert sc2._hourly_due(cap, 59) == 3


def test_hourly_due_is_monotonic_and_never_exceeds_cap():
    for cap in (1, 2, 3, 7, 24, 60):
        prev = 0
        for minute in range(60):
            due = sc2._hourly_due(cap, minute)
            assert due >= prev, "放行额度必须单调不减（cap=%d, m=%d）" % (cap, minute)
            assert due <= cap, "放行额度不得超过 cap（cap=%d, m=%d）" % (cap, minute)
            prev = due
        assert sc2._hourly_due(cap, 59) == cap, "第 60 分钟必须满额"


def test_hourly_due_zero_or_negative_means_unlimited():
    assert sc2._hourly_due(0, 10) > 10 ** 6
    assert sc2._hourly_due(-5, 10) > 10 ** 6


def test_uniform_minute_is_consistent_with_due():
    """`_uniform_minute` 反推的时间点，必须与 `_hourly_due` 的放行点一致。"""
    for cap in (1, 3, 6, 12):
        for idx in range(1, cap + 1):
            m = sc2._uniform_minute(cap, idx)
            assert sc2._hourly_due(cap, m) >= idx, \
                "第 %d 部在 %d 分应已放行（cap=%d）" % (idx, m, cap)
            if m > 0:
                assert sc2._hourly_due(cap, m - 1) < idx, \
                    "第 %d 部不该早于 %d 分放行（cap=%d）" % (idx, m, cap)


# --------------------------------------------------------------------------
# ② `_sched_allows_core`：与原闭包逐条等价
# --------------------------------------------------------------------------
def _sched(*, ws=8, we=10, week=""):
    return SCCategorySchedule(plan_version_id=1, category_key="k",
                              window_start=ws, window_end=we,
                              week_windows=week, enabled=True)


def test_sched_core_passes_when_no_schedule_row():
    """没有排期行的分类必须放行——`_ensure_schedules` 只给 TMDB genre 建行，
    `genre:unknown` 这类兜底桶本来就没有行，若不放行它们永远排不上。"""
    assert sc2._sched_allows_core(None, hour=3, weekday=1, per_category_count=0,
                                  cycle_hour_max=0, cycle_day_max=0) is True


def test_sched_core_window_boundaries():
    s = _sched(ws=8, we=10)
    assert sc2._sched_allows_core(s, hour=8, weekday=1, per_category_count=0,
                                  cycle_hour_max=0, cycle_day_max=0) is True
    assert sc2._sched_allows_core(s, hour=9, weekday=1, per_category_count=0,
                                  cycle_hour_max=0, cycle_day_max=0) is True
    # 右开区间：10 点不属于 8-10
    assert sc2._sched_allows_core(s, hour=10, weekday=1, per_category_count=0,
                                  cycle_hour_max=0, cycle_day_max=0) is False
    assert sc2._sched_allows_core(s, hour=7, weekday=1, per_category_count=0,
                                  cycle_hour_max=0, cycle_day_max=0) is False


def test_sched_core_cross_midnight():
    s = _sched(ws=22, we=3)
    for h, expect in ((22, True), (23, True), (0, True), (2, True), (3, False), (12, False)):
        assert sc2._sched_allows_core(s, hour=h, weekday=1, per_category_count=0,
                                      cycle_hour_max=0, cycle_day_max=0) is expect, h


def test_sched_core_same_start_end_means_all_day():
    s = _sched(ws=5, we=5)
    for h in (0, 5, 12, 23):
        assert sc2._sched_allows_core(s, hour=h, weekday=1, per_category_count=0,
                                      cycle_hour_max=0, cycle_day_max=0) is True


def test_sched_core_hour_and_day_caps():
    s = _sched(ws=8, we=12)
    assert sc2._sched_allows_core(s, hour=9, weekday=1, per_category_count=1,
                                  cycle_hour_max=2, cycle_day_max=0) is True
    assert sc2._sched_allows_core(s, hour=9, weekday=1, per_category_count=2,
                                  cycle_hour_max=2, cycle_day_max=0) is False
    assert sc2._sched_allows_core(s, hour=9, weekday=1, per_category_count=0,
                                  cycle_hour_max=0, cycle_day_max=5,
                                  today_count=5) is False
    assert sc2._sched_allows_core(s, hour=9, weekday=1, per_category_count=0,
                                  cycle_hour_max=0, cycle_day_max=5,
                                  today_count=4) is True


# --------------------------------------------------------------------------
# ③ forecast_schedule：前置条件与「只读」
# --------------------------------------------------------------------------
def test_forecast_without_active_version_reports_reason():
    with SessionLocal() as db:
        _channel(db)
    out = _forecast()
    assert out["ok"] is False
    assert "计划版本" in out["reason"]
    assert out["hours"] == []


def test_forecast_without_channel_reports_reason():
    with SessionLocal() as db:
        _plan(db)
        _category(db, "genre:剧情", "剧情")
    out = _forecast()
    assert out["ok"] is False
    assert "渠道" in out["reason"]


def test_forecast_is_strictly_readonly():
    """反证：预测绝不能顺手建任务 / 改状态 / 写游标。"""
    with SessionLocal() as db:
        _plan(db)
        _category(db, "genre:剧情", "剧情")
        _channel(db)
        _work(db, title="测试剧")
    before = _snapshot()
    out = _forecast()
    assert out["ok"] is True
    after = _snapshot()
    assert before == after, "forecast_schedule 不得写库，实际发生了变化：%s -> %s" % (
        before, after)


# --------------------------------------------------------------------------
# ④ forecast_schedule：排期内容
# --------------------------------------------------------------------------
def test_forecast_lists_hours_and_titles_for_backlog():
    with SessionLocal() as db:
        _plan(db)
        _category(db, "genre:剧情", "剧情")
        _channel(db)
        _work(db, title="测试剧")
    out = _forecast()
    assert out["ok"] is True
    assert out["today_total"] >= 1, "库里有可排作品，今天应当排得上"
    assert out["hours"], "必须给出逐小时排期"
    first = next(h for h in out["hours"] if h["total"] > 0)
    item = first["items"][0]
    assert item["category"] == "剧情", "分类要回显官方中文名，不能露 category_key"
    assert "测试剧" in item["titles"], "要能看到具体作品名"
    assert all(len(t) == 5 and t[2] == ":" for t in item["times"]), \
        "时间点必须是 HH:MM，实际 %s" % item["times"]


def test_forecast_times_are_paced_not_all_at_the_top(monkeypatch):
    """cap=3 时，同一小时的 3 个时间点必须摊开，不能都在 :00。

    时间固定在**本小时第 40 分钟**：匀速额度 `_hourly_due(3, 40) == 3`，
    当前小时的配额是满的，才排得下 3 部。若按真实时刻跑，整点刚过时
    `_hourly_due` 只放 1 部，这条用例会因为「本小时额度不够」而假红。
    """
    from datetime import timezone as _tz
    _real = sc2.utcnow()
    frozen = _real.replace(minute=40, second=0, microsecond=0)
    monkeypatch.setattr(sc2, "utcnow", lambda: frozen)
    assert sc2._hourly_due(3, frozen.astimezone(
        _tz(timedelta(hours=8))).minute) == 3

    with SessionLocal() as db:
        _plan(db)
        _category(db, "genre:剧情", "剧情")
        _channel(db)
        for i in range(3):
            _work(db, title="剧%d" % i, file_id="f-%d" % i)
    out = _forecast()
    rows = [h for h in out["hours"] if h["total"] >= 3]
    assert rows, "至少应有一个小时排到 3 部"
    times = rows[0]["items"][0]["times"]
    minutes = [int(t.split(":")[1]) for t in times]
    assert len(set(minutes)) == len(minutes), "同一小时的时间点不得重复"
    assert minutes != sorted([0] * len(minutes)), "不得全部堆在整点"
    assert minutes == [0, 20, 40], f"cap=3 应摊在 0/20/40 分钟，实际 {minutes}"


def test_forecast_skips_work_already_pushed_with_valid_link():
    """已推且链接仍有效的作品不能再排——否则预测会虚报。"""
    with SessionLocal() as db:
        _plan(db)
        _category(db, "genre:剧情", "剧情")
        ch = _channel(db)
        _res, _lv, wk = _work(db, title="已推剧")
        lst = SCPushList(list_key="l-1", name="t", status="active")
        db.add(lst)
        db.flush()
        item = SCPushListItem(list_id=lst.id, work_key=wk, chat_id=str(ch.chat_id),
                              dedup_key="%s:%s:first" % (wk, ch.chat_id),
                              status="published")
        db.add(item)
        db.flush()
        db.add(SCPublishTask(list_item_id=item.id, work_key=wk, kind="first",
                             status="published", share_url="https://t.me/c/1/2",
                             link_state="active"))
        db.commit()
    out = _forecast()
    assert out["ok"] is True
    assert out["today_total"] == 0, "已推且链接有效，不该再排"
    assert out["overflow"] == 0


def test_forecast_reports_category_limit_blocked_works():
    """分类数超过上限时，被截断的分类里若有在库作品，必须如实报出来。

    这不是凑数的字段——它就是「存量推不出去」的根因提示：
    调度只遍历前 category_limit 个分类，后面的分类连遍历都进不去。
    """
    with SessionLocal() as db:
        _plan(db, category_limit=2)
        _category(db, "genre:剧情", "剧情", priority=9)
        _category(db, "genre:动作", "动作", priority=8)
        _category(db, "genre:未分类", "未分类", priority=7)
        _channel(db)
        _work(db, title="被挡住的剧", category_key="genre:未分类", file_id="f-x")
    out = _forecast()
    assert out["ok"] is True
    assert out["blocked"], "被截断的分类有作品，必须报卡点"
    names = [b["category"] for b in out["blocked"]]
    assert "未分类" in names, "实际 %s" % names
    assert "上限" in out["reason"]


def test_forecast_notes_when_hourly_cap_missing():
    """没配每小时上限时，要提示「会一到位就推」，而不是假装均匀。"""
    with SessionLocal() as db:
        _plan(db)
        _category(db, "genre:剧情", "剧情")
        _channel(db)
        _work(db, title="测试剧")
        from app import runtime_config
        runtime_config.set_many(db, {"sc_quick_publish_enabled": "false"})
        db.commit()
    out = _forecast()
    assert out["ok"] is True
    assert any("每小时上限" in n for n in out["notes"]), out["notes"]


def test_forecast_config_echoes_effective_limits():
    with SessionLocal() as db:
        _plan(db, category_limit=7, work_limit=11)
        _category(db, "genre:剧情", "剧情")
        _channel(db)
        _work(db, title="测试剧")
    out = _forecast()
    assert out["config"]["category_limit"] == 7
    assert out["config"]["work_limit"] == 11


def test_forecast_reports_overflow_when_cap_cannot_clear_backlog():
    """每小时只能推 1 条、作品却有 30 部 → 今天必然推不完，overflow 必须 > 0。

    这条同时守住一个真实坑：存量盘点与模拟阶段如果共用同一个扫描计数器，
    作品一多就会被模拟阶段提前耗尽，overflow 恒为 0——等于骗用户「今天推得完」。
    """
    from app import runtime_config
    with SessionLocal() as db:
        _plan(db)
        _category(db, "genre:剧情", "剧情")
        _channel(db)
        for i in range(30):
            _work(db, title="剧%02d" % i, file_id="f-%02d" % i)
        runtime_config.set_many(db, {"sc_quick_publish_enabled": "true",
                                     "sc_quick_publish_hourly_limit": "1"})
        db.commit()
    out = _forecast()
    assert out["ok"] is True
    assert out["today_total"] <= 24, "每小时 1 条，今天最多 24 条，实际 %s" % out["today_total"]
    assert out["overflow"] > 0, "30 部作品、每小时 1 条：今天必然推不完"
    assert out["backlog"] >= 30, "待推总量应覆盖全部 30 部，实际 %s" % out["backlog"]


# --------------------------------------------------------------------------
# ⑤ 接入守卫：dispatch 必须真的用匀速额度
# --------------------------------------------------------------------------
def test_dispatch_uses_hourly_due_instead_of_raw_cap():
    """反证：dispatch 里不许再出现裸的 `submitted_this_hour >= hourly_cap`。

    旧写法就是「整点突发」的根源。这条守卫防止后续有人把它改回去。
    注意：**只扫代码行，剔除注释**——本文件的说明性注释里引用了旧写法，
    不剔除会让守卫被自己的注释误伤（这类假红排查起来极费时间）。
    """
    import inspect
    src = inspect.getsource(sc2.dispatch_ready_tasks)
    code = "\n".join(
        line.split("#", 1)[0] for line in src.splitlines()
        if not line.strip().startswith("#")
    )
    assert "_hourly_due(" in code, "dispatch 必须用匀速额度 _hourly_due"
    assert "submitted_this_hour >= hourly_cap" not in code, \
        "dispatch 不得回退成整点突发（submitted_this_hour >= hourly_cap）"
