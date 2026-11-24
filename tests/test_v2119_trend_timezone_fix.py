"""v2.1.19 bug2：推送趋势不随真实推送变化。

用户原话：
    bug2:刚才有推送，但推送趋势里面没有数据变化

三个缺陷叠在一起，任修一个都不够：

  ① **时区**：库里 `DateTime(timezone=True)` 但 SQLite 不存 tzinfo，读回来是
     naive 的 UTC。旧代码 `dt.replace(tzinfo=sh)` 直接贴 +8 标签 = 不转换，
     北京时间 08:01 的推送被画进 `00:00` 桶。
  ② **可视窗口**：「日」视图把**全部历史**按小时混桶，`sorted()` 后字符串最大的
     8 个恒为 16:00~23:00，前端只画 `slice(-8)` —— 早上/中午推的永远不进画面。
  ③ **失败口径**：非 published 一律记 failed，把在途 / 取消 / 归档都算成失败。

本文件锁死：
  ① 「刚才推的」必须落在**当前北京时间小时**桶里（时区反证：未修时落到 UTC 小时桶）；
  ② 日视图桶连续到当前小时，最后 8 个一定包含当前小时；
  ③ 昨天及更早的推送不进日视图，但进周/月视图；
  ④ 在途 / 取消 / 归档不计入失败线，只有真失败才计；
  ⑤ 周 / 月视图的时区分桶同样正确（同一条 key_of 路径）。
"""
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, func, select

from app import share_center_v2 as sc2
from app.db import SessionLocal, migrate
from app.models import (
    Bot,
    Channel,
    Resource,
    SCCategory,
    SCCategoryCursor,
    SCLibraryVersion,
    SCPublishTask,
    SCPushList,
    SCPushListItem,
    SCSharePlan,
    SCSharePlanVersion,
    SCWorkCategory,
)

WIPED = (SCPublishTask, SCPushListItem, SCPushList, SCLibraryVersion,
         SCCategoryCursor, SCWorkCategory, SCCategory, SCSharePlanVersion,
         SCSharePlan, Resource, Channel, Bot)

BJ = timezone(timedelta(hours=8))


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    for _ in range(2):
        with SessionLocal() as db:
            for model in WIPED:
                db.execute(delete(model))
            db.commit()
    yield


_seq = {"n": 0}


def _task(db, *, status, published_at=None, link_state="unknown", work_key=None):
    _seq["n"] += 1
    wk = work_key or f"work:t{_seq['n']}"
    lst = SCPushList(list_key=f"l-{_seq['n']}", name="t", status="active")
    db.add(lst)
    db.flush()
    item = SCPushListItem(list_id=lst.id, work_key=wk, chat_id="-1001234567890",
                          dedup_key=f"{wk}:{_seq['n']}", status="planned")
    db.add(item)
    db.flush()
    t = SCPublishTask(list_item_id=item.id, work_key=wk, kind="first",
                      status=status, link_state=link_state,
                      published_at=published_at)
    db.add(t)
    db.commit()
    return t


def _trend(period="day"):
    with SessionLocal() as db:
        return sc2._aggregate_trend(db, period)


def _bj_now():
    return datetime.now(timezone.utc).astimezone(BJ)


# --------------------------------------------------------------------------
# ① 时区：刚才推的必须落在当前北京时间小时桶
# --------------------------------------------------------------------------
def test_just_published_lands_in_current_beijing_hour_bucket():
    """未修时区时这条必红：naive UTC 被当成 +8，落到 (北京小时-8) 的桶。"""
    now_utc = datetime.now(timezone.utc)
    with SessionLocal() as db:
        _task(db, status="published", published_at=now_utc, link_state="active")

    out = _trend("day")
    series = out["series"]
    want = f"{_bj_now().hour:02d}:00"
    assert series[-1]["label"] == want, (
        f"最后一个桶应是当前北京时间 {want}，实际 {series[-1]['label']} "
        "（差 8 小时 = 时区没转换）")
    assert series[-1]["published"] == 1
    assert sum(s["published"] for s in series) == 1


def test_published_at_naive_utc_is_converted_not_relabelled():
    """直接构造 naive UTC 值（模拟 SQLite 读回的形态），验证被当成 UTC 转换。"""
    bj_now = _bj_now()
    # 取「昨天北京 08:00」对应的 UTC 时刻，写成 naive
    target_bj = (bj_now - timedelta(days=1)).replace(
        hour=8, minute=1, second=0, microsecond=0)
    naive_utc = target_bj.astimezone(timezone.utc).replace(tzinfo=None)
    with SessionLocal() as db:
        _task(db, status="published", published_at=naive_utc, link_state="active")

    # 日视图不包含昨天
    assert sum(s["published"] for s in _trend("day")["series"]) == 0
    # 周视图按天分桶，应落在那一天
    week = {s["label"]: s for s in _trend("week")["series"]}
    monday = (target_bj - timedelta(days=target_bj.weekday())).strftime("%Y-%m-%d")
    assert week.get(monday, {}).get("published") == 1, \
        f"应落在 {monday} 这一周，实际桶 {list(week)}"


# --------------------------------------------------------------------------
# ② 日视图窗口：连续到当前小时，前端 slice(-8) 必含当前小时
# --------------------------------------------------------------------------
def test_day_series_is_continuous_through_current_hour():
    now_utc = datetime.now(timezone.utc)
    with SessionLocal() as db:
        _task(db, status="published", published_at=now_utc, link_state="active")

    series = _trend("day")["series"]
    hour_now = _bj_now().hour
    assert len(series) == hour_now + 1, \
        f"应给 00:00 到 {hour_now:02d}:00 共 {hour_now + 1} 个桶，实际 {len(series)}"
    assert [s["label"] for s in series] == [f"{h:02d}:00" for h in range(hour_now + 1)]
    # 前端只画最后 8 个 → 当前小时必须在里面
    assert series[-8:][-1]["label"] == f"{hour_now:02d}:00"


def test_day_series_visible_window_contains_current_hour_after_eight_plus_hours():
    """超过 8 小时也不许把当前小时挤出可视区（旧实现固定停在 16:00~23:00）。"""
    now_utc = datetime.now(timezone.utc)
    with SessionLocal() as db:
        # 今天早些时候各小时都塞一条，模拟「一天推了不少」
        for back in range(0, 10):
            _task(db, status="published",
                  published_at=now_utc - timedelta(hours=back), link_state="active")
    series = _trend("day")["series"]
    tail = series[-8:]
    assert tail[-1]["label"] == f"{_bj_now().hour:02d}:00"
    assert sum(s["published"] for s in tail) >= 1


def test_yesterday_pushes_excluded_from_day_view():
    now_utc = datetime.now(timezone.utc)
    with SessionLocal() as db:
        _task(db, status="published", published_at=now_utc, link_state="active")
        _task(db, status="published",
              published_at=now_utc - timedelta(days=1), link_state="active")
        _task(db, status="published",
              published_at=now_utc - timedelta(days=30), link_state="active")
    assert sum(s["published"] for s in _trend("day")["series"]) == 1, \
        "日视图只统计今天，历史不得混进同一小时桶"


# --------------------------------------------------------------------------
# ③ 失败口径：在途 / 取消 / 归档都不算失败
# --------------------------------------------------------------------------
@pytest.mark.parametrize("status", [
    "planned", "building", "ready", "submitted", "accepted", "publishing",
    sc2.CLEANED_TASK_STATUS, "cancelled",
])
def test_non_failure_statuses_not_counted_as_failed(status):
    now_utc = datetime.now(timezone.utc)
    with SessionLocal() as db:
        _task(db, status=status, published_at=now_utc)
    series = _trend("day")["series"]
    assert sum(s["failed"] for s in series) == 0, \
        f"状态 {status} 不该计入失败线"
    assert sum(s["total"] for s in series) == 0


@pytest.mark.parametrize("status", ["failed", sc2.INVALID_TASK_STATUS])
def test_real_failures_are_counted(status):
    now_utc = datetime.now(timezone.utc)
    with SessionLocal() as db:
        _task(db, status=status, published_at=now_utc)
    series = _trend("day")["series"]
    assert sum(s["failed"] for s in series) == 1


def test_failure_and_success_kept_separate():
    now_utc = datetime.now(timezone.utc)
    with SessionLocal() as db:
        _task(db, status="published", published_at=now_utc, link_state="active")
        _task(db, status="failed", published_at=now_utc)
        _task(db, status="planned", published_at=now_utc)
    last = _trend("day")["series"][-1]
    assert last["published"] == 1 and last["failed"] == 1 and last["total"] == 2


# --------------------------------------------------------------------------
# ④ 周 / 月视图的时区分桶同样正确
# --------------------------------------------------------------------------
def test_week_bucket_uses_beijing_time():
    bj_now = _bj_now()
    # 北京时间周一 00:30 → UTC 是上周日 16:30。按 UTC 分桶会错记到上一周。
    monday_like = (bj_now - timedelta(days=bj_now.weekday())).replace(
        hour=0, minute=30, second=0, microsecond=0)
    utc_moment = monday_like.astimezone(timezone.utc)
    with SessionLocal() as db:
        _task(db, status="published", published_at=utc_moment, link_state="active")
    labels = [s["label"] for s in _trend("week")["series"] if s["published"]]
    assert labels == [monday_like.strftime("%Y-%m-%d")], \
        f"周一凌晨的推送必须归本周，实际 {labels}"


def test_month_bucket_is_beijing_date():
    bj_now = _bj_now()
    # 北京时间今天 00:30 → UTC 是昨天 16:30。按 UTC 会记到昨天。
    today_early = bj_now.replace(hour=0, minute=30, second=0, microsecond=0)
    with SessionLocal() as db:
        _task(db, status="published",
              published_at=today_early.astimezone(timezone.utc), link_state="active")
    labels = [s["label"] for s in _trend("month")["series"] if s["published"]]
    assert labels == [bj_now.strftime("%Y-%m-%d")], \
        f"北京今天凌晨的推送必须归今天，实际 {labels}"


# --------------------------------------------------------------------------
# ⑤ 空库不编造：无数据时 series 为空（前端显示空态，不画假曲线）
# --------------------------------------------------------------------------
def test_empty_db_returns_empty_series_not_fake_flat_line():
    """既有约定：无数据 series 为空。给一排 0 会被前端画成贴底的假曲线。"""
    with SessionLocal() as db:
        assert db.scalar(select(func.count(SCPublishTask.id))) == 0
    assert _trend("day")["series"] == []
    assert _trend("week")["series"] == []
    assert _trend("month")["series"] == []


def test_day_view_only_zero_hours_still_empty_today():
    """昨天有推送、今天没有 → 日视图仍为空态（不能拿昨天的数据占位）。"""
    now_utc = datetime.now(timezone.utc)
    with SessionLocal() as db:
        _task(db, status="published",
              published_at=now_utc - timedelta(days=1), link_state="active")
    assert _trend("day")["series"] == []
