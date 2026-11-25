"""v2.1.35：时 / 日 / 周 / 月与分类的推送上限**一律由发布规则给**，代码里不写死。

用户口径（2026-11-25 原话）：
  「时、日、周、月和分类的推送上限是由发布规则定，不是写死的值，别错了」

取证结论（改前实测）：
  · `plan_engine` 开头用 `_cycle_quota_gate(db, cycle)` 拦日/周/月额度与休止窗口，
    `cycle` 来自 `version.config_json["cycle_plan"]`（= 发布规则里配的那份）；
  · `forecast_schedule` **没接这道门禁** —— 实测 day_max=1 且今天已建 1 条任务时，
    `plan_engine` 返回 `{'tasks': 0, 'quota': 'day_quota'}`，而 `forecast_schedule`
    照旧预测出 2 部、`today_total=3`。同一屏里「实跑说 0、排期说有」又对不上。

本文件锁两件事：
  ① 额度满了预测必须跟着停（与实跑同口径）；
  ② 额度值随发布规则变 —— 调大就恢复预测，证明不是代码里写死的常量。
"""
import json
from datetime import datetime, timezone

import pytest
from sqlalchemy import delete, select

from app import share_center_v2 as sc2, utils
from app.db import SessionLocal, migrate
from app.models import (
    AppConfig, AuditLog, Bot, Channel, QueueItem, Resource, SCCategory,
    SCCategoryCursor, SCCategorySchedule, SCHourWindow, SCLibraryVersion,
    SCPublishTask, SCPushList, SCPushListItem, SCSharePlan, SCSharePlanVersion,
    SCWorkCategory, ShareTask,
)

from conftest import ensure_full_day_hour_window

WIPED = (SCPublishTask, SCPushListItem, SCPushList, SCLibraryVersion,
         SCCategoryCursor, SCCategorySchedule, SCWorkCategory, SCCategory,
         SCSharePlanVersion, SCSharePlan, ShareTask, QueueItem, Resource,
         Channel, Bot, AppConfig, AuditLog, SCHourWindow)

CAT = "genre:剧情"


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    for _ in range(2):
        with SessionLocal() as db:
            for model in WIPED:
                db.execute(delete(model))
            db.commit()
    yield


def _plan(db, cycle=None):
    plan = SCSharePlan(name="p", plan_type="daily", enabled=True)
    db.add(plan)
    db.flush()
    cfg = {"category_limit": 20, "work_limit": 50}
    cfg["cycle_plan"] = dict(cycle or {})
    cfg["cycle_plan"].setdefault("quiet_enabled", False)
    ver = SCSharePlanVersion(plan_id=plan.id, version_no=1, status="active",
                             config_json=json.dumps(cfg))
    db.add(ver)
    db.flush()
    ensure_full_day_hour_window(db, ver.id)
    db.add(SCCategory(category_key=CAT, name="剧情", enabled=True, priority=0))
    bot = Bot(name="b", token="x", enabled=True)
    db.add(bot)
    db.flush()
    db.add(Channel(bot_id=bot.id, name="@chan", chat_id="@chan",
                   kind="channel", enabled=True))
    db.commit()
    return ver


def _live(db, title, fid):
    """在库作品。`created_at` 设成 40 天前，避免走「上新即时推送」插队通道。"""
    old = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
    res = Resource(file_id=fid, name=f"{title}.mkv", ext=".mkv", size=1,
                   utime=1, identify_status="identified", media_type="movie",
                   tmdb_id=42, media_title=title, share_dir_name=title,
                   share_scope="series_dir", share_status="not_created")
    db.add(res)
    db.flush()
    wk = utils.work_group_key(res)
    db.add(SCLibraryVersion(work_key=wk, version_no=1, fingerprint=f"fp-{fid}",
                            media_type="movie",
                            resource_ids_json=json.dumps([res.id]),
                            metadata_json=json.dumps({"title": title},
                                                     ensure_ascii=False),
                            change_type="first", created_at=old))
    db.add(SCWorkCategory(work_key=wk, category_key=CAT))
    db.flush()
    return wk


def _seed(db, *, cycle, already=1):
    _plan(db, cycle=cycle)
    _live(db, "活A", "f-a")
    _live(db, "活B", "f-b")
    now = datetime.now(timezone.utc)
    for i in range(already):
        db.add(SCPublishTask(list_item_id=None, work_key=f"wk-old-{i}",
                             kind="first", status="planned", priority=0,
                             scheduled_at=now, created_at=now))
    db.commit()


def _predicted(out):
    keys = []
    for h in out["hours"]:
        for it in h["items"]:
            if it.get("kind") == "scheduled":
                continue
            for w in (it.get("works") or []):
                keys.append(str(w.get("work_key") or ""))
    return keys


# ---------------------------------------------------------------------------
# ① 日额度满 → 预测必须跟着停（此前只有实跑停）
# ---------------------------------------------------------------------------
def test_forecast_stops_when_day_quota_from_rule_is_full():
    with SessionLocal() as db:
        _seed(db, cycle={"day_max": 1}, already=1)
        engine = sc2.plan_engine(db)
        assert engine.get("tasks") == 0, f"实跑本该被日额度拦住，实际 {engine}"
        assert engine.get("quota") == "day_quota"

        out = sc2.forecast_schedule(db)

    assert _predicted(out) == [], f"日额度已满却还在预测：{_predicted(out)}"
    # `today_total` 只算当天真实在途任务（v2.1.34 回灌的那条已建任务），不算预测
    assert out["today_total"] == 1, f"今日总数应只剩已建的 1 条，实际 {out['today_total']}"
    # 卡在哪要说人话，且不得出现后端名词
    tells = [h.get("blocked") for h in out["hours"]] + list(out["notes"])
    assert any("发布规则上限" in str(t) for t in tells), f"没有说明卡在哪：{tells}"
    assert not any("quota" in str(t) for t in tells), f"页面文案露出了后端名词：{tells}"


# ---------------------------------------------------------------------------
# ② 额度值来自发布规则：调大就恢复预测（证明不是写死的常量）
# ---------------------------------------------------------------------------
def test_forecast_resumes_when_rule_raises_the_cap():
    with SessionLocal() as db:
        _seed(db, cycle={"day_max": 100}, already=1)
        # 先看预测（只读）——此时两部在库作品还没建任务，应能被预测出来
        out = sc2.forecast_schedule(db)
        # 再跑实跑，确认同一份额度下实跑也放行
        engine = sc2.plan_engine(db)
        assert engine.get("tasks", 0) > 0, f"额度够时实跑必须能排，实际 {engine}"

    keys = _predicted(out)
    assert keys, "额度够却排不出预测 —— 上限被写死了？"
    assert out["today_total"] > 0


def test_week_quota_is_retired_and_no_longer_blocks():
    """v2.1.42（用户口径）：周限额功能已下线，配了也不再拦截。

    注意：这条是**行为侧**回归，真正的守门人是下面的源码契约
    （`checks` 只剩日度一层、且窗口只有一天）—— 单靠行为断言不够，
    因为窗口已经不具备「按周统计」的能力。
    """
    with SessionLocal() as db:
        _seed(db, cycle={"week_max": 1}, already=1)
        out = sc2.forecast_schedule(db)
        engine = sc2.plan_engine(db)

    assert engine.get("quota") != "week_quota", f"周限额应已下线，实际仍拦：{engine}"
    assert _predicted(out), "周限额已下线，预测不该被拦空"


def test_month_quota_is_retired_and_no_longer_blocks():
    """v2.1.42（用户口径）：月限额功能同样下线（行为侧回归，见上一条说明）。"""
    with SessionLocal() as db:
        _seed(db, cycle={"month_max": 1}, already=1)
        out = sc2.forecast_schedule(db)
        engine = sc2.plan_engine(db)

    assert engine.get("quota") != "month_quota", f"月限额应已下线，实际仍拦：{engine}"
    assert _predicted(out), "月限额已下线，预测不该被拦空"


# ---------------------------------------------------------------------------
# ③ 源码契约：预测与实跑调的是**同一个**门禁函数，且上限都是 cycle.get 读出来的
# ---------------------------------------------------------------------------
def test_forecast_and_engine_share_the_same_quota_gate():
    src = (__import__("pathlib").Path(__file__).resolve().parents[1]
           / "backend/app/share_center_v2.py").read_text(encoding="utf-8")
    # def 一行 + plan_engine 一处 + forecast 一处
    assert src.count("_cycle_quota_gate(") >= 3, \
        "预测侧没有复用 plan_engine 的额度门禁"
    # v2.1.42：只剩日度一层（周/月限额已下线，勿把这两个键加回来）
    assert 'checks = (("day_max", "day"),)' in src, "额度门禁应只剩日度一层"
    assert '"week_max"' not in src and '"month_max"' not in src, \
        "周/月限额已下线，代码里不应再出现这两个键"
    assert "cap = int(cycle.get(key) or 0)" in src
