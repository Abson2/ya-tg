"""v2.1.46 验收：发布规则里的「一键 AI 调度」· 均匀排期。

用户口径（2026-11-25 原话）：
  「帮忙在发布规则里面增加一键ai调度的均匀排期」
  问题描述：「我们的作品排期可能不够均匀（比如每个分类下的作品数量不一致，
   有的分类作品很多、有的很少，有些分类可能不全，而且我们的排期是排期完成后又入库的）」
  本轮范围：「暂时先增加按钮，只做增加对应时段的排期作品数量，后面再优化
   调度逻辑，注意不能影响原有的分类时段和时段的ai建议，逻辑不能冲突」

改前取证（为什么排期不均）：
  `plan_engine` 按 `SCCategory.priority desc` 遍历分类，每个分类只要在窗口内就一路
  建任务直到 `hour_max` 用完 —— 优先级最高的分类把整点额度吃光，排在后面的分类
  每小时都拿 0 部。时段配置本身没问题，问题在**同一小时内的名额分配**。

本文件锁六件事：
  ① 名额分配算法（最大余额法）的总和、下限、权重、额度不足四种性质；
  ② 「没有每小时额度」时如实拒绝，**绝不编一个数字**出来；
  ③ 方案只影响「每小时某分类还能出几部」，`quota=None`（没生成过）时行为零变化；
  ④ 生成前后 `SCCategorySchedule` 一个字段都不变（时段 + AI 建议原样保留）；
  ⑤ 实跑与预测消费同一份方案、同一个判据（v2.1.19 红线：同口径）；
  ⑥ 前端有按钮 / 状态条 / 明细弹窗 / 撤销，且数字与明细同源。
"""
import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app import share_center_v2 as sc2, runtime_config, utils
from app.db import SessionLocal, migrate
from app.models import (
    AppConfig, AuditLog, Bot, Channel, QueueItem, Resource, SCCategory,
    SCCategoryCursor, SCCategorySchedule, SCHourWindow, SCLibraryVersion,
    SCPublishTask, SCPushList, SCPushListItem, SCSharePlan, SCSharePlanVersion,
    SCWorkCategory, ShareTask,
)

from conftest import ensure_full_day_hour_window

ROOT = __import__("pathlib").Path(__file__).resolve().parents[1]
SC2_SRC = (ROOT / "backend/app/share_center_v2.py").read_text(encoding="utf-8")
APP_VUE = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")

CAT_A = "genre:剧情"
CAT_B = "genre:动作"

WIPED = (SCPublishTask, SCPushListItem, SCPushList, SCLibraryVersion,
         SCCategoryCursor, SCCategorySchedule, SCWorkCategory, SCCategory,
         SCSharePlanVersion, SCSharePlan, ShareTask, QueueItem, Resource,
         Channel, Bot, AppConfig, AuditLog, SCHourWindow)


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    for _ in range(2):
        with SessionLocal() as db:
            for model in WIPED:
                db.execute(delete(model))
            db.commit()
    yield


# ---------------------------------------------------------------------------
# 造数
# ---------------------------------------------------------------------------
def _plan(db, cycle=None, cats=(CAT_A, CAT_B), category_limit=20):
    plan = SCSharePlan(name="p", plan_type="daily", enabled=True)
    db.add(plan)
    db.flush()
    cfg = {"category_limit": category_limit, "work_limit": 50}
    cfg["cycle_plan"] = dict(cycle or {})
    cfg["cycle_plan"].setdefault("quiet_enabled", False)
    ver = SCSharePlanVersion(plan_id=plan.id, version_no=1, status="active",
                             config_json=json.dumps(cfg))
    db.add(ver)
    db.flush()
    ensure_full_day_hour_window(db, ver.id)
    for i, key in enumerate(cats):
        db.add(SCCategory(category_key=key, name=key.split(":")[-1],
                          enabled=True, priority=len(cats) - i))
    bot = Bot(name="b", token="x", enabled=True)
    db.add(bot)
    db.flush()
    db.add(Channel(bot_id=bot.id, name="@chan", chat_id="@chan",
                   kind="channel", enabled=True))
    db.commit()
    return ver


def _live(db, hashed_title, fid, cat):
    old = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
    res = Resource(file_id=fid, name=f"{hashed_title}.mkv", ext=".mkv", size=1,
                   utime=1, identify_status="identified", media_type="movie",
                   tmdb_id=42, media_title=hashed_title,
                   share_dir_name=hashed_title,
                   share_scope="series_dir", share_status="not_created")
    db.add(res)
    db.flush()
    wk = utils.work_group_key(res)
    db.add(SCLibraryVersion(work_key=wk, version_no=1, fingerprint=f"fp-{fid}",
                            media_type="movie",
                            resource_ids_json=json.dumps([res.id]),
                            metadata_json=json.dumps({"title": hashed_title},
                                                     ensure_ascii=False),
                            change_type="first", created_at=old))
    db.add(SCWorkCategory(work_key=wk, category_key=cat))
    db.flush()
    return wk


def _seed(db, cycle=None, n_a=10, n_b=10, cats=(CAT_A, CAT_B)):
    ver = _plan(db, cycle=cycle, cats=cats)
    for i in range(n_a):
        _live(db, f"剧甲{i}", f"f-a{i}", CAT_A)
    for i in range(n_b):
        _live(db, f"剧乙{i}", f"f-b{i}", CAT_B)
    # 关掉「上新即时推送」通道：插队通道**不经过分类排期门禁**（v2.1.22 设计如此），
    # 开着它测出来的任务数其实是插队的那批，跟均匀排期没关系。
    runtime_config.set_many(db, {"sc_conflict_policy": "prefer_backlog"})
    db.commit()
    return ver


def _set_hourly(db, value):
    runtime_config.set_many(db, {"sc_quick_publish_enabled": "true",
                                 "sc_quick_publish_hourly_limit": str(value)})
    db.commit()


# ===========================================================================
# 一、名额分配算法
# ===========================================================================
def test_largest_remainder_sums_to_quota():
    out = sc2._largest_remainder({"a": 90, "b": 10}, 3)
    assert sum(out.values()) == 3, out
    assert out["a"] >= out["b"], "作品多的分类应至少分到同样多的名额"


def test_largest_remainder_gives_everyone_at_least_one():
    out = sc2._largest_remainder({"a": 1, "b": 1, "c": 1}, 5)
    assert sum(out.values()) == 5, out
    assert min(out.values()) >= 1, f"额度够时每类至少 1 部：{out}"


def test_largest_remainder_when_quota_smaller_than_categories():
    out = sc2._largest_remainder({"a": 5, "b": 5, "c": 5, "d": 5}, 3)
    assert sum(out.values()) == 3, f"绝不能超额：{out}"
    assert sorted(out.values()) == [0, 1, 1, 1], out


def test_largest_remainder_no_quota():
    assert sc2._largest_remainder({"a": 1}, 0) == {}
    assert sc2._largest_remainder({}, 5) == {}


def test_largest_remainder_favours_big_backlog():
    out = sc2._largest_remainder({"big": 100, "small": 1}, 10)
    assert sum(out.values()) == 10
    assert out["big"] > out["small"], out
    assert out["small"] >= 1, "作品再少也该有 1 部，不能被饿死"


# ===========================================================================
# 二、门禁：没方案就放行，有方案按配额
# ===========================================================================
def test_even_allows_is_transparent_without_plan():
    """`quota=None` ＝ 没生成过方案 → 全部放行，老路径行为零变化。"""
    assert sc2._even_allows(None, CAT_A, 999) is True


def test_even_allows_blocks_at_quota():
    q = {CAT_A: 2}
    assert sc2._even_allows(q, CAT_A, 0) is True
    assert sc2._even_allows(q, CAT_A, 1) is True
    assert sc2._even_allows(q, CAT_A, 2) is False, "到配额就该让出名额"
    assert sc2._even_allows(q, CAT_B, 0) is False, "方案里没给这个分类名额"


def test_even_hour_quota_reads_one_hour():
    plan = {"items": {"10": {CAT_A: 2}, "11": {CAT_B: 1}}}
    assert sc2.even_hour_quota(plan, 10) == {CAT_A: 2}
    assert sc2.even_hour_quota(plan, 12) == {}, "方案里没有这个小时 → 该小时不出货"
    assert sc2.even_hour_quota({}, 10) is None, "没有方案 → None（放行）"


# ===========================================================================
# 三、生成方案：不发明数字、只读时段
# ===========================================================================
def test_build_refuses_without_any_hourly_cap():
    """两个每小时上限都是 0（＝不限）→ 如实拒绝，绝不编一个额度。"""
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0})
        _set_hourly(db, 0)
        out = sc2.build_even_plan(db)
        ver = sc2._active_version(db)
        cfg = json.loads(ver.config_json or "{}")

    assert out.get("ok") is False, out
    assert "没有可分配的每小时额度" in out.get("reason", ""), out
    assert "even_plan" not in cfg, "拒绝了就不许往配置里写任何东西"


def test_build_uses_hourly_cap_from_config():
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0})
        _set_hourly(db, 4)
        out = sc2.build_even_plan(db)

    assert out["ok"] is True, out
    assert out["base_cap"] == 4
    assert out["cap_source"] == "每小时推送上限", "额度来源要如实写明"
    assert out["slots_total"] > 0


def test_build_falls_back_to_cycle_hour_max():
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 6})
        _set_hourly(db, 0)
        out = sc2.build_even_plan(db)

    assert out["ok"] is True, out
    assert out["base_cap"] == 6
    assert out["cap_source"] == "周期计划的每小时上限"


def test_build_counts_every_category_and_respects_cap():
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0})
        _set_hourly(db, 3)
        out = sc2.build_even_plan(db)
        plan = sc2.even_plan_of(db)

    assert out["ok"] is True, out
    items = plan["items"]
    assert items, "方案里一个小时都没有"
    for h, row in items.items():
        assert sum(row.values()) <= 3, f"{h} 点超额：{row}"
    names = {d["name"] for d in out["detail"]}
    assert len(names) == 2, f"两个分类都该排上，实际 {names}"


def test_build_gives_big_backlog_more_slots():
    """作品多的分类总配额应更多（按存量分配，不是平均主义）。"""
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0}, n_a=40, n_b=2)
        _set_hourly(db, 6)
        out = sc2.build_even_plan(db)

    assert out["ok"] is True, out
    by_name = {d["name"]: d for d in out["detail"]}
    a, b = by_name["剧情"], by_name["动作"]
    assert a["works"] > b["works"], f"作品多的分类该多排：{a} vs {b}"
    assert b["works"] >= 1, "作品少也得有排期，不能被饿死"


def test_build_skips_quiet_hours():
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0, "quiet_enabled": True,
                         "quiet_start": 0, "quiet_end": 7})
        _set_hourly(db, 3)
        out = sc2.build_even_plan(db)
        plan = sc2.even_plan_of(db)

    assert out["ok"] is True, out
    hours = {int(h) for h in plan["items"]}
    assert not (hours & set(range(0, 7))), f"静默小时不该出现在方案里：{sorted(hours)}"
    assert hours, "静默之外的小时必须有排期"


def test_build_respects_category_window():
    with SessionLocal() as db:
        ver = _seed(db, cycle={"hour_max": 0})
        db.add(SCCategorySchedule(plan_version_id=ver.id, category_key=CAT_A,
                                  window_start=8, window_end=10, enabled=True,
                                  window_auto=False))
        db.commit()
        _set_hourly(db, 3)
        out = sc2.build_even_plan(db)
        plan = sc2.even_plan_of(db)

    assert out["ok"] is True, out
    a_hours = {int(h) for h, row in plan["items"].items() if CAT_A in row}
    assert a_hours <= {8, 9}, f"分类只配了 8–10 点，方案却排到 {sorted(a_hours)}"


def test_build_never_touches_category_schedule():
    """用户红线：不能影响原有的分类时段和时段的 AI 建议。"""
    cols = ("window_start", "window_end", "week_windows", "ai_window_start",
            "ai_window_end", "ai_week_windows", "hourly_limit", "daily_limit",
            "enabled", "window_auto", "ai_adopted")
    with SessionLocal() as db:
        ver = _seed(db, cycle={"hour_max": 0})
        db.add(SCCategorySchedule(
            plan_version_id=ver.id, category_key=CAT_A,
            window_start=9, window_end=13, week_windows='{"1":[9,13]}',
            ai_window_start=10, ai_window_end=12, ai_week_windows='{"1":[10,12]}',
            hourly_limit=5, daily_limit=30,
            enabled=True, window_auto=True, ai_adopted=True))
        db.commit()
        before = {}
        for row in db.scalars(select(SCCategorySchedule)).all():
            before[row.category_key] = tuple(getattr(row, c) for c in cols)
        _set_hourly(db, 4)
        out = sc2.build_even_plan(db)
        db.expire_all()
        after = {}
        for row in db.scalars(select(SCCategorySchedule)).all():
            after[row.category_key] = tuple(getattr(row, c) for c in cols)

    assert out["ok"] is True, out
    assert before == after, "生成均匀排期改动了分类排期行（时段或 AI 建议被覆盖）"


def test_build_body_has_no_schedule_write():
    """源码级守门：生成逻辑里不许对排期行赋值（只读它）。"""
    start = SC2_SRC.index("def build_even_plan(")
    end = SC2_SRC.index("def clear_even_plan(", start)
    body = SC2_SRC[start:end]
    for bad in (".window_start =", ".window_end =", ".suggest_start =",
                ".suggest_end =", ".week_windows =", "SCCategorySchedule("):
        assert bad not in body, f"生成均匀排期时写入了分类排期字段：{bad}"


# ===========================================================================
# 四、落库与撤销
# ===========================================================================
def test_plan_persisted_and_cleared():
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0})
        _set_hourly(db, 3)
        assert sc2.build_even_plan(db)["ok"] is True
        assert sc2.even_plan_of(db), "方案没落到配置里"
        assert sc2.api_even_plan(db)["active"] is True

        assert sc2.clear_even_plan(db)["ok"] is True
        assert sc2.even_plan_of(db) == {}, "撤销后方案应清空"
        assert sc2.api_even_plan(db)["active"] is False


def test_api_even_plan_hides_backend_keys():
    """页面不得体现后端代码名词：回显里只能有中文分类名。"""
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0})
        _set_hourly(db, 3)
        sc2.build_even_plan(db)
        out = sc2.api_even_plan(db)

    blob = json.dumps(out, ensure_ascii=False)
    for bad in ("genre:", "even_plan", "items", "category_key"):
        assert bad not in blob, f"回显里露出了后端名词：{bad}"


# ===========================================================================
# 五、调度真的按方案分配（实跑）+ 与预测同源
# ===========================================================================
def _built_by_cat(db):
    rows = db.execute(
        select(SCPushListItem.category_key, SCPublishTask.id)
        .join(SCPushListItem, SCPublishTask.list_item_id == SCPushListItem.id)).all()
    out = {}
    for key, _ in rows:
        out[str(key)] = out.get(str(key), 0) + 1
    return out


def test_engine_spreads_slots_across_categories():
    """反证核心：不生成方案时第一个分类吃满额度、第二个 0 部；生成后两类都有。"""
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 3})
        _set_hourly(db, 3)
        # 反证前提：先看「没有方案」时的老行为
        sc2.plan_engine(db)
        old = _built_by_cat(db)
        assert old.get(CAT_A, 0) == 3 and old.get(CAT_B, 0) == 0, \
            f"反证前提不成立，改前分配不是「优先级高的吃满」：{old}"
        # 清掉这轮任务，再生成方案重跑
        db.execute(delete(SCPublishTask))
        db.execute(delete(SCPushListItem))
        db.commit()
        assert sc2.build_even_plan(db)["ok"] is True
        sc2.plan_engine(db)
        new = _built_by_cat(db)

    assert new.get(CAT_A, 0) >= 1 and new.get(CAT_B, 0) >= 1, \
        f"均匀排期后两个分类都该有排期，实际 {new}"
    assert sum(new.values()) <= 3, f"总额度不能被突破：{new}"


def test_engine_respects_zero_quota_category():
    """方案里没给某分类名额时，它这个小时一部都不该建。"""
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 3})
        _set_hourly(db, 3)
        ver = sc2._active_version(db)
        cfg = json.loads(ver.config_json or "{}")
        hour = str(datetime.now(timezone(timedelta(hours=8))).hour)
        # 手工只给 B 名额，A 为 0
        cfg["even_plan"] = {"generated_at": "2026-11-25 12:00", "base_cap": 3,
                            "cap_source": "每小时推送上限", "categories": 2,
                            "hours": 1, "slots_total": 3,
                            "items": {hour: {CAT_B: 3}}}
        ver.config_json = json.dumps(cfg)
        db.commit()
        sc2.plan_engine(db)
        built = _built_by_cat(db)

    assert built.get(CAT_A, 0) == 0, f"A 该小时名额为 0，却建了 {built.get(CAT_A)} 部"
    assert built.get(CAT_B, 0) >= 1, f"B 有 3 个名额却没排上：{built}"


def test_forecast_and_engine_use_same_even_gate():
    """预测与实跑必须消费同一份方案（v2.1.19 红线）。"""
    assert SC2_SRC.count("_even_allows(") >= 3, "预测与实跑没有共用同一个判据"
    assert SC2_SRC.count("even_hour_quota(") >= 3, "取配额的入口不统一"
    assert "even_plan = even_plan_of(db, version)" in SC2_SRC


def test_forecast_reflects_even_plan():
    """预测侧也吃同一份方案：方案给了两类名额，预测里两类都该出现。"""
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 3})
        _set_hourly(db, 3)
        assert sc2.build_even_plan(db)["ok"] is True
        out = sc2.forecast_schedule(db)

    assert out.get("hours"), "预测没出结果"
    keys = set()
    for h in out["hours"]:
        for it in h.get("items") or []:
            if it.get("category_key"):
                keys.add(it["category_key"])
    assert keys, "预测里一个分类都没有"
    assert len(keys) >= 2, f"方案把名额分给了两类，预测却只有 {keys}"


# ===========================================================================
# 六、前端
# ===========================================================================
def test_frontend_has_apply_button_and_strip():
    assert "一键 AI 调度" in APP_VUE, "发布规则里没有这个按钮"
    assert "scApplyEvenPlan" in APP_VUE
    assert "/sc/even-plan/apply" in APP_VUE
    assert "均匀排期已生效" in APP_VUE and "均匀排期未启用" in APP_VUE


def test_frontend_detail_matches_strip():
    """状态条与明细必须同源：同一份 scEvenPlan，不许各算一套。"""
    assert "scEvenPlan.detail.length" in APP_VUE, "明细条数与状态条数字不是同一来源"
    assert "scEvenPlan.slots_total" in APP_VUE
    assert "按分类看明细" in APP_VUE
    assert "scEvenDetailOpen" in APP_VUE


def test_frontend_has_undo():
    assert "scClearEvenPlan" in APP_VUE, "没有撤销入口，用户无法回到原分配方式"
    assert "/sc/even-plan/clear" in APP_VUE


def test_frontend_shows_cap_source():
    """额度从哪来要写清（每小时推送上限 / 周期计划小时上限），不能只给一个数。"""
    assert "scEvenPlan.cap_source" in APP_VUE
