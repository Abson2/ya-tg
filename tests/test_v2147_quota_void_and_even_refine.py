"""v2.1.47 验收：额度口径（作废不计）+ 均匀排期名额按「部」+ 余量回流 + 库存漂移。

用户口径（2026-11-25 原话）：
  「1、已作废不算任务条数」
  「3、继续优化优化调度逻辑」

改前取证（三处真实缺陷，都是源码级可指认的）：
  ① `_cycle_quota_gate` / `_category_today_count` / `_category_hour_counts` 都用
     `count(SCPublishTask.id)` **不看状态** → 用户取消 / 清理 / 判为无效的任务
     照样吃额度。真机表现是「明明没推出去几条，当日额度却满了」。
  ② 均匀排期的名额单位是**部**（「每个时段排几部作品」），但消费侧比的是
     **任务条数**：一部作品推频道 + 群是两条任务，于是「配额 2 部」在双渠道部署下
     只排得出 1 部 —— 实排永远只有配额的一半。
  ③ 名额是生成那一刻按存量权重分的静态数字。一个只有 2 部作品的小分类，也按权重
     拿到每小时 1–2 部，全天可排小时数 × 名额远大于它的库存，剩下的名额全天空转，
     作品多的分类被自己的配额卡住。

本文件锁五件事：
  ① 作废（取消 / 清理 / 无效）不计入任何额度，**且**额度门禁本身仍然有效（反证）；
  ② `skipped` 故意不算作废（无法确认是否真投递过，宁可占额度）；
  ③ 名额按「部」计数：双渠道下配额 2 部＝2 部作品＝4 条任务（实跑与预测同口径）；
  ④ 名额夹到库存以内，削下来的按剩余余量回流，总额绝不突破发布规则给的上限；
  ⑤ 页面能看到「生成时库存 → 现在库存」的差额（排期后又入库）。
"""
import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, distinct, select, func

from app import share_center_v2 as sc2, runtime_config, utils
from app.db import SessionLocal, migrate
from app.models import (
    AppConfig, AuditLog, Bot, Channel, QueueItem, Resource, SCCategory,
    SCCategoryCursor, SCCategorySchedule, SCHourWindow, SCLibraryVersion,
    SCPublishTask, SCPushList, SCPushListItem, SCSharePlan, SCSharePlanVersion,
    SCWorkCategory, ShareTask,
)

# 造数与清库复用 v2.1.46 的同一套（分类、作品、计划版本口径完全一致，
# 两版之间不会因为造数方式不同而各说各话）。
from test_v2146_even_schedule import (  # noqa: E402
    CAT_A, CAT_B, WIPED, _live, _plan, _seed, _set_hourly,
)

CAT_C = "genre:喜剧"

ROOT = __import__("pathlib").Path(__file__).resolve().parents[1]
SC2_SRC = (ROOT / "backend/app/share_center_v2.py").read_text(encoding="utf-8")
APP_VUE = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


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
# 造数：清单项 + 发布任务（可指定状态 / 落库时间 / 是否同一部作品）
# ---------------------------------------------------------------------------
def _push_rows(db, count, status, *, cat=CAT_A, work_key=None, chat_id="@chan",
               when=None, hour_shift=0):
    lst = db.scalar(select(SCPushList).where(SCPushList.list_key == "t47"))
    if lst is None:
        lst = SCPushList(list_key="t47", name="t47", status="active", source="plan")
        db.add(lst)
        db.flush()
    out = []
    for i in range(count):
        wk = work_key or f"wk-{status}-{cat}-{i}"
        item = SCPushListItem(
            list_id=lst.id, work_key=wk, library_version_id=0, category_key=cat,
            target_type="group" if str(chat_id).startswith("-") else "channel",
            bot_id=0, chat_id=chat_id,
            dedup_key=f"d-{status}-{wk}-{chat_id}-{i}"[:255],
            source_kind="first", status="planned")
        db.add(item)
        db.flush()
        task = SCPublishTask(list_item_id=item.id, work_key=wk, plan_version_id=0,
                             kind="first", status=status, priority=0,
                             scheduled_at=datetime.now(timezone.utc))
        if when is not None:
            task.created_at = when
        elif hour_shift:
            task.created_at = datetime.now(timezone.utc) + timedelta(hours=hour_shift)
        db.add(task)
        db.flush()
        out.append(task)
    db.commit()
    return out


def _add_channel(db, chat_id="-1008888"):
    """加第二个推送渠道：一部作品 → 两条任务（这是②的触发条件）。"""
    bot = db.scalar(select(Bot))
    ch = Channel(bot_id=bot.id, name=chat_id, chat_id=chat_id,
                 kind="group", enabled=True)
    db.add(ch)
    db.commit()
    return ch


def _inject_plan(db, items, base_cap=3, cap_source="每小时推送上限"):
    """手工注入一份方案（等价于点过一次一键调度），用于精确控制配额。"""
    ver = sc2._active_version(db)
    cfg = json.loads(ver.config_json or "{}")
    cfg["even_plan"] = {
        "generated_at": "2026-11-25 12:00", "base_cap": base_cap,
        "cap_source": cap_source, "categories": 2, "hours": len(items),
        "slots_total": sum(sum(v.values()) for v in items.values()),
        "items": {str(k): dict(v) for k, v in items.items()},
    }
    ver.config_json = json.dumps(cfg)
    db.commit()


def _this_hour():
    return datetime.now(timezone(timedelta(hours=8))).hour


# ===========================================================================
# 一、作废不占额度（用户口径第 1 条）
# ===========================================================================
def test_void_statuses_are_exactly_the_voided_ones():
    assert sc2.VOID_TASK_STATUSES == frozenset({"cancelled", "cleaned", "invalid"})
    # 口径说明：skipped（已跳过投递）故意不算作废 —— 它可能是「孤儿在途任务归档，
    # 无法确认到底发没发」，宁可让它继续占额度，也不凭猜疑多放一个名额。
    assert "skipped" not in sc2.VOID_TASK_STATUSES
    # 失败任务仍然算（它真的尝试投递过），不在排除名单里
    assert "failed" not in sc2.VOID_TASK_STATUSES


def test_day_quota_ignores_void_tasks():
    """4 条作废 + 4 条已推，day_max=6 → 只数 4，不该拦。"""
    with SessionLocal() as db:
        _plan(db)
        _push_rows(db, 4, "cleaned")
        _push_rows(db, 4, "published")
        blocked, reason = sc2._cycle_quota_gate(db, {"day_max": 6})

        # 反证：把这 8 条全当「已推」——即旧口径「不看状态数条数」——8 ≥ 6 必须拦。
        # 上面放行是因为排除了作废，而不是门禁失效。
        for t in db.scalars(select(SCPublishTask)).all():
            t.status = "published"
        db.commit()
        blocked_old, reason_old = sc2._cycle_quota_gate(db, {"day_max": 6})

    assert (blocked, reason) == (False, ""), "作废任务不该吃当日额度"
    assert blocked_old is True and reason_old == "day_quota", \
        "门禁本身必须仍然有效（反证不成立说明判据被改坏了）"


def test_day_quota_still_blocks_on_real_tasks():
    """真正推出去的条数到顶，仍然要拦——别把门禁改松了。"""
    with SessionLocal() as db:
        _plan(db)
        _push_rows(db, 3, "published")
        _push_rows(db, 2, "submitted")
        blocked, reason = sc2._cycle_quota_gate(db, {"day_max": 5})

    assert blocked is True and reason == "day_quota"


def test_category_day_count_ignores_void():
    with SessionLocal() as db:
        _plan(db)
        _push_rows(db, 3, "cleaned", cat=CAT_A)
        _push_rows(db, 2, "invalid", cat=CAT_A)
        _push_rows(db, 1, "published", cat=CAT_A)
        got = sc2._category_today_count(db, CAT_A)

    assert got == 1, f"单分类当日上限只该数真推出去的那 1 条，实际 {got}"


def test_hour_counts_ignore_void():
    with SessionLocal() as db:
        _plan(db)
        _push_rows(db, 3, "cleaned", cat=CAT_A)
        _push_rows(db, 1, "planned", cat=CAT_A, work_key="在排的作品")
        got = sc2._category_hour_counts(
            db, datetime.now(timezone(timedelta(hours=8))).replace(
                minute=0, second=0, microsecond=0))

    assert got.get(CAT_A, 0) == 1, f"本小时已建部数只该算活着的作品，实际 {got}"


# ===========================================================================
# 二、名额按「部」计（用户口径第 3 条 · 真 bug）
# ===========================================================================
def test_hour_counts_counts_works_not_tasks():
    """同一部作品推两个渠道是两条任务，但只算 1 部。"""
    with SessionLocal() as db:
        _plan(db)
        _push_rows(db, 1, "planned", cat=CAT_A, work_key="双渠道作品", chat_id="@chan")
        _push_rows(db, 1, "planned", cat=CAT_A, work_key="双渠道作品", chat_id="-1008888")
        got = sc2._category_hour_counts(
            db, datetime.now(timezone(timedelta(hours=8))).replace(
                minute=0, second=0, microsecond=0))
        tasks = db.scalar(select(func.count(SCPublishTask.id)))

    assert tasks == 2, "造数没造出两条任务，用例前提不成立"
    assert got.get(CAT_A, 0) == 1, f"两条任务属于同一部作品，应算 1 部，实际 {got}"


def test_hour_counts_sql_counts_distinct_work_key():
    assert "func.count(distinct(SCPublishTask.work_key))" in SC2_SRC, \
        "本小时已建部数没有按作品去重"


def test_engine_quota_counts_works_not_tasks():
    """配额 2 部 + 两个渠道 → 排 2 部作品（4 条任务），不是 1 部。"""
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 6})
        _set_hourly(db, 6)
        _add_channel(db)
        _inject_plan(db, {_this_hour(): {CAT_A: 2}}, base_cap=2)
        sc2.plan_engine(db)
        works = int(db.scalar(select(func.count(distinct(SCPublishTask.work_key)))) or 0)
        tasks = int(db.scalar(select(func.count(SCPublishTask.id))) or 0)

    # 口径反证：老口径下「1 部作品 = 2 条」会立刻把配额 2 用满，只排得出 1 部。
    assert sc2._even_allows({CAT_A: 2}, CAT_A, 2) is False
    assert works == 2, f"配额 2 部就该排 2 部作品，实际 {works}"
    assert tasks == 4, f"2 部 × 2 渠道 = 4 条任务，实际 {tasks}"


def test_engine_and_forecast_share_work_unit():
    """实跑与预测必须用同一个单位（v2.1.19 红线）。"""
    assert "category_work_counts" in SC2_SRC, "实跑侧没有作品级计数"
    assert "cat_works" in SC2_SRC, "预测侧没有作品级计数"
    # 均匀排期门禁不许再拿任务条数去比（比对的是部）
    start = SC2_SRC.index("def plan_engine(")
    body = SC2_SRC[start:SC2_SRC.index("def forecast_schedule(", start)]
    gate = body[body.index("if not _even_allows(even_quota, category.category_key"):]
    gate = gate[:gate.index("break")]
    assert "category_task_counts" not in gate, "实跑的作品级门禁还在按条数比"


def test_forecast_quota_is_in_works():
    """预测侧同样按部：配额 2 部 + 两个渠道 → 该小时 2 部作品、4 条任务。"""
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 6})
        _set_hourly(db, 6)
        _add_channel(db)
        _inject_plan(db, {h: {CAT_A: 2} for h in range(24)}, base_cap=2)
        out = sc2.forecast_schedule(db)

    # 只取「未来的整点」：当前小时受匀速放行（`_hourly_due`，按分钟折算）约束，
    # 到此刻可能只放下 1 部，与名额口径无关。
    row = next((it for h in out["hours"] if int(h["hour"]) > out["now_hour"]
                for it in (h.get("items") or [])
                if it.get("category_key") == CAT_A), None)
    assert row is not None, "预测里没有该分类的行"
    assert row["works_total"] == 2, f"配额 2 部该有 2 部作品，实际 {row['works_total']}"
    assert row["count"] == 4, f"2 部 × 2 渠道 = 4 条任务，实际 {row['count']}"


# ===========================================================================
# 三、余量回流：名额夹到库存以内（用户口径第 3 条 · 空转）
# ===========================================================================
def test_fit_never_exceeds_original_total():
    out = sc2._fit_quota_to_backlog({"a": 2, "b": 2}, {"a": 0, "b": 0})
    assert out == {}, "库存全为 0 时不该凭空留名额"


def test_fit_caps_each_category_by_room():
    assert sc2._fit_quota_to_backlog({"a": 5}, {"a": 2}) == {"a": 2}


def test_fit_gives_unused_slots_to_partner_with_stock():
    out = sc2._fit_quota_to_backlog({"a": 1, "b": 1, "c": 1},
                                    {"a": 0, "b": 5, "c": 5})
    assert "a" not in out, f"库存为 0 的分类不该留在方案里：{out}"
    assert sum(out.values()) == 3, f"削下来的名额必须回流，总额不变：{out}"


def test_fit_keeps_total_capped_by_room():
    """伙伴也没余量时，剩余名额只能空着 —— 绝不突破原总额。"""
    out = sc2._fit_quota_to_backlog({"a": 3, "b": 1}, {"a": 1, "b": 1})
    assert sum(out.values()) == 2, out
    assert out["a"] == 1 and out["b"] == 1


def test_fit_empty_input():
    assert sc2._fit_quota_to_backlog({}, {"a": 1}) == {}
    assert sc2._fit_quota_to_backlog({"a": 1}, {}) == {}


def test_build_skips_category_without_stock():
    """没有在库作品的分类不进方案，它那份名额回流给有货的分类。"""
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0}, cats=(CAT_A, CAT_B, CAT_C))
        _set_hourly(db, 3)
        out = sc2.build_even_plan(db)
        plan = sc2.even_plan_of(db)

    assert out["ok"] is True, out
    names = {d["name"] for d in out["detail"]}
    assert "喜剧" not in names, f"空库存的分类不该分到名额：{names}"
    for h, row in plan["items"].items():
        assert sum(row.values()) == 3, f"{h} 点的名额被空转掉了：{row}"


def test_build_reports_empty_library():
    """库是空的时候不许编一份「每小时 X 部」的方案出来。"""
    with SessionLocal() as db:
        _plan(db, cycle={"hour_max": 0})
        _set_hourly(db, 3)
        out = sc2.build_even_plan(db)
        ver = sc2._active_version(db)
        cfg = json.loads(ver.config_json or "{}")

    assert out["ok"] is False, out
    assert "没有在库作品" in out["reason"], out
    assert "even_plan" not in cfg, "拒绝了就不许往配置里写方案"


# ===========================================================================
# 四、库存漂移：排期后又入库要看得到
# ===========================================================================
def test_api_reports_drift_after_new_arrivals():
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0})
        _set_hourly(db, 4)
        assert sc2.build_even_plan(db)["ok"] is True
        base = sc2.api_even_plan(db)
        assert base["drift"] == 0 and base["stale"] is False, base

        # 排期完成之后又入库 3 部
        for i in range(3):
            _live(db, f"后入库{i}", f"f-late{i}", CAT_A)
        db.commit()
        after = sc2.api_even_plan(db)

    assert after["snapshot_works"] == base["snapshot_works"]
    assert after["drift"] == 3, after
    assert after["stale"] is True, "库存变了却不提示，用户永远不知道方案该重算"
    assert after["current_works"] == base["snapshot_works"] + 3


def test_api_drift_zero_when_library_unchanged():
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0})
        _set_hourly(db, 4)
        sc2.build_even_plan(db)
        out = sc2.api_even_plan(db)

    assert out["drift"] == 0 and out["stale"] is False
    assert out["snapshot_works"] == 20, out   # _seed 默认 10 + 10


def test_api_still_hides_backend_keys_with_drift():
    """加了漂移字段也不能把后端名词带到页面上。"""
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0})
        _set_hourly(db, 4)
        sc2.build_even_plan(db)
        blob = json.dumps(sc2.api_even_plan(db), ensure_ascii=False)

    for bad in ("genre:", "even_plan", "category_key", "backlog_snapshot"):
        assert bad not in blob, f"回显里露出了后端名词：{bad}"


# ===========================================================================
# 五、前端
# ===========================================================================
def test_frontend_shows_drift_hint():
    assert "scEvenPlan.stale" in APP_VUE, "库存漂移没有提示入口"
    assert "scEvenPlan.drift" in APP_VUE and "scEvenPlan.snapshot_works" in APP_VUE
    assert "scEvenPlan.current_works" in APP_VUE
    assert "sc-even-drift" in APP_VUE, "漂移提示没有自己的样式（会和普通明细混在一起）"


def test_version_is_current_everywhere():
    """版本锁（v2.1.47 建立，此后每版随发版一起推进）。"""
    assert 'version="2.1.49"' in (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
    assert "v2.1.49 · Asia/Shanghai" in APP_VUE
    assert json.loads((ROOT / "frontend/package.json").read_text(
        encoding="utf-8"))["version"] == "2.1.49"
    assert "当前版本：v2.1.49" in (ROOT / "README.md").read_text(encoding="utf-8")
