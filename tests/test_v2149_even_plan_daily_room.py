"""v2.1.49 验收：一键 AI 调度不再把名额押在「今天排不出」的分类上。

真机现象（用户 2026-11-25 原话）：「20 点整条 0 任务，额度白白作废」。
排查结论（上一轮，只读取证）：
  ① `build_even_plan` 分配名额时**只看在库待排量，不看当日还剩多少额度**。
     某分类当日已到 `category_day_max`，`plan_engine` 侧的 `_sched_allows_core`
     会直接拒掉它，可方案里照样写着「这个分类这个小时能出 N 部」→ 该整点 0 任务。
  ② 排期表里还有一行「动作冒险」这种**没有任何作品归属的分类键**
     （TMDB 官方分类是「动作」「冒险」两条，不存在合并的那种），它有排期行、
     库存恒为 0，同样白占名额，且在页面上被显示成「未分类」。
  ③ 名额按原始库存算权重，一个只剩 1 部额度的分类与一个还剩 40 部的分类
     在方案里看不出区别。

本文件锁五件事：
  ① 今日已达单分类上限的分类**不进方案**（反证：不过滤时它确实会拿到名额）；
  ② 权重与每小时上限用「今天真能推的部数」= min(在库待排量, 今日剩余额度 ÷ 渠道数)；
  ③ 排不上的分类进 `skipped` 并带**可读中文原因**，不静默消失、不暴露内部键；
  ④ **刻意不逐小时扣减**：方案是每小时上限，不是全天预算切片（反证见第 7 条用例）；
  ⑤ 全部排不出 / 当日总额度用尽 → 如实拒绝并说明，不生成一份假方案。
"""
import json
from datetime import datetime, timezone

import pytest
from sqlalchemy import delete, select

from app import share_center_v2 as sc2, runtime_config
from app.db import SessionLocal, migrate
from app.models import (
    AppConfig, AuditLog, Bot, Channel, QueueItem, Resource, SCCategory,
    SCCategoryCursor, SCCategorySchedule, SCHourWindow, SCLibraryVersion,
    SCPublishTask, SCPushList, SCPushListItem, SCSharePlan, SCSharePlanVersion,
    SCWorkCategory, ShareTask,
)

# 造数与清库复用 v2.1.46 的同一套（分类 / 作品 / 计划版本口径完全一致）
from test_v2146_even_schedule import (  # noqa: E402
    CAT_A, CAT_B, WIPED, _seed, _set_hourly,
)

CAT_C = "genre:喜剧"
# 「动作冒险」是真实工单里出现过的那行：TMDB 没有这个合并分类，作品永远不会归到它名下。
CAT_BOGUS = "genre:动作冒险"

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
# 造数：今天的发布任务（条），用来把某个分类顶到当日上限
# ---------------------------------------------------------------------------
def _today_tasks(db, cat, count, *, status="published", chat_id="@chan"):
    lst = db.scalar(select(SCPushList).where(SCPushList.list_key == "t49"))
    if lst is None:
        lst = SCPushList(list_key="t49", name="t49", status="active", source="plan")
        db.add(lst)
        db.flush()
    for i in range(count):
        wk = f"wk-{cat}-{i}"
        item = SCPushListItem(
            list_id=lst.id, work_key=wk, library_version_id=0, category_key=cat,
            target_type="channel", bot_id=0, chat_id=chat_id,
            dedup_key=f"d49-{cat}-{chat_id}-{i}"[:255],
            source_kind="first", status="planned")
        db.add(item)
        db.flush()
        db.add(SCPublishTask(list_item_id=item.id, work_key=wk, plan_version_id=0,
                             kind="first", status=status, priority=0,
                             scheduled_at=datetime.now(timezone.utc)))
        db.flush()
    db.commit()


def _add_channel(db, chat_id="-1008888"):
    bot = db.scalar(select(Bot))
    db.add(Channel(bot_id=bot.id, name=chat_id, chat_id=chat_id,
                   kind="group", enabled=True))
    db.commit()


def _names(out):
    return {d["name"] for d in out.get("detail", [])}


def _skip_names(out):
    return {s["name"]: s["reason"] for s in out.get("skipped", [])}


# ===========================================================================
# 一、当日已到上限的分类不进方案
# ===========================================================================
def test_category_at_daily_ceiling_is_excluded():
    """「剧情」今天已推满 4 部（上限 4）→ 方案里不该再有它，名额全给「动作」。"""
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0, "category_day_max": 4})
        _set_hourly(db, 6)
        _today_tasks(db, CAT_A, 4)          # 剧情：4/4，已触顶
        out = sc2.build_even_plan(db)
        plan = sc2.even_plan_of(db)

    assert out["ok"] is True, out
    assert "剧情" not in _names(out), f"触顶分类不该分到名额：{_names(out)}"
    assert _skip_names(out).get("剧情") == "今日已达单分类每日上限", _skip_names(out)
    # 反证：不做「今日剩余额度」过滤时，剧情有 10 部在库，按权重必然拿到名额。
    # 这里断言它连一个小时都不该出现（不是「分得少」而是「不该有」）。
    for hour, row in plan["items"].items():
        assert CAT_A not in row, f"{hour} 点仍给触顶分类分了名额：{row}"


def test_weight_reflects_remaining_day_room():
    """剧情只剩 1 部额度、动作还有 4 部 → 动作拿到的名额必须明显更多。"""
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0, "category_day_max": 4})
        _set_hourly(db, 6)
        _today_tasks(db, CAT_A, 3)          # 剧情：3/4 → 只剩 1 部
        out = sc2.build_even_plan(db)
        plan = sc2.even_plan_of(db)

    assert out["ok"] is True, out
    rows = plan["items"]
    sample = rows[sorted(rows, key=lambda x: int(x))[0]]
    assert sample.get(CAT_A, 0) <= 1, f"只剩 1 部额度却分了 {sample}"
    assert sample.get(CAT_B, 0) > sample.get(CAT_A, 0), f"额度应流向还有余量的分类：{sample}"


def test_multi_channel_day_room_is_divided_by_channels():
    """双渠道：一部作品两条任务 → 日额度折部数要除以渠道数，不能按条数当部数。"""
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0, "category_day_max": 4})
        _set_hourly(db, 6)
        _add_channel(db, "-1008889")        # 两个启用渠道
        out = sc2.build_even_plan(db)
        plan = sc2.even_plan_of(db)

    assert out["ok"] is True, out
    cap = {c["name"]: c["works"] for c in out["capacity"]}
    assert cap.get("剧情") == 2, f"日额度 4 条 ÷ 2 渠道 = 2 部：{cap}"
    for hour, row in plan["items"].items():
        assert row.get(CAT_A, 0) <= 2, f"{hour} 点给了超过日额度折算的名额：{row}"


# ===========================================================================
# 二、排不上的分类必须说清原因
# ===========================================================================
def test_bogus_genre_key_is_reported_not_silently_dropped():
    """排期表里那行「动作冒险」：库存恒为 0 → 进 skipped，且名称不能是内部键。"""
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0}, cats=(CAT_A, CAT_B, CAT_BOGUS))
        _set_hourly(db, 3)
        out = sc2.build_even_plan(db)
        blob = json.dumps(out, ensure_ascii=False)

    assert out["ok"] is True, out
    reasons = {s["reason"] for s in out["skipped"]}
    assert "当前没有在库待排作品" in reasons, out["skipped"]
    assert "genre:" not in blob, "回显里露出了后端分类键"
    assert "category_key" not in blob
    # 名称走 `_category_display_name`：不在 TMDB 字典里的键显示为「未分类」，
    # 绝不把 `genre:动作冒险` 原样丢给页面。
    assert all(not s["name"].startswith("genre:") for s in out["skipped"])


def test_all_categories_exhausted_rejects_with_reason():
    """所有分类今天都排不出 → 如实拒绝，并说清各有多少个。"""
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0, "category_day_max": 1})
        _set_hourly(db, 6)
        _today_tasks(db, CAT_A, 1)
        _today_tasks(db, CAT_B, 1)
        out = sc2.build_even_plan(db)

    assert out["ok"] is False, out
    assert "今天所有分类都排不出作品了" in out["reason"], out["reason"]
    assert "2 个已到当日上限" in out["reason"], out["reason"]


def test_daily_total_cap_reached_rejects():
    """单日推送总上限已用尽 → 拒绝生成（调度侧本来也会整轮拒绝）。"""
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0, "day_max": 2})
        _set_hourly(db, 6)
        _today_tasks(db, CAT_A, 2)
        out = sc2.build_even_plan(db)

    assert out["ok"] is False, out
    assert "今天的推送总上限已经用完了" in out["reason"], out["reason"]


def test_void_tasks_do_not_consume_day_room():
    """作废任务不吃额度：已作废 4 条不该把分类顶到当日上限（v2.1.47 口径）。"""
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0, "category_day_max": 4})
        _set_hourly(db, 6)
        _today_tasks(db, CAT_A, 4, status="cancelled")
        out = sc2.build_even_plan(db)

    assert out["ok"] is True, out
    assert "剧情" in _names(out), f"作废不该占额度：{_names(out)}"


# ===========================================================================
# 三、刻意不做逐小时扣减（设计决定，用反证锁住）
# ===========================================================================
def test_no_hourly_deduction_keeps_later_hours():
    """库存 2 部、每小时额度 6 部 → 每个可排小时都要有配额，不能越到后面越少。

    反证的是「逐小时扣减」那种写法：它会把下午之后的小时写成 0，
    而用户的排期本来就与入库交替进行，那样只会造成「下午起再也排不出东西」的假象。
    """
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0}, n_a=2, n_b=2)
        _set_hourly(db, 6)
        out = sc2.build_even_plan(db)
        plan = sc2.even_plan_of(db)

    assert out["ok"] is True, out
    hours = sorted(plan["items"], key=lambda x: int(x))
    assert len(hours) >= 12, f"可排小时数异常少：{hours}"
    totals = {h: sum(plan["items"][h].values()) for h in hours}
    assert min(totals.values()) == max(totals.values()), \
        f"名额不该随时间递减（这就是扣减写法的症状）：{totals}"
    for h in hours:
        assert plan["items"][h].get(CAT_A, 0) <= 2, f"{h} 点超过库存：{plan['items'][h]}"


# ===========================================================================
# 四、回显与前端
# ===========================================================================
def test_api_even_plan_exposes_skipped_and_capacity():
    with SessionLocal() as db:
        _seed(db, cycle={"hour_max": 0, "category_day_max": 4}, cats=(CAT_A, CAT_B, CAT_C))
        _set_hourly(db, 6)
        _today_tasks(db, CAT_A, 4)
        sc2.build_even_plan(db)
        out = sc2.api_even_plan(db)
        blob = json.dumps(out, ensure_ascii=False)

    assert out["active"] is True
    assert out["skipped"], out
    assert {s["name"] for s in out["skipped"]} == {"剧情", "喜剧"}, out["skipped"]
    assert out["capacity"], out
    for bad in ("genre:", "category_key", "room_works", "backlog_snapshot"):
        assert bad not in blob, f"回显里露出了后端名词：{bad}"


def test_frontend_shows_skip_reasons():
    """页面上要能看见「哪些分类没分到、为什么」，不能只在接口里给。"""
    assert "scEvenPlan.skipped" in APP_VUE, "状态条/明细弹窗没有消费 skipped"
    assert "本次没有分到名额的分类" in APP_VUE
    assert "sc-even-skip" in APP_VUE
    assert "scEvenPlan = ref({ active: false, detail: [], skipped: []" in APP_VUE \
        or "skipped: []" in APP_VUE.split("const scEvenPlan")[1][:200]


# ===========================================================================
# 五、源码守门
# ===========================================================================
def test_build_even_plan_uses_today_counts():
    """`build_even_plan` 必须真的去查「今日已推」，而不是又把库存当唯一依据。"""
    seg = SC2_SRC.split("def build_even_plan")[1].split("def clear_even_plan")[0]
    assert "_category_today_counts(db)" in seg, "没查今日各分类已推条数"
    assert "category_day_max" in seg, "排期前没看单分类每日上限"
    assert "_category_today_counts" in SC2_SRC, "批量查询函数缺失"


def test_static_allocation_path_is_gone():
    """旧写法（每个小时都用原始 backlog 当权重/余量）不能再回来。"""
    seg = SC2_SRC.split("def build_even_plan")[1].split("def clear_even_plan")[0]
    assert "backlog.get(c.category_key) or 0))\n                   for c in cands}" not in seg
    assert "room_works" in seg


def test_version_is_current_everywhere():
    """版本六处 + 版本锁：发版前必须全部对齐（本用例就是那把锁）。"""
    main_src = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
    assert 'version="2.1.65"' in main_src
    assert '"version": "2.1.65"' in main_src
    assert "v2.1.65 · Asia/Shanghai" in APP_VUE
    pkg = json.loads((ROOT / "frontend/package.json").read_text(encoding="utf-8"))
    assert pkg["version"] == "2.1.65"
    lock = json.loads((ROOT / "frontend/package-lock.json").read_text(encoding="utf-8"))
    assert lock["version"] == "2.1.65"
    assert lock["packages"][""]["version"] == "2.1.65"
    assert "当前版本：v2.1.65" in (ROOT / "README.md").read_text(encoding="utf-8")
