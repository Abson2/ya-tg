"""v2.1.53 验收：空档时段由「作品多的分类」补位。

用户原话（2026-11-26）：
  「能否让ai一键调度后让无作品的分类让作品多的分类做替补，弥补时段无作品可推的情况」

前置排查（同一天 15:04，线上只读）：
  方案已生成并落库（active version id=1、额度 60、18 个时段），但 `items` 里只有
  0–11 与 18–23 点，**12–17 点全部缺席**。那几个小时的分类（未分类到上限 / 儿童 /
  纪录 / 冒险 / 肥皂剧 / 西部 / 动作冒险）全都没货或到顶，`build_even_plan` 凑不出
  候选就 `continue` —— 额度再大也没用，因为没有可分配的对象。

必须先讲清的一个事实（否则会误改）：
  「同一时段内无货分类把名额让给有货分类」**早就实现了**（`_fit_quota_to_backlog`）。
  真正缺的只有一种：**整个时段的分类一个都出不了货**，只能跨时段借人。

因此本版动三处，缺一不可（只改 `build_even_plan` 等于白改 —— 方案写了配额，
调度侧仍会被「分类自己的时段窗口」拦下）：
  ① `build_even_plan`：空档小时按全库「今天真能推的部数」分一份配额，记入 `borrowed_hours`；
  ② `_sched_allows_core(..., check_window=False)`：补位小时只跳过「分类自己的时段窗口」，
     **每小时 / 每日上限照旧全部生效**；
  ③ `plan_engine` 与 `forecast_schedule` 两处同改（v2.1.19 红线：预测与实跑同源）。

三条边界（改代码才动，不是配置）：
  · 只补**当前小时到 24 点**：已经过去的小时补了也没意义（用户拍板口径）；
  · 绝不突破用户主动关掉的时间：主推送窗口外、静默保护内一律不补；
  · 不发明额度：`cap` 仍只来自发布规则，`SCCategorySchedule` 一行不写。

本文件锁：
  - `_borrowed_hours` 解析（缺失字段 → 空集，老方案行为零变化）
  - `_hour_open_for_borrow` 尊重主推送窗口与静默保护
  - `_borrow_quota` 不超 cap、不超单分类每小时上限、空库存返回空
  - `check_window=False` **只**跳过窗口，小时/日上限仍拦（关键反证）
  - 端到端：空档小时拿到配额且计入 `borrowed_hours`；关掉开关后空档仍然空
  - 不补已经过去的小时
  - 实跑与预测两处都按同一份 `borrowed_hours` 放行（源码守门）
  - `/sc/even-plan` 的 detail 带 `backlog`（修「待排作品」整列显示「—」的回显 bug）
"""
import json
from datetime import datetime, timezone

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
RC_SRC = (ROOT / "backend/app/runtime_config.py").read_text(encoding="utf-8")
APP_VUE = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")

CAT_A = "genre:剧情"          # 有货，窗口 0–8
CAT_B = "genre:喜剧"          # 有货，窗口 18–24
CAT_EMPTY = "genre:西部"      # 没货，窗口 8–18 → 8–17 点成为空档

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
def _live(db, title, fid, cat):
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
    db.add(SCWorkCategory(work_key=wk, category_key=cat))
    db.flush()
    return wk


SCHED = {CAT_A: (0, 8), CAT_EMPTY: (8, 18), CAT_B: (18, 24)}


def _seed(db, *, stock=None, cycle=None, with_empty_cat=True):
    """三个分类：剧情 0–8 点、西部 8–18 点（默认无货）、喜剧 18–24 点。

    于是 8–17 点「窗口里只有一个没货的分类」→ 空档，正是真机 12–17 点的形状。
    """
    stock = stock or {CAT_A: 5, CAT_B: 5}
    plan = SCSharePlan(name="p", plan_type="daily", enabled=True)
    db.add(plan)
    db.flush()
    cfg = {"category_limit": 20, "work_limit": 50,
           "cycle_plan": dict(cycle or {"hour_max": 0, "quiet_enabled": False})}
    cfg["cycle_plan"].setdefault("quiet_enabled", False)
    ver = SCSharePlanVersion(plan_id=plan.id, version_no=1, status="active",
                             config_json=json.dumps(cfg))
    db.add(ver)
    db.flush()
    ensure_full_day_hour_window(db, ver.id)
    cats = [CAT_A, CAT_EMPTY, CAT_B] if with_empty_cat else [CAT_A, CAT_B]
    for i, key in enumerate(cats):
        db.add(SCCategory(category_key=key, name=key.split(":")[-1],
                          enabled=True, priority=len(cats) - i))
        ws, we = SCHED[key]
        db.add(SCCategorySchedule(plan_version_id=ver.id, category_key=key,
                                  enabled=True, window_start=ws, window_end=we,
                                  week_windows=""))
    bot = Bot(name="b", token="x", enabled=True)
    db.add(bot)
    db.flush()
    db.add(Channel(bot_id=bot.id, name="@chan", chat_id="@chan",
                   kind="channel", enabled=True))
    runtime_config.set_many(db, {"sc_quick_publish_enabled": "true",
                                 "sc_quick_publish_hourly_limit": "6",
                                 "sc_conflict_policy": "prefer_backlog"})
    db.commit()
    for cat, n in stock.items():
        for i in range(n):
            _live(db, f"{cat}-{i}", f"f-{cat}-{i}", cat)
    db.commit()
    return ver


@pytest.fixture
def clock_1500(monkeypatch):
    """把 `build_even_plan` 看到的「现在」冻到 2026-11-26 15:00（上海）。

    不冻结的话「补位只算当前小时之后」这条会随真实时刻漂移，深夜跑全量必然假红。
    """
    real = datetime

    class _FakeDT(real):
        @classmethod
        def now(cls, tz=None):
            return real(2026, 11, 26, 15, 0, tzinfo=sc2.shanghai_tz())

    monkeypatch.setattr(sc2, "datetime", _FakeDT)
    return _FakeDT


EMPTY_SLOT_HOURS = list(range(8, 18))       # 西部独占窗口、却没货 → 空档


@pytest.fixture(autouse=True)
def _no_retime(monkeypatch):
    """v2.1.54：一键调度现在会**先按库存重排分类时段**（`_retime_schedules_by_stock`），
    空档从配置层就不存在了 —— 那正是用户要的效果。

    本文件验证的是**运行时补位兜底**（配置层万一没排满时的那道保险），
    所以这里把重排关掉，让补位机制单独被验证。重排本身由
    `test_v2154_schedule_by_stock.py` 负责锁。
    """
    from app import share_center_v2 as _sc2
    monkeypatch.setattr(
        _sc2, "_retime_schedules_by_stock",
        lambda *a, **k: {"moved": 0, "details": [], "truncated": 0})


# ===========================================================================
# 一、纯函数
# ===========================================================================
def test_borrowed_hours_parses_only_valid_hours():
    assert sc2._borrowed_hours({"borrowed_hours": [15, 16, "17"]}) == {15, 16, 17}
    # 老方案没有这个字段 → 空集，行为必须与改前一致
    assert sc2._borrowed_hours({}) == set()
    assert sc2._borrowed_hours({"borrowed_hours": None}) == set()
    # 越界 / 脏值一律丢掉，绝不让它把判据算歪
    assert sc2._borrowed_hours({"borrowed_hours": [-1, 24, 99, "x", 3]}) == {3}


def test_borrow_hour_respects_main_window_and_quiet():
    """补位**绝不**突破用户主动关掉的时间。"""
    assert sc2._hour_open_for_borrow(15, None, None) is True
    assert sc2._hour_open_for_borrow(15, None, (9, 22)) is True
    assert sc2._hour_open_for_borrow(23, None, (9, 22)) is False, "主推送窗口外不许补"
    assert sc2._hour_open_for_borrow(3, (1, 8), None) is False, "静默保护内不许补"
    assert sc2._hour_open_for_borrow(9, (1, 8), None) is True


def test_borrow_quota_never_exceeds_cap_or_hour_max():
    """补位用的是同一份 cap —— 不发明任何额度。"""
    q = sc2._borrow_quota({CAT_A: 5, CAT_B: 15}, 6, 0)
    assert sum(q.values()) == 6, "额度只有 6 份，补位不能变多"
    assert all(v > 0 for v in q.values())
    assert q[CAT_B] >= q[CAT_A], "作品多的分类应当多担"
    # 单分类每小时上限仍然生效
    q2 = sc2._borrow_quota({CAT_A: 5, CAT_B: 15}, 6, 1)
    assert all(v <= 1 for v in q2.values()), q2
    # 全库都没货 → 不补（绝不编一份配额出来）
    assert sc2._borrow_quota({}, 6, 0) == {}
    assert sc2._borrow_quota({CAT_A: 0}, 6, 0) == {}


def test_check_window_only_skips_the_window_gate():
    """关键反证：`check_window=False` **只**跳过窗口，小时/日上限一条都不能放。"""
    sched = SCCategorySchedule(category_key=CAT_A, window_start=0, window_end=8,
                               week_windows="", enabled=True)
    kw = dict(hour=15, weekday=4, per_category_count=0,
              cycle_hour_max=0, cycle_day_max=0)
    assert sc2._sched_allows_core(sched, **kw) is False, "窗口外本来就该拒"
    assert sc2._sched_allows_core(sched, **kw, check_window=False) is True
    # 上限照旧拦
    assert sc2._sched_allows_core(
        sched, hour=15, weekday=4, per_category_count=3,
        cycle_hour_max=3, cycle_day_max=0, check_window=False) is False
    assert sc2._sched_allows_core(
        sched, hour=15, weekday=4, per_category_count=0,
        cycle_hour_max=0, cycle_day_max=2, today_count=2,
        check_window=False) is False


def test_borrow_switch_reads_runtime_config():
    with SessionLocal() as db:
        assert sc2._borrow_enabled(db) is True, "默认开（用户要的就是这个能力）"
        runtime_config.set_many(db, {"sc_even_borrow_enabled": "false"})
        db.commit()
        assert sc2._borrow_enabled(db) is False


# ===========================================================================
# 二、端到端：空档小时真的拿到配额
# ===========================================================================
def test_empty_slot_gets_borrowed_quota(clock_1500):
    """15:00 生成 → 8–14 点已过去不补，15–17 点必须补上。"""
    with SessionLocal() as db:
        _seed(db)
        out = sc2.build_even_plan(db)
        plan = sc2.even_plan_of(db)

    assert out["ok"] is True, out
    assert out["borrow_enabled"] is True
    borrowed = set(out["borrowed_hours"])
    assert borrowed == {15, 16, 17}, f"空档应是 15–17 点：{sorted(borrowed)}"
    assert set(plan["borrowed_hours"]) == borrowed, "落库方案里必须带同一份名单"
    for h in borrowed:
        row = plan["items"][str(h)]
        assert row, f"{h} 点没有配额"
        assert sum(row.values()) <= out["base_cap"]
    # 三个空档小时的配额按库存权重分给「剧情」和「喜剧」，西部没货不参与
    assert CAT_EMPTY not in plan["items"]["15"]
    assert CAT_A in plan["items"]["15"] and CAT_B in plan["items"]["15"]


def test_borrow_never_touches_past_hours(clock_1500):
    """已经过去的小时补了也没意义 —— 用户拍板口径「只补当前到 24 点」。"""
    with SessionLocal() as db:
        _seed(db)
        out = sc2.build_even_plan(db)
    assert all(h >= 15 for h in out["borrowed_hours"]), out["borrowed_hours"]
    assert 8 not in out["borrowed_hours"] and 14 not in out["borrowed_hours"]


def test_borrow_off_keeps_slot_empty(clock_1500):
    """开关关掉 → 空档仍然空着，方案与改前完全一致。"""
    with SessionLocal() as db:
        _seed(db)
        runtime_config.set_many(db, {"sc_even_borrow_enabled": "false"})
        db.commit()
        out = sc2.build_even_plan(db)
        plan = sc2.even_plan_of(db)
    assert out["borrow_enabled"] is False
    assert out["borrowed_hours"] == []
    for h in EMPTY_SLOT_HOURS:
        assert str(h) not in plan["items"], f"{h} 点不该有配额"


def test_non_empty_slots_are_not_marked_borrowed(clock_1500):
    """本来就有货的时段不算补位 —— 别把正常时段也标成 borrowed。"""
    with SessionLocal() as db:
        _seed(db)
        out = sc2.build_even_plan(db)
        plan = sc2.even_plan_of(db)
    borrowed = set(out["borrowed_hours"])
    for h in (19, 20, 21):          # 喜剧的窗口，本来就有货
        assert str(h) in plan["items"] and h not in borrowed, h


# ===========================================================================
# 三、实跑 / 预测同源（v2.1.19 红线）
# ===========================================================================
def _built_by_cat(db):
    from sqlalchemy import func
    rows = db.execute(
        select(SCPushListItem.category_key, func.count(SCPublishTask.id))
        .join(SCPublishTask, SCPublishTask.list_item_id == SCPushListItem.id)
        .group_by(SCPushListItem.category_key)).all()
    return {str(k): int(n or 0) for k, n in rows if k}


def test_engine_actually_builds_tasks_in_borrowed_hour(clock_1500):
    """本文件的核心反证：15 点这个空档，补位后**真的**建得出任务。

    改前（不传 `check_window`）：方案里 15 点有配额，但剧情窗口 0–8、喜剧窗口 18–24，
    两个都被 `_sched_allows_core` 按窗口拒掉 → 15 点 0 任务。
    这正是真机「额度 60 却排不出来」的形状。
    """
    with SessionLocal() as db:
        _seed(db)
        assert sc2.build_even_plan(db)["ok"] is True
        sc2.plan_engine(db)
        built = _built_by_cat(db)
    assert sum(built.values()) > 0, f"补位没生效，15 点仍然 0 任务：{built}"
    assert built.get(CAT_A, 0) + built.get(CAT_B, 0) > 0, built
    assert CAT_EMPTY not in built, "没货的分类不该建任务"


def test_plan_engine_reads_borrowed_hours():
    src = SC2_SRC
    assert "borrowed_hours = _borrowed_hours(even_plan)" in src
    assert "if hour_window and hour_window.category_key and sh_hour not in borrowed_hours:" in src, (
        "补位小时仍会被「小时窗口绑定单分类」缩表 → 补了也排不出来")
    assert "check_window=sh_hour not in borrowed_hours" in src


def test_forecast_uses_the_same_borrow_rule():
    """预测侧必须与实跑同一份判据，否则同一小时预测 0、实跑有货 = 骗用户。"""
    assert "borrowed_hours = _borrowed_hours(even_plan)" in SC2_SRC
    assert "if bound_key and h not in borrowed_hours:" in SC2_SRC
    assert "check_window=h not in borrowed_hours" in SC2_SRC


def test_borrow_does_not_write_schedule_rows(clock_1500):
    """红线：`SCCategorySchedule` 只读不写 —— 补位不许动用户的时段配置。"""
    with SessionLocal() as db:
        _seed(db)
        before = [(s.category_key, s.window_start, s.window_end, s.ai_adopted)
                  for s in db.scalars(select(SCCategorySchedule).order_by(
                      SCCategorySchedule.id)).all()]
        sc2.build_even_plan(db)
        after = [(s.category_key, s.window_start, s.window_end, s.ai_adopted)
                 for s in db.scalars(select(SCCategorySchedule).order_by(
                     SCCategorySchedule.id)).all()]
    assert before == after, "补位改动了分类时段配置"


# ===========================================================================
# 四、顺手修的回显 bug：待排作品整列显示「—」
# ===========================================================================
def test_even_plan_api_detail_carries_backlog(clock_1500):
    """`/sc/even-plan` 的 detail 此前漏写 `backlog`，前端只能显示「—」。"""
    with SessionLocal() as db:
        _seed(db)
        sc2.build_even_plan(db)
        api = sc2.api_even_plan(db)
    assert api["active"] is True
    assert api["detail"], "明细不能为空"
    for row in api["detail"]:
        assert "backlog" in row, f"{row['name']} 的明细缺 backlog → 前端会显示「—」"
        assert isinstance(row["backlog"], int)
    backlog_by_name = {r["name"]: r["backlog"] for r in api["detail"]}
    assert backlog_by_name.get("剧情") == 5, backlog_by_name
    # 生成侧与回显侧必须是同一个数
    assert api["snapshot_works"] == 10


def test_even_plan_api_exposes_borrowed_hours(clock_1500):
    with SessionLocal() as db:
        _seed(db)
        sc2.build_even_plan(db)
        api = sc2.api_even_plan(db)
    assert api["borrowed_hours"] == [15, 16, 17]


# ===========================================================================
# 五、配置与页面
# ===========================================================================
def test_config_key_registered():
    from app.runtime_config import DEFAULTS
    assert "sc_even_borrow_enabled" in DEFAULTS
    assert DEFAULTS["sc_even_borrow_enabled"][1] == "true"
    assert "空档时段补位" in RC_SRC


def test_frontend_tells_user_which_hours_were_borrowed():
    """页面必须说出来，否则会被误读成「分类时段配置失效了」。"""
    assert "scEvenPlan.borrowed_hours" in APP_VUE
    assert "已改由库存最多的分类补位" in APP_VUE
    assert "不会被补位占用" in APP_VUE
    # 提示文案必须是业务语言，不许出现后端字段名
    seg = APP_VUE[APP_VUE.index("scEvenPlan.borrowed_hours.map"):]
    seg = seg[:seg.index("</span>")] if "</span>" in seg else seg[:400]
    for bad in ("check_window", "borrowed_hours=", "genre:"):
        assert bad not in seg, f"页面暴露了后端名词：{bad}"
