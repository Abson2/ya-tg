"""v2.1.54 排期按库存分配 / 多时段 / AI 调度升级专项验收。

## 用户口径（原文）

  2（2）「全天默认 12 个分类（根据库存情况允许重复），批量采纳 ai 建议则根据库存、
        分类热门来调整分类排期时间」
  3（2）「一键 ai 调度：调整当前及后续排期分类、时间搭配，填充无排期的空白时段」
  3（3）「注意需要推送任务、推送队列一并联动，且前后端一起动」

## 线上事实（2026-11-27 只读）

  678 部作品里 586 部卡在「未分类」，真实分类只有 92 部、分布在 12 个分类上；
  而排期表 27 行全都配了 2 小时段 —— **15 个零库存的分类各自白占一段**，
  那 15 段必然空档。v2.1.53 的运行时补位只是在补这个配置层的窟窿。

## 本文件锁住的判据

  1. `_segments_by_stock`：段数按库存、库存 0 一段都不给、货多的拿多段；
  2. 段位仍按热门（偏好序）—— 段数与段位是两件事，不许互相顶替；
  3. `_sched_allows_core` 认主段 + 额外段，任一命中即放行，上限照旧生效；
  4. `_ensure_schedules` 把多出来的段写进 `extra_windows`；
  5. `_retime_schedules_by_stock` 只改时段，不动上限 / 启用状态；
  6. `build_even_plan` 会真的重排时段并回显 `retimed`；
  7. 批量采纳跳过零库存分类并给出原因，采纳后算覆盖缺口。
"""
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

CAT_A = "genre:剧情"
CAT_B = "genre:动作"
CAT_C = "genre:悬疑"
CAT_EMPTY = "genre:西部"


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v2154sched_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 's.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _prefs(*keys):
    """造偏好序（顺序即热门排名）。"""
    return [{"category_key": k, "name": k.split(":")[-1], "rank": i + 1}
            for i, k in enumerate(keys)]


# ===========================================================================
# 一、段数按库存、段位按热门
# ===========================================================================
def test_segments_follow_stock_not_preference_order():
    """库存多的拿多段；库存 0 的一段都不给。"""
    from app import share_center_v2 as sc2
    primary = sc2.CATEGORY_WINDOW_SEGMENTS
    prefs = _prefs(CAT_A, CAT_B, CAT_C, CAT_EMPTY)
    # 剧情 60 部、动作 30 部、悬疑 10 部、西部 0 部
    stock = {CAT_A: 60, CAT_B: 30, CAT_C: 10, CAT_EMPTY: 0}
    got = sc2._segments_by_stock(prefs, primary, stock)

    assert CAT_EMPTY not in got, "零库存的分类不许占段 —— 占了就是空档"
    assert sum(len(v) for v in got.values()) == len(primary), "12 段要全部分完"
    assert len(got[CAT_A]) > len(got[CAT_C]), "库存 6 倍的分类必须拿到更多段"
    assert len(got[CAT_A]) >= 4, f"剧情 60/100 应拿到 4 段以上，实际 {len(got[CAT_A])}"
    # 每段都在段池里，且同一分类不重复占同一段
    for key, segs in got.items():
        for seg in segs:
            assert seg in primary, f"{key} 拿到段池外的 {seg}"
        assert len(set(segs)) == len(segs), f"{key} 同一段被占了两次"


def test_segment_position_still_follows_preference():
    """段位按热门：偏好第 1 名先拿白天段（第一个可用段）。"""
    from app import share_center_v2 as sc2
    primary = sc2.CATEGORY_WINDOW_SEGMENTS
    prefs = _prefs(CAT_A, CAT_B)
    stock = {CAT_A: 50, CAT_B: 50}
    got = sc2._segments_by_stock(prefs, primary, stock)
    assert got[CAT_A][0] == primary[0], "热门分类的**第一段**必须是段池排序里的第一段"
    assert got[CAT_B][0] == primary[len(got[CAT_A])], "第二个分类接在后面拿"


def test_all_zero_stock_returns_empty():
    """全库没货 → 一段都不分（绝不凭空造一份分配）。"""
    from app import share_center_v2 as sc2
    got = sc2._segments_by_stock(_prefs(CAT_A, CAT_B),
                                 sc2.CATEGORY_WINDOW_SEGMENTS,
                                 {CAT_A: 0, CAT_B: 0})
    assert got == {}


def test_stock_key_not_in_prefs_still_gets_segments():
    """有库存但不在偏好表里的分类也要分到 —— 否则它的库存无处安放。"""
    from app import share_center_v2 as sc2
    got = sc2._segments_by_stock(_prefs(CAT_A), sc2.CATEGORY_WINDOW_SEGMENTS,
                                 {CAT_A: 10, CAT_C: 90})
    assert CAT_C in got and got[CAT_C], "库存最多的分类不能因为不在偏好表里就被丢掉"
    assert len(got[CAT_C]) >= len(got[CAT_A])


def test_more_categories_than_segments_takes_top_stock():
    """分类数超过段数时，按库存取前 N 个，其余本轮到 0 段。"""
    from app import share_center_v2 as sc2
    keys = [f"genre:类{i}" for i in range(20)]
    stock = {k: i + 1 for i, k in enumerate(keys)}      # 类19 库存最多
    got = sc2._segments_by_stock(_prefs(*keys), sc2.CATEGORY_WINDOW_SEGMENTS, stock)
    assert sum(len(v) for v in got.values()) == len(sc2.CATEGORY_WINDOW_SEGMENTS)
    assert "genre:类19" in got, "库存最多的必须在里面"
    assert "genre:类0" not in got, "库存最少的轮不到段"


# ===========================================================================
# 二、时段窗口判定认额外段
# ===========================================================================
def test_hour_in_window_handles_all_shapes():
    from app import share_center_v2 as sc2
    assert sc2._hour_in_window(9, 8, 10) is True
    assert sc2._hour_in_window(10, 8, 10) is False, "止点不含"
    assert sc2._hour_in_window(23, 22, 2) is True, "跨午夜"
    assert sc2._hour_in_window(1, 22, 2) is True
    assert sc2._hour_in_window(12, 22, 2) is False
    assert sc2._hour_in_window(5, 0, 0) is True, "起止相同 = 全天"


def test_sched_allows_core_accepts_extra_windows(env):
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    S, _ = env
    db = S()
    sched = SCCategorySchedule(category_key=CAT_A, window_start=8, window_end=10,
                               extra_windows=json.dumps([[16, 18], [20, 22]]),
                               enabled=True)
    kw = dict(hour=16, weekday=4, per_category_count=0,
              cycle_hour_max=0, cycle_day_max=0)
    assert sc2._sched_allows_core(sched, **kw) is True, "16 点在额外段里，应放行"
    assert sc2._sched_allows_core(sched, **dict(kw, hour=21)) is True
    assert sc2._sched_allows_core(sched, **dict(kw, hour=13)) is False, "既不在主段也不在额外段"
    # 上限照旧生效：额外段不能变成"免检通道"
    assert sc2._sched_allows_core(sched, **dict(kw, hour=16, per_category_count=3,
                                                cycle_hour_max=3)) is False
    assert sc2._sched_allows_core(sched, **dict(kw, hour=16, cycle_day_max=2,
                                                today_count=2)) is False
    db.close()


def test_sched_extra_windows_drops_garbage(env):
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    S, _ = env
    db = S()
    sched = SCCategorySchedule(category_key=CAT_A,
                               extra_windows='[[16,18],"x",[1],[99,100],[-1,2],[20,22]]')
    assert sc2._sched_extra_windows(sched) == [(16, 18), (20, 22)], "脏值一律丢掉"
    sched.extra_windows = "not json"
    assert sc2._sched_extra_windows(sched) == []
    sched.extra_windows = ""
    assert sc2._sched_extra_windows(sched) == []
    db.close()


def test_schedule_open_hours_includes_extra(env):
    """`_schedule_open_hours` 要认额外段 —— 否则配了多时段的分类在方案里只剩主段。"""
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    S, _ = env
    db = S()
    sched = SCCategorySchedule(category_key=CAT_A, window_start=8, window_end=10,
                               extra_windows=json.dumps([[16, 18]]))
    hours = sc2._schedule_open_hours(sched, None, None)
    assert set(hours) == {8, 9, 16, 17}, hours
    db.close()


# ===========================================================================
# 三、_ensure_schedules 把多段写进库
# ===========================================================================
def _seed_plan(db, *, stock):
    from app.models import (SCSharePlan, SCSharePlanVersion, SCCategory,
                            SCLibraryVersion, SCWorkCategory, Resource)
    from app import utils
    plan = SCSharePlan(name="p", plan_type="daily", enabled=True)
    db.add(plan)
    db.flush()
    ver = SCSharePlanVersion(plan_id=plan.id, version_no=1, status="active",
                             config_json=json.dumps(
                                 {"category_limit": 30, "work_limit": 50,
                                  "cycle_plan": {"quiet_enabled": False}}))
    db.add(ver)
    db.flush()
    for i, (key, n) in enumerate(stock.items()):
        db.add(SCCategory(category_key=key, name=key.split(":")[-1],
                          enabled=True, priority=10 - i))
        for j in range(n):
            res = Resource(file_id=f"{key}-{j}", name=f"{key}-{j}.mkv", ext=".mkv",
                           size=1, utime=1, identify_status="identified",
                           media_type="movie", tmdb_id=1,
                           media_title=f"{key}-{j}", share_dir_name=f"{key}-{j}",
                           share_scope="series_dir", share_status="not_created")
            db.add(res)
            db.flush()
            wk = utils.work_group_key(res)
            db.add(SCLibraryVersion(work_key=wk, version_no=1,
                                    fingerprint=f"fp-{key}-{j}", media_type="movie",
                                    resource_ids_json=json.dumps([res.id]),
                                    metadata_json=json.dumps({"title": f"{key}-{j}"},
                                                             ensure_ascii=False),
                                    change_type="first"))
            db.add(SCWorkCategory(work_key=wk, category_key=key))
    db.commit()
    return ver


def test_ensure_schedules_writes_no_extra_windows(env):
    """v2.1.55 口径变更：**建行只写内置单段**，不再按库存制造多时段。

    原因（用户口径 5）：`_ensure_schedules` 在每次 GET `/category-schedules` 时都会跑，
    如果它按库存写额外时段，「一键恢复默认」刚把多时段清掉，页面一刷新就又回来了 ——
    用户看到的就是「默认点了没用」。多时段只由「一键 AI 调度 / 批量采纳」产生。
    """
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    from sqlalchemy import select
    S, _ = env
    db = S()
    ver = _seed_plan(db, stock={CAT_A: 30, CAT_B: 6})
    sc2._ensure_schedules(db, ver.id)
    db.commit()
    rows = {r.category_key: r for r in db.scalars(
        select(SCCategorySchedule).where(
            SCCategorySchedule.plan_version_id == ver.id)).all()}
    assert not sc2._sched_extra_windows(rows[CAT_A]), "建行不该写额外时段"
    assert not sc2._sched_extra_windows(rows[CAT_B])
    for row in rows.values():
        assert int(row.window_end or 0) > int(row.window_start or 0), "每行仍要有单段时段"
    db.close()


def test_ensure_schedules_keeps_ai_extra_windows(env):
    """带额外时段的行是 AI 的产物 —— 刷新页面不许把它抹回单段。"""
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    from sqlalchemy import select
    S, _ = env
    db = S()
    ver = _seed_plan(db, stock={CAT_A: 30, CAT_B: 6})
    sc2._ensure_schedules(db, ver.id)
    db.commit()
    row = db.scalar(select(SCCategorySchedule).where(
        SCCategorySchedule.category_key == CAT_A))
    row.extra_windows = json.dumps([[4, 6], [6, 8]])
    row.window_start, row.window_end = 2, 4
    row.window_auto = False
    db.commit()
    sc2._ensure_schedules(db, ver.id)
    db.commit()
    db.expire_all()
    row = db.scalar(select(SCCategorySchedule).where(
        SCCategorySchedule.category_key == CAT_A))
    assert (int(row.window_start), int(row.window_end)) == (2, 4), "AI 定的主段不许被改写"
    assert len(sc2._sched_extra_windows(row)) == 2, "AI 定的额外时段不许被抹掉"
    db.close()


def test_ensure_schedules_gives_zero_stock_no_extra(env):
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    from sqlalchemy import select
    S, _ = env
    db = S()
    ver = _seed_plan(db, stock={CAT_A: 10})
    sc2._ensure_schedules(db, ver.id)
    db.commit()
    rows = db.scalars(select(SCCategorySchedule).where(
        SCCategorySchedule.plan_version_id == ver.id)).all()
    for r in rows:
        if r.category_key == CAT_A:
            continue
        assert sc2._sched_extra_windows(r) == [], (
            f"{r.category_key} 零库存却拿到了额外时段")
    db.close()


# ===========================================================================
# 四、一键调度真的重排时段，但只动时段
# ===========================================================================
def test_retime_only_touches_windows(env):
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    from sqlalchemy import select
    S, _ = env
    db = S()
    ver = _seed_plan(db, stock={CAT_A: 40, CAT_B: 8})
    sc2._ensure_schedules(db, ver.id)
    db.commit()
    # 人为把两行改成「手工时段 + 非默认上限 + 停用」
    rows = {r.category_key: r for r in db.scalars(
        select(SCCategorySchedule)).all()}
    rows[CAT_A].window_start, rows[CAT_A].window_end = 3, 5
    rows[CAT_A].hourly_limit, rows[CAT_A].daily_limit = 7, 70
    rows[CAT_A].enabled = False
    db.commit()
    ret = sc2._retime_schedules_by_stock(db, ver, None, None, {CAT_A: 40, CAT_B: 8})
    db.commit()
    db.expire_all()
    row = db.scalar(select(SCCategorySchedule).where(
        SCCategorySchedule.category_key == CAT_A))
    assert ret["moved"] >= 1
    assert (int(row.window_start or 0), int(row.window_end or 0)) != (3, 5), "时段要被重排"
    assert (int(row.hourly_limit or 0), int(row.daily_limit or 0)) == (7, 70), "上限不许动"
    assert row.enabled is False, "启用状态不许动（用户关掉的分类不能被偷偷打开）"
    db.close()


def test_retime_clears_extra_for_zero_stock(env):
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    from sqlalchemy import select
    S, _ = env
    db = S()
    ver = _seed_plan(db, stock={CAT_A: 10})
    sc2._ensure_schedules(db, ver.id)
    db.commit()
    row = db.scalar(select(SCCategorySchedule).where(
        SCCategorySchedule.category_key == CAT_B))
    row.extra_windows = json.dumps([[16, 18]])
    db.commit()
    sc2._retime_schedules_by_stock(db, ver, None, None, {CAT_A: 10})
    db.commit()
    db.expire_all()
    row = db.scalar(select(SCCategorySchedule).where(
        SCCategorySchedule.category_key == CAT_B))
    assert sc2._sched_extra_windows(row) == [], "零库存分类的额外时段要收掉（占着也是空）"
    db.close()


def test_retime_respects_main_window(env):
    """分配出来的段不许落在主推送窗口之外。"""
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    from sqlalchemy import select
    S, _ = env
    db = S()
    ver = _seed_plan(db, stock={CAT_A: 10})
    # 先把排期行建起来（retime 只改已有行，不替用户造行），再手工挪到主窗口外
    sc2._ensure_schedules(db, ver.id)
    db.commit()
    row = db.scalar(select(SCCategorySchedule).where(
        SCCategorySchedule.category_key == CAT_A))
    row.window_start, row.window_end = 0, 2
    row.extra_windows = ""
    db.commit()

    ret = sc2._retime_schedules_by_stock(db, ver, None, (9, 21), {CAT_A: 10})
    db.commit()
    db.expire_all()
    row = db.scalar(select(SCCategorySchedule).where(
        SCCategorySchedule.category_key == CAT_A))
    segs = ([(int(row.window_start or 0), int(row.window_end or 0))]
            + sc2._sched_extra_windows(row))
    for s, e in segs:
        assert int(s) >= 9 and int(e) <= 21, f"段 {s}-{e} 落在主推送窗口 9-21 之外"
    assert ret["moved"] >= 1
    db.close()


def test_build_even_plan_reports_retimed(env):
    """一键调度要把「改了哪些时段」如实回显 —— 否则用户以为只改了数量。"""
    from app import share_center_v2 as sc2
    from app import runtime_config
    from app.models import (Bot, Channel)
    S, _ = env
    db = S()
    ver = _seed_plan(db, stock={CAT_A: 40, CAT_B: 10})
    bot = Bot(name="b", token="x", enabled=True)
    db.add(bot)
    db.flush()
    db.add(Channel(bot_id=bot.id, name="@c", chat_id="@c", kind="channel", enabled=True))
    runtime_config.set_many(db, {"sc_quick_publish_enabled": "true",
                                 "sc_quick_publish_hourly_limit": "3",
                                 "sc_conflict_policy": "prefer_backlog"})
    db.commit()
    # 先建好排期行，再把时段弄成「明显不是库存分配结果」的样子
    sc2._ensure_schedules(db, ver.id)
    db.commit()
    from app.models import SCCategorySchedule
    from sqlalchemy import select
    row = db.scalar(select(SCCategorySchedule).where(
        SCCategorySchedule.category_key == CAT_A))
    row.window_start, row.window_end = 2, 4
    db.commit()

    out = sc2.build_even_plan(db)
    assert out["ok"] is True, out
    assert "retimed" in out, "一键调度必须回报这次改了哪些分类时段"
    assert out["retimed"]["moved"] >= 1
    names = {d["name"] for d in out["retimed"]["details"]}
    assert "剧情" in names, f"剧情从 2-4 被重排，明细里应当有它：{names}"

    api = sc2.api_even_plan(db)
    assert api["retimed"]["moved"] >= 1, "刷新页面后仍要看得到上次改了哪些时段"
    db.close()


# ===========================================================================
# 五、批量采纳：跳过零库存 + 覆盖缺口
# ===========================================================================
def test_adopt_all_skips_zero_stock_with_reason(env):
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    from sqlalchemy import select
    S, _ = env
    db = S()
    ver = _seed_plan(db, stock={CAT_A: 20})
    sc2._ensure_schedules(db, ver.id)
    db.commit()
    out = sc2.api_adopt_schedule_ai_all(
        sc2.Payload(payload={"category_keys": [CAT_A, CAT_B]}), db)
    assert out["ok"] is True, out
    skipped_names = {s["name"] for s in out["skipped_items"]}
    assert "动作" in skipped_names, f"零库存的动作应被跳过并说明原因：{out['skipped_items']}"
    reason = next(s["reason"] for s in out["skipped_items"] if s["name"] == "动作")
    assert "没有待排作品" in reason, reason

    row = db.scalar(select(SCCategorySchedule).where(
        SCCategorySchedule.category_key == CAT_A))
    assert row.ai_adopted is True, "有货的分类必须被采纳"
    db.close()


def test_adopt_all_reports_coverage_gaps(env):
    """采纳完要算「还有哪些小时没人盖」并如实回显（空列表 = 没缺口）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    db = S()
    ver = _seed_plan(db, stock={CAT_A: 20})
    sc2._ensure_schedules(db, ver.id)
    db.commit()
    out = sc2.api_adopt_schedule_ai_all(sc2.Payload(payload={}), db)
    assert out["ok"] is True
    assert "gaps" in out and isinstance(out["gaps"], list)
    assert "filled_hours" in out
    # 补过的小时必须真的被某个分类的窗口覆盖
    if out["filled_hours"]:
        assert out["filled_by"], "补位必须说明是谁补的"
    db.close()
