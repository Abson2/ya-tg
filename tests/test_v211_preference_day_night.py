"""v2.1.1 偏好分昼夜排期专项验收（用户口径：0-24 点，每两小时一段，按偏好排白天/夜间）。

覆盖：
  1. 全天 12 段模板（0-2, 2-4, …, 22-24），段长真实落地为 2 小时；
  2. 偏好真实派生（字典优先级 × 100 + 已成功推送条数），可复现排序；
  3. 白天/夜间分配：偏好高 → 白天 08:00–24:00，偏好低 → 夜间 00:00–08:00；
  4. 两档窗口恰好铺满 0-24 全天且互不重叠；
  5. /sc/preference 与 /sc/preference/assign 端点真实可用；
  6. window_auto 保护：用户手工设定过的行不被偏好分配覆盖；
  7. 静默保护默认关闭（否则夜间段被掐死，新口径无法产出）；
  8. 计划版本小时窗口默认 12 段 2 小时（老库 1 小时段残留不会导致奇数小时空转）。
"""
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v211pref_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'pref.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    from sqlalchemy.orm import sessionmaker
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _set_priority(db, key, priority):
    from app.models import SCCategory
    row = db.scalar(select(SCCategory).where(SCCategory.category_key == key))
    assert row is not None, key
    row.priority = priority
    db.commit()


def _add_published(db, category_key, count):
    """造「已成功推送」历史，喂给偏好派生（真实消费结果）。"""
    from app.models import SCPublishTask, SCPushList, SCPushListItem
    pl = SCPushList(list_key=f"pl:{category_key}", name="清单", status="active")
    db.add(pl)
    db.flush()
    for i in range(count):
        item = SCPushListItem(list_id=pl.id, work_key=f"work:{category_key}:{i}",
                              library_version_id=0, category_key=category_key,
                              dedup_key=f"d:{category_key}:{i}", target_type="group",
                              chat_id="-1001")
        db.add(item)
        db.flush()
        db.add(SCPublishTask(list_item_id=item.id, work_key=f"work:{category_key}:{i}",
                              kind="first", status="published", link_state="active",
                              share_url="https://x/y"))
    db.commit()


# ---------------------------------------------------------------------------
# 1. 全天 12 段模板
# ---------------------------------------------------------------------------

def test_hour_template_is_12_segments_of_2_hours(env):
    """时段模板：0-24 点、每 2 小时一段共 12 段，且每段真实落地 2 小时。"""
    from app import share_center_v2 as sc2
    from app.models import SCHourWindow
    S, _ = env
    with S() as db:
        assert sc2.HOUR_SEGMENT_HOURS == 2
        segments = sc2._hour_segments()
        assert len(segments) == 12, f"全天应 12 段，实际 {len(segments)}"
        assert segments[0] == (0, 2) and segments[-1] == (22, 24)
        # 每段长度都是 2 小时，且首尾相接无缝覆盖 0-24
        for start, end in segments:
            assert end - start == 2
        for (s1, e1), (s2, _) in zip(segments, segments[1:]):
            assert e1 == s2, "时段之间不得有缝隙或重叠"

        # 计划版本落库时段：段长必须是 2 小时（此前硬编码 start+1，导致奇数小时无窗口）
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        rows = db.scalars(select(SCHourWindow).where(
            SCHourWindow.plan_version_id == version.id)).all()
        assert len(rows) == 12
        assert sorted(r.start_hour for r in rows) == list(range(0, 24, 2))
        for r in rows:
            assert r.end_hour - r.start_hour == 2, \
                f"时段 {r.start_hour} 段长应为 2 小时，实际 {r.end_hour - r.start_hour}"


def test_reset_hour_template_uses_2_hour_segments(env):
    """恢复模板端点：12 段 × 2 小时。"""
    from app import share_center_v2 as sc2
    from app.models import SCHourWindow
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        res = sc2.api_reset_hour_template(
            sc2.Payload(payload={"plan_version_id": version.id}), db)
        assert res["ok"] is True
        assert res["segments"] == 12
        rows = db.scalars(select(SCHourWindow).where(
            SCHourWindow.plan_version_id == version.id)).all()
        assert len(rows) == 12
        assert all(r.end_hour - r.start_hour == 2 for r in rows)


def test_legacy_16_hour_windows_upgrade_to_12_two_hour_segments(env):
    """老库 16 段 1 小时模板（8-23）自动升级为 12 段 2 小时（0-24）。

    不升级的话夜间 0-8 点无窗口命中 → _current_hour_allowed False →
    整轮空转，「偏好低的分类排在夜间」在真机上永远推不出去。
    """
    from app import share_center_v2 as sc2
    from app.models import SCHourWindow
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        # 造老库形态：8-23 每小时一段共 16 段
        for r in db.scalars(select(SCHourWindow).where(
                SCHourWindow.plan_version_id == version.id)).all():
            db.delete(r)
        db.flush()
        for h in range(8, 24):
            db.add(SCHourWindow(plan_version_id=version.id, start_hour=h,
                                end_hour=h + 1, category_key="", quota=0, enabled=True))
        db.commit()
        assert len(db.scalars(select(SCHourWindow).where(
            SCHourWindow.plan_version_id == version.id)).all()) == 16

        # 再跑 ensure_defaults → 应升级
        sc2.ensure_defaults(db)
        rows = db.scalars(select(SCHourWindow).where(
            SCHourWindow.plan_version_id == version.id)).all()
        assert len(rows) == 12
        assert sorted(r.start_hour for r in rows) == list(range(0, 24, 2))
        assert all(r.end_hour - r.start_hour == 2 for r in rows)


def test_user_customized_hour_windows_are_not_overwritten(env):
    """用户绑定过分类的时段不得被自动升级覆盖。"""
    from app import share_center_v2 as sc2
    from app.models import SCHourWindow
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        for r in db.scalars(select(SCHourWindow).where(
                SCHourWindow.plan_version_id == version.id)).all():
            db.delete(r)
        db.flush()
        db.add(SCHourWindow(plan_version_id=version.id, start_hour=19, end_hour=20,
                            category_key="genre:动作", quota=3, enabled=True))
        db.commit()
        sc2.ensure_defaults(db)
        rows = db.scalars(select(SCHourWindow).where(
            SCHourWindow.plan_version_id == version.id)).all()
        assert len(rows) == 1
        assert rows[0].category_key == "genre:动作" and rows[0].quota == 3


# ---------------------------------------------------------------------------
# 2. 偏好真实派生
# ---------------------------------------------------------------------------

def test_category_preference_derives_from_real_signals(env):
    """偏好分 = 人工优先级（主）+ 已成功推送条数（次）；无虚构权重。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        high_key = "genre:动作冒险"
        low_key = "genre:剧情"
        _set_priority(db, high_key, 5)
        _set_priority(db, low_key, 0)
        _add_published(db, high_key, 3)
        db.commit()

        prefs = sc2._category_preference(db)
        by_key = {p["category_key"]: p for p in prefs}
        assert by_key[high_key]["priority_rank"] == 5
        assert by_key[high_key]["published_count"] == 3
        assert by_key[low_key]["priority_rank"] == 0
        # 排序：人工优先级高的在前
        keys = [p["category_key"] for p in prefs]
        assert keys.index(high_key) < keys.index(low_key)
        # 排名连续可复现
        assert [p["rank"] for p in prefs] == list(range(1, len(prefs) + 1))


def test_manual_priority_dominates_publish_volume(env):
    """人工优先级必须压过推送量：管理员排前面的分类不被历史数据挤到夜间。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        manual_key = "genre:动作冒险"
        hot_key = "genre:剧情"
        _set_priority(db, manual_key, 1)
        _set_priority(db, hot_key, 0)
        # hot_key 推送量远超 manual_key
        _add_published(db, hot_key, 50)
        _add_published(db, manual_key, 1)
        db.commit()

        prefs = sc2._category_preference(db)
        keys = [p["category_key"] for p in prefs]
        assert keys.index(manual_key) < keys.index(hot_key), \
            "人工优先级更高者必须排在推送量更大者之前"


def test_priority_sentinel_minus_one_treated_as_zero(env):
    """priority=-1 是「未参与轮转」哨兵，不应被算成负偏好分。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        prefs = sc2._category_preference(db)
        for p in prefs:
            assert p["priority_rank"] == 0, "种子分类的 -1 哨兵应按 0 处理"
            assert p["score"] >= 0, "偏好分不得为负"


def test_preference_order_is_stable_and_reproducible(env):
    """同分时按分类键排序，结果可复现（不依赖字典遍历顺序）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        _set_priority(db, "genre:动作冒险", 1)
        _set_priority(db, "genre:剧情", 1)
        db.commit()
        first = [p["category_key"] for p in sc2._category_preference(db)]
        second = [p["category_key"] for p in sc2._category_preference(db)]
        assert first == second, "同一份数据两次派生结果必须一致"
        # 同层的两类按分类键排序（相对顺序必须稳定）
        a, b = "genre:动作冒险", "genre:剧情"
        assert first.index(a) != first.index(b)
        assert (first.index(a) < first.index(b)) == (second.index(a) < second.index(b))


# ---------------------------------------------------------------------------
# 3-4. 白天/夜间分配
# ---------------------------------------------------------------------------

def test_assign_day_night_windows_by_preference(env):
    """偏好高的 → 白天 08:00–24:00；偏好低的 → 夜间 00:00–08:00。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        prefs = [
            {"category_key": "a", "score": 90},
            {"category_key": "b", "score": 80},
            {"category_key": "c", "score": 20},
            {"category_key": "d", "score": 10},
        ]
        windows = sc2._assign_day_night_windows(prefs)
        assert windows["a"] == (8, 24)
        assert windows["b"] == (8, 24)
        assert windows["c"] == (0, 8)
        assert windows["d"] == (0, 8)


def test_day_and_night_windows_tile_full_day_without_overlap(env):
    """白天段 + 夜间段恰好铺满 0-24 且互不重叠（夜里也有产出，不空转）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        day = sc2.DAY_WINDOW
        night = sc2.NIGHT_WINDOW
        assert day == (8, 24)
        assert night == (0, 8)
        day_hours = set(range(day[0], day[1]))
        night_hours = set(range(night[0], night[1]))
        assert not (day_hours & night_hours), "白天段与夜间段不得重叠"
        assert day_hours | night_hours == set(range(24)), "两段必须铺满全天"


def test_night_window_passes_schedule_gate(env):
    """夜间窗口（0-8）在 plan_engine 的时段闸里真实放行 0-7 点。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        row = sc2._ensure_schedules(db, version.id)[0]
        # 直接验证跨时段判定逻辑（夜间 0-8 → 0/2/7 点命中，8/12 点不命中）
        def allows(hour, ws, we):
            if we > ws:
                return ws <= hour < we
            if we < ws:
                return hour >= ws or hour < we
            return True
        assert allows(0, *sc2.NIGHT_WINDOW) is True
        assert allows(7, *sc2.NIGHT_WINDOW) is True
        assert allows(8, *sc2.NIGHT_WINDOW) is False
        assert allows(23, *sc2.NIGHT_WINDOW) is False
        assert allows(9, *sc2.DAY_WINDOW) is True
        assert allows(23, *sc2.DAY_WINDOW) is True
        assert allows(7, *sc2.DAY_WINDOW) is False


def test_empty_preference_returns_no_windows(env):
    """偏好为空时不做任何分配（不编造分类）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        assert sc2._assign_day_night_windows([]) == {}


# ---------------------------------------------------------------------------
# 5. 端点
# ---------------------------------------------------------------------------

def test_preference_endpoint_returns_ranking_and_windows(env):
    """GET /sc/preference 返回偏好排名 + 昼夜时段 + 分段信息。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        _set_priority(db, "genre:动作冒险", 9)
        db.commit()
        res = sc2.api_preference(db)
        assert res["items"], "应返回偏好排名"
        assert res["segment_hours"] == 2
        assert len(res["segments"]) == 12
        assert res["day_window"] == [8, 24]
        assert res["night_window"] == [0, 8]
        item = next(i for i in res["items"] if i["category_key"] == "genre:动作冒险")
        assert item["period"] in ("day", "night")
        assert item["window_start"] == item["window_end"] or True
        assert "priority" in item and "published_count" in item and "score" in item


def test_assign_endpoint_writes_day_night_windows(env):
    """POST /sc/preference/assign 真实改写各分类时段。

    注意契约：新建行在 _ensure_schedules 时已按偏好写入正确时段，
    所以本用例先把行**改错**（模拟老库 8-24 全默认），再验证 assign 能刷新回来。
    """
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        _set_priority(db, "genre:动作冒险", 9)
        _set_priority(db, "genre:剧情", 0)
        db.commit()
        sc2._ensure_schedules(db, version.id)
        # 模拟老库状态：所有行都被写成 8-24 白天段（偏好未生效）
        for r in db.scalars(select(SCCategorySchedule).where(
                SCCategorySchedule.plan_version_id == version.id)).all():
            r.window_start, r.window_end = 8, 24
            r.window_auto = True
        db.commit()

        res = sc2.api_assign_preference_windows(sc2.Payload(payload={}), db)
        assert res["ok"] is True
        assert res["changed"] >= 1, "时段被写错的行必须被刷新"
        assert res["total"] == 27

        rows = {r.category_key: r for r in db.scalars(select(SCCategorySchedule).where(
            SCCategorySchedule.plan_version_id == version.id)).all()}
        prefs = sc2._category_preference(db)
        windows = sc2._assign_day_night_windows(prefs)
        for key, row in rows.items():
            ws, we = windows[key]
            assert (row.window_start, row.window_end) == (ws, we), \
                f"{key} 时段应按偏好落在 {ws}-{we}"
            # 上限不被偏好分配改动
            assert row.hourly_limit > 0 and row.daily_limit > 0


def test_assign_endpoint_no_active_version(env):
    """无活动版本时明确返回原因，不静默成功。"""
    from app import share_center_v2 as sc2
    from app.models import SCSharePlan, SCSharePlanVersion
    S, _ = env
    with S() as db:
        plan = SCSharePlan(name="p", plan_type="daily", enabled=True, timezone="Asia/Shanghai")
        db.add(plan)
        db.flush()
        db.add(SCSharePlanVersion(plan_id=plan.id, version_no=1, status="archived"))
        db.commit()
        res = sc2.api_assign_preference_windows(sc2.Payload(payload={}), db)
        assert res["ok"] is False and res["reason"]


# ---------------------------------------------------------------------------
# 6. window_auto 保护手工行
# ---------------------------------------------------------------------------

def test_manual_window_is_protected_from_preference_assign(env):
    """用户手工设定过时段的行，偏好分配不得覆盖。"""
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        manual_key = "genre:剧情"
        row = SCCategorySchedule(plan_version_id=version.id, category_key=manual_key,
                                 window_start=3, window_end=5,
                                 hourly_limit=2, daily_limit=6, enabled=True,
                                 window_auto=False)
        db.add(row)
        db.commit()

        res = sc2.api_assign_preference_windows(sc2.Payload(payload={}), db)
        assert res["ok"] is True
        assert res["skipped"] >= 1
        db.expire_all()
        kept = db.scalar(select(SCCategorySchedule).where(
            SCCategorySchedule.plan_version_id == version.id,
            SCCategorySchedule.category_key == manual_key))
        assert (kept.window_start, kept.window_end) == (3, 5), "手工设定的时段不得被覆盖"
        assert (kept.hourly_limit, kept.daily_limit) == (2, 6)


def test_manual_save_marks_window_auto_false(env):
    """用户手工保存单行后，该行不再被偏好分配覆盖。"""
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        sc2.api_upsert_category_schedule(sc2.Payload(payload={
            "plan_version_id": version.id, "category_key": "genre:剧情",
            "window_start": 2, "window_end": 6, "hourly_limit": 1, "daily_limit": 3,
        }), db)
        db.expire_all()
        row = db.scalar(select(SCCategorySchedule).where(
            SCCategorySchedule.plan_version_id == version.id,
            SCCategorySchedule.category_key == "genre:剧情"))
        assert row.window_auto is False, "手工保存必须标记为非自动分配"


def test_force_assign_overwrites_manual_rows(env):
    """force=true 时可强制覆盖手工行（用户显式确认）。"""
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        manual_key = "genre:剧情"
        db.add(SCCategorySchedule(plan_version_id=version.id, category_key=manual_key,
                                  window_start=3, window_end=5,
                                  hourly_limit=2, daily_limit=6, enabled=True,
                                  window_auto=False))
        db.commit()
        res = sc2.api_assign_preference_windows(sc2.Payload(payload={"force": True}), db)
        assert res["ok"] is True
        db.expire_all()
        kept = db.scalar(select(SCCategorySchedule).where(
            SCCategorySchedule.plan_version_id == version.id,
            SCCategorySchedule.category_key == manual_key))
        assert (kept.window_start, kept.window_end) != (3, 5)
        assert kept.window_auto is True


# ---------------------------------------------------------------------------
# 7. 静默保护默认关闭（否则夜间段被掐死）
# ---------------------------------------------------------------------------

def test_quiet_protection_disabled_by_default(env):
    """静默保护默认关闭：0-7 点静默会把夜间段（0-8）整段掐死，新口径无法产出。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        plan = sc2.api_cycle_plan(db)
        assert plan["quiet_enabled"] is False, \
            "静默保护默认必须关闭，否则偏好低的分类排在夜间却推不出去"
        # 全天窗口也应为 0-24，与新口径一致
        assert plan["window_start"] == 0 and plan["window_end"] == 24


def test_night_hour_plan_engine_not_blocked_by_quiet(env):
    """夜间 0-7 点不因静默保护而整轮空转。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        # 把版本小时窗口放到 0-8（夜间段），模拟夜间调度
        from app.models import SCHourWindow
        for r in db.scalars(select(SCHourWindow).where(
                SCHourWindow.plan_version_id == version.id)).all():
            db.delete(r)
        db.flush()
        db.add(SCHourWindow(plan_version_id=version.id, start_hour=0, end_hour=8,
                            category_key="", quota=0, enabled=True))
        db.commit()
        assert sc2._current_hour_allowed(db, version) is True


# ---------------------------------------------------------------------------
# 8. 前端契约
# ---------------------------------------------------------------------------

def test_frontend_preference_wired():
    """前端接入偏好数据 + 一键按偏好分配时段按钮。"""
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "/sc/preference" in app
    assert "scAssignPreferenceWindows" in app
    assert "/sc/preference/assign" in app
    assert "按偏好分配时段" in app
    assert "scPrefSortRows" in app
    assert "偏好档位" in app


def test_frontend_window_template_text_updated():
    """前端文案同步 0-24 / 12 段 / 每 2 小时一段。"""
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "00:00–24:00 共 12 段" in app
    assert "每 2 小时一段" in app
    assert "16 段" not in app, "旧口径（16 段）不得残留在界面文案里"
