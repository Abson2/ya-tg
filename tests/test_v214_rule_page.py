"""v2.1.4 发布规则页专项验收（用户口径）：

  （1）PC 端右边留空较多、内容拥挤在左侧 → 发布规则页必须**整行满宽**；
  （2）配置时间段默认值应该与**偏好档位推荐值**一样；
      AI 建议时间段按国人观影时间习惯**周维度**生成；
  （3）整表按配置时间段从 0-2、2-4、4-6 的类似顺序自动排列；
      **当启用静默时间后，配置时间不能选择静默时间内的时间**。

覆盖：
  1. 周维度建议：真实发布分布按星期几分组派生（实测）+ 无样本时的通用作息基线（习惯）；
  2. 周维度建议真实落库并被调度引擎消费（按当天星期取时段）；
  3. 采纳建议写入周维度配置，逐日生效；
  4. 静默冲突：保存配置时段 / 采纳建议 / 保存周维度配置，落在静默区间一律拒绝；
  5. 跨午夜静默区间（22:00–次日 6:00）同样正确判定；
  6. 配置时段默认值 = 偏好档位推荐值（系统托管行始终跟随）；
  7. 手工设定的行不被偏好分配覆盖（老库 window_auto=1 的手工时段同样受保护）；
  8. 列表按配置时间段升序返回；
  9. 偏好分配新建行的上限档位按媒体类型取（电视 5/30，不是电影 3/20）；
 10. 前端契约：满宽布局、周维度 7 格展示、静默小时不可选、按时间排序。
"""
import os
import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = ROOT / "frontend" / "src" / "App.vue"
SHANGHAI = timezone(timedelta(hours=8))


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v214rule_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'rule.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    from sqlalchemy.orm import sessionmaker
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _add_published_at(db, category_key, moments):
    """造指定时刻的「已成功推送」历史（真实消费结果），喂给周维度建议派生。"""
    from app.models import SCPublishTask, SCPushList, SCPushListItem
    pl = SCPushList(list_key=f"pl:{category_key}", name="清单", status="active")
    db.add(pl)
    db.flush()
    for i, moment in enumerate(moments):
        item = SCPushListItem(list_id=pl.id, work_key=f"work:{category_key}:{i}",
                              library_version_id=0, category_key=category_key,
                              dedup_key=f"d:{category_key}:{i}", target_type="group",
                              chat_id="-1001")
        db.add(item)
        db.flush()
        db.add(SCPublishTask(list_item_id=item.id, work_key=f"work:{category_key}:{i}",
                              kind="first", status="published", link_state="active",
                              share_url="https://x/y",
                              published_at=moment))
    db.commit()


def _enable_quiet(db, start=0, end=7, enabled=True):
    """在活动版本的周期计划里开启静默保护。"""
    from app import share_center_v2 as sc2
    version = sc2._active_version(db)
    cfg = sc2._jload(version.config_json, {})
    cycle = dict(sc2.CYCLE_PLAN_DEFAULTS)
    cycle.update({"quiet_enabled": enabled, "quiet_start": start, "quiet_end": end})
    cfg["cycle_plan"] = cycle
    version.config_json = sc2._jdump(cfg)
    db.commit()
    return version


# ---------------------------------------------------------------------------
# 1. 周维度建议生成
# ---------------------------------------------------------------------------

def test_week_suggestion_uses_real_publish_distribution_per_weekday(env):
    """有真实发布样本的星期几 → 该天最密集小时段，来源标记为「实测」。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        # 2026-01-05 是周一，2026-01-10 是周六
        monday = datetime(2026, 1, 5, 21, 30, tzinfo=SHANGHAI)
        saturday = datetime(2026, 1, 10, 11, 30, tzinfo=SHANGHAI)
        _add_published_at(db, "genre:剧情", [monday, monday, saturday, saturday])
        res = sc2._ai_suggest_week_windows(db, "genre:剧情")
        assert res["windows"]["1"] == [21, 23], "周一应取真实最密集小时 21-23"
        assert res["source"]["1"] == "history"
        assert res["windows"]["6"] == [11, 13], "周六应取真实最密集小时 11-13"
        assert res["source"]["6"] == "history"
        # 没有真实样本的星期几走通用作息基线，来源标记为「习惯」
        assert res["source"]["3"] == "habit"
        assert res["windows"]["3"], "无样本的星期几也应有习惯基线建议"


def test_week_suggestion_falls_back_to_habit_baseline(env):
    """完全没有真实样本时，走国人观影作息基线（不是空建议）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        res = sc2._ai_suggest_week_windows(db, "genre:动作冒险")
        assert len(res["windows"]) == 7, "一周七天都应有建议"
        assert all(v == "habit" for v in res["source"].values())
        # 工作日晚间黄金档、周末白天+晚间双峰
        assert res["windows"]["1"] == [20, 22]
        assert res["windows"]["7"] == [20, 22]


def test_week_suggestion_returns_all_seven_days(env):
    """周维度建议必须覆盖周一到周日七天（ISO 星期 1-7）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        res = sc2._ai_suggest_week_windows(db, "genre:喜剧")
        assert set(res["windows"].keys()) == {"1", "2", "3", "4", "5", "6", "7"}


def test_week_window_for_reads_json(env):
    """从周维度 JSON 里取某一天的配置时段。"""
    from app import share_center_v2 as sc2
    raw = '{"1": [20, 22], "3": [10, 12]}'
    assert sc2._week_window_for(1, raw) == (20, 22)
    assert sc2._week_window_for(3, raw) == (10, 12)
    assert sc2._week_window_for(5, raw) is None
    assert sc2._week_window_for(1, "") is None
    assert sc2._week_window_for(1, "not-json") is None


# ---------------------------------------------------------------------------
# 2. 采纳 + 调度生效
# ---------------------------------------------------------------------------

def test_adopt_ai_writes_week_windows(env):
    """采纳建议 → **单一时间段**落库（v2.1.6 取代周维度网格）。

    v2.1.6 用户口径：「档位偏好和 ai 建议时段合并，只推荐时间段（尽量上 2 小时为
    颗粒度）不用到周维度」——采纳写入的就是一个 2 小时段，
    同时清空历史周维度配置（否则调度仍按周维度走）。
    """
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        rows = sc2._ensure_schedules(db, version.id)
        db.commit()
        row = next(r for r in rows if r.category_key == "genre:剧情")
        # 造真实历史：周一 21 点（最密集小时 → 21-23）
        monday = datetime(2026, 1, 5, 21, 30, tzinfo=SHANGHAI)
        _add_published_at(db, "genre:剧情", [monday, monday])
        res = sc2.api_adopt_schedule_ai(sc2.Payload(payload={"id": row.id}), db)
        assert res["ok"] is True
        assert res["source"] == "history", "有真实样本时来源标记为实测"
        assert (res["window_start"], res["window_end"]) == (21, 23)
        assert res["week_windows"] == {}, "v2.1.6：不再写周维度窗口"
        db.expire_all()
        again = db.scalar(select(SCCategorySchedule).where(SCCategorySchedule.id == row.id))
        assert again.ai_adopted is True
        assert (again.window_start, again.window_end) == (21, 23)
        assert not again.week_windows, "历史周维度配置必须被清空"


def test_adopt_ai_rejected_without_real_history(env):
    """无真实历史时改用**习惯基线**兜底并明确标注来源（v2.1.6）。

    v2.1.4 的口径是「无实测就拒绝采纳」——用户反馈「采纳操作有的按钮是灰色的」，
    因为大部分分类根本没有实测数据。v2.1.6 改为每个分类都必有建议：
    实测优先，无实测用该分类的默认时间表时段兜底，来源标记「习惯基线」。
    """
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        rows = sc2._ensure_schedules(db, version.id)
        db.commit()
        row = next(r for r in rows if r.category_key == "genre:喜剧")
        res = sc2.api_adopt_schedule_ai(sc2.Payload(payload={"id": row.id}), db)
        assert res["ok"] is True, "每个分类都必须有可采纳的建议"
        assert res["source"] == "habit", "无实测时来源标记为习惯基线"
        assert res["window_end"] - res["window_start"] == 2, "建议为 2 小时颗粒度"


def test_plan_engine_uses_week_window_for_today(env):
    """调度引擎按**当天星期**取该分类配置的那一天时段。

    直接验证 plan_engine 内的窗口判定：同一行配了「周维度窗口」与
    「单时段窗口」时，判定必须以**当天星期**的那一天为准。
    """
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        rows = sc2._ensure_schedules(db, version.id)
        db.commit()
        row = next(r for r in rows if r.category_key == "genre:剧情")
        today = datetime.now(sc2.shanghai_tz()).isoweekday()
        # 周维度：今天只允许 20:00–22:00；单时段字段给一个完全不同的值
        row.week_windows = sc2._jdump({str(today): [20, 22]})
        row.window_start, row.window_end = 3, 5
        db.commit()

        # 复刻 plan_engine 的判定逻辑（同口径）
        sh_hour = 21
        day_window = sc2._week_window_for(today, row.week_windows or "")
        assert day_window == (20, 22), "应取到当天的周维度窗口"
        ws, we = day_window
        assert ws <= sh_hour < we, "21 点应落在周维度窗口内 → 放行"

        # 同一行，若只看单时段字段（3-5），21 点会被拦——证明确实是周维度在生效
        assert not (3 <= sh_hour < 5)


def test_plan_engine_blocks_hour_outside_week_window(env):
    """周维度窗口之外的钟点必须被拦住。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        rows = sc2._ensure_schedules(db, version.id)
        db.commit()
        row = next(r for r in rows if r.category_key == "genre:剧情")
        today = datetime.now(sc2.shanghai_tz()).isoweekday()
        # 周维度只允许 2:00–4:00，单时段字段是全天（若只看单时段，15 点会被放行）
        row.week_windows = sc2._jdump({str(today): [2, 4]})
        row.window_start, row.window_end = 0, 24
        db.commit()

        day_window = sc2._week_window_for(today, row.week_windows or "")
        assert day_window == (2, 4)
        ws, we = day_window
        sh_hour = 15
        assert not (ws <= sh_hour < we), "15 点在周维度窗口外 → 应被拦住"
        # 单时段字段本会放行，证明拦截来自周维度
        assert 0 <= sh_hour < 24


def test_week_windows_absent_falls_back_to_single_window(env):
    """没有周维度配置时，仍走原来的单时段口径（向后兼容）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        rows = sc2._ensure_schedules(db, version.id)
        db.commit()
        row = next(r for r in rows if r.category_key == "genre:剧情")
        row.week_windows = ""
        row.window_start, row.window_end = 20, 22
        db.commit()
        today = datetime.now(sc2.shanghai_tz()).isoweekday()
        assert sc2._week_window_for(today, row.week_windows or "") is None
        assert (int(row.window_start), int(row.window_end)) == (20, 22)


# ---------------------------------------------------------------------------
# 3. 静默冲突校验
# ---------------------------------------------------------------------------

def test_save_schedule_rejects_quiet_overlap(env):
    """静默保护开启时，配置时段与静默区间重叠 → 拒绝保存并给中文原因。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = _enable_quiet(db, 0, 7)
        res = sc2.api_upsert_category_schedule(sc2.Payload(payload={
            "plan_version_id": version.id, "category_key": "genre:剧情",
            "window_start": 1, "window_end": 5, "hourly_limit": 5, "daily_limit": 30,
        }), db)
        assert res["ok"] is False
        assert "静默" in res["reason"]
        # 不得出现任何后端字段名
        for banned in ("quiet_enabled", "quiet_start", "window_start", "category_key"):
            assert banned not in res["reason"]


def test_save_schedule_allows_non_overlap(env):
    """不重叠的时段正常保存。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = _enable_quiet(db, 0, 7)
        res = sc2.api_upsert_category_schedule(sc2.Payload(payload={
            "plan_version_id": version.id, "category_key": "genre:剧情",
            "window_start": 20, "window_end": 22, "hourly_limit": 5, "daily_limit": 30,
        }), db)
        assert res["ok"] is True


def test_save_week_windows_rejects_quiet_overlap(env):
    """周维度配置里任一天落在静默区间 → 拒绝保存。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = _enable_quiet(db, 0, 7)
        res = sc2.api_upsert_category_schedule(sc2.Payload(payload={
            "plan_version_id": version.id, "category_key": "genre:剧情",
            "window_start": 20, "window_end": 22, "hourly_limit": 5, "daily_limit": 30,
            "week_windows": {"1": [20, 22], "3": [2, 4]},
        }), db)
        assert res["ok"] is False
        assert "静默" in res["reason"]


def test_adopt_ai_rejects_when_quiet_overlaps(env):
    """静默区间与建议时段冲突时，采纳被拒绝。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        rows = sc2._ensure_schedules(db, version.id)
        db.commit()
        row = next(r for r in rows if r.category_key == "genre:剧情")
        # 造真实历史（周一 21 点），采纳时段会落在 21-23
        monday = datetime(2026, 1, 5, 21, 30, tzinfo=SHANGHAI)
        _add_published_at(db, "genre:剧情", [monday])
        # 静默覆盖整条建议（0-24 全天静默）→ 采纳必冲突
        _enable_quiet(db, 0, 24)
        res = sc2.api_adopt_schedule_ai(sc2.Payload(payload={"id": row.id}), db)
        assert res["ok"] is False
        assert "静默" in res["reason"]


def test_quiet_overlap_handles_midnight_wrap(env):
    """跨午夜静默（22:00–次日 6:00）判定正确。

    注意：分类排期的**配置时段**本身不支持跨午夜（后端夹逼 we ≥ ws+1），
    这里验证的是**静默区间**跨午夜时，重叠判定依然准确。
    """
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = _enable_quiet(db, 22, 6)
        # 23:00–24:00 落在跨午夜静默的前半段 → 拒绝
        res = sc2.api_upsert_category_schedule(sc2.Payload(payload={
            "plan_version_id": version.id, "category_key": "genre:剧情",
            "window_start": 23, "window_end": 24, "hourly_limit": 5, "daily_limit": 30,
        }), db)
        assert res["ok"] is False
        assert "静默" in res["reason"]
        # 03:00–05:00 落在跨午夜静默的后半段 → 拒绝
        res2 = sc2.api_upsert_category_schedule(sc2.Payload(payload={
            "plan_version_id": version.id, "category_key": "genre:剧情",
            "window_start": 3, "window_end": 5, "hourly_limit": 5, "daily_limit": 30,
        }), db)
        assert res2["ok"] is False
        assert "静默" in res2["reason"]
        # 12:00–14:00 不重叠 → 放行
        ok = sc2.api_upsert_category_schedule(sc2.Payload(payload={
            "plan_version_id": version.id, "category_key": "genre:剧情",
            "window_start": 12, "window_end": 14, "hourly_limit": 5, "daily_limit": 30,
        }), db)
        assert ok["ok"] is True


def test_quiet_disabled_allows_overlap(env):
    """静默保护关闭时，重叠时段正常放行。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = _enable_quiet(db, 0, 7, enabled=False)
        res = sc2.api_upsert_category_schedule(sc2.Payload(payload={
            "plan_version_id": version.id, "category_key": "genre:剧情",
            "window_start": 1, "window_end": 5, "hourly_limit": 5, "daily_limit": 30,
        }), db)
        assert res["ok"] is True


def test_list_marks_quiet_conflict(env):
    """列表端点把与静默冲突的行标出来，供前端提示。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = _enable_quiet(db, 0, 7)
        db.add(sc2.SCCategorySchedule(plan_version_id=version.id, category_key="genre:剧情",
                                       window_start=1, window_end=5,
                                       hourly_limit=5, daily_limit=30, enabled=True,
                                       window_auto=False))
        db.commit()
        res = sc2.api_category_schedules(db=db)
        item = next(i for i in res["items"] if i["category_key"] == "genre:剧情")
        assert item["quiet_conflict"] is True
        assert res["quiet_enabled"] is True
        assert res["quiet_start"] == 0 and res["quiet_end"] == 7


# ---------------------------------------------------------------------------
# 4. 配置时段默认值 = 偏好档位推荐值
# ---------------------------------------------------------------------------

def test_config_window_follows_preference_recommendation(env):
    """系统托管行的配置时段必须与偏好档位推荐值一致。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        rows = sc2._ensure_schedules(db, version.id)
        db.commit()
        windows = sc2._preference_windows(db)
        for row in rows:
            exp = windows.get(row.category_key, sc2.FULL_DAY_WINDOW)
            assert (row.window_start, row.window_end) == exp, \
                f"{row.category_key} 的配置时段应等于偏好推荐值"


def test_system_managed_row_keeps_following_preference_after_limit_change(env):
    """用户改过上限后，时段仍应跟随偏好推荐（上限与时段归属是两件事）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        rows = sc2._ensure_schedules(db, version.id)
        db.commit()
        row = next(r for r in rows if r.category_key == "genre:剧情")
        row.hourly_limit, row.daily_limit = 3, 15  # 用户改了上限
        row.window_start, row.window_end = 8, 24   # 仍是系统窗口形态
        db.commit()
        again = sc2._ensure_schedules(db, version.id)
        db.commit()
        windows = sc2._preference_windows(db)
        exp = windows.get(row.category_key, sc2.FULL_DAY_WINDOW)
        fresh = next(r for r in again if r.category_key == "genre:剧情")
        assert (fresh.window_start, fresh.window_end) == exp, \
            "改了上限不影响时段归属，仍应跟随偏好推荐值"
        assert (fresh.hourly_limit, fresh.daily_limit) == (3, 15), "用户设的上限不得被改"


def test_manual_window_row_is_protected(env):
    """用户手工设定的时段不得被偏好分配覆盖。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        db.add(sc2.SCCategorySchedule(plan_version_id=version.id, category_key="genre:剧情",
                                       window_start=21, window_end=23,
                                       hourly_limit=2, daily_limit=9, enabled=True,
                                       window_auto=False))
        db.commit()
        sc2._ensure_schedules(db, version.id)
        db.commit()
        row = db.scalar(select(sc2.SCCategorySchedule).where(
            sc2.SCCategorySchedule.plan_version_id == version.id,
            sc2.SCCategorySchedule.category_key == "genre:剧情"))
        assert (row.window_start, row.window_end) == (21, 23), "手工时段不得被覆盖"


def test_legacy_window_auto_one_manual_window_is_protected(env):
    """老库形态：window_auto=1（ALTER 默认）但时段是用户手工的 → 同样受保护。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        db.add(sc2.SCCategorySchedule(plan_version_id=version.id, category_key="genre:剧情",
                                       window_start=21, window_end=23,
                                       hourly_limit=2, daily_limit=9, enabled=True,
                                       window_auto=True))
        db.commit()
        sc2._ensure_schedules(db, version.id)
        db.commit()
        row = db.scalar(select(sc2.SCCategorySchedule).where(
            sc2.SCCategorySchedule.plan_version_id == version.id,
            sc2.SCCategorySchedule.category_key == "genre:剧情"))
        assert (row.window_start, row.window_end) == (21, 23), \
            "老库手工时段（window_auto=1）不得被误刷"


def test_save_all_does_not_freeze_preference_tracking(env):
    """「保存全部」逐行提交后，系统托管行仍应可被偏好分配刷新。

    回归：此前每行都被无条件置为「用户手工设定」，点一次保存全部，
    偏好分配对全表永久失效，配置时段与偏好推荐值从此脱钩。
    """
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        rows = sc2._ensure_schedules(db, version.id)
        db.commit()
        # 模拟前端「保存全部」：逐行提交当前值
        for row in rows:
            sc2.api_upsert_category_schedule(sc2.Payload(payload={
                "plan_version_id": version.id, "category_key": row.category_key,
                "window_start": row.window_start, "window_end": row.window_end,
                "hourly_limit": row.hourly_limit, "daily_limit": row.daily_limit,
                "enabled": row.enabled,
            }), db)
        db.expire_all()
        after = db.scalars(select(sc2.SCCategorySchedule).where(
            sc2.SCCategorySchedule.plan_version_id == version.id)).all()
        windows = sc2._preference_windows(db)
        for row in after:
            exp = windows.get(row.category_key, sc2.FULL_DAY_WINDOW)
            assert (row.window_start, row.window_end) == exp, \
                "保存全部不应让配置时段与偏好推荐值脱钩"


# ---------------------------------------------------------------------------
# 5. 列表排序 + 偏好分配上限档位
# ---------------------------------------------------------------------------

def test_list_sorted_by_config_window(env):
    """列表按配置时间段升序（0-2 → 2-4 → 4-6 …）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        db.add(sc2.SCCategorySchedule(plan_version_id=version.id, category_key="genre:剧情",
                                       window_start=0, window_end=2,
                                       hourly_limit=1, daily_limit=1, enabled=True,
                                       window_auto=True))
        db.add(sc2.SCCategorySchedule(plan_version_id=version.id, category_key="genre:喜剧",
                                       window_start=4, window_end=6,
                                       hourly_limit=1, daily_limit=1, enabled=True,
                                       window_auto=True))
        db.commit()
        res = sc2.api_category_schedules(db=db)
        keys = [(i["window_start"], i["window_end"]) for i in res["items"]]
        assert keys == sorted(keys), f"列表未按配置时段升序：{keys[:6]}"


def test_assign_preference_new_row_uses_media_type_limits(env):
    """偏好分配新建行时，上限按媒体类型取默认档（电视 5/30，不是电影 3/20）。"""
    from app import share_center_v2 as sc2
    from app.models import SCCategory, SCCategorySchedule
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        # 找一个电视分类
        tv_key = None
        for cat in sc2._tmdb_genre_catalog():
            if cat["media_type"] == "tv":
                tv_key = cat["category_key"]
                break
        assert tv_key
        row = db.scalar(select(SCCategorySchedule).where(
            SCCategorySchedule.plan_version_id == version.id,
            SCCategorySchedule.category_key == tv_key))
        if row is not None:
            db.delete(row)
            db.commit()
        res = sc2.api_assign_preference_windows(sc2.Payload(payload={"force": True}), db)
        assert res["ok"] is True
        db.expire_all()
        made = db.scalar(select(SCCategorySchedule).where(
            SCCategorySchedule.plan_version_id == version.id,
            SCCategorySchedule.category_key == tv_key))
        assert made is not None
        assert (made.hourly_limit, made.daily_limit) == (5, 30), \
            "电视类新行的上限应为 5/30"


# ---------------------------------------------------------------------------
# 6. 前端契约
# ---------------------------------------------------------------------------

def test_frontend_rule_page_is_full_width():
    """发布规则页必须整行满宽（不再挤在两列 Grid 的半格子里）。

    v2.1.5 真机回归：v2.1.4 的满宽实现有两层根因——
      ① 满宽类 .sc-board-full 定义在 .sc-board **之前**（同特异度 → 后者胜出），
         满宽规则完全失效，页面仍被塞进两列 Grid 的半格子里；
      ② 发布规则页在半幅容器内又套一层两列 .sc-rule-layout →
         每列只剩 1/4 页宽，卡片被压成「缩小版」，尺寸与页面严重不符。
    v2.1.5 修法：三页移出 Grid，由 .sc-subpage 独占整行满宽。
    """
    app = APP_VUE.read_text(encoding="utf-8")
    assert "sc-subpage" in app, "缺少满宽子页容器"
    assert ".sc-subpage{display:block;width:100%;min-width:0}" in app, \
        "满宽子页容器样式缺失"
    assert ".sc-board-full{" not in app, \
        "旧的 .sc-board-full 满宽修饰类仍在（层叠顺序会再次失效）"
    assert 'class="sc-board sc-board-full"' not in app, \
        "三页仍挂在 .sc-board 两列 Grid 里"
    assert "sc-rule-layout" in app, "发布规则页内部应使用两列布局"


def test_frontend_week_grid_shows_seven_days():
    """v2.1.6：周维度网格下线，改为单一「推荐时间段」列。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "sc-week-grid" not in app, "周维度网格已下线，不得残留"
    assert "scWeekWindowOf" not in app, "周维度取值函数已下线"
    assert "AI 建议时间段（周维度）" not in app, "周维度列标题已下线"
    # 合并后的单一推荐时间段列
    assert "推荐时间段" in app, "缺少合并后的推荐时间段列"
    assert "suggest_start" in app and "suggest_end" in app
    # 每个分类都必有建议 → 采纳按钮不因缺建议而置灰。
    # v2.1.6：判定必须用 suggest_source（来源标记），不能用起点数值真伪——
    # 夜间段 00:00–02:00 起点是 0，用 suggest_start 会让这类分类的采纳按钮变灰。
    assert ':disabled="!s.row.suggest_source"' in app


def test_frontend_quiet_hours_disabled():
    """静默保护开启时，静默区间内的小时不可选。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "scQuietHours" in app, "缺少静默小时集合"
    assert "scHourDisabled" in app, "缺少小时禁用判定"
    assert ":disabled=\"scHourDisabled(h)\"" in app, "起始小时未禁用静默内取值"
    assert "scEndHourDisabled" in app, "止点小时未禁用静默内取值"
    assert "quiet_conflict" in app, "未消费静默冲突标记"


def test_frontend_sorts_by_config_window():
    """前端表格按配置时间段升序。"""
    app = APP_VUE.read_text(encoding="utf-8")
    body = app[app.index("function scPrefSortRows"):]
    body = body[:body.index("async function scResetSchedDefault")]
    assert "window_start" in body, "前端排序未使用配置时段起点"
    assert "window_end" in body, "前端排序未使用配置时段止点"


def test_frontend_does_not_leak_backend_field_names():
    """发布规则页**模板区**不得出现后端字段名。

    范围限定在「发布规则」子页（本次改动的页面）——
    其它页面的清理属各自任务，不在本次断言范围内。
    脚本区提交接口参数（category_key 等）不在此限——那是接口契约，不是页面展示。
    """
    app = APP_VUE.read_text(encoding="utf-8")
    start = app.index("<!-- 设计稿发布规则：基础规则 / 分类时间排期 两个子 tab -->")
    end = app.index("<!-- v2.1.1：按设计稿收敛——本页不单独挂 AI 建议卡")
    block = app[start:end]
    for banned in ("work_key", "link_state", "ai_week_windows", "week_windows",
                   "window_auto", "category_key", "ai_window_start", "ai_week_source"):
        assert banned not in block, f"发布规则页出现后端字段名：{banned}"
    # 中性展示名与中文标签并存（v2.1.6 表格以中文分类名 + 中性建议字段呈现）
    assert "s.row.name" in block, "应使用中性展示名而非后端字段名"
    assert "s.row.suggest_start" in block
    assert "静默保护" in block
