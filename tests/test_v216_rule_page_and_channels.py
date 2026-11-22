"""v2.1.6 发布规则 + 推送渠道专项验收（用户口径逐条固化）。

用户原始口径：
  （1）冲突处理矛盾 → 改成 a、新上映优先；b、新入库优先；c、存量排期优先，
      且**推送逻辑一并改**（必须真实生效，不能是死配置）。
  （2）发布规则页：仅保留基础规则 tab 下的卡片，分类时间排期 tab 去掉重复的
      周期计划管理卡片；「上新即时推送」与「周期计划管理」两张卡片竖向对齐排列。
  （3）分类时间排期：
      a、去掉周期维度列；
      b、配置时段按分类观影习惯排默认时间表（热门分类排白天、冷门分类排夜间），
         默认两个小时为一个时间段，不要全都是 0-8，且避开静默时段；
      c、档位偏好和 AI 建议时段合并，只推荐时间段（2 小时颗粒度），不用周维度；
      d、采纳操作有的按钮是灰色的 → 每个分类都要匹配对应的 AI 建议；
      e、一键恢复默认 / 批量采纳 AI 建议 / 保存需要能对分类进行勾选，
         若都未勾选或者全勾选则表示全部分类选中；
         去掉无意义的「按偏好分配时间段」功能；去掉每小时上限、日上限；
         去掉每个分类后的复制按钮。
  （4）推送渠道应该为：群:对应的群名称（群id或用户名），
      频道:对应的频道名称（频道id或用户名）。

覆盖：
  1. 冲突处理三档定义、别名归一化、以及在 dispatch_ready_tasks 中的**真实排序**；
  2. 默认时间表：白天段 / 夜间段 2 小时粒度、热门白天冷门夜间、静默避让；
  3. 单一时间段建议：每分类必有建议（实测优先 / 习惯基线兜底）；
  4. 批量操作按勾选生效（category_keys 过滤），未勾选 = 全部；
  5. 分类级上限退出调度，统一由周期计划 category_hour_max / category_day_max 约束；
  6. 推送渠道：类型以 Channel.kind 为准、名称取 DiscoveredChat.title、
     标识取 username 优先，展示格式「群:名称（id或用户名）」；
  7. 前端契约：勾选列 / 合并后的推荐时间段列 / 去掉周期维度与上限列 /
     复制与按偏好分配按钮下线 / 渠道列新格式。
"""
import json
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
APP_VUE = ROOT / "frontend" / "src" / "App.vue"


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v216rule_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'rule.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    from sqlalchemy.orm import sessionmaker
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _read_app() -> str:
    return APP_VUE.read_text(encoding="utf-8")


def _rule_block() -> str:
    """发布规则子页模板区（用于「不出现后端字段名」这类断言）。"""
    app = _read_app()
    start = app.index("<!-- 设计稿发布规则：基础规则 / 分类时间排期 两个子 tab -->")
    end = app.index("<!-- v2.1.1：按设计稿收敛——本页不单独挂 AI 建议卡")
    return app[start:end]


# ---------------------------------------------------------------------------
# 1. 冲突处理三档（真实生效）
# ---------------------------------------------------------------------------

def test_conflict_policies_three_tiers():
    """冲突处理只有三档，语义互斥。"""
    from app import share_center_v2 as sc2
    assert sc2.CONFLICT_POLICIES == ("prefer_release", "prefer_new", "prefer_backlog")
    labels = set(sc2.CONFLICT_POLICY_LABELS.values())
    assert labels == {"新上映优先", "新入库优先", "存量排期优先"}
    # 旧值不得出现在三档里（此前 pause_normal / prefer_new 语义重复）
    assert "pause_normal" not in sc2.CONFLICT_POLICIES


def test_conflict_policy_alias_normalization():
    """旧值归一化：pause_normal → 新上映优先；非法值回落默认档。"""
    from app import share_center_v2 as sc2
    assert sc2._normalize_conflict_policy("pause_normal") == "prefer_release"
    assert sc2._normalize_conflict_policy("prefer_new") == "prefer_new"
    assert sc2._normalize_conflict_policy("prefer_new_entry") == "prefer_new"
    assert sc2._normalize_conflict_policy("backlog_first") == "prefer_backlog"
    assert sc2._normalize_conflict_policy("") == "prefer_release"
    assert sc2._normalize_conflict_policy("nonsense") == "prefer_release"


def test_conflict_policy_is_not_dead_config():
    """冲突策略必须被调度引擎真实消费（不是只存不读的死配置）。"""
    from app import share_center_v2 as sc2
    import inspect
    src = inspect.getsource(sc2.dispatch_ready_tasks)
    assert "conflict_policy" in src, "dispatch_ready_tasks 必须消费冲突策略"
    assert "_conflict_sort_key" in src, "冲突策略必须落到真实排序"


def _seed_conflict_task(db, work_key, release_date, scanned_at, scheduled_at=None):
    """造一条带代表资源的发布任务（冲突排序的真实输入形态）。"""
    from app.models import (Resource, SCLibraryVersion, SCPublishTask,
                            SCPushList, SCPushListItem)
    res = Resource(file_id=f"f:{work_key}", name=f"{work_key}.mkv", size=1,
                   release_date=release_date, media_type="movie")
    res.scanned_at = scanned_at
    db.add(res)
    db.flush()
    lv = SCLibraryVersion(work_key=work_key, version_no=1,
                          resource_ids_json=json.dumps([res.id]))
    db.add(lv)
    db.flush()
    pl = SCPushList(list_key=f"pl:{work_key}", name="清单", status="active")
    db.add(pl)
    db.flush()
    item = SCPushListItem(list_id=pl.id, work_key=work_key, library_version_id=lv.id,
                          category_key="genre:剧情", target_type="channel",
                          chat_id="-1", dedup_key=f"d:{work_key}")
    db.add(item)
    db.flush()
    task = SCPublishTask(list_item_id=item.id, work_key=work_key, kind="first",
                         status="planned", link_state="unknown")
    if scheduled_at is not None:
        task.scheduled_at = scheduled_at
        task.created_at = scheduled_at
    db.add(task)
    db.commit()
    return task


def test_conflict_sort_key_prefers_release(env):
    """新上映优先：近 N 天上映的作品排在前面。"""
    from app import share_center_v2 as sc2
    from datetime import datetime, timedelta, timezone
    sh = timezone(timedelta(hours=8))
    now = datetime.now(sh)
    S, _ = env
    with S() as db:
        fresh = _seed_conflict_task(db, "work:fresh",
                                    (now - timedelta(days=3)).date().isoformat(),
                                    now - timedelta(days=9))
        old = _seed_conflict_task(db, "work:old",
                                  (now - timedelta(days=400)).date().isoformat(),
                                  now - timedelta(days=1))
        resolver = sc2._RepResolver(db)
        cutoff = now - timedelta(days=sc2.NEW_RELEASE_WINDOW_DAYS)
        k_fresh = sc2._conflict_sort_key(db, fresh, "prefer_release", resolver, cutoff)
        k_old = sc2._conflict_sort_key(db, old, "prefer_release", resolver, cutoff)
        assert k_fresh < k_old, "新上映的作品必须排在前面"


def test_conflict_sort_key_prefers_new_entry(env):
    """新入库优先：刚入库的作品排在前面。"""
    from app import share_center_v2 as sc2
    from datetime import datetime, timedelta, timezone
    sh = timezone(timedelta(hours=8))
    now = datetime.now(sh)
    S, _ = env
    with S() as db:
        fresh = _seed_conflict_task(db, "work:fresh",
                                    (now - timedelta(days=400)).date().isoformat(),
                                    now - timedelta(days=1))
        old = _seed_conflict_task(db, "work:old",
                                  (now - timedelta(days=3)).date().isoformat(),
                                  now - timedelta(days=9))
        resolver = sc2._RepResolver(db)
        cutoff = now - timedelta(days=sc2.NEW_RELEASE_WINDOW_DAYS)
        k_fresh = sc2._conflict_sort_key(db, fresh, "prefer_new", resolver, cutoff)
        k_old = sc2._conflict_sort_key(db, old, "prefer_new", resolver, cutoff)
        assert k_fresh < k_old, "刚入库的作品必须排在前面"


def test_conflict_sort_key_prefers_backlog(env):
    """存量排期优先：已排期更久的存量作品排在前面。"""
    from app import share_center_v2 as sc2
    from datetime import datetime, timedelta, timezone
    sh = timezone(timedelta(hours=8))
    now = datetime.now(sh)
    S, _ = env
    with S() as db:
        backlog = _seed_conflict_task(db, "work:backlog",
                                       (now - timedelta(days=400)).date().isoformat(),
                                       now - timedelta(days=40),
                                       scheduled_at=now - timedelta(days=40))
        newcomer = _seed_conflict_task(db, "work:new",
                                       (now - timedelta(days=2)).date().isoformat(),
                                       now - timedelta(days=1),
                                       scheduled_at=now - timedelta(days=1))
        resolver = sc2._RepResolver(db)
        cutoff = now - timedelta(days=sc2.NEW_RELEASE_WINDOW_DAYS)
        k_backlog = sc2._conflict_sort_key(db, backlog, "prefer_backlog", resolver, cutoff)
        k_new = sc2._conflict_sort_key(db, newcomer, "prefer_backlog", resolver, cutoff)
        assert k_backlog < k_new, "存量排期的作品必须排在前面"


def test_runtime_config_default_conflict_policy():
    """运行配置默认值就是新口径的第一档。"""
    from app import runtime_config
    _key, default = runtime_config.DEFAULTS["sc_conflict_policy"]
    assert default == "prefer_release"


# ---------------------------------------------------------------------------
# 2. 默认时间表（热门白天 / 冷门夜间，2 小时一段，避开静默）
# ---------------------------------------------------------------------------

def test_default_segments_are_two_hour_granularity():
    """白天段 + 夜间段共 12 段，每段 2 小时，恰好铺满 0-24 全天。"""
    from app import share_center_v2 as sc2
    assert len(sc2.CATEGORY_DAY_SEGMENTS) == 9
    assert len(sc2.CATEGORY_NIGHT_SEGMENTS) == 3
    assert len(sc2.CATEGORY_WINDOW_SEGMENTS) == 12
    for ws, we in sc2.CATEGORY_WINDOW_SEGMENTS:
        assert we - ws == 2, "默认时间段必须是 2 小时颗粒度"


def test_default_segments_not_all_overnight():
    """默认时间表不得退化成「全都是 0-8」这种单一区间。"""
    from app import share_center_v2 as sc2
    starts = {ws for ws, _we in sc2.CATEGORY_WINDOW_SEGMENTS}
    assert len(starts) >= 6, "默认时段必须铺开多个起点"
    assert (0, 8) not in sc2.CATEGORY_WINDOW_SEGMENTS
    assert (8, 24) not in sc2.CATEGORY_WINDOW_SEGMENTS
    # 白天段覆盖 6:00–24:00，夜间段覆盖 0:00–6:00
    day_hours = set()
    for ws, we in sc2.CATEGORY_DAY_SEGMENTS:
        day_hours |= set(range(ws, we))
    assert day_hours == set(range(6, 24)), "白天段应覆盖 6:00–24:00"
    night_hours = set()
    for ws, we in sc2.CATEGORY_NIGHT_SEGMENTS:
        night_hours |= set(range(ws, we))
    assert night_hours == set(range(0, 6)), "夜间段应覆盖 0:00–6:00"


def test_default_segments_tile_full_day_without_gap():
    """12 段 × 2 小时必须恰好铺满 0-24，不重叠也不留空洞。

    v2.1.6 真实缺陷：段池只有 11 段（22 小时），22:00-24:00 是空洞——
    没有任何分类的排期窗口覆盖这一小时，而它恰是晚间黄金档，
    plan_engine 的分类过滤会把所有分类都筛掉 → 整个 22-24 点零产出。
    """
    from app import share_center_v2 as sc2
    hours: set[int] = set()
    for ws, we in sc2.CATEGORY_WINDOW_SEGMENTS:
        seg = set(range(ws, we))
        assert not (hours & seg), f"{ws}-{we} 与其它段重叠"
        hours |= seg
    assert hours == set(range(24)), "段池必须恰好铺满 0-24 全天"


def test_assign_category_windows_hot_day_cold_night():
    """热门分类排白天、冷门分类排夜间。"""
    from app import share_center_v2 as sc2
    prefs = [{"category_key": f"k{i}", "score": 100 - i} for i in range(12)]
    windows = sc2._assign_category_windows(prefs)
    day = list(sc2.CATEGORY_DAY_SEGMENTS)
    night = list(sc2.CATEGORY_NIGHT_SEGMENTS)
    for i in range(len(day)):
        assert windows[f"k{i}"] == day[i]
    for j in range(len(night)):
        assert windows[f"k{len(day) + j}"] == night[j]


def test_assign_category_windows_overflow_keeps_two_hour_granularity():
    """分类数 > 段数时，溢出分类**不得退化成全天窗口 0-24**。

    v2.1.6 真实缺陷：段用完后直接回退 FULL_DAY_WINDOW，
    27 个分类里 15 个的时段是「00:00–24:00」——既不是 2 小时颗粒度，
    又让长尾分类全天与热门分类抢同一批小时。
    修正：冷门优先**循环复用夜间段**，每个分类拿到的都是 2 小时段。
    """
    from app import share_center_v2 as sc2
    prefs = [{"category_key": f"k{i}", "score": 100 - i} for i in range(27)]
    windows = sc2._assign_category_windows(prefs)
    assert len(windows) == 27
    night = list(sc2.CATEGORY_NIGHT_SEGMENTS)
    for key, (ws, we) in windows.items():
        assert we - ws == 2, f"{key} 的时段必须是 2 小时颗粒度，不得退化成全天窗口"
        assert (ws, we) != sc2.FULL_DAY_WINDOW, f"{key} 不得退化成全天窗口"
    # 溢出部分落在夜间段（冷门排夜间）
    overflow_start = len(sc2.CATEGORY_DAY_SEGMENTS) + len(sc2.CATEGORY_NIGHT_SEGMENTS)
    for i in range(overflow_start, 27):
        assert windows[f"k{i}"] in night, f"溢出分类 k{i} 应落在夜间段"


def test_assign_category_windows_avoid_quiet():
    """静默保护开启时，落进静默区间的段整段跳过。"""
    from app import share_center_v2 as sc2
    prefs = [{"category_key": f"k{i}", "score": 100 - i} for i in range(11)]
    quiet = (0, 6)
    windows = sc2._assign_category_windows(prefs, quiet)
    for key, (ws, we) in windows.items():
        assert not sc2._window_overlaps_quiet((ws, we), quiet), \
            f"{key} 的时段 {ws}-{we} 不得与静默区间重叠"


def test_preference_endpoint_exposes_day_and_night_segments(env):
    """偏好端点下发白天段 / 夜间段。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        res = sc2.api_preference(db)
        assert len(res["day_segments"]) == 9
        assert len(res["night_segments"]) == 3
        assert res["segment_hours"] == 2


# ---------------------------------------------------------------------------
# 3. 单一时间段建议（每分类必有建议）
# ---------------------------------------------------------------------------

def test_every_category_has_suggestion(env):
    """每个分类都必有建议（实测优先，无实测用习惯基线兜底）。

    「有没有建议」必须用 **suggest_source** 判定，不能用起点数值真伪——
    夜间段 00:00–02:00 的起点是 0（合法值），用 `assert item["suggest_start"]`
    会把这类建议误判成「无建议」。这正是用户反馈「采纳按钮有的按钮是灰色」的根因。
    """
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        res = sc2.api_category_schedules(db=db)
        assert res["items"], "应有分类排期行"
        for item in res["items"]:
            assert item["suggest_source"], \
                f"{item['name']} 必须有推荐时段（用来源标记判定，不能用起点真伪）"
            assert item["suggest_source"] in ("history", "habit")
            assert item["suggest_end"] - item["suggest_start"] == 2, \
                f"{item['name']} 的推荐时段必须是 2 小时颗粒度"


def test_suggest_source_marks_measured_vs_habit(env):
    """来源标注区分「实测」与「习惯基线」，不把兜底当实测。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        res = sc2.api_category_schedules(db=db)
        assert all(i["suggest_source"] == "habit" for i in res["items"]), \
            "无历史样本时应全部标记为习惯基线"


def test_suggest_source_switches_to_history(env):
    """有真实发布样本时来源切换为「实测」。"""
    from app import share_center_v2 as sc2
    from datetime import datetime, timedelta, timezone
    from app.models import (SCPublishTask, SCPushList, SCPushListItem)
    sh = timezone(timedelta(hours=8))
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        pl = SCPushList(list_key="pl:1", name="清单", status="active")
        db.add(pl)
        db.flush()
        item = SCPushListItem(list_id=pl.id, work_key="work:x", library_version_id=0,
                              category_key="genre:剧情", target_type="channel",
                              chat_id="-1", dedup_key="d:1")
        db.add(item)
        db.flush()
        task = SCPublishTask(list_item_id=item.id, work_key="work:x", kind="first",
                             status="published", link_state="active",
                             published_at=datetime(2026, 1, 5, 21, 30, tzinfo=sh))
        db.add(task)
        db.commit()
        res = sc2.api_category_schedules(db=db)
        row = next(i for i in res["items"] if i["category_key"] == "genre:剧情")
        assert row["suggest_source"] == "history"
        assert (row["suggest_start"], row["suggest_end"]) == (21, 23)


# ---------------------------------------------------------------------------
# 4. 批量操作按勾选生效
# ---------------------------------------------------------------------------

def test_apply_defaults_respects_category_keys(env):
    """一键恢复默认只处理勾选的分类。"""
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        rows = sc2._ensure_schedules(db, version.id)
        db.commit()
        target = rows[0]
        other = rows[1]
        target.window_start, target.window_end = 3, 4
        other.window_start, other.window_end = 3, 4
        db.commit()
        res = sc2.api_apply_schedule_defaults(sc2.Payload(payload={
            "category_keys": [target.category_key], "force": True}), db)
        assert res["ok"] is True
        db.expire_all()
        t = db.scalar(select(SCCategorySchedule).where(SCCategorySchedule.id == target.id))
        o = db.scalar(select(SCCategorySchedule).where(SCCategorySchedule.id == other.id))
        assert (t.window_start, t.window_end) != (3, 4), "勾选的分类应被重置"
        assert (o.window_start, o.window_end) == (3, 4), "未勾选的分类不得被改动"


def test_adopt_ai_all_respects_category_keys(env):
    """批量采纳 AI 建议只处理勾选的分类。"""
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        rows = sc2._ensure_schedules(db, version.id)
        db.commit()
        target = rows[0]
        other = rows[1]
        other.window_start, other.window_end = 3, 4
        db.commit()
        res = sc2.api_adopt_schedule_ai_all(sc2.Payload(payload={
            "category_keys": [target.category_key]}), db)
        assert res["ok"] is True
        assert res["adopted"] == 1
        db.expire_all()
        o = db.scalar(select(SCCategorySchedule).where(SCCategorySchedule.id == other.id))
        assert (o.window_start, o.window_end) == (3, 4), "未勾选的分类不得被采纳"


def test_batch_without_category_keys_means_all(env):
    """不传 category_keys = 全部（未勾选或全勾选的语义）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        sc2._ensure_schedules(db, version.id)
        db.commit()
        res = sc2.api_adopt_schedule_ai_all(sc2.Payload(payload={}), db)
        assert res["ok"] is True
        assert res["total"] == len(res["items"]) if "items" in res else res["total"] > 0
        assert res["adopted"] > 1, "不传勾选时应处理全部分类"


# ---------------------------------------------------------------------------
# 5. 分类级上限退出调度，改由周期计划统一约束
# ---------------------------------------------------------------------------

def test_cycle_plan_exposes_category_day_max(env):
    """周期计划下发单分类每小时 / 每日上限。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        plan = sc2.api_cycle_plan(db)
        assert "category_hour_max" in plan
        assert "category_day_max" in plan


def test_cycle_plan_accepts_category_day_max(env):
    """周期计划保存后单分类每日上限真实落库。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        plan = sc2.api_cycle_plan(db)
        plan["category_day_max"] = 7
        res = sc2.api_save_cycle_plan(sc2.Payload(payload=plan), db)
        assert res["ok"] is True
        again = sc2.api_cycle_plan(db)
        assert again["category_day_max"] == 7


def test_upsert_schedule_keeps_limits_when_absent(env):
    """保存排期不携带分类级上限时，原值必须保留（不能被静默清零）。"""
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        rows = sc2._ensure_schedules(db, version.id)
        db.commit()
        row = rows[0]
        row.hourly_limit, row.daily_limit = 4, 11
        db.commit()
        sc2.api_upsert_category_schedule(sc2.Payload(payload={
            "plan_version_id": version.id,
            "category_key": row.category_key,
            "window_start": 9, "window_end": 11,
        }), db)
        db.expire_all()
        again = db.scalar(select(SCCategorySchedule).where(SCCategorySchedule.id == row.id))
        assert (again.hourly_limit, again.daily_limit) == (4, 11), \
            "分类级上限不再由前端提交，但库中原值不得被清零"


def test_category_limits_not_used_by_plan_engine():
    """plan_engine 不得再消费分类行上的 hourly_limit / daily_limit。"""
    from app import share_center_v2 as sc2
    import inspect
    src = inspect.getsource(sc2.plan_engine)
    assert "s.hourly_limit" not in src and "s.daily_limit" not in src, \
        "分类级上限已退出调度判定"
    assert "cycle_hour_max" in src and "cycle_day_max" in src, \
        "限流必须由周期计划统一约束"


# ---------------------------------------------------------------------------
# 6. 推送渠道：类型 / 名称 / 标识
# ---------------------------------------------------------------------------

def _seed_channel_task(db, *, chat_id, kind, channel_name, target_type,
                       discovered_title="", username=""):
    from app.models import (Bot, Channel, DiscoveredChat, SCPublishTask,
                            SCPushList, SCPushListItem)
    bot = Bot(name="b", token="t")
    db.add(bot)
    db.flush()
    db.add(Channel(bot_id=bot.id, name=channel_name, chat_id=chat_id, enabled=True,
                   kind=kind))
    if discovered_title or username:
        db.add(DiscoveredChat(bot_id=bot.id, chat_id=chat_id,
                              title=discovered_title, username=username))
    pl = SCPushList(list_key="pl:1", name="清单", status="active")
    db.add(pl)
    db.flush()
    item = SCPushListItem(list_id=pl.id, work_key="work:x", library_version_id=0,
                          category_key="genre:剧情", target_type=target_type,
                          bot_id=bot.id, chat_id=chat_id, dedup_key="d:1")
    db.add(item)
    db.flush()
    task = SCPublishTask(list_item_id=item.id, work_key="work:x", kind="first",
                         status="planned", link_state="unknown")
    db.add(task)
    db.commit()
    return task


def test_channel_kind_wins_over_item_target_type(env):
    """清单项 target_type 写错时，以 Channel 登记的 kind 为准。

    真机证据：channels 表把频道登记为 kind='channel'，但推送清单项的
    target_type 全写成 'group'——旧实现会把频道误判成群。
    """
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        task = _seed_channel_task(db, chat_id="-1003749901817", kind="channel",
                                  channel_name="订阅", target_type="group")
        res = sc2.api_tasks(db=db)
        item = next(i for i in res["items"] if i["id"] == task.id)
        assert item["channel_names"] == ["订阅"]
        assert item["group_names"] == [], "登记为频道的目标不得混进群"


def test_channel_name_comes_from_discovered_chat(env):
    """真实群名/频道名取自 Bot 发现台账（DiscoveredChat.title）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        task = _seed_channel_task(db, chat_id="-1003749901817", kind="channel",
                                  channel_name="订阅", target_type="channel",
                                  discovered_title="yatg影视分享", username="guanya_tg")
        res = sc2.api_tasks(db=db)
        item = next(i for i in res["items"] if i["id"] == task.id)
        assert item["channel_names"] == ["yatg影视分享"]
        assert item["channel_targets"][0]["label"] == "yatg影视分享"
        assert item["channel_targets"][0]["ref"] == "guanya_tg"


def test_channel_ref_falls_back_to_chat_id(env):
    """没有 @用户名时，标识退回 chat_id。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        task = _seed_channel_task(db, chat_id="-1003708133864", kind="group",
                                  channel_name="route:group:-1003708133864",
                                  target_type="group",
                                  discovered_title="ya-tg影视交流群")
        res = sc2.api_tasks(db=db)
        item = next(i for i in res["items"] if i["id"] == task.id)
        assert item["group_names"] == ["ya-tg影视交流群"], "route: 占位名不得露出"
        assert item["group_targets"][0]["ref"] == "-1003708133864"


def test_target_summary_format(env):
    """汇总文案格式为「群:名称（群id或用户名）」「频道:名称（频道id或用户名）」。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        task = _seed_channel_task(db, chat_id="-1001", kind="group",
                                  channel_name="影迷群", target_type="group",
                                  discovered_title="影迷群", username="movie_fans")
        res = sc2.api_tasks(db=db)
        item = next(i for i in res["items"] if i["id"] == task.id)
        assert "群：影迷群（movie_fans）" in item["target_summary"]


# ---------------------------------------------------------------------------
# 7. 前端契约
# ---------------------------------------------------------------------------

def test_frontend_schedule_table_has_selection_column():
    """分类排期表有勾选列，批量操作按勾选生效。"""
    app = _read_app()
    assert 'type="selection"' in app, "分类排期表缺少勾选列"
    assert "scSchedSelection" in app
    assert "scSchedTargetRows" in app
    assert "scSchedTargetKeys" in app


def test_frontend_batch_operations_use_selection():
    """一键恢复默认 / 批量采纳 / 保存都按勾选生效。"""
    app = _read_app()
    assert "category_keys" in app, "批量请求必须带勾选分类"
    # 未勾选或全勾选 = 全部
    assert "picked.length >= rows.length" in app


def test_frontend_schedule_table_dropped_columns():
    """去掉周期维度 / 偏好档位 / 每小时上限 / 日上限 / 周维度建议列。

    只查**列标签**（`label="..."`），不查整段文本——注释里会写明
    「去掉了哪些列」，那是给维护者看的变更说明，不是界面文案。
    """
    import re
    block = _rule_block()
    labels = set(re.findall(r'label="([^"]+)"', block))
    for banned in ("周期维度", "偏好档位", "每小时上限", "日上限",
                   "AI 建议时间段（周维度）", "AI 建议时间段"):
        assert banned not in labels, f"分类排期表不得出现「{banned}」列"
    # 合并后的新列必须在
    assert "推荐时间段" in labels
    assert "配置时间段" in labels


def test_frontend_merged_suggest_column():
    """档位偏好与 AI 建议合并为单一「推荐时间段」列。"""
    block = _rule_block()
    assert "推荐时间段" in block
    assert "s.row.suggest_start" in block
    assert "s.row.suggest_end" in block
    assert "s.row.pref_rank" in block, "偏好排名并入推荐时间段列"
    # 来源标签：实测 / 习惯基线
    assert "实测" in block and "习惯基线" in block


def test_frontend_adopt_button_not_disabled_by_default():
    """采纳按钮只在「完全没有建议」时置灰（每个分类都必有建议）。

    v2.1.6：置灰判定用 suggest_source（来源标记），不用起点数值真伪——
    夜间段 00:00–02:00 的起点是 0，用 suggest_start 会误置灰（用户实测反馈）。
    """
    block = _rule_block()
    assert ':disabled="!s.row.suggest_source"' in block
    assert "scWeekMeasured" not in app_all()


def app_all() -> str:
    return _read_app()


def test_frontend_removed_copy_and_preference_assign():
    """复制按钮与「按偏好分配时段」按钮已下线。"""
    app = _read_app()
    assert "scCopySched" not in app, "分类后的复制按钮已下线"
    assert "/sc/category-schedules/copy" not in app
    assert "scAssignPreferenceWindows" not in app, "按偏好分配按钮已下线"
    assert "/sc/preference/assign" not in app


def test_frontend_conflict_policy_three_options():
    """冲突处理下拉只有三档。"""
    app = _read_app()
    assert 'value="prefer_release"' in app
    assert 'value="prefer_new"' in app
    assert 'value="prefer_backlog"' in app
    assert "pause_normal" not in app, "旧的重复语义选项不得残留"


def test_frontend_cycle_plan_only_on_basic_tab():
    """周期计划管理卡片只在基础规则 tab 常驻，分类排期 tab 不重复露出。"""
    app = _read_app()
    assert 'v-show="scRuleTab===\'基础规则\'"' in app
    # 周期计划卡片与上新即时推送卡片都必须挂在基础规则 tab 上
    assert app.count('v-show="scRuleTab===\'基础规则\'"') >= 2


def test_frontend_rule_layout_single_column():
    """发布规则页内部改为单列竖排（两张卡片上下对齐）。"""
    app = _read_app()
    assert ".sc-rule-layout{display:grid;grid-template-columns:minmax(0,1fr)" in app


def test_frontend_channel_column_format():
    """推送渠道列按「群:名称（id或用户名）」格式展示。"""
    app = _read_app()
    assert "scChannelTargetLines" in app
    assert "频道:${t.label}（${t.ref}）" in app
    assert "群:${t.label}（${t.ref}）" in app
    # 旧的「频道 xxx」「群 xxx」拼接口径下线
    assert "频道 {{ s.row.channel_names.join" not in app
    assert "群 {{ s.row.group_names.join" not in app
