"""v2.1.45 验收：周期计划里的「周度 / 月度」整体下线（前端撤页 + 后端删判定）。

用户口径（2026-11-25 原话）：
  「周期计划管理里面周度、月度的配置已经下线，但界面还在，需要去掉周度、月度对应的
   前端展示（需注意后端代码同步去掉，避免后续被规则卡住，而不知情）」

背景（改前取证）：
  · v2.1.42 只砍掉了周 / 月**限额**（`week_max` / `month_max`），
    `_cycle_quota_gate` 里 `week_rest_enabled` / `month_rest_enabled` 两个**休止窗口**
    判定仍在，前端也仍留着「周度」「月度」两个子页，各挂一个开关。
  · 后果：库里一条 `week_rest_enabled=true` 的旧配置就能让普通排期在每周固定那天
    整段停排（`weekly_rest`），而页面上那两个子页一旦撤掉，用户再也找不到原因。
    用户担心的正是这个「被规则卡住而不自知」。

本文件锁四件事：
  ① 门禁不再读任何周 / 月键（休止开关开着也不拦，且与今天是不是休止日无关）；
  ② 默认值、原因码、保存夹逼里都不再有周 / 月休止痕迹；
  ③ 接口回显与保存会把历史遗留的周 / 月键清掉（不留在配置里当隐形开关）；
  ④ 前端「周期计划管理」只剩小时 / 日度两个子页，且日度与静默的配置一项不少。
"""
import json
from datetime import datetime, timedelta, timezone

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

SC2_SRC = (__import__("pathlib").Path(__file__).resolve().parents[1]
           / "backend/app/share_center_v2.py").read_text(encoding="utf-8")
APP_VUE = (__import__("pathlib").Path(__file__).resolve().parents[1]
           / "frontend/src/App.vue").read_text(encoding="utf-8")

CAT = "genre:剧情"

RETIRED = ("week_rest_enabled", "week_rest_day", "week_rest_hour",
           "month_rest_enabled")

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


def _seed(db, *, cycle):
    _plan(db, cycle=cycle)
    _live(db, "活A", "f-a")
    _live(db, "活B", "f-b")
    db.commit()


def _now_sh():
    return datetime.now(timezone(timedelta(hours=8)))


def _old_week_rest_branch_blocks() -> bool:
    """复刻 v2.1.44 的周休止分支：`week_rest_day=今天`、`week_rest_hour=0` 时当天**必拦**。

    用来反证「下面那组参数在旧代码下 100% 会被卡住」，否则「新代码不拦」
    可能只是因为参数凑巧没命中。
    """
    now_sh = _now_sh()
    rest_day = now_sh.weekday()
    rest_hour = 0
    return now_sh.weekday() == rest_day and now_sh.hour >= rest_hour


# ===========================================================================
# 一、门禁不再读周 / 月键
# ===========================================================================
def test_gate_ignores_rest_switches_on_any_day():
    """休止开关全开 + 日额度不限 → 门禁必须放行（(False, "")，与日期无关）。

    这条是行为层的核心：新实现里根本没有 read 这些键的分支，所以无论今天
    是不是休止日、是不是月末，结果都一样。
    """
    with SessionLocal() as db:
        cycle = {
            "day_max": 0,
            "week_rest_enabled": True, "week_rest_day": _now_sh().weekday(),
            "week_rest_hour": 0,
            "month_rest_enabled": True,
        }
        blocked, reason = sc2._cycle_quota_gate(db, cycle)
    assert (blocked, reason) == (False, ""), (
        f"休止开关已下线，不该再拦任何排期，实际 {blocked} / {reason}")


def test_gate_still_enforces_day_quota():
    """日度那层不能被顺手删掉——这是唯一保留的额度层。"""
    with SessionLocal() as db:
        _seed(db, cycle={"day_max": 1})
        now = datetime.now(timezone.utc)
        db.add(SCPublishTask(list_item_id=None, work_key="wk-old-0",
                             kind="first", status="planned", priority=0,
                             scheduled_at=now, created_at=now))
        db.commit()
        blocked, reason = sc2._cycle_quota_gate(db, {"day_max": 1})
    assert blocked is True and reason == "day_quota", f"日额度门禁失效：{blocked} / {reason}"


def test_rest_switch_cannot_block_engine_anymore():
    """反证：同一组参数在旧实现下必然 `weekly_rest` 拦截，新实现照常排期。"""
    assert _old_week_rest_branch_blocks() is True, "反证前提不成立：旧周休止分支没命中"

    with SessionLocal() as db:
        _seed(db, cycle={
            "day_max": 100,
            "week_rest_enabled": True, "week_rest_day": _now_sh().weekday(),
            "week_rest_hour": 0,
            "month_rest_enabled": True,
        })
        engine = sc2.plan_engine(db)

    assert engine.get("quota") not in ("weekly_rest", "monthly_rest"), \
        f"休止判定应已删除，实际仍拦：{engine}"
    assert engine.get("tasks", 0) > 0, f"休止开关不该再让排期空转，实际 {engine}"


def test_forecast_never_mentions_rest():
    """预测侧的原因文案里也不能再出现「休止 / 休刊」——用户已经无处可改。"""
    with SessionLocal() as db:
        _seed(db, cycle={
            "day_max": 100,
            "week_rest_enabled": True, "week_rest_day": _now_sh().weekday(),
            "week_rest_hour": 0,
            "month_rest_enabled": True,
        })
        out = sc2.forecast_schedule(db)

    tells = [h.get("blocked") for h in out["hours"]] + list(out["notes"])
    assert not any("休止" in str(t) or "休刊" in str(t) for t in tells), \
        f"预测文案还在说休止/休刊：{tells}"


# ===========================================================================
# 二、默认值 / 原因码 / 保存夹逼里不留周 · 月痕迹
# ===========================================================================
def test_defaults_drop_all_rest_keys():
    from app.share_center_v2 import CYCLE_PLAN_DEFAULTS, RETIRED_CYCLE_KEYS
    for key in RETIRED:
        assert key not in CYCLE_PLAN_DEFAULTS, f"{key} 不该再有默认值"
        assert key in RETIRED_CYCLE_KEYS, f"{key} 必须进下线清单，否则清不掉"
    # 保留的四项一个不能少
    for key in ("hour_max", "category_hour_max", "category_day_max", "day_max",
                "quiet_enabled", "quiet_start", "quiet_end",
                "window_start", "window_end"):
        assert key in CYCLE_PLAN_DEFAULTS, f"保留项 {key} 丢了"


def test_reason_codes_gone():
    # 原因码只可能出现在代码字面量里（注释/文档用反引号，不带英文双引号）
    assert '"weekly_rest"' not in SC2_SRC
    assert '"monthly_rest"' not in SC2_SRC
    assert '"day_quota"' in SC2_SRC, "日度原因码必须保留"


def test_save_does_not_clamp_rest_hour():
    start = SC2_SRC.index("def api_save_cycle_plan(")
    end = SC2_SRC.index("def ", start + 10)
    body = SC2_SRC[start:end]
    assert "week_rest_hour" not in body, "保存时还在夹逼已下线的键"
    assert 'for k in ("quiet_start", "window_start")' in body


# ===========================================================================
# 三、接口：回显过滤 + 保存清理（历史遗留值不许当隐形开关）
# ===========================================================================
def test_get_cycle_plan_hides_retired_keys():
    with SessionLocal() as db:
        _plan(db, cycle={
            "day_max": 90,
            "week_rest_enabled": True, "week_rest_day": 3, "week_rest_hour": 21,
            "month_rest_enabled": True,
        })
        out = sc2.api_cycle_plan(db)

    for key in RETIRED:
        assert key not in out, f"回显里还带着已下线的 {key}，页面上看不见却在配置里"
    assert out["day_max"] == 90, "正常配置必须原样回显"


def test_save_cycle_plan_cleans_retired_keys():
    with SessionLocal() as db:
        ver = _plan(db, cycle={
            "week_rest_enabled": True, "week_rest_day": 3, "week_rest_hour": 21,
            "month_rest_enabled": True,
        })
        body = sc2.Payload(payload={
            "day_max": 120,
            # 前端已不该带这两个键；这里故意带上，模拟旧页面 / 旧缓存回写
            "week_rest_enabled": True,
            "month_rest_enabled": True,
        })
        out = sc2.api_save_cycle_plan(body, db)
        assert out.get("ok") is True

        db.expire_all()
        row = db.get(SCSharePlanVersion, ver.id)
        saved = (json.loads(row.config_json or "{}").get("cycle_plan") or {})

    assert saved.get("day_max") == 120, "正常配置没存进去"
    for key in RETIRED:
        assert key not in saved, f"保存后配置里还留着已下线的 {key}"


# ===========================================================================
# 四、前端：只剩小时 / 日度
# ===========================================================================
def test_frontend_cycle_tabs_only_hour_and_day():
    assert "['小时','日度']" in APP_VUE, "子页应只剩小时 / 日度"
    assert "['小时','日度','周度','月度']" not in APP_VUE, "周度 / 月度子页没撤掉"


def test_frontend_has_no_rest_switches():
    for word in ("周度休闲窗口", "月度休刊",
                 "scCyclePlan.week_rest_enabled",
                 "scCyclePlan.month_rest_enabled"):
        assert word not in APP_VUE, f"页面还挂着已下线的「{word}」"


def test_frontend_keeps_hour_and_day_settings():
    """撤周/月不能顺手把小时、日度、静默的配置也删了。"""
    for word in ("scCyclePlan.hour_max", "scCyclePlan.category_hour_max",
                 "scCyclePlan.day_max", "scCyclePlan.category_day_max",
                 "scCyclePlan.window_start", "scCyclePlan.window_end",
                 "scCyclePlan.quiet_enabled", "scCyclePlan.quiet_start",
                 "scCyclePlan.quiet_end"):
        assert word in APP_VUE, f"保留项 {word} 被误删"
    assert "只按<b>日上限</b>控" in APP_VUE, "日度页少了「只按日上限控量」的说明"
