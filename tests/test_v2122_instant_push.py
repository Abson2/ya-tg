"""v2.1.22：「上新即时推送」三档语义 + 追更（有更新可重复推）的完整口径。

用户原话（逐条锁死）：
    针对上新即时推送，若不启用则资源库内影视全部当作存量排期持续轮转，若启用则：
    1、新上映优先（默认状态）：
        a、新上映的影视入库完优先加入推送队列（插队）立即推送；
        b、其余均按排期轮转；
    2、新入库优先：
        a、新上映的影视入库完优先加入推送队列（插队）立即推送；
        b、按照资源库入库的顺序优先排队推送；
        c、资源库存量按排期轮转推送；
    3、存量排期优先：全量按照排期轮转（即时推送关闭状态）；
    无论存量、新入库、新上映有更新可以重复推送（不重复建分享链接），
    均需要在推送队列、今日推送排期、推送任务列表体现。

本文件对应的实现锚点：
    · `instant_push_mode()`        —— 开关 + 三档策略 → `off` / `prefer_release` / `prefer_new`
    · `_instant_candidates()`      —— 插队候选（新上映按上映日倒序、新入库按首次入库升序）
    · `plan_engine()` 插队通道     —— priority 900（新上映）/ 800（新入库），且不受分类时段窗口约束
    · `_conflict_sort_key()` 短路  —— 插队任务不会被冲突策略洗回存量后面
    · `register_update_task()`     —— 追更记账（复用分享链接、不占首推名额、落 submitted）
    · `dispatch_ready_tasks()`     —— 显式排除追更任务（否则会被当首推再建一次链）
"""
import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app import runtime_config
from app import share_center_v2 as sc2
from app.db import SessionLocal, migrate
from app.models import (
    AppConfig,
    AuditLog,
    Bot,
    Channel,
    QueueItem,
    Resource,
    SCCategory,
    SCCategoryCursor,
    SCCategorySchedule,
    SCHourWindow,
    SCLibraryVersion,
    SCPublishTask,
    SCPushList,
    SCPushListItem,
    SCSharePlan,
    SCSharePlanVersion,
    SCWorkCategory,
    ShareTask,
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


# ---------------------------------------------------------------------------
# 造数
# ---------------------------------------------------------------------------

def _work(db, *, title, file_id, release_date="", entry_days_ago=100,
          entry_at=None, share_url="https://pan.quark.cn/s/abc"):
    """造一部作品：一条资源 + 一条库快照（快照登记时间 = 首次入库时间）。

    `entry_at` 直接给绝对时刻（v2.1.23 起「新入库」按上海自然日判定，需要精确
    控制「同一天内的先后」才能稳定验证排序，不能靠 days 这种粗粒度）。
    """
    res = Resource(file_id=file_id, name=f"{title}.mkv", ext=".mkv", size=100,
                   utime=1, ctime=0, identify_status="identified",
                   media_type="movie", tmdb_id=abs(hash(file_id)) % 100000,
                   media_title=title, share_dir_name=title, release_date=release_date,
                   share_status="active", share_url=share_url)
    db.add(res)
    db.flush()
    work_key = f"work:{title}"
    db.add(SCLibraryVersion(
        work_key=work_key, version_no=1, fingerprint=f"fp-{file_id}",
        media_type="movie", resource_ids_json=json.dumps([res.id]),
        metadata_json=json.dumps({"title": title}),
        change_type="created",
        created_at=(entry_at if entry_at is not None
                    else datetime.now(timezone.utc) - timedelta(days=entry_days_ago)),
    ))
    db.flush()
    return work_key, res


def _plan(db, *, category_limit=20, work_limit=50, cycle=None):
    """建一份生效中的计划版本 + 0-24 全天窗口 + 一个分类 + 一个启用渠道。"""
    plan = SCSharePlan(name="p", plan_type="daily", enabled=True)
    db.add(plan)
    db.flush()
    cfg = {"category_limit": category_limit, "work_limit": work_limit}
    if cycle is not None:
        cfg["cycle_plan"] = cycle
    ver = SCSharePlanVersion(plan_id=plan.id, version_no=1, status="active",
                             config_json=json.dumps(cfg))
    db.add(ver)
    db.flush()
    ensure_full_day_hour_window(db, ver.id)
    db.add(SCCategory(category_key=CAT, name="剧情", enabled=True, priority=0))
    bot = Bot(name="b", token="x", enabled=True)
    db.add(bot)
    db.flush()
    db.add(Channel(bot_id=bot.id, name="频道", chat_id="@ch", kind="channel",
                   enabled=True))
    db.commit()
    return ver


def _bind(db, work_key):
    db.add(SCWorkCategory(work_key=work_key, category_key=CAT, media_type="movie",
                          source="tmdb", confidence=1.0))
    db.flush()


def _set_rules(db, *, enabled="true", policy="prefer_release", hourly="0"):
    runtime_config.set_many(db, {
        "sc_quick_publish_enabled": enabled,
        "sc_conflict_policy": policy,
        "sc_quick_publish_hourly_limit": hourly,
    })


def _tasks(db):
    return list(db.scalars(select(SCPublishTask).order_by(SCPublishTask.id)).all())


def _rel(days_ago):
    return (datetime.now(sc2.shanghai_tz()) - timedelta(days=days_ago)).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# ① 档位判定：开关关闭 / 存量排期优先 都等价于「全量按排期轮转」
# ---------------------------------------------------------------------------

def test_switch_off_gives_off_mode_and_no_candidates():
    with SessionLocal() as db:
        _set_rules(db, enabled="false", policy="prefer_release")
        db.commit()
        assert sc2.instant_push_mode(db) == "off"
        assert sc2._instant_candidates(db, "off") == []


def test_default_mode_is_prefer_release():
    """没有任何配置时默认档 = 新上映优先（用户口径「默认状态」）。"""
    with SessionLocal() as db:
        assert sc2.instant_push_mode(db) == "prefer_release"


def test_prefer_backlog_equals_off():
    """存量排期优先 = 即时推送关闭状态（用户原话第 3 条）。"""
    with SessionLocal() as db:
        _set_rules(db, enabled="true", policy="prefer_backlog")
        db.commit()
        assert sc2.instant_push_mode(db) == "off"
        # 关掉即时推送后即便有刚上映的作品，也不该有插队候选
        _work(db, title="刚上映", file_id="f-r", release_date=_rel(3))
        db.commit()
        assert sc2._instant_candidates(db, sc2.instant_push_mode(db)) == []


def test_illegal_policy_falls_back_to_default():
    with SessionLocal() as db:
        _set_rules(db, enabled="true", policy="不存在")
        db.commit()
        assert sc2.instant_push_mode(db) == "prefer_release"


# ---------------------------------------------------------------------------
# ② 候选判定：新上映 / 新入库 的边界
# ---------------------------------------------------------------------------

def test_new_release_candidate_sorted_by_release_date_desc():
    with SessionLocal() as db:
        _work(db, title="较早上映", file_id="f-r1", release_date=_rel(20))
        _work(db, title="最新上映", file_id="f-r2", release_date=_rel(2))
        _work(db, title="没上映日期", file_id="f-r3")
        db.commit()
        rows = sc2._instant_candidates(db, "prefer_release")
        keys = [k for k, _lv, _lane in rows]
        assert keys == ["work:最新上映", "work:较早上映"]
        assert all(lane == "new_release" for _k, _lv, lane in rows)


def test_future_release_is_not_instant_candidate():
    """还没上映的作品不能插队——那是在编造「新上映」。"""
    with SessionLocal() as db:
        future = (datetime.now(sc2.shanghai_tz()) + timedelta(days=15)).strftime("%Y-%m-%d")
        _work(db, title="未上映", file_id="f-f", release_date=future)
        db.commit()
        assert sc2._instant_candidates(db, "prefer_release") == []


def test_release_outside_window_is_not_instant_candidate():
    with SessionLocal() as db:
        _work(db, title="老上映", file_id="f-o",
              release_date=_rel(sc2.NEW_RELEASE_WINDOW_DAYS + 10))
        db.commit()
        assert sc2._instant_candidates(db, "prefer_release") == []


def test_prefer_release_has_no_new_library_lane():
    """新上映优先档：刚入库的老片不该插队（口径 b「其余均按排期轮转」）。"""
    with SessionLocal() as db:
        _work(db, title="刚入库老片", file_id="f-n1", entry_days_ago=1)
        db.commit()
        assert sc2._instant_candidates(db, "prefer_release") == []


def test_prefer_new_orders_new_library_by_first_entry_time():
    """新入库优先档：按资源库入库的顺序排（先入库的先推）。

    v2.1.23：「新入库」= **当天**入库，所以三部作品都放在今天，只是入库先后不同；
    「现在」用 `now_sh` 钉在今天 12:00，避免用例在凌晨跑时算成"昨天入库"而假红。
    """
    with SessionLocal() as db:
        now_sh = datetime.now(sc2.shanghai_tz()).replace(
            hour=12, minute=0, second=0, microsecond=0)
        _work(db, title="先入库", file_id="f-a", entry_at=now_sh - timedelta(hours=8))
        _work(db, title="中入库", file_id="f-b", entry_at=now_sh - timedelta(hours=5))
        _work(db, title="后入库", file_id="f-c", entry_at=now_sh - timedelta(hours=2))
        db.commit()
        rows = sc2._instant_candidates(db, "prefer_new", now_sh=now_sh)
        assert [k for k, _lv, _lane in rows] == ["work:先入库", "work:中入库", "work:后入库"]
        assert all(lane == "new_library" for _k, _lv, lane in rows)


def test_prefer_new_puts_new_release_before_new_library():
    """第 2 档的 a 条：新上映仍然最先（插队），新入库排在它后面。"""
    with SessionLocal() as db:
        now_sh = datetime.now(sc2.shanghai_tz()).replace(
            hour=12, minute=0, second=0, microsecond=0)
        _work(db, title="刚入库", file_id="f-l1", entry_at=now_sh - timedelta(hours=3))
        _work(db, title="刚上映", file_id="f-r1", release_date=_rel(2),
              entry_at=now_sh - timedelta(hours=3))
        db.commit()
        rows = sc2._instant_candidates(db, "prefer_new", now_sh=now_sh)
        assert [k for k, _lv, _lane in rows] == ["work:刚上映", "work:刚入库"]
        assert [lane for _k, _lv, lane in rows] == ["new_release", "new_library"]


def test_old_work_with_new_version_is_not_new_library():
    """老片昨天更新了一版不算「新入库」——首次入库时间才是判据。"""
    with SessionLocal() as db:
        wk, res = _work(db, title="老片更新", file_id="f-up", entry_days_ago=180)
        db.add(SCLibraryVersion(
            work_key=wk, version_no=2, fingerprint="fp-up-v2", media_type="movie",
            resource_ids_json=json.dumps([res.id]), metadata_json="{}",
            change_type="updated",
            created_at=datetime.now(timezone.utc) - timedelta(days=1)))
        db.commit()
        assert sc2._instant_candidates(db, "prefer_new") == []


def test_instant_candidates_respect_limit():
    with SessionLocal() as db:
        for i in range(5):
            _work(db, title=f"新片{i}", file_id=f"f-lim{i}", release_date=_rel(i))
        db.commit()
        rows = sc2._instant_candidates(db, "prefer_release", limit=2)
        assert len(rows) == 2


# ---------------------------------------------------------------------------
# ③ plan_engine 插队：优先级与「不受分类时段窗口约束」
# ---------------------------------------------------------------------------

def test_plan_engine_new_release_jumps_with_priority_900():
    with SessionLocal() as db:
        ver = _plan(db)
        _set_rules(db, enabled="true", policy="prefer_release")
        wk_old, _ = _work(db, title="存量老片", file_id="f-old", entry_days_ago=200)
        wk_new, _ = _work(db, title="新上映片", file_id="f-new",
                          release_date=_rel(3), entry_days_ago=200)
        _bind(db, wk_old)
        _bind(db, wk_new)
        db.commit()

        sc2.plan_engine(db)
        by_key = {t.work_key: t for t in _tasks(db)}
        assert by_key[wk_new].priority == sc2.INSTANT_TASK_PRIORITY == 900
        # 存量那部仍按排期轮转生成（口径 b），优先级是分类优先级
        assert by_key[wk_old].priority == 0


def test_plan_engine_off_only_rotates_backlog():
    """开关关闭：新上映也不插队，全部按排期轮转。"""
    with SessionLocal() as db:
        _plan(db)
        _set_rules(db, enabled="false", policy="prefer_release")
        wk, _ = _work(db, title="新上映但不插队", file_id="f-off",
                      release_date=_rel(2), entry_days_ago=200)
        _bind(db, wk)
        db.commit()

        sc2.plan_engine(db)
        rows = _tasks(db)
        assert rows, "关闭即时推送后仍应走存量轮转生成任务"
        assert all(t.priority < sc2.NEW_ENTRY_TASK_PRIORITY for t in rows)


def test_plan_engine_new_release_ignores_category_window():
    """插队不受分类时段窗口约束——那是「排期」的约束。

    做法：把该分类的窗口设成**不包含当前小时**的一段。
    存量作品因此排不出来，但新上映的插队任务照样生成。
    """
    with SessionLocal() as db:
        ver = _plan(db)
        _set_rules(db, enabled="true", policy="prefer_release")
        cur = datetime.now(sc2.shanghai_tz()).hour
        start = (cur + 1) % 24
        db.add(SCCategorySchedule(plan_version_id=ver.id, category_key=CAT,
                                  window_start=start, window_end=(start + 1) % 24,
                                  enabled=True, window_auto=False))
        wk_old, _ = _work(db, title="排不出的存量", file_id="f-w1", entry_days_ago=200)
        wk_new, _ = _work(db, title="窗口外的插队", file_id="f-w2",
                          release_date=_rel(2), entry_days_ago=200)
        _bind(db, wk_old)
        _bind(db, wk_new)
        db.commit()

        sc2.plan_engine(db)
        keys = {t.work_key for t in _tasks(db)}
        assert wk_new in keys, "新上映的插队任务不该被分类时段窗口拦住"
        assert wk_old not in keys, "存量作品该被分类时段窗口拦住（对照组）"


def test_plan_engine_prefer_new_gives_priority_800():
    with SessionLocal() as db:
        _plan(db)
        _set_rules(db, enabled="true", policy="prefer_new")
        # v2.1.23：「新入库」按上海自然日判定，必须是**今天**入库（0 天前）
        wk, _ = _work(db, title="刚入库", file_id="f-p8", entry_days_ago=0)
        _bind(db, wk)
        db.commit()

        sc2.plan_engine(db)
        by_key = {t.work_key: t for t in _tasks(db)}
        assert by_key[wk].priority == sc2.NEW_ENTRY_TASK_PRIORITY == 800


def test_plan_engine_instant_lane_bounded_by_hour_max():
    with SessionLocal() as db:
        _plan(db, cycle={"hour_max": 1})
        _set_rules(db, enabled="true", policy="prefer_release")
        for i in range(3):
            wk, _ = _work(db, title=f"连映{i}", file_id=f"f-hm{i}", release_date=_rel(i))
            _bind(db, wk)
        db.commit()

        sc2.plan_engine(db)
        assert len(_tasks(db)) <= 1


# ---------------------------------------------------------------------------
# ④ 冲突策略不得把插队任务洗回存量后面
# ---------------------------------------------------------------------------

def test_conflict_sort_key_keeps_instant_tasks_first():
    """prefer_backlog 会按 scheduled_at 排序，插队任务必须短路保住 base。"""
    with SessionLocal() as db:
        policy = "prefer_backlog"
        cutoff = datetime.now(sc2.shanghai_tz()) - timedelta(
            days=sc2.NEW_RELEASE_WINDOW_DAYS)
        hot = SCPublishTask(list_item_id=0, work_key="work:x", kind="first",
                            status="planned", priority=sc2.INSTANT_TASK_PRIORITY)
        warm = SCPublishTask(list_item_id=0, work_key="work:y", kind="first",
                             status="planned", priority=sc2.NEW_ENTRY_TASK_PRIORITY)
        cold = SCPublishTask(list_item_id=0, work_key="work:z", kind="first",
                             status="planned", priority=0,
                             scheduled_at=datetime.now(timezone.utc) - timedelta(days=30))
        resolver = sc2._RepResolver(db)
        # 插队任务（900 / 800）即使策略是「存量排期优先」也必须拿到 base，
        # 否则按 scheduled_at 排序会把刚插队的任务洗到队尾。
        for task, pid in ((hot, 3), (warm, 2)):
            task.id = pid
            key = sc2._conflict_sort_key(db, task, policy, resolver, cutoff)
            assert key == (-int(task.priority), pid)
        # 对照：存量任务在该策略下仍按 scheduled_at 排序（前缀是时间戳），
        # 证明短路只对插队任务生效，没有把策略整体废掉。
        cold.id = 1
        cold_key = sc2._conflict_sort_key(db, cold, policy, resolver, cutoff)
        assert len(cold_key) == 3 and isinstance(cold_key[0], float)
        assert (sc2._conflict_sort_key(db, hot, policy, resolver, cutoff)
                < cold_key)


# ---------------------------------------------------------------------------
# ⑤ 追更：有更新可重复推，但复用链接、且要在三个卡片里看得见
# ---------------------------------------------------------------------------

def _published_task(db, work_key, share_url="https://pan.quark.cn/s/keepme"):
    lst = SCPushList(list_key=f"h:{work_key}", name="h", status="active", source="plan")
    db.add(lst)
    db.flush()
    item = SCPushListItem(list_id=lst.id, work_key=work_key, category_key=CAT,
                          dedup_key=f"first:{work_key}", source_kind="first",
                          status="done")
    db.add(item)
    db.flush()
    task = SCPublishTask(list_item_id=item.id, work_key=work_key, kind="first",
                         status="published", priority=0, share_url=share_url,
                         link_state="active",
                         published_at=datetime.now(timezone.utc))
    db.add(task)
    db.flush()
    return task


def test_register_update_task_records_without_rebuilding_link():
    with SessionLocal() as db:
        _plan(db)
        wk, res = _work(db, title="追更作品", file_id="f-upd", entry_days_ago=200)
        prior = _published_task(db, wk)
        ch = db.scalar(select(Channel))
        db.commit()

        task = sc2.register_update_task(db, res, channel_id=ch.id, event_id=7)
        assert task is not None
        assert task.kind == "update"
        assert task.status == "submitted"       # 只记账，不落 planned（否则会双推）
        assert task.share_url == prior.share_url   # 复用链接，不重复建
        assert task.link_state == "active"
        assert task.priority == 0               # 不占首推名额、也不插队
        item = db.get(SCPushListItem, task.list_item_id)
        assert item.source_kind == "update"
        assert item.dedup_key == f"update:{wk}:{ch.id}:7"


def test_register_update_task_is_idempotent_per_event():
    with SessionLocal() as db:
        _plan(db)
        wk, res = _work(db, title="重复消费", file_id="f-idem", entry_days_ago=200)
        _published_task(db, wk)
        ch = db.scalar(select(Channel))
        db.commit()

        first = sc2.register_update_task(db, res, channel_id=ch.id, event_id=3)
        again = sc2.register_update_task(db, res, channel_id=ch.id, event_id=3)
        assert first is not None and again is None
        assert len([t for t in _tasks(db) if t.kind == "update"]) == 1


def test_update_task_visible_in_queue_and_task_list():
    """用户口径：追更必须在推送队列 / 推送任务里体现。"""
    with SessionLocal() as db:
        _plan(db)
        wk, res = _work(db, title="看得见", file_id="f-vis", entry_days_ago=200)
        _published_task(db, wk)
        ch = db.scalar(select(Channel))
        db.commit()
        sc2.register_update_task(db, res, channel_id=ch.id, event_id=1)

        views = sc2._aggregate_views(db)
        assert [r["work_key"] for r in views["queue"]] == [wk]

        payload = sc2.api_tasks(db=db)
        assert [r["work_key"] for r in payload["items"] if r["kind"] == "update"] == [wk]


def test_forecast_reports_update_task_through_queue_merge():
    """今日推送排期面板顶部的档位说明依赖 forecast 的 instant_mode / instant_works。"""
    with SessionLocal() as db:
        _plan(db)
        _set_rules(db, enabled="true", policy="prefer_release")
        wk, _ = _work(db, title="排期里的新片", file_id="f-fc",
                      release_date=_rel(1), entry_days_ago=200)
        _bind(db, wk)
        db.commit()

        fc = sc2.forecast_schedule(db)
        assert fc["instant_mode"] == "prefer_release"
        assert fc["instant_works"] >= 1
        cats = [it.get("category") for h in fc["hours"] for it in (h.get("items") or [])]
        assert "上新即时推送" in cats


def test_forecast_reports_off_when_switch_disabled():
    with SessionLocal() as db:
        _plan(db)
        _set_rules(db, enabled="false", policy="prefer_release")
        wk, _ = _work(db, title="不插队新片", file_id="f-fc2",
                      release_date=_rel(1), entry_days_ago=200)
        _bind(db, wk)
        db.commit()

        fc = sc2.forecast_schedule(db)
        assert fc["instant_mode"] == "off"
        assert fc["instant_works"] == 0


# ---------------------------------------------------------------------------
# ⑥ dispatch 必须排除追更任务（否则手动重试会把追更当首推再建一次链）
# ---------------------------------------------------------------------------

def test_dispatch_skips_update_tasks(monkeypatch):
    with SessionLocal() as db:
        _plan(db)
        _set_rules(db, enabled="true", policy="prefer_release", hourly="0")
        wk, res = _work(db, title="追更要跳过", file_id="f-dsp", entry_days_ago=200)
        _published_task(db, wk)
        ch = db.scalar(select(Channel))
        db.commit()

        # 造两条同优先级的待发任务：一条普通首推、一条被手动重置成 planned 的追更。
        wk_first, _res_first = _work(db, title="普通首推", file_id="f-plain",
                                     entry_days_ago=200)
        lv_first = db.scalar(select(SCLibraryVersion).where(
            SCLibraryVersion.work_key == wk_first))
        lv_upd = db.scalar(select(SCLibraryVersion).where(
            SCLibraryVersion.work_key == wk))
        lst = SCPushList(list_key="d:1", name="d", status="active", source="plan")
        db.add(lst)
        db.flush()
        item_first = SCPushListItem(list_id=lst.id, work_key=wk_first,
                                    library_version_id=lv_first.id,
                                    category_key=CAT, dedup_key="d:first",
                                    source_kind="first", status="planned")
        item_upd = SCPushListItem(list_id=lst.id, work_key=wk,
                                  library_version_id=lv_upd.id,
                                  category_key=CAT, dedup_key="d:update",
                                  source_kind="update", status="planned")
        db.add_all([item_first, item_upd])
        db.flush()
        t_first = SCPublishTask(list_item_id=item_first.id, work_key=wk_first,
                                kind="first", status="planned", priority=0,
                                share_url="https://pan.quark.cn/s/first",
                                link_state="active")
        t_upd = SCPublishTask(list_item_id=item_upd.id, work_key=wk,
                              kind="update", status="planned", priority=0,
                              share_url="https://pan.quark.cn/s/upd",
                              link_state="active")
        db.add_all([t_first, t_upd])
        db.commit()

        gated: list[str] = []
        monkeypatch.setattr(sc2, "evaluate_push_gate",
                            lambda *a, **k: (gated.append("x") or
                                             {"passed": True, "reason": ""}))

        asyncio.run(sc2.dispatch_ready_tasks(db, limit=10))
        db.expire_all()

        assert db.get(SCPublishTask, t_upd.id).status == "planned", \
            "追更任务不该被 dispatch 处理（它只记账，投递由更新事件的队列项完成）"
        # 普通首推正常走完建链复用 → 提交（ready 或已提交到队列，取决于单轮进度）
        assert db.get(SCPublishTask, t_first.id).status in {"ready", "submitted"}
        # 门禁只对普通首推调用了一次，追更没有被当成首推走建链流程
        assert len(gated) == 1
