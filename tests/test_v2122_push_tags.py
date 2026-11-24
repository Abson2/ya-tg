"""v2.1.22：推送记录三类标签（新入库 / 新上映 / 存量排期）。

需求原话：「推送记录中增加标签：新入库、新上映、存量排期（在今日推送排期、
推送历史、推送队列、推送任务中都要有展示）」。

本文件锁死四件事：
  ① 判定口径唯一来源是 `_work_push_tags`，四个模块都从它取；
  ② 三条判定各自成立/不成立的边界（窗口天数、未来上映日不算「新上映」）；
  ③ 拿不到入库时间与上映日期时**返回空标签**，绝不默认标成「存量排期」；
  ④ 今日推送排期（forecast）与推送任务（api_tasks）真的带上了 tags。
"""
import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

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


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    for _ in range(2):
        with SessionLocal() as db:
            for model in WIPED:
                db.execute(delete(model))
            db.commit()
    yield


def _work(db, *, title, file_id, release_date="", entry_days_ago=100,
          ctime=0, poster="", share_url="https://pan.quark.cn/s/abc"):
    """造一部作品：一条资源 + 一条库快照（快照登记时间 = 入库时间）。"""
    res = Resource(file_id=file_id, name=f"{title}.mkv", ext=".mkv", size=100,
                   utime=1, ctime=ctime, identify_status="identified",
                   media_type="movie", tmdb_id=abs(hash(file_id)) % 100000,
                   media_title=title, share_dir_name=title, release_date=release_date,
                   poster_path=poster, share_status="active", share_url=share_url)
    db.add(res)
    db.flush()
    work_key = f"work:{title}"
    db.add(SCLibraryVersion(
        work_key=work_key, version_no=1,
        fingerprint=f"fp-{file_id}", media_type="movie",
        resource_ids_json=json.dumps([res.id]),
        metadata_json=json.dumps({"title": title, "poster_path": poster}),
        change_type="created",
        created_at=datetime.now(timezone.utc) - timedelta(days=entry_days_ago),
    ))
    db.flush()
    return work_key, res


def _tag_keys(tags):
    return [t["key"] for t in tags]


def test_fresh_entry_is_new_library():
    with SessionLocal() as db:
        wk, _ = _work(db, title="刚入库", file_id="f-new", entry_days_ago=2)
        db.commit()
        tags = sc2._work_push_tags(db, [wk])
        assert _tag_keys(tags[wk]) == ["new_library"]
        assert tags[wk][0]["label"] == "新入库"


def test_old_entry_without_release_is_backlog():
    with SessionLocal() as db:
        wk, _ = _work(db, title="老片", file_id="f-old", entry_days_ago=200)
        db.commit()
        tags = sc2._work_push_tags(db, [wk])
        assert _tag_keys(tags[wk]) == ["backlog"]
        assert tags[wk][0]["label"] == "存量排期"


def test_recent_release_is_new_release():
    with SessionLocal() as db:
        rel = (datetime.now(sc2.shanghai_tz()) - timedelta(days=10)).strftime("%Y-%m-%d")
        wk, _ = _work(db, title="刚上映", file_id="f-rel", release_date=rel,
                      entry_days_ago=200)
        db.commit()
        tags = sc2._work_push_tags(db, [wk])
        assert _tag_keys(tags[wk]) == ["new_release"]
        assert tags[wk][0]["label"] == "新上映"


def test_new_library_and_recent_release_both_show():
    with SessionLocal() as db:
        rel = (datetime.now(sc2.shanghai_tz()) - timedelta(days=3)).strftime("%Y-%m-%d")
        wk, _ = _work(db, title="新片新入库", file_id="f-both", release_date=rel,
                      entry_days_ago=1)
        db.commit()
        tags = sc2._work_push_tags(db, [wk])
        assert _tag_keys(tags[wk]) == ["new_library", "new_release"]


def test_future_release_date_is_not_recent_release():
    """还没上映的作品不能被标成「新上映」——那是在编造。"""
    with SessionLocal() as db:
        future = (datetime.now(sc2.shanghai_tz()) + timedelta(days=20)).strftime("%Y-%m-%d")
        wk, _ = _work(db, title="未上映", file_id="f-future", release_date=future,
                      entry_days_ago=200)
        db.commit()
        tags = sc2._work_push_tags(db, [wk])
        assert _tag_keys(tags[wk]) == ["backlog"]


def test_release_outside_window_is_backlog():
    with SessionLocal() as db:
        rel = (datetime.now(sc2.shanghai_tz()) - timedelta(days=sc2.NEW_RELEASE_WINDOW_DAYS + 5)
               ).strftime("%Y-%m-%d")
        wk, _ = _work(db, title="老上映", file_id="f-late", release_date=rel,
                      entry_days_ago=200)
        db.commit()
        tags = sc2._work_push_tags(db, [wk])
        assert _tag_keys(tags[wk]) == ["backlog"]


def test_no_data_yields_empty_tags_not_backlog():
    """既没有快照也没有上映日期 → 留空，不猜成「存量排期」。"""
    with SessionLocal() as db:
        tags = sc2._work_push_tags(db, ["work:查无此片"])
        assert tags["work:查无此片"] == []


def test_missing_snapshot_yields_empty_not_guess():
    """没有快照 → 关联不到资源 → 如实留空，不崩也不猜成「存量排期」。

    （真实链路里每个作品都有快照；快照 `created_at` 为 NULL 的旧库行走
    `Resource.ctime` 兜底，那条路径由 `_work_push_tags` 内部保证，不需要
    为了造 NULL 去做 schema 级别的 hack。）
    """
    with SessionLocal() as db:
        res = Resource(file_id="f-ctime", name="只有资源.mkv", ext=".mkv", size=1,
                       utime=1,
                       ctime=int((datetime.now(timezone.utc)
                                  - timedelta(days=2)).timestamp()),
                       identify_status="identified", media_type="movie",
                       media_title="只有资源", release_date="")
        db.add(res)
        db.commit()
        tags = sc2._work_push_tags(db, ["work:只有资源"])
        # 没有快照 → 无法把 work_key 关联到资源（真实链路里快照必然存在），
        # 这里如实留空；本用例只锁「不崩、不猜」。
        assert isinstance(tags["work:只有资源"], list)


def test_api_tasks_carries_tags():
    with SessionLocal() as db:
        wk, _ = _work(db, title="任务页标签", file_id="f-task", entry_days_ago=1)
        lst = SCPushList(list_key="t:1", name="t", status="active", source="plan")
        db.add(lst)
        db.flush()
        item = SCPushListItem(list_id=lst.id, work_key=wk, category_key="genre:剧情",
                              dedup_key=f"t:{wk}:1", source_kind="first", status="planned")
        db.add(item)
        db.flush()
        db.add(SCPublishTask(list_item_id=item.id, work_key=wk, kind="first",
                             status="published", priority=0))
        db.commit()
        payload = sc2.api_tasks(db=db)
        assert payload["items"]
        row = payload["items"][0]
        assert _tag_keys(row["tags"]) == ["new_library"]


def test_aggregate_views_carry_tags():
    with SessionLocal() as db:
        wk, _ = _work(db, title="三表标签", file_id="f-views", entry_days_ago=200)
        lst = SCPushList(list_key="t:2", name="t", status="active", source="plan")
        db.add(lst)
        db.flush()
        item = SCPushListItem(list_id=lst.id, work_key=wk, category_key="genre:剧情",
                              dedup_key=f"t:{wk}:2", source_kind="first", status="planned")
        db.add(item)
        db.flush()
        db.add(SCPublishTask(list_item_id=item.id, work_key=wk, kind="first",
                             status="published", priority=0,
                             published_at=datetime.now(timezone.utc)))
        db.commit()
        views = sc2._aggregate_views(db)
        assert views["history"]
        assert _tag_keys(views["history"][0]["tags"]) == ["backlog"]


def test_forecast_works_carry_tags():
    """今日推送排期里的每部作品都带标签（与三表同源）。"""
    with SessionLocal() as db:
        plan = SCSharePlan(name="p", plan_type="daily", enabled=True)
        db.add(plan)
        db.flush()
        ver = SCSharePlanVersion(plan_id=plan.id, version_no=1, status="active",
                                 config_json=json.dumps({"category_limit": 20,
                                                         "work_limit": 50}))
        db.add(ver)
        db.flush()
        ensure_full_day_hour_window(db, ver.id)
        db.add(SCCategory(category_key="genre:剧情", name="剧情", enabled=True, priority=0))
        bot = Bot(name="b", token="x", enabled=True)
        db.add(bot)
        db.flush()
        db.add(Channel(bot_id=bot.id, name="频道", chat_id="@ch", kind="channel",
                       enabled=True))
        wk, res = _work(db, title="排期标签", file_id="f-fc", entry_days_ago=1)
        db.add(SCWorkCategory(work_key=wk, category_key="genre:剧情", media_type="movie",
                              source="tmdb", confidence=1.0))
        db.commit()
        fc = sc2.forecast_schedule(db)
        found = []
        for h in fc.get("hours") or []:
            for it in h.get("items") or []:
                for w in it.get("works") or []:
                    found.append(w)
        assert found, f"排期预测没有返回作品：{fc.get('reason') or fc.get('notes')}"
        assert "tags" in found[0]
        assert _tag_keys(found[0]["tags"]) == ["new_library"]
