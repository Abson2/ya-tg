"""v2.1.20：推送排期 / 推送队列 / 推送任务三处口径必须一致。

现场问题两条：
  ① 「推送任务」「分享管理」里小气鬼没有缩略图 —— 根因在 `_work_posters`
     （最新快照海报为空挡住旧快照；`work:` 前缀不回查资源表），已在
     `test_v2120_work_posters.py` 锁死。
  ② 「推送队列」一条数据都没有 —— 因为它只取 `ACTIVE_TASK_STATUS` 的**已建任务**，
     而排期面板是内存预测（还没建任务）。两处本来就不是同一批数据，
     用户看到「排期有 10 部、队列 0 条」会以为口径打架。

本文件锁死：
  ① `_aggregate_views` 的三个视图（历史/队列/失败）都带 `cover`，不再一律「无封面」；
  ② `forecast_schedule` 的每个小时项带结构化 `works`（work_key/title/cover/time），
     队列才有东西可合并；
  ③ 同一部作品在「排期」与「任务」里的标题、封面**完全一致**（同源函数）；
  ④ 海报为空的作品在哪个模块都是空串，不编造。
"""
import json

import pytest
from sqlalchemy import delete

from app import share_center_v2 as sc2
from app import utils
from app.db import SessionLocal, migrate
from app.models import (
    Bot,
    Channel,
    Resource,
    SCCategory,
    SCCategoryCursor,
    SCCategorySchedule,
    SCLibraryVersion,
    SCPublishTask,
    SCPushList,
    SCPushListItem,
    SCSharePlan,
    SCSharePlanVersion,
    SCWorkCategory,
)

from conftest import ensure_full_day_hour_window

WIPED = (SCPublishTask, SCPushListItem, SCPushList, SCLibraryVersion,
         SCCategoryCursor, SCCategorySchedule, SCWorkCategory, SCCategory,
         SCSharePlanVersion, SCSharePlan, Resource, Channel, Bot)

POSTER = "/sJVh2U27BXwnfT05kpLOmo0ZUop.jpg"


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    for _ in range(2):
        with SessionLocal() as db:
            for model in WIPED:
                db.execute(delete(model))
            db.commit()
    yield


def _channel(db):
    bot = Bot(name="b", token="x", enabled=True)
    db.add(bot)
    db.flush()
    ch = Channel(bot_id=bot.id, name="频道", chat_id="-1001234567890",
                 kind="channel", enabled=True)
    db.add(ch)
    db.commit()
    return ch


def _plan(db):
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
    db.commit()
    return ver


def _work(db, *, title="小气鬼", file_id="f-1", poster=POSTER,
          latest_poster="", older_poster=POSTER):
    """造一部在库作品：资源 + 两版快照（旧有海报、新海报为空）+ 分类映射。

    这正是现场小气鬼的形态：最新快照 LV495 poster 空，旧快照 LV29 有。
    """
    res = Resource(file_id=file_id, name=f"{title}.S01E01.mkv", ext=".mkv",
                   size=100, utime=1, identify_status="identified", media_type="tv",
                   tmdb_id=1, media_title=title, share_dir_name=title,
                   share_scope="series_dir", share_status="not_created",
                   poster_path=poster)
    db.add(res)
    db.flush()
    wk = utils.work_group_key(res)
    db.add(SCLibraryVersion(work_key=wk, version_no=1, fingerprint="fp1",
                            media_type="tv", resource_ids_json=json.dumps([res.id]),
                            metadata_json=json.dumps(
                                {"title": title, "poster_path": older_poster},
                                ensure_ascii=False),
                            change_type="first"))
    db.add(SCLibraryVersion(work_key=wk, version_no=2, fingerprint="fp2",
                            media_type="tv", resource_ids_json=json.dumps([res.id]),
                            metadata_json=json.dumps(
                                {"title": f"{title}.S01E01.mkv",
                                 "poster_path": latest_poster},
                                ensure_ascii=False),
                            change_type="update"))
    db.add(SCWorkCategory(work_key=wk, category_key="genre:剧情"))
    db.commit()
    return res, wk


_seq = {"n": 0}


def _task(db, wk, *, status="planned", chat_id="-1001234567890"):
    _seq["n"] += 1
    lst = SCPushList(list_key=f"l-{_seq['n']}", name="t", status="active")
    db.add(lst)
    db.flush()
    item = SCPushListItem(list_id=lst.id, work_key=wk, chat_id=chat_id,
                          dedup_key=f"{wk}:{chat_id}:{_seq['n']}", status="planned")
    db.add(item)
    db.flush()
    t = SCPublishTask(list_item_id=item.id, work_key=wk, kind="first",
                      status=status, link_state="unknown")
    db.add(t)
    db.commit()
    return t


def _views():
    with SessionLocal() as db:
        return sc2._aggregate_views(db, limit=12)


def _forecast():
    with SessionLocal() as db:
        return sc2.forecast_schedule(db)


# --------------------------------------------------------------------------
# ① 三个视图都带 cover（修复前三表一律「无封面」）
# --------------------------------------------------------------------------
def test_all_three_views_carry_cover():
    with SessionLocal() as db:
        _plan(db)
        _channel(db)
        _res, wk = _work(db)
        _task(db, wk, status="planned")
        _task(db, wk, status="published")
        _task(db, wk, status="failed")

    v = _views()
    for name in ("history", "queue", "failed"):
        assert v[name], f"{name} 应有行"
        for row in v[name]:
            assert "cover" in row, f"{name} 行缺 cover 字段（前端一律显示无封面）"
            assert row["cover"] == POSTER, f"{name} 行封面应取到 {POSTER}"


def test_cover_is_empty_string_when_truly_missing():
    with SessionLocal() as db:
        _plan(db)
        _channel(db)
        _res, wk = _work(db, title="无海报剧", file_id="f-9", poster="",
                         latest_poster="", older_poster="")
        _task(db, wk, status="planned")
    row = _views()["queue"][0]
    assert row["cover"] == "", "取不到海报就是空串，不编造"


# --------------------------------------------------------------------------
# ② forecast 的小时项带结构化 works（队列合并的数据基础）
# --------------------------------------------------------------------------
def test_forecast_items_expose_structured_works():
    with SessionLocal() as db:
        _plan(db)
        _channel(db)
        _res, wk = _work(db)
    out = _forecast()
    items = [it for h in out["hours"] for it in h["items"]]
    assert items, "至少应有一个小时项"
    works = [w for it in items for w in it.get("works", [])]
    assert works, "小时项必须带结构化 works，否则队列没东西可合并"
    w0 = works[0]
    for field in ("work_key", "title", "cover", "time"):
        assert field in w0, f"works 项缺 {field}"
    assert w0["work_key"] == wk
    assert w0["cover"] == POSTER, "排期项的封面要与资源库同源"
    assert w0["title"], "排期项必须有可读标题"
    assert len(w0["time"]) == 5 and w0["time"][2] == ":", f"时间点形态异常：{w0['time']}"


def test_forecast_works_keep_compat_titles_field():
    """`titles` 是旧字段，前端排期面板还在用，不能因为加 works 就删掉。"""
    with SessionLocal() as db:
        _plan(db)
        _channel(db)
        _work(db)
    items = [it for h in _forecast()["hours"] for it in h["items"]]
    assert items and items[0]["titles"], "titles 字段必须保留"


# --------------------------------------------------------------------------
# ③ 同一套口径：排期 / 队列 / 分享记录对同一作品给出同样的标题与封面
# --------------------------------------------------------------------------
def test_forecast_works_match_canonical_title_and_cover():
    """排期项必须与 `_work_titles` / `_work_posters` 完全一致（同源，不另算一套）。"""
    with SessionLocal() as db:
        _plan(db)
        _channel(db)
        _res, wk = _work(db)
    works = [w for h in _forecast()["hours"] for it in h["items"]
             for w in it.get("works", [])]
    assert works, "排期应有该作品"
    w0 = works[0]
    with SessionLocal() as db:
        title = sc2._work_titles(db, [wk])[wk]
        cover = sc2._work_posters(db, [wk])[wk]
    assert w0["title"] == title, f"标题不同源：{w0['title']!r} vs {title!r}"
    assert w0["cover"] == cover, f"封面不同源：{w0['cover']!r} vs {cover!r}"


def test_queue_rows_match_canonical_title_and_cover():
    with SessionLocal() as db:
        _plan(db)
        _channel(db)
        _res, wk = _work(db)
        _task(db, wk, status="planned")
    row = _views()["queue"][0]
    with SessionLocal() as db:
        title = sc2._work_titles(db, [wk])[wk]
        cover = sc2._work_posters(db, [wk])[wk]
    assert row["title"] == title
    assert row["cover"] == cover


def test_three_modules_agree_on_one_work(monkeypatch):
    """同一部作品：排期项与任务行（同一 work_key）给出同一个标题与封面。

    排期里出现该作品的唯一前提是**还没有在途任务**，所以这里分两次快照：
    先取排期结果，再建任务取任务行，比对两者。
    """
    with SessionLocal() as db:
        _plan(db)
        _channel(db)
        _res, wk = _work(db)
    plan_works = [w for h in _forecast()["hours"] for it in h["items"]
                  for w in it.get("works", [])]
    assert plan_works, "排期应有该作品"
    with SessionLocal() as db:
        _task(db, wk, status="planned")
    task_row = _views()["queue"][0]

    assert plan_works[0]["work_key"] == task_row["work_key"] == wk
    assert plan_works[0]["title"] == task_row["title"], \
        f"同一作品标题不一致：排期 {plan_works[0]['title']!r} vs 任务 {task_row['title']!r}"
    assert plan_works[0]["cover"] == task_row["cover"], \
        f"同一作品封面不一致：排期 {plan_works[0]['cover']!r} vs 任务 {task_row['cover']!r}"


# --------------------------------------------------------------------------
# ④ 队列不再为空：排期里有作品时，前端合并后必然有行
# --------------------------------------------------------------------------
def test_queue_has_plan_items_even_when_no_task_exists():
    """没有在途任务时，排期里有作品 → 前端合并后队列非空。"""
    with SessionLocal() as db:
        _plan(db)
        _channel(db)
        _work(db, title="待排期剧")
    assert _views()["queue"] == [], "此时确实没有在途任务（这就是用户看到的空）"
    works = [w for h in _forecast()["hours"] for it in h["items"]
             for w in it.get("works", [])]
    assert works, "但排期里有作品 —— 前端把它并入队列后就不空"
    assert works[0]["title"] and works[0]["cover"]
