"""v2.1.25：刚入库被当追更（存量模式还推）+ 分类与推送类型混在一列 + 海报两处不一致。

三件事的现场证据与判定层/通道层两道防线：

① **刚入库被当追更、存量排期还推送**
   - 触发源：扫描用 `size:utime` 当内容指纹，utime 抖动 96 秒就判「内容更新」
     （真机全量 252 条更新事件里 141 条是这种假追更，真连载剧只有 2 条）；
   - 通道层：追更走 `process_change_events` 复活队列项的独立通道，**不经过 dispatch**，
     所以 `conflict_policy` 的排序与门禁都管不到它 —— 存量模式下「说不推新片」，
     追更却照推。
   本文件锁死：① 指纹只看 size，utime 单变不产生事件（反证：改 size 仍必须产生）；
              ② `prefer_backlog` 下追更全部拦截、不建任务不记账、事件不重复消费；
              ③ 其余两档追更照旧（positive control，别把闸门做成永远关）。

② **队列「分类」列既有追更又有 TMDB 分类**
   前端把 `kind`（推送类型）和 `category_name`（TMDB 分类）拼在同一格
   （`index-*.js:1149888`），不是数据错。本文件锁死：后端两处 API 各自吐
   `kind` 与 `category_name` 两个字段；前端「推送类型」「分类」两列独立，
   旧的混合表达式不得回归；排期里的「上新即时推送」是通道，标 `kind=instant`。

③ **银河护卫队2 资源库与分享中心海报不同**
   资源库取当前资源行 `/1vN…`；分享中心 `_work_posters` 按版本倒序取第一个非空，
   命中旧快照的 `/9RT…` 就停，从没用最新快照登记的当前资源。本文件锁死：
   实时资源优先、取不到才退快照（反证：旧顺序会返回旧快照的值）。
"""
import json

import pytest
from sqlalchemy import delete, select

from app import main
from app import runtime_config
from app import share_center_v2 as sc2
from app import utils
from app.db import SessionLocal, migrate
from app.models import (
    AppConfig,
    Bot,
    Channel,
    DistributionEvent,
    QueueItem,
    Resource,
    ResourceVersion,
    Rule,
    SCCategory,
    SCLibraryVersion,
    SCPublishTask,
    SCPushList,
    SCPushListItem,
    SCWorkCategory,
    ShareTask,
)
from app.scanner import _record_change_events, _upsert_resource

WIPED = (DistributionEvent, ResourceVersion, SCPublishTask, SCPushListItem,
         SCPushList, SCLibraryVersion, SCWorkCategory, SCCategory,
         QueueItem, ShareTask, Rule, Resource, Channel, Bot, AppConfig)

POSTER = "/1vNwQdM3nKk9oSXb0lT4uZ.jpg"      # 资源库当前资源行的海报
STALE = "/9RToldSnapshotPoster.jpg"           # 旧快照里登记的海报


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    for _ in range(2):
        with SessionLocal() as db:
            for model in WIPED:
                db.execute(delete(model))
            db.commit()
    yield


# ==========================================================================
# ① 扫描指纹：utime 单变不算更新
# ==========================================================================
def _item(*, fid="f-1", size=100, utime=1000, name="剧.S01E01.mkv"):
    return {"fileId": fid, "fileName": name, "fileSize": size, "utime": utime,
            "fileType": 0, "thumbnail": ""}


def test_utime_only_jitter_is_not_an_update():
    """utime 抖动（size 不变）不得判成内容更新 —— 141/252 条假追更的根因。"""
    with SessionLocal() as db:
        _upsert_resource(db, _item(size=100, utime=1000), "dir-1")
        db.commit()
        row, created, change = _upsert_resource(db, _item(size=100, utime=1096), "dir-1")
        db.commit()
        assert created is False
        assert change is None, (
            "utime 从 1000 抖到 1096（+96 秒）而 size 未变 → 必须判为「没变化」；"
            "判成变化就会生成假追更事件，把刚入库的作品又推一遍")
        assert int(row.utime) == 1096, "utime 本身仍要刷新，只是不参与「是否变化」判定"
        assert int(row.revision or 0) == 0, "未变不得累加 revision"


def test_size_change_still_produces_exactly_one_event():
    """反证：真实内容变化（size 变）必须仍被识别，且按作品合并成一条事件。"""
    with SessionLocal() as db:
        res, _created, _ = _upsert_resource(db, _item(size=100, utime=1000), "dir-1")
        db.commit()
        _res, created, change = _upsert_resource(
            db, _item(size=200, utime=1000), "dir-1")
        db.commit()
        assert created is False
        assert change is not None, "size 变了却不算更新 = 漏更，比多更更严重"
        assert change["new"] == "200", f"指纹必须是纯 size，实际 {change['new']!r}"
        assert _record_change_events(db, [change]) == 1
        db.commit()
        events = list(db.scalars(select(DistributionEvent)).all())
        assert len(events) == 1
        assert events[0].event_type == "updated"


def test_fingerprint_has_no_utime_component():
    """锁死口径：指纹里不许再出现 utime（防止将来「顺手加回去」）。"""
    with SessionLocal() as db:
        _upsert_resource(db, _item(size=100, utime=1000), "dir-1")
        db.commit()
        _res, _c, change = _upsert_resource(db, _item(size=300, utime=9999), "dir-1")
        db.commit()
        assert ":" not in str(change["new"]), "size:utime 形态已废弃"
        assert "9999" not in str(change["new"])


# ==========================================================================
# ② 存量优先档：追更整条通道拦截
# ==========================================================================
def _channel(db):
    bot = Bot(name="b", token="x", enabled=True)
    db.add(bot)
    db.flush()
    ch = Channel(bot_id=bot.id, name="频道", chat_id="-1001234567890",
                 kind="channel", enabled=True)
    db.add(ch)
    db.commit()
    return ch


def _work_with_update(db, *, title="银河护卫队2", file_id="f-gotg2"):
    """造一部「已推过、有分享链接、有一条待处理更新事件」的作品。"""
    res = Resource(file_id=file_id, name=f"{title}.S01E01.mkv", ext=".mkv",
                   size=100, utime=1, identify_status="identified", media_type="movie",
                   tmdb_id=1, media_title=title, share_dir_name=title,
                   share_scope="series_dir", share_status="created",
                   share_url="https://share/stale", poster_path=POSTER)
    db.add(res)
    db.flush()
    wk = utils.work_group_key(res)
    db.add(SCLibraryVersion(
        work_key=wk, version_no=1, fingerprint="fp-1", media_type="movie",
        resource_ids_json=json.dumps([res.id]),
        metadata_json=json.dumps({"title": title, "poster_path": POSTER},
                                 ensure_ascii=False),
        change_type="created"))
    db.add(SCWorkCategory(work_key=wk, category_key="genre:动作"))
    db.add(SCCategory(category_key="genre:动作", name="动作", enabled=True, priority=0))
    ch = _channel(db)
    q = QueueItem(resource_id=res.id, channel_id=ch.id, status="sent",
                  telegram_message_id="m-1")
    db.add(q)
    ev = DistributionEvent(work_key=wk, event_type="updated", fingerprint="digest-1",
                           changed_files=1, queue_id=0)
    db.add(ev)
    db.commit()
    return res, wk, ch, q, ev


def test_backlog_policy_blocks_all_update_pushes():
    """存量排期优先 → 追更全部拦截：不复活队列项、不建 v2 追更任务、不记账。"""
    with SessionLocal() as db:
        _res, wk, _ch, q, ev = _work_with_update(db)
        runtime_config.set_many(db, {"sc_conflict_policy": "prefer_backlog"})
        db.commit()
        qid, evid = q.id, ev.id

        stats = main.process_change_events(db, publish=True)
        db.commit()

        assert stats["blocked"] == 1, f"应拦下 1 条，实际 {stats}"
        assert stats["queued"] == 0, "阻断档下不得把队列项复活成待发送"
        q2 = db.get(QueueItem, qid)
        assert q2.status == "sent", "队列项必须原样保留（不能偷偷改成 pending 再发一次）"
        assert q2.sent_at is None and q2.attempts == 0, "队列项不得被重置"
        tasks = list(db.scalars(select(SCPublishTask)).all())
        assert not [t for t in tasks if t.kind == "update"], (
            "阻断档下不得登记追更任务（推送任务 / 队列 / 历史三张卡片都不该多出这条）")
        ev2 = db.get(DistributionEvent, evid)
        assert ev2.queue_id == main.DIST_EVENT_BLOCKED, "被拦事件要打标记，避免每轮重算"
        assert not ev2.share_task_id, "不得挂任何任务号"


def test_blocked_event_is_not_reprocessed():
    """被拦事件不允许反复被捞起（否则每轮扫描都白算一遍，且日志刷屏）。"""
    with SessionLocal() as db:
        _work_with_update(db)
        runtime_config.set_many(db, {"sc_conflict_policy": "prefer_backlog"})
        db.commit()
        assert main.process_change_events(db)["blocked"] == 1
        db.commit()
        again = main.process_change_events(db)
        assert again["events"] == 0, "已拦事件仍是 queue_id=0 → 会被无限重复处理"
        assert again["blocked"] == 0


def test_update_push_still_works_under_other_policies():
    """反证：另两档（新上映优先 / 新入库优先）追更必须照旧即时推。

    闸门若做成「永远关」，存量作品就再也不会随更新重推 —— 那是另一个方向的缺陷。
    """
    for policy in ("prefer_release", "prefer_new"):
        with SessionLocal() as db:
            for model in WIPED:
                db.execute(delete(model))
            db.commit()
            _res, wk, _ch, _q, _ev = _work_with_update(db)
            runtime_config.set_many(db, {"sc_conflict_policy": policy})
            db.commit()

            stats = main.process_change_events(db, publish=True)
            db.commit()
            assert stats["blocked"] == 0, f"{policy} 档不该拦追更：{stats}"
            assert stats["queued"] == 1, f"{policy} 档追更应进队列：{stats}"
            updates = [t for t in db.scalars(select(SCPublishTask)).all()
                       if t.kind == "update"]
            assert updates, f"{policy} 档应登记一条追更任务（三个卡片才看得见）"
            assert str(updates[0].share_url or "").endswith("/stale"), (
                "追更必须复用原分享链接，不得重建")


def test_suppress_helper_only_fires_for_backlog():
    """判定层单点：只有 prefer_backlog（含旧别名 backlog_first）算「停用追更」。"""
    assert callable(getattr(sc2, "update_push_suppressed", None))
    with SessionLocal() as db:
        for raw, expected in (("prefer_release", False), ("prefer_new", False),
                              ("prefer_backlog", True), ("backlog_first", True),
                              ("", False), ("prefer_new_entry", False)):
            runtime_config.set_many(db, {"sc_conflict_policy": raw})
            db.commit()
            assert sc2.update_push_suppressed(db) is expected, f"{raw!r} 判定错误"
            # 与「新片插队」的开关口径必须一致：存量档下两者都为「关」
            if expected:
                assert sc2.instant_push_mode(db) == "off"


# ==========================================================================
# ③ 分类 / 推送类型：两个字段、两列
# ==========================================================================
def _list_item_task(db, wk, *, kind="first", status="planned",
                    category_key="genre:动作"):
    lst = SCPushList(list_key=f"l-{kind}-{category_key}", name="t", status="active")
    db.add(lst)
    db.flush()
    item = SCPushListItem(list_id=lst.id, work_key=wk, chat_id="-1001234567890",
                          dedup_key=f"{wk}:{kind}:{category_key}",
                          category_key=category_key, status="planned")
    db.add(item)
    db.flush()
    task = SCPublishTask(list_item_id=item.id, work_key=wk, kind=kind,
                         status=status, link_state="unknown")
    db.add(task)
    db.commit()
    return task


def test_views_rows_carry_tmdb_category_separately_from_kind():
    """三视图每行同时给 kind（推送类型）与 category_name（TMDB 分类），互不顶替。"""
    with SessionLocal() as db:
        res, wk, _ch, _q, _ev = _work_with_update(db, title="小气鬼", file_id="f-x")
        _list_item_task(db, wk, kind="update", status="planned",
                        category_key="genre:动作")
        _list_item_task(db, wk, kind="first", status="published",
                        category_key="genre:动作")

    with SessionLocal() as db:
        views = sc2._aggregate_views(db)
    for name in ("history", "queue", "failed"):
        for row in views[name]:
            assert "category_name" in row and "category_key" in row, \
                f"{name} 行缺分类字段，前端就只能拿 kind 去冒充"
            assert row["category_name"] == "动作", f"{name} 分类应取 TMDB 名称"
            assert row["kind"] in ("first", "update", "force")
            assert row["category_name"] != "追更", "分类里出现推送类型 = 又混了"


def test_tasks_api_exposes_category_name():
    with SessionLocal() as db:
        res, wk, _ch, _q, _ev = _work_with_update(db, title="小气鬼", file_id="f-y")
        _list_item_task(db, wk, kind="first", status="planned")
        payload = sc2.api_tasks(db=db)
    assert payload["items"], "应有一条推送任务"
    row = payload["items"][0]
    assert row["kind"] == "first"
    assert row["category_name"] == "动作", "推送任务列表也要给 TMDB 分类"
    with SessionLocal() as db:
        detail = sc2.api_task_detail(int(row["id"]), db=db)
    assert detail["item"]["category_name"] == "动作", "详情页同口径"


def _plan(db):
    """建一个活动计划版本 + 全天窗口，让 forecast 真的能排出东西（否则用例空转）。"""
    from app.models import SCSharePlan, SCSharePlanVersion

    from conftest import ensure_full_day_hour_window
    plan = SCSharePlan(name="p", plan_type="daily", enabled=True)
    db.add(plan)
    db.flush()
    ver = SCSharePlanVersion(plan_id=plan.id, version_no=1, status="active",
                             config_json=json.dumps({"category_limit": 20,
                                                     "work_limit": 50}))
    db.add(ver)
    db.flush()
    ensure_full_day_hour_window(db, ver.id)
    db.commit()
    return ver


def test_forecast_works_carry_own_category_and_instant_kind():
    """排期 works 带作品自己的分类；「上新即时推送」是通道，标 kind=instant。"""
    with SessionLocal() as db:
        _plan(db)
        _res, wk, _ch, _q, _ev = _work_with_update(db, title="黑豹2", file_id="f-bp2")
        out = sc2.forecast_schedule(db)
    items = [it for h in out["hours"] for it in h["items"]]
    works = [w for it in items for w in it.get("works", [])]
    assert works, "排期必须真的排出作品，否则本用例空转（前提失效要立刻发现）"
    for it in items:
        assert it.get("kind") in ("category", "instant"), "小时项必须标明是分类还是插队通道"
        if it["kind"] == "category":
            assert it.get("category_key"), "分类行必须给出分类键，否则无法与 TMDB 对齐"
    for w in works:
        assert "category" in w, "作品项要带自己的 TMDB 分类，队列待排期行才不用猜"
        assert w["category"] == "动作", f"分类应取 TMDB 名称，实际 {w['category']!r}"


def test_frontend_queue_has_two_columns_not_one_mixed():
    """前端：推送类型与分类分列；旧的混合表达式不得回归（反证）。"""
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    app = (root / "frontend/src/App.vue").read_text(encoding="utf-8")

    assert "label=\"推送类型\"" in app, "队列/历史/失败/任务都要有「推送类型」列"
    assert "function scQueueKindLabel" in app, "推送类型要收敛成一个函数"
    assert "function scQueueCategoryLabel" in app, "分类要收敛成一个函数"

    # 反证：旧的「一列兼两种含义」写法不得回来
    assert "s.row._src === 'plan' ? (s.row.category || '未分类') : scKindLabel(s.row.kind, true)" not in app, \
        "待排期行显示分类、在途行显示 kind —— 这就是用户看到「分类列里有追更」的原因"
    assert "s.row.kind==='force' ? '手动推送' : (s.row.kind==='first' ? '电影/剧' : '追更')" not in app, \
        "分享管理里把「电影/剧」（媒体形态）当推送类型显示，属于第三套口径"

    # 分类列不许渲染 kind
    for block in app.split('label="分类"')[1:]:
        head = block[:400]
        assert "scKindLabel" not in head, "「分类」列不得再渲染推送类型"


def test_frontend_plan_rows_use_work_category_not_channel_name():
    """待排期行的分类取作品自己的 TMDB 分类，不许拿「上新即时推送」充数。"""
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    app = (root / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "category: w.category || ''" in app, "plan 行应取 works[].category"
    assert "category: it.category || ''" not in app, (
        "旧写法直接把通道名（上新即时推送）当分类塞给 plan 行")


def test_forecast_instant_chip_is_visually_distinct():
    """排期表把插队批标成「插队」，不再让它看起来像一个题材分类。"""
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    app = (root / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "it.kind === 'instant'" in app
    assert ".sc-fc-cat.is-src" in app, "插队批要有独立配色，否则和分类同色仍会误读"


# ==========================================================================
# ④ 海报：资源库实时值优先
# ==========================================================================
def test_live_resource_poster_beats_stale_snapshot():
    """银河护卫队2 现场：旧快照有 /9RT…，当前资源行是 /1vN… → 必须显示 /1vN…。"""
    with SessionLocal() as db:
        res, wk, _ch, _q, _ev = _work_with_update(db, title="银河护卫队2",
                                                  file_id="f-gotg-live")
        # 旧快照登记的是「上一轮 B 库重建前」的资源 id，海报是 /9RT…
        res.poster_path = POSTER
        db.commit()
        old = Resource(file_id="f-gotg-old", name="银河护卫队2.old.mkv", ext=".mkv",
                       size=100, utime=1, identify_status="identified",
                       media_type="movie", media_title="银河护卫队2",
                       poster_path=STALE)
        db.add(old)
        db.flush()
        old_id = old.id
        # v1 快照指向旧资源、登记 /9RT…；v2（最新）指向当前资源
        lv1 = db.scalar(select(SCLibraryVersion).where(
            SCLibraryVersion.work_key == wk).order_by(SCLibraryVersion.id.desc()))
        lv1.version_no = 1
        lv1.resource_ids_json = json.dumps([old_id])
        lv1.metadata_json = json.dumps({"title": "银河护卫队2",
                                        "poster_path": STALE}, ensure_ascii=False)
        db.add(SCLibraryVersion(
            work_key=wk, version_no=2, fingerprint="fp-2", media_type="movie",
            resource_ids_json=json.dumps([res.id]),
            metadata_json=json.dumps({"title": "银河护卫队2",
                                      "poster_path": STALE}, ensure_ascii=False),
            change_type="updated"))
        db.commit()
        rid = res.id

    with SessionLocal() as db:
        got = sc2._work_posters(db, [wk])
    assert got[wk] == POSTER, (
        f"分享中心必须取当前资源的海报（{POSTER}），实际 {got[wk]!r} —— "
        "命中旧快照就停就是「资源库与分享中心海报不一致」的根因")


def test_latest_snapshot_resource_ids_win_over_older_ids():
    """多版快照各自登记不同资源时，越新的快照越优先（按版本倒序逐条试）。"""
    with SessionLocal() as db:
        new = Resource(file_id="f-new", name="A.new.mkv", ext=".mkv", size=1, utime=1,
                       identify_status="identified", poster_path="/new-live.jpg")
        old = Resource(file_id="f-old", name="A.old.mkv", ext=".mkv", size=1, utime=1,
                       identify_status="identified", poster_path="/old-live.jpg")
        db.add_all([new, old])
        db.flush()
        wk = utils.work_group_key(new)
        for ver, rid in ((1, old.id), (2, new.id)):
            db.add(SCLibraryVersion(
                work_key=wk, version_no=ver, fingerprint=f"fp-{ver}", media_type="movie",
                resource_ids_json=json.dumps([rid]),
                metadata_json=json.dumps({"poster_path": STALE}, ensure_ascii=False),
                change_type="updated"))
        db.commit()
    with SessionLocal() as db:
        assert sc2._work_posters(db, [wk])[wk] == "/new-live.jpg"


def test_snapshot_poster_still_used_when_resource_gone():
    """反证：资源已归档/删除时仍要退到快照海报，不能因为改了顺序就变「无封面」。"""
    with SessionLocal() as db:
        res, wk, _ch, _q, _ev = _work_with_update(db, title="已归档作品",
                                                  file_id="f-gone")
        res.clean_state = "cleaned"
        res.clean_reason = "测试归档"
        db.commit()
    with SessionLocal() as db:
        got = sc2._work_posters(db, [wk])
    assert got[wk] == POSTER, "实时资源不可用时必须退回快照登记的海报"
