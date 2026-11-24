"""v2.1.21：手动推送（force）必须写进 v2 任务表，三个卡片才看得见。

现场问题（2026-11-24）：「手动推送的鹿鼎记没进推送任务、推送历史」。
推送本身是成功的（push_queue 13/14 已 sent、push_sent 审计 6191/6192 都在），
断在**记录写错了表**：

  `main.publish_resource` 调 `share_center.shadow_register(kind="force")`
  → 写旧链的 `share_tasks`；
  而「推送任务 / 推送历史 / 推送队列」三个卡片读的是 `sc_publish_tasks`。

双写期两张表没打通 → 手动推送的记录永远不出现在这三个模块里
（自动推送走 v2，所以能在历史看到）。旧表里那条 force 任务
（id=245、kind=force、status=published）确实存在，只是没人读它。

本文件锁死：
  ① `register_force_tasks` 建的是 `sc_push_list_items` + `sc_publish_tasks`；
  ② 三个视图（历史/队列）都能看到这条 force 任务；
  ③ 同一作品反复手动推送时复用清单项（dedup_key 唯一约束不能撞）；
  ④ force **不占首推名额**——手动推过的作品之后仍按发布规则排首推；
  ⑤ force 绕过推送门禁（手动是明确动作），自动任务仍受门禁约束；
  ⑥ 拿不到快照时如实返回空列表，绝不伪造任务行。
"""
import asyncio
import json

import pytest
from sqlalchemy import delete, select

from app import share_center_v2 as sc2
from app import utils
from app.db import SessionLocal, migrate
from app.models import (
    AppConfig,
    AuditLog,
    Bot,
    Channel,
    QueueItem,
    Resource,
    Rule,
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
    ShareTask,
)

from conftest import ensure_full_day_hour_window

# 必须**全清**：本文件既有端点级用例（走 publish_resource，会被别的文件留下的
# Rule / push_routes_json / publish_paused 直接影响），又有「旧表不该被写」的断言
# （残留的 ShareTask 行会把它顶红）。少清一张表就会变成「单跑绿、全量红」。
WIPED = (SCPublishTask, SCPushListItem, SCPushList, SCLibraryVersion,
         SCCategoryCursor, SCCategorySchedule, SCWorkCategory, SCCategory,
         SCSharePlanVersion, SCSharePlan, ShareTask, QueueItem, Resource,
         Channel, Rule, Bot, AppConfig, AuditLog)

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


def _channel(db, *, chat_id="-1001234567890", name="频道"):
    bot = Bot(name="b", token="x", enabled=True)
    db.add(bot)
    db.flush()
    ch = Channel(bot_id=bot.id, name=name, chat_id=chat_id,
                 kind="group" if str(chat_id).startswith("-") else "channel",
                 enabled=True)
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


def _work(db, *, title="鹿鼎记II：神龙教", file_id="f-1", poster=POSTER,
          share_url=""):
    res = Resource(file_id=file_id, name=f"{title}.1992.mkv", ext=".mkv",
                   size=100, utime=1, identify_status="identified", media_type="movie",
                   tmdb_id=1, media_title=title, share_dir_name=title,
                   share_scope="series_dir",
                   share_status="active" if share_url else "not_created",
                   share_url=share_url, poster_path=poster)
    db.add(res)
    db.flush()
    wk = utils.work_group_key(res)
    db.add(SCLibraryVersion(work_key=wk, version_no=1, fingerprint="fp1",
                            media_type="movie",
                            resource_ids_json=json.dumps([res.id]),
                            metadata_json=json.dumps(
                                {"title": title, "poster_path": poster},
                                ensure_ascii=False),
                            change_type="first"))
    db.add(SCWorkCategory(work_key=wk, category_key="genre:剧情"))
    db.commit()
    return res, wk


# --------------------------------------------------------------------------
# ① 写进 v2 两张表（修复前写的是旧表 share_tasks）
# --------------------------------------------------------------------------
def test_force_task_lands_in_v2_tables():
    with SessionLocal() as db:
        _plan(db)
        ch = _channel(db)
        res, wk = _work(db)
        tasks = sc2.register_force_tasks(db, res, [ch.id])

        assert len(tasks) == 1
        task = db.get(SCPublishTask, tasks[0].id)
        assert task.work_key == wk
        assert task.kind == "force"
        assert task.status == "planned"
        # 手动动作必须比自动任务先被 dispatch 捞起
        assert task.priority >= sc2.FORCE_TASK_PRIORITY

        item = db.get(SCPushListItem, task.list_item_id)
        assert item is not None
        assert item.source_kind == "force"
        assert item.chat_id == ch.chat_id
        assert item.library_version_id > 0


def test_force_task_does_not_touch_legacy_share_tasks():
    """旧表 share_tasks 不再新增 force 行（历史行保留不动）。"""
    with SessionLocal() as db:
        _plan(db)
        ch = _channel(db)
        res, _wk = _work(db)
        sc2.register_force_tasks(db, res, [ch.id])
        assert db.scalars(select(ShareTask)).all() == []


# --------------------------------------------------------------------------
# ② 三个卡片看得见
# --------------------------------------------------------------------------
def test_force_task_shows_in_queue_view_then_history():
    with SessionLocal() as db:
        _plan(db)
        ch = _channel(db)
        res, wk = _work(db)
        tasks = sc2.register_force_tasks(db, res, [ch.id])
        task_id = tasks[0].id

    with SessionLocal() as db:
        views = sc2._aggregate_views(db)
    assert [r["id"] for r in views["queue"]] == [task_id]
    assert views["queue"][0]["kind"] == "force"
    assert views["queue"][0]["title"]           # 标题非空，页面不出现裸 work_key

    # 推送成功 → 落到「推送历史」
    with SessionLocal() as db:
        task = db.get(SCPublishTask, task_id)
        task.status = "published"
        task.published_at = sc2.utcnow()
        db.commit()
    with SessionLocal() as db:
        views = sc2._aggregate_views(db)
    assert views["queue"] == []
    assert [r["id"] for r in views["history"]] == [task_id]


def test_force_task_shows_in_task_list_api():
    with SessionLocal() as db:
        _plan(db)
        ch = _channel(db)
        res, wk = _work(db)
        task_id = sc2.register_force_tasks(db, res, [ch.id])[0].id
    with SessionLocal() as db:
        payload = sc2.api_tasks(db=db)
    assert [r["id"] for r in payload["items"]] == [task_id]
    assert payload["items"][0]["kind"] == "force"


# --------------------------------------------------------------------------
# ③ 反复手动推送：复用清单项，不撞 dedup_key 唯一键
# --------------------------------------------------------------------------
def test_second_manual_push_reuses_list_item():
    with SessionLocal() as db:
        _plan(db)
        ch = _channel(db)
        res, wk = _work(db)
        sc2.register_force_tasks(db, res, [ch.id])
        sc2.register_force_tasks(db, res, [ch.id])

        items = db.scalars(select(SCPushListItem).where(
            SCPushListItem.work_key == wk)).all()
        tasks = db.scalars(select(SCPublishTask)).all()
        assert len(items) == 1, "dedup_key 唯一约束：第二次必须复活同一条清单项"
        assert len(tasks) == 2, "两次手动推送应是两条任务（历史各自独立）"


# --------------------------------------------------------------------------
# ④ 不占首推名额
# --------------------------------------------------------------------------
def test_force_does_not_consume_first_slot():
    with SessionLocal() as db:
        _plan(db)
        ch = _channel(db)
        res, wk = _work(db)
        sc2.register_force_tasks(db, res, [ch.id])
        db.flush()
        index = sc2._SlotIndex(db)
        state, _reuse = index.state(wk, ch.chat_id)
        # force 清单项不进名额索引 → 仍判「从未首推」
        assert state == sc2.SLOT_NONE


def test_plan_engine_still_queues_first_after_manual_push():
    """手动推过的作品，之后仍按发布规则排首推（不被这次手动动作占掉名额）。"""
    with SessionLocal() as db:
        _plan(db)
        ch = _channel(db)
        res, wk = _work(db)
        sc2.register_force_tasks(db, res, [ch.id])
        # 手动那条先落终态，模拟真实时序（推完才轮到下一轮计划）
        for t in db.scalars(select(SCPublishTask)).all():
            t.status = "published"
        db.commit()

        stats = sc2.plan_engine(db)
        kinds = [t.kind for t in db.scalars(select(SCPublishTask)).all()]
        assert stats["tasks"] >= 1
        assert "first" in kinds


# --------------------------------------------------------------------------
# ⑤ 绕过推送门禁（自动任务仍受约束）
# --------------------------------------------------------------------------
def test_force_bypasses_push_gate_but_auto_still_blocked(monkeypatch):
    # v2.1.19 起「上新即时推送每小时上限」按分钟匀速放行：整点刚过只放 1 部。
    # 本用例要先推 force 再轮到 auto，必须把时间钉在本小时第 59 分（额度放满），
    # 否则 auto 会被 throttled 直接 break，测不到门禁分支。
    _real_utcnow = sc2.utcnow
    monkeypatch.setattr(sc2, "utcnow",
                        lambda: _real_utcnow().replace(minute=59, second=0, microsecond=0))
    with SessionLocal() as db:
        _plan(db)
        ch = _channel(db)
        # 没有海报 → 自动任务必被门禁拦。
        res, wk = _work(db, poster="", share_url="https://example.com/s/abc")
        task = sc2.register_force_tasks(db, res, [ch.id])[0]
        force_id = task.id
        # 对照：同一作品再造一条自动 first 任务
        lst = sc2._force_push_list(db, 0)
        item = SCPushListItem(list_id=lst.id, work_key=wk, chat_id=ch.chat_id,
                              dedup_key=f"auto:{wk}", source_kind="first",
                              status="planned",
                              library_version_id=db.scalars(
                                  select(SCLibraryVersion.id)).first())
        db.add(item)
        db.flush()
        auto = SCPublishTask(list_item_id=item.id, work_key=wk, kind="first",
                             status="planned")
        db.add(auto)
        db.commit()
        auto_id = auto.id

    with SessionLocal() as db:
        asyncio.run(sc2.dispatch_ready_tasks(db, limit=10))

    with SessionLocal() as db:
        force = db.get(SCPublishTask, force_id)
        auto = db.get(SCPublishTask, auto_id)
        # force 有缓存链接 → 直接 ready/submitted，门禁不拦
        assert force.status in ("ready", "submitted"), force.error
        assert force.error == ""
        # 自动任务缺海报 → 被门禁挡回 planned，并给出中文原因
        assert auto.status == "planned"
        assert auto.error


def test_force_submit_writes_v2_queue_linkage():
    """force 提交到队列时必须是 v2 语义，publisher 才能把结果回写到任务行。"""
    with SessionLocal() as db:
        _plan(db)
        ch = _channel(db)
        ch_id = ch.id
        res, wk = _work(db, share_url="https://example.com/s/abc")
        task_id = sc2.register_force_tasks(db, res, [ch_id])[0].id

    with SessionLocal() as db:
        asyncio.run(sc2.dispatch_ready_tasks(db, limit=10))

    with SessionLocal() as db:
        task = db.get(SCPublishTask, task_id)
        assert task.status == "submitted"
        q = db.scalar(select(QueueItem).where(QueueItem.channel_id == ch_id))
        assert q is not None
        assert q.push_reason == "sc_v2"
        assert q.event_id == task_id
        assert q.share_task_id == 0, "不得再挂旧链 share_tasks"


# --------------------------------------------------------------------------
# ⑥ 降级如实：拿不到快照就不建行
# --------------------------------------------------------------------------
def test_no_snapshot_returns_empty_without_rows():
    with SessionLocal() as db:
        _plan(db)
        ch = _channel(db)
        res = Resource(file_id="f-x", name="孤儿.mkv", ext=".mkv",
                       size=1, utime=1, identify_status="identified",
                       media_type="movie", media_title="孤儿")
        db.add(res)
        db.commit()
        out = sc2.register_force_tasks(db, res, [ch.id])
        assert out == []
        assert db.scalars(select(SCPublishTask)).all() == []
        assert db.scalars(select(SCPushListItem)).all() == []


def test_empty_channel_list_returns_empty():
    with SessionLocal() as db:
        _plan(db)
        res, _wk = _work(db)
        assert sc2.register_force_tasks(db, res, []) == []


# --------------------------------------------------------------------------
# ⑦ 多目标渠道 / 缓存链接复用
# --------------------------------------------------------------------------
def test_multiple_channels_create_one_task_each():
    with SessionLocal() as db:
        _plan(db)
        c1 = _channel(db, chat_id="-1001234567890", name="频道")
        c2 = _channel(db, chat_id="-1009876543210", name="群")
        res, wk = _work(db)
        tasks = sc2.register_force_tasks(db, res, [c1.id, c2.id])
        assert len(tasks) == 2
        chats = {db.get(SCPushListItem, t.list_item_id).chat_id for t in tasks}
        assert chats == {c1.chat_id, c2.chat_id}


def test_cached_share_url_is_reused():
    with SessionLocal() as db:
        _plan(db)
        ch = _channel(db)
        res, _wk = _work(db, share_url="https://example.com/s/abc")
        task = sc2.register_force_tasks(db, res, [ch.id])[0]
        db.refresh(task)
        assert task.share_url == "https://example.com/s/abc"
        assert task.link_state == "active"


def test_no_cached_share_url_left_blank():
    """没有缓存链接就是空串 + unknown，绝不填假值。"""
    with SessionLocal() as db:
        _plan(db)
        ch = _channel(db)
        res, _wk = _work(db)
        task = sc2.register_force_tasks(db, res, [ch.id])[0]
        db.refresh(task)
        assert task.share_url == ""
        assert task.link_state == "unknown"


# --------------------------------------------------------------------------
# ⑧ 端点级：手动推送之后三个卡片真的能看到（这是现场原话的复现）
# --------------------------------------------------------------------------
def test_publish_endpoint_registers_visible_task():
    """`POST /api/resources/{id}/publish` 之后，推送任务/历史/队列里必须有一条。

    这就是线上那句「手动推送的鹿鼎记没进推送任务、推送历史」的回归锁：
    修复前 `publish_resource` 调 `shadow_register` 写旧表 share_tasks，
    `sc_publish_tasks` 一条没有 → 三个卡片全空（消息却真发出去了）。
    """
    from unittest.mock import AsyncMock, patch

    from app import main

    with SessionLocal() as db:
        _plan(db)
        ch = _channel(db)
        db.add(Rule(name="影视", channel_id=ch.id, enabled=True))
        db.commit()
        res, wk = _work(db)
        rid = res.id

    with SessionLocal() as db:
        with patch.object(main, "process_queue",
                          new=AsyncMock(return_value={"ok": True})):
            out = asyncio.run(main.publish_resource(rid, db))

    assert out["ok"] is True
    assert out["queued"] == 1
    assert out["tasks_registered"] == 1, "必须登记进 sc_publish_tasks，否则页面看不见"
    assert out["detail"] == [] or all(d.get("status") != "failed" for d in out["detail"])

    with SessionLocal() as db:
        tasks = db.scalars(select(SCPublishTask)).all()
        assert len(tasks) == 1
        assert tasks[0].kind == "force"
        assert tasks[0].work_key == wk
        # 旧表不再被写（历史行保留，不新增 force）
        assert db.scalars(select(ShareTask)).all() == []
        # 队列项不再挂旧链
        q = db.scalar(select(QueueItem).where(QueueItem.resource_id == rid))
        assert q is not None and q.share_task_id == 0

    with SessionLocal() as db:
        views = sc2._aggregate_views(db)
    assert [r["kind"] for r in views["queue"]] == ["force"]


def test_legacy_path_reproduces_the_reported_symptom():
    """**反证**：把登记换成旧实现（`shadow_register` 写 share_tasks）时，
    三个卡片必然是空的 —— 这正是线上「推了但看不到」的症状。

    这条用例的作用是证明「可见」不是顺手来的：换回旧写法立刻回到缺陷状态，
    所以上面那条端点级断言确实覆盖了 bug，而不是因为别的原因恰好为真。
    """
    from unittest.mock import AsyncMock, patch

    from app import main
    from app import share_center

    def _legacy_register(db, resource, channel_ids):
        share_center.shadow_register(db, resource, kind="force",
                                     channel_ids=channel_ids)
        return []

    with SessionLocal() as db:
        _plan(db)
        ch = _channel(db)
        db.add(Rule(name="影视", channel_id=ch.id, enabled=True))
        db.commit()
        res, _wk = _work(db)
        rid = res.id

    with SessionLocal() as db:
        with patch.object(main, "process_queue", new=AsyncMock(return_value={"ok": True})), \
                patch.object(sc2, "register_force_tasks", side_effect=_legacy_register):
            out = asyncio.run(main.publish_resource(rid, db))

    assert out["tasks_registered"] == 0
    with SessionLocal() as db:
        assert db.scalars(select(SCPublishTask)).all() == [], "旧路径不该写 v2 表"
        # 旧路径写的是旧表 —— 这正是断链的形状
        assert len(db.scalars(select(ShareTask)).all()) == 1
        views = sc2._aggregate_views(db)
    assert views["queue"] == [] and views["history"] == []


