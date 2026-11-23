"""v2.1.16 验收：队列 = 资源库作品经发布规则处理后的队列，且状态可回收。

用户口径（原话）：
  1. 「推送任务内容应该是资源库中的影视作品经过发布规则处理后的队列，资源库中有
     影视，就应该有队列；重建了资源库、手动清理了资源库、手动取消了推送任务、
     取消了分享链接，就应该将对应的任务归到初始状态，而不是一直在后台记录已经
     首推，包括手动扫描也是触发推送任务队列的方式」
  2. 「有明确的发布规则进入推送队列的影视则生成分享链接；在已推送里面若取消了
     分享链接，该推送记录判断为无效，待推送时若无推送链接、校验资源库有文件则
     重新触发分享链接，若分享成功则推送，若不成功则标记无效推送」

对应实现：
  - 名额判定改为四态 active/valid/invalid/none（`_SlotIndex`），不再有永久历史闸；
  - plan_engine 候选改**环形轮转**，取代「越过就永不回头」的水位游标；
  - dispatch 复用仍有效的分享链接，建链失败落 `invalid`（无效推送）；
  - 取消推送任务 / 取消分享链接同时作废链接态，释放名额；
  - 手动扫描（三个刷新入口）结束后主动跑一轮分享中心周期。
"""
import asyncio
import json
import os
import shutil
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine, delete, select
from sqlalchemy.orm import sessionmaker

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
MAIN = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v2116_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'v.db')}")
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    eng.dispose()
    shutil.rmtree(d, ignore_errors=True)


def _resource(db, *, file_id="f1", tmdb_id=1, title="Show"):
    from app.models import Resource
    r = Resource(
        file_id=file_id, name=f"{title}.S01E01.mkv", ext=".mkv", size=100, utime=1,
        identify_status="identified", media_type="tv", tmdb_id=tmdb_id,
        media_title=title, path_hint=f"{title} (2024)",
        share_dir_id=f"dir_{file_id}", share_scope="series_dir", share_dir_name=title,
        genres=json.dumps([{"id": 18, "name": "Drama"}]),
        poster_path="/poster.jpg",
    )
    db.add(r)
    db.commit()
    return r


def _channel(db, chat_id="-1001"):
    from app.models import Bot, Channel
    bot = Bot(name="b", token="enc", enabled=True)
    db.add(bot)
    db.flush()
    ch = Channel(bot_id=bot.id, name="g", chat_id=chat_id, enabled=True, kind="group")
    db.add(ch)
    db.commit()
    return ch


def _wkey(db, res):
    from app import utils
    return utils.work_group_key(res)


def _ready(db, works=1):
    """建资源 + 快照 + 渠道 + 整日窗口；返回 (资源列表, 渠道)。"""
    from app import share_center_v2 as sc2
    from conftest import ensure_full_day_hour_window
    from app.models import SCCategorySchedule
    sc2.ensure_defaults(db)
    resources = [_resource(db, file_id=f"f{i}", tmdb_id=i,
                           title="Show" if i == 1 else "Other")
                 for i in range(1, works + 1)]
    sc2.sync_library_versions(db)
    ch = _channel(db)
    # 分类排期行会按配置时段拦截（真机上导致「只有某些小时才建任务」）。
    # 本文件只验证队列与名额语义，清掉排期行让 _sched_allows 恒放行。
    for row in db.scalars(select(SCCategorySchedule)).all():
        db.delete(row)
    db.commit()
    ensure_full_day_hour_window(db, sc2._active_version(db).id)
    return resources, ch


def _one(db):
    """单作品的常用入口：返回 (资源, 渠道)。"""
    resources, ch = _ready(db)
    return resources[0], ch


def _first_item(db, res, ch, *, status="published", link_state="active", share_url=None):
    from app.models import SCPublishTask, SCPushListItem
    wk = _wkey(db, res)
    it = SCPushListItem(list_id=1, work_key=wk, chat_id=str(ch.chat_id),
                        dedup_key=f"{wk}:{ch.id}:first", source_kind="first",
                        status="planned")
    db.add(it)
    db.flush()
    url = share_url if share_url is not None else (
        "https://example/s/x" if link_state == "active" else "")
    task = SCPublishTask(list_item_id=it.id, work_key=wk, kind="first",
                         status=status, link_state=link_state, share_url=url)
    db.add(task)
    db.commit()
    return it, task


def _plan(db, res, ch):
    from app import share_center_v2 as sc2
    wk = _wkey(db, res)
    return sc2.first_dedup_plan(db, work_key=wk, chat_id=str(ch.chat_id),
                                dedup=f"{wk}:{ch.id}:first")


# ---------------------------------------------------------------------------
# ① 名额可回收：取消 / 清理 / 链接作废 / 资源移除 → 都能重新排队
# ---------------------------------------------------------------------------

def test_published_with_live_link_blocks(env):
    """已推成功 + 链接仍有效 → 不重复首推（唯一允许「跳过」的情形之一）。"""
    S, _ = env
    with S() as db:
        res, ch = _one(db)
        _first_item(db, res, ch, status="published", link_state="active")
        assert _plan(db, res, ch) == ("skip", None)


def test_cancelled_task_releases_slot(env):
    """手动取消推送任务 → 名额释放，可复活重排。"""
    S, _ = env
    with S() as db:
        res, ch = _one(db)
        it, _t = _first_item(db, res, ch, status="cancelled", link_state="active")
        action, reuse = _plan(db, res, ch)
        assert action == "revive" and reuse.id == it.id


def test_cleaned_task_releases_slot(env):
    S, _ = env
    with S() as db:
        res, ch = _one(db)
        it, _t = _first_item(db, res, ch, status="cleaned", link_state="cleaned")
        action, reuse = _plan(db, res, ch)
        assert action == "revive" and reuse.id == it.id


def test_cancelled_share_link_releases_slot(env):
    """在「已推送」里取消分享链接 → 该推送记录判为无效 → 可重推。"""
    S, _ = env
    with S() as db:
        res, ch = _one(db)
        it, task = _first_item(db, res, ch, status="published", link_state="active")
        # 用户取消分享链接：链接作废（share_url 保留供追溯）
        task.link_state = "cleaned"
        db.commit()
        action, reuse = _plan(db, res, ch)
        assert action == "revive" and reuse.id == it.id


def test_published_but_work_removed_releases_slot(env):
    """手动清理资源库 / 重建资源库把作品清掉 → 该记录不再占用名额。"""
    S, _ = env
    with S() as db:
        res, ch = _one(db)
        _first_item(db, res, ch, status="published", link_state="active")
        wk = _wkey(db, res)
        db.execute(delete(__import__("app.models", fromlist=["Resource"]).Resource))
        db.commit()
        from app import share_center_v2 as sc2
        action, _reuse = sc2.first_dedup_plan(
            db, work_key=wk, chat_id=str(ch.chat_id),
            dedup=f"{wk}:{ch.id}:first", live_keys=sc2._live_work_keys(db))
        assert action == "revive", "作品已不在资源库，名额必须回收"


# ---------------------------------------------------------------------------
# ② 旧推送队列的历史：看队列行自身是否仍有效
# ---------------------------------------------------------------------------

def _legacy(db, res, ch, *, clean_state="", share_status="active",
            share_url="https://t.me/c/1/2"):
    from app.models import AuditLog, QueueItem
    res.share_status = share_status
    # v2.1.17：判据收紧为「active 且确实还有链接」——真实数据里 active 必然有
    # URL（publisher 写 `"active" if share_url else "failed"`），这里如实还原。
    res.share_url = share_url
    q = QueueItem(resource_id=res.id, channel_id=ch.id, status="sent",
                  clean_state=clean_state)
    db.add(q)
    db.add(AuditLog(action="push_sent", target_type="queue", target_id="1",
                    detail=json.dumps({"resource_id": res.id,
                                       "channel_id": ch.id, "message_id": "9"})))
    db.commit()
    return q


def test_legacy_push_with_live_queue_row_blocks(env):
    S, _ = env
    with S() as db:
        res, ch = _one(db)
        _legacy(db, res, ch)
        assert _plan(db, res, ch) == ("skip", None)


def test_legacy_push_not_created_releases(env):
    """v2.1.17 核心反证：分享已归零（清空 B 库 / 清理过）→ 历史推送作废。

    这是线上「资源库有影视，推送任务一个都没有」的直接成因：`share_status`
    停在 not_created，旧队列判据却把这条历史 `push_sent` 当成「推送仍生效」，
    名额被永久占死，作品永远排不进队列。
    """
    S, _ = env
    with S() as db:
        res, ch = _one(db)
        _legacy(db, res, ch, share_status="not_created", share_url="")
        action, _ = _plan(db, res, ch)
        assert action == "create", "分享已不存在，名额必须回收"


def test_legacy_push_failed_share_releases(env):
    """建链失败（share_status=failed）同样意味着没有可用分享，不得占名额。"""
    S, _ = env
    with S() as db:
        res, ch = _one(db)
        _legacy(db, res, ch, share_status="failed", share_url="")
        action, _ = _plan(db, res, ch)
        assert action == "create"


def test_legacy_push_active_but_empty_link_releases(env):
    """状态是 active 但链接为空 → 仍视为无效（判据看的是链接在不在）。"""
    S, _ = env
    with S() as db:
        res, ch = _one(db)
        _legacy(db, res, ch, share_status="active", share_url="")
        action, _ = _plan(db, res, ch)
        assert action == "create"


def test_legacy_push_archived_queue_row_releases(env):
    """队列行已归档 → 这条历史推送已作废，允许重推。"""
    S, _ = env
    with S() as db:
        res, ch = _one(db)
        _legacy(db, res, ch, clean_state="cleaned")
        action, _ = _plan(db, res, ch)
        assert action == "create", "从未有清单项，应新建"


def test_legacy_push_share_cancelled_releases(env):
    """资源分享已被取消（旧链「取消分享」）→ 历史推送作废。"""
    S, _ = env
    with S() as db:
        res, ch = _one(db)
        _legacy(db, res, ch, share_status="cancelled")
        action, _ = _plan(db, res, ch)
        assert action == "create"


def test_legacy_audit_without_attribution_is_ignored(env):
    """detail 缺 resource_id/channel_id → 无法归属，不冒充结论。"""
    from app import share_center_v2 as sc2
    from app.models import AuditLog
    S, _ = env
    with S() as db:
        _ready(db)
        db.add(AuditLog(action="push_sent", target_type="queue", target_id="1",
                        detail=json.dumps({"message_id": "1"})))
        db.add(AuditLog(action="push_sent", target_type="queue", target_id="2",
                        detail="not-json"))
        db.commit()
        assert sc2._valid_legacy_push_pairs(db) == set()


# ---------------------------------------------------------------------------
# ③ 环形轮转：资源库有作品就有队列，且不会因游标越过而永久卡死
# ---------------------------------------------------------------------------

def test_cursor_past_snapshots_still_yields_tasks(env):
    """反证 v2.1.15 的第三道锁已根除：游标停在末尾，环形轮转照样能取到作品。"""
    from app import share_center_v2 as sc2
    from app.models import SCCategoryCursor, SCWorkCategory
    S, _ = env
    with S() as db:
        _ready(db)
        version = sc2._active_version(db)
        cat = db.scalar(select(SCWorkCategory)).category_key
        db.add(SCCategoryCursor(plan_version_id=version.id, category_key=cat,
                                last_library_version_id=999999))
        db.commit()
        out = sc2.plan_engine(db)
        assert out["tasks"] >= 1, out


def test_ring_rotation_covers_every_work(env):
    """work_limit=1 时，两轮必须覆盖两部不同作品（环形轮转，不是只盯最前面一部）。"""
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env
    with S() as db:
        _ready(db, works=2)
        version = sc2._active_version(db)
        cfg = json.loads(version.config_json or "{}")
        cfg["work_limit"] = 1
        version.config_json = json.dumps(cfg)
        db.commit()

        got = []
        for _ in range(3):
            if sc2.plan_engine(db)["tasks"]:
                rows = list(db.scalars(select(SCPublishTask)).all())
                got = [t.work_key for t in rows]
            if len(set(got)) >= 2:
                break
        assert len(set(got)) >= 2, f"两部作品都应被轮到，实际：{got}"


def test_work_removed_from_library_is_not_planned(env):
    """资源库里已经没有这部作品 → 不再为它建队列。"""
    from app import share_center_v2 as sc2
    from app.models import Resource, SCPublishTask
    S, _ = env
    with S() as db:
        _ready(db)
        db.execute(delete(Resource))
        db.commit()
        out = sc2.plan_engine(db)
        assert out["tasks"] == 0, out
        assert not list(db.scalars(select(SCPublishTask)).all())


# ---------------------------------------------------------------------------
# ④ 取消推送任务 / 取消分享链接：作废链接并释放名额
# ---------------------------------------------------------------------------

def test_cancel_endpoint_invalidates_link(env):
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env
    with S() as db:
        res, ch = _one(db)
        it, task = _first_item(db, res, ch, status="published", link_state="active")
        out = sc2.api_task_cancel(task.id, db)
        assert out["ok"] is True
        t = db.get(SCPublishTask, task.id)
        assert t.status == "cancelled"
        assert t.link_state == "cleaned", "取消分享必须把链接一并作废"
        assert t.share_url, "share_url 保留供追溯，不物理抹掉"
        action, reuse = _plan(db, res, ch)
        assert action == "revive" and reuse.id == it.id


# ---------------------------------------------------------------------------
# ⑤ dispatch：复用有效链接 / 建链失败标记无效推送
# ---------------------------------------------------------------------------

def _dispatch_env(db):
    """构造一条门禁可通过的任务（英文名已就绪 + 海报/资料齐全）。"""
    from test_v213_push_gate import _bp_file, _bp_media, _bp_resource, _bot_channel, _publish_task
    r = _bp_resource(db)
    m = _bp_media(db, english_title="Black Panther: Wakanda Forever")
    _bp_file(db, m.id, rename_status="done", target_name="Black Panther 2.mkv")
    task, _, _ = _publish_task(db, resource_ids=[r.id])
    _bot_channel(db)
    return task


def test_dispatch_reuses_existing_link(env, monkeypatch):
    """已有仍有效的分享链接 → 直接复用，不再调网盘建链。"""
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env
    calls = {"n": 0}

    async def fake_create(client, file_id, title, duration):
        calls["n"] += 1
        return "sid", "https://www.guangyapan.com/s/new"

    monkeypatch.setattr(sc2, "create_share_parsed", fake_create)
    with S() as db:
        task = _dispatch_env(db)
        task.share_url = "https://www.guangyapan.com/s/existing"
        task.link_state = "active"
        db.commit()
        stats = asyncio.run(sc2.dispatch_ready_tasks(db, limit=20))
        assert stats["reused"] == 1, stats
        assert stats["built"] == 0 and calls["n"] == 0, "有链接就不得重建"
        t = db.get(SCPublishTask, task.id)
        assert t.share_url == "https://www.guangyapan.com/s/existing"
        assert t.status == "submitted"


def test_dispatch_rebuilds_when_link_invalid(env, monkeypatch):
    """链接已作废 → 重新建链再推送（用户口径：无推送链接才重新触发）。"""
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env
    calls = {"n": 0}

    async def fake_create(client, file_id, title, duration):
        calls["n"] += 1
        return "sid", "https://www.guangyapan.com/s/fresh"

    monkeypatch.setattr(sc2, "create_share_parsed", fake_create)
    with S() as db:
        task = _dispatch_env(db)
        task.share_url = "https://www.guangyapan.com/s/dead"
        task.link_state = "cleaned"
        db.commit()
        stats = asyncio.run(sc2.dispatch_ready_tasks(db, limit=20))
        assert stats["built"] == 1 and calls["n"] == 1, stats
        t = db.get(SCPublishTask, task.id)
        assert t.share_url == "https://www.guangyapan.com/s/fresh"


def test_dispatch_marks_invalid_when_build_fails(env, monkeypatch):
    """分享不成功 → 标记「无效推送」，并给退避窗口避免每 5 秒重建。"""
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env

    async def boom(client, file_id, title, duration):
        raise RuntimeError("网盘返回 500")

    monkeypatch.setattr(sc2, "create_share_parsed", boom)
    with S() as db:
        task = _dispatch_env(db)
        stats = asyncio.run(sc2.dispatch_ready_tasks(db, limit=20))
        assert stats["invalid"] == 1, stats
        t = db.get(SCPublishTask, task.id)
        assert t.status == sc2.INVALID_TASK_STATUS
        assert t.link_state == sc2.INVALID_TASK_STATUS
        assert t.next_retry_at is not None, "必须进退避窗口"
        assert "分享链接创建失败" in (t.error or "")


# ---------------------------------------------------------------------------
# ⑥ 手动扫描触发队列
# ---------------------------------------------------------------------------

def test_manual_scan_triggers_share_cycle():
    body = MAIN[MAIN.index("async def _refresh_library_from_b"):]
    body = body[:body.index("@app.post(\"/api/scan-now\"")]
    assert "share_cycle" in body, "手动扫描必须触发一轮分享中心周期"
    assert "_run_share_center_v2_cycle_in_thread" in body
    assert "asyncio.to_thread" in body, "run_cycle 含同步 commit，必须走线程"


# ---------------------------------------------------------------------------
# ⑦ 前端
# ---------------------------------------------------------------------------

def test_frontend_has_invalid_push_label():
    assert "invalid: '无效推送'" in APP
    assert "无效推送" in APP


def test_cancel_confirm_explains_slot_release():
    block = APP[APP.index("async function scTaskAction"):]
    block = block[:block.index("// v2.1.1") if "// v2.1.1" in block else 1800]
    assert "名额立即释放" in block, "取消必须如实说明名额释放，不能只说「已取消」"
    assert "重新生成分享链接" in block


def test_frontend_does_not_leak_backend_terms_in_cancel():
    block = APP[APP.index("async function scTaskAction"):][:1600]
    for term in ("link_state", "dedup_key", "SCPublishTask", "work_key", "invalid"):
        assert term not in block, f"页面文案不得体现后端名词：{term}"
