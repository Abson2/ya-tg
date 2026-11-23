"""v2.1.15 验收：轮转游标锁死恢复 + 历史推送防重复首推。

真机事实（2026-11-23 23:21 复查，两道锁之外还有第三道）：
  - 分类游标 `last_library_version_id` 已被推进到**全部快照之后**，
    plan_engine 只取 `SCLibraryVersion.id > 游标` → 即使去重修好，也永远不再回头看
    那 486 个快照。这是「等 45 分钟也不会自愈」的直接原因。
  - 库里 233 条历史 push_sent 属于**旧推送队列**（只写审计，不写
    SCPublishTask.status='published'）→ 若只按 published 判重，重建首推会把早已
    推过的作品再推一遍。

本文件锁定：① 审计精确归属（作品 + 渠道）；② 预演数字与执行同源；
③ 游标归零后存量能被重扫；④ 已推送过绝不重复首推；⑤ 不删任何历史行。
"""
import json
import os
import shutil
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v2115_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'v.db')}")
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    eng.dispose()
    shutil.rmtree(d, ignore_errors=True)


def _resource(db, file_id="f1", name="Show (2024) {tmdb-1}/Show.S01E01.mkv"):
    from app.models import Resource
    r = Resource(
        file_id=file_id, name=name, ext=".mkv", size=100, utime=1,
        identify_status="identified", media_type="tv", tmdb_id=1,
        media_title="Show", path_hint="Show (2024)",
        share_dir_id="dir_show", share_scope="series_dir", share_dir_name="Show",
        genres=json.dumps([{"id": 18, "name": "Drama"}]),
        poster_path="/poster.jpg",
    )
    db.add(r)
    db.commit()
    return r


def _channel(db, chat_id="-1001"):
    from app.models import Bot, Channel
    bot = db.scalar(select(Bot))
    if not bot:
        bot = Bot(name="test", token="enc", enabled=True)
        db.add(bot)
        db.flush()
    ch = Channel(bot_id=bot.id, name="group", chat_id=chat_id, enabled=True, kind="group")
    db.add(ch)
    db.commit()
    return ch


def _work_key(db, res):
    from app import utils
    return utils.work_group_key(res)


def _audit_push(db, resource_id, channel_id):
    from app.models import AuditLog
    db.add(AuditLog(action="push_sent", target_type="queue", target_id="1",
                    detail=json.dumps({"resource_id": resource_id,
                                       "channel_id": channel_id,
                                       "message_id": "99"})))
    db.commit()


def _ready(db):
    """建资源 + 快照 + 渠道 + 整日窗口，让 plan_engine 与运行时刻无关。"""
    from app import share_center_v2 as sc2
    from conftest import ensure_full_day_hour_window
    sc2.ensure_defaults(db)
    res = _resource(db)
    sc2.sync_library_versions(db)
    ch = _channel(db)
    ensure_full_day_hour_window(db, sc2._active_version(db).id)
    return res, ch


def _work_category(db):
    from app.models import SCWorkCategory
    row = db.scalar(select(SCWorkCategory))
    return row.category_key if row else ""


# ---------- ① 历史推送审计的精确归属 ----------

def test_audit_push_blocks_only_that_channel(env):
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        res, ch_a = _ready(db)
        ch_b = _channel(db, "-1002")
        _audit_push(db, res.id, ch_a.id)
        wk = _work_key(db, res)
        assert sc2._ever_published(db, work_key=wk, chat_id=str(ch_a.chat_id)) is True
        assert sc2._ever_published(db, work_key=wk, chat_id=str(ch_b.chat_id)) is False


def test_audit_without_attribution_is_ignored(env):
    """detail 缺 resource_id/channel_id → 无法归属，不冒充结论。"""
    from app import share_center_v2 as sc2
    from app.models import AuditLog
    S, _ = env
    with S() as db:
        _ready(db)
        db.add(AuditLog(action="push_sent", target_type="queue", target_id="1",
                        detail=json.dumps({"message_id": "1"})))
        db.add(AuditLog(action="push_sent", target_type="queue", target_id="2", detail="not-json"))
        db.commit()
        assert sc2._pushed_pairs_from_audit(db) == set()


def test_published_task_also_blocks(env):
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask, SCPushListItem
    S, _ = env
    with S() as db:
        res, ch = _ready(db)
        wk = _work_key(db, res)
        it = SCPushListItem(list_id=1, work_key=wk, chat_id=str(ch.chat_id),
                            dedup_key=f"{wk}:{ch.id}:first", source_kind="first",
                            status="planned")
        db.add(it)
        db.flush()
        db.add(SCPublishTask(list_item_id=it.id, work_key=wk, kind="first", status="published"))
        db.commit()
        assert sc2._ever_published(db, work_key=wk, chat_id=str(ch.chat_id)) is True
        action, _ = sc2.first_dedup_plan(db, work_key=wk, chat_id=str(ch.chat_id),
                                        dedup=f"{wk}:{ch.id}:first")
        assert action == "skip"


# ---------- ② 第三道锁：游标越过 → 归零后可重扫 ----------

def test_cursor_past_snapshots_yields_zero_tasks(env):
    """反证第三道锁：游标越过全部快照时，plan_engine 一条任务都建不出来。"""
    from app import share_center_v2 as sc2
    from app.models import SCCategoryCursor
    S, _ = env
    with S() as db:
        _ready(db)
        version = sc2._active_version(db)
        cat = _work_category(db)
        assert cat, "作品必须有分类，否则用例前提不成立"
        db.add(SCCategoryCursor(plan_version_id=version.id, category_key=cat,
                                last_library_version_id=999999))
        db.commit()
        out = sc2.plan_engine(db)
        assert out["tasks"] == 0, out


def test_rebuild_resets_cursor_and_creates_tasks(env):
    from app import share_center_v2 as sc2
    from app.models import SCCategoryCursor, SCPublishTask
    S, _ = env
    with S() as db:
        res, ch = _ready(db)
        version = sc2._active_version(db)
        # 造出与真机一致的卡死现场：游标已越过全部快照
        cat = _work_category(db)
        db.add(SCCategoryCursor(plan_version_id=version.id, category_key=cat,
                                last_library_version_id=999999))
        db.commit()
        assert sc2.plan_engine(db)["tasks"] == 0, "游标越过全部快照时应一条都建不出"

        # 预演：应报「可重排 1 条」，且不写库
        pre = sc2.preview_first_rebuild(db)
        assert pre["ok"] is True
        assert pre["would_create"] + pre["would_revive"] >= 1, pre
        assert not list(db.scalars(select(SCPublishTask)).all()), "预演不得写库"

        out = sc2.api_rebuild_first(sc2.Payload(payload={"dry_run": False}), db)
        assert out["ok"] and out["cursors_reset"] >= 1, out
        # 游标归零后，plan_engine 真的能重扫历史快照并建出任务
        assert out["plan"]["tasks"] >= 1, out
        cursors = list(db.scalars(select(SCCategoryCursor)).all())
        # 归零后本轮又把该分类推进到了真实快照位置（1），不再停在 999999
        assert all(c.last_library_version_id < 999999 for c in cursors), \
            [c.last_library_version_id for c in cursors]
        assert any(c.last_library_version_id == 1 for c in cursors)


def test_rebuild_preview_is_dry_by_default(env):
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env
    with S() as db:
        _ready(db)
        out = sc2.api_rebuild_first(sc2.Payload(payload={}), db)
        assert out["ok"] is True and "cursors_reset" not in out
        assert not list(db.scalars(select(SCPublishTask)).all())


# ---------- ③ 已推送过的不重复首推 ----------

def test_rebuild_skips_work_already_pushed(env):
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env
    with S() as db:
        res, ch = _ready(db)
        _audit_push(db, res.id, ch.id)
        pre = sc2.preview_first_rebuild(db)
        assert pre["blocked_pushed"] >= 1, pre
        assert pre["would_create"] == 0 and pre["would_revive"] == 0, pre
        out = sc2.api_rebuild_first(sc2.Payload(payload={"dry_run": False}), db)
        assert out["plan"]["tasks"] == 0, out
        assert not list(db.scalars(select(SCPublishTask)).all())


def test_rebuild_keeps_all_history_rows(env):
    """重建只归零游标，绝不物理删除清单项与任务行。"""
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask, SCPushListItem
    S, _ = env
    with S() as db:
        res, ch = _ready(db)
        wk = _work_key(db, res)
        it = SCPushListItem(list_id=1, work_key=wk, chat_id=str(ch.chat_id),
                            dedup_key=f"{wk}:{ch.id}:first", source_kind="first",
                            status="cleaned")
        db.add(it)
        db.flush()
        db.add(SCPublishTask(list_item_id=it.id, work_key=wk, kind="first", status="cleaned"))
        db.commit()
        before_items = len(list(db.scalars(select(SCPushListItem)).all()))
        before_tasks = len(list(db.scalars(select(SCPublishTask)).all()))
        sc2.api_rebuild_first(sc2.Payload(payload={"dry_run": False}), db)
        assert len(list(db.scalars(select(SCPushListItem)).all())) >= before_items
        assert len(list(db.scalars(select(SCPublishTask)).all())) >= before_tasks


# ---------- ④ 前端入口 ----------

def test_frontend_has_rebuild_button_and_dry_run_first():
    assert "重建首推任务" in APP
    assert "scRebuildFirst" in APP
    assert "/sc/plan/rebuild-first" in APP
    assert "dry_run: true" in APP and "dry_run: false" in APP, "必须先预演再执行"
    assert "已推送过而跳过" in APP, "确认框必须如实报出被跳过的条数"


def test_frontend_does_not_leak_backend_terms():
    block_start = APP.index("async function scRebuildFirst")
    block = APP[block_start:block_start + 2600]
    for term in ("last_library_version_id", "first_dedup_plan", "SCPushListItem",
                 "SCPublishTask", "work_key"):
        assert term not in block, f"页面文案不得体现后端名词：{term}"
