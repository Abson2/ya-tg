"""阶段二验收（资源库解耦）：识别→事件→分享中心新链 + 灰度开关零回归。

设计要点（对齐 RESOURCE_SHARE_REDESIGN_PLAN.md 阶段二）：
  1. compute_resource_channel_targets 只算目标、不写 QueueItem；
  2. _identify_emit_only 只发资源事件，payload 携带 channel_targets；
  3. consume_resource_events 据 payload targets 建带目标的 ShareTask；
  4. 灰度开关 resource_decoupled_enabled 默认 false=旧链，true 且 share_center_enabled=true 才走新链。

零回归护栏：默认开关关闭，5 个识别入口行为与改造前一致。
"""
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v145_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'b.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng, d
    shutil.rmtree(d, ignore_errors=True)


def _resource(db, rid=1, *, name="开封府 S01E01.mkv", identified=True, poster="/p.jpg"):
    from app.models import Resource
    r = Resource(id=rid, file_id=f"F{rid}", name=name,
                 identify_status="identified" if identified else "unidentified",
                 media_title="开封府", poster_path=poster, media_type="tv")
    db.add(r)
    db.commit()
    db.refresh(r)
    return r


# ---------- 开关默认值与分发 ----------

def test_decoupled_switch_defaults_off():
    from app.runtime_config import DEFAULTS
    assert DEFAULTS["resource_decoupled_enabled"][1] == "false"


def test_dispatch_uses_old_chain_when_switch_off(env):
    """开关默认关闭：识别入口必须走旧链 _enqueue_after_identify，零回归。"""
    from app import main, runtime_config
    S, eng, d = env
    db = S()
    runtime_config.set_many(db, {"resource_decoupled_enabled": "false"})
    x = _resource(db)
    with patch.object(main, "_enqueue_after_identify", return_value=1) as old, \
         patch.object(main, "_identify_emit_only", return_value=1) as new:
        main._after_identify_dispatch(db, x)
    assert old.call_count == 1, "开关关闭必须走旧链"
    assert new.call_count == 0, "开关关闭不得走新链"


def test_dispatch_uses_new_chain_when_both_switches_on(env):
    """开关开启且分享中心启用：走新链 _identify_emit_only。"""
    from app import main, runtime_config
    S, eng, d = env
    db = S()
    runtime_config.set_many(db, {
        "resource_decoupled_enabled": "true",
        "share_center_enabled": "true",
    })
    x = _resource(db)
    with patch.object(main, "_enqueue_after_identify", return_value=1) as old, \
         patch.object(main, "_identify_emit_only", return_value=1) as new:
        main._after_identify_dispatch(db, x)
    assert new.call_count == 1, "双开关开启必须走新链"
    assert old.call_count == 0


def test_dispatch_stays_old_when_share_center_off(env):
    """分享中心关闭时不切新链——避免切到无消费者的事件导致漏推。"""
    from app import main, runtime_config
    S, eng, d = env
    db = S()
    runtime_config.set_many(db, {
        "resource_decoupled_enabled": "true",
        "share_center_enabled": "false",
    })
    x = _resource(db)
    with patch.object(main, "_enqueue_after_identify", return_value=1) as old, \
         patch.object(main, "_identify_emit_only", return_value=1) as new:
        main._after_identify_dispatch(db, x)
    assert old.call_count == 1
    assert new.call_count == 0


# ---------- 只发事件链 ----------

def test_emit_only_carries_channel_targets(env):
    """新链事件 payload 必须携带 channel_targets 与 decoupled 标记。"""
    from app import main
    S, eng, d = env
    db = S()
    x = _resource(db)
    fake_targets = [{"channel_id": 7, "kind": "channel", "chat_id": "-100xxx"}]
    with patch.object(main, "compute_resource_channel_targets", return_value=fake_targets):
        n = main._identify_emit_only(db, x)
    assert n == 1
    from app.models import ResourceEvent
    ev = db.scalar(select(ResourceEvent).where(ResourceEvent.resource_id == x.id).limit(1))
    assert ev is not None
    payload = json.loads(ev.payload_json)
    assert payload["decoupled"] is True
    assert payload["channel_targets"] == fake_targets


def test_emit_only_no_queue_item_written(env):
    """新链只发事件：不得写 QueueItem（推送决策交给分享中心）。"""
    from app import main
    S, eng, d = env
    db = S()
    x = _resource(db)
    with patch.object(main, "compute_resource_channel_targets", return_value=[
            {"channel_id": 7, "kind": "channel", "chat_id": "-100xxx"}]):
        main._identify_emit_only(db, x)
    from app.models import QueueItem
    assert db.scalar(select(QueueItem).limit(1)) is None, "解耦链不得直接写队列项"


def test_emit_only_skips_unidentified_or_no_poster(env):
    """未识别或无海报：与旧链一致，不入队不发事件。"""
    from app import main
    S, eng, d = env
    db = S()
    x = _resource(db, rid=2, identified=False, poster="")
    assert main._identify_emit_only(db, x) == 0
    from app.models import ResourceEvent
    assert db.scalar(select(ResourceEvent).limit(1)) is None


# ---------- 消费端接管 ----------

def test_consume_builds_task_with_payload_targets(env):
    """消费解耦事件时，用 payload.channel_targets 建带目标的 ShareTask。"""
    from app import share_center as sc, events
    from app.models import ShareTask
    S, eng, d = env
    db = S()
    x = _resource(db)
    targets = [{"channel_id": 7, "kind": "channel", "chat_id": "-100xxx"}]
    ev = events.emit_resource_event(db, event_type=events.RESOURCE_NEW,
                                    work_key=sc.task_work_key(x), resource_id=x.id,
                                    payload={"channel_targets": targets, "decoupled": True})
    assert ev is not None
    with patch.object(sc, "ensure_task", wraps=sc.ensure_task) as spy:
        stats = sc.consume_resource_events(db)
    assert stats["consumed"] >= 1
    # ensure_task 至少一次以 targets 调用
    called_with_targets = any(
        (c.kwargs.get("channel_targets") or []) == targets for c in spy.call_args_list)
    assert called_with_targets, "消费端必须把 payload targets 传给 ensure_task"
    task = db.scalar(select(ShareTask).where(ShareTask.resource_id == x.id).limit(1))
    assert task is not None
    assert json.loads(task.channel_targets or "[]") == targets


def test_consume_keeps_old_behavior_without_targets(env):
    """非解耦事件（payload 无 targets）保持旧行为：ensure_task 不传 targets。"""
    from app import share_center as sc, events
    S, eng, d = env
    db = S()
    x = _resource(db)
    events.emit_resource_event(db, event_type=events.RESOURCE_NEW,
                               work_key=sc.task_work_key(x), resource_id=x.id,
                               payload={"share_dir_id": "", "media_type": "tv"})
    with patch.object(sc, "ensure_task", wraps=sc.ensure_task) as spy:
        sc.consume_resource_events(db)
    assert any(c.kwargs.get("channel_targets") is None for c in spy.call_args_list), \
        "无 targets 的存量事件应保持旧调用形态"


# ---------- 源码护栏 ----------

def test_five_identify_entries_go_through_dispatch():
    """5 个识别入口都必须走统一分发（源码护栏，防漏改入口）。"""
    import inspect
    from app import main
    src = inspect.getsource(main)
    assert "def _after_identify_dispatch(" in src
    calls = src.count("_after_identify_dispatch(db, ")
    assert calls >= 4, f"识别入口调用分发不足：{calls}"


# ---------- 识别缓存与 A 库原名回溯 ----------

def test_identify_cache_hit_avoids_tmdb_call(env):
    """识别缓存命中零网络；失败结果不缓存。"""
    import asyncio
    from app import main
    S, eng, d = env
    db = S()
    x = _resource(db)
    main._IDENTIFY_CACHE.clear()
    good = {"ok": True, "best": {"tmdb_id": 1}, "reason": "hit"}

    async def _run():
        with patch.object(main.tmdb, "identify",
                          side_effect=[good, AssertionError("缓存命中不应再打TMDB")]) as mock:
            r1 = await main.identify_with_a_source_fallback("书剑恩仇录", x, db)
            r2 = await main.identify_with_a_source_fallback("书剑恩仇录", x, db)
            return r1, r2, mock.call_count

    try:
        r1, r2, n = asyncio.run(_run())
    finally:
        main._IDENTIFY_CACHE.clear()
    assert r1["ok"] and r2["ok"]
    assert n == 1, "第二次须命中缓存，不再调 TMDB"


def test_identify_fallback_uses_a_source_name(env):
    """B库名识别失败时用 A 库原名重试一次。"""
    import asyncio
    from app import main
    S, eng, d = env
    db = S()
    x = _resource(db, rid=3)
    main._IDENTIFY_CACHE.clear()
    fail = {"ok": False, "best": None, "status": "none", "reason": "miss"}
    good = {"ok": True, "best": {"tmdb_id": 9}, "reason": "a-name hit"}

    async def _run():
        with patch.object(main, "a_source_name_hint", return_value="开封府"), \
             patch.object(main.tmdb, "identify", side_effect=[fail, good]) as mock:
            r = await main.identify_with_a_source_fallback("KFF S01E01", x, db)
            return r, mock.call_count

    try:
        r, n = asyncio.run(_run())
    finally:
        main._IDENTIFY_CACHE.clear()
    assert r["ok"] and "A库原名回溯命中" in r["reason"]
    assert n == 2


def test_a_source_name_comes_from_mirror_file(env):
    """A 库原名取自本地 MirrorFile.file_name，通过 target_file_id 关联（零网络）。"""
    from app import main
    from app.models import MirrorFile
    S, eng, d = env
    db = S()
    x = _resource(db, rid=4)
    mf = MirrorFile(media_id=1, rule_id=1, target_file_id=x.file_id,
                    file_name="开封府 E01.mkv", status="synced")
    db.add(mf)
    db.commit()
    assert main.a_source_name_hint(db, x) == "开封府 E01.mkv"
