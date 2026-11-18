"""v0.8.5 回归：镜像完成 → 刷新入库 → 识别 → 只推本轮识别成功的影视。

用户 2026-11-18 三轮明确口径：
  1. 「同步完没有自动触发资源库扫描入库」；
  2. 「扫描入库完，识别成功（获取到海报），自动按照 tg 分发规则推送」；
  3. 「镜像完成不是群推送，镜像完成，触发资源库入库刷新，识别影视，识别成功的影视就推送」。

第 3 条反转了 v0.8.0~v0.8.4 写死的 V2.0 第十章「镜像完成不自动推 TG」，但**没有**
放开两道闸门：不得全库群发、不得批量补推历史 queue_id=0 的内容变化事件。

锁死的实现契约：
  - MirrorRule.auto_identify 不再是死配置，手动与定时两条路径都必须读它；
  - 管线复用 _post_scan_identify(publish_after_identify=True)，只认「已识别 + 有海报」；
  - 绝不调用 process_change_events；返回的 changes_pending 只是只读统计；
  - 作品级去重沿用 _enqueue_ready，已有队列项不重复入队。
"""
import asyncio
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from app import main
from app.db import SessionLocal, migrate
from app.models import (AuditLog, Bot, Channel, DistributionEvent, MirrorRule,
                        QueueItem, Resource, Rule)
from app.security import encrypt_secret

ROOT = Path(__file__).resolve().parents[1]
A_ROOT, B_ROOT = "A_ROOT", "B_ROOT"

_SYNC_STATS = {"media": 1, "new": 1, "changed": 0, "unchanged": 0, "done": 1,
               "failed": 0, "unknown": 0, "retries": 0}
_SCAN = {"files": 3, "directories": 2, "complete": True, "seen_file_ids": ["F1"]}
_BEST = {"tmdb_id": 135646, "media_type": "movie", "title": "大醉侠",
         "poster_path": "/poster.jpg", "finish_status": ""}
_IDENT_OK = {"ok": True, "status": "identified", "source": "tmdb",
             "reason": "测试命中", "best": _BEST}
_IDENT_FAIL = {"ok": False, "status": "failed", "source": "tmdb",
               "reason": "无候选", "best": None}

TABLES = [QueueItem, Rule, Channel, Bot, DistributionEvent, AuditLog,
          Resource, MirrorRule]


def _clean():
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        db.commit()


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    _clean()
    yield
    _clean()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as session:
        try:
            yield session
        finally:
            session.rollback()


def _rule(db, *, auto_identify=True, enabled=True):
    r = MirrorRule(name="v085-镜像", source_root_id=A_ROOT, target_root_id=B_ROOT,
                   interval_minutes=60, enabled=enabled, auto_identify=auto_identify,
                   star_naming=True, auto_create_dirs=True, file_idle_seconds=0.5)
    db.add(r)
    db.commit()
    return r


def _seed(db, *, identify_status="unidentified", poster="", name="大醉侠 (2002).mkv"):
    b = Bot(name="v085-bot", token=encrypt_secret("1:FAKE"), enabled=True)
    db.add(b)
    db.flush()
    c = Channel(name="v085-ch", bot_id=b.id, chat_id="-1001", enabled=True)
    db.add(c)
    db.flush()
    db.add(Rule(name="v085-rule", channel_id=c.id, auto_publish=True, enabled=True))
    x = Resource(file_id="F1", name=name, parent_id="P1",
                 path_hint="大醉侠 (2002)", identify_status=identify_status,
                 poster_path=poster)
    db.add(x)
    db.commit()
    return x


@contextmanager
def _patched(identify_result=None, scan_result=None):
    """把网络/扫描/TMDB 全部隔离，只留 DB 与调度逻辑做真实断言。"""
    m = SimpleNamespace(
        sync=AsyncMock(return_value=dict(_SYNC_STATS)),
        backfill=AsyncMock(return_value=0),
        scan=AsyncMock(return_value=dict(scan_result or _SCAN)),
        identify=AsyncMock(return_value=identify_result if identify_result is not None
                           else dict(_IDENT_OK)),
        changes=MagicMock(),
    )
    with patch("app.main.mirror.sync_rule", new=m.sync), \
         patch("app.main.mirror.backfill_target_file_ids", new=m.backfill), \
         patch("app.main.run_scan", new=m.scan), \
         patch("app.main.tmdb.identify", new=m.identify), \
         patch("app.main.process_change_events", new=m.changes):
        yield m


def _run(coro):
    return asyncio.run(coro)


# ---------- 1. 手动路径：auto_identify 真正生效 ----------
def test_auto_identify_default_runs_pipeline_and_enqueues(db):
    """默认开关：立即同步后必须扫描入库、识别、按分发规则入队。"""
    _rule(db)
    x = _seed(db)
    with _patched() as m:
        out = _run(main.mirror_run(db=db))
    assert out["pipeline_enabled"] is True
    p = out["pipeline"]
    assert p["files"] == 3 and p["complete"] is True
    assert p["identify"]["identified"] == 1 and p["queued"] == 1
    assert m.scan.await_count == 1 and m.identify.await_count == 1
    db.expire_all()
    assert db.get(Resource, x.id).identify_status == "identified"
    q = db.scalar(select(QueueItem))
    assert q is not None and q.resource_id == x.id and q.status == "pending"


def test_auto_identify_off_skips_pipeline(db):
    """关掉开关：立即同步只复制，不扫描、不识别、不入队。"""
    _rule(db, auto_identify=False)
    _seed(db)
    with _patched() as m:
        out = _run(main.mirror_run(db=db))
    assert out["pipeline_enabled"] is False and "pipeline" not in out
    assert m.scan.await_count == 0 and m.identify.await_count == 0
    assert db.scalar(select(QueueItem)) is None


def test_explicit_force_overrides_switch_off(db):
    """「强制入库识别」即使开关关闭也要执行，供排障用。"""
    _rule(db, auto_identify=False)
    _seed(db)
    with _patched() as m:
        out = _run(main.mirror_run(post_identify=True, db=db))
    assert out["pipeline_enabled"] is True and out["pipeline"]["queued"] == 1
    assert m.scan.await_count == 1


def test_explicit_skip_overrides_switch_on(db):
    """「仅同步」即使开关开启也要跳过，用户可随时止血。"""
    _rule(db, auto_identify=True)
    _seed(db)
    with _patched() as m:
        out = _run(main.mirror_run(post_identify=False, db=db))
    assert out["pipeline_enabled"] is False
    assert m.scan.await_count == 0 and db.scalar(select(QueueItem)) is None


# ---------- 2. 推送闸门：只推本轮识别成功且有海报的 ----------
def test_failed_identification_is_not_enqueued(db):
    _rule(db)
    _seed(db)
    with _patched(identify_result=dict(_IDENT_FAIL)):
        out = _run(main.mirror_run(db=db))
    assert out["pipeline"]["queued"] == 0
    assert db.scalar(select(QueueItem)) is None


def test_identified_without_poster_is_not_enqueued(db):
    """识别成功但没海报也不入队（沿用 v0.6.5 闸门，用户明确要求「获取到海报」）。"""
    _rule(db)
    _seed(db)
    no_poster = {**_IDENT_OK, "best": {**_BEST, "poster_path": ""}}
    with _patched(identify_result=no_poster):
        out = _run(main.mirror_run(db=db))
    assert out["pipeline"]["identify"]["identified"] == 1
    assert out["pipeline"]["queued"] == 0
    assert db.scalar(select(QueueItem)) is None


def test_already_identified_resource_is_not_repushed(db):
    """历史已识别资源不在待识别集合里，管线不得回溯推送（防全库群发）。"""
    _rule(db)
    _seed(db, identify_status="identified", poster="/old.jpg")
    with _patched() as m:
        out = _run(main.mirror_run(db=db))
    assert out["pipeline"]["queued"] == 0
    assert m.identify.await_count == 0
    assert db.scalar(select(QueueItem)) is None


def test_existing_queue_item_blocks_duplicate(db):
    """同一作品同频道已有队列项，本轮识别成功也不得再插一条。"""
    _rule(db)
    x = _seed(db)
    c = db.scalar(select(Channel))
    db.add(QueueItem(resource_id=x.id, channel_id=c.id, status="sent"))
    db.commit()
    with _patched():
        out = _run(main.mirror_run(db=db))
    assert out["pipeline"]["queued"] == 0
    assert len(list(db.scalars(select(QueueItem)).all())) == 1


# ---------- 3. 核心红线：不是群推送 ----------
def test_historical_change_events_are_not_batch_published(db):
    """镜像管线绝不调用 process_change_events，历史 queue_id=0 事件保持原样。

    v0.8.0~v0.8.4 刻意积压了内容变化事件；在这里放开会一次性批量补推，
    正是用户「镜像完成不是群推送」明确禁止的行为。
    """
    _rule(db)
    _seed(db)
    db.add(DistributionEvent(work_key="legacy:1", event_type="updated",
                             fingerprint="old-fp", changed_files=2, queue_id=0))
    db.add(DistributionEvent(work_key="legacy:2", event_type="updated",
                             fingerprint="old-fp2", changed_files=1, queue_id=0))
    db.commit()
    with _patched() as m:
        out = _run(main.mirror_run(db=db))
    assert m.changes.call_count == 0, "镜像链路不得处理内容变化事件"
    assert out["pipeline"]["changes_pending"] == 2
    db.expire_all()
    assert all(ev.queue_id == 0 for ev in db.scalars(select(DistributionEvent)).all())
    # 只有本轮新识别的这一条队列项，没有历史事件被转成推送
    assert len(list(db.scalars(select(QueueItem)).all())) == 1


def test_pipeline_does_not_reconcile_or_rebuild_shares(db):
    """管线只扫描入库，不做对账删除、不重建分享。"""
    _rule(db)
    x = _seed(db)
    x.share_url = "https://example.invalid/s/keep"
    db.commit()
    with patch("app.main._reconcile_library", new=AsyncMock()) as rec, _patched():
        _run(main.mirror_run(db=db))
    assert rec.await_count == 0
    db.expire_all()
    assert db.get(Resource, x.id).share_url == "https://example.invalid/s/keep"


# ---------- 4. 定时路径 ----------
def test_scheduled_mirror_runs_pipeline_when_switch_on(db):
    """定时同步也必须接线（v0.8.4 及之前只同步 + backfill，没有扫描）。"""
    _rule(db)
    _seed(db)
    db.commit()
    with _patched() as m:
        _run(main._scheduled_mirror())
    assert m.sync.await_count == 1 and m.scan.await_count == 1
    db.expire_all()
    assert db.scalar(select(QueueItem)) is not None
    assert "自动入库" in (db.scalar(select(MirrorRule)).last_message or "")


def test_scheduled_mirror_respects_switch_off(db):
    _rule(db, auto_identify=False)
    _seed(db)
    db.commit()
    with _patched() as m:
        _run(main._scheduled_mirror())
    assert m.sync.await_count == 1 and m.scan.await_count == 0
    db.expire_all()
    assert db.scalar(select(QueueItem)) is None


# ---------- 5. 口径与版本一致性 ----------
def test_config_status_and_note_reflect_new_policy(db):
    _rule(db, auto_identify=False)
    cfg = main.mirror_config_get(db=db)
    assert cfg["auto_identify"] is False
    assert "识别成功" in cfg["note"] and "全库群发" in cfg["note"]
    assert "不会自动推送 Telegram" not in cfg["note"]
    assert "*" not in cfg["note"]
    st = main.mirror_status(db=db)
    assert st["config"]["auto_identify"] is False
    assert "绝不触发" not in (main._scheduled_mirror.__doc__ or "")
    assert "群推" in (main._mirror_post_sync_pipeline.__doc__ or "")


def test_version_is_current_everywhere():
    """版本一致性以 frontend/package.json 为准；以后发版不必再改这条用例。"""
    pkg = json.loads((ROOT / "frontend/package.json").read_text(encoding="utf-8"))
    v = pkg["version"]
    assert main.app.version == v
    assert main.health()["version"] == v
    assert f'version="{v}"' in (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
    vue = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert f"v{v} · Asia/Shanghai" in vue
    assert "runMirror('auto')" in vue and "runMirror('force')" in vue
    assert "同步并识别（不推送）" not in vue
    assert "v-model=\"mirrorCfg.auto_identify\"" in vue
    assert f"v{v}" in (ROOT / "README.md").read_text(encoding="utf-8")
