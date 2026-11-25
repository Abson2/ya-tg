"""v2.1.39（真机反馈）：一轮任务结束后，流水线快照必须归位，不能永久停在「进行中」。

现场：规则 `last_status=success`、批次已 idle，但流水线快照还是
`running=true / stage=copy`、`copied=0 / copy_tasks=0 / works_done=0`。
页面「处理流程」因此一直显示「复制中」，看着像还有任务在跑 —— 实际早就结束了。

根因（定时镜像 `_scheduled_mirror`）：
  · 中间帧 `stage="copy", running=True`（1777）是**先 commit** 的；
  · 终态帧只在两条正常分支写（入库识别后 / 关入库或改名门禁）；
  · 任何一步抛异常 → `except` 只写 `last_status / last_message`，**不写 running=False**
    → 快照永久停在 running=true，直到下一轮成功才被覆盖；
  · 规则被停用 / 目录 ID 被清空 → 连 1777 都不执行，旧残留同样没人归位。

手动路径（`_do_mirror_run` 的两个 except）一直有这一帧，定时路径漏了，属疏漏。

修复前本文件应当红（快照 running 仍为 true）。
"""
import asyncio
import json

import pytest
from sqlalchemy import select
from unittest.mock import patch

from app import main as M
from app.db import SessionLocal, migrate
from app.models import (
    AppConfig, AuditLog, Bot, Channel, MirrorDir, MirrorFile, MirrorMedia,
    MirrorRule, MirrorTask, QueueItem, Resource, ScanTask,
)

TABLES = [MirrorFile, MirrorTask, MirrorDir, MirrorMedia, MirrorRule, QueueItem,
          Resource, ScanTask, Channel, Bot, AppConfig, AuditLog]


def _clean():
    with SessionLocal() as db:
        for t in TABLES:
            try:
                db.query(t).delete()
            except Exception:
                db.rollback()
        db.commit()


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    _clean()
    yield
    _clean()


def _rule(db, *, enabled=True, src="A_ROOT", tgt="B_ROOT", snapshot=None):
    r = MirrorRule(name="测试镜像", source_root_id=src, target_root_id=tgt,
                   interval_minutes=60, enabled=enabled, auto_create_dirs=True,
                   star_naming=True, file_idle_seconds=0.01)
    if snapshot is not None:
        r.last_pipeline_json = json.dumps(snapshot, ensure_ascii=False)
    db.add(r)
    db.commit()
    return r.id


def _snap(rule_id):
    with SessionLocal() as db:
        r = db.get(MirrorRule, rule_id)
        return json.loads(str(r.last_pipeline_json or "{}") or "{}")


def _run_scheduled():
    asyncio.run(M._scheduled_mirror())


# ---------------------------------------------------------------------------
# ① 核心：同步阶段抛异常 → 必须写 running=False 的失败帧
# ---------------------------------------------------------------------------
def test_exception_writes_failed_snapshot_instead_of_staying_running():
    with SessionLocal() as db:
        rid = _rule(db)

    with patch("app.mirror.sync_rule", side_effect=RuntimeError("模拟同步炸了")):
        _run_scheduled()

    snap = _snap(rid)
    assert snap.get("running") is False, (
        f"一轮已经异常结束了，快照还停在 running={snap.get('running')!r}"
        f"（stage={snap.get('stage')!r}）—— 页面会一直显示「进行中」，"
        f"看着像还有任务在跑")
    assert snap.get("stage") == "failed", snap

    with SessionLocal() as db:
        r = db.get(MirrorRule, rid)
        assert r.last_status == "failed", r.last_status


# ---------------------------------------------------------------------------
# ② 改名 / 入库识别阶段抛异常，同样要归位（中间帧已经 commit 过了）
# ---------------------------------------------------------------------------
def test_exception_after_copy_stage_also_finishes_snapshot():
    with SessionLocal() as db:
        rid = _rule(db)

    async def ok_sync(db_, r_):
        return {"done": 3, "media": 1, "batch_size": 1, "new": 3}

    async def boom(*a, **kw):
        raise RuntimeError("模拟识别炸了")

    with patch("app.mirror.sync_rule", ok_sync), \
         patch("app.mirror.backfill_target_file_ids", boom):
        _run_scheduled()

    snap = _snap(rid)
    assert snap.get("running") is False, snap
    assert snap.get("stage") == "failed", snap


# ---------------------------------------------------------------------------
# ③ 规则停用 / 目录被清空 → 旧残留的「进行中」同样要归位
# ---------------------------------------------------------------------------
def test_disabled_rule_resets_stale_running_snapshot():
    with SessionLocal() as db:
        rid = _rule(db, enabled=False,
                    snapshot={"mode": "定时同步", "running": True,
                              "stage": "copy", "copied": 3})

    _run_scheduled()

    snap = _snap(rid)
    assert snap.get("running") is False, (
        "规则停用后没人再跑，残留的「进行中」快照永远不会被覆盖 —— "
        "页面会一直显示在跑")
    assert snap.get("stage") == "skipped", snap


def test_active_rule_without_roots_resets_stale_running_snapshot():
    with SessionLocal() as db:
        rid = _rule(db, src="", tgt="",
                    snapshot={"mode": "定时同步", "running": True, "stage": "copy"})

    _run_scheduled()

    assert _snap(rid).get("running") is False, _snap(rid)


# ---------------------------------------------------------------------------
# ④ 反证保护：旧快照本来就不在 running 时，绝不能把上一轮的成功计数清零
# ---------------------------------------------------------------------------
def test_finished_snapshot_is_left_untouched():
    done = {"mode": "定时同步", "running": False, "stage": "done",
            "copied": 5, "copy_tasks": 5, "works_done": 4}
    with SessionLocal() as db:
        rid = _rule(db, enabled=False, snapshot=dict(done))

    _run_scheduled()

    snap = _snap(rid)
    assert snap.get("copied") == 5 and snap.get("stage") == "done", (
        f"归位动作把上一轮已完成的快照覆盖了（现在 {snap}）—— 看板会倒退回 0")


# ---------------------------------------------------------------------------
# ⑤ 正常路径不受影响：仍然写 done / running=False
# ---------------------------------------------------------------------------
def test_success_path_still_writes_done_snapshot():
    with SessionLocal() as db:
        rid = _rule(db)

    async def ok_sync(db_, r_):
        return {"done": 0, "media": 0, "batch_size": 0, "new": 0}

    async def ok_backfill(db_, r_):
        return 0

    async def ok_pipeline(db_, r_):
        return {"files": 0, "queued": 0, "changes_pending": 0,
                "identify": {"identified": 0, "work_stats": {}}, "enqueue_diag": {}}

    with patch("app.mirror.sync_rule", ok_sync), \
         patch("app.mirror.backfill_target_file_ids", ok_backfill), \
         patch("app.main._mirror_post_sync_pipeline", ok_pipeline):
        _run_scheduled()

    snap = _snap(rid)
    assert snap.get("running") is False, snap
    assert snap.get("stage") == "done", snap
