"""v2.1.54 验收：网络 I/O 期间**不得持有 DB 连接**。

真机事故（2026-11-27）的另一半成因：镜像流程整轮只提交一次（`sync_rule` 944 行里
唯一的 `db.commit()` 在函数末尾），于是「读几行配置 → 递归列目录几十分钟 → 再写库」
这种顺序会让一条连接被占住整轮。加上 `poll_pending_tasks` 每部任务要 sleep+网络 10 秒、
`backfill_target_file_ids` 每部都列一次 B 库，连接池就被长任务慢慢吃光。

本文件不看代码长什么样，只在**网络调用真正发生的那一刻**去读
`engine.pool.checkedout()`：
  - 如果为 0，说明事务已在网络前收口、连接已归还 —— 正确；
  - 如果大于 0，说明手里还攥着连接 —— 正是要根治的形态。

这类断言改前必红：旧代码在 `_walk_source` / `_poll_task` 被调用时，连接都还在手上。
"""
import asyncio

import pytest

from app import mirror
from app.db import SessionLocal, engine, migrate
from app.models import (AppConfig, AuditLog, Bot, Channel, MirrorFile, MirrorMedia,
                        MirrorRule, MirrorTask, QueueItem, Resource, ScanTask)

TABLES = [MirrorFile, MirrorTask, MirrorMedia, MirrorRule, QueueItem, Resource,
          ScanTask, Channel, Bot, AppConfig, AuditLog]


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


@pytest.fixture()
def rule_id():
    with SessionLocal() as db:
        r = MirrorRule(name="t", source_root_id="A_ROOT", target_root_id="B_ROOT")
        db.add(r)
        db.commit()
        return r.id


def test_walk_source_is_called_without_holding_a_connection(monkeypatch, rule_id):
    """列目录是全流程最长的网络段（轻量轮几分钟、全量轮几十分钟），
    进它之前连接必须已经归还。"""
    seen = {}

    async def fake_walk(*args, **kwargs):
        seen["checkedout"] = engine.pool.checkedout()
        # 返回「不完整」让 sync_rule 立刻中止，不必把后面整条链路都构造出来
        return {}, False

    monkeypatch.setattr(mirror, "_walk_source", fake_walk)
    monkeypatch.setattr(mirror, "GuangYaWebFiles", lambda db=None: object())

    with SessionLocal() as db:
        rule = db.get(MirrorRule, rule_id)
        try:
            asyncio.run(mirror.sync_rule(db, rule, max_works=1))
        except Exception:
            pass          # 分页不完整 → 抛错中止，这里只关心"进网络前有没有还连接"

    assert "checkedout" in seen, "根本没走到 _walk_source，用例失效"
    assert seen["checkedout"] == 0, (
        f"进 _walk_source 时仍持有 {seen['checkedout']} 条连接 —— "
        "长任务又会整轮占着池，2026-11-27 的假死会复现")


def test_poll_task_is_called_without_holding_a_connection(monkeypatch, rule_id):
    """轮询复制任务：单个任务 5 轮 × (sleep 2s + 网络)，tasks 一多就是几十秒到几分钟。"""
    seen = {}

    with SessionLocal() as db:
        media = MirrorMedia(rule_id=rule_id, work_key="wk1", title_standard="测试",
                            target_dir_id="B_W1")
        db.add(media)
        db.commit()
        db.add(MirrorTask(rule_id=rule_id, media_id=media.id, task_id="t1", status="pending"))
        db.commit()

    async def fake_poll(api, task, **kwargs):
        seen["checkedout"] = engine.pool.checkedout()
        task.status = "unknown"
        return False

    monkeypatch.setattr(mirror, "_poll_task", fake_poll)
    monkeypatch.setattr(mirror, "GuangYaWebFiles", lambda db=None: object())

    with SessionLocal() as db:
        rule = db.get(MirrorRule, rule_id)
        asyncio.run(mirror.poll_pending_tasks(db, rule))

    assert "checkedout" in seen, "根本没走到 _poll_task，用例失效"
    assert seen["checkedout"] == 0, (
        f"轮询网络期间仍持有 {seen['checkedout']} 条连接 —— "
        "这正是 share_center 从 21:44 起一轮不结束的那个形态")


def test_backfill_is_called_without_holding_a_connection(monkeypatch, rule_id):
    """补 fileId：每部作品列一次 B 库，几十部就是几分钟。"""
    seen = {}

    with SessionLocal() as db:
        media = MirrorMedia(rule_id=rule_id, work_key="wk2", title_standard="测试2",
                            target_dir_id="B_W2")
        db.add(media)
        db.commit()
        db.add(MirrorFile(media_id=media.id, rule_id=rule_id, source_file_id="f1",
                          fingerprint="100", status="synced", target_file_id=""))
        db.commit()

    async def fake_cands(api, media):
        seen["checkedout"] = engine.pool.checkedout()
        return {}, True

    monkeypatch.setattr(mirror, "_b_file_candidates", fake_cands)
    monkeypatch.setattr(mirror, "GuangYaWebFiles", lambda db=None: object())

    with SessionLocal() as db:
        rule = db.get(MirrorRule, rule_id)
        asyncio.run(mirror.backfill_target_file_ids(db, rule))

    assert "checkedout" in seen, "根本没走到 _b_file_candidates，用例失效"
    assert seen["checkedout"] == 0, (
        f"列 B 库期间仍持有 {seen['checkedout']} 条连接")
