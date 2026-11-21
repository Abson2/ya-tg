"""阶段二路线B C3：严格顺序镜像执行器。

核心断言：
  1. 严格顺序——下一个 copy_file 必须在上一个任务确认 done 之后才发起（事件序列）；
  2. 每文件立即 commit、任务序列落库（batch_id/sequence_id/task_status）；
  3. 暂停指令在任务边界生效，余下任务保持 pending、批次状态 paused；
  4. 跳过指令推进且不复制；取消指令终止余下；
  5. 未知状态保留任务供续跑、严格模式下不破坏 done 统计。
用独立临时库 + FakeAPI，不碰生产网盘。
"""
import asyncio
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app import db as dbmod
from app import mirror, mirror_batch, models  # noqa: F401

A_ROOT, B_ROOT, A_WORK, B_WORK = "A_ROOT", "B_ROOT", "A_W1", "B_W1"


class StrictAPI:
    """记录事件序列；前 N 次 get_task_status 返回 pending，之后 done。"""

    def __init__(self, db, events, pending_times=1):
        self.db = db
        self.events = events
        self.pending_times = pending_times
        self.polls = 0
        self.copies = 0
        self.b_files = []

    async def list_files(self, parent_id, page=0, page_size=100):
        if page > 0:
            return {"list": [], "total": 0}
        if parent_id == A_ROOT:
            return {"list": [{"fileId": A_WORK, "resType": 2,
                              "fileName": "侠客行 (2001) {tmdb-135646}"}], "total": 1}
        if parent_id == A_WORK:
            return {"list": [
                {"fileId": "F1", "resType": 1, "fileName": "X.S01E01.mkv",
                 "fileSize": 100, "utime": 1, "parentId": A_WORK},
                {"fileId": "F2", "resType": 1, "fileName": "X.S01E02.mkv",
                 "fileSize": 100, "utime": 1, "parentId": A_WORK},
            ], "total": 2}
        if parent_id == B_ROOT:
            return {"list": [{"fileId": B_WORK, "resType": 2,
                              "fileName": "侠客行 (2001) {tmdb-135646}"}], "total": 1}
        if parent_id == B_WORK:
            return {"list": [dict(f) for f in self.b_files], "total": len(self.b_files)}
        return {"list": [], "total": 0}

    async def copy_file(self, file_ids, parent_id):
        self.copies += 1
        # v1.4：strict 目录级批量，一次 copy_file 带同目录全部 fileId（旧版每次 1 个）
        self.events.append(("copy", list(file_ids), self.copies))
        for fid in file_ids:
            self.b_files.append({"fileId": f"BF{self.copies}_{fid}", "resType": 1,
                                 "fileName": f"{fid}.mkv", "fileSize": 100,
                                 "utime": 1, "parentId": parent_id})
        return f"TASK{self.copies}"

    async def get_task_status(self, task_id):
        self.polls += 1
        if self.polls <= self.pending_times:
            self.events.append(("poll-pending", task_id))
            return {"status": 1, "detail": {}}
        self.events.append(("poll-done", task_id))
        return {"status": 2, "detail": {}}


@pytest.fixture
def env():
    root = Path(tempfile.mkdtemp(prefix="v100_strict_", dir="/tmp"))
    eng = create_engine(f"sqlite:///{root / 's.db'}")
    dbmod.Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False, autocommit=False)
    with S() as db:
        rule = models.MirrorRule(name="t", source_root_id=A_ROOT, target_root_id=B_ROOT,
                                 interval_minutes=60, enabled=True, file_idle_seconds=0.0)
        db.add(rule)
        db.commit()
        db.refresh(rule)
        yield db, rule, S, eng
    eng.dispose()
    shutil.rmtree(root, ignore_errors=True)


def _api_factory(events, pending_times):
    def make(db):
        return StrictAPI(db, events, pending_times)
    return make


def test_strict_sequence_copy_after_confirm(env):
    db, rule, _, _ = env
    events = []
    api_factory = _api_factory(events, pending_times=1)
    with patch("app.mirror.GuangYaWebFiles", api_factory):
        stats = asyncio.run(mirror.sync_rule(db, rule, strict=True,
                                             max_polls=5, poll_interval=0.0))
    assert stats["done"] == 2, stats
    # v1.4 目录级批量：同目录 2 文件合并为 1 次 copy_file，确认后才算完成。
    copies = [(i, e) for i, e in enumerate(events) if e[0] == "copy"]
    assert len(copies) == 1, "同目录两文件应一次批量提交"
    assert set(copies[0][1][1]) == {"F1", "F2"}, events  # 一次带两个 fileId
    # 批量提交后必须 poll 到 done 才落终态
    assert any(e[0] == "poll-done" and e[1] == "TASK1" for e in events), events
    # 任务序列落库：每个文件仍占一个 sequence（顺序记录粒度不变）
    tasks = list(db.scalars(select(models.MirrorTask).order_by(models.MirrorTask.sequence_id)).all())
    assert len(tasks) == 2
    assert [t.sequence_id for t in tasks] == [1, 2]
    assert all(t.task_status == "completed" for t in tasks)
    assert all(t.batch_id == stats["batch_id"] for t in tasks)
    assert rule.batch_status == "idle"


def test_strict_sequence_files_in_natural_order(env):
    db, rule, _, _ = env
    events = []
    with patch("app.mirror.GuangYaWebFiles", _api_factory(events, pending_times=0)):
        asyncio.run(mirror.sync_rule(db, rule, strict=True, max_polls=5, poll_interval=0.0))
    copies = [e[1] for e in events if e[0] == "copy"]
    # v1.4：同目录一次批量，fileId 保持自然序 F1→F2
    assert len(copies) == 1 and copies[0] == ["F1", "F2"], events


def test_strict_sequence_pause_at_task_boundary(env):
    db, rule, _, _ = env
    events = []
    api_factory = _api_factory(events, pending_times=0)

    # v1.4 目录级批量：同目录只有 1 个桶边界。首边界派发并消费 pause → 整桶不复制。
    def control_pause(d, r):
        r.control_action = ""
        d.commit()
        return "pause"

    with patch("app.mirror.mirror_batch.consume_control", side_effect=control_pause), \
         patch("app.mirror.GuangYaWebFiles", api_factory):
        stats = asyncio.run(mirror.sync_rule(db, rule, strict=True,
                                             max_polls=5, poll_interval=0.0))
    assert stats.get("paused") is True
    assert rule.batch_status == "paused"
    # pause 在首个（唯一）桶边界生效：不复制任何文件
    copies = [e for e in events if e[0] == "copy"]
    assert len(copies) == 0, events


def test_strict_sequence_skip_and_cancel(env):
    db, rule, _, _ = env
    events = []
    # skip 场景：第一个边界就 skip，第一个文件不复制，第二个文件复制
    calls = {"n": 0}

    def control_skip_once(d, r):
        calls["n"] += 1
        if calls["n"] == 1:
            r.control_action = ""
            return "skip"
        return ""

    with patch("app.mirror.mirror_batch.consume_control", control_skip_once), \
         patch("app.mirror.GuangYaWebFiles", _api_factory(events, pending_times=0)):
        stats = asyncio.run(mirror.sync_rule(db, rule, strict=True,
                                             max_polls=5, poll_interval=0.0))
    # v1.4：skip 是目录级——首（唯一）桶整桶跳过，不复制任何文件
    copies = [e[1] for e in events if e[0] == "copy"]
    assert len(copies) == 0, events
    assert stats["skipped"] >= 2, stats  # 整桶 2 文件都标 skipped


def test_strict_sequence_cancel_stops_all(env):
    db, rule, _, _ = env
    events = []
    original_control = mirror_batch.consume_control

    def control_cancel(d, r):
        r.control_action = "cancel"
        d.commit()
        return original_control(d, r)

    with patch("app.mirror.mirror_batch.consume_control", control_cancel), \
         patch("app.mirror.GuangYaWebFiles", _api_factory(events, pending_times=0)):
        stats = asyncio.run(mirror.sync_rule(db, rule, strict=True,
                                             max_polls=5, poll_interval=0.0))
    assert stats.get("cancelled") is True
    assert rule.batch_status == "cancelled"
    assert [e for e in events if e[0] == "copy"] == [], events  # 无任何复制
