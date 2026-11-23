"""阶段二路线B C3：镜像复制执行器（v2.1.13 起为**单一路径**）。

v2.1.13 变更：严格顺序引擎（逐文件占位序列 + 续跑游标）已删除。
  - 处理顺序由光鸭网盘自身的文件名升序决定（`guangya.LIST_ORDER_BY/LIST_SORT_TYPE`，
    v2.1.12 #296 落地），本地不再重排、也不再靠「确认完再提交下一个」保证顺序；
  - 只剩一套复制路径：作品内按目标目录分桶 → 批量 copy_file → 批量轮询；
  - 批次控制（暂停 / 恢复 / 跳过当前目录桶 / 取消批次）下沉到**桶边界**，常态生效。

核心断言：
  1. 同目录多文件合并为一次 copy_file，poll 到 done 后才落终态 synced；
  2. 一个复制请求落一条 MirrorTask（续跑依据是 `status`，不再是 `task_status` 游标）；
  3. 暂停指令在桶边界生效，本桶不复制、批次状态 paused；
  4. 跳过指令按目录桶整桶跳过且不复制；取消指令不发起任何复制；
  5. `strict=True` 作为兼容入参仍可调通（旧前端 `?strict=1`），但不改变任何行为。
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


class OrderAPI:
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
        return OrderAPI(db, events, pending_times)
    return make


def test_single_path_batch_copy_then_synced(env):
    """同目录两文件 → 一次批量 copy_file → 确认 done → 整批 synced、批次收尾 idle。

    v2.1.13：只剩这一套复制路径，因此：
      - `stats["done"]` 的计数单位是**复制任务**（本场景 1 条），不再是文件数；
      - `MirrorTask.task_status` 不再使用（那是旧严格引擎的逐文件续跑游标载体），
        续跑依据改为 `MirrorTask.status`（done / failed / unknown）。
    """
    db, rule, _, _ = env
    events = []
    api_factory = _api_factory(events, pending_times=1)
    with patch("app.mirror.GuangYaWebFiles", api_factory):
        stats = asyncio.run(mirror.sync_rule(db, rule, max_polls=5, poll_interval=0.0))
    assert stats["done"] == 1, stats          # 1 个复制任务（含 2 个文件）
    assert stats["failed"] == 0, stats
    assert stats["unknown"] == 0, stats
    # 同目录 2 文件合并为 1 次 copy_file，确认后才算完成。
    copies = [e for e in events if e[0] == "copy"]
    assert len(copies) == 1, "同目录两文件应一次批量提交"
    assert set(copies[0][1]) == {"F1", "F2"}, events  # 一次带两个 fileId
    # 批量提交后必须 poll 到 done 才落终态
    assert any(e[0] == "poll-done" and e[1] == "TASK1" for e in events), events
    # 任务落库：一个复制请求一条 MirrorTask
    tasks = list(db.scalars(select(models.MirrorTask)).all())
    assert len(tasks) == 1, "单一路径下不再逐文件占位 sequence"
    assert tasks[0].status == "done", tasks[0].status
    assert tasks[0].task_status == "", "task_status 是旧严格引擎的游标字段，默认路径不再写"
    # 两个文件都拿到终态
    rows = list(db.scalars(select(models.MirrorFile)).all())
    assert len(rows) == 2
    assert all(r.status == "synced" for r in rows), [r.status for r in rows]
    assert all(r.error == "" for r in rows)
    assert rule.batch_status == "idle"


def test_single_path_files_keep_server_order(env):
    db, rule, _, _ = env
    events = []
    with patch("app.mirror.GuangYaWebFiles", _api_factory(events, pending_times=0)):
        asyncio.run(mirror.sync_rule(db, rule, max_polls=5, poll_interval=0.0))
    copies = [e[1] for e in events if e[0] == "copy"]
    # 同目录一次批量，fileId 保持网盘返回序（顺序真源在 guangya.LIST_ORDER_BY/LIST_SORT_TYPE）
    assert len(copies) == 1 and copies[0] == ["F1", "F2"], events


def test_single_path_pause_at_bucket_boundary(env):
    db, rule, _, _ = env
    events = []
    api_factory = _api_factory(events, pending_times=0)

    # 同目录只有 1 个桶边界。首边界消费 pause → 整桶不复制。
    def control_pause(d, r):
        r.control_action = ""
        d.commit()
        return "pause"

    with patch("app.mirror.mirror_batch.consume_control", side_effect=control_pause), \
         patch("app.mirror.GuangYaWebFiles", api_factory):
        stats = asyncio.run(mirror.sync_rule(db, rule, max_polls=5, poll_interval=0.0))
    assert stats.get("paused") is True
    assert rule.batch_status == "paused"
    # pause 在首个（唯一）桶边界生效：不复制任何文件
    copies = [e for e in events if e[0] == "copy"]
    assert len(copies) == 0, events


def test_single_path_skip_and_cancel(env):
    db, rule, _, _ = env
    events = []
    # skip 场景：第一个边界就 skip → 第一个目录桶整桶不复制
    calls = {"n": 0}

    def control_skip_once(d, r):
        calls["n"] += 1
        if calls["n"] == 1:
            r.control_action = ""
            return "skip"
        return ""

    with patch("app.mirror.mirror_batch.consume_control", control_skip_once), \
         patch("app.mirror.GuangYaWebFiles", _api_factory(events, pending_times=0)):
        stats = asyncio.run(mirror.sync_rule(db, rule, max_polls=5, poll_interval=0.0))
    # skip 是目录级——首（唯一）桶整桶跳过，不复制任何文件
    copies = [e[1] for e in events if e[0] == "copy"]
    assert len(copies) == 0, events
    assert stats["skipped"] >= 2, stats  # 整桶 2 文件都标 skipped
    # v2.1.13：skip 按文件落 skipped 序列行并推进游标，否则「恢复被跳过项」是死按钮
    skipped_rows = list(db.scalars(select(models.MirrorTask).where(
        models.MirrorTask.task_status == "skipped")).all())
    assert len(skipped_rows) >= 2, [t.task_status for t in skipped_rows]
    assert rule.cursor_sequence >= 2


def test_single_path_cancel_stops_all(env):
    db, rule, _, _ = env
    events = []
    original_control = mirror_batch.consume_control

    def control_cancel(d, r):
        r.control_action = "cancel"
        d.commit()
        return original_control(d, r)

    with patch("app.mirror.mirror_batch.consume_control", control_cancel), \
         patch("app.mirror.GuangYaWebFiles", _api_factory(events, pending_times=0)):
        stats = asyncio.run(mirror.sync_rule(db, rule, max_polls=5, poll_interval=0.0))
    assert stats.get("cancelled") is True
    assert rule.batch_status == "cancelled"
    assert [e for e in events if e[0] == "copy"] == [], events  # 无任何复制


def test_strict_param_accepted_but_ignored(env):
    """v2.1.13：`strict=True` 仅为兼容旧前端/旧脚本而保留，不得改变任何行为。"""
    db, rule, _, _ = env
    events = []
    with patch("app.mirror.GuangYaWebFiles", _api_factory(events, pending_times=0)):
        stats = asyncio.run(mirror.sync_rule(db, rule, strict=True,
                                             max_polls=5, poll_interval=0.0))
    assert stats["done"] == 1, stats
    # 仍是「同目录一次批量」，没有退化成逐文件提交
    copies = [e[1] for e in events if e[0] == "copy"]
    assert len(copies) == 1 and copies[0] == ["F1", "F2"], events
    # 严格顺序引擎的执行器必须已经不存在（防止被误加回来）
    assert not hasattr(mirror, "_copy_jobs_strict")
    assert not hasattr(mirror, "_wait_one_task")
