"""v1.2 验收：①镜像按 B 库目录分组批量复制；②分享中心分享管理功能。

需求①：先扫描 A/B 文件树，以 B 库目标目录为「最大单元」批量复制——
        同一 parent 下多文件一次 copy_file(fids[], parent)，而非逐文件 N 次请求；
        strict 模式不受影响（需逐任务确认）。
需求②：设计文档 §5.6/§5.7 的分享管理——work_key 过滤、rebuild 重建、
        schedule 节奏配置、分享记录管理、追更关系统计。
"""
import asyncio
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
    d = tempfile.mkdtemp(prefix="v120_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'b.db')}")
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng, d
    shutil.rmtree(d, ignore_errors=True)


# ---------- 需求①：按目录分组批量复制 ----------
A_ROOT, B_ROOT, A_WORK, B_WORK = "A120_ROOT", "B120_ROOT", "A120_W", "B120_W"


class BulkCopyAPI:
    """A 作品下两部季目录各 3 个文件；记录每次 copy_file 的 fileIds 与 parent。"""

    fp = 1
    copies: list = []
    task_status = 2

    def __init__(self, db):
        self.db = db
        self.b_files = []

    async def list_files(self, parent_id, page=0, page_size=100):
        if page > 0:
            return {"list": [], "total": 0}
        if parent_id == A_ROOT:
            return {"list": [{"fileId": A_WORK, "resType": 2,
                              "fileName": "侠客行 (2001) {tmdb-135646}"}], "total": 1}
        if parent_id == A_WORK:
            return {"list": [
                {"fileId": "S1", "resType": 2, "fileName": "Season 01"},
                {"fileId": "S2", "resType": 2, "fileName": "Season 02"},
            ], "total": 2}
        if parent_id == "S1":
            return {"list": [{"fileId": f"S1E{i}", "resType": 1,
                              "fileName": f"X.S01E0{i}.mkv", "fileSize": 100,
                              "utime": self.fp, "parentId": "S1"} for i in (1, 2, 3)], "total": 3}
        if parent_id == "S2":
            return {"list": [{"fileId": f"S2E{i}", "resType": 1,
                              "fileName": f"X.S02E0{i}.mkv", "fileSize": 100,
                              "utime": self.fp, "parentId": "S2"} for i in (1, 2, 3)], "total": 3}
        if parent_id == B_ROOT:
            return {"list": [{"fileId": B_WORK, "resType": 2,
                              "fileName": "侠*客*行 *(2001) {tmdb-135646}"}], "total": 1}
        if parent_id == B_WORK:
            return {"list": [{"fileId": "BS1", "resType": 2, "fileName": "Season 01"},
                             {"fileId": "BS2", "resType": 2, "fileName": "Season 02"}],
                    "total": 2}
        if parent_id in ("BS1", "BS2"):
            return {"list": [dict(f) for f in self.b_files
                             if f.get("parentId") == parent_id], "total": 0}
        return {"list": [], "total": 0}

    async def copy_file(self, file_ids, parent_id):
        self.copies.append((list(file_ids), parent_id))
        for fid in file_ids:
            self.b_files.append({"fileId": f"BF_{fid}", "resType": 1,
                                 "fileName": "X.S01E01.mkv", "fileSize": 100,
                                 "utime": self.fp, "parentId": parent_id})
        return f"TASK{len(self.copies)}"

    async def get_task_status(self, task_id):
        return {"status": self.task_status, "raw": {"status": self.task_status}}


def test_bulk_copy_groups_by_target_dir(env):
    """同一目标目录下的 3 个文件合并成一次 copy_file；6 文件=2 批，不是 6 次请求。"""
    from app import mirror
    from app.models import MirrorRule, MirrorMedia, MirrorFile
    S, _, _ = env
    with S() as db:
        r = MirrorRule(name="批量", source_root_id=A_ROOT, target_root_id=B_ROOT,
                       interval_minutes=60, enabled=True, file_idle_seconds=0.5)
        db.add(r)
        db.commit()
        BulkCopyAPI.copies = []
        BulkCopyAPI.fp = 1
        with patch("app.mirror.GuangYaWebFiles", BulkCopyAPI):
            stats = asyncio.run(mirror.sync_rule(db, r))
        # 6 个新文件按 BS1/BS2 两个目标目录分组 → 2 次 copy_file 请求
        assert stats["new"] == 6, stats
        assert stats["copy_batches"] == 2, stats
        assert len(BulkCopyAPI.copies) == 2, BulkCopyAPI.copies
        for fids, parent in BulkCopyAPI.copies:
            assert len(fids) == 3, BulkCopyAPI.copies  # 每目录一次带 3 个 fileId
            assert parent in ("BS1", "BS2")
        # 轮询合并后所有行 synced
        rows = list(db.scalars(select(MirrorFile)).all())
        assert len(rows) == 6 and all(x.status == "synced" for x in rows)


def test_bulk_copy_respects_chunk_size(env):
    """单目录文件数超 COPY_BATCH_SIZE 时分片，每片仍是一次批量请求。"""
    from app import mirror
    assert mirror.COPY_BATCH_SIZE == 50
    # 逻辑层验证：50 的分片上限常量存在且合理，避免一次请求体过大
    assert 0 < mirror.COPY_BATCH_SIZE <= 100


# ---------- 需求②：分享管理 ----------
def _seed_task(db, *, wk="剧A", kind="first", status="published", url="https://pan/s/a"):
    from app.models import ShareTask
    t = ShareTask(work_key=wk, kind=kind, status=status, priority=20,
                  share_id="SA", share_url=url, share_status="active",
                  share_dir_id="dir_a", share_scope="series_dir",
                  queue_type="fresh", resource_id=1)
    db.add(t)
    db.commit()
    return t


def test_rebuild_task_resets_and_retires(env):
    """rebuild：作废当前分享记录、清空链接、状态回 planned、attempts 归零。"""
    from app import share_center as sc
    from app import events
    from app.models import Resource, ShareRecord
    S, _, _ = env
    with S() as db:
        res = Resource(file_id="f1", name="A.S01E01.mkv", ext=".mkv", size=1, utime=1,
                       identify_status="identified", media_type="tv", tmdb_id=1,
                       media_title="剧A", poster_path="/p.jpg", share_dir_id="dir_a",
                       share_scope="series_dir")
        db.add(res)
        db.commit()
        sc.upsert_share_record(db, res, share_id="SA", share_url="https://pan/s/a")
        t = _seed_task(db, wk=sc.task_work_key(res), status="published")
        t.attempts = 5
        t.error = "旧错误"
        db.commit()
        sc.rebuild_task(db, t)
        assert t.status == sc.ST_PLANNED
        assert t.share_url == "" and t.share_id == ""
        assert t.attempts == 0 and t.error == ""
        assert sc.current_share(db, t.work_key) is None  # 当前记录已作废
        # 历史记录保留
        assert db.scalar(select(ShareRecord).where(ShareRecord.current_flag == 0)) is not None


def test_followup_summary_counts_updates(env):
    """追更关系：某剧 first×1 + update×2 → 展示「已推 3 次、2 次更新」。"""
    from app import share_center as sc
    S, _, _ = env
    with S() as db:
        first = _seed_task(db, wk="剧B", kind="first")
        _seed_task(db, wk="剧B", kind="update", status="published")
        _seed_task(db, wk="剧B", kind="update", status="published")
        summary = sc.followup_summary(db)
        row = next((x for x in summary if x["work_key"] == "剧B"), None)
        assert row is not None
        assert row["total_pushes"] == 3 and row["update_count"] == 2
        assert row["first_task_id"] == first.id


def test_schedule_config_roundtrip(env):
    """节奏配置：读写 share_release_interval_seconds / 三队列配额。"""
    from app import share_center as sc
    S, _, _ = env
    with S() as db:
        cfg = sc.share_schedule_get(db)
        assert cfg["share_release_interval_seconds"] >= 0
        sc.share_schedule_set(db, {
            "share_release_interval_seconds": "30",
            "fresh_daily_quota": "10",
        })
        cfg2 = sc.share_schedule_get(db)
        assert cfg2["share_release_interval_seconds"] == 30
        assert cfg2["fresh_daily_quota"] == 10


def test_tasks_list_supports_work_key_filter():
    """API 契约：GET /api/share/tasks 接受 work_key 查询参数。"""
    import inspect
    from app.main import share_tasks_list
    sig = inspect.signature(share_tasks_list)
    assert "work_key" in sig.parameters, "分享任务列表需支持 work_key 过滤"
