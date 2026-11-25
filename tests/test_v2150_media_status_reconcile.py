"""v2.1.50：作品级状态只有一套判据（根治「重试成功仍显示失败」）。

真机现象（2026-11-25 20:46 只读实测）：某部作品 284 行 **全部** `status='synced'` +
`rename_status='done'`、`target_name` / `target_file_id` 全有值，`MirrorMedia.status`
却仍是 `failed`，界面上一直显示失败 → 用户以为重试没生效。

根因不是某一行算错，而是「作品状态由谁定」有三套实现、口径各不同：
  ① 扫描轮尾部        —— 看本作品所有文件行（唯一正确的那套）；
  ② `poll_pending_tasks` —— 按「最后轮询到的那个任务」直接定生死；
  ③ `retry_failed_files` —— **完全不重算**，只写 `last_synced_at`。
外加 ④ 复制轮尾部只看本轮 `copy_jobs` 里有没有 failed，历史失败行被无视。

本文件锁死「收敛之后」的口径：
  · 判据只有 `media_status_from`：任一行不在 (synced, superseded) → failed；
  · 写入口只有 `reconcile_media_status`，四个调用点全走它；
  · 定向重试复制成功后要顺手回填 `target_file_id`（光鸭 copy_file 只回 taskId）。

修复前本文件应红：`media_status_from` 不存在 / 重试后作品仍 failed / fileId 依旧空。
"""
import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import select

from app import mirror
from app.db import SessionLocal, migrate
from app.models import (
    AppConfig, AuditLog, Bot, Channel, MirrorDir, MirrorFile, MirrorMedia,
    MirrorRule, MirrorTask, QueueItem, Resource, ScanTask,
)

MIRROR_SRC = (Path(__file__).resolve().parents[1] / "backend/app/mirror.py").read_text(
    encoding="utf-8")

TABLES = [MirrorFile, MirrorTask, MirrorDir, MirrorMedia, MirrorRule, QueueItem,
          Resource, ScanTask, Channel, Bot, AppConfig, AuditLog]
A_ROOT, B_ROOT, A_WORK, B_WORK = "A_ROOT", "B_ROOT", "A_W1", "B_W1"
WORK_A = "完美世界 (2021) {tmdb-124003}"
# B 库目录名与 A 库规范化后必须一致，否则会走 create_dir 建新目录（与本用例无关）
WORK_B = "完美世界 (2021) {tmdb-124003}"
FP, SIZE = 111, 1000


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
    FakeAPI.a_files = []
    FakeAPI.b_files = []
    FakeAPI.copies = []
    FakeAPI.task_status = 2
    yield
    _clean()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as session:
        try:
            yield session
        finally:
            session.rollback()


class FakeAPI:
    """可控 A / B 库；`copy_file` 只回 taskId（复刻光鸭真实契约）。"""

    a_files = []
    b_files = []
    copies = []
    task_status = 2

    def __init__(self, db):
        self.db = db

    async def list_files(self, parent_id, page=0, page_size=100):
        if page > 0:
            return {"list": [], "total": 0}
        if parent_id == A_ROOT:
            return {"list": [{"fileId": A_WORK, "resType": 2, "fileName": WORK_A}],
                    "total": 1}
        if parent_id == A_WORK:
            return {"list": [dict(f) for f in self.a_files], "total": len(self.a_files)}
        if parent_id == B_ROOT:
            return {"list": [{"fileId": B_WORK, "resType": 2, "fileName": WORK_B}],
                    "total": 1}
        if parent_id == B_WORK:
            return {"list": [dict(f) for f in self.b_files], "total": len(self.b_files)}
        return {"list": [], "total": 0}

    async def copy_file(self, file_ids, parent_id):
        self.copies.append((list(file_ids), parent_id))
        # 复刻真实契约：任务失败时 B 库**不会**多出文件（否则回填会「自证成功」）
        if self.task_status != 2:
            return f"T{len(self.copies)}"
        n = len(self.b_files) + 1
        self.b_files.append({"fileId": f"BF{n}", "resType": 1,
                             "fileName": "S01E%02d.mkv" % n,
                             "fileSize": SIZE, "utime": FP, "parentId": parent_id})
        return f"T{len(self.copies)}"

    async def get_task_status(self, task_id):
        return {"status": self.task_status, "raw": {"status": self.task_status}}

    async def delete_files(self, ids):
        self.b_files = [f for f in self.b_files if f["fileId"] not in set(ids)]


def _src(fid="F1", name="S01E01.mkv", size=SIZE, utime=FP):
    return {"fileId": fid, "resType": 1, "fileName": name,
            "fileSize": size, "utime": utime, "parentId": A_WORK}


def _rule(db):
    r = MirrorRule(name="测试镜像", source_root_id=A_ROOT, target_root_id=B_ROOT,
                   interval_minutes=60, enabled=True, auto_create_dirs=True,
                   star_naming=True, file_idle_seconds=0.01)
    db.add(r)
    db.commit()
    return r


def _sync(db):
    return asyncio.run(mirror.sync_rule(db, db.scalar(select(MirrorRule)),
                                        max_polls=4, poll_interval=0.0))


def _files(db):
    return list(db.scalars(select(MirrorFile)).all())


def _media(db):
    return db.scalar(select(MirrorMedia))


# ---------------------------------------------------------------------------
# ① 判据本身：任一行没干完就是 failed
# ---------------------------------------------------------------------------
class _Row:
    def __init__(self, status):
        self.status = status


def test_predicate_all_done_is_synced():
    assert mirror.media_status_from([]) == "synced"
    assert mirror.media_status_from([_Row("synced"), _Row("synced")]) == "synced"


def test_predicate_superseded_counts_as_done():
    """superseded（被新内容取代）没有活要干，不能把作品拖成失败。"""
    assert mirror.media_status_from([_Row("synced"), _Row("superseded")]) == "synced"


@pytest.mark.parametrize("bad", ["failed", "pending", "copying", "unknown", "archived", ""])
def test_predicate_any_unfinished_is_failed(bad):
    assert mirror.media_status_from([_Row("synced"), _Row(bad)]) == "failed"


def test_only_one_predicate_implementation():
    """判据只许有一份实现：不许再出现 `pending_left = any(...)` 这种平行写法。"""
    assert "def media_status_from(" in MIRROR_SRC
    assert MIRROR_SRC.count("def media_status_from(") == 1
    assert "pending_left = any(" not in MIRROR_SRC, \
        "扫描轮又自己写了一份判据 —— 这正是三套口径打架的起点"
    assert "def reconcile_media_status(" in MIRROR_SRC
    # 四个调用点：扫描轮尾部 / 复制轮尾部 / 任务轮询 / 定向重试
    assert MIRROR_SRC.count("media_status_from(") >= 3
    assert MIRROR_SRC.count("reconcile_media_status(") >= 4


# ---------------------------------------------------------------------------
# ② reconcile 落库：状态重算 + 可选刷新时间
# ---------------------------------------------------------------------------
def test_reconcile_writes_status_to_db(db):
    FakeAPI.a_files = [_src("F1"), _src("F2", "S01E02.mkv")]
    _rule(db)
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)
    media = _media(db)
    media.status = "failed"                 # 模拟定向重试后被落下的旧状态
    db.commit()

    assert mirror.reconcile_media_status(db, media.id) == "synced"
    db.commit()
    db.expire_all()
    assert _media(db).status == "synced"

    # 只要有一行被改回 failed，重算结果必须立刻回到 failed
    row = _files(db)[0]
    row.status = "failed"
    db.commit()
    assert mirror.reconcile_media_status(db, media.id) == "failed"


def test_reconcile_missing_media_returns_empty(db):
    assert mirror.reconcile_media_status(db, 999999) == ""


# ---------------------------------------------------------------------------
# ③ 真机那一条：定向重试成功后，作品必须变 synced 且 fileId 补上
# ---------------------------------------------------------------------------
def _break_everything(db):
    """把已同步的作品打回「重试前」的样子：行 failed、无 fileId、B 库清空。"""
    FakeAPI.b_files = []
    for r in _files(db):
        r.status = "failed"
        r.error = "模拟：复制任务超时"
        r.target_file_id = ""
        r.target_name = ""
        r.rename_status = ""
        r.attempts = 3
    media = _media(db)
    media.status = "failed"
    db.commit()
    return media


def test_retry_success_flips_media_status_and_backfills_file_ids(db):
    FakeAPI.a_files = [_src("F1", "S01E01.mkv"), _src("F2", "S01E02.mkv")]
    _rule(db)
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)
    media = _break_everything(db)
    media_id = media.id

    ids = [r.id for r in _files(db)]
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        out = asyncio.run(mirror.retry_failed_files(
            db, db.scalar(select(MirrorRule)), ids, max_polls=4, poll_interval=0.0))

    assert out["done"] == 2, out
    db.expire_all()
    rows = _files(db)
    assert {r.status for r in rows} == {"synced"}, [(r.status, r.error) for r in rows]
    assert all(str(r.target_file_id or "").strip() for r in rows), \
        ("定向重试后 target_file_id 仍为空 —— 光鸭 copy_file 只回 taskId，"
         "这一步以前根本没人补，映射行会一直是空的")
    assert _media(db).status == "synced", (
        f"284 行全同步完了作品仍是 {_media(db).status!r} —— 用户看到的就是「重试没用」")
    assert out.get("backfilled") == 2, out


def test_retry_partial_failure_keeps_media_failed(db):
    """一半成功一半失败：作品不许跟着成功的那些变 synced。"""
    FakeAPI.a_files = [_src("F1", "S01E01.mkv"), _src("F2", "S01E02.mkv")]
    _rule(db)
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)
    _break_everything(db)

    ids = [r.id for r in _files(db)]
    # 全部复制任务返回失败（status=3）
    FakeAPI.task_status = 3
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        out = asyncio.run(mirror.retry_failed_files(
            db, db.scalar(select(MirrorRule)), ids, max_polls=2, poll_interval=0.0))
    FakeAPI.task_status = 2

    assert out["failed"] == 2, out
    db.expire_all()
    assert _media(db).status == "failed"


# ---------------------------------------------------------------------------
# ④ 任务轮询：不再「最后一个任务说了算」
# ---------------------------------------------------------------------------
class _PollAPI(FakeAPI):
    """按 taskId 分别返回状态，用来复刻「同一部作品两个任务、一个成功一个失败」。"""

    statuses = {}

    async def get_task_status(self, task_id):
        st = self.statuses.get(task_id, 2)
        return {"status": st, "raw": {"status": st}}

    async def list_files(self, parent_id, page=0, page_size=100):
        return {"list": [], "total": 0}


def test_poll_does_not_let_last_task_decide_media_status(db):
    rule = _rule(db)
    media = MirrorMedia(rule_id=rule.id, title_standard=WORK_A,
                        source_dir_id=A_WORK, target_dir_id=B_WORK, status="syncing")
    db.add(media)
    db.commit()
    # 失败任务先入库、成功任务后入库：旧写法「后处理的覆盖前面的」→ 会错成 synced
    db.add(MirrorFile(rule_id=rule.id, media_id=media.id, source_file_id="F1",
                      file_name="S01E01.mkv", size=SIZE, utime=FP,
                      status="copying", task_id="TF"))
    db.add(MirrorFile(rule_id=rule.id, media_id=media.id, source_file_id="F2",
                      file_name="S01E02.mkv", size=SIZE, utime=FP,
                      status="copying", task_id="TD"))
    db.add(MirrorTask(rule_id=rule.id, media_id=media.id, task_id="TF", status="running"))
    db.add(MirrorTask(rule_id=rule.id, media_id=media.id, task_id="TD", status="running"))
    db.commit()

    _PollAPI.statuses = {"TF": 3, "TD": 2}
    with patch("app.mirror.GuangYaWebFiles", _PollAPI):
        res = asyncio.run(mirror.poll_pending_tasks(db, rule, max_polls=2,
                                                    interval=0.0))
    assert res["done"] == 1 and res["failed"] == 1, res
    db.expire_all()
    assert _media(db).status == "failed", (
        "同一部作品还有一个失败任务，作品却被标成 synced —— 失败被掩盖了")


def test_poll_all_done_flips_media_to_synced(db):
    rule = _rule(db)
    media = MirrorMedia(rule_id=rule.id, title_standard=WORK_A,
                        source_dir_id=A_WORK, target_dir_id=B_WORK, status="syncing")
    db.add(media)
    db.commit()
    db.add(MirrorFile(rule_id=rule.id, media_id=media.id, source_file_id="F1",
                      file_name="S01E01.mkv", size=SIZE, utime=FP,
                      status="copying", task_id="TD"))
    db.add(MirrorTask(rule_id=rule.id, media_id=media.id, task_id="TD", status="running"))
    db.commit()

    _PollAPI.statuses = {"TD": 2}
    with patch("app.mirror.GuangYaWebFiles", _PollAPI):
        asyncio.run(mirror.poll_pending_tasks(db, rule, max_polls=2, interval=0.0))
    db.expire_all()
    assert _media(db).status == "synced"
    assert _media(db).last_synced_at is not None


# ---------------------------------------------------------------------------
# ⑤ 同步正常路径：历史失败行不许被本轮成功掩盖
# ---------------------------------------------------------------------------
def test_sync_round_does_not_hide_previous_failed_rows(db):
    """本轮复制成功不许把整部作品标 synced —— 历史失败行还在。"""
    FakeAPI.a_files = [_src("F1", "S01E01.mkv"), _src("F2", "S01E02.mkv")]
    _rule(db)
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)

    # 模拟「E02 的源文件被删了、B 库那份也没了」→ 行收敛为 failed（v2.1.36 口径）
    FakeAPI.a_files = [_src("F1", "S01E01.mkv")]
    FakeAPI.b_files = [f for f in FakeAPI.b_files if f["fileName"] != "S01E02.mkv"]
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)
    db.expire_all()
    failed_rows = [r for r in _files(db) if r.status == "failed"]
    assert failed_rows, [(r.status, r.error) for r in _files(db)]
    assert _media(db).status == "failed"

    # 再放一集新文件：本轮会走复制且成功，但 E02 那条失败行不该被掩盖
    FakeAPI.a_files.append(_src("F3", "S01E03.mkv"))
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)
    db.expire_all()
    assert _media(db).status == "failed", (
        "本轮复制成功就把整部作品标 synced，那条历史失败行被无视了")
