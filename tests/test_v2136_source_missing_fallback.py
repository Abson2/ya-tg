"""v2.1.36（第二条真机反馈）：A 库源文件被删时，补复制必须收敛，不能无限停在「待复制」。

用户线下核实（2026-11-25）：**A 库源文件已被删除**。
这把定性改了：不是「重试机制有 bug」，而是**补复制客观上不可能成功**，
系统却把映射行留在 pending 空转，于是问题诊断的「N 项待处理」永远清不掉。

代码对照（本文件锁死这个差异）：
  · 手动「定向重试」`retry_failed_files`：A 库枚举不到源文件 → 直接标 failed +
    原因「A 库该作品目录下未找到源文件，可能已被移动/删除」✅
  · 自动补复制（作品目录还在、个别集被删）：B 库核对发现已同步文件不见了 → 把行
    打回 pending，然后等 A 库枚举到同一个 (fileId, 指纹) 才 pop 出来重发。
    **源文件被删就永远枚举不到**，循环结束没人处理 → 一直 pending ❌
  · A 库整部作品消失（预扫描 C 类）：`_clear_media_mappings` 也统一重置成 pending，
    同样永远挂着 ❌

修法：
  ① 主循环内，本轮 A 库枚举**完整**时，凡仍未被认领的补复制行 → failed + 如实原因
     （区分「源文件已删」与「内容已变」），并在 last_message 里回显条数；
  ② 预扫描 C 类 → 直接 failed + 「A 库已无该作品…」；
  ③ 两条都保留「源文件放回 A 库后下一轮自动重建」的恢复路径（不写死、不拉黑）。

修复前本文件应当红（行停在 pending）。
"""
import asyncio
from unittest.mock import patch

import pytest
from sqlalchemy import select

from app import mirror
from app.db import SessionLocal, migrate
from app.guangya import WebFileError
from app.models import (
    AppConfig, AuditLog, Bot, Channel, MirrorDir, MirrorFile, MirrorMedia,
    MirrorRule, MirrorTask, QueueItem, Resource, ScanTask,
)

TABLES = [MirrorFile, MirrorTask, MirrorDir, MirrorMedia, MirrorRule, QueueItem,
          Resource, ScanTask, Channel, Bot, AppConfig, AuditLog]
A_ROOT, B_ROOT, A_WORK, B_WORK = "A_ROOT", "B_ROOT", "A_W1", "B_W1"
WORK_A = "缅北电诈覆灭纪实 (2026) {tmdb-337399}"
WORK_B = "缅北电诈覆灭纪实 *(2026) {tmdb-337399}"
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
    """可控 A / B 库：改 `a_files` 即模拟「A 库某集被删」。"""

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
        self.b_files.append({"fileId": f"BF{len(self.b_files) + 1}", "resType": 1,
                             "fileName": "S01E0%d.mkv" % (len(self.b_files) + 1),
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


def _by_fid(db, fid):
    """同一个 source_file_id 可能有多行（内容变化会新建行），这里全部返回。"""
    return [r for r in _files(db) if str(r.source_file_id) == fid]


# ---------------------------------------------------------------------------
# ① 核心：作品目录还在、个别集被删 → 那条补复制行必须收敛为失败
# ---------------------------------------------------------------------------
def test_auto_retry_marks_failed_when_source_file_deleted(db):
    FakeAPI.a_files = [_src("F1", "S01E01.mkv"), _src("F2", "S01E02.mkv")]
    _rule(db)
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        first = _sync(db)
    assert first["new"] == 2, first
    assert {r.status for r in _files(db)} == {"synced"}, [(r.status, r.error) for r in _files(db)]

    # 用户线下核实的情形：E01 在 A 库被删（E02 还在），且 B 库两边都没了
    FakeAPI.a_files = [_src("F2", "S01E02.mkv")]
    FakeAPI.b_files = []
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        second = _sync(db)

    gone = _by_fid(db, "F1")
    assert gone, _files(db)
    assert gone[0].status == "failed", (
        f"源文件已删却还挂在 {gone[0].status!r} —— 自动补复制会无限空转，"
        f"问题诊断就永远清不掉（error={gone[0].error!r}）")
    assert "A 库未找到源文件" in (gone[0].error or ""), gone[0].error
    assert second.get("source_missing") == 1, second
    # 还在 A 库的那一集走正常补复制，不受影响
    assert _by_fid(db, "F2")[0].status in ("copying", "synced"), _by_fid(db, "F2")[0].status


def test_failed_reason_distinguishes_removed_from_changed(db):
    """源文件还在但内容变了 → 同样不能无限等，原因要如实写成「内容已变化」。"""
    FakeAPI.a_files = [_src("F1")]
    _rule(db)
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)

    FakeAPI.a_files = [_src("F1", size=SIZE + 500, utime=FP + 7)]
    FakeAPI.b_files = []
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)

    rows = _files(db)
    failed = [r for r in rows if r.status == "failed"]
    assert failed, [(r.status, r.error) for r in rows]
    assert any("内容已变化" in (r.error or "") for r in failed), \
        [(r.status, r.error) for r in rows]


# ---------------------------------------------------------------------------
# ② A 库整部作品消失（预扫描 C 类）：同样不能重置成待复制永远挂着
# ---------------------------------------------------------------------------
def test_c_class_whole_work_gone_marks_failed(db):
    FakeAPI.a_files = [_src("F1")]
    _rule(db)
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)

    # 整部作品从 A 库消失（连目录都不再是「作品」）
    FakeAPI.a_files = []
    FakeAPI.b_files = []
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        second = _sync(db)

    assert second["prepass"]["c_deleted_a"] == 1, second["prepass"]
    row = _by_fid(db, "F1")[0]
    assert row.status == "failed", (
        f"A 库已无该作品，行却留在 {row.status!r} —— 「待处理 N 项」会永远清不掉")
    assert "A 库已无该作品" in (row.error or ""), row.error
    assert "A库已无作品 1 部" in (db.scalar(select(MirrorRule)).last_message or "")


# ---------------------------------------------------------------------------
# ③ 保护：A 库枚举不完整时，整轮同步直接中止（不会误判「源文件不存在」）
# ---------------------------------------------------------------------------
def test_incomplete_source_walk_aborts_instead_of_marking_failed(db):
    FakeAPI.a_files = [_src("F1")]
    _rule(db)
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)

    FakeAPI.a_files = []
    real_walk = mirror._walk_source

    async def walk_incomplete(api, root_id, **kw):
        nodes, _ = await real_walk(api, root_id, **kw)
        return nodes, False      # 分页没取全

    with patch("app.mirror._walk_source", walk_incomplete), \
         patch("app.mirror.GuangYaWebFiles", FakeAPI):
        with pytest.raises(WebFileError):
            _sync(db)

    row = _by_fid(db, "F1")[0]
    assert row.status != "failed", "枚举不完整就把行标失败 —— 会把「没扫到」误判成「已删」"


# ---------------------------------------------------------------------------
# ④ 前端：勾选不能被 2 秒轮询刷掉（用户实测「选中后被自动取消」）
# ---------------------------------------------------------------------------
def test_frontend_keeps_selection_across_polling():
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "frontend/src/App.vue").read_text(
        encoding="utf-8")
    assert "reserve-selection" in src, (
        "镜像页勾选列没有 reserve-selection —— /mirror/status 每 2 秒整体替换一次，"
        "el-table 会清空选中，「批量重试选中」永远是 (0)")
    assert src.count("reserve-selection") >= 3, \
        "待创建目录 / 待清理重建 / 待重试文件 三处都要保住勾选"
