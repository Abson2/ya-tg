"""v2.1.38（真机反馈）：用户确认 A 库源文件被删后，要能**手动清理**这些待处理记录。

v2.1.37 已把「补复制等不到源文件」的行收敛成「复制失败 + 明确原因」，但用户明明知道
片被删了 —— 他想要的是把这条记录从「问题诊断 → N 项待处理」里清掉、别再每轮重算。
此前唯一的清理入口是「清理无效影视映射」：整部作品 + 联网核对 B 库，颗粒太粗。

本版新增：`mirror.archive_retryable_files()`（经 `POST /api/mirror/cleanup`
scope=archive 调用）。口径（项目红线：清理用归档，不物理删除）：
  ① 只归档 status ∈ RETRYABLE_FILE_STATUS 的行，synced / superseded 一律不动；
  ② 归档 = status 置 archived，`error` **保留原原因**再追加「已确认…手动清理归档」；
  ③ 不删行、不删 B 库文件、不动资源 / 分享 / 队列；
  ④ 归档后不再计入待处理、不再每轮被优先处理；
  ⑤ **可恢复**：源文件放回 A 库后下一轮同步按**同一行**复活（不新建行 —— 否则撞
     (media_id, source_file_id, fingerprint) 唯一键）；
  ⑥ A 库整部作品消失时，C 类预扫描不得把已归档的行翻回 failed（否则用户清了又冒）。

修复前这些断言应当红：行停在 failed / 计数不减 / 复活时新建行撞唯一键。
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


def _retryable(db):
    return [r for r in _files(db) if r.status in mirror.RETRYABLE_FILE_STATUS]


# ---------------------------------------------------------------------------
# ① 归档只动待处理行；synced 的已同步记录绝不能被这个按钮清掉
# ---------------------------------------------------------------------------
def test_archive_only_touches_retryable_rows(db):
    FakeAPI.a_files = [_src("F1"), _src("F2", "S01E02.mkv")]
    rule = _rule(db)
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)
    # 造一条待处理行（B 库核对不到 → 自动打回 pending 是正常自愈路径）
    FakeAPI.b_files = []
    FakeAPI.a_files = [_src("F2", "S01E02.mkv")]      # E01 源文件被删
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)
    retry = _retryable(db)
    assert retry, [(r.source_file_id, r.status) for r in _files(db)]

    synced_before = [r.id for r in _files(db) if r.status == "synced"]
    out = mirror.archive_retryable_files(db, rule, [], dry_run=False)

    assert out["archived"] == len(retry), out
    for r in _files(db):
        if r.id in synced_before:
            assert r.status == "synced", (
                "「清理选中」把已同步的记录也归档了 —— 这会清掉正常映射")
    assert all(r.status == mirror.ARCHIVED_FILE_STATUS for r in retry)


# ---------------------------------------------------------------------------
# ② 归档后不再计入「待处理」，但行必须还在（不物理删除，历史要留）
# ---------------------------------------------------------------------------
def test_archived_rows_leave_retryable_count_but_stay_in_db(db):
    FakeAPI.a_files = [_src("F1")]
    rule = _rule(db)
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)
    FakeAPI.a_files, FakeAPI.b_files = [], []
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)
    retry = _retryable(db)
    assert retry, [(r.source_file_id, r.status) for r in _files(db)]
    n = len(retry)

    mirror.archive_retryable_files(db, rule, [r.id for r in retry])

    assert len(_retryable(db)) == 0, (
        "归档后还有行留在待处理集合里 —— 「N 项待处理」降不下来，用户清了等于没清")
    # 不删行：总数不变，只是状态变了
    assert len(_files(db)) == n, "归档把行物理删掉了 —— 项目红线要求保留历史与执行记录"


# ---------------------------------------------------------------------------
# ③ 原原因必须保留：用户事后还得能看到「当初为什么没复制成功」
# ---------------------------------------------------------------------------
def test_archived_row_keeps_original_reason(db):
    FakeAPI.a_files = [_src("F1")]
    rule = _rule(db)
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)
    FakeAPI.a_files, FakeAPI.b_files = [], []
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)
    row = _files(db)[0]
    original = row.error or ""
    assert original, "前提：这行应当已有失败原因"

    mirror.archive_retryable_files(db, rule, [row.id])
    db.refresh(row)
    assert row.status == mirror.ARCHIVED_FILE_STATUS, row.status
    assert original in row.error, f"归档把原原因覆盖了：{row.error!r}（原：{original!r}）"
    assert "手动清理归档" in row.error, row.error
    assert row.task_id == "", "归档后不该还挂着旧的复制任务号"


# ---------------------------------------------------------------------------
# ④ 可恢复：源文件放回 A 库 → 按**同一行**复活，不新建（否则撞唯一键）
# ---------------------------------------------------------------------------
def test_archived_row_is_revived_in_place_when_source_returns(db):
    FakeAPI.a_files = [_src("F1")]
    rule = _rule(db)
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)
    FakeAPI.a_files, FakeAPI.b_files = [], []
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)
    row = _files(db)[0]
    mirror.archive_retryable_files(db, rule, [row.id])
    row_id = row.id

    # 用户把源文件放回 A 库
    FakeAPI.a_files = [_src("F1")]
    FakeAPI.b_files = []
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        back = _sync(db)

    rows = [r for r in _files(db) if str(r.source_file_id) == "F1"]
    assert len(rows) == 1, (
        f"复活时新建了行（{len(rows)} 条）—— (media_id, source_file_id, fingerprint) "
        f"是唯一键，真机上会直接 500")
    assert rows[0].id == row_id, "复活没有复用归档行"
    assert rows[0].status in ("copying", "synced"), rows[0].status
    assert rows[0].status == "synced", rows[0].status
    assert back.get("restored") == 1, back
    assert "已归档恢复 1 项" in (db.scalar(select(MirrorRule)).last_message or "")


# ---------------------------------------------------------------------------
# ⑤ 保护：整部作品从 A 库消失时，C 类预扫描不得把已归档的行翻回失败
# ---------------------------------------------------------------------------
def test_c_class_does_not_unarchive_rows(db):
    FakeAPI.a_files = [_src("F1")]
    rule = _rule(db)
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)
    FakeAPI.a_files, FakeAPI.b_files = [], []
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db)
    row = _files(db)[0]
    mirror.archive_retryable_files(db, rule, [row.id])

    # 整部作品从 A 库彻底消失（连作品目录都不再出现）
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        second = _sync(db)

    # v2.1.40：计数口径拆分 —— c_candidates=本轮识别到几部「A 库已无」（无状态），
    # c_deleted_a=本轮实际处置几部（幂等）。这部作品的行全是已归档（已处理干净），
    # 本轮不重复计数、不重复写库，所以前者仍为 1、后者为 0。
    assert second["prepass"]["c_candidates"] == 1, second["prepass"]
    assert second["prepass"]["c_deleted_a"] == 0, second["prepass"]
    db.refresh(row)
    assert row.status == mirror.ARCHIVED_FILE_STATUS, (
        f"C 类预扫描把已归档的行翻回了 {row.status!r} —— 用户清完下一轮又冒出来，等于白清")


# ---------------------------------------------------------------------------
# ⑥ 前端契约：按钮存在、接归档明细、归档不算「待处理」
# ---------------------------------------------------------------------------
def _app_src() -> str:
    return (Path(__file__).resolve().parents[1] / "frontend/src/App.vue").read_text(
        encoding="utf-8")


def test_frontend_has_archive_button_and_panel():
    src = _app_src()
    assert "archiveFiles" in src, "前端没有清理归档的处理函数"
    assert "清理选中" in src, "待重试文件面板没有「清理选中」按钮"
    assert "scope: 'archive'" in src, "前端调用清理端点时没带 scope=archive"
    assert "archived_files" in src, "已归档明细没接到页面上（数字必须有一份同谓词明细）"
    assert "已手动清理" in src, "已归档面板缺失"


def test_frontend_archive_does_not_count_as_pending():
    """归档的行不是「待处理」——不能又被加回诊断数字里。"""
    src = _app_src()
    start = src.index("const diagCount = computed")
    block = src[start:src.index("})", start)]
    # 剥掉注释再断言，避免注释里的说明文字误伤
    body = "\n".join(line.split("//")[0] for line in block.splitlines())
    assert "files_archived" not in body, (
        "diagCount 把已归档的行又算进「待处理」了 —— 用户清完数字不减，等于没清")


# ---------------------------------------------------------------------------
# ⑦ 关键契约：清理归档**不申请镜像互斥锁**
#
# 真机反馈：定时镜像正在跑（持锁 457 秒）时点「批量重试选中」被 409 挡住，
# 「另有 1 个定时任务排队」。用户此刻往往只是想清掉「已确认源文件被删」的记录，
# 这类操作不碰 A/B 库、不写外部接口，**没有任何理由等这把锁**。
# 归档是纯本地记录操作 → 必须放在 scope 分发里、且在 `with _mirror_guard()` 之前。
# ---------------------------------------------------------------------------
def _main_src() -> str:
    return (Path(__file__).resolve().parents[1] / "backend/app/main.py").read_text(
        encoding="utf-8")


def test_archive_scope_does_not_take_the_mirror_lock():
    src = _main_src()
    start = src.index("async def mirror_cleanup(")
    end = src.index("async def ", start + 10)
    fn = src[start:end]
    a = fn.index('if scope == "archive":')
    b = fn.index('if scope == "pruned":')
    branch = fn[a:b]
    # 剥掉注释再断言：这段代码的注释里本来就会提到 `_mirror_guard`（说明「为什么不进」）
    code = "\n".join(line.split("#")[0] for line in branch.splitlines())
    assert "_mirror_guard" not in code, (
        "清理归档去抢镜像互斥锁了 —— 定时镜像在跑时用户就清不掉记录，"
        "正是真机反馈的那个死结（「批量重试选中」被 409 挡住）")
    assert a < fn.index("with _mirror_guard()"), "归档分支必须排在拿锁的 file / dir 分支之前"


def test_lock_busy_hint_points_to_archive():
    """被锁挡住时，报错文案要告诉用户还有一条不需要等锁的路。"""
    src = _main_src()
    assert "清理选中" in src, (
        "镜像互斥锁被占用的 409 文案没有提示「清理选中」——用户被挡住时"
        "只会看到「等它跑完」，而他要做的清理本来就不需要等")

