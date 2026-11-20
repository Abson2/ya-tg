"""v0.9.17.2 回归：镜像失败任务定向批量重试（不扫全库）。

锁死以下红线：
  - 失败目录：只对选中的 missing 行重发 create_dir；成功回写 target_dir_id/status，
    失败保留错误，非 missing / 非本规则的行跳过；带 · 规则与标准名降级。
  - 失败文件：只对选中行处理；先核对 B 库已存在则回填 synced、不重发 copy_file；
    未存在才 copy_file + 轮询；复用同一行（不新建）、清 rename 状态。
  - 关键性能红线：只 walk 选中文件所属作品的 A 目录（media.source_dir_id），
    绝不扫 A 库根 source_root_id；季目录相对链映射到 B 库正确父目录。
  - API：/api/mirror/retry/dirs 与 /api/mirror/retry/files 存在；missing_dirs 带 id。
  - 前端：两个失败表有 selection 列与批量重试按钮，勾选状态被重置。
"""
import asyncio
from unittest.mock import patch

import pytest

from app import mirror
from app.db import SessionLocal, migrate
from app.guangya import WebFileError
from app.models import MirrorDir, MirrorFile, MirrorMedia, MirrorRule, MirrorTask

TABLES = (MirrorFile, MirrorTask, MirrorDir, MirrorMedia, MirrorRule)


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        db.commit()
    yield
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        db.commit()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as s:
        yield s


def _rule(db):
    r = MirrorRule(name="镜像", source_root_id="A_ROOT", target_root_id="B_ROOT",
                   interval_minutes=60, enabled=True, file_idle_seconds=0.5,
                   auto_create_dirs=True, star_naming=True)
    db.add(r)
    db.commit()
    return r


def _media(db, rule, src="A_W1", target="B_W1"):
    m = MirrorMedia(rule_id=rule.id, work_key="k1", title_standard="测试剧 (2020) {tmdb-1}",
                    media_type="tv", source_dir_id=src, target_dir_id=target,
                    category_path="国产剧", status="failed")
    db.add(m)
    db.commit()
    return m


class RetryAPI:
    """A 库只暴露单部作品目录 A_W1（含季子目录 S1）；B 库 B_W1 可读。

    记录 list_files 被请求的 parent_id，用于断言「没扫 A_ROOT」。
    """
    listed = []
    created = []
    copies = []
    b_existing = set()   # (name, size) 视为 B 库已存在
    task_status = 2

    def __init__(self, db):
        self.db = db

    async def list_files(self, parent_id, page=0, page_size=50):
        RetryAPI.listed.append(parent_id)
        if page > 0:
            return {"list": [], "total": 0}
        if parent_id == "A_W1":
            return {"list": [
                {"fileId": "S1", "resType": 2, "fileName": "Season 1"},
            ], "total": 1}
        if parent_id == "S1":
            return {"list": [
                {"fileId": "F1", "resType": 1, "fileName": "测试剧.S01E01.mkv",
                 "fileSize": 100, "utime": 1},
            ], "total": 1}
        if parent_id == "B_W1":
            # 季目录已存在（模拟真实场景：目录已建好，只是文件复制失败）
            return {"list": [{"fileId": "B_S1", "resType": 2, "fileName": "Season 1"}],
                    "total": 1}
        if parent_id == "B_S1":
            items = []
            for (name, size) in RetryAPI.b_existing:
                items.append({"fileId": "BFILE", "resType": 1,
                              "fileName": name, "fileSize": size})
            return {"list": items, "total": len(items)}
        if parent_id == "B_ROOT":
            return {"list": [], "total": 0}
        return {"list": [], "total": 0}

    async def create_dir(self, parent_id, dir_name):
        RetryAPI.created.append((parent_id, dir_name))
        return {"file_id": "B_NEWDIR", "name": dir_name, "parent_id": parent_id, "raw": {}}

    async def copy_file(self, file_ids, parent_id):
        RetryAPI.copies.append((list(file_ids), parent_id))
        return "T1"

    async def get_task_status(self, task_id):
        return {"status": RetryAPI.task_status, "detail": {}}


def _reset():
    RetryAPI.listed = []
    RetryAPI.created = []
    RetryAPI.copies = []
    RetryAPI.b_existing = set()
    RetryAPI.task_status = 2


# ---------------- 失败目录定向重建 ----------------
def test_retry_dirs_recreates_only_selected_missing(db):
    r = _rule(db)
    d1 = MirrorDir(rule_id=r.id, source_dir_id="SD1", parent_target_id="B_ROOT",
                   name_standard="甲·剧 (2020) {tmdb-1}", kind="work",
                   category_path="国产剧", status="missing", error="create_dir 失败 code=117")
    d2 = MirrorDir(rule_id=r.id, source_dir_id="SD2", parent_target_id="B_ROOT",
                   name_standard="乙 (2021) {tmdb-2}", kind="work",
                   category_path="国产剧", status="missing", error="create_dir 失败")
    d3 = MirrorDir(rule_id=r.id, source_dir_id="SD3", parent_target_id="B_ROOT",
                   name_standard="丙", kind="category", category_path="国产剧",
                   status="mapped", target_dir_id="B3")  # 已映射，不该重建
    db.add_all([d1, d2, d3])
    db.commit()
    _reset()
    with patch("app.mirror.GuangYaWebFiles", RetryAPI):
        out = asyncio.run(mirror.retry_failed_dirs(db, r, [d1.id, d3.id, 99999]))
    assert out["recreated"] == 1
    assert out["already_ok"] == 1
    assert out["skipped"] == 1
    db.refresh(d1)
    assert d1.status == "created"
    assert d1.target_dir_id == "B_NEWDIR"
    assert d1.error == ""
    # 带 · 的目录名被保留（网盘接受 ·）
    assert any("·" in name for _, name in RetryAPI.created)
    db.refresh(d3)
    assert d3.status == "mapped" and d3.target_dir_id == "B3"


def test_retry_dirs_failure_keeps_missing_and_error(db):
    r = _rule(db)
    d = MirrorDir(rule_id=r.id, source_dir_id="SD1", parent_target_id="B_ROOT",
                  name_standard="甲 (2020)", kind="work", status="missing", error="旧错误")
    db.add(d)
    db.commit()
    _reset()

    class FailAPI(RetryAPI):
        async def create_dir(self, parent_id, dir_name):
            raise WebFileError("code=117 无效token", 117)

    with patch("app.mirror.GuangYaWebFiles", FailAPI):
        out = asyncio.run(mirror.retry_failed_dirs(db, r, [d.id]))
    assert out["failed"] == 1
    db.refresh(d)
    assert d.status == "missing"
    assert "117" in d.error and "定向重试" in d.error


def test_retry_dirs_dot_fallback_to_plain(db):
    r = _rule(db)
    d = MirrorDir(rule_id=r.id, source_dir_id="SD1", parent_target_id="B_ROOT",
                  name_standard="循环·更新 (2020) {tmdb-9}", kind="work",
                  category_path="国产剧", status="missing", error="")
    db.add(d)
    db.commit()
    _reset()

    class RejectDotAPI(RetryAPI):
        async def create_dir(self, parent_id, dir_name):
            if "·" in dir_name:
                raise WebFileError("code=141 非法字符 ·", 141)
            return await super().create_dir(parent_id, dir_name)

    with patch("app.mirror.GuangYaWebFiles", RejectDotAPI):
        out = asyncio.run(mirror.retry_failed_dirs(db, r, [d.id]))
    assert out["recreated"] == 1
    # 带 · 的那次请求失败被抛异常（未计入 created），降级标准名（去掉 ·）后才成功记录
    assert len(RetryAPI.created) == 1
    assert "·" not in RetryAPI.created[0][1]
    db.refresh(d)
    assert d.status == "created_fallback"


# ---------------- 失败文件定向重发 ----------------
def test_retry_files_copies_only_selected_and_reuses_same_row(db):
    r = _rule(db)
    m = _media(db, r)
    f1 = MirrorFile(media_id=m.id, rule_id=r.id, identity="tmdb:1:tv:1:1",
                    source_file_id="F1", file_name="测试剧.S01E01.mkv", size=100,
                    utime=1, fingerprint="100:1", status="failed", error="copy_file 失败 code=117",
                    target_name="old_en", rename_status="done")
    f2 = MirrorFile(media_id=m.id, rule_id=r.id, identity="tmdb:1:tv:1:2",
                    source_file_id="F2", file_name="测试剧.S01E02.mkv", size=200,
                    utime=2, fingerprint="200:2", status="synced", target_file_id="X")
    db.add_all([f1, f2])
    db.commit()
    before = db.query(MirrorFile).count()
    _reset()
    with patch("app.mirror.GuangYaWebFiles", RetryAPI):
        out = asyncio.run(mirror.retry_failed_files(db, r, [f1.id]))
    db.refresh(f1)
    assert f1.status == "synced" and f1.error == ""
    assert f1.target_name == "" and f1.rename_status == ""  # 补复制回中文原名，作废改名
    assert out["done"] == 1
    # 只 walk 作品目录与季目录，绝不扫 A_ROOT
    assert "A_ROOT" not in RetryAPI.listed
    assert "A_W1" in RetryAPI.listed and "S1" in RetryAPI.listed
    # 复制到 B 库季目录（不是作品根目录）
    assert RetryAPI.copies == [(["F1"], "B_S1")]
    # 复用同一行，不新建
    assert db.query(MirrorFile).count() == before
    # 已 synced 的 f2 未被触碰
    db.refresh(f2)
    assert f2.status == "synced" and f2.target_file_id == "X"


def test_retry_files_already_in_b_reuses_without_copy(db):
    r = _rule(db)
    m = _media(db, r)
    f = MirrorFile(media_id=m.id, rule_id=r.id, identity="tmdb:1:tv:1:1",
                   source_file_id="F1", file_name="测试剧.S01E01.mkv", size=100,
                   utime=1, fingerprint="100:1", status="failed", error="轮询超时")
    db.add(f)
    db.commit()
    _reset()
    # B 库季目录里其实已有同名同大小文件
    RetryAPI.b_existing = {("测试剧.S01E01.mkv", 100)}
    with patch("app.mirror.GuangYaWebFiles", RetryAPI):
        out = asyncio.run(mirror.retry_failed_files(db, r, [f.id]))
    assert out["reused"] == 1 and out["done"] == 0
    assert RetryAPI.copies == []  # 未重发复制
    db.refresh(f)
    assert f.status == "synced" and f.target_file_id == "BFILE"


def test_retry_files_copy_failure_marks_failed(db):
    r = _rule(db)
    m = _media(db, r)
    f = MirrorFile(media_id=m.id, rule_id=r.id, identity="tmdb:1:tv:1:1",
                   source_file_id="F1", file_name="测试剧.S01E01.mkv", size=100,
                   utime=1, fingerprint="100:1", status="failed", error="旧错误")
    db.add(f)
    db.commit()
    _reset()

    class FailCopyAPI(RetryAPI):
        async def copy_file(self, file_ids, parent_id):
            raise WebFileError("code=117 无效token", 117)

    with patch("app.mirror.GuangYaWebFiles", FailCopyAPI):
        out = asyncio.run(mirror.retry_failed_files(db, r, [f.id]))
    assert out["failed"] == 1
    db.refresh(f)
    assert f.status == "failed" and "117" in f.error


def test_retry_files_unknown_task_keeps_pollable(db):
    r = _rule(db)
    m = _media(db, r)
    f = MirrorFile(media_id=m.id, rule_id=r.id, identity="tmdb:1:tv:1:1",
                   source_file_id="F1", file_name="测试剧.S01E01.mkv", size=100,
                   utime=1, fingerprint="100:1", status="failed")
    db.add(f)
    db.commit()
    _reset()
    RetryAPI.task_status = 999  # 未确认状态
    with patch("app.mirror.GuangYaWebFiles", RetryAPI):
        out = asyncio.run(mirror.retry_failed_files(db, r, [f.id], max_polls=1,
                                                    poll_interval=0.01))
    db.refresh(f)
    assert f.status == "unknown"
    assert db.query(MirrorTask).count() == 1  # 保留任务供继续轮询


# ---------------- API 端点与状态接口 ----------------
def test_retry_endpoints_registered():
    from app.main import app
    paths = {(r.path, tuple(sorted(getattr(r, "methods", []) or []))) for r in app.routes}
    assert ("/api/mirror/retry/dirs", ("POST",)) in paths
    assert ("/api/mirror/retry/files", ("POST",)) in paths


def test_mirror_status_missing_dirs_have_id(db):
    from app.main import mirror_status
    r = _rule(db)
    d = MirrorDir(rule_id=r.id, source_dir_id="SD1", parent_target_id="B_ROOT",
                  name_standard="甲", kind="work", status="missing", error="x")
    db.add(d)
    db.commit()
    out = mirror_status(db)
    md = [x for x in out["missing_dirs"] if x["name"] == "甲"]
    assert md and md[0]["id"] == d.id


# ---------------- 前端源码断言 ----------------
def test_frontend_batch_retry_wiring():
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    src = (root / "frontend" / "src" / "App.vue").read_text(encoding="utf-8")
    assert "missingDirSelected" in src
    assert "fileErrSelected" in src
    assert "@click=\"retryFailedDirs\"" in src
    assert "@click=\"retryFailedFiles\"" in src
    assert "/mirror/retry/dirs" in src
    assert "/mirror/retry/files" in src
    assert 'type="selection"' in src  # 两个失败表都有勾选列
