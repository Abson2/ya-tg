"""v0.8.8 回归：补复制后 B 库文件恢复中文原名，改名必须以 B 库实际名为准。

用户 2026-11-18 在 v0.8.7 环境的实测反馈：

    「同步 4 部：新增 0 · 重试 4 · 变化 0 · 未变 0 · 完成 4 · 复制失败 0 ·
     待确认 0 · 新建目录 4 · 重试 4 个未成功文件 · 首个文件错误：
     小黄人大眼萌 (2015) {tmdb-211672}/…：B 库未找到已同步文件，已重新入复制队列 ·
     自动入库 4 文件 · 识别成功 4 · 入队 0 · 改名 0 · 待翻译作品 0 · 改名失败 0
     现在文件是有了，不过影视单集、电影文件汉字名称没有翻译」

根因（本文件锁死）：v0.8.7 的空目录补复制把 B 库文件从 A 库重新复制成
**中文原名**，但复用同一 `MirrorFile` 行时只重置了 `target_file_id`，
`rename_status=done` / `target_name=英文名` 残留 → `rename_mirror_files()`
按 DB 幂等整批跳过（skipped），三个提示计数全零，文件永远保持中文。

v0.8.8 两条修复：
  ① `sync_rule()` 补复制时同步清空改名状态；
  ② `rename_mirror_files()` 以 B 库**实际文件名**判断是否已达标，不再只信 DB。
另补诊断：整体异常 / 跳过原因显式带出，不再出现「三个零」黑洞。
"""
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from app import main, mirror
from app.db import SessionLocal, migrate
from app.file_naming import english_file_name
from app.models import (AuditLog, MirrorDir, MirrorFile, MirrorMedia, MirrorRule,
                        MirrorTask, QueueItem, Resource, ResourceScanBatch)
from app.utils import normalize_work_name

ROOT = Path(__file__).resolve().parents[1]
A_ROOT, A_WORK, A_FILE = "A_ROOT", "A_WORK", "F1"
B_ROOT, B_WORK, B_FILE = "B_ROOT", "B_WORK", "BF1"
WORK = "大醉侠 (2002) {tmdb-135646}"
MIRROR_DIR = "大·醉·侠 (2002) {tmdb-135646}"
ZH_NAME = "大醉侠 第04集 2160p.WEB-DL.H.265.mkv"
EN_NAME = "The Drunk Hero 第04集 2160p.WEB-DL.H.265.mkv"

TABLES = [MirrorFile, MirrorTask, MirrorMedia, MirrorDir, MirrorRule,
          Resource, QueueItem, AuditLog, ResourceScanBatch]


def _clean():
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        db.commit()


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    _clean()
    FakeAPI.reset()
    SyncAPI.reset()
    yield
    _clean()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as session:
        try:
            yield session
        finally:
            session.rollback()


def _run(coro):
    return asyncio.run(coro)


def _noop_sleep():
    async def _sleep(*_a, **_kw):
        return None
    return _sleep


def _b_file(name=ZH_NAME, fid=B_FILE, size=1024):
    return {"fileId": fid, "fileName": name, "fileSize": size, "resType": 1}


class FakeAPI:
    """只模拟 B 库列目录与改名；B 库当前文件名由 b_files 决定。"""

    b_files = []
    renamed = []
    fail_listing = False

    def __init__(self, db):
        self.db = db

    @classmethod
    def reset(cls, files=None, fail_listing=False):
        cls.b_files = [dict(f) for f in (files if files is not None else [_b_file()])]
        cls.renamed = []
        cls.fail_listing = fail_listing

    async def list_files(self, parent_id="", page=0, page_size=50):
        if self.fail_listing:
            raise RuntimeError("B 库列目录超时")
        if page == 0:
            return {"list": [dict(f) for f in self.b_files], "total": len(self.b_files)}
        return {"list": [], "total": len(self.b_files)}

    async def rename_file(self, file_id, new_name):
        self.renamed.append((file_id, new_name))
        for f in self.b_files:
            if str(f.get("fileId")) == str(file_id):
                f["fileName"] = new_name

    async def create_dir(self, parent_id, dir_name):
        raise AssertionError("改名链路绝不建目录")

    async def copy_file(self, file_ids, parent_id):
        raise AssertionError("改名链路绝不重新复制文件")


class SyncAPI:
    """A 库有作品目录+文件；B 库作品目录为空 → 触发 v0.8.7 空目录补复制。"""

    b_files = []
    copies = []

    def __init__(self, db):
        self.db = db

    @classmethod
    def reset(cls):
        cls.b_files = []
        cls.copies = []

    async def list_files(self, parent_id="", page=0, page_size=50):
        if page > 0:
            return {"list": [], "total": 0}
        if parent_id == A_ROOT:
            return {"list": [{"fileId": A_WORK, "resType": 2, "fileName": WORK}],
                    "total": 1}
        if parent_id == A_WORK:
            return {"list": [{"fileId": A_FILE, "resType": 1, "fileName": ZH_NAME,
                              "fileSize": 1024, "utime": 1, "parentId": A_WORK}],
                    "total": 1}
        if parent_id == B_ROOT:
            return {"list": [{"fileId": B_WORK, "resType": 2,
                              "fileName": MIRROR_DIR}], "total": 1}
        if parent_id == B_WORK:
            return {"list": [dict(f) for f in self.b_files], "total": len(self.b_files)}
        return {"list": [], "total": 0}

    async def create_dir(self, parent_id, dir_name):
        return {"file_id": "NEW_DIR"}

    async def copy_file(self, file_ids, parent_id):
        self.copies.append((list(file_ids), parent_id))
        # 关键：从 A 库复制过来的文件叫**中文原名**
        self.b_files.append(_b_file(ZH_NAME))
        return "TASK1"

    async def get_task_status(self, task_id):
        return {"status": 2, "detail": {}, "raw": {"status": 2}}

    async def rename_file(self, file_id, new_name):
        raise AssertionError("sync_rule 本身不改名")


def _seed(db, *, status="synced", rename_status="done", target_name=EN_NAME,
          target_file_id=B_FILE, english_title="The Drunk Hero"):
    """造一条「历史上改过英文名」的记录，模拟补复制前 DB 残留状态。"""
    rule = MirrorRule(name="v088", source_root_id=A_ROOT, target_root_id=B_ROOT,
                      enabled=True, star_naming=True, auto_identify=False,
                      rename_files=True, file_idle_seconds=0.5, auto_create_dirs=True)
    db.add(rule)
    db.flush()
    media = MirrorMedia(rule_id=rule.id,
                        work_key="work:" + normalize_work_name(WORK),
                        title_standard=WORK, title_mirror=MIRROR_DIR,
                        source_dir_id=A_WORK, target_dir_id=B_WORK,
                        english_title=english_title, english_source="tmdb",
                        status="synced")
    db.add(media)
    db.flush()
    row = MirrorFile(media_id=media.id, rule_id=rule.id,
                     identity="tmdb:135646:tv:1:4", source_file_id=A_FILE,
                     target_file_id=target_file_id, file_name=ZH_NAME,
                     target_name=target_name, rename_status=rename_status,
                     size=1024, utime=1, fingerprint="1024:1", status=status)
    db.add(row)
    db.commit()
    return rule, media, row


def _rename_mocks():
    tmdb_mock = AsyncMock(return_value="The Drunk Hero")
    ai_mock = AsyncMock(return_value={"ok": True, "titles": {"en": "The Drunk Hero"},
                                      "queries": ["The Drunk Hero"]})
    return (patch("app.mirror.GuangYaWebFiles", FakeAPI),
            patch("app.mirror.tmdb.english_title", new=tmdb_mock),
            patch("app.mirror.ai_assist.translate_title", new=ai_mock),
            patch("app.mirror.asyncio.sleep", new=_noop_sleep()))


def _with_rename_mocks():
    patches = _rename_mocks()
    class _Bundle:
        def __enter__(self):
            for p in patches:
                p.start()
            return self
        def __exit__(self, *a):
            for p in reversed(patches):
                p.stop()
    return _Bundle()


# ---------- 1. 用户现场文件名：纯命名规则本身没问题 ----------
def test_user_reported_filename_is_translated():
    src = ("小黄人大眼萌 (2015) - 2160p.BluRay REMUX.HDR.TrueHD "
           "7.1 Atmos-DreamHD.mkv")
    out = english_file_name(src, "小黄人大眼萌", "Minions")
    assert out.startswith("Minions (2015)")
    assert out.endswith("2160p.BluRay REMUX.HDR.TrueHD 7.1 Atmos-DreamHD.mkv")
    assert "小黄人大眼萌" not in out


# ---------- 2. 头号根因：DB 说改过、B 库实际是中文名 → 必须重改 ----------
def test_db_done_but_b_chinese_is_renamed_again(db):
    rule, media, row = _seed(db)
    FakeAPI.reset(files=[_b_file(ZH_NAME)])
    with _with_rename_mocks():
        out = _run(mirror.rename_mirror_files(db, rule))
    assert out["renamed"] == 1 and out["skipped"] == 0, out
    assert FakeAPI.renamed == [(B_FILE, EN_NAME)]
    db.expire_all()
    r2 = db.get(MirrorFile, row.id)
    assert r2.rename_status == "done" and r2.target_name == EN_NAME
    assert r2.file_name == ZH_NAME  # A 原名仍是映射锚点
    assert r2.source_file_id == A_FILE and r2.fingerprint == "1024:1"


def test_b_already_english_skips_without_api_call(db):
    """B 库确实已是英文名 → 不重复调接口，并顺手纠正 DB 状态。"""
    rule, media, row = _seed(db)
    row.rename_status = ""  # 补复制后连 DB 状态也可能残留成未处理
    db.commit()
    FakeAPI.reset(files=[_b_file(EN_NAME)])
    with _with_rename_mocks():
        out = _run(mirror.rename_mirror_files(db, rule))
    assert out["renamed"] == 0 and out["skipped"] == 1, out
    assert FakeAPI.renamed == []
    db.expire_all()
    r2 = db.get(MirrorFile, row.id)
    assert r2.rename_status == "done" and r2.target_name == EN_NAME


def test_listing_failure_marks_pending_visible_without_rename(db):
    """列目录失败 = 没读到，不能据此乱改名；标待可见，下轮复查。"""
    rule, media, row = _seed(db)
    FakeAPI.reset(fail_listing=True)
    with _with_rename_mocks():
        out = _run(mirror.rename_mirror_files(db, rule))
    assert out["renamed"] == 0 and out["waiting_visible"] == 1, out
    assert FakeAPI.renamed == []
    db.expire_all()
    r2 = db.get(MirrorFile, row.id)
    assert r2.rename_status == "pending_visible" and r2.target_name == EN_NAME


def test_no_synced_rows_is_reported(db):
    """没有可改名文件时也必须给出原因，不能再返回三个零让人猜。"""
    rule, media, row = _seed(db, status="copying")
    FakeAPI.reset(files=[_b_file(ZH_NAME)])
    with _with_rename_mocks():
        out = _run(mirror.rename_mirror_files(db, rule))
    assert out["no_rows"] == 1 and out["renamed"] == 0, out


# ---------- 3. 补复制分支必须清空旧改名状态 ----------
def test_recopy_clears_stale_rename_state(db):
    rule, media, row = _seed(db)
    SyncAPI.reset()  # B 库作品目录为空
    with patch("app.mirror.GuangYaWebFiles", SyncAPI), \
         patch("app.mirror.asyncio.sleep", new=_noop_sleep()):
        out = _run(mirror.sync_rule(db, rule))
    assert out["missing_in_b"] == 1, out
    assert len(SyncAPI.copies) == 1
    db.expire_all()
    r2 = db.get(MirrorFile, row.id)
    assert r2.target_name == "" and r2.rename_status == "" and r2.rename_error == ""
    assert r2.file_name == ZH_NAME and r2.source_file_id == A_FILE


# ---------- 4. 整体异常不再只留在本次响应里 ----------
def test_safe_rename_failure_is_surfaced_and_persisted(db):
    rule, media, row = _seed(db)
    with patch("app.main.mirror.sync_rule", new=AsyncMock(return_value={"media": 1})), \
         patch("app.main.mirror.backfill_target_file_ids", new=AsyncMock(return_value=0)), \
         patch("app.mirror.rename_mirror_files",
               new=AsyncMock(side_effect=RuntimeError("TMDB 网关整体不可用"))), \
         patch("app.main._mirror_post_sync_pipeline", new=AsyncMock(return_value={})):
        out = _run(main.mirror_run(db=db))
    assert out["rename"]["errors"], out
    assert "文件改名未执行" in out["rename"]["errors"][0]
    assert out["rename"]["no_rows"] == 0
    db.expire_all()
    assert "首个改名错误" in (db.get(MirrorRule, rule.id).last_message or "")


def test_frontend_prompt_exposes_skip_reasons():
    vue = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    for s in ("已达标跳过", "无中文跳过", "无待改名文件", "首个改名错误",
              "本次未自动改名（未返回统计）", "r?.errors?.length"):
        assert s in vue, s


# ---------- 5. 诊断写入本身不能把整次同步拖成 failed ----------
def test_rename_error_survives_null_last_message(db):
    """旧库 / 手工 SQL 可能留下 NULL 的 last_message；追加诊断不能抛 TypeError。"""
    from sqlalchemy.orm.attributes import set_committed_value
    rule, media, row = _seed(db)
    set_committed_value(rule, "last_message", None)
    with patch("app.main.mirror.sync_rule", new=AsyncMock(return_value={"media": 1})), \
         patch("app.main.mirror.backfill_target_file_ids", new=AsyncMock(return_value=0)), \
         patch("app.mirror.rename_mirror_files",
               new=AsyncMock(side_effect=RuntimeError("TMDB 网关整体不可用"))), \
         patch("app.main._mirror_post_sync_pipeline", new=AsyncMock(return_value={})):
        out = _run(main.mirror_run(db=db))
    assert out["rename"]["errors"], out
    db.expire_all()
    assert "首个改名错误" in (db.get(MirrorRule, rule.id).last_message or "")


# ---------- 6. v0.9.2：复制完成后文件可见性延迟，改名必须等看见再发 ----------
def test_rename_waits_for_b_visibility(db):
    """第一次列目录还没看到文件 → 不调用 rename；复查看到后才改名。"""
    class VisibilityAPI(FakeAPI):
        calls = 0

        async def list_files(self, parent_id="", page=0, page_size=50):
            type(self).calls += 1
            if self.calls == 1:
                return {"list": [], "total": 0}
            return {"list": [dict(f) for f in self.b_files], "total": len(self.b_files)}

    rule, media, row = _seed(db, rename_status="", target_name="", target_file_id="")
    FakeAPI.reset(files=[_b_file(ZH_NAME)])
    VisibilityAPI.calls = 0
    with patch("app.mirror.GuangYaWebFiles", VisibilityAPI), \
         patch("app.mirror.tmdb.english_title", new=AsyncMock(return_value="The Drunk Hero")), \
         patch("app.mirror.ai_assist.translate_title", new=AsyncMock(
             return_value={"ok": True, "titles": {"en": "The Drunk Hero"}})), \
         patch("app.mirror.asyncio.sleep", new=_noop_sleep()):
        out = _run(mirror.rename_mirror_files(db, rule))
    assert VisibilityAPI.calls >= 2
    assert out["renamed"] == 1 and out["waiting_visible"] == 0, out
    assert FakeAPI.renamed == [(B_FILE, EN_NAME)]
    db.expire_all()
    r2 = db.get(MirrorFile, row.id)
    assert r2.rename_status == "done" and r2.target_name == EN_NAME


def test_background_job_rechecks_pending_visible(db):
    """后台短任务只复查 pending/failed 行，不重复扫已经 done 的文件。"""
    rule, media, row = _seed(db, rename_status="pending_visible", target_name="")
    with patch("app.main._safe_rename", new=AsyncMock(return_value={"renamed": 1})) as mock:
        _run(main._job_rename_pending())
    assert mock.await_count == 1
    row.rename_status = "done"
    db.commit()
    with patch("app.main._safe_rename", new=AsyncMock(return_value={"renamed": 0})) as mock:
        _run(main._job_rename_pending())
    assert mock.await_count == 0
