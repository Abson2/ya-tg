"""v0.8.2 回归：镜像建目录失败必须可诊断，且不能被静默当成「空目录」。

锁死用户实测问题（42 部作品全部 missing / blocked / 文件 0）：
  - create_dir 的真实报错写入 MirrorDir.error，并原样出现在状态接口里，
    不再被硬编码的「请在 B 库该父目录下建立此目录后重新同步」覆盖。
  - B 库根目录不可访问时明确中止，不把访问失败误判成「B 库没有这个目录」，
    更不会逐部作品调 42 次 create_dir 全部失败。
  - 网盘拒绝带 · 目录名时自动降级为不带标记的标准影视名，同步仍能完成。
  - v0.8.3：旧 * 名称就地换成 ·，真实环境 code=141 不再出现。
  - 自动创建被关闭时，失败原因明确说明是开关关闭。
  - 状态接口回显实际生效配置，一眼看出 auto_create_dirs 是否为 False。
"""
import asyncio
from unittest.mock import patch

import pytest
from sqlalchemy import select

from app import mirror
from app.db import SessionLocal, migrate
from app.guangya import WebFileError
from app.main import MirrorRule, QueueItem, Resource, ScanTask, mirror_status
from app.models import MirrorDir, MirrorFile, MirrorMedia, MirrorTask

TABLES = [MirrorFile, MirrorTask, MirrorDir, MirrorMedia, MirrorRule, QueueItem,
          Resource, ScanTask]
A_ROOT, B_ROOT = "A_ROOT", "B_ROOT"
A_WORK = "A_W1"
WORK_NAME = "侠客行 (2001) {tmdb-135646}"


def _clean():
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        db.commit()


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    _clean()
    yield
    _clean()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as session:
        try:
            yield session
        finally:
            session.rollback()


def _rule(db, **kw):
    r = MirrorRule(name="测试镜像", source_root_id=A_ROOT, target_root_id=B_ROOT,
                   interval_minutes=60, enabled=True, file_idle_seconds=0.5,
                   auto_create_dirs=True, star_naming=True)
    for k, v in kw.items():
        setattr(r, k, v)
    db.add(r)
    db.commit()
    return r


class FakeAPI:
    """A 库：根目录 → 分类目录 → 作品目录 → 单集；B 库默认可读且为空。"""

    created = []
    copies = []
    fail_b_root = False
    fail_create = False
    reject_star = False
    reject_dot = False

    def __init__(self, db):
        self.db = db

    async def list_files(self, parent_id, page=0, page_size=100):
        if page > 0:
            return {"list": [], "total": 0}
        if parent_id == B_ROOT and self.fail_b_root:
            raise WebFileError("网页端文件列表失败 code=403 msg=无权限访问该目录", 403)
        if parent_id == A_ROOT:
            return {"list": [{"fileId": "CAT", "resType": 2,
                              "fileName": "动画电影"}], "total": 1}
        if parent_id == "CAT":
            return {"list": [{"fileId": A_WORK, "resType": 2,
                              "fileName": WORK_NAME}], "total": 1}
        if parent_id == A_WORK:
            return {"list": [{"fileId": "F1", "resType": 1,
                              "fileName": "X.S01E01.mkv", "fileSize": 100,
                              "utime": 1}], "total": 1}
        return {"list": [], "total": 0}

    async def create_dir(self, parent_id, dir_name):
        if self.fail_create:
            raise WebFileError("网页端 /userres/v1/file/create_dir 失败 "
                               "code=400 msg=目录名包含非法字符", 400)
        if self.reject_star and "*" in dir_name:
            raise WebFileError("网页端 /userres/v1/file/create_dir 失败 "
                               "code=141 msg=文件夹名包含非法字符: *", 141)
        if self.reject_dot and "·" in dir_name:
            raise WebFileError("网页端 /userres/v1/file/create_dir 失败 "
                               "code=141 msg=文件夹名包含非法字符: ·", 141)
        self.created.append((parent_id, dir_name))
        return {"file_id": f"B_{len(self.created)}", "name": dir_name,
                "parent_id": parent_id, "raw": {}}

    async def copy_file(self, file_ids, parent_id):
        self.copies.append((list(file_ids), parent_id))
        return "T1"

    async def get_task_status(self, task_id):
        return {"status": 2, "raw": {"status": 2}}


def _reset(**kw):
    FakeAPI.created, FakeAPI.copies = [], []
    FakeAPI.fail_b_root = False
    FakeAPI.fail_create = False
    FakeAPI.reject_star = False
    FakeAPI.reject_dot = False
    for k, v in kw.items():
        setattr(FakeAPI, k, v)


# ---------- 1. 真实报错不再被通用文案吞掉 ----------
def test_create_dir_failure_is_recorded_and_returned(db):
    r = _rule(db)
    _reset(fail_create=True)
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        out = asyncio.run(mirror.sync_rule(db, r))

    assert out["dirs_created"] == 0 and out["blocked"] == 1, out
    assert out["dir_errors"], out
    assert "非法字符" in out["dir_errors"][0], out["dir_errors"]
    assert "create_dir 失败" in out["missing_dirs"][0]["error"], out["missing_dirs"]

    rows = list(db.scalars(select(MirrorDir)).all())
    assert rows and rows[0].status == "missing", rows
    assert "非法字符" in (rows[0].error or ""), rows[0].error
    assert "首个建目录错误" in (r.last_message or ""), r.last_message

    st = mirror_status(db=db)
    assert st["missing_dirs"], st
    assert "非法字符" in st["missing_dirs"][0]["error"], st["missing_dirs"]
    assert "非法字符" in st["missing_dirs"][0]["tip"], st["missing_dirs"]
    assert st["missing_dirs"][0]["tip"] != "在 B 库该父目录下建立此目录后重新同步"
    assert st["dirs"][0]["error"], st["dirs"]
    assert st["counts"]["dirs_failed"] == 1, st["counts"]


# ---------- 2. B 库根目录不可访问 ≠ B 库是空目录 ----------
def test_unreadable_b_root_aborts_instead_of_42_failed_creates(db):
    r = _rule(db)
    _reset(fail_b_root=True)
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        with pytest.raises(WebFileError) as e:
            asyncio.run(mirror.sync_rule(db, r))

    msg = str(e.value)
    assert "B 镜像库根目录不可访问" in msg and B_ROOT in msg, msg
    assert "无权限访问" in msg, msg
    # 关键：不能逐部作品试建目录，更不能留下 42 条 missing 记录
    assert FakeAPI.created == [], FakeAPI.created
    assert db.query(MirrorDir).count() == 0
    assert db.query(MirrorMedia).count() == 0


# ---------- 3. 网盘拒绝 · 名称时自动降级，同步仍能完成 ----------
def test_dot_name_rejected_falls_back_to_plain_name(db):
    r = _rule(db)
    _reset(reject_dot=True)
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        out = asyncio.run(mirror.sync_rule(db, r))

    assert out["new"] == 1 and out["done"] == 1, out
    assert out["dirs_fallback"] == 1, out
    assert out["missing_dirs"] == [], out
    assert out["dirs_created"] == 2, out
    names = [n for _, n in FakeAPI.created]
    assert names[0] == "动画电影", names
    assert "*" not in names[1] and "·" not in names[1], names  # 降级后不带任何标记
    assert names[1] == WORK_NAME, names

    work_row = db.scalar(select(MirrorDir).where(MirrorDir.kind == "work"))
    assert work_row.status == "created_fallback", work_row.status
    assert "降级" in (work_row.error or ""), work_row.error
    media = db.scalar(select(MirrorMedia))
    assert media.title_mirror == WORK_NAME, media.title_mirror
    assert media.status == "synced", media.status
    assert db.query(QueueItem).count() == 0     # 镜像仍不推 TG
    st = mirror_status(db=db)
    assert st["counts"]["dirs_fallback"] == 1, st["counts"]


# ---------- 3b. 真实环境 code=141：旧 * 名称就地换成 ·，不再被网盘拒绝 ----------
def test_legacy_star_is_converted_to_dot_no_code_141(db):
    r = _rule(db)
    _reset(reject_star=True)  # 网盘只拒绝 *，模拟用户实测的 code=141
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        out = asyncio.run(mirror.sync_rule(db, r))

    assert out["new"] == 1 and out["done"] == 1, out
    assert out["dirs_fallback"] == 0 and out["missing_dirs"] == [], out
    names = [n for _, n in FakeAPI.created]
    assert all("*" not in n for n in names), names
    assert names[1].startswith("侠·客·行"), names
    assert "(2001) {tmdb-135646}" in names[1], names


# ---------- 4. 自动创建被关闭时给出明确原因 ----------
def test_auto_create_disabled_error_explains_the_switch(db):
    r = _rule(db, auto_create_dirs=False)
    _reset()
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        out = asyncio.run(mirror.sync_rule(db, r))

    assert out["dirs_created"] == 0 and out["missing_dirs"], out
    assert FakeAPI.created == [], FakeAPI.created
    assert "自动创建目录已关闭" in out["missing_dirs"][0]["error"], out["missing_dirs"]
    rows = list(db.scalars(select(MirrorDir)).all())
    assert rows and "自动创建目录已关闭" in (rows[0].error or ""), rows[0].error


# ---------- 5. 状态接口回显实际生效配置 ----------
def test_mirror_status_returns_effective_config(db):
    _rule(db, auto_create_dirs=False, star_naming=False, file_idle_seconds=2.0)
    st = mirror_status(db=db)
    cfg = st["config"]
    assert cfg["auto_create_dirs"] is False, cfg
    assert cfg["star_naming"] is False, cfg
    assert cfg["target_root_id"] == B_ROOT, cfg
    assert cfg["source_root_id"] == A_ROOT, cfg
    assert cfg["file_idle_seconds"] == 2.0, cfg
    assert st["min_idle_seconds"] == mirror.MIN_FILE_IDLE_SECONDS

# ---------- 6. 自动创建开关在配置接口里如实回显 ----------
def test_mirror_config_exposes_auto_create_switch(db):
    from app.main import mirror_config_get

    r = _rule(db)
    assert mirror_config_get(db=db)["auto_create_dirs"] is True
    r.auto_create_dirs = False
    db.commit()
    assert mirror_config_get(db=db)["auto_create_dirs"] is False
    assert mirror_status(db=db)["config"]["auto_create_dirs"] is False
