"""v0.8.1 回归：分类目录 1:1 镜像、命名规则、自动建目录、电影目录分享、风控下限。

锁死以下红线：
  - 分类目录在 B 库保持原名，绝不改成带 · 的影视名（原名里的中点也不许被吃掉）。
  - v0.8.3 起标记由 * 改为 ·（网盘 code=141 拒绝 *），只加在主标题。
  - create_dir / rename / delete_file 按抓包原文实现，未取证的接口仍不猜参数。
  - 文件操作空闲时间有不可调低的硬下限（官方写接口 2 次/秒）。
  - 电影与剧集一律按作品目录分享，不按单文件。
  - 扫描根目录直接引用镜像同步里填过的 B 库 ID。
  - 镜像链路依旧绝不写 push_queue。
"""
import asyncio
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from app import mirror
from app.db import SessionLocal, migrate
from app.main import (MirrorConfigInput, MirrorRule, QueueItem, Resource, ScanTask,
                      _effective_scan_root, mirror_config_save)
from app.models import MirrorDir, MirrorFile, MirrorMedia, MirrorTask
from app.utils import NAME_MARK, normalize_work_name, series_dir_info, star_media_name

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
    """A 库：根目录 → 分类目录 → 作品目录 → 单集；B 库初始为空。"""

    created = []
    copies = []

    def __init__(self, db):
        self.db = db

    async def list_files(self, parent_id, page=0, page_size=100):
        if page > 0:
            return {"list": [], "total": 0}
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
        self.created.append((parent_id, dir_name))
        return {"file_id": f"B_{len(self.created)}", "name": dir_name,
                "parent_id": parent_id, "raw": {}}

    async def copy_file(self, file_ids, parent_id):
        self.copies.append((list(file_ids), parent_id))
        return "T1"

    async def get_task_status(self, task_id):
        return {"status": 2, "raw": {"status": 2}}


# ---------- 1. 命名规则 ----------
def test_star_media_name_keeps_year_and_tags_unchanged():
    got = star_media_name("大醉侠 (2002) {tmdb-135646}")
    assert got == "大·醉·侠 (2002) {tmdb-135646}", got
    assert "·(2002)" not in got and "(2002)" in got
    assert "{tmdb-135646}" in got
    # A 标准名与 B 带 * 名必须归一到同一作品键
    assert normalize_work_name(got) == normalize_work_name("大醉侠 (2002) {tmdb-135646}")


def test_star_media_name_ascii_uses_word_separators():
    got = star_media_name("Spider Man No Way Home (2021)")
    assert got == "Spider·Man·No·Way·Home (2021)", got
    assert "S·p·i·d·e·r" not in got


# ---------- 2. 分类目录识别 ----------
def test_discover_works_treats_category_dirs_as_path():
    nodes = {
        A_ROOT: {"id": A_ROOT, "name": "", "parent": "", "path": "",
                 "files": [], "children": ["CAT"]},
        "CAT": {"id": "CAT", "name": "动画电影", "parent": A_ROOT, "path": "动画电影",
                "files": [], "children": [A_WORK]},
        A_WORK: {"id": A_WORK, "name": WORK_NAME, "parent": "CAT",
                 "path": f"动画电影/{WORK_NAME}", "children": [],
                 "files": [{"fileId": "F1", "fileName": "X.S01E01.mkv",
                            "fileSize": 100, "utime": 1}]},
    }
    works = mirror._discover_works(nodes, A_ROOT)
    assert len(works) == 1, works
    work, cats = works[0]
    assert work["id"] == A_WORK
    assert cats == [("动画电影", "CAT")], cats


def test_category_dir_is_never_starred():
    # v0.8.3：分类目录直传 A 库原名，绝不经过标记剥离
    assert "*" not in mirror.standard_media_name("动画电影")
    assert NAME_MARK not in mirror.standard_media_name("动画电影")
    assert mirror.standard_media_name("动画电影") == "动画电影"


# ---------- 3. 自动建目录且不误改分类名 ----------
def test_mirror_auto_creates_category_and_work_dirs(db):
    r = _rule(db)
    FakeAPI.created, FakeAPI.copies = [], []
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        out = asyncio.run(mirror.sync_rule(db, r))
    assert out["new"] == 1 and out["done"] == 1, out
    assert out["dirs_created"] == 2, out
    names = [n for _, n in FakeAPI.created]
    assert names[0] == "动画电影", names          # 分类目录原名，不加标记
    assert names[1].startswith("侠·客·行"), names  # 作品主目录才加 ·
    assert "*" not in names[1] and NAME_MARK in names[1], names
    assert "(2001) {tmdb-135646}" in names[1], names
    assert FakeAPI.copies == [(["F1"], "B_2")], FakeAPI.copies
    # 只锁「sync_rule 复制函数不写队列」；v0.8.5 起上层管线另按规则入队
    assert db.query(QueueItem).count() == 0
    kinds = {d.kind: d.status for d in db.scalars(select(MirrorDir)).all()}
    assert kinds == {"category": "created", "work": "created"}, kinds


def test_auto_create_disabled_only_lists_missing_dirs(db):
    r = _rule(db, auto_create_dirs=False)
    FakeAPI.created, FakeAPI.copies = [], []
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        out = asyncio.run(mirror.sync_rule(db, r))
    assert out["new"] == 0 and out["dirs_created"] == 0, out
    assert out["missing_dirs"], out
    assert FakeAPI.created == [] and FakeAPI.copies == []
    assert all(d.status == "missing" for d in db.scalars(select(MirrorDir)).all())


# ---------- 4. 文件操作风控下限 ----------
def test_file_idle_hard_floor_cannot_be_lowered(db):
    assert mirror.MIN_FILE_IDLE_SECONDS == 0.5
    r = _rule(db, file_idle_seconds=0.01)  # 试图把间隔调到远低于官方限频
    FakeAPI.created, FakeAPI.copies = [], []
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        out = asyncio.run(mirror.sync_rule(db, r))
    assert out["idle_seconds"] == 0.5, out

    with patch("app.main._schedule_mirror_job"):
        with pytest.raises(HTTPException) as e:
            mirror_config_save(MirrorConfigInput(
                source_root_id=A_ROOT, target_root_id=B_ROOT, interval_minutes=60,
                file_idle_seconds=0.1), db=db)
    assert e.value.status_code == 400
    assert "不可调整" in str(e.value.detail)


# ---------- 5. 电影按作品目录分享 ----------
def test_movie_shares_work_directory_not_single_file():
    info = series_dir_info("大醉侠 (2002) {tmdb-135646}", '["P1","R1"]', "", "movie")
    assert info["scope"] == "series_dir" and info["dir_id"] == "P1", info
    assert info["dir_name"] == "大醉侠 (2002) {tmdb-135646}"


def test_movie_scattered_in_category_dir_falls_back_to_file():
    info = series_dir_info("动画电影", '["P1"]', "", "movie")
    assert info["scope"] == "file", info  # 不把整个分类目录当一部作品分享


def test_tv_season_dir_still_uses_work_dir():
    info = series_dir_info("侠客行/Season 1", '["S1","W1","R1"]', "", "tv")
    assert info["scope"] == "series_dir" and info["dir_id"] == "W1", info


# ---------- 6. 扫描根目录自动引用 B 库 ----------
def test_scan_root_reuses_mirror_target(db):
    _rule(db)
    root, source = _effective_scan_root(db)
    assert root == B_ROOT and source == "mirror_target"


def test_scan_root_falls_back_when_no_mirror(db):
    db.add(ScanTask(cron="*/30 * * * *", root_id="OLD_ROOT", enabled=True))
    db.commit()
    root, source = _effective_scan_root(db)
    assert root == "OLD_ROOT" and source == "scan_task"


# ---------- 7. 抓包确认的写接口契约 ----------
def test_web_write_endpoints_match_har_contract(db):
    from app.guangya import GuangYaWebFiles
    from app.models import AuthToken
    from app.security import encrypt_secret

    old = db.get(AuthToken, 1)
    if old:
        db.delete(old)
        db.flush()
    db.add(AuthToken(id=1, flow="sms", access_token_enc=encrypt_secret("tok"),
                     device_id="did-1"))
    db.commit()
    calls = []

    class Capture(GuangYaWebFiles):
        async def _post_web(self, path, body):
            calls.append((path, body))
            if "create_dir" in path:
                return {"fileId": "D1", "fileName": body["dirName"],
                        "parentId": body["parentId"], "resType": 2}
            if "delete_file" in path:
                return {"taskId": "T9"}
            return {}

    api = Capture(db)
    made = asyncio.run(api.create_dir("P1", "动画电影"))
    asyncio.run(api.rename_file("D1", "新名字"))
    tid = asyncio.run(api.delete_files(["D1", "D2"]))
    assert made["file_id"] == "D1" and tid == "T9"
    assert calls[0] == ("/userres/v1/file/create_dir",
                        {"parentId": "P1", "dirName": "动画电影"}), calls[0]
    assert calls[1] == ("/userres/v1/file/rename",
                        {"fileId": "D1", "newName": "新名字"}), calls[1]
    assert calls[2] == ("/userres/v1/file/delete_file",
                        {"fileIds": ["D1", "D2"]}), calls[2]


def test_only_transfer_remains_unverified():
    # v0.8.4 更正取证口径：w.har 里 54 条请求没有任何一次「复制到」操作，
    # copy_file 只有前端 JS 调用契约，不能与 create_dir/rename/delete_file
    # 一起算「抓包已确认」。这里如实列为待复核项。
    assert any("投稿分享转存" in x for x in mirror.UNVERIFIED_ENDPOINTS)
    assert any("copy_file" in x for x in mirror.UNVERIFIED_ENDPOINTS)
    assert any("create_dir" in x for x in mirror.CONFIRMED_WRITE_ENDPOINTS)
    assert any("仅前端 JS 契约" in x for x in mirror.CONFIRMED_WRITE_ENDPOINTS)
