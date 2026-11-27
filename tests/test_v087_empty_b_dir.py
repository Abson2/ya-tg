"""v0.8.7 回归：B 库影视文件夹下为空目录（用户反复反馈的第一个 bug）。

用户三条口径（2026-11-18）：
  1. 「又出现之前的问题，B库内影视文件夹下为空目录」；
  2. 「资源库里面刷新定义为读取最新的B库内容，B库文件夹已无该影视则清理出面板」；
  3. 「目录结构未来可能是 影视/电视剧/国产剧/侠客行（2001），目录树也需要参考一起镜像，
      只改动侠客行文件夹及单集」。

本文件先锁死第 1 条的代码根因：v0.8.4 只修了「copying/failed/unknown 被当成
unchanged」，但**DB 标记 synced 同样不代表 B 库真的有文件**——文件被删、B 库换目录、
复制任务没落地时，记录永远停在 synced，再点几次「立即同步」都不会重发复制。

v0.8.7 起 sync_rule 每部作品只读递归列一次 B 库核对：
  - 列目录完整可信 + 确实找不到 → 复用同一行重置为 pending，自动重新复制；
  - 列目录失败 / 分页不完整 / 目录层级超限 → 一律不动 synced（防接口抖动重复复制）；
  - B 库文件数不少于已同步记录数 → 绝不重置（多半只是改名 / 重名加后缀）。
"""
import asyncio
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from app import main, mirror
from app.db import SessionLocal, migrate
from app.models import (AuditLog, MirrorDir, MirrorFile, MirrorMedia, MirrorRule,
                        MirrorTask, QueueItem, Resource, ResourceScanBatch)
from app.utils import normalize_work_name

ROOT = Path(__file__).resolve().parents[1]
A_ROOT, A_WORK, A_FILE = "A_ROOT", "A_WORK", "F1"
B_ROOT, B_WORK, B_FILE = "B_ROOT", "B_WORK", "BF1"
SEASON = "B_SEASON"
WORK_A = "侠客行 (2002) {tmdb-135646}"
WORK_B = "侠·客·行 (2002) {tmdb-135646}"
FNAME = "侠客行 第01集 2160p.WEB-DL.H.265.mkv"

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
    """A 库固定一棵目录树；B 库作品目录按 mode 切换「有文件 / 空 / 列目录失败」等。"""

    mode = "has_file"
    b_files = []
    copies = []
    dir_writes = []

    def __init__(self, db):
        self.db = db

    @classmethod
    def reset(cls, mode="has_file", files=None):
        cls.mode = mode
        cls.copies = []
        cls.dir_writes = []
        cls.b_files = [dict(f) for f in (files if files is not None else [_bfile()])]

    @staticmethod
    def _b_listing():
        return {"list": [dict(f) for f in FakeAPI.b_files],
                "total": len(FakeAPI.b_files)}

    async def list_files(self, parent_id, page=0, page_size=50):
        if page > 0:
            return {"list": [], "total": 0}
        if parent_id == A_ROOT:
            return {"list": [{"fileId": A_WORK, "resType": 2, "fileName": WORK_A}],
                    "total": 1}
        if parent_id == A_WORK:
            return {"list": [{"fileId": A_FILE, "resType": 1, "fileName": FNAME,
                              "fileSize": 100, "utime": 1, "parentId": A_WORK}],
                    "total": 1}
        if parent_id == B_ROOT:
            return {"list": [{"fileId": B_WORK, "resType": 2, "fileName": WORK_B}],
                    "total": 1}
        if parent_id == B_WORK:
            if self.mode == "broken":
                raise RuntimeError("B 库列目录超时")
            if self.mode == "incomplete":
                return {"list": [], "total": 99}  # 分页不完整 → verified=False
            if self.mode == "deep":
                return {"list": [{"fileId": SEASON, "resType": 2,
                                  "fileName": "Season 1"}], "total": 1}
            if self.mode == "empty":
                return {"list": [], "total": 0}
            return self._b_listing()
        if parent_id == SEASON:
            return self._b_listing()
        return {"list": [], "total": 0}

    async def copy_file(self, file_ids, parent_id):
        self.copies.append((list(file_ids), parent_id))
        return f"TASK{len(self.copies)}"

    async def get_task_status(self, task_id):
        return {"status": 2, "detail": {}, "raw": {"status": 2}}

    async def create_dir(self, parent_id, dir_name):
        self.dir_writes.append((parent_id, dir_name))
        return {"file_id": "NEW_DIR"}

    async def rename_file(self, file_id, new_name):
        raise AssertionError("本组用例不涉及改名")


def _bfile(name=FNAME, fid=B_FILE, size=100):
    return {"fileId": fid, "fileName": name, "fileSize": size, "resType": 1}


def _seed(db, *, status="synced", target_file_id=B_FILE, target_name=""):
    """造一条「历史上已同步」的记录；B 库是否真有文件由 FakeAPI.mode 决定。"""
    rule = MirrorRule(name="v087", source_root_id=A_ROOT, target_root_id=B_ROOT,
                      enabled=True, star_naming=True, auto_identify=False,
                      rename_files=False, file_idle_seconds=0.5, auto_create_dirs=True)
    db.add(rule)
    db.flush()
    media = MirrorMedia(rule_id=rule.id,
                        work_key="work:" + normalize_work_name(WORK_A),
                        title_standard=WORK_A, title_mirror=WORK_B,
                        source_dir_id=A_WORK, target_dir_id=B_WORK, status="synced")
    db.add(media)
    db.flush()
    row = MirrorFile(media_id=media.id, rule_id=rule.id,
                     identity="tmdb:135646:tv:1:1", source_file_id=A_FILE,
                     target_file_id=target_file_id, file_name=FNAME,
                     target_name=target_name, size=100, utime=1,
                     fingerprint="100:1", status=status)
    db.add(row)
    db.commit()
    return rule, media, row


def _run(coro):
    return asyncio.run(coro)


# ---------- 1. 空目录 bug 的核心回归 ----------
def test_synced_but_b_empty_is_requeued_and_recopied(db):
    """DB 说 synced、B 库却空 → 必须自动重新复制，不能再判 unchanged。"""
    rule, media, row = _seed(db)
    FakeAPI.reset(mode="empty")
    with patch("app.mirror.GuangYaWebFiles", FakeAPI), \
         patch("app.mirror.asyncio.sleep", new=_noop_sleep()):
        out = _run(mirror.sync_rule(db, rule))
    assert out["missing_in_b"] == 1, out
    assert len(FakeAPI.copies) == 1, "空目录必须重新发起一次复制"
    assert FakeAPI.copies[0][0] == [A_FILE]
    db.expire_all()
    assert db.get(MirrorFile, row.id).status == "synced"
    assert db.get(MirrorFile, row.id).error == ""


def test_b_has_file_never_recopies(db):
    """B 库文件还在 → 判 unchanged，绝不重复复制（v0.8.0 增量红线）。"""
    rule, media, row = _seed(db)
    FakeAPI.reset(mode="has_file")
    with patch("app.mirror.GuangYaWebFiles", FakeAPI), \
         patch("app.mirror.asyncio.sleep", new=_noop_sleep()):
        out = _run(mirror.sync_rule(db, rule))
    assert out["missing_in_b"] == 0 and out["unchanged"] == 1, out
    assert FakeAPI.copies == []


def test_broken_or_incomplete_listing_does_not_trigger_recopy(db):
    """列目录失败 / 分页不完整 = 没读到，不等于 B 库空。绝不误判重复复制。"""
    for mode in ("broken", "incomplete"):
        _clean()
        db.expunge_all()  # _clean 用的是另一个 Session，清掉本 Session 的 identity map
        FakeAPI.reset(mode=mode)
        rule, media, row = _seed(db)
        with patch("app.mirror.GuangYaWebFiles", FakeAPI), \
             patch("app.mirror.asyncio.sleep", new=_noop_sleep()):
            out = _run(mirror.sync_rule(db, rule))
        assert out["missing_in_b"] == 0, (mode, out)
        assert FakeAPI.copies == [], (mode, "接口抖动不得触发重复制")


def test_deep_subdirectory_files_are_not_mistaken_as_empty(db):
    """剧集在 作品/Season 1/ 深层时，递归列目录必须找到，不能误判为空。"""
    rule, media, row = _seed(db)
    FakeAPI.reset(mode="deep")
    with patch("app.mirror.GuangYaWebFiles", FakeAPI), \
         patch("app.mirror.asyncio.sleep", new=_noop_sleep()):
        out = _run(mirror.sync_rule(db, rule))
    assert out["missing_in_b"] == 0, out
    assert FakeAPI.copies == []


def test_name_mismatch_with_enough_files_never_recopies(db):
    """B 库文件数不少于已同步记录数时，名字对不上也绝不重置（防网盘改名/加后缀误判）。"""
    rule, media, row = _seed(db, target_name="The Drunk Hero 第01集 2160p.mkv")
    FakeAPI.reset(mode="has_file", files=[_bfile(name="B 库自动加了 (1) 后缀.mkv")])
    with patch("app.mirror.GuangYaWebFiles", FakeAPI), \
         patch("app.mirror.asyncio.sleep", new=_noop_sleep()):
        out = _run(mirror.sync_rule(db, rule))
    assert out["missing_in_b"] == 0 and out["name_mismatch"] == 1, out
    assert FakeAPI.copies == []


def test_reset_uses_same_row_and_keeps_a_original_name(db):
    """补复制复用同一行，file_name 仍是 A 原名，A→B 映射不丢。"""
    rule, media, row = _seed(db)
    before = row.id
    FakeAPI.reset(mode="empty")
    with patch("app.mirror.GuangYaWebFiles", FakeAPI), \
         patch("app.mirror.asyncio.sleep", new=_noop_sleep()):
        _run(mirror.sync_rule(db, rule))
    rows = list(db.scalars(select(MirrorFile)).all())
    assert len(rows) == 1 and rows[0].id == before
    assert rows[0].file_name == FNAME
    assert rows[0].source_file_id == A_FILE


def _noop_sleep():
    async def _sleep(*_a, **_kw):
        return None
    return _sleep


# ---------- 2. 三个刷新入口统一「读取最新 B 库内容」 ----------
def _batch(db, **kw):
    b = ResourceScanBatch(root_id=B_ROOT, status="success", complete=True,
                          resumed=False, reconcile_allowed=False,
                          files=0, directories=3, seen_count=0, seen_json="[]")
    for k, v in kw.items():
        setattr(b, k, v)
    db.add(b)
    db.commit()
    return b


def _old_resources(db, n=2):
    for i in range(n):
        db.add(Resource(file_id=f"OLD_{i}", name=f"旧影视{i}.mkv", ext=".mkv",
                        size=1, utime=1))
    db.commit()


def test_trusted_empty_batch_clears_panel_on_user_refresh(db):
    """B 库确实空、批次完整且扫到目录 → 用户主动刷新把旧作品清出面板。"""
    b = _batch(db)
    _old_resources(db)
    out = _run(main._reconcile_library(db, [], scan_complete=True, resumed=False,
                                       batch_id=b.id, allow_empty=True))
    assert out["removed"] == 2 and out["empty_seen_allowed"] is True, out
    db.commit()  # SessionLocal autoflush=False，端点提交后才真正落库
    assert db.query(Resource).count() == 0


def test_empty_batch_without_explicit_allow_is_still_skipped(db):
    """非用户主动刷新（定时/其它链路）必须保持 empty_seen 保护。"""
    b = _batch(db)
    _old_resources(db)
    out = _run(main._reconcile_library(db, [], scan_complete=True, resumed=False,
                                       batch_id=b.id, allow_empty=False))
    assert out["skipped"] == "batch_not_allowed", out
    assert db.query(Resource).count() == 2


def test_empty_scan_without_directories_is_untrusted(db):
    """B 库根目录都读不到（directories=0）时不信任，绝不清空资源库。"""
    b = _batch(db, directories=0)
    _old_resources(db)
    out = _run(main._reconcile_library(db, [], scan_complete=True, resumed=False,
                                       batch_id=b.id, allow_empty=True))
    assert out["skipped"] == "empty_scan_untrusted", out
    assert db.query(Resource).count() == 2


def test_incomplete_or_failed_batch_never_clears(db):
    for kw in ({"complete": False}, {"status": "failed"}, {"resumed": True}):
        _clean()
        db.expunge_all()
        b = _batch(db, **kw)
        _old_resources(db)
        out = _run(main._reconcile_library(db, [], scan_complete=True, resumed=False,
                                           batch_id=b.id, allow_empty=True))
        assert out["skipped"] == "batch_not_allowed", (kw, out)
        assert db.query(Resource).count() == 2, kw


def test_three_refresh_entries_all_reconcile_with_allow_empty(db):
    """顶部刷新 / 立即扫描 / 重新扫描库都必须走同一重建管线并允许可信空批次。"""
    b = _batch(db)
    scan_data = {"files": 0, "directories": 3, "resumed": False, "complete": True,
                 "seen_file_ids": [], "batch_id": b.id, "change_events": []}
    with patch("app.main._effective_scan_root", return_value=(B_ROOT, "mirror_b")), \
         patch("app.main.run_scan", new=AsyncMock(return_value=scan_data)), \
         patch("app.main._post_scan_identify", new=AsyncMock(return_value={})), \
         patch("app.main.process_change_events", return_value={}), \
         patch("app.main._reconcile_library",
               new=AsyncMock(return_value={"removed": 0})) as rc:
        _run(main.scan_now(db=db))
        _run(main.rescan_library(db=db))
        _run(main.refresh_library(db=db))
    assert rc.await_count == 3
    assert all(c.kwargs.get("allow_empty") is True for c in rc.await_args_list)
    assert all(c.kwargs.get("batch_id") == b.id for c in rc.await_args_list)


def test_scheduled_scan_reconciles_but_never_with_empty_batch(db):
    """v2.1.56（用户口径 2026-11-28）：定时扫描发现 B 库已无的内容，资源库也要移除。

    此前定时扫描**完全不做对账**，B 库整部删掉后资源库那行会一直挂着，
    用户看到的表现就是「资源库里的作品删不掉」。现在接入对账，但**绝不传
    allow_empty** —— 定时扫描不许用空批次清库，接口抖动时宁可保留旧数据。
    """
    @contextmanager
    def _sess():
        yield db

    b = _batch(db)
    scan_data = {"files": 0, "directories": 3, "resumed": False, "complete": True,
                 "seen_file_ids": [], "batch_id": b.id, "change_events": []}
    with patch("app.main.SessionLocal", _sess), \
         patch("app.main._effective_scan_root", return_value=(B_ROOT, "mirror_b")), \
         patch("app.main.run_scan", new=AsyncMock(return_value=scan_data)), \
         patch("app.main._post_scan_identify", new=AsyncMock(return_value={})), \
         patch("app.main.process_change_events", return_value={}), \
         patch("app.main._reconcile_library",
               new=AsyncMock(return_value={"removed": 0})) as rc:
        _run(main._scheduled_scan(B_ROOT))
    assert rc.await_count == 1, "定时扫描必须对账"
    kw = rc.await_args.kwargs
    assert kw.get("allow_empty") is False, "定时扫描绝不可以用空批次清库"
    assert kw.get("batch_id") == b.id
    assert kw.get("scan_complete") is True
    assert kw.get("resumed") is False


def test_scheduled_scan_reconcile_failure_does_not_break_the_round(db):
    """对账单独 try：它失败不能把这一轮的扫描 / 识别一起废掉。"""
    @contextmanager
    def _sess():
        yield db

    b = _batch(db)
    scan_data = {"files": 5, "directories": 3, "resumed": False, "complete": True,
                 "seen_file_ids": ["OLD_0"], "batch_id": b.id, "change_events": []}
    ident = AsyncMock(return_value={})
    with patch("app.main.SessionLocal", _sess), \
         patch("app.main._effective_scan_root", return_value=(B_ROOT, "mirror_b")), \
         patch("app.main.run_scan", new=AsyncMock(return_value=scan_data)), \
         patch("app.main._post_scan_identify", new=ident), \
         patch("app.main.process_change_events", return_value={}), \
         patch("app.main._reconcile_library",
               new=AsyncMock(side_effect=RuntimeError("对账炸了"))):
        _run(main._scheduled_scan(B_ROOT))
    assert ident.await_count == 1, "对账失败后识别仍要继续"


# ---------- 3. 多层分类目录树 1:1 镜像 ----------
class DeepTreeAPI:
    """A：影视/电视剧/国产剧/侠客行（2001）/Season 1/E01.mkv；B 初始空，逐层自动建。"""

    a_tree = {}
    b_tree = {}
    created = []
    copies = []
    renames = []

    def __init__(self, db):
        self.db = db

    @classmethod
    def reset(cls):
        def d(fid, name):
            return {"fileId": fid, "fileName": name, "resType": 2}

        def f(fid, name):
            return {"fileId": fid, "fileName": name, "resType": 1,
                    "fileSize": 100, "utime": 1}

        cls.a_tree = {
            A_ROOT: [d("CAT1", "影视")],
            "CAT1": [d("CAT2", "电视剧")],
            "CAT2": [d("CAT3", "国产剧")],
            "CAT3": [d("WORK", "侠客行（2001）")],
            "WORK": [d("SEASON", "Season 1")],
            "SEASON": [f("F1", "侠客行 第01集 2160p.WEB-DL.H.265.mkv")],
        }
        cls.b_tree = {}
        cls.created = []
        cls.copies = []
        cls.renames = []

    async def list_files(self, parent_id, page=0, page_size=50):
        if page > 0:
            return {"list": [], "total": 0}
        if parent_id == B_ROOT or str(parent_id).startswith("BDIR"):
            items = self.b_tree.get(parent_id, [])
        else:
            items = self.a_tree.get(parent_id, [])
        return {"list": [dict(x) for x in items], "total": len(items)}

    async def create_dir(self, parent_id, dir_name):
        fid = f"BDIR{len(self.created) + 1}"
        self.created.append((parent_id, dir_name))
        self.b_tree.setdefault(parent_id, []).append(
            {"fileId": fid, "fileName": dir_name, "resType": 2})
        return {"file_id": fid}

    async def copy_file(self, file_ids, parent_id):
        self.copies.append((list(file_ids), parent_id))
        return "TASK1"

    async def get_task_status(self, task_id):
        return {"status": 2, "detail": {}, "raw": {"status": 2}}

    async def rename_file(self, file_id, new_name):
        self.renames.append((file_id, new_name))


def _deep_rule(db):
    r = MirrorRule(name="v087-tree", source_root_id=A_ROOT, target_root_id=B_ROOT,
                   enabled=True, star_naming=True, auto_identify=False,
                   rename_files=False, file_idle_seconds=0.5, auto_create_dirs=True)
    db.add(r)
    db.commit()
    return r


def test_deep_category_tree_is_mirrored_verbatim(db):
    """影视/电视剧/国产剧 逐层原名镜像，只有作品主目录按 · 命名。"""
    rule = _deep_rule(db)
    DeepTreeAPI.reset()
    with patch("app.mirror.GuangYaWebFiles", DeepTreeAPI), \
         patch("app.mirror.asyncio.sleep", new=_noop_sleep()):
        out = _run(mirror.sync_rule(db, rule))
    names = [n for _, n in DeepTreeAPI.created]
    assert names[:3] == ["影视", "电视剧", "国产剧"], names
    assert "侠·客·行" in names[3] and "（2001）" in names[3], names
    assert names[4] == "Season 1", names
    assert out["new"] == 1 and out["done"] == 1, out


def test_deep_tree_only_changes_work_dir_and_episode(db):
    """A 库全程只读；分类目录不被改名，也没有任何针对 A 的写操作。"""
    rule = _deep_rule(db)
    DeepTreeAPI.reset()
    with patch("app.mirror.GuangYaWebFiles", DeepTreeAPI), \
         patch("app.mirror.asyncio.sleep", new=_noop_sleep()):
        _run(mirror.sync_rule(db, rule))
    # create_dir 的 parent 必须全部是 B 库目录（B_ROOT / BDIR*）
    assert all(p == B_ROOT or str(p).startswith("BDIR")
               for p, _ in DeepTreeAPI.created), DeepTreeAPI.created
    assert DeepTreeAPI.renames == []
    # 复制目标是「作品主目录下的 Season 1」，不是分类目录
    assert len(DeepTreeAPI.copies) == 1
    assert DeepTreeAPI.copies[0][0] == ["F1"]
    assert str(DeepTreeAPI.copies[0][1]).startswith("BDIR")
    media = db.scalar(select(MirrorMedia))
    assert media.category_path == "影视/电视剧/国产剧", media.category_path


def test_work_dir_without_year_but_with_season_child_is_not_a_category():
    """作品名不带年份/TMDB 标记且下挂 Season 1 时，Season 1 不能被当成作品。"""
    from app.mirror import _discover_works

    def n(i, name, parent, children, files):
        return {"id": i, "name": name, "parent": parent,
                "children": children, "files": files}

    nodes = {
        "A_ROOT": n("A_ROOT", "", "", ["C1"], []),
        "C1": n("C1", "影视", "A_ROOT", ["C2"], []),
        "C2": n("C2", "电视剧", "C1", ["C3"], []),
        "C3": n("C3", "国产剧", "C2", ["WORK"], []),
        "WORK": n("WORK", "侠客行", "C3", ["SEASON"], []),
        "SEASON": n("SEASON", "Season 1", "WORK", [],
                    [{"fileId": "F1", "fileName": "E01.mkv"}]),
    }
    works = _discover_works(nodes, "A_ROOT")
    assert len(works) == 1
    assert works[0][0]["name"] == "侠客行"
    assert [c[0] for c in works[0][1]] == ["影视", "电视剧", "国产剧"]


def test_deep_tree_work_without_year_keeps_full_category_chain(db):
    """端到端：作品名无年份时分类链仍是三层，B 库不会多出一层侠客行。"""
    rule = _deep_rule(db)
    DeepTreeAPI.reset()
    DeepTreeAPI.a_tree["CAT3"] = [
        {"fileId": "WORK", "fileName": "侠客行", "resType": 2}]
    with patch("app.mirror.GuangYaWebFiles", DeepTreeAPI), \
         patch("app.mirror.asyncio.sleep", new=_noop_sleep()):
        _run(mirror.sync_rule(db, rule))
    names = [n for _, n in DeepTreeAPI.created]
    assert names[:3] == ["影视", "电视剧", "国产剧"], names
    assert "侠·客·行" in names[3] and names[4] == "Season 1", names
    assert len(names) == 5, names
    assert db.scalar(select(MirrorMedia)).category_path == "影视/电视剧/国产剧"


def test_deep_tree_with_two_works_does_not_recreate_categories(db):
    """多部作品共享分类链时，cache 必须复用，不能重复建目录。"""
    rule = _deep_rule(db)
    DeepTreeAPI.reset()
    DeepTreeAPI.a_tree["CAT3"].append(
        {"fileId": "WORK2", "fileName": "大醉侠（2002）", "resType": 2})
    DeepTreeAPI.a_tree["WORK2"] = [
        {"fileId": "F2", "fileName": "大醉侠 第01集.mkv", "resType": 1,
         "fileSize": 100, "utime": 1}]
    with patch("app.mirror.GuangYaWebFiles", DeepTreeAPI), \
         patch("app.mirror.asyncio.sleep", new=_noop_sleep()):
        _run(mirror.sync_rule(db, rule))
    cat_names = [n for _, n in DeepTreeAPI.created
                 if n in ("影视", "电视剧", "国产剧")]
    assert cat_names == ["影视", "电视剧", "国产剧"], cat_names
