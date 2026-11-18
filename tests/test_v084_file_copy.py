"""v0.8.4 回归：B 库目录建好但文件没镜像过去（用户实测反馈）。

锁死四个已确认的代码缺陷：
  1. sync_rule 旧版把 copying/failed/unknown 的 MirrorFile 也放进 existing
     判 unchanged，上一轮没复制成功的文件被永久跳过，再点同步也没用。
  2. get_task_status 丢掉 data.detail，且只认 status=2 为完成；前端 JS p5 明确
     status=3 / status=2 且 detail.code≠0 都是失败，旧版误记成 unknown。
  3. 文件级错误只在「立即同步」那一次响应里，刷新页面即丢；状态接口无
     files_failed / file_errors，界面显示「已同步」但 B 库是空目录。
  4. list_files 自造 needSubFolderStat、page*100>=total 的分页判定与抓包
     （pageSize=50 / orderBy=3 / sortType=1）不一致，可能把完整目录判成不完整。

另锁死一条安全红线：重试前必须核对 B 库，任务其实已成功但轮询超时的
文件不得重复复制（需求禁止「先删旧再复制」）。
"""
import asyncio
import json
from unittest.mock import patch

import pytest
from sqlalchemy import select

from app import guangya, mirror
from app.db import SessionLocal, migrate
from app.guangya import WebFileError
from app.main import MirrorRule, QueueItem, Resource, ScanTask, mirror_status
from app.models import MirrorDir, MirrorFile, MirrorMedia, MirrorTask

TABLES = [MirrorFile, MirrorTask, MirrorDir, MirrorMedia, MirrorRule, QueueItem,
          Resource, ScanTask]
A_ROOT, B_ROOT = "A_ROOT", "B_ROOT"
CAT, A_WORK = "CAT", "A_W1"
WORK_NAME = "侠客行 (2001) {tmdb-135646}"
FILE_NAME = "X.S01E01.mkv"
A_FILE = {"fileId": "F1", "resType": 1, "fileName": FILE_NAME,
          "fileSize": 100, "utime": 1}


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


def _rule(db):
    r = MirrorRule(name="测试镜像", source_root_id=A_ROOT, target_root_id=B_ROOT,
                   interval_minutes=60, enabled=True, file_idle_seconds=0.5,
                   auto_create_dirs=True, star_naming=True)
    db.add(r)
    db.commit()
    return r


class FakeAPI:
    """A 库：根目录 → 分类 → 作品 → 单集；B 库由 b_files 模拟实际内容。"""

    copies = []
    created = []
    copy_error = None
    task_status = 2
    task_detail = {}
    b_files = {}

    def __init__(self, db):
        self.db = db

    @classmethod
    def reset(cls, **kw):
        cls.copies, cls.created = [], []
        cls.copy_error, cls.task_status, cls.task_detail, cls.b_files = None, 2, {}, {}
        for k, v in kw.items():
            setattr(cls, k, v)

    async def list_files(self, parent_id, page=0, page_size=100):
        if page > 0:
            return {"list": [], "total": 0}
        if parent_id == A_ROOT:
            return {"list": [{"fileId": CAT, "resType": 2,
                              "fileName": "动画电影"}], "total": 1}
        if parent_id == CAT:
            return {"list": [{"fileId": A_WORK, "resType": 2,
                              "fileName": WORK_NAME}], "total": 1}
        if parent_id == A_WORK:
            return {"list": [dict(A_FILE)], "total": 1}
        if parent_id in self.b_files:
            items = list(self.b_files[parent_id])
            return {"list": items, "total": len(items)}
        return {"list": [], "total": 0}

    async def create_dir(self, parent_id, dir_name):
        self.created.append((parent_id, dir_name))
        return {"file_id": f"B_{len(self.created)}", "name": dir_name,
                "parent_id": parent_id, "raw": {}}

    async def copy_file(self, file_ids, parent_id):
        if self.copy_error:
            raise WebFileError(self.copy_error, 400)
        self.copies.append((list(file_ids), parent_id))
        return f"T{len(self.copies)}"

    async def get_task_status(self, task_id):
        return {"status": self.task_status, "detail": dict(self.task_detail),
                "raw": {"status": self.task_status}}


def _sync(db, r, **kw):
    kw.setdefault("max_polls", 1)
    kw.setdefault("poll_interval", 0)
    return asyncio.run(mirror.sync_rule(db, r, **kw))


# ---------- 1. 复制失败后下一轮必须重试（旧版永久跳过） ----------
def test_failed_copy_is_retried_on_next_sync(db):
    r = _rule(db)
    FakeAPI.reset(copy_error="网页端 /userres/v1/file/copy_file 失败 code=400 msg=无权限复制")
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        out1 = _sync(db, r)
    assert out1["failed"] == 1 and out1["file_errors"], out1
    row = db.scalar(select(MirrorFile))
    assert row.status == "failed" and "无权限复制" in row.error, row.error
    assert "首个文件错误" in r.last_message, r.last_message

    FakeAPI.reset()  # 第二轮接口恢复正常
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        out2 = _sync(db, r)
    assert out2["retries"] == 1 and out2["done"] == 1, out2
    assert len(FakeAPI.copies) == 1  # reset 后本轮只复制一次
    assert db.scalar(select(MirrorFile)).status == "synced"


# ---------- 2. status=3 是明确失败，不是「待确认」 ----------
def test_task_status_3_marks_file_failed(db):
    r = _rule(db)
    FakeAPI.reset(task_status=3)
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        out = _sync(db, r)
    assert out["failed"] == 1 and out["unknown"] == 0, out
    assert db.scalar(select(MirrorFile)).status == "failed"
    t = db.scalar(select(MirrorTask))
    assert t.status == "failed" and "status=3" in t.error, t.error
    assert "任务失败" in out["file_errors"][0] or "status=3" in out["file_errors"][0]


# ---------- 3. status=2 但 detail.code≠0 也是失败（如 157 空间不足） ----------
def test_task_detail_code_nonzero_marks_failed(db):
    r = _rule(db)
    FakeAPI.reset(task_status=2, task_detail={"code": 157, "msg": "云盘空间不足"})
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        out = _sync(db, r)
    assert out["failed"] == 1, out
    assert "云盘空间不足" in db.scalar(select(MirrorFile)).error
    assert db.scalar(select(MirrorTask)).status == "failed"


# ---------- 4. 任务其实成功但轮询超时 → 核对 B 库，不重复复制 ----------
def test_task_already_in_b_is_confirmed_without_duplicate_copy(db):
    r = _rule(db)
    FakeAPI.reset(task_status=1)  # 一直 pending，轮询超时标 unknown
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        out1 = _sync(db, r)
    assert out1["unknown"] == 1 and len(FakeAPI.copies) == 1, out1
    assert db.scalar(select(MirrorFile)).status == "copying"

    # 第二轮：B 库作品目录里其实已经有这个文件（copy_file 异步完成了）
    b_item = {"fileId": "BF1", "resType": 1, "fileName": FILE_NAME,
              "fileSize": 100, "utime": 1}
    FakeAPI.reset(task_status=1, b_files={"B_2": [b_item]})
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        out2 = _sync(db, r)
    assert len(FakeAPI.copies) == 0, "B 库已存在时绝不能重复复制"
    assert out2["retries"] == 0, out2
    row = db.scalar(select(MirrorFile))
    assert row.status == "synced" and row.target_file_id == "BF1", row.target_file_id


# ---------- 5. 失败诊断必须持久化并回显状态接口 ----------
def test_status_exposes_file_errors_after_page_reload(db):
    r = _rule(db)
    FakeAPI.reset(copy_error="code=403 msg=目标目录无写入权限")
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        _sync(db, r)
    st = mirror_status(db=db)
    assert st["counts"]["files_failed"] == 1, st["counts"]
    assert st["counts"]["files_retryable"] == 1, st["counts"]
    assert st["file_errors"] and "目标目录无写入权限" in st["file_errors"][0]["error"]
    assert st["file_errors"][0]["file"] == FILE_NAME
    assert "首个文件错误" in st["rule"]["last_message"]


# ---------- 6. 任务状态语义与前端 JS p5 一致 ----------
def test_task_outcome_matches_frontend_js():
    assert mirror._task_outcome(2, {}) == ("done", "")
    assert mirror._task_outcome("2", {"code": 0}) == ("done", "")
    assert mirror._task_outcome(2, {"code": 157, "msg": "云盘空间不足"}) == (
        "failed", "云盘空间不足")
    assert mirror._task_outcome(3, {})[0] == "failed"
    assert mirror._task_outcome(3, {"code": 157, "msg": "空间不足"}) == (
        "failed", "空间不足")
    assert mirror._task_outcome(1, {})[0] == "pending"
    assert mirror._task_outcome(None, {})[0] == "pending"


# ---------- 7. 列目录分页按实际页大小累计判定，短页判不完整 ----------
def test_list_dir_uses_har_page_size_and_accumulated_count():
    class Paged:
        def __init__(self):
            self.calls = []

        async def list_files(self, parent_id, page=0, page_size=100):
            self.calls.append(page_size)
            sizes = {0: 50, 1: 50, 2: 20}
            n = sizes.get(page, 0)
            return {"list": [{"fileId": f"F{page}_{i}", "resType": 1,
                              "fileName": f"a{page}_{i}.mkv"} for i in range(n)],
                    "total": 120}

    api = Paged()
    items, ok = asyncio.run(mirror._list_dir(api, "X"))
    assert ok is True and len(items) == 120, (ok, len(items))
    assert set(api.calls) == {mirror.LIST_PAGE_SIZE} and mirror.LIST_PAGE_SIZE == 50

    class Short:
        async def list_files(self, parent_id, page=0, page_size=100):
            return {"list": [{"fileId": "F1", "resType": 1,
                              "fileName": "a.mkv"}], "total": 120}

    items, ok = asyncio.run(mirror._list_dir(Short(), "X"))
    assert ok is False, "接口提前返回短页时不得当作完整批次"


# ---------- 8. list_files 请求体与 w.har 抓包一致 ----------
def test_list_files_body_matches_har(monkeypatch):
    captured = {}

    class FakeResp:
        status_code = 200
        text = '{"msg":"success","data":{"list":[],"total":0}}'

        def json(self):
            return json.loads(self.text)

    class FakeClient:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None, headers=None):
            captured["url"], captured["body"] = url, json
            return FakeResp()

    monkeypatch.setattr(guangya.httpx, "AsyncClient", FakeClient)
    api = object.__new__(guangya.GuangYaWebFiles)
    api.base, api.headers = "https://api.guangyapan.com", {}
    asyncio.run(api.list_files("P1", 0, 50))
    assert captured["url"].endswith("/userres/v1/file/get_file_list")
    assert captured["body"] == {"page": 0, "pageSize": 50, "parentId": "P1",
                                "orderBy": 3, "sortType": 1}, captured["body"]
    assert "needSubFolderStat" not in captured["body"]


# ---------- 9. get_task_status 必须把 detail 交给调用方 ----------
def test_get_task_status_returns_detail(monkeypatch):
    class FakeResp:
        status_code = 200
        text = '{"msg":"success","data":{"status":2,"detail":{"code":157,"msg":"空间不足"}}}'

        def json(self):
            return json.loads(self.text)

    class FakeClient:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None, headers=None):
            return FakeResp()

    monkeypatch.setattr(guangya.httpx, "AsyncClient", FakeClient)
    api = object.__new__(guangya.GuangYaWebFiles)
    api.base, api.headers = "https://api.guangyapan.com", {}
    out = asyncio.run(api.get_task_status("T1"))
    assert out["detail"] == {"code": 157, "msg": "空间不足"}, out


# ---------- 10. copy_file 未被 HAR 取证的口径必须如实标注 ----------
def test_copy_file_is_marked_as_js_contract_only():
    assert any("copy_file" in x for x in mirror.UNVERIFIED_ENDPOINTS)
    assert any("仅前端 JS 契约" in x for x in mirror.CONFIRMED_WRITE_ENDPOINTS)
