"""v2.1.67 验收：空目录壳清理内置到镜像轮（C 类 + 存量幽灵），默认开真删。

真机（2026-11-28）
  v2.1.66 做了独立脚本 purge_empty_archived_dirs.py，但 Dockerfile 从不 COPY
  scripts/ 目录 → 脚本进不了容器，用户重建容器后无法执行。用户改为要求
  「项目本身有这种能力」→ 逻辑内置到 mirror.py，随镜像轮自动跑。

内置方案（用户拍板）
  · 触发环节：镜像同步轮 C 类（删文件后顺手删空目录） + 存量幽灵目录扫描
    （C 类只遍历有 mirror_media 映射的作品，诉讼女王这类无映射存量扫不到）。
  · 默认开真删：runtime_config.mirror_purge_empty_dirs 默认 true。

安全不变量（锁死）
  ① 只删递归确认 0 文件的目录；列举失败/超深/未知 resType 一律跳过；
  ② 后序删（子先父后），每删前二次确认仍为空；
  ③ 开关 false 只统计不删（dry-run）；
  ④ 只动网盘目录，不改库内任何行；
  ⑤ C 类删文件后先轮询任务落地再判空（否则撞上未删完的文件误判非空）。
"""
import asyncio
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

from app import mirror, runtime_config  # noqa: E402

MIRROR_SRC = (BACKEND / "app" / "mirror.py").read_text(encoding="utf-8")


class FakeDB:
    pass


class FakeWF:
    def __init__(self, tree):
        self.tree = tree
        self.deleted = []

    async def list_files(self, parent_id, page=0, page_size=100):
        if parent_id not in self.tree:
            from app.guangya import WebFileError
            raise WebFileError("目录不可列举")
        return {"list": self.tree[parent_id]}

    async def delete_files(self, ids):
        self.deleted.extend(ids)
        for fid in ids:
            for parent in self.tree:
                self.tree[parent] = [it for it in self.tree[parent]
                                     if str(it.get("fileId")) != str(fid)]
        return "task-ok"

    async def get_task_status(self, tid):
        return {"status": 2, "detail": {}}


def d(fid):
    return {"fileId": fid, "resType": "2"}


def f(fid):
    return {"fileId": fid, "resType": "1"}


@pytest.fixture
def purge_on(monkeypatch):
    orig = runtime_config.get
    runtime_config.get = lambda db, key, *a, **kw: (
        "true" if key == "mirror_purge_empty_dirs" else
        ("50" if key == "mirror_purge_ghost_dirs_max" else orig(db, key, *a, **kw))
    )
    yield
    runtime_config.get = orig


@pytest.fixture
def purge_off(monkeypatch):
    orig = runtime_config.get
    runtime_config.get = lambda db, key, *a, **kw: (
        "false" if key == "mirror_purge_empty_dirs" else orig(db, key, *a, **kw)
    )
    yield
    runtime_config.get = orig


def test_purge_empty_shell(purge_on):
    wf = FakeWF({"root": [d("season")], "season": []})
    r = asyncio.run(mirror._purge_empty_b_dir_tree(FakeDB(), wf, "root"))
    assert r["empty"] == 1 and r["dirs_deleted"] == 2
    assert wf.deleted == ["season", "root"]  # 后序：子先父后


def test_skip_when_has_files(purge_on):
    """完美世界式：有文件绝不删。"""
    wf = FakeWF({"root": [d("season")], "season": [f("e1"), f("e2")]})
    r = asyncio.run(mirror._purge_empty_b_dir_tree(FakeDB(), wf, "root"))
    assert r["skipped"] == 1 and wf.deleted == []


def test_skip_on_list_failure(purge_on):
    """列举失败 → 跳过，绝不把异常当空。"""
    wf = FakeWF({"root": [d("gone")]})
    r = asyncio.run(mirror._purge_empty_b_dir_tree(FakeDB(), wf, "root"))
    assert r["skipped"] == 1 and wf.deleted == []


def test_skip_on_unknown_restype(purge_on):
    wf = FakeWF({"root": [{"fileId": "x", "resType": "9"}]})
    r = asyncio.run(mirror._purge_empty_b_dir_tree(FakeDB(), wf, "root"))
    assert r["skipped"] == 1 and wf.deleted == []


def test_dry_run_when_off(purge_off):
    wf = FakeWF({"root": []})
    r = asyncio.run(mirror._purge_empty_b_dir_tree(FakeDB(), wf, "root"))
    assert r["dry_run"] is True and r["empty"] == 1 and wf.deleted == []


def test_empty_dir_id_skipped(purge_on):
    wf = FakeWF({"root": []})
    r = asyncio.run(mirror._purge_empty_b_dir_tree(FakeDB(), wf, ""))
    assert r["candidates"] == 0 and wf.deleted == []


def test_second_confirm_blocks(purge_on):
    """删前二次确认发现新内容 → 不删。"""
    class ChangingWF(FakeWF):
        async def list_files(self, parent_id, page=0, page_size=100):
            if not hasattr(self, "_probe_done"):
                self._probe_done = True
                return {"list": []}
            return {"list": [f("new")]}
    wf = ChangingWF({"root": []})
    r = asyncio.run(mirror._purge_empty_b_dir_tree(FakeDB(), wf, "root"))
    assert r["empty"] == 1 and r["failed"] >= 1 and wf.deleted == []


def test_source_anchors():
    """源码反证：C类与存量两处调用、默认开、轮询等待、只动网盘目录。"""
    assert "await _purge_empty_b_dir_tree(db, api, _target_dir, label=wk)" in MIRROR_SRC
    assert 'label="ghost"' in MIRROR_SRC
    assert '"mirror_purge_empty_dirs") or "true"' in MIRROR_SRC
    assert "mirror_purge_ghost_dirs_max" in MIRROR_SRC
    # C 类删文件后先轮询任务落地
    assert "get_task_status(_del_task)" in MIRROR_SRC
    # 不碰库内归档/映射（函数体内没有对 Resource/MirrorMedia 的写）
    start = MIRROR_SRC.index("async def _purge_empty_b_dir_tree")
    end = MIRROR_SRC.index("async def reconcile_mappings_prepass", start)
    body = MIRROR_SRC[start:end]
    assert "db.delete(" not in body
    assert ".update(" not in body
