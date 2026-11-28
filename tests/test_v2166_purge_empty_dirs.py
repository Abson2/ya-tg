"""v2.1.66 验收：B 库空目录壳清理脚本（已归档 + 实时确认为空才删）。

真机现场（2026-11-28 线上只读探针）：
  诉讼女王等资源已归档（clean_state=cleaned / clean_reason='B库内容已移除，归档资源库记录'）、
  B 库文件已被 C 类删净，但作品目录 + 季目录空壳留在网盘。6 个归档目录实测：
    MobLand / 缅北-纪录片 / 缅北-剧集 / 诉讼女王 = 递归 0 文件（真空壳，可删）；
    完美世界 = 递归 200 文件、不为人知的故事 = 8 文件（已归档但 B 库仍有内容，必须跳过）。
  → 「已归档」≠「可删」，必须每次实时递归列举确认。

取证（har_summary.md，w.har 2026-11-18 entry #37/39）：
  create_dir 出的 resType=2 目录被 delete_file {"fileIds":[目录id]} 成功删除、taskId status=2。
  → 网盘 delete_file 支持删目录；项目缺的是「识别空壳 + 调它」的上层动作。

锁死的安全不变量：
  ① 候选只来自已归档 + clean_reason 精确匹配，按 share_dir_id 去重；
  ② 递归 0 文件才判空；列举失败 / 超深 / 未知 resType 一律不判空、不删；
  ③ 删除后序（子目录先于父目录），删前二次确认仍为空；
  ④ 默认 dry-run 零写；--apply 才调 delete_file；失败逐个记录不中断。
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
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

import purge_empty_archived_dirs as purge  # noqa: E402

PURGE_SRC = (SCRIPTS / "purge_empty_archived_dirs.py").read_text(encoding="utf-8")


class FakeWF:
    """内存网盘：list_files 按 parent_id 返回条目，缺失 parent 抛异常（模拟列举失败）。"""

    def __init__(self, tree: dict):
        self.tree = tree
        self.calls: list[str] = []
        self.deleted: list[str] = []

    async def list_files(self, parent_id, page=0, page_size=100):
        self.calls.append(parent_id)
        if parent_id not in self.tree:
            from app.guangya import WebFileError
            raise WebFileError("目录不可列举")
        return {"list": self.tree[parent_id]}

    async def delete_files(self, ids):
        self.deleted.extend(ids)
        for fid in ids:
            # 模拟网盘：从所有父目录的条目里移除该 id
            for parent, items in self.tree.items():
                self.tree[parent] = [it for it in items if str(it.get("fileId")) != str(fid)]
        return "task-ok"

    async def get_task_status(self, task_id):
        return {"status": 2, "detail": {}}


def d(fid):
    return {"fileId": fid, "resType": "2"}


def f(fid):
    return {"fileId": fid, "resType": "1"}


# ---------------- 候选口径 ----------------

def test_candidate_source_is_archived_only():
    """源码锁：候选 SQL 必须带 clean_state='cleaned' 与精确 clean_reason，绝不扫正常目录。"""
    assert "clean_state = 'cleaned'" in PURGE_SRC
    assert "ARCHIVED_REASON =" in PURGE_SRC
    assert "ARCHIVED_REASON = \"B库内容已移除，归档资源库记录\"" in PURGE_SRC or \
           'ARCHIVED_REASON = "B库内容已移除，归档资源库记录"' in PURGE_SRC or \
           "ARCHIVED_REASON = \"B库内容已移除，归档资源库记录\"" in PURGE_SRC or \
           "'B库内容已移除，归档资源库记录'" in PURGE_SRC
    assert "GROUP BY share_dir_id" in PURGE_SRC  # distinct by dir


def test_load_candidates_filters_and_dedups():
    """候选按 dir_id 去重、按 work 关键词过滤（标题或目录名）。"""
    from types import SimpleNamespace
    from app.db import SessionLocal, migrate
    from app.models import Resource

    migrate()
    db = SessionLocal()
    try:
        db.query(Resource).delete()
        for i in range(2):
            db.add(Resource(file_id=f"ff{i}", name="x", share_dir_id="DIR-A",
                            share_dir_name="诉·讼·女·王 (2025) {tmdb-258742}",
                            media_title="诉讼女王", clean_state="cleaned",
                            clean_reason=purge.ARCHIVED_REASON))
        db.add(Resource(file_id="ff2", name="x", share_dir_id="DIR-B",
                        share_dir_name="完·美·世·界 (2021) {tmdb-124003}",
                        media_title="完美世界", clean_state="cleaned",
                        clean_reason=purge.ARCHIVED_REASON))
        # 正常资源（未归档）绝不能进候选
        db.add(Resource(file_id="ff3", name="x", share_dir_id="DIR-C",
                        share_dir_name="择·日·飞·升", media_title="择日飞升",
                        clean_state="", clean_reason=""))
        db.commit()
        got = purge.load_candidates(db)
        ids = [c["dir_id"] for c in got]
        assert ids == ["DIR-A", "DIR-B"], ids
        a = next(c for c in got if c["dir_id"] == "DIR-A")
        assert a["archived_rows"] == 2  # 两条归档聚成一组
        # work 过滤
        only = purge.load_candidates(db, work_filter="完美")
        assert [c["dir_id"] for c in only] == ["DIR-B"]
    finally:
        db.query(Resource).delete()
        db.commit()
        db.close()


# ---------------- 判空逻辑 ----------------

def test_probe_empty_shell():
    return asyncio.run(_async_probe_empty_shell())

async def _async_probe_empty_shell():
    wf = FakeWF({"root": [d("s1")], "s1": []})
    r = await purge.probe_tree(wf, "root")
    assert r["empty"] is True and r["files"] == 0
    assert r["dirs"] == ["s1", "root"]  # 后序：子在前


def test_probe_has_files_skips():
    return asyncio.run(_async_probe_has_files_skips())

async def _async_probe_has_files_skips():
    """完美世界式：已归档但 B 库递归 200 文件 → 绝不判空。"""
    wf = FakeWF({"root": [d("s1")], "s1": [f("e1"), f("e2")]})
    r = await purge.probe_tree(wf, "root")
    assert r["empty"] is False and r["files"] == 2


def test_probe_list_failure_never_deletes():
    return asyncio.run(_async_probe_list_failure_never_deletes())

async def _async_probe_list_failure_never_deletes():
    """任一子目录列举失败 → 整部 skip（绝不把异常当空）。"""
    wf = FakeWF({"root": [d("gone")]})
    r = await purge.probe_tree(wf, "root")
    assert r["empty"] is False and "列举" in r["error"]


def test_probe_unknown_restype_never_deletes():
    return asyncio.run(_async_probe_unknown_restype_never_deletes())

async def _async_probe_unknown_restype_never_deletes():
    wf = FakeWF({"root": [{"fileId": "x", "resType": "9"}]})
    r = await purge.probe_tree(wf, "root")
    assert r["empty"] is False and r["unknown"] == 1


def test_probe_too_deep_never_deletes():
    return asyncio.run(_async_probe_too_deep_never_deletes())

async def _async_probe_too_deep_never_deletes():
    """超过 MAX_DEPTH 一律不判空（防止无限递归，也防深树误判）。"""
    tree = {}
    for i in range(purge.MAX_DEPTH + 2):
        tree[f"n{i}"] = [d(f"n{i+1}")]
    tree[f"n{purge.MAX_DEPTH+2}"] = []
    wf = FakeWF(tree)
    r = await purge.probe_tree(wf, "n0")
    assert r["empty"] is False and r["too_deep"] is True


def test_confirm_empty_before_delete():
    return asyncio.run(_async_confirm_empty_before_delete())

async def _async_confirm_empty_before_delete():
    """删前二次确认：有内容 / 列举失败都返回 False。"""
    assert await purge.confirm_dir_empty(FakeWF({"root": []}), "root") is True
    assert await purge.confirm_dir_empty(FakeWF({"root": [f("new")]}), "root") is False
    assert await purge.confirm_dir_empty(FakeWF({}), "root") is False


# ---------------- 任务状态 ----------------

def test_wait_task_success_and_failures():
    return asyncio.run(_async_wait_task_success_and_failures())

async def _async_wait_task_success_and_failures():
    class WF:
        def __init__(self, seq): self.seq = seq; self.i = 0
        async def get_task_status(self, tid):
            v = self.seq[min(self.i, len(self.seq) - 1)]
            self.i += 1
            return v
    assert await purge.wait_task(WF([{"status": 2, "detail": {}}]), "t") is True
    assert await purge.wait_task(WF([{"status": 2, "detail": {"code": 157}}]), "t") is False
    assert await purge.wait_task(WF([{"status": 3, "detail": {}}]), "t") is False


# ---------------- 全链路 dry-run / apply ----------------

def test_run_dry_run_never_deletes(monkeypatch, capsys):
    return asyncio.run(_async_run_dry_run_never_deletes(monkeypatch, capsys))

async def _async_run_dry_run_never_deletes(monkeypatch, capsys):
    """默认 dry-run 零写：即使识别出空壳也绝不调 delete_files。"""
    from types import SimpleNamespace

    monkeypatch.setattr(purge, "SessionLocal", lambda: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(purge, "load_candidates", lambda db, *, work_filter="": [
        {"dir_id": "root", "dir_name": "诉·讼·女·王", "path_hint": "剧集/欧美剧集/...",
         "media_title": "诉讼女王", "archived_rows": 2}])
    wf = FakeWF({"root": [d("s1")], "s1": []})
    monkeypatch.setattr(purge, "GuangYaWebFiles", lambda db: wf)
    rc = await purge.run(SimpleNamespace(work="", limit=0, apply=False, sleep=0))
    assert rc == 0
    assert wf.deleted == []
    out = capsys.readouterr().out
    assert "dry-run" in out and "可删空壳 1" in out


def test_run_apply_deletes_postorder(monkeypatch):
    return asyncio.run(_async_run_apply_deletes_postorder(monkeypatch))

async def _async_run_apply_deletes_postorder(monkeypatch):
    """--apply：后序删除（子先父后），每删前二次确认，删后轮询成功。"""
    from types import SimpleNamespace

    monkeypatch.setattr(purge, "SessionLocal", lambda: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(purge, "load_candidates", lambda db, *, work_filter="": [
        {"dir_id": "root", "dir_name": "诉·讼·女·王", "path_hint": "",
         "media_title": "诉讼女王", "archived_rows": 2}])
    # root→s1 空壳；删 s1 后 root 也空（FakeWF 不真的移除，但二次确认仍为空即可）
    wf = FakeWF({"root": [d("s1")], "s1": []})
    monkeypatch.setattr(purge, "GuangYaWebFiles", lambda db: wf)
    rc = await purge.run(SimpleNamespace(work="", limit=0, apply=True, sleep=0))
    assert rc == 0
    assert wf.deleted == ["s1", "root"]  # 后序


def test_run_apply_skips_nonempty(monkeypatch):
    return asyncio.run(_async_run_apply_skips_nonempty(monkeypatch))

async def _async_run_apply_skips_nonempty(monkeypatch):
    """完美世界式非空：即使--apply也不删。"""
    from types import SimpleNamespace

    monkeypatch.setattr(purge, "SessionLocal", lambda: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(purge, "load_candidates", lambda db, *, work_filter="": [
        {"dir_id": "root", "dir_name": "完·美·世·界", "path_hint": "",
         "media_title": "完美世界", "archived_rows": 284}])
    wf = FakeWF({"root": [d("s1")], "s1": [f("e1"), f("e2")]})
    monkeypatch.setattr(purge, "GuangYaWebFiles", lambda db: wf)
    rc = await purge.run(SimpleNamespace(work="", limit=0, apply=True, sleep=0))
    assert rc == 0
    assert wf.deleted == []


def test_run_apply_second_confirm_blocks(monkeypatch):
    return asyncio.run(_async_run_apply_second_confirm_blocks(monkeypatch))

async def _async_run_apply_second_confirm_blocks(monkeypatch):
    """删前二次确认发现新文件 → 跳过该目录，不调 delete_files。"""
    from types import SimpleNamespace

    monkeypatch.setattr(purge, "SessionLocal", lambda: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(purge, "load_candidates", lambda db, *, work_filter="": [
        {"dir_id": "root", "dir_name": "诉·讼·女·王", "path_hint": "",
         "media_title": "诉讼女王", "archived_rows": 2}])

    class ChangingWF(FakeWF):
        async def list_files(self, parent_id, page=0, page_size=100):
            # probe_tree 阶段（第 1 次调用）为空；confirm 阶段（第 2 次）返回新文件
            if len(self.calls) >= 1:
                return {"list": [f("just-uploaded")]}
            return await super().list_files(parent_id, page, page_size)

    wf = ChangingWF({"root": []})
    monkeypatch.setattr(purge, "GuangYaWebFiles", lambda db: wf)
    rc = await purge.run(SimpleNamespace(work="", limit=0, apply=True, sleep=0))
    assert wf.deleted == []
    assert rc == 1  # 有失败项


# ---------------- 源码锚点（反证） ----------------

def test_no_delete_of_normal_dirs_source_anchor():
    """反证：脚本没有 hardcode 任何目录 id、没有 DELETE FROM resources、没有改库内归档行。"""
    assert "DELETE FROM resources" not in PURGE_SRC.upper()
    assert "1973626330418212961" not in PURGE_SRC  # 不硬编码线上 id
    assert "--apply" in PURGE_SRC


# ============================================================================
# v2.1.66 第二部分：对账前复活「A 库仍有源文件」的归档行
# ============================================================================

MAIN_SRC = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")


@pytest.fixture
def revive_db():
    """建表 + 每个用例清空相关表，避免串扰。"""
    from app.db import SessionLocal, migrate
    from app.models import MirrorFile, MirrorMedia, MirrorRule, Resource
    migrate()
    db = SessionLocal()
    for m in (MirrorFile, MirrorMedia, MirrorRule, Resource):
        db.query(m).delete()
    db.commit()
    yield db
    for m in (MirrorFile, MirrorMedia, MirrorRule, Resource):
        db.query(m).delete()
    db.commit()
    db.close()


def _add_archived(db, fid, sname, *, title=""):
    from app.models import Resource
    r = Resource(file_id=fid, name=f"sname_{fid}", media_title=title,
                 source_name=sname, source_title=title, path_hint="剧集/x/y",
                 clean_state="cleaned", clean_reason="B库内容已移除，归档资源库记录")
    db.add(r)
    db.flush()
    return r


def _add_mirror_file(db, fname, *, status="synced", src_id="A_SRC"):
    from app.models import MirrorFile, MirrorMedia, MirrorRule
    rule = db.query(MirrorRule).first()
    if not rule:
        rule = MirrorRule(name="r", source_root_id="s", target_root_id="t", enabled=1)
        db.add(rule); db.flush()
    mm = db.query(MirrorMedia).first()
    if not mm:
        mm = MirrorMedia(rule_id=rule.id, work_key="work:probe", source_dir_id="A_DIR",
                         target_dir_id="B_DIR", status="synced")
        db.add(mm); db.flush()
    mf = MirrorFile(media_id=mm.id, rule_id=rule.id, file_name=fname,
                    source_file_id=src_id, target_file_id="B_FILE", status=status)
    db.add(mf); db.flush()
    return mf


def test_revive_exact_a_source_match(revive_db):
    """精确匹配：归档 source_name == mirror_files.file_name 且 A 源文件在 → 复活。"""
    from app.main import _revive_archived_with_a_source
    from app.models import Resource, include_archived

    r1 = _add_archived(revive_db, "B_OLD_1", "剧名.S01E01.mkv", title="剧名")
    r2 = _add_archived(revive_db, "B_OLD_2", "剧名.S01E02.mkv", title="剧名")
    _add_mirror_file(revive_db, "剧名.S01E01.mkv")  # 只有 E01 在 A 库
    revive_db.commit()

    fids = _revive_archived_with_a_source(revive_db)
    revive_db.commit()
    assert fids == ["B_OLD_1"]
    with include_archived():
        db_r1 = revive_db.get(Resource, r1.id)
        db_r2 = revive_db.get(Resource, r2.id)
    assert db_r1.clean_state == "" and db_r1.cleaned_at is None and db_r1.clean_reason == ""
    assert db_r2.clean_state == "cleaned"  # E02 不在 A 库，保持归档


def test_revive_skips_non_synced_and_no_a_source(revive_db):
    """mirror_files 非 synced/done 或无 source_file_id → 不复活（A 库已删/失败）。"""
    from app.main import _revive_archived_with_a_source
    from app.models import Resource, include_archived

    r_fail = _add_archived(revive_db, "B_FAIL", "fail.mkv")
    r_nosrc = _add_archived(revive_db, "B_NOSRC", "nosrc.mkv")
    _add_mirror_file(revive_db, "fail.mkv", status="failed")
    _add_mirror_file(revive_db, "nosrc.mkv", src_id="")
    revive_db.commit()

    assert _revive_archived_with_a_source(revive_db) == []
    with include_archived():
        assert revive_db.get(Resource, r_fail.id).clean_state == "cleaned"
        assert revive_db.get(Resource, r_nosrc.id).clean_state == "cleaned"


def test_revive_no_title_and_name_ambiguity(revive_db):
    """归档行无 media_title 也能按 source_name 复活（线上 1063 条无标题）。"""
    from app.main import _revive_archived_with_a_source
    r = _add_archived(revive_db, "B_NOTITLE", "无标题剧.S01E03.mkv", title="")
    _add_mirror_file(revive_db, "无标题剧.S01E03.mkv")
    revive_db.commit()
    assert _revive_archived_with_a_source(revive_db) == ["B_NOTITLE"]


def test_revive_source_anchor():
    """源码反证：复活在归档判定之前、清三字段、纳入 seen、精确文件名匹配。"""
    assert "_revive_archived_with_a_source(db)" in MAIN_SRC
    assert "seen = seen | set(revived_fids)" in MAIN_SRC
    assert 'MirrorFile.status.in_(("synced", "done"))' in MAIN_SRC
    assert 'Resource.source_name == ""' not in MAIN_SRC  # 不猜空名
    assert '"revived": len(revived_fids)' in MAIN_SRC
