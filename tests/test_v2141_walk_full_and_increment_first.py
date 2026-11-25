"""v2.1.41 验收：A 树全量遍历（不再静默截断）+ 每轮增量优先的执行顺序。

真机问题（2026-11-25，只读实测）：
  `_walk_source` 的 `max_dirs` 默认写死 400，而且 `complete` 只反映「每次列目录请求
  成功没有」，**与树有没有走完无关**。撞到 400 提前退出时只要请求都成功，
  `complete` 仍是 True，`if not complete: raise` 不触发 → 静默截断。
  线上 A 库真实目录数 ≥1436（已列 741 个时 frontier 还剩 695 个未展开），
  每轮只看到约 28%，所以 62T 的库只镜像出 11T，用户看到的就是「只处理了一部分」。

用户口径（本轮定）：① 按顺序把 A 库全量作品镜像过去；② 每轮开始时先查已复制作品
有没有增量/变化，有则优先处理，没有才继续复制存量；③ 全新作品按目录既定顺序排队。

覆盖：
  - 默认不限目录数、走完整棵树
  - 撞上限时 `walk_truncated` 必须为 True（且不再伪报「完整」）
  - 并发遍历每个目录只列一次
  - 列目录瞬时失败会退避重试，重试成功仍算完整
  - 截断时禁止做「A 库已无该作品」的清理判定（否则会误删 B 库文件）
  - `detect_increments`：新增集 / 内容变化能识别，仅 utime 抖动不算，
    没镜像过的新作品不算增量，已归档行按新增处理
  - 执行顺序：增量 → 补丁/待重试 → 存量轮转，且同一部作品一轮只处理一次
"""
import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

MIRROR_SRC = (Path(__file__).resolve().parents[1] / "backend/app/mirror.py").read_text(
    encoding="utf-8")


# ===========================================================================
# A 树全量遍历
# ===========================================================================

class FakeTree:
    """最小可用的光鸭列目录桩：{dirId: [条目]}，支持注入前 N 次失败。"""

    def __init__(self, tree: dict, fail_times: int = 0):
        self.tree = tree
        self.fail_times = fail_times
        self.calls: list[str] = []

    async def list_files(self, parent_id, page=0, page_size=100):
        self.calls.append(parent_id)
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("simulated 127")
        if page > 0:
            return {"list": [], "total": 0}
        items = list(self.tree.get(parent_id, []))
        return {"list": items, "total": len(items)}


def _dir(fid, name):
    return {"fileId": fid, "resType": 2, "fileName": name}


def _file(fid, name, size=100, utime=1):
    return {"fileId": fid, "resType": 1, "fileName": name,
            "fileSize": size, "utime": utime}


def _wide_tree(n=20):
    """根 + n 个分类目录，每个目录一个文件。"""
    tree = {"R": [_dir(f"D{i}", f"分类{i}") for i in range(n)]}
    for i in range(n):
        tree[f"D{i}"] = [_file(f"F{i}", f"S{i}.mkv")]
    return tree


def _walk(api, **kw):
    from app import mirror
    stats: dict = {}
    nodes, complete = asyncio.run(mirror._walk_source(api, "R", stats=stats, **kw))
    return nodes, complete, stats


def test_walk_default_lists_the_whole_tree():
    """默认不再有 400 目录上限：根 + 500 个目录要走完。"""
    tree = {"R": [_dir(f"D{i}", f"分类{i}") for i in range(500)]}
    for i in range(500):
        tree[f"D{i}"] = [_file(f"F{i}", f"S{i}.mkv")]
    nodes, complete, stats = _walk(FakeTree(tree))
    assert complete is True
    assert stats["walk_dirs"] == 501
    assert stats["walk_truncated"] is False
    assert stats["walk_a_files"] == 500


def test_walk_reports_truncation_instead_of_faking_complete():
    """撞上限：请求都成功（complete=True），但必须明确置 walk_truncated。"""
    nodes, complete, stats = _walk(FakeTree(_wide_tree(20)), max_dirs=5)
    assert complete is True, "请求都成功了，旧语义下这里就是 True —— 正是静默截断的来源"
    assert stats["walk_truncated"] is True, "截断必须可见，不能再伪报完整"
    assert stats["walk_dirs"] == 5


def test_walk_lists_each_dir_exactly_once():
    api = FakeTree(_wide_tree(12))
    _nodes_, _ok, stats = _walk(api)
    assert len(api.calls) == len(set(api.calls)) == stats["walk_list_calls"] == 13


def test_walk_retries_transient_failures():
    """瞬时失败（限流 127）退避重试后仍应走完整棵树。"""
    api = FakeTree(_wide_tree(6), fail_times=1)
    nodes, complete, stats = _walk(api, retries=2)
    assert complete is True
    assert stats["walk_dirs"] == 7
    assert stats["walk_truncated"] is False


def test_walk_gives_up_after_retries():
    """重试到底仍失败 → 抛错（沿用「整轮中止」的老语义，不拿残缺树做变更）。"""
    api = FakeTree(_wide_tree(3), fail_times=99)
    from app import mirror
    with pytest.raises(RuntimeError):
        asyncio.run(mirror._walk_source(api, "R", stats={}, retries=2))


def test_walk_max_dirs_default_is_zero_unlimited():
    """源码契约：默认值不能又变回 400（这是全量镜像的命门）。"""
    assert "max_dirs: int = 0" in MIRROR_SRC
    assert "max_dirs: int = 400" not in MIRROR_SRC
    assert "walk_truncated" in MIRROR_SRC


# ===========================================================================
# 增量检测（纯内存）
# ===========================================================================

@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v2141_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'b.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng, d
    shutil.rmtree(d, ignore_errors=True)


def _rule(db):
    from app.models import MirrorRule
    r = MirrorRule(name="m41", source_root_id="R", target_root_id="B",
                   interval_minutes=60, enabled=True, file_idle_seconds=0.5)
    db.add(r)
    db.commit()
    db.refresh(r)
    return r


def _media(db, rule, name, files):
    """建作品 + 映射行。files = [(fileId, size, status)]。"""
    from app.models import MirrorMedia, MirrorFile
    m = MirrorMedia(rule_id=rule.id, work_key="work:" + name, title_standard=name,
                    source_dir_id="A-" + name, target_dir_id="B-" + name, status="synced")
    db.add(m)
    db.flush()
    for fid, size, status in files:
        db.add(MirrorFile(media_id=m.id, rule_id=rule.id, identity=f"name:{fid}",
                          source_file_id=fid, target_file_id=f"B-{fid}",
                          file_name=f"{fid}.mkv", fingerprint=f"{size}:1",
                          size=size, status=status))
    db.commit()
    db.refresh(m)
    return m


def _work(name, files):
    node = {"id": "A-" + name, "name": name, "parent": "R", "path": name,
            "files": [], "children": []}
    return node, files


def _nodes_for(work, files):
    return {work["id"]: {"id": work["id"], "name": work["name"], "parent": "R",
                         "path": work["name"], "files": files, "children": []}}


def test_detect_increments_new_episode_in_mirrored_work(env):
    from app import mirror
    S, eng, d = env
    db = S()
    rule = _rule(db)
    _media(db, rule, "追更剧", [("F1", 100, "synced")])
    w, files = _work("追更剧", [_file("F1", "E01.mkv", 100), _file("F2", "E02.mkv", 100)])
    hits, out = mirror.detect_increments(db, rule, [(w, [])], _nodes_for(w, files))
    assert hits == ["work:追更剧"]
    assert out == {"works": 1, "new_files": 1, "changed_files": 0}


def test_detect_increments_size_change(env):
    from app import mirror
    S, eng, d = env
    db = S()
    rule = _rule(db)
    _media(db, rule, "变化剧", [("F1", 100, "synced")])
    w, files = _work("变化剧", [_file("F1", "E01.mkv", 777)])
    hits, out = mirror.detect_increments(db, rule, [(w, [])], _nodes_for(w, files))
    assert hits == ["work:变化剧"]
    assert out["changed_files"] == 1 and out["new_files"] == 0


def test_detect_increments_ignores_utime_only_change(env):
    """只比 size 段：网盘 utime 抖动不能造成假增量（v2.1.31 红线）。"""
    from app import mirror
    S, eng, d = env
    db = S()
    rule = _rule(db)
    _media(db, rule, "稳定剧", [("F1", 100, "synced")])
    w, files = _work("稳定剧", [_file("F1", "E01.mkv", 100, utime=999)])
    hits, out = mirror.detect_increments(db, rule, [(w, [])], _nodes_for(w, files))
    assert hits == []
    assert out["works"] == 0


def test_detect_increments_skips_never_mirrored_works(env):
    """完全新进来的作品不算增量 —— 它要走存量轮转、按目录既定顺序排队。"""
    from app import mirror
    S, eng, d = env
    db = S()
    rule = _rule(db)
    w, files = _work("新作品", [_file("N1", "E01.mkv")])
    hits, out = mirror.detect_increments(db, rule, [(w, [])], _nodes_for(w, files))
    assert hits == []
    assert out["works"] == 0


def test_detect_increments_treats_archived_row_as_new(env):
    """已手动清理的行：源文件回来了要算新增（否则复活只能干等轮转）。"""
    from app import mirror
    S, eng, d = env
    db = S()
    rule = _rule(db)
    _media(db, rule, "回归剧", [("F1", 100, mirror.ARCHIVED_FILE_STATUS)])
    w, files = _work("回归剧", [_file("F1", "E01.mkv", 100)])
    hits, out = mirror.detect_increments(db, rule, [(w, [])], _nodes_for(w, files))
    assert hits == ["work:回归剧"]
    assert out["new_files"] == 1


def test_detect_increments_ignores_non_video_files(env):
    from app import mirror
    S, eng, d = env
    db = S()
    rule = _rule(db)
    _media(db, rule, "剧", [("F1", 100, "synced")])
    w, files = _work("剧", [_file("F1", "E01.mkv", 100),
                            _file("F2", "poster.jpg", 10)])
    hits, _out = mirror.detect_increments(db, rule, [(w, [])], _nodes_for(w, files))
    assert hits == []


# ===========================================================================
# 编排顺序与护栏（源码契约 + 集成）
# ===========================================================================

def test_sync_uses_runtime_walk_limits():
    assert 'runtime_config.get(db, "mirror_walk_max_dirs")' in MIRROR_SRC
    assert 'runtime_config.get(db, "mirror_walk_concurrency")' in MIRROR_SRC
    from app.runtime_config import DEFAULTS
    assert DEFAULTS["mirror_walk_max_dirs"][1] == "0", "默认必须不限，否则又会截断"
    assert int(DEFAULTS["mirror_walk_concurrency"][1]) > 1, "不靠降到 1 解决限流"


def test_truncated_tree_disables_absence_judgement():
    """A 树被截断时绝不能判「A 库已无该作品」——那会清清单、删 B 库文件。"""
    assert "tree_ok = complete and not bool(stats.get(\"walk_truncated\"))" in MIRROR_SRC
    assert "complete=tree_ok)" in MIRROR_SRC, "预扫描（C 类清理）必须用 tree_ok 把关"
    assert "if tree_ok and retry_rows:" in MIRROR_SRC, "「源文件已删」的收敛也要把关"


def test_run_queue_order_is_increment_then_fix_then_stock():
    """用户口径：① 增量优先 ② 补丁/待重试 ③ 存量按目录顺序。"""
    assert "_pair in incr_works + priority_works + batch:" in MIRROR_SRC
    assert "detect_increments(db, rule, works, nodes)" in MIRROR_SRC
    # 去重：同一部作品一轮只处理一次
    assert "_seen_wk" in MIRROR_SRC


def test_increment_batch_capped_per_round():
    """增量再多也最多先做 max_works 部，且不占存量轮转的部数预算。"""
    assert "if max_works > 0 and len(incr_works) > max_works:" in MIRROR_SRC
    assert 'stats["increment_works"] = len(incr_works)' in MIRROR_SRC
    assert "增量优先" in MIRROR_SRC


def test_last_message_warns_on_truncation():
    assert "本轮只扫了一部分" in MIRROR_SRC
    assert "不要把没扫到当成已删除" in MIRROR_SRC


# ---------------------------------------------------------------------------
# 集成：截断的一轮不得清理任何映射
# ---------------------------------------------------------------------------

class FakeAPI:
    """A 根下有一部作品、B 库也有对应目录；用于验证截断时不误删。"""

    def __init__(self, db):
        self.db = db
        self.a_files = []

    async def list_files(self, parent_id, page=0, page_size=100):
        if page > 0:
            return {"list": [], "total": 0}
        if parent_id == "A_ROOT":
            return {"list": [{"fileId": "A_W1", "resType": 2,
                              "fileName": "缅北电诈 (2026)"}], "total": 1}
        if parent_id == "A_W1":
            return {"list": [dict(f) for f in self.a_files], "total": len(self.a_files)}
        if parent_id == "B_ROOT":
            return {"list": [{"fileId": "B_W1", "resType": 2,
                              "fileName": "缅北电诈 ·(2026)"}], "total": 1}
        return {"list": [], "total": 0}

    async def copy_file(self, file_ids, parent_id):
        return "T1"

    async def get_task_status(self, task_id):
        return {"status": 2, "raw": {"status": 2}}

    async def delete_files(self, ids):
        raise AssertionError("截断的一轮不允许删 B 库文件")


def test_truncated_round_does_not_clear_mappings(env):
    from app import mirror, runtime_config
    from app.models import MirrorFile, MirrorMedia, MirrorRule
    from app.db import SessionLocal, migrate

    migrate()
    db = S = SessionLocal()
    try:
        db.query(MirrorFile).delete()
        db.query(MirrorMedia).delete()
        db.query(MirrorRule).delete()
        db.commit()
        r = MirrorRule(name="t41", source_root_id="A_ROOT", target_root_id="B_ROOT",
                       interval_minutes=60, enabled=True, auto_create_dirs=True,
                       star_naming=True, file_idle_seconds=0.01)
        db.add(r)
        db.commit()
        m = MirrorMedia(rule_id=r.id, work_key="work:缅北电诈", title_standard="缅北电诈",
                        source_dir_id="A_W1", target_dir_id="B_W1", status="synced")
        db.add(m)
        db.flush()
        db.add(MirrorFile(media_id=m.id, rule_id=r.id, identity="name:F1",
                          source_file_id="F1", target_file_id="B-F1",
                          file_name="E01.mkv", fingerprint="100:1", size=100,
                          status="synced"))
        db.commit()

        # 只允许列 1 个目录 → 只看到 A 根，作品目录全被截断
        runtime_config.set_many(db, {"mirror_walk_max_dirs": "1"})
        api = FakeAPI(db)
        with patch("app.mirror.GuangYaWebFiles", lambda _db: api):
            stats = asyncio.run(mirror.sync_rule(db, db.scalar(select(MirrorRule)),
                                                 max_polls=1, poll_interval=0.0))
        assert stats["walk_truncated"] is True
        assert stats["walk_dirs"] == 1
        # 关键：一行都不许被清 —— 「没扫到」不等于「已删除」
        db.expire_all()
        row = db.scalar(select(MirrorFile))
        assert row is not None and row.status == "synced"
        assert row.target_file_id == "B-F1"
        assert "只扫了一部分" in (db.scalar(select(MirrorRule)).last_message or "")
    finally:
        db.rollback()
        db.close()
