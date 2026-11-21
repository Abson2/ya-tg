"""镜像1.1 验收：单轮开始 A树/B树/映射清单三方对比查漏补缺（a/b/c/d）。

对齐 MIRROR_1.1_DESIGN.md：
  - a 单文件变化：只统计不占剧部数；
  - b 整部在 B 缺失（A 还在）：重置映射+优先重处理键；
  - c A 库已删：默认只清清单+统计、不删 B 库（真删靠开关）；
  - d 无变化：不动作；
  - 护栏：complete=False 跳过 c/b；delete 开关默认关；性能上限。
"""
import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v148_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'b.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng, d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(autouse=True)
def _restore_b_candidates():
    """用例里直接给 mirror._b_file_candidates 打桩，结束后恢复原函数避免跨文件污染。"""
    from app import mirror
    original = mirror._b_file_candidates
    yield
    mirror._b_file_candidates = original


def _rule(db):
    from app.models import MirrorRule
    r = MirrorRule(name="m11", source_root_id="A-ROOT", target_root_id="B-ROOT",
                   interval_minutes=60, enabled=True, file_idle_seconds=0.5)
    db.add(r)
    db.commit()
    db.refresh(r)
    return r


def _nodes(files_by_dir: dict[str, list[dict]]) -> dict:
    """构造 A 树 nodes：{did: {id,name,parent,path,files,children}}。"""
    nodes = {}
    for did, files in files_by_dir.items():
        nodes[did] = {"id": did, "name": did, "parent": "A-ROOT", "path": did,
                      "files": files, "children": []}
    return nodes


def _a_file(fid, size=100, utime=1):
    return {"fileId": fid, "fileName": f"{fid}.mkv", "fileSize": size, "utime": utime}


def _media(db, rule, wk, *, src_ids, fps=None, target_dir="BDIR"):
    from app.models import MirrorMedia, MirrorFile
    m = MirrorMedia(rule_id=rule.id, work_key=wk, title_standard=wk,
                    source_dir_id="A-" + wk, target_dir_id=target_dir, status="synced")
    db.add(m)
    db.flush()
    fps = fps or {}
    for i, sid in enumerate(src_ids):
        db.add(MirrorFile(media_id=m.id, rule_id=rule.id, identity=f"name:{wk}:E{i}",
                          source_file_id=sid, target_file_id=f"B-{sid}",
                          file_name=f"{sid}.mkv", fingerprint=fps.get(sid, "100:1"),
                          size=100, status="synced"))
    db.commit()
    db.refresh(m)
    return m


def _works(*names):
    return [({"id": f"A-{n}", "name": n, "parent": "A-ROOT", "path": n,
              "files": [], "children": []}, []) for n in names]


def _api(b_candidates=None, verified=True):
    api = AsyncMock()
    api.delete_files = AsyncMock(return_value={"data": {"taskId": "t1"}})
    api._bc = (b_candidates or {}, verified)
    return api


# ---------- 默认开关与护栏 ----------

def test_defaults(env):
    from app.runtime_config import DEFAULTS
    assert DEFAULTS["mirror_prepass_enabled"][1] == "true"
    assert DEFAULTS["mirror_prepass_max_media"][1] == "50"
    assert DEFAULTS["mirror_mode"][1] == "full", "默认全镜像"


def test_build_a_facts():
    from app.mirror import _build_a_facts
    nodes = _nodes({"d1": [_a_file("F1", 100, 1), _a_file("F2", 200, 2)]})
    works = _works("剧甲", "剧乙")
    ids, fps, keys = _build_a_facts(nodes, works)
    assert ids == {"F1", "F2"}
    assert fps == {"F1": "100:1", "F2": "200:2"}
    assert keys == {"work:剧甲", "work:剧乙"}


# ---------- c 类：A 已删 ----------

def test_c_class_full_mirror_deletes_b_and_clears_by_default(env):
    """默认全镜像：A 已删 → 删 B 库对应文件 + 清映射清单。"""
    from app import mirror
    S, eng, d = env
    db = S()
    rule = _rule(db)
    m = _media(db, rule, "已删剧", src_ids=["GONE1", "GONE2"])
    nodes = _nodes({"other": [_a_file("F1")]})
    works = _works("现存剧")
    stats = {}
    api = _api()

    async def _bc(api_, media):
        return {}, True
    mirror._b_file_candidates = _bc  # c 类不查 B，但保险起见 mock

    got, _missing = asyncio.run(mirror.reconcile_mappings_prepass(
        db, rule, nodes, works, api, stats, complete=True))
    pp = stats["prepass"]
    assert pp["mirror_mode"] == "full"
    assert pp["c_deleted_a"] == 1
    assert pp["c_mapping_cleared"] >= 1, "全镜像清清单"
    assert pp["c_b_files_removed"] == 2, "全镜像删 B 库对应 2 个文件"
    api.delete_files.assert_awaited_once()
    assert got == []  # c 类不进重处理
    db.rollback()


def test_c_class_half_mirror_keeps_b_and_mapping(env):
    """半镜像：A 已删 → B 库文件与映射清单都原样保留，完全不动。"""
    from app import mirror, runtime_config
    S, eng, d = env
    db = S()
    rule = _rule(db)
    m = _media(db, rule, "已删剧", src_ids=["GONE1", "GONE2"])
    runtime_config.set_many(db, {"mirror_mode": "half"})
    nodes = _nodes({"other": [_a_file("F1")]})
    stats = {}
    api = _api()

    got, _missing = asyncio.run(mirror.reconcile_mappings_prepass(
        db, rule, nodes, _works("现存剧"), api, stats, complete=True))
    pp = stats["prepass"]
    assert pp["mirror_mode"] == "half"
    assert pp["c_deleted_a"] == 1
    assert pp["c_kept_in_half_mode"] == 1
    assert pp["c_mapping_cleared"] == 0
    assert pp["c_b_files_removed"] == 0
    api.delete_files.assert_not_called()
    # 映射清单原封不动
    rows = db.scalars(select(mirror.MirrorFile).where(mirror.MirrorFile.media_id == m.id)).all()
    assert all(r.target_file_id == f"B-{r.source_file_id}" and r.status == "synced"
               for r in rows)


def test_c_class_skipped_when_incomplete(env):
    """complete=False 时 c 类不判、不删、不清理。"""
    from app import mirror, runtime_config
    S, eng, d = env
    db = S()
    rule = _rule(db)
    _media(db, rule, "已删剧", src_ids=["GONE1"])
    runtime_config.set_many(db, {"mirror_mode": "full"})
    stats = {}
    api = _api()
    asyncio.run(mirror.reconcile_mappings_prepass(
        db, rule, _nodes({}), _works(), api, stats, complete=False))
    assert stats["prepass"]["c_deleted_a"] == 0
    assert stats["prepass"]["c_b_files_removed"] == 0
    api.delete_files.assert_not_called()


# ---------- b 类：整部在 B 缺失 ----------

def test_b_class_returns_reprocess_key_without_reset(env):
    from app import mirror
    S, eng, d = env
    db = S()
    rule = _rule(db)
    m = _media(db, rule, "失效剧", src_ids=["KEEP1"])
    nodes = _nodes({"d": [_a_file("KEEP1")]})  # A 还在
    works = _works("失效剧")

    async def _bc(api_, media):
        return {}, True  # B 库整部为空、verified
    mirror._b_file_candidates = _bc
    stats = {}
    api = _api()
    got, _missing = asyncio.run(mirror.reconcile_mappings_prepass(
        db, rule, nodes, works, api, stats, complete=True))
    assert stats["prepass"]["b_invalid_reprocess"] == 1
    assert "work:失效剧" in got
    assert "work:失效剧" not in _missing, "B类整部失效走重排键，不进缺集补丁批次"
    # 预扫描只登记重排键，不提前清 pending：重置/补复制/计数交给主循环 v0.8.7。
    # 提前清会让主循环 synced_rows 变空、missing_in_b 丢失（v087/v088 回归根因）。
    rows = db.scalars(select(mirror.MirrorFile).where(mirror.MirrorFile.media_id == m.id)).all()
    assert all(r.status == "synced" for r in rows)


# ---------- a / d 类 ----------

def test_a_class_single_file_change_not_counted(env):
    from app import mirror
    S, eng, d = env
    db = S()
    rule = _rule(db)
    _media(db, rule, "变化剧", src_ids=["CHG1"], fps={"CHG1": "100:1"})
    nodes = _nodes({"d": [_a_file("CHG1", size=999, utime=9)]})  # A 文件变了
    stats = {}

    async def _bc(api_, media):
        return {"chg1.mkv:999": "B-CHG1"}, True  # B 库有文件
    mirror._b_file_candidates = _bc
    api = _api()
    got, _missing = asyncio.run(mirror.reconcile_mappings_prepass(
        db, rule, nodes, _works("变化剧"), api, stats, complete=True))
    assert stats["prepass"]["a_single_file_fix"] == 1
    assert stats["prepass"]["b_invalid_reprocess"] == 0
    assert got == []  # 单文件补齐不占重处理键（不计剧部数）


def test_d_class_unchanged(env):
    from app import mirror
    S, eng, d = env
    db = S()
    rule = _rule(db)
    _media(db, rule, "稳定剧", src_ids=["SAME1"], fps={"SAME1": "100:1"})
    nodes = _nodes({"d": [_a_file("SAME1", size=100, utime=1)]})
    stats = {}

    async def _bc(api_, media):
        return {"same1.mkv:100": "B-SAME1"}, True
    mirror._b_file_candidates = _bc
    api = _api()
    got, _missing = asyncio.run(mirror.reconcile_mappings_prepass(
        db, rule, nodes, _works("稳定剧"), api, stats, complete=True))
    assert stats["prepass"]["d_unchanged"] == 1
    assert got == []


# ---------- 开关关闭 ----------

def test_prepass_disabled_short_circuits(env):
    from app import mirror, runtime_config
    S, eng, d = env
    db = S()
    rule = _rule(db)
    runtime_config.set_many(db, {"mirror_prepass_enabled": "false"})
    stats = {}
    api = _api()
    got, _missing = asyncio.run(mirror.reconcile_mappings_prepass(
        db, rule, _nodes({}), _works(), api, stats, complete=True))
    assert got == []
    assert stats["prepass"]["enabled"] is False


# ---------- A 类缺集：真机「我不是大师 S01E23」根因闭环 ----------

def test_a_class_missing_episode_goes_to_missing_keys(env):
    """B 库单集缺失、映射仍 synced、A 库仍在 → A 类缺集键，不误判 d_unchanged。"""
    from app import mirror
    S, eng, d = env
    db = S()
    rule = _rule(db)
    m = _media(db, rule, "缺集剧", src_ids=["E22", "E23"])
    nodes = _nodes({"d": [_a_file("E22"), _a_file("E23")]})
    stats = {}

    from app.utils import normalize_work_name

    async def _bc(api_, media):
        # key 口径与 _b_file_candidates 一致：normalize_work_name("E22.mkv")="e22mkv"
        return {f"{normalize_work_name('E22.mkv')}:100": "B-E22"}, True  # E23 真缺失
    mirror._b_file_candidates = _bc

    got, missing = asyncio.run(mirror.reconcile_mappings_prepass(
        db, rule, nodes, _works("缺集剧"), _api(), stats, complete=True))
    pp = stats["prepass"]
    assert pp["a_missing_episodes"] == 1
    assert pp["a_single_file_fix"] == 1
    assert pp["d_unchanged"] == 0
    assert "work:缺集剧" in missing
    assert got == [], "缺集走补丁批次，不占 B 类重排键"
    # 红线：预扫描只登记，绝不提前改 MirrorFile；补复制交主循环 v0.8.7
    rows = db.scalars(select(mirror.MirrorFile).where(mirror.MirrorFile.media_id == m.id)).all()
    assert all(r.status == "synced" for r in rows)


def test_a_class_name_mismatch_not_treated_as_missing(env):
    """B 库文件数 >= synced 数（改名加后缀）→ 不判缺集，避免整部重复复制。"""
    from app import mirror
    S, eng, d = env
    db = S()
    rule = _rule(db)
    _media(db, rule, "改名剧", src_ids=["R1", "R2"])
    nodes = _nodes({"d": [_a_file("R1"), _a_file("R2")]})
    stats = {}

    from app.utils import normalize_work_name

    async def _bc(api_, media):
        # 两集都在 B 库，只是名字改了（键口径同真机 _b_file_candidates）
        return {f"{normalize_work_name('R1.mkv')}:100": "B-R1",
                f"{normalize_work_name('R2.mkv')}:100": "B-R2"}, True
    mirror._b_file_candidates = _bc

    got, missing = asyncio.run(mirror.reconcile_mappings_prepass(
        db, rule, nodes, _works("改名剧"), _api(), stats, complete=True))
    assert stats["prepass"]["a_missing_episodes"] == 0
    assert "work:改名剧" not in missing
    assert stats["prepass"]["d_unchanged"] == 1


def test_priority_fix_batch_does_not_consume_max_works():
    """缺集补丁批次先于正常批执行，且不占 max_works / batch_size 名额。"""
    src = (Path(__file__).resolve().parents[1] / "backend" / "app" / "mirror.py").read_text(
        encoding="utf-8")
    assert "for work, cats in priority_works + batch:" in src
    assert 'stats["batch_size"] = len(batch)' in src
    assert 'stats["prepass"]["priority_fix_works"]' in src
    assert "works = [p for p in works if work_key_of(p[0]) not in missing_set]" in src
    assert "另优先补齐" in src
