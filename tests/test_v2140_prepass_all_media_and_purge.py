"""v2.1.40 验收：C 类预扫描覆盖全库 + 归档记录可彻底删除。

真机问题（2026-11-25）：
  1. 「缅北电诈覆灭纪实」整部从 A 库消失（A 库目录还在但已空），2 条文件记录挂在
     「待复制」整周。C 类本该兜住它，但 `reconcile_mappings_prepass` 把 media 取成
     `order_by(id)[:max_media]` —— 只有最老入库的 50 部有保护，且永不轮转。
     线上 260 部 → 覆盖率 19%，该作品 media id=220 永远扫不到。
  2. 归档（v2.1.38）只是从「待处理」里摘出去，行还在，「已手动清理」清单只增不减，
     没有任何入口能删掉。

覆盖：
  - C 类判定覆盖 max_media 之外的作品（核心回归）
  - C 类幂等：处理干净后不重复计数、不重复删 B 库
  - 单轮清理额度：超出的整部留到下一轮，不半途清空 B 库句柄
  - 异常护栏：A 树枚举出 0 部作品 / 一次命中过多 → 一律不动数据
  - B/A 类仍受 max_media 限量（接口护栏不变）
  - purge 只删 archived 行，synced / failed 一律不动
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
    d = tempfile.mkdtemp(prefix="v2140_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'b.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng, d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(autouse=True)
def _restore_b_candidates():
    from app import mirror
    original = mirror._b_file_candidates
    yield
    mirror._b_file_candidates = original


def _rule(db):
    from app.models import MirrorRule
    r = MirrorRule(name="m40", source_root_id="A-ROOT", target_root_id="B-ROOT",
                   interval_minutes=60, enabled=True, file_idle_seconds=0.5)
    db.add(r)
    db.commit()
    db.refresh(r)
    return r


def _a_file(fid, size=100, utime=1):
    return {"fileId": fid, "fileName": f"{fid}.mkv", "fileSize": size, "utime": utime}


def _media(db, rule, wk, *, src_ids, status="synced"):
    from app.models import MirrorMedia, MirrorFile
    m = MirrorMedia(rule_id=rule.id, work_key=wk, title_standard=wk,
                    source_dir_id="A-" + wk, target_dir_id="BDIR", status=status)
    db.add(m)
    db.flush()
    for i, sid in enumerate(src_ids):
        db.add(MirrorFile(media_id=m.id, rule_id=rule.id, identity=f"name:{wk}:E{i}",
                          source_file_id=sid, target_file_id=f"B-{sid}",
                          file_name=f"{sid}.mkv", fingerprint="100:1",
                          size=100, status="synced"))
    db.commit()
    db.refresh(m)
    return m


def _works(*names):
    return [({"id": f"A-{n}", "name": n, "parent": "A-ROOT", "path": n,
              "files": [], "children": []}, []) for n in names]


def _nodes(files_by_dir):
    return {did: {"id": did, "name": did, "parent": "A-ROOT", "path": did,
                  "files": files, "children": []} for did, files in files_by_dir.items()}


def _api():
    api = AsyncMock()
    api.delete_files = AsyncMock(return_value={"data": {"taskId": "t1"}})
    return api


def _run(db, rule, nodes, works, stats, *, api, complete=True):
    from app import mirror
    return asyncio.run(mirror.reconcile_mappings_prepass(
        db, rule, nodes, works, api, stats, complete=complete))


# ---------- 核心回归：C 类必须覆盖 max_media 之外的作品 ----------

def test_c_class_covers_media_beyond_max_media(env):
    """第 60 部（远超 max_media=10）整部消失 → 必须被 C 类识别并收敛。

    真机上这就是「缅北电诈」：media id=220，离线前 50 的切片之外，永远扫不到。
    """
    from app import mirror, runtime_config
    S, eng, d = env
    db = S()
    rule = _rule(db)
    runtime_config.set_many(db, {"mirror_prepass_max_media": "10", "mirror_mode": "full"})

    alive = [f"剧{i:02d}" for i in range(59)]
    nodes, works = {}, []
    for n in alive:
        fid = f"F-{n}"
        nodes[f"D-{n}"] = {"id": f"D-{n}", "name": n, "parent": "A-ROOT", "path": n,
                          "files": [_a_file(fid)], "children": []}
        works.append(({"id": f"A-{n}", "name": n, "parent": "A-ROOT",
                       "path": n, "files": [], "children": []}, []))
        _media(db, rule, n, src_ids=[fid])
    gone = _media(db, rule, "已删剧", src_ids=["GONE1", "GONE2"])   # 第 60 部
    assert gone.id > 10, "前置条件：这部作品必须在 max_media 之外"

    stats = {}
    api = _api()

    async def _bc(api_, media):
        return {}, True
    mirror._b_file_candidates = _bc

    _run(db, rule, nodes, works, stats, api=api)
    pp = stats["prepass"]
    assert pp["c_candidates"] == 1
    assert pp["c_deleted_a"] == 1, "max_media 之外的整部消失也必须被 C 类收敛"
    assert pp["c_anomaly"] == 0
    rows = db.scalars(select(mirror.MirrorFile)
                      .where(mirror.MirrorFile.media_id == gone.id)).all()
    assert all(r.status == "failed" and r.target_file_id == "" for r in rows)
    assert "A 库已无该作品" in (rows[0].error or "")
    api.delete_files.assert_awaited_once()


def test_b_class_still_limited_by_max_media(env):
    """B/A 类要反查 B 库，仍按 max_media 限量：超出预算的作品本轮不动。"""
    from app import mirror, runtime_config
    S, eng, d = env
    db = S()
    rule = _rule(db)
    runtime_config.set_many(db, {"mirror_prepass_max_media": "1"})
    m1 = _media(db, rule, "剧甲", src_ids=["F1"])
    m2 = _media(db, rule, "剧乙", src_ids=["F2"])
    nodes = _nodes({"D1": [_a_file("F1")], "D2": [_a_file("F2")]})
    works = _works("剧甲", "剧乙")

    async def _bc(api_, media):
        return {}, True  # B 库整部为空 → 本该判 B 类
    mirror._b_file_candidates = _bc

    stats = {}
    got, _missing = _run(db, rule, nodes, works, stats, api=_api())
    assert m1.id < m2.id
    assert got == ["work:剧甲"], "只有预算内的第一部会反查 B 库"
    assert stats["prepass"]["b_over_budget"] >= 1


# ---------- 幂等与额度 ----------

def test_c_class_idempotent_across_rounds(env):
    """第二轮不得重复计数、不得重复删 B 库、不得把原因文本叠加。"""
    from app import mirror, runtime_config
    S, eng, d = env
    db = S()
    rule = _rule(db)
    runtime_config.set_many(db, {"mirror_prepass_max_media": "50"})
    gone = _media(db, rule, "已删剧", src_ids=["GONE1"])
    nodes = _nodes({"DX": [_a_file("KEEP1")]})
    works = _works("现存剧")
    api = _api()

    stats1 = {}
    _run(db, rule, nodes, works, stats1, api=api)
    assert stats1["prepass"]["c_deleted_a"] == 1

    db.expire_all()
    first_err = db.scalars(select(mirror.MirrorFile)
                           .where(mirror.MirrorFile.media_id == gone.id)).first().error

    stats2 = {}
    _run(db, rule, nodes, works, stats2, api=api)
    pp2 = stats2["prepass"]
    assert pp2["c_candidates"] == 1, "命中量仍要如实统计（用户看得到还有几部）"
    assert pp2["c_deleted_a"] == 0, "已处理干净的作品不再重复计数"
    assert pp2["c_mapping_cleared"] == 0
    assert api.delete_files.await_count == 1, "B 库不得被重复删除"
    db.expire_all()
    again = db.scalars(select(mirror.MirrorFile)
                       .where(mirror.MirrorFile.media_id == gone.id)).first()
    assert again.error == first_err, "原因文本不得每轮叠加"


def test_c_class_defers_beyond_delete_budget(env):
    """单轮清理额度用尽 → 整部留到下一轮，绝不清空 B 库句柄（否则永久残留）。"""
    from app import mirror, runtime_config
    S, eng, d = env
    db = S()
    rule = _rule(db)
    runtime_config.set_many(db, {"mirror_prepass_max_media": "1"})
    gone1 = _media(db, rule, "已删甲", src_ids=["G1"])
    gone2 = _media(db, rule, "已删乙", src_ids=["G2"])
    nodes = _nodes({"DX": [_a_file("KEEP1")]})
    works = _works("现存剧")
    api = _api()

    stats = {}
    _run(db, rule, nodes, works, stats, api=api)
    pp = stats["prepass"]
    assert pp["c_candidates"] == 2
    assert pp["c_deleted_a"] == 1
    assert pp["c_deferred"] == 1
    db.expire_all()
    deferred = [m for m in (gone1, gone2)
                if db.scalars(select(mirror.MirrorFile)
                              .where(mirror.MirrorFile.media_id == m.id)).first()
                .target_file_id]
    assert len(deferred) == 1, "暂缓的那部必须保留 B 库句柄，下一轮才删得掉"


# ---------- 异常护栏 ----------

def test_c_class_paused_when_a_tree_has_no_works(env):
    """A 库一部作品都没枚举到、而库里有几十部映射 ≈ 取数出问题，绝不能照常动刀。"""
    from app import mirror, runtime_config
    S, eng, d = env
    db = S()
    rule = _rule(db)
    runtime_config.set_many(db, {"mirror_prepass_max_media": "50"})
    medias = [_media(db, rule, f"已删{i:02d}", src_ids=[f"G{i}"]) for i in range(12)]
    api = _api()

    stats = {}
    _run(db, rule, _nodes({}), [], stats, api=api)
    pp = stats["prepass"]
    assert pp["c_candidates"] == 12
    assert pp["c_anomaly"] == 12
    assert pp["c_deleted_a"] == 0
    api.delete_files.assert_not_called()
    db.expire_all()
    row = db.scalars(select(mirror.MirrorFile)
                     .where(mirror.MirrorFile.media_id == medias[0].id)).first()
    assert row.status == "synced" and row.target_file_id == "B-G0", "护栏下数据必须原样"


def test_c_class_paused_when_hits_exceed_guard(env):
    """一轮命中两三成库 ≈ 枚举异常，而非用户真删这么多片 → 暂缓并回显告警。"""
    from app import mirror, runtime_config
    S, eng, d = env
    db = S()
    rule = _rule(db)
    runtime_config.set_many(db, {"mirror_prepass_max_media": "50"})
    for i in range(12):
        _media(db, rule, f"已删{i:02d}", src_ids=[f"G{i}"])
    _media(db, rule, "现存剧", src_ids=["KEEP1"])
    nodes = _nodes({"DX": [_a_file("KEEP1")]})
    api = _api()

    async def _bc(api_, media):
        return {}, True
    mirror._b_file_candidates = _bc

    stats = {}
    _run(db, rule, nodes, _works("现存剧"), stats, api=api)
    pp = stats["prepass"]
    assert pp["c_candidates"] == 12
    assert pp["c_anomaly"] == 12
    assert pp["c_deleted_a"] == 0
    api.delete_files.assert_not_called()


def test_c_class_runs_when_hits_below_guard(env):
    """小批量命中（真实删片场景）必须照常处理，护栏不能把正常清理也挡住。"""
    from app import mirror, runtime_config
    S, eng, d = env
    db = S()
    rule = _rule(db)
    runtime_config.set_many(db, {"mirror_prepass_max_media": "50"})
    for i in range(3):
        _media(db, rule, f"已删{i:02d}", src_ids=[f"G{i}"])
    _media(db, rule, "现存剧", src_ids=["KEEP1"])
    nodes = _nodes({"DX": [_a_file("KEEP1")]})
    api = _api()

    async def _bc(api_, media):
        return {}, True
    mirror._b_file_candidates = _bc

    stats = {}
    _run(db, rule, nodes, _works("现存剧"), stats, api=api)
    pp = stats["prepass"]
    assert pp["c_anomaly"] == 0
    assert pp["c_deleted_a"] == 3


# ---------- 归档记录彻底删除 ----------

def _archived(db, rule, wk, sid, status="archived"):
    from app.models import MirrorMedia, MirrorFile
    m = MirrorMedia(rule_id=rule.id, work_key=wk, title_standard=wk,
                    source_dir_id="A-" + wk, target_dir_id="BDIR", status="failed")
    db.add(m)
    db.flush()
    f = MirrorFile(media_id=m.id, rule_id=rule.id, identity=f"name:{wk}",
                   source_file_id=sid, target_file_id="", file_name=f"{sid}.mkv",
                   fingerprint="100:1", size=100, status=status,
                   error="A 库未找到源文件（可能已被移动或删除）")
    db.add(f)
    db.commit()
    db.refresh(f)
    return f


def test_purge_only_deletes_archived_rows(env):
    """只删 archived：已同步 / 复制失败一律不动（这是唯一物理删行的入口）。"""
    from app import mirror
    S, eng, d = env
    db = S()
    rule = _rule(db)
    a = _archived(db, rule, "已清理剧", "GONE1", status="archived")
    b = _archived(db, rule, "已同步剧", "OK1", status="synced")
    c = _archived(db, rule, "失败剧", "BAD1", status="failed")

    out = mirror.purge_archived_files(db, rule, [a.id, b.id, c.id])
    assert out["purged"] == 1
    assert out["ids"] == [a.id]
    assert out["skipped"] == 2
    db.expire_all()
    assert db.get(mirror.MirrorFile, a.id) is None
    assert db.get(mirror.MirrorFile, b.id) is not None
    assert db.get(mirror.MirrorFile, c.id) is not None


def test_purge_without_ids_touches_only_archived(env):
    from app import mirror
    S, eng, d = env
    db = S()
    rule = _rule(db)
    a1 = _archived(db, rule, "清理甲", "G1", status="archived")
    a2 = _archived(db, rule, "清理乙", "G2", status="archived")
    keep = _archived(db, rule, "同步剧", "OK1", status="synced")

    out = mirror.purge_archived_files(db, rule, [])
    assert out["purged"] == 2
    db.expire_all()
    assert db.get(mirror.MirrorFile, a1.id) is None
    assert db.get(mirror.MirrorFile, a2.id) is None
    assert db.get(mirror.MirrorFile, keep.id) is not None


def test_purge_dry_run_does_not_delete(env):
    from app import mirror
    S, eng, d = env
    db = S()
    rule = _rule(db)
    a = _archived(db, rule, "清理剧", "G1", status="archived")
    out = mirror.purge_archived_files(db, rule, [a.id], dry_run=True)
    assert out["purged"] == 1 and out["dry_run"] is True
    db.expire_all()
    assert db.get(mirror.MirrorFile, a.id) is not None


def test_purge_does_not_touch_other_rule(env):
    from app import mirror
    from app.models import MirrorRule
    S, eng, d = env
    db = S()
    rule = _rule(db)
    other = MirrorRule(name="m40-b", source_root_id="A2", target_root_id="B2",
                       interval_minutes=60, enabled=False)
    db.add(other)
    db.commit()
    a = _archived(db, rule, "清理剧", "G1", status="archived")
    b = _archived(db, other, "别的规则", "G2", status="archived")
    out = mirror.purge_archived_files(db, rule, [])
    assert out["purged"] == 1
    db.expire_all()
    assert db.get(mirror.MirrorFile, a.id) is None
    assert db.get(mirror.MirrorFile, b.id) is not None, "不得跨规则误删"


# ---------- 端点与前端接线 ----------

def test_cleanup_accepts_purge_archive_scope():
    src = (Path(__file__).resolve().parents[1] / "backend" / "app" / "main.py").read_text(
        encoding="utf-8")
    assert '"media", "archive", "purge_archive"' in src
    assert 'scope == "purge_archive"' in src
    assert "mirror.purge_archived_files(" in src
    assert "mirror_files_purged" in src


def test_purge_scope_does_not_take_the_mirror_lock():
    """纯 DB 操作不该被正在跑的长任务挡在锁外（与 v2.1.38 归档同口径）。"""
    src = (Path(__file__).resolve().parents[1] / "backend" / "app" / "main.py").read_text(
        encoding="utf-8")
    start = src.index('if scope == "purge_archive":')
    end = src.index('if scope == "pruned":', start)
    branch = src[start:end]
    code = "\n".join(line.split("#")[0] for line in branch.splitlines())
    assert "_mirror_guard" not in code


def test_frontend_has_purge_button_and_function():
    vue = (Path(__file__).resolve().parents[1] / "frontend" / "src" / "App.vue").read_text(
        encoding="utf-8")
    assert "purgeArchivedFiles" in vue
    assert "purge_archive" in vue
    assert "删除选中记录" in vue
    assert "archivedTableRef" in vue
    assert "archivedSelected" in vue


def test_frontend_archived_panel_keeps_row_key_selection():
    """归档表加勾选后仍要 row-key + reserve-selection，否则轮询会把选中刷掉（v2.1.37 教训）。"""
    vue = (Path(__file__).resolve().parents[1] / "frontend" / "src" / "App.vue").read_text(
        encoding="utf-8")
    marker = 'ref="archivedTableRef"'
    start = vue.index(marker)
    snippet = vue[start:start + 400]
    assert 'row-key="id"' in snippet
    assert "reserve-selection" in snippet
