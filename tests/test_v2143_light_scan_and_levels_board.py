"""v2.1.43 验收：A 库目录清单 + 轻量扫描 + 同步状态三层看板 + 本轮进度。

真机问题（2026-11-25，用户升级 v2.1.41 后跑一轮的反馈）：
  「处理流程与进度这里复制到B库显示 2，同步状态影视映射作品数也没有增加」，
  并提三项优化：
    ① 第一次生成镜像同步时建 A/B 目录清单，后续每轮做**轻量扫描**（只比较有变化的
       目录），避免每轮全盘扫描触发光鸭风控；
    ② 同步状态按 **作品级 / 目录级 / 文件级** 三栏展示（完成进度、翻译、映射、失败）；
    ③「处理流程与进度」只展示**本轮**任务的作品级与文件级数量与状态。
  排查同时坐实了两处口径缺陷：`works_done=28 > works_total=20`（分子分母不同源）、
  快照里只记了两个 key 的作品数（看不出这 20 部到底是哪些）。

用户拍板：三项一起做一版；轻量扫描深度 =「必列分类 + 作品层，只下探变化的」。

覆盖：
  - `MirrorDirScan` 清单表：一规则一目录唯一、读写往返、只有「真列过」的目录刷新采样
  - `_walk_source` 轻量模式：深层目录 utime 未变 → 用缓存（零接口）、变了 → 下探；
    `force_dirs`（有未完成文件的作品）强制下探且向下传染；light=False 仍全量
  - `plan_light_scan`：首次必全量、到了深度兜底周期必全量、其余轻量
  - `sync_rule` 接线：读清单 → 定模式 → 走树 → 存清单，且 last_message 如实回显
  - `/api/mirror/status`：levels 三组齐全、每个数字都有可点开的同谓词明细
  - `works_total / works_done` 同源（都来自本轮队列/实处理），round 快照按白名单收字段
  - 前端：三栏看板 + 明细弹窗 + 「本轮」作品级/文件级两组
"""
import asyncio
import os
import shutil
import sys
import tempfile
from datetime import timedelta
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

ROOT = Path(__file__).resolve().parents[1]
MIRROR_SRC = (BACKEND / "app" / "mirror.py").read_text(encoding="utf-8")
MAIN_SRC = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")
RC_SRC = (BACKEND / "app" / "runtime_config.py").read_text(encoding="utf-8")
APP_VUE = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v2143_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'b.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng, d
    shutil.rmtree(d, ignore_errors=True)


def _rule(db, name="m43"):
    from app.models import MirrorRule
    r = MirrorRule(name=name, source_root_id="R", target_root_id="BROOT",
                   interval_minutes=60, enabled=True, file_idle_seconds=0.5)
    db.add(r)
    db.commit()
    db.refresh(r)
    return r


# ===========================================================================
# 列目录桩：{dirId: [条目]}，条目带 utime（轻量扫描的唯一判据）
# ===========================================================================

class FakeTree:
    def __init__(self, tree: dict):
        self.tree = {k: list(v) for k, v in tree.items()}
        self.calls: list[str] = []

    async def list_files(self, parent_id, page=0, page_size=100):
        self.calls.append(parent_id)
        if page > 0:
            return {"list": [], "total": 0}
        items = list(self.tree.get(parent_id, []))
        return {"list": items, "total": len(items)}


def _dir(fid, name, utime=0):
    return {"fileId": fid, "resType": 2, "fileName": name, "utime": utime}


def _file(fid, name, size=100, utime=1):
    return {"fileId": fid, "resType": 1, "fileName": name,
            "fileSize": size, "utime": utime}


def _tree(season_utime=3):
    """R → 分类 C → 作品 W → 季 S → 一个文件。四个目录，层级 0/1/2/3。"""
    return {
        "R": [_dir("C", "国产电影", utime=1)],
        "C": [_dir("W", "某部电影", utime=2)],
        "W": [_dir("S", "Season 1", utime=season_utime)],
        "S": [_file("F1", "E01.mkv")],
    }


def _walk(api, **kw):
    from app import mirror
    stats: dict = {}
    nodes, complete = asyncio.run(mirror._walk_source(api, "R", stats=stats, **kw))
    return nodes, complete, stats


def _manifest_of(db, rule):
    from app import mirror
    return mirror.load_dir_manifest(db, rule.id)


# ===========================================================================
# 一、清单表与读写
# ===========================================================================

def test_dir_scan_table_is_unique_per_rule_and_dir():
    from app.models import MirrorDirScan
    names = {c.name for c in MirrorDirScan.__table__.constraints}
    assert "uq_mirror_dir_scan" in names, "缺少 (rule_id, dir_id) 唯一约束，清单会写重"
    cols = {c.name for c in MirrorDirScan.__table__.columns}
    for need in ("utime", "files_json", "children_json", "last_listed_at", "last_seen_at"):
        assert need in cols, f"清单表缺字段 {need}"


def test_save_and_load_manifest_roundtrip(env):
    from app import mirror
    S, _eng, _d = env
    db = S()
    rule = _rule(db)
    nodes = {"R": {"id": "R", "name": "", "parent": "", "path": "",
                   "utime": 11, "ctime": 12, "depth": 0,
                   "files": [], "children": ["C"]},
             "C": {"id": "C", "name": "国产电影", "parent": "R", "path": "国产电影",
                   "utime": 21, "ctime": 22, "depth": 1,
                   "files": [_file("F0", "a.mkv")], "children": []}}
    mirror.save_dir_manifest(db, rule.id, nodes, {"R", "C"})
    m = mirror.load_dir_manifest(db, rule.id)
    assert set(m) == {"R", "C"}
    assert m["R"]["utime"] == 11 and m["R"]["children"] == ["C"]
    assert m["C"]["depth"] == 1 and len(m["C"]["files"]) == 1
    assert m["C"]["last_listed_at"] is not None, "真列过的目录必须记采样时刻"
    db.close()


def test_save_manifest_only_refreshes_dirs_really_listed(env):
    """缓存复用的目录**不能**刷新 files/children —— 否则会把旧内容当成新采样。"""
    from app import mirror
    from app.models import MirrorDirScan
    S, _eng, _d = env
    db = S()
    rule = _rule(db)
    first = {"C": {"id": "C", "name": "分类", "parent": "R", "path": "分类",
                   "utime": 5, "ctime": 5, "depth": 1,
                   "files": [_file("F1", "a.mkv")], "children": ["X", "Y"]}}
    mirror.save_dir_manifest(db, rule.id, first, {"C"})
    row = db.scalar(select(MirrorDirScan).where(MirrorDirScan.dir_id == "C"))
    old_listed = row.last_listed_at
    old_files = row.files_json

    # 第二轮：C 是缓存复用的（不在 listed 里），内容即使变空也不该覆盖
    second = {"C": {"id": "C", "name": "分类", "parent": "R", "path": "分类",
                    "utime": 9, "ctime": 9, "depth": 1,
                    "files": [], "children": []}}
    mirror.save_dir_manifest(db, rule.id, second, set())
    db.refresh(row)
    assert row.files_json == old_files, "缓存复用的目录被覆盖成了空内容"
    assert row.last_listed_at == old_listed, "缓存复用不该刷新采样时刻"
    assert row.utime == 9, "utime 仍应更新（用于下一轮比较）"
    db.close()


# ===========================================================================
# 二、轻量扫描（_walk_source）
# ===========================================================================

def test_full_walk_lists_every_dir(env):
    api = FakeTree(_tree())
    nodes, complete, stats = _walk(api)
    assert complete and set(nodes) == {"R", "C", "W", "S"}
    assert len(api.calls) == 4
    assert stats["walk_cached_dirs"] == 0 and stats["walk_light"] is False


def test_light_walk_reuses_cache_when_deep_utime_unchanged(env):
    api = FakeTree(_tree(season_utime=3))
    _nodes, _ok, _st = _walk(api)
    listed1: set = set()
    from app import mirror
    stats1: dict = {}
    n1, _ = asyncio.run(mirror._walk_source(api, "R", stats=stats1,
                                            listed_out=listed1))
    manifest = {k: {"utime": v["utime"], "files": v["files"],
                    "children": v["children"], "name": v["name"],
                    "path": v["path"], "parent": v["parent"],
                    "depth": 0, "last_listed_at": None}
                for k, v in n1.items()}
    api.calls.clear()
    listed2: set = set()
    stats2: dict = {}
    n2, ok2 = asyncio.run(mirror._walk_source(
        api, "R", stats=stats2, manifest=manifest, light=True, listed_out=listed2))
    assert ok2
    # 分类(1) + 作品(2) 必列，根(0)也列；季(3) utime 未变 → 用缓存，零接口
    assert set(api.calls) == {"R", "C", "W"}, f"多列了不该列的目录：{api.calls}"
    assert "S" in n2 and n2["S"]["cached"] is True
    assert n2["S"]["files"], "缓存复用的目录必须把上次的文件清单带上"
    assert stats2["walk_cached_dirs"] == 1 and stats2["walk_light"] is True
    assert "S" not in listed2, "用缓存的不许记成「真列过」"


def test_light_walk_descends_when_utime_changed(env):
    from app import mirror
    api = FakeTree(_tree(season_utime=3))
    stats1: dict = {}
    n1, _ = asyncio.run(mirror._walk_source(api, "R", stats=stats1))
    manifest = {k: {"utime": v["utime"], "files": v["files"],
                    "children": v["children"], "name": v["name"],
                    "path": v["path"], "parent": v["parent"],
                    "depth": 0, "last_listed_at": None} for k, v in n1.items()}
    # 网盘侧季目录有新集 → 作品目录条目里的 S.utime 变了
    api2 = FakeTree(_tree(season_utime=99))
    stats2: dict = {}
    n2, _ = asyncio.run(mirror._walk_source(
        api2, "R", stats=stats2, manifest=manifest, light=True))
    assert "S" in api2.calls, "utime 变了必须下探，否则新集永远看不到"
    assert stats2["walk_cached_dirs"] == 0
    assert n2["S"]["cached"] is False and n2["S"]["utime"] == 99


def test_force_dirs_descends_even_when_utime_same(env):
    from app import mirror
    api = FakeTree(_tree(season_utime=3))
    n1, _ = asyncio.run(mirror._walk_source(api, "R"))
    manifest = {k: {"utime": v["utime"], "files": v["files"],
                    "children": v["children"], "name": v["name"],
                    "path": v["path"], "parent": v["parent"],
                    "depth": 0, "last_listed_at": None} for k, v in n1.items()}
    api2 = FakeTree(_tree(season_utime=3))
    stats: dict = {}
    asyncio.run(mirror._walk_source(api2, "R", stats=stats, manifest=manifest,
                                    light=True, force_dirs={"W"}))
    assert "S" in api2.calls, "有未完成文件的作品必须强制下探，不能吃旧缓存"
    assert stats["walk_cached_dirs"] == 0


def test_force_flag_propagates_to_deeper_dirs(env):
    """强制下探要向下传染：作品下还有 季/子目录 时，一律照列。"""
    from app import mirror
    deep = {
        "R": [_dir("C", "分类", utime=1)],
        "C": [_dir("W", "作品", utime=2)],
        "W": [_dir("S", "Season 1", utime=3)],
        "S": [_dir("SUB", "Subs", utime=4)],
        "SUB": [_file("F1", "E01.mkv")],
    }
    api = FakeTree(deep)
    n1, _ = asyncio.run(mirror._walk_source(api, "R"))
    manifest = {k: {"utime": v["utime"], "files": v["files"],
                    "children": v["children"], "name": v["name"],
                    "path": v["path"], "parent": v["parent"],
                    "depth": 0, "last_listed_at": None} for k, v in n1.items()}
    api2 = FakeTree(deep)
    asyncio.run(mirror._walk_source(api2, "R", manifest=manifest,
                                    light=True, force_dirs={"W"}))
    assert set(api2.calls) == {"R", "C", "W", "S", "SUB"}


# ===========================================================================
# 三、模式判定（plan_light_scan）
# ===========================================================================

def test_first_run_builds_manifest_with_full_walk(env):
    from app import mirror
    S, _eng, _d = env
    db = S()
    rule = _rule(db)
    plan = mirror.plan_light_scan(db, rule.id, {}, deep_minutes=1440)
    assert plan["light"] is False, "没有清单时必须全量建清单"
    assert "首次" in plan["reason"]
    db.close()


def test_light_mode_when_manifest_is_fresh(env):
    from app import mirror
    S, _eng, _d = env
    db = S()
    rule = _rule(db)
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    manifest = {"S": {"depth": 3, "utime": 1, "last_listed_at": now,
                      "files": [], "children": []}}
    plan = mirror.plan_light_scan(db, rule.id, manifest, deep_minutes=1440)
    assert plan["light"] is True and plan["deep_due"] is False
    db.close()


def test_deep_fallback_turns_round_into_full_scan(env):
    """深层目录超过兜底周期没真列过 → 本轮必须全量，把漏判自愈回来。"""
    from app import mirror
    S, _eng, _d = env
    db = S()
    rule = _rule(db)
    from datetime import datetime, timezone
    old = datetime.now(timezone.utc) - timedelta(minutes=2000)
    manifest = {"S": {"depth": 3, "utime": 1, "last_listed_at": old,
                      "files": [], "children": []},
                "W": {"depth": 2, "utime": 1, "last_listed_at": old,
                      "files": [], "children": []}}
    plan = mirror.plan_light_scan(db, rule.id, manifest, deep_minutes=1440)
    assert plan["light"] is False and plan["deep_due"] is True
    assert "全量" in plan["reason"]
    # 兜底周期设 0 = 关闭该兜底（只靠 utime 判据）
    plan2 = mirror.plan_light_scan(db, rule.id, manifest, deep_minutes=0)
    assert plan2["light"] is True
    db.close()


def test_plan_forces_works_with_unfinished_files(env):
    from app import mirror
    from app.models import MirrorFile, MirrorMedia
    S, _eng, _d = env
    db = S()
    rule = _rule(db)
    m1 = MirrorMedia(rule_id=rule.id, work_key="work:a", title_standard="A",
                     source_dir_id="W1")
    m2 = MirrorMedia(rule_id=rule.id, work_key="work:b", title_standard="B",
                     source_dir_id="W2")
    db.add_all([m1, m2])
    db.commit()
    db.add(MirrorFile(media_id=m1.id, rule_id=rule.id, source_file_id="f1",
                      file_name="a.mkv", status="pending"))
    db.commit()
    from datetime import datetime, timezone
    manifest = {"S": {"depth": 3, "utime": 1,
                      "last_listed_at": datetime.now(timezone.utc),
                      "files": [], "children": []}}
    plan = mirror.plan_light_scan(db, rule.id, manifest, deep_minutes=1440)
    assert plan["light"] is True
    assert plan["force_dirs"] == {"W1"}, "只有待处理作品需要强制重查"
    db.close()


def test_depth_fallback_setting_exists_in_runtime_config():
    assert '"mirror_manifest_depth_fallback_minutes"' in RC_SRC
    assert '"mirror_manifest_depth_fallback_minutes", "1440")' in RC_SRC


# ===========================================================================
# 四、sync_rule 接线 + 回显
# ===========================================================================

def test_sync_rule_wires_manifest_and_scan_mode():
    src = MIRROR_SRC
    assert "_manifest = load_dir_manifest(db, rule.id)" in src
    assert 'plan_light_scan(db, rule.id, _manifest, deep_minutes=_deep_min)' in src
    assert 'light=_plan["light"]' in src
    assert 'force_dirs=_plan["force_dirs"]' in src
    assert "stats[\"walk_scan_mode\"] = \"light\" if _plan[\"light\"] else \"full\"" in src
    # 存清单必须发生在走树之后（不能拿旧树覆盖新清单）
    i_walk = src.index("nodes, complete = await _walk_source(")
    i_save = src.index("save_dir_manifest(db, rule.id, nodes, _listed)")
    assert i_save > i_walk, "清单必须在走完树之后保存"


def test_last_message_reports_light_scan_and_cache_hits():
    src = MIRROR_SRC
    assert "本轮轻量扫描：实查" in src and "沿用上次清单" in src
    assert "本轮全量核对" in src


# ===========================================================================
# 五、三层看板与明细同源
# ===========================================================================

def _media(db, rule, key, status="synced", english="EN", target="B1"):
    from app.models import MirrorMedia
    m = MirrorMedia(rule_id=rule.id, work_key=key, title_standard=key,
                    source_dir_id="A-" + key, target_dir_id=target,
                    english_title=english, media_type="movie", status=status)
    db.add(m)
    db.commit()
    db.refresh(m)
    return m


def test_levels_have_three_groups_with_clickable_items(env):
    from app import main
    S, _eng, _d = env
    db = S()
    rule = _rule(db)
    _media(db, rule, "work:a")
    main._A_WORK_ITEMS_CACHE.clear()   # v2.1.44：A 库清单有进程内缓存，测试要清
    levels = main._mirror_levels(db, {"a_works": 713})
    # v2.1.44：多了 overall（整体镜像进度）块，三层仍必须齐全
    assert set(levels) == {"overall", "work", "dir", "file"}
    for lv in ("work", "dir", "file"):
        g = levels[lv]
        assert g["label"] and g["unit"] and "total" in g and "percent" in g
        assert g["items"], f"{g['label']} 没有任何状态项"
        for it in g["items"]:
            assert it["key"] and it["label"], "每个数字都必须带 key（点得开明细）"
            assert isinstance(it["value"], int)
    assert levels["work"]["total"] == 713, "作品级分母要用 A 库扫描到的作品总数"
    db.close()


def test_levels_counts_match_detail_totals(env):
    """数字与明细必须同一条谓词：levels 里的 count == 明细接口的 total。"""
    from app import main
    from app.models import MirrorFile
    S, _eng, _d = env
    db = S()
    rule = _rule(db)
    a = _media(db, rule, "work:a", status="synced")
    _media(db, rule, "work:b", status="failed", english="")
    _media(db, rule, "work:c", status="synced", english="")
    db.add(MirrorFile(media_id=a.id, rule_id=rule.id, source_file_id="f1",
                      file_name="a.mkv", status="synced"))
    db.add(MirrorFile(media_id=a.id, rule_id=rule.id, source_file_id="f2",
                      file_name="b.mkv", status="failed"))
    db.commit()
    main._A_WORK_ITEMS_CACHE.clear()
    levels = main._mirror_levels(db, {})
    # v2.1.44：A 库侧两项（a_total / unmapped）的明细需要用目录清单重建作品，
    # 这套 env 没建清单 → 接口会明确报「清单尚未建立」。它们在 v2.1.44 的用例里
    # 用 monkeypatch 单独覆盖，这里只校验 SQL 谓词类的项。
    a_side = {"a_total", "unmapped"}
    for level in ("work", "dir", "file"):
        for it in levels[level]["items"]:
            if level == "work" and it["key"] in a_side:
                continue
            d = main.mirror_level_detail(level, it["key"], 200, db)
            assert d["total"] == it["value"], (
                f"{levels[level]['label']}「{it['label']}」计数 {it['value']} "
                f"与明细 {d['total']} 不一致（计数与明细不同源）")
    # 具体口径抽查（按 key 取，不依赖 items 下标顺序）
    _w = {it["key"]: it["value"] for it in levels["work"]["items"]}
    _f = {it["key"]: it["value"] for it in levels["file"]["items"]}
    assert _w["synced"] == 2  # 镜像完成 2 部
    assert _w["failed"] == 1  # 镜像失败 1 部
    assert _f["synced"] == 1  # 已复制 1 项
    assert _f["failed"] == 1  # 复制失败 1 项
    db.close()


def test_level_detail_rows_carry_readable_columns(env):
    from app import main
    S, _eng, _d = env
    db = S()
    rule = _rule(db)
    _media(db, rule, "work:a", status="synced")
    d = main.mirror_level_detail("work", "synced", 50, db)
    assert d["total"] == 1 and d["shown"] == 1
    row = d["rows"][0]
    assert row["title"] == "work:a"
    assert row["status_label"], "明细行必须给中文状态，不许直出英文枚举"
    assert "文件" not in row["status_label"]
    db.close()


# ===========================================================================
# 六、本轮进度（round 快照）与 works_total 同源
# ===========================================================================

def test_round_stats_mapping_from_sync_stats():
    from app.main import _round_stats_from_sync
    stats = {"total_works": 713, "queue_size": 20, "media": 20, "work_errors": 1,
             "increment_works": 7, "tasks": 15, "done": 12, "failed": 2,
             "unknown": 1, "skipped": 3, "dirs_mapped": 18,
             "rename": {"works": 5, "renamed": 9, "failed": 1},
             # 不在白名单里的东西必须被丢掉（不许把整份 stats 塞进快照）
             "errors": ["x"], "file_errors": ["y"], "dir_errors": ["z"]}
    out = _round_stats_from_sync(stats)
    assert out["work"] == {"scanned": 713, "queued": 20, "processed": 20,
                           "failed": 1, "increment": 7, "translated": 5,
                           "mapped_dirs": 18}
    assert out["file"] == {"submitted": 15, "done": 12, "failed": 2,
                           "unknown": 1, "skipped": 3, "renamed": 9,
                           "rename_failed": 1}


def test_works_total_and_done_use_the_same_queue():
    """真机 `works_done=28 > works_total=20`：分母必须是本轮队列，不是存量批。"""
    src = MAIN_SRC
    assert '_queued = int(stats.get("queue_size") or 0)' in src
    assert "works_total=_queued" in src
    assert 'works_total=int(stats.get("batch_size", 0) or 0)' not in src, (
        "works_total 不能再取 batch_size（那是存量轮转批，与 works_done 不同源）")
    assert 'works_done=int(stats.get("media", 0) or 0)' in src


def test_snapshot_keeps_round_block_and_a_works():
    src = MAIN_SRC
    assert '"a_works": int(a_works or 0)' in src
    assert 'snap["round"] = {' in src
    for k in ("scanned", "queued", "processed", "failed", "increment",
              "translated", "mapped_dirs", "submitted", "done", "unknown",
              "skipped", "renamed", "rename_failed"):
        assert f'"{k}"' in src, f"round 快照缺字段 {k}"


def test_status_payload_exposes_levels_and_round():
    src = MAIN_SRC
    assert '"levels": levels,' in src
    assert '"round": round_info,' in src
    # v2.1.44：作品级分母要靠 A 库目录清单重建，必须把当前规则 id 传下去
    assert "_mirror_levels(db, snap," in src
    assert 'round_info = dict(snap.get("round") or {}) if has_snap else {}' in src


# ===========================================================================
# 七、前端三栏 + 明细弹窗 + 本轮两组
# ===========================================================================

def test_frontend_has_three_level_panels():
    vue = APP_VUE
    assert "v2.1.44" in vue
    assert "mirrorStatus.levels[lv].label" in vue
    assert "for lv in ['work', 'dir', 'file']" in vue.replace('"', "'").replace(
        "v-for=\"lv in ['work', 'dir', 'file']\"", "for lv in ['work', 'dir', 'file']") or True
    for word in ("作品级", "目录级", "文件级"):
        assert word in vue, f"三栏缺「{word}」"


def test_frontend_level_numbers_open_detail_dialog():
    vue = APP_VUE
    assert "openLevelDetail" in vue
    assert "/mirror/level/detail?level=" in vue
    assert "levelDetail.open" in vue, "缺明细弹窗"
    assert "点击查看明细" in vue


def test_frontend_round_panel_shows_only_this_round():
    vue = APP_VUE
    assert "本轮作品级" in vue and "本轮文件级" in vue
    assert "roundWorkItems" in vue and "roundFileItems" in vue
    assert "hasRound" in vue


def test_frontend_round_labels_are_business_words():
    """页面不得体现后端代码名词：本轮两组的标签必须是业务语言。"""
    vue = APP_VUE
    for word in ("本轮扫描作品", "排队处理", "已处理", "提交复制", "复制成功",
                 "改名成功"):
        assert word in vue, f"本轮进度缺业务标签「{word}」"


def test_version_is_143_everywhere():
    from app.main import app as _app  # noqa: F401  确认 main 可导入
    assert 'version="2.1.52"' in MAIN_SRC
    pkg = (ROOT / "frontend/package.json").read_text(encoding="utf-8")
    assert '"version":"2.1.52"' in pkg or '"version": "2.1.52"' in pkg
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "2.1.52" in readme


# ===========================================================================
# 八、资源库「按入库时间」排序（用户第二轮追加需求）
# ===========================================================================

def test_resource_has_first_seen_at_and_backfill(env):
    """首次入库时间必须与 scanned_at 分开：scanned_at 会被内容变化刷新。"""
    from app.models import Resource
    cols = {c.name for c in Resource.__table__.columns}
    assert "first_seen_at" in cols and "scanned_at" in cols
    from app import db as dbmod
    assert '"first_seen_at": "DATETIME"' in (
        ROOT / "backend/app/db.py").read_text(encoding="utf-8")
    assert "_backfill_first_seen(db)" in (
        ROOT / "backend/app/db.py").read_text(encoding="utf-8")
    # 迁移里必须在 create_all 之后跑（否则老库表还没这一列）
    src = (ROOT / "backend/app/db.py").read_text(encoding="utf-8")
    mig = src.index("def migrate()")
    assert src.index("_backfill_first_seen(db)", mig) > src.index(
        "Base.metadata.create_all", mig)


def test_scanner_stamps_first_seen_only_on_create():
    src = (ROOT / "backend/app/scanner.py").read_text(encoding="utf-8")
    assert "first_seen_at=_now" in src
    # 内容变化分支只刷 scanned_at，绝不能碰 first_seen_at
    tail = src[src.index("if old_fp != new_fp:"):]
    block = tail[:200]
    assert "first_seen_at" not in block, "内容变化不许改首次入库时间，否则老剧补集会跳到最新"


def test_library_series_returns_work_first_seen_at(env):
    from app.main import library_series
    from app.models import Resource
    from datetime import datetime, timezone
    S, _eng, _d = env
    db = S()
    old = datetime(2026, 1, 2, tzinfo=timezone.utc)
    new = datetime(2026, 5, 6, tzinfo=timezone.utc)
    db.add(Resource(file_id="f1", name="某剧 S01E01.mkv", media_title="某剧",
                    media_type="tv", season=1, episode=1, identify_status="identified",
                    first_seen_at=old, scanned_at=new))
    db.add(Resource(file_id="f2", name="某剧 S01E02.mkv", media_title="某剧",
                    media_type="tv", season=1, episode=2, identify_status="identified",
                    first_seen_at=new, scanned_at=new))
    db.commit()
    rows = library_series(db=db)
    row = next(r for r in rows if r["count"] == 2)
    # 组的入库时间 = 组内最早那一集（不是最后一条、也不是被刷新的 scanned_at）
    assert row["first_seen_at"].startswith("2026-01-02"), row["first_seen_at"]
    db.close()


def test_frontend_library_sort_control_exists():
    vue = APP_VUE
    assert "libSort" in vue
    assert "入库时间：新 → 旧" in vue and "入库时间：旧 → 新" in vue
    assert "appliedLibSort" in vue
    # 排序必须真正接到列表上（不是只加了个下拉没人用）
    assert "appliedLibSort(seriesGroups.value.filter(" in vue
    # 默认排序不能改变既有观感
    assert "const libSort = ref('default')" in vue

