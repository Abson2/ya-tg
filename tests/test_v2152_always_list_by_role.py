"""v2.1.52 验收：轻量扫描的「每轮必列」按**业务角色**判定，不再只看层数。

真机问题（2026-11-26，用户上报）：
  「庶女 (2026) {tmdb-342038} A 镜像库 S01E01 等剧集有变化，为什么没变量同步到
  B 库（建议：在兼容不频繁全量扫库到情况下，优化这种增量、变量未同步的情况）」
  只读排查坐实：A 库 E01–E06 整批换源（新 fileId、size 3–5 亿 → 15–21 亿、
  2160p.HDR Vivid），B 库与 DB 仍旧版；作品根 utime 未变且 DB 缓存一致；
  季目录缓存 utime 是旧的，A 库实时 utime 已变。

根因（源码归因，非猜测）：
  `MANIFEST_ALWAYS_LIST_DEPTH = 2` 的注释写的是「0=源根、1=分类目录、2=作品目录」，
  这个前提只在**分类只有一级**时成立。结构一旦是
  `A根/国产剧/古装/庶女/Season 1`，作品根落到 depth 3 → 走「缓存可复用」分支 →
  `_materialize_cached` 把季目录的**旧 utime** 一起带进 A 树 → 季目录也被判「未变」
  → 换源的新 fileId 永远进不了 A 树，`detect_increments` 无从发现。
  唯一能补的通道只剩 24 小时深度兜底（`mirror_manifest_depth_fallback_minutes`）。

修法 = 把必列判据从「层数」换成「业务角色」（不加深层数、不提高全量频率）：
  · 已知作品目录（`MirrorMedia.source_dir_id`）每轮必列 —— 列它是为了拿到季目录的
    实时 utime（作品目录自身的 utime 不会变，这正是常量注释里那条实测结论）；
  · 这些目录的分类祖先链也每轮必列 —— 多级分类层不列，新增作品同样看不见。

覆盖：
  - `always_list_dirs`：作品目录 + 祖先链、按规则隔离、off 关闭、N 轮转、成环不死循环
  - `_walk_source(always_dirs=...)`：多级分类下作品层必列（反证：不传就吃缓存）
  - **绝不向下传染**：季目录 utime 未变时不许因为父在 always_dirs 里就跟着列
  - 单级分类结构调用量零变化（回归守门：轻量扫描不退化）
  - 端到端：多级分类 + 换源 → `detect_increments` 必须命中（反证：不传必漏）
  - `plan_light_scan` / `sync_rule` 接线与统计回显
"""
import asyncio
import os
import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

MIRROR_SRC = (BACKEND / "app" / "mirror.py").read_text(encoding="utf-8")
RC_SRC = (BACKEND / "app" / "runtime_config.py").read_text(encoding="utf-8")


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v2152_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'b.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    from sqlalchemy.orm import sessionmaker
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng, d
    shutil.rmtree(d, ignore_errors=True)


def _rule(db, name="m52"):
    from app.models import MirrorRule
    r = MirrorRule(name=name, source_root_id="R", target_root_id="BROOT",
                   interval_minutes=60, enabled=True, file_idle_seconds=0.5)
    db.add(r)
    db.commit()
    db.refresh(r)
    return r


def _media(db, rule, key, source_dir_id, title="庶女"):
    from app.models import MirrorMedia
    m = MirrorMedia(rule_id=rule.id, work_key=key, title_standard=title,
                    source_dir_id=source_dir_id, target_dir_id="B-" + key,
                    english_title="EN", media_type="tv", status="synced")
    db.add(m)
    db.commit()
    db.refresh(m)
    return m


# ===========================================================================
# 列目录桩
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


def _multi_tree(season_utime=3, files=None):
    """多级分类：R(0) / 国产剧(1) / 古装(2) / 庶女(3) / Season 1(4)。

    作品根落在 **depth 3** —— 这正是真机结构，也是 `MANIFEST_ALWAYS_LIST_DEPTH=2`
    覆盖不到的那一层。
    """
    return {
        "R": [_dir("C1", "国产剧", utime=1)],
        "C1": [_dir("C2", "古装", utime=2)],
        "C2": [_dir("W", "庶女", utime=2)],          # 作品根 utime 不变
        "W": [_dir("S", "Season 1", utime=season_utime)],
        "S": list(files if files is not None else [_file("F1", "E01.mkv", 100)]),
    }


def _flat_tree(season_utime=3):
    """单级分类：R(0) / 国产剧(1) / 庶女(2) / Season 1(3) —— 改前的正常结构。"""
    return {
        "R": [_dir("C", "国产剧", utime=1)],
        "C": [_dir("W", "庶女", utime=2)],
        "W": [_dir("S", "Season 1", utime=season_utime)],
        "S": [_file("F1", "E01.mkv", 100)],
    }


def _walk(api, **kw):
    from app import mirror
    stats: dict = {}
    nodes, complete = asyncio.run(mirror._walk_source(api, "R", stats=stats, **kw))
    return nodes, complete, stats


def _light(api, **kw) -> tuple[dict, dict]:
    """轻量轮：返回 (nodes, stats)，stats 由调用方断言。"""
    from app import mirror
    stats: dict = kw.pop("stats", None) or {}
    nodes, _ok = asyncio.run(mirror._walk_source(api, "R", stats=stats, **kw))
    return nodes, stats


def _manifest_from(nodes, listed_at=None):
    return {k: {"utime": v["utime"], "files": v["files"],
                "children": v["children"], "name": v["name"],
                "path": v["path"], "parent": v["parent"],
                "depth": v["depth"], "last_listed_at": listed_at,
                "files_broken": False}
            for k, v in nodes.items()}


# ===========================================================================
# 一、always_list_dirs：谁是「必须每轮点名」的目录
# ===========================================================================

def test_known_work_dirs_are_scoped_to_rule(env):
    from app import mirror
    S, _eng, _d = env
    db = S()
    r1 = _rule(db, "m52-a")
    r2 = _rule(db, "m52-b")
    _media(db, r1, "work:a", "WA")
    _media(db, r1, "work:b", "WB")
    _media(db, r2, "work:c", "WC")
    assert mirror.known_work_dirs(db, r1.id) == {"WA", "WB"}
    assert mirror.known_work_dirs(db, r2.id) == {"WC"}, "按规则隔离，不许串台"
    db.close()


def test_always_list_covers_work_dir_and_category_ancestors(env):
    """作品目录 + 它的多级分类祖先链都要每轮列 —— 少一层就漏一类问题：
    少了作品层 → 季目录实时 utime 拿不到（换源看不见）；
    少了分类层 → 分类里新增作品看不见。"""
    from app import mirror
    S, _eng, _d = env
    db = S()
    rule = _rule(db)
    _media(db, rule, "work:庶女", "W")
    manifest = {"R": {"parent": "", "depth": 0},
                "C1": {"parent": "R", "depth": 1},
                "C2": {"parent": "C1", "depth": 2},
                "W": {"parent": "C2", "depth": 3},
                "S": {"parent": "W", "depth": 4}}
    got = mirror.always_list_dirs(db, rule.id, manifest)
    assert got == {"W", "C2", "C1", "R"}, got
    assert "S" not in got, "季目录不在必列名单里：它只按 utime 下探"
    db.close()


def test_always_list_off_returns_nothing(env, monkeypatch):
    from app import mirror
    from app import runtime_config
    S, _eng, _d = env
    db = S()
    rule = _rule(db)
    _media(db, rule, "work:庶女", "W")
    monkeypatch.setattr(runtime_config, "get",
                        lambda db_, key, *a, **k: "off"
                        if key == "mirror_always_list_works" else "")
    assert mirror.always_list_dirs(db, rule.id, {"W": {"parent": "", "depth": 3}}) == set()
    db.close()


def test_always_list_n_rotates_from_stalest(env, monkeypatch):
    """填 N 时按「最久没真列过」取前 N 部 —— 不许固定取目录序头部（尾部会饿死）。"""
    from app import mirror
    from app import runtime_config
    S, _eng, _d = env
    db = S()
    rule = _rule(db)
    _media(db, rule, "work:a", "WA")
    _media(db, rule, "work:b", "WB")
    _media(db, rule, "work:c", "WC")
    monkeypatch.setattr(runtime_config, "get",
                        lambda db_, key, *a, **k: "1"
                        if key == "mirror_always_list_works" else "")
    now = datetime.now(timezone.utc)
    manifest = {"WA": {"parent": "", "depth": 3, "last_listed_at": now},
                "WB": {"parent": "", "depth": 3,
                       "last_listed_at": now - timedelta(hours=30)},
                "WC": {"parent": "", "depth": 3, "last_listed_at": None}}
    got = mirror.always_list_dirs(db, rule.id, manifest)
    assert "WC" in got, "没有采样时刻 = 最久没列过，必须排最前"
    assert len(got) == 1
    db.close()


def test_always_list_survives_parent_cycle(env):
    """清单被写坏（parent 成环）也不许死循环 —— 宁可少列，不能挂住整轮同步。"""
    from app import mirror
    S, _eng, _d = env
    db = S()
    rule = _rule(db)
    _media(db, rule, "work:a", "W")
    manifest = {"W": {"parent": "X", "depth": 3}, "X": {"parent": "W", "depth": 4}}
    got = mirror.always_list_dirs(db, rule.id, manifest)
    assert got == {"W", "X"}
    db.close()


def test_always_list_limit_setting_registered():
    from app.runtime_config import DEFAULTS
    assert "mirror_always_list_works" in DEFAULTS
    assert DEFAULTS["mirror_always_list_works"][1] == "all", (
        "默认必须是「全部每轮必列」，否则真机的换源问题依旧存在")


# ===========================================================================
# 二、_walk_source：多级分类下作品层必须真列
# ===========================================================================

def test_multi_level_work_dir_is_listed_every_round(env):
    """正面：作品根在 depth 3，凭 `always_dirs` 必须每轮真列。"""
    api1 = FakeTree(_multi_tree())
    n1, _ok, _st = _walk(api1)
    manifest = _manifest_from(n1)

    api2 = FakeTree(_multi_tree())
    n2, stats = _light(api2, manifest=manifest, light=True,
                       always_dirs={"R", "C1", "C2", "W"})
    assert "W" in api2.calls, (
        "多级分类下作品根（depth 3）没被列 → 季目录实时 utime 拿不到，换源永远看不见")
    assert stats["walk_always_listed"] == 1
    assert stats["walk_always_dirs"] == 4


def test_multi_level_work_dir_cached_without_always_list(env):
    """反证：不传 always_dirs（即 v2.1.51 的行为）→ 作品根吃缓存，季目录不列。

    这条用例把真机 bug 固定下来：只要有人把 always_dirs 去掉，它就必须变红。
    """
    api1 = FakeTree(_multi_tree())
    n1, _ok, _st = _walk(api1)
    manifest = _manifest_from(n1)

    api2 = FakeTree(_multi_tree())
    n2, stats = _light(api2, manifest=manifest, light=True)
    assert "W" not in api2.calls, "作品根在 depth 3，按老判据不该被列"
    assert n2["W"]["cached"] is True
    assert "S" not in api2.calls, "季目录跟着作品根一起被缓存挡住"
    assert stats["walk_always_listed"] == 0


def test_always_list_does_not_propagate_downward(env):
    """红线：`always_dirs` 只列这一层，**绝不向下传染**。

    传染就等于对这些作品做全量下探，轻量扫描直接废掉（用户口径是「不频繁全量扫库」）。
    季目录 utime 没变时，就算父目录在 always_dirs 里也不许列。
    """
    api1 = FakeTree(_multi_tree(season_utime=3))
    n1, _ok, _st = _walk(api1)
    manifest = _manifest_from(n1)

    api2 = FakeTree(_multi_tree(season_utime=3))     # 季目录 utime 未变
    _light(api2, manifest=manifest, light=True,
           always_dirs={"R", "C1", "C2", "W"})
    assert "W" in api2.calls, "作品层必须列"
    assert "S" not in api2.calls, (
        "季目录被 always_dirs 传染了 —— 轻量扫描退化成全量")


def test_season_utime_change_still_descends_after_always_list(env):
    """列了作品层之后，季目录 utime 变了仍必须照旧下探（发现换源的那一跳）。"""
    api1 = FakeTree(_multi_tree(season_utime=3))
    n1, _ok, _st = _walk(api1)
    manifest = _manifest_from(n1)

    api2 = FakeTree(_multi_tree(season_utime=99))
    n2, _stats = _light(api2, manifest=manifest, light=True,
                        always_dirs={"R", "C1", "C2", "W"})
    assert "S" in api2.calls
    assert n2["S"]["cached"] is False and n2["S"]["utime"] == 99


def test_flat_category_calls_unchanged(env):
    """回归守门：单级分类结构下，按角色必列**一个请求都不多**。

    线上绝大多数作品是单级分类（作品根 depth 2，本来就在必列范围内），
    这一版绝不能把它们的每轮调用量抬上去。
    """
    api1 = FakeTree(_flat_tree())
    n1, _ok, _st = _walk(api1)
    manifest = _manifest_from(n1)

    api_a = FakeTree(_flat_tree())
    _light(api_a, manifest=manifest, light=True)
    api_b = FakeTree(_flat_tree())
    _light(api_b, manifest=manifest, light=True,
           always_dirs={"R", "C", "W"})
    assert api_a.calls == api_b.calls, "单级分类下调用量变了 —— 轻量扫描被抬高"
    assert api_b.calls == ["R", "C", "W"]


# ===========================================================================
# 三、端到端：多级分类 + 换源，必须被 detect_increments 发现
# ===========================================================================

def _swap_setup(env):
    """建一部「庶女」：已映射、已同步一集（F1/size 100），清单缓存的是换源前的树。"""
    from app.models import MirrorFile
    from app import mirror
    S, _eng, _d = env
    db = S()
    rule = _rule(db)
    media = _media(db, rule, "work:庶女", "W")
    db.add(MirrorFile(media_id=media.id, rule_id=rule.id, identity="s01e01",
                      source_file_id="F1", file_name="E01.mkv", size=100,
                      utime=1, fingerprint="100:1", status="synced"))
    db.commit()
    api1 = FakeTree(_multi_tree(season_utime=3))
    n1, _ok, _st = _walk(api1)
    manifest = _manifest_from(n1, listed_at=datetime.now(timezone.utc))
    return db, rule, media, manifest


def test_swap_detected_under_multi_level_categories(env):
    """真机场景复现：A 库把 E01 换成新 fileId + 更大 size。

    作品根 utime 不变、季目录 utime 变了。带 always_dirs 时必须当轮发现。
    """
    from app import mirror
    db, rule, _media_row, manifest = _swap_setup(env)
    swapped = _multi_tree(season_utime=99,
                          files=[_file("F1-NEW", "E01.mkv", 1500)])
    api2 = FakeTree(swapped)
    nodes, _stats = _light(api2, manifest=manifest, light=True,
                           always_dirs=mirror.always_list_dirs(db, rule.id, manifest))
    works = mirror._discover_works(nodes, "R")
    hits, st = mirror.detect_increments(db, rule, works, nodes)
    assert "work:庶女" in hits, "换源没被发现 —— B 库永远拿不到新版"
    assert st["new_files"] == 1 and st["works"] == 1
    db.close()


def test_swap_missed_without_always_list(env):
    """反证：不传 always_dirs → 换源漏掉（这就是 2026-11-26 的真机现象）。"""
    from app import mirror
    db, rule, _media_row, manifest = _swap_setup(env)
    swapped = _multi_tree(season_utime=99,
                          files=[_file("F1-NEW", "E01.mkv", 1500)])
    api2 = FakeTree(swapped)
    nodes, _stats = _light(api2, manifest=manifest, light=True)
    works = mirror._discover_works(nodes, "R")
    hits, st = mirror.detect_increments(db, rule, works, nodes)
    assert "work:庶女" not in hits and st["new_files"] == 0
    db.close()


# ===========================================================================
# 四、接线与统计
# ===========================================================================

def test_plan_light_scan_exposes_always_dirs(env):
    from app import mirror
    S, _eng, _d = env
    db = S()
    rule = _rule(db)
    _media(db, rule, "work:庶女", "W")
    plan = mirror.plan_light_scan(db, rule.id, {}, deep_minutes=1440)
    assert plan["always_dirs"] == set(), "全量轮用不上必列名单"
    now = datetime.now(timezone.utc)
    manifest = {"R": {"parent": "", "depth": 0, "utime": 1,
                      "files": [], "children": ["W"], "last_listed_at": now},
                "W": {"parent": "R", "depth": 1, "utime": 2,
                      "files": [], "children": [], "last_listed_at": now}}
    plan2 = mirror.plan_light_scan(db, rule.id, manifest, deep_minutes=1440)
    assert plan2["light"] is True
    assert plan2["always_dirs"] == {"W", "R"}
    assert "按角色必列 2 个目录" in plan2["reason"], plan2["reason"]
    db.close()


def test_sync_rule_passes_always_dirs_to_walk():
    src = MIRROR_SRC
    assert "always_dirs: set[str] | None = None" in src, "_walk_source 缺 always_dirs 形参"
    assert 'always_dirs=_plan.get("always_dirs") or set()' in src, (
        "sync_rule 没把必列名单传给走树 —— 多级分类下换源又会同步不过去")
    # 必列名单与「强制下探」必须是两份，绝不能合并（合并 = 传染 = 变全量）
    assert "force = set(force_dirs or ())" in src
    assert 'always = {str(x) for x in (always_dirs or ()) if str(x or "").strip()}' in src
    assert "force |= always" not in src and "force.update(always" not in src


def test_last_message_reports_always_listed_count():
    assert "个作品或分类目录的定点补查" in MIRROR_SRC
    assert 'stats["walk_always_listed"] = always_listed' in MIRROR_SRC


def test_runtime_config_documents_the_switch():
    assert '"mirror_always_list_works": ("mirror_always_list_works", "all")' in RC_SRC
    assert "多级分类" in RC_SRC
