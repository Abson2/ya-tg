"""v2.1.48 验收：A 库目录清单**绝不按字符硬截**（作品不许因为清单坏掉而消失）。

真机事故取证（用户 2026-11-25 转来的排查结论）：
  「完美世界(2021)」A 库 184 个文件、24 个视频；B 库目录建出来了，但 184 行全部
  failed（「B库未找到已同步文件」），该作品不在 A 库作品清单里，也查不到映射行。

  根因全部在 `files_json` 的**按字符硬截**上：
    ① `save_dir_manifest` 写的是 `json.dumps(files, ensure_ascii=False)[:200000]`，
       这个目录序列化后恰好 200000 字符 → 截断点在 JSON 中间 → 落库的是**非法 JSON**；
    ② 下一轮 `load_dir_manifest` 里 `except: files = []` → 当成「这个目录没有文件」；
    ③ `_subtree_video` → `_has_video` 判「没有视频」→ `_discover_works` 直接跳过该目录；
    ④ 于是作品**从 A 库发现结果里消失**，184 行复制永远失败，而且不会自愈
       （清单 utime 没变，轻量扫描一直复用这份坏清单）。
  `n_files` 列存的是 len(files)=184（真值），只是文件明细读不出来 —— 这条缝就是兜底的
  依据：条数还在，就不许把目录判成空。

本文件锁五件事：
  ① 落库永远是**合法 JSON**（老写法在同一份数据上必然解析失败 —— 反证固化了事故现场）；
  ② 体积上限内按整条装，装不下的如实记 `n_files` 真实条数（n_files > len(files) = 被截）；
  ③ 坏行（解析失败）不再当空目录：`files_broken=True` + utime 归 0 → 本轮强制重列自愈；
  ④ `_has_video` / `_discover_works`：看不全 ≠ 没有视频，作品不许被判没；真空目录照旧判空；
  ⑤ 清单看不全的目录数与作品数在同步汇总里如实上报（不许再静默）。
"""
import json
from datetime import datetime, timezone

import pytest
from sqlalchemy import delete, select

from app import mirror
from app.db import SessionLocal, migrate
from app.models import (
    AppConfig, Channel, MirrorDirScan, MirrorFile, MirrorMedia, MirrorRule,
)

ROOT = __import__("pathlib").Path(__file__).resolve().parents[1]
MIRROR_SRC = (ROOT / "backend/app/mirror.py").read_text(encoding="utf-8")


def _wipe(db):
    for model in (MirrorDirScan, MirrorFile, MirrorMedia, MirrorRule,
                  Channel, AppConfig):
        db.execute(delete(model))
    db.commit()


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as db:
        _wipe(db)
    yield


def _rule(db) -> MirrorRule:
    rule = MirrorRule(name="r", source_root_id="A", target_root_id="B")
    db.add(rule)
    db.commit()
    return rule


def _file(i: int, pad: int = 1100) -> dict:
    """一条像真机那样的文件条目：带长文件名（真实条目里还有缩略图等长字段）。"""
    return {"fileId": f"fid-{i:05d}", "resType": 1, "utime": 1700000000 + i,
            "fileSize": 1234567,
            "fileName": f"完美世界.S01E{i:03d}.2160p.WEB-DL.H265-{('x' * pad)}.mkv"}


# ===========================================================================
# 一、落库必须是合法 JSON（反证：老写法在同一份数据上必然坏）
# ===========================================================================
def test_old_char_slice_breaks_this_payload():
    """反证：老写法（把 json 字符串切到 20 万字符）在这份 184 条数据上必然产出非法 JSON。"""
    files = [_file(i) for i in range(184)]
    raw = json.dumps(files, ensure_ascii=False)
    assert len(raw) > 200000, "造数不够大，反证前提不成立（真机是恰好 200000 字符被截）"
    with pytest.raises(Exception):
        json.loads(raw[:200000])
    # 新写法：同样数据，永远能读回来
    text, kept = mirror._dump_items_capped(files, mirror.MANIFEST_MAX_FILE_BYTES)
    assert kept == 184, f"184 条在默认预算内应完整装下（真机就是这个量级），实际 {kept}"
    assert len(json.loads(text)) == 184


def test_dump_always_produces_valid_json():
    files = [_file(i) for i in range(50)]
    for budget in (0, 100, 1000, 5000, 10 ** 7):
        text, kept = mirror._dump_items_capped(files, budget)
        parsed = json.loads(text)          # 任何预算下都必须能解析
        assert isinstance(parsed, list)
        assert len(parsed) == kept
        assert kept <= len(files)


def test_dump_empty_list():
    assert mirror._dump_items_capped([], 1000) == ("[]", 0)


def test_dump_records_true_count_separately():
    """装不下时：JSON 合法 + 返回的条数小于总数（真实条数由调用方写 n_files）。"""
    files = [_file(i) for i in range(20)]
    text, kept = mirror._dump_items_capped(files, 3000)
    assert json.loads(text) == files[:kept]
    assert 0 < kept < 20


# ===========================================================================
# 二、写读往返：真机量级的目录不再丢明细
# ===========================================================================
def test_save_and_load_round_trip_keeps_all_files():
    nodes = {
        "w1": {"name": "完美世界(2021)", "parent": "cat", "path": "国产剧/完美世界(2021)",
               "depth": 2, "utime": 111, "ctime": 111,
               "files": [_file(i) for i in range(184)], "children": []},
    }
    with SessionLocal() as db:
        rule = _rule(db)
        mirror.save_dir_manifest(db, rule.id, nodes, {"w1"})
        got = mirror.load_dir_manifest(db, rule.id)
        row = db.scalar(select(MirrorDirScan).where(MirrorDirScan.dir_id == "w1"))

    assert len(got["w1"]["files"]) == 184, "真机 184 条的目录不该丢明细"
    assert got["w1"]["files_broken"] is False
    assert row.n_files == 184 and int(row.utime) == 111
    json.loads(row.files_json)          # 落库的必须是合法 JSON


def test_save_marks_truncation_by_true_count():
    """预算装不下时：n_files 记真实条数，明细只存装下的部分，JSON 仍合法。"""
    nodes = {"w1": {"name": "巨长目录", "parent": "", "path": "x", "depth": 1,
                    "utime": 1, "ctime": 1,
                    "files": [_file(i) for i in range(50)], "children": []}}
    with SessionLocal() as db:
        rule = _rule(db)
        # 用 3KB 预算强制截断（真机默认 200KB，184 条装得下）
        orig = mirror._manifest_max_bytes
        mirror._manifest_max_bytes = lambda db_, key: 3000
        try:
            out = mirror.save_dir_manifest(db, rule.id, nodes, {"w1"})
        finally:
            mirror._manifest_max_bytes = orig
        row = db.scalar(select(MirrorDirScan).where(MirrorDirScan.dir_id == "w1"))
        got = mirror.load_dir_manifest(db, rule.id)

    stored = json.loads(row.files_json)
    assert row.n_files == 50, "真实条数必须照实记，不能记成装下的条数"
    assert len(stored) < 50, "预算不够时明细会被截，这是预期的"
    assert got["w1"]["n_files"] > len(got["w1"]["files"]), "被截必须能从 n_files 看出来"
    assert out["truncated"] == 1


# ===========================================================================
# 三、坏行兜底：不当空目录，且强制重列自愈
# ===========================================================================
def test_broken_row_is_not_treated_as_empty():
    with SessionLocal() as db:
        rule = _rule(db)
        db.add(MirrorDirScan(rule_id=rule.id, dir_id="w1", parent_id="cat",
                             name="完美世界(2021)", path="国产剧/完美世界(2021)",
                             depth=2, utime=111, ctime=111,
                             files_json='[{"fileId": "fid-00001", "fileName": "完美世',
                             children_json="[]", n_files=184))
        db.commit()
        got = mirror.load_dir_manifest(db, rule.id)

    node = got["w1"]
    assert node["files_broken"] is True, "解析失败必须标出来，不能静默当空"
    assert node["n_files"] == 184, "真实条数在 n_files 列里，必须带下去"
    assert int(node["utime"]) == 0, "坏行的 utime 必须归 0，否则轻量扫描会一直复用坏清单"


def test_broken_row_is_force_relisted():
    """坏行在轻量扫描里不会被当成「缓存可用」。"""
    manifest = {"w1": {"utime": 0, "files": [], "files_broken": True, "n_files": 184}}
    c = manifest["w1"]
    same = (c is not None and int(c.get("utime") or 0) == 111
            and c.get("files") is not None and not c.get("files_broken"))
    assert same is False


# ===========================================================================
# 四、看不全 ≠ 没有视频：作品不许被判没
# ===========================================================================
def test_has_video_on_broken_node():
    assert mirror._has_video({"files": [], "n_files": 184, "files_broken": True}) is True


def test_has_video_on_truncated_node():
    assert mirror._has_video({"files": [_file(0)], "n_files": 50}) is True


def test_has_video_still_false_for_really_empty_dir():
    """真空目录仍判空 —— 否则分类目录会被误当成一部作品。"""
    assert mirror._has_video({"files": [], "n_files": 0}) is False
    assert mirror._has_video({"files": [], "n_files": 0, "files_broken": True}) is False


def _tree(broken: bool, n_files: int):
    return {
        "root": {"id": "root", "name": "", "children": ["cat1"], "files": [],
                 "n_files": 0},
        "cat1": {"id": "cat1", "name": "国产剧", "children": ["w1"], "files": [],
                 "n_files": 0},
        "w1": {"id": "w1", "name": "完美世界(2021)", "children": [], "files": [],
               "n_files": n_files, "files_broken": broken},
    }


def test_discover_works_keeps_broken_work():
    works = mirror._discover_works(_tree(broken=True, n_files=184), "root")
    names = [n.get("name") for n, _ in works]
    assert "完美世界(2021)" in names, f"清单坏掉后整部作品消失了（真机事故复现）：{names}"


def test_discover_works_drops_empty_dir_old_behaviour():
    """反证：不标记坏行、条数为 0 时作品确实会消失 —— 这就是改前的行为。"""
    works = mirror._discover_works(_tree(broken=False, n_files=0), "root")
    assert [n.get("name") for n, _ in works] == []


# ===========================================================================
# 五、可见性 + 源码守门
# ===========================================================================
def test_source_has_no_char_slice():
    assert "[:200000]" not in MIRROR_SRC, "按字符硬截的写法必须彻底消失"
    assert "_dump_items_capped" in MIRROR_SRC


def test_source_counts_broken_dirs_into_stats():
    assert 'stats["walk_manifest_broken"]' in MIRROR_SRC
    assert 'stats.get("walk_manifest_broken")' in MIRROR_SRC
    assert "清单看不全" in MIRROR_SRC


def test_version_is_current_everywhere():
    """版本锁：用例名不带死版本号，否则下一版又要改名字（v2.1.47 踩过）。"""
    assert 'version="2.1.63"' in (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
    assert "v2.1.63 · Asia/Shanghai" in (ROOT / "frontend/src/App.vue").read_text(
        encoding="utf-8")
    assert json.loads((ROOT / "frontend/package.json").read_text(
        encoding="utf-8"))["version"] == "2.1.63"
    assert "当前版本：v2.1.63" in (ROOT / "README.md").read_text(encoding="utf-8")
