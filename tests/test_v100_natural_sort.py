"""阶段二路线B C2：确定性自然排序。

验证 natural_key 数值段按数值比较（S2<S10、E9<E10），以及 _walk_source 的
排序注入不破坏"分页不完整即中止"红线；_discover_works / _media_files 的
输出顺序完全由节点名自然序决定，与节点在 dict / children 列表中的插入顺序无关。

不改身份/指纹/唯一键/映射：本文件不含任何 DB 写断言，纯函数级覆盖。
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
from app import mirror  # noqa: E402


def test_natural_key_numeric_order():
    names = ["S10", "S2", "S01", "E9", "E10", "E2", "第3季", "第11季", "第2季"]
    assert sorted(names, key=mirror.natural_key) == [
        "E2", "E9", "E10", "S01", "S2", "S10", "第2季", "第3季", "第11季",
    ]


def test_natural_key_mixed_text_and_number():
    # 中文/英文混合、大小写不敏感、数字边界
    names = ["Movie 2", "movie 10", "Movie 1", "movie 01"]
    assert sorted(names, key=mirror.natural_key) == [
        "Movie 1", "movie 01", "Movie 2", "movie 10",
    ]


def _node(nid, name, children=None, files=None, parent=""):
    return {"id": nid, "name": name, "parent": parent, "path": name,
            "files": files or [], "children": children or []}


def _file(fid, name):
    return {"fileId": fid, "fileName": name, "fileSize": 1, "utime": 1, "resType": 1}


def test_media_files_returns_natural_order_regardless_of_insertion_order():
    """_media_files：文件名 E2/E9/E10 按自然序，与 files 列表原始顺序无关。"""
    root = _node("work", "剧集")
    root["files"] = [
        _file("f10", "E10.mkv"),
        _file("f2", "E2.mkv"),
        _file("f9", "E9.mkv"),
    ]
    nodes = {"work": root}
    got = [mirror._dir_name(item) for _, item in mirror._media_files(root, nodes)]
    assert got == ["E2.mkv", "E9.mkv", "E10.mkv"]


def test_media_files_dirs_in_natural_order():
    """_media_files：Season 子目录按自然序（S2 在 S10 前），父目录先。"""
    s10 = _node("d10", "Season 10", files=[_file("x", "S10E01.mkv")], parent="work")
    s2 = _node("d2", "Season 2", files=[_file("y", "S02E01.mkv")], parent="work")
    root = _node("work", "剧集", children=["d10", "d2"], parent="root")
    nodes = {"work": root, "d10": s10, "d2": s2}
    got = [mirror._dir_name(item) for _, item in mirror._media_files(root, nodes)]
    assert got == ["S02E01.mkv", "S10E01.mkv"]


def test_discover_works_natural_order():
    """_discover_works：根目录多部作品按自然序返回，与 children 顺序无关。"""
    w2 = _node("w2", "剧集2 (2020)", files=[_file("a", "E01.mkv")], parent="root")
    w10 = _node("w10", "剧集10 (2020)", files=[_file("b", "E01.mkv")], parent="root")
    w1 = _node("w1", "剧集1 (2020)", files=[_file("c", "E01.mkv")], parent="root")
    root = _node("root", "*", children=["w10", "w2", "w1"])
    nodes = {"root": root, "w2": w2, "w10": w10, "w1": w1}
    works = mirror._discover_works(nodes, "root")
    assert [w["name"] for w, _ in works] == ["剧集1 (2020)", "剧集2 (2020)", "剧集10 (2020)"]


def test_walk_source_natural_order(monkeypatch):
    """_walk_source：子目录/文件按自然序写入 nodes（排序注入生效）。"""
    calls = {"root": 0, "d2": 0, "d10": 0}

    class FakeGW:
        def __init__(self, db=None):
            pass

        async def list_files(self, parent_id, page, page_size):
            if parent_id == "root":
                rows = [
                    {"fileId": "d10", "fileName": "Season 10", "resType": 2},
                    {"fileId": "d2", "fileName": "Season 2", "resType": 2},
                ]
            elif parent_id == "d10":
                rows = [_file("x", "E10.mkv")]
            else:
                rows = [_file("y", "E2.mkv")]
            return {"list": rows, "total": len(rows)}

    monkeypatch.setattr(mirror, "GuangYaWebFiles", FakeGW)
    import asyncio
    nodes, complete = asyncio.run(mirror._walk_source(FakeGW(), "root"))
    assert complete is True
    # children 列表按自然序（S2 在 S10 前）
    assert nodes["root"]["children"] == ["d2", "d10"]
    # 文件自然序
    assert [mirror._dir_name(f) for f in nodes["d2"]["files"]] == ["E2.mkv"]
