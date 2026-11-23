"""v2.1.12 #296：处理顺序固定为「光鸭网盘自身的返回顺序」。

用户口径（原话）：「我建议将顺序固定按光鸭网盘自身的文件名排序升序排列进行修改逻辑」。

落法：
  - 顺序的唯一真源 = 请求参数 `LIST_ORDER_BY=3 / LIST_SORT_TYPE=1`
    （`app.guangya` 模块常量，见那里的 [事实]/[推断] 注释）；
  - `mirror` 侧**不再**本地重排（旧版按 `natural_key()` 重排 4 处，全部撤掉）。

为什么撤掉本地重排：本地再排一次，等于用我们自己的规则覆盖网盘顺序，
两边一旦不一致，用户看到的就是「网页里是 A 序、系统按 B 序处理」。

本文件同时保留 `natural_key` 的纯函数测试——它仍是公共工具函数
（展示层可能用），只是不再参与镜像遍历/处理顺序。
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
from app import mirror  # noqa: E402


# ---------- natural_key 纯函数（保留，作为工具函数契约） ----------

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


# ---------- 顺序 = 网盘返回顺序（不再本地重排） ----------

def test_media_files_keeps_server_file_order():
    """_media_files 保持网盘返回的文件顺序，不再按 E2<E9<E10 重排。"""
    server_order = ["E10.mkv", "E2.mkv", "E9.mkv"]
    root = _node("work", "剧集")
    root["files"] = [_file("f10", "E10.mkv"), _file("f2", "E2.mkv"), _file("f9", "E9.mkv")]
    nodes = {"work": root}
    got = [mirror._dir_name(item) for _, item in mirror._media_files(root, nodes)]
    assert got == server_order, "处理顺序必须与网盘返回顺序一致"


def test_media_files_keeps_server_dir_order():
    """_media_files 子目录按 children 原序递归（Season 10 在 Season 2 前也照原样）。"""
    s10 = _node("d10", "Season 10", files=[_file("x", "S10E01.mkv")], parent="work")
    s2 = _node("d2", "Season 2", files=[_file("y", "S02E01.mkv")], parent="work")
    root = _node("work", "剧集", children=["d10", "d2"], parent="root")
    nodes = {"work": root, "d10": s10, "d2": s2}
    got = [mirror._dir_name(item) for _, item in mirror._media_files(root, nodes)]
    assert got == ["S10E01.mkv", "S02E01.mkv"]


def test_discover_works_keeps_server_order():
    """_discover_works 根目录按 children 原序返回，不再按作品名自然序重排。"""
    w2 = _node("w2", "剧集2 (2020)", files=[_file("a", "E01.mkv")], parent="root")
    w10 = _node("w10", "剧集10 (2020)", files=[_file("b", "E01.mkv")], parent="root")
    w1 = _node("w1", "剧集1 (2020)", files=[_file("c", "E01.mkv")], parent="root")
    root = _node("root", "*", children=["w10", "w2", "w1"])
    nodes = {"root": root, "w2": w2, "w10": w10, "w1": w1}
    works = mirror._discover_works(nodes, "root")
    assert [w["name"] for w, _ in works] == [
        "剧集10 (2020)", "剧集2 (2020)", "剧集1 (2020)"]


def test_walk_source_keeps_server_order(monkeypatch):
    """_walk_source：children / files 均按接口返回序写入，不注入本地排序。"""
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
                rows = [_file("x", "E10.mkv"), _file("x2", "E2.mkv")]
            else:
                rows = [_file("y", "E2.mkv")]
            return {"list": rows, "total": len(rows)}

    monkeypatch.setattr(mirror, "GuangYaWebFiles", FakeGW)
    import asyncio
    nodes, complete = asyncio.run(mirror._walk_source(FakeGW(), "root"))
    assert complete is True
    assert nodes["root"]["children"] == ["d10", "d2"], "顺序按接口返回，不重排"
    assert [mirror._dir_name(f) for f in nodes["d10"]["files"]] == ["E10.mkv", "E2.mkv"]


def test_walk_source_does_not_sort_pagination_boundary(monkeypatch):
    """顺序真源在请求参数上：mirror 侧不得再出现任何 natural_key 排序调用。"""
    import inspect
    src = inspect.getsource(mirror)
    for fn in ("_walk_source", "_discover_works", "_media_files"):
        body = inspect.getsource(getattr(mirror, fn))
        assert "sorted(" not in body, f"{fn} 不应再本地重排"
        assert "natural_key(" not in body, f"{fn} 不应再按 natural_key 重排"


def test_list_dir_dedups_across_pages(monkeypatch):
    """翻页期间顺序变动产生的重复 fileId 必须去重，否则同一文件会被复制两次。"""
    page_size = mirror.LIST_PAGE_SIZE
    first = [{"fileId": f"f{i}", "fileName": f"{i}.mkv", "resType": 1}
             for i in range(page_size)]
    # 第二页把 f49 又返回一次（翻页期间顺序变动），再给两个新文件
    second = [{"fileId": f"f{page_size - 1}", "fileName": "dup.mkv", "resType": 1},
              {"fileId": "n1", "fileName": "n1.mkv", "resType": 1},
              {"fileId": "n2", "fileName": "n2.mkv", "resType": 1}]
    pages = {0: first, 1: second}

    class FakeGW:
        async def list_files(self, parent_id, page, page_size):
            rows = pages.get(page, [])
            return {"list": rows, "total": page_size + 2}

    import asyncio
    items, complete = asyncio.run(mirror._list_dir(FakeGW(), "root"))
    assert complete is True
    ids = [it["fileId"] for it in items]
    assert len(ids) == page_size + 2, "重复项只保留一份"
    assert len(set(ids)) == len(ids)
    assert ids[-2:] == ["n1", "n2"]


def test_order_constants_are_the_single_source():
    """顺序参数集中在 guangya 模块常量里，改一处即全局生效。"""
    from app import guangya
    assert (guangya.LIST_ORDER_BY, guangya.LIST_SORT_TYPE) == (3, 1)
    import inspect
    src = inspect.getsource(guangya.GuangYaWebFiles.list_files)
    assert '"orderBy": LIST_ORDER_BY' in src
    assert '"sortType": LIST_SORT_TYPE' in src
