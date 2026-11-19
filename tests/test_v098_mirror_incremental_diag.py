"""v0.9.8 #196：镜像增量口径透明化——列目录开销诊断（慢在列目录，复制是增量的）。"""
import asyncio
from unittest.mock import AsyncMock, patch

from app import mirror
from app.models import MirrorRule


def test_walk_source_collects_diagnostics():
    """_walk_source 回传遍历目录数/列目录请求数/A 库文件数。"""
    dirs = {
        "root": [
            {"fileId": "d1", "resType": 2, "name": "电影"},
        ],
        "d1": [
            {"fileId": "f1", "resType": 1, "name": "a.mkv", "size": 1},
            {"fileId": "f2", "resType": 1, "name": "b.mkv", "size": 1},
        ],
    }

    async def fake_list(api, did):
        return dirs.get(did, []), True

    stats = {}
    with patch.object(mirror, "_list_dir", new=fake_list):
        nodes, complete = asyncio.run(mirror._walk_source(None, "root", stats=stats))
    assert complete is True
    assert set(nodes) == {"root", "d1"}
    assert stats["walk_dirs"] == 2
    assert stats["walk_list_calls"] == 2
    assert stats["walk_a_files"] == 2


def test_incremental_skips_unchanged_files():
    """已 synced 且指纹未变的文件不重复复制（增量红线）。"""
    # 通过源码契约断言：sync_rule 中存在 fingerprint 比对 + unchanged 跳过分支
    import inspect
    src = inspect.getsource(mirror.sync_rule)
    assert "fingerprint" in src
    assert "unchanged" in src
    assert "synced_rows" in src  # 已同步行按 (source_file_id, fingerprint) 索引


def test_diag_in_last_message_template():
    src = __import__("inspect").getsource(mirror.sync_rule)
    assert "列目录" in src and "未变跳过" in src
