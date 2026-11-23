"""v0.7.1 回归：整剧集数/大小、扫描后自动识别、对账批次保护、完结回填。

对应用户反馈的四个「需求关键点」：
  1. 推送集数显示不对（同剧未识别单集被漏算）
  2. 完结标签没实现（存量已识别资源无 status 回填）
  3. 扫描自动识别没实现（扫描完成后不触发 TMDB 识别）
  4. 重新扫描库功能没有（全量重扫 + 对账移除 + 取消分享）
以及 V2.0 第 12.2 节「扫描批次保护」：不完整批次绝不清理资源。
"""
import asyncio
from unittest.mock import patch

import pytest
from sqlalchemy import select

from app.db import SessionLocal, migrate
from app.main import (_post_scan_identify, _reconcile_library, _series_group_key,
                      rescan_library)
from app.models import QueueItem, Resource
from app.publisher import _build_message
from app.utils import (is_video_file, normalize_work_name, work_episode_numbers,
                       work_group_key)


def _clean():
    with SessionLocal() as db:
        db.query(QueueItem).delete()
        db.query(Resource).delete()
        db.commit()


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    _clean()
    yield
    _clean()


@pytest.fixture
def db(_fresh):
    """会话统一托管：先于 _fresh 清理前 rollback/close，断言失败也不会锁死 SQLite。"""
    with SessionLocal() as session:
        try:
            yield session
        finally:
            session.rollback()


def _res(fid, name, **kw):
    base = dict(file_id=fid, name=name, ext=".mkv", size=1, utime=1)
    base.update(kw)
    return Resource(**base)


# ---------- 1. 作品键：A 库 {tmdb-} 标记不再导致同剧裂卡 ----------
def test_work_key_handles_tmdb_tag_and_mixed_identify_state():
    """已识别单集(share_dir_name) 与未识别单集(目录名含 {tmdb-}) 必须同键。"""
    a = _res("w1", "侠客行.S01E01.mkv", share_dir_name="侠客行",
             media_title="侠客行", tmdb_id=135646, media_type="tv",
             identify_status="identified", poster_path="/p.jpg")
    b = _res("w2", "侠客行.S01E02.mkv",
             path_hint="影视/侠客行 (2001) {tmdb-135646}/Season 1")
    assert work_group_key(a) == work_group_key(b)
    assert _series_group_key(a) == work_group_key(a)  # main 复用共享实现
    assert normalize_work_name("大醉侠 (2002) {tmdb-135646}") == "大醉侠"
    assert normalize_work_name("剧名 (2002) tmdb-135646") == "剧名"


# ---------- 2. 整剧集数 / 总大小 ----------
def test_episode_count_includes_unidentified_episodes(db):
    """36 集里只有 1 集识别成功，也必须显示 36 集 / 72GB（旧逻辑只算 1 集）。"""
    for i in range(1, 37):
        ok = (i == 1)
        db.add(_res(
            f"v071-e{i}", f"侠客行.2001.S01E{i:02d}.1080p.mkv", size=2 * 1024 ** 3,
            path_hint="影视/侠客行 (2001) {tmdb-135646}/Season 1",
            share_dir_name="侠客行", media_title="侠客行" if ok else "",
            media_type="tv" if ok else "", tmdb_id=135646 if ok else None,
            identify_status="identified" if ok else "unidentified",
            poster_path="/p.jpg" if ok else "", season=1 if ok else None,
            episode=i if ok else None))
    db.commit()
    rep = db.scalar(select(Resource).where(Resource.file_id == "v071-e1"))
    text, _ = _build_message(db, rep, "https://share/x")
    assert "共 36 集" in text, text
    assert "72.0GB" in text, text


def test_non_video_files_excluded_from_episode_count(db):
    """字幕/nfo 不计入集数，避免虚高。"""
    for i in (1, 2):
        db.add(_res(f"v071-v{i}", f"Show.S01E0{i}.mkv", size=1024 ** 3,
                    path_hint="影视/Show/Season 1", share_dir_name="Show",
                    media_type="tv", identify_status="identified", poster_path="/p.jpg",
                    media_title="Show", tmdb_id=99))
    db.add(_res("v071-sub", "Show.S01E01.srt", ext=".srt", size=1024,
                path_hint="影视/Show/Season 1", share_dir_name="Show"))
    db.commit()
    rep = db.scalar(select(Resource).where(Resource.file_id == "v071-v1"))
    text, _ = _build_message(db, rep, "https://share/x")
    assert "共 2 集" in text, text
    assert "2.0GB" in text, text
    assert is_video_file(_res("x", "a.srt", ext=".srt")) is False


def test_episode_numbers_parsed_from_filename_when_db_empty():
    """未识别单集的集号从文件名现场解析，不依赖 _apply_identification。"""
    x = _res("v071-p", "Tian.Xian.Pei.S01E28.2007.mkv", path_hint="影视/天仙配")
    assert work_episode_numbers(x) == (1, 28)


# ---------- 3. 扫描后自动识别 + 完结回填 ----------
def test_post_scan_identify_and_finish_backfill(db):
    db.add(_res("v071-id1", "大醉侠.2002.1080p.mkv",
                path_hint="影视/大醉侠 (2002) {tmdb-135646}",
                identify_status="unidentified"))
    # 存量已识别但缺完结状态（v0.7.0 之前识别的资源）
    db.add(_res("v071-old", "老剧.S01E01.mkv", media_type="tv", tmdb_id=135646,
                identify_status="identified", poster_path="/p.jpg",
                media_title="老剧", path_hint="影视/老剧/Season 1"))
    db.commit()

    best = {"tmdb_id": 135646, "media_type": "movie", "media_title": "大醉侠",
            "poster_path": "/p.jpg", "finish_status": "", "genres": [],
            "origin_country": [], "overview": "", "original_title": "",
            "vote_average": 0}
    detail = {"tmdb_id": 135646, "media_type": "tv", "finish_status": "ended",
              "title": "老剧"}

    async def fake_identify(name, db=None, path_hint="", use_ai=True,
                         *, forced_tmdb_id=0, forced_type=""):
        return {"ok": True, "status": "identified", "source": "tmdb",
                "reason": "", "hint": {}, "ai": None, "candidates": [best], "best": best}

    async def fake_detail(tid, mt, db=None):
        return detail

    with patch("app.main.tmdb.identify", fake_identify), \
         patch("app.main.tmdb.detail", fake_detail):
        stats = asyncio.run(_post_scan_identify(db, publish_after_identify=False, limit=50))
    db.commit()
    assert stats["identified"] == 1, stats
    assert stats["finish_backfilled"] >= 1, stats
    got = db.scalar(select(Resource).where(Resource.file_id == "v071-id1"))
    assert got.identify_status == "identified" and got.poster_path == "/p.jpg"
    old = db.scalar(select(Resource).where(Resource.file_id == "v071-old"))
    assert old.finish_status == "ended"
    # 显式传 publish_after_identify=False 的调用方仍绝不自动入队。
    # 注意：v0.8.5 起镜像链路改传 True（只推本轮识别成功影视），本用例锁的是
    # 「函数参数本身可控」，不再代表镜像链路的默认行为。
    assert db.query(QueueItem).count() == 0


def test_post_scan_identify_survives_tmdb_failure(db):
    """TMDB 未配置/401 不得阻断扫描链路（项目既定降级约定）。"""
    db.add(_res("v071-fail", "X.2024.mkv", identify_status="unidentified"))
    db.commit()

    async def boom(*a, **k):
        raise RuntimeError("TMDB 401")

    with patch("app.main.tmdb.identify", boom):
        stats = asyncio.run(_post_scan_identify(db, publish_after_identify=False))
    assert stats["identified"] == 0 and stats["failed"] == 1, stats


# ---------- 4. 对账批次保护（V2.0 12.2）----------
def test_reconcile_only_on_complete_full_scan(db):
    db.add(_res("v071-keep", "Keep.mkv"))
    db.add(_res("v071-gone", "Gone.mkv", share_id="sid-1", share_url="https://s/1",
                share_status="active"))
    db.commit()

    # ① 本轮没扫到 v071-gone，但批次不完整 / 断点续扫 → 一律不删
    for kw in (dict(scan_complete=False), dict(resumed=True)):
        r = asyncio.run(_reconcile_library(db, ["v071-keep"], **kw))
        assert r["skipped"], (kw, r)
    assert db.scalar(select(Resource).where(Resource.file_id == "v071-gone")) is not None

    # ② 空 seen 绝不删
    r = asyncio.run(_reconcile_library(db, [], scan_complete=True))
    assert r["skipped"] == "empty_seen"
    assert db.scalar(select(Resource).where(Resource.file_id == "v071-gone")) is not None

    # ③ 只有完整全量批次才真正清理（并取消分享）
    async def fake_delete(self, sid):
        return {}

    with patch("app.main.GuangYaShareClient.delete", fake_delete):
        r = asyncio.run(_reconcile_library(db, ["v071-keep"], scan_complete=True))
    assert r["removed"] == 1 and r["shares_cancelled"] == 1, r
    db.commit()
    assert db.scalar(select(Resource).where(Resource.file_id == "v071-gone")) is None
    assert db.scalar(select(Resource).where(Resource.file_id == "v071-keep")) is not None


def test_scan_marks_incomplete_batch_when_page_returns_short(db):
    """total=150 但第 2 页提前返回空：批次不完整，且对账必须被拒绝。"""
    from app.scanner import run_scan

    class ShortPageAPI:
        def __init__(self, db):
            pass

        async def list_files(self, parent, page, page_size):
            if page == 0:
                return {"list": [{"fileId": f"v071-pg{i}", "fileName": f"P{i}.mkv",
                                  "resType": 1} for i in range(100)], "total": 150}
            return {"list": [], "total": 150}

    with patch("app.scanner.GuangYaWebFiles", ShortPageAPI), \
         patch("app.scanner.GuangYaOpenAPI", ShortPageAPI):
        out = asyncio.run(run_scan("", resume=False, db=db))
    assert out["complete"] is False, out
    assert len(out["seen_file_ids"]) == 100
    r = asyncio.run(_reconcile_library(db, out["seen_file_ids"],
                                       scan_complete=out["complete"],
                                       resumed=out["resumed"]))
    assert r["skipped"] == "incomplete_pages", r


# ---------- 5. 重新扫描库端点 ----------
def test_rescan_library_returns_stats(db):
    scan_data = {"files": 2, "directories": 1, "resumed": False,
                 "complete": True, "seen_file_ids": ["a", "b"]}

    async def fake_scan(root="", resume=True, db=None):
        return scan_data

    async def fake_identify(db, publish_after_identify=True, limit=200):
        return {"identified": 1, "failed": 0, "queued": 0, "finish_backfilled": 1}

    async def fake_reconcile(db, seen, **kw):
        return {"removed": 1, "shares_cancelled": 1, "queue_cleared": 0}

    with patch("app.main.run_scan", fake_scan), \
         patch("app.main._post_scan_identify", fake_identify), \
         patch("app.main._reconcile_library", fake_reconcile):
        out = asyncio.run(rescan_library(auto_identify=True, reconcile=True, db=db))
    assert out["ok"] is True and out["files"] == 2, out
    assert out["reconcile"]["removed"] == 1, out
    assert out["identify"]["identified"] == 1, out
    assert "seen_file_ids" not in out, out
