"""阶段五 B-07：求片白名单采纳时 TMDB 元数据本地缓存，减少重复网络请求。

只覆盖 tgto.upsert_keyword 链路；失败结果不缓存（不固化暂时抖动）；配置变更清空
缓存立即生效；缓存有 TTL + 硬上限 GC，不内存泄漏。
"""
import asyncio
from unittest.mock import AsyncMock, patch

from app import tgto
from app.db import SessionLocal


def test_cache_hit_avoids_second_tmdb_call():
    tgto.clear_tmdb_meta_cache()
    calls = {"n": 0}

    async def fake_detail(tmdb_id, media_type, db=None):
        calls["n"] += 1
        return {"original_title": "Cached Title", "overview": "ov",
                "poster_path": "/p.jpg", "backdrop_path": "/b.jpg", "year": "2020"}

    with patch("app.tmdb.configured", return_value=True), \
         patch("app.tmdb.detail", new=AsyncMock(side_effect=fake_detail)), \
         patch("app.tmdb.poster_url", side_effect=lambda p, s: f"http://img/{s}/{p}"):
        with SessionLocal() as db:
            r1 = asyncio.run(tgto._tmdb_meta(db, 12345, "tv"))
            r2 = asyncio.run(tgto._tmdb_meta(db, 12345, "tv"))
    assert calls["n"] == 1, "同一 tmdb_id 第二次应命中缓存，不再请求 TMDB"
    assert r1 == r2
    assert r2["original_title"] == "Cached Title"
    tgto.clear_tmdb_meta_cache()


def test_failed_lookup_not_cached():
    tgto.clear_tmdb_meta_cache()
    calls = {"n": 0}

    async def flaky(tmdb_id, media_type, db=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("TMDB 临时故障")
        return {"original_title": "Recovered", "overview": "", "poster_path": "",
                "backdrop_path": "", "year": ""}

    with patch("app.tmdb.configured", return_value=True), \
         patch("app.tmdb.detail", new=AsyncMock(side_effect=flaky)):
        with SessionLocal() as db:
            r1 = asyncio.run(tgto._tmdb_meta(db, 99999, "movie"))
            r2 = asyncio.run(tgto._tmdb_meta(db, 99999, "movie"))
    assert calls["n"] == 2, "失败结果不缓存，下次仍可重试补全"
    assert r1["original_title"] == ""
    assert r2["original_title"] == "Recovered"
    tgto.clear_tmdb_meta_cache()


def test_clear_cache_config_change_takes_effect():
    tgto.clear_tmdb_meta_cache()
    with patch("app.tmdb.configured", return_value=True), \
         patch("app.tmdb.detail", new=AsyncMock(return_value={
             "original_title": "Old", "overview": "", "poster_path": "",
             "backdrop_path": "", "year": ""})):
        with SessionLocal() as db:
            asyncio.run(tgto._tmdb_meta(db, 55555, "tv"))
    assert tgto._tmdb_meta_cache_get("55555:tv") is not None
    tgto.clear_tmdb_meta_cache()
    assert tgto._tmdb_meta_cache_get("55555:tv") is None
    with patch("app.tmdb.configured", return_value=True), \
         patch("app.tmdb.detail", new=AsyncMock(return_value={
             "original_title": "New", "overview": "", "poster_path": "",
             "backdrop_path": "", "year": ""})):
        with SessionLocal() as db:
            r = asyncio.run(tgto._tmdb_meta(db, 55555, "tv"))
    assert r["original_title"] == "New", "配置变更清空后应立即用新值"
    tgto.clear_tmdb_meta_cache()


def test_cache_has_hard_cap_gc():
    tgto.clear_tmdb_meta_cache()
    for i in range(tgto._TMDB_META_MAX + 30):
        tgto._tmdb_meta_cache_put(f"key-{i}", {"original_title": "x"})
    assert len(tgto._tmdb_meta_cache) <= tgto._TMDB_META_MAX, "缓存必须有硬上限"
    tgto.clear_tmdb_meta_cache()
