"""v0.8.9 回归：TMDB 找不到英文名不再卡死，直接直译；长任务不锁死管理页。

用户 2026-11-18 两条实测反馈：
  - 「整理剧集，修改名称的过程中，后台管理页面卡死，打不开了」
  - 「在 tmdb 上找不到英文名称的剧集卡死了；找不到英文名称的剧集直接使用直译」

三个叠加根因（本文件逐条锁死）：
  ① `_resolve_english_title()` 对 media_type 未知的作品串行查 TMDB movie + tv
     （httpx 超时 20 秒/次）再等 AI，一部剧就能卡几十秒；旧版还把「查不到」
     空缓存每轮重查，于是「第一次不成功、多点几次才成功」。
  ② `mirror.py` 的改名 / 复制循环用 `db.flush()` 却不提交，`await` 网络等待期间
     SQLite 写事务一直持有；v0.8.8 只在 `scanner.run_scan()` 修过同类问题。
     再叠加 30 秒 busy_timeout，管理页所有写请求排队 → 页面卡死。
  ③ 重复点击「立即同步」会并发跑多个分钟级长任务，互相抢写锁、放大请求。

v0.8.9：TMDB 单次 6 秒硬超时 + 只查一次 → AI 直译（10 秒硬超时）→ 本地拼音
兜底，保证必有 ASCII 剧名；所有写操作改 `db.commit()`；三个长任务入口共用互斥锁。
"""
import asyncio
import json
import sqlite3
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from app import main, mirror
from app.db import SessionLocal, engine, migrate
from app.file_naming import pinyin_title
from app.models import (AuditLog, MirrorFile, MirrorMedia, MirrorRule,
                        Resource, ResourceScanBatch)
from app.utils import normalize_work_name

ROOT = Path(__file__).resolve().parents[1]
A_ROOT, B_ROOT = "A_ROOT", "B_ROOT"
A_WORK, A_FILE, B_WORK, B_FILE = "A_WORK", "A_FILE", "B_WORK", "BF1"
WORK = "大醉侠 (2002) {tmdb-135646}"
MIRROR_DIR = "大·醉·侠 (2002) {tmdb-135646}"
ZH_NAME = "大醉侠 第04集 2160p.WEB-DL.H.265.mkv"

TABLES = [MirrorFile, MirrorMedia, MirrorRule, Resource, AuditLog, ResourceScanBatch]


def _clean():
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        db.commit()


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    _clean()
    ProbeAPI.reset()
    yield
    _clean()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as session:
        try:
            yield session
        finally:
            session.rollback()


def _run(coro):
    return asyncio.run(coro)


def _noop_sleep():
    async def _sleep(*_a, **_kw):
        return None
    return _sleep


def _b_file(name=ZH_NAME):
    return {"fileId": B_FILE, "fileName": name, "fileSize": 1024, "resType": 1}


def _write_lock_available() -> bool:
    """独立连接尝试拿 SQLite 写锁：主 session 若跨 await 持事务，这里 0.2 秒即失败。"""
    path = engine.url.database
    conn = sqlite3.connect(path, timeout=0.2)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.rollback()
        return True
    except sqlite3.OperationalError:
        return False
    finally:
        conn.close()


class ProbeAPI:
    """改名网络 await 期间检测主 session 是否仍占着写锁。"""

    b_files = []
    renamed = []
    lock_results = []

    def __init__(self, db):
        self.db = db

    @classmethod
    def reset(cls):
        cls.b_files = [_b_file()]
        cls.renamed = []
        cls.lock_results = []

    async def list_files(self, parent_id="", page=0, page_size=50):
        if page == 0:
            return {"list": [dict(f) for f in self.b_files], "total": len(self.b_files)}
        return {"list": [], "total": len(self.b_files)}

    async def rename_file(self, file_id, new_name):
        self.lock_results.append(_write_lock_available())
        self.renamed.append((file_id, new_name))
        for f in self.b_files:
            if str(f.get("fileId")) == str(file_id):
                f["fileName"] = new_name


def _seed(db, english_title="", english_source=""):
    rule = MirrorRule(name="v089", source_root_id=A_ROOT, target_root_id=B_ROOT,
                      enabled=True, star_naming=True, auto_identify=False,
                      rename_files=True, file_idle_seconds=0.5, auto_create_dirs=True)
    db.add(rule)
    db.commit()
    media = MirrorMedia(rule_id=rule.id,
                        work_key="work:" + normalize_work_name(WORK),
                        title_standard=WORK, title_mirror=MIRROR_DIR,
                        source_dir_id=A_WORK, target_dir_id=B_WORK,
                        english_title=english_title, english_source=english_source,
                        status="synced")
    db.add(media)
    db.commit()
    row = MirrorFile(media_id=media.id, rule_id=rule.id,
                     identity="tmdb:135646:tv:1:4", source_file_id=A_FILE,
                     target_file_id=B_FILE, file_name=ZH_NAME,
                     size=1024, utime=1, fingerprint="1024:1", status="synced")
    db.add(row)
    db.commit()
    return rule, media, row


def _resolve_mocks(tmdb_value="", ai_en="", *, tmdb_side=None, ai_side=None):
    tmdb_mock = AsyncMock(return_value=tmdb_value, side_effect=tmdb_side)
    ai_mock = AsyncMock(return_value={"ok": bool(ai_en), "titles": {"en": ai_en},
                                      "queries": [ai_en] if ai_en else []},
                        side_effect=ai_side)
    return (patch("app.mirror.GuangYaWebFiles", ProbeAPI),
            patch("app.mirror.tmdb.english_title", new=tmdb_mock),
            patch("app.mirror.ai_assist.translate_title", new=ai_mock),
            patch("app.mirror.asyncio.sleep", new=_noop_sleep()),
            tmdb_mock, ai_mock)


class _Bundle:
    def __init__(self, patches):
        self.patches = patches

    def __enter__(self):
        for p in self.patches:
            p.start()
        return self

    def __exit__(self, *a):
        for p in reversed(self.patches):
            p.stop()


# ---------- 1. 零网络直译兜底 ----------
def test_pinyin_title_is_ascii_and_keeps_chinese_only():
    assert pinyin_title("大醉侠") == "Da Zui Xia"
    assert pinyin_title("侠客行 (2001)") == "Xia Ke Xing 2001"
    assert pinyin_title("Already English") == ""
    assert pinyin_title("") == ""


def test_tmdb_miss_queries_only_once():
    """旧版对 media_type 未知会串行查 movie + tv 两次（最多 40 秒）。"""
    db = SessionLocal()
    try:
        rule, media, _row = _seed(db)
        p = _resolve_mocks(tmdb_value="", ai_en="")
        with _Bundle(p[:3]):
            eng, source = _run(mirror._resolve_english_title(db, media))
        assert eng.startswith("Da Zui Xia") and source == "pinyin", (eng, source)
        assert p[4].await_count == 1, p[4].await_count
    finally:
        db.rollback()
        db.close()


def test_tmdb_timeout_falls_back_instead_of_hanging():
    """TMDB 卡住时必须在硬上限内放弃，立刻直译，绝不拖住整轮同步。"""
    db = SessionLocal()
    try:
        rule, media, _row = _seed(db)

        async def _slow(*_a, **_kw):
            await asyncio.sleep(2)
            return ""

        p = _resolve_mocks(ai_en="", tmdb_side=_slow)
        # 这里刻意不 patch asyncio.sleep：wait_for 要靠真实事件循环触发超时
        with _Bundle(p[:3]), patch.object(mirror, "ENGLISH_TITLE_TMDB_TIMEOUT", 0.05):
            eng, source = _run(mirror._resolve_english_title(db, media))
        assert eng.startswith("Da Zui Xia") and source == "pinyin", (eng, source)
    finally:
        db.rollback()
        db.close()


def test_ai_translation_wins_over_pinyin():
    """用户要的「直接使用直译」：AI 可用时优先 AI，拼音只作最后一层。"""
    db = SessionLocal()
    try:
        rule, media, _row = _seed(db)
        p = _resolve_mocks(tmdb_value="", ai_en="The Drunk Hero")
        with _Bundle(p[:3]):
            eng, source = _run(mirror._resolve_english_title(db, media))
        assert eng == "The Drunk Hero" and source == "ai", (eng, source)
        assert p[5].await_count == 1
    finally:
        db.rollback()
        db.close()


def test_no_official_name_renames_and_reports_literal_sources(db):
    """TMDB 查不到 → 直译生效 → 改名/入库/分享主链路照常，不再整部跳过。"""
    rule, media, row = _seed(db)
    p = _resolve_mocks(tmdb_value="", ai_en="")
    with _Bundle(p[:3]):
        out = _run(mirror.rename_mirror_files(db, rule))
    assert out["renamed"] == 1 and out["untranslated"] == 0, out
    assert out["literal_pinyin"] == 1, out
    assert ProbeAPI.renamed[0][1].startswith("Da Zui Xia")
    db.expire_all()
    assert db.get(MirrorFile, row.id).file_name == ZH_NAME  # A 原名锚点不变


# ---------- 2. 改名期间绝不占着 SQLite 写锁 ----------
def test_write_lock_is_free_during_rename_await(db):
    """每个 `await rename_file` 时独立连接必须能立刻拿到写锁。

    若把 `db.commit()` 改回 `db.flush()`，主 session 会跨 await 持有写事务，
    这里会立刻失败——正是「后台管理页卡死打不开」的根因。
    """
    rule, media, _row = _seed(db)
    p = _resolve_mocks(tmdb_value="The Drunk Hero")
    with _Bundle(p[:3]):
        out = _run(mirror.rename_mirror_files(db, rule))
    assert out["renamed"] == 1, out
    assert ProbeAPI.lock_results and all(ProbeAPI.lock_results), ProbeAPI.lock_results


def test_mirror_module_never_flushes():
    """红线：mirror.py 里不允许再出现 db.flush()（会重新引入跨 await 写事务）。"""
    src = (ROOT / "backend/app/mirror.py").read_text(encoding="utf-8")
    assert "db.flush()" not in src


# ---------- 3. 互斥锁：不许并发长任务 ----------
def test_mirror_guard_rejects_concurrent_call():
    with main._mirror_guard():
        with pytest.raises(HTTPException) as ei:
            with main._mirror_guard():
                pass
        assert ei.value.status_code == 409
        assert "已有镜像同步" in str(ei.value.detail)
    # 释放后锁可重新获取，避免异常路径把锁永久占住
    with main._mirror_guard():
        pass


def test_all_long_entrypoints_are_guarded():
    src = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
    # run / rename / tasks poll / mappings prune / media clear-invalid(v0.9.8)：
    # 任何分钟级长任务或批量删映射都必须共用同一把锁
    assert src.count("with _mirror_guard():") == 5
    assert "_mirror_lock.acquire(blocking=False)" in src  # 定时同步跳过而非排队
    assert "_mirror_lock.release()" in src


# ---------- 4. 改名异常绝不阻断入库 / 分享 / 推送 ----------
# ---------- 5. v0.8.9.1 回归：中文名改英文名能力丢了的三个根因 ----------
def _seed_no_mark(db, identity="tmdb:206484:tv:S1:E4", work_title="侠客行 (2001)"):
    """A 库目录名**不带** {tmdb-} 标记——现场只配 TMDB 的真实形态。"""
    rule = MirrorRule(name="v0891", source_root_id=A_ROOT, target_root_id=B_ROOT,
                      enabled=True, star_naming=True, auto_identify=False,
                      rename_files=True, file_idle_seconds=0.5, auto_create_dirs=True)
    db.add(rule)
    db.commit()
    media = MirrorMedia(rule_id=rule.id,
                        work_key="work:" + normalize_work_name(work_title),
                        title_standard=work_title, title_mirror="侠客行 (2001)",
                        source_dir_id=A_WORK, target_dir_id=B_WORK,
                        english_title="", status="synced")  # media_type 默认空
    db.add(media)
    db.commit()
    row = MirrorFile(media_id=media.id, rule_id=rule.id, identity=identity,
                     source_file_id=A_FILE, target_file_id=B_FILE, file_name=ZH_NAME,
                     size=1024, utime=1, fingerprint="1024:1", status="synced")
    db.add(row)
    db.commit()
    return rule, media, row


def test_identity_supplies_tmdb_id_when_dir_has_no_mark(db):
    """A 目录无 {tmdb-} 标记时，从 MirrorFile.identity 补 tid，绝不跳过 TMDB。"""
    rule, media, _row = _seed_no_mark(db, identity="tmdb:206484:tv:S1:E4")
    p = _resolve_mocks(tmdb_value="The Legend of the Condor Heroes", ai_en="")
    with _Bundle(p[:3]):
        eng, source = _run(mirror._resolve_english_title(db, media))
    assert eng == "The Legend of the Condor Heroes" and source == "tmdb", (eng, source)
    assert p[4].await_count >= 1, "TMDB 必须真的被调用"


def test_identity_supplies_tv_type_so_tv_endpoint_is_queried(db):
    """剧集从 identity(:tv:) 取类型，优先查 /tv/{id}，修复 v0.8.9 一律 movie 的回归。"""
    rule, media, _row = _seed_no_mark(db, identity="tmdb:206484:tv:S1:E4")

    async def _tmdb(tid, mt, _db):
        return "TV Title" if mt == "tv" else ""

    p = _resolve_mocks(tmdb_value="", ai_en="")
    with patch("app.mirror.GuangYaWebFiles", ProbeAPI), \
         patch("app.mirror.tmdb.english_title", new=_tmdb), \
         patch("app.mirror.ai_assist.translate_title", new=p[3]):
        eng, source = _run(mirror._resolve_english_title(db, media))
    assert eng == "TV Title" and source == "tmdb", (eng, source)


def test_known_tid_unknown_type_queries_both(db):
    """目录带 tid 但类型未知：movie 查不到时补查 tv，仍能拿到剧集英文名。"""
    rule, media, _row = _seed_no_mark(db, identity="raw:fileid:no:type")
    media.english_title = ""
    db.commit()
    # 给 A 目录名补 tmdb 标记，让 tid 有值、mtype 仍空
    media.title_standard = "侠客行 (2001) {tmdb-206484}"
    db.commit()
    calls = []

    async def _tmdb(tid, mt, _db):
        calls.append(mt)
        return "The Legend of the Condor Heroes" if mt == "tv" else ""

    p = _resolve_mocks(tmdb_value="", ai_en="")
    with patch("app.mirror.GuangYaWebFiles", ProbeAPI), \
         patch("app.mirror.tmdb.english_title", new=_tmdb), \
         patch("app.mirror.ai_assist.translate_title", new=p[3]):
        eng, source = _run(mirror._resolve_english_title(db, media))
    assert "movie" in calls and "tv" in calls, calls
    assert eng == "The Legend of the Condor Heroes" and source == "tmdb", (eng, source)


def test_rename_failure_does_not_block_post_sync_pipeline(db):
    _rule, _media, _row = _seed(db)
    pipeline = AsyncMock(return_value={"files": 1, "identify": {"identified": 1},
                                       "queued": 1, "changes_pending": 0})
    with patch("app.main.mirror.sync_rule", new=AsyncMock(return_value={"media": 1})), \
         patch("app.main.mirror.backfill_target_file_ids", new=AsyncMock(return_value=0)), \
         patch("app.mirror.rename_mirror_files",
               new=AsyncMock(side_effect=RuntimeError("TMDB 网关整体不可用"))), \
         patch("app.main._mirror_post_sync_pipeline", new=pipeline):
        out = _run(main.mirror_run(post_identify=True, db=db))
    assert out["rename"]["errors"], out
    assert pipeline.await_count == 1
    assert out["pipeline"]["files"] == 1
