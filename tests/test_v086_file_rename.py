"""v0.8.6 回归：B 库单集/单电影文件按英文剧名改名。

用户 2026-11-18 明确边界：
  1. 「影视文件的审核没通过」→ 要对剧集每一集、电影文件改名，把中文翻译为英文剧名；
  2. 「A 库仍然保持不动」；
  3. 「B 库影视文件夹还是保持之前的命名」（分类目录原名 1:1、主目录 `·` 约定不变）；
  4. 「别忘记，仍然需要和 A 库保持映射关系，便于后期对比变量更新」。

选项确认：英文剧名取 TMDB en-US 官方标题优先、AI 翻译兜底；
**只换中文片名段**（季集号/年份/画质/压制组/扩展名原样保留）；存量文件一键批量改名。

锁死的实现契约：
  - `MirrorFile.file_name` 永远是 A 库原名，改名结果只写 `target_name`；
  - 增量比对仍基于 `source_file_id + fingerprint(size:utime)`，与文件名无关；
  - 改名后 `_lookup_b_file()` / `backfill_target_file_ids()` 必须仍能反查 B fileId，
    否则下一轮会把已改名的文件误判不存在 → 重复复制；
  - `rename_file` 只对 **B 库 fileId** 调用，A 库与 B 库目录名一律不动。
"""
import asyncio
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from app import main, mirror
from app.db import SessionLocal, migrate
from app.file_naming import (english_file_name, sanitize_english_title,
                             with_conflict_suffix, work_core_title)
from app.models import AuditLog, MirrorFile, MirrorMedia, MirrorRule, Resource
from app.utils import normalize_work_name

ROOT = Path(__file__).resolve().parents[1]
A_ROOT, B_ROOT = "A_ROOT", "B_ROOT"
A_FILE_ID = "A_FILE_1"
B_FILE_ID = "B_FILE_1"
ZH_NAME = "大醉侠 第04集 2160p.WEB-DL.H.265.mkv"
EN_NAME = "The Drunk Hero 第04集 2160p.WEB-DL.H.265.mkv"
WORK = "大醉侠 (2002) {tmdb-135646}"
MIRROR_DIR_NAME = "大·醉·侠 (2002) {tmdb-135646}"

TABLES = [MirrorFile, MirrorMedia, MirrorRule, Resource, AuditLog]


def _clean():
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        db.commit()


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    _clean()
    yield
    _clean()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as session:
        try:
            yield session
        finally:
            session.rollback()


class FakeAPI:
    """只模拟 B 库列目录与改名；A 库从来不出现在任何写调用里。"""

    b_files = []
    renamed = []
    fail_ids = set()
    dir_writes = []

    def __init__(self, db):
        self.db = db

    @classmethod
    def reset(cls, files=None, fail_ids=None):
        cls.b_files = [dict(f) for f in (files if files is not None else [_b_file()])]
        cls.renamed = []
        cls.fail_ids = set(fail_ids or [])
        cls.dir_writes = []

    async def list_files(self, parent_id="", page=0, page_size=50):
        if page == 0:
            return {"list": [dict(f) for f in self.b_files], "total": len(self.b_files)}
        return {"list": [], "total": len(self.b_files)}

    async def rename_file(self, file_id, new_name):
        if str(file_id) in self.fail_ids:
            raise RuntimeError("网盘拒绝了该文件改名")
        self.renamed.append((file_id, new_name))
        for f in self.b_files:
            if str(f.get("fileId")) == str(file_id):
                f["fileName"] = new_name

    async def create_dir(self, parent_id, dir_name):
        self.dir_writes.append((parent_id, dir_name))
        raise AssertionError("改名链路绝不建目录")

    async def copy_file(self, file_ids, parent_id):
        raise AssertionError("改名链路绝不重新复制文件")


def _b_file(name=ZH_NAME, fid=B_FILE_ID, size=1024):
    return {"fileId": fid, "fileName": name, "fileSize": size, "resType": 1}


def _seed(db, *, rename_files=True, target_file_id=B_FILE_ID,
          file_name=ZH_NAME, status="synced", english_title=""):
    rule = MirrorRule(name="v086", source_root_id=A_ROOT, target_root_id=B_ROOT,
                      enabled=True, rename_files=rename_files,
                      file_idle_seconds=0.5, auto_identify=False)
    db.add(rule)
    db.flush()
    media = MirrorMedia(rule_id=rule.id, work_key="work:dazuixia",
                        title_standard=WORK, title_mirror=MIRROR_DIR_NAME,
                        source_dir_id="A_DIR", target_dir_id="B_DIR",
                        english_title=english_title, status="synced")
    db.add(media)
    db.flush()
    row = MirrorFile(media_id=media.id, rule_id=rule.id, identity="tmdb:135646:tv:1:4",
                     source_file_id=A_FILE_ID, target_file_id=target_file_id,
                     file_name=file_name, size=1024, utime=100,
                     fingerprint="1024:100", status=status)
    db.add(row)
    db.commit()
    return rule, media, row


@contextmanager
def _mocks(files=None, fail_ids=None, tmdb_title="The Drunk Hero", ai_en=""):
    FakeAPI.reset(files=files, fail_ids=fail_ids)
    if isinstance(tmdb_title, Exception):
        tmdb_mock = AsyncMock(side_effect=tmdb_title)
    else:
        tmdb_mock = AsyncMock(return_value=tmdb_title)
    ai_mock = AsyncMock(return_value={"ok": bool(ai_en), "titles": {"en": ai_en},
                                      "queries": [ai_en] if ai_en else []})
    sleep_mock = AsyncMock()
    with patch("app.mirror.GuangYaWebFiles", FakeAPI), \
         patch("app.mirror.tmdb.english_title", new=tmdb_mock), \
         patch("app.mirror.ai_assist.translate_title", new=ai_mock), \
         patch("app.mirror.asyncio.sleep", new=sleep_mock):
        yield SimpleNamespace(tmdb=tmdb_mock, ai=ai_mock, sleep=sleep_mock)


def _run(coro):
    return asyncio.run(coro)


# ---------- 1. 纯命名规则：只换中文片名段 ----------
def test_only_chinese_title_segment_is_replaced():
    assert english_file_name(ZH_NAME, "大醉侠", "The Drunk Hero") == EN_NAME
    assert english_file_name("大醉侠 (2002) 1080p.mkv", "大醉侠",
                             "The Drunk Hero") == "The Drunk Hero (2002) 1080p.mkv"
    # A 目录名无标记、文件名带 `·`：按最长变体吃掉整段片名，不能只替换第一个汉字
    assert english_file_name("大·醉·侠 (2002).mkv", "大醉侠",
                             "The Drunk Hero") == "The Drunk Hero (2002).mkv"
    # 中文季集描述按用户要求原样保留
    assert "第04集" in english_file_name(ZH_NAME, "大醉侠", "The Drunk Hero")


def test_ascii_and_invalid_titles_are_not_renamed():
    assert english_file_name("The.Silent.Storm.S01E04.mkv", "谷雨",
                             "The Silent Storm") == ""
    assert english_file_name(ZH_NAME, "大醉侠", "") == ""
    assert english_file_name(ZH_NAME, "大醉侠", "中文剧名") == ""
    assert sanitize_english_title("The Drunk Hero: Part 1") == "The Drunk Hero Part 1"
    assert sanitize_english_title("大醉侠") == ""


def test_work_core_title_strips_year_and_tags():
    assert work_core_title(WORK) == ("大醉侠", "2002")
    assert work_core_title("大·醉·侠 (2002) {tmdb-135646}") == ("大醉侠", "2002")
    assert with_conflict_suffix("A.mkv", 2) == "A-v2.mkv"


# ---------- 2. 主链路：TMDB 优先、映射保持 ----------
def test_tmdb_english_is_used_and_mapping_is_preserved(db):
    rule, media, row = _seed(db)
    before = (row.source_file_id, row.fingerprint, row.file_name)
    with _mocks() as m:
        out = _run(mirror.rename_mirror_files(db, rule))
    assert out["renamed"] == 1 and out["failed"] == 0
    assert FakeAPI.renamed == [(B_FILE_ID, EN_NAME)]
    db.expire_all()
    row2 = db.get(MirrorFile, row.id)
    assert row2.target_name == EN_NAME and row2.rename_status == "done"
    # 这三个字段是 A→B 映射与增量比对的锚点，改名一个都不许动
    assert (row2.source_file_id, row2.fingerprint, row2.file_name) == before
    media2 = db.get(MirrorMedia, media.id)
    assert media2.english_title == "The Drunk Hero" and media2.english_source == "tmdb"
    assert m.tmdb.await_count == 1 and m.ai.await_count == 0


def test_ai_fallback_when_tmdb_unavailable(db):
    rule, media, row = _seed(db)
    with _mocks(tmdb_title=RuntimeError("TMDB 401"), ai_en="Drunk Hero AI") as m:
        out = _run(mirror.rename_mirror_files(db, rule))
    assert out["renamed"] == 1
    assert m.ai.await_count == 1
    db.expire_all()
    assert db.get(MirrorMedia, media.id).english_source == "ai"
    assert db.get(MirrorFile, row.id).target_name.startswith("Drunk Hero AI")


def test_no_english_title_uses_local_transliteration(db):
    """v0.8.9 用户口径：TMDB 查不到英文名就直接直译，不许卡住、不许跳过主链路。

    v0.8.6~v0.8.8 的行为是「列入待翻译、整部跳过」，实测会让剧集卡在 TMDB
    等待上、后台管理页打不开。现在 AI 不可用时还有本地拼音兜底，保证必有剧名。
    """
    rule, media, row = _seed(db)
    with _mocks(tmdb_title="", ai_en=""):
        out = _run(mirror.rename_mirror_files(db, rule))
    assert out["renamed"] == 1 and out["untranslated"] == 0, out
    assert out["literal_pinyin"] == 1, out
    assert FakeAPI.renamed and FakeAPI.renamed[0][1].startswith("Da Zui Xia")
    db.expire_all()
    assert db.get(MirrorMedia, media.id).english_source == "pinyin"
    assert db.get(MirrorFile, row.id).target_name.startswith("Da Zui Xia")
    # A 原名仍是映射锚点，直译只改 B 库文件名
    assert db.get(MirrorFile, row.id).file_name == ZH_NAME


def test_rename_is_idempotent(db):
    rule, media, row = _seed(db)
    with _mocks() as m:
        first = _run(mirror.rename_mirror_files(db, rule))
        FakeAPI.reset(files=[_b_file(EN_NAME)])
        second = _run(mirror.rename_mirror_files(db, rule))
    assert first["renamed"] == 1
    assert second["renamed"] == 0 and second["skipped"] == 1
    assert m.tmdb.await_count == 1  # 英文名按作品缓存，不逐集重复打 TMDB


# ---------- 3. 改名后仍能反查 B fileId（防重复复制、保映射） ----------
def test_renamed_file_can_still_be_looked_up_in_b(db):
    _rule, _media, row = _seed(db, target_file_id="")
    row.target_name = EN_NAME
    db.commit()
    key = normalize_work_name(EN_NAME)
    candidates = {f"{key}:1024": B_FILE_ID, key: B_FILE_ID}
    assert mirror._lookup_b_file(candidates, row) == B_FILE_ID
    # 只按 A 原名查是查不到的，这正是旧逻辑会重复复制的根因
    assert normalize_work_name(row.file_name) not in candidates
    assert mirror._b_names(row) == [EN_NAME, ZH_NAME]


def test_backfill_finds_renamed_file_without_duplicate_copy(db):
    rule, media, row = _seed(db, target_file_id="", status="copying")
    row.target_name = EN_NAME
    db.commit()
    with _mocks(files=[_b_file(EN_NAME)]):
        n = _run(mirror.backfill_target_file_ids(db, rule))
    assert n == 1
    db.expire_all()
    row2 = db.get(MirrorFile, row.id)
    assert row2.target_file_id == B_FILE_ID and row2.status == "synced"
    assert row2.file_name == ZH_NAME  # A 原名仍是映射锚点


# ---------- 4. 失败隔离、冲突与风控 ----------
def test_rename_failure_is_isolated_and_persisted(db):
    rule, media, row = _seed(db)
    row2 = MirrorFile(media_id=media.id, rule_id=rule.id, identity="tmdb:135646:tv:1:5",
                      source_file_id="A_FILE_2", target_file_id="B_FILE_2",
                      file_name="大醉侠 第05集 1080p.mkv", size=2048, utime=200,
                      fingerprint="2048:200", status="synced")
    db.add(row2)
    db.commit()
    files = [_b_file(), _b_file("大醉侠 第05集 1080p.mkv", "B_FILE_2", 2048)]
    with _mocks(files=files, fail_ids={B_FILE_ID}):
        out = _run(mirror.rename_mirror_files(db, rule))
    assert out["renamed"] == 1 and out["failed"] == 1
    assert len(out["errors"]) == 1
    db.expire_all()
    assert db.get(MirrorFile, row.id).rename_status == "failed"
    assert "网盘拒绝" in db.get(MirrorFile, row.id).rename_error
    assert db.get(MirrorFile, row2.id).rename_status == "done"
    assert db.get(MirrorFile, row.id).file_name == ZH_NAME


def test_conflict_gets_ascii_suffix(db):
    """A 库内容更新后新旧版本目标名相同：加 ASCII 序号，不覆盖别人、不引入中文。"""
    rule, media, row = _seed(db)
    files = [_b_file(EN_NAME, "B_OLD", 1024), _b_file()]
    with _mocks(files=files):
        out = _run(mirror.rename_mirror_files(db, rule))
    assert out["renamed"] == 1
    assert FakeAPI.renamed[0][1] == "The Drunk Hero 第04集 2160p.WEB-DL.H.265-v2.mkv"


def test_risk_idle_between_file_renames(db):
    rule, media, row = _seed(db)
    row2 = MirrorFile(media_id=media.id, rule_id=rule.id, identity="tmdb:135646:tv:1:6",
                      source_file_id="A_FILE_3", target_file_id="B_FILE_3",
                      file_name="大醉侠 第06集 1080p.mkv", size=3072, utime=300,
                      fingerprint="3072:300", status="synced")
    db.add(row2)
    db.commit()
    files = [_b_file(), _b_file("大醉侠 第06集 1080p.mkv", "B_FILE_3", 3072)]
    with _mocks(files=files) as m:
        out = _run(mirror.rename_mirror_files(db, rule))
    # v1.4：并发默认开（3），2 个文件落在同一窗口并发发出，窗口内不再逐文件 sleep。
    # idle 只在「窗口之间」留，N 文件从 N-1 次等待降为 ceil(N/并发)-1 次。
    assert out["renamed"] == 2
    assert m.sleep.await_count == 0, "同窗口内并发改名不应逐文件等待"
    # 风控下限仍守住（若真有窗口间等待，间隔不得低于下限）
    assert mirror.MIN_RENAME_IDLE_SECONDS == 0.2


# ---------- 5. 边界：A 库只读、目录名不变、开关与手动入口 ----------
def test_a_library_and_directory_names_never_touched(db):
    rule, media, row = _seed(db)
    with _mocks():
        _run(mirror.rename_mirror_files(db, rule))
    assert all(fid != A_FILE_ID for fid, _ in FakeAPI.renamed)
    assert FakeAPI.dir_writes == []
    db.expire_all()
    assert db.get(MirrorMedia, media.id).title_mirror == MIRROR_DIR_NAME
    assert db.get(MirrorMedia, media.id).source_dir_id == "A_DIR"
    assert db.get(MirrorMedia, media.id).target_dir_id == "B_DIR"
    assert db.get(MirrorFile, row.id).file_name == ZH_NAME


def test_manual_rename_endpoint_works_even_when_switch_off(db):
    rule, media, row = _seed(db, rename_files=False)
    with _mocks():
        out = _run(main.mirror_rename_files(force=False, db=db))
    assert out["renamed"] == 1
    assert FakeAPI.renamed[0][0] == B_FILE_ID


def test_run_respects_switch_and_status_exposes_state(db):
    rule, media, row = _seed(db, rename_files=False)
    with _mocks(), \
         patch("app.main.mirror.sync_rule", new=AsyncMock(return_value={"media": 1})), \
         patch("app.main.mirror.backfill_target_file_ids", new=AsyncMock(return_value=0)), \
         patch("app.main.mirror.rename_mirror_files",
               new=AsyncMock(return_value={"renamed": 9})) as rn, \
         patch("app.main._mirror_post_sync_pipeline", new=AsyncMock(return_value={})):
        out = _run(main.mirror_run(db=db))
    assert out["rename_enabled"] is False and "rename" not in out
    assert rn.await_count == 0
    st = main.mirror_status(db=db)
    assert st["config"]["rename_files"] is False
    assert "files_renamed" in st["counts"] and "works_without_english" in st["counts"]
    assert isinstance(st["rename_errors"], list)
    assert isinstance(st["untranslated_works"], list)
    assert any("rename" in e for e in mirror.CONFIRMED_WRITE_ENDPOINTS)
    assert main.MirrorConfigInput().rename_files is True


def test_rename_exception_never_blocks_sync_pipeline(db):
    """v2.1.0 改名硬门禁：整体异常不阻断已完成的复制，但阻断后续入库 / 推送。

    用户决策（2026-11）：改名失败即视为单轮镜像任务未完成，英文名没改对前绝不
    先推给群 / 频道。复制照常落库；rename_gate_blocked=True 时跳过 post 管线。
    """
    rule, media, row = _seed(db)
    with patch("app.main.mirror.sync_rule", new=AsyncMock(return_value={"media": 1})), \
         patch("app.main.mirror.backfill_target_file_ids", new=AsyncMock(return_value=0)), \
         patch("app.main.mirror.rename_mirror_files",
               new=AsyncMock(side_effect=RuntimeError("列 B 库超时"))), \
         patch("app.main._mirror_post_sync_pipeline",
               new=AsyncMock(return_value={"files": 3})) as pipe:
        out = _run(main.mirror_run(post_identify=True, db=db))
    assert out["rename_enabled"] is True
    assert out["rename"]["errors"] and "列 B 库超时" in out["rename"]["errors"][0]
    # 复制已完成、管线意图开启，但改名硬门禁拦截：不进 post 管线
    assert out["pipeline_enabled"] is True
    assert out.get("rename_gate_blocked") is True
    assert pipe.await_count == 0
    assert "pipeline" not in out


def test_scheduled_rename_exception_never_blocks_pipeline(db):
    """v2.1.0 定时镜像改名硬门禁：与手动路径同口径，改名失败跳过入库推送。"""
    rule, media, row = _seed(db, rename_files=True)
    rule.enabled = True
    rule.auto_identify = True
    db.commit()
    with patch("app.main.mirror.sync_rule", new=AsyncMock(return_value={"media": 1})), \
         patch("app.main.mirror.backfill_target_file_ids", new=AsyncMock(return_value=0)), \
         patch("app.main.mirror.rename_mirror_files",
               new=AsyncMock(side_effect=RuntimeError("TMDB 网关异常"))), \
         patch("app.main._mirror_post_sync_pipeline",
               new=AsyncMock(return_value={"files": 2, "identify": {"identified": 1},
                                           "queued": 1, "changes_pending": 0})) as pipe:
        _run(main._scheduled_mirror())
    assert pipe.await_count == 0


def test_version_and_ui_contract():
    pkg = json.loads((ROOT / "frontend/package.json").read_text(encoding="utf-8"))
    v = pkg["version"]
    assert main.app.version == v and main.health()["version"] == v
    vue = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert f"v{v} · Asia/Shanghai" in vue
    assert "renameMirrorFiles(false)" in vue and "renameMirrorFiles(true)" in vue
    assert "v-model=\"mirrorCfg.rename_files\"" in vue
    assert "按英文规范改名" in vue
    assert "英文改名待处理" in vue
