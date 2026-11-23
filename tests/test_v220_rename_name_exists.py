"""v2.1.10 验收：光鸭 code=160「文件名已存」的幂等收敛 + 状态一致性。

真机取证（2026-11-23）：63 条 code160 集中在大唐双龙传(25) / 大军师司马懿(22) /
大唐迷雾(16) 三部剧，全是中文名 → 英文名改名。同一��剧「间隔式」成败：
部分集 done、部分集 160。用户判断「不是真错误，是重复改名——B 库里英文名已经
存在了，多数其实已经改名成功，只是 DB 状态没跟上」。

本文件锁死修复后的行为：
  1. 160 与 127 严格区分（160 不可退避重试——重试仍是占用）；
  2. 160 命中且 B 库当前名就是目标名 → **幂等收敛**为完成态（不标失败、
     不计入熔断、清掉两个错误位）；
  3. 160 命中但目标名被别的文件占用 → 真冲突，标「待下轮重试」，同样不标失败；
  4. 判定用的列目录**按窗口批量查一次**（63 条 160 不能变 63 次递归列目录）；
  5. 准备阶段幂等闸：DB 记的目标名已经是最终名且查不到实际名 → 不发重复请求；
  6. 「已完成」只有一个落库入口，清掉 rename_error 与 error，杜绝
     「rename_status=done 却挂着 code=160 错误」这种自相矛盾状态；
  7. 一次性迁移：修掉历史矛盾状态，把 160 假失败退回待处理。
"""
import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

ROOT = Path(__file__).resolve().parents[1]
A_ROOT, B_ROOT = "A_ROOT", "B_ROOT"
A_WORK, B_WORK = "A_WORK", "B_WORK"
WORK = "大醉侠 (2002) {tmdb-135646}"
ZH_NAME = "大醉侠 第04集 2160p.WEB-DL.H.265.mkv"
ENG = "The Drunk Hero"
TARGET_EN = "The Drunk Hero 第04集 2160p.WEB-DL.H.265.mkv"
B_FILE = "BF1"


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v220a_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'd.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


class FakeAPI:
    """假光鸭客户端。

    `apply_then_160=True`  → 改名**已经生效**（文件真名被改成目标名），
    但接口回报「文件名已存」。这正是真机上的那类「重复改名」：
    文件其实早已改名成功，DB 记录没跟上。
    `apply_then_160=False` → 改名没生效，160 是真冲突（名字被别的文件占着）。
    """

    b_files: list = []
    rename_calls: list = []
    list_calls = 0
    apply_then_160 = False

    def __init__(self, db):
        self.db = db

    @classmethod
    def reset(cls, files=None, *, apply_then_160=False):
        cls.b_files = [dict(f) for f in (files or [])]
        cls.rename_calls = []
        cls.list_calls = 0
        cls.apply_then_160 = apply_then_160

    async def list_files(self, parent_id="", page=0, page_size=50):
        type(self).list_calls += 1
        if page == 0:
            return {"list": [dict(f) for f in self.b_files],
                    "total": len(self.b_files)}
        return {"list": [], "total": len(self.b_files)}

    async def rename_file(self, file_id, new_name):
        from app import guangya
        type(self).rename_calls.append((file_id, new_name))
        if type(self).apply_then_160:
            # 改名其实已经生效，然后才回报「文件名已存」
            for f in type(self).b_files:
                if str(f.get("fileId")) == str(file_id):
                    f["fileName"] = new_name
            raise guangya.WebFileError("网页端 rename 失败 code=160 msg=文件名已存", 160)
        raise guangya.WebFileError("网页端 rename 失败 code=160 msg=文件名已存", 160)


def _noop_sleep():
    async def _sleep(*_a, **_kw):
        return None
    return _sleep


def _seed(db, *, file_name=ZH_NAME, target_name="", rename_status="",
          rename_error="", error="", size=1024):
    from app.models import MirrorFile, MirrorMedia, MirrorRule
    from app.utils import normalize_work_name
    rule = MirrorRule(name="v220a", source_root_id=A_ROOT, target_root_id=B_ROOT,
                      enabled=True, star_naming=True, auto_identify=False,
                      rename_files=True, file_idle_seconds=0.5, auto_create_dirs=True)
    db.add(rule)
    db.commit()
    media = MirrorMedia(rule_id=rule.id,
                        work_key="work:" + normalize_work_name(WORK),
                        title_standard=WORK, title_mirror="大·醉·侠 (2002) {tmdb-135646}",
                        source_dir_id=A_WORK, target_dir_id=B_WORK,
                        english_title=ENG, english_source="tmdb", status="synced")
    db.add(media)
    db.commit()
    row = MirrorFile(media_id=media.id, rule_id=rule.id,
                     identity="tmdb:135646:tv:1:4", source_file_id="AF1",
                     target_file_id=B_FILE, file_name=file_name,
                     target_name=target_name,
                     size=size, utime=1, fingerprint=f"{size}:1", status="synced",
                     rename_status=rename_status, rename_error=rename_error,
                     error=error)
    db.add(row)
    db.commit()
    return rule, media, row


def _run_rename(db, rule):
    from app import mirror
    tmdb_mock = AsyncMock(return_value="")
    ai_mock = AsyncMock(return_value={"ok": False, "titles": {}, "queries": []})
    with patch("app.mirror.GuangYaWebFiles", FakeAPI), \
            patch("app.mirror.tmdb.english_title", new=tmdb_mock), \
            patch("app.mirror.ai_assist.translate_title", new=ai_mock), \
            patch("app.mirror.asyncio.sleep", new=_noop_sleep()):
        return asyncio.run(mirror.rename_mirror_files(db, rule))


# ---------- 1. 160 与 127 严格区分 ----------

def test_name_exists_detection_is_strictly_separate_from_rate_limit():
    from app import guangya, mirror
    # 160 的识别
    assert mirror._is_name_exists_error(guangya.WebFileError("code=160", 160))
    assert mirror._is_name_exists_error(RuntimeError("code=160 msg=文件名已存"))
    assert mirror._is_name_exists_error(RuntimeError("文件名已存在"))
    assert mirror._is_name_exists_error(RuntimeError("name already exists"))
    # 127 不是 160
    assert not mirror._is_name_exists_error(guangya.WebFileError("code=127", 127))
    assert not mirror._is_name_exists_error(RuntimeError("too many requests"))
    assert not mirror._is_name_exists_error(RuntimeError("database is locked"))
    # 160 也不是限流（否则会被拿去退避重试，重试仍是占用，毫无意义）
    assert not mirror._is_rate_limit_error(guangya.WebFileError("code=160", 160))
    assert not mirror._is_rate_limit_error(RuntimeError("文件名已存"))


def test_rename_one_does_not_retry_160():
    """160 不可重试：只发一次请求，带独立标记，且不带限流标记。"""
    from app import guangya, mirror

    calls = {"n": 0}

    class API:
        async def rename_file(self, fid, final):
            calls["n"] += 1
            raise guangya.WebFileError("code=160 msg=文件名已存", 160)

    delays = []
    real_sleep = asyncio.sleep

    async def fake_sleep(d):
        delays.append(d)
        await real_sleep(0)

    with patch.object(mirror.asyncio, "sleep", fake_sleep):
        ok, err = asyncio.run(mirror._rename_one_with_backoff(API(), "F1", "Ep01"))

    assert ok is False
    assert calls["n"] == 1, "160 不可重试（重试仍是占用）"
    assert delays == [], "160 不该退避"
    assert err.startswith(mirror.RENAME_NAME_EXISTS_MARK)
    assert mirror.RENAME_RATE_LIMIT_MARK not in err


def test_rename_one_still_retries_127():
    """回归保护：127 限流仍走指数退避并最终成功。"""
    from app import guangya, mirror

    calls = {"n": 0}

    class API:
        async def rename_file(self, fid, final):
            calls["n"] += 1
            if calls["n"] <= 2:
                raise guangya.WebFileError("code=127 msg=操作过于频繁", 127)
            return None

    delays = []
    real_sleep = asyncio.sleep

    async def fake_sleep(d):
        delays.append(d)
        await real_sleep(0)

    with patch.object(mirror.asyncio, "sleep", fake_sleep):
        ok, err = asyncio.run(mirror._rename_one_with_backoff(API(), "F1", "Ep01"))

    assert ok is True and err == ""
    assert calls["n"] == 3
    assert delays == [1.0, 2.0]


# ---------- 2. 160 幂等收敛为完成态 ----------

def test_160_converges_to_done_and_clears_error(env):
    """改名其实早已生效（网盘回报 160）→ 收敛为完成态，清掉两个错误位。"""
    S, _ = env
    from app.models import MirrorFile
    with S() as db:
        rule, _media, row = _seed(
            db, rename_status="failed",
            rename_error="网页端 rename 失败 code=160 msg=文件名已存",
            error="网页端 rename 失败 code=160 msg=文件名已存")
        FakeAPI.reset([{"fileId": B_FILE, "fileName": ZH_NAME,
                        "fileSize": 1024, "resType": 1}],
                      apply_then_160=True)
        out = _run_rename(db, rule)

        db.expire_all()
        fresh = db.get(MirrorFile, row.id)
        assert out["name_exists_recovered"] == 1, out
        assert out["failed"] == 0, "160 收敛后不算失败"
        assert not out["errors"], out["errors"]
        assert fresh.rename_status == "done", fresh.rename_status
        assert fresh.rename_error == "", "完成态不得再挂错误文案"
        assert fresh.error == "", "完成态不得再挂复制错误文案"
        assert fresh.target_name, "完成态必须有目标名（推送门禁要看）"


def test_160_real_conflict_is_pending_not_failed(env):
    """目标名真被别的文件占用 → 标「待下轮重试」，不标失败、不计入熔断。"""
    S, _ = env
    from app.models import MirrorFile
    with S() as db:
        rule, _media, row = _seed(db, rename_status="failed",
                                  rename_error="网页端 rename 失败 code=160 msg=文件名已存")
        FakeAPI.reset([{"fileId": B_FILE, "fileName": ZH_NAME,
                        "fileSize": 1024, "resType": 1}],
                      apply_then_160=False)
        out = _run_rename(db, rule)

        db.expire_all()
        fresh = db.get(MirrorFile, row.id)
        assert out["name_exists_conflict"] == 1, out
        assert out["failed"] == 0, "真冲突不算失败（不触发熔断）"
        assert fresh.rename_status == "pending_visible", fresh.rename_status
        assert fresh.rename_error, "必须写明原因"


def test_160_does_not_trip_circuit_breaker(env):
    """连续 160 不得触发「连续失败熔断」——160 不是失败。"""
    S, _ = env
    from app.models import MirrorFile, MirrorMedia
    from app.utils import normalize_work_name
    with S() as db:
        rule = MirrorMedia  # 占位，避免误用
        from app.models import MirrorRule
        r = MirrorRule(name="v220a", source_root_id=A_ROOT, target_root_id=B_ROOT,
                       enabled=True, star_naming=True, auto_identify=False,
                       rename_files=True, file_idle_seconds=0.5, auto_create_dirs=True)
        db.add(r)
        db.commit()
        media = MirrorMedia(rule_id=r.id,
                            work_key="work:" + normalize_work_name(WORK),
                            title_standard=WORK, title_mirror="大·醉·侠 (2002) {tmdb-135646}",
                            source_dir_id=A_WORK, target_dir_id=B_WORK,
                            english_title=ENG, english_source="tmdb", status="synced")
        db.add(media)
        db.commit()
        files = []
        for i in range(6):
            files.append(MirrorFile(
                media_id=media.id, rule_id=r.id, identity=f"tmdb:135646:tv:1:{i}",
                source_file_id=f"AF{i}", target_file_id=f"BF{i}",
                file_name=f"大醉侠 第0{i}集 2160p.mkv", size=1024 + i,
                utime=1, fingerprint=f"{1024 + i}:1", status="synced"))
        db.add_all(files)
        db.commit()
        FakeAPI.reset([{"fileId": f"BF{i}", "fileName": f"大醉侠 第0{i}集 2160p.mkv",
                        "fileSize": 1024 + i, "resType": 1} for i in range(6)],
                      apply_then_160=False)
        out = _run_rename(db, r)
        # 熔断阈值是 5；6 个连续 160 若被当成失败，第 5 个就会停一轮
        assert len(FakeAPI.rename_calls) == 6, FakeAPI.rename_calls
        assert out.get("truncated_reason") in ("", None), out.get("truncated_reason")
        assert out["name_exists_conflict"] == 6, out


# ---------- 3. 批量判定只列一次目录 ----------

def test_160_batch_listing_is_done_once_per_window(env):
    """N 条 160 只做一次列目录批量判定（不能变 N 次递归列目录）。"""
    S, _ = env
    from app.models import MirrorFile, MirrorMedia, MirrorRule
    from app.utils import normalize_work_name
    with S() as db:
        r = MirrorRule(name="v220a", source_root_id=A_ROOT, target_root_id=B_ROOT,
                       enabled=True, star_naming=True, auto_identify=False,
                       rename_files=True, file_idle_seconds=0.5, auto_create_dirs=True)
        db.add(r)
        db.commit()
        media = MirrorMedia(rule_id=r.id,
                            work_key="work:" + normalize_work_name(WORK),
                            title_standard=WORK, title_mirror="大·醉·侠 (2002) {tmdb-135646}",
                            source_dir_id=A_WORK, target_dir_id=B_WORK,
                            english_title=ENG, english_source="tmdb", status="synced")
        db.add(media)
        db.commit()
        for i in range(4):
            db.add(MirrorFile(
                media_id=media.id, rule_id=r.id, identity=f"tmdb:135646:tv:2:{i}",
                source_file_id=f"CF{i}", target_file_id=f"CF{i}",
                file_name=f"大醉侠 第1{i}集 2160p.mkv", size=2048 + i,
                utime=1, fingerprint=f"{2048 + i}:1", status="synced"))
        db.commit()
        FakeAPI.reset([{"fileId": f"CF{i}", "fileName": f"大醉侠 第1{i}集 2160p.mkv",
                        "fileSize": 2048 + i, "resType": 1} for i in range(4)],
                      apply_then_160=False)
        _run_rename(db, r)
        # 准备阶段 1 次 + 160 批量判定 1 次 = 2 次；逐条回查会是 1 + 4 = 5 次
        assert FakeAPI.list_calls <= 2, f"列目录次数过多：{FakeAPI.list_calls}"
        assert len(FakeAPI.rename_calls) == 4, FakeAPI.rename_calls


# ---------- 4. 准备阶段幂等闸 ----------

def test_prepare_stage_skips_duplicate_request_when_name_already_done(env):
    """DB 记的目标名已是最终名 → 准备阶段就跳过，不再发重复改名请求。"""
    S, _ = env
    from app.models import MirrorFile
    with S() as db:
        rule, _media, row = _seed(db, target_name=TARGET_EN,
                                  rename_status="done", rename_error="")
        FakeAPI.reset([{"fileId": B_FILE, "fileName": TARGET_EN,
                        "fileSize": 1024, "resType": 1}])
        out = _run_rename(db, rule)

        db.expire_all()
        fresh = db.get(MirrorFile, row.id)
        assert FakeAPI.rename_calls == [], "已达成的文件不该再发改名请求"
        assert fresh.rename_status == "done"
        assert out["renamed"] == 0 and out["failed"] == 0


def test_done_row_never_keeps_stale_error_text(env):
    """完成态不得挂着任何错误文案（真机：done 却带 code=160）。"""
    S, _ = env
    from app.models import MirrorFile
    with S() as db:
        rule, _media, row = _seed(db, target_name=TARGET_EN,
                                  rename_status="done",
                                  rename_error="网页端 rename 失败 code=160 msg=文件名已存",
                                  error="网页端 rename 失败 code=160 msg=文件名已存")
        FakeAPI.reset([{"fileId": B_FILE, "fileName": TARGET_EN,
                        "fileSize": 1024, "resType": 1}])
        _run_rename(db, rule)

        db.expire_all()
        fresh = db.get(MirrorFile, row.id)
        assert fresh.rename_status == "done"
        assert fresh.rename_error == "", fresh.rename_error
        assert fresh.error == "", fresh.error


# ---------- 5. 统一落库入口 + 中性标签 ----------

def test_mark_rename_done_clears_both_error_fields():
    from app import mirror
    from app.models import MirrorFile

    class _Row:
        target_file_id = ""
        target_name = ""
        rename_status = "failed"
        rename_error = "code=160 msg=文件名已存"
        error = "code=160 msg=文件名已存"
        updated_at = None

    row = _Row()
    mirror._mark_rename_done(row, "BF1", "The Drunk Hero 第04集.mkv")
    assert row.rename_status == "done"
    assert row.rename_error == ""
    assert row.error == ""
    assert row.target_file_id == "BF1"
    assert row.target_name == "The Drunk Hero 第04集.mkv"
    assert isinstance(row, MirrorFile) or row is not None


def test_rename_error_label_never_leaks_code():
    from app import mirror
    assert "code" not in mirror.rename_error_label("code=160 msg=文件名已存").lower()
    assert "160" not in mirror.rename_error_label("code=160 msg=文件名已存")
    assert "127" not in mirror.rename_error_label("code=127 msg=操作过于频繁")
    assert mirror.rename_error_label("") == "未记录原因"


# ---------- 6. 一次性迁移：修历史矛盾状态 ----------

def test_migration_repairs_inconsistent_rename_state(env):
    S, _ = env
    from app.db import _repair_rename_state_consistency
    from app.models import AppConfig, MirrorFile, MirrorMedia, MirrorRule
    from app.utils import normalize_work_name
    with S() as db:
        r = MirrorRule(name="v220a", source_root_id=A_ROOT, target_root_id=B_ROOT,
                       enabled=True, star_naming=True, auto_identify=False,
                       rename_files=True, file_idle_seconds=0.5, auto_create_dirs=True)
        db.add(r)
        db.commit()
        media = MirrorMedia(rule_id=r.id,
                            work_key="work:" + normalize_work_name(WORK),
                            title_standard=WORK, title_mirror="大·醉·侠",
                            source_dir_id=A_WORK, target_dir_id=B_WORK,
                            english_title=ENG, english_source="tmdb", status="synced")
        db.add(media)
        db.commit()
        # ① done 却挂着 160 错误文案
        db.add(MirrorFile(media_id=media.id, rule_id=r.id, identity="i1",
                           source_file_id="s1", target_file_id="t1",
                           file_name="大醉侠 第04集.mkv", size=1, utime=1,
                           fingerprint="1:1", status="synced",
                           rename_status="done",
                           rename_error="code=160 msg=文件名已存",
                           error="code=160 msg=文件名已存"))
        # ② failed 且原因是「文件名已存」→ 假失败，应退回待处理
        db.add(MirrorFile(media_id=media.id, rule_id=r.id, identity="i2",
                           source_file_id="s2", target_file_id="t2",
                           file_name="大醉侠 第05集.mkv", size=2, utime=1,
                           fingerprint="2:1", status="synced",
                           rename_status="failed",
                           rename_error="网页端 rename 失败 code=160 msg=文件名已存"))
        # ③ 真失败（数据库锁）→ 保持原样
        db.add(MirrorFile(media_id=media.id, rule_id=r.id, identity="i3",
                           source_file_id="s3", target_file_id="t3",
                           file_name="大醉侠 第06集.mkv", size=3, utime=1,
                           fingerprint="3:1", status="synced",
                           rename_status="failed",
                           rename_error="database is locked"))
        db.commit()

        changed = _repair_rename_state_consistency(db)
        assert changed == 2, changed
        db.expire_all()
        rows = {r.identity: r for r in db.query(MirrorFile).all()}
        assert rows["i1"].rename_status == "done"
        assert rows["i1"].rename_error == ""
        assert rows["i1"].error == ""
        assert rows["i2"].rename_status == "", "160 假失败应退回待处理"
        assert rows["i2"].rename_error == ""
        assert rows["i3"].rename_status == "failed", "真失败保持原样"
        # 只跑一次
        assert _repair_rename_state_consistency(db) == 0
        assert db.get(AppConfig, "rename_state_consistency_repair_v210") is not None


# ---------- 7. 门禁收敛到作品级 ----------

def test_rename_gate_is_work_scoped_not_round_scoped():
    """改名门禁收敛到作品级：整轮拦截默认关闭，且统计能点名受影响作品。"""
    src = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
    assert "RENAME_GATE_BLOCK_WHOLE_ROUND = False" in src
    assert "gate_block_round = rename_failed and RENAME_GATE_BLOCK_WHOLE_ROUND" in src
    # 受影响作品清单必须来自后端统计（不是前端猜）
    msrc = (ROOT / "backend/app/mirror.py").read_text(encoding="utf-8")
    assert '"failed_works"' in msrc


def test_failed_works_are_collected_in_rename_stats(env):
    S, _ = env
    with S() as db:
        rule, _media, _row = _seed(db)
        FakeAPI.reset([{"fileId": B_FILE, "fileName": ZH_NAME,
                        "fileSize": 1024, "resType": 1}],
                      apply_then_160=False)
        out = _run_rename(db, rule)
        assert out["failed_works"], "改名失败时必须给出受影响作品清单"
        assert WORK.split(" (")[0] in out["failed_works"][0]
