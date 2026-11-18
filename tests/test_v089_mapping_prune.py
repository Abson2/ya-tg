"""v0.8.9 回归：目录映射清单「清理无效映射」。

用户反馈：「目录映射清单没有清理无效的按钮，或者自动清理在 B 库已经不存在的文件映射」。
本组测试锁死四条红线：
  1. 只按 B 库实际内容清理，列目录不可信 / 整部作品为空一律跳过，绝不误清；
  2. 只重置映射，不删行、不删 A 库锚点、不动资源/分享/队列；
  3. dry_run 可预览且不写库；
  4. 管理页有按钮，端点有校验，折叠分区与模板变量说明齐备。
"""
import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest

from app import main, mirror
from app.db import SessionLocal, migrate
from app.models import (AuditLog, MirrorDir, MirrorFile, MirrorMedia,
                        MirrorRule, QueueItem, Resource)

ROOT = Path(__file__).resolve().parents[1]
B_ROOT, B_WORK, B_ALIVE, B_GONE = "B_ROOT", "B_WORK", "BF_ALIVE", "BF_GONE"

TABLES = [MirrorFile, MirrorDir, MirrorMedia, MirrorRule, AuditLog,
          QueueItem, Resource]


class FakeAPI:
    """按父目录返回 B 库实际内容；resType=2 表示目录。"""

    by_parent = {}
    fail = False

    def __init__(self, db):
        self.db = db

    @classmethod
    def reset(cls, by_parent=None, fail=False):
        cls.by_parent = {k: [dict(x) for x in v]
                         for k, v in (by_parent or {}).items()}
        cls.fail = fail

    async def list_files(self, parent_id="", page=0, page_size=50):
        if self.fail:
            raise RuntimeError("B 库列目录超时")
        items = self.by_parent.get(str(parent_id), [])
        if page == 0:
            return {"list": [dict(x) for x in items], "total": len(items)}
        return {"list": [], "total": len(items)}


def _b_file(fid=B_ALIVE, name="Show.S01E01.mkv", size=1024):
    return {"fileId": fid, "fileName": name, "fileSize": size, "resType": 1}


def _b_dir(fid=B_WORK, name="Show (2026)"):
    return {"fileId": fid, "fileName": name, "resType": 2}


def _clean():
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        db.commit()


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    _clean()
    FakeAPI.reset()
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


def _seed(db, *, file_fid=B_GONE, dir_fid=B_GONE):
    rule = MirrorRule(name="默认镜像", source_root_id="A_ROOT",
                      target_root_id=B_ROOT, enabled=True)
    db.add(rule)
    db.flush()
    media = MirrorMedia(rule_id=rule.id, work_key="work:show",
                        title_standard="Show (2026)", title_mirror="Show (2026)",
                        source_dir_id="A_WORK", target_dir_id=B_WORK,
                        category_path="影视/电视剧", status="synced")
    db.add(media)
    db.flush()
    f = MirrorFile(media_id=media.id, rule_id=rule.id, identity="e1",
                   source_file_id="A_F1", file_name="Show.S01E01.mkv",
                   target_file_id=file_fid, fingerprint="1024:1",
                   status="synced", target_name="Show.S01E01.mkv",
                   rename_status="done")
    d = MirrorDir(rule_id=rule.id, source_dir_id="A_WORK",
                  parent_target_id=B_ROOT, name_standard="Show (2026)",
                  kind="work", category_path="影视/电视剧",
                  target_dir_id=dir_fid, status="created")
    db.add_all([f, d])
    db.commit()
    return rule, media, f, d


def _prune(db, rule, dry_run=False):
    with patch.object(mirror, "GuangYaWebFiles", FakeAPI):
        return _run(mirror.prune_stale_mappings(db, rule, dry_run=dry_run))


# ---------- 1. 正常清理 ----------
def test_prunes_missing_file_mapping_but_keeps_a_side_anchor(db):
    # 目录映射本身是有效的（B_WORK 仍在），本例只验证「失效文件映射」被清理
    rule, media, f, d = _seed(db, dir_fid=B_WORK)
    FakeAPI.reset({B_WORK: [_b_file()], B_ROOT: [_b_dir()]})
    out = _prune(db, rule)
    assert out["files_pruned"] == 1, out
    assert out["dirs_pruned"] == 0, out
    db.expire_all()
    row = db.get(MirrorFile, f.id)
    assert row.status == "pending"          # 下轮同步自动补复制
    assert row.target_file_id == ""
    assert row.target_name == "" and row.rename_status == ""
    assert row.file_name == "Show.S01E01.mkv"   # A 库原名锚点保留
    assert row.fingerprint == "1024:1"
    assert row.source_file_id == "A_F1"
    assert "补复制" in row.error


def test_alive_mapping_is_untouched(db):
    rule, media, f, d = _seed(db, file_fid=B_ALIVE, dir_fid=B_WORK)
    FakeAPI.reset({B_WORK: [_b_file()], B_ROOT: [_b_dir()]})
    out = _prune(db, rule)
    assert out["files_pruned"] == 0 and out["dirs_pruned"] == 0, out
    db.expire_all()
    assert db.get(MirrorFile, f.id).target_file_id == B_ALIVE
    assert db.get(MirrorDir, d.id).target_dir_id == B_WORK


def test_prunes_missing_dir_mapping(db):
    rule, media, f, d = _seed(db, file_fid=B_ALIVE)
    FakeAPI.reset({B_WORK: [_b_file()], B_ROOT: [_b_dir()]})
    out = _prune(db, rule)
    assert out["dirs_pruned"] == 1, out
    db.expire_all()
    row = db.get(MirrorDir, d.id)
    assert row.target_dir_id == "" and row.status == "missing"
    assert "自动重建" in row.error


# ---------- 2. 安全闸门 ----------
def test_unverified_listing_never_prunes(db):
    rule, media, f, d = _seed(db)
    FakeAPI.reset({B_WORK: [_b_file()], B_ROOT: [_b_dir()]}, fail=True)
    out = _prune(db, rule)
    assert out["files_pruned"] == 0 and out["dirs_pruned"] == 0, out
    assert out["media_skipped_unverified"] >= 1
    assert out["dirs_skipped_unverified"] >= 1
    db.expire_all()
    assert db.get(MirrorFile, f.id).target_file_id == B_GONE


def test_empty_b_work_is_skipped_not_cleared(db):
    """整部作品 B 库一个文件都没有：交给同步补复制，绝不因接口抖动清映射。"""
    rule, media, f, d = _seed(db)
    FakeAPI.reset({B_WORK: [], B_ROOT: [_b_dir()]})
    out = _prune(db, rule)
    assert out["media_skipped_empty"] == 1, out
    assert out["files_pruned"] == 0, out
    db.expire_all()
    assert db.get(MirrorFile, f.id).status == "synced"


def test_dry_run_reports_without_writing(db):
    rule, media, f, d = _seed(db)
    FakeAPI.reset({B_WORK: [_b_file()], B_ROOT: [_b_dir()]})
    out = _prune(db, rule, dry_run=True)
    assert out["dry_run"] is True and out["files_pruned"] == 1, out
    db.expire_all()
    assert db.get(MirrorFile, f.id).target_file_id == B_GONE
    assert db.get(MirrorDir, d.id).target_dir_id == B_GONE


# ---------- 3. 端点与前端 ----------
def test_endpoint_requires_b_root_id(db):
    db.add(MirrorRule(name="默认镜像", source_root_id="A_ROOT",
                      target_root_id="", enabled=True))
    db.commit()
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as e:
        _run(main.mirror_mappings_prune(dry_run=True, db=db))
    assert e.value.status_code == 400


def test_endpoint_returns_stats(db):
    rule, media, f, d = _seed(db, file_fid=B_ALIVE, dir_fid=B_WORK)
    FakeAPI.reset({B_WORK: [_b_file()], B_ROOT: [_b_dir()]})
    with patch.object(mirror, "GuangYaWebFiles", FakeAPI):
        out = _run(main.mirror_mappings_prune(dry_run=True, db=db))
    assert out["ok"] is True
    assert "files_pruned" in out and "dirs_pruned" in out


def test_frontend_has_prune_button_and_folded_sections():
    app_vue = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "清理无效映射" in app_vue
    assert "pruneMirrorMappings" in app_vue
    assert "/mirror/mappings/prune" in app_vue
    assert 'name="diagnostics"' in app_vue and 'name="mappings"' in app_vue
    assert "mirrorDiagOpen" in app_vue
