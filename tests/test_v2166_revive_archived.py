"""v2.1.66 验收（保留部分）：对账前复活「A 库仍有源文件」的归档行。

原 tests/test_v2166_purge_empty_dirs.py 的脚本部分已随 v2.1.67 删除——
用户改为要求「项目本身有能力」，清理逻辑内置到 mirror.py（见 test_v2167_mirror_purge_empty_dirs.py）。
复活逻辑（main._revive_archived_with_a_source）仍是 v2.1.66 有效功能，测试保留在此。
"""
import asyncio
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

MAIN_SRC = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")


# ============================================================================
# v2.1.66 第二部分：对账前复活「A 库仍有源文件」的归档行
# ============================================================================


@pytest.fixture
def revive_db():
    """建表 + 每个用例清空相关表，避免串扰。"""
    from app.db import SessionLocal, migrate
    from app.models import MirrorFile, MirrorMedia, MirrorRule, Resource
    migrate()
    db = SessionLocal()
    for m in (MirrorFile, MirrorMedia, MirrorRule, Resource):
        db.query(m).delete()
    db.commit()
    yield db
    for m in (MirrorFile, MirrorMedia, MirrorRule, Resource):
        db.query(m).delete()
    db.commit()
    db.close()


def _add_archived(db, fid, sname, *, title=""):
    from app.models import Resource
    r = Resource(file_id=fid, name=f"sname_{fid}", media_title=title,
                 source_name=sname, source_title=title, path_hint="剧集/x/y",
                 clean_state="cleaned", clean_reason="B库内容已移除，归档资源库记录")
    db.add(r)
    db.flush()
    return r


def _add_mirror_file(db, fname, *, status="synced", src_id="A_SRC"):
    from app.models import MirrorFile, MirrorMedia, MirrorRule
    rule = db.query(MirrorRule).first()
    if not rule:
        rule = MirrorRule(name="r", source_root_id="s", target_root_id="t", enabled=1)
        db.add(rule); db.flush()
    mm = db.query(MirrorMedia).first()
    if not mm:
        mm = MirrorMedia(rule_id=rule.id, work_key="work:probe", source_dir_id="A_DIR",
                         target_dir_id="B_DIR", status="synced")
        db.add(mm); db.flush()
    mf = MirrorFile(media_id=mm.id, rule_id=rule.id, file_name=fname,
                    source_file_id=src_id, target_file_id="B_FILE", status=status)
    db.add(mf); db.flush()
    return mf


def test_revive_exact_a_source_match(revive_db):
    """精确匹配：归档 source_name == mirror_files.file_name 且 A 源文件在 → 复活。"""
    from app.main import _revive_archived_with_a_source
    from app.models import Resource, include_archived

    r1 = _add_archived(revive_db, "B_OLD_1", "剧名.S01E01.mkv", title="剧名")
    r2 = _add_archived(revive_db, "B_OLD_2", "剧名.S01E02.mkv", title="剧名")
    _add_mirror_file(revive_db, "剧名.S01E01.mkv")  # 只有 E01 在 A 库
    revive_db.commit()

    fids = _revive_archived_with_a_source(revive_db)
    revive_db.commit()
    assert fids == ["B_OLD_1"]
    with include_archived():
        db_r1 = revive_db.get(Resource, r1.id)
        db_r2 = revive_db.get(Resource, r2.id)
    assert db_r1.clean_state == "" and db_r1.cleaned_at is None and db_r1.clean_reason == ""
    assert db_r2.clean_state == "cleaned"  # E02 不在 A 库，保持归档


def test_revive_skips_non_synced_and_no_a_source(revive_db):
    """mirror_files 非 synced/done 或无 source_file_id → 不复活（A 库已删/失败）。"""
    from app.main import _revive_archived_with_a_source
    from app.models import Resource, include_archived

    r_fail = _add_archived(revive_db, "B_FAIL", "fail.mkv")
    r_nosrc = _add_archived(revive_db, "B_NOSRC", "nosrc.mkv")
    _add_mirror_file(revive_db, "fail.mkv", status="failed")
    _add_mirror_file(revive_db, "nosrc.mkv", src_id="")
    revive_db.commit()

    assert _revive_archived_with_a_source(revive_db) == []
    with include_archived():
        assert revive_db.get(Resource, r_fail.id).clean_state == "cleaned"
        assert revive_db.get(Resource, r_nosrc.id).clean_state == "cleaned"


def test_revive_no_title_and_name_ambiguity(revive_db):
    """归档行无 media_title 也能按 source_name 复活（线上 1063 条无标题）。"""
    from app.main import _revive_archived_with_a_source
    r = _add_archived(revive_db, "B_NOTITLE", "无标题剧.S01E03.mkv", title="")
    _add_mirror_file(revive_db, "无标题剧.S01E03.mkv")
    revive_db.commit()
    assert _revive_archived_with_a_source(revive_db) == ["B_NOTITLE"]


def test_revive_source_anchor():
    """源码反证：复活在归档判定之前、清三字段、纳入 seen、精确文件名匹配。"""
    assert "_revive_archived_with_a_source(db)" in MAIN_SRC
    assert "seen = seen | set(revived_fids)" in MAIN_SRC
    assert 'MirrorFile.status.in_(("synced", "done"))' in MAIN_SRC
    assert 'Resource.source_name == ""' not in MAIN_SRC  # 不猜空名
    assert '"revived": len(revived_fids)' in MAIN_SRC
