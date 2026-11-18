"""v0.8.3 回归：真实网盘拒绝 *（code=141）、扫描根目录不再重复填写、日志保留策略。

锁死用户三条反馈：
  1. B 库影视主目录标记由 * 改为 ·；旧 * 名称就地转换；分类目录原名（含中点）1:1。
  2. 资源库「扫描计划」里不再有任何根目录 ID 输入框，后端也不再保存第二份 ID。
  3. 所有日志/历史留痕都有保留天数 + 条数上限 + 自动清理 + 手动清空，
     且绝不误删待推送队列、未完成批次、待处理事件与业务映射。
"""
import asyncio
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from app import log_retention, mirror
from app.db import SessionLocal, migrate
from app.main import (LogClearInput, LogConfigInput, MirrorRule, ScanConfig, ScanTask,
                      _effective_scan_root, logs_clear, logs_config_save, logs_overview)
from app.models import (AppConfig, AuditLog, DistributionEvent, MirrorDir, MirrorFile,
                        MirrorMedia, MirrorTask, QueueItem, ResourceScanBatch,
                        ResourceVersion)
from app.utils import (NAME_MARK, normalize_work_name, standard_media_name,
                       star_media_name, utcnow)

ROOT = Path(__file__).resolve().parents[1]
A_ROOT, B_ROOT = "A_ROOT", "B_ROOT"
CAT_NAME = "动画·电影"                      # 分类目录原名里带中点，不能被当成标记吃掉
WORK_NAME = "侠客行 (2001) {tmdb-135646}"

TABLES = [AuditLog, ResourceScanBatch, MirrorTask, ResourceVersion, DistributionEvent,
          QueueItem, MirrorFile, MirrorDir, MirrorMedia, MirrorRule, ScanTask]


def _clean():
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        db.query(AppConfig).filter(AppConfig.key.like("log_%")).delete(
            synchronize_session=False)
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


def _rule(db):
    r = MirrorRule(name="测试镜像", source_root_id=A_ROOT, target_root_id=B_ROOT,
                   interval_minutes=60, enabled=True, file_idle_seconds=0.5,
                   auto_create_dirs=True, star_naming=True)
    db.add(r)
    db.commit()
    return r


class FakeAPI:
    """A 库：根目录 → 分类目录（名字含 ·）→ 作品目录 → 单集；B 库为空。"""

    created = []
    copies = []

    def __init__(self, db):
        self.db = db

    async def list_files(self, parent_id, page=0, page_size=100):
        if page > 0:
            return {"list": [], "total": 0}
        if parent_id == A_ROOT:
            return {"list": [{"fileId": "CAT", "resType": 2,
                              "fileName": CAT_NAME}], "total": 1}
        if parent_id == "CAT":
            return {"list": [{"fileId": "A_W1", "resType": 2,
                              "fileName": WORK_NAME}], "total": 1}
        if parent_id == "A_W1":
            return {"list": [{"fileId": "F1", "resType": 1,
                              "fileName": "X.S01E01.mkv", "fileSize": 100,
                              "utime": 1}], "total": 1}
        return {"list": [], "total": 0}          # B 库初始为空

    async def create_dir(self, parent_id, dir_name):
        self.created.append((parent_id, dir_name))
        return {"file_id": f"B_{len(self.created)}", "name": dir_name,
                "parent_id": parent_id, "raw": {}}

    async def copy_file(self, file_ids, parent_id):
        self.copies.append((list(file_ids), parent_id))
        return "T1"

    async def get_task_status(self, task_id):
        return {"status": 2, "raw": {"status": 2}}


# ---------- 1. 命名：· 取代 *，且旧数据与分类目录都兼容 ----------
def test_name_mark_is_dot_and_year_untouched():
    got = star_media_name("大醉侠 (2002) {tmdb-135646}")
    assert got == "大·醉·侠 (2002) {tmdb-135646}", got
    assert NAME_MARK in got and "*" not in got
    assert "(2002)" in got and "{tmdb-135646}" in got
    assert standard_media_name(got) == "大醉侠 (2002) {tmdb-135646}"
    assert normalize_work_name(got) == normalize_work_name("大醉侠 (2002) {tmdb-135646}")


def test_legacy_star_names_are_still_understood():
    assert star_media_name("大*醉*侠 (2002)") == "大·醉·侠 (2002)"
    assert standard_media_name("大*醉*侠 (2002)") == "大醉侠 (2002)"
    assert normalize_work_name("大*醉*侠") == normalize_work_name("大·醉·侠") == "大醉侠"
    # v0.8.2 已降级创建的无标记目录，必须与新的 · 目录归一到同一作品键（不重复建）
    assert normalize_work_name("大醉侠") == normalize_work_name("大·醉·侠")


def test_category_name_with_dot_is_mirrored_verbatim(db):
    r = _rule(db)
    FakeAPI.created, FakeAPI.copies = [], []
    with patch("app.mirror.GuangYaWebFiles", FakeAPI):
        out = asyncio.run(mirror.sync_rule(db, r))
    assert out["dirs_created"] == 2, out
    names = [n for _, n in FakeAPI.created]
    assert names[0] == CAT_NAME, names            # 原名 1:1，中点不能被剥离
    assert names[1].startswith("侠·客·行"), names  # 只有作品主目录加 ·
    assert "*" not in names[1]


# ---------- 2. 扫描根目录：界面与提交体都不再有第二份 ID ----------
def test_scan_config_no_longer_accepts_root_id():
    assert "root_id" not in ScanConfig.model_fields
    text = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "扫描根目录（自动引用，无需填写）" not in text
    assert "rootSourceLabel" not in text
    assert 'v-model="task.root_id"' not in text
    assert "扫描根目录自动引用" in text           # 只保留说明文字，不再有输入框


def test_effective_root_prefers_mirror_b_over_stale_scan_task(db):
    db.add(ScanTask(cron="*/30 * * * *", root_id="OLD_SAVED_ID"))
    db.add(MirrorRule(name="默认镜像", source_root_id="A_ID",
                      target_root_id="B_ID", interval_minutes=60))
    db.commit()
    assert _effective_scan_root(db) == ("B_ID", "mirror_target")


# ---------- 3. 日志保留：按天数、按条数、保护业务关键行 ----------
def test_prune_removes_old_logs_and_keeps_recent(db):
    db.add_all([
        AuditLog(action="old", ts=utcnow() - timedelta(days=100)),
        AuditLog(action="recent", ts=utcnow() - timedelta(days=1)),
    ])
    db.commit()
    out = log_retention.prune(db, days=30, max_rows=5000)
    assert out["deleted"] == 1, out
    assert {a.action for a in db.scalars(select(AuditLog)).all()} == {"recent"}


def test_prune_enforces_per_table_row_limit(db):
    for i in range(12):
        db.add(AuditLog(action=f"a{i}", ts=utcnow() - timedelta(minutes=i + 1)))
    db.commit()
    out = log_retention.prune(db, days=3650, max_rows=5)
    assert db.query(AuditLog).count() == 5
    assert out["deleted"] == 7, out


def test_clear_never_deletes_pending_or_business_rows(db):
    old = utcnow() - timedelta(days=100)
    db.add_all([
        QueueItem(resource_id=1, channel_id=1, status="pending", created_at=old),
        QueueItem(resource_id=2, channel_id=1, status="sent", created_at=old),
        ResourceScanBatch(status="running", started_at=old),
        ResourceScanBatch(status="success", started_at=old),
        MirrorTask(rule_id=1, status="pending", created_at=old),
        MirrorTask(rule_id=1, status="done", created_at=old),
        DistributionEvent(work_key="w1", fingerprint="fp1", queue_id=0, created_at=old),
        DistributionEvent(work_key="w2", fingerprint="fp2", queue_id=7, created_at=old),
        MirrorFile(media_id=1, rule_id=1, source_file_id="a", fingerprint="f1",
                   status="synced", created_at=old),
        MirrorFile(media_id=1, rule_id=1, source_file_id="b", fingerprint="f2",
                   status="superseded", created_at=old),
    ])
    db.commit()
    out = log_retention.prune(db, mode="clear")
    assert out["deleted"] == 5, out
    assert [q.status for q in db.scalars(select(QueueItem)).all()] == ["pending"]
    assert [b.status for b in db.scalars(select(ResourceScanBatch)).all()] == ["running"]
    assert [t.status for t in db.scalars(select(MirrorTask)).all()] == ["pending"]
    assert [e.queue_id for e in db.scalars(select(DistributionEvent)).all()] == [0]
    assert [f.status for f in db.scalars(select(MirrorFile)).all()] == ["synced"]


def test_logs_api_config_and_confirmed_clear(db):
    ov = logs_overview(db=db)
    assert ov["config"]["retention_days"] == 30
    assert ov["config"]["max_rows"] == 5000
    assert any(t["key"] == "audit" for t in ov["tables"])

    cfg = logs_config_save(LogConfigInput(retention_days=7, max_rows=1000,
                                          auto_prune=False), db=db)
    assert cfg["config"]["retention_days"] == 7
    assert cfg["config"]["max_rows"] == 1000
    assert cfg["config"]["auto_prune"] is False
    assert logs_overview(db=db)["config"]["auto_prune"] is False

    with pytest.raises(HTTPException) as e:
        logs_clear(LogClearInput(confirm=False), db=db)
    assert e.value.status_code == 400

    out = logs_clear(LogClearInput(confirm=True), db=db)
    assert out["mode"] == "clear"
    # 清空本身会留下一条 logs_cleared 审计，这是唯一允许保留的新记录
    assert [a.action for a in db.scalars(select(AuditLog)).all()] == ["logs_cleared"]


def test_auto_prune_switch_can_be_turned_off(db):
    log_retention.save_config(db, retention_days=1, max_rows=500, auto_prune=False)
    db.commit()
    cfg = log_retention.config(db)
    assert cfg["auto_prune"] is False and cfg["retention_days"] == 1
    assert "每天 03:30" in cfg["schedule"]


# ---------- 4. 容器 stdout 日志也有硬限额 ----------
def test_compose_limits_container_stdout_logs():
    for name in ("docker-compose.yml", "docker-compose.build.yml"):
        text = (ROOT / name).read_text(encoding="utf-8")
        assert 'max-size: "10m"' in text and 'max-file: "3"' in text, name
