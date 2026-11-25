"""v2.1.36：问题诊断的「计数」与「明细」必须同源。

用户真机反馈（2026-11-25）：
  「镜像同步问题诊断提示 2 项待处理，可是点开只有已完成的复制任务，
    看不到是哪两个问题，也没有重新处理的按钮」

线上取证（只读，DB 时间 UTC / 北京 +8）：
  · diagCount 逐项复算 = 2，**全部来自 files_retryable=2**，其余 14 项为 0；
  · 这 2 项是 mirror_files 里 status='pending' 的两集，
    报错「B 库未找到已同步文件，自动重新复制」；
  · 而诊断面板的「文件复制失败」只查 `status == 'failed'`（pending 不在里面），
    「复制任务」表只取最近 30 条且全状态混排（那批任务已 done）——
    所以数字说有 2 项，页面上一条都点不开。

两条病根（本文件逐条锁死）：
  ① **口径不同源**：计数用 `RETRYABLE_FILE_STATUS`（pending/copying/failed/unknown）
     的并集，明细却只有 failed → 补一份与计数同源的 `retryable_files`；
  ② **重复计数**：`files_retryable` 已经是 failed/copying/unknown 的并集，
     前端又把这三个逐项加了一遍 → 同一个文件被算两次以上；
  ③ 顺带：任务侧四个计数（失败/待确认/跳过/取消）也没有专属明细，补 `task_issues`。

修复前本文件应当红（`retryable_files` 不存在 / 前端仍在重复相加）。
"""
import json
import re
from pathlib import Path

import pytest
from sqlalchemy import select

from app import mirror
from app.db import SessionLocal, migrate
from app.main import (
    MirrorFile, MirrorMedia, MirrorRule, MirrorTask, QueueItem, Resource,
    ScanTask, mirror_status,
)
from app.models import MirrorDir

TABLES = [MirrorFile, MirrorTask, MirrorDir, MirrorMedia, MirrorRule, QueueItem,
          Resource, ScanTask]
A_ROOT, B_ROOT = "A_ROOT", "B_ROOT"
APP_VUE = Path(__file__).resolve().parents[1] / "frontend/src/App.vue"


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


def _rule(db):
    r = MirrorRule(name="测试镜像", source_root_id=A_ROOT, target_root_id=B_ROOT,
                   interval_minutes=60, enabled=True, file_idle_seconds=0.5,
                   auto_create_dirs=True, star_naming=True)
    db.add(r)
    db.commit()
    return r


def _media(db, rule):
    m = MirrorMedia(rule_id=rule.id, title_standard="缅北电诈覆灭纪实 (2026)",
                    title_mirror="缅北电诈覆灭纪实 ·", media_type="tv",
                    status="synced", source_dir_id="A_W1", target_dir_id="B_W1")
    db.add(m)
    db.commit()
    return m


def _file(db, rule, media, *, status, name, error=""):
    f = MirrorFile(media_id=media.id, rule_id=rule.id, identity=f"id-{name}",
                   source_file_id=f"sf-{name}", file_name=name, size=1, utime=1,
                   fingerprint=f"1:1", status=status, error=error, task_id="1972461165756473430")
    db.add(f)
    db.commit()
    return f


def _task(db, rule, media, *, status="done", task_status="", error=""):
    t = MirrorTask(rule_id=rule.id, media_id=media.id,
                   task_id=f"t-{status}-{task_status}-{error[:6]}",
                   status=status, task_status=task_status, polls=3, error=error)
    db.add(t)
    db.commit()
    return t


# ---------------------------------------------------------------------------
# ① 核心：pending 文件必须在诊断里有明细（修复前该字段不存在 → 红）
# ---------------------------------------------------------------------------
def test_pending_files_have_their_own_detail_list(db):
    rule = _rule(db)
    media = _media(db, rule)
    err = "B 库未找到已同步文件，自动重新复制"
    _file(db, rule, media, status="pending", name="S01E01.mkv", error=err)
    _file(db, rule, media, status="pending", name="S01E02.mkv", error=err)
    _file(db, rule, media, status="synced", name="S01E03.mkv")
    _task(db, rule, media, status="done", task_status="completed")

    out = mirror_status(db)

    assert out["counts"]["files_retryable"] == 2, out["counts"]["files_retryable"]
    rows = out.get("retryable_files")
    assert rows is not None, "状态接口没有返回待重试文件明细 —— 数字说有项、页面无处可看"
    names = sorted(r["file"] for r in rows)
    assert names == ["S01E01.mkv", "S01E02.mkv"], names
    assert all(r["status_label"] == "待复制" for r in rows), [r["status_label"] for r in rows]
    assert all(err in r["error"] for r in rows), "原因没带出来，用户看不出为什么"
    # 反证：原来那份「文件复制失败」清单确实看不到它们（这正是用户点开一片空的原因）
    assert out["file_errors"] == [], out["file_errors"]


def test_detail_list_covers_all_retryable_states(db):
    """四类可重试态一个不漏：待复制 / 复制中 / 复制失败 / 状态待确认。"""
    rule = _rule(db)
    media = _media(db, rule)
    for st in ("pending", "copying", "failed", "unknown"):
        _file(db, rule, media, status=st, name=f"{st}.mkv", error=f"{st} 的原因")
    _file(db, rule, media, status="synced", name="ok.mkv")

    out = mirror_status(db)
    got = {r["status"] for r in out["retryable_files"]}
    assert got == set(mirror.RETRYABLE_FILE_STATUS), got
    # 明细集合与计数用的是同一个谓词 → 数目必然相等
    assert len(out["retryable_files"]) == out["counts"]["files_retryable"]
    labels = {r["status"]: r["status_label"] for r in out["retryable_files"]}
    assert set(labels.values()) == {"待复制", "复制中", "复制失败", "状态待确认"}, labels


# ---------------------------------------------------------------------------
# ② 重复计数：files_retryable 已是并集，前端不能再逐项相加
# ---------------------------------------------------------------------------
def test_retryable_is_union_so_adding_sub_counts_double_counts(db):
    rule = _rule(db)
    media = _media(db, rule)
    _file(db, rule, media, status="failed", name="a.mkv")
    _file(db, rule, media, status="copying", name="b.mkv")
    _file(db, rule, media, status="unknown", name="c.mkv")

    c = mirror_status(db)["counts"]
    # 三个子计数各自为 1，但它们都包含在 files_retryable 里
    assert (c["files_failed"], c["files_copying"], c["files_unknown"]) == (1, 1, 1)
    assert c["files_retryable"] == 3, c
    assert c["files_retryable"] == (c["files_failed"] + c["files_copying"]
                                    + c["files_unknown"])


def _diag_block() -> str:
    src = APP_VUE.read_text(encoding="utf-8")
    start = src.index("const diagCount = computed(")
    end = src.index("})", start) + 2
    block = src[start:end]
    # 只去整行注释可能残留，逐行剥 `//` 注释（本块内没有含 // 的字符串）
    return re.sub(r"//[^\n]*", "", block)


def test_frontend_no_longer_double_counts():
    block = _diag_block()
    for bad in ("files_failed", "files_copying", "files_unknown"):
        assert bad not in block, (
            f"diagCount 仍在累加 {bad} —— 它已包含在 files_retryable 里，"
            "相加就是同一批文件被算两次")


def test_frontend_counts_use_backend_counts_not_list_length():
    """计数不能用清单长度：清单有 limit，条数一多就又对不上了。"""
    block = _diag_block()
    assert "length" not in block, f"diagCount 仍在用清单长度当计数：{block}"
    for key in ("unmapped_dirs", "files_retryable", "works_without_english",
                "files_rename_failed", "tasks_failed", "tasks_unknown",
                "tasks_skipped", "tasks_cancelled", "invalid_mappings"):
        assert key in block, f"diagCount 漏了 {key}"


# ---------------------------------------------------------------------------
# ③ 任务侧：四个计数也要有专属明细
# ---------------------------------------------------------------------------
def test_task_issues_cover_failed_skipped_cancelled_unknown(db):
    rule = _rule(db)
    media = _media(db, rule)
    _task(db, rule, media, status="failed")
    _task(db, rule, media, status="done", task_status="skipped")
    _task(db, rule, media, status="done", task_status="cancelled")
    _task(db, rule, media, status="unknown")
    _task(db, rule, media, status="done", task_status="completed")

    out = mirror_status(db)
    rows = out.get("task_issues")
    assert rows is not None, "状态接口没有返回需处理任务明细"
    assert len(rows) == 4, [(r["status"], r["task_status"]) for r in rows]
    labels = {r["status_label"] for r in rows}
    assert labels == {"失败", "跳过本序列", "已取消", "状态待确认"}, labels


def test_frontend_renders_the_new_panels():
    src = APP_VUE.read_text(encoding="utf-8")
    assert "mirrorStatus.retryable_files" in src, "「待重试文件」面板没有接上明细数据"
    assert "mirrorStatus.task_issues" in src, "「需处理任务」面板没有接上明细数据"
    # 重处理入口必须还在（用户诉求：没有重新处理的按钮）
    assert "@click=\"retryFailedFiles\"" in src
    assert "只显示最近" in src or "仅显示最近" in src, "清单被 limit 截断时必须明示"
