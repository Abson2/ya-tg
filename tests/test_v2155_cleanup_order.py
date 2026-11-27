"""v2.1.55 验收：清理无效影视映射「先本地、后联网」+ 前端不再像死按钮。

真机背景（2026-11-27）：用户点了「清理无效影视映射」，两部 `黑帮领地 (2025)` /
`缅北电诈覆灭纪实 (2026)` 迟迟清不掉。线上只读复核得到的结论是：

  · 680 部映射里 `prune_missing_media_rows` 只扫**最新 200 部**，这两部排第 419 / 461 位，
    联网核对根本轮不到它们；
  · 唯一能删它们的本地判据（无 synced 文件、且不在同步中）却排在整批核对**之后**，
    而整批核对实测单部 0.5 秒、200 部要 1-2 分钟；
  · 前端还把 `mirrorBusy` 写在确认框之后 —— 最慢的那一分钟里按钮毫无变化，
    用户只能以为「点了没反应」，于是反复点（线上日志可见同一动作被连点多次）。

本文件锁三件事：
  ① 本地判据必须在联网核对**之前**执行且立即 commit（联网那步炸了也不能回滚它）；
  ② 本地已处理的行不许再进联网核对（否则 dry_run 预览数字虚高）；
  ③ 前端预览阶段就要 loading，且 media 清理拆成「先秒清、再慢扫」两步。
"""
import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest

from app import main, mirror
from app.db import SessionLocal, migrate
from app.models import MirrorFile, MirrorMedia, MirrorRule

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")

TABLES = (MirrorFile, MirrorMedia, MirrorRule)


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as s:
        for t in TABLES:
            s.query(t).delete()
        s.commit()
    yield
    with SessionLocal() as s:
        for t in TABLES:
            s.query(t).delete()
        s.commit()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as s:
        yield s


def _rule(db) -> MirrorRule:
    r = MirrorRule(name="r", source_root_id="A", target_root_id="B", enabled=True)
    db.add(r)
    db.commit()
    return r


def _media(db, rule, title, *, status="failed", n_synced=0, n_failed=0) -> int:
    """造一部映射。`n_synced` 是已同步文件行数，`n_failed` 是失败文件行数。"""
    m = MirrorMedia(rule_id=rule.id, work_key=f"work:{title}", title_standard=title,
                    status=status, target_dir_id="B-dir-1")
    db.add(m)
    db.flush()
    for i in range(n_synced):
        db.add(MirrorFile(media_id=m.id, rule_id=rule.id,
                          file_name=f"s{i}.mkv", status="synced"))
    for i in range(n_failed):
        db.add(MirrorFile(media_id=m.id, rule_id=rule.id,
                          file_name=f"f{i}.mkv", status="failed"))
    db.commit()
    return int(m.id)


# ---------------------------------------------------------------- 后端：顺序
def test_local_rows_removed_before_b_sweep(db):
    """本地判据命中的行先删掉，且**不再**送进联网核对（skip_ids 生效）。"""
    r = _rule(db)
    dead = _media(db, r, "零文件僵尸", status="failed")           # 本地判据命中
    live = _media(db, r, "B库已删但有映射", status="synced", n_synced=1)  # 只能联网发现

    seen = []

    async def fake(api, media):
        seen.append(int(media.id))
        return {}, True

    with patch.object(mirror, "GuangYaWebFiles", return_value=object()), \
         patch.object(mirror, "_b_file_candidates", new=fake):
        out = asyncio.run(main.mirror_media_clear_invalid_async(dry_run=False, db=db))

    assert out["local_media"] == 1
    assert out["b_deleted"] == 1
    assert out["removed_media"] == 2
    assert dead not in seen, "本地已删的行不该再去联网核对"
    assert live in seen, "有 synced 文件的行只能靠联网核对发现"
    assert db.get(MirrorMedia, dead) is None
    assert db.get(MirrorMedia, live) is None


def test_local_deletion_survives_b_sweep_failure(db):
    """反证：联网核对整段抛异常时，本地那批删除**必须已经生效**。

    这正是改序前丢掉的东西——原来本地删除排在整批核对之后，核对一炸，
    用户看到的「清掉了 0 条」和「一条没删」等价。
    """
    r = _rule(db)
    dead = _media(db, r, "零文件僵尸", status="failed")
    _media(db, r, "有映射的作品", status="synced", n_synced=1)

    async def boom(*a, **kw):
        raise RuntimeError("B 库核对整体失败")

    with patch.object(mirror, "prune_missing_media_rows", new=boom):
        out = asyncio.run(main.mirror_media_clear_invalid_async(dry_run=False, db=db))

    assert db.get(MirrorMedia, dead) is None, "本地判据的删除不该被联网失败回滚"
    assert out["local_media"] == 1
    assert out["skipped_unverified"] == -1, "核对整体失败要显式标记，不许冒充成功"
    assert any("B 库核对失败" in e for e in out["errors"])


def test_deep_false_never_touches_b_library(db):
    """deep=False 只做本地判据：一次列目录都不许发。"""
    r = _rule(db)
    dead = _media(db, r, "零文件僵尸", status="failed")
    keep = _media(db, r, "有映射的作品", status="synced", n_synced=1)

    called = []

    async def fake(api, media):
        called.append(int(media.id))
        return {}, True

    with patch.object(mirror, "GuangYaWebFiles", return_value=object()), \
         patch.object(mirror, "_b_file_candidates", new=fake):
        out = asyncio.run(main.mirror_media_clear_invalid_async(
            dry_run=False, deep=False, db=db))

    assert called == [], "deep=False 不许联网"
    assert out["deep"] is False
    assert out["b_checked"] == 0
    assert db.get(MirrorMedia, dead) is None
    assert db.get(MirrorMedia, keep) is not None, "有 synced 文件的行本地判据不该碰"


def test_ids_only_touch_given_rows(db):
    """ids 定向：此前 media 分支完全忽略 p.ids，勾了行也白勾。"""
    r = _rule(db)
    a = _media(db, r, "僵尸甲", status="failed")
    b = _media(db, r, "僵尸乙", status="failed")

    out = asyncio.run(main.mirror_media_clear_invalid_async(
        dry_run=False, deep=False, ids=[a], db=db))

    assert out["local_media"] == 1
    assert db.get(MirrorMedia, a) is None
    assert db.get(MirrorMedia, b) is not None


def test_dry_run_counts_each_row_once(db):
    """dry_run 预览不许把同一行算两次（本地一次 + 联网一次）。"""
    r = _rule(db)
    _media(db, r, "零文件僵尸", status="failed")
    _media(db, r, "B库已删但有映射", status="synced", n_synced=1)

    seen = []

    async def fake(api, media):
        seen.append(int(media.id))
        return {}, True

    with patch.object(mirror, "GuangYaWebFiles", return_value=object()), \
         patch.object(mirror, "_b_file_candidates", new=fake):
        out = asyncio.run(main.mirror_media_clear_invalid_async(dry_run=True, db=db))

    assert out["local_media"] == 1
    assert out["b_deleted"] == 1
    assert out["removed_media"] == 2, "2 条就是 2 条，不许变 3"
    assert len(seen) == 1, "本地命中的那部不该再核一次"
    assert db.query(MirrorMedia).count() == 2, "dry_run 不写库"


def test_prune_missing_skips_do_not_eat_the_200_slots(db):
    """skip_ids 要在 `max_media` 截断**之前**排除，否则白占名额、真正该核的扫不到。"""
    r = _rule(db)
    a = _media(db, r, "甲", status="synced", n_synced=1)
    b = _media(db, r, "乙", status="synced", n_synced=1)
    c = _media(db, r, "丙", status="synced", n_synced=1)

    seen = []

    async def fake(api, media):
        seen.append(int(media.id))
        return {"f": "x.mkv"}, True

    with patch.object(mirror, "GuangYaWebFiles", return_value=object()), \
         patch.object(mirror, "_b_file_candidates", new=fake):
        asyncio.run(mirror.prune_missing_media_rows(
            db, r, dry_run=True, max_media=1, skip_ids=[c, b]))

    assert seen == [a], f"跳过的行不该占名额，应核到 {a}，实际 {seen}"


def test_prune_missing_ids_limits_scope(db):
    r = _rule(db)
    a = _media(db, r, "甲", status="synced", n_synced=1)
    _media(db, r, "乙", status="synced", n_synced=1)

    seen = []

    async def fake(api, media):
        seen.append(int(media.id))
        return {"f": "x.mkv"}, True

    with patch.object(mirror, "GuangYaWebFiles", return_value=object()), \
         patch.object(mirror, "_b_file_candidates", new=fake):
        asyncio.run(mirror.prune_missing_media_rows(db, r, dry_run=True, ids=[a]))

    assert seen == [a]


# ---------------------------------------------------------------- 前端：别像死按钮
def _fn_src(name: str, span: int = 3000) -> str:
    i = APP_VUE.index(f"async function {name}")
    return APP_VUE[i:i + span]


def test_preview_stage_sets_busy_before_request():
    """预览（dry_run）本身就可能跑 1-2 分钟，busy 必须在请求**之前**置位。"""
    seg = _fn_src("runMirrorCleanup")
    assert seg.index("mirrorBusy.value = true") < seg.index("await api('/mirror/cleanup'")


def test_clear_invalid_media_is_two_stage():
    """先本地秒清、再确认、再逐部核对 B 库；取消时本地那步也不许落地。"""
    seg = _fn_src("clearInvalidMedia", 3600)
    assert "deep: false" in seg, "第一步必须走本地判据"
    assert "deep: true" in seg, "第二步才逐部核对 B 库"
    assert "1–2 分钟" in seg, "确认框要说清耗时，否则用户还会以为卡死"
    assert seg.index("confirmBox") < seg.index("dry_run: false"), \
        "确认框必须在真正写库之前"
    assert seg.count("mirrorBusy.value = true") >= 2


def test_clear_invalid_media_keeps_preview_before_delete():
    """第一步的写库必须排在 dry_run 预览之后，取消就什么都不发生。"""
    seg = _fn_src("clearInvalidMedia", 3600)
    assert seg.index("dry_run: true") < seg.index("dry_run: false")
