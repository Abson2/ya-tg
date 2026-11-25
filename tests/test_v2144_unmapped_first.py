"""v2.1.44 验收：单轮名额严格等于配置值 + 每轮接着上一轮游标往后走 + 未镜像优先。

真机问题（2026-11-25 只读实测）：
  A 库 713 部、mirror_media 只映射 261 部（452 部从未复制）。老口径把 max_works=20
  的名额按目录顺序切给"全部作品"，切到的 20 部大多是早已同步的老片 —— 它们的文件
  指纹全命中 synced_rows，逐部 `unchanged += 1; continue`，一部都不复制。用户看到
  「每轮说处理 20 部、实际只复制到 2 部」，全量永远推不完。

用户口径（两条硬要求）：
  ① 每轮必须**接着上一轮的游标往后走**，不能每轮从目录头部重扫；
  ② 单轮名额**就是配置的 max_works_per_run**，一部不多 —— 不另开「不占名额」的旁路批。

实现：`plan_work_batch` 增加可选 `mapped_keys` 参数。传入时从**游标之后**的环形顺序
里先收未镜像作品，收不满用已镜像存量按目录顺序补位，总长不超过 max_works；游标取
「本轮取到的最远那一部」，下一轮从它之后接着走。传 None 时完全保持 v0.9.0 旧语义。

覆盖：
  - 名额硬约束：len(batch) <= max_works，绝不出现 20+20
  - 游标连续：第二轮从第一轮结束处往后，不回头重复
  - 未镜像优先、存量补位、回绕取头部未镜像
  - 头阻塞消除：某部永远复制不成功也不会卡在队首反复占名额
  - 反证老 bug：20 部老片在前 + 20 部新片在后，同一名额里必须取到新片
  - 接线：sync_rule 传 mapped_keys、run_queue 无旁路批、故障降级退回旧口径
"""
import os
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

MIRROR_SRC = (BACKEND / "app" / "mirror.py").read_text(encoding="utf-8")
MAIN_SRC = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")
MAIN_SRC_A = MAIN_SRC
APP_VUE = (BACKEND.parent / "frontend" / "src" / "App.vue").read_text(encoding="utf-8")


def _works(names):
    """构造作品列表，格式与 _discover_works 输出一致（node, cats）二元组。"""
    return [({"id": "D-" + n, "name": n, "parent": "R", "path": n,
              "files": [], "children": []}, []) for n in names]


def _keys(mirror, pairs):
    return {mirror.work_key_of(p[0]) for p in pairs}


def _names(batch):
    return [w["name"] for w, _ in batch]


# ===========================================================================
# ① 名额硬约束：就是配置的单轮作品数，一部不多
# ===========================================================================

def test_quota_is_exactly_configured_value():
    from app import mirror
    works = _works([f"片{i:03d}" for i in range(100)])
    batch, _cur, _w = mirror.plan_work_batch(works, 20, "", mapped_keys=set())
    assert len(batch) == 20, "配置 20 部就只做 20 部，绝不允许批大于名额"


def test_quota_not_doubled_by_side_batch():
    """反证 v2.1.44 初版缺陷：新作品批另开旁路会让单轮变成 2× 配置量。"""
    from app import mirror
    works = _works([f"片{i:03d}" for i in range(100)])
    batch, _cur, _w = mirror.plan_work_batch(works, 20, "", mapped_keys=set())
    assert len(batch) <= 20
    # 源码层面：run_queue 里不允许再有独立的新作品旁路批
    assert "incr_works + priority_works + batch" in MIRROR_SRC
    assert "new_works + priority_works" not in MIRROR_SRC


def test_quota_zero_means_unlimited_legacy():
    from app import mirror
    works = _works([f"片{i}" for i in range(8)])
    batch, cursor, wrapped = mirror.plan_work_batch(works, 0, "some-old-cursor",
                                                   mapped_keys=set())
    assert len(batch) == 8 and cursor == "" and wrapped is False


# ===========================================================================
# ② 游标连续：每轮接着上一轮往后走，不从头重扫
# ===========================================================================

def test_cursor_continues_round_after_round():
    from app import mirror
    works = _works([f"片{i:03d}" for i in range(100)])
    seen = []
    cursor = ""
    for _ in range(3):
        batch, cursor, _w = mirror.plan_work_batch(works, 20, cursor,
                                                  mapped_keys=set())
        seen.extend(_names(batch))
    # 三轮共 60 部，且完全不重复：0-59 依次推进
    assert seen == [f"片{i:03d}" for i in range(60)]
    assert len(set(seen)) == 60, "每轮都从头开始会导致大量重复"


def test_second_round_starts_after_first_round_cursor():
    from app import mirror
    works = _works(["甲", "乙", "丙", "丁", "戊"])
    b1, c1, _ = mirror.plan_work_batch(works, 2, "", mapped_keys=set())
    assert _names(b1) == ["甲", "乙"]
    assert c1 == mirror.work_key_of(works[1][0])
    b2, _, _ = mirror.plan_work_batch(works, 2, c1, mapped_keys=set())
    assert _names(b2) == ["丙", "丁"], "第二轮必须从上一轮结束处往后，不能回到甲"


def test_first_round_copies_works_that_became_mapped_midway():
    """真实时序：第一轮复制完的会变成已映射，第二轮仍要往后走而不是重取它们。"""
    from app import mirror
    works = _works([f"片{i:02d}" for i in range(12)])
    b1, c1, _ = mirror.plan_work_batch(works, 5, "", mapped_keys=set())
    mapped = _keys(mirror, b1)          # 第一轮复制成功 → 变成已映射
    b2, c2, _ = mirror.plan_work_batch(works, 5, c1, mapped_keys=mapped)
    assert _names(b2) == [f"片{i:02d}" for i in range(5, 10)]
    assert c2 == mirror.work_key_of(works[9][0])


# ===========================================================================
# 未镜像优先 / 存量补位 / 回绕
# ===========================================================================

def test_unmapped_first_within_quota():
    from app import mirror
    works = _works([f"片{i:02d}" for i in range(10)])
    mapped = _keys(mirror, works[:5])        # 前 5 部已镜像
    batch, _c, _w = mirror.plan_work_batch(works, 6, "", mapped_keys=mapped)
    # 名额 6：先把未镜像的 5-9 收进来，再用存量 0 补一位
    assert _names(batch) == [f"片{i:02d}" for i in range(5, 10)] + ["片00"]


def test_stock_tops_up_without_exceeding_quota():
    from app import mirror
    works = _works([f"片{i:02d}" for i in range(30)])
    mapped = _keys(mirror, works[:27])       # 只有 27-29 未镜像
    batch, cur, _w = mirror.plan_work_batch(works, 10, "", mapped_keys=mapped)
    assert len(batch) == 10, "未镜像不够时必须用存量补满名额，不能只做 3 部"
    assert _names(batch)[:3] == ["片27", "片28", "片29"]
    assert cur == mirror.work_key_of(works[6][0]), "游标取本轮最远那一部"


def test_wrap_picks_unmapped_from_head():
    """游标已到尾部、未镜像却在头部时，回绕要能取到它们。"""
    from app import mirror
    works = _works([f"片{i}" for i in range(10)])
    mapped = _keys(mirror, works) - _keys(mirror, [works[1], works[2]])
    cur = mirror.work_key_of(works[7][0])
    batch, _c, _w = mirror.plan_work_batch(works, 4, cur, mapped_keys=mapped)
    names = _names(batch)
    assert "片1" in names and "片2" in names, "回绕后仍未镜像优先"
    assert len(batch) == 4


def test_no_head_of_line_blocking():
    """某部永远复制不成功（始终未映射）也不能卡在队首反复吃名额。"""
    from app import mirror
    works = _works([f"片{i}" for i in range(10)])
    stuck = _keys(mirror, [works[0]])
    b1, c1, _ = mirror.plan_work_batch(works, 4, "", mapped_keys=set())
    assert _names(b1) == ["片0", "片1", "片2", "片3"]
    # 第一轮只有片1-3 成功，片0 卡住（仍不在 mapped 里）
    mapped = _keys(mirror, b1) - stuck
    b2, _c2, _ = mirror.plan_work_batch(works, 4, c1, mapped_keys=mapped)
    assert "片0" not in _names(b2), "卡住的作品不能每轮都从队首重占名额"
    assert _names(b2) == ["片4", "片5", "片6", "片7"]


# ===========================================================================
# 反证老 bug：老片在前、新片在后
# ===========================================================================

def test_old_bug_scenario_now_picks_new_works():
    """20 部已映射老片在前 + 20 部未镜像新片在后，名额 20。

    老口径（mapped_keys=None）只会切到前 20 部老片 → 指纹全命中 → 一部都不复制；
    新口径必须在**同一个名额**里优先取到那 20 部新片。
    """
    from app import mirror
    old = _works([f"老片{i:02d}" for i in range(20)])
    new = _works([f"新片{i:02d}" for i in range(20)])
    works = old + new
    mapped = _keys(mirror, old)

    legacy, _c, _w = mirror.plan_work_batch(works, 20, "")
    assert _names(legacy) == _names(old), "老口径确实只切到老片——这是真机病根"

    batch, cur, _w = mirror.plan_work_batch(works, 20, "", mapped_keys=mapped)
    assert _names(batch) == _names(new)
    assert len(batch) == 20
    assert cur == mirror.work_key_of(works[39][0])


def test_legacy_semantics_untouched_when_mapped_keys_none():
    """默认参数必须与 v0.9.0 逐字一致，其他调用点零影响。"""
    from app import mirror
    works = _works([f"片{i}" for i in range(5)])
    cur = mirror.work_key_of(works[3][0])
    batch, new_cursor, wrapped = mirror.plan_work_batch(works, 3, cur)
    assert wrapped is True
    assert _names(batch) == ["片4", "片0", "片1"]
    assert new_cursor == mirror.work_key_of(works[1][0])


def test_empty_works_is_safe():
    from app import mirror
    assert mirror.plan_work_batch([], 3, "", mapped_keys=set()) == ([], "", False)


# ===========================================================================
# 接线验证（源码级，防止被改回旁路批 / 头部重扫）
# ===========================================================================

def test_sync_rule_passes_mapped_keys_into_batch():
    assert "hard_max=max_media, mapped_keys=mapped_keys)" in MIRROR_SRC


def test_no_side_batch_anymore():
    """红线：不能再有「不占名额」的新作品旁路批。"""
    assert "select_new_works(" not in MIRROR_SRC
    assert "new_works, new_left = " not in MIRROR_SRC


def test_cursor_still_persisted():
    assert "rule.works_cursor = new_cursor" in MIRROR_SRC


def test_mapping_query_failure_falls_back_to_legacy():
    """故障降级：映射清单查不到时 mapped_keys=None，退回纯游标轮转，不重复制全库。"""
    assert "mapped_keys = None" in MIRROR_SRC
    assert "本轮退回纯游标轮转" in MIRROR_SRC


def test_last_message_reports_new_works_within_quota():
    assert "其中新作品" in MIRROR_SRC
    assert "new_works_left" in MIRROR_SRC
    # 新作品在名额之内：不能再有「新作品优先 N 部（不占限额）」这种旁路口径文案
    assert "新作品优先" not in MIRROR_SRC


# ===========================================================================
# 占不占名额的分野（用户口径）
#   增量（已镜像作品内有变化）→ 队首优先、**不占**名额
#   全新作品 → 按游标顺序、**占**名额
#   剩余作品 → 名额没填满时按游标补位
# ===========================================================================

def test_increment_does_not_consume_round_quota():
    """增量只受自己的上限约束，不进 batch 的名额核算。"""
    # 增量单独截断（不与 batch 共用名额）
    assert "if max_works > 0 and len(incr_works) > max_works:" in MIRROR_SRC
    # 切批时传给 plan_work_batch 的仍是完整 max_works，没有被增量扣减
    assert "works, max_works, str(getattr(rule, \"works_cursor\", \"\") or \"\")" in MIRROR_SRC


def test_round_queue_order_is_increment_first_then_batch():
    """队首是增量，其后是修补批，最后才是占名额的本批。"""
    assert "for _pair in incr_works + priority_works + batch:" in MIRROR_SRC


def test_new_works_take_quota_slots():
    """全新作品占名额：未镜像作品多到填满名额时，批里全是新作品且正好 20 部。"""
    from app import mirror
    works = _works([f"片{i:03d}" for i in range(50)])
    batch, _c, _w = mirror.plan_work_batch(works, 20, "", mapped_keys=set())
    assert len(batch) == 20, "名额就是 20，新作品占满即止，不再额外追加"
    assert _names(batch) == [f"片{i:03d}" for i in range(20)]
    # 名额填满后没有被追加第 21 部——说明「占名额」而不是「另开旁路」


# ===========================================================================
# 看板必须同时有 A 库与 B 库（用户口径：只有 B 库的、整体镜像进度看不到）
# ===========================================================================

def _fresh_db():
    from app.db import SessionLocal, migrate
    migrate()
    return SessionLocal()


def test_snapshot_a_works_not_wiped_by_stage_frames():
    """根因：stage=copy/skipped/failed 等帧不传 a_works，默认 0 会覆盖掉 A 库分母。"""
    src = MAIN_SRC
    assert "_prev_snap.get(\"a_works\")" in src, (
        "快照写 a_works 前必须先读旧快照沿用，否则作品级分母会被抹成 0")
    assert "if not a_works:" in src


def test_levels_overall_uses_a_library_manifest(monkeypatch):
    from app import main
    db = _fresh_db()
    try:
        monkeypatch.setattr(main, "_a_work_items", lambda db, rid=0: [
            {"id": f"A{i}", "name": f"片{i}"} for i in range(10)])
        monkeypatch.setattr(main, "_mirror_mapped_source_ids",
                            lambda db: {"A0", "A1", "A2"})
        lv = main._mirror_levels(db, {"a_works": 713}, 1)
        ov = lv["overall"]
        assert ov["a_total"] == 10, "有 A 库清单时以清单为准，不吃快照旧值"
        assert ov["unmapped"] == 7
        assert ov["a_known"] is True
        assert lv["work"]["total"] == 10
        assert lv["work"]["total_label"] == "A 库作品（目录清单）"
        _w = {it["key"]: it["value"] for it in lv["work"]["items"]}
        assert _w["a_total"] == 10 and _w["unmapped"] == 7
    finally:
        db.close()


def test_detail_a_side_numbers_match_board(monkeypatch):
    """红线：页面上的 A 库数字点开，条数必须与数字完全一致。"""
    from app import main
    db = _fresh_db()
    try:
        items = [{"id": f"A{i}", "name": f"片{i}"} for i in range(10)]
        monkeypatch.setattr(main, "_a_work_items", lambda db, rid=0: items)
        monkeypatch.setattr(main, "_mirror_mapped_source_ids",
                            lambda db: {"A0", "A1", "A2"})
        lv = main._mirror_levels(db, {}, 1)
        for scope in ("a_total", "unmapped"):
            d = main.mirror_level_detail("work", scope, 200, db)
            expect = {it["key"]: it["value"] for it in lv["work"]["items"]}[scope]
            assert d["total"] == expect, f"{scope} 计数与明细不同源"
        d = main.mirror_level_detail("work", "unmapped", 200, db)
        assert {r["title"] for r in d["rows"]} == {f"片{i}" for i in range(3, 10)}
    finally:
        db.close()


def test_detail_a_side_without_manifest_is_honest(monkeypatch):
    """A 库清单还没建立时不许编造明细，明确报错让用户先同步一次。"""
    from fastapi import HTTPException
    from app import main
    db = _fresh_db()
    try:
        monkeypatch.setattr(main, "_a_work_items", lambda db, rid=0: None)
        try:
            main.mirror_level_detail("work", "unmapped", 200, db)
        except HTTPException as e:
            assert e.status_code == 409 and "目录清单尚未建立" in str(e.detail)
        else:
            raise AssertionError("没有 A 库清单时必须报 409，不能返回假明细")
    finally:
        db.close()


def test_levels_fall_back_honestly_without_a_library():
    """既没清单也没 a_works 时，标签必须写明是「映射清单作品」，不许冒充 A 库。"""
    from app import main
    db = _fresh_db()
    try:
        main._A_WORK_ITEMS_CACHE.clear()
        lv = main._mirror_levels(db, {}, 999999)   # 不存在的规则 → 无清单
        assert lv["work"]["total_label"] == "映射清单作品"
        assert lv["overall"]["a_known"] is False
    finally:
        db.close()


def test_a_work_items_rebuilt_from_manifest(monkeypatch):
    """作品清单必须复用 mirror._discover_works 的判定，不许另写一套近似 SQL。"""
    assert "mirror._discover_works(nodes, root)" in MAIN_SRC_A
    assert "load_dir_manifest" in MAIN_SRC_A


def test_frontend_shows_overall_progress():
    vue = APP_VUE
    assert "整体镜像进度" in vue
    assert "mirrorStatus.levels.overall" in vue
    assert "A 库共" in vue and "未镜像" in vue
    assert "分母口径" in vue


def test_a_work_items_end_to_end_from_manifest():
    """真造一棵 A 库目录树，验证作品清单与未镜像数是从清单真算出来的。"""
    import json
    from app import main
    from app.db import SessionLocal, migrate
    from app.models import MirrorDirScan, MirrorMedia, MirrorRule

    migrate()
    db = SessionLocal()
    rule = None
    try:
        rule = MirrorRule(name="v2144-e2e", source_root_id="R")
        db.add(rule)
        db.commit()
        db.refresh(rule)

        def _vid(n):
            return [{"fileId": f"f-{n}", "name": f"{n}.mkv",
                     "utime": 1, "fileSize": 100, "resType": 1}]

        rows = [
            ("V2144-R", "", "根", 0, [], ["V2144-W1", "V2144-W2"]),
            ("V2144-W1", "V2144-R", "剧甲", 1, _vid("E01"), []),
            ("V2144-W2", "V2144-R", "剧乙", 1, _vid("E02"), []),
        ]
        for did, parent, name, depth, files, children in rows:
            db.add(MirrorDirScan(
                rule_id=rule.id, dir_id=did, parent_id=parent, name=name,
                path=name, depth=depth, utime=1, ctime=1,
                files_json=json.dumps(files, ensure_ascii=False),
                children_json=json.dumps(children, ensure_ascii=False),
                n_files=len(files), n_dirs=len(children)))
        db.commit()

        main._A_WORK_ITEMS_CACHE.clear()
        items = main._a_work_items(db, rule.id)
        assert items is not None, "有清单时必须能重建出作品清单"
        assert {x["name"] for x in items} == {"剧甲", "剧乙"}
        assert {x["id"] for x in items} == {"V2144-W1", "V2144-W2"}

        # 映射了 W1 → 未镜像只剩 W2
        db.add(MirrorMedia(rule_id=rule.id, work_key="work:剧甲",
                           title_standard="剧甲", source_dir_id="V2144-W1",
                           target_dir_id="B1", status="synced"))
        db.commit()
        main._A_WORK_ITEMS_CACHE.clear()
        lv = main._mirror_levels(db, {}, rule.id)
        assert lv["work"]["total"] == 2
        assert lv["overall"]["unmapped"] == 1
        d = main.mirror_level_detail("work", "unmapped", 200, db, rule.id)
        assert d["total"] == 1 and d["rows"][0]["title"] == "剧乙"
    finally:
        try:
            db.query(MirrorMedia).filter(
                MirrorMedia.rule_id == (rule.id if rule else -1)).delete()
            db.query(MirrorDirScan).filter(
                MirrorDirScan.rule_id == (rule.id if rule else -1)).delete()
            db.query(MirrorRule).filter(
                MirrorRule.id == (rule.id if rule else -1)).delete()
            db.commit()
        except Exception:
            db.rollback()
        main._A_WORK_ITEMS_CACHE.clear()
        db.close()
