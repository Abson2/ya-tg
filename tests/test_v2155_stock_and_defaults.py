"""v2.1.55：库存列 / 无作品自动关闭 / 恢复默认回到单段 专项验收。

## 用户口径（2026-11-27 真机反馈）

  1. 去掉「添加排期规则」入口，分类保持官方全部分类；
  2. 要加时段就点该行的「+ 时段」，不另开一个「加分类」的弹窗；
  3. 分类无作品时通过**不启用排期**减少空窗 —— 无作品的分类自动关闭、
     有作品时自动打开，空出来的时段分给其他分类；
  4. 勾选后会被立刻取消勾选（已修：模板里调函数导致 el-table 换数据源）；
  5. **恢复默认**要保留内置的分类推送时段，去掉同分类多时段；
  6. 表里增加一列「库存」，数字取资源库的在库待排作品数。

## 本文件锁住的行为

  · `_apply_stock_enabled` 的三条边界（全库零库存不关 / 只开系统关的 / 不动时段上限）；
  · 建行、按库存重排、批量采纳、恢复默认四处都按库存启停；
  · 「恢复默认」= 内置单段 + 清空额外时段（不许再制造多时段）；
  · 库存数字与 `_category_backlog` 同谓词，且 `/sc/category-works` 明细与之对得上；
  · 前端不再有「添加排期规则」、勾选走稳定引用、库存列存在。
"""
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

APP_VUE = Path(__file__).resolve().parents[1] / "frontend" / "src" / "App.vue"


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v2155stock_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'stock.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _res(db, *, file_id, name, genres, media_type="tv"):
    from app.models import Resource
    r = Resource(
        file_id=file_id, name=name, path_hint=name, parent_id="p1",
        size=1024, ext=".mkv", tmdb_id=1, media_type=media_type,
        media_title=name, poster_path="/p.jpg",
        genres=json.dumps(genres, ensure_ascii=False) if genres else "",
        identify_status="identified", share_status="not_created", vote_average=7.5,
    )
    db.add(r)
    db.flush()
    return r


def _seed_stock(db, *, n_action=2, n_comedy=1):
    """造「动作有 N 部、喜剧有 M 部、其余分类零库存」的资源库。"""
    from app import share_center_v2 as sc2
    for i in range(n_action):
        _res(db, file_id=f"a{i}", name=f"动作片{i}", genres=["动作"])
    for i in range(n_comedy):
        _res(db, file_id=f"c{i}", name=f"喜剧片{i}", genres=["喜剧"])
    sc2.sync_library_versions(db)
    db.commit()


def _sched(db, version_id, key):
    from app.models import SCCategorySchedule
    return db.scalar(select(SCCategorySchedule).where(
        SCCategorySchedule.plan_version_id == version_id,
        SCCategorySchedule.category_key == key))


# ===========================================================================
# 一、_apply_stock_enabled 的三条边界
# ===========================================================================
def test_closes_zero_stock_and_flags_it(env):
    """无作品 → 关闭 + 打上「系统关的」标记（有货时系统才敢自己打开）。"""
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        v = sc2._active_version(db)
        rows = sc2._ensure_schedules(db, v.id)
        stock = {"genre:动作": 3}
        out = sc2._apply_stock_enabled(rows, stock, known=True)
        assert out["closed"] > 0
        closed = [r for r in rows if not r.enabled]
        assert closed, "零库存分类应被关闭"
        assert all(int(r.auto_disabled) == 1 for r in closed)
        act = _sched(db, v.id, "genre:动作")
        assert act.enabled and int(act.auto_disabled or 0) == 0


def test_unknown_stock_closes_nothing(env):
    """全库零库存（资源库还没同步）= 一个都不许关，否则新用户看到整表关闭。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        v = sc2._active_version(db)
        rows = sc2._ensure_schedules(db, v.id)
        before = [r.enabled for r in rows]
        out = sc2._apply_stock_enabled(rows, {}, known=False)
        assert out == {"closed": 0, "opened": 0}
        assert [r.enabled for r in rows] == before


def test_opens_only_what_system_closed(env):
    """有货时：系统关的自动打开；用户手工关的不许擅自打开。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        v = sc2._active_version(db)
        rows = sc2._ensure_schedules(db, v.id)
        sys_row = rows[0]
        sys_row.enabled, sys_row.auto_disabled = False, 1      # 系统关的
        user_row = rows[1]
        user_row.enabled, user_row.auto_disabled = False, 0     # 用户手工关的
        stock = {sys_row.category_key: 5, user_row.category_key: 5}
        out = sc2._apply_stock_enabled(rows, stock, known=True)
        assert out["opened"] == 1
        assert sys_row.enabled is True and int(sys_row.auto_disabled) == 0
        assert user_row.enabled is False, "用户手工关闭的行不许被系统打开"


def test_toggle_never_touches_windows_or_limits(env):
    """启停只动 enabled / auto_disabled，时段与上限一个字都不许变。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        v = sc2._active_version(db)
        rows = sc2._ensure_schedules(db, v.id)
        snap = {r.category_key: (r.window_start, r.window_end, str(r.extra_windows or ""),
                                 int(r.hourly_limit or 0), int(r.daily_limit or 0))
                for r in rows}
        sc2._apply_stock_enabled(rows, {"genre:动作": 9}, known=True)
        for r in rows:
            assert (r.window_start, r.window_end, str(r.extra_windows or ""),
                    int(r.hourly_limit or 0), int(r.daily_limit or 0)) == snap[r.category_key]


# ===========================================================================
# 二、建行 / 重排 / 采纳 三处都按库存启停
# ===========================================================================
def test_ensure_schedules_disables_categories_without_stock(env):
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        _seed_stock(db, n_action=2, n_comedy=1)
        v = sc2._active_version(db)
        sc2._ensure_schedules(db, v.id)
        db.commit()
        act = _sched(db, v.id, "genre:动作")
        com = _sched(db, v.id, "genre:喜剧")
        other = _sched(db, v.id, "genre:恐怖")
        assert act.enabled is True
        assert com.enabled is True
        assert other.enabled is False and int(other.auto_disabled) == 1


def test_retime_reports_closed_and_opened(env):
    """一键调度按库存重排后，回显里要能看到关了哪些/开了哪些。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        _seed_stock(db)
        v = sc2._active_version(db)
        sc2._ensure_schedules(db, v.id)
        for r in sc2._ensure_schedules(db, v.id):   # 全部先打开，模拟老数据
            r.enabled, r.auto_disabled = True, 0
        db.commit()
        out = sc2._retime_schedules_by_stock(db, v, None, None, sc2._category_stock(db))
        assert out["closed"] > 0, "零库存分类应被关闭"
        assert _sched(db, v.id, "genre:动作").enabled is True


def test_adopt_all_closes_zero_stock_and_reports(env):
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        _seed_stock(db)
        res = sc2.api_adopt_schedule_ai_all(sc2.Payload(payload={}), db)
        assert res["ok"] is True
        assert res["closed"] > 0
        assert res["adopted"] >= 1
        reasons = " ".join(x["reason"] for x in res["skipped_items"])
        assert "自动关闭" in reasons


# ===========================================================================
# 三、恢复默认 = 内置单段（去掉同分类多时段）
# ===========================================================================
def test_apply_defaults_writes_single_builtin_window(env):
    """恢复默认不许再按库存写额外时段 —— 用户口径 5。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        _seed_stock(db, n_action=6, n_comedy=1)   # 动作库存多，旧逻辑会给它多段
        v = sc2._active_version(db)
        rows = sc2._ensure_schedules(db, v.id)
        for r in rows:
            r.extra_windows = "[[0,2],[2,4]]"     # 先弄脏
        db.commit()
        out = sc2.api_apply_schedule_defaults(sc2.Payload(payload={"force": True}), db)
        assert out["ok"] is True
        got = list(db.scalars(select(sc2.SCCategorySchedule).where(
            sc2.SCCategorySchedule.plan_version_id == v.id)).all())
        assert all(not str(r.extra_windows or "") for r in got), "恢复默认后不该有多时段"
        act = _sched(db, v.id, "genre:动作")
        assert act.window_end > act.window_start
        assert act.enabled is True
        assert out["closed"] > 0, "零库存分类恢复默认后仍是关闭"


def test_reset_single_row_clears_extra_windows(env):
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        _seed_stock(db)
        v = sc2._active_version(db)
        sc2._ensure_schedules(db, v.id)
        row = _sched(db, v.id, "genre:动作")
        row.extra_windows = "[[4,6]]"
        db.commit()
        res = sc2.api_reset_category_schedule_default(
            sc2.Payload(payload={"id": row.id}), db)
        assert res["ok"] is True
        db.refresh(row)
        assert not str(row.extra_windows or "")
        assert row.enabled is True


# ===========================================================================
# 四、库存列 + 明细（同谓词）
# ===========================================================================
def test_schedules_carry_stock_number(env):
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        _seed_stock(db, n_action=3, n_comedy=1)
        data = sc2.api_category_schedules(db=db)
        by_key = {x["ref"]: x for x in data["items"]}
        assert by_key["genre:动作"]["stock"] == 3
        assert by_key["genre:喜剧"]["stock"] == 1
        assert by_key["genre:恐怖"]["stock"] == 0


def test_stock_detail_matches_the_number(env):
    """明细条数必须与那个数字一模一样（同一谓词，v2.1.36 红线）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        _seed_stock(db, n_action=3, n_comedy=1)
        data = sc2.api_category_schedules(db=db)
        for item in data["items"]:
            if not item["stock"]:
                continue
            detail = sc2.api_category_works(
                category_key=item["ref"], limit=200, db=db)
            assert detail["ok"] is True
            assert detail["total"] == item["stock"], (
                f"{item['name']} 计数 {item['stock']} 与明细 {detail['total']} 不一致")
            assert detail["items"][0]["work"]


def test_stock_detail_rejects_empty_key(env):
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        out = sc2.api_category_works(category_key="", db=db)
        assert out["ok"] is False and out["items"] == []


# ===========================================================================
# 五、前端：入口去掉 / 勾选稳定 / 库存列
# ===========================================================================
def _template() -> str:
    """只取模板区 —— 注释里解释「以前是什么样」时会出现旧写法，不该算违规。"""
    app = APP_VUE.read_text(encoding="utf-8")
    start = app.index("<template>")
    return app[start:app.rindex("</template>")]


def test_frontend_has_no_add_rule_entry():
    tpl = _template()
    assert "添加排期规则" not in tpl
    assert "scOpenAddRule" not in tpl


def test_frontend_table_uses_stable_data_reference():
    """勾选被立即清空的根因是模板里调函数 → 每次重渲染换数组。"""
    tpl = _template()
    app = APP_VUE.read_text(encoding="utf-8")
    assert ':data="scPrefSortRows(scSchedRows)"' not in tpl
    assert ':data="scSchedSorted"' in tpl
    assert 'row-key="ref"' in tpl
    assert "const scSchedSorted = computed(" in app


def test_frontend_has_stock_column_and_detail():
    tpl = _template()
    app = APP_VUE.read_text(encoding="utf-8")
    assert 'label="库存"' in tpl
    assert "scOpenStock" in tpl
    assert "/sc/category-works" in app


def test_frontend_marks_auto_closed_rows():
    tpl = _template()
    assert "s.row.auto_closed" in tpl
    assert "无作品，已自动关闭" in tpl


def test_frontend_default_confirm_mentions_single_window():
    app = APP_VUE.read_text(encoding="utf-8")
    assert "每个分类只占一个时段" in app
    assert "多出来的时段会被去掉" in app
