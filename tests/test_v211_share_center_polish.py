"""v2.1.1 分享中心打磨验收：默认建议值 + 数据异常根因修复 + 单一分享中心。

覆盖本轮 5 个后端根因与 3 组前端契约：
  1. 排期内置默认建议值：分档默认值、存量回填、一键应用、单行恢复、enabled 不误停用；
  2. 推送清单按日复用（不再每轮新建 → 真机一天 4000+ 条空清单）；
  3. 分类占比按真实归属过滤（不再「剧情 100%」）；
  4. 代表资源快照失效时按 work_key 实时重解析（不再全部「代表资源不存在」）；
  5. 趋势图按粒度分桶 + 单点补零桶（不再单月单点尖峰）；
  6. 分享管理统计口径统一 + 独立真实端点 /sc/share-records；
  7. 前后端仅用一套分享中心：统一总开关，旧链降级为内部工具库；
  8. 界面不出现后端代码名词（work_key / link_state / v2 / 旧链）。
"""
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

ROOT = Path(__file__).resolve().parents[1]
SH = timezone(timedelta(hours=8))


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v211_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'v211.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _resource(db, file_id="f1", name="Show (2024) {tmdb-1}/Show.S01E01.mkv",
              media_type="tv", title="剧集名", genres='[{"id": 18, "name": "Drama"}]'):
    from app.models import Resource
    r = Resource(
        file_id=file_id, name=name, ext=".mkv", size=100, utime=1,
        identify_status="identified", media_type=media_type, tmdb_id=1,
        media_title=title, path_hint="Show (2024)",
        share_dir_id="dir_show", share_scope="series_dir", share_dir_name="Show",
        genres=genres, poster_path="/poster.jpg",
    )
    db.add(r)
    db.commit()
    return r


def _channel(db, chat_id="-1001"):
    from app.models import Bot, Channel
    bot = db.scalar(select(Bot))
    if not bot:
        bot = Bot(name="test", token="enc", enabled=True)
        db.add(bot)
        db.flush()
    ch = Channel(bot_id=bot.id, name="group", chat_id=chat_id, enabled=True, kind="group")
    db.add(ch)
    db.commit()
    return ch


# ---------------------------------------------------------------------------
# 1. 排期内置默认建议值
# ---------------------------------------------------------------------------

def test_default_schedule_by_media_type(env):
    """默认建议值按 TMDB 媒体类型分档，且上下限都非 0（0 = 不限 = 形同虚设）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        movie = sc2._default_schedule_for("movie")
        tv = sc2._default_schedule_for("tv")
        # v2.1.2：时段默认由**偏好分配**决定（高峰独占段或全天窗口），
        # 未传窗口时兜底为全天 0-24；分档差异体现在速率上限：
        # 电视量大、电影量小。
        assert movie == (0, 24, 3, 20), "电影档：3 条每时 / 20 条每日（不限时段）"
        assert tv == (0, 24, 5, 30), "电视档：5 条每时 / 30 条每日（不限时段）"
        for value in (movie, tv):
            ws, we, hl, dl = value
            assert we > ws, "时段必须为正长度"
            assert hl > 0 and dl > 0, "每小时/每日上限不得为 0（0 = 不限）"
        # 未知媒体类型按电影档兜底，不返回 0/0
        assert sc2._default_schedule_for("") == movie
        # 传入高峰独占段时，时段跟随窗口走，上限仍按媒体类型分档
        assert sc2._default_schedule_for("movie", (20, 22)) == (20, 22, 3, 20)
        assert sc2._default_schedule_for("tv", (18, 20)) == (18, 20, 5, 30)
        # 全部 27 个官方分类都能取到非零默认档
        catalog = sc2._tmdb_genre_catalog()
        assert len(catalog) == 27
        for cat in catalog:
            ws, we, hl, dl = sc2._default_schedule_for(cat["media_type"])
            assert hl > 0 and dl > 0, f"{cat['name']} 的默认上限不得为 0"


def test_ensure_schedules_backfills_untouched_rows(env):
    """从未配置过的行回填默认建议值；用户改过的行不被覆盖。"""
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        assert version is not None

        tv_key = "genre:动作冒险"        # tv 档
        movie_key = "genre:剧情"          # movie 档
        custom_key = "genre:科幻"         # 用户自定义过的分类

        # 模拟真机存量：初始 8-24 + 0/0（从未配置过，window_auto 为默认真）
        db.add(SCCategorySchedule(plan_version_id=version.id, category_key=tv_key,
                                  window_start=8, window_end=24,
                                  hourly_limit=0, daily_limit=0, enabled=False))
        db.add(SCCategorySchedule(plan_version_id=version.id, category_key=movie_key,
                                  window_start=8, window_end=24,
                                  hourly_limit=0, daily_limit=0, enabled=False))
        db.commit()

        # 用户已经配置过的行：走**真实用户保存路径**（api_upsert_category_schedule），
        # 该路径会把 window_auto 置 0 —— 这才是「用户手工设定过」的权威标记。
        # v2.1.2：不能用 db.add 直插来模拟用户改过的行：window_auto 列默认 1，
        # 直插出来的行会被误判成「系统托管」，从而被高峰分配覆盖时段。
        res = sc2.api_upsert_category_schedule(sc2.Payload(payload={
            "plan_version_id": version.id, "category_key": custom_key,
            "window_start": 20, "window_end": 22,
            "hourly_limit": 2, "daily_limit": 9, "enabled": True,
        }), db)
        assert res["ok"] is True

        windows = sc2._preference_windows(db)
        rows = sc2._ensure_schedules(db, version.id)
        by_key = {r.category_key: r for r in rows}

        tv_row = by_key[tv_key]
        tv_ws, tv_we = windows.get(tv_key, sc2.FULL_DAY_WINDOW)
        assert (tv_row.window_start, tv_row.window_end) == (tv_ws, tv_we)
        assert (tv_row.hourly_limit, tv_row.daily_limit) == (5, 30)
        assert tv_row.enabled is True, "回填后该分类的排期必须真正生效"

        movie_row = by_key[movie_key]
        mv_ws, mv_we = windows.get(movie_key, sc2.FULL_DAY_WINDOW)
        assert (movie_row.window_start, movie_row.window_end) == (mv_ws, mv_we)
        assert (movie_row.hourly_limit, movie_row.daily_limit) == (3, 20)

        custom_row = by_key[custom_key]
        assert (custom_row.window_start, custom_row.window_end) == (20, 22)
        assert (custom_row.hourly_limit, custom_row.daily_limit) == (2, 9), \
            "用户改过的行不得被默认建议值覆盖"
        assert custom_row.window_auto is False, "用户手工保存的行必须脱离系统托管"

        # 新建行（此前不存在）直接带默认值并默认启用
        new_key = "genre:恐怖"
        new_row = by_key[new_key]
        new_ws, new_we = windows.get(new_key, sc2.FULL_DAY_WINDOW)
        assert (new_row.window_start, new_row.window_end) == (new_ws, new_we)
        assert new_row.hourly_limit == 3 and new_row.daily_limit == 20
        assert new_row.enabled is True, "新建排期行默认启用，避免流程跑不通"


def test_ensure_schedules_keeps_legacy_manual_window(env):
    """老库迁移场景：window_auto 因 ALTER 补列默认成 1，用户手工时段仍不得被覆盖。"""
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        key = "genre:科幻"
        # 老库形态：window_auto=1（ALTER DEFAULT），但上限是用户自己配的 2/9，
        # 时段也是用户手工设的 21-23（不属于任何系统分配过的窗口形态）。
        db.add(SCCategorySchedule(plan_version_id=version.id, category_key=key,
                                  window_start=21, window_end=23,
                                  hourly_limit=2, daily_limit=9, enabled=True,
                                  window_auto=True))
        db.commit()
        rows = sc2._ensure_schedules(db, version.id)
        row = next(r for r in rows if r.category_key == key)
        assert (row.window_start, row.window_end) == (21, 23), \
            "用户手工时段不得被系统高峰分配覆盖"
        assert (row.hourly_limit, row.daily_limit) == (2, 9)


def test_apply_defaults_endpoint(env):
    """一键应用默认建议：未配置过的行被写入并启用，已改过的行保持原样。"""
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        untouched_key = "genre:动作冒险"
        custom_key = "genre:恐怖"
        db.add(SCCategorySchedule(plan_version_id=version.id, category_key=untouched_key,
                                  window_start=8, window_end=24,
                                  hourly_limit=0, daily_limit=0, enabled=False))
        db.add(SCCategorySchedule(plan_version_id=version.id, category_key=custom_key,
                                  window_start=21, window_end=23,
                                  hourly_limit=1, daily_limit=4, enabled=False))
        db.commit()

        res = sc2.api_apply_schedule_defaults(sc2.Payload(payload={}), db)
        assert res["ok"] is True
        assert res["applied"] >= 1
        assert res["total"] == 27

        windows = sc2._preference_windows(db)
        untouched = db.scalar(select(SCCategorySchedule).where(
            SCCategorySchedule.plan_version_id == version.id,
            SCCategorySchedule.category_key == untouched_key))
        # v2.1.6：时段取**默认时间表**（热门白天 / 冷门夜间，2 小时一段）。
        u_ws, u_we = windows.get(untouched_key, sc2.FULL_DAY_WINDOW)
        assert (untouched.window_start, untouched.window_end) == (u_ws, u_we)
        # v2.1.6：分类级「每小时上限 / 日上限」已从分类表移除，
        # 速率统一由周期计划管理约束 → 一键应用**不回写**分类级上限；
        # 库中残留值仍是 v2.1.1 回填的默认档（历史兼容，调度已不消费）。
        assert (untouched.hourly_limit, untouched.daily_limit) == (5, 30)
        assert untouched.enabled is True, "一键应用必须同时启用该行"

        custom = db.scalar(select(SCCategorySchedule).where(
            SCCategorySchedule.plan_version_id == version.id,
            SCCategorySchedule.category_key == custom_key))
        assert (custom.window_start, custom.window_end) == (21, 23)
        assert (custom.hourly_limit, custom.daily_limit) == (1, 4), \
            "已改过的行不被一键应用覆盖"
        assert custom.enabled is False, "已改过的行不擅自启用"

        # force=True 时强制覆盖
        res2 = sc2.api_apply_schedule_defaults(sc2.Payload(payload={"force": True}), db)
        assert res2["ok"] is True
        db.expire_all()
        custom2 = db.scalar(select(SCCategorySchedule).where(
            SCCategorySchedule.plan_version_id == version.id,
            SCCategorySchedule.category_key == custom_key))
        c_ws, c_we = windows.get(custom_key, sc2.FULL_DAY_WINDOW)
        assert (custom2.window_start, custom2.window_end) == (c_ws, c_we)
        # v2.1.6：force 只覆盖**时段**，分类级上限不再被回写。
        assert (custom2.hourly_limit, custom2.daily_limit) == (1, 4)
        assert custom2.enabled is True


def test_reset_default_endpoint_single_row(env):
    """单行「默认」按钮：按媒体类型恢复内置默认建议值。"""
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        row = SCCategorySchedule(plan_version_id=version.id, category_key="genre:动作冒险",
                                 window_start=3, window_end=4,
                                 hourly_limit=99, daily_limit=99, ai_adopted=True)
        db.add(row)
        db.commit()
        res = sc2.api_reset_category_schedule_default(sc2.Payload(payload={"id": row.id}), db)
        assert res["ok"] is True
        item = res["item"]
        # v2.1.6：恢复默认的时段取**默认时间表**窗口（热门白天 / 冷门夜间）。
        r_ws, r_we = sc2._preference_windows(db).get(row.category_key, sc2.FULL_DAY_WINDOW)
        assert (item["window_start"], item["window_end"]) == (r_ws, r_we)
        # v2.1.6：分类级上限已移除 → 恢复默认不改上限（原值保留）。
        assert (item["hourly_limit"], item["daily_limit"]) == (99, 99)
        assert item["ai_adopted"] is False, "恢复默认应清除 AI 采纳标记"

        # 电影档单行恢复
        movie_row = SCCategorySchedule(plan_version_id=version.id, category_key="genre:剧情",
                                       window_start=3, window_end=4,
                                       hourly_limit=99, daily_limit=99)
        db.add(movie_row)
        db.commit()
        res2 = sc2.api_reset_category_schedule_default(
            sc2.Payload(payload={"id": movie_row.id}), db)
        m_ws, m_we = sc2._preference_windows(db).get(movie_row.category_key, sc2.FULL_DAY_WINDOW)
        assert (res2["item"]["window_start"], res2["item"]["window_end"]) == (m_ws, m_we)
        # v2.1.6：分类级上限不再由恢复默认回写。
        assert (res2["item"]["hourly_limit"], res2["item"]["daily_limit"]) == (99, 99)


def test_upsert_schedule_keeps_enabled_when_absent(env):
    """保存单行排期未带 enabled 时不得把该分类意外停用。"""
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        row = SCCategorySchedule(plan_version_id=version.id, category_key="genre:剧情",
                                 window_start=15, window_end=24,
                                 hourly_limit=3, daily_limit=20, enabled=True)
        db.add(row)
        db.commit()

        # 不带 enabled 保存 → 保持启用
        sc2.api_upsert_category_schedule(sc2.Payload(payload={
            "plan_version_id": version.id, "category_key": "genre:剧情",
            "window_start": 16, "window_end": 23, "hourly_limit": 4, "daily_limit": 22,
        }), db)
        db.expire_all()
        kept = db.scalar(select(SCCategorySchedule).where(
            SCCategorySchedule.plan_version_id == version.id,
            SCCategorySchedule.category_key == "genre:剧情"))
        assert kept.enabled is True, "未传 enabled 时必须保持原启用状态"
        assert kept.window_start == 16 and kept.hourly_limit == 4

        # 显式传 enabled=False → 允许停用
        sc2.api_upsert_category_schedule(sc2.Payload(payload={
            "plan_version_id": version.id, "category_key": "genre:剧情",
            "window_start": 16, "window_end": 23, "hourly_limit": 4, "daily_limit": 22,
            "enabled": False,
        }), db)
        db.expire_all()
        stopped = db.scalar(select(SCCategorySchedule).where(
            SCCategorySchedule.plan_version_id == version.id,
            SCCategorySchedule.category_key == "genre:剧情"))
        assert stopped.enabled is False, "显式传 enabled=False 必须生效"


def test_category_schedules_api_exposes_media_type(env):
    """排期列表返回媒体类型，前端据此展示默认档位。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        res = sc2.api_category_schedules(0, db)
        assert res["items"], "排期表应返回 27 个官方分类"
        assert len(res["items"]) == 27
        types = {i["category_key"]: i["media_type"] for i in res["items"]}
        assert types["genre:动作冒险"] == "tv"
        assert types["genre:剧情"] == "movie"
        for item in res["items"]:
            assert item["hourly_limit"] > 0 and item["daily_limit"] > 0, \
                f"{item['category_key']} 的默认上限不得为 0"


# ---------------------------------------------------------------------------
# 2. 推送清单按日复用（修复清单爆炸）
# ---------------------------------------------------------------------------

def test_plan_engine_reuses_push_list_per_day(env):
    """同一自然日多轮调度复用同一条清单，不再每轮新建。"""
    from app import share_center_v2 as sc2
    from app.models import SCPushList
    from conftest import ensure_full_day_hour_window
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        _resource(db)
        sc2.sync_library_versions(db)
        _channel(db, "-1001")
        # 排期窗口默认 8-24，夜间整轮不排期——补整日窗口让断言与运行时刻无关。
        ensure_full_day_hour_window(db, sc2._active_version(db).id)
        sc2.plan_engine(db)
        first = db.scalars(select(SCPushList)).all()
        assert len(first) == 1
        # 再跑 3 轮
        for _ in range(3):
            sc2.plan_engine(db)
        lists = db.scalars(select(SCPushList)).all()
        assert len(lists) == 1, f"同一天不应产生多条清单，当前 {len(lists)} 条"
        day_key = datetime.now(SH).strftime("%Y%m%d")
        assert lists[0].list_key == f"plan:{lists[0].plan_version_id}:{day_key}"
        assert lists[0].status == "active"


def test_prune_empty_push_lists_keeps_filled(env):
    """空清单被清理，有清单项的清单必须保留。"""
    from app import share_center_v2 as sc2
    from app.models import SCPushList, SCPushListItem
    S, _ = env
    with S() as db:
        # 25 条空清单（超出保留的最近 20 条）
        for i in range(25):
            db.add(SCPushList(list_key=f"empty:{i}", name=f"空 {i}", status="active"))
        # 1 条有清单项的清单
        keep = SCPushList(list_key="keep:1", name="有内容", status="active")
        db.add(keep)
        db.flush()
        db.add(SCPushListItem(list_id=keep.id, work_key="work:x",
                               library_version_id=0, category_key="genre:drama",
                               dedup_key="keep:1:item", target_type="group",
                               chat_id="-1001"))
        db.commit()

        removed = sc2._prune_empty_push_lists(db)
        db.commit()
        assert removed == 5, "25 条空清单保留最近 20 条，应删除 5 条"
        keys = {r.list_key for r in db.scalars(select(SCPushList)).all()}
        assert "keep:1" in keys, "有清单项的清单不得被清理"
        assert len([k for k in keys if k.startswith("empty:")]) == 20


# ---------------------------------------------------------------------------
# 3. 分类占比按真实归属过滤
# ---------------------------------------------------------------------------

def test_plan_engine_scopes_works_per_category(env):
    """作品只进入其真实归属分类的清单项，其他分类不得抢走同一批作品。"""
    from app import share_center_v2 as sc2
    from app.models import SCPushListItem, SCWorkCategory
    from conftest import ensure_full_day_hour_window
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        _resource(db)
        sc2.sync_library_versions(db)
        _channel(db, "-1001")
        ensure_full_day_hour_window(db, sc2._active_version(db).id)
        sc2.plan_engine(db)
        items = db.scalars(select(SCPushListItem)).all()
        assert items
        own = {r.work_key: r.category_key for r in db.scalars(select(SCWorkCategory)).all()}
        for item in items:
            assert item.category_key == own.get(item.work_key), \
                "清单项分类必须与作品真实归属一致"


# ---------------------------------------------------------------------------
# 4. 代表资源快照失效 → 实时重解析
# ---------------------------------------------------------------------------

def test_rep_resolver_falls_back_to_live_resource(env):
    """快照资源 id 失效时，按 work_key 从实时 Resource 表重新解析出代表资源。"""
    from app import share_center_v2 as sc2
    from app.models import SCLibraryVersion
    S, _ = env
    with S() as db:
        res = _resource(db, file_id="live-1")
        work_key = sc2.utils.work_group_key(res)
        # 快照里的 id 指向一个已被删除的资源
        lv = SCLibraryVersion(work_key=work_key, version_no=1, fingerprint="fp",
                              media_type="tv", resource_ids_json=json.dumps([99999]))
        db.add(lv)
        db.commit()

        resolver = sc2._RepResolver(db)
        rep, err = resolver.resolve(lv)
        assert rep is not None, "快照失效时应按 work_key 从实时表重解析"
        assert err == ""
        assert rep.id == res.id

        # work_key 也没有实时匹配 → 失败原因细分，不是笼统的「代表资源不存在」
        orphan = SCLibraryVersion(work_key="work:不存在", version_no=1, fingerprint="fp2",
                                  media_type="tv", resource_ids_json=json.dumps([99999]))
        db.add(orphan)
        db.commit()
        rep2, err2 = resolver.resolve(orphan)
        assert rep2 is None
        assert "资源" in err2 and "代表资源不存在" not in err2


def test_rep_resolver_uses_snapshot_when_valid(env):
    """快照 id 仍有效时优先用快照资源。"""
    from app import share_center_v2 as sc2
    from app.models import SCLibraryVersion
    S, _ = env
    with S() as db:
        res = _resource(db, file_id="snap-1")
        work_key = sc2.utils.work_group_key(res)
        lv = SCLibraryVersion(work_key=work_key, version_no=1, fingerprint="fp",
                              media_type="tv", resource_ids_json=json.dumps([res.id]))
        db.add(lv)
        db.commit()
        resolver = sc2._RepResolver(db)
        rep, err = resolver.resolve(lv)
        assert rep is not None and rep.id == res.id and err == ""


# ---------------------------------------------------------------------------
# 5. 趋势图分桶 + 单点补零桶
# ---------------------------------------------------------------------------

def test_trend_day_uses_hour_buckets_and_pads(env, monkeypatch):
    """日视图按小时分桶，单点时补零桶，折线不再是一个孤立尖峰。

    v2.1.34：把「现在」冻结到**今天上海 12:00**。日视图的桶是「今天 00:00 到当前
    小时」的连续区间，用真实 now 的话**凌晨 00:xx 只有 1 个桶**，`len(labels) > 1`
    必然假红（本机 00:17 撞上）。这条与 v2.1.32 / v2.1.33 那两条 flaky 同类，
    都只在深夜跑全量时现形。
    """
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env
    noon_sh = datetime.now(timezone.utc).astimezone(sc2.shanghai_tz()).replace(
        hour=12, minute=0, second=0, microsecond=0)
    fixed = noon_sh.astimezone(timezone.utc)
    monkeypatch.setattr(sc2, "utcnow", lambda: fixed)
    with S() as db:
        now = fixed
        task = SCPublishTask(list_item_id=0, work_key="work:a", kind="first",
                             status="published", created_at=now,
                             updated_at=now, published_at=now)
        db.add(task)
        db.commit()
        trend = sc2._aggregate_trend(db, "day")
        labels = [p["label"] for p in trend["series"]]
        assert len(labels) == 13, f"00:00 到 12:00 应有 13 个桶，实际 {labels}"
        assert all(":" in lb for lb in labels), "日视图应按 HH:00 分桶"
        assert len(set(labels)) == len(labels), "补的桶不得与已有桶重复"
        assert sum(p["total"] for p in trend["series"]) == 1, "补的桶是真实 0，总量不变"
        assert sum(1 for p in trend["series"] if p["total"] == 1) == 1
        assert all(p["total"] == 0 for p in trend["series"] if p["total"] != 1)


def test_trend_week_and_month_use_day_buckets(env):
    """周/月视图按天分桶，week 真正按自然周聚合。"""
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env
    with S() as db:
        now = datetime.now(timezone.utc)
        db.add(SCPublishTask(list_item_id=0, work_key="work:a", kind="first",
                             status="published", created_at=now, updated_at=now,
                             published_at=now))
        db.commit()
        week = sc2._aggregate_trend(db, "week")
        assert week["period"] == "week"
        assert all("-" in p["label"] and ":" not in p["label"] for p in week["series"])
        month = sc2._aggregate_trend(db, "month")
        assert month["period"] == "month"
        assert all(len(p["label"]) == 10 for p in month["series"])


# ---------------------------------------------------------------------------
# 6. 分享管理统计口径 + 独立端点
# ---------------------------------------------------------------------------

def test_share_stats_matches_record_rows(env):
    """分享管理统计口径统一到清单项+任务真实表，不与表格行数脱节。"""
    from app import share_center_v2 as sc2
    from app.models import (SCPublishTask, SCPushList, SCPushListItem)
    S, _ = env
    with S() as db:
        pl = SCPushList(list_key="l1", name="清单", status="active")
        db.add(pl)
        db.flush()
        for i, (state, status) in enumerate([("active", "published"),
                                             ("unknown", "ready"),
                                             ("unknown", "failed")]):
            item = SCPushListItem(list_id=pl.id, work_key=f"work:{i}",
                                   library_version_id=0, category_key="genre:drama",
                                   dedup_key=f"d{i}", target_type="group",
                                   chat_id="-1001")
            db.add(item)
            db.flush()
            db.add(SCPublishTask(list_item_id=item.id, work_key=f"work:{i}",
                                  kind="first", status=status, link_state=state,
                                  share_url=f"https://x/{i}"))
        db.commit()

        stats = sc2._aggregate_share_stats(db)
        assert stats["total"] == 3, "总数 = 有清单项的分享记录数"
        assert stats["active"] == 1
        assert stats["pending"] == 2
        assert stats["history"] == 1
        assert stats["invalid"] == 1, "failed 任务计入失效"

        recs = sc2.api_share_records(db)
        assert len(recs["items"]) == 3, "统计总数必须与表格行数一致"
        assert recs["stats"]["total"] == len(recs["items"])


def test_share_records_endpoint_fields(env):
    """分享记录端点返回参考图所需列：分享码、标题、校验时间、推送时间。"""
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask, SCPushList, SCPushListItem
    S, _ = env
    with S() as db:
        pl = SCPushList(list_key="l1", name="清单", status="active")
        db.add(pl)
        db.flush()
        item = SCPushListItem(list_id=pl.id, work_key="work:剧集名",
                               library_version_id=0, category_key="genre:drama",
                               dedup_key="d1", target_type="group", chat_id="-1001")
        db.add(item)
        db.flush()
        now = datetime.now(timezone.utc)
        db.add(SCPublishTask(list_item_id=item.id, work_key="work:剧集名",
                              kind="first", status="published", link_state="active",
                              share_url="https://guangya.example/s/abc123",
                              created_at=now, updated_at=now, published_at=now))
        db.commit()

        res = sc2.api_share_records(db)
        row = res["items"][0]
        assert row["share_code"] == "abc123", "分享码应从链接尾段解析"
        assert row["share_title"] == "剧集名", "标题用可读作品名，不暴露 work_key 前缀"
        assert row["last_checked_at"], "最后校验时间来自真实 updated_at"
        assert row["last_pushed_at"], "最近推送时间来自真实 published_at"
        assert row["link_state"] == "active" and row["status"] == "published"


def test_overview_includes_share_stats(env):
    """总览接口带分享统计，前端不再拿 link_state_counts 冒充分享记录数。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        res = sc2.api_overview(db)
        assert "share_stats" in res
        stats = res["share_stats"]
        for key in ("total", "active", "pending", "invalid", "history"):
            assert key in stats


# ---------------------------------------------------------------------------
# 7. 单一分享中心（前后端仅一套）
# ---------------------------------------------------------------------------

def test_single_share_center_switch(env):
    """统一总开关 share_center_enabled；v2 开关只作兼容回退。"""
    from app import runtime_config
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        runtime_config.set_many(db, {"share_center_enabled": "true",
                                     "share_center_v2_enabled": "true"})
        db.commit()
        assert sc2.is_enabled(db) is True

        runtime_config.set_many(db, {"share_center_enabled": "false"})
        db.commit()
        assert sc2.is_enabled(db) is False, "总开关关闭时必须停止驱动"

        runtime_config.set_many(db, {"share_center_enabled": "true",
                                     "share_center_v2_enabled": "false"})
        db.commit()
        assert sc2.is_enabled(db) is False, "兼容开关关闭时同样停驱动"


def test_legacy_share_center_is_internal_helper_only():
    """旧链不再是独立分享中心：只保留被调用的内部函数与状态常量。"""
    src = (ROOT / "backend/app/share_center.py").read_text(encoding="utf-8")
    assert "不再是独立的分享中心" in src or "不是独立分享中心" in src
    assert "@router." not in src, "旧链不得再挂 HTTP 路由"
    # 仍需保留被 publisher/main 调用的内部能力
    for fn in ("def share_url_for_queue_item", "def ensure_task",
               "def claim_tasks", "def shadow_register",
               "def retire_share_record", "def build_share"):
        assert fn in src, f"旧链内部工具函数 {fn} 不得被删"


# ---------------------------------------------------------------------------
# 8. 前端契约
# ---------------------------------------------------------------------------

def test_frontend_schedule_defaults_controls():
    """前端有「一键恢复默认」与单行「默认」入口，并说明默认时间表口径。

    v2.1.6：语义由「一键应用默认建议（按媒体类型分档的速率上限）」改为
    「一键恢复默认（按观影习惯的 2 小时时间段）」——分类级上限已从分类表移除，
    速率统一由周期计划管理约束，页面不再出现「5 条每时 / 30 条每日」这类档位文案。
    """
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "scApplySchedDefaults" in app
    assert "/sc/category-schedules/apply-defaults" in app
    assert "scResetSchedDefault" in app
    assert "/sc/category-schedules/reset-default" in app
    assert "一键恢复默认" in app
    # v2.1.6：旧的「一键应用默认建议」语义下线
    assert "一键应用默认建议" not in app
    # v2.1.6：分类级速率档位文案下线（限流统一由周期计划管理约束）
    assert "5 条每时" not in app and "30 条每日" not in app
    assert "3 条每时" not in app and "20 条每日" not in app
    # 默认时段不再是写死的 8-24，改由默认时间表（热门白天 / 冷门夜间）派生
    assert "8:00–24:00" not in app, "写死的 8:00–24:00 时段文案必须清掉"
    assert "day_segments" in app and "night_segments" in app and "full_day_window" in app
    assert "批量采纳 AI 建议" in app
    assert "/sc/category-schedules/adopt-ai-all" in app


def test_frontend_no_backend_code_terms_in_share_center():
    """分享中心界面不得出现后端代码名词（用户看不懂）。"""
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    # 这些字符串只允许出现在脚本里的字段引用，不得出现在用户可见文案里
    for bad in ("发布任务 link_state", "status=published", "UNKNOWN", "老链"):
        assert bad not in app, f"界面不得暴露后端名词：{bad}"
    # 必须提供中文映射函数
    assert "scLinkStateLabel" in app and "scTaskStatusLabel" in app
    assert "已成功推送" in app and "待建链" in app


def test_frontend_share_records_and_actions():
    """分享管理用独立真实端点 + 复制链接；推送任务有导出。"""
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "/sc/share-records?limit=200" in app
    assert "scShareRecords" in app
    assert "scShareStats" in app
    assert "scCopyShareLink" in app and "复制链接" in app
    assert "scExportTasks" in app and "导出结果" in app


def test_frontend_out_of_scope_cards_removed():
    """超参考范围的卡片整块删除（不是隐藏）。"""
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    import re
    titles = set(re.findall(r"<h3>([^<]+)</h3>", app))
    for bad in ("调度总览", "数据总览", "发布清单", "推送节奏", "作品分类", "智能建议"):
        assert bad not in titles, f"超范围卡片「{bad}」必须整块删除"
    # 死 CSS 一并清除（只查样式定义处，注释里提及已删除的名字不算）
    for dead in ("sc-pushlist-grid", "sc-mini-counts", "sc-pacing-now"):
        assert f".{dead}" not in app, f"死样式 {dead} 必须清除"
