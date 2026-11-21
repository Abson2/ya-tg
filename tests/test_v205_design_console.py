"""v2.0.5 设计稿控制台验收：4 张设计稿对应功能均为真实数据/真实持久化，非壳。

覆盖：
  1. TMDB 官方 genre 字典种子（27 项）来自 tmdb.py::TMDB_GENRES，无设计稿虚构分类；
  2. overview 的 trend/category_ratio/hot_works/views 全部真实聚合，空库无编造；
  3. 热门作品榜用已落库 TMDB 评分（vote_average），标注来源，非虚构热度 98.2；
  4. 分类-时间排期 CRUD + AI 建议采纳（真实发布分布派生）+ 复制；
  5. 基础规则持久化于 app_config；周期计划持久化于版本 config_json；
  6. 周期计划静默保护真实生效于 plan_engine；
  7. 前端 4 tab 按设计稿排版且所有交互接真实 /sc/* 端点，无示例数字。
"""
import os
import shutil
import sys
import tempfile
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v205_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'd.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def test_tmdb_genre_catalog_is_official_and_seeded(env):
    from app import share_center_v2 as sc2
    from app.models import SCCategory
    from app.tmdb import TMDB_GENRES
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        cat = sc2._tmdb_genre_catalog()
        assert len(cat) == len(TMDB_GENRES) == 27
        assert all(x["source"] == "tmdb" and x["category_key"].startswith("genre:") for x in cat)
        seeded = {c.category_key for c in db.query(SCCategory).all() if c.priority < 0}
        assert seeded == {x["category_key"] for x in cat}, "官方字典应全量种子入库"
        r = sc2.api_category_catalog(db)
        assert r["source"] == "tmdb.py::TMDB_GENRES" and r["total"] == 27
        # 设计稿虚构分类不得作为官方字典项
        names = {x["name"] for x in r["items"]}
        assert "连续剧" not in names and "院线电影" not in names and "综艺" not in names


def test_overview_design_aggregates_are_real(env):
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask, SCPushList, SCPushListItem, SCCategory, Resource
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        ov = sc2.api_overview(db)
        assert ov["trend"]["series"] == [] and ov["category_ratio"]["total"] == 0
        assert ov["hot_works"]["items"] == [] and ov["views"]["history"] == []
        assert ov["hot_works"]["metric"] == "TMDB 评分"
        pl = SCPushList(list_key="k", name="l", status="active")
        db.add(pl); db.flush()
        # genre:动作 已由 ensure_defaults 作为官方字典种子入库，复用不重复添加
        item = SCPushListItem(list_id=pl.id, work_key="w", category_key="genre:动作",
                              dedup_key="d", target_type="channel", chat_id="1")
        db.add(item); db.flush()
        now = sc2.utcnow()
        db.add(SCPublishTask(list_item_id=item.id, work_key="w", kind="first",
                             status="published", link_state="active",
                             published_at=now, created_at=now - timedelta(hours=1)))
        db.add(SCPublishTask(work_key="w2", kind="first", status="failed",
                             created_at=now, error="timeout"))
        db.add(Resource(file_id="f1", name="r", media_title="真实作品", vote_average=9.1,
                        identify_status="identified", media_type="movie"))
        db.commit()
        ov = sc2.api_overview(db)
        assert ov["category_ratio"]["total"] == 1
        assert ov["category_ratio"]["items"][0]["name"] == "动作"
        assert ov["hot_works"]["items"][0]["title"] == "真实作品"
        assert ov["hot_works"]["items"][0]["score"] == 9.1
        assert len(ov["views"]["history"]) == 1 and len(ov["views"]["failed"]) == 1
        assert len(ov["trend"]["series"]) >= 1
        assert ov["kpi"]["success_rate"] == 50.0
        assert "98.2" not in str(ov["hot_works"])  # 设计稿虚构热度不得出现


def test_category_schedule_crud_adopt_copy(env):
    from app import share_center_v2 as sc2
    from app.models import SCPushList, SCPushListItem, SCPublishTask, SCCategory
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        vid = sc2._active_version(db).id
        rows = sc2.api_category_schedules(vid, db)["items"]
        assert len(rows) == 27, "每分类一行"
        assert all(r["name"] for r in rows), "每行有真实 TMDB 中文名"
        key_action = next(r for r in rows if r["name"] == "动作")["category_key"]
        saved = sc2.api_upsert_category_schedule(SimpleNamespace(payload={
            "plan_version_id": vid, "category_key": key_action,
            "window_start": 9, "window_end": 22, "hourly_limit": 6,
            "daily_limit": 40, "enabled": True}), db)
        assert saved["ok"]
        row = next(r for r in sc2.api_category_schedules(vid, db)["items"]
                   if r["category_key"] == key_action)
        assert (row["window_start"], row["hourly_limit"]) == (9, 6)
        # 无历史发布采纳应失败（不编造建议）
        no_ai = sc2.api_adopt_schedule_ai(SimpleNamespace(payload={"id": row["id"]}), db)
        assert not no_ai["ok"]
        # 造一条真实已发布任务 → AI 建议来自其真实小时分布
        pl = SCPushList(list_key="k", name="l", status="active")
        db.add(pl); db.flush()
        item = SCPushListItem(list_id=pl.id, work_key="w", category_key=key_action,
                              dedup_key="d", target_type="channel", chat_id="1")
        db.add(item); db.flush()
        now = sc2.utcnow()
        db.add(SCPublishTask(list_item_id=item.id, work_key="w", kind="first",
                             status="published", published_at=now))
        db.commit()
        adopted = sc2.api_adopt_schedule_ai(SimpleNamespace(payload={"id": row["id"]}), db)
        assert adopted["ok"] and 0 <= adopted["window_start"] <= 23
        key_scifi = next(r for r in sc2.api_category_schedules(vid, db)["items"]
                         if r["name"] == "科幻")["category_key"]
        copied = sc2.api_copy_category_schedule(SimpleNamespace(payload={
            "id": row["id"], "target_keys": [key_scifi]}), db)
        assert copied["ok"] and copied["copied"] == 1


def test_basic_rules_and_cycle_plan_persist(env):
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        sc2.api_save_basic_rules(SimpleNamespace(payload={
            "update_detect_minutes": 15, "quick_publish_hourly_limit": 5,
            "conflict_policy": "prefer_new"}), db)
        rules = sc2.api_basic_rules(db)
        assert rules["update_detect_minutes"] == "15"
        assert rules["conflict_policy"] == "prefer_new"
        # 周期计划持久化
        sc2.api_save_cycle_plan(SimpleNamespace(payload={"hour_max": 33, "quiet_start": 2,
                                             "quiet_end": 6, "quiet_enabled": True}), db)
        plan = sc2.api_cycle_plan(db)
        assert plan["hour_max"] == 33 and plan["quiet_start"] == 2
        # 重新读取仍持久（来自 config_json）
        assert sc2.api_cycle_plan(db)["hour_max"] == 33


def test_quiet_hours_really_blocks_plan_engine(env):
    from app import share_center_v2 as sc2
    from app.models import Channel, Resource
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        db.add(Channel(name="测试频道", bot_id=1, chat_id="c", enabled=True))
        db.add(Resource(file_id="f1", name="r", media_title="w", identify_status="identified"))
        db.commit()
        version = sc2._active_version(db)
        cfg = {"hour_max": 0, "quiet_enabled": True,
               "quiet_start": 0, "quiet_end": 24}  # 全时段静默
        version.config_json = sc2._jdump({"cycle_plan": cfg})
        db.commit()
        result = sc2.plan_engine(db)
        assert result.get("quiet") == 1 and result["tasks"] == 0


def test_frontend_design_console_wired(env):
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    # 4 tab 结构保持
    assert "sc-tabbar" in app and "发布总览" in app and "发布规则" in app and "推送任务" in app
    assert "分享管理" in app and "系统设置" not in app
    # 设计稿新交互均接真实端点
    for token in ("/sc/categories/catalog", "/sc/category-schedules",
                  "/sc/basic-rules", "/sc/cycle-plan", "/sc/tasks/",
                  "trend", "category_ratio", "hot_works", "views",
                  "TMDB 评分", "采纳建议", "静默保护"):
        assert token in app, token
    # 无设计稿示例数字/虚构分类作为数据
    for bad in ("热度98.2", "1,286", "1286"):
        assert bad not in app, bad
