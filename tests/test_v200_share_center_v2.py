"""分享中心 v2 新数据体系验收（全量功能重构基线）。

覆盖：
  1. sc_* 新表由 create_all 创建，且 v2 不读旧 ShareTask/ShareRecord；
  2. 作品级资源版本 + TMDB 主分类唯一归属，重复同步不重复建版本；
  3. 10 分钟更新合并窗口；
  4. 计划版本 + 分类游标生成推送清单/清单项/任务；
  5. 频道/群推送间隔默认与官方下限夹逼；
  6. OpenAI 未配置时本地统计降级，AI 不直接推送；
  7. 前端 v2 管理页按钮全部接真实后端；
  8. publisher 发送前调用节奏服务、发送后回写 v2 任务。
"""
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select, inspect
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
    d = tempfile.mkdtemp(prefix="v200_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'v2.db')}")
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _resource(db, file_id="f1", name="Show (2024) {tmdb-1}/Show.S01E01.mkv"):
    from app.models import Resource
    r = Resource(
        file_id=file_id, name=name, ext=".mkv", size=100, utime=1,
        identify_status="identified", media_type="tv", tmdb_id=1,
        media_title="Show", path_hint="Show (2024)",
        share_dir_id="dir_show", share_scope="series_dir", share_dir_name="Show",
        genres=json.dumps([{"id": 18, "name": "Drama"}]),
        poster_path="/poster.jpg",
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


def test_sc_tables_created_and_v2_does_not_read_old_share_tables(env):
    from app import share_center_v2 as sc2
    S, eng = env
    names = set(inspect(eng).get_table_names())
    expected = {
        "sc_categories", "sc_work_categories", "sc_library_versions",
        "sc_push_lists", "sc_push_list_items", "sc_share_plans",
        "sc_share_plan_versions", "sc_hour_windows", "sc_category_cursors",
        "sc_update_windows", "sc_publish_tasks", "sc_publish_executions",
        "sc_ai_suggestions", "sc_ai_apply_logs", "sc_push_pacing_rules",
        "sc_push_pacing_state",
    }
    assert expected <= names, sorted(expected - names)
    src = (ROOT / "backend/app/share_center_v2.py").read_text(encoding="utf-8")
    assert "from .models import" in src
    import_block = src.split("from .models import (", 1)[1].split(")", 1)[0]
    assert "ShareTask" not in import_block and "ShareRecord" not in import_block
    assert "db.get(ShareTask" not in src and "select(ShareTask" not in src
    assert 'q.push_reason = "sc_v2"' in src


def test_library_version_and_primary_category(env):
    from app import share_center_v2 as sc2
    from app.models import SCLibraryVersion, SCWorkCategory
    S, _ = env
    with S() as db:
        _resource(db)
        result = sc2.sync_library_versions(db)
        assert result["versions_created"] == 1
        lv = db.scalar(select(SCLibraryVersion))
        assert lv.work_key.startswith("work:")
        assert lv.version_no == 1
        assert lv.fingerprint
        own = db.scalar(select(SCWorkCategory))
        assert own.category_key.startswith("genre:")
        # 重复同步不重复建版本
        again = sc2.sync_library_versions(db)
        assert again["versions_created"] == 0
        assert len(db.scalars(select(SCLibraryVersion)).all()) == 1


def test_update_window_merges_ten_minutes(env):
    from app import share_center_v2 as sc2
    from app.models import ResourceEvent, SCUpdateWindow
    S, _ = env
    with S() as db:
        _resource(db)
        db.add(ResourceEvent(idem_key="updated|work:show|1", type="updated",
                             work_key="work:show", resource_id=1, payload_json="{}"))
        db.add(ResourceEvent(idem_key="updated|work:show|2", type="updated",
                             work_key="work:show", resource_id=1, payload_json="{}"))
        db.commit()
        result = sc2.merge_update_windows(db, window_seconds=600)
        assert result["events_merged"] == 2
        windows = db.scalars(select(SCUpdateWindow)).all()
        assert len(windows) == 1
        assert len(json.loads(windows[0].event_keys_json)) == 2
        assert all(e.consumed_at for e in db.scalars(select(ResourceEvent)).all())


def test_plan_engine_creates_list_items_tasks_and_cursors(env):
    from app import share_center_v2 as sc2
    from app.models import (SCCategoryCursor, SCPublishTask, SCPushList,
                            SCPushListItem, SCPushPacingRule)
    from conftest import ensure_full_day_hour_window
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        assert len(db.scalars(select(SCPushPacingRule)).all()) >= 3
        _resource(db)
        sc2.sync_library_versions(db)
        _channel(db, "-1001")
        # 排期窗口默认 8-24，夜间（真实墙钟）整轮不排期——补一条整日窗口，
        # 让本用例断言与运行时刻无关（生产行为不变）。
        ensure_full_day_hour_window(db, sc2._active_version(db).id)
        result = sc2.plan_engine(db)
        assert result["lists"] == 1
        assert result["items"] >= 1
        assert result["tasks"] >= 1
        assert db.scalar(select(SCPushList))
        assert db.scalar(select(SCPushListItem))
        assert db.scalar(select(SCPublishTask))
        cursors = db.scalars(select(SCCategoryCursor)).all()
        assert cursors
        # v2.1.1：plan_engine 现在按分类真实过滤作品（JOIN sc_work_categories），
        # 只有**该分类名下确实有作品**的分类才会推进游标。
        # 旧实现每个分类都抢同一批作品（导致「分类占比 100% 剧情」），
        # 游标因此全被推进——那正是 bug 而非契约。此处断言真正的语义：
        # 至少有一个游标推进，且推进的分类确实拥有该作品。
        advanced = [c for c in cursors if c.last_library_version_id > 0]
        assert advanced, "有作品的分类必须推进游标"
        # 测试资源的 genres 是英文 "Drama"。v2.1.54 起归类前先过
        # `tmdb.genre_name_cn()` 归一，于是 key 变成 genre:剧情 ——
        # 改前是 genre:drama，那正是线上 586 部「未分类」的成因（英文名
        # 进不了 27 项官方字典），所以这里断言的是修好之后的口径。
        # 断言推进游标的分类必须是该作品真实归属的分类（而不是所有分类）。
        assert all(c.category_key == "genre:剧情" for c in advanced), \
            "只应推进真实拥有作品的分类"


def test_pacing_defaults_and_official_minimum(env):
    from app import share_center_v2 as sc2
    from app.models import SCPushPacingRule
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        ch = sc2.resolve_pacing_rule(db, "channel")
        gr = sc2.resolve_pacing_rule(db, "group")
        assert ch["interval"] == 2.0 and ch["min"] == 1.0
        assert gr["interval"] == 4.0 and gr["min"] == 3.0
        # 即使配置试图降到 0，后端夹逼到官方下限
        rule = db.scalar(select(SCPushPacingRule).where(
            SCPushPacingRule.scope_key == "global:group"))
        rule.default_interval_seconds = 0.1
        rule.min_interval_seconds = 0.1
        db.commit()
        gr2 = sc2.resolve_pacing_rule(db, "group")
        assert gr2["min"] == 3.0 and gr2["interval"] == 3.0
        assert sc2.OFFICIAL_MIN_SECONDS == {"channel": 1.0, "private": 1.0, "group": 3.0}


def test_ai_local_fallback_and_review_boundary(env):
    from app import share_center_v2 as sc2
    from app.models import SCAISuggestion, SCAIApplyLog, SCSharePlanVersion
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        result = sc2.generate_weekly_suggestions(db, "2026-11-21")
        assert result["source"] == "local"
        assert result["created"] >= 1
        rows = db.scalars(select(SCAISuggestion)).all()
        assert rows and all(r.status == "pending" for r in rows)
        # 未显式开启自动应用时拒绝自动应用；手动审核应用写审计
        denied = sc2.apply_suggestion(db, rows[0].id, auto=True)
        assert denied["ok"] is False
        applied = sc2.apply_suggestion(db, rows[0].id, reviewer="tester")
        assert applied["ok"] is True
        assert db.scalar(select(SCAIApplyLog))
        version = db.scalar(select(SCSharePlanVersion).where(
            SCSharePlanVersion.status == "active"))
        assert version and version.config_json


def test_frontend_share_center_pages_use_real_api():
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    import re
    card_titles = set(re.findall(r"<h3>([^<]+)</h3>", app))
    # v2.1.1：分享中心收敛为 4 页设计稿，模块卡片以设计稿为准。
    for token in ("发布总览", "发布规则", "推送任务", "分享管理",
                  "推送趋势", "分类推送占比", "本周热门候选",
                  "分类-时间排期配置", "周期计划管理"):
        assert token in app, token
    # 超设计稿范围的卡片标题已整块删除（不是隐藏）——按标题精确比对，
    # 避免误伤「智能建议：新增 N 条」这类操作提示语。
    for gone in ("调度总览", "发布清单", "作品分类", "发布计划",
                 "时段窗口", "智能建议", "推送节奏"):
        assert gone not in card_titles, gone
    for dev_name in ("V2总览", "V2任务", "scV2", "跑一轮 v2"):
        assert dev_name not in app, dev_name
    for token in ("/sc/overview", "/sc/run", "/sc/push-lists", "/sc/plans",
                  "/sc/hour-windows", "/sc/pacing-rules", "/sc/ai/generate", "/sc/tasks",
                  "/sc/share-records"):
        assert token in app, token
    assert "localStorage.setItem('scPlan" not in app
    assert "setInterval(() => api('/sc/run'" not in app


def test_publisher_wires_pacing_and_v2_result():
    src = (ROOT / "backend/app/publisher.py").read_text(encoding="utf-8")
    assert "pacing_wait_seconds(db, q)" in src
    assert "mark_pacing_attempt(db, q" in src
    assert "_sc2.mark_result(db, q.event_id" in src
    shared = (ROOT / "backend/app/share_center.py").read_text(encoding="utf-8")
    assert 'push_reason", "") == "sc_v2"' in shared
    assert "SCPublishTask" in shared


def test_pause_endpoint_shares_dispatch_key(env):
    """分享中心卡片化后急停走 /sc/pause，读写与 dispatch 同一 publish_paused 键。"""
    from app import runtime_config, share_center_v2 as sc2
    S, _ = env
    db = S()
    assert sc2._is_paused(db) is False
    sc2.api_pause_set(sc2.Payload(payload={"paused": True}), db)
    assert sc2._is_paused(db) is True
    assert runtime_config.get(db, "publish_paused") == "true"
    # dispatch 读取的键被真正置位（同一键，不需旧链 /share/rules）
    assert sc2.api_pause_get(db)["paused"] is True
    sc2.api_pause_set(sc2.Payload(payload={"paused": False}), db)
    assert sc2._is_paused(db) is False
    db.close()


def test_overview_counts_include_library_versions(env):
    from app import share_center_v2 as sc2
    S, _ = env
    db = S()
    sc2.ensure_defaults(db)
    res = sc2.api_overview(db)
    assert "versions" in res["counts"]
    assert "paused" in res and "pacing" in res
    db.close()
