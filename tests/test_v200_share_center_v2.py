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
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        assert len(db.scalars(select(SCPushPacingRule)).all()) >= 3
        _resource(db)
        sc2.sync_library_versions(db)
        _channel(db, "-1001")
        result = sc2.plan_engine(db)
        assert result["lists"] == 1
        assert result["items"] >= 1
        assert result["tasks"] >= 1
        assert db.scalar(select(SCPushList))
        assert db.scalar(select(SCPushListItem))
        assert db.scalar(select(SCPublishTask))
        cursors = db.scalars(select(SCCategoryCursor)).all()
        assert cursors and all(c.last_library_version_id > 0 for c in cursors)


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


def test_frontend_v2_pages_use_real_api():
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    for token in ("V2总览", "推送清单", "分类管理", "周期计划", "小时排期", "AI调度", "V2任务"):
        assert token in app, token
    for token in ("/sc/overview", "/sc/run", "/sc/push-lists", "/sc/plans",
                  "/sc/hour-windows", "/sc/pacing-rules", "/sc/ai/generate", "/sc/tasks"):
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
