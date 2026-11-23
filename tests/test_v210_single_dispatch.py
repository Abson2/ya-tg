"""v2.1.0 分流层收敛验收：单一生产者 + 规则全量消费 + 改名硬门禁 + 诊断端点。

四板块链路：镜像同步(单轮完成) → 资源库(识别成功) → 分享中心(分流) → 分发中心(推送)。
"""
import os
import shutil
import sys
import tempfile
from datetime import datetime
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


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v210_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'd.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def test_publisher_no_legacy_cycle_when_v2_on():
    """静态守卫：v2 开启时只调 v2 helper，旧链 helper 不在此分支。"""
    src = (ROOT / "backend/app/publisher.py").read_text(encoding="utf-8")
    assert "def _run_share_center_v2_in_thread" in src
    assert "v2_enabled = _sc2.is_enabled(db)" in src
    anchor = src.index("if v2_enabled:")
    else_anchor = src.index("else:", anchor)
    v2_branch = src[anchor:else_anchor]
    assert "_run_share_center_v2_in_thread" in v2_branch
    assert "_run_share_center_in_thread" not in v2_branch.replace(
        "_run_share_center_v2_in_thread", "")


def test_removed_event_link_invalidation_in_v2(env):
    """v2 消费 removed 事件要做链接失效联动，不再依赖旧链 consume。"""
    from app import share_center_v2 as sc2
    from app.models import ResourceEvent, ShareRecord
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        db.add(ShareRecord(work_key="work:test", resource_id=1, share_id="s",
                           share_url="https://x", share_status="active",
                           current_flag=1, link_state="active"))
        db.add(ResourceEvent(idem_key="rm:1", type="removed", work_key="work:test",
                             resource_id=1, payload_json="{}"))
        db.commit()
        sc2.merge_update_windows(db)
        rec = db.scalar(select(ShareRecord).where(ShareRecord.current_flag == 1).limit(1))
        assert rec is not None and rec.link_state == "suspicious", \
            "removed 事件应把当前分享标记为疑似失效"


def test_plan_engine_consumes_category_schedule(env):
    """SCCategorySchedule 时段真实驱动 plan_engine（此前只落库）。"""
    from app import share_center_v2 as sc2
    from app.models import (Bot, Channel, Resource, SCCategory,
                            SCCategorySchedule, SCLibraryVersion)
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        db.add(Bot(id=1, name="b", token="t", enabled=True))
        db.add(Channel(name="c", bot_id=1, chat_id="100", enabled=True))
        db.add(Resource(file_id="f1", name="r", media_title="真实作品",
                        identify_status="identified", genres="[28]"))
        db.commit()
        sc2.sync_library_versions(db)
        version = sc2._active_version(db)
        # 给所有启用分类都配窗外排期，资源无论归到哪个分类都应被拦截
        now_hour = datetime.now(sc2.shanghai_tz()).hour
        out_hour = (now_hour + 2) % 24
        for cat in db.scalars(select(SCCategory).where(SCCategory.enabled.is_(True))).all():
            row = db.scalar(select(SCCategorySchedule).where(
                SCCategorySchedule.plan_version_id == version.id,
                SCCategorySchedule.category_key == cat.category_key))
            if not row:
                row = SCCategorySchedule(plan_version_id=version.id,
                                         category_key=cat.category_key)
                db.add(row)
            row.enabled = True
            row.window_start = out_hour
            row.window_end = (out_hour + 1) % 24
        db.commit()
        r = sc2.plan_engine(db)
        assert r["tasks"] == 0, "所有分类排期窗口均未命中当前小时，不应产任务"


def test_merge_update_windows_reads_basic_rule(env):
    """sc_update_detect_minutes 真实作为更新合并窗口长度。"""
    from app import runtime_config
    from app import share_center_v2 as sc2
    from app.models import ResourceEvent, SCUpdateWindow
    S, _ = env
    with S() as db:
        runtime_config.set_many(db, {"sc_update_detect_minutes": "7"})
        db.commit()
        db.add(ResourceEvent(idem_key="u:1", type="updated", work_key="work:w",
                             resource_id=1, payload_json="{}"))
        db.commit()
        sc2.merge_update_windows(db)
        w = db.scalar(select(SCUpdateWindow).limit(1))
        assert w is not None
        delta = (w.window_end - w.window_start).total_seconds()
        assert 7 * 60 - 2 <= delta <= 7 * 60 + 2


def test_diagnostics_endpoint_exists_and_readonly(env):
    """诊断端点存在且只读（不改配置、不产任务）。"""
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        result = sc2.api_diagnostics(db)
        assert result["active_chain"] in ("v2", "legacy", "off")
        assert "switches" in result and "rules_in_effect" in result and "segments" in result
        assert "share_center_v2_enabled" in result["switches"]
        assert db.scalar(select(SCPublishTask.id).limit(1)) is None, \
            "诊断端点不得产生任务"


def test_rename_hard_gate_source_guard():
    """静态守卫：改名失败时的后置管线闸门（main.py）。

    v2.1.10 起门禁**收敛到作品级**：整轮拦截默认关闭
    （`RENAME_GATE_BLOCK_WHOLE_ROUND = False`）——真机上一部剧的
    光鸭 160「假失败」曾把同轮另外 19 部健康作品全拦在门外，
    资源库里一条都没有。「英文名没改对不推群/频道」由推送侧的作品级闸门兜底。

    故此处断言的是**作品级口径**，不是旧的整轮拦截条件。
    """
    src = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
    assert "rename_gate_blocked" in src
    assert "RENAME_GATE_BLOCK_WHOLE_ROUND = False" in src, \
        "整轮拦截必须默认关闭（否则假失败会拖垮整轮）"
    assert "gate_block_round = rename_failed and RENAME_GATE_BLOCK_WHOLE_ROUND" in src
    assert "if do_pipeline and not gate_block_round:" in src
    assert "改名硬门禁" in src
