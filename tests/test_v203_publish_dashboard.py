"""分享中心发布总览/控制台验收：真实数据聚合 + 全交互（v2.0.3 引入，v2.0.4 扩展 tab 布局与小时分类排期）。

覆盖：
  1. /sc/overview 返回 kpi / link_state_counts / kind_counts / schedule /
     windows / recent_tasks，且全部来自 sc_* 真实聚合（不读旧表、不用示例值）；
  2. KPI 语义正确：待发布=planned+ready、有效=link_state active、
     待建链=unknown、推送中=submitted/accepted/publishing/building；
  3. 调度进度按 kind 分组，rate=published/total；
  4. 前端左侧子导航 + KPI 点击筛选 + 任务详情抽屉 + 各模块保存交互均接真实端点；
  5. 无 V2 开发命名、无写死示例数字（128/1286/45% 等）。
"""
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

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v203_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'd.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def test_overview_dashboard_fields_are_real(env):
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        rows = [
            SCPublishTask(work_key="work:a", kind="first", status="planned"),
            SCPublishTask(work_key="work:b", kind="first", status="ready"),
            SCPublishTask(work_key="work:c", kind="update", status="published",
                          link_state="active"),
            SCPublishTask(work_key="work:d", kind="update", status="building",
                          link_state="unknown"),
            SCPublishTask(work_key="work:e", kind="first", status="published",
                          link_state="active"),
        ]
        db.add_all(rows)
        db.commit()
        ov = sc2.api_overview(db)
        kpi = ov["kpi"]
        assert kpi["pending_publish"] == 2       # planned + ready
        assert kpi["active_shares"] == 2          # link_state=active
        # unknown 为所有尚未建链的任务：planned/ready/building 共 3 条
        assert kpi["pending_link"] == 3
        assert kpi["in_flight_push"] == 1         # building
        assert ov["kind_counts"].get("first") == 3
        assert ov["kind_counts"].get("update") == 2
        assert ov["link_state_counts"].get("active") == 2
        assert ov["schedule"]["first"]["total"] == 3
        assert ov["schedule"]["first"]["published"] == 1
        assert ov["schedule"]["first"]["rate"] == pytest.approx(33.3, abs=0.1)
        assert ov["schedule"]["overall"]["total"] == 5
        assert isinstance(ov["recent_tasks"], list)
        assert ov["recent_tasks"] and "work_key" in ov["recent_tasks"][0]
        assert isinstance(ov["windows"], list)
        assert ov["pacing"] and "max_per_minute" in ov["pacing"][0]


def test_overview_empty_db_has_no_fabricated_numbers(env):
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        ov = sc2.api_overview(db)
        assert ov["kpi"]["pending_publish"] == 0
        assert ov["kpi"]["active_shares"] == 0
        assert ov["schedule"]["overall"]["rate"] == 0.0
        assert ov["recent_tasks"] == []


def test_frontend_dashboard_interactions_use_real_endpoints():
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    # 顶部 tab（4 个，无系统设置）+ 仪表盘结构
    for token in ("sc-tabbar", "sc-tab", "scSubNavs", "发布总览", "分享管理",
                  "推送任务", "发布规则", "sc-kpi-grid", "sc-health-grid", "sc-progress"):
        assert token in app, token
    assert "系统设置" not in app, "系统设置空 tab 应移除"
    assert "sc-sidebar" not in app, "左侧子导航应改为顶部 tab"
    # 小时分类排期 + 周期计划管理常驻（真实数据/真实交互，保留）
    for token in ("小时分类排期", "sc-hour-row", "scSaveAllHourRows", "scSaveHourRow",
                  "周期计划管理", "scCyclePlan"):
        assert token in app, token
    # v2.1.1：按 4 张设计稿收敛——发布总览只保留「六 KPI + 趋势/占比/热门候选 + 三表」。
    # 下列模块不在参考图范围内，必须不得出现（否则又变成超范围堆料）。
    for token in ("sc-pushlist-grid", "待首次分享", "更新待推送", "已完成首次推送",
                  "调度总览", "数据总览", "sc-mini-counts", "sc-sched-card",
                  "发布清单", "推送节奏"):
        assert token not in app, token
    # v2.1.1：分享管理新增真实端点与参考图要求的列/操作
    for token in ("/sc/share-records", "scShareStats", "scShareRecords",
                  "分享码", "最后校验时间", "最近推送时间", "复制链接", "取消分享"):
        assert token in app, token
    # v2.1.0 用户明确要求：严禁把「流程图」照搬成页面组件。
    # 设计稿顶部的资源→识别→TG 流程示意、各模块运作流程、四级计划流程均不得做成 UI 节点。
    for token in ("sc-pipeline", "sc-pipe-node", "sc-flow", "sc-flow-node",
                  "sc-plan-flow", "各模块运作流程", "统一推送清单", "统一调度引擎",
                  "Telegram 推送及结果回写", "月度计划 · 控制总释放量"):
        assert token not in app, token
    # KPI 点击筛选 + 任务详情抽屉 + 批量 + 各模块保存均有真实端点
    for token in ("scFilterQueue", "scOpenTaskDetail", "scBatchTask",
                  "scSaveCategory", "scReassignCategories",
                  "scRestoreWindowTemplate", "scExportTasks", "scCopyShareLink"):
        assert token in app, token
    for ep in ("/sc/overview", "/sc/categories", "/sc/categories/reassign",
               "/sc/plans", "/sc/hour-windows/reset-template",
               "/sc/tasks/${id}", "/sc/push-lists"):
        assert ep in app, ep
    # 无 V2 开发命名、无截图示例写死数字（45%/60% 是 CSS 通用样式，不在此列）
    for bad in ("scV2", "V2总览", "V2任务", "1,286", "1286", "待发布</small><b>128"):
        assert bad not in app, bad


def test_overview_source_disclosed_not_pretend_full_link_states():
    """链接状态说明须标注真实来源，不伪装 suspicious/inactive 已由新体系写入。

    v2.1.1：界面文案改为用户可读表述（不再暴露 link_state 等内部字段名），
    但「只写已建链/待建链两态」这一真实来源披露必须保留。
    """
    src = (ROOT / "backend/app/share_center_v2.py").read_text(encoding="utf-8")
    assert "link_state_counts" in src
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "链接状态说明" in app
    assert "已建链 / 待建链" in app
    # 界面上不得暴露内部枚举名与字段名
    for bad in ("发布任务 link_state", "link_state（v2 仅写"):
        assert bad not in app, bad


def test_push_list_counts_are_real_derived(env):
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        db.add_all([
            SCPublishTask(work_key="work:a", kind="first", status="planned"),
            SCPublishTask(work_key="work:b", kind="first", status="published"),
            SCPublishTask(work_key="work:c", kind="update", status="failed"),
            SCPublishTask(work_key="work:d", kind="update", status="published"),
        ])
        db.commit()
        ov = sc2.api_overview(db)
        c = ov["push_list_counts"]
        assert c["pending_first"] == 1
        assert c["pending_update"] == 1
        assert c["published_first"] == 1


def test_hour_window_persists_category_and_reset_template(env):
    from app import share_center_v2 as sc2
    from app.models import SCHourWindow
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        vid = sc2._active_version(db).id
        # v2.1.1：默认模板改为 0-24 点、每 2 小时一段共 12 段
        rows = db.scalars(select(SCHourWindow).where(
            SCHourWindow.plan_version_id == vid)).all()
        assert len(rows) == 12
        assert all(r.category_key == "" for r in rows)  # 默认不限分类，不编造示例
        assert all(r.end_hour - r.start_hour == 2 for r in rows)  # 每段 2 小时
        # 单行保存分类绑定（真实写库）
        d = sc2.api_upsert_hour_window(sc2.Payload(payload={
            "plan_version_id": vid, "start_hour": 18, "end_hour": 20,
            "category_key": "genre:动作", "quota": 3, "enabled": True}), db)
        assert d["ok"] is True
        r = db.scalar(select(SCHourWindow).where(
            SCHourWindow.plan_version_id == vid, SCHourWindow.start_hour == 18))
        assert r.category_key == "genre:动作" and r.quota == 3
        # 恢复模板：清空分类绑定，重建 12 段
        d2 = sc2.api_reset_hour_template(sc2.Payload(payload={"plan_version_id": vid}), db)
        assert d2["ok"] is True and d2["segments"] == 12
        rows2 = db.scalars(select(SCHourWindow).where(
            SCHourWindow.plan_version_id == vid)).all()
        assert len(rows2) == 12 and all(x.category_key == "" for x in rows2)


def test_db_migration_has_sc_hour_windows_category():
    db_src = (ROOT / "backend/app/db.py").read_text(encoding="utf-8")
    assert '"sc_hour_windows"' in db_src
    assert '"category_key": "TEXT DEFAULT \'\'"' in db_src
