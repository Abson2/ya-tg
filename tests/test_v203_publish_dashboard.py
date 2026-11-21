"""v2.0.3 发布总览仪表盘验收：真实数据聚合 + 全交互控制台。

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
    # 左侧子导航 + 仪表盘结构
    for token in ("sc-sidebar", "scSubNavs", "发布总览", "发布队列", "分享管理",
                  "推送任务", "发布规则", "sc-kpi-grid", "sc-health-grid", "sc-progress"):
        assert token in app, token
    # KPI 点击筛选 + 任务详情抽屉 + 批量 + 各模块保存均有真实端点
    for token in ("scFilterQueue", "scOpenTaskDetail", "scBatchTask",
                  "scSaveCategory", "scReassignCategories", "scCreatePlan",
                  "scCreatePlanVersion", "scActivateVersion", "scRestoreWindowTemplate"):
        assert token in app, token
    for ep in ("/sc/overview", "/sc/categories", "/sc/categories/reassign",
               "/sc/plans", "/sc/hour-windows", "/sc/tasks/${id}", "/sc/push-lists"):
        assert ep in app, ep
    # 无 V2 开发命名、无截图示例写死数字（45%/60% 是 CSS 通用样式，不在此列）
    for bad in ("scV2", "V2总览", "V2任务", "1,286", "1286", "待发布</small><b>128"):
        assert bad not in app, bad


def test_overview_source_disclosed_not_pretend_full_link_states():
    """链接健康四宫格须标注真实来源，不伪装 suspicious/inactive 已由 v2 写入。"""
    src = (ROOT / "backend/app/share_center_v2.py").read_text(encoding="utf-8")
    assert "link_state_counts" in src
    assert "发布任务 link_state" in (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
