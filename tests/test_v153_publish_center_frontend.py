"""阶段六验收：发布中心前端全重做的后端支撑。

覆盖：
  - publish_paused 默认值保守（false）+ is_paused 判定；
  - run_cycle 急停门禁：paused 时空转，不建分享不提交，不撤销在途任务；
  - publish_overview 聚合：配额进度/待处理事件/链接分布/急停状态；
  - publish_rules 读写白名单 + 热更新；
  - share_records_list 返回阶段五 link_state 字段；
  - share_run 端点线程化静态守卫（第六次死锁同源漏网防护）；
  - 前端 5 页结构静态守卫（子标签 + 真实端点绑定 + key 兼容）。
"""
import asyncio
import inspect
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v153_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'c.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _pause(db, val: str):
    from app import runtime_config
    runtime_config.set_many(db, {"publish_paused": val})
    db.commit()


# ---------- 默认值 + 判定 ----------
def test_publish_paused_default_is_false(env):
    from app.runtime_config import DEFAULTS
    assert DEFAULTS["publish_paused"][1] == "false"


def test_is_paused_true_values(env):
    from app import share_center as sc
    S, _ = env
    with S() as db:
        assert sc.is_paused(db) is False
        for val in ("true", "1", "YES", "on"):
            _pause(db, val)
            assert sc.is_paused(db) is True, val
        _pause(db, "false")
        assert sc.is_paused(db) is False


# ---------- 急停门禁 ----------
def test_run_cycle_paused_is_noop(env, monkeypatch):
    from app import share_center as sc
    S, _ = env
    with S() as db:
        _pause(db, "true")
        # 造一个 planned 任务，paused 时不应被领取/建分享
        from app.models import ShareTask
        t = ShareTask(work_key="work:暂停剧", kind="first", status=sc.ST_PLANNED,
                      resource_id=1, queue_type="fresh")
        db.add(t)
        db.commit()
        called = {"build": 0}
        monkeypatch.setattr(sc, "build_share", lambda *a, **k: called.update(build=1))
        stats = asyncio.run(sc.run_cycle(db, limit=20))
        assert stats.get("paused") is True
        assert stats["built"] == 0 and stats["submitted"] == 0
        assert called["build"] == 0
        # 任务状态未被改动（不撤销在途）
        db.refresh(t)
        assert t.status == sc.ST_PLANNED


# ---------- 发布总览聚合 ----------
def test_publish_overview_shape(env):
    from app import share_center as sc
    from app.models import ShareRecord
    S, _ = env
    with S() as db:
        db.add(ShareRecord(work_key="work:A", resource_id=1, share_id="s1",
                           share_url="u", share_status="active", current_flag=1,
                           link_state="active"))
        db.add(ShareRecord(work_key="work:B", resource_id=2, share_id="s2",
                           share_url="u2", share_status="active", current_flag=1,
                           link_state="suspicious"))
        db.commit()
        ov = sc.publish_overview(db)
        assert ov["paused"] is False
        assert isinstance(ov["enabled"], bool)
        assert ov["publish_today"]["unlimited"] is True  # 默认 limit=0
        assert ov["push_today"]["used"] == 0
        assert ov["pending_events"] == 0
        assert ov["link_states"].get("active") == 1
        assert ov["link_states"].get("suspicious") == 1


def test_publish_overview_quota_progress(env):
    from app import share_center as sc
    from app import runtime_config
    S, _ = env
    with S() as db:
        runtime_config.set_many(db, {"share_daily_limit": "10"})
        db.commit()
        ov = sc.publish_overview(db)
        assert ov["publish_today"]["limit"] == 10
        assert ov["publish_today"]["unlimited"] is False


# ---------- 发布规则读写白名单 ----------
def test_publish_rules_get_defaults(env):
    from app import share_center as sc
    S, _ = env
    with S() as db:
        rules = sc.publish_rules_get(db)
        assert "publish_paused" in rules
        assert "auto_cleanup_enabled" in rules
        assert rules["link_inactive_confirm_count"] == "2"


def test_publish_rules_set_whitelist(env):
    from app import share_center as sc
    S, _ = env
    with S() as db:
        sc.publish_rules_set(db, {"publish_paused": True, "share_daily_limit": 5,
                                  "evil_key": "pwned"})
        rules = sc.publish_rules_get(db)
        assert rules["publish_paused"] == "true"
        assert rules["share_daily_limit"] == "5"
        assert "evil_key" not in rules


# ---------- 分享记录 link_state 字段 ----------
def test_share_records_list_has_link_state(env):
    from app import share_center as sc
    from app.models import ShareRecord
    S, _ = env
    with S() as db:
        db.add(ShareRecord(work_key="work:C", resource_id=1, share_id="s3",
                           share_url="u3", share_status="active", current_flag=1,
                           link_state="inactive", verify_fail_count=2))
        db.commit()
        items = sc.share_records_list(db)
        row = next(r for r in items if r["work_key"] == "work:C")
        assert row["link_state"] == "inactive"
        assert row["verify_fail_count"] == 2
        assert "cleaned_at" in row and "clean_state" in row


# ---------- share_run 线程化静态守卫 ----------
def test_share_run_uses_to_thread_with_independent_session():
    main_src = (ROOT / "backend" / "app" / "main.py").read_text(encoding="utf-8")
    marker = "async def share_run("
    i = main_src.index(marker)
    seg = main_src[i:i + 320]
    assert "asyncio.to_thread" in seg, "share_run 必须 to_thread 防写锁冻结"
    assert "_run_share_cycle_in_thread" in seg
    # 全局守卫：不得有主协程裸 await run_cycle（publisher 已有同款守卫）
    assert "await _sc.run_cycle(db" not in main_src
    # helper 只能定义一次（重复定义会覆盖 _job_share_center 的调用），且独立 Session
    assert main_src.count("def _run_share_cycle_in_thread(") == 1
    helper = "def _run_share_cycle_in_thread("
    j = main_src.index(helper)
    helper_seg = main_src[j:j + 1300]
    assert "SessionLocal()" in helper_seg
    assert "asyncio.run" in helper_seg
    assert "rollback()" in helper_seg


# ---------- 前端分享中心全卡片化静态守卫（旧 5 页已移除，统一走 /api/sc/*） ----------
def test_frontend_share_center_cards_and_real_endpoints():
    app = (ROOT / "frontend" / "src" / "App.vue").read_text(encoding="utf-8")
    # 卡片化布局 + 规范业务模块标题（不再用 V2 等开发命名）
    for label in ("调度总览", "发布清单", "作品分类", "发布计划",
                  "时段窗口", "智能建议", "推送节奏", "发布任务"):
        assert label in app, f"缺模块卡片 {label}"
    assert "sc-board" in app and "sc-card" in app, "分享中心未卡片化"
    # 旧 5 页分页组件与旧链端点均已移除
    for gone in ("shareSubTab", "share/publish-overview", "share/followups",
                 "share/records?", "share/link/review", "share/link/cleanup"):
        assert gone not in app, f"旧模块仍残留 {gone}"
    # 新体系真实端点绑定
    for ep in ("/sc/overview", "/sc/pause", "/sc/push-lists", "/sc/tasks",
               "/sc/pacing-rules", "/sc/ai/generate"):
        assert ep in app, f"缺真实端点 {ep}"
    # nav key 兼容：原 key 保留
    assert "推送队列" in app
