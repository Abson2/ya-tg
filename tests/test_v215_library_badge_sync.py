"""v2.1.3 资源库「已分享 / 已推送」标签同步验收。

真机问题：资源库卡片上「已推」标签亮着、却没有「已分享」标签。
根因：「已分享」只看 legacy `Resource.share_status`，而**分享中心 v2 建链从不回写
该字段**——v2 建好的链接只落在 `sc_publish_tasks.share_url`。

覆盖：
  1. v2 已建链（active + 有链接）→ 资源库显示「已分享」（share_status=active）；
  2. v2 已推送（published）→ 资源库显示「已推送」（pushed=true）；
  3. 两个标签由同一份真相派生：已推送 ⇒ 已分享（不再出现矛盾组合）；
  4. legacy 已分享（share_status=active + share_url）仍然照常显示；
  5. 未建链未推送的作品仍显示「未分享 / 无已推送」；
  6. 前端：卡片状态文案中文化，不得暴露后端状态码（link_state 原值）。
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
    d = tempfile.mkdtemp(prefix="v215_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'v215.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _res(db, file_id="f1", title="黑豹2", share_status="not_created", share_url=""):
    from app.models import Resource
    r = Resource(file_id=file_id, name="bp.mkv", ext=".mkv", size=1, utime=1,
                 identify_status="identified", media_type="movie",
                 media_title=title, path_hint=f"{title} (2022)",
                 share_dir_name=f"{title} (2022)", share_dir_id="dir1",
                 poster_path="/p.jpg", share_status=share_status, share_url=share_url)
    db.add(r)
    db.commit()
    return r


def _v2_task(db, work_key="work:黑豹2", *, status="submitted", link_state="active",
             share_url="https://www.guangyapan.com/s/ok"):
    from app.models import SCPublishTask
    t = SCPublishTask(work_key=work_key, status=status, link_state=link_state,
                      share_url=share_url)
    db.add(t)
    db.commit()
    return t


def _series(env, **kw):
    """跑一次资源库聚合（真实端点逻辑）。"""
    from app.main import library_series
    S, _ = env
    with S() as db:
        rows = library_series(db=db)
        return {r["key"]: r for r in rows}


# ---------------------------------------------------------------------------
# 后端：两个标签同步
# ---------------------------------------------------------------------------

def test_v2_linked_work_shows_shared(env):
    """v2 已建链（active + 有链接）→ 资源库「已分享」。"""
    S, _ = env
    with S() as db:
        _res(db)
        _v2_task(db, link_state="active", share_url="https://www.guangyapan.com/s/ok")
        db.commit()
    rows = _series(env)
    g = rows["work:黑豹2"]
    assert g["share_status"] == "active", g
    assert g["share_url"] == "https://www.guangyapan.com/s/ok", g


def test_v2_published_work_shows_pushed_and_shared(env):
    """v2 已推送（published）→「已推送」且「已分享」，两个标签同步。"""
    S, _ = env
    with S() as db:
        _res(db)
        _v2_task(db, status="published", link_state="active",
                 share_url="https://www.guangyapan.com/s/ok")
        db.commit()
    rows = _series(env)
    g = rows["work:黑豹2"]
    assert g["pushed"] is True, g
    assert g["share_status"] == "active", "已推送必然已分享，两标签不得矛盾"


def test_pushed_implies_shared_even_without_link_record(env):
    """v2 任务已推送但链接字段为空时，也不该出现「已推但未分享」的矛盾。"""
    S, _ = env
    with S() as db:
        _res(db)
        _v2_task(db, status="published", link_state="unknown", share_url="")
        db.commit()
    rows = _series(env)
    g = rows["work:黑豹2"]
    assert g["pushed"] is True
    assert g["share_status"] == "active"


def test_legacy_shared_still_shows(env):
    """legacy 已分享（share_status=active + share_url）仍然照常显示。"""
    S, _ = env
    with S() as db:
        _res(db, share_status="active", share_url="https://www.guangyapan.com/s/legacy")
        db.commit()
    rows = _series(env)
    g = rows["work:黑豹2"]
    assert g["share_status"] == "active"
    assert g["share_url"] == "https://www.guangyapan.com/s/legacy"


def test_plain_work_shows_not_shared_not_pushed(env):
    """既没建链也没推送 → 「未分享 / 未推送」。"""
    S, _ = env
    with S() as db:
        _res(db)
        db.commit()
    rows = _series(env)
    g = rows["work:黑豹2"]
    assert g["share_status"] == "not_created"
    assert g["pushed"] is False


def test_legacy_sent_queue_item_counts_as_pushed(env):
    """legacy 队列有实际发送记录 → 「已推送」。

    判据是 `sent_at` 非空（真实发送时刻），不是 status 字面值——
    队列里 status=sent 但没写 sent_at 的历史数据不算已送达。
    """
    from datetime import datetime, timezone

    from app.models import QueueItem
    S, _ = env
    with S() as db:
        r = _res(db, share_status="active", share_url="https://www.guangyapan.com/s/x")
        db.add(QueueItem(resource_id=r.id, channel_id=1, status="sent",
                         sent_at=datetime.now(timezone.utc)))
        db.commit()
    rows = _series(env)
    g = rows["work:黑豹2"]
    assert g["pushed"] is True
    assert g["share_status"] == "active"


# ---------------------------------------------------------------------------
# 前端契约
# ---------------------------------------------------------------------------

def test_frontend_library_badges_are_chinese():
    """资源库卡片状态文案中文化，不得暴露后端状态码。"""
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "const mediaShareLabel = " in app
    assert "const mediaLinkLabel = " in app
    # 卡片用统一的中文映射
    assert "mediaShareLabel(g.share_status)" in app
    assert "mediaLinkLabel(g.link_state)" in app
    # 旧的原值直出必须消失
    assert "'链接状态（本地推断）：'+g.link_state" not in app
    assert "链接{{ g.link_state }}" not in app


def test_frontend_push_badge_text_is_full():
    """「已推」补全为「已推送」，与其它页面用词一致。"""
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert '>已推送</span>' in app
    assert '>已推</span>' not in app


def test_frontend_share_filter_labels_match_card():
    """筛选下拉与卡片标签用同一套说法。"""
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert '<el-option label="已分享" value="active"/>' in app
    assert '<el-option label="未分享" value="not_created"/>' in app
    assert 'label="已创建" value="active"' not in app
