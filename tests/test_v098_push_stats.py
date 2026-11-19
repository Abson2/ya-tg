"""v0.9.8 #194：推送统计与图文报表（只做真实可得指标）。"""
import json
from pathlib import Path

import pytest

from app import main
from app.db import SessionLocal, migrate
from app.models import (Bot, Channel, DistributionEvent, QueueItem, Resource, utcnow)

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


@pytest.fixture
def session():
    migrate()
    with SessionLocal() as s:
        s.query(DistributionEvent).delete()
        s.query(QueueItem).delete()
        s.query(Resource).delete()
        s.query(Channel).delete()
        s.query(Bot).delete()
        s.commit()
        try:
            yield s
        finally:
            s.query(DistributionEvent).delete()
            s.query(QueueItem).delete()
            s.query(Resource).delete()
            s.query(Channel).delete()
            s.query(Bot).delete()
            s.commit()


def _seed(session):
    bot = Bot(name="stats-bot", token="x", enabled=True)
    session.add(bot); session.flush()
    ch = Channel(bot_id=bot.id, name="ch", chat_id="-100", enabled=True)
    session.add(ch); session.flush()
    res = Resource(file_id="f-stats-1", name="剧", parent_id="*")
    session.add(res); session.flush()
    now = utcnow()
    # QueueItem 有 (resource_id, channel_id) 唯一约束，每条用不同 resource
    for i, (status, attempts, sent_at) in enumerate([
        ("sent", 1, now), ("sent", 3, now), ("failed", 2, None)
    ]):
        r = Resource(file_id=f"f-stats-q{i}", name=f"剧{i}", parent_id="*")
        session.add(r); session.flush()
        session.add(QueueItem(resource_id=r.id, channel_id=ch.id, status=status,
                              attempts=attempts, sent_at=sent_at))
    session.add_all([
        DistributionEvent(work_key="w1", fingerprint="fp1", event_type="created"),
        DistributionEvent(work_key="w1", fingerprint="fp2", event_type="updated"),
    ])
    session.commit()


def test_push_stats_real_metrics(session):
    _seed(session)
    out = main.push_stats(period="day", days=7, db=session)
    assert out["totals"]["sent"] == 2
    assert out["totals"]["failed"] == 1
    assert out["totals"]["retries"] == 2      # 第二条 attempts=3 → +2
    assert out["totals"]["events"] == 2
    assert out["totals"]["success_rate"] == 66.7
    assert len(out["series"]) >= 1
    assert "sent" in out["series"][0] and "success_rate" in out["series"][0]


def test_push_stats_unavailable_explained(session):
    out = main.push_stats(db=session)
    keys = {x["key"] for x in out["unavailable"]}
    assert {"reads", "clicks", "transfers"}.issubset(keys)
    for item in out["unavailable"]:
        assert item["reason"]  # 每条都要有不可得原因，不造假数
    for item in ("阅读数", "发送成功数"):
        assert item in (out["available"] + [x["label"] for x in out["unavailable"]])


def test_push_stats_period_validated(session):
    with pytest.raises(Exception):
        main.push_stats(period="year", db=session)


def test_push_stats_empty_ok(session):
    out = main.push_stats(db=session)
    assert out["totals"]["success_rate"] == 0.0
    assert out["series"] == []


def test_frontend_report_ui():
    assert "推送效果报表" in APP
    assert "push-stats" in APP
    assert "阅读数" in APP and "点击数" in APP and "转存数" in APP
    assert "无法统计" in APP or "不可得" in APP


def test_report_is_on_overview_page():
    """报表必须放在首页「概览」，不能留在「分发与互动中心」。"""
    overview_start = APP.index("tab === '概览'")
    overview_end = APP.index("tab === '光鸭授权'")
    overview_html = APP[overview_start:overview_end]
    assert "推送效果报表" in overview_html
    assert "pushStats" in overview_html
    # 分发与互动中心 section（模板区）不应再含报表标题；截到 <style 前避免 CSS 注释干扰
    dist_start = APP.index("tab === '分发与互动中心'")
    dist_html = APP[dist_start:APP.index("<style")]
    assert "推送效果报表" not in dist_html
