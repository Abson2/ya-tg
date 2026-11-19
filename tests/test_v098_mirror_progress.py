"""v0.9.8 #195：镜像同步进度显示 + 入库→识别→分享→推送 流程节点。"""
from pathlib import Path

import pytest

from app import main
from app.db import SessionLocal, migrate
from app.models import (Bot, Channel, DistributionEvent, MirrorFile, MirrorMedia,
                        MirrorRule, QueueItem, Resource, utcnow)

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


@pytest.fixture
def session():
    migrate()
    with SessionLocal() as s:
        for m in (MirrorFile, MirrorMedia, MirrorRule, QueueItem,
                  DistributionEvent, Resource, Channel, Bot):
            s.query(m).delete()
        s.commit()
        try:
            yield s
        finally:
            for m in (MirrorFile, MirrorMedia, MirrorRule, QueueItem,
                      DistributionEvent, Resource, Channel, Bot):
                s.query(m).delete()
            s.commit()


def _seed(session):
    rule = MirrorRule(name="prog", source_root_id="A", target_root_id="B",
                      enabled=True, auto_identify=True)
    session.add(rule); session.flush()
    media = MirrorMedia(rule_id=rule.id, work_key="wk", title_standard="剧",
                        status="synced")
    session.add(media); session.flush()
    for i in range(3):
        session.add(MirrorFile(media_id=media.id, rule_id=rule.id,
                               identity=f"tmdb:1:tv:1:{i}", source_file_id=f"s{i}",
                               file_name=f"e{i}.mkv", size=1, utime=1,
                               fingerprint=f"1:{i}",
                               status="synced" if i < 2 else "pending"))
    res = Resource(file_id="fb", name="剧", parent_id="*",
                   identify_status="identified", poster_path="/p.jpg",
                   share_url="https://share/x")
    session.add(res); session.flush()
    bot = Bot(name="b", token="x", enabled=True)
    session.add(bot); session.flush()
    ch = Channel(bot_id=bot.id, name="ch", chat_id="-1", enabled=True)
    session.add(ch); session.flush()
    session.add(QueueItem(resource_id=res.id, channel_id=ch.id, status="sent",
                          sent_at=utcnow()))
    session.commit()


def test_pipeline_nodes_present(session):
    _seed(session)
    out = main.mirror_status(db=session)
    nodes = {n["key"]: n for n in out["pipeline"]}
    assert set(nodes) == {"copy", "rename", "identify", "share", "push"}
    assert nodes["copy"]["value"] == 2 and nodes["copy"]["total"] == 3
    assert nodes["copy"]["status"] == "active"
    assert nodes["identify"]["value"] == 1
    assert nodes["share"]["value"] == 1
    assert nodes["push"]["value"] == 1
    for n in nodes.values():
        assert n["label"] and n["detail"]


def test_progress_percent(session):
    _seed(session)
    out = main.mirror_status(db=session)
    assert out["progress"]["synced"] == 2
    assert out["progress"]["total"] == 3
    assert out["progress"]["percent"] == 66.7


def test_pipeline_empty_ok(session):
    out = main.mirror_status(db=session)
    assert out["progress"]["percent"] == 0.0
    assert len(out["pipeline"]) == 5


def test_frontend_progress_ui():
    assert "处理流程与进度" in APP
    assert "pipeline" in APP and "progress" in APP
    assert "pipeline-flow" in APP and "sync-progress" in APP
    assert "镜像 → 入库 → 推送 进度" in APP
