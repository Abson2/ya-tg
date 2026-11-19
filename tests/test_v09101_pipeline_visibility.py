"""v0.9.10.1：镜像/识别成功后「卡在生成分享、推送入队」的可观测性修复。

真机反馈：镜像成功、识别成功，进度节点看起来卡在④生成分享、⑤推送入队。
根因两条：
  1. ④生成分享 value 写死 0、status 有快照就恒 active —— 永远是「0 且进行中」的假卡死。
  2. 入队拦截诊断 enqueue_diag 没进本轮快照，节点⑤只在 last_message 字符串里翻，
     被截断/覆盖后用户看不到「为什么入队 0」。
修复：快照带 enqueue_diag；节点④显示队列实时待推数；节点⑤直接读快照诊断。
"""
import json
from pathlib import Path

import pytest

from app import main
from app.db import SessionLocal, json_dumps, migrate
from app.models import Bot, Channel, MirrorRule, QueueItem, Resource
from app.utils import utcnow

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")

TABLES = (QueueItem, Resource, Channel, Bot, MirrorRule)


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as s:
        for m in TABLES:
            s.query(m).delete()
        s.commit()
    yield
    with SessionLocal() as s:
        for m in TABLES:
            s.query(m).delete()
        s.commit()


def _rule():
    with SessionLocal() as s:
        r = MirrorRule(name="r", source_root_id="A", target_root_id="B",
                       enabled=True, auto_identify=True)
        s.add(r); s.commit()
        return r.id


def test_snapshot_carries_enqueue_diag():
    """写快照时把入队诊断结构化落库，供节点直接读。"""
    rid = _rule()
    with SessionLocal() as s:
        r = s.get(MirrorRule, rid)
        main._write_pipeline_snapshot(
            s, r, mode="镜像同步", copied=3, identified=5, queued=0,
            total_files=8, enqueue_diag={"skip_no_poster": 4, "skip_route_no_match": 1})
        s.commit()
        snap = json.loads(r.last_pipeline_json)
        assert snap["enqueue_diag"] == {"skip_no_poster": 4, "skip_route_no_match": 1}
        assert snap["queued"] == 0 and snap["identified"] == 5


def test_node5_shows_diag_from_snapshot():
    """入队 0 时，节点⑤ detail 直接显示快照里的拦截原因。"""
    rid = _rule()
    with SessionLocal() as s:
        r = s.get(MirrorRule, rid)
        snap = {"mode": "镜像同步", "copied": 3, "renamed": 0, "identified": 5,
                "queued": 0, "total_files": 8, "at": utcnow().isoformat(),
                "enqueue_diag": {"skip_no_poster": 4}}
        r.last_pipeline_json = json_dumps(snap)
        s.commit()
        out = main.mirror_status(db=s)
        nodes = {n["key"]: n for n in out["pipeline"]}
        assert "无海报闸门 4" in nodes["push"]["detail"]
        assert nodes["push"]["value"] == 0


def test_node4_not_stuck_zero_active():
    """④生成分享反映队列真实状态：有待推项则 active，全空则 idle，不再恒 0+active。"""
    rid = _rule()
    with SessionLocal() as s:
        r = s.get(MirrorRule, rid)
        r.last_pipeline_json = json_dumps(
            {"mode": "镜像同步", "identified": 3, "queued": 2,
             "at": utcnow().isoformat(), "enqueue_diag": {"enqueued": 2}})
        s.commit()
        out = main.mirror_status(db=s)
        share = {n["key"]: n for n in out["pipeline"]}["share"]
        assert share["status"] == "idle" and share["value"] == 0
        # 造一条待推队列项 -> active
        b = Bot(name="b", token="x", enabled=True); s.add(b); s.flush()
        ch = Channel(bot_id=b.id, name="c", chat_id="-1", enabled=True); s.add(ch); s.flush()
        res = Resource(file_id="f", name="r", identify_status="identified",
                       poster_path="/p.jpg"); s.add(res); s.flush()
        s.add(QueueItem(resource_id=res.id, channel_id=ch.id, status="pending")); s.commit()
        out = main.mirror_status(db=s)
        share = {n["key"]: n for n in out["pipeline"]}["share"]
        # v0.9.10.2：value=已分享部数、total=队列作品数；待推时 value<total 且 active
        assert share["status"] == "active" and share["value"] == 0 and share["total"] == 1
        assert "队列" in share["detail"]


def test_frontend_hint_matches_current_semantics():
    # v0.9.10.2：提示改为「2 秒自动刷新 + 作品部数口径 + 分享清空才 100%」
    assert "每 2 秒自动刷新" in APP
    assert "剧集部数" in APP and "电影部数" in APP
    assert "累积队列数" not in APP
