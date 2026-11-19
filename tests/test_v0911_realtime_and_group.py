"""v0.9.11：进度看板实时化 + 作品去重计数 + 群聊自动推送根因 + 模版并排按钮。

真机反馈与修复：
  1. 进度卡片有延迟 —— 后端分阶段写 running/stage 快照，前端 2 秒轮询。
  2. 分享/推送没完成就显示 100% —— 进度条只有队列清空且非运行时才 100%。
  3. 入库识别/生成分享/推送入队显示集数 —— 改为按 TMDB 作品去重的剧集/电影部数。
  4. 群聊自动推送仍收不到 —— _ensure_route_channel 命中已有 Channel 时不同步 bot_id，
     测试用新 Bot（能收）、正式用旧 bot_id（失效/不在群）。改为运行时真相源强制校准。
  5. 白名单 deny 队列永久卡死 —— next_retry_at=None + status 仍 pending，每轮取出每轮 deny。
     改为计退避重试。
  6. sendPhoto 的 HTTP 200 + ok=false 不走回退 —— 补条件。
  7. 三个跳转按钮并排同一行、简介压到 70 字。
"""
import asyncio
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from app import bot_listener, main, publisher, runtime_config
from app.db import SessionLocal, migrate
from app.models import Bot, Channel, MirrorRule, QueueItem, Resource, AppConfig
from app.security import encrypt_secret

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")

TABLES = (QueueItem, Resource, Channel, Bot, MirrorRule, AppConfig)

CFG_KEYS = ("tg_channel_link", "tg_group_link", "tg_bot_link", "tg_link_style",
            "interaction_bot_id", "contribution_bot_token", "tg_detail_level")


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as s:
        for m in TABLES:
            s.query(m).delete()
        for k in CFG_KEYS:
            row = s.get(AppConfig, k)
            if row:
                s.delete(row)
        s.commit()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as s:
        yield s


def _rule():
    with SessionLocal() as s:
        r = MirrorRule(name="r", source_root_id="A", target_root_id="B",
                       enabled=True, auto_identify=True)
        s.add(r); s.commit()
        return r.id


# ---------- 作品去重 helper ----------
def test_work_type_key_and_breakdown():
    assert main._work_type_key("tv", 123) == ("tv", "123")
    assert main._work_type_key("movie", 456) == ("movie", "456")
    assert main._work_type_key("", 123) is None       # 类型不明不算
    assert main._work_type_key("tv", None) is None    # 无 TMDB ID 不算
    assert main._work_type_key("tv", "abc") is None   # 非数字 ID 不算
    keys = {("tv", "1"), ("tv", "2"), ("movie", "9"), ("tv", "1")}  # 重复 tv:1 去重
    assert main._work_breakdown(keys) == {"tv": 2, "movie": 1, "total": 3}


# ---------- 快照带 works / running / stage ----------
def test_snapshot_carries_works_and_runtime_stage():
    rid = _rule()
    with SessionLocal() as s:
        r = s.get(MirrorRule, rid)
        main._write_pipeline_snapshot(
            s, r, mode="镜像同步", stage="identify", running=True, copied=3, identified=8,
            works={"identified": {"tv": 2, "movie": 1, "total": 3},
                   "queued": {"tv": 2, "movie": 0, "total": 2}})
        s.commit()
        snap = json.loads(r.last_pipeline_json)
        assert snap["stage"] == "identify" and snap["running"] is True
        assert snap["works"]["identified"] == {"tv": 2, "movie": 1, "total": 3}


# ---------- 进度节点：入库识别/推送入队显示部数；进度条完成条件 ----------
def _seed_resource(s, *, media_type="tv", tmdb_id, status="sent", poster=""):
    r = Resource(file_id=f"f-{tmdb_id}-{status}", name=f"w{tmdb_id}.mkv", ext=".mkv", size=1,
                 utime=1, identify_status="identified", media_type=media_type,
                 media_title=f"w{tmdb_id}", tmdb_id=tmdb_id, poster_path=poster)
    s.add(r); s.flush()
    return r


def test_mirror_status_works_breakdown_and_progress_not_100():
    """有识别部数但队列未清空（running）时，节点显示剧集/电影部数、进度 <100。"""
    rid = _rule()
    with SessionLocal() as s:
        b = Bot(name="b", token=encrypt_secret("t"), enabled=True); s.add(b); s.flush()
        c = Channel(bot_id=b.id, name="c", chat_id="-100", kind="group", enabled=True)
        s.add(c); s.flush()
        r1 = _seed_resource(s, tmdb_id=11)              # tv
        r2 = _seed_resource(s, media_type="movie", tmdb_id=22)  # movie
        q = QueueItem(resource_id=r1.id, channel_id=c.id, status="pending"); s.add(q)
        rr = s.get(MirrorRule, rid)
        main._write_pipeline_snapshot(
            s, rr, mode="镜像同步", stage="done", running=False, copied=2, identified=2, queued=2,
            total_files=2,
            works={"identified": {"tv": 1, "movie": 1, "total": 2},
                   "queued": {"tv": 1, "movie": 1, "total": 2}})
        s.commit()
        status = main.mirror_status(db=s)
    pipe = {n["key"]: n for n in status["pipeline"]}
    assert "剧集 1 部" in pipe["identify"]["detail"]
    assert "电影 1 部" in pipe["identify"]["detail"]
    assert pipe["identify"]["value"] == 2  # 部数，不是集数
    assert "剧集" in pipe["push"]["detail"]
    # 队列还有 pending -> 不允许 100%
    assert status["progress"]["percent"] < 100


def test_mirror_status_progress_100_when_queue_drained():
    """队列清空且非运行时进度可到 100。"""
    rid = _rule()
    with SessionLocal() as s:
        rr = s.get(MirrorRule, rid)
        main._write_pipeline_snapshot(
            s, rr, mode="镜像同步", stage="done", running=False, copied=2, identified=2, queued=2,
            total_files=2,
            works={"identified": {"tv": 2, "movie": 0, "total": 2},
                   "queued": {"tv": 2, "movie": 0, "total": 2}})
        s.commit()
        status = main.mirror_status(db=s)
    assert status["progress"]["percent"] == 100.0


# ---------- 群聊自动推送根因：运行时同步 bot_id ----------
def test_ensure_route_channel_syncs_bot_id_on_hit():
    """命中已有路由 Channel 时把 bot_id 校准到全局 Bot（存量数据自愈）。"""
    with SessionLocal() as s:
        old = Bot(name="old", token=encrypt_secret("old"), enabled=True); s.add(old); s.flush()
        new = Bot(name="new", token=encrypt_secret("new"), enabled=True); s.add(new); s.flush()
        ch = Channel(bot_id=old.id, name="route:group:-999", chat_id="-999",
                     kind="group", enabled=True)
        s.add(ch); s.flush()
        rt = runtime_config.RuntimeConfig if hasattr(runtime_config, "RuntimeConfig") else None
        runtime_config.set_many(s, {"interaction_bot_id": str(new.id)})
        s.commit()
        got = main._ensure_route_channel(s, {"kind": "group", "chat_id": "-999"})
        s.commit()
        assert got.id == ch.id
        assert got.bot_id == new.id, "运行时入队命中已有 Channel 必须同步 bot_id"


# ---------- 白名单 deny 不再永久卡死 ----------
def test_process_queue_whitelist_deny_retries(db):
    b, c, r, q = _fresh_queue(db)
    runtime_config.set_many(db, {"bot_gate_mode": "whitelist", "bot_push_channels_json": "[]",
                                 "bot_push_groups_json": "[]"})
    db.commit()
    with patch("app.bot_listener.is_push_allowed", return_value=(False, "不在白名单")):
        asyncio.run(publisher.process_queue(limit=5))
    db.refresh(q)
    assert q.status in ("retry", "failed"), "deny 不能再让队列项永久 pending 原地卡死"
    assert q.attempts >= 1
    assert "白名单" in q.error or "不在白名单" in q.error


def _fresh_queue(db):
    b = Bot(name="b", token=encrypt_secret("t"), enabled=True); db.add(b); db.flush()
    c = Channel(bot_id=b.id, name="c", chat_id="-100", kind="group", enabled=True)
    db.add(c); db.flush()
    r = Resource(file_id="q1", name="m.mkv", ext=".mkv", size=1, utime=1,
                 identify_status="identified", media_type="movie", media_title="m",
                 poster_path="", share_url="https://share/x")
    db.add(r); db.flush()
    q = QueueItem(resource_id=r.id, channel_id=c.id, status="pending"); db.add(q); db.commit()
    return b, c, r, q


# ---------- sendPhoto HTTP 200 + ok=false 也回退 ----------
def test_sendphoto_200_ok_false_falls_back(db):
    b, c, r, q = _fresh_queue(db)
    r.poster_path = "/p.jpg"; db.commit()
    calls = []

    class Resp:
        def __init__(self, ok, status, mid="8"):
            self.status_code = status; self.text = ""
            self._d = {"ok": ok, "result": {"message_id": mid}}
        def json(self):
            return self._d

    async def fake_post(self, url, json=None, **kw):
        calls.append((url.rsplit("/", 1)[-1], json))
        return Resp(False, 200) if "sendPhoto" in url else Resp(True, 200)

    with patch("httpx.AsyncClient.post", new=fake_post):
        ok, mid, err, _ = asyncio.run(publisher._send_telegram(db, q, r, c, b))
    assert ok is True and mid == "8"
    assert [m for m, _ in calls] == ["sendPhoto", "sendMessage"]


# ---------- 简介压到 70 字 ----------
def test_overview_compact_70(db):
    _, _, r, _ = _fresh_queue(db)
    r.overview = "剧情简介" * 40
    db.commit()
    text, _ = publisher._build_message(db, r, "https://share/x")
    # 70 字上限 + 省略号；完整 160 字不该出现
    assert "…" in text
    assert ("剧情简介" * 40) not in text
    # 模板若带简介变量，渲染出的简介应在 70 字出头（截断 + 省略号）
    idx = text.find("剧情简介")
    if idx >= 0:
        # 从第一次出现处数到省略号，长度受控
        seg = text[idx:text.find("…", idx) + 1]
        assert len(seg) <= 72


# ---------- 前端：2 秒轮询 + stageLabel + 并排按钮提示 ----------
def test_frontend_realtime_poll_and_labels():
    assert "setInterval(pollMirrorStatus, 2000)" in APP
    assert "mirrorPollTimer" in APP and "clearInterval(mirrorPollTimer)" in APP
    assert "复制到 B 库" in APP and "生成分享 / 推送" in APP
    assert "剧集部数" in APP and "电影部数" in APP
    assert "每 2 秒自动刷新" in APP


# ---------- 真机根因①：路由 dir_id 填 B 库根 ID 也要匹配季子目录资源 ----------
def test_match_route_root_id_treated_as_whole_library(db):
    """实测：路由 dir_id=B 库根 ID、资源在季子目录、dir_chain 不含根 → 必须视为通配。"""
    import json
    from app import bot_listener
    runtime_config.set_many(db, {"push_routes_json": json.dumps([
        {"dir_id": "B_ROOT", "channel_id": "-100CH", "group_id": "-100GRP", "note": ""}])})
    db.commit()
    res = Resource(file_id="root-match", name="S01E01.mkv", ext=".mkv", size=1, utime=1,
                   parent_id="SEASON_DIR", dir_chain_json=json.dumps(["SEASON_DIR", "SHOW_DIR"]))
    db.add(res); db.commit()
    # 不传 root_ids：老逻辑，不匹配
    assert bot_listener.match_push_routes(db, res) == []
    # 传 B 库根：匹配频道 + 群两条
    matched = bot_listener.match_push_routes(db, res, root_ids=["B_ROOT"])
    assert {m["kind"] for m in matched} == {"channel", "group"}


def test_scan_root_ids_collects_mirror_and_scan_roots(db):
    """root_ids 从 MirrorRule.target_root_id 与 ScanTask.root_id 收集。"""
    from app.models import ScanTask
    s = MirrorRule(name="r", source_root_id="A", target_root_id="B_ROOT", enabled=True)
    db.add(s)
    t = ScanTask(name="scan", root_id="SCAN_ROOT", enabled=True)
    db.add(t); db.commit()
    roots = bot_listener.scan_root_ids(db)
    assert "B_ROOT" in roots and "SCAN_ROOT" in roots


# ---------- 真机根因②：手动「立即推送」要同时覆盖 push_routes 的频道与群 ----------
def test_publish_resource_enqueues_group_route(db):
    """实测：规则表只推频道；群只在 push_routes。手动推送必须也入队群。"""
    import json
    from httpx import AsyncClient
    b = Bot(name="b", token=encrypt_secret("t"), enabled=True); db.add(b); db.flush()
    db.commit()
    runtime_config.set_many(db, {
        "interaction_bot_id": str(b.id),
        "push_routes_json": json.dumps([
            {"dir_id": "", "channel_id": "-100CH", "group_id": "-100GRP", "note": ""}])})
    db.commit()
    res = Resource(file_id="pub", name="movie.mkv", ext=".mkv", size=1, utime=1,
                   identify_status="identified", media_type="movie", tmdb_id=7,
                   media_title="Movie", poster_path="/p.jpg", share_url="https://share/x")
    db.add(res); db.commit()

    captured = []
    class Resp:
        status_code = 200
        text = ""
        def json(self):
            return {"ok": True, "result": {"message_id": "99"}}
    async def fake_post(self, url, json=None, **kw):
        captured.append((json or {}).get("chat_id"))
        return Resp()
    with patch("httpx.AsyncClient.post", new=fake_post):
        out = asyncio.run(main.publish_resource(res.id, db=db))
    assert out["queued"] >= 2, "手动推送应为频道+群两条入队"
    assert "-100CH" in captured and "-100GRP" in captured, "群 chat_id 必须收到推送"
