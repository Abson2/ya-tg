"""v0.6.1 回归测试：用户反馈的 4 个 bug + 两项追加需求。

1. 谷雨 (2026)(1)/Season 1/The.Silent.Storm.S01E04 不再被认成「寂静之噪」
2. 中文剧名优先，中文标题可自动译成英/日/韩标题多语搜索（带海报）
3. 保存规则后回溯已扫描资源入队，规则校验给中文提示而非 [object Object]
4. Bot 测试推送真实发消息；Bot/频道独立启停，停用跳过队列不算失败
5. 扫描记录目录名链到 Resource.path_hint
"""
import asyncio
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import select

from app import publisher, tmdb
from app.db import SessionLocal, migrate
from app.main import app
from app.models import AppConfig, Bot, Channel, QueueItem, Resource, Rule, ScanCursor
from app.security import encrypt_secret
from app.utils import parse_media_name

H = {"X-Admin-Password": "testpass"}
FILE = "The.Silent.Storm.S01E04.2026.2160p.WEB-DL.HDR10.HQ.H265 10bit.AAC.mp4"
DIR_PATH = "谷雨 (2026)(1)/Season 1"


class FakeResponse:
    def __init__(self, data, status_code=200, headers=None):
        self._data, self.status_code, self.text = data, status_code, str(data)
        self.headers = headers or {}

    def json(self):
        return self._data


def _item(title, mt, tmdb_id, year="", poster=""):
    return {
        "tmdb_id": tmdb_id, "media_type": mt, "type": mt,
        "type_label": tmdb.type_label(mt), "title": title,
        "original_title": "The Silent Storm" if title == "谷雨" else title,
        "year": year, "poster_path": poster, "backdrop_path": "", "overview": "简介",
        "vote_average": 7.2, "popularity": 90, "genres": ["剧情"],
        "origin_country": ["CN"], "score": 0.0,
    }


def _wipe():
    db = SessionLocal()
    res_ids = [r.id for r in db.scalars(
        select(Resource).where(Resource.file_id.like("v061-%"))).all()]
    if res_ids:
        db.query(QueueItem).filter(QueueItem.resource_id.in_(res_ids)).delete(
            synchronize_session=False)
    db.query(QueueItem).filter(QueueItem.id > 0).delete(synchronize_session=False)
    db.query(Rule).filter(Rule.name.like("v061-%")).delete(synchronize_session=False)
    db.query(Channel).filter(Channel.name.like("v061-%")).delete(synchronize_session=False)
    db.query(Bot).filter(Bot.name.like("v061-%")).delete(synchronize_session=False)
    db.query(Resource).filter(Resource.file_id.like("v061-%")).delete(synchronize_session=False)
    db.query(ScanCursor).delete(synchronize_session=False)
    db.query(AppConfig).filter(AppConfig.key == "telegram_api_base").delete(
        synchronize_session=False)
    db.commit()
    db.close()


def _seed():
    migrate()
    _wipe()
    db = SessionLocal()
    b = Bot(name="v061-bot", token=encrypt_secret("123:FAKE"), enabled=True)
    db.add(b)
    db.flush()
    c = Channel(name="v061-channel", bot_id=b.id, chat_id="-100123", enabled=True)
    db.add(c)
    db.flush()
    res = Resource(file_id="v061-res", name="x.mkv", ext=".mkv", size=1, utime=1,
                   share_url="https://share.example/x")
    db.add(res)
    db.commit()
    ids = (b.id, c.id, res.id)
    db.close()
    return ids


# ---------- 1. 目录名优先 + 中文剧名 ----------
def test_directory_name_is_primary_title():
    h = parse_media_name(FILE, DIR_PATH)
    assert h["primary_title"] == "谷雨" and h["clean_title"] == "谷雨"
    assert h["dir_title"] == "谷雨" and h["title_source"] == "directory"
    assert h["has_chinese"] is True
    assert h["type_hint"] == "tv" and h["season"] == 1 and h["episode"] == 4
    assert h["year"] == "2026"
    assert h["alt_title"]  # 英文单集文件名保留为备用，但不作为主查询


def test_chinese_dir_wins_and_english_fallback_never_autopicks():
    calls = []

    async def fake_search(title, media_type=None, db=None):
        calls.append(title)
        if title == "谷雨":
            return [_item("谷雨", "tv", 88888, "2026", "/poster.jpg")]
        if "Silent" in title:
            return [_item("寂静之噪", "tv", 314531, "2019")]
        return []

    with patch.object(tmdb, "search", fake_search):
        r = asyncio.run(tmdb.identify(FILE, None, path_hint=DIR_PATH, use_ai=False))
    assert r["ok"] and r["best"]["tmdb_id"] == 88888
    assert r["best"]["title"] == "谷雨" and calls[0] == "谷雨"

    async def only_english(title, media_type=None, db=None):
        return [_item("The Silent Noise", "tv", 314531, "2019")] if "Silent" in title else []

    with patch.object(tmdb, "search", only_english):
        r2 = asyncio.run(tmdb.identify(FILE, None, path_hint=DIR_PATH, use_ai=False))
    assert r2["status"] == "ambiguous" and r2["best"] is None
    assert "手动匹配" in r2["reason"]


def test_english_title_is_replaced_by_chinese_display_name():
    async def fake_search(title, media_type=None, db=None):
        return [_item("The Silent Storm", "tv", 88888, "2026")] if title == "谷雨" else []

    with patch.object(tmdb, "search", fake_search):
        r = asyncio.run(tmdb.identify(FILE, None, path_hint=DIR_PATH, use_ai=False))
    assert r["best"]["title"] == "谷雨" and r["best"]["title_source"] == "directory"


# ---------- 2. 中文标题多语翻译搜索 ----------
def test_multilingual_search_returns_posters_and_translated_queries():
    async def fake_translate(title, year="", db=None):
        return {"ok": True, "queries": ["The Silent Storm", "グーユー"],
                "message": "已生成外语标题"}

    async def fake_search(title, media_type=None, db=None):
        if title == "谷雨":
            return [_item("谷雨", "tv", 88888, "2026", "/p.jpg")]
        if title == "The Silent Storm":
            return [_item("谷雨", "tv", 88888, "2026", "/p.jpg"),
                    _item("The Silent Storm", "tv", 99999, "2026", "/p2.jpg")]
        return []

    with patch("app.ai_assist.translate_title", fake_translate), \
         patch.object(tmdb, "search", fake_search):
        r = asyncio.run(tmdb.search_multilingual("谷雨", "tv", "2026", None, None))
    assert r["ok"] and r["translated"] == ["The Silent Storm", "グーユー"]
    assert any(t["query"] == "The Silent Storm" for t in r["tried"])
    assert any(str(c.get("poster_url") or "").endswith("p.jpg") for c in r["candidates"])
    # 同一 tmdb_id+type 只保留一条
    keys = [(c["media_type"], c["tmdb_id"]) for c in r["candidates"]]
    assert len(keys) == len(set(keys))


# ---------- 3. 扫描记录目录名链 ----------
class _FakeFiles:
    def __init__(self, db):
        pass

    async def list_files(self, parent_id="", page=0, page_size=100):
        if parent_id in ("", "*"):
            return {"list": [{"fileId": "v061-d1", "fileName": "谷雨 (2026)(1)", "resType": 2}],
                    "total": 1}
        if parent_id == "v061-d1":
            return {"list": [{"fileId": "v061-d2", "fileName": "Season 1", "resType": 2}],
                    "total": 1}
        if parent_id == "v061-d2":
            return {"list": [{"fileId": "v061-file", "fileName": FILE, "resType": 1,
                              "parentId": "v061-d2", "fileSize": 123, "utime": 1}], "total": 1}
        return {"list": [], "total": 0}


def test_scan_persists_directory_path_hint_and_cursor():
    migrate()
    _wipe()
    from app.scanner import run_scan
    db = SessionLocal()
    with patch("app.scanner.GuangYaWebFiles", _FakeFiles), \
         patch("app.scanner.GuangYaOpenAPI", _FakeFiles):
        result = asyncio.run(run_scan("", resume=False, db=db))
    assert result["files"] == 1
    row = db.scalar(select(Resource).where(Resource.file_id == "v061-file"))
    assert row is not None and row.path_hint == DIR_PATH
    cursor = db.get(ScanCursor, 1)
    assert cursor and "谷雨" in (cursor.dir_paths_json or "")
    db.close()
    _wipe()


# ---------- 4. 规则保存回溯入队 / 中文校验 / 立即推送 ----------
def test_rule_save_backfills_queue_and_gives_friendly_error():
    bid, cid, resid = _seed()
    # v0.6.5：回溯只补「已识别 + 有海报」的资源，未识别资源不应入队
    db = SessionLocal()
    res = db.get(Resource, resid)
    res.identify_status = "identified"
    res.media_type = "movie"
    res.media_title = "测试影片"
    res.poster_path = "/poster.jpg"
    db.commit(); db.close()
    c = TestClient(app)
    r = c.post("/api/rules", headers=H,
               json={"name": "v061-rule", "channel_id": cid})
    assert r.status_code == 200, r.text
    assert r.json()["queued"] >= 1 and "自动发布" in r.json()["message"]
    db = SessionLocal()
    q = db.scalar(select(QueueItem).where(QueueItem.resource_id == resid,
                                          QueueItem.channel_id == cid))
    assert q is not None and q.status == "pending"  # auto_publish 默认开启
    db.close()

    bad = c.post("/api/rules", headers=H, json={"name": "v061-no-channel"})
    assert bad.status_code == 400 and "目标频道" in str(bad.json()["detail"])
    _wipe()


def test_bot_test_message_and_independent_toggles():
    bid, cid, resid = _seed()
    c = TestClient(app)
    calls = []

    async def fake_post(self, url, **kwargs):
        calls.append((url, kwargs.get("json") or {}))
        return FakeResponse({"ok": True, "result": {"message_id": 77}})

    with patch("httpx.AsyncClient.post", fake_post):
        m = c.post(f"/api/bots/{bid}/test-message", headers=H, json={})
    assert m.status_code == 200 and m.json()["ok"] is True
    assert m.json()["message_id"] == "77"
    assert any(u.endswith("/sendMessage") for u, _ in calls)

    off = c.put(f"/api/bots/{bid}", headers=H, json={"enabled": False})
    assert off.status_code == 200 and off.json()["enabled"] is False
    ch_off = c.put(f"/api/channels/{cid}", headers=H, json={"enabled": True})
    assert ch_off.status_code == 200 and ch_off.json()["enabled"] is True

    db = SessionLocal()
    db.add(QueueItem(resource_id=resid, channel_id=cid, status="pending"))
    db.commit()
    qid = db.scalar(select(QueueItem.id).where(QueueItem.resource_id == resid,
                                               QueueItem.channel_id == cid))
    db.close()
    with patch("httpx.AsyncClient.post", fake_post):
        asyncio.run(publisher.process_queue(limit=10))
    db = SessionLocal()
    q = db.get(QueueItem, qid)
    assert q.status == "pending" and q.attempts == 0  # 停用跳过，不计失败、不消耗重试
    assert "停用" in (q.error or "")
    db.close()

    c.put(f"/api/bots/{bid}", headers=H, json={"enabled": True})
    with patch("httpx.AsyncClient.post", fake_post):
        asyncio.run(publisher.process_queue(limit=10))
    db = SessionLocal()
    assert db.get(QueueItem, qid).status == "sent"  # 重新启用后自动继续
    db.close()
    _wipe()


def test_publish_resource_and_diagnostics():
    bid, cid, resid = _seed()
    c = TestClient(app)
    db = SessionLocal()
    db.add(Rule(name="v061-rule", channel_id=cid, auto_publish=True, enabled=True))
    db.commit()
    db.close()

    async def fake_post(self, url, **kwargs):
        return FakeResponse({"ok": True, "result": {"message_id": 88}})

    with patch("httpx.AsyncClient.post", fake_post):
        r = c.post(f"/api/resources/{resid}/publish", headers=H)
    assert r.status_code == 200 and r.json()["sent"] >= 1

    d = c.get("/api/diagnostics", headers=H)
    assert d.status_code == 200 and "queue" in d.json() and "blockers" in d.json()
    qr = c.post("/api/queue/run", headers=H)
    assert qr.status_code == 200
    _wipe()
