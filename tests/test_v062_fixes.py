"""v0.6.2 回归测试：发布组目录下剧集散落、推送链路可观测、界面重构。

1. [高清剧集网发布 www.THDT] 目录下的 Tian.Xian.Pei.S01E28.2007 不再把站点名当剧名
2. 目录线索不可信时，文件名命中允许自动落库（不再一律转人工）
3. 真实「中文目录剧名 + 英文单集名」仍需转人工（v0.6.1 保护不回退）
4. 立即推送端点产生队列项并返回结构化结果（失败原因可见）
"""
import os
from unittest.mock import patch

os.environ.setdefault("ADMIN_PASSWORD", "testpass")
os.environ.setdefault("APP_SECRET", "x" * 40)

from fastapi.testclient import TestClient
from sqlalchemy import select

from app import tmdb
from app.db import SessionLocal, migrate
from app.main import app
from app.models import Bot, Channel, QueueItem, Resource, Rule
from app.security import encrypt_secret
from app.utils import parse_media_name

H = {"X-Admin-Password": "testpass"}
SITE_DIR = "推广专用/[高清剧集网发布 www.THDT.cc]"
FILE = "Tian.Xian.Pei.S01E28.2007.1080p.WEB-DL.H265.AAC-BlackTV.mkv"


class FakeResponse:
    def __init__(self, data, status_code=200):
        self._data, self.status_code = data, status_code
        self.text, self.headers = str(data), {}

    def json(self):
        return self._data


def _clean():
    db = SessionLocal()
    db.query(QueueItem).delete(synchronize_session=False)
    db.query(Rule).delete(synchronize_session=False)
    db.query(Channel).delete(synchronize_session=False)
    db.query(Bot).delete(synchronize_session=False)
    db.query(Resource).filter(Resource.file_id.like("v062-%")).delete(synchronize_session=False)
    db.commit()
    db.close()


def _candidate(title="天仙配", mt="tv", tmdb_id=206484, year="2007"):
    return {"tmdb_id": tmdb_id, "media_type": mt, "type": mt,
            "title": title, "original_title": "Tian Xian Pei", "year": year,
            "poster_path": "", "overview": "简介", "vote_average": 7.0,
            "popularity": 50, "genres": ["剧情"], "origin_country": ["CN"],
            "score": 0.0}


# ---------- 1. 发布组目录不污染作品名 ----------
def test_site_dir_is_not_primary_title():
    h = parse_media_name(FILE, SITE_DIR)
    assert h["dir_title"] == ""
    assert h["dir_unreliable"] is True
    assert h["primary_title"] and "高清剧集网" not in h["primary_title"]
    assert "Tian" in h["primary_title"] and "Xian" in h["primary_title"]
    assert h["type_hint"] == "tv" and h["season"] == 1 and h["episode"] == 28
    assert h["year"] == "2007"


def test_real_directory_title_still_wins():
    """v0.6.1 保护：真实中文目录剧名不能被英文单集文件名顶掉。"""
    h = parse_media_name("The.Silent.Storm.S01E04.2026.mp4", "谷雨 (2026)(1)/Season 1")
    assert h["primary_title"] == "谷雨"
    assert h["dir_unreliable"] is False
    assert h["alt_title"] and "Silent" in h["alt_title"]


# ---------- 2. 不可信目录允许文件名自动落库 ----------
def test_site_dir_filename_hit_auto_identified():
    import asyncio

    async def fake_search(query, mt, db=None, **kwargs):
        return [_candidate()] if "Tian" in query else []

    with patch.object(tmdb, "search", fake_search), \
         patch.object(tmdb, "search_both", lambda q, db=None: asyncio.run(fake_search(q, "", db))):
        d = asyncio.run(tmdb.identify(FILE, path_hint=SITE_DIR, use_ai=False))
    assert d["status"] == "identified", d
    assert d["best"]["title"] == "天仙配"
    assert d["hint"]["dir_unreliable"] is True


# ---------- 4. 推送端点产生可观测队列项 ----------
def test_publish_creates_queue_item_with_result():
    migrate()
    _clean()
    db = SessionLocal()
    b = Bot(name="v062-bot", token=encrypt_secret("123:FAKE"), enabled=True)
    db.add(b); db.flush()
    c = Channel(name="v062-ch", bot_id=b.id, chat_id="-100123", enabled=True)
    db.add(c); db.flush()
    r = Resource(file_id="v062-res", name=FILE, ext=".mkv", size=1, utime=1,
                 share_url="https://share.example/x", share_status="active",
                 identify_status="identified", media_type="tv")
    db.add(r); db.flush()
    db.add(Rule(name="v062-rule", channel_id=c.id, auto_publish=True, enabled=True))
    db.commit()
    rid, cid = r.id, c.id
    db.close()

    async def ok_post(self, url, **kwargs):
        return FakeResponse({"ok": True, "result": {"message_id": 99}})

    client = TestClient(app)
    with patch("httpx.AsyncClient.post", ok_post):
        resp = client.post(f"/api/resources/{rid}/publish", headers=H)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["queued"] >= 1 and body["sent"] >= 1

    q = client.get("/api/queue", headers=H)
    assert q.status_code == 200
    assert any(item["resource"] == FILE for item in q.json())
    _clean()
