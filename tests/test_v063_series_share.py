"""v0.6.3 回归测试：剧集目录级分享 + 立即推送强制重推。

1. series_dir_info 未整理/已整理/无链/JSON 链四种层级
2. 识别后 tv 打上 share_scope/share_dir_id，movie 清空
3. ensure_share 用剧集目录 fileId 建分享，同目录兄弟复用一个链接
4. publish_resource 对 sent 队列项强制重推，并返回 skipped 原因
"""
import asyncio
import os
from unittest.mock import patch

os.environ.setdefault("ADMIN_PASSWORD", "testpass")
os.environ.setdefault("APP_SECRET", "x" * 40)

from fastapi.testclient import TestClient
from sqlalchemy import select

from app.db import SessionLocal, migrate
from app.main import _apply_identification, app
from app.models import Bot, Channel, QueueItem, Resource, Rule
from app.publisher import ensure_share
from app.security import encrypt_secret
from app.utils import series_dir_info

H = {"X-Admin-Password": "testpass"}


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
    db.query(Resource).filter(Resource.file_id.like("v063-%")).delete(synchronize_session=False)
    db.commit()
    db.close()


# ---------- 1. 剧集目录层级判定 ----------
def test_series_dir_unorganized_parent():
    """未整理：剧名/单集 → 父目录即剧集目录"""
    info = series_dir_info("发布组/天仙配", ["dir_show", "dir_root"])
    assert info["scope"] == "series_dir"
    assert info["dir_id"] == "dir_show"
    assert info["dir_name"] == "天仙配"


def test_series_dir_organized_season_goes_up():
    """已整理：剧名/Season 01/单集 → 上上层剧名目录"""
    info = series_dir_info("发布组/天仙配/Season 01", ["dir_s1", "dir_show", "dir_root"])
    assert info["scope"] == "series_dir"
    assert info["dir_id"] == "dir_show"
    assert info["dir_name"] == "天仙配"
    assert "季目录" in info["reason"]


def test_series_dir_no_chain_falls_back_to_file():
    """缺目录链（升级前老资源）退化为按单集分享，不崩"""
    info = series_dir_info("发布组/天仙配/Season 01", [], "parent-fid")
    assert info["scope"] == "file"
    assert info["dir_id"] == "parent-fid"


def test_series_dir_accepts_json_chain_and_cn_season():
    """DB 读回的是 JSON 字符串；第N季同样识别为季目录"""
    info = series_dir_info("剧名/第2季", '["dir_s2","dir_show"]')
    assert info["scope"] == "series_dir"
    assert info["dir_id"] == "dir_show"
    assert info["dir_name"] == "剧名"


# ---------- 2. 识别后打标 ----------
def test_apply_identification_tags_tv_and_clears_movie():
    migrate()
    _clean()
    db = SessionLocal()
    tv = Resource(file_id="v063-tag-tv", name="E01.mkv", ext=".mkv", size=1, utime=1,
                  parent_id="dir_s1", path_hint="发布组/天仙配/Season 1",
                  dir_chain_json='["dir_s1","dir_show","dir_root"]')
    mv = Resource(file_id="v063-tag-movie", name="Film.2024.mkv", ext=".mkv", size=1, utime=1,
                  parent_id="dir_movie", path_hint="发布组/电影",
                  dir_chain_json='["dir_movie","dir_root"]')
    db.add_all([tv, mv]); db.commit()
    _apply_identification(tv, {"media_type": "tv", "title": "天仙配"},
                          source="tmdb", hint={"season": 1, "episode": 1})
    _apply_identification(mv, {"media_type": "movie", "title": "某片"},
                          source="tmdb", hint={})
    db.commit()
    assert tv.share_scope == "series_dir"
    assert tv.share_dir_id == "dir_show"
    assert tv.share_dir_name == "天仙配"
    assert mv.share_scope == "file" and mv.share_dir_id == ""
    db.close()
    _clean()


# ---------- 3. 目录级建分享 + 兄弟复用 ----------
def test_ensure_share_uses_dir_id_and_reuses_sibling():
    migrate()
    _clean()
    db = SessionLocal()
    a = Resource(file_id="v063-share-a", name="E01.mkv", ext=".mkv", size=1, utime=1,
                 media_type="tv", media_title="天仙配", season=1,
                 share_scope="series_dir", share_dir_id="dir_show", share_dir_name="天仙配")
    b = Resource(file_id="v063-share-b", name="E02.mkv", ext=".mkv", size=1, utime=1,
                 media_type="tv", media_title="天仙配", season=1,
                 share_scope="series_dir", share_dir_id="dir_show", share_dir_name="天仙配")
    db.add_all([a, b]); db.commit()
    a_id, b_id = a.id, b.id
    db.close()

    calls = []

    async def fake_create(self, file_id, title, duration):
        calls.append((file_id, title))
        return {"data": {"shareId": "s-dir_show", "shareUrl": "https://share.test/dir_show"}}

    db = SessionLocal()
    with patch("app.guangya.GuangYaShareClient.create", fake_create):
        url_a = asyncio.run(ensure_share(db, db.get(Resource, a_id)))
        url_b = asyncio.run(ensure_share(db, db.get(Resource, b_id)))
    db.close()

    assert url_a == url_b == "https://share.test/dir_show"
    assert len(calls) == 1, calls          # 同目录只建一次分享
    assert calls[0][0] == "dir_show", calls  # 传的是目录 fileId，不是单集 fileId
    assert "第1季" in calls[0][1]
    _clean()


# ---------- 4. 立即推送强制重推 ----------
def test_publish_forces_resend_of_sent_item():
    migrate()
    _clean()
    db = SessionLocal()
    b = Bot(name="v063-bot", token=encrypt_secret("123:FAKE"), enabled=True)
    db.add(b); db.flush()
    c = Channel(name="v063-ch", bot_id=b.id, chat_id="-100123", enabled=True)
    db.add(c); db.flush()
    r = Resource(file_id="v063-pub", name="E01.mkv", ext=".mkv", size=1, utime=1,
                 share_url="https://share.test/x", share_status="active",
                 identify_status="identified", media_type="tv")
    db.add(r); db.flush()
    db.add(QueueItem(resource_id=r.id, channel_id=c.id, status="sent",
                     telegram_message_id=1, attempts=3))
    db.add(Rule(name="v063-rule", channel_id=c.id, auto_publish=True, enabled=True))
    db.commit()
    rid = r.id
    db.close()

    async def no_process(limit=50):
        return {"sent": 0, "failed": 0, "shares_created": 0}

    client = TestClient(app)
    with patch("app.main.process_queue", no_process):
        resp = client.post(f"/api/resources/{rid}/publish", headers=H)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["queued"] == 1, body          # 旧逻辑会返回 0，用户以为按钮没反应

    db = SessionLocal()
    q = db.scalar(select(QueueItem).where(QueueItem.resource_id == rid))
    assert q.status == "pending"              # sent 被强制重置
    assert q.attempts == 0 and q.error == ""
    db.close()
    _clean()


def test_publish_reports_skipped_disabled_channel():
    migrate()
    _clean()
    db = SessionLocal()
    b = Bot(name="v063-bot2", token=encrypt_secret("123:FAKE"), enabled=True)
    db.add(b); db.flush()
    c = Channel(name="v063-ch2", bot_id=b.id, chat_id="-100123", enabled=False)
    db.add(c); db.flush()
    r = Resource(file_id="v063-pub2", name="E01.mkv", ext=".mkv", size=1, utime=1,
                 share_url="https://share.test/y", share_status="active",
                 identify_status="identified", media_type="tv")
    db.add(r); db.flush()
    db.add(Rule(name="v063-rule2", channel_id=c.id, auto_publish=True, enabled=True))
    db.commit()
    rid = r.id
    db.close()

    client = TestClient(app)
    resp = client.post(f"/api/resources/{rid}/publish", headers=H)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["queued"] == 0
    assert any("停用" in s for s in body["skipped"]), body
    _clean()
