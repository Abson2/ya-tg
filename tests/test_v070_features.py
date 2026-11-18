"""v0.7.0 回归测试：授权脱敏、完结状态、批量管理、TG 配置、消息精简/按钮化。"""
import asyncio
from unittest.mock import AsyncMock, patch

from sqlalchemy import select

from app.db import SessionLocal, migrate
from app.utils import mask_account
from app import tmdb
from app.models import Resource, QueueItem, Bot, Channel, Rule
from app.main import (batch_library, BatchLibraryInput, library_series,
                      tg_extra_config_get, tg_extra_config_save, TgExtraConfigInput,
                      reward_config_get, reward_config_save, RewardConfigInput,
                      _apply_identification)
from app.security import encrypt_secret


def _clean():
    db = SessionLocal()
    db.query(QueueItem).delete(synchronize_session=False)
    db.query(Rule).delete(synchronize_session=False)
    db.query(Channel).delete(synchronize_session=False)
    db.query(Bot).delete(synchronize_session=False)
    db.query(Resource).filter(Resource.file_id.like("v070-%")).delete(synchronize_session=False)
    db.commit(); db.close()


def _res(fid, name, **kw):
    kw.setdefault("size", 1)
    return Resource(file_id=fid, name=name, ext=".mkv", utime=1, **kw)


# ---------- 1. 脱敏 ----------
def test_mask_account():
    assert mask_account("13812345678") == "138****5678"
    assert mask_account("a1b2c3d4e5f6g7") == "a1****g7"
    assert mask_account("abc123") == "******"
    assert mask_account("abcdefgh") == "ab****gh"
    assert mask_account("") == ""


# ---------- 2. TMDB 归一化输出 finish_status ----------
def test_tmdb_normalize_finish_status():
    ended = tmdb._normalize({"id": 1, "name": "剧A", "status": "Ended"}, "tv")
    assert ended["finish_status"] == "ended"
    returning = tmdb._normalize({"id": 2, "name": "剧B", "status": "Returning Series"}, "tv")
    assert returning["finish_status"] == "returning"
    movie = tmdb._normalize({"id": 3, "title": "电影C"}, "movie")
    assert movie["finish_status"] == ""


def test_apply_identification_stores_finish_status():
    migrate(); _clean()
    db = SessionLocal()
    r = _res("v070-finish", "Show.mkv")
    db.add(r); db.flush()
    d = {"tmdb_id": 99, "media_type": "tv", "title": "剧", "poster_path": "/p.jpg",
         "finish_status": "ended"}
    _apply_identification(r, d)
    db.commit()
    assert r.finish_status == "ended"
    db.close(); _clean()


# ---------- 3. 批量管理 ----------
def test_batch_cancel_share():
    migrate(); _clean()
    db = SessionLocal()
    b = Bot(name="v070-bot", token=encrypt_secret("1:X"), enabled=True); db.add(b); db.flush()
    c = Channel(name="v070-ch", bot_id=b.id, chat_id="-1", enabled=True); db.add(c); db.flush()
    r1 = _res("v070-s1", "E01.mkv", share_id="sid-x", share_url="https://s/x", share_status="active")
    r2 = _res("v070-s2", "E02.mkv", share_id="sid-x", share_url="https://s/x", share_status="active")
    db.add_all([r1, r2]); db.commit()
    rid, rid2 = r1.id, r2.id

    class FakeClient:
        def __init__(self, *a, **k): pass
        async def delete(self, sid): return {}

    with patch("app.main.GuangYaShareClient", FakeClient):
        result = asyncio.run(batch_library(BatchLibraryInput(ids=[rid], action="cancel_share"), db=db))
    assert result["succeeded"] == 1, result
    db = SessionLocal()
    for r in (db.get(Resource, rid), db.get(Resource, rid2)):
        assert r.share_url == "" and r.share_id == "" and r.share_status == "cancelled"
    db.close(); _clean()


def test_batch_set_duration():
    migrate(); _clean()
    db = SessionLocal()
    r = _res("v070-dur", "E01.mkv", share_id="sid-d", share_status="active")
    db.add(r); db.commit()
    rid = r.id

    class FakeClient:
        def __init__(self, *a, **k): pass
        async def update(self, sid, title, duration): assert duration == 604800

    with patch("app.main.GuangYaShareClient", FakeClient):
        result = asyncio.run(batch_library(
            BatchLibraryInput(ids=[rid], action="set_duration", duration=604800), db=db))
    assert result["succeeded"] == 1, result
    assert db.get(Resource, rid).share_duration == 604800
    db.close(); _clean()


def test_library_series_status_filters():
    migrate(); _clean()
    db = SessionLocal()
    db.add(_res("v070-f1", "A.mkv", media_type="tv", tmdb_id=1, media_title="完结剧",
                finish_status="ended", identify_status="identified", path_hint="完结剧"))
    db.add(_res("v070-f2", "B.mkv", media_type="tv", tmdb_id=2, media_title="追更剧",
                finish_status="returning", identify_status="identified", path_hint="追更剧"))
    db.commit(); db.close()
    db = SessionLocal()
    ended = library_series(finish="ended", db=db)
    returning = library_series(finish="returning", db=db)
    db.close()
    assert len(ended) == 1 and ended[0]["title"] == "完结剧"
    assert len(returning) == 1 and returning[0]["title"] == "追更剧"
    _clean()


# ---------- 4. TG 配置 ----------
def test_tg_extra_and_reward_config():
    migrate()
    db = SessionLocal()
    d = tg_extra_config_save(TgExtraConfigInput(
        push_mode="custom_bot_group", custom_bot_token="1:TOKEN",
        custom_chat_id="-999", fubao_bot="fubao_bot", link_style="button",
        detail_level="compact"), db=db)
    assert d["push_mode"] == "custom_bot_group" and d["fubao_bot"] == "fubao_bot"
    got = tg_extra_config_get(db=db)
    assert got["custom_chat_id"] == "-999"
    rw = reward_config_save(RewardConfigInput(code="https://img/reward.png"), db=db)
    assert "reward.png" in rw["code"]
    assert "reward.png" in reward_config_get(db=db)["code"]
    # 复原默认
    tg_extra_config_save(TgExtraConfigInput(), db=db)
    reward_config_save(RewardConfigInput(code=""), db=db)
    db.close()


# ---------- 5. 消息精简 ----------
def test_message_compact_overview():
    migrate(); _clean()
    db = SessionLocal()
    long = "剧情简介" * 60
    r = _res("v070-msg", "Movie.mkv", media_type="movie", media_title="电影",
             identify_status="identified", poster_path="/p.jpg", overview=long, share_url="https://s/x")
    db.add(r); db.commit()
    from app.publisher import _build_message
    text, _ = _build_message(db, r, "https://s/x")
    assert "…" in text and len(text) < 500, len(text)
    db.close(); _clean()
