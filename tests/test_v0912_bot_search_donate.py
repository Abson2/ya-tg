"""v0.9.12：搜索会话化 + 转发已推送卡片 + 打赏发图片 + 合并打赏窗 + README 支持章节。

锁死：
  - 点「搜索」/ 发 /搜索 进会话态，直接输文字即搜索，无需 / 前缀；结束词退出。
  - 搜索只返回「已推送(sent)且仍有 share_url」的资源，按作品去重，action 携带资源 id。
  - 打赏开启且配图时返回 donate_photo:<url>，不再把 URL 塞进用户文本。
  - poll_once 对 search_cards / donate_photo 分别发卡片 / 发图片。
  - 打赏 UI 只有单一图片地址入口；README 含「支持」与赞赏码图。
"""
import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from app import bot_listener as bl
from app import runtime_config
from app.db import SessionLocal, migrate
from app.models import AppConfig, Bot, Channel, QueueItem, Resource

TABLES = (QueueItem, Resource, Channel, Bot)
CFG_KEYS = ("donation_enabled", "donation_image_url", "donation_text",
            "contribution_enabled", "contribution_bot_token")


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    bl._flow_sessions.clear()
    bl._pick_sessions.clear()
    with SessionLocal() as db:
        for k in CFG_KEYS:
            row = db.get(AppConfig, k)
            if row:
                db.delete(row)
        for t in TABLES:
            db.query(t).delete()
        db.commit()
    yield
    bl._flow_sessions.clear()
    bl._pick_sessions.clear()
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        db.commit()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as s:
        yield s


def _res(fid, title, share_url="https://share/x", poster="/p.jpg"):
    return Resource(file_id=fid, name=fid + ".mkv", ext=".mkv", size=1, utime=1,
                    media_type="tv", media_title=title, share_dir_id=f"dir-{fid}",
                    share_url=share_url, poster_path=poster)


def test_search_bare_command_enters_flow(db):
    action, reply = asyncio.run(bl.handle_text(db, "/搜索", tg_user_id="u1"))
    assert action == "flow_search"
    assert bl._flow_get("u1") == bl.FLOW_SEARCH
    assert "直接输入" in reply


def test_search_flow_text_without_slash(db):
    bl._flow_set("u1", bl.FLOW_SEARCH)
    action, reply = asyncio.run(bl.handle_text(db, "狂花", tg_user_id="u1"))
    assert action.startswith("search_cards")
    # 无已推送资源时提示找不到，action 无 id
    assert action == "search_cards:"
    assert "没有找到" in reply


def test_end_word_clears_search_flow(db):
    bl._flow_set("u1", bl.FLOW_SEARCH)
    action, reply = asyncio.run(bl.handle_text(db, "结束", tg_user_id="u1"))
    assert action == "flow_end"
    assert bl._flow_get("u1") == ""


def test_search_returns_only_sent_and_dedup(db):
    from app.security import encrypt_secret
    b = Bot(name="b", token=encrypt_secret("1:FAKE"), enabled=True)
    db.add(b); db.flush()
    c = Channel(name="c", bot_id=b.id, chat_id="-100", enabled=True)
    db.add(c); db.flush()
    # 同一作品两集 + 一部没推过的
    r1 = _res("s1", "十万狂花入梦来")
    r1.share_dir_id = "dir-same"
    r2 = _res("s2", "十万狂花入梦来")
    r2.share_dir_id = "dir-same"
    r3 = _res("s3", "没推过的剧", share_url="")
    db.add_all([r1, r2, r3]); db.flush()
    db.add(QueueItem(resource_id=r1.id, channel_id=c.id, status="sent"))
    db.add(QueueItem(resource_id=r2.id, channel_id=c.id, status="sent"))
    db.commit()
    text, ids = asyncio.run(bl._do_search(db, "狂花"))
    assert len(ids) == 1, (text, ids)  # 同一作品去重为一个
    assert ids[0] in (r1.id, r2.id)
    assert "正在转发" in text


def test_search_skips_resource_without_share_url(db):
    from app.security import encrypt_secret
    b = Bot(name="b2", token=encrypt_secret("1:FAKE"), enabled=True)
    db.add(b); db.flush()
    c = Channel(name="c2", bot_id=b.id, chat_id="-101", enabled=True)
    db.add(c); db.flush()
    r = _res("s4", "有推送但无分享链接", share_url="")
    db.add(r); db.flush()
    db.add(QueueItem(resource_id=r.id, channel_id=c.id, status="sent"))
    db.commit()
    text, ids = asyncio.run(bl._do_search(db, "有推送"))
    assert ids == [] and "没有找到" in text


def test_donate_returns_photo_action(db):
    runtime_config.set_many(db, {"donation_enabled": "true",
                                 "donation_image_url": "https://img/reward.png",
                                 "donation_text": "感谢支持"})
    action, text = asyncio.run(bl.handle_text(db, "/打赏", tg_user_id="u2"))
    assert action == "donate_photo:https://img/reward.png"
    assert text == "感谢支持"
    assert "https://img/reward.png" not in text  # 不再把网址塞给用户


def test_donate_text_has_no_bare_url(db):
    runtime_config.set_many(db, {"donation_enabled": "true",
                                 "donation_image_url": "https://img/reward.png"})
    text = bl._donate_text(db)
    assert "https://img/reward.png" not in text


def test_send_search_cards_uses_photo(db):
    r = _res("sc1", "测试剧")
    db.add(r); db.commit()
    with patch.object(bl, "_send_photo", new=AsyncMock()) as sp:
        asyncio.run(bl._send_search_cards(db, "token", "u3", [r.id]))
    assert sp.await_count == 1
    assert sp.await_args.args[2] == "u3"


def test_frontend_and_readme():
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    app = (root / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "赞赏码图片地址（直接发给用户）" in app
    assert "单独保存打赏" not in app
    readme = (root / "README.md").read_text(encoding="utf-8")
    assert "## 支持" in readme
    assert "花了80大洋购买token" in readme
    assert "assets/reward-qr.jpg" in readme
    assert (root / "assets/reward-qr.jpg").exists()
