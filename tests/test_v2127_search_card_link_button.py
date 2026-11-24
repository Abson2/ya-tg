"""v2.1.27：Bot 搜索卡片的分享链接要跟频道/群推送一样显示成按钮。

现场（用户截图）：`/搜索` 返回的卡片，正文最后一行是裸链接
`https://www.guangyapan.com/s/1972266938678329380_...`；而同一个机器人在频道/群里
推同一条资源时，链接是 `📥 获取资源` 按钮，正文里没有裸链接。

根因：`publisher._send_telegram`（频道/群）里有一段「按 `tg_link_style` 决定按钮 +
剥掉正文末尾纯文本链接」的逻辑，而 Bot 搜索走的是 `bot_listener._send_search_cards`
→ `_send_photo`，**完全没经过这段**，于是正文带着裸链接、手上没有键盘。

本文件锁死：
  ① 两处共用 `publisher.share_link_presentation`（同一函数、同一配置口径）；
  ② `button` 档：搜索卡片带 `📥 获取资源` 按钮，且正文里的裸链接被剥掉；
  ③ `plain` 档：不造按钮、正文链接原样保留（反证：别把按钮做成无条件）；
  ④ 没有分享链接时不凭空造按钮；
  ⑤ sendPhoto 失败降级发文字时，按钮仍要带上（否则退化成裸链接）。
"""
import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from app import bot_listener as bl
from app import publisher
from app import runtime_config
from app.db import SessionLocal, migrate
from app.models import AppConfig, Bot, Channel, QueueItem, Resource

TABLES = (QueueItem, Resource, Channel, Bot)
CFG_KEYS = ("tg_link_style",)
SHARE = "https://www.guangyapan.com/s/1972266938678329380_aey8CVbCzY9joZbY"


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as db:
        for k in CFG_KEYS:
            row = db.get(AppConfig, k)
            if row:
                db.delete(row)
        for t in TABLES:
            db.query(t).delete()
        db.commit()
    yield
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        db.commit()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as s:
        yield s


def _res(fid="s1", title="鹿鼎记II：神龙教", share_url=SHARE, poster="/p.jpg"):
    return Resource(file_id=fid, name=fid + ".mkv", ext=".mkv", size=1, utime=1,
                    media_type="movie", media_title=title,
                    share_dir_id=f"dir-{fid}", share_url=share_url,
                    poster_path=poster)


def _capture(db, *, style="button", res=None):
    """跑一次 `_send_search_cards`，把发给 Telegram 的 payload 抓下来。"""
    res = res or _res()
    db.add(res)
    db.commit()
    runtime_config.set_many(db, {"tg_link_style": style})
    db.commit()
    sent: list[tuple[str, dict]] = []

    async def fake_api(_db, _token, method, payload):
        sent.append((method, payload))
        return {"ok": True, "result": {}}

    with patch.object(bl, "_api", new=fake_api):
        asyncio.run(bl._send_search_cards(db, "token", "u1", [res.id]))
    return res, sent


def test_search_card_uses_link_button_like_channel(db):
    """按钮档：搜索卡片必须带 `📥 获取资源` 按钮，正文里不留裸链接。"""
    res, sent = _capture(db)
    assert len(sent) == 1, f"应当发一条卡片，实际 {sent}"
    method, payload = sent[0]
    assert method == "sendPhoto"
    markup = payload.get("reply_markup") or {}
    rows = markup.get("inline_keyboard") or []
    assert rows and rows[0][0]["url"] == res.share_url, (
        f"搜索卡片没带分享按钮：{markup}")
    assert rows[0][0]["text"] == "📥 获取资源", "按钮文案要和频道推送一致"
    caption = payload.get("caption") or ""
    assert res.share_url not in caption, (
        "按钮档下正文不该再出现裸链接（否则按钮 + 裸链接重复，也就是用户截图的样子）")


def test_search_card_plain_style_keeps_text_link(db):
    """反证：`plain` 档不造按钮、正文链接原样保留（按钮不是无条件加的）。"""
    res, sent = _capture(db, style="plain")
    payload = sent[0][1]
    assert "reply_markup" not in payload, "plain 档不该出现按钮"
    assert res.share_url in (payload.get("caption") or "")


def test_channel_and_search_share_one_presentation_function(db):
    """同一个函数出结果：频道推送与搜索卡片对同一部作品给出一模一样的正文与键盘。"""
    res = _res()
    db.add(res)
    db.commit()
    runtime_config.set_many(db, {"tg_link_style": "button"})
    db.commit()
    caption, _ = publisher._build_message(db, res, res.share_url)
    c_channel, m_channel = publisher.share_link_presentation(db, res, caption)

    captured = {}

    async def fake_api(_db, _token, method, payload):
        captured.update(payload)
        return {"ok": True, "result": {}}

    with patch.object(bl, "_api", new=fake_api):
        asyncio.run(bl._send_search_cards(db, "token", "u1", [res.id]))
    assert captured.get("caption") == c_channel
    assert captured.get("reply_markup") == m_channel


def test_no_share_url_no_button(db):
    """没有分享链接 → 不凭空造按钮（也不该把空 URL 塞进键盘）。"""
    res = _res(share_url="")
    db.add(res)
    db.commit()
    caption, markup = publisher.share_link_presentation(db, res, "正文")
    assert markup is None
    assert caption == "正文"


def test_photo_failure_fallback_keeps_button(db):
    """sendPhoto 失败降级发文字时，按钮也要带上 —— 否则同一入口又退回裸链接。"""
    res = _res()
    db.add(res)
    db.commit()
    runtime_config.set_many(db, {"tg_link_style": "button"})
    db.commit()
    seen: list[tuple[str, dict]] = []

    async def fake_api(_db, _token, method, payload):
        seen.append((method, payload))
        if method == "sendPhoto":
            raise RuntimeError("telegram down")
        return {"ok": True, "result": {}}

    with patch.object(bl, "_api", new=fake_api):
        asyncio.run(bl._send_search_cards(db, "token", "u1", [res.id]))
    assert [m for m, _ in seen] == ["sendPhoto", "sendMessage"], seen
    fallback = seen[1][1]
    rows = (fallback.get("reply_markup") or {}).get("inline_keyboard") or []
    assert rows and rows[0][0]["url"] == res.share_url, (
        "降级成文字后按钮丢了，用户又只能看到（或被剥掉的）裸链接")
