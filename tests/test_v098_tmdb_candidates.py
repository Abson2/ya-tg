"""v0.9.8：投稿/求片 TMDB 候选多轮交互 + TMDB 按场景限流。

覆盖：
  1. 无 ID 剧名 → TMDB 候选列表（求片/投稿）
  2. 候选点选落库，记录影号/中文名/年份/类型
  3. 投稿选完候选后补链接落库
  4. TMDB 不可用降级到旧直接登记
  5. TMDB 分场景限流（bot 频次低、间隔久；scan 独立）
"""
import asyncio
import time
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from app import bot_listener, main, runtime_config, tmdb
from app.db import SessionLocal, migrate
from app.models import MediaRequest, UserContribution

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def session():
    migrate()
    # 阶段七：与同域 bot 测试（v0910/v0912/v0916）对齐全局状态清理，
    # 消除整包顺序下偶发的 _flow_sessions / 去重计数残留导致的候选字段断言失败。
    bot_listener._flow_sessions.clear()
    bot_listener._pick_sessions.clear()
    bot_listener._AUTO_CLEARED_TOKENS.clear()
    with SessionLocal() as s:
        s.query(MediaRequest).delete()
        s.query(UserContribution).delete()
        s.commit()
        try:
            yield s
        finally:
            s.query(MediaRequest).delete()
            s.query(UserContribution).delete()
            s.commit()
            bot_listener._flow_sessions.clear()
            bot_listener._pick_sessions.clear()
            bot_listener._AUTO_CLEARED_TOKENS.clear()


CANDIDATES = [
    {"tmdb_id": 261315, "title": "十万狂花入梦来", "year": "2026",
     "media_type": "tv", "vote_average": 9.0},
    {"tmdb_id": 999999, "title": "同名电影", "year": "2024",
     "media_type": "movie", "vote_average": 6.5},
]


def test_want_plain_title_returns_candidates(session):
    with patch.object(bot_listener, "search_candidates", new=AsyncMock(return_value=CANDIDATES)):
        action, reply = _run(bot_listener.handle_text(
            session, "/求片 十万狂花入梦来", tg_user_id="u1", tg_username="alice"))
    assert action == "want_pick"
    assert "十万狂花入梦来" in reply and "候选" in reply
    assert session.query(MediaRequest).count() == 0  # 未落库，等点选
    sess = bot_listener._pick_get("u1")
    assert sess and sess["flow"] == "want"


def test_want_pick_lands_tmdb_fields(session):
    bot_listener._pick_set("u2", "want", "十万狂花入梦来", CANDIDATES)
    with patch.object(bot_listener, "answerCallbackQuery", create=True):
        with patch.object(bot_listener.tgto, "upsert_keyword",
                          new=AsyncMock(return_value={"ok": True})):
            out = _run(bot_listener._handle_pick(
                session, "tok", "pk.want.0", uid="u2", chat_id="c2", message_id=1))
    assert out == "pick"
    r = session.query(MediaRequest).first()
    assert r is not None
    assert r.tmdb_id == 261315 and r.year == "2026" and r.media_type == "tv"
    assert "十万狂花入梦来" in r.title
    assert r.review_status == "pending"


def test_contribute_pick_then_link(session):
    bot_listener._pick_set("u3", "contribute", "十万狂花入梦来", CANDIDATES)
    with patch.object(bot_listener, "answerCallbackQuery", create=True):
        _run(bot_listener._handle_pick(
            session, "tok", "pk.contribute.0", uid="u3", chat_id="c3", message_id=1))
    assert session.query(UserContribution).count() == 0  # 等链接
    action, reply = _run(bot_listener.handle_text(
        session, "https://www.guangyapan.com/s/xyz", tg_user_id="u3"))
    assert action == "contribute"
    session.flush()
    c = session.query(UserContribution).first()
    assert c.tmdb_id == 261315 and c.year == "2026" and c.media_type == "tv"
    assert c.share_url == "https://www.guangyapan.com/s/xyz"
    assert "十万狂花入梦来" in c.title


def test_contribute_with_link_keeps_direct_legacy(session):
    action, reply = _run(bot_listener.handle_text(
        session, "/投稿 我的片子 https://www.guangyapan.com/s/a", tg_user_id="u4"))
    assert action == "contribute"
    session.flush()
    c = session.query(UserContribution).first()
    assert c.share_url == "https://www.guangyapan.com/s/a" and c.tmdb_id is None


def test_want_tmdb_unavailable_degrades_to_direct(session):
    with patch.object(bot_listener, "search_candidates", new=AsyncMock(return_value=[])):
        with patch.object(bot_listener.tgto, "upsert_keyword",
                          new=AsyncMock(return_value={"ok": False})):
            action, reply = _run(bot_listener.handle_text(
                session, "/求片 某剧", tg_user_id="u5"))
    assert action == "want" and "已登记" in reply
    assert session.query(MediaRequest).count() == 1


def test_pick_cancel_and_skip_clear_session(session):
    bot_listener._pick_set("u6", "want", "x", CANDIDATES)
    with patch.object(bot_listener, "answerCallbackQuery", create=True):
        _run(bot_listener._handle_pick(
            session, "tok", "pk.want.cancel", uid="u6", chat_id="c6", message_id=1))
    assert bot_listener._pick_get("u6") is None
    assert session.query(MediaRequest).count() == 0


def test_api_exposes_tmdb_fields(session):
    r = MediaRequest(title="剧", tmdb_id=123, year="2025", media_type="tv",
                     status="pending", review_status="pending")
    c = UserContribution(title="投稿", tmdb_id=456, year="2024", media_type="movie",
                         share_url="https://x", status="pending")
    session.add_all([r, c]); session.commit()
    out = main.media_requests(db=session)
    assert out[0]["year"] == "2025" and out[0]["tmdb_id"] == 123
    out2 = main.contributions(db=session)
    assert out2[0]["year"] == "2024" and out2[0]["tmdb_id"] == 456


def test_frontend_shows_tmdb_fields():
    assert "TMDB 候选" in APP
    assert "s.row.tmdb_id" in APP and "s.row.year" in APP


# ---- TMDB 按场景限流 ----
def test_rate_limit_defaults_conservative_for_bot():
    defaults = runtime_config.DEFAULTS
    assert "tmdb_bot_min_interval" in defaults and "tmdb_bot_max_calls" in defaults
    assert "tmdb_scan_min_interval" in defaults and "tmdb_scan_max_calls" in defaults
    # DEFAULTS 值为 (settings_attr, default)
    bot_interval = float(defaults["tmdb_bot_min_interval"][1])
    scan_interval = float(defaults["tmdb_scan_min_interval"][1])
    bot_calls = int(defaults["tmdb_bot_max_calls"][1])
    scan_calls = int(defaults["tmdb_scan_max_calls"][1])
    # bot 默认间隔 >= scan；bot 默认频次 <= scan
    assert bot_interval >= scan_interval
    assert bot_calls <= scan_calls


def test_scenario_limiter_bot_stricter(session):
    tmdb.reset_rate_limiters()
    runtime_config.set_many(session, {
        # 间隔设 0，单独验证频次上限；两场景频次不同
        "tmdb_scan_min_interval": "0", "tmdb_scan_max_calls": "100",
        "tmdb_bot_min_interval": "0", "tmdb_bot_max_calls": "3",
    })
    # 扫描场景：连续 10 次都应放行（间隔 0、频次 100）
    for _ in range(10):
        assert tmdb.check_rate_limit(session, "scan") is True
    # bot 场景：前 3 次放行，第 4 次被频次挡住
    for _ in range(3):
        assert tmdb.check_rate_limit(session, "bot") is True
    assert tmdb.check_rate_limit(session, "bot") is False
    tmdb.reset_rate_limiters()


def test_bot_interval_blocks_burst(session):
    """bot 场景间隔>0 时，连续突发调用被间隔挡住（防恶意刷）。"""
    tmdb.reset_rate_limiters()
    runtime_config.set_many(session, {
        "tmdb_bot_min_interval": "60", "tmdb_bot_max_calls": "100",
    })
    assert tmdb.check_rate_limit(session, "bot") is True
    assert tmdb.check_rate_limit(session, "bot") is False  # 间隔未到
    tmdb.reset_rate_limiters()


def test_tmdb_status_exposes_rate_limit(session):
    st = main.tmdb_status(db=session)
    assert "rate_limit" in st
    for k in ("scan_min_interval", "scan_max_calls", "bot_min_interval",
              "bot_max_calls", "rate_window_seconds"):
        assert k in st["rate_limit"]


def test_tmdb_config_saves_rate_limit(session):
    orig = {k: runtime_config.get(session, k) for k in
            ("tmdb_bot_min_interval", "tmdb_bot_max_calls", "tmdb_scan_max_calls")}
    try:
        main.tmdb_config_save(main.TmdbConfigInput(
            bot_min_interval="9", bot_max_calls="5", scan_max_calls="30"), db=session)
        assert runtime_config.get(session, "tmdb_bot_min_interval") == "9"
        assert runtime_config.get(session, "tmdb_bot_max_calls") == "5"
        assert runtime_config.get(session, "tmdb_scan_max_calls") == "30"
    finally:
        runtime_config.set_many(session, orig)
        session.commit()
        tmdb.reset_rate_limiters()


def test_frontend_tmdb_rate_limit_controls():
    assert "调用限流" in APP
    assert "tmdbForm.bot_min_interval" in APP and "tmdbForm.bot_max_calls" in APP
    assert "tmdbForm.scan_min_interval" in APP and "tmdbForm.scan_max_calls" in APP
    assert "rate_limit" in APP
