"""v0.9.13：6 项优化回归。

锁死：
  #249 搜索卡片 sendPhoto 带 parse_mode=HTML，无海报降级 sendMessage 也带；打赏保持纯文本。
  #250 favicon / 品牌 logo 已接入 index.html 与 App.vue。
  #251 窄屏 stats-cards / pipeline-flow 降列 + grid 子项 min-width:0 防溢出。
  #252 tab 用 localStorage 持久化，历史「审核中心」值回退概览。
  #253 审核中心导航项与 section 删除。
  #254 TgtoDrive 联动：a/b/c 字段写入真机契约（含开关），删单联动只删本项目 kw_ 规则、
       护栏齐全；审核通过才写、拒绝/删除联动移除；投稿审核通过转发分享链接到配置 chat。
"""
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from app import bot_listener as bl
from app import main as main_mod
from app import runtime_config, tgto
from app.db import SessionLocal, migrate
from app.models import AppConfig, MediaRequest, UserContribution

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = ROOT / "frontend" / "src" / "App.vue"
INDEX_HTML = ROOT / "frontend" / "index.html"

CFG_KEYS = (
    "tgto_rule_resolution", "tgto_rule_effect", "tgto_rule_required_keyword",
    "tgto_contribute_bot_chat_id", "tgto_base_url", "tgto_api_key", "tgto_scene",
)


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as db:
        for k in CFG_KEYS:
            row = db.get(AppConfig, k)
            if row:
                db.delete(row)
        db.query(UserContribution).delete()
        db.query(MediaRequest).delete()
        db.commit()
    yield
    with SessionLocal() as db:
        db.query(UserContribution).delete()
        db.query(MediaRequest).delete()
        db.commit()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as s:
        yield s


# ---------- 前端静态断言（#250/#251/#252/#253） ----------
def test_favicon_and_brand_logo():
    html = INDEX_HTML.read_text(encoding="utf-8")
    assert 'rel="icon"' in html and "/logo.svg" in html
    assert "favicon.png" in html
    app = APP_VUE.read_text(encoding="utf-8")
    assert 'class="brand-logo"' in app and 'src="/logo.svg"' in app


def test_tab_persist_and_review_center_removed():
    app = APP_VUE.read_text(encoding="utf-8")
    assert "ya_tg_current_tab" in app
    assert "localStorage.setItem(TAB_STORAGE_KEY" in app
    assert "_savedTab === '审核中心'" in app  # 历史残留回退
    assert "setTab(it.key)" in app
    # 审核中心导航项与 section 已删除
    assert "key: '审核中心'" not in app
    assert "tab === '审核中心'" not in app


def test_mobile_responsive_guard():
    app = APP_VUE.read_text(encoding="utf-8")
    assert ".stats-cards>*,.pipeline-flow>*" in app
    assert ".stats-cards{grid-template-columns:repeat(2,1fr)" in app
    assert ".pipeline-flow{grid-template-columns:repeat(2,1fr)" in app
    assert "min-width:0" in app


# ---------- #249：HTML parse_mode ----------
def test_search_photo_uses_html_parse_mode(db):
    calls = []

    async def fake_api(db_, token, method, payload):
        calls.append((method, payload))

    res = type("R", (), {"id": 1, "share_url": "https://s/x", "poster_path": "/p.jpg"})()
    with patch.object(bl, "_api", fake_api), \
         patch.object(bl.tmdb, "poster_url", lambda p: "https://img/p.jpg"), \
         patch("app.publisher._build_message", return_value=("<b>片名</b>", None)):
        # 用真实 Resource 走 db.get 分支
        from app.models import Resource
        db.add(Resource(file_id="f249", name="f249.mkv", ext=".mkv", size=1, utime=1,
                        share_url="https://s/x", poster_path="/p.jpg"))
        db.commit()
        r = db.scalar(select(Resource))
        asyncio.run(bl._send_search_cards(db, "tok", "42", [r.id]))
    assert calls and calls[0][0] == "sendPhoto"
    assert calls[0][1].get("parse_mode") == "HTML"
    assert "<b>" in calls[0][1]["caption"]


def test_donate_photo_has_no_parse_mode(db):
    calls = []

    async def fake_api(db_, token, method, payload):
        calls.append((method, payload))

    with patch.object(bl, "_api", fake_api):
        asyncio.run(bl._send_photo(db, "tok", "42", "https://qr/img", "打赏文案 <x>"))
    assert calls[0][0] == "sendPhoto" and "parse_mode" not in calls[0][1]


# ---------- #254：a/b/c 字段契约 ----------
def test_build_rule_abc_fields():
    r = tgto.build_rule(title="测试剧", tmdb_id=123, media_type="tv", season=1,
                        year="2026", resolution="4k", effect="杜比视界",
                        required_keyword="265")
    assert r["resolution"] == "4k"
    assert r["effect"] == "dolby"
    assert r["required_keyword"] == "265" and r["include_required_keyword"] is True
    assert r["year"] == "2026" and r["include_year"] is True
    assert r["tmdb_id"] == "123" and r["rule_type"] == "structured"
    assert r["id"] == "kw_123_1"
    assert "must_include" not in r and "picture_effect" not in r
    # 必含留空 → 显式空串 + 关闭开关
    r2 = tgto.build_rule(title="x", resolution="1080p", effect="hdr", required_keyword="")
    assert r2["resolution"] == "1080p" and r2["effect"] == "hdr"
    assert r2["required_keyword"] == "" and r2["include_required_keyword"] is False


def test_upsert_reads_abc_from_config(db):
    runtime_config.set_many(db, {
        "tgto_rule_resolution": "720p",
        "tgto_rule_effect": "sdr",
        "tgto_rule_required_keyword": "HEVC",
    })
    store = {"current": {"key": "ENV_FILTER_GUANGYA", "rules": []},
             "scenes": {"ENV_FILTER_GUANGYA": {"key": "ENV_FILTER_GUANGYA", "rules": []}}}
    captured = {}

    async def fake_fetch(db_):
        return store, "ENV_FILTER_GUANGYA"

    async def fake_save(db_, data, scene, obj):
        captured["rules"] = list(obj["rules"])

    with patch.object(tgto, "fetch_config", fake_fetch), patch.object(tgto, "save_config", fake_save):
        out = asyncio.run(tgto.upsert_keyword(db, title="配置驱动", tmdb_id=999, year="2025"))
    assert out["ok"]
    rule = captured["rules"][0]
    assert rule["resolution"] == "720p" and rule["effect"] == "sdr"
    assert rule["required_keyword"] == "HEVC" and rule["include_required_keyword"] is True


# ---------- #254：删单联动护栏 ----------
def test_remove_keyword_only_own_rule_and_guard(db):
    mine = {"id": "kw_777_1", "source": "ya-tg", "title": "我的"}
    other = {"id": "tmdb_movie_1", "source": "admin", "title": "用户自建"}
    foreign_kw = {"id": "kw_888_1", "source": "other", "title": "别的工单"}
    store = {"current": {"key": "ENV_FILTER_GUANGYA", "rules": [mine, other, foreign_kw]},
             "scenes": {"ENV_FILTER_GUANGYA": {"key": "ENV_FILTER_GUANGYA",
                                               "rules": [mine, other, foreign_kw]}}}
    writes = []

    async def fake_fetch(db_):
        return store, "ENV_FILTER_GUANGYA"

    async def fake_save(db_, data, scene, obj):
        writes.append([r["id"] for r in obj["rules"]])

    with patch.object(tgto, "fetch_config", fake_fetch), patch.object(tgto, "save_config", fake_save):
        out = asyncio.run(tgto.remove_keyword(db, tmdb_id=777, season=1))
    assert out["ok"] and out["changed"] and out["removed"] == ["kw_777_1"]
    assert writes and writes[0] == ["tmdb_movie_1", "kw_888_1"]  # 只删本项目规则

    # 未命中 → 不 POST
    writes.clear()
    with patch.object(tgto, "fetch_config", fake_fetch), patch.object(tgto, "save_config", fake_save):
        out2 = asyncio.run(tgto.remove_keyword(db, tmdb_id=666, season=1))
    assert out2["changed"] is False and writes == []


def test_remove_keyword_aborts_on_unknown_structure(db):
    wrote = []

    async def fake_fetch(db_):
        return {"success": True, "weird": {}}, "ENV_FILTER_GUANGYA"

    async def fake_save(db_, data, scene, obj):
        wrote.append(1)

    with patch.object(tgto, "fetch_config", fake_fetch), patch.object(tgto, "save_config", fake_save):
        out = asyncio.run(tgto.remove_keyword(db, tmdb_id=777, season=1))
    assert out["ok"] is False and wrote == []


# ---------- #254：审核钩子 ----------
def test_media_request_writes_whitelist_on_approve(db):
    req = MediaRequest(title="审核剧", tmdb_id=555, media_type="movie", year="2024",
                       keyword_id=tgto.keyword_id(555, None, "审核剧"),
                       status="pending", review_status="pending")
    db.add(req)
    db.commit()
    written = []

    async def fake_upsert(db_, **kw):
        written.append(kw)
        return {"ok": True, "status": "created"}

    body = main_mod.MediaRequestHandleInput(review_status="adopted", note="")
    with patch.object(tgto, "upsert_keyword", fake_upsert), \
         patch.object(bl, "notify_user", AsyncMock(return_value=False)):
        out = asyncio.run(main_mod.handle_media_request(req.id, body, db))
    assert out["ok"] and written and written[0]["tmdb_id"] == 555
    assert written[0]["year"] == "2024"
    assert db.get(MediaRequest, req.id).status == "synced"


def test_delete_media_request_removes_whitelist(db):
    req = MediaRequest(title="删除剧", tmdb_id=888, media_type="tv", season=2,
                       keyword_id=tgto.keyword_id(888, 2, "删除剧"),
                       status="synced", review_status="adopted")
    db.add(req)
    db.commit()
    removed = []

    async def fake_remove(db_, **kw):
        removed.append(kw)
        return {"ok": True, "changed": True}

    with patch.object(tgto, "remove_keyword", fake_remove):
        out = asyncio.run(main_mod.delete_media_request(req.id, db))
    assert out["ok"] and out["keyword_removed"] is True
    assert removed and removed[0]["tmdb_id"] == 888 and removed[0]["season"] == 2
    assert db.get(MediaRequest, req.id) is None


def test_contribution_approve_forwards_share_url(db):
    """v0.9.18：审核通过触发分发（分发逻辑已迁到 contribution_dispatch）。"""
    c = UserContribution(title="投稿剧", share_url="https://www.guangyapan.com/s/x",
                         status="pending", tg_user_id="42")
    db.add(c)
    db.commit()
    calls = []

    async def fake_run(db_, cid, **kw):
        calls.append(cid)
        return True, "已提交", None

    body = main_mod.ContributionHandleInput(status="approved", note="")
    with patch.object(main_mod.contribution_dispatch, "run_dispatch_for_contribution", new=fake_run), \
         patch.object(bl, "notify_user", AsyncMock(return_value=False)):
        out = asyncio.run(main_mod.handle_contribution(c.id, body, db))
    assert out["ok"] and calls and calls[0] == c.id
    assert db.get(UserContribution, c.id).status == "approved"


def test_forward_contribution_requires_chat_id(db):
    ok, msg = asyncio.run(bl.forward_contribution_to_tgto(db, "https://s/x", "标题"))
    assert ok is False and "chat_id" in msg
