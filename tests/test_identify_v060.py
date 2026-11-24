"""v0.6.0 识别决策与 TMDB 分类打标回归测试。

覆盖用户反馈的三类问题：
1. 剧集单集（S01E04）被识别成电影 → 类型硬约束，绝不回退 movie
2. TMDB 标签分类打标 → type / origin_country / genres / tags 变量
3. 识别失败人工处理 → 手动匹配 / 无匹配端点；AI 配置与降级
"""
import asyncio
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import select

from app import publisher, tmdb
from app.db import SessionLocal, json_dumps, migrate
from app.main import app
from app.models import AppConfig, MessageTemplate, Resource
from app.utils import parse_media_name, render_template

H = {"X-Admin-Password": "testpass"}


def _item(title, mt, tmdb_id, year="", genres=None, countries=None):
    return {
        "tmdb_id": tmdb_id, "media_type": mt, "type": mt,
        "type_label": tmdb.type_label(mt), "title": title, "original_title": title,
        "year": year, "poster_path": "", "backdrop_path": "", "overview": "简介",
        "vote_average": 7.2, "popularity": 90,
        "genres": genres or ["剧情"], "origin_country": countries or ["US"], "score": 0.0,
    }


class FakeResponse:
    def __init__(self, data, status_code=200):
        self._data, self.status_code, self.text = data, status_code, str(data)

    def json(self):
        return self._data


def test_parse_media_name_type_hints():
    h = parse_media_name("The.Silent.Storm.S01E04.2026.2160p.WEB-DL.DDP5.1.mkv")
    assert h["type_hint"] == "tv" and h["type_source"] == "episode"
    assert h["season"] == 1 and h["episode"] == 4
    assert h["clean_title"] == "The Silent Storm" and h["year"] == "2026"

    h2 = parse_media_name("诛仙.E04.mkv", "/影视/电视剧/动漫剧/诛仙 (2022) (tmdb-206484)")
    assert h2["tmdb_id"] == 206484 and h2["type_hint"] == "tv"

    h3 = parse_media_name("Inception.2010.1080p.BluRay.mkv")
    assert h3["type_hint"] == "unknown" and h3["clean_title"] == "Inception"

    h4 = parse_media_name("保镖恋人.2026.2160p.WEB-DL.mkv", "/影视/电影")
    assert h4["type_hint"] == "movie" and h4["type_source"] == "directory"


def test_tv_hint_never_falls_back_to_movie():
    """用户截图：The.Silent.Storm.S01E04 被误配成电影 Cavalcade d'amour (TMDB #279155)。"""
    async def fake_search(title, media_type=None, db=None):
        if media_type == "tv":
            return [_item("The Silent Storm", "tv", 1001, "2026")]
        if media_type == "movie":
            return [_item("Cavalcade d'amour", "movie", 279155, "1930")]
        return []

    with patch.object(tmdb, "search", fake_search):
        r = asyncio.run(tmdb.identify("The.Silent.Storm.S01E04.2026.2160p.WEB-DL.mkv",
                                      None, use_ai=False))
    assert r["ok"] and r["status"] == "identified"
    assert r["best"]["media_type"] == "tv" and r["best"]["tmdb_id"] == 1001
    assert all(c["media_type"] == "tv" for c in r["candidates"])

    async def empty(title, media_type=None, db=None):
        return []

    with patch.object(tmdb, "search", empty):
        r2 = asyncio.run(tmdb.identify("Unknown.Show.S02E03.2024.mkv", None, use_ai=False))
    assert r2["status"] == "failed" and not r2["candidates"]


def test_tmdb_id_tag_exact_match():
    async def fake_detail(tmdb_id, media_type, db=None):
        return _item("诛仙", media_type, tmdb_id, "2022", ["动画", "奇幻"], ["CN"])

    with patch.object(tmdb, "detail", fake_detail):
        r = asyncio.run(tmdb.identify("诛仙.E04.mkv (tmdb-206484)", None, use_ai=False))
    assert r["source"] == "id" and r["best"]["tmdb_id"] == 206484
    assert r["best"]["media_type"] == "tv"


def test_unknown_type_ambiguous_goes_manual():
    async def fake_both(title, db=None):
        return [_item("Inception", "movie", 27205, "2010"),
                _item("Inception", "tv", 99999, "2010")]

    with patch.object(tmdb, "search_both", fake_both):
        r = asyncio.run(tmdb.identify("Inception.2010.1080p.mkv", None, use_ai=False))
    assert r["status"] == "ambiguous" and r["best"] is None
    assert len(r["candidates"]) == 2


def test_render_template_supports_nested_if():
    ctx = {"year": "2026", "tags": "#电影", "title": "X"}
    out = render_template("<b>{{title}}</b>{{#if year}}({{year}}){{#if tags}} {{tags}}{{/if}}{{/if}}", ctx)
    assert out == "<b>X</b>(2026) #电影"
    assert render_template("{{#if year}}{{#if tags}}T{{/if}}{{/if}}",
                           {"year": "", "tags": "T"}) == ""


def test_default_template_emits_tmdb_classification_tags():
    migrate()
    db = SessionLocal()
    res = Resource(file_id="v060-tpl", name="保镖恋人.2026.2160p.WEB-DL.mkv", ext=".mkv",
                   size=15 * 1024 ** 3, utime=1, media_title="保镖恋人", media_type="movie",
                   tmdb_id=1440098, genres=json_dumps(["爱情", "惊悚"]),
                   origin_country=json_dumps(["ES"]), vote_average=7.0,
                   identify_status="identified")
    db.add(res)
    db.commit()
    text, _ = publisher._build_message(db, res, "https://share.example/xxx")
    # v0.6.4：新默认模板改为整剧展示，用标签串替代 type=/origin_country=/genres= 调试行
    assert "#保镖恋人" in text and "#电影" in text and "#爱情" in text
    assert "资源：" not in text and ".mkv" not in text  # 不再暴露单集文件名
    # v2.1.22：默认图文改为 🏷 标签行（标题不再带 🖼️ 后缀）
    assert "🏷 #保镖恋人" in text
    tpl = db.scalar(select(MessageTemplate).where(MessageTemplate.is_default.is_(True)))
    # v2.1.22：入口链接（频道/投稿·求片）由渲染层统一追加，模板里不再有 {{buttons_text}}
    assert tpl and "{{tags}}" in tpl.content and "{{share_url}}" in tpl.content
    db.delete(res)
    db.commit()
    db.close()


def test_manual_match_no_match_and_ai_config():
    migrate()
    db = SessionLocal()
    db.query(AppConfig).filter(AppConfig.key.like("ai_%")).delete(synchronize_session=False)
    res = Resource(file_id="v060-manual", name="诛仙.S01E04.2022.mkv", ext=".mkv", size=1, utime=1)
    db.add(res)
    db.commit()
    rid = res.id
    db.close()

    c = TestClient(app)
    r = c.post("/api/ai/config", headers=H, json={
        "enabled": True, "base_url": "https://api.openai.com/v1",
        "api_key": "sk-test", "model": "gpt-4o-mini",
        "prompt": "片名用中文", "timeout": 30})
    assert r.status_code == 200 and r.json()["status"]["active"] is True
    assert r.json()["status"]["custom_prompt"] == "片名用中文"

    async def fake_ai_post(self, url, **kwargs):
        return FakeResponse({"choices": [{"message": {"content":
                            '{"title":"诛仙","type":"tv","year":"2022","confidence":0.9}'}}]})

    with patch("httpx.AsyncClient.post", fake_ai_post):
        p = c.post("/api/ai/parse", headers=H, json={"name": "诛仙.S01E04.2022.mkv"})
    assert p.status_code == 200 and p.json()["ok"] is True and p.json()["type"] == "tv"

    async def fake_detail(tmdb_id, media_type, db=None):
        return _item("诛仙", media_type, tmdb_id, "2022", ["动画", "奇幻"], ["CN"])

    with patch.object(tmdb, "detail", fake_detail):
        m = c.post(f"/api/resources/{rid}/identify", headers=H,
                   json={"tmdb_id": 206484, "media_type": "tv", "note": "手动匹配"})
    assert m.status_code == 200 and m.json()["type"] == "tv"
    db = SessionLocal()
    x = db.get(Resource, rid)
    assert x.identify_status == "identified" and x.identify_source == "manual"
    assert tmdb.as_list(x.genres) == ["动画", "奇幻"]
    assert x.season == 1 and x.episode == 4
    db.close()

    n = c.post(f"/api/resources/{rid}/match-none", headers=H)
    assert n.status_code == 200
    db = SessionLocal()
    x = db.get(Resource, rid)
    assert x.identify_status == "none" and x.tmdb_id is None
    db.delete(x)
    db.query(AppConfig).filter(AppConfig.key.like("ai_%")).delete(synchronize_session=False)
    db.commit()
    db.close()
