"""v0.9.15：大批量任务稳定性三处根治回归。

  #267 WebFiles 不再快照 token：每次请求从独立短会话读最新凭证；遇 code=117
       强制刷新后重建 header 重试一次；长任务 session 的 identity map 缓存旧对象也不受影响。
  #268 TgtoDrive 规则写入前用 tmdb.detail() 补海报/背景/简介/原名，TgtoDrive 前端
       才能从「正在匹配 TMDB」变为「已匹配」；TMDB 失败安全降级，不阻断审核。
  #269 推理模型 content 为空时回退 reasoning_content 抽 JSON；三处 token 预算提到 600。
"""
import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from app import ai_assist, guangya, runtime_config, tgto
from app.db import SessionLocal, migrate
from app.models import AppConfig, AuthToken
from app.security import encrypt_secret


@pytest.fixture
def db():
    with SessionLocal() as s:
        yield s


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as db:
        db.query(AuthToken).delete()
        db.query(AppConfig).delete()
        db.commit()


def _seed_token(token="tk-old", flow="sms"):
    with SessionLocal() as db:
        db.query(AuthToken).delete()
        db.add(AuthToken(id=1, client_id="c", access_token_enc=encrypt_secret(token),
                         refresh_token_enc=encrypt_secret("rt"), flow=flow,
                         device_id="d" * 32, sub="+86 13800138000"))
        db.commit()


class FakeResponse:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code
        self.text = str(data)

    def json(self):
        return self._data


# ---------- #267：token 快照竞态 ----------
def test_headers_reread_latest_token_each_request():
    """对象构造后 DB token 被后台刷新，下一次请求必须带新 token，而非构造时快照。"""
    _seed_token("tk-old")
    db = SessionLocal()
    captured = []

    async def fake_post(self, url, **kw):
        captured.append(kw.get("headers", {}).get("Authorization"))
        return FakeResponse({"code": 0, "data": {"list": [], "total": 0}})

    with patch("httpx.AsyncClient.post", fake_post):
        client = guangya.GuangYaWebFiles(db)
        asyncio.run(client.list_files("*", 0, 10))  # 第一次：tk-old
        _seed_token("tk-new")  # 模拟后台 _job_token_refresh 替换 DB 里的 token
        asyncio.run(client.list_files("*", 0, 10))  # 第二次：应读到 tk-new
    db.close()
    assert captured[0] == "Bearer tk-old"
    assert captured[1] == "Bearer tk-new", "构造后刷新的 token 必须在下一次请求生效"


def test_stale_session_identity_map_does_not_block_fresh_token():
    """长任务 session 缓存旧 AuthToken 对象，也不影响 _fresh_headers 读最新值。"""
    _seed_token("tk-old")
    stale = SessionLocal()
    stale_token = stale.get(AuthToken, 1)  # 在 identity map 中固定旧对象
    assert stale_token.access_token_enc

    _seed_token("tk-new")
    client = guangya.GuangYaWebFiles(stale)
    headers = client._fresh_headers()  # 独立短会话读，应绕过 identity map
    stale.close()
    from app.security import decrypt_secret
    assert decrypt_secret(headers["Authorization"].replace("Bearer ", "")) == "tk-new"


def test_117_forces_refresh_and_retries_once():
    """写接口遇 117 → 强制刷新凭证 → 重建 header 重试成功，且不无限重试。"""
    _seed_token("tk-old")
    db = SessionLocal()
    seen_tokens = []
    refresh_calls = []

    async def fake_post(self, url, **kw):
        auth = kw.get("headers", {}).get("Authorization")
        seen_tokens.append(auth)
        # 第一次用旧 token → 117；刷新后用新 token → 成功
        if auth == "Bearer tk-old":
            return FakeResponse({"code": 117, "msg": "无效token"})
        return FakeResponse({"code": 0, "data": {"taskId": "task-1"}})

    async def fake_refresh(self):
        refresh_calls.append(1)
        _seed_token("tk-new")

    with patch("httpx.AsyncClient.post", fake_post), \
         patch.object(guangya.GuangYaWebFiles, "_refresh_token", fake_refresh):
        client = guangya.GuangYaWebFiles(db)
        task_id = asyncio.run(client.copy_file(["f1"], "pid"))
    db.close()
    assert task_id == "task-1"
    assert len(refresh_calls) == 1
    assert seen_tokens[0] == "Bearer tk-old" and seen_tokens[-1] == "Bearer tk-new"


def test_117_refresh_only_once_then_raise():
    """刷新后仍 117（token 依旧无效）→ 只刷新一次即抛错，不循环。"""
    _seed_token("tk-old")
    db = SessionLocal()
    refresh_calls = []

    async def fake_post(self, url, **kw):
        return FakeResponse({"code": 117, "msg": "无效token"})

    async def fake_refresh(self):
        refresh_calls.append(1)

    from app.guangya import WebFileError
    with patch("httpx.AsyncClient.post", fake_post), \
         patch.object(guangya.GuangYaWebFiles, "_refresh_token", fake_refresh):
        client = guangya.GuangYaWebFiles(db)
        raised = False
        try:
            asyncio.run(client.create_dir("pid", "name"))
        except WebFileError as exc:
            raised = exc.code == 117
    db.close()
    assert raised and len(refresh_calls) == 1


# ---------- #268：TgtoDrive 规则补 TMDB 元数据 ----------
def test_build_rule_metadata_fields():
    r = tgto.build_rule(title="凡人修仙传", tmdb_id=106449, media_type="tv", season=1,
                        original_title="A Record of Mortal's Journey", overview="简介",
                        poster_path="/p.jpg", poster_url="https://img/p.jpg",
                        backdrop_path="/b.jpg", backdrop_url="https://img/b.jpg")
    assert r["poster_path"] == "/p.jpg" and r["poster_url"] == "https://img/p.jpg"
    assert r["backdrop_path"] == "/b.jpg" and r["backdrop_url"] == "https://img/b.jpg"
    assert r["original_title"].startswith("A Record") and r["overview"] == "简介"
    # 旧调用不传元数据仍全空串，兼容
    r2 = tgto.build_rule(title="x")
    assert r2["poster_path"] == "" and r2["overview"] == ""


def test_upsert_fills_tmdb_metadata(db):
    store = {"current": {"key": "ENV_FILTER_GUANGYA", "rules": []},
             "scenes": {"ENV_FILTER_GUANGYA": {"key": "ENV_FILTER_GUANGYA", "rules": []}}}
    captured = {}

    async def fake_fetch(db_):
        return store, "ENV_FILTER_GUANGYA"

    async def fake_save(db_, data, scene, obj):
        captured["rules"] = list(obj["rules"])

    async def fake_detail(db_, *a, **kw):
        return {"original_title": "The Silent Storm", "overview": "剧情梗概",
                "poster_path": "/poster.jpg", "backdrop_path": "/backdrop.jpg"}

    with patch.object(tgto, "fetch_config", fake_fetch), \
         patch.object(tgto, "save_config", fake_save), \
         patch("app.tmdb.configured", return_value=True), \
         patch("app.tmdb.detail", new=fake_detail):
        out = asyncio.run(tgto.upsert_keyword(db, title="谷雨", tmdb_id=123, media_type="tv", season=1))
    assert out["ok"]
    rule = captured["rules"][0]
    assert rule["poster_path"] == "/poster.jpg"
    assert rule["poster_url"].endswith("/w500/poster.jpg")
    assert rule["backdrop_url"].endswith("/w780/backdrop.jpg")
    assert rule["overview"] == "剧情梗概" and rule["original_title"] == "The Silent Storm"


def test_upsert_metadata_failure_degrades_without_blocking(db):
    """TMDB 查询抛异常时仍写入关键词（元数据空），不阻断求片审核。"""
    store = {"current": {"key": "ENV_FILTER_GUANGYA", "rules": []},
             "scenes": {"ENV_FILTER_GUANGYA": {"key": "ENV_FILTER_GUANGYA", "rules": []}}}
    captured = {}

    async def fake_fetch(db_):
        return store, "ENV_FILTER_GUANGYA"

    async def fake_save(db_, data, scene, obj):
        captured["rules"] = list(obj["rules"])

    async def boom(*a, **kw):
        raise RuntimeError("TMDB down")

    with patch.object(tgto, "fetch_config", fake_fetch), \
         patch.object(tgto, "save_config", fake_save), \
         patch("app.tmdb.configured", return_value=True), \
         patch("app.tmdb.detail", new=boom):
        out = asyncio.run(tgto.upsert_keyword(db, title="谷雨", tmdb_id=123))
    assert out["ok"] and captured["rules"]
    assert captured["rules"][0]["poster_path"] == ""


# ---------- #269：推理模型 content 兜底与 token 预算 ----------
def test_chat_falls_back_to_reasoning_content():
    captured = {}

    async def fake_post(self, url, **kw):
        captured["max_tokens"] = kw["json"]["max_tokens"]
        return FakeResponse({"choices": [{"message": {
            "content": "", "reasoning_content": '{"en":"The Silent Storm"}'}}]})

    with patch("httpx.AsyncClient.post", fake_post):
        with patch.object(ai_assist, "_cfg", return_value={
                "enabled": True, "base_url": "https://x/v1", "api_key": "k",
                "model": "deepseek-v4.1-flash", "custom_prompt": "", "timeout": 30}):
            content = asyncio.run(ai_assist._chat([{"role": "user", "content": "x"}], None))
    assert content == '{"en":"The Silent Storm"}'
    assert ai_assist._extract_json(content)["en"] == "The Silent Storm"


def test_chat_default_and_call_site_token_budgets():
    import inspect
    assert inspect.signature(ai_assist._chat).parameters["max_tokens"].default == 600
    src = inspect.getsource(ai_assist.translate_title)
    assert "max_tokens=600" in src
    dis = inspect.getsource(ai_assist.disambiguate)
    assert "max_tokens=600" in dis


def test_translate_title_uses_reasoning_content(db):
    """端到端：翻译请求 content 空、reasoning_content 有 JSON 时返回可用查询词。"""
    runtime_config.set_many(db, {
        "ai_enabled": "true", "ai_api_key": "k", "ai_model": "deepseek-v4.1-flash",
    })

    async def fake_post(self, url, **kw):
        return FakeResponse({"choices": [{"message": {
            "content": "", "reasoning_content": '{"en":"The Silent Storm","ja":"","ko":"","original":"谷雨"}'}}]})

    with patch("httpx.AsyncClient.post", fake_post):
        out = asyncio.run(ai_assist.translate_title("谷雨", "2024", db))
    assert out["ok"] and "The Silent Storm" in out["queries"]
