"""TMDB 凭证配置与 401 友好提示的回归测试。

背景：用户填了无效/错位密钥后，资源库识别报 `TMDB 请求失败 HTTP 401:
{"status_code":7,"status_message":"Invalid API key..."}`。修复后：
- 凭证可在管理页配置（DB 加密存储，无需改 .env / 重启）
- 按值自动识别 v3 key（32 位 hex）/ v4 token（eyJ...），避免把 v4 当 v3 传
- 未配置或 401 时返回 400 + 可操作中文提示（而非 502 + 原始英文报错）
- 未配置不阻断扫描/建分享/推送（publisher 只读本地字段，不发起 TMDB 请求）
"""
import asyncio
from unittest.mock import patch

from fastapi.testclient import TestClient

from app import tmdb
from app.db import SessionLocal, migrate
from app.main import app
from app.models import AppConfig, Resource

V3_KEY = "a" * 32
V4_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.signature_part"

H = {"X-Admin-Password": "testpass"}


class FakeResponse:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code
        self.text = str(data)

    def json(self):
        return self._data


def _clear_tmdb_config():
    db = SessionLocal()
    db.query(AppConfig).filter(AppConfig.key.like("tmdb_%")).delete(synchronize_session=False)
    db.commit()
    db.close()


def test_credentials_auto_detect_v3_v4_and_swap():
    migrate()
    _clear_tmdb_config()
    # 用户把 v4 token 填进 api_key 字段（最常见的错位）→ 自动识别为 bearer
    db = SessionLocal()
    with patch.object(tmdb.runtime_config, "get", side_effect=lambda d, k, *a: {
        "tmdb_bearer_token": "", "tmdb_api_key": V4_TOKEN, "tmdb_language": "zh-CN"}.get(k, "")):
        c = tmdb.credentials(db)
    assert c["bearer"] == V4_TOKEN and c["key"] == ""
    # 用户把 v3 key 填进 bearer 字段 → 自动识别为 key
    with patch.object(tmdb.runtime_config, "get", side_effect=lambda d, k, *a: {
        "tmdb_bearer_token": V3_KEY, "tmdb_api_key": "", "tmdb_language": "zh-CN"}.get(k, "")):
        c = tmdb.credentials(db)
    assert c["key"] == V3_KEY and c["bearer"] == ""
    db.close()


def test_same_value_in_both_fields_not_misrouted():
    migrate()
    db = SessionLocal()
    with patch.object(tmdb.runtime_config, "get", side_effect=lambda d, k, *a: {
        "tmdb_bearer_token": V3_KEY, "tmdb_api_key": V3_KEY, "tmdb_language": "zh-CN"}.get(k, "")):
        c = tmdb.credentials(db)
    db.close()
    assert c["key"] == V3_KEY and c["bearer"] == ""


def test_auth_error_is_actionable_on_missing_key():
    migrate()
    _clear_tmdb_config()
    db = SessionLocal()
    with patch.object(tmdb.runtime_config, "get", return_value=""):
        try:
            asyncio.run(tmdb._get("/search/movie", {"query": "x"}, db))
            raised = False
        except tmdb.TmdbAuthError as exc:
            raised = True
            assert "尚未配置" in str(exc) and "TMDB 设置" in str(exc)
    db.close()
    assert raised is True


def test_http_401_becomes_actionable_auth_error():
    migrate()
    db = SessionLocal()

    async def fake_get(self, url, **kwargs):
        return FakeResponse({"status_code": 7, "status_message": "Invalid API key"}, 401)

    with patch.object(tmdb.runtime_config, "get", side_effect=lambda d, k, *a:
                      {"tmdb_bearer_token": "", "tmdb_api_key": V3_KEY,
                       "tmdb_language": "zh-CN"}.get(k, "")), \
         patch("httpx.AsyncClient.get", fake_get):
        try:
            asyncio.run(tmdb._get("/search/movie", {"query": "x"}, db))
            raised = False
        except tmdb.TmdbAuthError as exc:
            raised = True
            assert "密钥无效" in str(exc) and "测试连接" in str(exc)
    db.close()
    assert raised is True


def test_tmdb_config_endpoints_and_friendly_identify():
    """不进入 lifespan（避免全局 APScheduler 重复 start），migrate() 已手动建表。"""
    migrate()
    _clear_tmdb_config()
    db = SessionLocal()
    res = Resource(file_id="tmdb-test-1", name="Some.Movie.2024.1080p.mkv", ext=".mkv",
                   size=1, utime=1, identify_status="unidentified")
    db.add(res)
    db.commit()
    rid = res.id
    db.close()

    c = TestClient(app)
    # 初始未配置
    st = c.get("/api/tmdb/status", headers=H).json()
    assert st["configured"] is False and st["mode"] == "none"
    # 未配置时识别：400 + 中文引导，不是 502 + 原始 401
    with patch.object(tmdb.runtime_config, "get", return_value=""):
        r = c.post(f"/api/resources/{rid}/auto-identify", headers=H)
    assert r.status_code == 400, f"{r.status_code} {r.text}"
    assert "尚未配置 TMDB" in r.text

    # 管理页保存密钥（无需改 .env / 重启）
    r = c.post("/api/tmdb/config", headers=H,
               json={"api_key": V3_KEY, "bearer_token": "", "language": "zh-CN"})
    assert r.status_code == 200, r.text
    assert r.json()["status"]["configured"] is True
    st = c.get("/api/tmdb/status", headers=H).json()
    assert st["mode"] == "v3-key" and st["configured"] is True
    # test 端点返回结构化结果（不抛 500）
    async def fake_get(self, url, **kwargs):
        return FakeResponse({"results": []}, 200)
    with patch("httpx.AsyncClient.get", fake_get):
        t = c.post("/api/tmdb/test", headers=H)
    assert t.status_code == 200 and t.json()["ok"] is True

    db = SessionLocal()
    db.query(Resource).filter(Resource.id == rid).delete()
    db.commit()
    db.close()
    _clear_tmdb_config()
