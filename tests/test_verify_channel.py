"""v0.6.0：验证连接按授权通道分流，短信登录不再误调官方 get_user_info。

背景：用户点顶栏「验证连接」报 `/openapi/v1/user/get_user_info` 失败
code=120 msg=缺少clientId。v0.5.3 把扫描切到网页端时漏了这个端点：
短信登录凭证没有开放平台应用 ID，硬调官方业务接口必然 120。
"""
import asyncio
from datetime import timedelta
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app import guangya, runtime_config
from app.db import SessionLocal, migrate
from app.main import app
from app.models import AuthToken
from app.security import encrypt_secret
from app.utils import utcnow

H = {"X-Admin-Password": "testpass"}


class FakeResponse:
    def __init__(self, data, status_code=200):
        self._data, self.status_code, self.text = data, status_code, str(data)

    def json(self):
        return self._data


def _set_token(flow: str, client_id: str = "aMe-8VSlkrbQXpUR"):
    db = SessionLocal()
    db.query(AuthToken).delete()
    db.add(AuthToken(id=1, client_id=client_id,
                     access_token_enc=encrypt_secret("sms-token"),
                     refresh_token_enc=encrypt_secret("refresh"),
                     device_id="did-for-test", flow=flow,
                     expires_at=utcnow() + timedelta(hours=2),
                     granted_at=utcnow()))
    db.commit()
    db.close()


def test_verify_sms_uses_web_file_api_not_openapi():
    migrate()
    _set_token("sms")
    captured = {}

    async def fake_post(self, url, **kwargs):
        captured["url"] = url
        captured["headers"] = kwargs.get("headers") or {}
        return FakeResponse({"code": 0, "msg": "success", "data": {"list": [], "total": 0}})

    async def forbidden(self):
        raise AssertionError("短信登录不应调用官方 OpenAPI get_user_info")

    c = TestClient(app)
    with patch.object(guangya.GuangYaOpenAPI, "get_user_info", forbidden), \
         patch("httpx.AsyncClient.post", fake_post):
        r = c.get("/api/auth/openapi/verify", headers=H)
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["ok"] is True and d["channel"] == "web"
    assert "/userres/v1/file/get_file_list" in captured["url"]
    assert "x-client-id" not in captured["headers"]
    assert captured["headers"]["did"] == "did-for-test"
    assert "dt" in captured["headers"]


def test_verify_official_flow_still_uses_get_user_info():
    migrate()
    _set_token("device", client_id="official-app-id")
    captured = {}

    async def fake_request(self, method, url, **kwargs):
        captured["url"] = url
        captured["headers"] = kwargs.get("headers") or {}
        return FakeResponse({"code": 0, "data": {"userId": "u-1", "vipStatus": 1}})

    c = TestClient(app)
    with patch("httpx.AsyncClient.request", fake_request):
        r = c.get("/api/auth/openapi/verify", headers=H)
    assert r.status_code == 200, r.text
    assert r.json()["channel"] == "openapi" and r.json()["user_id"] == "u-1"
    assert "/openapi/v1/user/get_user_info" in captured["url"]


def test_openapi_call_guard_for_sms_credentials():
    """前置防护：任何残留的官方接口调用都给出中文说明，而不是裸 code=120。"""
    migrate()
    _set_token("sms")
    db = SessionLocal()
    with patch.object(runtime_config, "get",
                      side_effect=lambda d, k, *a: "" if k == "guangya_client_id" else "x"):
        with pytest.raises(guangya.GuangYaError) as ei:
            asyncio.run(guangya.GuangYaOpenAPI(db).get_user_info())
    db.close()
    assert ei.value.code == 120
    assert "网页端短信登录" in str(ei.value) and "120" in str(ei.value)
