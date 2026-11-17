"""一键连接光鸭（TgtoDrive 短信登录网关）闭环测试。"""
import asyncio
from unittest.mock import patch

from app import runtime_config, tgto_bridge
from app.db import SessionLocal, migrate
from app.guangya import GuangYaShareClient
from app.models import AuthToken
from app.security import decrypt_secret


class FakeResponse:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code
        self.text = __import__("json").dumps(data)

    def json(self):
        return self._data


def _fake_client_factory(responses):
    """依次返回 responses 里的 FakeResponse。"""
    queue = list(responses)

    class FakeAsyncClient:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def request(self, method, url, json=None, headers=None):
            return queue.pop(0)

    return FakeAsyncClient


def test_sms_login_persists_credentials_and_share_reads_them():
    migrate()
    db = SessionLocal()
    # 清空可能残留
    db.query(AuthToken).delete()
    for k in ("guangya_share_authorization", "guangya_share_did", "guangya_share_dt"):
        row = db.get(runtime_config.AppConfig, k)
        if row:
            db.delete(row)
    runtime_config.set_many(db, {"tgto_api_token": "fake-tgto-token"})
    db.commit()

    token_payload = {
        "success": True, "access_token": "gy-access-xyz", "token_type": "Bearer",
        "device_id": "a" * 32, "dt": "4", "expires_at": 0,
    }

    async def scenario():
        # 1) 发送验证码
        with patch("httpx.AsyncClient", _fake_client_factory([FakeResponse({"success": True})])):
            r = await tgto_bridge.send_code(db, "13800000000")
            assert r["ok"] is True
        # 2) 验证码登录 -> 拉取并落库凭证
        with patch("httpx.AsyncClient", _fake_client_factory([
            FakeResponse({"success": True}),
            FakeResponse(token_payload),
        ])):
            r = await tgto_bridge.verify_code(db, "123456")
            assert r["ok"] is True and r["did_set"] is True

    asyncio.run(scenario())
    db.expire_all()

    # 3) AuthToken 落库且为 tgto 通道
    t = db.get(AuthToken, 1)
    assert t and t.flow == "tgto"
    assert decrypt_secret(t.access_token_enc) == "gy-access-xyz"
    # 4) 分享凭证来自运行时配置（DB），建分享 client 能构造成功且 header 正确
    assert runtime_config.get(db, "guangya_share_did") == "a" * 32
    assert runtime_config.source_map(db)["guangya_share_authorization"] == "db"
    client = GuangYaShareClient(db)
    assert client.headers["Authorization"] == "gy-access-xyz"
    assert client.headers["did"] == "a" * 32
    db.close()


def test_refresh_uses_gateway():
    migrate()
    db = SessionLocal()
    runtime_config.set_many(db, {"tgto_api_token": "fake-tgto-token"})
    db.commit()
    token_payload = {
        "success": True, "access_token": "gy-access-refreshed", "token_type": "Bearer",
        "device_id": "b" * 32, "dt": "4", "expires_at": 0,
    }
    with patch("httpx.AsyncClient", _fake_client_factory([FakeResponse(token_payload)])):
        new_token = asyncio.run(tgto_bridge.refresh(db))
    assert new_token == "gy-access-refreshed"
    db.expire_all()
    assert decrypt_secret(db.get(AuthToken, 1).access_token_enc) == "gy-access-refreshed"
    db.close()
