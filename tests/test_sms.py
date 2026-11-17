"""光鸭自研短信登录闭环测试（mock 光鸭账号接口，无真实网络）。"""
import asyncio
from unittest.mock import patch

from app import guangya_sms
from app.db import SessionLocal, migrate
from app.guangya import GuangYaShareClient
from app.models import AppConfig, AuthToken
from app.security import decrypt_secret


class FakeResponse:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code
        self.text = __import__("json").dumps(data, ensure_ascii=False)

    def json(self):
        return self._data


def test_sms_login_persists_token_and_share_falls_back():
    migrate()
    db = SessionLocal()
    db.query(AuthToken).delete()
    db.query(AppConfig).filter(AppConfig.key == "sms_pending").delete()
    db.commit()

    responses = [
        FakeResponse({"captcha_token": "capt-1"}),                 # init
        FakeResponse({"verification_id": "vid-1"}),                # send
        FakeResponse({"verification_token": "vtoken-1"}),          # verify
        FakeResponse({"access_token": "gy-token-xyz", "refresh_token": "rt-1",
                      "expires_in": 3600}),                        # signin
    ]
    seq = iter(responses)

    def fake_post(url, headers=None, json=None, timeout=None):
        return next(seq)

    with patch("httpx.post", side_effect=fake_post):
        r = asyncio.run(guangya_sms.send_code(db, "13800138000"))
        assert r["ok"] is True
        r2 = asyncio.run(guangya_sms.verify_code(db, "123456"))
        assert r2["ok"] is True

    db.expire_all()
    t = db.get(AuthToken, 1)
    assert t and t.flow == "sms"
    assert decrypt_secret(t.access_token_enc) == "gy-token-xyz"
    assert decrypt_secret(t.refresh_token_enc) == "rt-1"
    assert t.device_id and len(t.device_id) == 32
    # 分享客户端优先使用短信登录 token
    client = GuangYaShareClient(db)
    assert client.headers["Authorization"] == "Bearer gy-token-xyz"
    assert client.headers["did"] == t.device_id
    st = GuangYaShareClient.config_status(db)
    assert st["configured"] is True and "短信登录" in st["source"]
    db.close()


def test_refresh_updates_token():
    migrate()
    db = SessionLocal()
    t = db.get(AuthToken, 1)
    assert t and t.refresh_token_enc
    with patch("httpx.post", return_value=FakeResponse(
            {"access_token": "gy-refreshed", "refresh_token": "rt-2", "expires_in": 7200})):
        new = asyncio.run(guangya_sms.refresh(db))
    assert new == "gy-refreshed"
    db.expire_all()
    assert decrypt_secret(db.get(AuthToken, 1).access_token_enc) == "gy-refreshed"
    db.close()
