"""v0.9.2 回归：光鸭网页端扫码登录（设备码协议，零开放平台凭证）。

锁死以下红线：
  - start_webqr 用网页端 client_id / account base，无需 project_id / sign_secret。
  - 请求体含 scope=user 与 meta.scene=skill_desktop_login。
  - poll_webqr：authorization_pending → pending，不落 Token。
  - 成功 → 落 AuthToken(flow=webqr)，client_id 为网页端公开 ID（供 refresh）。
  - 过期二维码清空会话并报错。
  - 全部 mock httpx，不触网、不落任何用户数据到外部。
"""
import asyncio
from datetime import timedelta
from unittest.mock import patch

import pytest

from app import guangya_auth
from app.config import settings
from app.db import SessionLocal, migrate
from app.models import AuthSession, AuthToken
from app.utils import utcnow


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as db:
        db.query(AuthToken).delete()
        db.query(AuthSession).delete()
        db.commit()
    yield
    with SessionLocal() as db:
        db.query(AuthToken).delete()
        db.query(AuthSession).delete()
        db.commit()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as s:
        try:
            yield s
        finally:
            s.rollback()


class _Resp:
    def __init__(self, data, status=200):
        self._data = data
        self.status_code = status
        self.text = data if isinstance(data, str) else ""

    def json(self):
        return self._data


def _code_payload():
    return {"device_code": "dc_test", "user_code": "ABCD-1234",
            "verification_url": "https://account.guangyapan.com/__/auth/device/?client_id=x",
            "verification_uri_complete": "https://account.guangyapan.com/__/auth/device/?client_id=x&uc=ABCD",
            "expires_in": 600, "interval": 2}


def test_status_exposes_webqr_supported(db):
    st = guangya_auth.status(db)
    assert st["webqr_supported"] is True


def test_start_webqr_request_shape_and_persist(db):
    captured = {}

    async def fake_post(self, url, json=None, **kw):
        captured["url"] = url
        captured["json"] = json
        return _Resp(_code_payload())

    with patch("httpx.AsyncClient.post", new=fake_post):
        out = asyncio.run(guangya_auth.start_webqr(db))
    assert out["flow"] == "webqr"
    assert out["user_code"] == "ABCD-1234"
    assert out["interval"] == 2 and out["expires_in"] == 600
    assert captured["url"] == f"{settings.guangya_webqr_account_base.rstrip('/')}/v1/auth/device/code"
    assert captured["json"]["client_id"] == settings.guangya_webqr_client_id
    assert captured["json"]["scope"] == "user"
    assert captured["json"]["meta"]["scene"] == "skill_desktop_login"
    s = db.get(AuthSession, 1)
    assert s.purpose == "webqr" and s.device_code == "dc_test"


def test_poll_webqr_pending_does_not_store_token(db):
    s = guangya_auth._session(db)
    s.purpose, s.device_code = "webqr", "dc_test"
    s.expires_at = utcnow() + timedelta(seconds=600)
    db.commit()

    async def fake_post(self, url, json=None, **kw):
        assert json["grant_type"] == "urn:ietf:params:oauth:grant-type:device_code"
        assert json["client_id"] == settings.guangya_webqr_client_id
        return _Resp("authorization_pending", status=400)

    with patch("httpx.AsyncClient.post", new=fake_post):
        out = asyncio.run(guangya_auth.poll_webqr(db))
    assert out["status"] == "pending"
    assert db.get(AuthToken, 1) is None  # pending 不落 Token


def test_poll_webqr_success_stores_webqr_flow(db):
    s = guangya_auth._session(db)
    s.purpose, s.device_code = "webqr", "dc_test"
    s.expires_at = utcnow() + timedelta(seconds=600)
    db.commit()

    async def fake_post(self, url, json=None, **kw):
        return _Resp({"access_token": "tok_webqr", "refresh_token": "ref_webqr",
                      "expires_in": 3600, "sub": "u_webqr"})

    with patch("httpx.AsyncClient.post", new=fake_post):
        out = asyncio.run(guangya_auth.poll_webqr(db))
    assert out["status"] == "authorized"
    t = db.get(AuthToken, 1)
    assert t is not None and t.flow == "webqr"
    # _save_token 优先 OpenAPI client_id（测试环境配了 test-client），否则落网页端公开 ID。
    assert t.client_id in (settings.guangya_client_id, settings.guangya_webqr_client_id)
    assert t.client_id  # refresh 能带上非空 client_id


def test_poll_webqr_expired_clears_session(db):
    s = guangya_auth._session(db)
    s.purpose, s.device_code = "webqr", "dc_test"
    s.expires_at = utcnow() - timedelta(seconds=1)
    db.commit()
    with pytest.raises(guangya_auth.AuthError) as ei:
        asyncio.run(guangya_auth.poll_webqr(db))
    assert ei.value.code == "expired"
    assert db.get(AuthSession, 1).purpose == ""


def test_poll_webqr_no_pending(db):
    with pytest.raises(guangya_auth.AuthError) as ei:
        asyncio.run(guangya_auth.poll_webqr(db))
    assert ei.value.code == "no_pending"
