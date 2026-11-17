"""回归测试：短信登录后，页面刷新重拉状态不能因 naive/aware 时间比较而 500。

背景：SQLite 的 DateTime(timezone=True) 读回会丢 tzinfo（naive UTC），
而 guangya_auth.status() 用 aware 的 utcnow() 与之比较，导致 /api/auth/status
抛 TypeError 500；前端把三个状态请求放在同一 try 里，一个失败就全部显示
"未授权/—"，表现得像"凭证掉了"。本测试锁定该故障不再复现。
"""
import asyncio
from unittest.mock import patch

from fastapi.testclient import TestClient

from app import guangya_sms
from app.db import SessionLocal
from app.main import app


class FakeResponse:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code
        self.text = str(data)

    def json(self):
        return self._data


def _sms_login():
    """mock 走一遍完整短信登录，让 token 以 aware datetime 落库。"""
    seq = iter([
        FakeResponse({"captcha_token": "capt"}),
        FakeResponse({"verification_id": "vid"}),
        FakeResponse({"verification_token": "vtok"}),
        FakeResponse({"access_token": "tok", "refresh_token": "rt", "expires_in": 3600}),
    ])
    db = SessionLocal()
    with patch("httpx.post", side_effect=lambda url, **kw: next(seq)):
        asyncio.run(guangya_sms.send_code(db, "13800138000"))
        asyncio.run(guangya_sms.verify_code(db, "123456"))
    db.close()


def test_all_status_endpoints_ok_after_sms_login():
    headers = {"X-Admin-Password": "testpass"}

    with TestClient(app) as c:
        _sms_login()
        # 模拟页面刷新：重新拉取全部状态端点
        for ep in ("/api/sms/status", "/api/auth/status", "/api/share/status", "/api/overview"):
            r = c.get(ep, headers=headers)
            assert r.status_code == 200, f"{ep} 返回 {r.status_code}: {r.text[:200]}"

        sms = c.get("/api/sms/status", headers=headers).json()
        auth = c.get("/api/auth/status", headers=headers).json()
        assert sms["logged_in"] is True
        assert auth["authenticated"] is True
        assert auth["flow"] == "sms"
        # 倒计时字段存在且为正数
        assert sms["expires_in_seconds"] is not None and sms["expires_in_seconds"] > 0
        assert sms["auto_refresh"] is True
        assert sms["expires_at"].endswith("Z")
        # 前端可读的绝对时间（带 Z）不应被当成本地时间
        from datetime import datetime
        assert datetime.fromisoformat(sms["expires_at"].replace("Z", "+00:00")).tzinfo is not None


def test_as_utc_normalizes_naive_datetime():
    """SQLite 读回的 naive UTC 时间必须能安全地与 aware 时间比较。"""
    from datetime import timedelta
    from app.utils import as_utc, utcnow
    naive = utcnow().replace(tzinfo=None)
    # 归一化后不能再抛 offset-naive/aware 比较错误
    assert as_utc(naive) <= utcnow() + timedelta(minutes=10)
    assert as_utc(naive).tzinfo is not None
    assert as_utc(None) is None
