"""回归测试：短信登录用户扫描走网页端文件接口（无需开放平台 client_id）。

背景：官方 OpenAPI /openapi/v1/file/get_file_list 要求开放平台分配的
x-client-id（应用 ID），普通用户短信登录拿的是网页端凭证，导致 code=120
"缺少clientId"。修复后扫描改用网页端接口 POST /userres/v1/file/get_file_list，
复用短信登录的 Bearer + did + dt，字段结构与原 scanner 完全兼容。
"""
import asyncio
from unittest.mock import patch

from app.db import SessionLocal, migrate
from app.guangya import GuangYaWebFiles
from app.models import AuthToken
from app.security import encrypt_secret


class FakeResponse:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code
        self.text = str(data)

    def json(self):
        return self._data


def _seed_sms_token():
    db = SessionLocal()
    db.query(AuthToken).delete()
    db.add(AuthToken(id=1, client_id="aMe-8VSlkrbQXpUR",
                     access_token_enc=encrypt_secret("sms-token"),
                     refresh_token_enc=encrypt_secret("rt"),
                     flow="sms", device_id="d" * 32, sub="+86 13800138000"))
    db.commit()
    db.close()


def test_web_files_uses_sms_credentials_and_no_client_id():
    migrate()
    _seed_sms_token()
    db = SessionLocal()
    captured = {}

    async def fake_post(self, url, **kwargs):
        captured["url"] = url
        captured["headers"] = kwargs.get("headers", {})
        captured["body"] = kwargs.get("json", {})
        return FakeResponse({"code": 0, "data": {
            "list": [{"fileId": "f1", "fileName": "Movie.2024.mkv", "resType": 1,
                      "fileSize": 10, "ext": ".mkv", "utime": 1}], "total": 1}})

    with patch("httpx.AsyncClient.post", fake_post):
        client = GuangYaWebFiles(db)
        result = asyncio.run(client.list_files("", 0, 100))
    db.close()

    assert captured["url"].endswith("/userres/v1/file/get_file_list")
    assert captured["headers"]["Authorization"] == "Bearer sms-token"
    assert captured["headers"]["did"] == "d" * 32
    assert captured["headers"]["dt"] == "4"
    # 关键：绝不能出现 x-client-id（那会触发官方 OpenAPI 的 code=120）
    assert "x-client-id" not in captured["headers"]
    # 根目录映射为 "*"
    assert captured["body"]["parentId"] == "*"
    assert captured["body"]["pageSize"] == 100
    assert len(result["list"]) == 1
    # 清理，避免污染后续用例
    db2 = SessionLocal()
    db2.query(AuthToken).delete()
    db2.commit()
    db2.close()


def test_web_files_rejects_non_sms_login():
    migrate()
    db = SessionLocal()
    db.query(AuthToken).delete()
    db.add(AuthToken(id=1, flow="device", access_token_enc=encrypt_secret("x")))
    db.commit()
    from app.guangya import WebFileError
    try:
        GuangYaWebFiles(db)
        raised = False
    except WebFileError:
        raised = True
    db.query(AuthToken).delete()  # 清理，避免污染后续用例
    db.commit()
    db.close()
    assert raised is True
