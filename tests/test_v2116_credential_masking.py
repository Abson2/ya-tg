"""v2.1.16 回归：配置页凭证「掩码回写」——TgtoDrive OpenAPI 恒 401 的根因。

事故链（2026-11-23 线上实测，用户自带证据链核对成立）：
  ① 后端 GET /api/interaction/config 用 runtime_config.masked() 把 mask_secret() 的
     产物当字段值回显。mask_secret 规则是 value[:4] + 6 个星号 + value[-4:]，
     真 Token 被回显成 `tgto******4ifI`（14 字符）。
  ② 前端只判「非空」就提交（`if (c.tgto_api_key) body.tgto_api_key = ...`），
     掩码非空 → 被提交。
  ③ 后端 set_many() 对任何非空串加密落库 → 掩码变成「凭证」。
  ④ TgtoDrive 端当然查不到这串假凭据，返回 401；而用户每次进配置页顺手保存一次，
     就会把刚填好的真 Token 再打回掩码，表现为「每次升级完就出问题」。

本文件锁死三层防线：
  ① GET 对凭证 key 一律返回空串，只给 *_configured / *_needs_reset 状态；
  ② POST 丢弃任何形如掩码的提交（旧前端 / 浏览器缓存旧 JS / 手工调 API 同样拦住）；
  ③ 前端提交前剔除掩码，并在输入框下如实标注「已配置」或「存的是掩码，需重填」。
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from app import runtime_config, tgto
from app.db import SessionLocal, migrate
from app.main import app
from app.models import AppConfig
from app.security import encrypt_secret, is_masked_secret, mask_secret

H = {"X-Admin-Password": "testpass"}
SECRET_KEYS = ("tgto_api_key", "contribution_bot_token")


def _set(db, key, value):
    row = db.get(AppConfig, key)
    if row is None:
        db.add(AppConfig(key=key, value_enc=encrypt_secret(str(value))))
    else:
        row.value_enc = encrypt_secret(str(value))
    db.flush()


def _drop(db, key):
    row = db.get(AppConfig, key)
    if row is not None:
        db.delete(row)
        db.flush()


@pytest.fixture(autouse=True)
def _fresh():
    """每个用例一张干净的配置表。

    这两个 key 在 Settings 里没有对应字段，`_env_value` 走 getattr 兜底恒为空，
    所以只要清掉 DB 行就是「未配置」，不需要动环境变量。
    """
    migrate()
    with SessionLocal() as db:
        for k in SECRET_KEYS:
            _drop(db, k)
        db.commit()
    yield
    with SessionLocal() as db:
        for k in SECRET_KEYS:
            _drop(db, k)
        db.commit()


# ---------- ① 掩码判据本身 ----------
def test_mask_shape_matches_production_incident():
    """生产库里那串假值 `tgto******4ifI` 必须能被 mask_secret 精确复现、并被识别为掩码。"""
    token = "tgtoQ2xpZW50SWQ4ifI"          # 形如真 Token：4 字符前缀 + 结尾 4ifI
    masked = mask_secret(token)
    assert masked == "tgto" + "*" * 6 + "4ifI"
    assert len(masked) == 14                # 与线上那串假值的长度一致
    assert is_masked_secret(masked) is True
    # 短串掩码是整串星号，同样要认出来
    assert is_masked_secret(mask_secret("short")) is True


def test_real_tokens_are_not_treated_as_masked():
    """真凭证不能被误判成掩码，否则会「永不保存」。"""
    for token in ("tgtoQ2xpZW50SWQ0ifI", "1234567890abcdef", "a-b_c.d~e"):
        assert is_masked_secret(token) is False, token
    assert is_masked_secret("") is False


# ---------- ② 后端 GET：不回显，只回状态 ----------
def test_get_never_echoes_stored_secret():
    token = "tgtoQ2xpZW50SWQ0ifI"
    with SessionLocal() as db:
        _set(db, "tgto_api_key", token)
        db.commit()
    body = TestClient(app).get("/api/interaction/config", headers=H).json()

    # 值恒为空串——连掩码都不给，掩码是这次事故的载体
    assert body["tgto_api_key"] == ""
    assert body["contribution_bot_token"] == ""
    assert "******" not in str(body["tgto_api_key"])
    assert body["tgto_api_key_configured"] is True
    assert body["tgto_api_key_needs_reset"] is False


def test_get_reports_masked_stored_value_as_needs_reset():
    """已被污染的库（存的就是掩码）必须被识别出来——否则页面显示「已配置」，
    用户永远不知道该重填，401 会一直存在。"""
    with SessionLocal() as db:
        _set(db, "tgto_api_key", "tgto" + "*" * 6 + "4ifI")
        db.commit()
    body = TestClient(app).get("/api/interaction/config", headers=H).json()

    assert body["tgto_api_key"] == ""
    assert body["tgto_api_key_configured"] is False   # 不是真凭证，不能说「已配置」
    assert body["tgto_api_key_needs_reset"] is True   # 明确告知需要重填


def test_secret_state_helper_is_the_single_source_of_truth():
    with SessionLocal() as db:
        _set(db, "tgto_api_key", "tgtoQ2xpZW50SWQ0ifI")
        _set(db, "contribution_bot_token", "*" * 10)
        db.flush()
        state = runtime_config.secret_state(db, list(SECRET_KEYS))
    assert state["tgto_api_key"] == {"configured": True, "needs_reset": False}
    assert state["contribution_bot_token"] == {"configured": False, "needs_reset": True}


# ---------- ③ 后端 POST：掩码不得入库 ----------
def test_save_rejects_masked_value_and_keeps_original():
    """真凭据已存好的情况下，误提交掩码必须**不动**原值。"""
    real = "tgtoQ2xpZW50SWQ0ifI"
    with SessionLocal() as db:
        _set(db, "tgto_api_key", real)
        db.commit()
    r = TestClient(app).post("/api/interaction/config", headers=H,
                             json={"tgto_api_key": mask_secret(real)})
    assert r.status_code == 200, r.text
    assert r.json()["ignored_masked"] == ["tgto_api_key"]

    with SessionLocal() as db:
        assert runtime_config.get(db, "tgto_api_key") == real


def test_save_rejects_masked_value_even_when_db_empty():
    """库里本来没有值，提交掩码也不能把它变成「已配置」。"""
    r = TestClient(app).post("/api/interaction/config", headers=H,
                             json={"tgto_api_key": "tgto" + "*" * 6 + "4ifI"})
    assert r.status_code == 200 and r.json()["ignored_masked"] == ["tgto_api_key"]
    with SessionLocal() as db:
        assert runtime_config.get(db, "tgto_api_key") == ""


def test_save_writes_real_token_and_omitted_key_keeps_value():
    """真值正常写入；不传该字段（= 页面留空）时保持原值，不清空。"""
    real = "tgtoQ2xpZW50SWQ0ifI"
    c = TestClient(app)
    c.post("/api/interaction/config", headers=H, json={"tgto_api_key": real})
    with SessionLocal() as db:
        assert runtime_config.get(db, "tgto_api_key") == real

    c.post("/api/interaction/config", headers=H, json={"tgto_base_url": "http://127.0.0.1:9"})
    with SessionLocal() as db:
        assert runtime_config.get(db, "tgto_api_key") == real   # 未被清空

    body = c.get("/api/interaction/config", headers=H).json()
    assert body["tgto_api_key"] == "" and body["tgto_api_key_configured"] is True


def test_save_does_not_persist_any_masked_key_in_db():
    """总账：无论提交什么形态的掩码，库里都不能出现连续 6 个星号。"""
    c = TestClient(app)
    for payload in ("*" * 12, "tgto" + "*" * 6 + "4ifI", "ab******cd"):
        c.post("/api/interaction/config", headers=H, json={"tgto_api_key": payload})
    with SessionLocal() as db:
        rows = db.scalars(select(AppConfig).where(AppConfig.key.in_(list(SECRET_KEYS)))).all()
        assert rows == []


# ---------- ④ 前端与文案 ----------
def _app_vue() -> str:
    from pathlib import Path
    return (Path(__file__).resolve().parents[1] / "frontend/src/App.vue").read_text(encoding="utf-8")


def test_frontend_strips_masked_token_before_submit():
    src = _app_vue()
    assert "looksMasked" in src
    assert "!looksMasked(c.tgto_api_key)" in src
    # 旧写法「只判非空」必须消失，否则掩码照样能过
    assert "if (c.tgto_api_key) body.tgto_api_key = c.tgto_api_key" not in src
    assert "ignored_masked" in src


def test_frontend_surfaces_needs_reset_state():
    src = _app_vue()
    assert "interactionCfg.tgto_api_key_needs_reset" in src
    assert "interactionCfg.tgto_api_key_configured" in src
    # 不回显这件事必须在界面上说清楚
    assert "出于安全不回显" in src


def test_401_hint_warns_about_blank_after_save():
    assert "输入框会重新变空" in tgto.TOKEN_401_HINT
