"""TgtoDrive 光鸭认证网关桥接。

用户链路：管理页填手机号 → 短信验证码 → TgtoDrive 代登录光鸭大号 →
拉取 access_token / device_id(did) / dt → 加密落库 → 扫描与建分享直接复用 →
到期由定时任务经 TgtoDrive 自动刷新。

这样普通用户不需要理解或申请 CLIENT_ID / PROJECT_ID / SIGN_SECRET / 会话凭证，
只做一次短信登录即可。TgtoDrive 项目令牌在 TgtoDrive「开放接口」页生成。
"""
from __future__ import annotations

from datetime import datetime, timezone

import httpx
from sqlalchemy.orm import Session

from . import runtime_config
from .config import settings
from .models import AppConfig, AuthToken
from .security import encrypt_secret
from .utils import utcnow


class TgtoError(RuntimeError):
    def __init__(self, message: str, code: str = ""):
        super().__init__(message)
        self.code = code


def _endpoint(db: Session) -> tuple[str, str]:
    base = (runtime_config.get(db, "tgto_base_url") or settings.tgto_base_url).rstrip("/")
    token = runtime_config.get(db, "tgto_api_token")
    if not token:
        raise TgtoError(
            "请先填写 TgtoDrive 项目令牌：在 TgtoDrive「开放接口」页复制 Bearer 令牌，"
            "填入管理页「一键连接」的连接设置。",
            "missing_tgto_token",
        )
    return base, token


async def _call(db: Session, method: str, path: str, payload: dict | None = None) -> dict:
    base, token = _endpoint(db)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=False) as client:
            r = await client.request(method, f"{base}{path}", json=payload, headers=headers)
    except httpx.HTTPError as exc:
        raise TgtoError(f"无法连接 TgtoDrive（{base}）：{exc}", "unreachable") from exc

    text = (r.text or "").strip()
    try:
        data = r.json()
        if not isinstance(data, dict):
            data = {"raw": data}
    except Exception:
        data = {"raw": text[:500]}

    if r.status_code == 401:
        raise TgtoError("TgtoDrive 项目令牌无效或已过期，请在 TgtoDrive 开放接口页重新生成", "bad_tgto_token")
    if r.status_code == 403:
        raise TgtoError("TgtoDrive 拒绝了该操作（403），请检查令牌的业务权限", "forbidden")
    if r.status_code >= 400:
        raise TgtoError(f"TgtoDrive 接口 {path} 返回 HTTP {r.status_code}：{text[:200]}", str(r.status_code))
    _raise_business_error(data)
    return data


def _raise_business_error(data: dict) -> None:
    """TgtoDrive 透传网页响应，错误可能藏在 success/msg/code 里。"""
    if data.get("success") is False:
        raise TgtoError(str(data.get("msg") or data.get("message") or data.get("error") or "光鸭接口返回失败")[:300], "business")
    code = data.get("code")
    if code not in (None, 0, "0", 200, "200", "success"):
        msg = str(data.get("msg") or data.get("message") or data.get("error") or data.get("raw") or "")[:300]
        raise TgtoError(msg or f"光鸭接口返回 code={code}", str(code))


def configured(db: Session) -> bool:
    return bool(runtime_config.get(db, "tgto_api_token"))


async def send_code(db: Session, phone: str) -> dict:
    phone = (phone or "").strip()
    if not phone:
        raise TgtoError("请输入光鸭账号手机号", "missing_phone")
    await _call(db, "POST", "/api/guangya/login/send-code", {"phone": phone})
    return {"ok": True, "message": "验证码已发送，请查收短信"}


async def verify_code(db: Session, code: str) -> dict:
    code = (code or "").strip()
    if not code:
        raise TgtoError("请输入短信验证码", "missing_code")
    await _call(db, "POST", "/api/guangya/login/verify-code", {"code": code})
    return await pull_token(db, force_refresh=False)


async def pull_token(db: Session, force_refresh: bool = False) -> dict:
    """从 TgtoDrive 拉取当前光鸭大号凭证并加密落库。refresh 定时也复用此函数。"""
    data = await _call(db, "POST", "/api/guangya/openapi-token", {"force_refresh": bool(force_refresh)})
    token = str(data.get("access_token") or "")
    if not token:
        hint = str(data.get("raw") or data.get("msg") or data.get("message") or "")[:200]
        raise TgtoError(f"TgtoDrive 未返回光鸭凭证，大号可能尚未登录：{hint}", "no_token")

    did = str(data.get("device_id") or "")
    dt = str(data.get("dt") or "4") or "4"
    expires_at = data.get("expires_at") or 0

    _save_auth_token(db, token, int(expires_at or 0))
    runtime_config.set_many(db, {
        "guangya_share_authorization": token,
        "guangya_share_did": did,
        "guangya_share_dt": dt,
    })
    db.commit()
    return {
        "ok": True,
        "did_set": bool(did),
        "dt": dt,
        "expires_at": datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat() if expires_at else None,
        "message": "光鸭登录成功，凭证已加密保存",
    }


async def refresh(db: Session) -> str:
    """定时任务/117 失效时调用；返回最新 access_token。"""
    await pull_token(db, force_refresh=True)
    t = db.get(AuthToken, 1)
    from .security import decrypt_secret
    return decrypt_secret(t.access_token_enc) if t else ""


async def status(db: Session) -> dict:
    result = {
        "bridge_configured": configured(db),
        "tgto_base_url": runtime_config.get(db, "tgto_base_url"),
        "tgto_token_source": "db" if (db.get(AppConfig, "tgto_api_token") and db.get(AppConfig, "tgto_api_token").value_enc) else ("env" if settings.tgto_api_token else "empty"),
        "logged_in": False,
        "account": "",
        "local_token": bool(db.get(AuthToken, 1) and db.get(AuthToken, 1).access_token_enc),
        "flow": (db.get(AuthToken, 1).flow if db.get(AuthToken, 1) else "") or "",
        "reachable": False,
        "message": "",
    }
    if not result["bridge_configured"]:
        result["message"] = "未配置 TgtoDrive 项目令牌，当前使用直连授权方式"
        return result
    try:
        data = await _call(db, "GET", "/api/guangya/token/status")
        result["reachable"] = True
        result["logged_in"] = bool(data.get("logged_in") or data.get("is_login") or data.get("success"))
        result["account"] = str(data.get("phone") or data.get("account") or data.get("userId") or "")
        result["message"] = "TgtoDrive 已连接" if result["logged_in"] else "TgtoDrive 已连接，但光鸭大号未登录"
    except TgtoError as exc:
        result["message"] = exc.code if exc.code in ("bad_tgto_token", "unreachable", "forbidden") else str(exc)[:200]
    return result


def _save_auth_token(db: Session, token: str, expires_at_unix: int) -> None:
    t = db.get(AuthToken, 1)
    if not t:
        t = AuthToken(id=1)
        db.add(t)
    t.access_token_enc = encrypt_secret(token)
    t.expires_at = datetime.fromtimestamp(expires_at_unix, tz=timezone.utc) if expires_at_unix > 0 else None
    t.flow = "tgto"
    t.sub = t.sub or ""
    t.granted_at = t.granted_at or utcnow()
    t.updated_at = utcnow()
