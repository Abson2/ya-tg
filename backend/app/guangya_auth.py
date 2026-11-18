import time
from datetime import timedelta
from urllib.parse import urlencode

import httpx
from sqlalchemy.orm import Session

from .config import settings
from .db import json_dumps
from .models import AuthSession, AuthToken
from .security import decrypt_secret, encrypt_secret, secret_configured
from .utils import as_utc, device_sign, generate_pkce, traceparent, utcnow


class AuthError(RuntimeError):
    def __init__(self, message: str, code: str = ""):
        super().__init__(message)
        self.code = code


def _token(db: Session) -> AuthToken | None:
    return db.get(AuthToken, 1)


def _session(db: Session) -> AuthSession:
    obj = db.get(AuthSession, 1)
    if not obj:
        obj = AuthSession(id=1)
        db.add(obj)
        db.flush()
    return obj


def status(db: Session) -> dict:
    from . import runtime_config
    from .utils import mask_account
    t = _token(db)
    s = _session(db)
    now = utcnow()
    expires = as_utc(t.expires_at) if t else None
    flow = t.flow if t else ""
    client_id = runtime_config.get(db, "guangya_client_id")
    share_configured = bool(runtime_config.get(db, "guangya_share_authorization"))
    return {
        "configured": bool(client_id) or flow in ("sms", "tgto"),
        "authenticated": bool(t and t.access_token_enc),
        "has_refresh_token": bool(t and t.refresh_token_enc) or flow in ("sms", "tgto"),
        "client_id": client_id,
        "flow": flow,
        "share_configured": share_configured,
        "sub": mask_account(t.sub) if t else "",  # v0.7.0：用户 ID 脱敏
        "expires_at": expires.isoformat().replace("+00:00", "Z") if expires else None,
        "expires_in_seconds": max(0, int((expires - now).total_seconds())) if expires else None,
        "expired": bool(expires and expires <= now),
        "needs_refresh": bool(expires and expires <= now + timedelta(minutes=5)),
        "pending_flow": s.purpose or None,
        "pending_message": s.last_message if s.purpose else "",
        "device_supported": bool(settings.guangya_client_id and settings.guangya_project_id and settings.guangya_sign_secret),
        "pkce_supported": bool(settings.guangya_client_id and settings.guangya_oauth_redirect_uri),
    }


def seed_from_config(db: Session) -> None:
    """把 .env 里的旧 Token 作为种子导入一次，后续以库内 Token 为准。"""
    if _token(db) or not settings.guangya_client_id or not settings.guangya_access_token:
        return
    if not secret_configured():
        return  # 未配置加密密钥时不导入，避免启动崩溃；请设置 APP_SECRET 后重启
    db.add(AuthToken(
        id=1,
        client_id=settings.guangya_client_id,
        access_token_enc=encrypt_secret(settings.guangya_access_token),
        refresh_token_enc=encrypt_secret(settings.guangya_refresh_token),
        expires_at=None,  # 旧配置未携带 expires_in，未知过期时间；靠 117 触发刷新
        sub="",
        flow="seed",
        granted_at=utcnow(),
        updated_at=utcnow(),
    ))
    db.commit()


async def start_device(db: Session) -> dict:
    """OpenAPI v1.3 §4.1/4.2：设备码由后端用 sign_secret 签名申请。"""
    cid = settings.guangya_client_id
    if not cid or not settings.guangya_project_id or not settings.guangya_sign_secret:
        raise AuthError("Device 授权需要 GUANGYA_CLIENT_ID / GUANGYA_PROJECT_ID / GUANGYA_SIGN_SECRET", "missing_config")
    timestamp = str(int(time.time()))
    sign = device_sign(cid, timestamp, settings.guangya_sign_secret)
    headers = {
        "Content-Type": "application/json",
        "x-client-id": cid,
        "x-project-id": settings.guangya_project_id,
        "x-device-id": settings.guangya_device_id,
        "timestamp": timestamp,
        "sign": sign,
        "traceparent": traceparent(),
    }
    body = {"scope": "", "client_id": cid, "project_id": settings.guangya_project_id}
    async with httpx.AsyncClient(timeout=settings.guangya_timeout) as client:
        r = await client.post(f"{settings.guangya_api_base.rstrip('/')}/v1/auth/device/code",
                              headers=headers, json=body)
    payload = _json(r)
    code = int(payload.get("code", 0) or 0)
    if r.status_code != 200 or code not in (0, None):
        msg = str(payload.get("msg") or payload.get("message") or r.text[:300])
        if code == 111:
            msg += "；请在光鸭开放平台报备本机/容器出口 IP 白名单（Device 申请设备码按出口 IP 校验）"
        raise AuthError(msg, str(code))
    expires_in = int(payload.get("expires_in", 120) or 120)
    s = _session(db)
    s.purpose = "device"
    s.device_code = payload.get("device_code", "")
    s.user_code = payload.get("user_code", "")
    s.verification_url = payload.get("verification_url", "")
    s.verification_uri_complete = payload.get("verification_uri_complete", "")
    s.interval = max(2, int(payload.get("interval", 3) or 3))
    s.expires_at = utcnow() + timedelta(seconds=expires_in)
    s.last_message = ""
    s.updated_at = utcnow()
    db.commit()
    return {
        "user_code": s.user_code,
        "verification_url": s.verification_url,
        "verification_uri_complete": s.verification_uri_complete,
        "interval": s.interval,
        "expires_in": expires_in,
        "schema_url": f"gyp://auth?url={s.verification_uri_complete}",
    }


async def poll_device(db: Session) -> dict:
    """§4.4：authorization_pending 为纯文本，成功为 JSON；无 sign。"""
    s = _session(db)
    if s.purpose != "device" or not s.device_code:
        raise AuthError("没有进行中的 Device 授权，请先发起设备码授权", "no_pending")
    if s.expires_at and as_utc(s.expires_at) <= utcnow():
        s.purpose = ""
        db.commit()
        raise AuthError("设备码已过期，请重新发起授权", "expired")
    body = {
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        "device_code": s.device_code,
        "client_id": settings.guangya_client_id,
    }
    headers = {
        "Content-Type": "application/json",
        "x-client-id": settings.guangya_client_id,
        "x-device-id": settings.guangya_device_id,
        "x-project-id": settings.guangya_project_id,
        "traceparent": traceparent(),
    }
    async with httpx.AsyncClient(timeout=settings.guangya_timeout) as client:
        r = await client.post(f"{settings.guangya_account_base.rstrip('/')}/v1/auth/token",
                              headers=headers, json=body)
    text = (r.text or "").strip()
    if "authorization_pending" in text:
        s.last_message = "等待用户在光鸭 App 中确认授权"
        db.commit()
        return {"status": "pending"}
    if "expired" in text.lower() or "invalid" in text.lower():
        s.purpose = ""
        db.commit()
        raise AuthError(f"设备授权失败：{text[:200]}", "expired")
    payload = _json(r)
    if not payload.get("access_token"):
        s.last_message = text[:200]
        db.commit()
        return {"status": "pending", "message": s.last_message}
    _save_token(db, payload, "device")
    s.purpose = ""
    s.device_code = ""
    s.last_message = "授权成功"
    db.commit()
    return {"status": "authorized", "sub": payload.get("sub", "")}


def start_pkce(db: Session) -> dict:
    """§5.1/5.2：state 与 code_verifier 暂存后端会话，不下发 sign_secret。"""
    if not settings.guangya_client_id or not settings.guangya_oauth_redirect_uri:
        raise AuthError("Web PKCE 需要 GUANGYA_CLIENT_ID 与 GUANGYA_OAUTH_REDIRECT_URI", "missing_config")
    import secrets

    verifier, challenge = generate_pkce()
    state = secrets.token_urlsafe(24)
    s = _session(db)
    s.purpose = "pkce"
    s.state = state
    s.code_verifier = verifier
    s.redirect_uri = settings.guangya_oauth_redirect_uri
    s.expires_at = utcnow() + timedelta(minutes=10)
    s.last_message = ""
    s.updated_at = utcnow()
    db.commit()
    params = {
        "client_id": settings.guangya_client_id,
        "response_type": "code",
        "scope": "user offline",
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "redirect_uri": settings.guangya_oauth_redirect_uri,
        "state": state,
    }
    return {"auth_url": f"{settings.guangya_www_base.rstrip('/')}/oauth/?{urlencode(params)}", "state": state}


async def exchange_pkce(db: Session, code: str, state: str, error: str = "", error_description: str = "") -> dict:
    s = _session(db)
    if s.purpose != "pkce" or not s.state:
        raise AuthError("没有进行中的 Web 授权", "no_pending")
    if state != s.state:
        raise AuthError("state 校验失败，疑似 CSRF 或会话已失效，请重新发起授权", "bad_state")
    if error:
        s.purpose = ""
        db.commit()
        raise AuthError(f"用户拒绝授权：{error_description or error}", "denied")
    if s.expires_at and as_utc(s.expires_at) <= utcnow():
        s.purpose = ""
        db.commit()
        raise AuthError("授权会话已过期，请重新发起", "expired")
    body = {
        "client_id": settings.guangya_client_id,
        "grant_type": "authorization_code",
        "code": code,
        "code_verifier": s.code_verifier,
        "redirect_uri": s.redirect_uri,
    }
    headers = {"Content-Type": "application/json", "x-client-id": settings.guangya_client_id,
               "traceparent": traceparent()}
    async with httpx.AsyncClient(timeout=settings.guangya_timeout) as client:
        r = await client.post(f"{settings.guangya_account_base.rstrip('/')}/v1/auth/token",
                              headers=headers, json=body)
    payload = _json(r)
    if not payload.get("access_token"):
        raise AuthError(str(payload.get("msg") or r.text[:300]))
    _save_token(db, payload, "pkce")
    s.purpose = ""
    s.code_verifier = ""
    s.state = ""
    db.commit()
    return {"status": "authorized", "sub": payload.get("sub", "")}


async def refresh(db: Session, force: bool = False) -> str:
    """刷新 Token。短信登录通道走光鸭账号服务；官方 Device/PKCE 通道走 refresh_token。"""
    t = _token(db)
    if t and t.flow in ("sms", "tgto"):
        from . import guangya_sms
        try:
            return await guangya_sms.refresh(db)
        except guangya_sms.SmsError as exc:
            raise AuthError(str(exc), "sms_refresh_failed") from exc
    if not t or not t.refresh_token_enc:
        raise AuthError("没有可用 Refresh Token，请重新授权", "no_refresh_token")
    if not force and t.expires_at and as_utc(t.expires_at) > utcnow() + timedelta(minutes=5):
        return decrypt_secret(t.access_token_enc)
    body = {
        "client_id": settings.guangya_client_id or t.client_id,
        "grant_type": "refresh_token",
        "refresh_token": decrypt_secret(t.refresh_token_enc),
    }
    headers = {"Content-Type": "application/json", "x-client-id": body["client_id"],
               "traceparent": traceparent()}
    async with httpx.AsyncClient(timeout=settings.guangya_timeout) as client:
        r = await client.post(f"{settings.guangya_account_base.rstrip('/')}/v1/auth/token",
                              headers=headers, json=body)
    payload = _json(r)
    if not payload.get("access_token"):
        raise AuthError(f"刷新失败：{payload.get('msg') or r.text[:200]}，请重新授权", "refresh_failed")
    _save_token(db, payload, t.flow or "refresh")
    db.commit()
    return payload["access_token"]


async def get_access_token(db: Session) -> str:
    t = _token(db)
    if not t or not t.access_token_enc:
        raise AuthError("尚未完成光鸭授权，请在管理后台「光鸭授权」页完成授权", "not_authorized")
    if t.expires_at and as_utc(t.expires_at) <= utcnow() + timedelta(minutes=5):
        return await refresh(db)
    return decrypt_secret(t.access_token_enc)


def _save_token(db: Session, payload: dict, flow: str) -> None:
    t = _token(db) or AuthToken(id=1)
    t.client_id = settings.guangya_client_id or t.client_id
    t.access_token_enc = encrypt_secret(payload["access_token"])
    if payload.get("refresh_token"):
        t.refresh_token_enc = encrypt_secret(payload["refresh_token"])
    t.expires_at = utcnow() + timedelta(seconds=int(payload.get("expires_in", 3600) or 3600))
    t.sub = str(payload.get("sub", "") or t.sub)
    t.flow = flow
    t.granted_at = t.granted_at or utcnow()
    t.updated_at = utcnow()
    if t not in db:
        db.add(t)


def _json(r: httpx.Response) -> dict:
    try:
        data = r.json()
        return data if isinstance(data, dict) else {"raw": data}
    except Exception:
        return {"raw": r.text[:500]}
