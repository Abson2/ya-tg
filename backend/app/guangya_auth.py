from datetime import timedelta

import httpx
from sqlalchemy.orm import Session

from .config import settings
from .db import json_dumps
from .models import AuthSession, AuthToken
from .security import decrypt_secret, encrypt_secret, secret_configured
from .utils import as_utc, traceparent, utcnow


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
    # v0.9.5：扫码登录（webqr）写入 AuthToken(id=1) 且补 did，与短信二选一都能跑业务，
    # 因此与 sms/tgto 同等视为「已配置、可自动刷新」。
    return {
        "configured": bool(client_id) or flow in ("sms", "tgto", "webqr"),
        "authenticated": bool(t and t.access_token_enc),
        "has_refresh_token": bool(t and t.refresh_token_enc) or flow in ("sms", "tgto", "webqr"),
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
        # v0.9.2：网页端扫码登录无需开放平台应用凭证，默认即可用
        "webqr_supported": bool(settings.guangya_webqr_account_base and settings.guangya_webqr_client_id),
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


# ---------- v0.9.2：网页端扫码登录（官方 Skill 同款设备码协议，零配置） ----------
# 与官方 OpenAPI Device 的区别：无需 project_id / sign_secret / IP 白名单，
# 使用网页应用公开 client_id（非隐私），端点是 account.guangyapan.com 而非 openapi。
# 协议经只读实测确认：code 申请无签名；token 换取 grant_type=device_code，
# pending 时 HTTP 400 + "authorization_pending"文本，成功为 JSON。
# v0.9.5：扫码 token 独立存 runtime_config，绝不覆盖 AuthToken(id=1) 短信业务凭证
# （镜像/扫描/分享只认带 did 的 sms 凭证；扫码 token 无 did，写入 id=1 会让业务 401）。

def _webqr_base() -> str:
    return settings.guangya_webqr_account_base.rstrip("/")


def _webqr_cid() -> str:
    return settings.guangya_webqr_client_id


async def start_webqr(db: Session) -> dict:
    """申请网页端设备码，返回供光鸭 App 扫一扫的授权地址与 user_code。"""
    cid = _webqr_cid()
    if not cid or not _webqr_base():
        raise AuthError("网页端扫码登录未配置 client_id / account base", "missing_config")
    body = {"client_id": cid, "scope": "user",
            "meta": {"scene": "skill_desktop_login", "device": "ya-tg"}}
    async with httpx.AsyncClient(timeout=settings.guangya_timeout) as client:
        r = await client.post(f"{_webqr_base()}/v1/auth/device/code", json=body)
    payload = _json(r)
    if r.status_code != 200 or not payload.get("device_code"):
        msg = str(payload.get("msg") or payload.get("message") or r.text[:300])
        raise AuthError(f"申请扫码授权失败：{msg}", str(payload.get("code", "")))
    expires_in = int(payload.get("expires_in", 600) or 600)
    s = _session(db)
    s.purpose = "webqr"
    s.device_code = str(payload.get("device_code") or "")
    s.user_code = str(payload.get("user_code") or "")
    s.verification_url = str(payload.get("verification_url") or "")
    s.verification_uri_complete = str(payload.get("verification_uri_complete")
                                      or payload.get("short_uri_complete")
                                      or payload.get("verification_url") or "")
    s.interval = max(2, int(payload.get("interval", 2) or 2))
    s.expires_at = utcnow() + timedelta(seconds=expires_in)
    s.last_message = ""
    s.updated_at = utcnow()
    db.commit()
    # 二维码载荷是 App 内授权地址，不能当网页链接直接用浏览器打开。
    return {
        "user_code": s.user_code,
        "verification_url": s.verification_url,
        "verification_uri_complete": s.verification_uri_complete,
        "interval": s.interval,
        "expires_in": expires_in,
        "flow": "webqr",
    }


async def poll_webqr(db: Session) -> dict:
    """轮询扫码授权结果；pending 保持会话，成功落 AuthToken(flow=webqr)。"""
    s = _session(db)
    if s.purpose != "webqr" or not s.device_code:
        raise AuthError("没有进行中的扫码授权，请先发起扫码登录", "no_pending")
    if s.expires_at and as_utc(s.expires_at) <= utcnow():
        s.purpose = ""
        db.commit()
        raise AuthError("二维码已过期，请重新发起扫码登录", "expired")
    body = {
        "client_id": _webqr_cid(),
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        "device_code": s.device_code,
    }
    async with httpx.AsyncClient(timeout=settings.guangya_timeout) as client:
        r = await client.post(f"{_webqr_base()}/v1/auth/token", json=body)
    text = (r.text or "").strip()
    if "authorization_pending" in text:
        s.last_message = "等待用户在光鸭 App 中扫码并确认授权"
        db.commit()
        return {"status": "pending"}
    if "expired" in text.lower() or "invalid" in text.lower():
        s.purpose = ""
        db.commit()
        raise AuthError(f"扫码授权失败：{text[:200]}", "expired")
    payload = _json(r)
    if not payload.get("access_token"):
        s.last_message = text[:200]
        db.commit()
        return {"status": "pending", "message": s.last_message}
    _save_token(db, payload, "webqr")
    s.purpose = ""
    s.device_code = ""
    s.last_message = "扫码授权成功"
    db.commit()
    return {"status": "authorized", "sub": str(payload.get("sub", "") or "")}


async def refresh(db: Session, force: bool = False) -> str:
    """刷新 Token。短信登录走光鸭账号服务；扫码登录走网页端设备码 refresh_token。"""
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
    # v0.9.5：扫码登录（webqr）是网页端设备码协议，access/refresh token 都在
    # account.guangyapan.com 域名、用 webqr 公开 client_id；不能用官方 OpenAPI 账号服务刷新。
    is_webqr = t.flow == "webqr"
    cid = _webqr_cid() if is_webqr else (settings.guangya_client_id or t.client_id)
    base = _webqr_base() if is_webqr else settings.guangya_account_base
    body = {
        "client_id": cid,
        "grant_type": "refresh_token",
        "refresh_token": decrypt_secret(t.refresh_token_enc),
    }
    headers = {"Content-Type": "application/json", "x-client-id": cid,
               "traceparent": traceparent()}
    async with httpx.AsyncClient(timeout=settings.guangya_timeout) as client:
        r = await client.post(f"{base.rstrip('/')}/v1/auth/token", headers=headers, json=body)
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
    # v0.9.2：扫码登录没有 OpenAPI client_id 时，落到网页端公开 client_id，
    # 保证后续 refresh_token 能带上正确的 client_id。
    cid = settings.guangya_client_id or (settings.guangya_webqr_client_id if flow == "webqr" else "")
    t.client_id = cid or t.client_id
    t.access_token_enc = encrypt_secret(payload["access_token"])
    if payload.get("refresh_token"):
        t.refresh_token_enc = encrypt_secret(payload["refresh_token"])
    t.expires_at = utcnow() + timedelta(seconds=int(payload.get("expires_in", 3600) or 3600))
    t.sub = str(payload.get("sub", "") or t.sub)
    t.flow = flow
    # v0.9.5：扫码登录也要能跑镜像/扫描/分享业务，这些接口要求 Bearer+did+dt。
    # did 是本地生成的设备标识（与短信登录一致），网页端业务接口只校验非空、不绑定 client_id。
    # 缺失 did 会让 GuangYaWebFiles 直接抛 401——这正是「扫码后业务凭证失效」的根因。
    if flow == "webqr":
        from .guangya_sms import generate_did
        existing_did = getattr(t, "device_id", "") or ""
        t.device_id = existing_did or generate_did()
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
