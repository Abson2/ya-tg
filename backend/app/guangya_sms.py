"""光鸭账号短信登录（自研实现，借鉴开源客户端的接口契约，不依赖任何第三方项目）。

流程：captcha/init → auth/verification(发送) → verification/verify → auth/signin
返回 access_token / refresh_token / expires_in，加密落库到 AuthToken（flow=sms）。
Token 过期或 401 时经 /v1/auth/token 刷新。

接口与请求头来自光鸭网页端账号服务（account.guangyapan.com），client_id 为网页端
公开标识，不是需要用户去开放平台申请的应用凭证，因此最终用户零配置。
"""
from __future__ import annotations

import secrets
from datetime import datetime, timezone
from typing import Callable

import httpx
from sqlalchemy.orm import Session

from .models import AuthToken
from .security import decrypt_secret, encrypt_secret
from .utils import utcnow

# 光鸭网页客户端公开标识（非用户应用凭证）
WEB_CLIENT_ID = "aMe-8VSlkrbQXpUR"
ACCOUNT_BASE = "https://account.guangyapan.com"

_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36")


class SmsError(RuntimeError):
    def __init__(self, message: str, code: str = ""):
        super().__init__(message)
        self.code = code


def generate_did() -> str:
    """模拟设备 ID：MD5(16 随机字节)。"""
    import hashlib
    return hashlib.md5(secrets.token_bytes(16)).hexdigest()


def _account_headers(did: str) -> dict:
    return {
        "accept": "*/*",
        "content-type": "application/json",
        "origin": "https://www.guangyapan.com",
        "referer": "https://www.guangyapan.com/",
        "user-agent": _UA,
        "x-client-id": WEB_CLIENT_ID,
        "x-client-version": "0.0.1",
        "x-device-id": did,
        "x-device-model": "chrome%2F147.0.0.0",
        "x-device-name": "PC-Chrome",
        "x-device-sign": f"wdi10.{did}{secrets.token_hex(16)}",
        "x-net-work-type": "NONE",
        "x-os-version": "MacIntel",
        "x-platform-version": "1",
        "x-protocol-version": "301",
        "x-provider-name": "NONE",
        "x-sdk-version": "9.0.2",
    }


def _post(path: str, body: dict, headers: dict, timeout: float = 20.0) -> dict:
    try:
        r = httpx.post(f"{ACCOUNT_BASE}{path}", headers=headers, json=body, timeout=timeout)
    except httpx.HTTPError as exc:
        raise SmsError(f"连接光鸭账号服务失败：{exc}", "unreachable") from exc
    try:
        data = r.json()
    except Exception:
        raise SmsError(f"光鸭接口返回异常（HTTP {r.status_code}）：{r.text[:200]}", "bad_response")
    if not isinstance(data, dict):
        raise SmsError("光鸭接口返回格式异常", "bad_response")
    code = data.get("code")
    if r.status_code >= 400 or (code not in (None, 0, "0", 200, "200", "success") and code is not None):
        msg = str(data.get("msg") or data.get("message") or data.get("error") or r.text[:200])
        raise SmsError(msg or f"光鸭接口返回 code={code}", str(code or r.status_code))
    return data


def normalize_phone(phone: str) -> str:
    """转成带国际区号格式：+86 13800138000。"""
    p = (phone or "").strip().replace(" ", "").replace("-", "")
    if not p:
        raise SmsError("请输入手机号", "missing_phone")
    if p.startswith("+"):
        return f"+{p[1:]}"
    if p.startswith("0086"):
        return "+86 " + p[4:]
    if len(p) == 11 and p.isdigit():
        return "+86 " + p
    return "+86 " + p


async def send_code(db: Session, phone: str) -> dict:
    """第一步+第二步： captcha/init 然后发送短信。返回 verification_id 供验证用。"""
    username = normalize_phone(phone)
    row = db.get(AuthToken, 1)
    did = (row.device_id if row and getattr(row, "device_id", "") else "") or generate_did()
    headers = _account_headers(did)
    init = _post("/v1/shield/captcha/init", {
        "client_id": WEB_CLIENT_ID,
        "action": "POST:/v1/auth/verification",
        "device_id": did,
        "meta": {"phone_number": username},
    }, headers)
    # 部分场景需要人机验证：init 返回 url 而没有 captcha_token
    if not init.get("captcha_token"):
        raise SmsError("光鸭要求人机验证，请在光鸭 App/网页完成验证后重试", "captcha_required")
    captcha_token = init["captcha_token"]
    sent = _post("/v1/auth/verification", {
        "phone_number": username,
        "target": "ANY",
        "client_id": WEB_CLIENT_ID,
    }, {**headers, "x-captcha-token": captcha_token})
    verification_id = str(sent.get("verification_id") or "")
    if not verification_id:
        raise SmsError("发送验证码失败：未获取到 verification_id", "no_verification_id")
    # 暂存本轮的验证上下文（username/did/captcha_token/verification_id）
    _save_pending(db, {
        "username": username,
        "did": did,
        "captcha_token": captcha_token,
        "verification_id": verification_id,
    })
    return {"ok": True, "message": "验证码已发送", "masked_phone": _mask_phone(username)}


async def verify_code(db: Session, code: str) -> dict:
    """第三步+第四步：verify 验证码，然后 signin 拿 token 并落库。"""
    code = (code or "").strip()
    if not code:
        raise SmsError("请输入短信验证码", "missing_code")
    pending = _load_pending(db)
    if not pending:
        raise SmsError("没有进行中的短信登录，请先获取验证码", "no_pending")
    headers = _account_headers(pending["did"])
    verified = _post("/v1/auth/verification/verify", {
        "verification_id": pending["verification_id"],
        "verification_code": code,
        "client_id": WEB_CLIENT_ID,
    }, headers)
    verification_token = str(verified.get("verification_token") or "")
    if not verification_token:
        raise SmsError("验证码校验失败：未获取到 verification_token", "bad_code")
    signin = _post("/v1/auth/signin", {
        "verification_code": code,
        "verification_token": verification_token,
        "username": pending["username"],
        "client_id": WEB_CLIENT_ID,
    }, {**headers, "x-captcha-token": pending["captcha_token"]})
    token = str(signin.get("access_token") or "")
    if not token:
        raise SmsError("登录失败：未获取到 access_token", "no_token")
    _save_token(db, token, signin.get("refresh_token", ""), int(signin.get("expires_in", 0) or 0),
                pending["did"], pending["username"])
    _clear_pending(db)
    return {"ok": True, "message": "光鸭登录成功，凭证已加密保存",
            "expires_at": _exp_iso(int(signin.get("expires_in", 0) or 0))}


async def refresh(db: Session) -> str:
    """用 refresh_token 换取新 access_token；返回新 token。"""
    t = db.get(AuthToken, 1)
    if not t or not t.refresh_token_enc:
        raise SmsError("没有可用 Refresh Token，请重新短信登录", "no_refresh_token")
    refresh_token = decrypt_secret(t.refresh_token_enc)
    did = getattr(t, "device_id", "") or generate_did()
    data = _post("/v1/auth/token", {
        "client_id": WEB_CLIENT_ID,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }, {**_account_headers(did), "x-action": "401"})
    token = str(data.get("access_token") or "")
    if not token:
        raise SmsError("刷新失败，请重新短信登录", "refresh_failed")
    _save_token(db, token, data.get("refresh_token") or refresh_token,
                int(data.get("expires_in", 0) or 0), did, t.sub)
    return token


def status(db: Session) -> dict:
    from .utils import as_utc, mask_account, utcnow
    t = db.get(AuthToken, 1)
    flow = t.flow if t else ""
    expires = as_utc(t.expires_at) if (t and t.expires_at) else None
    now = utcnow()
    # 剩余秒数：SQLite 读回的 expires_at 是 naive UTC，必须归一化后再比较，
    # 否则与 aware 的 now 相减会抛 offset-naive/aware 错误。
    seconds_left = int((expires - now).total_seconds()) if expires else None
    has_refresh = bool(t and t.refresh_token_enc)
    logged_in = bool(t and t.access_token_enc)
    # 自动刷新：每 60 秒巡检一次，到期前 10 分钟触发；有 refresh_token 才具备能力。
    # v0.9.5：扫码登录（webqr）也在到期前自动刷新（走 guangya_auth.refresh 网页端设备码通道）
    auto_refresh = bool(logged_in and has_refresh and flow in ("sms", "tgto", "webqr"))
    needs_refresh = bool(expires and seconds_left is not None and seconds_left <= 600)
    if not logged_in:
        message = "尚未登录"
    elif needs_refresh and auto_refresh:
        message = "凭证即将到期，系统正在自动刷新"
    elif needs_refresh and not auto_refresh:
        message = "凭证即将到期，请重新登录"
    elif flow == "sms":
        message = "已登录（短信）· 自动刷新已开启"
    elif flow == "tgto":
        message = "已登录 · 自动刷新已开启"
    elif flow == "webqr":
        message = "已登录（扫码）· 自动刷新已开启"
    else:
        message = f"已授权（{flow}）"
    return {
        "logged_in": logged_in,
        "flow": flow,
        # v0.9.5：扫码登录也具备自动刷新，前端凭证卡据此显示「已开启」
        "sms_mode": flow in ("sms", "tgto", "webqr"),  # 兼容旧 tgto 通道已有 token
        "account": mask_account(t.sub) if t else "",  # v0.7.0：账号脱敏，避免手机号明文泄露
        # 带 Z 的 UTC 时间，前端 Date 解析无歧义（naive 会被当成本地时间）
        "expires_at": expires.isoformat().replace("+00:00", "Z") if expires else None,
        "expires_in_seconds": max(0, seconds_left) if seconds_left is not None else None,
        "has_refresh_token": has_refresh,
        "auto_refresh": auto_refresh,
        "needs_refresh": needs_refresh,
        "pending": bool(_load_pending(db)),
        "message": message,
    }


# ---- 内部：Token 与待验证上下文落库 ----
def _save_token(db: Session, token: str, refresh_token: str, expires_in: int, did: str, sub: str) -> None:
    t = db.get(AuthToken, 1) or AuthToken(id=1)
    t.access_token_enc = encrypt_secret(token)
    if refresh_token:
        t.refresh_token_enc = encrypt_secret(refresh_token)
    t.expires_at = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() + expires_in, tz=timezone.utc) if expires_in > 0 else None
    t.client_id = WEB_CLIENT_ID
    t.flow = "sms"
    t.sub = sub or t.sub
    t.granted_at = t.granted_at or utcnow()
    t.updated_at = utcnow()
    if hasattr(t, "device_id"):
        t.device_id = did
    db.add(t)
    db.commit()


def _save_pending(db: Session, ctx: dict) -> None:
    import json
    from .models import AppConfig
    key = "sms_pending"
    row = db.get(AppConfig, key)
    enc = encrypt_secret(json.dumps(ctx))
    if row:
        row.value_enc = enc
    else:
        db.add(AppConfig(key=key, value_enc=enc))
    db.commit()


def _load_pending(db: Session) -> dict:
    import json
    from .models import AppConfig
    row = db.get(AppConfig, "sms_pending")
    if not row or not row.value_enc:
        return {}
    try:
        return json.loads(decrypt_secret(row.value_enc))
    except Exception:
        return {}


def _clear_pending(db: Session) -> None:
    from .models import AppConfig
    row = db.get(AppConfig, "sms_pending")
    if row:
        db.delete(row)
        db.commit()


def _mask_phone(username: str) -> str:
    if len(username) < 7:
        return "***"
    return username[:3] + "****" + username[-4:]


def _exp_iso(expires_in: int) -> str | None:
    if not expires_in:
        return None
    return datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() + expires_in, tz=timezone.utc).isoformat()
