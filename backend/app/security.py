import base64
import hashlib
import logging
from cryptography.fernet import Fernet, InvalidToken

from .config import settings

logger = logging.getLogger(__name__)


def secret_configured() -> bool:
    value = (settings.app_secret or "").strip()
    return bool(value) and value != "CHANGE_ME" and "请替换" not in value and "请设置" not in value


def _cipher() -> Fernet:
    if not secret_configured():
        raise RuntimeError("请先在 .env 设置强随机 APP_SECRET（至少 32 位），用于凭证加密")
    key = base64.urlsafe_b64encode(hashlib.sha256(settings.app_secret.encode("utf-8")).digest())
    return Fernet(key)


def encrypt_secret(value: str) -> str:
    if not value:
        return ""
    return _cipher().encrypt(value.encode("utf-8")).decode("ascii")


def decrypt_secret(value: str) -> str:
    if not value:
        return ""
    try:
        return _cipher().decrypt(value.encode("ascii")).decode("utf-8")
    except InvalidToken:
        # 兼容 v0.1.0 明文存量数据：解密失败时按明文返回，写入时即被加密（行为不变）。
        # B-11：此前完全静默——APP_SECRET 变更/密文损坏时把密文当明文用会隐蔽鉴权失败，
        # 现在记 warning 告警（只记长度不记内容，绝不泄露凭证片段），便于排查根因。
        logger.warning("[security] 凭证解密失败（InvalidToken），按明文回退："
                       "若为 APP_SECRET 变更或密文损坏请核对；长度=%s", len(value))
        return value


def mask_secret(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}{'*' * 6}{value[-4:]}"
