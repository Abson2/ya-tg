"""运行时凭证配置层。

页面「一键连接光鸭」注入的凭证加密落库到 app_config；所有业务模块通过本模块读取，
优先级：数据库（页面配置） > .env 环境变量（镜像内置/手工配置）。

这样普通用户只用管理页短信登录即可完成全部配置，无需编辑 .env；
开发者仍可用 .env 预置（适合镜像内置公共应用凭证）。
"""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from .config import settings
from .models import AppConfig
from .security import decrypt_secret, encrypt_secret, mask_secret

# key -> (settings 属性名, 默认值)。数据库有值（非空）时覆盖环境变量。
DEFAULTS: dict[str, tuple[str, str]] = {
    "guangya_client_id": ("guangya_client_id", ""),
    "guangya_project_id": ("guangya_project_id", ""),
    "guangya_sign_secret": ("guangya_sign_secret", ""),
    "guangya_device_id": ("guangya_device_id", "ya-tg"),
    "guangya_share_authorization": ("guangya_share_authorization", ""),
    "guangya_share_did": ("guangya_share_did", ""),
    "guangya_share_dt": ("guangya_share_dt", "4"),
    "guangya_share_smid": ("guangya_share_smid", ""),
}


def _env_value(key: str) -> str:
    attr, default = DEFAULTS.get(key, (key, ""))
    return str(getattr(settings, attr, default) or default)


def get(db: Session, key: str) -> str:
    """读取单个凭证：数据库优先，回退环境变量。"""
    row = db.get(AppConfig, key)
    if row and row.value_enc:
        try:
            value = decrypt_secret(row.value_enc)
            if value:
                return value
        except Exception:
            pass
    return _env_value(key)


def get_many(db: Session, keys: list[str]) -> dict[str, str]:
    return {k: get(db, k) for k in keys}


def set_many(db: Session, values: dict[str, str]) -> None:
    """写入凭证。空字符串表示清空该 key（删除数据库覆盖，回退环境变量）。"""
    for key, value in values.items():
        if key not in DEFAULTS:
            continue
        value = (value or "").strip()
        row = db.get(AppConfig, key)
        if not value:
            if row:
                db.delete(row)
            continue
        enc = encrypt_secret(value)
        if row:
            row.value_enc = enc
        else:
            db.add(AppConfig(key=key, value_enc=enc))
    db.flush()


def source_map(db: Session) -> dict[str, str]:
    """每个 key 的来源（db/env/empty），供前端展示，不暴露明文。"""
    result = {}
    for key in DEFAULTS:
        row = db.get(AppConfig, key)
        if row and row.value_enc and decrypt_secret(row.value_enc):
            result[key] = "db"
        elif _env_value(key):
            result[key] = "env"
        else:
            result[key] = "empty"
    return result


def masked(db: Session, keys: list[str] | None = None) -> dict[str, str]:
    keys = keys or list(DEFAULTS)
    return {k: mask_secret(get(db, k)) for k in keys}
