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
    "tmdb_api_key": ("tmdb_api_key", ""),
    "tmdb_bearer_token": ("tmdb_bearer_token", ""),
    "tmdb_language": ("tmdb_language", "zh-CN"),
    # AI 辅助识别：OpenAI 兼容接口，用户可自定义服务端与模型
    "ai_enabled": ("ai_enabled", "false"),
    "ai_base_url": ("ai_base_url", "https://api.openai.com/v1"),
    "ai_api_key": ("ai_api_key", ""),
    "ai_model": ("ai_model", "gpt-4o-mini"),
    "ai_prompt": ("ai_prompt", ""),
    "ai_timeout": ("ai_timeout", "30"),
    # Telegram API 反代地址：国内网络直连 api.telegram.org 常超时，允许管理页配置
    "telegram_api_base": ("telegram_api_base", "https://api.telegram.org"),
    # v0.6.4：队列推送调度。interval 秒数优先；cron 非空时覆盖（CronTrigger 5 段）
    "queue_interval_seconds": ("queue_interval_seconds", "30"),
    "queue_cron": ("queue_cron", ""),
    # v0.6.5：每条 TG 消息发送后的节流秒数，避免短时间连发触发 Telegram 风控
    "telegram_send_delay": ("telegram_send_delay", "2"),
    # v0.7.0：TG 分发配置升级
    "tg_push_mode": ("tg_push_mode", "channel"),          # channel/bot_channel/custom_bot_group
    "tg_custom_bot_token": ("tg_custom_bot_token", ""),   # 自定义 bot 推送的 token
    "tg_custom_chat_id": ("tg_custom_chat_id", ""),       # 自定义 bot 推送的群聊
    "tg_fubao_bot": ("tg_fubao_bot", ""),                  # 福袋 bot 用户名（跳转入口）
    "tg_link_style": ("tg_link_style", "button"),         # button/plain：分享链接按钮化
    "tg_detail_level": ("tg_detail_level", "compact"),    # compact/full：精简详情
    "reward_code": ("reward_code", ""),                    # 赞赏码（图片 URL 或文本）
    # v0.8.0：TgtoDrive OpenAPI（独立外部系统，只读配置+关键词合并，不集成源码/数据库）
    "tgto_base_url": ("tgto_base_url", ""),
    "tgto_api_key": ("tgto_api_key", ""),
    "tgto_scene": ("tgto_scene", "ENV_FILTER_GUANGYA"),
    # v0.8.0：用户互动 / 打赏
    "contribution_enabled": ("contribution_enabled", "false"),
    "contribution_bot_token": ("contribution_bot_token", ""),
    "contribution_update_offset": ("contribution_update_offset", "0"),
    "donation_enabled": ("donation_enabled", "false"),
    "donation_image_url": ("donation_image_url", ""),
    "donation_text": ("donation_text", ""),
    "donation_button_text": ("donation_button_text", "打赏支持"),
}


def telegram_base(db: Session) -> str:
    """Telegram Bot API 根地址：管理页配置优先、.env 兜底；main 与 publisher 共用。"""
    return (get(db, "telegram_api_base") or settings.telegram_api_base
            or "https://api.telegram.org").rstrip("/")


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
