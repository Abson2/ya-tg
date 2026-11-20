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
    # v0.9.8：TMDB 按调用场景独立限流。bot 场景默认更保守（间隔久、频次低），
    # 防止 Bot 被恶意刷求片/投稿导致 TMDB 账号触发风控；scan 场景供内部识别使用。
    "tmdb_scan_min_interval": ("tmdb_scan_min_interval", "0.2"),  # 资源库识别：最小间隔秒
    "tmdb_scan_max_calls": ("tmdb_scan_max_calls", "60"),         # 每窗口最大调用数
    "tmdb_bot_min_interval": ("tmdb_bot_min_interval", "3"),      # Bot 场景：默认间隔 3 秒
    "tmdb_bot_max_calls": ("tmdb_bot_max_calls", "20"),           # Bot 场景：每窗口 20 次
    "tmdb_rate_window_seconds": ("tmdb_rate_window_seconds", "60"),  # 频次统计窗口
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
    # v0.9.13 #254：求片审核通过后写入白名单时的三档规则字段（真机取值：
    # resolution=4k/all，effect=all/dolby/hdr/sdr，required_keyword 小写英文串）。
    "tgto_rule_resolution": ("tgto_rule_resolution", "4k"),       # 4k/1080p/720p
    "tgto_rule_effect": ("tgto_rule_effect", "all"),              # all/dolby/hdr/sdr
    "tgto_rule_required_keyword": ("tgto_rule_required_keyword", "265"),  # 必含关键词
    # 投稿审核通过后把分享链接转发到该 TgtoDrive 绑定 Bot（chat_id），由其监控转存
    "tgto_contribute_bot_chat_id": ("tgto_contribute_bot_chat_id", ""),
    # v0.9.18：投稿分发（审核态/分发态解耦）。mode: relay(中转群)/openapi/off。
    # relay 用交互 Bot 发消息到中转群（群里有 TGtoDrive 个人账号 + 万能转发），
    # 绝不 Bot→Bot 私聊（Telegram USER_BOT_TO_BOT_DISABLED 根因）。
    "contribution_dispatch_mode": ("contribution_dispatch_mode", "relay"),
    "contribution_relay_chat_id": ("contribution_relay_chat_id", ""),  # 专用中转群 chat_id
    # 中转消息模板：仅必要链接信息，不含投稿人隐私。占位符 {ref}/{title}/{share_url}/{code}
    "contribution_relay_template": ("contribution_relay_template", ""),
    # v0.8.0：用户互动 / 打赏
    "contribution_enabled": ("contribution_enabled", "false"),
    "contribution_bot_token": ("contribution_bot_token", ""),  # v0.9.4 起仅作旧配置回退
    "interaction_bot_id": ("interaction_bot_id", ""),  # v0.9.4：交互 Bot 直接引用 bots 表 id（token 去重）
    # v0.9.15 #276：交互 Bot 的用户名（getMe 成功后自动缓存；手动填写作兜底/覆盖）。
    # 用于生成 https://t.me/<username>?startgroup=<payload> 官方授权链接，绝不存 token。
    "interaction_bot_username": ("interaction_bot_username", ""),
    "contribution_update_offset": ("contribution_update_offset", "0"),
    "donation_enabled": ("donation_enabled", "false"),
    "donation_image_url": ("donation_image_url", ""),
    "donation_text": ("donation_text", ""),
    "donation_button_text": ("donation_button_text", "打赏支持"),
    # v0.8.3：日志保留策略（管理页配置，不新增 .env 项）
    "log_retention_days": ("log_retention_days", "30"),
    "log_max_rows": ("log_max_rows", "5000"),
    "log_auto_prune": ("log_auto_prune", "true"),
    # v0.9.0：Bot 交互与推送白名单。内置指令固定四项（求片/投稿/搜索/打赏），
    # 其余为管理员后台自定义的指令名→说明。白名单只放行 Bot 自己管理的频道/群聊，
    # 绝不向用户私聊推送，避免刷屏。
    "bot_commands_json": ("bot_commands_json", "[]"),       # 自定义指令 [{command,description}]
    # v0.9.9 #207：已自动清空过 "/" 指令栏的 bot token 指纹（逗号分隔），每 token 只清一次
    "bot_commands_cleared_fp": ("bot_commands_cleared_fp", ""),
    "bot_push_channels_json": ("bot_push_channels_json", "[]"),  # 白名单频道 ID（需管理员）
    "bot_push_groups_json": ("bot_push_groups_json", "[]"),      # 白名单群聊 ID（无需管理员）
    "bot_restrict_enabled": ("bot_restrict_enabled", "false"),   # 使用限制开关
    "bot_restrict_cache_seconds": ("bot_restrict_cache_seconds", "120"),
    "bot_required_channels_json": ("bot_required_channels_json", "[]"),  # 必须关注频道
    "bot_required_groups_json": ("bot_required_groups_json", "[]"),      # 必须加入群聊
    # v0.9.2：使用限制与推送白名单二选一。
    # whitelist（默认）= 只按白名单过滤推送目标；
    # membership = 只校验用户关注/加群，不按白名单过滤；
    # off = 两者都不启用。空值时兼容旧 bot_restrict_enabled=true → membership。
    "bot_gate_mode": ("bot_gate_mode", ""),
    # v0.9.5：推送白名单改为「目录路由表」——不同 B 库目录经同一 Bot 推到不同频道/群。
    # 每条 {dir_id, kind(channel|group), chat_id}；默认 1 条、最多 10 条。
    "push_routes_json": ("push_routes_json", "[]"),
    # v0.9.5：使用限制跳转链接（用户未关注频道/未加群时推给他的 t.me/@ 链接）。
    "bot_required_channel_link": ("bot_required_channel_link", ""),
    "bot_required_group_link": ("bot_required_group_link", ""),
    # v0.9.5：推送消息里的频道/群跳转按钮（与打赏配置放一起，模板变量 channel_link/group_link）。
    "tg_channel_link": ("tg_channel_link", ""),
    "tg_group_link": ("tg_group_link", ""),
    # v0.9.10：推送消息正文里的机器人跳转文字链接（指向推送 Bot 本身），留空则不显示
    "tg_bot_link": ("tg_bot_link", ""),
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
