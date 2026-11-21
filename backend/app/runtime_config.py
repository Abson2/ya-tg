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
    # v0.9.19：投稿转存——审核通过后直接调光鸭原生 API 转存到指定目录，不经 TgtoDrive。
    # 去掉 v0.9.18 的中转群/OpenAPI 分发与旧「投稿转存监控 Bot chat_id」（Bot→Bot 私聊被
    # Telegram USER_BOT_TO_BOT_DISABLED 拒绝，是绕路链路）。
    "contribution_restore_enabled": ("contribution_restore_enabled", "false"),
    "contribution_restore_target_dir_id": ("contribution_restore_target_dir_id", ""),
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
    # 四模块重构：分享中心主链开关。v1.1 起默认 true=分享中心真正接管主链
    # （建分享→提交→原子领取→只读链接发送→回执）；设 false 可临时回退旧链排障。
    "share_center_enabled": ("share_center_enabled", "true"),
    # 分享中心 v2（全新数据体系）：true=使用 sc_* 新表与四级计划引擎；
    # 用户口径：旧库数据不用管，新模块上线后全部使用新数据。
    "share_center_v2_enabled": ("share_center_v2_enabled", "true"),
    # AI 调度建议默认只进审核队列；显式打开后才允许自动应用，且永远不直接推送。
    "sc_ai_auto_apply": ("sc_ai_auto_apply", "false"),
    # 阶段二（资源库解耦）灰度开关：true=识别入口只发资源事件（携带 channel_targets），
    # 推送决策完全由分享中心 consume_resource_events→submit_to_dispatch 新链接管；
    # false（默认）=识别入口仍走 _enqueue_after_identify 旧链直连入队，零回归。
    # 开启前须确保新链全量测试通过，且 share_center_enabled=true。
    "resource_decoupled_enabled": ("resource_decoupled_enabled", "false"),
    # 阶段六（分享中心全重做）全局急停：true=run_cycle 直接空转，不发任何新分享。
    # 保守默认 false；只拦「新提交」，不撤销已在途任务，也不动分发中心发送核心。
    "publish_paused": ("publish_paused", "false"),
    # v1.5.1 P0：building 中间态超时自愈阈值（分钟）。build_share 先 commit building
    # 落盘，网络/锁异常后 rollback 救不回已提交状态；候选查询又不扫 building → 永久僵尸。
    # run_cycle 每轮把 building_at 早于 now-N 分钟的任务重置 planned 重进候选。默认5分钟。
    "share_building_timeout_minutes": ("share_building_timeout_minutes", "5"),
    # 阶段五（链接校验与清理，用户决策1）：有效性=本地元数据+删除事件驱动，非光鸭实时状态。
    # link_inactive_confirm_count：连续失效信号达此次数才可能判 inactive（默认2）；
    # link_inactive_min_minutes：两次失效信号最小间隔分钟（默认30），防抖动误判；
    # auto_cleanup_enabled：自动清理默认关，只提供手动清理端点。
    "link_inactive_confirm_count": ("link_inactive_confirm_count", "2"),
    "link_inactive_min_minutes": ("link_inactive_min_minutes", "30"),
    "auto_cleanup_enabled": ("auto_cleanup_enabled", "false"),
    # 发布配额（设计文档 §4.6）：三队列每小时/每日上限，后台可配，0=不限。
    "share_daily_limit": ("share_daily_limit", "0"),
    "push_daily_limit": ("push_daily_limit", "0"),
    "stock_daily_quota": ("stock_daily_quota", "0"),
    "fresh_daily_quota": ("fresh_daily_quota", "0"),
    "followup_daily_quota": ("followup_daily_quota", "0"),
    # v1.2 发布节奏（设计文档 §5.4/§10）：相邻分享任务的最小释放间隔秒数，0=不限。
    # run_cycle 用它在同轮多个任务之间铺开 scheduled_at，避免瞬时并发推送。
    "share_release_interval_seconds": ("share_release_interval_seconds", "0"),
    # v1.5.1 P0 第七次：TG 发送循环单条网络耗时。旧版硬编码 timeout=20s，
    # sendPhoto 失败再回退 sendMessage 一次（+20s），单条最坏 40s；逐条串行 +
    # 全局 _queue_lock 把一轮拖到数分钟，队列看似停摆。改为可配置：HTTP 超时默认
    # 8s，整条 _send_telegram 硬上限默认 12s（wait_for 截断回退），仍退避重试。
    "telegram_http_timeout_seconds": ("telegram_http_timeout_seconds", "8"),
    "telegram_send_hard_limit_seconds": ("telegram_send_hard_limit_seconds", "12"),
    # v1.4 改名提速：改名 idle 与复制 idle 解耦（下限 0.2）。
    # v1.4.1：默认从 0.3 上调到 0.5（≈6 req/s 配合并发3），给光鸭风控留余量；
    # 并发度仍保持 3（不消极降到 1），限流靠 127 专用指数退避+自动重试根治。
    # 遇极端风控可把并发度调 1 退回串行，但默认不必牺牲吞吐。
    "rename_idle_seconds": ("rename_idle_seconds", "0.5"),
    "rename_concurrency": ("rename_concurrency", "3"),
    # 镜像1.1（查漏补缺）：单轮开始做 A树/B树/映射清单三方对比预扫描。
    # prepass_enabled 默认开（核对+统计，C类删 B 库由 mirror_mode 决定）；
    # prepass_max_media 预扫描单轮最多核对多少部作品（性能护栏，防接口风暴）。
    "mirror_prepass_enabled": ("mirror_prepass_enabled", "true"),
    "mirror_prepass_max_media": ("mirror_prepass_max_media", "50"),
    # 镜像模式（用户口径 2026-11-21，默认全镜像）：
    #   full（全镜像，默认）= A 库已删的文件/作品 → 预扫描自动删 B 库对应文件 + 清映射清单，
    #                          保持 A/B 一致；仅 A 树完整时才删（complete 闸门）。
    #   half（半镜像）       = A 库已删 → B 库文件与映射清单都原样保留，B 是 A 的超集，
    #                          完全不动。
    # 边界：A 库只读（任何模式都不写 A）。旧键 mirror_delete_b_on_a_missing 保留兼容读取，
    # 但以 mirror_mode 为准；full 即等价于"允许删 B 库"。
    "mirror_mode": ("mirror_mode", "full"),
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
    """阶段三 D-03：一次批量取所有 key，{} 来源优先级与 get() 完全一致。"""
    if not keys:
        return {}
    rows = {r.key: r for r in db.scalars(
        select(AppConfig).where(AppConfig.key.in_(list(set(keys))))).all()}
    out: dict[str, str] = {}
    for k in keys:
        row = rows.get(k)
        value = ""
        if row and row.value_enc:
            try:
                value = decrypt_secret(row.value_enc) or ""
            except Exception:
                value = ""
        out[k] = value if value else _env_value(k)
    return out


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
    """每个 key 的来源（db/env/empty），供前端展示，不暴露明文。

    阶段三 D-03：批量取 AppConfig，避免逐 key db.get（~80 key → 1 次查询）。
    """
    rows = {r.key: r for r in db.scalars(
        select(AppConfig).where(AppConfig.key.in_(list(DEFAULTS)))).all()}
    result = {}
    for key in DEFAULTS:
        row = rows.get(key)
        try:
            from_db = bool(row and row.value_enc and decrypt_secret(row.value_enc))
        except Exception:
            from_db = False
        if from_db:
            result[key] = "db"
        elif _env_value(key):
            result[key] = "env"
        else:
            result[key] = "empty"
    return result


def masked(db: Session, keys: list[str] | None = None) -> dict[str, str]:
    keys = keys or list(DEFAULTS)
    return {k: mask_secret(get(db, k)) for k in keys}
