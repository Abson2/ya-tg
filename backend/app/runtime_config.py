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
from .security import decrypt_secret, encrypt_secret, is_masked_secret, mask_secret

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
    # v2.1.33：手动搜索独立档。原来后台手动搜索落 scan 档，和批量识别抢同一份
    # 配额 —— 批量跑起来时管理员点一次搜索就被拒。人手动点的频率天然很低，
    # 这里给 1 秒 / 30 次：够点，又不会真把 TMDB 打穿。
    # 不照搬 Bot 档的 3 秒 / 20 次：那个数字是防脚本刷的，用在后台自己人身上
    # 就变成「每搜一次干等 3 秒」。
    "tmdb_manual_min_interval": ("tmdb_manual_min_interval", "1"),  # 后台手动搜索：最小间隔秒
    "tmdb_manual_max_calls": ("tmdb_manual_max_calls", "30"),       # 后台手动搜索：每窗口 30 次
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
    # 分享中心总开关：true=分享中心接管主链（建分享→提交→原子领取→只读链接发送→回执）。
    # v2.1.1 用户口径「前后端仅用一套最新重构后的分享中心」：本键成为**唯一**总开关，
    # 原 share_center_v2_enabled 已并入本键（读它作为兼容回退），对外不再区分版本。
    "share_center_enabled": ("share_center_enabled", "true"),
    # 兼容保留：旧版本写入的 v2 开关键。读取时以 share_center_enabled 为准，
    # 此键仅作回退兜底，新代码不再单独使用它做分支判断。
    "share_center_v2_enabled": ("share_center_v2_enabled", "true"),
    # AI 调度建议默认只进审核队列；显式打开后才允许自动应用，且永远不直接推送。
    "sc_ai_auto_apply": ("sc_ai_auto_apply", "false"),
    # v2.0.5：分享中心「基础规则」——上新即时推送。更新检测频率合并窗口（分钟）、
    # 上新即时推送每小时上限（条，0=不限）、冲突处理策略。
    # v2.1.6：冲突处理由两档（pause_normal/prefer_new，语义重复且后端从未消费）
    # 改为三档**互斥且真实生效**的口径：
    #   prefer_release —— 新上映优先：同一小时内同时有「新上架」与「存量排期」待发时，
    #                    先发新上架（release_date 落在近期窗口内）的作品；
    #   prefer_new     —— 新入库优先：先发刚入库（scanned_at 最新）的作品；
    #   prefer_backlog —— 存量排期优先：先发已排期的存量作品，新入库的排到后面。
    # 三者决定的是**同批次内的先后顺序**，不改变任何配额上限。
    "sc_quick_publish_enabled": ("sc_quick_publish_enabled", "true"),
    "sc_update_detect_minutes": ("sc_update_detect_minutes", "10"),
    "sc_quick_publish_hourly_limit": ("sc_quick_publish_hourly_limit", "3"),
    "sc_conflict_policy": ("sc_conflict_policy", "prefer_release"),
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
    # v2.1.41（真机反馈，全量镜像的命门）：A 树单轮最多列多少个目录。
    # 老代码把 400 写死在 `_walk_source` 默认值里，撞上限时还照旧报「完整」→
    # 线上 A 库实测 ≥1436 个目录，每轮只看到约 28%，七成作品永远轮不到
    # （62T 的库只镜像出 11T）。默认 **0 = 不限**，把全树走完；
    # 想限制单轮耗时再调成具体数字，被截断时会在同步结果里明确告警且停用清理判定。
    "mirror_walk_max_dirs": ("mirror_walk_max_dirs", "0"),
    # 列目录并发度（1 = 串行）。全量走完 1400+ 目录，串行要十几分钟，并发 3 是
    # 实测 0 失败的量级；遇到 code=127 靠退避重试根治，不靠把并发降到 1。
    "mirror_walk_concurrency": ("mirror_walk_concurrency", "3"),
    # 镜像模式（用户口径 2026-11-21，默认全镜像）：
    #   full（全镜像，默认）= A 库已删的文件/作品 → 预扫描自动删 B 库对应文件 + 清映射清单，
    #                          保持 A/B 一致；仅 A 树完整时才删（complete 闸门）。
    #   half（半镜像）       = A 库已删 → B 库文件与映射清单都原样保留，B 是 A 的超集，
    #                          完全不动。
    # 边界：A 库只读（任何模式都不写 A）。旧键 mirror_delete_b_on_a_missing 保留兼容读取，
    # 但以 mirror_mode 为准；full 即等价于"允许删 B 库"。
    "mirror_mode": ("mirror_mode", "full"),
    # ------------------------------------------------------------------------
    # v2.1.7 #241：Telegram 阅读量采集（MTProto user account）
    # Bot API 拿不到消息浏览量，只有 MTProto 的 Message.views 才有 → 必须用户账号接入。
    # 凭证走本表（Fernet 加密落库 + 管理页配置），明文**绝不**回前端。
    # api_id 是 my.telegram.org 申请的数字；api_hash 是 32 位十六进制。
    # 登录验证码是**一次性**的，只在登录流程内存活，不落库。
    # ------------------------------------------------------------------------
    "tg_stats_enabled": ("tg_stats_enabled", "false"),
    "tg_api_id": ("tg_api_id", ""),
    "tg_api_hash": ("tg_api_hash", ""),
    "tg_phone": ("tg_phone", ""),
    # 会话文件目录（会话文件等同账号凭据，只落本地，绝不通过接口返回）
    "tg_session_dir": ("tg_session_dir", "./data/tg_session"),
    # 采集回溯窗口（小时）与单轮上限（防接口风暴护栏）
    "tg_stats_lookback_hours": ("tg_stats_lookback_hours", "72"),
    "tg_stats_max_per_run": ("tg_stats_max_per_run", "200"),
    # 定时采集间隔（小时）。阅读量是累计值，趋势必须靠周期采集才能形成，
    # 只靠手动点按钮永远只有一个数据点。0 = 关闭定时采集（仅手动触发）。
    "tg_stats_collect_interval_hours": ("tg_stats_collect_interval_hours", "0"),
    # MTProto 接入点（默认官方；支持自建 DC）
    "tg_api_base": ("tg_api_base", ""),
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


def secret_state(db: Session, keys: list[str]) -> dict[str, dict[str, bool]]:
    """凭证类 key 的**状态**——只回状态，绝不回显任何片段。

    v2.1.16 事故：此前配置页 GET 用 masked() 把掩码（形如 tgto******4ifI）当字段值返回，
    前端只判「非空」就提交，掩码被 set_many 加密写库，TgtoDrive OpenAPI 从此恒 401。

    现在 GET 一律返回空值 + 这两个标志：
      configured  —— 库里/环境里是**真凭证**，可直接使用
      needs_reset —— 存的是掩码（已被污染），必须由人工重新填写真值才能恢复
    """
    values = get_many(db, keys)
    out: dict[str, dict[str, bool]] = {}
    for key in keys:
        value = (values.get(key) or "").strip()
        polluted = is_masked_secret(value)
        out[key] = {"configured": bool(value) and not polluted, "needs_reset": polluted}
    return out
