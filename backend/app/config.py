from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # 管理端
    admin_password: str = "CHANGE_ME"
    # v0.9.20：管理后台未活跃有效期（分钟），超过后会话失效需重新输密码；范围 1~1440
    admin_idle_minutes: int = 120
    app_secret: str = "CHANGE_ME"
    database_url: str = "sqlite:///./app.db"

    # 光鸭开放平台（OpenAPI，业务接口 x-client-id + Bearer）
    guangya_client_id: str = ""
    guangya_access_token: str = ""      # 兼容旧配置：仅作为首次导入的种子，不推荐长期手工填写
    guangya_refresh_token: str = ""     # 同上
    guangya_project_id: str = ""        # Device 申请设备码必需
    guangya_sign_secret: str = ""       # 仅后端持有，仅用于 /v1/auth/device/code 签名
    guangya_device_id: str = "guangya-tg-publisher"
    guangya_oauth_redirect_uri: str = ""  # Web PKCE 回调，须与平台登记一致
    guangya_api_base: str = "https://openapi.guangyapan.com"
    guangya_account_base: str = "https://openapi-account.guangyapan.com"
    guangya_www_base: str = "https://www.guangyapan.com"
    guangya_timeout: float = 20.0
    # v0.9.2：网页端扫码登录（与官方 OpenAPI Device 不同通道，无需 project_id/sign_secret）。
    # 端点取自光鸭官方 Skill 1.0.3 的网页端设备码协议；client_id 为网页应用公开 ID，非隐私。
    guangya_webqr_account_base: str = "https://account.guangyapan.com"
    guangya_webqr_client_id: str = "atcFkeidp8ndm9yC"

    # 光鸭网页端分享接口（抓包适配，与 OpenAPI 认证无关，不在公开 OpenAPI 文档内）
    guangya_share_authorization: str = ""
    guangya_share_did: str = ""
    guangya_share_dt: str = "4"
    guangya_share_smid: str = ""
    guangya_share_base: str = "https://api.guangyapan.com"

    # TMDB。v3 api_key 或 v4 read access token 二选一，优先 bearer。
    tmdb_api_key: str = ""
    tmdb_bearer_token: str = ""
    tmdb_language: str = "zh-CN"

    # AI 辅助识别（OpenAI 兼容接口，可选；推荐在管理页配置，不用改 .env）
    ai_enabled: bool = False
    ai_base_url: str = "https://api.openai.com/v1"
    ai_api_key: str = ""
    ai_model: str = "gpt-4o-mini"
    ai_prompt: str = ""
    ai_timeout: float = 30.0

    # 分发策略
    auto_create_share: bool = True
    share_duration: int = 0            # 0=永久 / 86400 / 604800 / 2592000
    queue_backoff_base: int = 60       # 秒
    queue_backoff_max: int = 3600
    telegram_api_base: str = "https://api.telegram.org"

    scan_cron: str = "*/30 * * * *"
    scan_root_id: str = ""


settings = Settings()

VALID_DURATIONS = {0, 86400, 604800, 2592000}
