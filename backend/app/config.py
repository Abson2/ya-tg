from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # 管理端
    admin_password: str = "CHANGE_ME"
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

    # 光鸭网页端分享接口（抓包适配，与 OpenAPI 认证无关，不在公开 OpenAPI 文档内）
    guangya_share_authorization: str = ""
    guangya_share_did: str = ""
    guangya_share_dt: str = "4"
    guangya_share_smid: str = ""
    guangya_share_base: str = "https://api.guangyapan.com"

    # TgtoDrive 光鸭认证网关（可选）：管理页手机号短信登录，自动获取并定期刷新光鸭凭证。
    # 适合普通用户零配置；不填则保留 Device/PKCE 直连授权方式。
    tgto_base_url: str = "http://host.docker.internal:12366"
    tgto_api_token: str = ""

    # TMDB。v3 api_key 或 v4 read access token 二选一，优先 bearer。
    tmdb_api_key: str = ""
    tmdb_bearer_token: str = ""
    tmdb_language: str = "zh-CN"

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
