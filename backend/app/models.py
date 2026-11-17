from datetime import datetime, timezone
from sqlalchemy import String, Integer, BigInteger, Boolean, DateTime, Float, Text, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from .db import Base


def utcnow():
    return datetime.now(timezone.utc)


class Resource(Base):
    __tablename__ = "resources"
    id: Mapped[int] = mapped_column(primary_key=True)
    file_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    parent_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    name: Mapped[str] = mapped_column(String(512))
    size: Mapped[int] = mapped_column(BigInteger, default=0)
    file_type: Mapped[int] = mapped_column(Integer, default=0)
    res_type: Mapped[int] = mapped_column(Integer, default=1)
    ext: Mapped[str] = mapped_column(String(32), default="")
    ctime: Mapped[int] = mapped_column(BigInteger, default=0)
    utime: Mapped[int] = mapped_column(BigInteger, default=0)
    thumbnail: Mapped[str] = mapped_column(Text, default="")
    tmdb_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    media_type: Mapped[str] = mapped_column(String(16), default="")
    media_title: Mapped[str] = mapped_column(String(512), default="")
    original_title: Mapped[str] = mapped_column(String(512), default="")
    overview: Mapped[str] = mapped_column(Text, default="")
    poster_path: Mapped[str] = mapped_column(String(512), default="")
    genres: Mapped[str] = mapped_column(Text, default="")          # JSON 数组，中文类型名
    origin_country: Mapped[str] = mapped_column(Text, default="")  # JSON 数组，ISO 国家码
    vote_average: Mapped[float] = mapped_column(Float, default=0)
    season: Mapped[int | None] = mapped_column(Integer, nullable=True)
    episode: Mapped[int | None] = mapped_column(Integer, nullable=True)
    identify_status: Mapped[str] = mapped_column(String(24), default="unidentified")
    identify_source: Mapped[str] = mapped_column(String(24), default="")  # id/tmdb/ai/manual/none
    identify_note: Mapped[str] = mapped_column(Text, default="")
    share_id: Mapped[str] = mapped_column(String(64), default="")
    share_url: Mapped[str] = mapped_column(Text, default="")
    share_status: Mapped[str] = mapped_column(String(24), default="not_created")
    share_duration: Mapped[int] = mapped_column(Integer, default=0)
    revision: Mapped[int] = mapped_column(Integer, default=0)  # utime/size 变化次数
    scanned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ScanTask(Base):
    __tablename__ = "scan_tasks"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), default="光鸭全局扫描")
    cron: Mapped[str] = mapped_column(String(128), default="*/30 * * * *")
    root_id: Mapped[str] = mapped_column(String(64), default="")
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    last_run: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_status: Mapped[str] = mapped_column(String(24), default="never")
    last_message: Mapped[str] = mapped_column(Text, default="")


class ScanCursor(Base):
    """扫描断点：保存已访问目录、待扫栈与下一页，失败后可从断点续扫。"""
    __tablename__ = "scan_cursors"
    id: Mapped[int] = mapped_column(primary_key=True)
    root_id: Mapped[str] = mapped_column(String(64), default="")
    status: Mapped[str] = mapped_column(String(24), default="never")  # running/done/failed
    visited_json: Mapped[str] = mapped_column(Text, default="[]")
    stack_json: Mapped[str] = mapped_column(Text, default="[]")
    next_page: Mapped[int] = mapped_column(Integer, default=0)
    files_count: Mapped[int] = mapped_column(Integer, default=0)
    dirs_count: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str] = mapped_column(Text, default="")
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class AuthToken(Base):
    """当前授权用户的 Token，密文落库。单行 id=1。"""
    __tablename__ = "auth_tokens"
    id: Mapped[int] = mapped_column(primary_key=True)
    client_id: Mapped[str] = mapped_column(String(128), default="")
    access_token_enc: Mapped[str] = mapped_column(Text, default="")
    refresh_token_enc: Mapped[str] = mapped_column(Text, default="")
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    sub: Mapped[str] = mapped_column(String(128), default="")
    device_id: Mapped[str] = mapped_column(String(64), default="")
    flow: Mapped[str] = mapped_column(String(24), default="")
    granted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class AuthSession(Base):
    """进行中的 Device / PKCE 授权临时状态。单行 id=1。"""
    __tablename__ = "auth_sessions"
    id: Mapped[int] = mapped_column(primary_key=True)
    purpose: Mapped[str] = mapped_column(String(16), default="")  # device / pkce
    device_code: Mapped[str] = mapped_column(Text, default="")
    user_code: Mapped[str] = mapped_column(String(64), default="")
    verification_url: Mapped[str] = mapped_column(Text, default="")
    verification_uri_complete: Mapped[str] = mapped_column(Text, default="")
    interval: Mapped[int] = mapped_column(Integer, default=5)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    state: Mapped[str] = mapped_column(String(128), default="")
    code_verifier: Mapped[str] = mapped_column(Text, default="")
    redirect_uri: Mapped[str] = mapped_column(Text, default="")
    last_message: Mapped[str] = mapped_column(Text, default="")
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Bot(Base):
    __tablename__ = "bots"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128))
    token: Mapped[str] = mapped_column(Text)  # Fernet 密文；v0.1 明文存量可平滑迁移
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)


class Channel(Base):
    __tablename__ = "channels"
    id: Mapped[int] = mapped_column(primary_key=True)
    bot_id: Mapped[int] = mapped_column(ForeignKey("bots.id"))
    name: Mapped[str] = mapped_column(String(128))
    chat_id: Mapped[str] = mapped_column(String(128))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)


class Rule(Base):
    __tablename__ = "dispatch_rules"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128))
    directory_id: Mapped[str] = mapped_column(String(64), default="")
    keyword: Mapped[str] = mapped_column(String(256), default="")
    channel_id: Mapped[int] = mapped_column(ForeignKey("channels.id"))
    auto_publish: Mapped[bool] = mapped_column(Boolean, default=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)


class QueueItem(Base):
    __tablename__ = "push_queue"
    id: Mapped[int] = mapped_column(primary_key=True)
    resource_id: Mapped[int] = mapped_column(ForeignKey("resources.id"), index=True)
    channel_id: Mapped[int] = mapped_column(ForeignKey("channels.id"))
    status: Mapped[str] = mapped_column(String(24), default="pending", index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str] = mapped_column(Text, default="")
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    telegram_message_id: Mapped[str] = mapped_column(String(64), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    __table_args__ = (UniqueConstraint("resource_id", "channel_id", name="uq_resource_channel"),)


class MessageTemplate(Base):
    __tablename__ = "message_templates"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128))
    content: Mapped[str] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class AppConfig(Base):
    """运行时凭证配置（key -> 密文值）。页面「一键连接」写入，优先于 .env。"""
    __tablename__ = "app_config"
    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value_enc: Mapped[str] = mapped_column(Text, default="")
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=utcnow)


class AuditLog(Base):
    __tablename__ = "audit_logs"
    id: Mapped[int] = mapped_column(primary_key=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    action: Mapped[str] = mapped_column(String(64), index=True)
    target_type: Mapped[str] = mapped_column(String(48), default="")
    target_id: Mapped[str] = mapped_column(String(64), default="")
    detail: Mapped[str] = mapped_column(Text, default="")
    actor: Mapped[str] = mapped_column(String(48), default="admin")
