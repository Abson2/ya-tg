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
    # v0.6.1：扫描时记录的目录显示名链，如「谷雨 (2026)(1)/Season 1」。
    # 剧集单集文件名往往是英文译名，作品名要靠父目录，识别必须带上它。
    path_hint: Mapped[str] = mapped_column(Text, default="")
    # v0.6.3：从直接父目录往上到根的 fileId 链，如 ["f_s1","f_show","f_root"]。
    # 用于定位剧集目录：未整理=父目录，已整理(剧名/Season N/单集)=上上层。
    dir_chain_json: Mapped[str] = mapped_column(Text, default="[]")
    # v0.6.3：分享粒度与目标目录。file=单集；series_dir=整剧/季目录。
    share_scope: Mapped[str] = mapped_column(String(16), default="file")
    share_dir_id: Mapped[str] = mapped_column(String(64), default="")
    share_dir_name: Mapped[str] = mapped_column(String(256), default="")
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
    # v0.7.0：完结/追更状态（来自 TMDB status），用于资源库角标与一级状态栏筛选
    finish_status: Mapped[str] = mapped_column(String(16), default="")  # ended/returning/""
    # v0.8.0：用户在资源库手动修正完结/追更后锁定，禁止 TMDB 自动回填覆盖人工判断
    finish_locked: Mapped[bool] = mapped_column(Boolean, default=False)
    # v0.8.9：TMDB 官方总集数（tv.number_of_episodes），供推送模板「更新至 N/总 M 集」使用。
    # 与本地实际收录集数（模板 episode_count）刻意分开：追更剧两者不等才有意义。
    total_episodes: Mapped[int] = mapped_column(Integer, default=0)
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
    # v0.6.1：目录 fileId -> 显示名路径，断点续扫后仍能给资源补上目录线索
    dir_paths_json: Mapped[str] = mapped_column(Text, default="{}")
    # v0.6.3：目录 fileId -> 到根的 fileId 链，断点续扫后仍能定位剧集目录层级
    dir_chains_json: Mapped[str] = mapped_column(Text, default="{}")
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
    # v0.9.4：频道 / 群聊统一为一条推送目标；kind 只用于菜单分类展示，推送逻辑不区分。
    kind: Mapped[str] = mapped_column(String(16), default="channel")


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


# ============================================================================
# v0.8.0：A/B 镜像同步 + 扫描批次 + 内容变化 + 用户互动
# 设计原则（最终版需求文档）：
#   - A 源头库只读，B 镜像库才进入扫描/资源库/分享/TG 分发。
#   - A/B fileId 不同，必须落映射表，禁止把 A 的 dir_chain/share_dir_id 搬到 B。
#   - 身份以 TMDB ID + media_type + season + episode 为准，身份不明确禁止替换。
#   - 镜像完成不得自动触发 TG 推送，业务状态独立保存。
# ============================================================================

class ResourceScanBatch(Base):
    """全量扫描批次。只有 complete=1 且非断点续扫的批次才允许做对账删除（12.2）。"""
    __tablename__ = "resource_scan_batches"
    id: Mapped[int] = mapped_column(primary_key=True)
    root_id: Mapped[str] = mapped_column(String(64), default="")
    status: Mapped[str] = mapped_column(String(24), default="running")  # running/success/failed
    files: Mapped[int] = mapped_column(Integer, default=0)
    directories: Mapped[int] = mapped_column(Integer, default=0)
    seen_count: Mapped[int] = mapped_column(Integer, default=0)
    complete: Mapped[bool] = mapped_column(Boolean, default=False)
    resumed: Mapped[bool] = mapped_column(Boolean, default=False)
    reconcile_allowed: Mapped[bool] = mapped_column(Boolean, default=False)
    seen_json: Mapped[str] = mapped_column(Text, default="[]")
    error: Mapped[str] = mapped_column(Text, default="")
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class MirrorRule(Base):
    """一条 A→B 镜像规则。A 只读，B 用于后续全部分发链路。"""
    __tablename__ = "mirror_rules"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), default="默认镜像")
    source_root_id: Mapped[str] = mapped_column(String(64), default="")   # A 源头库目录
    target_root_id: Mapped[str] = mapped_column(String(64), default="")   # B 镜像库目录
    interval_minutes: Mapped[int] = mapped_column(Integer, default=60)    # 按钮式间隔，非手写 Cron
    enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    star_naming: Mapped[bool] = mapped_column(Boolean, default=True)      # B 库主目录插入 · 的约定
    auto_identify: Mapped[bool] = mapped_column(Boolean, default=True)    # 镜像后自动识别（不推送）
    # v0.8.1：每次文件复制操作之间的空闲秒数，可在管理页调高；硬下限由 mirror.py 强制，
    # 依据官方 OpenAPI v1.3 §10 写接口 2 次/秒，用户不可调低。
    file_idle_seconds: Mapped[float] = mapped_column(Float, default=1.0)
    # v0.8.1：B 库缺目录时自动按 1:1 结构创建（create_dir 接口已抓包确认）。
    # 关闭后退回旧行为：只登记待创建清单，由用户手工建立。
    auto_create_dirs: Mapped[bool] = mapped_column(Boolean, default=True)
    # v0.8.6：B 库单集/单电影文件按英文剧名改名（只换中文片名段）。
    # A 库只读、B 库目录命名不变；改名结果落在 MirrorFile.target_name。
    rename_files: Mapped[bool] = mapped_column(Boolean, default=True)
    # v0.9.0：单次同步最多整理多少部整部剧/电影（0=不限量）。到上限即停，
    # 剩余作品等下一次定时同步继续；works_cursor 记上一轮最后处理完的作品 key，
    # 用轮转保证每部作品都会被覆盖，不会每轮都只处理前面几部。
    max_works_per_run: Mapped[int] = mapped_column(Integer, default=0)
    works_cursor: Mapped[str] = mapped_column(String(256), default="")
    last_run: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_status: Mapped[str] = mapped_column(String(24), default="never")
    last_message: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class MirrorDir(Base):
    """v0.8.1：A 库目录 → B 库目录的 1:1 镜像映射。

    分类目录（动画电影/国产电影 等）在 B 库保持**原名**，不加标记；只有影视主目录
    才按 star_naming 加 ·（v0.8.3 起，网盘拒绝 * 返回 code=141）。create_dir 接口已由抓包 HAR 确认，缺目录时自动创建；
    失败原因写入 error 字段供管理页展示，绝不静默记成「请手工建立」。
    """
    __tablename__ = "mirror_dirs"
    id: Mapped[int] = mapped_column(primary_key=True)
    rule_id: Mapped[int] = mapped_column(ForeignKey("mirror_rules.id"), index=True)
    source_dir_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    parent_target_id: Mapped[str] = mapped_column(String(64), default="")
    name_standard: Mapped[str] = mapped_column(String(512), default="")   # B 库期望名称
    kind: Mapped[str] = mapped_column(String(24), default="category")      # category/work/season
    category_path: Mapped[str] = mapped_column(String(512), default="")
    target_dir_id: Mapped[str] = mapped_column(String(64), default="")
    # mapped / created / created_fallback / missing
    status: Mapped[str] = mapped_column(String(24), default="missing")
    # v0.8.2：建目录失败的真实原因（接口报错 / 自动创建关闭 / 降级说明），不再被前端通用文案覆盖
    error: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    __table_args__ = (UniqueConstraint("rule_id", "source_dir_id", name="uq_mirror_dir_rule_source"),)


class MirrorMedia(Base):
    """一部影视的 A/B 目录映射。work_key 与资源库作品键共用同一归一化规则。"""
    __tablename__ = "mirror_media"
    id: Mapped[int] = mapped_column(primary_key=True)
    rule_id: Mapped[int] = mapped_column(ForeignKey("mirror_rules.id"), index=True)
    work_key: Mapped[str] = mapped_column(String(256), default="", index=True)
    title_standard: Mapped[str] = mapped_column(String(512), default="")   # 展示/推送用标准名
    title_mirror: Mapped[str] = mapped_column(String(512), default="")     # B 库带 · / 不带标记的真实目录名
    tmdb_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    media_type: Mapped[str] = mapped_column(String(16), default="")
    source_dir_id: Mapped[str] = mapped_column(String(64), default="")     # A 目录 fileId
    target_dir_id: Mapped[str] = mapped_column(String(64), default="")     # B 目录 fileId
    # v0.8.1：A 库中该作品之上的分类目录路径（如「动画电影/国产」），B 库同名镜像。
    category_path: Mapped[str] = mapped_column(String(512), default="")
    # v0.8.6：整部作品只解析一次英文剧名并缓存，避免每一集都打 TMDB / AI。
    # english_source: tmdb / ai / resource / manual；空 english_title 表示「待翻译」。
    english_title: Mapped[str] = mapped_column(String(512), default="")
    english_source: Mapped[str] = mapped_column(String(24), default="")
    status: Mapped[str] = mapped_column(String(24), default="pending")     # pending/syncing/synced/blocked/failed
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    __table_args__ = (UniqueConstraint("rule_id", "work_key", name="uq_mirror_rule_work"),)


class MirrorFile(Base):
    """单个文件的 A→B 映射与身份指纹。替换只在同一 identity 内进行。

    v0.8.6：`file_name` 永远是 **A 库原名**（改名也不覆盖，增量比对锚点），
    `target_name` 才是 B 库改名后的实际文件名。A 文件是否变化只看
    `source_file_id + fingerprint(size:utime)`，与两边文件名无关。
    """
    __tablename__ = "mirror_files"
    id: Mapped[int] = mapped_column(primary_key=True)
    media_id: Mapped[int] = mapped_column(ForeignKey("mirror_media.id"), index=True)
    rule_id: Mapped[int] = mapped_column(ForeignKey("mirror_rules.id"), index=True)
    identity: Mapped[str] = mapped_column(String(128), default="", index=True)  # tmdb:type:S:E 或 文件名兜底
    source_file_id: Mapped[str] = mapped_column(String(64), default="")
    target_file_id: Mapped[str] = mapped_column(String(64), default="")
    file_name: Mapped[str] = mapped_column(String(512), default="")
    size: Mapped[int] = mapped_column(BigInteger, default=0)
    utime: Mapped[int] = mapped_column(BigInteger, default=0)
    fingerprint: Mapped[str] = mapped_column(String(128), default="")      # size + utime
    status: Mapped[str] = mapped_column(String(24), default="pending")     # pending/copying/synced/superseded/failed
    task_id: Mapped[str] = mapped_column(String(64), default="")
    error: Mapped[str] = mapped_column(Text, default="")
    # v0.8.6：B 库改名结果。rename_status: ""(未处理)/done/failed/skipped_ascii。
    # 这些字段只描述「B 库文件名」，绝不参与 A→B 身份比对。
    target_name: Mapped[str] = mapped_column(String(512), default="")
    rename_status: Mapped[str] = mapped_column(String(24), default="")
    rename_error: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # 同一 identity 允许保留历史版本（superseded）与当前版本，靠 source_file_id 区分。
    # 内容变化时只新增复制，不删除 B 库旧文件（删除接口 8.6 待验证）。
    __table_args__ = (UniqueConstraint("media_id", "source_file_id", "fingerprint",
                                       name="uq_mirror_media_source_fingerprint"),)


class MirrorTask(Base):
    """光鸭复制任务轮询记录。只把 status=2 视为完成，其它取值原样记录不猜。"""
    __tablename__ = "mirror_tasks"
    id: Mapped[int] = mapped_column(primary_key=True)
    rule_id: Mapped[int] = mapped_column(ForeignKey("mirror_rules.id"), index=True)
    media_id: Mapped[int] = mapped_column(Integer, default=0, index=True)
    task_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    status: Mapped[str] = mapped_column(String(24), default="pending")  # pending/running/done/failed/unknown
    raw_status: Mapped[str] = mapped_column(String(32), default="")
    polls: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ResourceVersion(Base):
    """B 库文件内容变化留痕。已分享影视内容变化时只记版本，不重建分享。"""
    __tablename__ = "resource_versions"
    id: Mapped[int] = mapped_column(primary_key=True)
    resource_id: Mapped[int] = mapped_column(Integer, index=True)
    file_id: Mapped[str] = mapped_column(String(64), default="")
    work_key: Mapped[str] = mapped_column(String(256), default="", index=True)
    fingerprint: Mapped[str] = mapped_column(String(128), default="")
    old_fingerprint: Mapped[str] = mapped_column(String(128), default="")
    size: Mapped[int] = mapped_column(BigInteger, default=0)
    utime: Mapped[int] = mapped_column(BigInteger, default=0)
    change_type: Mapped[str] = mapped_column(String(24), default="updated")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class DistributionEvent(Base):
    """作品级分发事件：同一批文件变化合并为一次「内容更新」推送，不逐集重复。"""
    __tablename__ = "distribution_events"
    id: Mapped[int] = mapped_column(primary_key=True)
    work_key: Mapped[str] = mapped_column(String(256), default="", index=True)
    event_type: Mapped[str] = mapped_column(String(24), default="updated")
    fingerprint: Mapped[str] = mapped_column(String(128), default="")
    changed_files: Mapped[int] = mapped_column(Integer, default=0)
    queue_id: Mapped[int] = mapped_column(Integer, default=0)
    share_reused: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    __table_args__ = (UniqueConstraint("work_key", "fingerprint", name="uq_event_work_fingerprint"),)


class UserContribution(Base):
    """Telegram 用户投稿。转存接口未验证前只登记，不落 B 库。"""
    __tablename__ = "user_contributions"
    id: Mapped[int] = mapped_column(primary_key=True)
    tg_user_id: Mapped[str] = mapped_column(String(64), default="")
    tg_username: Mapped[str] = mapped_column(String(128), default="")
    title: Mapped[str] = mapped_column(String(512), default="")
    content: Mapped[str] = mapped_column(Text, default="")
    share_url: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(24), default="pending")  # pending/approved/rejected/transferred
    note: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    handled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class MediaRequest(Base):
    """用户「我想看」→ TgtoDrive 关键词监控规则（幂等 id，不覆盖原有规则）。"""
    __tablename__ = "media_requests"
    id: Mapped[int] = mapped_column(primary_key=True)
    tg_user_id: Mapped[str] = mapped_column(String(64), default="")
    tg_username: Mapped[str] = mapped_column(String(128), default="")
    title: Mapped[str] = mapped_column(String(512), default="")
    tmdb_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    media_type: Mapped[str] = mapped_column(String(16), default="")
    season: Mapped[int | None] = mapped_column(Integer, nullable=True)
    keyword_id: Mapped[str] = mapped_column(String(128), default="")
    status: Mapped[str] = mapped_column(String(24), default="pending")  # pending/synced/failed
    note: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
