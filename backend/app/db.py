import json
import logging
from sqlalchemy import create_engine, event, inspect, or_, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from .config import settings

logger = logging.getLogger(__name__)

_IS_SQLITE = settings.database_url.startswith("sqlite")
args = {"check_same_thread": False} if _IS_SQLITE else {}
if _IS_SQLITE:
    # v0.8.8：SQLite 只有一个写者。后台 job（3 秒 Token 轮询 / 10 秒 Bot 轮询 /
    # 30 秒队列 worker）与手动同步、定时扫描并发写时，默认 5 秒等待极易抛
    # `database is locked`。把等待放宽到 30 秒，并开 WAL 让读写不再互斥。
    args["timeout"] = 30
engine = create_engine(settings.database_url, connect_args=args, pool_pre_ping=True)

if _IS_SQLITE:
    @event.listens_for(engine, "connect")
    def _sqlite_pragmas(dbapi_conn, _record):
        """每个新连接都生效；journal_mode=WAL 是库级持久设置，重复执行无副作用。

        顺序与隔离都有讲究：busy_timeout 是纯连接级、不需要拿锁，必须最先设且
        单独 try。若把 journal_mode=WAL 放在前面，别的连接正持写锁时它自己就可能
        抛 `database is locked`，整个 try 一起失败会把最关键的 30 秒等待也吞掉——
        恰好是最需要它的时刻。synchronous / journal_mode 各自独立，互不影响。
        """
        for pragma in ("PRAGMA busy_timeout=30000",
                       "PRAGMA synchronous=NORMAL",
                       "PRAGMA journal_mode=WAL"):
            try:
                cur = dbapi_conn.cursor()
                cur.execute(pragma)
                cur.close()
            except Exception:
                # 内存库 / 只读文件系统 / 并发写锁等场景单项失败不影响其余设置
                pass


SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# v0.1.0 -> v0.2.0 轻量迁移：SQLite 不支持 CREATE COLUMN IF NOT EXISTS，逐列检查后 ALTER。
_COLUMNS = {
    "resources": {
        "parent_id": "TEXT DEFAULT ''",
        "utime": "BIGINT DEFAULT 0",
        "media_title": "TEXT DEFAULT ''",
        "identify_status": "TEXT DEFAULT 'unidentified'",
        "revision": "INTEGER DEFAULT 0",
        # v0.6.0：TMDB 分类标签与季集、识别来源
        "original_title": "TEXT DEFAULT ''",
        "genres": "TEXT DEFAULT ''",
        "origin_country": "TEXT DEFAULT ''",
        "vote_average": "FLOAT DEFAULT 0",
        "season": "INTEGER",
        "episode": "INTEGER",
        "identify_source": "TEXT DEFAULT ''",
        "identify_note": "TEXT DEFAULT ''",
        # v0.7.0：完结/追更状态（TMDB status 推断）
        "finish_status": "TEXT DEFAULT ''",
        # v0.8.0：手动修正完结/追更后锁定，TMDB 自动回填不得覆盖
        "finish_locked": "BOOLEAN DEFAULT 0",
        # v0.8.9：TMDB 官方总集数，供「更新至 N / 共 M 集」模板变量
        "total_episodes": "INTEGER DEFAULT 0",
        # v0.9.9 #202：TMDB 首播/上映完整日期，供 {{release_date}}
        "release_date": "TEXT DEFAULT ''",
        # v0.6.1：目录名链，供按作品目录识别（修剧集单集认错作品）
        "path_hint": "TEXT DEFAULT ''",
        # v0.6.3：目录 fileId 链 + 剧集目录级分享
        "dir_chain_json": "TEXT DEFAULT '[]'",
        "share_scope": "TEXT DEFAULT 'file'",
        "share_dir_id": "TEXT DEFAULT ''",
        "share_dir_name": "TEXT DEFAULT ''",
    },
    "push_queue": {
        "next_retry_at": "DATETIME",
        # 四模块重构 阶段A：分享中心 ↔ 分发中心交接列（老库 ALTER 补列）
        "share_task_id": "INTEGER DEFAULT 0",
        "push_reason": "TEXT DEFAULT ''",
        "event_id": "INTEGER DEFAULT 0",
        "status_ext": "TEXT DEFAULT ''",
        "accept_worker": "TEXT DEFAULT ''",
        "accepted_at": "DATETIME",
    },
    # 四模块重构 阶段A：分享任务表增加索引列（表本身由 create_all 新建，老库若已存在补列）
    "share_tasks": {
        "dedup_key": "TEXT",
        "queue_type": "TEXT DEFAULT 'fresh'",
        "submitted_at": "DATETIME",
        # v1.5.1 P0：building 超时自愈时间戳（老库幂等补列）
        "building_at": "DATETIME",
    },
    # v1.1：当前/历史分享记录表（真相源），表由 create_all 新建，老库补列兜底
    "share_records": {
        "work_key": "TEXT DEFAULT ''",
        "share_dir_id": "TEXT DEFAULT ''",
        "share_url": "TEXT DEFAULT ''",
        "share_status": "TEXT DEFAULT 'active'",
        "current_flag": "INTEGER DEFAULT 1",
        "cancelled_at": "DATETIME",
        "failure_reason": "TEXT DEFAULT ''",
        "last_verified_at": "DATETIME",
        # 阶段五：链接有效性判定与清理（幂等增列）
        "link_state": "TEXT DEFAULT 'active'",
        "verified_result": "TEXT DEFAULT ''",
        "verify_fail_count": "INTEGER DEFAULT 0",
        "cleaned_at": "DATETIME",
        "clean_state": "TEXT DEFAULT ''",
        "created_at": "DATETIME",
    },
    # 四模块重构 v1.1：模块间事件表（表本身由 create_all 新建，这里给极老库补列兜底）
    "mirror_events": {
        "idem_key": "TEXT DEFAULT ''",
        "consumed_at": "DATETIME",
    },
    "resource_events": {
        "idem_key": "TEXT DEFAULT ''",
        "consumed_at": "DATETIME",
    },
    "auth_tokens": {
        "device_id": "TEXT DEFAULT ''",
    },
    "scan_cursors": {
        "dir_paths_json": "TEXT DEFAULT '{}'",
        "dir_chains_json": "TEXT DEFAULT '{}'",
    },
    # v0.8.1：镜像目录 1:1 与文件操作节流（表由 create_all 新建，老库走 ALTER 补列）
    "mirror_rules": {
        "file_idle_seconds": "FLOAT DEFAULT 1.0",
        "auto_create_dirs": "BOOLEAN DEFAULT 1",
        # v0.8.6：B 库单集/电影文件按英文剧名改名开关
        "rename_files": "BOOLEAN DEFAULT 1",
        # v0.9.0：单次同步整部作品上限 + 轮转游标（0=不限量）
        "max_works_per_run": "INTEGER DEFAULT 0",
        "works_cursor": "TEXT DEFAULT ''",
        # v0.9.10：本轮任务各节点处理量快照，进度条只显示当前任务不显示历史累计
        "last_pipeline_json": "TEXT DEFAULT ''",
        # 阶段二路线B：严格顺序镜像引擎批次控制（旧库 ALTER 补列）
        "batch_id": "TEXT DEFAULT ''",
        "batch_status": "TEXT DEFAULT 'idle'",
        "control_action": "TEXT DEFAULT ''",
        "cursor_sequence": "INTEGER DEFAULT 0",
    },
    "mirror_media": {
        "category_path": "TEXT DEFAULT ''",
        # v0.8.6：每部作品缓存一次英文剧名，避免逐集打 TMDB / AI
        "english_title": "TEXT DEFAULT ''",
        "english_source": "TEXT DEFAULT ''",
    },
    "mirror_files": {
        # v0.8.6：file_name 仍是 A 原名；target_name 才是 B 库改名后的名字
        "target_name": "TEXT DEFAULT ''",
        "rename_status": "TEXT DEFAULT ''",
        "rename_error": "TEXT DEFAULT ''",
    },
    # 阶段二路线B：严格顺序执行序列持久化（旧库 ALTER 补列，旧数据默认空/0）
    "mirror_tasks": {
        "batch_id": "TEXT DEFAULT ''",
        "sequence_id": "INTEGER DEFAULT 0",
        "parent_sequence_id": "INTEGER DEFAULT 0",
        "task_status": "TEXT DEFAULT ''",
        "retry_count": "INTEGER DEFAULT 0",
    },
    # v0.8.2：建目录失败原因，管理页如实展示，不再用通用文案掩盖接口报错
    "mirror_dirs": {
        "error": "TEXT DEFAULT ''",
    },
    # v0.9.4：推送目标统一表增加类型标记（频道/群聊），仅菜单分类展示用。
    "channels": {
        "kind": "TEXT DEFAULT 'channel'",
    },
    # v0.9.8：求片审核状态与 TgtoDrive 同步状态解耦，后台审核不再显示 failed
    "media_requests": {
        "review_status": "TEXT DEFAULT 'pending'",
        "review_note": "TEXT DEFAULT ''",
        "handled_at": "DATETIME",
        "year": "TEXT DEFAULT ''",  # v0.9.8：TMDB 候选选定后的年份
        # v0.9.17.1 #284：同作品求片去重合并计数
        "submit_count": "INTEGER DEFAULT 1",
        "last_tg_user_id": "TEXT DEFAULT ''",
        "last_tg_username": "TEXT DEFAULT ''",
        "last_submitted_at": "DATETIME",
    },
    # v0.9.8：投稿记录 TMDB 候选选定的影号/中文名/年份/类型
    "user_contributions": {
        "tmdb_id": "INTEGER",
        "media_type": "TEXT DEFAULT ''",
        "year": "TEXT DEFAULT ''",
        # v0.9.17 #281：同作品去重合并计数
        "dedup_key": "TEXT DEFAULT ''",
        "submit_count": "INTEGER DEFAULT 1",
        "last_share_url": "TEXT DEFAULT ''",
        "last_tg_user_id": "TEXT DEFAULT ''",
        "last_submitted_at": "DATETIME",
    },
    # 四模块重构 阶段A：分发事件 → 分享/推送任务溯源（强制项3）
    "distribution_events": {
        "share_task_id": "INTEGER DEFAULT 0",
        "push_task_ids": "TEXT DEFAULT ''",
    },
    # v0.9.15 #276：发现目标补成员状态/归属 Bot 用户名/最后 update_id/更新时间
    "discovered_chats": {
        "member_status": "TEXT DEFAULT 'unknown'",
        "bot_username": "TEXT DEFAULT ''",
        "last_update_id": "BIGINT DEFAULT 0",
        "updated_at": "DATETIME",
        # v0.9.18.1：仅从发现列表移除的标记（不动白名单/Channel/Telegram）
        "hidden": "BOOLEAN DEFAULT 0",
    },
}


# v0.6.5 默认模板：参考「保镖恋人」示例，整剧/整季展示，不暴露单集文件名。
# v0.8.9 补「剧集状态 / 集数进度」两行；v0.9.0 剧集状态改季集标签：
# 追更剧「📡 更新至 S01E07 · 连载中」，完结剧「📡 S01E24 · 完结」，
# 进度由 episode_progress（N/M）单独承载；episode_count 为「已收录」，避免重复。
# v0.9.9 #202：默认模板改为用户指定样式（纯文字按钮标签，不做 inline 按钮）。
# 变量全部由 publisher._build_message 的 ctx 提供：分项技术规格从文件名解析，
# 上映时间取 TMDB release_date（无则回退 year），评分用 vote_plain（整数优先）。
DEFAULT_TEMPLATE = (
    "<b>{{title}}{{#if year}} ({{year}}){{/if}} 🖼️</b>\n"
    "{{#if type_label}}类型：{{type_label}}\n{{/if}}"
    "{{#if tmdb_id}}🆔 TMDB ID: {{tmdb_id}}\n{{/if}}"
    "{{#if air_date}}📅 上映时间: {{air_date}}\n{{/if}}"
    "{{#if vote_plain}}⭐️ 评分: {{vote_plain}}{{/if}}"
    "{{#if source}} | 💿 来源: {{source}}{{/if}}\n"
    "{{#if quality}}📺 画质: {{quality}}\n{{/if}}"
    "{{#if video_codec}}🎞️ 编码: {{video_codec}}{{/if}}"
    "{{#if audio_codec}} | 🔊 音频: {{audio_codec}}{{/if}}\n"
    "{{#if season_count}}📋 季集：共{{season_count}}季{{/if}}"
    "{{#if episode_count}} | 共 {{episode_count}} 集{{/if}}"
    "{{#if episode_status}}（{{episode_status}}）{{/if}}"
    "{{#if episode_range}} {{episode_range}}{{/if}}\n"
    "{{#if size}}📦 大小：{{size}}\n{{/if}}"
    "{{#if overview}}📝 简介: {{overview}}\n{{/if}}"
    "{{#if tags}}🏷️ 标签： {{tags}}\n{{/if}}"
    "{{buttons_text}}\n"
    "{{share_url}}"
)
# v0.9.6：内置模板预设（供管理页下拉一键载入，不落库；用户载入后可改可另存）。
# 全部只用现有 {{变量}} 与 {{#if}} 语法，不引入新变量，兼容 publisher 渲染器。
# 默认图文沿用 DEFAULT_TEMPLATE，另给精简 / 追更 / 纯文本三套，解决「默认图文模板重复、
# 只有一种样式」的问题。键 name 用于下拉展示，content 用于填充编辑框。
TEMPLATE_PRESETS = [
    {"name": "默认图文（整剧 · 推荐）", "content": DEFAULT_TEMPLATE},
    {"name": "精简卡片", "content": (
        "<b>🎬 {{title}}{{#if year}}（{{year}}）{{/if}}</b>\n"
        "{{#if season_label}}📺 {{season_label}}\n{{/if}}"
        "{{#if episode_status}}📡 {{episode_status}}\n{{/if}}"
        "{{#if vote_average}}⭐ {{vote_average}}/10\n{{/if}}"
        "{{#if resolution}}📐 {{resolution}}\n{{/if}}"
        "{{#if size}}💾 {{size}}\n{{/if}}"
        "{{share_url}}"
    )},
    {"name": "追更剧集（带进度）", "content": (
        "<b>🎬 {{title}}{{#if year}}（{{year}}）{{/if}}</b>\n"
        "{{#if season_label}}📺 {{season_label}}\n{{/if}}"
        "{{#if episode_status}}📡 {{episode_status}}\n{{/if}}"
        "{{#if episode_progress}}🔢 进度 {{episode_progress}}\n{{/if}}"
        "{{#if episode_count}}📼 已收录 {{episode_count}} 集\n{{/if}}"
        "{{#if overview}}\n📖 {{overview}}\n\n{{/if}}"
        "{{share_url}}"
    )},
    {"name": "纯文本分享", "content": (
        "🎬 {{title}}{{#if year}}（{{year}}）{{/if}}\n"
        "{{#if season_label}}📺 {{season_label}}\n{{/if}}"
        "{{#if episode_status}}📡 {{episode_status}}\n{{/if}}"
        "{{#if vote_average}}⭐ {{vote_average}}/10 · {{/if}}{{#if resolution}}{{resolution}} · {{/if}}{{#if size}}{{size}}\n{{/if}}"
        "{{#if overview}}📖 {{overview}}\n{{/if}}"
        "资源：{{share_url}}"
    )},
]

# v0.6.0 旧版默认模板：仍在使用它（用户没改过）时自动升级到 v0.6.4 新版
PREV_DEFAULT_TEMPLATE = (
    "<b>{{title}}</b>{{#if year}}（{{year}}）{{/if}}\n"
    "{{#if type_label}}类型：{{type_label}}\n{{/if}}"
    "{{#if vote_average}}评分：{{vote_average}}\n{{/if}}"
    "{{#if overview}}{{overview}}\n\n{{/if}}"
    "{{#if origin_country}}地区：{{origin_country}}\n{{/if}}"
    "{{#if genres}}分类：{{genres}}\n{{/if}}"
    "{{#if resolution}}画质：{{resolution}}\n{{/if}}"
    "{{#if size}}大小：{{size}}\n{{/if}}"
    "{{#if tags}}标签：{{tags}}\n{{/if}}"
    "{{#if type}}type={{type}} · origin_country={{origin_country}} · genres={{genres}}\n{{/if}}"
    "{{#if tmdb_url}}TMDB：<a href=\"{{tmdb_url}}\">#{{tmdb_id}}</a>\n{{/if}}"
    "资源：{{filename}}\n\n{{share_url}}"
)
OLD_DEFAULT_TEMPLATE = (
    "<b>{{title}}</b>\n\n{{overview}}\n\n"
    "{{#if year}}年份：{{year}}\n{{/if}}"
    "{{#if resolution}}画质：{{resolution}}\n{{/if}}"
    "资源：{{filename}}\n\n{{share_url}}"
)


def _is_legacy_default(content: str) -> bool:
    """v0.6.5：识别旧版默认模板（含单集文件名/type= 调试行），用于可靠迁移。

    不用精确整串比对——用户可能点过「一键恢复」把旧模板写回，或与基准有细微差异，
    精确匹配会漏掉。这里用「旧特征且无新特征」判断，用户真正自定义的模板不会被误改。
    """
    c = content or ""
    if not c:
        return False
    # v0.9.9 #202：新样式含类型行「类型：{{type_label}}」但已是当前版本，不能误判为 legacy
    # （否则每次启动 migrate 都把用户新模板重写一遍）。用新样式独占特征优先排除。
    if "{{buttons_text}}" in c or "🆔 TMDB ID" in c or "🖼️" in c:
        return False
    if "🎬" in c or "{{version}}" in c:  # 已经是新版
        return False
    legacy_marks = ("资源：{{filename}}", "type={{type}}", "类型：{{type_label}}", "地区：{{origin_country}}")
    return any(m in c for m in legacy_marks)


def _needs_template_upgrade(content: str) -> bool:
    """判断存量默认模板是否需要升级到当前版本。

    覆盖四类：
      1) v0.5.x/v0.6.0 老模板（含单集文件名/type= 调试行）；
      2) v0.6.4 整剧模板（含 🎬，但缺 v0.6.5 新增的 {{episode_count}} 行）；
      3) v0.6.5~v0.8.8 默认模板（含 v0.8.9 之前的集数行原句，缺剧集状态行）；
      4) v0.9.9 #202：v0.8.9 旧默认模板（含 {{episode_status}} 但无新样式特征
         {{buttons_text}} / 🆔 TMDB ID），升级到用户指定的新样式。
    自定义模板一律不动：第 3/4 类都用「旧默认骨架特征同时命中 + 无新样式特征」
    判断，用户自己改过内容的模板缺这些组合标记，不会被覆盖。
    """
    c = content or ""
    if not c:
        return False
    if "{{buttons_text}}" in c or "🆔 TMDB ID" in c:  # 已是 v0.9.9 新版样式
        return False
    # v0.9.9 #202：v0.8.9 旧默认模板（🎬 标题 + 📡 剧集状态 + 🔗 TMDB 链接骨架）。
    # 三者同时命中、且无新样式特征，才升级——这是用户反复反馈「改了模板没生效」的根因：
    # 旧判定见 episode_status 就 return False，存量库永远停在 v0.8.9 样式。
    if ("{{episode_status}}" in c and "🎬 {{title}}" in c and "🔗 TMDB" in c):
        return True
    if "{{episode_status}}" in c:  # 含状态但非旧默认骨架：其它版本/自定义，不动
        return False
    # v0.6.5~v0.8.8 默认模板：集数行原句 + 默认骨架。两者同时命中才升级，
    # 用户自己改过、恰好也用了 {{episode_count}} 的自定义模板一律不动。
    if ("📼 集数：共 {{episode_count}} 集" in c
            and "🎬 {{title}}" in c and "🔗 TMDB" in c):
        return True
    if "{{episode_count}}" in c:  # 自定义模板：保持原样
        return False
    if _is_legacy_default(c):
        return True
    # v0.6.4 默认模板特征：🎬 标题 + 📺 季 + 🎞 版本 + 💾 大小，且无单集文件名
    v064_marks = ("🎬", "📺", "🎞", "💾")
    if all(m in c for m in v064_marks) and "{{filename}}" not in c:
        return True
    return False


def _converge_legacy_queue(db) -> int:
    """v0.6.5 一次性收敛：升级前已入队的旧残留（同一部剧逐集排队）只保留最早一条。

    只处理未发送（pending/retry/review）项，已 sent/failed 的历史不动。
    按作品分组：剧集用 share_dir_id（整剧目录），其次 tmdb:类型:ID，最后退化到单条资源。
    同一「频道 + 作品」只留 id 最小的一条，其余删除，避免升级后仍一集一集推。
    """
    from .models import Resource
    rows = list(db.execute(text(
        "SELECT q.id, q.channel_id, q.resource_id, q.status "
        "FROM push_queue q WHERE q.status IN ('pending','retry','review') ORDER BY q.id"
    )).all())
    groups: dict[tuple, list[int]] = {}
    for qid, cid, rid, status in rows:
        res = db.get(Resource, rid)
        if not res:
            key = f"file:{rid}"
        else:
            dir_id = str(getattr(res, "share_dir_id", "") or "")
            if dir_id:
                key = f"dir:{dir_id}"
            elif res.tmdb_id:
                key = f"tmdb:{res.media_type or ''}:{res.tmdb_id}"
            else:
                key = f"file:{rid}"
        groups.setdefault((cid, key), []).append(qid)
    removed = 0
    for ids in groups.values():
        if len(ids) > 1:
            for qid in ids[1:]:
                db.execute(text("DELETE FROM push_queue WHERE id = :q"), {"q": qid})
                removed += 1
    return removed


def _backfill_pruned_dirs(db) -> int:
    """v0.9.6.1：给历史无标记 prune 行补 [pruned]。只改 error 文案，不删行。"""
    from .models import AppConfig, MirrorDir, utcnow
    from .mirror import PRUNED_DIR_MARK, is_pruned_dir_error

    marker_key = "pruned_dir_backfill_v0961"
    if db.get(AppConfig, marker_key):
        return 0
    changed = 0
    for d in db.query(MirrorDir).filter(MirrorDir.status == "missing").all():
        err = str(getattr(d, "error", "") or "")
        if err.startswith(PRUNED_DIR_MARK) or not is_pruned_dir_error(err):
            continue
        d.error = PRUNED_DIR_MARK + err
        d.updated_at = utcnow()
        changed += 1
    db.add(AppConfig(key=marker_key, value_enc=str(changed), updated_at=utcnow()))
    db.commit()
    return changed


def _mark_legacy_rename_skipped(db) -> int:
    """v0.9.6.1：升级前未完成的单集改名行标记「历史不回刷」。

    用户明确修正口径：旧版只改文件夹不改单集，新版不应回刷历史清单。
    只标记 synced 且尚未 done 的行；新复制的行仍是 pending，照常改名。
    force=True 可显式解除，保留人工入口。
    """
    from .models import AppConfig, MirrorFile, utcnow

    marker_key = "rename_legacy_skip_v0961"
    if db.get(AppConfig, marker_key):
        return 0
    rows = db.query(MirrorFile).filter(
        MirrorFile.status == "synced",
        or_(MirrorFile.rename_status.is_(None),
            MirrorFile.rename_status.in_(["", "pending_visible", "failed"]))).all()
    changed = 0
    for f in rows:
        f.rename_status = "skipped_legacy"
        f.rename_error = "历史条目不回刷英文单集改名；如需处理请点「重新解析英文名并改名」"
        f.updated_at = utcnow()
        changed += 1
    db.add(AppConfig(key=marker_key, value_enc=str(changed), updated_at=utcnow()))
    db.commit()
    return changed


def migrate() -> None:
    Base.metadata.create_all(bind=engine)
    insp = inspect(engine)
    tables = set(insp.get_table_names())
    for table, columns in _COLUMNS.items():
        if table not in tables:
            continue
        existing = {c["name"] for c in insp.get_columns(table)}
        for name, ddl in columns.items():
            if name not in existing:
                with engine.begin() as conn:
                    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}"))
    # Bot token 存量明文由 security.decrypt_secret 读取时兼容，写入时自动加密。
    # 默认模板只建一次；若仍是 v0.5.x 原版且用户没改过，自动升级到带标签的新版。
    from .models import AppConfig, MessageTemplate, utcnow

    with SessionLocal() as db:
        # v0.9.6.1：一次性历史迁移（助手函数各自用 AppConfig 标记保证只跑一次）。
        # 运行期 mirror.is_pruned_dir_error() 已能识别旧文案，落盘补标记只为口径一致。
        _backfill_pruned_dirs(db)
        _mark_legacy_rename_skipped(db)

        tpl = db.query(MessageTemplate).filter(MessageTemplate.name == "默认图文模板").first()
        # v0.9.6：去重——历史上「一键恢复默认模板」在并发 / 空表时可能写入多条同名
        # 「默认图文模板」（用户截图出现 5 条）。这里保留第一条（优先 is_default），
        # 删除其余同名重复行；用户真正自定义的其它名字模板一律不动。
        dupes = db.query(MessageTemplate).filter(MessageTemplate.name == "默认图文模板").all()
        if len(dupes) > 1:
            keeper = next((x for x in dupes if x.is_default), dupes[0])
            for x in dupes:
                if x.id != keeper.id:
                    db.delete(x)
            db.commit()
            tpl = keeper
        if not tpl:
            db.add(MessageTemplate(name="默认图文模板", content=DEFAULT_TEMPLATE,
                                   enabled=True, is_default=True))
            db.commit()
        elif _needs_template_upgrade(tpl.content):
            # v0.6.5：不再依赖精确字符串匹配（存量模板可能被"一键恢复"写回旧版、
            # 或与基准有细微差异而漏升级）。改用特征识别：老版整段结构、或 v0.6.4
            # 整剧模板缺集数行，都升级到当前版本；用户自定义模板不动。
            tpl.content = DEFAULT_TEMPLATE
            db.commit()

        # v0.6.5：一次性收敛升级前的旧队列残留（同剧逐集排队）。用标记确保只跑一次，
        # 避免用户手动清库重新扫描后、正常的单集队列被误合并。
        marker_key = "queue_converge_v065"
        if not db.get(AppConfig, marker_key):
            try:
                removed = _converge_legacy_queue(db)
                db.add(AppConfig(key=marker_key, value_enc=str(removed), updated_at=utcnow()))
                db.commit()
            except Exception as exc:
                db.rollback()  # 收敛失败不阻断启动，下次启动可重试（标记未写入）
                # A-09：此前静默吞掉，旧队列残留合并失败在日志侧完全不可见。
                logger.warning("[db] 旧队列收敛失败（下次启动重试）：%s", exc)


def rowdict(row) -> dict:
    return {c.key: getattr(row, c.key) for c in row.__table__.columns}


def json_dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False)
