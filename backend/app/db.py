import json
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from .config import settings

args = {"check_same_thread": False} if settings.database_url.startswith("sqlite") else {}
engine = create_engine(settings.database_url, connect_args=args, pool_pre_ping=True)
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
    },
    "mirror_media": {
        "category_path": "TEXT DEFAULT ''",
    },
}


# v0.6.5 默认模板：参考「保镖恋人」示例，整剧/整季展示，不暴露单集文件名。
# v0.6.5 在 v0.6.4 基础上补「集数」行，让 TG 消息显示整剧总集数而非单集信息。
DEFAULT_TEMPLATE = (
    "<b>🎬 {{title}}{{#if year}}（{{year}}）{{/if}}</b>\n"
    "{{#if season_label}}📺 {{season_label}}\n{{/if}}"
    "{{#if episode_count}}📼 集数：共 {{episode_count}} 集\n{{/if}}"
    "{{#if tmdb_url}}🔗 TMDB：<a href=\"{{tmdb_url}}\">#{{tmdb_id}}</a>\n{{/if}}"
    "{{#if vote_average}}⭐ 评分：{{vote_average}}/10\n{{/if}}"
    "{{#if resolution}}📐 画质：{{resolution}}\n{{/if}}"
    "{{#if version}}🎞 版本：{{version}}\n{{/if}}"
    "{{#if size}}💾 大小：{{size}}\n{{/if}}"
    "{{#if overview}}\n📖 {{overview}}\n\n{{/if}}"
    "{{#if tags}}🏷 {{tags}}\n{{/if}}"
    "{{share_url}}"
)
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
    if "🎬" in c or "{{version}}" in c:  # 已经是新版
        return False
    legacy_marks = ("资源：{{filename}}", "type={{type}}", "类型：{{type_label}}", "地区：{{origin_country}}")
    return any(m in c for m in legacy_marks)


def _needs_template_upgrade(content: str) -> bool:
    """v0.6.5：判断存量默认模板是否需要升级到当前版本（老版整剧结构缺集数行）。

    覆盖两类：
      1) v0.5.x/v0.6.0 老模板（含单集文件名/type= 调试行）；
      2) v0.6.4 整剧模板（含 🎬，但缺 v0.6.5 新增的 {{episode_count}} 行）。
    这样从 v0.6.4 升级的用户也能拿到集数显示——正是「已升级但模板没变」的根因。
    用户真正自定义的模板（不含默认模板特征）不会被改动。
    """
    c = content or ""
    if not c:
        return False
    if "{{episode_count}}" in c:  # 已是 v0.6.5 新版
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
        tpl = db.query(MessageTemplate).filter(MessageTemplate.name == "默认图文模板").first()
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
            except Exception:
                db.rollback()  # 收敛失败不阻断启动，下次启动可重试（标记未写入）


def rowdict(row) -> dict:
    return {c.key: getattr(row, c.key) for c in row.__table__.columns}


def json_dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False)
