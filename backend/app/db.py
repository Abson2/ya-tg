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
}


# v0.6.0 默认模板：带 TMDB 分类标签（type / origin_country / genres / tags）
DEFAULT_TEMPLATE = (
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
    from .models import MessageTemplate

    with SessionLocal() as db:
        tpl = db.query(MessageTemplate).filter(MessageTemplate.name == "默认图文模板").first()
        if not tpl:
            db.add(MessageTemplate(name="默认图文模板", content=DEFAULT_TEMPLATE,
                                   enabled=True, is_default=True))
            db.commit()
        elif tpl.content == OLD_DEFAULT_TEMPLATE:
            tpl.content = DEFAULT_TEMPLATE
            db.commit()


def rowdict(row) -> dict:
    return {c.key: getattr(row, c.key) for c in row.__table__.columns}


def json_dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False)
