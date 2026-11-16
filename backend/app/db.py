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
    },
    "push_queue": {
        "next_retry_at": "DATETIME",
    },
}


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
    # 默认消息模板只建一次。
    from .models import MessageTemplate

    with SessionLocal() as db:
        if not db.query(MessageTemplate).filter(MessageTemplate.name == "默认图文模板").first():
            db.add(MessageTemplate(
                name="默认图文模板",
                content=(
                    "<b>{{title}}</b>\n\n{{overview}}\n\n"
                    "{{#if year}}年份：{{year}}\n{{/if}}"
                    "{{#if resolution}}画质：{{resolution}}\n{{/if}}"
                    "资源：{{filename}}\n\n{{share_url}}"
                ),
                enabled=True,
                is_default=True,
            ))
            db.commit()


def rowdict(row) -> dict:
    return {c.key: getattr(row, c.key) for c in row.__table__.columns}


def json_dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False)
