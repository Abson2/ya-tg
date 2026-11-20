"""阶段二路线B C1：严格顺序引擎 DB 扩展的旧库迁移兼容性。

验证生产 migrate() 所依赖的两件事：
  1. db._COLUMNS 已声明路线B新列（mirror_rules / mirror_tasks）；
  2. 用与 migrate() 同源的 CREATE ALL + inspect + ALTER 逻辑，旧库（缺新列）
     能补齐新列，且旧 MirrorTask 行可读、旧 status 保留、新字段默认正确、幂等。
不调用完整 migrate()：其后半段历史 backfill 依赖 app_config/message_templates
等表，与本测试关注的"列迁移"无关，且需要全局库上下文。
"""
import shutil
import tempfile
from pathlib import Path

from sqlalchemy import create_engine, inspect, text

from app import db as dbmod
from app import models  # noqa: F401  注册 Base.metadata 的全部表


def _make_old_db(path: Path):
    """建一个"升级前"的库：表结构缺路线B新列，且有一条旧任务记录。"""
    eng = create_engine(f"sqlite:///{path}")
    with eng.begin() as c:
        c.execute(text("""CREATE TABLE mirror_rules (
            id INTEGER PRIMARY KEY, name TEXT DEFAULT '', source_root_id TEXT DEFAULT '',
            target_root_id TEXT DEFAULT '', interval_minutes INTEGER DEFAULT 60,
            enabled BOOLEAN DEFAULT 0, star_naming BOOLEAN DEFAULT 1,
            auto_identify BOOLEAN DEFAULT 1, file_idle_seconds FLOAT DEFAULT 1.0,
            auto_create_dirs BOOLEAN DEFAULT 1, rename_files BOOLEAN DEFAULT 1,
            max_works_per_run INTEGER DEFAULT 0, works_cursor TEXT DEFAULT '',
            last_run DATETIME, last_status TEXT DEFAULT 'never', last_message TEXT DEFAULT '',
            last_pipeline_json TEXT DEFAULT '', created_at DATETIME)"""))
        c.execute(text("""CREATE TABLE mirror_tasks (
            id INTEGER PRIMARY KEY, rule_id INTEGER, media_id INTEGER DEFAULT 0,
            task_id TEXT DEFAULT '', status TEXT DEFAULT 'pending', raw_status TEXT DEFAULT '',
            polls INTEGER DEFAULT 0, error TEXT DEFAULT '', created_at DATETIME,
            finished_at DATETIME)"""))
        c.execute(text("INSERT INTO mirror_tasks (id, rule_id, task_id, status, raw_status) "
                       "VALUES (1, 9, 'legacy-task-001', 'done', '2')"))
    return eng


def _apply_column_migration(eng):
    """复刻 migrate() 的 CREATE ALL + 逐列 ALTER 段（数据源同为 db._COLUMNS）。"""
    dbmod.Base.metadata.create_all(bind=eng)
    insp = inspect(eng)
    tables = set(insp.get_table_names())
    for table, columns in dbmod._COLUMNS.items():
        if table not in tables:
            continue
        existing = {c["name"] for c in insp.get_columns(table)}
        for name, ddl in columns.items():
            if name not in existing:
                with eng.begin() as conn:
                    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}"))


def test_new_columns_declared_in_columns_map():
    """生产 _COLUMNS 必须声明全部路线B新列，否则旧库补不到。"""
    rules = dbmod._COLUMNS["mirror_rules"]
    tasks = dbmod._COLUMNS["mirror_tasks"]
    assert {"batch_id", "batch_status", "control_action", "cursor_sequence"} <= set(rules)
    assert {"batch_id", "sequence_id", "parent_sequence_id",
            "task_status", "retry_count"} <= set(tasks)


def test_old_db_migrates_and_keeps_legacy_rows():
    root = Path(tempfile.mkdtemp(prefix="v100_mig_", dir="/tmp"))
    old_path = root / "old.db"
    old_eng = _make_old_db(old_path)
    try:
        _apply_column_migration(old_eng)
        _apply_column_migration(old_eng)  # 幂等

        insp = inspect(old_eng)
        rule_cols = {c["name"] for c in insp.get_columns("mirror_rules")}
        task_cols = {c["name"] for c in insp.get_columns("mirror_tasks")}
        for col in ("batch_id", "batch_status", "control_action", "cursor_sequence"):
            assert col in rule_cols, col
        for col in ("batch_id", "sequence_id", "parent_sequence_id",
                    "task_status", "retry_count"):
            assert col in task_cols, col

        with old_eng.connect() as c:
            row = c.execute(text("SELECT status, batch_id, task_status, sequence_id, "
                                 "retry_count FROM mirror_tasks WHERE id=1")).mappings().one()
        assert row["status"] == "done"          # 旧状态原样保留
        assert row["batch_id"] == ""             # 新字段默认值
        assert row["task_status"] == ""
        assert row["sequence_id"] == 0
        assert row["retry_count"] == 0
    finally:
        old_eng.dispose()
        shutil.rmtree(root, ignore_errors=True)


def test_fresh_db_has_new_columns():
    """新库 create_all 后新列即存在（无需 ALTER）。"""
    root = Path(tempfile.mkdtemp(prefix="v100_fresh_", dir="/tmp"))
    eng = create_engine(f"sqlite:///{root / 'fresh.db'}")
    dbmod.Base.metadata.create_all(bind=eng)
    insp = inspect(eng)
    rule_cols = {c["name"] for c in insp.get_columns("mirror_rules")}
    task_cols = {c["name"] for c in insp.get_columns("mirror_tasks")}
    assert {"batch_id", "batch_status", "control_action", "cursor_sequence"} <= rule_cols
    assert {"batch_id", "sequence_id", "parent_sequence_id",
            "task_status", "retry_count"} <= task_cols
    eng.dispose()
    shutil.rmtree(root, ignore_errors=True)
