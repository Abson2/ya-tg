"""阶段三 D-03：运行时配置批量读取。

验证 get_many / source_map 批量化后：
  1. 结果与逐条 get 完全一致（db 覆盖优先、env 回退、空值口径）；
  2. SQL 数与 key 数量无关（68 key 只 1 次查询，而非 68 次 db.get）；
  3. 前端 App.vue load() 已改为并行（Promise.allSettled）。
"""
from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy import event

from app import runtime_config
from app.db import SessionLocal, engine, migrate
from app.models import AppConfig

migrate()
ROOT = Path(__file__).resolve().parents[1]


@contextmanager
def count_sql():
    counts = [0]

    def before(conn, cursor, statement, parameters, context, executemany):
        counts[0] += 1

    event.listen(engine, "before_cursor_execute", before)
    try:
        yield counts
    finally:
        event.remove(engine, "before_cursor_execute", before)


@pytest.fixture
def db():
    with SessionLocal() as d:
        # 清掉测试可能写入的覆盖，恢复 env/empty 口径
        d.query(AppConfig).delete()
        d.commit()
        yield d
        d.query(AppConfig).delete()
        d.commit()


def test_get_many_matches_individual_get(db):
    keys = list(runtime_config.DEFAULTS)[:20]
    with count_sql() as c_many:
        many = runtime_config.get_many(db, keys)
    assert c_many[0] <= 1  # 批量：1 次查询
    individual = {k: runtime_config.get(db, k) for k in keys}
    assert many == individual  # 口径完全一致


def test_get_many_with_db_override(db):
    key = "guangya_client_id"
    runtime_config.set_many(db, {key: "override-value"})
    db.commit()
    many = runtime_config.get_many(db, [key])
    assert many[key] == "override-value"
    assert runtime_config.source_map(db)[key] == "db"


def test_source_map_is_one_query(db):
    with count_sql() as c:
        sm = runtime_config.source_map(db)
    assert c[0] == 1, c[0]
    assert len(sm) == len(runtime_config.DEFAULTS)
    assert set(sm) == set(runtime_config.DEFAULTS)


def test_frontend_load_parallelized():
    src = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "Promise.allSettled" in src
    assert "async function load()" in src
