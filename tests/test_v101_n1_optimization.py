"""阶段三性能优化：N+1 查询与日志聚合。

A-01 投稿列表 / A-02 队列 / A-03 审核：SQL 数与行数无关（批量 map 替代逐条 db.get）。
A-12 日志统计：7 张表由 28 次查询降到 7 次聚合。
返回结构与字段口径完全不变（同时断言字段一致）。
"""
from contextlib import contextmanager

import pytest
from sqlalchemy import event

from app import log_retention
from app.db import SessionLocal, engine, migrate
from app.main import contributions, queue, review
from app.models import (AuditLog, Bot, Channel, ContributionRestore, QueueItem, Resource,
                        UserContribution)

migrate()

TABLES = [ContributionRestore, UserContribution, QueueItem, Resource, Channel, AuditLog, Bot]


@contextmanager
def count_sql():
    """监听本进程 SQL 执行条数。"""
    counts = [0]

    def before(conn, cursor, statement, parameters, context, executemany):
        counts[0] += 1

    event.listen(engine, "before_cursor_execute", before)
    try:
        yield counts
    finally:
        event.remove(engine, "before_cursor_execute", before)


@pytest.fixture(autouse=True)
def fresh():
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        db.commit()
    yield


def _seed(n, start=0):
    with SessionLocal() as db:
        bot_id = 90000
        if not db.get(Bot, bot_id):
            db.add(Bot(id=bot_id, name="b", token="t"))
        for i in range(start, start + n):
            db.add(Resource(id=10000 + i, file_id=f"f{i}", name=f"res{i}"))
            db.add(Channel(id=20000 + i, bot_id=bot_id, name=f"ch{i}", chat_id=f"c{i}"))
            db.add(UserContribution(id=30000 + i, title=f"c{i}", status="pending"))
            db.add(ContributionRestore(contribution_id=30000 + i, status="pending"))
            db.add(QueueItem(id=40000 + i, resource_id=10000 + i,
                             channel_id=20000 + i, status="pending"))
        db.commit()


def test_contributions_no_n1():
    with SessionLocal() as db:
        _seed(5)
        with count_sql() as c5:
            r5 = contributions(status="", limit=5, db=db)
        _seed(95, start=5)  # 累计 100
        with count_sql() as c100:
            r100 = contributions(status="", limit=100, db=db)
    # SQL 数与行数无关：两次都是 2 条查询（contributions + restore_map）
    assert c5[0] == c100[0], (c5[0], c100[0])
    assert c5[0] <= 3
    assert len(r5) == 5 and len(r100) == 100
    # 字段口径不变
    assert {"restore_status", "restore_status_label", "submit_count"} <= set(r100[0])


def test_queue_and_review_no_n1():
    with SessionLocal() as db:
        _seed(5)
        with count_sql() as q5:
            q5r = queue(status="", limit=5, db=db)
        with count_sql() as v5:
            review(db=db)
        _seed(95, start=5)
        with count_sql() as q100:
            q100r = queue(status="", limit=100, db=db)
        with count_sql() as v100:
            review(db=db)
    assert q5[0] == q100[0] and q5[0] <= 4, (q5[0], q100[0])
    assert v5[0] == v100[0] and v5[0] <= 3, (v5[0], v100[0])
    assert len(q5r) == 5 and len(q100r) == 100
    assert {"id", "resource", "channel", "status"} <= set(q100r[0])


def test_log_stats_is_7_queries():
    with SessionLocal() as db:
        _seed(10)
        with count_sql() as c:
            out = log_retention.stats(db)
    assert c[0] == 7, c[0]
    assert len(out) == 7
    assert {"key", "rows", "pruneable", "oldest", "newest"} <= set(out[0])
