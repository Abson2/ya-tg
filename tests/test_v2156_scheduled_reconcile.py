"""v2.1.56 验收：定时扫描也要对账 + 对账不再全量实例化 ORM。

两个口径来自同一天的两条真机反馈：

① 用户 2026-11-28：「定时扫描到 B 库没有了，是需要将资源库中的内容移除」。
   此前 `_scheduled_scan` 明确「不做对账删除」，B 库整部删掉后资源库那行一直挂着，
   用户看到的表现就是「资源库里的作品删不掉」。现在定时扫描也走 `_reconcile_library`，
   但绝不传 `allow_empty`（不许用空批次清库）。

② 同一天用户报内存 1G。实测（tracemalloc，容器内）：单个 `Resource` ORM 对象
   5.14 KiB，2.4 万行一起加载 = **+120.8 MiB**。既然定时扫描也要对账，这 120 MiB
   就是每小时一笔。改成投影查询 + 批量 UPDATE 后不再实例化任何 Resource 实体。
"""
import asyncio
import json
from pathlib import Path

import pytest
from sqlalchemy import select

from app import main
from app.db import SessionLocal, migrate
from app.models import (QueueItem, Resource, ResourceScanBatch,
                        include_archived)

ROOT = Path(__file__).resolve().parents[1]
MAIN_SRC = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
B_ROOT = "B_ROOT"


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as s:
        for t in (QueueItem, Resource, ResourceScanBatch):
            s.query(t).delete()
        s.commit()
    yield
    with SessionLocal() as s:
        for t in (QueueItem, Resource, ResourceScanBatch):
            s.query(t).delete()
        s.commit()


@pytest.fixture
def db():
    with SessionLocal() as s:
        yield s


def _batch(db, **kw):
    b = ResourceScanBatch(root_id=B_ROOT, status="success", complete=True,
                          resumed=False, reconcile_allowed=True,
                          files=0, directories=3, seen_count=1, seen_json="[]")
    for k, v in kw.items():
        setattr(b, k, v)
    db.add(b)
    db.commit()
    return b


def _old_resources(db, n=2):
    for i in range(n):
        db.add(Resource(file_id=f"OLD_{i}", name=f"旧影视{i}.mkv", ext=".mkv",
                        size=1, utime=1))
    db.commit()


def test_reconcile_archives_rows_missing_from_seen(db):
    """核心口径：本次没扫到的行 → 归档（clean_state=cleaned），不物理删除。

    注意：`resources` 的 SELECT 有全局归档过滤（`db.py` 的 `do_orm_execute`），
    归档后普通查询**本来就查不到** —— 这正是「资源库里不再显示」的实现方式。
    要看行还在不在，必须进 `include_archived()`。
    """
    b = _batch(db)
    _old_resources(db, 2)

    out = _run(main._reconcile_library(
        db, ["NEW_1"], scan_complete=True, resumed=False,
        batch_id=b.id, allow_empty=False))
    db.commit()

    assert out["removed"] == 2, out
    assert out["archived"] is True

    # ① 面板视角：归档行不可见（这就是「从资源库移除」）
    db.expire_all()
    assert db.query(Resource).count() == 0

    # ② 历史视角：行还在，只是标了 cleaned（红线：归档不物理删除）
    with include_archived():
        db.expire_all()
        rows = list(db.scalars(select(Resource)).all())
    assert len(rows) == 2, "归档不物理删除，行必须还在"
    for r in rows:
        assert r.clean_state == "cleaned"
        assert r.cleaned_at is not None
        assert r.clean_reason == "B库内容已移除，归档资源库记录"
        assert r.share_status == "not_created"


def test_reconcile_keeps_rows_that_were_seen(db):
    b = _batch(db)
    _old_resources(db, 2)
    out = _run(main._reconcile_library(
        db, ["OLD_0", "OLD_1"], scan_complete=True, resumed=False,
        batch_id=b.id, allow_empty=False))
    db.commit()
    assert out["removed"] == 0
    db.expire_all()
    assert all(r.clean_state in (None, "") for r in db.scalars(select(Resource)).all())


def test_reconcile_does_not_instantiate_resource_entities():
    """源码级：对账那条路不许再出现「整表 ORM」写法（120.8 MiB 的来源）。"""
    i = MAIN_SRC.index("async def _reconcile_library")
    seg = MAIN_SRC[i:i + 9000]
    assert "db.scalars(select(Resource).where(" not in seg, \
        "对账又退回全量 ORM 实例化了（单个对象 5.14 KiB）"
    assert "Resource.clean_state: \"cleaned\"" in seg, "归档应改走批量 UPDATE"
    assert "synchronize_session=False" in seg


def test_scheduled_scan_calls_reconcile_without_allow_empty():
    """定时扫描必须对账，但绝不能传 allow_empty=True。"""
    i = MAIN_SRC.index("async def _scheduled_scan")
    seg = MAIN_SRC[i:i + 4200]
    assert "_reconcile_library(" in seg, "定时扫描必须调对账"
    assert "allow_empty=False" in seg
    assert "allow_empty=True" not in seg, "定时扫描不许用空批次清库"


def test_scheduled_scan_docstring_states_new_policy():
    """口径写进 docstring，避免以后又被人按旧注释改回去。"""
    i = MAIN_SRC.index("async def _scheduled_scan")
    seg = MAIN_SRC[i:i + 900]
    assert "定时扫描到 B 库没有了" in seg
    assert "不传 `allow_empty`" in seg
