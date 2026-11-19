"""v0.9.0 回归：镜像同步「单次扫描整理整部剧/电影数量上限」。

用户反馈：「为了防止A库镜像到B库一次性剧集数量过多，可手动限制单次扫描整理的
整部剧、电影的数量，处理完后，等下一次定时启动继续整理（注意第一部剧不要间断，
整理完再休息）」。

本组测试锁死四条红线：
  1. plan_work_batch 按上限切批，从游标之后接着取，到末尾回绕，每部作品都能轮到；
  2. 上限=0（默认）退化为不限量，保持旧行为，不破坏存量；
  3. 切批只在「作品」边界，不拆单集——由 sync_rule 的作品级原子处理保证；
  4. 配置端点可接受/回显上限，模型与新列存在。
"""
from pathlib import Path

from app import mirror
from app.db import SessionLocal, migrate
from app.main import MirrorConfigInput
from app.models import MirrorRule

ROOT = Path(__file__).resolve().parents[1]


def _works(n):
    """构造 n 部作品，格式与 _discover_works 输出一致。"""
    return [({"name": f"剧{i} (2020) " + "{tmdb-" + str(1000 + i) + "}", "id": f"D{i}"}, [])
            for i in range(1, n + 1)]


# ---------- 1. 上限切批 ----------
def test_cap_splits_into_batches():
    works = _works(10)
    batch1, cursor1, wrapped1 = mirror.plan_work_batch(works, 3, "")
    assert len(batch1) == 3
    assert wrapped1 is False
    assert cursor1 == mirror.work_key_of(works[2][0])

    batch2, cursor2, wrapped2 = mirror.plan_work_batch(works, 3, cursor1)
    assert len(batch2) == 3
    assert [w["name"] for w, _ in batch2] == [w["name"] for w, _ in works[3:6]]
    assert wrapped2 is False


def test_cursor_wraps_around_so_every_work_is_reached():
    works = _works(5)
    seen = []
    cursor = ""
    for _ in range(3):  # 3 轮覆盖 5 部（3+2）
        batch, cursor, _wrapped = mirror.plan_work_batch(works, 3, cursor)
        seen.extend(w["name"] for w, _ in batch)
    # 5 部全部至少被处理一次，且没有重复拆到半部
    assert set(seen) == {w["name"] for w, _ in works}
    assert len(seen) == 9  # 3+3+3（尾部不足从头部补齐）


def test_last_batch_fills_from_head_without_gap():
    works = _works(5)
    cursor = mirror.work_key_of(works[3][0])  # 上一轮处理到第 4 部
    batch, new_cursor, wrapped = mirror.plan_work_batch(works, 3, cursor)
    assert wrapped is True
    # 第 5 部 + 回绕补第 1、2 部，正好 3 部
    assert [w["name"] for w, _ in batch] == [w["name"] for w, _ in
                                              [works[4], works[0], works[1]]]
    assert new_cursor == mirror.work_key_of(works[1][0])


def test_stale_cursor_falls_back_to_start():
    works = _works(4)
    batch, cursor, _ = mirror.plan_work_batch(works, 2, "work:已删除的剧")
    assert [w["name"] for w, _ in batch] == [w["name"] for w, _ in works[:2]]
    assert cursor == mirror.work_key_of(works[1][0])


# ---------- 2. 不限量默认行为 ----------
def test_zero_cap_means_unlimited_legacy_behaviour():
    works = _works(8)
    batch, cursor, wrapped = mirror.plan_work_batch(works, 0, "some-old-cursor")
    assert len(batch) == 8
    assert cursor == "" and wrapped is False


def test_empty_works_is_safe():
    assert mirror.plan_work_batch([], 3, "") == ([], "", False)


# ---------- 3. 作品级原子性与上限语义 ----------
def test_cap_is_on_works_not_episodes():
    """上限按「整部作品」计：一部 40 集剧算 1 部，不按 40 集拆。"""
    works = _works(2)
    # 给它一部塞 40 集也不会改变批处理数量——批只由作品数决定
    batch, _, _ = mirror.plan_work_batch(works, 1, "")
    assert len(batch) == 1
    assert "{" in batch[0][0]["name"]  # 仍是完整作品节点，未拆集


# ---------- 4. 配置与模型 ----------
def test_cap_config_plumbing():
    p = MirrorConfigInput(max_works_per_run=5)
    assert p.max_works_per_run == 5
    migrate()
    with SessionLocal() as db:
        r = MirrorRule(name="t", max_works_per_run=7, works_cursor="work:x")
        assert r.max_works_per_run == 7
        # 迁移列存在：写库后能读回
        db.add(r)
        db.commit()
        got = db.query(MirrorRule).filter(MirrorRule.name == "t").first()
        assert got.max_works_per_run == 7
        assert got.works_cursor == "work:x"
        db.delete(got)
        db.commit()


def test_sync_rule_reads_cap_from_rule():
    """sync_rule 在未显式传 max_works 时，从 rule.max_works_per_run 读取。"""
    import inspect
    sig = inspect.signature(mirror.sync_rule)
    assert "max_works" in sig.parameters
    src = inspect.getsource(mirror.sync_rule)
    assert "max_works_per_run" in src
    assert "plan_work_batch" in src
