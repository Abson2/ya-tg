"""v2.1.63：一部作品只生成一条分享链接，无论哪个渠道推送都复用这一条。

真机 2026-11-28（线上只读探针）：
  · 439 部作品每部都配 2 个群渠道（chat_id 只有 -1003749901817 / -1003708133864）；
  · 但只有 26 部真出 2 条不同链接，全部集中在 11-28 当天首建；
  · 每部双链作品的两条 published 任务 created_at 相差 0.5ms（同一轮 dispatch），
    pub 相差 20-30s，各建一条不同 guangyapan 链接。

根因：v2.1.57 的 `_existing_share_on_dir` 只查 Resource 表，而作品级建链只写
`task.share_url`、从不写回 Resource → 第二条渠道任务查不到 → 又建一条。
修法：按 work_key **跨 task** 查同作品已建的有效链接，查到就复用、不再新建。

本文件锁死：
  ① `_existing_work_share_url` 只返回同 work_key 的有效链接（最小 id / 非失效态）；
  ② 失效态（cleaned/invalid）不复用，空 work_key 不复用；
  ③ 不同作品绝不串台；
  ④ dispatch 的复用判定：自己有有效链接 → reused；自己没有但同作品有 → linked
     且把链接写到当前任务；都没有 → 才走新建（stats built）。
"""
from datetime import datetime, timezone

from app import share_center_v2 as sc2
from app.db import SessionLocal, migrate
from app.models import (
    SCPublishTask, SCPushList, SCPushListItem,
    SCLibraryVersion, Resource, Channel, Bot, AppConfig, QueueItem,
)
from sqlalchemy import delete

WIPED = (SCPublishTask, SCPushListItem, SCPushList, SCLibraryVersion,
         QueueItem, Resource, Channel, Bot, AppConfig)


def _setup():
    import pytest
    migrate()
    with SessionLocal() as db:
        for m in WIPED:
            db.execute(delete(m))
        db.commit()


def _task(db, wk, *, tid=None, url="", link_state="unknown", status="planned",
          list_item_id=0):
    t = SCPublishTask(work_key=wk, kind="first", status=status,
                      share_url=url, link_state=link_state,
                      list_item_id=list_item_id,
                      created_at=datetime(2026, 11, 28, 6, 0, 0,
                                          microsecond=tid or 0))
    db.add(t)
    db.flush()
    return t


# ---------------------------------------------------------------------------
# ① helper：按作品取有效链接
# ---------------------------------------------------------------------------
def test_work_share_url_reuses_own_work_link():
    _setup()
    with SessionLocal() as db:
        first = _task(db, "work:三喜临门", url="https://www.guangyapan.com/s/AAA",
                      link_state="active", status="published")
        second = _task(db, "work:三喜临门", url="", link_state="unknown")
        db.commit()
        assert sc2._existing_work_share_url(db, "work:三喜临门", second.id) == \
            "https://www.guangyapan.com/s/AAA"


def test_work_share_url_skips_invalid_states():
    _setup()
    with SessionLocal() as db:
        _task(db, "work:甲", url="https://s/cleaned", link_state="cleaned",
              status="cleaned")
        _task(db, "work:甲", url="https://s/invalid", link_state="invalid",
              status="invalid")
        _task(db, "work:甲", url="https://s/live", link_state="active",
              status="published")
        probe = _task(db, "work:甲")
        db.commit()
        # 失效态被跳过，取到仍有效的那条
        assert sc2._existing_work_share_url(db, "work:甲", probe.id) == "https://s/live"


def test_work_share_url_all_invalid_returns_empty():
    _setup()
    with SessionLocal() as db:
        _task(db, "work:乙", url="https://s/dead", link_state="cleaned",
              status="cleaned")
        probe = _task(db, "work:乙")
        db.commit()
        assert sc2._existing_work_share_url(db, "work:乙", probe.id) == ""


def test_work_share_url_no_cross_work_leak():
    _setup()
    with SessionLocal() as db:
        _task(db, "work:甲", url="https://s/A", link_state="active",
              status="published")
        probe = _task(db, "work:乙")
        db.commit()
        assert sc2._existing_work_share_url(db, "work:乙", probe.id) == ""


def test_work_share_url_empty_key_and_self_excluded():
    _setup()
    with SessionLocal() as db:
        t = _task(db, "work:丙", url="https://s/self", link_state="active")
        db.commit()
        assert sc2._existing_work_share_url(db, "") == ""
        # 排除自己后查不到别的任务 → 空（不能拿自己的链接当「同作品已有」）
        assert sc2._existing_work_share_url(db, "work:丙", t.id) == ""


# ---------------------------------------------------------------------------
# ② 源码判定：dispatch 确实按作品复用，而不是只认自己的 share_url
# ---------------------------------------------------------------------------
def test_dispatch_reuse_decision_is_work_scoped():
    """反证：锁住「自己没有链接但同作品有 → 复用」这段判定，防止回退成
    只认 task 自己的 share_url（真机 26 部双链的根因）。"""
    import inspect
    src = inspect.getsource(sc2.dispatch_ready_tasks)
    assert "work_link = _existing_work_share_url(" in src, src[:200]
    assert "if not reuse_link:" in src
    assert "stats[\"reused\" if reuse_link else \"linked\"] += 1" in src
    # 复用分支必须把链接写到当前任务，否则 second 渠道发出的消息没链接
    assert "task.share_url = work_link" in src
    # 反证：不能退回「只查自己的 share_url 就建新链」
    assert src.count("_existing_work_share_url") >= 1
