"""阶段五 C-02：投稿转存 restoring 残留超时自愈。

A-06 把 restoring 状态在 await 前 commit 落盘；若进程 SIGKILL / 容器强停，
该行永久停在 restoring，而 pick_pending 只扫 pending/retry_wait 永远捞不到。
后台每 30s 的 pick_pending 先调用 reap_stale_restoring 重置超时残留。
"""
from datetime import timedelta

import pytest

from app import contribution_restore
from app.db import SessionLocal, migrate
from app.models import ContributionRestore, UserContribution, utcnow

migrate()


@pytest.fixture
def restore_row():
    ids = []
    with SessionLocal() as db:
        c = UserContribution(title="C02测试", share_url="https://www.guangyapan.com/s/c02test",
                             status="approved")
        db.add(c)
        db.flush()
        r = ContributionRestore(contribution_id=c.id, status="restoring", attempts=1,
                                error="", last_response="", verify_note="",
                                created_at=utcnow(), updated_at=utcnow())
        db.add(r)
        db.commit()
        ids.append((c.id, r.id))
        yield r.id
        for cid, rid in ids:
            rr = db.get(ContributionRestore, rid)
            if rr:
                db.delete(rr)
            cc = db.get(UserContribution, cid)
            if cc:
                db.delete(cc)
        db.commit()


def test_fresh_restoring_not_reaped(restore_row):
    """刚进入 restoring 的任务（未超时）不得被误重置——在跑的真任务不能被腰斩。"""
    with SessionLocal() as db:
        out = contribution_restore.reap_stale_restoring(db)
        assert out["scanned"] >= 1
        row = db.get(ContributionRestore, restore_row)
        assert row.status == "restoring"


def test_stale_restoring_reset_to_retry_wait(restore_row):
    """超过阈值的 restoring 残留重置为 retry_wait，并被 pick_pending 捞到。"""
    with SessionLocal() as db:
        row = db.get(ContributionRestore, restore_row)
        row.updated_at = utcnow() - timedelta(minutes=contribution_restore.RESTORING_STALE_MINUTES + 5)
        db.commit()
        out = contribution_restore.reap_stale_restoring(db)
        assert out["reaped"] >= 1
        db.refresh(row)
        assert row.status == "retry_wait"
        assert "自愈" in (row.error or "")
        assert contribution_restore.pick_pending(db)  # 至少能捞到本行


def test_custom_stale_minutes(restore_row):
    """测试可传小阈值，避免测试等 30 分钟。"""
    with SessionLocal() as db:
        row = db.get(ContributionRestore, restore_row)
        row.updated_at = utcnow() - timedelta(minutes=2)
        db.commit()
        out = contribution_restore.reap_stale_restoring(db, stale_minutes=1)
        assert out["reaped"] >= 1
        db.refresh(row)
        assert row.status == "retry_wait"


def test_pick_pending_invokes_reap(restore_row, monkeypatch):
    """pick_pending 必须先做残留回收（SIGKILL 后唯一恢复入口）。"""
    called = {"n": 0}

    def fake_reap(db, *, stale_minutes=None):
        called["n"] += 1
        return {"scanned": 0, "reaped": 0}

    monkeypatch.setattr(contribution_restore, "reap_stale_restoring", fake_reap)
    with SessionLocal() as db:
        contribution_restore.pick_pending(db)
    assert called["n"] == 1
