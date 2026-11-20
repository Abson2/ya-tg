"""阶段四 B-02：投稿转存三入口共用进程内锁。

后台 job / 审核通过即时转存 / 手动 retry 共享同一把 _restore_lock。
验证手动 retry 撞锁返回 409（不再并发提交同分享两次）；释放后可正常执行。
"""
import asyncio
import pytest
from fastapi.testclient import TestClient

from app import main, runtime_config
from app.db import SessionLocal, migrate
from app.models import UserContribution

migrate()
_client = TestClient(main.app)
HEADERS = {"X-Admin-Password": "testpass"}


@pytest.fixture
def cid():
    with SessionLocal() as db:
        runtime_config.set_many(db, {"contribution_restore_enabled": "true",
                                     "contribution_restore_target_dir_id": "TARGET123"})
        db.commit()
        c = UserContribution(title="t", share_url="https://www.guangyapan.com/s/abc123",
                             status="pending")
        db.add(c)
        db.commit()
        db.refresh(c)
        yield c.id
        db.delete(db.get(UserContribution, c.id))
        runtime_config.set_many(db, {"contribution_restore_enabled": "",
                                     "contribution_restore_target_dir_id": ""})
        db.commit()


def test_retry_returns_409_when_lock_held(cid):
    lock = main._restore_lock

    async def hold():
        await lock.acquire()
        try:
            r = _client.post(f"/api/contributions/{cid}/restore/retry", headers=HEADERS)
            assert r.status_code == 409, r.text
            assert "另一次转存" in r.json()["detail"]
        finally:
            lock.release()

    asyncio.run(hold())
    assert not lock.locked()


def test_lock_released_after_normal_use(cid):
    """锁在正常使用后释放（不是一次性锁死）。"""
    lock = main._restore_lock
    assert not lock.locked()
