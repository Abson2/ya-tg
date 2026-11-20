"""阶段四 B-03：镜像锁残留接管必须真正持有底层锁 + fencing token 防过期持有者回写。

v0.9.21 的接管分支只覆盖元数据、未重新 acquire 底层 threading.Lock，且无 owner
概念——误判残留时旧持有者迟到的 release()/heartbeat() 会误放新持有者的锁、重置
其空闲计时。本测试用 contextvars 隔离模拟「不同任务里的新旧持有者」。
"""
import contextvars
import time

from app import main


def _acquire(lock, owner=""):
    return lock.acquire(blocking=False, owner=owner)


def test_takeover_reacquires_underlying_lock():
    """接管后底层锁必须真正被新持有者拿到：第三方 acquire 应失败（未超时）。"""
    lock = main._TimedMirrorLock()
    lock.STALE_SECONDS = 0
    assert lock.acquire(owner="卡死任务") is True
    time.sleep(0.01)
    assert lock.acquire(owner="接管者") is True
    # 接管后刚打点，STALE 拉长；第三方此刻拿不到锁 = 底层锁确实被持有
    lock.STALE_SECONDS = 3600
    assert lock.acquire(owner="第三方") is False
    assert lock.info()["busy"] is True
    lock.release()


def test_fence_increments_on_takeover():
    """每次 acquire（含接管）代次递增，info 暴露 fence 供诊断。"""
    lock = main._TimedMirrorLock()
    lock.STALE_SECONDS = 0
    assert lock.acquire(owner="A") is True
    f1 = lock.info()["fence"]
    time.sleep(0.01)
    assert lock.acquire(owner="B") is True
    assert lock.info()["fence"] == f1 + 1
    lock.release()


def test_stale_holder_late_release_cannot_unlock_new_holder():
    """旧持有者在自己的上下文里迟到 release，不得释放新持有者的锁。"""
    lock = main._TimedMirrorLock()
    lock.STALE_SECONDS = 0
    # 在独立上下文里让旧持有者拿锁（其 fencing token 留在该上下文）
    old_ctx = contextvars.copy_context()
    old_ctx.run(_acquire, lock, "旧持有者")
    time.sleep(0.01)
    # 新持有者在同一主线程（新上下文）接管
    assert lock.acquire(owner="新持有者") is True
    new_owner_fence = lock.info()["fence"]

    # 旧持有者在其原上下文里迟到 release——应被 fencing 拒绝
    old_ctx.run(lock.release)
    assert lock.info()["busy"] is True, "旧持有者迟到 release 不应释放新持有者的锁"
    assert lock.info()["fence"] == new_owner_fence
    assert lock.info()["owner"] in ("新持有者", "镜像任务(残留回收)")

    # 新持有者自己的 release 仍生效（其上下文 token 与当前 fence 一致）
    lock.release()
    assert lock.info()["busy"] is False


def test_stale_holder_late_heartbeat_cannot_reset_idle():
    """旧持有者迟到 heartbeat，不得重置新持有者的空闲计时。"""
    lock = main._TimedMirrorLock()
    lock.STALE_SECONDS = 0
    old_ctx = contextvars.copy_context()
    old_ctx.run(_acquire, lock, "旧持有者")
    time.sleep(0.01)
    assert lock.acquire(owner="新持有者") is True
    # 让新持有者空闲一段时间
    time.sleep(0.2)
    idle_before = lock._idle_seconds()
    # 旧持有者迟到 heartbeat
    old_ctx.run(lock.heartbeat)
    assert lock._idle_seconds() >= idle_before - 0.02, "旧持有者 heartbeat 不应重置新持有者 idle"
    lock.release()


def test_normal_single_holder_release_unaffected():
    """正常单持有者路径：acquire→release 往返不受 fencing 影响。"""
    lock = main._TimedMirrorLock()
    lock.STALE_SECONDS = 3600
    assert lock.acquire(owner="正常任务") is True
    assert lock.info()["busy"] is True
    lock.release()
    assert lock.info()["busy"] is False
