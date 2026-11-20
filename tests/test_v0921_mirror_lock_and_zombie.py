"""v0.9.21：镜像锁进度心跳 + 僵尸任务自愈 + 待翻译原因区分。

真机诊断纠正：用户给的 P0-1（token 快照竞态 code=117）已被 v0.9.15 #267 修复
（_fresh_headers 每请求重读 DB + 117 强制刷新重试），本批不重复实现；只修确实
属实的锁无心跳 / 僵尸无自愈 / 文案误导三项。英文名解析链路与按名+大小回填不改动。
"""
import time
from datetime import timedelta

from app import main, mirror
from app.db import SessionLocal, migrate
from app.models import MirrorRule, MirrorTask
from app.utils import utcnow


# ---- 1. 镜像锁：进度心跳 ----
def test_lock_heartbeat_prevents_stale_reap_of_long_task():
    """长任务持有很久但持续打点，不应被当作残留回收。"""
    lock = main._TimedMirrorLock()
    lock.STALE_SECONDS = 1
    assert lock.acquire(owner="长剧复制") is True
    # 模拟长任务持续推进：多次 heartbeat 跨越 stale 阈值
    for _ in range(4):
        time.sleep(0.3)
        lock.heartbeat()
    # 另一个上下文仍拿不到锁（持有者一直有进展）
    assert lock.acquire(owner="手动同步") is False
    info = lock.info()
    assert info["busy"] is True and info["owner"] == "长剧复制"
    assert info["held_seconds"] >= 1  # 总持有很久
    assert info["idle_seconds"] < 1   # 但距最近心跳很短
    lock.release()


def test_lock_no_heartbeat_reaped_after_stale():
    """持有后无心跳、超过阈值 → 判残留强制回收。"""
    lock = main._TimedMirrorLock()
    lock.STALE_SECONDS = 0
    assert lock.acquire(owner="卡死任务") is True
    time.sleep(0.05)
    assert lock.acquire(owner="手动同步") is True
    info = lock.info()
    assert info["busy"] is True                      # 接管后真正持锁（旧实现此处锁是 unlocked）
    assert info["owner"] == "镜像任务(残留回收)"      # 内部接管标识，调用方随后 set_owner
    assert info["owner"] != "卡死任务"
    lock.release()
    assert lock.info()["busy"] is False              # finally release 能正常释放


def test_lock_heartbeat_noop_without_holder():
    """未持锁时 heartbeat 不应报错，也不产生 idle。"""
    lock = main._TimedMirrorLock()
    lock.heartbeat()
    assert lock.info()["busy"] is False


# ---- 2. 僵尸任务自愈 ----
def _mk_task(db, *, status, age_minutes, polls=0):
    r = MirrorRule(source_root_id="a", target_root_id="b", enabled=True)
    db.add(r)
    db.flush()
    t = MirrorTask(rule_id=r.id, status=status, polls=polls,
                   created_at=utcnow() - timedelta(minutes=age_minutes))
    db.add(t)
    db.commit()
    return t


def test_reap_marks_stale_running_failed_and_keeps_fresh():
    migrate()
    with SessionLocal() as db:
        stale = _mk_task(db, status="running", age_minutes=90, polls=0)
        fresh = _mk_task(db, status="running", age_minutes=2, polls=3)
        done = _mk_task(db, status="done", age_minutes=120)
        out = main.reap_stale_mirror_tasks(db, stale_minutes=30)
        assert out["reaped"] == 1 and out["scanned"] == 1
        db.refresh(stale)
        db.refresh(fresh)
        db.refresh(done)
        assert stale.status == "failed"
        assert "僵尸" in stale.error and "polls=0" in stale.error
        assert stale.finished_at is not None
        assert fresh.status == "running"       # 未超时不误伤
        assert done.status == "done"           # 已终结不动
        # 幂等：再跑一次不再重复收割
        assert main.reap_stale_mirror_tasks(db, stale_minutes=30)["reaped"] == 0
        # 清理测试数据
        db.delete(stale); db.delete(fresh); db.delete(done)
        db.delete(db.get(MirrorRule, fresh.rule_id))
        db.delete(db.get(MirrorRule, stale.rule_id))
        db.delete(db.get(MirrorRule, done.rule_id))
        db.commit()


def test_reap_pending_and_unknown_also_covered():
    migrate()
    with SessionLocal() as db:
        t1 = _mk_task(db, status="pending", age_minutes=60)
        t2 = _mk_task(db, status="unknown", age_minutes=60)
        out = main.reap_stale_mirror_tasks(db, stale_minutes=30)
        assert out["reaped"] == 2
        rules = [t1.rule_id, t2.rule_id]
        db.delete(t1); db.delete(t2)
        for rid in rules:
            db.delete(db.get(MirrorRule, rid))
        db.commit()


# ---- 3. 待翻译原因区分 ----
def test_untranslated_reason_distinguishes_states():
    locked = {"busy": True, "owner": "手动同步 / 改名", "held_seconds": 3600, "idle_seconds": 1800}
    r = main.untranslated_reason(locked, True, True)
    assert "被镜像任务占用" in r and "手动同步 / 改名" in r and "3600" in r
    assert "补齐 TMDB" not in r

    assert "配置缺失" in main.untranslated_reason({"busy": False}, False, False)
    assert "TMDB 未配置" in main.untranslated_reason({"busy": False}, False, True)
    assert "AI 未配置" in main.untranslated_reason({"busy": False}, True, False)
    assert "配置均正常" in main.untranslated_reason({"busy": False}, True, True)


# ---- 4. 诊断纠正的静态检查：P0-1 已在 v0.9.15 修，解析链路与回填不动 ----
def test_fresh_headers_and_no_double_implementation():
    gy = (mirror.__file__ and open(mirror.__file__, encoding="utf-8").read())
    main_src = open(main.__file__, encoding="utf-8").read()
    from app import guangya
    gg = open(guangya.__file__, encoding="utf-8").read()
    assert "_fresh_headers" in gg and "code == 117" in gg  # 竞态修复已在
    assert "set_progress_heartbeat" in gy and "_beat()" in gy
    assert "_job_mirror_task_reap" in main_src
    assert "untranslated_reason(" in main_src
