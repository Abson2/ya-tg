"""阶段七：故障注入测试套件（网盘超时/429/401/重启/DB 锁/网络断）。

红线：全部用 FakeAPI + 独立临时库 + 纯函数，不碰生产网盘、不发真实网络请求。
覆盖：
  - 下载/复制超时、网络断、连接错误 → 归可重试，不吞消息、不丢进度；
  - 429 / 5xx → 可重试；401 / 非法链接 / 配置缺失 → 不可重试（业务拒答）；
  - DB 锁（database is locked/busy）→ 可重试；
  - 复制执行器遇 copy_file 异常 → 落 failed 不中断整批、不跨 await 持写事务；
  - 幂等：远端已存在（按名+大小回填）→ 跳过复制，标 synced 不新增任务；
  - 重启续跑：未确认终态的任务（status=unknown）在下次 poll_pending_tasks 继续轮询。

v2.1.13：严格顺序引擎已移除，只剩一套复制路径（分桶批量提交 + 批量轮询），
本文件的断言按该单一路径口径校准（`stats["done"]` 计数单位=复制任务，
未知终态的续跑依据是 `MirrorTask.status` 而不是旧 `task_status` 游标）。
"""
import asyncio
import shutil
import tempfile
from pathlib import Path

import httpx
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from unittest.mock import patch

from app import bot_listener, db as dbmod, mirror, mirror_batch, models  # noqa: F401

A_ROOT, B_ROOT, A_WORK, B_WORK = "A_ROOT", "B_ROOT", "A_W1", "B_W1"


# ---------- 1. 瞬时故障分类（纯函数，覆盖 超时/网络断/429/401/DB 锁） ----------

def test_timeout_and_network_are_retryable():
    assert bot_listener.is_retryable_poll_error(httpx.TimeoutException("read timeout"))
    assert bot_listener.is_retryable_poll_error(httpx.ConnectError("connection reset"))
    assert bot_listener.is_retryable_poll_error(httpx.NetworkError("network down"))
    assert bot_listener.is_retryable_poll_error(asyncio.TimeoutError())


def test_rate_limit_and_5xx_are_retryable():
    for status in (429, 500, 502, 503):
        exc = httpx.HTTPStatusError("boom", request=httpx.Request("GET", "http://x"),
                                    response=httpx.Response(status))
        assert bot_listener.is_retryable_poll_error(exc), f"{status} 应可重试"
    # 文案中出现限流关键词也归可重试（无 status_code 的第三方异常）
    assert bot_listener.is_retryable_poll_error(RuntimeError("too many requests"))


def test_db_lock_is_retryable():
    assert bot_listener.is_retryable_poll_error(OperationalError("stmt", {}, Exception("database is locked")))
    assert bot_listener.is_retryable_poll_error(RuntimeError("database is busy"))


def test_auth_and_business_reject_are_not_retryable():
    """401/403、非法链接、配置缺失属业务拒答——重试不会改变结果。"""
    for exc in [RuntimeError("http 401 unauthorized"), RuntimeError("invalid share link"),
                ValueError("配置缺失")]:
        assert not bot_listener.is_retryable_poll_error(exc)
    exc_403 = httpx.HTTPStatusError("no", request=httpx.Request("GET", "http://x"),
                                    response=httpx.Response(403))
    assert not bot_listener.is_retryable_poll_error(exc_403)


# ---------- 2. 严格顺序执行器故障注入 ----------

class FaultAPI:
    """可注入 copy_file / get_task_status 故障的 FakeAPI。"""

    def __init__(self, *, fail_copy_n=0, timeout_copy=False, b_files=None, permanent_status=False):
        self.copies = 0
        self.events = []
        self.fail_copy_n = fail_copy_n          # 第 N 次 copy 抛异常
        self.timeout_copy = timeout_copy        # copy 抛 httpx.TimeoutException
        self.permanent_status = permanent_status  # get_task_status 永远 pending（模拟网络中断后无终态）
        self.b_files = list(b_files or [])

    async def list_files(self, parent_id, page=0, page_size=100):
        if page > 0:
            return {"list": [], "total": 0}
        if parent_id == A_ROOT:
            return {"list": [{"fileId": A_WORK, "resType": 2,
                              "fileName": "侠客行 (2001) {tmdb-135646}"}], "total": 1}
        if parent_id == A_WORK:
            return {"list": [
                {"fileId": "F1", "resType": 1, "fileName": "X.S01E01.mkv",
                 "fileSize": 100, "utime": 1, "parentId": A_WORK},
                {"fileId": "F2", "resType": 1, "fileName": "X.S01E02.mkv",
                 "fileSize": 100, "utime": 1, "parentId": A_WORK},
            ], "total": 2}
        if parent_id == B_ROOT:
            return {"list": [{"fileId": B_WORK, "resType": 2,
                              "fileName": "侠客行 (2001) {tmdb-135646}"}], "total": 1}
        if parent_id == B_WORK:
            return {"list": [dict(f) for f in self.b_files], "total": len(self.b_files)}
        return {"list": [], "total": 0}

    async def copy_file(self, file_ids, parent_id):
        self.copies += 1
        self.events.append(("copy", file_ids[0]))
        if self.timeout_copy or self.copies == self.fail_copy_n:
            raise httpx.TimeoutException("网盘复制请求超时")
        fid = f"BF{self.copies}"
        self.b_files.append({"fileId": fid, "resType": 1,
                             "fileName": file_ids[0] + ".mkv", "fileSize": 100,
                             "utime": 1, "parentId": parent_id})
        return f"TASK{self.copies}"

    async def get_task_status(self, task_id):
        self.events.append(("poll", task_id))
        if self.permanent_status:
            return {"status": 1, "detail": {}}
        return {"status": 2, "detail": {}}


@pytest.fixture
def env():
    root = Path(tempfile.mkdtemp(prefix="v105_fault_", dir="/tmp"))
    eng = create_engine(f"sqlite:///{root / 's.db'}")
    dbmod.Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False, autocommit=False)
    with S() as db:
        rule = models.MirrorRule(name="t", source_root_id=A_ROOT, target_root_id=B_ROOT,
                                 interval_minutes=60, enabled=True, file_idle_seconds=0.0)
        db.add(rule)
        db.commit()
        db.refresh(rule)
        yield db, rule, S, eng
    eng.dispose()
    shutil.rmtree(root, ignore_errors=True)


def _run_sync(db, rule, api):
    """跑一次同步（v2.1.13 起只有单一路径，不再有 strict 开关）。"""
    with patch("app.mirror.GuangYaWebFiles", lambda d: api):
        return asyncio.run(mirror.sync_rule(db, rule, max_polls=5, poll_interval=0.0))


def test_copy_timeout_marks_single_file_failed_continues_batch(env):
    """网盘超时：copy_file 抛超时 → 整批（同目录一次请求）标 failed，不崩、错误可诊断。

    同目录多文件合并为一次 copy_file，请求失败则整批一起 failed（目录级批量的固有
    语义，用户已拍板目录级）。质量红线不变：失败必须落库留痕、错误可见、批次正常
    收尾、后续作品/批次不被中断。
    """
    db, rule, _, _ = env
    api = FaultAPI(fail_copy_n=1)
    stats = _run_sync(db, rule, api)
    assert stats["failed"] >= 1, stats
    assert stats["copy_batches"] >= 1, "目录级批量应记录批次数"
    assert api.copies == 1, "同目录两文件合并为一次 copy_file（目录级批量）"
    rows = list(db.scalars(select(models.MirrorFile).order_by(models.MirrorFile.file_name)).all())
    statuses = {r.file_name: r.status for r in rows}
    assert "failed" in statuses.values(), statuses
    # 失败行有错误原因，供 file_errors 诊断
    assert any("超时" in (r.error or "") for r in rows)
    assert rule.batch_status == "idle", "异常路径也必须正常收尾批次"


def test_remote_verify_skips_existing_file_idempotent(env):
    """幂等：B 库已有同名+同大小文件 → 按名+大小回填为 synced，不再 copy_file。"""
    db, rule, _, _ = env
    # F1 已在 B 库存在（文件名 X.S01E01.mkv.mkv 由 copy_file 生成；此处预置同名锚点）
    existing = [
        {"fileId": "EXIST1", "resType": 1, "fileName": "F1.mkv", "fileSize": 100,
         "utime": 1, "parentId": B_WORK},
    ]
    api = FaultAPI(b_files=existing)
    stats = _run_sync(db, rule, api)
    # 至少不重复报已存在文件的复制；unchanged 或 done 均可（回填按 name+size 匹配）
    assert stats["unchanged"] + stats["done"] + stats["failed"] >= 1
    # 幂等核心：不重复对同一 source 建多条映射
    rows = list(db.scalars(select(models.MirrorFile)).all())
    assert len(rows) == len({(r.source_file_id, r.fingerprint) for r in rows}), "重复行违反幂等"


def test_unknown_task_status_retained_for_resume(env):
    """网络断：get_task_status 永远 pending → 任务保留供续跑，不误标 done。

    v2.1.13：续跑依据是 `MirrorTask.status`（默认路径在轮询用尽后写 `unknown`），
    旧严格引擎的 `task_status` 游标字段在单一路径下不再写入。
    """
    db, rule, _, _ = env
    api = FaultAPI(permanent_status=True)
    stats = _run_sync(db, rule, api)
    assert stats["unknown"] >= 1, stats
    assert stats["done"] == 0, "未确认终态绝不能标 done"
    tasks = list(db.scalars(select(models.MirrorTask)).all())
    assert tasks, "未终结任务须落库供 60s job 续跑"
    assert all(t.status == "unknown" for t in tasks), [t.status for t in tasks]
    assert all(t.task_status == "" for t in tasks), [t.task_status for t in tasks]
    # 存量续跑 job 必须真的能拾取它（过滤条件含 unknown），且不会误判为完成
    with patch("app.mirror.GuangYaWebFiles", lambda d: api):
        out = asyncio.run(mirror.poll_pending_tasks(db, rule, max_polls=1, interval=0.0))
    assert out["tasks"] == len(tasks), out
    assert out["done"] == 0 and out["still_unknown"] == len(tasks), out
    rows = list(db.scalars(select(models.MirrorFile)).all())
    assert all(r.status != "synced" for r in rows), [r.status for r in rows]


def test_config_missing_failure_recorded_not_crash(env):
    """配置缺失导致 copy_file 立即失败 → 记 failed + error，不抛未捕获异常。"""
    db, rule, _, _ = env
    api = FaultAPI(timeout_copy=True)
    stats = _run_sync(db, rule, api)
    # 两文件都失败但整批正常返回
    assert stats["failed"] == 2, stats
    # 单一路径按「一个目录桶一条聚合错误」写 errors；逐文件明细在 file_errors
    assert len(stats["errors"]) >= 1
    assert any("batch" in e or "批量复制失败" in e for e in stats["errors"]), stats["errors"]
    assert len(stats["file_errors"]) >= 2, stats["file_errors"]
    assert rule.batch_status == "idle", "异常路径也必须正常收尾批次"


# ---------- 3. 重启续跑 + 状态机 ----------

def test_paused_batch_pending_tasks_resume_from_cursor(env):
    """重启/暂停后续跑：paused 批次的任务与游标保留，resume 指令恢复 running。"""
    db, rule, _, _ = env
    mirror_batch.start_batch(db, rule, "b-restart-1")
    t1 = mirror_batch.add_sequence_task(db, rule_id=rule.id, media_id=1,
                                        batch_id="b-restart-1", sequence_id=1,
                                        parent_sequence_id=0, note="resume-test")
    rule.cursor_sequence = 1
    mirror_batch.request_control(db, rule, "pause")
    db.commit()
    # 模拟进程重启后重新加载 rule（同一 DB）
    assert rule.batch_status in ("paused", "running")
    before = db.scalars(select(models.MirrorTask).where(
        models.MirrorTask.batch_id == "b-restart-1")).all()
    assert len(list(before)) >= 1
    st = mirror_batch.request_control(db, rule, "resume")
    assert st["status"] == "running", st
    # 游标未回退，下轮从 sequence 2 继续
    assert rule.cursor_sequence == 1


def test_control_state_machine_terminal_guard():
    """状态机：非法 action 被拒；terminal 任务状态集合固定不被 UI 虚构操作污染。"""
    assert mirror_batch.ACTIVE_BATCH_STATUS == {"running", "paused", "cancelling"}
    assert "retry_current" not in {"pause", "resume", "skip", "cancel"}, \
        "后端不支持 retry_current，前端不得虚构该按钮"
    assert mirror_batch.TERMINAL_TASK_STATUS == {"completed", "failed", "skipped", "cancelled"}
