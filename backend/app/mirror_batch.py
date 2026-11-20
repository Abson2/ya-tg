"""阶段二路线B：严格顺序镜像引擎的批次控制与序列持久化底座。

职责（低耦合，纯 DB 操作，不碰网络、不写队列）：
  - 批次 ID 生成、开启/收尾；
  - 暂停/恢复/跳过/取消控制指令的派发与消费（在任务边界生效，不中途腰斩复制）；
  - 序列任务落库、进度查询、重启续跑定位。

所有新列在 db.py:_COLUMNS 有 ALTER 定义，旧库自动兼容；旧 MirrorTask
task_status='' 视为历史记录，不参与新执行器。

设计红线：本模块不 import main、不 import mirror（避免循环依赖），
只依赖 models/db 的基础类型；由 mirror.sync_rule_strict 调用。
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import MirrorRule, MirrorTask
from .utils import utcnow

# 批次状态：idle/running/paused/cancelling/cancelled
ACTIVE_BATCH_STATUS = {"running", "paused", "cancelling"}
TERMINAL_TASK_STATUS = {"completed", "failed", "skipped", "cancelled"}


def new_batch_id(rule_id: int, at: datetime | None = None) -> str:
    """生成可读、无并发冲突的批次 ID：规则号 + UTC 时间戳 + 微秒。"""
    at = at or utcnow()
    return f"b{rule_id}-{at.strftime('%Y%m%d%H%M%S')}-{at.microsecond:06d}"


def start_batch(db: Session, rule: MirrorRule, batch_id: str | None = None) -> str:
    """开启批次：写 batch_id / status=running，清空上次控制指令与游标。"""
    rule.batch_id = batch_id or new_batch_id(rule.id)
    rule.batch_status = "running"
    rule.control_action = ""
    rule.cursor_sequence = 0
    db.commit()
    return rule.batch_id


def request_control(db: Session, rule: MirrorRule, action: str) -> dict:
    """派发控制指令（pause/resume/skip/cancel）。幂等，返回当前批次状态。

    - pause：仅在 running 时置指令；paused 时重复调用无副作用。
    - resume：paused/cancelling 时直接恢复 running 并清指令。
    - skip/cancel：仅 running 时可派发。
    指令由执行器在"任务边界"消费，不打断正在进行的 copy_file/轮询。
    """
    action = (action or "").strip().lower()
    if action not in {"pause", "resume", "skip", "cancel"}:
        raise ValueError(f"未知批次控制指令: {action}")

    if action == "resume":
        if rule.batch_status in {"paused", "cancelling"}:
            rule.batch_status = "running"
        rule.control_action = ""
    elif action == "pause":
        if rule.batch_status == "running":
            rule.control_action = "pause"
    elif action in {"skip", "cancel"}:
        if rule.batch_status == "running":
            rule.control_action = action
    db.commit()
    return batch_status(db, rule)


def batch_status(db: Session, rule: MirrorRule) -> dict:
    """轻量批次状态回显（不查任务表，供控制端点即时返回）。"""
    db.refresh(rule)
    return {
        "batch_id": rule.batch_id or "",
        "status": rule.batch_status or "idle",
        "control_action": rule.control_action or "",
        "cursor_sequence": int(rule.cursor_sequence or 0),
    }


def consume_control(db: Session, rule: MirrorRule) -> str:
    """读取并清空一次性指令（skip/cancel 消费一次；pause 由执行器改状态后清）。"""
    action = rule.control_action or ""
    if action:
        rule.control_action = ""
        db.commit()
    return action


def finish_batch(db: Session, rule: MirrorRule, *, status: str = "idle") -> None:
    """收尾：置终态，保留 batch_id 供历史查询，不清 cursor_sequence。"""
    rule.batch_status = status
    rule.control_action = ""
    db.commit()


def add_sequence_task(db: Session, *, rule_id: int, media_id: int,
                      batch_id: str, sequence_id: int, parent_sequence_id: int,
                      task_id: str = "", note: str = "") -> MirrorTask:
    """落库一个带序列的任务行。旧字段 status 同步置 running 以兼容既有查询/展示。"""
    task = MirrorTask(
        rule_id=rule_id, media_id=media_id, task_id=task_id,
        status="running", task_status="pending",
        batch_id=batch_id, sequence_id=sequence_id,
        parent_sequence_id=parent_sequence_id, retry_count=0,
        error=note or "",
    )
    db.add(task)
    db.commit()
    return task


def mark_sequence_status(db: Session, batch_id: str, sequence_id: int,
                         status: str, *, error: str = "") -> MirrorTask | None:
    """把指定序列任务落成终态（skip/cancel 用），带终态时间。

    sequence_id 可能尚无任务行（skip 发生在 add_sequence_task 之前），此时
    现场补一条 skipped/cancelled 占位行，保证任务序列完整可追溯、进度统计准确。
    """
    task = db.scalar(select(MirrorTask).where(
        MirrorTask.batch_id == batch_id,
        MirrorTask.sequence_id == sequence_id).limit(1))
    if task is None:
        task = MirrorTask(
            rule_id=0, media_id=0, task_id="", status=status,
            task_status=status, batch_id=batch_id, sequence_id=sequence_id,
            parent_sequence_id=0, retry_count=0, error=error or f"{status} by control",
            finished_at=utcnow())
        db.add(task)
    else:
        task.task_status = status
        task.status = status
        task.finished_at = utcnow()
        if error:
            task.error = error[:1000]
    db.commit()
    return task


def cancel_remaining(db: Session, batch_id: str) -> int:
    """批次取消：把该批次所有未终结序列任务标 cancelled，返回取消条数。"""
    pending = pending_sequences(db, batch_id)
    now = utcnow()
    for t in pending:
        t.task_status = "cancelled"
        t.status = "cancelled"
        t.finished_at = now
    if pending:
        db.commit()
    return len(pending)


def sequence_tasks(db: Session, batch_id: str) -> list[MirrorTask]:
    """按 sequence_id 升序返回某批次全部任务。"""
    return list(db.scalars(select(MirrorTask).where(
        MirrorTask.batch_id == batch_id,
    ).order_by(MirrorTask.sequence_id.asc())).all())


def pending_sequences(db: Session, batch_id: str) -> list[MirrorTask]:
    """返回尚未终结的任务（按序列升序），供严格顺序执行器推进/重启续跑。"""
    return [t for t in sequence_tasks(db, batch_id)
            if t.task_status not in TERMINAL_TASK_STATUS]


def resume_sequence(db: Session, rule: MirrorRule) -> MirrorTask | None:
    """重启续跑定位：返回当前批次第一个未终结任务；无未完成则返回 None。"""
    if not rule.batch_id:
        return None
    pending = pending_sequences(db, rule.batch_id)
    return pending[0] if pending else None


def batch_progress(db: Session, batch_id: str) -> dict:
    """批次进度统计，供 /api/mirror/batch/status。"""
    rows = sequence_tasks(db, batch_id)
    total = len(rows)
    done = sum(1 for t in rows if t.task_status in TERMINAL_TASK_STATUS)
    current = next((t for t in rows if t.task_status not in TERMINAL_TASK_STATUS), None)
    return {
        "batch_id": batch_id,
        "total": total,
        "done": done,
        "pending": total - done,
        "percent": int(done * 100 / total) if total else 0,
        "current_sequence": current.sequence_id if current else 0,
        "status_count": {s: sum(1 for t in rows if t.task_status == s)
                         for s in sorted({t.task_status for t in rows})},
    }
