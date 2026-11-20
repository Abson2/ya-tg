"""分享中心（四模块重构 阶段A/B/C）。

职责边界（FOUR_MODULE_REFACTOR_DESIGN.md）：
  - 以资源库「已识别」的资源为输入，负责分享生命周期：first/update/force/restore。
  - 一部剧只建一个分享（按 B 库目录 ID），更新复用原链接但生成独立推送任务（独立事件 ID）。
  - 提交（submitted）→ 分发中心原子领取（accepted）→ 发送 → 回执，任务接收有确认。
  - 不识别、不发 Telegram、不改 Bot/群/频道配置；分发与互动中心保持不动。

双写兼容：本模块提交任务时写 push_queue 的交接列（share_task_id/status_ext/push_reason），
旧链路（publisher.process_queue 直接扫 pending）仍可消费，保证阶段 A/B 期间零回归；
阶段 C 把消费端切到「只消费 accepted」后，交接确认才成为强约束。
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from . import runtime_config
from .config import settings
from .db import json_dumps
from .guangya import GuangYaError, GuangYaShareClient
from .models import (AuditLog, DistributionEvent, QueueItem, Resource,
                     ShareTask, utcnow)
from .utils import series_dir_info, work_group_key

# 状态机（强制项4：任务接收确认）
ST_PLANNED = "planned"
ST_BUILDING = "building"
ST_READY = "ready"
ST_SUBMITTED = "submitted"
ST_ACCEPTED = "accepted"
ST_PUBLISHING = "publishing"
ST_PUBLISHED = "published"
ST_FAILED = "failed"
ST_CANCELLED = "cancelled"

KIND_PRIORITY = {"force": 30, "first": 20, "update": 10, "restore": 5}
ACTIVE_STATUS = {ST_READY, ST_SUBMITTED, ST_ACCEPTED, ST_PUBLISHING}


def is_enabled(db: Session) -> bool:
    """灰度开关（设计文档 §8）：默认关闭=走旧链，零回归；开启=新链交接生效。

    存在 app_config，键 share_center_enabled=true/1/yes 视为开启。
    """
    try:
        v = str(runtime_config.get(db, "share_center_enabled") or "").strip().lower()
    except Exception:
        return False
    return v in ("1", "true", "yes", "on")


def task_work_key(res: Resource) -> str:
    """分享中心作品键：与资源库展示/识别/扫描事件统一用 utils.work_group_key。"""
    return work_group_key(res)


def shadow_register(db: Session, res: Resource, *, kind: str = "first",
                    channel_ids: list[int] | None = None,
                    event_id: int = 0) -> ShareTask | None:
    """双写期影子登记：识别/入队时同步建一条 ShareTask，但不改变 QueueItem 行为。

    灰度开关关闭时由 main 调用，保证分享中心任务表与新入队同步积累，
    待开关打开后 run_cycle 可从这些 planned 任务接管建分享与交接。
    任何异常都吞掉并 rollback 到保存点，绝不影响主入队链（影子不能拖垮主流程）。
    """
    try:
        task, _ = ensure_task(db, res, kind=kind, event_id=event_id,
                              channel_targets=[{"channel_id": c} for c in (channel_ids or [])])
        return task
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return None


def _ensure_share_scope(db: Session, res: Resource) -> None:
    """缺目录线索时现场补算 share_scope/share_dir_id（对齐 main._enqueue_ready 的兜底）。"""
    if (getattr(res, "share_dir_id", "") or "").strip():
        return
    info = series_dir_info(res.path_hint or "", getattr(res, "dir_chain_json", "[]"),
                           res.parent_id, res.media_type or "")
    res.share_scope = info["scope"]
    res.share_dir_id = info["dir_id"]
    res.share_dir_name = info["dir_name"]
    db.flush()


def find_first_task(db: Session, work_key: str) -> ShareTask | None:
    """整剧首次分享任务（分享链接真相源）。"""
    return db.scalar(select(ShareTask).where(
        ShareTask.work_key == work_key, ShareTask.kind == "first",
        ShareTask.status != ST_CANCELLED).order_by(ShareTask.id).limit(1))


def _dedup_key(kind: str, work_key: str, event_id: int) -> str | None:
    """幂等键：first 整剧一次；update/restore 按事件；force 不去重（允许多次强推）。"""
    if kind == "first":
        return work_key
    if kind in ("update", "restore") and event_id:
        return f"{kind}:{event_id}"
    return None


def ensure_task(
    db: Session,
    res: Resource,
    *,
    kind: str = "first",
    event_id: int = 0,
    channel_targets: list[dict] | None = None,
    force: bool = False,
) -> tuple[ShareTask, bool]:
    """幂等创建分享任务。返回 (task, created)。

    - first：dedup_key=work_key，同剧只一条；已存在则复用（force=True 时另建）。
    - update/restore：dedup_key=kind:event_id，同一变化事件只一条；parent 指向 first，复用其链接。
    - force：dedup_key=None，每次新建，priority 最高。
    """
    if kind not in ("first", "update", "force", "restore"):
        raise ValueError(f"未知分享任务类型：{kind}")
    _ensure_share_scope(db, res)
    wk = task_work_key(res)
    dkey = _dedup_key(kind, wk, event_id)
    if force:
        dkey = None
    existing = None
    if dkey:
        existing = db.scalar(select(ShareTask).where(ShareTask.dedup_key == dkey).limit(1))
    if existing and not force:
        return existing, False

    parent = None
    if kind in ("update", "restore"):
        parent = find_first_task(db, wk)
    task = ShareTask(
        work_key=wk,
        kind=kind,
        resource_id=res.id,
        share_dir_id=res.share_dir_id or "",
        share_scope=res.share_scope or "file",
        parent_task_id=parent.id if parent else 0,
        parent_event_id=event_id,
        status=ST_PLANNED,
        priority=KIND_PRIORITY.get(kind, 0),
        channel_targets=json_dumps(channel_targets or []),
        dedup_key=dkey,
    )
    # 追更/恢复复用首次分享链接（规则1：更新复用原链接）
    if parent and parent.share_url:
        task.share_id = parent.share_id
        task.share_url = parent.share_url
        task.share_status = "active"
        task.status = ST_READY
    elif (res.share_url or "").strip():
        # 资源行缓存命中（旧数据兼容），双写回任务
        task.share_id = res.share_id or ""
        task.share_url = res.share_url
        task.share_status = res.share_status or "active"
        task.status = ST_READY
    db.add(task)
    db.flush()  # autoflush=False：显式落库，供同批次后续任务查重看到
    return task, True


async def build_share(db: Session, task: ShareTask) -> str:
    """建分享（幂等）：优先复用已有链接，否则按 B 库目录 ID 调光鸭建分享。

    返回 share_url；失败抛 GuangYaError。状态 planned/building→ready。
    """
    if task.share_url:
        return task.share_url
    if task.status == ST_BUILDING:
        return ""  # 并发保护：另一次正在建
    res = db.get(Resource, task.resource_id)
    if not res:
        raise GuangYaError("分享任务关联的资源已不存在")
    task.status = ST_BUILDING
    task.attempts = int(task.attempts or 0) + 1
    db.commit()  # await 前落盘，不跨网络持写事务（项目红线）

    # 同目录幂等复用：先查 share_tasks，再查 resources 兄弟缓存
    if task.share_scope == "series_dir" and task.share_dir_id:
        sibling = db.scalar(select(ShareTask).where(
            ShareTask.share_dir_id == task.share_dir_id,
            ShareTask.share_url != "", ShareTask.id != task.id).limit(1))
        if sibling and sibling.share_url:
            task.share_id, task.share_url = sibling.share_id, sibling.share_url
            task.share_status = "active"
            task.status = ST_READY
            db.commit()
            return task.share_url
        from sqlalchemy import select as _select
        sib_res = db.scalar(_select(Resource).where(
            Resource.share_dir_id == task.share_dir_id,
            Resource.share_url != "", Resource.id != res.id).limit(1))
        if sib_res and sib_res.share_url:
            task.share_id, task.share_url = sib_res.share_id, sib_res.share_url
            task.share_status = "active"
            task.status = ST_READY
            db.commit()
            return task.share_url

    if not settings.auto_create_share:
        task.status = ST_FAILED
        task.error = "AUTO_CREATE_SHARE=false，且该资源尚无分享链接"
        db.commit()
        raise GuangYaError(task.error)

    target_id = task.share_dir_id if (task.share_scope == "series_dir"
                                      and task.share_dir_id) else res.file_id
    title = res.media_title or res.name or ""
    if task.share_scope == "series_dir" and res.season:
        suffix = f" 第{int(res.season)}季"
        if title and suffix not in title:
            title = f"{title}{suffix}"
    client = GuangYaShareClient(db)
    result = await client.create(target_id, title, settings.share_duration)
    data = result.get("data") or {}
    share_id = str(data.get("shareId") or data.get("share_id") or data.get("id") or "")
    share_url = str(data.get("shareUrl") or data.get("share_url") or data.get("url") or "")
    if not share_url:
        task.status = ST_FAILED
        task.error = "分享接口返回成功但未找到 shareUrl"
        db.commit()
        raise GuangYaError(task.error)
    task.share_id = share_id
    task.share_url = share_url
    task.share_status = "active"
    task.status = ST_READY
    task.error = ""
    # 双写回资源缓存（兼容旧前端/导出；真相源仍是 share_tasks）
    res.share_id, res.share_url = share_id, share_url
    res.share_status, res.share_duration = "active", settings.share_duration
    db.add(AuditLog(action="share_created", target_type="share_task", target_id=str(task.id),
                    detail=json_dumps({"work_key": task.work_key, "kind": task.kind,
                                       "file_id": target_id, "share_id": share_id})))
    db.commit()
    return share_url


def _upsert_queue_item(db: Session, task: ShareTask, channel_id: int) -> QueueItem:
    """写交接队列项：复用同 (resource_id, channel_id) 的旧项并重置，或新建。

    这样更新推送虽是独立 ShareTask（独立事件 ID），物理上仍复用 QueueItem 行，
    不破坏 push_queue 的 uq_resource_channel 唯一约束。
    """
    q = db.scalar(select(QueueItem).where(
        QueueItem.resource_id == task.resource_id,
        QueueItem.channel_id == channel_id).limit(1))
    if q:
        q.status = "pending"  # 旧链兼容：仍从 pending 消费
        q.attempts, q.error = 0, ""
        q.next_retry_at, q.sent_at = None, None
        q.share_task_id = task.id
        q.push_reason = task.kind
        q.event_id = task.parent_event_id
        q.status_ext = ST_SUBMITTED
        q.accept_worker = ""
        q.accepted_at = None
    else:
        q = QueueItem(resource_id=task.resource_id, channel_id=channel_id,
                      status="pending", share_task_id=task.id,
                      push_reason=task.kind, event_id=task.parent_event_id,
                      status_ext=ST_SUBMITTED)
        db.add(q)
    return q


def submit_to_dispatch(db: Session, task: ShareTask) -> int:
    """ready → submitted：把任务提交给分发中心，写 channel_targets 对应的队列项。

    返回本次新增/重置的队列项数量。分发中心领取时才置 accepted（强制项4）。
    """
    if task.status not in (ST_PLANNED, ST_READY, ST_SUBMITTED, ST_FAILED):
        raise ValueError(f"任务状态 {task.status} 不可提交（需 planned/ready/submitted/failed）")
    # 分享失败也允许提交（降级为无链接推送，对齐 publisher 现有「建分享失败仍发送」语义）；
    # 消息正文/按钮是否带链接由分发中心按 share_url 是否为空决定，分享中心不反建。
    targets = json.loads(task.channel_targets or "[]")
    if not targets:
        return 0
    ids = []
    for t in targets:
        ch_id = int(t.get("channel_id") or 0)
        if ch_id:
            q = _upsert_queue_item(db, task, ch_id)
            ids.append(q.id)
    task.status = ST_SUBMITTED
    task.channel_targets = json.dumps(targets, ensure_ascii=False)
    # 溯源：关联事件 → 分享任务 + 推送队列项
    if task.parent_event_id:
        ev = db.get(DistributionEvent, task.parent_event_id)
        if ev:
            ev.share_task_id = task.id
            ev.push_task_ids = json.dumps(ids)
    # 提交是跨 session 的交接动作，必须持久化，供分发中心另一 Session 领取
    db.commit()
    return len(ids)


def claim_tasks(db: Session, worker: str, limit: int = 20) -> list[int]:
    """分发中心原子领取：submitted → accepted（强制项4）。

    逐条条件 UPDATE（WHERE status_ext=submitted），返回真正领到的 share_task_id。
    单进程仍由 publisher._queue_lock 二次保险；多进程靠条件更新互斥。
    """
    claimed: list[int] = []
    rows = db.scalars(select(QueueItem).where(
        QueueItem.status_ext == ST_SUBMITTED,
        QueueItem.status.in_(["pending", "retry"])).order_by(QueueItem.id).limit(limit)).all()
    now = utcnow()
    for q in rows:
        q.status_ext = ST_ACCEPTED
        q.accept_worker = worker
        q.accepted_at = now
        task = db.get(ShareTask, q.share_task_id) if q.share_task_id else None
        if task and task.status == ST_SUBMITTED:
            task.status = ST_ACCEPTED
        claimed.append(q.share_task_id or q.id)
    if claimed:
        db.commit()
    return claimed


def share_url_for_queue_item(db: Session, q: QueueItem) -> str:
    """分发中心只读：从已交接的 ShareTask 取分享链接，不再自己建分享。

    开关开启且队列项带 share_task_id 时返回任务 URL（可能为空=降级无链接）；
    否则返回 None 表示「不由分享中心管」，调用方走旧 ensure_share。
    """
    if not q.share_task_id:
        return None
    task = db.get(ShareTask, q.share_task_id)
    if not task:
        return None
    return task.share_url or ""


def mark_result(db: Session, share_task_id: int, *, ok: bool,
                message_id: str = "", error: str = "") -> None:
    """分发中心回执 → 分享任务终态。"""
    task = db.get(ShareTask, share_task_id)
    if not task:
        return
    now = utcnow()
    if ok:
        task.status, task.error = ST_PUBLISHED, ""
        task.published_at = now
    else:
        task.status, task.error = ST_FAILED, str(error)[:1500]
    db.commit()


async def run_cycle(db: Session, limit: int = 20) -> dict:
    """分享中心调度周期：planned → 建分享 → 提交分发中心（submitted）。

    由 scheduler / 手动 API 调用。排序：priority 高优先，其次 scheduled_at/created_at。
    建分享失败的任务按退避留到下一轮重试（attempts 上限由 MAX_SHARE_ATTEMPTS 控制），
    不阻断其它任务。返回统计供前端任务中心展示。
    """
    from .utils import backoff_seconds
    max_attempts = 8
    stats = {"built": 0, "submitted": 0, "skipped": 0, "failed": 0}
    now = utcnow()
    candidates = list(db.scalars(select(ShareTask).where(
        ShareTask.status.in_([ST_PLANNED, ST_FAILED]),
        ShareTask.kind != "force",  # force 由显式 API 立即处理，不进后台轮转
    ).order_by(ShareTask.priority.desc(), ShareTask.id).limit(limit)).all())
    # failed 任务需过退避窗口
    due = []
    for t in candidates:
        if t.status == ST_FAILED and t.next_retry_at and t.next_retry_at > now:
            continue
        if t.status == ST_FAILED and int(t.attempts or 0) >= max_attempts:
            continue
        due.append(t)
    for task in due:
        try:
            if not task.share_url:
                await build_share(db, task)
                stats["built"] += 1
            if task.status == ST_READY:
                n = submit_to_dispatch(db, task)
                if n:
                    stats["submitted"] += n
                else:
                    stats["skipped"] += 1
        except GuangYaError as exc:
            task.status = ST_FAILED
            task.error = str(exc)[:1000]
            task.next_retry_at = now + timedelta(seconds=backoff_seconds(int(task.attempts or 0)))
            db.commit()
            stats["failed"] += 1
        except Exception as exc:
            task.status = ST_FAILED
            task.error = f"分享任务异常：{str(exc)[:800]}"
            db.commit()
            stats["failed"] += 1
    return stats


def cancel_task(db: Session, task: ShareTask, *, cancel_queue: bool = True) -> int:
    """取消分享任务：终态 cancelled，并可撤销未发送的交接队列项。"""
    task.status = ST_CANCELLED
    cancelled = 0
    if cancel_queue and task.share_url:
        for q in db.scalars(select(QueueItem).where(
                QueueItem.share_task_id == task.id,
                QueueItem.status.in_(["pending", "retry"]))).all():
            q.status = "failed"
            q.error = "分享任务已取消"
            q.status_ext = ST_CANCELLED
            cancelled += 1
    db.commit()
    return cancelled
