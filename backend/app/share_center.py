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
import logging
from datetime import datetime, timedelta

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from . import runtime_config
from .config import settings
from .db import json_dumps
from .guangya import GuangYaError, GuangYaShareClient
from .models import (AuditLog, DistributionEvent, QueueItem, Resource,
                     ShareRecord, ShareTask, utcnow)
from .utils import as_utc, series_dir_info, work_group_key

logger = logging.getLogger(__name__)

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
    """整剧首次分享任务（兼容旧数据；真相源优先 current_share_record）。"""
    return db.scalar(select(ShareTask).where(
        ShareTask.work_key == work_key, ShareTask.kind == "first",
        ShareTask.status != ST_CANCELLED).order_by(ShareTask.id).limit(1))


def current_share(db: Session, work_key: str) -> ShareRecord | None:
    """读取作品当前有效分享记录（追更复用链接的真相源）。"""
    return db.scalar(select(ShareRecord).where(
        ShareRecord.work_key == work_key, ShareRecord.current_flag == 1).limit(1))


def upsert_share_record(db: Session, res: Resource, *, share_id: str,
                        share_url: str, share_status: str = "active") -> ShareRecord:
    """建/更新当前有效分享记录：先把旧 current 作废(current_flag=0)保留历史，
    再建新 current（部分唯一索引保证同一作品仅一条 current=1）。"""
    wk = task_work_key(res)
    old = current_share(db, wk)
    if old and old.share_url == share_url and old.current_flag == 1:
        # 同一链接：仅刷新状态/核验时间，不制造新记录
        old.share_id = share_id or old.share_id
        old.share_status = share_status
        old.last_verified_at = utcnow()
        db.flush()
        return old
    if old:
        old.current_flag = 0
        db.flush()
    rec = ShareRecord(
        work_key=wk, resource_id=res.id,
        resource_type=res.media_type or "",
        share_dir_id=res.share_dir_id or "",
        share_id=share_id, share_url=share_url,
        share_status=share_status, current_flag=1)
    db.add(rec)
    db.flush()
    return rec


def retire_share_record(db: Session, work_key: str, *, reason: str = "") -> int:
    """B 库删除/分享失效：作废当前分享记录（保留历史），返回作废条数。"""
    cur = current_share(db, work_key)
    if not cur or not cur.current_flag:
        return 0
    cur.current_flag = 0
    cur.share_status = "cancelled"
    cur.cancelled_at = utcnow()
    if reason:
        cur.failure_reason = reason[:1000]
    db.flush()
    return 1


def classify_queue(db: Session, work_key: str, kind: str) -> str:
    """三类队列归属（设计文档 §4.2）：
    - followup：已有当前有效分享记录（追更/恢复，复用原链接）
    - stock：首次但由存量批次标记（share_tasks.queue_type 预设）
    - fresh：其余全新资源
    """
    if kind in ("update", "restore"):
        return "followup"
    if current_share(db, work_key):
        return "followup"
    return "fresh"


def _int_cfg(db: Session, key: str, default: int = 0) -> int:
    try:
        return max(0, int(runtime_config.get(db, key) or default))
    except (TypeError, ValueError):
        return default


def can_publish(db: Session, task: ShareTask) -> tuple[bool, str]:
    """统一发布门禁（设计文档 §4.7）：过不了就等待/阻塞，绝不提前调用分发中心。"""
    res = db.get(Resource, task.resource_id)
    if not res:
        return False, "资源不存在"
    # force（手动强推）绕过海报/识别闸门（v0.9.11 口径：手动强制动作），但仍校验 B 库目录存在。
    if task.kind != "force":
        if res.identify_status != "identified":
            return False, "资源未识别成功"
        if not (res.poster_path or "").strip():
            return False, "资源无海报（发布标准）"
    if task.kind in ("update", "restore"):
        cur = current_share(db, task.work_key)
        if not cur or not cur.share_url or cur.share_status != "active":
            return False, "追更资源无有效原分享链接"
    # 配额门禁：分队列按当日已提交数统计
    qtype = task.queue_type or "fresh"
    quota_key = {"stock": "stock_daily_quota", "fresh": "fresh_daily_quota",
                 "followup": "followup_daily_quota"}.get(qtype, "fresh_daily_quota")
    daily = _int_cfg(db, quota_key) or _int_cfg(db, "push_daily_limit")
    if daily > 0:
        # 用 func.date 归一到日期字符串，避开 SQLite naive/aware datetime 比较错误
        today_str = utcnow().date().isoformat()
        used = db.scalar(select(func.count(ShareTask.id)).where(
            ShareTask.queue_type == qtype,
            ShareTask.submitted_at.isnot(None),
            func.date(ShareTask.submitted_at) == today_str)) or 0
        if int(used) >= daily:
            return False, f"{qtype} 队列当日配额已满({daily})"
    return True, ""


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
        # 规则2（B库删除→镜像补齐重建）：旧的 first 任务已 cancelled，重新入库时要复活它，
        # 否则 dedup_key=work_key 唯一约束会挡住新建、作品永远无法重新分享推送。
        if existing.status == ST_CANCELLED:
            existing.status = ST_PLANNED
            existing.error = ""
            existing.share_id, existing.share_url = "", ""
            existing.share_status = "not_created"
            existing.attempts = 0
            existing.next_retry_at = None
            existing.submitted_at = None
            existing.published_at = None
            existing.queue_type = classify_queue(db, wk, kind)
            existing.resource_id = res.id
            existing.share_dir_id = res.share_dir_id or ""
            existing.share_scope = res.share_scope or existing.share_scope
            existing.channel_targets = json_dumps(channel_targets or json.loads(existing.channel_targets or "[]"))
            db.flush()
            return existing, True
        return existing, False

    parent = None
    if kind in ("update", "restore"):
        parent = find_first_task(db, wk)
    # 追更/恢复：优先用当前分享记录的链接，找不到再回退首次任务
    cur = current_share(db, wk)
    qtype = classify_queue(db, wk, kind)
    task = ShareTask(
        work_key=wk,
        kind=kind,
        queue_type=qtype,
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
    # 追更/恢复复用当前有效分享链接（规则1：更新复用原链接，真相源=share_records）
    reuse = cur if (cur and cur.share_url) else parent
    if reuse and reuse.share_url:
        task.share_id = reuse.share_id
        task.share_url = reuse.share_url
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

    # 同作品幂等复用真相源：当前有效分享记录（一部剧只一个有效分享）
    cur = current_share(db, task.work_key)
    if cur and cur.share_url:
        task.share_id, task.share_url = cur.share_id, cur.share_url
        task.share_status = "active"
        task.status = ST_READY
        db.commit()
        return task.share_url

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
    # 双写回资源缓存（兼容旧前端/导出）
    res.share_id, res.share_url = share_id, share_url
    res.share_status, res.share_duration = "active", settings.share_duration
    # 真相源：写当前有效分享记录（旧记录作废保留历史）
    try:
        upsert_share_record(db, res, share_id=share_id, share_url=share_url, share_status="active")
    except Exception:
        pass  # 记录写入失败不回滚已建分享（分享已真实存在）
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
    task.submitted_at = utcnow()
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
    # v1.2 发布节奏（§5.4）：相邻任务按配置错开 scheduled_at，避免一瞬间并发提交。
    # 0=不限（保持 v1.1 行为）；只对本次真正提交的任务顺延，不改门禁/退避逻辑。
    release_gap = _int_cfg(db, "share_release_interval_seconds")
    candidates = list(db.scalars(select(ShareTask).where(
        # ready 也要扫：force 任务在 ensure_task 时因资源已有 share_url 直接置 ready，
        # 若只扫 planned/failed 则这些 ready 任务永远不被提交，「立即推送」卡死。
        ShareTask.status.in_([ST_PLANNED, ST_READY, ST_FAILED]),
        or_(ShareTask.scheduled_at.is_(None), ShareTask.scheduled_at <= now),
    ).order_by(ShareTask.priority.desc(), ShareTask.id).limit(limit)).all())
    # failed 任务需过退避窗口
    due = []
    for t in candidates:
        # SQLite 读回的 DateTime 可能是 naive，统一 as_utc 再比较（项目通用红线）
        if t.status == ST_FAILED and t.next_retry_at and (as_utc(t.next_retry_at) or now) > now:
            continue
        if t.status == ST_FAILED and int(t.attempts or 0) >= max_attempts:
            continue
        due.append(t)

    # v1.2 发布节奏：release_gap>0 时，每提交一条就把紧邻的下一条 scheduled_at
    # 顺延一个间隔，形成「分享中心按 scheduled_at 释放」的节奏链；与分发中心
    # 已有的 2s 发送节流叠加、不冲突。默认 0＝不施加节奏（v1.1 行为不变）。
    paced = 0

    def _pace_after(idx: int) -> None:
        nonlocal paced
        if release_gap <= 0:
            return
        paced += 1
        future = due[idx + 1] if idx + 1 < len(due) else None
        if future and future.status in (ST_PLANNED, ST_READY) and not future.scheduled_at:
            future.scheduled_at = now + timedelta(seconds=release_gap * paced)
        db.commit()

    for idx, task in enumerate(due):
        # 被节奏顺延到未来的任务等下一轮释放，不在本轮提交
        if task.scheduled_at and (as_utc(task.scheduled_at) or now) > now:
            continue
        try:
            if not task.share_url:
                await build_share(db, task)
                stats["built"] += 1
            if task.status == ST_READY:
                # 统一发布门禁（§4.7）：未达标只阻塞等待，绝不提前调用分发中心
                ok, reason = can_publish(db, task)
                if not ok:
                    task.status = ST_PLANNED
                    task.error = f"发布门禁阻塞：{reason}"
                    task.scheduled_at = now + timedelta(seconds=60)
                    db.commit()
                    stats["skipped"] += 1
                    continue
                n = submit_to_dispatch(db, task)
                if n:
                    stats["submitted"] += n
                    _pace_after(idx)
                else:
                    stats["skipped"] += 1
        except Exception as exc:
            # 分享失败降级（设计文档 §4.6 / 场景10）：分享只是消息里的「获取资源」入口，
            # 建分享失败绝不能让整条推送卡死——对齐旧链「建分享失败仍发送消息」语义：
            # 立即标 ready（share_url 为空）照常提交，分发中心按空链接发无按钮正文。
            # 任务留 error 供前端诊断；下一轮 run_cycle 会自动重试建分享（task.status 已是
            # submitted/accepted/published，不再进 planned 候选——故另用 ShareRecord 修复
            # job 补建，这里不阻塞推送）。分享失败与推送失败彻底解耦。
            task.error = f"[建分享失败·降级无链接推送] {str(exc)[:600]}"
            task.status = ST_READY
            task.share_url = ""
            task.next_retry_at = None
            db.commit()
            stats["degraded"] = int(stats.get("degraded", 0)) + 1
            # 降级后继续走门禁+提交（不 continue），确保消息送达
            try:
                ok, reason = can_publish(db, task)
                if ok:
                    n = submit_to_dispatch(db, task)
                    if n:
                        stats["submitted"] = int(stats.get("submitted", 0)) + n
                        _pace_after(idx)
                else:
                    stats["skipped"] = int(stats.get("skipped", 0)) + 1
            except Exception:
                db.rollback()
    return stats


# P0 毒事件隔离：单条事件连续失败超过该次数就强制消费跳过，避免队头永久卡死。
# 2026-11-21 事故：事件 #6 identified|work:开封府 卡 8 小时，pending 按 id 升序，
# 它永远排第一 → 每轮都先在它身上等写锁超时 → 死亡螺旋。
MAX_EVENT_CONSUME_FAILURES = 3


def consume_resource_events(db: Session, limit: int = 100) -> dict:
    """消费资源库事件（设计文档 §3.7/§6.2）：把资源变化转成分享中心动作。

    - identified/restored：确保该作品有 first 任务；restored 场景 ensure_task 会复活
      被 cancelled 的旧任务（规则2 重建闭环）。
    - removed：兜底作废当前分享记录（生产者 _reconcile_library 已作废，这里幂等）。
    幂等：事件处理完置 consumed_at；同一事件重复消费不会重复建任务。

    P0 加固（2026-11-21）：
      1. 每条事件独立 try，失败只 rollback 自己并继续下一条，绝不中止整批；
      2. 失败计数写进 payload_json._consume_fail（不加列、幂等迁移零负担），
         连续失败超过 MAX_EVENT_CONSUME_FAILURES 就强制 mark_consumed 跳过，
         保证毒事件不会永久占住队头拖垮整个调度器。
    """
    from . import events as _ev
    stats = {"consumed": 0, "tasks_created": 0, "records_retired": 0,
             "skipped": 0, "skipped_poison": 0, "failed_once": 0}
    for ev in _ev.pending_resource_events(db, limit=limit):
        try:
            if ev.type == _ev.RESOURCE_REMOVED:
                try:
                    stats["records_retired"] += retire_share_record(
                        db, ev.work_key, reason="resource_removed_event")
                except Exception:
                    pass
            elif ev.type in (_ev.RESOURCE_NEW, _ev.RESOURCE_RESTORED):
                res = db.get(Resource, ev.resource_id) if ev.resource_id else None
                if not res or res.identify_status != "identified":
                    stats["skipped"] += 1
                else:
                    _, created = ensure_task(db, res, kind="first")
                    stats["tasks_created"] += 1 if created else 0
            _ev.mark_consumed(db, ev)
            stats["consumed"] += 1
        except Exception as exc:
            # 失败先回滚本条的部分写，再独立记录失败次数（二次异常不影响主流程）
            try:
                db.rollback()
            except Exception:
                pass
            stats["failed_once"] += 1
            fail_n = 1
            try:
                payload = json.loads(ev.payload_json or "{}") or {}
                fail_n = int(payload.get("_consume_fail", 0) or 0) + 1
                payload["_consume_fail"] = fail_n
                payload["_consume_last_error"] = str(exc)[:300]
                ev.payload_json = json.dumps(payload, ensure_ascii=False)
                if fail_n >= MAX_EVENT_CONSUME_FAILURES:
                    # 毒事件：连续失败到阈值，强制消费跳过，释放队头。
                    # 事件本身不删（保留可追溯），下游有定时全量对账兜底，
                    # 漏掉一次事件通知不会丢数据。
                    _ev.mark_consumed(db, ev)
                    stats["skipped_poison"] += 1
                    logger.error(
                        "[share_center] 事件 #%s 连续失败 %s 次，强制跳过避免队头死锁：%s",
                        ev.id, fail_n, exc)
                else:
                    db.commit()
            except Exception:
                try:
                    db.rollback()
                except Exception:
                    pass
            logger.warning("[share_center] 消费资源事件 #%s 失败（第 %s 次）：%s",
                           ev.id, fail_n, exc)
    return stats


def cancel_task(db: Session, task: ShareTask, *, cancel_queue: bool = True) -> int:
    """取消分享任务：终态 cancelled，作废当前分享记录（保留历史），并撤销未发送队列项。"""
    task.status = ST_CANCELLED
    try:
        retire_share_record(db, task.work_key, reason="share task cancelled")
    except Exception:
        pass
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


# ============================================================================
# v1.2 分享管理（设计文档 §5.6 / §5.7）：重建 / 节奏配置 / 追更关系 / 分享记录
# ============================================================================

def rebuild_task(db: Session, task: ShareTask) -> dict:
    """重建分享（§5.6 POST /api/share/tasks/{id}/rebuild）。

    把任务打回 planned 重新走「建分享→提交→推送」，不新建任务行（保留 dedup_key
    与追更链），因此一部剧仍只有一个 first。作废当前 ShareRecord（历史保留），
    清空链接/尝试次数/错误，撤销未发送队列项，下一轮 run_cycle 会重新建分享。
    返回受影响统计。
    """
    prev_url = task.share_url or ""
    retired = 0
    try:
        retired = retire_share_record(db, task.work_key, reason="manual_rebuild")
    except Exception as exc:
        logger.warning("[share_center] rebuild 作废分享记录失败 task#%s：%s", task.id, exc)
    task.status = ST_PLANNED
    task.share_id, task.share_url = "", ""
    task.share_status = "not_created"
    task.attempts, task.error = 0, ""
    task.next_retry_at, task.scheduled_at = None, None
    task.submitted_at, task.published_at = None, None
    revoked = 0
    for q in db.scalars(select(QueueItem).where(
            QueueItem.share_task_id == task.id,
            QueueItem.status.in_(["pending", "retry"]))).all():
        q.status = "failed"
        q.error = "分享任务已重建，旧队列项作废"
        q.status_ext = ST_CANCELLED
        revoked += 1
    # 资源缓存也清空，避免旧前端/导出读回过期链接
    res = db.get(Resource, task.resource_id)
    if res:
        res.share_id, res.share_url, res.share_status = "", "", "not_created"
    db.add(AuditLog(action="share_rebuilt", target_type="share_task", target_id=str(task.id),
                    detail=json_dumps({"work_key": task.work_key, "old_url": prev_url,
                                       "retired": retired})))
    db.commit()
    return {"ok": True, "retired_records": retired, "queue_revoked": revoked}


# 分享节奏/排序配置键（§5.4 发布排序与节奏；§10 已知优化项在 v1.2 落地）
SCHEDULE_KEYS = ("share_release_interval_seconds", "share_daily_limit", "push_daily_limit",
                 "stock_daily_quota", "fresh_daily_quota", "followup_daily_quota")


def share_schedule_get(db: Session) -> dict:
    """读取发布节奏配置（秒数/配额），供前端「发布节奏」面板展示。"""
    cfg: dict[str, int] = {}
    for key in SCHEDULE_KEYS:
        try:
            cfg[key] = max(0, int(runtime_config.get(db, key) or 0))
        except (TypeError, ValueError):
            cfg[key] = 0
    return cfg


def share_schedule_set(db: Session, values: dict) -> dict:
    """写入发布节奏配置：只接受白名单键、非负整数，脏值忽略不抛错。"""
    clean: dict[str, str] = {}
    for key in SCHEDULE_KEYS:
        if key not in values:
            continue
        try:
            clean[key] = str(max(0, int(values[key])))
        except (TypeError, ValueError):
            continue
    if clean:
        runtime_config.set_many(db, clean)
        db.commit()
    return share_schedule_get(db)


def followup_summary(db: Session) -> list[dict]:
    """追更管理（§5.4/§5.7）：按作品聚合分享任务，展示「已推 N 次 / 更新 M 次」。"""
    from sqlalchemy import case
    rows = db.execute(select(
        ShareTask.work_key,
        func.count(ShareTask.id).label("total"),
        func.sum(case((ShareTask.kind == "update", 1), else_=0)).label("updates"),
        func.min(ShareTask.id).label("first_id"),
        func.max(ShareTask.published_at).label("last_at"),
    ).group_by(ShareTask.work_key).order_by(func.count(ShareTask.id).desc())).all()
    out: list[dict] = []
    for wk, total, updates, first_id, last_at in rows:
        out.append({
            "work_key": wk,
            "total_pushes": int(total or 0),
            "update_count": int(updates or 0),
            "first_task_id": int(first_id or 0),
            "last_published_at": last_at.isoformat() if last_at else "",
            "current_share_url": (current_share(db, wk).share_url
                                  if current_share(db, wk) else ""),
        })
    return out


def share_records_list(db: Session, *, current_only: bool = False,
                       limit: int = 200) -> list[dict]:
    """分享记录管理（§5.2/§4.3 真相源）：当前与历史分享记录列表。"""
    q = select(ShareRecord)
    if current_only:
        q = q.where(ShareRecord.current_flag == 1)
    rows = db.scalars(q.order_by(ShareRecord.id.desc()).limit(min(limit, 500))).all()
    return [{
        "id": r.id, "work_key": r.work_key, "resource_type": r.resource_type,
        "share_dir_id": r.share_dir_id, "share_id": r.share_id,
        "share_url": r.share_url, "share_status": r.share_status,
        "current": bool(r.current_flag),
        "cancelled_at": r.cancelled_at.isoformat() if r.cancelled_at else "",
        "failure_reason": r.failure_reason,
        "last_verified_at": r.last_verified_at.isoformat() if r.last_verified_at else "",
        "created_at": r.created_at.isoformat() if r.created_at else "",
    } for r in rows]
