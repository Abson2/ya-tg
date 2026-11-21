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

from sqlalchemy import func, or_, select, update
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
    """旧链周期是否可驱动（v2.1.1：前后端仅用一套分享中心）。

    本模块在 v2.1.1 起**不再是独立的分享中心**，只作为内部工具库被调用：
    share_url_for_queue_item / ensure_share / claim_tasks / 状态常量 /
    shadow_register / retire_share_record。分享中心的数据体系与调度引擎
    统一由 share_center_v2（sc_* 新表）承担。

    因此旧链**永远不独立驱动 run_cycle**——它只有在分享中心总开关关闭时
    才作为排障兜底被调用（见 publisher._process_queue_impl 的分流）。
    """
    try:
        v = str(runtime_config.get(db, "share_center_enabled") or "").strip().lower()
    except Exception:
        return False
    return v in ("1", "true", "yes", "on")


def is_paused(db: Session) -> bool:
    """阶段六全局急停：publish_paused=true 时 run_cycle 空转。

    保守默认 false。只拦新一轮「建分享+提交」，不撤销已在途任务、不触碰
    分发中心发送核心（_send_telegram/_build_message/process_queue）。
    """
    try:
        v = str(runtime_config.get(db, "publish_paused") or "").strip().lower()
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
        # 阶段五：失效/已清理/作废的链接不再复用（inactive/cleaned/retired），
        # 让追更改走"重建新分享"，避免把失效链接继续推给用户。
        from . import share_link as _sl
        if str(cur.link_state or _sl.LINK_ACTIVE) in _sl.NON_REUSABLE_LINK_STATES:
            return False, f"追更原链接已失效（{cur.link_state}），需重建分享"
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
    task.building_at = utcnow()  # v1.5.1：building 进入时间，超时自愈用
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
    # 阶段四：统一走光鸭建分享解析适配器（与 publisher/main 口径一致）
    from .guangya import create_share_parsed
    share_id, share_url = await create_share_parsed(client, target_id, title, settings.share_duration)
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

    阶段四真原子改造：旧版先 select 再逐条改属性最后 commit（select-then-modify），
    单进程靠 publisher._queue_lock 能过，但多进程两个 worker 同时 select 会拿到同一批、
    重复领取。新版用**条件 UPDATE 抢占**：
      1. select 候选 id（只读，无锁）；
      2. 一条 `UPDATE push_queue SET status_ext=accepted, worker=?, at=?
         WHERE id IN (...) AND status_ext=submitted AND status IN (pending,retry)`，
         SQLite 单写者保证这条语句原子——两个 worker 并发时只有一个的 rowcount>0；
      3. 只更新真正抢到的行的 ShareTask 状态；rowcount=0 的候选直接跳过。
    返回真正领到的 share_task_id（可能少于 limit）。
    """
    rows = db.scalars(select(QueueItem).where(
        QueueItem.status_ext == ST_SUBMITTED,
        QueueItem.status.in_(["pending", "retry"])).order_by(QueueItem.id).limit(limit)).all()
    if not rows:
        return []
    candidate_ids = [q.id for q in rows]
    by_id = {q.id: q for q in rows}
    now = utcnow()
    # 条件 UPDATE 抢占：只有仍是 submitted 的行才被本 worker 拿下（真原子）。
    # accept_worker=worker 作为「归属标记」——下面据此精确回查哪些行是本 worker 抢到的
    # （rowcount 只给总数，无法区分候选集部分重叠时具体哪些行被别人先抢走）。
    db.execute(
        update(QueueItem)
        .where(QueueItem.id.in_(candidate_ids),
               QueueItem.status_ext == ST_SUBMITTED,
               QueueItem.status.in_(["pending", "retry"]))
        .values(status_ext=ST_ACCEPTED, accept_worker=worker, accepted_at=now))
    # 回查归属：同一事务内，只有被打上本 worker 标记的行才是真正领到的
    won_ids = set(db.scalars(select(QueueItem.id).where(
        QueueItem.id.in_(candidate_ids),
        QueueItem.status_ext == ST_ACCEPTED,
        QueueItem.accept_worker == worker)).all())
    claimed: list[int] = []
    for qid in candidate_ids:
        if qid not in won_ids:
            continue  # 被别的 worker 先抢走，跳过
        q = by_id[qid]
        q.status_ext = ST_ACCEPTED  # 同步 ORM 内存态（条件 UPDATE 绕过 ORM）
        task = db.get(ShareTask, q.share_task_id) if q.share_task_id else None
        if task and task.status == ST_SUBMITTED:
            task.status = ST_ACCEPTED
        claimed.append(q.share_task_id or qid)
    if claimed:
        db.commit()
    else:
        # 一行都没抢到：回滚本次只读事务，不改动任何东西
        db.rollback()
    return claimed


def share_url_for_queue_item(db: Session, q: QueueItem) -> str:
    """分发中心只读：从已交接的 ShareTask 取分享链接，不再自己建分享。

    开关开启且队列项带 share_task_id 时返回任务 URL（可能为空=降级无链接）；
    否则返回 None 表示「不由分享中心管」，调用方走旧 ensure_share。
    """
    # 分享中心 v2：新模块不写旧 ShareTask，队列项以 event_id 反向引用 sc_publish_tasks。
    if getattr(q, "push_reason", "") == "sc_v2" and getattr(q, "event_id", 0):
        from .models import SCPublishTask
        v2_task = db.get(SCPublishTask, q.event_id)
        return (v2_task.share_url or "") if v2_task else ""
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
    stats = {"built": 0, "submitted": 0, "skipped": 0, "failed": 0}
    if is_paused(db):
        stats["paused"] = True
        return stats
    max_attempts = 8
    now = utcnow()
    # v1.5.1 P0：building 僵尸自愈。build_share 先把 building 落盘（328行），
    # 后续建分享异常 rollback 救不回已提交状态，而本函数候选只扫 planned/ready/failed，
    # building 任务永久卡死（真机 task4 attempts=1354）。这里在取候选前把超时未完成的
    # building 重置 planned，让它重进候选自愈。超时阈值可配，默认5分钟。
    # 用条件 UPDATE 一次批量重置，不逐条 ORM；写失败不影响本轮其余流程。
    try:
        timeout_min = max(1, _int_cfg(db, "share_building_timeout_minutes") or 5)
        cutoff = now - timedelta(minutes=timeout_min)
        stale = db.scalars(select(ShareTask).where(
            ShareTask.status == ST_BUILDING,
            or_(ShareTask.building_at.is_(None), ShareTask.building_at <= cutoff))).all()
        for st in stale:
            st.status = ST_PLANNED
            st.building_at = None
            st.error = f"[building超时自愈] 卡 building 超过 {timeout_min} 分钟，已重置 planned"
            st.scheduled_at = None
        if stale:
            db.commit()
            stats["building_recovered"] = len(stale)
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
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
            # P0 第六次死锁根治：异常可能就是 database is locked / PendingRollbackError，
            # 此时 Session 处于 failed 态，必须先 rollback 再写，否则下面的 commit 本身
            # 再抛 PendingRollbackError，把整批 run_cycle 拖垮（真机日志同款）。
            try:
                db.rollback()
            except Exception:
                pass
            stats["degraded"] = int(stats.get("degraded", 0)) + 1
            try:
                task.error = f"[建分享失败·降级无链接推送] {str(exc)[:600]}"
                task.status = ST_READY
                task.share_url = ""
                task.next_retry_at = None
                task.building_at = None  # v1.5.1：异常降级必清 building 中间态，防僵尸
                db.commit()
            except Exception:
                try:
                    db.rollback()
                except Exception:
                    pass
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
                try:
                    db.rollback()
                except Exception:
                    pass
    return stats


# P0 毒事件隔离：单条事件连续失败超过该次数就强制消费跳过，避免队头永久卡死。
# 2026-11-21 事故：事件 #6 identified|work:开封府 卡 8 小时，pending 按 id 升序，
# 它永远排第一 → 每轮都先在它身上等写锁超时 → 死亡螺旋。
MAX_EVENT_CONSUME_FAILURES = 3
# v1.4.2 洞1根治：失败计数改进程内存态，**绝不写 DB**。
# 2026-11-21 第四次复现（事件 #10）：v1.4.0 把 fail_n 写进 payload_json 再 commit，
# 但真机写锁被占时这个恢复用的 commit 自己也等 30s → 失败 → rollback → 计数没落库，
# 下次重读还是 0 → fail_n 永远停在「第 1 次」，到不了 3 次跳过阈值。
# 解药（记失败次数）与毒药（写锁）共用同一条写通道 = 死循环。改内存态彻底断开：
# 计数只在进程内累加，不碰 DB；进程重启计数清零可接受（重启本身释放写锁，是新一轮）。
# 只记 id→次数，无个人信息；上限清理避免长期运行内存增长。
_EVENT_FAIL_COUNT: dict[int, int] = {}
_EVENT_FAIL_COUNT_MAX = 4096


def _bump_event_fail(event_id: int) -> int:
    """内存态累加事件失败次数，返回累加后的值。不写 DB、不抛异常。"""
    try:
        n = int(_EVENT_FAIL_COUNT.get(event_id, 0) or 0) + 1
    except (TypeError, ValueError):
        n = 1
    if len(_EVENT_FAIL_COUNT) >= _EVENT_FAIL_COUNT_MAX:
        # 简单有界清理：清掉最老的一半（dict 插入序），避免无界增长。
        for k in list(_EVENT_FAIL_COUNT)[: _EVENT_FAIL_COUNT_MAX // 2]:
            _EVENT_FAIL_COUNT.pop(k, None)
    _EVENT_FAIL_COUNT[event_id] = n
    return n


def _clear_event_fail(event_id: int) -> None:
    _EVENT_FAIL_COUNT.pop(event_id, None)


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
            if ev.type in (_ev.RESOURCE_REMOVED, _ev.RESOURCE_DELETED):
                # 阶段五：删除事件 = 失效信号（本地元数据+事件驱动，非光鸭实时状态）。
                # 第一次→suspicious；累计2次且间隔≥30min→inactive+归档 current_flag=0。
                # 网络异常不进这里（只有已确认的删除事件才调），防误判。
                try:
                    from . import share_link as _sl
                    r1 = _sl.mark_suspicious(db, ev.work_key,
                                             reason=f"{ev.type}:resource_removed_event")
                    r2 = _sl.confirm_inactive_if_due(db, ev.work_key)
                    if r2.get("changed"):
                        # inactive 后归档当前记录（保留历史行，不删 B 库）
                        stats["records_retired"] += retire_share_record(
                            db, ev.work_key, reason="link_inactive_confirmed")
                    if r1.get("changed") or r2.get("changed"):
                        stats["link_marked"] = stats.get("link_marked", 0) + 1
                except Exception:
                    pass
            elif ev.type in (_ev.RESOURCE_NEW, _ev.RESOURCE_RESTORED):
                res = db.get(Resource, ev.resource_id) if ev.resource_id else None
                if not res or res.identify_status != "identified":
                    stats["skipped"] += 1
                else:
                    # 阶段二（资源库解耦）：解耦事件 payload 携带 channel_targets，
                    # 消费端据此建带目标的 ShareTask，run_cycle→submit_to_dispatch 才能
                    # 写入 QueueItem 完成推送。无 targets 的非解耦事件保持旧行为（兼容存量）。
                    payload = {}
                    try:
                        payload = json.loads(ev.payload_json or "{}")
                    except Exception:
                        payload = {}
                    targets = payload.get("channel_targets") if isinstance(payload, dict) else None
                    if isinstance(targets, list) and targets:
                        # channel_targets 两种形态兼容：解耦事件 [{channel_id,...}]；
                        # shadow_register 旧态 [{channel_id:int}] 也是同一结构，直接用。
                        clean = [t for t in targets
                                 if isinstance(t, dict) and int(t.get("channel_id") or 0) > 0]
                        _, created = ensure_task(db, res, kind="first", channel_targets=clean)
                    else:
                        _, created = ensure_task(db, res, kind="first")
                    stats["tasks_created"] += 1 if created else 0
            _ev.mark_consumed(db, ev)
            _clear_event_fail(ev.id)  # v1.4.2：成功即清计数，防止陈旧计数误伤下次失败
            stats["consumed"] += 1
        except Exception as exc:
            # v1.4.2 洞1根治：失败计数走进程内存态，**不写 DB、不再 db.commit()**。
            # 旧版在这里写 payload_json._consume_fail 再 commit，写锁被占时这个恢复事务
            # 自己也卡 30s→rollback→计数没落库→fail_n 永远=1→到不了3次跳过阈值（第四次复现根因）。
            # 现在计数纯内存累加，恢复路径零 DB 写，与毒药（写锁）彻底解耦。
            try:
                db.rollback()
            except Exception:
                pass
            stats["failed_once"] += 1
            fail_n = _bump_event_fail(ev.id)
            if fail_n >= MAX_EVENT_CONSUME_FAILURES:
                # 毒事件：连续失败到阈值，强制消费跳过，释放队头。
                # mark_consumed 本身要写库；若此刻写锁仍被占，它也会抛异常，
                # 被本 except 的 try 吞掉——下一次失败计数继续累加、再次尝试跳过，
                # 直到写锁释放后成功。计数不因跳过失败而丢失（内存态），这是与旧版的关键区别。
                marked = False
                try:
                    _ev.mark_consumed(db, ev)
                    marked = True
                except Exception:
                    try:
                        db.rollback()
                    except Exception:
                        pass
                if marked:
                    _clear_event_fail(ev.id)
                    stats["skipped_poison"] += 1
                    logger.error(
                        "[share_center] 事件 #%s 连续失败 %s 次，强制跳过避免队头死锁：%s",
                        ev.id, fail_n, exc)
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
        # 阶段五链接生命周期：前端分享管理页展示（本地推断，非光鸭实时）
        "link_state": str(r.link_state or "active"),
        "verify_fail_count": int(r.verify_fail_count or 0),
        "cleaned_at": r.cleaned_at.isoformat() if r.cleaned_at else "",
        "clean_state": str(r.clean_state or ""),
    } for r in rows]


# ============================================================================
# 阶段六：发布总览聚合 + 发布规则读写（只读为主，不碰发送核心）
# ============================================================================

def publish_overview(db: Session) -> dict:
    """发布总览聚合（设计文档 §阶段六 1）：急停 / 今日配额进度 /
    待处理资源事件数 / 链接有效性分布。全部本地库只读统计，不调用光鸭。
    """
    from .models import ResourceEvent
    from . import share_link as _sl

    today = utcnow().date()
    day_start = datetime(today.year, today.month, today.day)
    # 今日已发布分享任务数 / 今日已推送队列数（作为配额进度分母）
    published_today = db.scalar(select(func.count()).select_from(ShareTask).where(
        ShareTask.status == ST_PUBLISHED,
        ShareTask.published_at.isnot(None),
        ShareTask.published_at >= day_start)) or 0
    sent_today = db.scalar(select(func.count()).select_from(QueueItem).where(
        QueueItem.sent_at.isnot(None), QueueItem.sent_at >= day_start)) or 0
    # 待处理资源事件（未消费）
    pending_events = db.scalar(select(func.count()).select_from(ResourceEvent).where(
        ResourceEvent.consumed_at.is_(None))) or 0
    # 链接有效性分布：按当前分享记录的 link_state 聚合
    link_rows = db.execute(select(ShareRecord.link_state, func.count()).where(
        ShareRecord.current_flag == 1).group_by(ShareRecord.link_state)).all()
    link_dist = {str(s or _sl.LINK_ACTIVE): int(n) for s, n in link_rows}

    def _progress(used: int, key: str) -> dict:
        limit = _int_cfg(db, key)
        return {"used": int(used), "limit": limit,
                "unlimited": limit == 0,
                "pct": 0 if limit == 0 else min(100, round(used / limit * 100, 1))}

    return {
        "paused": is_paused(db),
        "enabled": is_enabled(db),
        "publish_today": _progress(published_today, "share_daily_limit"),
        "push_today": _progress(sent_today, "push_daily_limit"),
        "pending_events": int(pending_events),
        "link_states": link_dist,
    }


# 发布规则页白名单：保守默认值 + 开关（设计文档 §阶段六 5 / §阶段五）
PUBLISH_RULE_KEYS = (
    "share_center_enabled", "resource_decoupled_enabled", "publish_paused",
    "link_inactive_confirm_count", "link_inactive_min_minutes",
    "auto_cleanup_enabled", "share_daily_limit", "push_daily_limit",
    "stock_daily_quota", "fresh_daily_quota", "followup_daily_quota",
    "share_release_interval_seconds",
)


def publish_rules_get(db: Session) -> dict:
    """发布规则只读：布尔开关原样返回字符串值，前端按 0/1/true/false 判定。"""
    out: dict[str, str] = {}
    for key in PUBLISH_RULE_KEYS:
        out[key] = str(runtime_config.get(db, key) or "")
    return out


def publish_rules_set(db: Session, values: dict) -> dict:
    """写入发布规则：只接受白名单键、转字符串；脏值忽略不抛错。"""
    clean: dict[str, str] = {}
    for key in PUBLISH_RULE_KEYS:
        if key not in values:
            continue
        v = values[key]
        if isinstance(v, bool):
            clean[key] = "true" if v else "false"
        else:
            clean[key] = str(v).strip()
    if clean:
        runtime_config.set_many(db, clean)
        db.commit()
    return publish_rules_get(db)
