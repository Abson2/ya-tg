"""四模块内部事件总线（设计文档 §6.2）：持久化事件 + 幂等消费 + 定时对账兜底。

单向数据流：
  镜像同步 --emit_mirror_event--> mirror_events --consume_mirror_events--> 资源库
  资源库   --emit_resource_event--> resource_events --consume_resource_events--> 分享中心

生产者在本模块自己的事务内落事件并 commit；消费者按 idem_key 幂等处理，处理后
置 consumed_at。事件保留不删，可重放、可追溯。任何消费异常只回滚该条，不阻断
其它事件。事件是「加快下游刷新的通知」，不是唯一数据依据——下游仍有定时全量对账。
"""
from __future__ import annotations

import json
import logging

from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import MirrorEvent, ResourceEvent, utcnow

logger = logging.getLogger(__name__)


# 镜像事件类型（设计文档 §2.9）
MIRROR_COMPLETED = "completed"
MIRROR_UPDATED = "updated"
MIRROR_RESTORED = "restored"
MIRROR_DELETED = "deleted"
MIRROR_FAILED = "failed"

# 资源事件类型（设计文档 §3.7）
RESOURCE_NEW = "identified"
RESOURCE_EPISODE_ADDED = "episode_added"
RESOURCE_FILE_UPDATED = "updated"
RESOURCE_REMOVED = "removed"
RESOURCE_RESTORED = "restored"
# 阶段三补齐的 5 类资源事件（原为死常量/未定义）：
# created=资源首次入库；file_changed=单文件内容变化；renamed=改名；moved=目录移动；deleted=删除。
# 这些类型供资源库内部追踪与下游按需订阅；分享中心当前只消费 identified/restored/removed，
# 新增类型的 emit 不会再触发自动推送（避免高频文件事件造成重复推送）。
RESOURCE_CREATED = "created"
RESOURCE_FILE_CHANGED = "file_changed"
RESOURCE_RENAMED = "renamed"
RESOURCE_MOVED = "moved"
RESOURCE_DELETED = "deleted"

ALL_RESOURCE_EVENT_TYPES = frozenset({
    RESOURCE_NEW, RESOURCE_EPISODE_ADDED, RESOURCE_FILE_UPDATED, RESOURCE_REMOVED,
    RESOURCE_RESTORED, RESOURCE_CREATED, RESOURCE_FILE_CHANGED, RESOURCE_RENAMED,
    RESOURCE_MOVED, RESOURCE_DELETED,
})


def emit_mirror_event(db: Session, *, work_key: str, rule_id: int = 0,
                      batch_id: str = "", title_standard: str = "",
                      media_id: int = 0, b_file_ids: list[str] | None = None,
                      dir_hints: dict | None = None,
                      scope: str = MIRROR_COMPLETED) -> MirrorEvent | None:
    """镜像完成后落事件（幂等：同 rule+batch+work_key 只一条）。"""
    idem = f"{rule_id}|{batch_id}|{work_key}|{scope}"
    try:
        existing = db.scalar(select(MirrorEvent).where(MirrorEvent.idem_key == idem).limit(1))
        if existing:
            return existing
        ev = MirrorEvent(
            idem_key=idem, rule_id=rule_id, batch_id=batch_id,
            work_key=work_key, title_standard=title_standard, media_id=media_id,
            b_file_ids=json.dumps(b_file_ids or []),
            dir_hints=json.dumps(dir_hints or {}, ensure_ascii=False),
            scope=scope, created_at=utcnow())
        db.add(ev)
        db.commit()
        return ev
    except Exception as exc:
        db.rollback()
        logger.warning("[events] 镜像事件落库失败 work_key=%s：%s", work_key, exc)
        return None


def emit_resource_event(db: Session, *, event_type: str, work_key: str,
                        resource_id: int = 0,
                        payload: dict | None = None,
                        fingerprint: str = "") -> ResourceEvent | None:
    """资源状态变化落事件（幂等：同 type+work_key+resource_id 未消费时只一条）。

    阶段三：新增可选 fingerprint（内容指纹/序号）。传入时幂等键扩展为
    `type|work_key|resource_id|fingerprint`，使「同一资源同一类型的多次变化」能各自
    落一条事件；不传则保持旧键格式（零迁移、兼容存量事件与所有旧调用方）。
    """
    idem = f"{event_type}|{work_key}|{resource_id}"
    if fingerprint:
        idem = f"{idem}|{str(fingerprint)[:64]}"
    try:
        existing = db.scalar(select(ResourceEvent).where(ResourceEvent.idem_key == idem).limit(1))
        if existing:
            return existing
        ev = ResourceEvent(
            idem_key=idem, type=event_type, work_key=work_key,
            resource_id=resource_id,
            payload_json=json.dumps(payload or {}, ensure_ascii=False),
            created_at=utcnow())
        db.add(ev)
        db.commit()
        return ev
    except Exception as exc:
        db.rollback()
        logger.warning("[events] 资源事件落库失败 %s work_key=%s：%s", event_type, work_key, exc)
        return None


def pending_mirror_events(db: Session, limit: int = 200) -> list[MirrorEvent]:
    return list(db.scalars(select(MirrorEvent).where(
        MirrorEvent.consumed_at.is_(None)).order_by(MirrorEvent.id).limit(limit)).all())


def pending_resource_events(db: Session, limit: int = 200) -> list[ResourceEvent]:
    return list(db.scalars(select(ResourceEvent).where(
        ResourceEvent.consumed_at.is_(None)).order_by(ResourceEvent.id).limit(limit)).all())


def mark_consumed(db: Session, ev: MirrorEvent | ResourceEvent) -> None:
    ev.consumed_at = utcnow()
    db.commit()


def consume_mirror_events(db: Session, limit: int = 200) -> dict:
    """阶段三：消费并归档镜像事件（镜像同步 → 资源库的可追溯通道）。

    设计纪律（对齐五轮死锁教训 + 不虚构关联红线）：
      - 纯短事务、零网络、零高频配置写、零 to_thread 需求，不制造新持锁源；
      - **不做事件类型转换**：镜像事件带 MirrorMedia.id，分享中心消费端要 Resource.id，
        两者无可靠映射（Resource 无 work_group_key 列可反查），强行转换会虚构关联。
        镜像恢复仍走已验证的可靠链路：mirror.py 重建 → pipeline run_scan→post_identify
        →_enqueue_ready 按 ever_cancelled 判 restored（main.py:3079-3082）。
      - MIRROR_COMPLETED 不触发全量重扫——镜像后处理 pipeline 已同步完成刷新，
        重复触发只会叠加长任务；本函数只标记消费，防止 mirror_events 未消费堆积。
      - 每条独立 try，失败只 rollback 自己并继续，毒事件不阻断整批。

    返回 {"consumed": n, "failed": n}。
    """
    stats = {"consumed": 0, "failed": 0}
    for ev in pending_mirror_events(db, limit=limit):
        try:
            mark_consumed(db, ev)
            stats["consumed"] += 1
        except Exception as exc:
            try:
                db.rollback()
            except Exception:
                pass
            stats["failed"] += 1
            logger.warning("[events] 消费镜像事件 #%s 失败：%s", ev.id, exc)
    return stats
