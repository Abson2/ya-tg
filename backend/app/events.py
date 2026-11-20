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
                        payload: dict | None = None) -> ResourceEvent | None:
    """资源状态变化落事件（幂等：同 type+work_key+resource_id 未消费时只一条）。"""
    idem = f"{event_type}|{work_key}|{resource_id}"
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
