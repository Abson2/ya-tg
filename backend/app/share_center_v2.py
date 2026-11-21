"""分享中心 v2：全新数据体系的发布计划、清单、调度与节奏服务。

设计基线：SHARE_CENTER_REDESIGN_PHASE2_DESIGN.md。
红线：不读旧 ShareTask/ShareRecord；不迁移旧数据；AI 不直接推送；
频道/群发送间隔按目标类型可调且不得低于 Telegram 官方风控下限。
"""
from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select, update, func, or_
from sqlalchemy.orm import Session

from . import runtime_config, utils
from .db import get_db
from .guangya import GuangYaShareClient, create_share_parsed
from .models import (
    Channel,
    QueueItem,
    Resource,
    ResourceEvent,
    SCAIApplyLog,
    SCAISuggestion,
    SCCategory,
    SCCategoryCursor,
    SCHourWindow,
    SCLibraryVersion,
    SCPublishExecution,
    SCPublishTask,
    SCPushList,
    SCPushListItem,
    SCPushPacingRule,
    SCPushPacingState,
    SCSharePlan,
    SCSharePlanVersion,
    SCUpdateWindow,
    SCWorkCategory,
)

router = APIRouter()

OFFICIAL_MIN_SECONDS = {"channel": 1.0, "private": 1.0, "group": 3.0}
DEFAULT_INTERVAL_SECONDS = {"channel": 2.0, "private": 2.0, "group": 4.0}
ACTIVE_TASK_STATUS = {"planned", "building", "ready", "submitted", "accepted", "publishing"}
TERMINAL_TASK_STATUS = {"published", "failed", "cancelled"}


class Payload(BaseModel):
    payload: dict[str, Any] = {}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _jload(value: str | None, default):
    try:
        return json.loads(value or "")
    except Exception:
        return default


def _jdump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def is_enabled(db: Session) -> bool:
    return str(runtime_config.get(db, "share_center_v2_enabled") or "true").lower() in (
        "1", "true", "yes", "on"
    )


def ensure_defaults(db: Session) -> None:
    """幂等初始化 v2 默认配置：全局节奏规则 + 默认日计划。"""
    changed = False
    for target_type in ("channel", "group", "private"):
        key = f"global:{target_type}"
        exists = db.scalar(select(SCPushPacingRule.id).where(SCPushPacingRule.scope_key == key))
        if not exists:
            db.add(SCPushPacingRule(
                scope_key=key,
                target_type=target_type,
                default_interval_seconds=DEFAULT_INTERVAL_SECONDS[target_type],
                min_interval_seconds=OFFICIAL_MIN_SECONDS[target_type],
                enabled=True,
                updated_by="system",
            ))
            changed = True
    plan = db.scalar(select(SCSharePlan).where(SCSharePlan.name == "默认日计划"))
    if not plan:
        plan = SCSharePlan(name="默认日计划", plan_type="daily", enabled=True,
                           timezone="Asia/Shanghai")
        db.add(plan)
        db.flush()
        changed = True
    version = db.scalar(
        select(SCSharePlanVersion).where(SCSharePlanVersion.plan_id == plan.id,
                                         SCSharePlanVersion.status == "active")
    )
    if not version:
        version = SCSharePlanVersion(plan_id=plan.id, version_no=1, status="active",
                                     config_json=_jdump({"category_limit": 20,
                                                         "work_limit": 50,
                                                         "window_start": 8,
                                                         "window_end": 24}))
        db.add(version)
        db.flush()
        changed = True
        has_window = db.scalar(select(SCHourWindow.id).where(
            SCHourWindow.plan_version_id == version.id,
            SCHourWindow.start_hour == 8, SCHourWindow.end_hour == 24))
        if not has_window:
            db.add(SCHourWindow(plan_version_id=version.id, start_hour=8,
                                end_hour=24, quota=0, enabled=True))
    if changed:
        db.commit()


def _primary_category(resource: Resource) -> tuple[str, str]:
    media_type = str(getattr(resource, "media_type", "") or "")
    genres = _jload(getattr(resource, "genres", "[]"), [])
    primary = ""
    if isinstance(genres, list):
        for item in genres:
            if isinstance(item, dict):
                primary = str(item.get("name") or "")
            else:
                primary = str(item or "")
            if primary:
                break
    elif isinstance(genres, str) and genres.strip():
        primary = genres.strip()
    category_key = f"genre:{utils.normalize_work_name(primary)}" if primary else "genre:unknown"
    return category_key, media_type


def _ensure_category(db: Session, category_key: str, media_type: str, name: str = "") -> None:
    row = db.scalar(select(SCCategory).where(SCCategory.category_key == category_key))
    if row:
        return
    db.add(SCCategory(category_key=category_key, name=name or category_key,
                      source="tmdb", media_type=media_type, enabled=True))
    db.flush()


def sync_library_versions(db: Session) -> dict[str, int]:
    """按资源事实生成作品级版本快照与唯一主归属。"""
    resources = list(db.scalars(select(Resource)).all())
    grouped: dict[str, list[Resource]] = {}
    for res in resources:
        grouped.setdefault(utils.work_group_key(res), []).append(res)

    created = 0
    categories = 0
    for work_key, rows in grouped.items():
        ordered = sorted(rows, key=lambda r: (str(r.file_id or ""), int(r.size or 0), int(r.utime or 0)))
        fingerprint_src = "|".join(
            f"{r.file_id}:{r.size or 0}:{r.utime or 0}" for r in ordered
        )
        fingerprint = hashlib.sha256(fingerprint_src.encode("utf-8")).hexdigest()
        exists = db.scalar(
            select(SCLibraryVersion).where(SCLibraryVersion.work_key == work_key,
                                           SCLibraryVersion.fingerprint == fingerprint)
        )
        if exists:
            continue
        latest_no = db.scalar(
            select(func.coalesce(func.max(SCLibraryVersion.version_no), 0)).where(
                SCLibraryVersion.work_key == work_key)
        ) or 0
        rep = ordered[0]
        category_key, media_type = _primary_category(rep)
        _ensure_category(db, category_key, media_type)
        metadata = {
            "title": getattr(rep, "media_title", "") or getattr(rep, "name", ""),
            "poster_path": getattr(rep, "poster_path", ""),
            "share_dir_id": getattr(rep, "share_dir_id", "") or getattr(rep, "parent_id", ""),
            "share_dir_name": getattr(rep, "share_dir_name", ""),
            "tmdb_id": getattr(rep, "tmdb_id", 0),
            "media_type": media_type,
            "resource_count": len(ordered),
        }
        db.add(SCLibraryVersion(
            work_key=work_key,
            version_no=int(latest_no) + 1,
            fingerprint=fingerprint,
            media_type=media_type,
            resource_ids_json=_jdump([r.id for r in ordered]),
            metadata_json=_jdump(metadata),
            change_type="created" if latest_no == 0 else "updated",
        ))
        created += 1

        ownership = db.scalar(select(SCWorkCategory).where(SCWorkCategory.work_key == work_key))
        if not ownership:
            db.add(SCWorkCategory(work_key=work_key, category_key=category_key,
                                  media_type=media_type, source="tmdb", confidence=1.0))
            categories += 1
    db.commit()
    return {"versions_created": created, "categories_created": categories,
            "works": len(grouped)}


def merge_update_windows(db: Session, window_seconds: int = 600) -> dict[str, int]:
    """把 10 分钟内同一作品的资源事件合并为一个更新窗口。"""
    now = utcnow()
    events = list(db.scalars(
        select(ResourceEvent).where(ResourceEvent.consumed_at.is_(None)).order_by(ResourceEvent.id)
    ).all())
    merged = 0
    for ev in events:
        work_key = ev.work_key or ""
        if not work_key:
            ev.consumed_at = now
            continue
        window = db.scalar(
            select(SCUpdateWindow).where(SCUpdateWindow.work_key == work_key,
                                         SCUpdateWindow.status == "open")
            .order_by(SCUpdateWindow.id.desc())
        )
        if window and utils.as_utc(window.window_end) and utils.as_utc(window.window_end) > now:
            keys = _jload(window.event_keys_json, [])
            keys.append(ev.idem_key)
            window.event_keys_json = _jdump(keys)
            window.fingerprint = ev.idem_key
        else:
            db.add(SCUpdateWindow(
                work_key=work_key,
                window_start=now,
                window_end=now + timedelta(seconds=window_seconds),
                event_keys_json=_jdump([ev.idem_key]),
                fingerprint=ev.idem_key,
                status="open",
            ))
            # 必须立即 flush：同一批事件要能看到刚建的开窗，否则会重复建同一 (work_key, window_start)。
            db.flush()
        ev.consumed_at = now
        merged += 1
    db.commit()
    return {"events_merged": merged}


def _active_version(db: Session) -> SCSharePlanVersion | None:
    return db.scalar(
        select(SCSharePlanVersion).where(SCSharePlanVersion.status == "active")
        .order_by(SCSharePlanVersion.id.desc())
    )


def _current_hour_allowed(db: Session, version: SCSharePlanVersion) -> bool:
    hour = datetime.now(timezone(timedelta(hours=8))).hour
    windows = list(db.scalars(
        select(SCHourWindow).where(SCHourWindow.plan_version_id == version.id,
                                   SCHourWindow.enabled.is_(True))
    ).all())
    if not windows:
        return True
    return any(w.start_hour <= hour < w.end_hour for w in windows)


def plan_engine(db: Session) -> dict[str, int]:
    """按活动计划版本与分类游标生成推送清单、清单项和发布任务。"""
    version = _active_version(db)
    if not version or not _current_hour_allowed(db, version):
        return {"lists": 0, "items": 0, "tasks": 0}
    cfg = _jload(version.config_json, {})
    category_limit = max(1, int(cfg.get("category_limit") or 20))
    work_limit = max(1, int(cfg.get("work_limit") or 50))

    channels = list(db.scalars(select(Channel).where(Channel.enabled.is_(True))).all())
    if not channels:
        return {"lists": 0, "items": 0, "tasks": 0}

    categories = list(db.scalars(
        select(SCCategory).where(SCCategory.enabled.is_(True))
        .order_by(SCCategory.priority.desc(), SCCategory.id)
    ).all())[:category_limit]

    list_key = f"plan:{version.id}:{uuid.uuid4().hex[:12]}"
    push_list = SCPushList(list_key=list_key, name=f"计划版本 {version.version_no}",
                           status="active", source="plan", plan_version_id=version.id)
    db.add(push_list)
    db.flush()

    item_count = 0
    task_count = 0
    for category in categories:
        cursor = db.scalar(
            select(SCCategoryCursor).where(SCCategoryCursor.plan_version_id == version.id,
                                                  SCCategoryCursor.category_key == category.category_key)
        )
        if not cursor:
            cursor = SCCategoryCursor(plan_version_id=version.id,
                                      category_key=category.category_key)
            db.add(cursor)
            db.flush()
        versions = list(db.scalars(
            select(SCLibraryVersion).where(SCLibraryVersion.id > cursor.last_library_version_id)
            .order_by(SCLibraryVersion.id).limit(work_limit)
        ).all())
        for lv in versions:
            for channel in channels:
                target_type = "group" if str(channel.chat_id or "").startswith("-") else "channel"
                dedup = f"{lv.work_key}:{lv.fingerprint}:{channel.id}"
                exists = db.scalar(select(SCPushListItem.id).where(SCPushListItem.dedup_key == dedup))
                if exists:
                    continue
                item = SCPushListItem(
                    list_id=push_list.id, work_key=lv.work_key,
                    library_version_id=lv.id, category_key=category.category_key,
                    target_type=target_type, bot_id=channel.bot_id or 0,
                    chat_id=str(channel.chat_id or ""), dedup_key=dedup,
                    source_kind="first", status="planned",
                )
                db.add(item)
                db.flush()
                item_count += 1
                task = SCPublishTask(
                    list_item_id=item.id, work_key=lv.work_key,
                    plan_version_id=version.id, kind="first", status="planned",
                    priority=category.priority, scheduled_at=utcnow(),
                )
                db.add(task)
                task_count += 1
            cursor.last_library_version_id = lv.id
            cursor.last_work_key = lv.work_key
            cursor.last_success_at = utcnow()
            cursor.status = "idle"
    db.commit()
    return {"lists": 1, "items": item_count, "tasks": task_count}


async def dispatch_ready_tasks(db: Session, limit: int = 20) -> dict[str, int]:
    """建分享并提交到分发队列；发送核心仍由 publisher 负责。"""
    if str(runtime_config.get(db, "publish_paused") or "false").lower() in ("1", "true", "yes", "on"):
        return {"built": 0, "submitted": 0, "failed": 0, "paused": 1}
    now = utcnow()
    tasks = list(db.scalars(
        select(SCPublishTask).where(SCPublishTask.status.in_({"planned", "ready", "failed"}))
        .where(or_(SCPublishTask.scheduled_at.is_(None), SCPublishTask.scheduled_at <= now))
        .where(or_(SCPublishTask.next_retry_at.is_(None), SCPublishTask.next_retry_at <= now))
        .order_by(SCPublishTask.priority.desc(), SCPublishTask.id).limit(limit)
    ).all())
    stats = {"built": 0, "submitted": 0, "failed": 0, "paused": 0}
    for task in tasks:
        item = db.get(SCPushListItem, task.list_item_id)
        lv = db.get(SCLibraryVersion, item.library_version_id) if item else None
        if not item or not lv:
            task.status = "failed"
            task.error = "清单项或资源版本不存在"
            db.commit()
            stats["failed"] += 1
            continue
        metadata = _jload(lv.metadata_json, {})
        resource_ids = _jload(lv.resource_ids_json, [])
        rep = db.get(Resource, resource_ids[0]) if resource_ids else None
        if not rep:
            task.status = "failed"
            task.error = "代表资源不存在"
            db.commit()
            stats["failed"] += 1
            continue
        task.status = "building"
        task.error = ""
        db.add(SCPublishExecution(task_id=task.id, stage="build", status="running",
                                  started_at=utcnow(), request_json=_jdump({"work_key": task.work_key})))
        db.commit()
        try:
            client = GuangYaShareClient(db)
            share_id, share_url = await create_share_parsed(
                client, str(metadata.get("share_dir_id") or rep.file_id),
                str(metadata.get("title") or task.work_key), 0,
            )
            task.share_url = share_url
            task.link_state = "active"
            task.status = "ready"
            stats["built"] += 1
        except Exception as exc:
            task.status = "failed"
            task.error = str(exc)[:500]
            stats["failed"] += 1
            execution = db.scalar(select(SCPublishExecution).where(
                SCPublishExecution.task_id == task.id,
                SCPublishExecution.stage == "build",
                SCPublishExecution.status == "running").order_by(SCPublishExecution.id.desc()))
            if execution:
                execution.status = "failed"
                execution.error = str(exc)[:1000]
                execution.finished_at = utcnow()
            db.commit()
            continue

        # item.bot_id 是目标 Bot，不是 Channel.id；用 chat_id 精确匹配目标频道/群。
        channel = db.scalar(select(Channel).where(Channel.chat_id == item.chat_id))
        if not channel:
            channel = db.scalar(select(Channel).where(Channel.enabled.is_(True)).limit(1))
        if not channel:
            task.status = "failed"
            task.error = "无可用推送目标"
            db.commit()
            stats["failed"] += 1
            continue
        q = db.scalar(select(QueueItem).where(QueueItem.resource_id == rep.id,
                                             QueueItem.channel_id == channel.id))
        if not q:
            q = QueueItem(resource_id=rep.id, channel_id=channel.id)
            db.add(q)
        q.status = "pending"
        q.status_ext = "submitted"
        q.attempts = 0
        q.error = ""
        q.next_retry_at = None
        q.share_task_id = 0
        q.push_reason = "sc_v2"
        q.event_id = task.id
        q.accept_worker = ""
        q.accepted_at = None
        task.status = "submitted"
        db.add(SCPublishExecution(task_id=task.id, stage="submit", status="success",
                                  started_at=utcnow(), finished_at=utcnow(),
                                  response_json=_jdump({"queue_channel_id": channel.id})))
        db.commit()
        stats["submitted"] += 1
    return stats


async def run_cycle(db: Session, limit: int = 20) -> dict[str, Any]:
    if not is_enabled(db):
        return {"enabled": False}
    ensure_defaults(db)
    result: dict[str, Any] = {
        "enabled": True,
        "library": sync_library_versions(db),
        "update": merge_update_windows(db),
        "plan": plan_engine(db),
    }
    result["dispatch"] = await dispatch_ready_tasks(db, limit=limit)
    return result


def claim_tasks(db: Session, worker: str, limit: int = 20) -> list[SCPublishTask]:
    """可选的新任务租约领取；当前分发队列仍以 push_queue 为执行载体。"""
    now = utcnow()
    stale = utcnow() - timedelta(minutes=10)
    rows = list(db.scalars(
        select(SCPublishTask).where(SCPublishTask.status == "submitted").limit(limit)
    ).all())
    claimed = []
    for task in rows:
        lease = utils.as_utc(task.lease_expires_at)
        if task.lease_owner and lease and lease > now:
            continue
        task.lease_owner = worker
        task.lease_expires_at = now + timedelta(minutes=10)
        task.status = "accepted"
        claimed.append(task)
    for task in db.scalars(select(SCPublishTask).where(
        SCPublishTask.status == "accepted", SCPublishTask.lease_expires_at < stale
    )).all():
        task.status = "submitted"
        task.lease_owner = ""
        task.lease_expires_at = None
    db.commit()
    return claimed


def mark_result(db: Session, task_id: int, success: bool, error: str = "") -> dict[str, Any]:
    task = db.get(SCPublishTask, task_id)
    if not task:
        return {"ok": False, "reason": "任务不存在"}
    task.status = "published" if success else "failed"
    task.error = "" if success else error[:1000]
    task.updated_at = utcnow()
    if success:
        task.published_at = utcnow()
        task.lease_owner = ""
        task.lease_expires_at = None
    execution = db.scalar(
        select(SCPublishExecution).where(SCPublishExecution.task_id == task_id,
                                         SCPublishExecution.status == "running")
        .order_by(SCPublishExecution.id.desc())
    )
    if not execution:
        execution = SCPublishExecution(task_id=task_id, stage="send", status="running",
                                       started_at=utcnow())
        db.add(execution)
    execution.status = "success" if success else "failed"
    execution.error = "" if success else error[:1000]
    execution.finished_at = utcnow()
    db.commit()
    return {"ok": True, "status": task.status}


# ---------------------------------------------------------------------------
# 发送节奏：频道/群分型可调 + Telegram 官方下限夹逼 + 429 冷却
# ---------------------------------------------------------------------------

def target_type_of(chat_id: str | int | None) -> str:
    return "group" if str(chat_id or "").startswith("-") else "channel"


def _rule_value(rule: SCPushPacingRule) -> dict[str, Any]:
    official = OFFICIAL_MIN_SECONDS.get(rule.target_type, 1.0)
    min_value = max(float(rule.min_interval_seconds or 0), official)
    interval = max(float(rule.default_interval_seconds or min_value), min_value)
    if int(rule.max_per_minute or 0) > 0:
        interval = max(interval, 60.0 / int(rule.max_per_minute))
    return {"interval": interval, "min": min_value, "jitter": max(0.0, float(rule.jitter_seconds or 0))}


def resolve_pacing_rule(db: Session, target_type: str, bot_id: int = 0,
                        chat_id: str = "") -> dict[str, Any]:
    candidates = [
        f"chat:{chat_id}" if chat_id else "",
        f"bot:{bot_id}:{target_type}" if bot_id else "",
        f"global:{target_type}",
    ]
    for key in candidates:
        if not key:
            continue
        rule = db.scalar(select(SCPushPacingRule).where(SCPushPacingRule.scope_key == key,
                                                       SCPushPacingRule.enabled.is_(True)))
        if rule:
            return _rule_value(rule)
    return {"interval": DEFAULT_INTERVAL_SECONDS.get(target_type, 2.0),
            "min": OFFICIAL_MIN_SECONDS.get(target_type, 1.0), "jitter": 0.0}


def pacing_wait_seconds(db: Session, q: QueueItem) -> float:
    """只读取，不在等待前写库，避免跨 await 持写事务。"""
    channel = db.get(Channel, q.channel_id)
    if not channel:
        return 0.0
    ttype = target_type_of(channel.chat_id)
    target_key = f"bot:{channel.bot_id}:chat:{channel.chat_id}"
    state = db.scalar(select(SCPushPacingState).where(SCPushPacingState.target_key == target_key))
    now = utcnow()
    if state:
        until = utils.as_utc(state.retry_after_until)
        if until and until > now:
            return max(0.0, (until - now).total_seconds())
    rule = resolve_pacing_rule(db, ttype, channel.bot_id or 0, str(channel.chat_id or ""))
    wait = rule["interval"] + rule["jitter"]
    if state:
        last = utils.as_utc(state.last_sent_at)
        if last:
            wait = max(wait, rule["interval"] - (now - last).total_seconds())
    return max(0.0, wait)


def mark_pacing_attempt(db: Session, q: QueueItem, retry_after: float = 0.0,
                        success: bool = False) -> None:
    channel = db.get(Channel, q.channel_id)
    if not channel:
        return
    ttype = target_type_of(channel.chat_id)
    target_key = f"bot:{channel.bot_id}:chat:{channel.chat_id}"
    state = db.scalar(select(SCPushPacingState).where(SCPushPacingState.target_key == target_key))
    if not state:
        state = SCPushPacingState(target_key=target_key, target_type=ttype)
        db.add(state)
    now = utcnow()
    state.last_attempt_at = now
    if success:
        state.last_sent_at = now
        state.retry_after_seconds = 0.0
        state.retry_after_until = None
        state.consecutive_429 = 0
    else:
        state.retry_after_seconds = max(0.0, float(retry_after or 0.0))
        state.retry_after_until = now + timedelta(seconds=state.retry_after_seconds) if state.retry_after_seconds else None
        state.consecutive_429 = int(state.consecutive_429 or 0) + 1
    state.updated_at = now
    db.commit()


# ---------------------------------------------------------------------------
# AI 周度优化：只生成建议；审核/自动应用写审计，不直接推送
# ---------------------------------------------------------------------------

def _local_weekly_suggestions(db: Session, week_start: str) -> list[dict[str, Any]]:
    task_counts = dict(db.execute(
        select(SCPublishTask.status, func.count(SCPublishTask.id)).group_by(SCPublishTask.status)
    ).all())
    category_rows = db.execute(
        select(SCWorkCategory.category_key, func.count(SCWorkCategory.id))
        .group_by(SCWorkCategory.category_key)
    ).all()
    top_category = max(category_rows, key=lambda x: x[1])[0] if category_rows else "genre:unknown"
    return [
        {
            "suggestion_type": "plan_weights",
            "payload": {"priority_boost": {top_category: 5}},
            "reason": f"本地统计：{top_category} 作品最多，建议提高权重",
            "confidence": 0.6,
        },
        {
            "suggestion_type": "interval",
            "payload": {"group_default_interval_seconds": 4.0},
            "reason": f"本地统计：published={task_counts.get('published', 0)}, failed={task_counts.get('failed', 0)}，维持群聊保守间隔",
            "confidence": 0.5,
        },
    ]


def generate_weekly_suggestions(db: Session, week_start: str | None = None) -> dict[str, Any]:
    week_start = week_start or utcnow().date().isoformat()
    ai_enabled = str(runtime_config.get(db, "ai_enabled") or "false").lower() in ("1", "true", "yes", "on")
    api_key = runtime_config.get(db, "ai_api_key") or ""
    suggestions: list[dict[str, Any]]
    source = "local"
    if ai_enabled and api_key:
        try:
            base = (runtime_config.get(db, "ai_base_url") or "https://api.openai.com/v1").rstrip("/")
            model = runtime_config.get(db, "ai_model") or "gpt-4o-mini"
            timeout = float(runtime_config.get(db, "ai_timeout") or 30)
            stats = {
                "published": db.scalar(select(func.count(SCPublishTask.id)).where(
                    SCPublishTask.status == "published")) or 0,
                "failed": db.scalar(select(func.count(SCPublishTask.id)).where(
                    SCPublishTask.status == "failed")) or 0,
            }
            prompt = (
                "你是 Telegram 发布调度优化器。只输出 JSON 数组，元素含 suggestion_type/payload/reason/confidence。"
                "不得建议直接发送消息。统计数据：" + _jdump(stats)
            )
            resp = httpx.post(
                f"{base}/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={"model": model, "messages": [{"role": "user", "content": prompt}],
                      "temperature": 0.2},
                timeout=timeout,
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            if isinstance(parsed, dict):
                parsed = parsed.get("suggestions", [])
            if isinstance(parsed, list) and parsed:
                suggestions = [x for x in parsed if isinstance(x, dict)][:10]
                source = "openai"
            else:
                suggestions = _local_weekly_suggestions(db, week_start)
        except Exception:
            suggestions = _local_weekly_suggestions(db, week_start)
    else:
        suggestions = _local_weekly_suggestions(db, week_start)

    created = 0
    for item in suggestions:
        stype = str(item.get("suggestion_type") or "plan_weights")
        exists = db.scalar(select(SCAISuggestion.id).where(
            SCAISuggestion.week_start == week_start,
            SCAISuggestion.suggestion_type == stype))
        if exists:
            continue
        db.add(SCAISuggestion(
            week_start=week_start,
            suggestion_type=stype,
            payload_json=_jdump(item.get("payload") or {}),
            reason=str(item.get("reason") or "")[:1000],
            confidence=float(item.get("confidence") or 0.0),
            status="pending",
        ))
        created += 1
    db.commit()
    return {"source": source, "created": created, "week_start": week_start}


def apply_suggestion(db: Session, suggestion_id: int, reviewer: str = "admin",
                     auto: bool = False) -> dict[str, Any]:
    suggestion = db.get(SCAISuggestion, suggestion_id)
    if not suggestion:
        return {"ok": False, "reason": "建议不存在"}
    if suggestion.status not in ("pending", "approved"):
        return {"ok": False, "reason": "建议已处理"}
    if auto and str(runtime_config.get(db, "sc_ai_auto_apply") or "false").lower() not in (
        "1", "true", "yes", "on"
    ):
        return {"ok": False, "reason": "自动应用未开启"}
    version = _active_version(db)
    before = _jload(version.config_json, {}) if version else {}
    payload = _jload(suggestion.payload_json, {})
    after = dict(before)
    if isinstance(payload, dict):
        if isinstance(payload.get("priority_boost"), dict):
            after["priority_boost"] = payload["priority_boost"]
        if "group_default_interval_seconds" in payload:
            rule = db.scalar(select(SCPushPacingRule).where(
                SCPushPacingRule.scope_key == "global:group"))
            if rule:
                value = max(float(payload["group_default_interval_seconds"]),
                            OFFICIAL_MIN_SECONDS["group"])
                rule.default_interval_seconds = value
                rule.updated_by = f"ai:{reviewer}"
                rule.updated_at = utcnow()
    if version:
        version.config_json = _jdump(after)
    suggestion.status = "applied"
    suggestion.reviewed_by = reviewer
    suggestion.reviewed_at = utcnow()
    suggestion.applied_at = utcnow()
    db.add(SCAIApplyLog(suggestion_id=suggestion.id, action="apply",
                        before_json=_jdump(before), after_json=_jdump(after),
                        result="success"))
    db.commit()
    return {"ok": True, "status": "applied", "auto": auto}


# ---------------------------------------------------------------------------
# API：前端只作为管理入口，所有按钮走真实后端
# ---------------------------------------------------------------------------

@router.get("/overview")
def api_overview(db: Session = Depends(get_db)):
    """发布总览仪表盘数据。

    全部字段来自 sc_* 真实表聚合：不读旧 ShareRecord/ShareTask，不使用截图示例值。
    注意：link_state 仅由发布建链写入 active（unknown 为未建链默认值）；
    suspicious/inactive/cleaned 属旧链接核验链路，v2 发布任务不写，故分布中可能缺席。
    """
    ensure_defaults(db)
    counts = {}
    for model, key in ((SCLibraryVersion, "versions"), (SCPushList, "lists"),
                       (SCPushListItem, "items"), (SCPublishTask, "tasks"),
                       (SCUpdateWindow, "update_windows"),
                       (SCAISuggestion, "ai_suggestions")):
        counts[key] = db.scalar(select(func.count(model.id))) or 0
    status_rows = db.execute(
        select(SCPublishTask.status, func.count(SCPublishTask.id)).group_by(SCPublishTask.status)
    ).all()
    status_counts = {str(k): int(v or 0) for k, v in status_rows}
    link_rows = db.execute(
        select(SCPublishTask.link_state, func.count(SCPublishTask.id)).group_by(SCPublishTask.link_state)
    ).all()
    link_state_counts = {str(k): int(v or 0) for k, v in link_rows}
    kind_rows = db.execute(
        select(SCPublishTask.kind, func.count(SCPublishTask.id)).group_by(SCPublishTask.kind)
    ).all()
    kind_counts = {str(k): int(v or 0) for k, v in kind_rows}

    def _sum_status(*names: str) -> int:
        return sum(status_counts.get(n, 0) for n in names)

    # KPI 四卡：语义固定、来源可追溯
    kpi = {
        "pending_publish": _sum_status("planned", "ready"),
        "active_shares": link_state_counts.get("active", 0),
        "pending_link": link_state_counts.get("unknown", 0),
        "in_flight_push": _sum_status("submitted", "accepted", "publishing", "building"),
    }

    # 发布调度：按真实 kind 分组，进度=已发布/该类任务总数
    published_total = status_counts.get("published", 0)

    def _schedule_row(kind: str, label: str) -> dict[str, Any]:
        total = kind_counts.get(kind, 0)
        done = db.scalar(select(func.count(SCPublishTask.id)).where(
            SCPublishTask.kind == kind, SCPublishTask.status == "published")) or 0
        done = int(done or 0)
        return {"kind": kind, "label": label, "total": total, "published": done,
                "rate": round(done * 100 / total, 1) if total else 0.0}

    schedule = {
        "first": _schedule_row("first", "全新剧"),
        "update": _schedule_row("update", "追更剧"),
        "overall": {"kind": "overall", "label": "全部任务", "total": counts["tasks"],
                    "published": published_total,
                    "rate": round(published_total * 100 / counts["tasks"], 1)
                    if counts["tasks"] else 0.0},
    }
    # 真实启用的时段窗口（供仪表盘展示配额区间；无窗口时前端显示"按计划窗口"）
    windows = [
        {"plan_version_id": w.plan_version_id, "start_hour": w.start_hour,
         "end_hour": w.end_hour, "quota": w.quota}
        for w in db.scalars(select(SCHourWindow).where(SCHourWindow.enabled.is_(True))
                            .order_by(SCHourWindow.plan_version_id, SCHourWindow.start_hour)).all()
    ]
    # 最近发布动态：最近 8 条真实任务
    recent_rows = db.scalars(
        select(SCPublishTask).order_by(SCPublishTask.id.desc()).limit(8)).all()
    recent_tasks = []
    for row in recent_rows:
        data = _model_dict(row)
        recent_tasks.append({k: data.get(k) for k in (
            "id", "work_key", "kind", "status", "link_state", "share_url",
            "published_at", "updated_at", "error")})

    pacing = []
    for t in ("channel", "group", "private"):
        rule = resolve_pacing_rule(db, t)
        base = db.scalar(select(SCPushPacingRule).where(
            SCPushPacingRule.scope_key == f"global:{t}",
            SCPushPacingRule.enabled.is_(True)))
        pacing.append({"target_type": t, "interval_seconds": rule["interval"],
                       "min_seconds": rule["min"],
                       "max_per_minute": int(base.max_per_minute or 0) if base else 0})
    return {"enabled": is_enabled(db), "paused": _is_paused(db), "counts": counts,
            "status_counts": status_counts, "pacing": pacing, "kpi": kpi,
            "link_state_counts": link_state_counts, "kind_counts": kind_counts,
            "schedule": schedule, "windows": windows, "recent_tasks": recent_tasks}


@router.post("/run")
def api_run(db: Session = Depends(get_db)):
    import asyncio
    return asyncio.run(run_cycle(db, limit=50))


def _is_paused(db: Session) -> bool:
    return str(runtime_config.get(db, "publish_paused") or "false").strip().lower() in (
        "1", "true", "yes", "on")


@router.get("/pause")
def api_pause_get(db: Session = Depends(get_db)):
    """发布急停状态：与 dispatch_ready_tasks 读取同一 publish_paused 键。"""
    return {"paused": _is_paused(db)}


@router.post("/pause")
def api_pause_set(body: Payload, db: Session = Depends(get_db)):
    """发布急停开关：只拦新一轮建分享/提交，不触碰发送核心、不撤销在途任务。"""
    paused = bool(body.payload.get("paused"))
    runtime_config.set_many(db, {"publish_paused": "true" if paused else "false"})
    return {"ok": True, "paused": paused}


@router.get("/categories")
def api_categories(db: Session = Depends(get_db)):
    rows = db.scalars(select(SCCategory).order_by(SCCategory.priority.desc(), SCCategory.id)).all()
    return {"items": [_model_dict(r) for r in rows]}


@router.post("/categories")
def api_upsert_category(body: Payload, db: Session = Depends(get_db)):
    p = body.payload
    key = str(p.get("category_key") or "").strip()
    if not key:
        return {"ok": False, "reason": "category_key 不能为空"}
    row = db.scalar(select(SCCategory).where(SCCategory.category_key == key))
    if not row:
        row = SCCategory(category_key=key)
        db.add(row)
    row.name = str(p.get("name") or key)
    row.media_type = str(p.get("media_type") or "")
    row.enabled = bool(p.get("enabled", True))
    row.priority = int(p.get("priority") or 0)
    row.updated_at = utcnow()
    db.commit()
    return {"ok": True, "item": _model_dict(row)}


@router.post("/categories/reassign")
def api_reassign_categories(db: Session = Depends(get_db)):
    result = sync_library_versions(db)
    return {"ok": True, **result}


@router.get("/library-versions")
def api_library_versions(work_key: str = "", limit: int = 100, db: Session = Depends(get_db)):
    q = select(SCLibraryVersion)
    if work_key:
        q = q.where(SCLibraryVersion.work_key.contains(work_key))
    rows = db.scalars(q.order_by(SCLibraryVersion.id.desc()).limit(min(limit, 500))).all()
    return {"items": [_model_dict(r) for r in rows]}


@router.get("/push-lists")
def api_push_lists(limit: int = 100, db: Session = Depends(get_db)):
    rows = db.scalars(select(SCPushList).order_by(SCPushList.id.desc()).limit(min(limit, 500))).all()
    return {"items": [_model_dict(r) for r in rows]}


@router.get("/push-lists/{list_id}/items")
def api_push_list_items(list_id: int, limit: int = 200, db: Session = Depends(get_db)):
    rows = db.scalars(select(SCPushListItem).where(SCPushListItem.list_id == list_id)
                      .order_by(SCPushListItem.id).limit(min(limit, 1000))).all()
    return {"items": [_model_dict(r) for r in rows]}


@router.get("/plans")
def api_plans(db: Session = Depends(get_db)):
    ensure_defaults(db)
    rows = db.scalars(select(SCSharePlan).order_by(SCSharePlan.id)).all()
    versions = db.scalars(select(SCSharePlanVersion).order_by(SCSharePlanVersion.id.desc())).all()
    return {"items": [_model_dict(r) for r in rows],
            "versions": [_model_dict(r) for r in versions]}


@router.post("/plans")
def api_create_plan(body: Payload, db: Session = Depends(get_db)):
    p = body.payload
    row = SCSharePlan(name=str(p.get("name") or "新计划"),
                      plan_type=str(p.get("plan_type") or "daily"),
                      enabled=bool(p.get("enabled", True)),
                      timezone=str(p.get("timezone") or "Asia/Shanghai"))
    db.add(row)
    db.flush()
    db.add(SCSharePlanVersion(plan_id=row.id, version_no=1, status="draft",
                              config_json=_jdump(p.get("config") or {})))
    db.commit()
    return {"ok": True, "item": _model_dict(row)}


@router.post("/plans/{plan_id}/versions")
def api_create_plan_version(plan_id: int, body: Payload, db: Session = Depends(get_db)):
    plan = db.get(SCSharePlan, plan_id)
    if not plan:
        return {"ok": False, "reason": "计划不存在"}
    max_no = db.scalar(select(func.coalesce(func.max(SCSharePlanVersion.version_no), 0)).where(
        SCSharePlanVersion.plan_id == plan_id)) or 0
    version = SCSharePlanVersion(plan_id=plan_id, version_no=int(max_no) + 1,
                                 status="draft", config_json=_jdump(body.payload or {}))
    db.add(version)
    db.commit()
    return {"ok": True, "item": _model_dict(version)}


@router.post("/plans/{version_id}/activate")
def api_activate_version(version_id: int, db: Session = Depends(get_db)):
    target = db.get(SCSharePlanVersion, version_id)
    if not target:
        return {"ok": False, "reason": "版本不存在"}
    for row in db.scalars(select(SCSharePlanVersion).where(
        SCSharePlanVersion.plan_id == target.plan_id,
        SCSharePlanVersion.status == "active")
    ).all():
        row.status = "archived"
    target.status = "active"
    db.commit()
    return {"ok": True, "version_id": version_id}


@router.get("/hour-windows")
def api_hour_windows(version_id: int = 0, db: Session = Depends(get_db)):
    q = select(SCHourWindow)
    if version_id:
        q = q.where(SCHourWindow.plan_version_id == version_id)
    rows = db.scalars(q.order_by(SCHourWindow.plan_version_id, SCHourWindow.start_hour)).all()
    return {"items": [_model_dict(r) for r in rows]}


@router.post("/hour-windows")
def api_upsert_hour_window(body: Payload, db: Session = Depends(get_db)):
    p = body.payload
    version_id = int(p.get("plan_version_id") or 0)
    start = int(p.get("start_hour") or 0)
    end = int(p.get("end_hour") or 0)
    if not (0 <= start < end <= 24):
        return {"ok": False, "reason": "小时窗口必须满足 0 <= start < end <= 24"}
    row = db.scalar(select(SCHourWindow).where(SCHourWindow.plan_version_id == version_id,
                                               SCHourWindow.start_hour == start,
                                               SCHourWindow.end_hour == end))
    if not row:
        row = SCHourWindow(plan_version_id=version_id, start_hour=start, end_hour=end)
        db.add(row)
    row.quota = int(p.get("quota") or 0)
    row.enabled = bool(p.get("enabled", True))
    db.commit()
    return {"ok": True, "item": _model_dict(row)}


@router.get("/cursors")
def api_cursors(version_id: int = 0, db: Session = Depends(get_db)):
    q = select(SCCategoryCursor)
    if version_id:
        q = q.where(SCCategoryCursor.plan_version_id == version_id)
    rows = db.scalars(q.order_by(SCCategoryCursor.id)).all()
    return {"items": [_model_dict(r) for r in rows]}


@router.get("/tasks")
def api_tasks(status: str = "", limit: int = 200, db: Session = Depends(get_db)):
    q = select(SCPublishTask)
    if status:
        q = q.where(SCPublishTask.status == status)
    rows = db.scalars(q.order_by(SCPublishTask.id.desc()).limit(min(limit, 1000))).all()
    return {"items": [_model_dict(r) for r in rows]}


@router.get("/tasks/{task_id}")
def api_task_detail(task_id: int, db: Session = Depends(get_db)):
    task = db.get(SCPublishTask, task_id)
    if not task:
        return {"ok": False, "reason": "任务不存在"}
    executions = db.scalars(select(SCPublishExecution).where(
        SCPublishExecution.task_id == task_id).order_by(SCPublishExecution.id)).all()
    return {"item": _model_dict(task), "executions": [_model_dict(x) for x in executions]}


@router.post("/tasks/{task_id}/retry")
def api_task_retry(task_id: int, db: Session = Depends(get_db)):
    task = db.get(SCPublishTask, task_id)
    if not task:
        return {"ok": False, "reason": "任务不存在"}
    task.status = "planned"
    task.error = ""
    task.next_retry_at = None
    task.lease_owner = ""
    task.lease_expires_at = None
    db.commit()
    return {"ok": True, "status": task.status}


@router.post("/tasks/{task_id}/cancel")
def api_task_cancel(task_id: int, db: Session = Depends(get_db)):
    task = db.get(SCPublishTask, task_id)
    if not task:
        return {"ok": False, "reason": "任务不存在"}
    task.status = "cancelled"
    task.lease_owner = ""
    task.lease_expires_at = None
    db.commit()
    return {"ok": True, "status": task.status}


@router.get("/pacing-rules")
def api_pacing_rules(db: Session = Depends(get_db)):
    ensure_defaults(db)
    rows = db.scalars(select(SCPushPacingRule).order_by(SCPushPacingRule.id)).all()
    return {"items": [_model_dict(r) for r in rows],
            "official_min": OFFICIAL_MIN_SECONDS}


@router.post("/pacing-rules")
def api_upsert_pacing_rule(body: Payload, db: Session = Depends(get_db)):
    ensure_defaults(db)
    p = body.payload
    scope = str(p.get("scope_key") or "").strip()
    target_type = str(p.get("target_type") or "channel")
    if not scope:
        return {"ok": False, "reason": "scope_key 不能为空"}
    official = OFFICIAL_MIN_SECONDS.get(target_type, 1.0)
    minimum = max(float(p.get("min_interval_seconds") or official), official)
    interval = max(float(p.get("default_interval_seconds") or minimum), minimum)
    row = db.scalar(select(SCPushPacingRule).where(SCPushPacingRule.scope_key == scope))
    if not row:
        row = SCPushPacingRule(scope_key=scope)
        db.add(row)
    row.target_type = target_type
    row.bot_id = int(p.get("bot_id") or 0)
    row.chat_id = str(p.get("chat_id") or "")
    row.min_interval_seconds = minimum
    row.default_interval_seconds = interval
    row.max_per_minute = int(p.get("max_per_minute") or 0)
    row.jitter_seconds = max(0.0, float(p.get("jitter_seconds") or 0))
    row.enabled = bool(p.get("enabled", True))
    row.updated_by = str(p.get("updated_by") or "admin")
    row.updated_at = utcnow()
    db.commit()
    return {"ok": True, "item": _model_dict(row)}


@router.get("/pacing-state")
def api_pacing_state(limit: int = 200, db: Session = Depends(get_db)):
    rows = db.scalars(select(SCPushPacingState).order_by(SCPushPacingState.id.desc())
                      .limit(min(limit, 500))).all()
    return {"items": [_model_dict(r) for r in rows]}


@router.get("/ai/suggestions")
def api_ai_suggestions(status: str = "", limit: int = 100, db: Session = Depends(get_db)):
    q = select(SCAISuggestion)
    if status:
        q = q.where(SCAISuggestion.status == status)
    rows = db.scalars(q.order_by(SCAISuggestion.id.desc()).limit(min(limit, 500))).all()
    return {"items": [_model_dict(r) for r in rows]}


@router.post("/ai/generate")
def api_ai_generate(body: Payload, db: Session = Depends(get_db)):
    return generate_weekly_suggestions(db, str(body.payload.get("week_start") or "") or None)


@router.post("/ai/suggestions/{suggestion_id}/apply")
def api_ai_apply(suggestion_id: int, body: Payload, db: Session = Depends(get_db)):
    return apply_suggestion(db, suggestion_id,
                            reviewer=str(body.payload.get("reviewer") or "admin"),
                            auto=bool(body.payload.get("auto", False)))


@router.post("/ai/suggestions/{suggestion_id}/reject")
def api_ai_reject(suggestion_id: int, db: Session = Depends(get_db)):
    row = db.get(SCAISuggestion, suggestion_id)
    if not row:
        return {"ok": False, "reason": "建议不存在"}
    row.status = "rejected"
    row.reviewed_at = utcnow()
    db.add(SCAIApplyLog(suggestion_id=suggestion_id, action="reject", result="success"))
    db.commit()
    return {"ok": True}


def _model_dict(row) -> dict[str, Any]:
    data = {}
    for column in row.__table__.columns:
        value = getattr(row, column.name)
        if isinstance(value, datetime):
            value = value.isoformat()
        data[column.name] = value
    return data
