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
from sqlalchemy import select, update, func, or_, delete
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
    SCCategorySchedule,
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
# v2.1.1：每分钟上限与抖动的内置默认。旧默认 0/0.0 = 不限 + 无错峰，
# 真机节奏卡显示两个 0，等于没有风控。
# 取值与既有节奏红线自洽（**不抬高**默认间隔，也不突破官方下限）：
#   channel 2s → 30 条/分；group 4s → 20 条/分（=官方群上限，不超）；
#   private 2s → 30 条/分。group 留 1s 抖动做错峰。
DEFAULT_MAX_PER_MINUTE = {"channel": 30, "private": 30, "group": 20}
DEFAULT_JITTER_SECONDS = {"channel": 0.0, "private": 0.0, "group": 1.0}
ACTIVE_TASK_STATUS = {"planned", "building", "ready", "submitted", "accepted", "publishing"}
TERMINAL_TASK_STATUS = {"published", "failed", "cancelled"}

# v2.0.5：TMDB 官方 genre 全集为影视分类唯一权威（源 tmdb.py::TMDB_GENRES，27 项）。
# 设计稿原写「连续剧/院线电影/综艺」，但那不是 TMDB 官方 genre 概念；按用户决策改为
# 官方全集。category_key 沿用 genre:<规范化中文名>，与 _primary_category 的归类口径
# 完全一致——这样分类字典、作品归类、环图统计、排期表天然对齐，不产生 key 分裂。
# 懒加载避免模块导入环（tmdb 不依赖本模块）。
def _tmdb_genre_catalog() -> list[dict[str, Any]]:
    from .tmdb import TMDB_GENRES
    tv_ids = {10759, 10762, 10763, 10764, 10765, 10766, 10767, 10768}
    out = []
    for gid, name in sorted(TMDB_GENRES.items(), key=lambda kv: kv[0]):
        out.append({"category_key": f"genre:{utils.normalize_work_name(name)}",
                    "tmdb_genre_id": gid, "name": name,
                    "media_type": "tv" if gid in tv_ids else "movie",
                    "source": "tmdb"})
    return out


# 分类排期默认窗口（每分类一行）。
# v2.1.1：此前是单一 (8, 24, 0, 0) —— 所有分类同质、且每小时/每日上限全是 0（=不限），
# 真机上表现为"排期表 27 行全是 8-24 且上限为 0"，规则形同虚设。
# 现按 TMDB 媒体类型分档内置默认：**时段沿用 8-24**（与计划版本的小时窗口一致，
# 避免上线后排期把上午时段掐掉造成"推送停了"的误判），差异体现在速率上限：
# 电视追更连续、量大 → 5 条/时、30 条/日；电影零散、量小 → 3 条/时、20 条/日。
# 分档依据是 media_type（真实派生），不是设计稿示例数字；
# 具体数值可在前端逐行调整并持久化，也可一键恢复默认。
DEFAULT_SCHEDULE_WINDOW = (8, 24, 0, 0)
DEFAULT_SCHEDULE_WINDOW_MOVIE = (8, 24, 3, 20)
DEFAULT_SCHEDULE_WINDOW_TV = (8, 24, 5, 30)


class Payload(BaseModel):
    payload: dict[str, Any] = {}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def shanghai_tz() -> timezone:
    return timezone(timedelta(hours=8))


def _jload(value: str | None, default):
    try:
        return json.loads(value or "")
    except Exception:
        return default


def _jdump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def is_enabled(db: Session) -> bool:
    """分享中心总开关（v2.1.1：前后端仅用一套，不再区分 v1/v2）。

    唯一开关是 share_center_enabled；share_center_v2_enabled 仅作旧配置兼容回退
    （两者都为 false 才关闭）。调用方据此判断是否驱动分享中心周期。
    """
    def _on(key: str, default: str = "true") -> bool:
        return str(runtime_config.get(db, key) or default).lower() in ("1", "true", "yes", "on")
    return _on("share_center_enabled") and _on("share_center_v2_enabled")


def _upgrade_hour_windows(db: Session, version_id: int) -> bool:
    """把计划版本的小时窗口升级为 0-24 点、每 2 小时一段共 12 段。

    v2.1.1（用户口径：时间改成 0-24 点，每两个小时为一段）：
      老库模板是 08:00–24:00 每小时一段共 16 段，只覆盖 8-23 点；
      夜间 0-8 点没有任何窗口命中 → _current_hour_allowed 返回 False →
      plan_engine 整轮空转，「偏好低的分类排在夜间」在真机上永远推不出去。

    升级条件（保守，避免覆盖用户配置）：
      ① 现有窗口全部是「未绑定分类的整点模板」（category_key 为空、段长 1 小时）；
      ② 或根本没有任何窗口（新建版本）。
    只要用户绑定过分类或改过段长/配额，就保持原样，由页面「恢复默认模板」处理。
    """
    rows = list(db.scalars(select(SCHourWindow).where(
        SCHourWindow.plan_version_id == version_id)).all())
    if rows:
        pristine = all((not str(r.category_key or "").strip())
                        and int(r.quota or 0) == 0
                        and int(r.end_hour or 0) - int(r.start_hour or 0) == 1
                        for r in rows)
        if not pristine:
            return False
        for r in rows:
            db.delete(r)
        db.flush()
    for start, cat in DEFAULT_HOUR_TEMPLATE:
        db.add(SCHourWindow(plan_version_id=version_id, start_hour=start,
                            end_hour=start + HOUR_SEGMENT_HOURS, category_key=cat,
                            quota=0, enabled=True))
    db.flush()
    return True


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
                # v2.1.1：内置每分钟上限与抖动。旧默认 max_per_minute=0（=不限）、
                # jitter=0（无抖动），真机上节奏卡显示两个 0，等于没有任何风控。
                # 按 Telegram 官方口径给保守默认：频道 20 条/分、群 15 条/分、私聊 10 条/分；
                # 抖动 0.5~1.5s 随机错峰，降低同秒连发被判定为机器人的概率。
                max_per_minute=DEFAULT_MAX_PER_MINUTE[target_type],
                jitter_seconds=DEFAULT_JITTER_SECONDS[target_type],
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
                                                         "window_start": 0,
                                                         "window_end": 24}))
        db.add(version)
        db.flush()
        changed = True
    # v2.1.1：把小时窗口统一到 0-24 点、每 2 小时一段共 12 段。
    # 老库是 08:00–24:00 每小时一段共 16 段——那段模板**只覆盖 8-23 点**，
    # 夜间 0-8 点没有窗口命中，_current_hour_allowed 直接 False，整轮空转，
    # 「偏好低的分类排夜间」在真机上永远推不出去。
    # 升级只在「仍是未绑定分类的旧整点模板」时做，用户自定义过的时段保持不动。
    if _upgrade_hour_windows(db, version.id):
        changed = True
    # v2.0.5：种子 TMDB 官方 genre 全集（27 项）作为分类字典。
    # priority=-1 让其排在真实归类（priority>=0）之后，不抢占 category_limit 轮转名额；
    # 用户启用/停用、改名、分类排期都在这套字典上操作。
    for cat in _tmdb_genre_catalog():
        row = db.scalar(select(SCCategory).where(SCCategory.category_key == cat["category_key"]))
        if not row:
            db.add(SCCategory(category_key=cat["category_key"], name=cat["name"],
                              source="tmdb", media_type=cat["media_type"],
                              enabled=True, priority=-1))
            changed = True
        elif row.name in ("", row.category_key):
            # 归一历史懒生成行（name 曾等于 category_key）为官方中文名
            row.name = cat["name"]
            row.media_type = cat["media_type"]
            changed = True
    if changed:
        db.commit()


# v2.0.4：默认排期模板。
# v2.1.1（用户口径：时间改成 0-24 点，每两个小时为一段）：
#   此前是 08:00–24:00 每小时一段共 16 段（整点粒度）。
#   现改为 **0-24 点、每 2 小时一段共 12 段**（0-2, 2-4, …, 22-24），
#   覆盖全天：分类只在自身偏好档位的时段里参与排期，夜里也有产出。
# category_key 默认留空 = 该时段不限分类（沿用全局分类轮转），不编造 PRD 截图里的
# 示例分类（那是设计稿数字，真实分类 key 由资源 TMDB genres 派生，见 _primary_category）。
# 管理员在前端为每个时段选择真实 SCCategory 后才绑定并驱动调度。
DEFAULT_HOUR_TEMPLATE: list[tuple[int, str]] = [
    (h, "") for h in range(0, 24, 2)
]
# 时段段长（小时）。v2.1.1：固定 2 小时一段，全天 12 段。
HOUR_SEGMENT_HOURS = 2
# 白天 / 夜间的分界小时（上海时区）。偏好高的分类排在白天段，偏好低的排在夜间段。
# 选 8 点为界：影视消费高峰在 8 点后，8 点前留给偏好低的分类（错峰，不空转）。
DAY_START_HOUR = 8
NIGHT_END_HOUR = 8
# 白天段 08:00–24:00（8 段）、夜间段 00:00–08:00（4 段），
# 两段**恰好铺满 0-24 全天 12 段且互不重叠**：
#   偏好高的分类 → 白天段（覆盖 8 点后的全部活跃时段，含 22-24 黄金档）；
#   偏好低的分类 → 夜间段（凌晨到清晨，长尾内容错峰投放）。
DAY_WINDOW = (DAY_START_HOUR, 24)
NIGHT_WINDOW = (0, NIGHT_END_HOUR)


def _hour_segments() -> list[tuple[int, int]]:
    """全天 2 小时一段的 (起, 止) 列表，共 12 段。"""
    return [(h, h + HOUR_SEGMENT_HOURS) for h in range(0, 24, HOUR_SEGMENT_HOURS)]


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
        # v2.0.5：字典种子行 priority=-1（不抢轮转名额）；一旦被真实资源归类命中，
        # 提升为 0 表示「有实际作品使用」，进入轮转。用户手动改过的 priority>0 不覆盖。
        if row.priority < 0:
            row.priority = 0
            if media_type:
                row.media_type = media_type
            db.flush()
        return
    db.add(SCCategory(category_key=category_key, name=name or category_key,
                      source="tmdb", media_type=media_type, enabled=True, priority=0))
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


def merge_update_windows(db: Session, window_seconds: int = 0) -> dict[str, int]:
    """把 N 分钟内同一作品的资源事件合并为一个更新窗口。

    v2.1.0：窗口秒数真实消费基础规则 sc_update_detect_minutes（默认 10 分钟），
    此前该键只落库不生效。显式传 window_seconds>0 时以传值为准（测试/内部调用）。
    """
    if window_seconds <= 0:
        try:
            detect_min = int(runtime_config.get(db, "sc_update_detect_minutes") or 10)
        except (TypeError, ValueError):
            detect_min = 10
        window_seconds = max(1, detect_min) * 60
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
        # v2.1.0 分流层收敛：B 库删除/移出事件在 v2 模式下也要触发链接失效联动。
        # 旧链 consume_resource_events 被切断后，removed/deleted 只能在这里处理，
        # 否则「B库删→链接失效」断链。share_link 是纯本地元数据状态机、无网络无队列，
        # 复用它不违反「v2 不读旧 ShareTask/ShareRecord」红线——ShareRecord 是分享链接
        # 生命周期表，不是旧推送任务表。异常吞掉不阻断窗口合并。
        if ev.type in ("removed", "deleted"):
            try:
                from . import share_link as _sl
                r1 = _sl.mark_suspicious(db, work_key,
                                         reason=f"{ev.type}:resource_removed_event")
                r2 = _sl.confirm_inactive_if_due(db, work_key)
                if r2.get("changed"):
                    try:
                        from . import share_center as _sc
                        _sc.retire_share_record(db, work_key,
                                                reason="link_inactive_confirmed")
                    except Exception:
                        pass
                    ev.consumed_at = now
                    merged += 1
                    continue
            except Exception:
                pass
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


def _current_hour_window(db: Session, version: SCSharePlanVersion) -> SCHourWindow | None:
    """当前小时命中的启用窗口（Asia/Shanghai）。多条重叠取第一条。"""
    hour = datetime.now(timezone(timedelta(hours=8))).hour
    return db.scalar(
        select(SCHourWindow).where(SCHourWindow.plan_version_id == version.id,
                                   SCHourWindow.enabled.is_(True),
                                   SCHourWindow.start_hour <= hour,
                                   SCHourWindow.end_hour > hour)
    )


def _current_hour_allowed(db: Session, version: SCSharePlanVersion) -> bool:
    windows = list(db.scalars(
        select(SCHourWindow).where(SCHourWindow.plan_version_id == version.id,
                                   SCHourWindow.enabled.is_(True))
    ).all())
    if not windows:
        return True
    return _current_hour_window(db, version) is not None


def _category_today_count(db: Session, category_key: str) -> int:
    """某分类当日已创建任务数（分类排期 daily_limit 用）。上海自然日。"""
    now_sh = datetime.now(shanghai_tz())
    day_start = datetime(now_sh.year, now_sh.month, now_sh.day,
                         tzinfo=shanghai_tz()).astimezone(timezone.utc)
    day_end = day_start + timedelta(days=1)
    return int(db.scalar(
        select(func.count(SCPublishTask.id))
        .join(SCPushListItem, SCPublishTask.list_item_id == SCPushListItem.id)
        .where(SCPushListItem.category_key == category_key,
               SCPublishTask.created_at >= day_start,
               SCPublishTask.created_at < day_end)
    ) or 0)


def _cycle_quota_gate(db: Session, cycle: dict[str, Any]) -> tuple[bool, str]:
    """周期计划日/周/月配额与休止窗口门禁（v2.1.0 真实消费，此前只落库）。

    统计口径：当日/周/月已提交的任务数（submitted_at 或 created_at 落库时间），
    0 = 不限。休止窗口命中时直接拦截（周末/月末指定小时后停普通排期），
    quiet 已在调用方处理。返回 (是否拦截, 原因)。
    """
    now_sh = datetime.now(shanghai_tz())
    # 周度休止：如「周六 20:00 后暂停普通排期」
    if cycle.get("week_rest_enabled"):
        rest_day = int(cycle.get("week_rest_day") or 6)  # 0=周一 ... 6=周日
        rest_hour = int(cycle.get("week_rest_hour") or 20)
        if now_sh.weekday() == rest_day and now_sh.hour >= rest_hour:
            return True, "weekly_rest"
    # 月度休止
    if cycle.get("month_rest_enabled"):
        month_end = (now_sh + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        if now_sh.date() == month_end.date():
            return True, "monthly_rest"

    def period_start(dt: datetime, kind: str) -> datetime:
        if kind == "day":
            return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        if kind == "week":
            return (dt - timedelta(days=dt.weekday())).replace(
                hour=0, minute=0, second=0, microsecond=0)
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    checks = (("day_max", "day"), ("week_max", "week"), ("month_max", "month"))
    for key, kind in checks:
        cap = int(cycle.get(key) or 0)
        if cap <= 0:
            continue
        start = period_start(now_sh, kind).astimezone(timezone.utc)
        if kind == "day":
            end = start + timedelta(days=1)
        elif kind == "week":
            end = start + timedelta(days=7)
        else:
            end = (period_start(now_sh, kind) + timedelta(days=32)).replace(day=1).astimezone(timezone.utc)
        used = db.scalar(select(func.count(SCPublishTask.id)).where(
            SCPublishTask.created_at >= start, SCPublishTask.created_at < end)) or 0
        if int(used) >= cap:
            return True, f"{kind}_quota"
    return False, ""


def _prune_empty_push_lists(db: Session) -> int:
    """清理无清单项的空清单（历史遗留垃圾），保留仍有数据的清单。

    v2.1.1：旧版 plan_engine 每轮新建一条清单，实机一天可堆积 4000+ 条空清单，
    把「发布清单」列表彻底淹没。此处只删**没有任何清单项**的清单行，
    有项的清单（含已归档状态）一律保留，不影响任务、分享记录与去重约束。
    """
    empty_ids = list(db.scalars(
        select(SCPushList.id)
        .where(~SCPushList.id.in_(select(SCPushListItem.list_id)))
        .order_by(SCPushList.id.desc())
    ).all())
    if not empty_ids:
        return 0
    # 保留最近 20 条空清单用于排查，其余删除（避免一次事务删太多行）
    doomed = empty_ids[20:]
    if not doomed:
        return 0
    db.execute(delete(SCPushList).where(SCPushList.id.in_(doomed)))
    db.flush()
    return len(doomed)


def plan_engine(db: Session) -> dict[str, int]:
    """按活动计划版本与分类游标生成推送清单、清单项和发布任务。"""
    version = _active_version(db)
    if not version or not _current_hour_allowed(db, version):
        return {"lists": 0, "items": 0, "tasks": 0}
    cfg = _jload(version.config_json, {})
    # v2.0.5：周期计划静默保护真实生效——命中静默时段不生成任何任务。
    cycle = cfg.get("cycle_plan") or {}
    if cycle.get("quiet_enabled"):
        sh_hour = datetime.now(shanghai_tz()).hour
        q_start = int(cycle.get("quiet_start") or 0)
        q_end = int(cycle.get("quiet_end") or 0)
        in_quiet = (q_start <= sh_hour < q_end) if q_end > q_start else (sh_hour >= q_start or sh_hour < q_end)
        if in_quiet:
            return {"lists": 0, "items": 0, "tasks": 0, "quiet": 1}
    # v2.1.0 分流层全量消费：周期计划日/周/月配额与休止窗口真实拦截（此前只落库）。
    quota_gate, quota_reason = _cycle_quota_gate(db, cycle)
    if quota_gate:
        return {"lists": 0, "items": 0, "tasks": 0, "quota": quota_reason}
    category_limit = max(1, int(cfg.get("category_limit") or 20))
    work_limit = max(1, int(cfg.get("work_limit") or 50))
    # v2.0.5：周期计划的小时上限真实夹逼单轮任务数（0=不限）
    hour_max = int((cycle.get("hour_max") or 0))

    channels = list(db.scalars(select(Channel).where(Channel.enabled.is_(True))).all())
    if not channels:
        return {"lists": 0, "items": 0, "tasks": 0}

    categories = list(db.scalars(
        select(SCCategory).where(SCCategory.enabled.is_(True))
        .order_by(SCCategory.priority.desc(), SCCategory.id)
    ).all())[:category_limit]
    # v2.0.4：小时分类排期真正驱动调度——当前时段绑定了真实分类时，只为该分类生成任务。
    # 绑定 key 不在启用分类字典中（如分类被停用）时退回全局轮转，不产出空清单。
    hour_window = _current_hour_window(db, version)
    if hour_window and hour_window.category_key:
        scoped = [c for c in categories if c.category_key == hour_window.category_key]
        if scoped:
            categories = scoped
    # v2.1.0：SCCategorySchedule 分类-时间排期真实生效——每分类按自己的窗口时段与
    # hourly/daily 上限过滤。此前整表只落库不被消费，前端配的排期不产生调度效果。
    # 某分类未启用排期行/配了窗口但当前小时不在窗口内 → 跳过该分类；上限 0 = 不限。
    sh_hour = datetime.now(shanghai_tz()).hour
    sched_map = {s.category_key: s for s in db.scalars(
        select(SCCategorySchedule).where(SCCategorySchedule.plan_version_id == version.id,
                                         SCCategorySchedule.enabled.is_(True))).all()}
    def _sched_allows(key: str, per_category_count: int) -> bool:
        s = sched_map.get(key)
        if not s:
            return True  # 未配置排期行的分类保持原轮转行为（兼容存量）
        ws, we = int(s.window_start or 0), int(s.window_end or 0)
        if we > ws:
            in_win = ws <= sh_hour < we
        elif we < ws:
            in_win = sh_hour >= ws or sh_hour < we  # 跨午夜
        else:
            in_win = True
        if not in_win:
            return False
        hl, dl = int(s.hourly_limit or 0), int(s.daily_limit or 0)
        if hl and per_category_count >= hl:
            return False
        if dl and _category_today_count(db, key) >= dl:
            return False
        return True

    category_task_counts: dict[str, int] = {}

    # v2.1.1：清单不再每轮新建。此前每轮 plan_engine 都用 uuid 建一条新 SCPushList，
    # 真机跑一天即堆积 4000+ 条空清单（列表页被"发布清单"淹没）。
    # 改为按「计划版本 + 自然日」复用同一条清单：同一天内多轮调度往同一清单追加项，
    # 跨天才新建。清单项本身有 dedup_key 唯一约束，重复轮转不会重复建项。
    day_key = datetime.now(shanghai_tz()).strftime("%Y%m%d")
    list_key = f"plan:{version.id}:{day_key}"
    push_list = db.scalar(select(SCPushList).where(SCPushList.list_key == list_key))
    if push_list is None:
        push_list = SCPushList(list_key=list_key, name=f"计划版本 {version.version_no} · {day_key}",
                               status="active", source="plan", plan_version_id=version.id)
        db.add(push_list)
        db.flush()
    else:
        # 复用已有清单：置回 active（历史清单可能已被归档），保证列表页能按状态筛到。
        push_list.status = "active"

    # 清理历史遗留的空清单（无任何清单项）——它们是旧版每轮新建策略留下的垃圾，
    # 只清空清单、不动任何有项的清单，不影响任务与分享记录。
    _prune_empty_push_lists(db)

    item_count = 0
    task_count = 0
    for category in categories:
        # v2.1.0：分类排期限流——当前小时不在该分类排期窗口、或已达每小时上限则跳过。
        if not _sched_allows(category.category_key, category_task_counts.get(category.category_key, 0)):
            continue
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
            select(SCLibraryVersion)
            .join(SCWorkCategory, SCWorkCategory.work_key == SCLibraryVersion.work_key)
            .where(SCWorkCategory.category_key == category.category_key)
            .where(SCLibraryVersion.id > cursor.last_library_version_id)
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
                category_task_counts[category.category_key] = category_task_counts.get(
                    category.category_key, 0) + 1
            cursor.last_library_version_id = lv.id
            cursor.last_work_key = lv.work_key
            cursor.last_success_at = utcnow()
            cursor.status = "idle"
            if hour_max and task_count >= hour_max:
                break
        if hour_max and task_count >= hour_max:
            break
    db.commit()
    return {"lists": 1, "items": item_count, "tasks": task_count}


class _RepResolver:
    """发布任务的代表资源解析器（v2.1.1）。

    根因：SCLibraryVersion.resource_ids_json 存的是**建快照那一刻**的资源 id 列表。
    资源被镜像替换或对账删除后，这些 id 全部失效，db.get 返回 None，
    导致每一个任务都报「代表资源不存在」——真机上表现为推送 100% 失败。
    修法：①先按快照 id 命中；②快照全失效时按 work_key 从**实时** Resource 表重新解析，
    与 sync_library_versions 同口径（work_group_key 分组 + file_id/size/utime 排序取首项）。
    """

    def __init__(self, db: Session):
        self.db = db
        self._by_key: dict[str, Resource] | None = None

    def _build(self) -> None:
        grouped: dict[str, list[Resource]] = {}
        for res in self.db.scalars(select(Resource)).all():
            grouped.setdefault(utils.work_group_key(res), []).append(res)
        table: dict[str, Resource] = {}
        for wk, rows in grouped.items():
            ordered = sorted(rows, key=lambda r: (str(r.file_id or ""), int(r.size or 0), int(r.utime or 0)))
            if ordered:
                table[wk] = ordered[0]
        self._by_key = table

    def resolve(self, lv: SCLibraryVersion) -> tuple[Resource | None, str]:
        """返回 (代表资源, 失败原因)。失败原因为空表示解析成功。"""
        for rid in _jload(lv.resource_ids_json, []) or []:
            try:
                res = self.db.get(Resource, int(rid))
            except (TypeError, ValueError):
                res = None
            if res is not None:
                return res, ""
        if self._by_key is None:
            self._build()
        live = (self._by_key or {}).get(str(lv.work_key or ""))
        if live is not None:
            return live, ""
        if _jload(lv.resource_ids_json, []) or []:
            return None, "作品资源已从资源库移除（快照资源失效且实时无匹配）"
        return None, "资源版本快照缺少资源 id"


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
    stats = {"built": 0, "submitted": 0, "failed": 0, "paused": 0, "throttled": 0}
    # v2.1.1：代表资源解析器在整轮内复用一次实时索引，避免每条任务全表扫描。
    _rep_resolver = _RepResolver(db)
    # v2.1.0：基础规则「上新即时推送每小时上限」真实消费。统计本自然小时已提交
    # (status=submitted 及以上) 的任务数，达上限则本轮不再提交新任务，留到下一小时。
    # 0/非法 = 不限；quick_publish_enabled=false 时也跳过该限流（保持旧行为）。
    hourly_cap = 0
    try:
        if str(runtime_config.get(db, "sc_quick_publish_enabled") or "true").lower() in (
                "1", "true", "yes", "on"):
            hourly_cap = max(0, int(runtime_config.get(db, "sc_quick_publish_hourly_limit") or 0))
    except (TypeError, ValueError):
        hourly_cap = 0
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    submitted_this_hour = 0
    if hourly_cap:
        submitted_this_hour = int(db.scalar(select(func.count(SCPublishTask.id)).where(
            SCPublishTask.status.in_({"submitted", "accepted", "published", "failed"}),
            SCPublishTask.scheduled_at >= hour_start)) or 0)
    for task in tasks:
        if hourly_cap and submitted_this_hour >= hourly_cap:
            stats["throttled"] += 1
            break
        item = db.get(SCPushListItem, task.list_item_id)
        lv = db.get(SCLibraryVersion, item.library_version_id) if item else None
        if not item or not lv:
            task.status = "failed"
            task.error = "清单项或资源版本不存在"
            db.commit()
            stats["failed"] += 1
            continue
        metadata = _jload(lv.metadata_json, {})
        rep, rep_err = _rep_resolver.resolve(lv)
        if not rep:
            task.status = "failed"
            task.error = rep_err or "代表资源不存在"
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
        submitted_this_hour += 1
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
# v2.0.5 设计稿真实聚合：趋势 / 分类占比 / 热门作品 / 三视图
# 全部来自 sc_* 与 Resource 真实表，不使用设计稿示例数字
# ---------------------------------------------------------------------------

def _aggregate_trend(db: Session, period: str = "day") -> dict[str, Any]:
    """推送趋势：published_at 成功计数 + created_at 失败计数，按日/周/月分桶。

    真实来源 sc_publish_tasks；无数据时 series 为空（前端显示空态，不画假曲线）。

    v2.1.1 修复两处真机异常：
    ① 粒度区分——此前 day 与 week 都返回 "%Y-%m-%d"，week 实际没按周聚合，
       且月视图只有 1 个桶时前端会画成三角形尖峰。现改为：
       day→按小时（HH:00）、week→按天、month→按天（近 30 天）。
    ② 补齐空桶——只补最近窗口内的零值桶，折线才有走势而不是孤立尖点。
       补齐的桶是真实计数 0，不是编造数据。
    """
    rows = list(db.scalars(select(SCPublishTask)).all())
    buckets: dict[str, dict[str, int]] = {}
    sh = timezone(timedelta(hours=8))

    def key_of(dt: datetime) -> str:
        local = dt.astimezone(sh) if dt.tzinfo else dt.replace(tzinfo=sh)
        if period == "hour":
            return local.strftime("%Y-%m-%d %H:00")
        if period == "month":
            return local.strftime("%Y-%m-%d")
        if period == "week":
            monday = local - timedelta(days=local.weekday())
            return monday.strftime("%Y-%m-%d")
        # day 视图按小时展开：一天的推送分布才有曲线形状
        return local.strftime("%H:00")

    def add(label: str, field: str) -> None:
        b = buckets.setdefault(label, {"published": 0, "failed": 0, "total": 0})
        b[field] += 1
        b["total"] += 1

    for t in rows:
        moment = t.published_at or t.updated_at or t.created_at
        if moment:
            add(key_of(moment), "published" if t.status == "published" else "failed")

    # v2.1.1：补齐空桶，让折线连续。单点时补一段 0 值区间，避免三角形尖峰。
    labels = sorted(buckets)
    if labels:
        series = [{"label": k, **buckets[k]} for k in labels]
        if len(series) == 1:
            first = series[0]["label"]
            if ":" in first and "-" not in first:
                # 小时粒度（HH:00）
                base = datetime.strptime(first, "%H:00")
                padded = []
                for i in range(4, 0, -1):
                    lbl = (base - timedelta(hours=i)).strftime("%H:00")
                    padded.append({"label": lbl, "published": 0, "failed": 0, "total": 0})
                padded.extend(series)
                for i in range(1, 5):
                    lbl = (base + timedelta(hours=i)).strftime("%H:00")
                    padded.append({"label": lbl, "published": 0, "failed": 0, "total": 0})
                series = padded
            else:
                # 天粒度（YYYY-MM-DD）
                base = datetime.strptime(first, "%Y-%m-%d")
                padded = []
                for i in range(6, 0, -1):
                    lbl = (base - timedelta(days=i)).strftime("%Y-%m-%d")
                    padded.append({"label": lbl, "published": 0, "failed": 0, "total": 0})
                padded.extend(series)
                for i in range(1, 7):
                    lbl = (base + timedelta(days=i)).strftime("%Y-%m-%d")
                    padded.append({"label": lbl, "published": 0, "failed": 0, "total": 0})
                series = padded
    else:
        series = []
    # 轻量时间上限，避免筛选器里塞入上一年所有历史 buckets
    return {"period": period, "series": series[-90:]}


def _aggregate_category_ratio(db: Session) -> dict[str, Any]:
    """分类推送占比：已发布任务 JOIN 清单项分类 + 分类字典中文名。"""
    rows = db.execute(
        select(SCPushListItem.category_key, func.count(SCPublishTask.id))
        .join(SCPublishTask, SCPublishTask.list_item_id == SCPushListItem.id)
        .where(SCPublishTask.status == "published")
        .group_by(SCPushListItem.category_key)
    ).all()
    names = {c.category_key: c.name for c in db.scalars(select(SCCategory)).all()}
    items = []
    total = sum(int(n or 0) for _, n in rows)
    for key, n in rows:
        count = int(n or 0)
        items.append({"category_key": str(key or ""),
                      "name": names.get(str(key or ""), str(key or "未分类")),
                      "count": count,
                      "ratio": round(count * 100 / total, 1) if total else 0.0})
    items.sort(key=lambda x: x["count"], reverse=True)
    return {"total": total, "items": items}


def _aggregate_hot_works(db: Session, limit: int = 5) -> dict[str, Any]:
    """热门作品榜：按 TMDB 评分 vote_average 取已识别作品 TopN（真实评分，非虚构热度）。

    vote_average 已在 Resource 落库（识别时写入），是系统内唯一可信的「作品热度」代理值。
    前端明确标注「来源：TMDB 评分」，不冒充设计稿的热度 98.2。
    """
    rows = list(db.scalars(
        select(Resource).where(Resource.vote_average > 0,
                               Resource.identify_status == "identified")
        .order_by(Resource.vote_average.desc(), Resource.id).limit(limit)
    ).all())
    # 同名作品（剧集多集）取一条代表
    seen: set[str] = set()
    items = []
    for r in rows:
        title = str(getattr(r, "media_title", "") or getattr(r, "name", ""))
        if title in seen:
            continue
        seen.add(title)
        items.append({"title": title, "score": round(float(r.vote_average or 0), 1),
                      "media_type": str(getattr(r, "media_type", "") or ""),
                      "tmdb_id": getattr(r, "tmdb_id", None),
                      "source": "TMDB 评分"})
    return {"items": items, "metric": "TMDB 评分", "note": "按已落库的 TMDB 评分排序，非设计稿示例热度值"}


def _aggregate_views(db: Session, limit: int = 12) -> dict[str, Any]:
    """推送三视图：历史(published)/队列(active)/失败(failed)真实任务行。"""
    def rows_for(statuses: set[str] | None) -> list[dict[str, Any]]:
        q = select(SCPublishTask)
        if statuses:
            q = q.where(SCPublishTask.status.in_(statuses))
        rows = db.scalars(q.order_by(SCPublishTask.id.desc()).limit(limit)).all()
        return [{k: v for k, v in _model_dict(r).items() if k in (
            "id", "work_key", "kind", "status", "link_state", "share_url",
            "error", "attempts", "created_at", "published_at", "updated_at")} for r in rows]

    return {"history": rows_for({"published"}),
            "queue": rows_for(ACTIVE_TASK_STATUS),
            "failed": rows_for({"failed"})}


def _aggregate_share_stats(db: Session) -> dict[str, Any]:
    """分享管理统计卡（v2.1.1）：口径统一到「清单项 + 任务」真实表。

    此前前端直接拿 link_state_counts（发布任务全量）当分享记录数，
    导致「待校验 146」这种和表格行数对不上的数字。真实口径：
      total     —— 有清单项的发布记录数（即表格里能看到的分享记录）
      active    —— 其中 link_state=active（已建链且有效）
      pending   —— 其中 link_state=unknown（已建单未建链/待校验）
      invalid   —— 任务已 failed 或链接被判失效
      history   —— 已 published 的记录数（历史归档）
    """
    total = int(db.scalar(select(func.count(SCPushListItem.id))) or 0)
    rows = db.execute(
        select(SCPublishTask.link_state, SCPublishTask.status, func.count(SCPublishTask.id))
        .join(SCPushListItem, SCPublishTask.list_item_id == SCPushListItem.id)
        .group_by(SCPublishTask.link_state, SCPublishTask.status)
    ).all()
    active = pending = invalid = history = 0
    for link_state, status, n in rows:
        n = int(n or 0)
        state = str(link_state or "unknown")
        st = str(status or "")
        if state == "active":
            active += n
        elif state in ("suspicious", "inactive", "cleaned"):
            invalid += n
        elif state == "unknown":
            pending += n
        if st == "published":
            history += n
        elif st == "failed":
            invalid += n
    # 失效与待校验会重叠于 failed+unknown 的组合，这里以任务终态为准做互斥修正
    return {"total": total, "active": active, "pending": pending,
            "invalid": invalid, "history": history}


# ---------------------------------------------------------------------------
# v2.1.1：用户偏好 → 分类时段分配（0-24 点、每 2 小时一段）
# ---------------------------------------------------------------------------

def _category_preference(db: Session) -> list[dict[str, Any]]:
    """按真实数据派生分类偏好排序（用户口径：偏好高的排白天，偏好低的排夜间）。

    排序采用**分层**口径（人工意图优先，消费热度次之）：
      ① 人工优先级 priority_rank = max(priority, 0)
         —— 管理员在分类字典里手工设置，越大越靠前；
         —— priority = -1 是「尚未参与轮转」的哨兵值（TMDB 种子分类的初始状态），
            不代表「偏好最低」，按 0 处理，否则所有未使用分类会被算成负分。
      ② 同优先级内按该分类**已成功推送**的作品数降序（真实消费热度）。
      ③ 完全同分时按分类 key 升序，保证结果稳定可复现。

    为什么不把两项加权求和：加权和会让「推送量」在量大时压过「人工偏好」，
    导致管理员明确排在前面的分类被历史数据挤到夜间。分层排序保证
    **人工设定永远优先**，热度只在其内部分配先后。

    不引入任何虚构权重：priority 是人工意图，published_count 是真实消费结果。
    """
    published_rows = db.execute(
        select(SCPushListItem.category_key, func.count(SCPublishTask.id))
        .join(SCPublishTask, SCPublishTask.list_item_id == SCPushListItem.id)
        .where(SCPublishTask.status == "published")
        .group_by(SCPushListItem.category_key)
    ).all()
    published = {str(k or ""): int(n or 0) for k, n in published_rows}
    items = []
    for row in db.scalars(select(SCCategory)).all():
        # priority=-1 是「尚未参与轮转」的哨兵值，不代表偏好最低，按 0 处理。
        priority_rank = max(0, int(row.priority or 0))
        pub = published.get(str(row.category_key), 0)
        items.append({
            "category_key": row.category_key,
            "name": row.name or row.category_key,
            "priority": int(row.priority or 0),
            "priority_rank": priority_rank,
            "published_count": pub,
            # 展示用偏好分：人工优先级为主、推送热度为辅（排序仍以分层键为准）
            "score": priority_rank * 100 + pub,
            "enabled": bool(row.enabled),
        })
    # 分层排序：人工优先级降序 → 推送热度降序 → 分类键升序（稳定可复现）
    items.sort(key=lambda x: (-x["priority_rank"], -x["published_count"], x["category_key"]))
    for idx, item in enumerate(items):
        item["rank"] = idx + 1
    return items


def _assign_day_night_windows(prefs: list[dict[str, Any]]) -> dict[str, tuple[int, int]]:
    """按偏好把分类分配到白天 / 夜间时段窗口（每 2 小时一段）。

    规则（用户口径）：
      - 偏好高的分类 → 白天段 08:00–24:00（DAY_WINDOW）；
      - 偏好低的分类 → 夜间段 00:00–08:00（NIGHT_WINDOW）。

    两段恰好铺满 0-24 全天 12 段且互不重叠，夜里也有产出、不会空转。

    分界按**中位数**：偏好排名前一半进白天、后一半进夜间。
    用中位数而不是写死条数，是为了让分类数量变化时不需要改代码，
    且不因「某分类分数异常高」把其余全挤进夜间。
    """
    if not prefs:
        return {}
    n = len(prefs)
    day_count = (n + 1) // 2
    out: dict[str, tuple[int, int]] = {}
    for idx, item in enumerate(prefs):
        out[item["category_key"]] = DAY_WINDOW if idx < day_count else NIGHT_WINDOW
    return out


@router.get("/preference")
def api_preference(db: Session = Depends(get_db)):
    """分类偏好排名（真实派生）+ 按偏好应分配的白天/夜间时段窗口。

    前端用于展示「偏好高的排白天、偏好低的排夜间」的分配依据，
    并提供一键按偏好重新分配时段。
    """
    prefs = _category_preference(db)
    windows = _assign_day_night_windows(prefs)
    items = []
    for item in prefs:
        ws, we = windows.get(item["category_key"], DAY_WINDOW)
        data = dict(item)
        data["window_start"] = ws
        data["window_end"] = we
        data["period"] = "day" if ws == DAY_WINDOW[0] else "night"
        data["cross_midnight"] = ws > we
        items.append(data)
    return {"items": items,
            "segments": _hour_segments(),
            "day_start_hour": DAY_START_HOUR,
            "segment_hours": HOUR_SEGMENT_HOURS,
            "day_window": list(DAY_WINDOW),
            "night_window": list(NIGHT_WINDOW),
            "source": "分类字典人工优先级（主）+ 该分类已成功推送任务数（次）"}


@router.post("/preference/assign")
def api_assign_preference_windows(body: Payload, db: Session = Depends(get_db)):
    """一键按用户偏好把分类排期重新分配到白天 / 夜间时段。

    只改**时段**（window_start/window_end），不动用户已设的每小时/每日上限，
    也不覆盖用户手工改过的时段——除非显式传 force=true。

    判定「是否用户手工改过」用 **window_auto 标记列**（1=自动分配，0=用户手工设定），
    不用「从未配置过」指纹：指纹要求上限全 0 + 时段 8-24，一旦行被默认建议值
    回填过（上限非 0），指纹即失效，会导致本接口永远跳过所有行、分配形同虚设。
    """
    p = body.payload or {}
    force = bool(p.get("force"))
    version = _active_version(db)
    if not version:
        return {"ok": False, "reason": "无活动计划版本"}
    prefs = _category_preference(db)
    if not prefs:
        return {"ok": False, "reason": "分类字典为空，无法按偏好分配"}
    windows = _assign_day_night_windows(prefs)
    rows = {r.category_key: r for r in db.scalars(
        select(SCCategorySchedule).where(
            SCCategorySchedule.plan_version_id == version.id)).all()}
    changed = 0
    skipped = 0
    for item in prefs:
        ws, we = windows.get(item["category_key"], DAY_WINDOW)
        row = rows.get(item["category_key"])
        if row is None:
            db.add(SCCategorySchedule(
                plan_version_id=version.id, category_key=item["category_key"],
                window_start=ws, window_end=we,
                hourly_limit=DEFAULT_SCHEDULE_WINDOW_MOVIE[2],
                daily_limit=DEFAULT_SCHEDULE_WINDOW_MOVIE[3],
                ai_adopted=False, enabled=True, window_auto=True))
            changed += 1
            continue
        # 用户手工设定过的行永不被自动分配覆盖，除非 force。
        if not force and not bool(row.window_auto):
            skipped += 1
            continue
        if (int(row.window_start or 0) != ws or int(row.window_end or 0) != we):
            row.window_start, row.window_end = ws, we
            row.window_auto = True
            row.updated_at = utcnow()
            changed += 1
    db.commit()
    return {"ok": True, "changed": changed, "skipped": skipped, "total": len(prefs)}


# ---------------------------------------------------------------------------
# API：前端只作为管理入口，所有按钮走真实后端
# ---------------------------------------------------------------------------

@router.get("/overview")
def api_overview(db: Session = Depends(get_db), period: str = "day"):
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
    # 推送清单三视图计数（按 kind × status 交叉，真实派生，不写死）
    kind_status_rows = db.execute(
        select(SCPublishTask.kind, SCPublishTask.status, func.count(SCPublishTask.id))
        .group_by(SCPublishTask.kind, SCPublishTask.status)
    ).all()
    kind_status = {(str(k), str(s)): int(n or 0) for k, s, n in kind_status_rows}

    def _ks(kinds: tuple[str, ...], statuses: tuple[str, ...]) -> int:
        return sum(v for (k, s), v in kind_status.items() if k in kinds and s in statuses)

    unfinished = ("planned", "ready", "building", "submitted", "accepted", "publishing", "failed")
    push_list_counts = {
        "pending_first": _ks(("first",), unfinished),
        "pending_update": _ks(("update",), unfinished),
        "published_first": _ks(("first",), ("published",)),
    }

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
    active_version = _active_version(db)
    # v2.0.5 设计稿总览：趋势 / 分类占比 / 热门作品 / 三视图（全部真实聚合）
    now_sh = datetime.now(shanghai_tz())
    day_start_utc = datetime(now_sh.year, now_sh.month, now_sh.day, tzinfo=shanghai_tz()).astimezone(timezone.utc)
    day_end_utc = day_start_utc + timedelta(days=1)
    today_published = db.scalar(select(func.count(SCPublishTask.id)).where(
        SCPublishTask.status == "published",
        SCPublishTask.published_at >= day_start_utc,
        SCPublishTask.published_at < day_end_utc)) or 0
    total_terminal = status_counts.get("published", 0) + status_counts.get("failed", 0)
    success_rate = round(published_total * 100 / total_terminal, 1) if total_terminal else 0.0
    enriched_kpi = dict(kpi)
    enriched_kpi.update({
        "pending_list": push_list_counts["pending_first"] + push_list_counts["pending_update"],
        "today_published": today_published,
        "auto_delivered": status_counts.get("published", 0),
        "success_rate": success_rate,
        "weekly_ai_candidates": db.scalar(select(func.count(SCAISuggestion.id)).where(
            SCAISuggestion.status == "pending")) or 0,
        "failed_pending": status_counts.get("failed", 0),
    })
    return {"enabled": is_enabled(db), "paused": _is_paused(db), "counts": counts,
            "status_counts": status_counts, "pacing": pacing, "kpi": enriched_kpi,
            "link_state_counts": link_state_counts, "kind_counts": kind_counts,
            "schedule": schedule, "windows": windows, "recent_tasks": recent_tasks,
            "push_list_counts": push_list_counts,
            "share_stats": _aggregate_share_stats(db),
            "active_version_id": active_version.id if active_version else 0,
            "trend": _aggregate_trend(db, period if period in ("day", "week", "month") else "day"),
            "category_ratio": _aggregate_category_ratio(db),
            "hot_works": _aggregate_hot_works(db),
            "views": _aggregate_views(db)}


@router.get("/share-records")
def api_share_records(db: Session = Depends(get_db), limit: int = 200):
    """分享管理记录列表（v2.1.1）。

    真实来源：sc_push_list_items JOIN sc_publish_tasks，
    每条代表一次「作品 × 目标」的分享记录。附带参考图所需的列：
    share_code（分享码，从 share_url 尾段解析）、share_title（作品标题，真实可读名）、
    last_checked_at（最后校验时间）、last_pushed_at（最近推送时间）。
    没有分享链接的记录不返回——分享管理管的是**已建链**的分享。
    """
    rows = db.execute(
        select(SCPushListItem, SCPublishTask)
        .join(SCPublishTask, SCPublishTask.list_item_id == SCPushListItem.id)
        .where(SCPublishTask.share_url.isnot(None), SCPublishTask.share_url != "")
        .order_by(SCPublishTask.id.desc()).limit(min(limit, 500))).all()
    names = {c.category_key: c.name for c in db.scalars(select(SCCategory)).all()}
    items = []
    for item, task in rows:
        data = _model_dict(task)
        title = str(item.work_key or "")
        if title.startswith("work:"):
            title = title[5:]
        items.append({
            "id": task.id,
            "list_item_id": item.id,
            "work_key": item.work_key,
            "share_title": title,
            "kind": data.get("kind") or "first",
            "category_key": item.category_key,
            "category_name": names.get(item.category_key, item.category_key),
            "share_url": data.get("share_url") or "",
            "share_code": _share_code_from_url(data.get("share_url") or ""),
            "link_state": data.get("link_state") or "unknown",
            "status": data.get("status") or "",
            "chat_id": item.chat_id,
            "target_type": item.target_type,
            "error": data.get("error") or "",
            "created_at": data.get("created_at"),
            "last_checked_at": data.get("updated_at"),
            "last_pushed_at": data.get("published_at"),
            "published_at": data.get("published_at"),
        })
    return {"items": items, "stats": _aggregate_share_stats(db)}


def _share_code_from_url(url: str) -> str:
    """从分享链接提取可展示的分享码（尾段去扩展名），非空才返回。"""
    text = str(url or "").strip()
    if not text:
        return ""
    tail = text.rstrip("/").rsplit("/", 1)[-1]
    if not tail or tail == text:
        return ""
    return tail.split("?", 1)[0].split("#", 1)[0]


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
    # v2.0.4：小时分类排期——绑定该时段主分类（空=不限分类，沿用全局轮转）
    row.category_key = str(p.get("category_key") or "").strip()
    row.quota = int(p.get("quota") or 0)
    row.enabled = bool(p.get("enabled", True))
    db.commit()
    return {"ok": True, "item": _model_dict(row)}


@router.post("/hour-windows/reset-template")
def api_reset_hour_template(body: Payload, db: Session = Depends(get_db)):
    """恢复默认 00:00–24:00 共 12 段（每 2 小时一段）模板（分类留空=按计划轮转）。幂等：先删该版本旧窗口。"""
    version_id = int((body.payload or {}).get("plan_version_id") or 0)
    if not version_id or not db.get(SCSharePlanVersion, version_id):
        return {"ok": False, "reason": "计划版本不存在，请先选择有效版本"}
    for old in db.scalars(select(SCHourWindow).where(
            SCHourWindow.plan_version_id == version_id)).all():
        db.delete(old)
    db.flush()
    for start, cat in DEFAULT_HOUR_TEMPLATE:
        db.add(SCHourWindow(plan_version_id=version_id, start_hour=start,
                            end_hour=start + HOUR_SEGMENT_HOURS, category_key=cat,
                            quota=0, enabled=True))
    db.commit()
    return {"ok": True, "version_id": version_id, "segments": len(DEFAULT_HOUR_TEMPLATE)}


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


# ---------------------------------------------------------------------------
# v2.0.5 发布规则端点：TMDB 分类字典 / 分类时间排期 / 基础规则 / 周期计划
# ---------------------------------------------------------------------------

@router.get("/categories/catalog")
def api_category_catalog(db: Session = Depends(get_db)):
    """TMDB 官方 genre 全集字典（27 项），带本地启用/改名/priority 状态。"""
    ensure_defaults(db)
    local = {c.category_key: c for c in db.scalars(select(SCCategory)).all()}
    items = []
    for cat in _tmdb_genre_catalog():
        row = local.get(cat["category_key"])
        items.append({**cat,
                      "id": row.id if row else 0,
                      "enabled": bool(row.enabled) if row else True,
                      "priority": int(row.priority) if row else -1,
                      "custom_name": row.name if row and row.name != cat["name"] else ""})
    return {"items": items, "source": "tmdb.py::TMDB_GENRES", "total": len(items)}


def _default_limits_for(media_type: str) -> tuple[int, int]:
    """按 TMDB 媒体类型取内置默认速率上限（每小时, 每日）。

    v2.1.1：这是「开箱即用」的默认建议值——分类排期不能是 0/0
    （0 = 不限，等于没有限流规则），否则整个排期机制形同虚设。
    分档依据是真实 media_type，不是设计稿示例数字：
      电视（长线追更，量大）→ 5 条/时，30 条/日
      电影（零散，量小）    → 3 条/时，20 条/日
    """
    if media_type == "tv":
        return DEFAULT_SCHEDULE_WINDOW_TV[2], DEFAULT_SCHEDULE_WINDOW_TV[3]
    return DEFAULT_SCHEDULE_WINDOW_MOVIE[2], DEFAULT_SCHEDULE_WINDOW_MOVIE[3]


def _default_schedule_for(media_type: str,
                          window: tuple[int, int] | None = None) -> tuple[int, int, int, int]:
    """按 TMDB 媒体类型取内置默认排期（时段起, 时段止, 每小时上限, 日上限）。

    v2.1.1：这是「开箱即用」的默认建议值——分类排期不能是 8-24 + 0/0
    （0 = 不限，等于没有限流规则），否则整个排期机制形同虚设。
    分档依据是真实 media_type，不是设计稿示例数字。

    v2.1.1（用户口径：按偏好高的分类排白天、偏好低的排夜间）：
    时段默认由**偏好分配**决定（window 参数传入白天/夜间窗口），
    只有媒体类型决定速率上限。这样「一键应用默认建议」与
    「按偏好分配时段」两套动作口径一致，不会互相覆盖。
    """
    hl, dl = _default_limits_for(media_type)
    ws, we = window if window else (DEFAULT_SCHEDULE_WINDOW_MOVIE[0], DEFAULT_SCHEDULE_WINDOW_MOVIE[1])
    return ws, we, hl, dl


def _preference_windows(db: Session) -> dict[str, tuple[int, int]]:
    """当前偏好排名对应的白天/夜间时段窗口（分类 key → (起, 止)）。"""
    return _assign_day_night_windows(_category_preference(db))


def _is_untouched_schedule(row: SCCategorySchedule) -> bool:
    """判断一行分类排期是否「从未被用户配置过」。

    指纹 = 上限全 0（不限）+ 时段仍是初始 8-24。这是 v2.0.5 建表时写入的默认值，
    用户在页面上保存过就会带上真实时段/上限（哪怕填的是 0），指纹随之失效。
    用途：只对「未配置过」的行回填默认建议值，不覆盖用户已改过的行。
    """
    return (int(row.hourly_limit or 0) == 0
            and int(row.daily_limit or 0) == 0
            and int(row.window_start or 0) == 8
            and int(row.window_end or 0) == 24)


def _ensure_schedules(db: Session, version_id: int) -> list[SCCategorySchedule]:
    """为版本幂等补齐所有 TMDB 官方分类的排期行，并回填内置默认建议值。

    v2.1.1（用户口径：排期要内置默认建议值，避免流程跑不通）：
      ① 新建行直接写入按媒体类型分档的默认时段与上限，并默认启用——
         之前 enabled=False + 0/0，等于新建后调度完全不受排期约束，
         用户勾选启用才生效但参数仍是「不限」，配置形同虚设。
      ② **存量行回填**：已存在的行若是「全零上限 + 默认 8-24 时段」，
         说明从未被用户配置过（未配置 = 空白），回填默认建议值**并启用**，
         让排期规则开箱即用；用户已改过的行保持原样，不覆盖。

    v2.1.1（用户口径：按偏好高的分类排白天、偏好低的排夜间）：
      ③ 时段不再写死 8-24，而是取**偏好分配**给出的白天/夜间窗口，
         新建行一落库就落在正确的时段档位上。
    """
    rows = {r.category_key: r for r in db.scalars(
        select(SCCategorySchedule).where(SCCategorySchedule.plan_version_id == version_id)).all()}
    windows = _preference_windows(db)
    added = False
    changed = False
    for cat in _tmdb_genre_catalog():
        key = cat["category_key"]
        win = windows.get(key, DAY_WINDOW)
        ws, we, hl, dl = _default_schedule_for(cat.get("media_type") or "", win)
        existing = rows.get(key)
        if existing is None:
            row = SCCategorySchedule(plan_version_id=version_id, category_key=key,
                                     window_start=ws, window_end=we, hourly_limit=hl,
                                     daily_limit=dl, ai_adopted=False, enabled=True,
                                     window_auto=True)
            db.add(row)
            added = True
            continue
        # v2.1.1：用户手工设定过的行（window_auto=False）永不被系统回填覆盖——
        # 即使用户恰好保存了 0/0 上限 + 8-24 时段，也属于「已配置」，不该被改写。
        if not bool(existing.window_auto):
            continue
        # 存量行回填：从未配置过（上限全 0 且时段仍是初始 8-24）才回填默认建议值。
        if not _is_untouched_schedule(existing):
            continue
        if (int(existing.hourly_limit or 0) != hl
                or int(existing.daily_limit or 0) != dl
                or int(existing.window_start or 0) != ws
                or int(existing.window_end or 0) != we
                or not existing.enabled):
            existing.window_start, existing.window_end = ws, we
            existing.hourly_limit, existing.daily_limit = hl, dl
            existing.enabled = True
            existing.window_auto = True
            existing.updated_at = utcnow()
            changed = True
    if added or changed:
        db.flush()
    return list(db.scalars(select(SCCategorySchedule).where(
        SCCategorySchedule.plan_version_id == version_id)).all())


def _ai_suggest_window(db: Session, category_key: str) -> tuple[int, int]:
    """AI 建议时段：按该分类已发布任务的小时分布取最密集连续小时（真实派生，不编造）。

    无历史发布数据时返回 (0,0)，前端显示「暂无建议」，不伪造时段。
    """
    rows = db.execute(
        select(SCPublishTask.published_at)
        .join(SCPushListItem, SCPublishTask.list_item_id == SCPushListItem.id)
        .where(SCPublishTask.status == "published",
               SCPushListItem.category_key == category_key)
    ).all()
    hours: dict[int, int] = {}
    for (moment,) in rows:
        if moment:
            h = moment.astimezone(shanghai_tz()).hour
            hours[h] = hours.get(h, 0) + 1
    if not hours:
        return (0, 0)
    peak_hour = max(hours, key=lambda h: (hours[h], -h))
    return (peak_hour, min(24, peak_hour + 2))


@router.post("/category-schedules/apply-defaults")
def api_apply_schedule_defaults(body: Payload, db: Session = Depends(get_db)):
    """一键把全部分类排期应用为内置默认建议值（v2.1.1）。

    用户口径「排期要内置默认建议值，避免流程跑不通」：提供一键初始化入口，
    把每行排期按 TMDB 媒体类型写成默认时段 + 默认上限，并启用。
    仅覆盖**从未配置过**的行（上限全 0 且时段为初始 8-24），
    用户已改过的行保持原样；如需强制覆盖请传 force=true。

    注意：_ensure_schedules 已会把「未配置过」的行回填为默认值，所以
    「未配置过」必须在本函数调用 _ensure_schedules **之前**快照——
    否则回填后指纹失效，一键应用会永远 applied=0（v2.1.1 实测缺陷）。
    """
    p = body.payload
    version = _active_version(db)
    if not version:
        return {"ok": False, "reason": "无活动计划版本"}
    force = bool(p.get("force"))
    # 先快照「从未配置过」的行（回填前取，否则指纹被回填动作抹掉）
    before = list(db.scalars(select(SCCategorySchedule).where(
        SCCategorySchedule.plan_version_id == version.id)).all())
    untouched_keys = {r.category_key for r in before if _is_untouched_schedule(r)}
    rows = _ensure_schedules(db, version.id)
    windows = _preference_windows(db)
    applied = 0
    updated = 0
    for row in rows:
        cat = next((c for c in _tmdb_genre_catalog()
                    if c["category_key"] == row.category_key), None)
        win = windows.get(row.category_key, DAY_WINDOW)
        ws, we, hl, dl = _default_schedule_for((cat or {}).get("media_type") or "", win)
        if not force and row.category_key not in untouched_keys:
            continue
        changed = (int(row.window_start or 0) != ws
                   or int(row.window_end or 0) != we
                   or int(row.hourly_limit or 0) != hl
                   or int(row.daily_limit or 0) != dl
                   or not row.enabled)
        row.window_start, row.window_end = ws, we
        row.hourly_limit, row.daily_limit = hl, dl
        row.enabled = True
        # v2.1.1：一键应用默认 = 系统托管 → 该行重新可被「按偏好分配时段」刷新。
        row.window_auto = True
        row.updated_at = utcnow()
        applied += 1
        if changed:
            updated += 1
    db.commit()
    return {"ok": True, "applied": applied, "updated": updated, "total": len(rows)}


@router.post("/category-schedules/reset-default")
def api_reset_category_schedule_default(body: Payload, db: Session = Depends(get_db)):
    """把一行分类排期恢复为该媒体类型的内置默认（v2.1.1）。

    参考图「操作」列有「默认」按钮：按 TMDB 媒体类型分档的内置默认
    （电影 15:00–24:00 / 3 条每时 / 20 条每日；电视 10:00–24:00 / 5 条每时 / 30 条每日）。
    """
    p = body.payload
    row = db.get(SCCategorySchedule, int(p.get("id") or 0))
    if not row:
        return {"ok": False, "reason": "排期行不存在"}
    cat = next((c for c in _tmdb_genre_catalog()
                if c["category_key"] == row.category_key), None)
    media_type = cat.get("media_type") if cat else ""
    # v2.1.1：时段取偏好分配（白天/夜间），与「按偏好分配时段」口径一致。
    win = _preference_windows(db).get(row.category_key, DAY_WINDOW)
    ws, we, hl, dl = _default_schedule_for(media_type, win)
    row.window_start, row.window_end = ws, we
    row.hourly_limit, row.daily_limit = hl, dl
    row.ai_adopted = False
    # v2.1.1：「恢复默认」= 交还系统托管 → 该行重新可被「按偏好分配时段」刷新。
    row.window_auto = True
    row.updated_at = utcnow()
    db.commit()
    return {"ok": True, "item": _model_dict(row)}


@router.get("/category-schedules")
def api_category_schedules(version_id: int = 0, db: Session = Depends(get_db)):
    """分类-时间排期表：每分类一行，含 AI 建议时段（真实派生）。"""
    version = _active_version(db) if not version_id else db.get(SCSharePlanVersion, version_id)
    if not version:
        return {"items": [], "version_id": 0, "reason": "无活动计划版本"}
    rows = _ensure_schedules(db, version.id)
    db.commit()
    names = {c.category_key: c.name for c in db.scalars(select(SCCategory)).all()}
    cat_map = {c["category_key"]: c for c in _tmdb_genre_catalog()}
    items = []
    for r in rows:
        d = _model_dict(r)
        d["name"] = names.get(r.category_key, cat_map.get(r.category_key, {}).get("name", r.category_key))
        d["tmdb_genre_id"] = cat_map.get(r.category_key, {}).get("tmdb_genre_id", 0)
        ai_start, ai_end = _ai_suggest_window(db, r.category_key)
        if not r.ai_adopted:
            d["ai_window_start"], d["ai_window_end"] = ai_start, ai_end
        # v2.1.1：补媒体类型，前端「周期维度」列据此区分默认档位（tv / movie）。
        d["media_type"] = cat_map.get(r.category_key, {}).get("media_type", "")
        items.append(d)
    items.sort(key=lambda x: (x.get("tmdb_genre_id") or 0))
    return {"items": items, "version_id": version.id}


@router.post("/category-schedules")
def api_upsert_category_schedule(body: Payload, db: Session = Depends(get_db)):
    p = body.payload
    active = _active_version(db)
    version_id = int(p.get("plan_version_id") or (active.id if active else 0))
    category_key = str(p.get("category_key") or "").strip()
    if not version_id or not category_key:
        return {"ok": False, "reason": "plan_version_id 与 category_key 必填"}
    row = db.scalar(select(SCCategorySchedule).where(
        SCCategorySchedule.plan_version_id == version_id,
        SCCategorySchedule.category_key == category_key))
    if not row:
        row = SCCategorySchedule(plan_version_id=version_id, category_key=category_key)
        db.add(row)
    row.window_start = max(0, min(23, int(p.get("window_start") or 0)))
    row.window_end = max(row.window_start + 1, min(24, int(p.get("window_end") or 24)))
    row.hourly_limit = max(0, int(p.get("hourly_limit") or 0))
    row.daily_limit = max(0, int(p.get("daily_limit") or 0))
    row.ai_window_start = max(0, min(23, int(p.get("ai_window_start") or 0)))
    row.ai_window_end = max(row.ai_window_start + 1, min(24, int(p.get("ai_window_end") or 0))) if p.get("ai_window_end") else 0
    row.ai_adopted = bool(p.get("ai_adopted", False))
    # v2.1.1：enabled 不传时**保持原值**。此前默认 False，导致前端保存单行排期
    # （未显式带 enabled）会把该分类意外停用，排期形同虚设。
    if "enabled" in p:
        row.enabled = bool(p.get("enabled"))
    # v2.1.1：用户在页面上手工保存过该行 → 标记 window_auto=False，
    # 之后「按偏好分配时段」不再覆盖这一行（用户手工设定优先）。
    row.window_auto = False
    row.updated_at = utcnow()
    db.commit()
    return {"ok": True, "item": _model_dict(row)}


@router.post("/category-schedules/adopt-ai")
def api_adopt_schedule_ai(body: Payload, db: Session = Depends(get_db)):
    """采纳 AI 建议时段：把真实派生的建议窗口写入当前配置并标记已采纳。"""
    p = body.payload
    row = db.get(SCCategorySchedule, int(p.get("id") or 0))
    if not row:
        return {"ok": False, "reason": "排期行不存在"}
    ai_start, ai_end = _ai_suggest_window(db, row.category_key)
    if not ai_start:
        return {"ok": False, "reason": "该分类暂无历史发布数据，无真实建议可采纳"}
    row.window_start, row.window_end = ai_start, ai_end
    row.ai_adopted = True
    # v2.1.1：采纳 AI 建议是用户手工动作 → 该行不再被「按偏好分配时段」覆盖。
    row.window_auto = False
    row.updated_at = utcnow()
    db.commit()
    return {"ok": True, "window_start": ai_start, "window_end": ai_end}


@router.post("/category-schedules/copy")
def api_copy_category_schedule(body: Payload, db: Session = Depends(get_db)):
    """把一行排期配置复制到目标分类（真实写库）。"""
    p = body.payload
    src = db.get(SCCategorySchedule, int(p.get("id") or 0))
    target_keys = [str(x).strip() for x in (p.get("target_keys") or []) if str(x).strip()]
    if not src or not target_keys:
        return {"ok": False, "reason": "源排期与目标分类必填"}
    version_id = src.plan_version_id
    copied = 0
    for key in target_keys:
        if key == src.category_key:
            continue
        row = db.scalar(select(SCCategorySchedule).where(
            SCCategorySchedule.plan_version_id == version_id,
            SCCategorySchedule.category_key == key))
        if not row:
            row = SCCategorySchedule(plan_version_id=version_id, category_key=key)
            db.add(row)
        row.window_start, row.window_end = src.window_start, src.window_end
        row.hourly_limit, row.daily_limit = src.hourly_limit, src.daily_limit
        row.enabled = src.enabled
        row.ai_adopted = False
        # v2.1.1：复制是用户手工动作 → 目标行不再被「按偏好分配时段」覆盖。
        row.window_auto = False
        row.updated_at = utcnow()
        copied += 1
    db.commit()
    return {"ok": True, "copied": copied}


BASIC_RULE_KEYS = {
    "quick_publish_enabled": "sc_quick_publish_enabled",
    "update_detect_minutes": "sc_update_detect_minutes",
    "quick_publish_hourly_limit": "sc_quick_publish_hourly_limit",
    "conflict_policy": "sc_conflict_policy",
}


@router.get("/basic-rules")
def api_basic_rules(db: Session = Depends(get_db)):
    """上新即时推送基础规则（真实持久化于 app_config）。"""
    vals = runtime_config.get_many(db, list(BASIC_RULE_KEYS.values()))
    return {field: vals.get(key, "") for field, key in BASIC_RULE_KEYS.items()}


@router.post("/basic-rules")
def api_save_basic_rules(body: Payload, db: Session = Depends(get_db)):
    p = body.payload
    values = {}
    for field, key in BASIC_RULE_KEYS.items():
        if field in p:
            values[key] = str(p[field])
    if values:
        runtime_config.set_many(db, values)
        db.commit()
    return {"ok": True, "rules": api_basic_rules(db)}


# 周期计划默认值。
# v2.1.1（用户口径：时间改成 0-24 点，夜间也要有产出）：
#   此前 quiet_start=0 / quiet_end=7 且 quiet_enabled=True，把 0-8 点整段掐死；
#   而「偏好低的分类排夜间」正是要用 0-8 这段——两者直接冲突，导致夜间分类永远推不出去。
#   因此默认改为：静默保护**默认关闭**（quiet_enabled=False），保留 0-7 的时间范围，
#   用户想要「深夜完全静默」时可在页面上单独打开。
#   同时 window_start/window_end 由 8-24 放宽为 0-24，与全天分段的新口径一致。
CYCLE_PLAN_DEFAULTS = {
    "hour_max": 60, "category_hour_max": 8, "quiet_start": 0, "quiet_end": 7,
    "quiet_enabled": False, "day_max": 180, "window_start": 0, "window_end": 24,
    "week_max": 1200, "week_rest_enabled": False, "week_rest_day": 6, "week_rest_hour": 20,
    "month_max": 5000, "month_rest_enabled": False,
}

@router.get("/cycle-plan")
def api_cycle_plan(db: Session = Depends(get_db)):
    """周期计划（小时/日/周/月配额 + 静默保护），持久化于活动版本 config_json.cycle_plan。"""
    version = _active_version(db)
    if not version:
        return {**CYCLE_PLAN_DEFAULTS, "version_id": 0}
    cfg = _jload(version.config_json, {})
    saved = cfg.get("cycle_plan") or {}
    return {**CYCLE_PLAN_DEFAULTS, **saved, "version_id": version.id}


@router.post("/cycle-plan")
def api_save_cycle_plan(body: Payload, db: Session = Depends(get_db)):
    version = _active_version(db)
    if not version:
        return {"ok": False, "reason": "无活动计划版本"}
    cfg = _jload(version.config_json, {})
    saved = {k: body.payload.get(k, v) for k, v in CYCLE_PLAN_DEFAULTS.items() if k in body.payload}
    cycle = {**CYCLE_PLAN_DEFAULTS, **(cfg.get("cycle_plan") or {}), **saved}
    # 数值夹逼，防非法输入破坏调度
    for k in ("hour_max", "category_hour_max", "day_max", "week_max", "month_max"):
        cycle[k] = max(0, int(cycle.get(k) or 0))
    for k in ("quiet_start", "window_start", "week_rest_hour"):
        cycle[k] = max(0, min(23, int(cycle.get(k) or 0)))
    for k in ("quiet_end", "window_end"):
        cycle[k] = max(1, min(24, int(cycle.get(k) or 24)))
    cfg["cycle_plan"] = cycle
    version.config_json = _jdump(cfg)
    db.commit()
    return {"ok": True, "cycle_plan": cycle}


def _model_dict(row) -> dict[str, Any]:
    data = {}
    for column in row.__table__.columns:
        value = getattr(row, column.name)
        if isinstance(value, datetime):
            value = value.isoformat()
        data[column.name] = value
    return data


@router.get("/diagnostics")
def api_diagnostics(db: Session = Depends(get_db)):
    """只读链路诊断（v2.1.0）：暴露四个板块的关键开关、生效链路与段间状态。

    只读，不改任何配置、不触发任何周期。用于核实「资源库→分享中心→分发中心」
    当前到底走哪条链、规则是否生效、各段有无积压。前端「分享总览」可显示。
    """
    from . import share_center as _sc

    def _on(val: Any) -> bool:
        return str(val or "false").lower() in ("1", "true", "yes", "on")

    v2_on = _on(is_enabled(db))
    old_on = _on(_sc.is_enabled(db)) and not v2_on
    active_chain = "v2" if v2_on else ("legacy" if old_on else "off")
    version = _active_version(db)
    cycle = ({**CYCLE_PLAN_DEFAULTS, **((_jload(version.config_json, {}).get("cycle_plan") or {}) if version else {})})
    task_status: dict[str, int] = {}
    for (st,) in db.execute(select(SCPublishTask.status).group_by(SCPublishTask.status)).all():
        task_status[st] = int(db.scalar(select(func.count(SCPublishTask.id)).where(
            SCPublishTask.status == st)) or 0)
    pending_events = int(db.scalar(select(func.count(ResourceEvent.id)).where(
        ResourceEvent.consumed_at.is_(None))) or 0)
    queue_pending = int(db.scalar(select(func.count(QueueItem.id)).where(
        QueueItem.status.in_(("pending", "retry")))) or 0)
    link_counts: dict[str, int] = {}
    for st, n in db.execute(select(SCSharePlanVersion.status, func.count(SCSharePlanVersion.id))
                            .group_by(SCSharePlanVersion.status)).all():
        link_counts[st] = int(n or 0)
    return {
        "active_chain": active_chain,
        "switches": {
            "share_center_v2_enabled": v2_on,
            "share_center_legacy_enabled": old_on,
            "publish_paused": _on(runtime_config.get(db, "publish_paused")),
            "resource_decoupled_enabled": _on(runtime_config.get(db, "resource_decoupled_enabled")),
        },
        "rules_in_effect": {
            "quiet_enabled": bool(cycle.get("quiet_enabled")),
            "hour_max": int(cycle.get("hour_max") or 0),
            "day_max": int(cycle.get("day_max") or 0),
            "week_max": int(cycle.get("week_max") or 0),
            "month_max": int(cycle.get("month_max") or 0),
            "category_schedules_enabled": int(db.scalar(select(func.count(SCCategorySchedule.id)).where(
                SCCategorySchedule.enabled.is_(True))) or 0),
            "quick_publish_enabled": _on(runtime_config.get(db, "sc_quick_publish_enabled")),
            "quick_publish_hourly_limit": runtime_config.get(db, "sc_quick_publish_hourly_limit"),
            "update_detect_minutes": runtime_config.get(db, "sc_update_detect_minutes"),
        },
        "segments": {
            "resource_events_pending": pending_events,
            "sc_publish_tasks_by_status": task_status,
            "push_queue_pending": queue_pending,
            "plan_versions_by_status": link_counts,
            "active_version_id": int(version.id) if version else 0,
        },
        "note": "链路判定：v2_on 时 publisher 只驱动 v2 分流层（旧链双推已切断）；"
                "resource_decoupled_enabled 控制资源识别后走事件新链还是旧链入队。",
    }
