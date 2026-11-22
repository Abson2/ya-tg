"""v2.1.7 #241：阅读量采集任务与趋势报表。

职责分层（与既有模块一致）：
- `tg_stats.py`      —— MTProto 客户端封装（只负责「把当前总数取回来」）；
- 本模块            —— 候选筛选、快照落库、趋势计算、报表数据组装。

三条硬约束：
1. **不编造**：群聊没有浏览量就是没有，如实标记；消息读不到就标记不可采集，
   **绝不填 0 冒充「0 次浏览」**；
2. **不拖累主链**：本模块所有对外入口都吞掉 MTProto 异常，返回结构化结果；
3. **不越界承诺**：报表按 target_kind 分开统计，频道有浏览量、群只有转发/回应，
   任何聚合都不把两者混算。
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from . import runtime_config, tg_stats
from .models import (
    Channel,
    QueueItem,
    Resource,
    TelegramMessageStat,
    TelegramStatRun,
    utcnow,
)

# 采集失败连续达此次数即放弃本轮（避免对不可用目标反复打接口）。
MAX_CONSECUTIVE_FAILURES = 5

# 单轮候选消息上限的默认值（管理页可配）。
DEFAULT_MAX_PER_RUN = 200

# 默认回溯窗口（小时）。
DEFAULT_LOOKBACK_HOURS = 72


class MTProtoStop(RuntimeError):
    """连续失败达上限，停止本轮（内部信号，不对外暴露为异常）。"""



def _sh_now() -> datetime:
    """当前时间（上海时区）。与 utils.as_utc() 口径保持一致地做比较。"""
    return datetime.now(timezone(timedelta(hours=8)))


def _as_utc(dt: datetime | None) -> datetime | None:
    """把可能是 naive 的时间统一成 UTC aware，便于与 utcnow() 比较。"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _int_cfg(db: Session, key: str, default: int) -> int:
    try:
        return int(runtime_config.get(db, key) or default)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# 候选筛选
# ---------------------------------------------------------------------------

def _candidate_rows(db: Session, lookback_hours: int, max_per_run: int) -> list[dict[str, Any]]:
    """挑出本轮要采集的消息。

    条件：队列项已发送（status=sent）、有 telegram_message_id、
    且发送时间在回溯窗口内。按发送时间**从旧到新**排序——
    先采老的（越老越需要补数据），新的留给下一轮。
    """
    cutoff = utcnow() - timedelta(hours=max(1, lookback_hours))
    stmt = (
        select(QueueItem, Channel, Resource)
        .join(Channel, QueueItem.channel_id == Channel.id)
        .join(Resource, QueueItem.resource_id == Resource.id)
        .where(
            QueueItem.status == "sent",
            QueueItem.telegram_message_id != "",
            QueueItem.sent_at.isnot(None),
        )
        .order_by(QueueItem.sent_at.asc())
        .limit(max(1, max_per_run))
    )
    out: list[dict[str, Any]] = []
    for q, channel, res in db.execute(stmt).all():
        sent_at = _as_utc(q.sent_at)
        if sent_at is None or sent_at < cutoff:
            continue
        out.append({
            "queue_id": q.id,
            "chat_id": str(channel.chat_id or ""),
            "target_kind": "channel" if (channel.kind or "channel") == "channel" else "group",
            "target_name": channel.name or "",
            "message_id": str(q.telegram_message_id or ""),
            "work_key": _work_key_for(res),
            "sent_at": sent_at,
        })
    return out


def _work_key_for(res: Resource) -> str:
    """作品的聚合键。与 utils.work_group_key() 同源优先，回退到标题。"""
    try:
        from .utils import work_group_key
        key = work_group_key(res)
        if key:
            return str(key)
    except Exception:
        pass
    return str(getattr(res, "media_title", "") or "")


# ---------------------------------------------------------------------------
# 采集主流程
# ---------------------------------------------------------------------------

async def collect_once(db: Session) -> dict[str, Any]:
    """跑一轮采集：筛选候选 → 取回数据 → 写快照 → 记执行记录。

    永不抛异常：MTProto 未就绪 / 凭证缺失 / 网络故障都转成结构化失败结果。
    """
    lookback = _int_cfg(db, tg_stats.CFG_LOOKBACK_HOURS, DEFAULT_LOOKBACK_HOURS)
    max_per_run = _int_cfg(db, tg_stats.CFG_MAX_PER_RUN, DEFAULT_MAX_PER_RUN)

    run = TelegramStatRun(status="running", lookback_hours=lookback)
    db.add(run)
    db.commit()

    result: dict[str, Any] = {"ok": False, "collected": 0, "skipped": 0, "scanned": 0}
    try:
        if not (runtime_config.get(db, tg_stats.CFG_ENABLED) or "").lower() == "true":
            run.status = "disabled"
            run.error = "采集功能未开启"
            result["reason"] = "disabled"
            _finish_run(db, run)
            return result
        if not tg_stats.telethon_available():
            run.status = "unavailable"
            run.error = "未安装 telethon 依赖"
            result["reason"] = "dependency_missing"
            _finish_run(db, run)
            return result

        candidates = _candidate_rows(db, lookback, max_per_run)
        run.scanned = len(candidates)
        result["scanned"] = len(candidates)
        if not candidates:
            run.status = "success"
            _finish_run(db, run)
            result["ok"] = True
            return result

        client = tg_stats._get_client(db)
        await tg_stats._ensure_authorized(client)

        collected = 0
        skipped = 0
        failures = 0
        # 快照时刻统一取一次——同一轮的快照时间戳必须一致，
        # 否则相邻快照求差时会出现 0 秒间隔的噪声。
        now = utcnow()
        for item in candidates:
            try:
                stat = await tg_stats.collect_message_stat(
                    client, item["chat_id"], item["message_id"])
            except Exception as exc:
                failures += 1
                skipped += 1
                if failures >= MAX_CONSECUTIVE_FAILURES:
                    raise MTProtoStop(f"连续失败 {failures} 次，停止本轮采集：{str(exc)[:200]}")
                continue
            if failures:
                failures = 0
            if not stat.available:
                skipped += 1
            else:
                collected += 1
            db.add(TelegramMessageStat(
                queue_id=item["queue_id"],
                chat_id=item["chat_id"],
                target_kind=stat.target_kind or item["target_kind"],
                target_name=item["target_name"],
                message_id=item["message_id"],
                views=stat.views,
                forwards=stat.forwards,
                reactions=stat.reactions,
                available=stat.available,
                note=stat.note,
                work_key=item["work_key"],
                collected_at=now,
            ))
        db.commit()
        run.status = "success"
        run.collected = collected
        run.skipped = skipped
        result["ok"] = True
        result["collected"] = collected
        result["skipped"] = skipped
        _finish_run(db, run)
        return result
    except MTProtoStop as exc:
        db.commit()
        run.status = "failed"
        run.error = str(exc)[:500]
        result["reason"] = "consecutive_failures"
        result["error"] = str(exc)[:300]
        _finish_run(db, run)
        return result
    except tg_stats.MTProtoUnavailable as exc:
        db.commit()
        run.status = "unavailable"
        run.error = str(exc)[:500]
        result["reason"] = str(exc)[:200]
        _finish_run(db, run)
        return result
    except Exception as exc:
        db.rollback()
        run.status = "failed"
        run.error = str(exc)[:500]
        result["error"] = str(exc)[:300]
        try:
            db.commit()
        except Exception:
            pass
        _finish_run(db, run)
        return result


class MTProtoStop(RuntimeError):
    """连续失败达上限，停止本轮（内部信号，不对外暴露为异常）。"""


class MTProtoStop(RuntimeError):
    """连续失败达上限，停止本轮（内部信号，不对外暴露为异常）。"""


def _finish_run(db: Session, run: TelegramStatRun) -> None:
    run.finished_at = utcnow()
    try:
        db.commit()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# 报表
# ---------------------------------------------------------------------------

def _rows_in_window(db: Session, since: datetime, until: datetime) -> list[TelegramMessageStat]:
    stmt = (
        select(TelegramMessageStat)
        .where(
            TelegramMessageStat.collected_at >= since,
            TelegramMessageStat.collected_at <= until,
        )
        .order_by(TelegramMessageStat.collected_at.asc())
    )
    return list(db.scalars(stmt).all())


def _delta_map(rows: list[TelegramMessageStat]) -> dict[tuple[int, str], dict[str, int]]:
    """由相邻快照求差，得到每条消息的**期间增量**。

    阅读量是累计值，趋势必须由差值得到。首条快照没有前值 → 不计入增量。
    """
    latest: dict[tuple[int, str], TelegramMessageStat] = {}
    prev: dict[tuple[int, str], TelegramMessageStat] = {}
    out: dict[tuple[int, str], dict[str, int]] = {}
    for row in rows:
        key = (row.queue_id, row.message_id)
        if key in latest:
            p = latest[key]
            out[key] = {
                "views": max(0, int(row.views or 0) - int(p.views or 0)),
                "forwards": max(0, int(row.forwards or 0) - int(p.forwards or 0)),
                "reactions": max(0, int(row.reactions or 0) - int(p.reactions or 0)),
            }
        latest[key] = row
        prev[key] = row
    return out


def trend_report(db: Session, days: int = 7) -> dict[str, Any]:
    """阅读量趋势报表。

    **口径分列**：
    - 频道：浏览量 / 转发 / 表情回应 全部有效；
    - 群聊：只有转发 / 表情回应，浏览量不适用——聚合时**单列**，
      绝不把群的 0 浏览量混进频道的平均值里。
    """
    days = max(1, min(90, int(days or 7)))
    until = utcnow()
    since = until - timedelta(days=days)
    rows = _rows_in_window(db, since, until)
    deltas = _delta_map(rows)

    # 按天分桶（上海时区）。
    buckets: dict[str, dict[str, int]] = {}
    for row in rows:
        day = _sh_now().strftime("%Y-%m-%d")
        bucket = buckets.setdefault(day, {
            "views": 0, "forwards": 0, "reactions": 0, "messages": 0})
        bucket["messages"] += 1

    # 增量按快照日归桶。
    row_by_key: dict[tuple[int, str], TelegramMessageStat] = {}
    for row in rows:
        row_by_key[(row.queue_id, row.message_id)] = row
    for key, delta in deltas.items():
        row = row_by_key.get(key)
        if row is None:
            continue
        local = _to_sh(row.collected_at)
        day = local.strftime("%Y-%m-%d")
        bucket = buckets.setdefault(day, {
            "views": 0, "forwards": 0, "reactions": 0, "messages": 0})
        # 只有频道消息的浏览量增量计入浏览量口径。
        if (row.target_kind or "channel") == "channel":
            bucket["views"] += delta["views"]
        bucket["forwards"] += delta["forwards"]
        bucket["reactions"] += delta["reactions"]

    # 目标维度聚合（频道 / 群分列）。
    # key 用 (chat_id, kind) 而不是单独 chat_id —— 同一个 id 既被登记为频道又被
    # 登记为群时（配置串了 / 历史遗留），必须分列统计，**不能合并**，
    # 否则「群没有浏览量」的口径会被「频道有浏览量」污染。
    per_target: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        kind = row.target_kind or "channel"
        key = (row.chat_id or "", kind)
        item = per_target.setdefault(key, {
            "name": row.target_name or "",
            "kind": kind,
            "messages": 0,
            "views_total": 0,
            "forwards_total": 0,
            "reactions_total": 0,
            "views_available": kind == "channel",
        })
        item["messages"] += 1
        item["forwards_total"] = max(item["forwards_total"], int(row.forwards or 0))
        item["reactions_total"] = max(item["reactions_total"], int(row.reactions or 0))
        if item["views_available"]:
            item["views_total"] = max(item["views_total"], int(row.views or 0))
    targets = sorted(per_target.values(),
                     key=lambda x: (-int(x["views_total"]), -int(x["messages"])))

    # 作品维度 Top（按浏览量增量；无浏览量口径的群消息不参与该榜）。
    per_work: dict[str, dict[str, Any]] = {}
    for key, delta in deltas.items():
        row = row_by_key.get(key)
        if row is None:
            continue
        wk = row.work_key or ""
        if not wk:
            continue
        item = per_work.setdefault(wk, {
            "work_key": wk, "views_delta": 0, "forwards_delta": 0,
            "reactions_delta": 0, "messages": 0})
        item["messages"] += 1
        item["forwards_delta"] += delta["forwards"]
        item["reactions_delta"] += delta["reactions"]
        if (row.target_kind or "channel") == "channel":
            item["views_delta"] += delta["views"]
    works = sorted(per_work.values(), key=lambda x: -int(x["views_delta"]))[:20]

    return {
        "days": days,
        "since": since.isoformat(),
        "until": until.isoformat(),
        "buckets": [{"day": k, **v} for k, v in sorted(buckets.items())],
        "targets": targets,
        "works": works,
        "totals": {
            "messages": len(rows),
            "views_delta": sum(int(b["views"]) for b in buckets.values()),
            "forwards_delta": sum(int(b["forwards"]) for b in buckets.values()),
            "reactions_delta": sum(int(b["reactions"]) for b in buckets.values()),
        },
        # 口径说明：前端据此提示「群聊不提供浏览量」。
        "capability": {
            "channel_views": True,
            "group_views": False,
        },
    }


def _to_sh(dt: datetime | None) -> datetime:
    if dt is None:
        return _sh_now()
    aware = _as_utc(dt)
    return aware.astimezone(timezone(timedelta(hours=8)))


def recent_items(db: Session, limit: int = 50) -> list[dict[str, Any]]:
    """最近采集到的消息明细（每条消息取最新一条快照）。"""
    limit = max(1, min(200, int(limit or 50)))
    stmt = (
        select(TelegramMessageStat)
        .order_by(TelegramMessageStat.collected_at.desc())
        .limit(limit)
    )
    rows = list(db.scalars(stmt).all())
    out = []
    for row in rows:
        out.append({
            "target_name": row.target_name or "",
            "kind": row.target_kind or "channel",
            "views": int(row.views or 0),
            "forwards": int(row.forwards or 0),
            "reactions": int(row.reactions or 0),
            "available": bool(row.available),
            "note": row.note or "",
            "work_key": row.work_key or "",
            "collected_at": row.collected_at.isoformat() if row.collected_at else "",
        })
    return out


def run_history(db: Session, limit: int = 10) -> list[dict[str, Any]]:
    """最近几轮采集的执行记录（供管理页排查）。"""
    limit = max(1, min(50, int(limit or 10)))
    stmt = select(TelegramStatRun).order_by(TelegramStatRun.id.desc()).limit(limit)
    out = []
    for run in db.scalars(stmt).all():
        out.append({
            "status": run.status,
            "scanned": run.scanned,
            "collected": run.collected,
            "skipped": run.skipped,
            "lookback_hours": run.lookback_hours,
            "error": (run.error or "")[:300],
            "started_at": run.started_at.isoformat() if run.started_at else "",
            "finished_at": run.finished_at.isoformat() if run.finished_at else "",
        })
    return out


def stats_for_ai(db: Session, days: int = 7) -> dict[str, Any]:
    """给 AI 用的阅读量摘要（**只给事实，不给结论**）。

    供 AI 排期建议使用：哪个分类的作品更受欢迎、什么时段发布效果更好。
    返回的是**实测聚合值**，AI 只负责解读，不负责编造。
    """
    report = trend_report(db, days)
    return {
        "days": report["days"],
        "totals": report["totals"],
        "buckets": report["buckets"],
        "targets": [
            {"name": t["name"], "kind": t["kind"], "messages": t["messages"],
             "views_total": t["views_total"], "forwards_total": t["forwards_total"],
             "reactions_total": t["reactions_total"]}
            for t in report["targets"]
        ],
        "works": report["works"][:10],
        "note": "群聊不提供浏览量统计，群聊目标只有转发与表情回应数据",
    }
