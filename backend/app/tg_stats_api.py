"""v2.1.7 #241：阅读量采集与趋势报表的 HTTP 接口。

对外字段命名口径（红线）：**页面不得出现后端字段名**。
- 不出现 `chat_id` / `message_id` / `work_key` / `queue_id` / `views` 等；
- 一律用中性展示名：`target`（推送目标）/ `title`（作品标题）/ `reads`（阅读量）
  / `shares`（转发）/ `reactions`（表情回应）。
- 内部键（`work_key`）只在**后端聚合**时使用，转成可读标题后才出接口
  （与分享中心 `_work_titles()` 的既有做法一致）。

口径边界同样体现在接口上：`capability.group_views = false` 明确告知前端
「群聊不提供浏览量」，前端据此不显示群消息的阅读量列。
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from . import runtime_config, tg_stats, tg_stats_service
from .db import get_db
from .models import Resource, TelegramMessageStat

router = APIRouter()
logger = logging.getLogger(__name__)


class Payload(BaseModel):
    payload: dict[str, Any] = {}


# ---------------------------------------------------------------------------
# 标题翻译：内部键 → 可读标题
# ---------------------------------------------------------------------------

def _titles_for_work_keys(db: Session, keys: list[str]) -> dict[str, str]:
    """把作品聚合键翻译成可读标题（复用分享中心的既有翻译逻辑）。"""
    keys = [k for k in keys if k]
    if not keys:
        return {}
    try:
        from . import share_center_v2 as sc2
        return sc2._work_titles(db, keys)
    except Exception:
        pass
    # 回退：按标题前缀直接查资源库。
    out: dict[str, str] = {}
    for key in keys:
        name = key
        for prefix in ("work:", "tmdb:"):
            if name.startswith(prefix):
                name = name[len(prefix):]
        out[key] = name or "未知作品"
    return out


# ---------------------------------------------------------------------------
# 状态与配置
# ---------------------------------------------------------------------------

@router.get("/stats/status")
def api_stats_status(db: Session = Depends(get_db)):
    """采集能力状态。**不返回任何明文凭证**，只报「能不能用、为什么不能用」。"""
    st = tg_stats.status(db)
    return {
        "ready": bool(st.get("dependency_ready") and st.get("configured")
                      and st.get("logged_in")),
        "dependency_ready": bool(st.get("dependency_ready")),
        "configured": bool(st.get("configured")),
        "logged_in": bool(st.get("logged_in")),
        "enabled": bool(st.get("enabled")),
        "lookback_hours": int(st.get("lookback_hours") or 72),
        "max_per_run": int(st.get("max_per_run") or 200),
        "reason": st.get("reason", ""),
        # 口径说明（前端据此提示用户）
        "capability": {
            "channel_reads": True,
            "group_reads": False,
            "group_note": "群聊不提供浏览量统计，仅有转发与表情回应",
        },
    }


@router.post("/stats/config")
def api_stats_config(body: Payload, db: Session = Depends(get_db)):
    """保存采集配置（开关 / 回溯窗口 / 单轮上限 / MTProto 凭证）。

    凭证走 runtime_config（Fernet 加密落库）。**明文不返回**，
    只回「是否已配置」。
    """
    data = body.payload or {}
    updates: dict[str, str] = {}
    if "enabled" in data:
        updates[tg_stats.CFG_ENABLED] = "true" if data["enabled"] else "false"
    if "lookback_hours" in data:
        try:
            hours = max(1, min(720, int(data["lookback_hours"])))
        except (TypeError, ValueError):
            hours = 72
        updates[tg_stats.CFG_LOOKBACK_HOURS] = str(hours)
    if "max_per_run" in data:
        try:
            cap = max(1, min(2000, int(data["max_per_run"])))
        except (TypeError, ValueError):
            cap = 200
        updates[tg_stats.CFG_MAX_PER_RUN] = str(cap)
    if "api_id" in data:
        updates[tg_stats.CFG_API_ID] = str(data["api_id"] or "").strip()
    if "api_hash" in data:
        updates[tg_stats.CFG_API_HASH] = str(data["api_hash"] or "").strip()
    if "phone" in data:
        updates[tg_stats.CFG_PHONE] = str(data["phone"] or "").strip()
    if updates:
        runtime_config.set_many(db, updates)
        db.commit()
    st = tg_stats.status(db)
    return {
        "ok": True,
        "configured": bool(st.get("configured")),
        "logged_in": bool(st.get("logged_in")),
        "enabled": bool(st.get("enabled")),
        "lookback_hours": int(st.get("lookback_hours") or 72),
        "max_per_run": int(st.get("max_per_run") or 200),
    }


@router.post("/stats/login/code")
async def api_stats_login_code(body: Payload, db: Session = Depends(get_db)):
    """第一步：获取登录验证码。验证码由 Telegram 直接发给用户，不落库。"""
    try:
        result = await tg_stats.request_login_code(db)
        return {"ok": True, "sent": bool(result.get("sent")),
                "reason": result.get("reason", "")}
    except tg_stats.MTProtoUnavailable as exc:
        return {"ok": False, "error": str(exc)[:300]}


@router.post("/stats/login/verify")
async def api_stats_login_verify(body: Payload, db: Session = Depends(get_db)):
    """第二步：提交验证码完成登录。验证码一次性，不落库。"""
    code = str((body.payload or {}).get("code") or "")
    try:
        await tg_stats.complete_login(db, code)
        return {"ok": True, "logged_in": True}
    except tg_stats.MTProtoUnavailable as exc:
        return {"ok": False, "error": str(exc)[:300]}


@router.post("/stats/logout")
def api_stats_logout(db: Session = Depends(get_db)):
    """退出登录（删除本地会话文件）。"""
    try:
        result = tg_stats.logout(db)
        return {"ok": True, "removed": bool(result.get("removed"))}
    except tg_stats.MTProtoUnavailable as exc:
        return {"ok": False, "error": str(exc)[:300]}


# ---------------------------------------------------------------------------
# 采集与报表
# ---------------------------------------------------------------------------

@router.post("/stats/collect")
async def api_stats_collect(db: Session = Depends(get_db)):
    """手动触发一轮采集。"""
    result = await tg_stats_service.collect_once(db)
    return {
        "ok": bool(result.get("ok")),
        "collected": int(result.get("collected") or 0),
        "skipped": int(result.get("skipped") or 0),
        "scanned": int(result.get("scanned") or 0),
        "reason": str(result.get("reason") or ""),
        "error": str(result.get("error") or ""),
    }


@router.get("/stats/trend")
def api_stats_trend(days: int = 7, db: Session = Depends(get_db)):
    """阅读量趋势报表。"""
    report = tg_stats_service.trend_report(db, days)
    work_keys = [w["work_key"] for w in report.get("works", [])]
    titles = _titles_for_work_keys(db, work_keys)
    works = [{
        "title": titles.get(w["work_key"], "未知作品"),
        "reads_gain": int(w["views_delta"]),
        "shares_gain": int(w["forwards_delta"]),
        "reactions_gain": int(w["reactions_delta"]),
        "posts": int(w["messages"]),
    } for w in report.get("works", [])]
    targets = [{
        "name": t["name"] or "未命名目标",
        "kind": t["kind"],
        "kind_label": "频道" if t["kind"] == "channel" else "群聊",
        "posts": int(t["messages"]),
        "reads_total": int(t["views_total"]) if t["views_available"] else None,
        "shares_total": int(t["forwards_total"]),
        "reactions_total": int(t["reactions_total"]),
        # 群聊没有浏览量 → 前端据此显示「不适用」而非 0。
        # 字段名用中性词（reads_shown），**不暴露**后端的 views_available。
        "reads_shown": bool(t["views_available"]),
    } for t in report.get("targets", [])]
    buckets = [{
        "day": b["day"],
        "reads_gain": int(b["views"]),
        "shares_gain": int(b["forwards"]),
        "reactions_gain": int(b["reactions"]),
        "posts": int(b["messages"]),
    } for b in report.get("buckets", [])]
    totals = report.get("totals", {})
    return {
        "ok": True,
        "days": int(report.get("days") or 7),
        "buckets": buckets,
        "targets": targets,
        "works": works,
        "totals": {
            "posts": int(totals.get("messages") or 0),
            "reads_gain": int(totals.get("views_delta") or 0),
            "shares_gain": int(totals.get("forwards_delta") or 0),
            "reactions_gain": int(totals.get("reactions_delta") or 0),
        },
        "capability": report.get("capability", {}),
    }


@router.get("/stats/items")
def api_stats_items(limit: int = 50, db: Session = Depends(get_db)):
    """最近采集到的消息明细。"""
    items = tg_stats_service.recent_items(db, limit)
    work_keys = [i["work_key"] for i in items if i.get("work_key")]
    titles = _titles_for_work_keys(db, work_keys)
    out = []
    for i in items:
        out.append({
            "target": i["target_name"] or "未命名目标",
            "kind_label": "频道" if i["kind"] == "channel" else "群聊",
            "title": titles.get(i["work_key"], "未知作品"),
            "reads": int(i["views"]) if i["kind"] == "channel" else None,
            "shares": int(i["forwards"]),
            "reactions": int(i["reactions"]),
            "available": bool(i["available"]),
            "note": i["note"],
            "collected_at": i["collected_at"],
        })
    return {"ok": True, "items": out}


@router.get("/stats/runs")
def api_stats_runs(limit: int = 10, db: Session = Depends(get_db)):
    """最近几轮采集的执行记录。"""
    return {"ok": True, "runs": tg_stats_service.run_history(db, limit)}


@router.get("/stats/ai-summary")
def api_stats_ai_summary(days: int = 7, db: Session = Depends(get_db)):
    """给 AI 用的实测摘要（只给事实，不给结论）。"""
    return {"ok": True, "summary": tg_stats_service.stats_for_ai(db, days)}
