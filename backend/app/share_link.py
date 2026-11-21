"""阶段五：分享链接有效性状态机（链接校验与清理，不虚构光鸭接口）。

设计红线（RESOURCE_SHARE_REDESIGN_PLAN.md §阶段五，用户决策1）：
  - 有效性权威 = 本地元数据 + B 库删除事件驱动，**不是光鸭实时状态**。
    抓包取证若发现网页端确有有效性接口再评估接入，未取证前不接入、不虚构。
  - 8 态：unknown/active/suspicious/inactive/retired/replaced/cleaned/manual_review。
  - 失效信号：RESOURCE_REMOVED / MIRROR_DELETED（B 库文件真没了→链接大概率失效）。
    网络异常**不计**失效信号（防误判）——本模块只接收已确认的删除事件，不接网络错误。
  - 失效判定：verify_fail_count >= link_inactive_confirm_count(默认2) 且两次信号间隔
    >= link_inactive_min_minutes(默认30) 才置 inactive。
  - 清理：手动端点触发，只置 link_state=cleaned + current_flag=0（归档），
    **绝不删 B 库资源、不删历史记录、不删 QueueItem/DistributionEvent**。
  - auto_cleanup_enabled 默认 false：不做自动清理，只提供手动端点。

存储：ShareRecord 新列 link_state / verified_result / verify_fail_count /
cleaned_at / clean_state。为不加多余列，"上次失效信号时间"存进 verified_result 的
JSON（{"last_signal_at": iso, "reason": str, ...}）；解析失败按无时间处理。
"""
import json
import logging
from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from . import runtime_config
from .models import ShareRecord
from .utils import as_utc, utcnow

logger = logging.getLogger(__name__)

# 8 态
LINK_UNKNOWN = "unknown"
LINK_ACTIVE = "active"
LINK_SUSPICIOUS = "suspicious"
LINK_INACTIVE = "inactive"
LINK_RETIRED = "retired"
LINK_REPLACED = "replaced"
LINK_CLEANED = "cleaned"
LINK_MANUAL_REVIEW = "manual_review"

ALL_LINK_STATES = frozenset({
    LINK_UNKNOWN, LINK_ACTIVE, LINK_SUSPICIOUS, LINK_INACTIVE,
    LINK_RETIRED, LINK_REPLACED, LINK_CLEANED, LINK_MANUAL_REVIEW,
})

# 不可复用旧链接的状态（can_publish 门禁用）：失效/清理/作废的链接不再复用
NON_REUSABLE_LINK_STATES = frozenset({LINK_INACTIVE, LINK_CLEANED, LINK_RETIRED})


def _int_cfg(db: Session, key: str, default: int) -> int:
    try:
        return max(0, int(runtime_config.get(db, key) or default))
    except (TypeError, ValueError):
        return default


def _load_verified(result: str | None) -> dict:
    try:
        d = json.loads(result or "{}")
        return d if isinstance(d, dict) else {}
    except (TypeError, ValueError):
        return {}


def _dump_verified(d: dict) -> str:
    try:
        return json.dumps(d, ensure_ascii=False)
    except (TypeError, ValueError):
        return ""


def current_link(db: Session, work_key: str) -> ShareRecord | None:
    """当前有效分享记录（link_state 视角；current_flag 仍真相源）。"""
    return db.scalar(select(ShareRecord).where(
        ShareRecord.work_key == work_key, ShareRecord.current_flag == 1).limit(1))


def mark_suspicious(db: Session, work_key: str, reason: str = "") -> dict:
    """收到删除事件 → 当前链接置 suspicious，累计失效信号次数并记录时间。

    幂等：同一 work_key 的当前记录若已是 suspicious/inactive/cleaned/retired，
    不重复累加（事件重放安全）。返回 {"changed": bool, "state": str, "fail_count": int}。
    """
    rec = current_link(db, work_key)
    if not rec:
        return {"changed": False, "state": "", "fail_count": 0}
    state = str(rec.link_state or LINK_ACTIVE)
    if state in (LINK_INACTIVE, LINK_CLEANED, LINK_RETIRED, LINK_REPLACED):
        return {"changed": False, "state": state, "fail_count": int(rec.verify_fail_count or 0)}
    now = utcnow()
    info = _load_verified(rec.verified_result)
    info["last_signal_at"] = now.isoformat()
    if reason:
        info["reason"] = str(reason)[:300]
    info["source"] = "local_event"  # 明确标注：本地事件推断，非光鸭实时校验
    rec.verify_fail_count = int(rec.verify_fail_count or 0) + 1
    rec.link_state = LINK_SUSPICIOUS
    rec.last_verified_at = now
    rec.verified_result = _dump_verified(info)
    db.flush()
    return {"changed": True, "state": LINK_SUSPICIOUS, "fail_count": int(rec.verify_fail_count)}


def confirm_inactive_if_due(db: Session, work_key: str) -> dict:
    """二次确认：fail_count >= 阈值 且距上次信号 >= 间隔 → inactive。

    网络异常不进入本函数（调用方只传删除事件）。返回是否转为 inactive。
    """
    rec = current_link(db, work_key)
    if not rec or str(rec.link_state or LINK_ACTIVE) != LINK_SUSPICIOUS:
        return {"changed": False, "state": str(rec.link_state or "") if rec else ""}
    need = _int_cfg(db, "link_inactive_confirm_count", 2)
    gap_min = _int_cfg(db, "link_inactive_min_minutes", 30)
    if int(rec.verify_fail_count or 0) < max(1, need):
        return {"changed": False, "state": LINK_SUSPICIOUS}
    info = _load_verified(rec.verified_result)
    last_raw = info.get("last_signal_at") or ""
    try:
        # last_signal_at 存的是 ISO 字符串（verified_result JSON），先 fromisoformat
        # 再走 as_utc 统一时区；直接把 str 传给 as_utc 会 AttributeError。
        last = as_utc(datetime.fromisoformat(last_raw)) if last_raw else None
    except Exception:
        last = None
    if not last:
        return {"changed": False, "state": LINK_SUSPICIOUS}
    if utcnow() - last < timedelta(minutes=max(0, gap_min)):
        return {"changed": False, "state": LINK_SUSPICIOUS}
    rec.link_state = LINK_INACTIVE
    info["inactive_at"] = utcnow().isoformat()
    rec.verified_result = _dump_verified(info)
    db.flush()
    return {"changed": True, "state": LINK_INACTIVE}


def mark_manual_review(db: Session, work_key: str, note: str = "") -> dict:
    """人工核验入队 → manual_review（等待人工判定，不自动改 inactive）。"""
    rec = current_link(db, work_key)
    if not rec:
        return {"changed": False}
    rec.link_state = LINK_MANUAL_REVIEW
    info = _load_verified(rec.verified_result)
    if note:
        info["manual_note"] = str(note)[:300]
    rec.verified_result = _dump_verified(info)
    db.flush()
    return {"changed": True, "state": LINK_MANUAL_REVIEW}


def cleanup_link(db: Session, work_key: str, operator: str = "manual") -> dict:
    """手动清理：置 cleaned + current_flag=0（归档）+ cleaned_at。

    只动 ShareRecord 本地记录，**不删 B 库、不删历史行、不碰队列/分发**。
    current_flag=0 后该 work_key 可再建新 current（部分唯一索引已处理）。
    """
    rec = current_link(db, work_key)
    if not rec:
        return {"changed": False, "reason": "no_current"}
    now = utcnow()
    rec.link_state = LINK_CLEANED
    rec.clean_state = f"manual:{operator}"
    rec.cleaned_at = now
    rec.current_flag = 0
    info = _load_verified(rec.verified_result)
    info["cleaned_at"] = now.isoformat()
    rec.verified_result = _dump_verified(info)
    db.flush()
    return {"changed": True, "work_key": work_key, "share_id": rec.share_id}


def link_summary(db: Session, work_key: str) -> dict:
    """只读：给前端/门禁展示某作品当前链接状态。"""
    rec = current_link(db, work_key)
    if not rec:
        return {"state": LINK_UNKNOWN, "fail_count": 0, "reusable": False}
    state = str(rec.link_state or LINK_ACTIVE)
    return {"state": state, "fail_count": int(rec.verify_fail_count or 0),
            "reusable": state not in NON_REUSABLE_LINK_STATES,
            "verified_result": rec.verified_result or "",
            "source": "local_event"}
