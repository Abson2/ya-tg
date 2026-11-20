"""v0.8.3：日志与历史留痕的统一保留策略。

背景：AuditLog / 扫描批次 / 镜像任务 / 版本留痕 / 分发事件 / 推送队列历史
在 v0.8.2 之前只增不减，长期运行会让 SQLite 无限膨胀，管理页列表也越拉越长。

本模块**只清理日志与历史留痕**，绝不触碰业务主数据：
  - Resource / MirrorMedia / MirrorDir / 分享链接 / MirrorFile 的当前映射一律不动
  - 未完成的扫描批次、待轮询的镜像任务、待处理分发事件、
    pending / retry / review 队列项一律保留（删了会漏推送或丢失对账依据）

策略：保留天数（默认 30）+ 每表最大条数（默认 5000）+ 自动清理开关（默认开）。
自动清理在启动时跑一次，之后每天 03:30（Asia/Shanghai）跑一次；
管理页「操作审计」可随时按策略立即清理或一键清空全部可清理日志。
容器 stdout 日志另由 docker-compose 的 json-file max-size/max-file 限额，不进数据库。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import case, func, select, true
from sqlalchemy.orm import Session

from . import runtime_config
from .models import (AuditLog, DistributionEvent, MirrorFile, MirrorTask,
                     QueueItem, ResourceScanBatch, ResourceVersion)

DEFAULT_RETENTION_DAYS = 30
DEFAULT_MAX_ROWS = 5000


@dataclass(frozen=True)
class LogSpec:
    key: str
    label: str
    model: Any
    time_col: Any
    pruneable: Any          # 只有满足该条件的行才允许被删除
    protected: str          # 给管理页看的「什么不会被删」
    note: str


SPECS: tuple[LogSpec, ...] = (
    LogSpec("audit", "操作审计", AuditLog, AuditLog.ts, true(), "无",
            "授权、建分享、推送、审核等全量操作记录"),
    LogSpec("scan_batches", "扫描批次", ResourceScanBatch, ResourceScanBatch.started_at,
            ResourceScanBatch.status != "running", "running 批次",
            "含 seen_json 全量文件 ID，单条体积最大"),
    LogSpec("mirror_tasks", "镜像复制任务", MirrorTask, MirrorTask.created_at,
            MirrorTask.status.notin_(["pending", "running"]), "pending / running 任务",
            "copy_file 任务轮询留痕"),
    LogSpec("resource_versions", "内容版本", ResourceVersion, ResourceVersion.created_at,
            true(), "无", "B 库文件内容变化留痕"),
    LogSpec("distribution_events", "分发事件", DistributionEvent, DistributionEvent.created_at,
            DistributionEvent.queue_id != 0, "queue_id=0 的待处理事件",
            "作品级内容更新事件"),
    LogSpec("queue_history", "推送队列历史", QueueItem, QueueItem.created_at,
            QueueItem.status.notin_(["pending", "retry", "review"]),
            "pending / retry / review", "sent / failed 队列项，未推送的绝不删"),
    LogSpec("mirror_superseded", "镜像历史版本", MirrorFile, MirrorFile.created_at,
            MirrorFile.status == "superseded", "当前生效的文件映射",
            "内容变化后被替代的旧版本记录"),
)


def _int_config(db: Session, key: str, default: int, low: int, high: int) -> int:
    try:
        value = int((runtime_config.get(db, key) or "").strip() or default)
    except (TypeError, ValueError):
        value = default
    return max(low, min(high, value))


def config(db: Session) -> dict:
    """读取生效配置（管理页配置优先、.env/默认值兜底），并夹到安全范围。"""
    raw = (runtime_config.get(db, "log_auto_prune") or "true").strip().lower()
    return {
        "retention_days": _int_config(db, "log_retention_days", DEFAULT_RETENTION_DAYS, 1, 3650),
        "max_rows": _int_config(db, "log_max_rows", DEFAULT_MAX_ROWS, 100, 200000),
        "auto_prune": raw not in ("false", "0", "no", "off"),
        "min_days": 1, "max_days": 3650,
        "min_rows": 100, "max_rows_limit": 200000,
        "schedule": "每天 03:30（Asia/Shanghai），启动时也会执行一次",
    }


def save_config(db: Session, *, retention_days: int, max_rows: int, auto_prune: bool) -> dict:
    cfg = config(db)
    retention_days = max(cfg["min_days"], min(cfg["max_days"], int(retention_days)))
    max_rows = max(cfg["min_rows"], min(cfg["max_rows_limit"], int(max_rows)))
    runtime_config.set_many(db, {
        "log_retention_days": str(retention_days),
        "log_max_rows": str(max_rows),
        "log_auto_prune": "true" if auto_prune else "false",
    })
    return config(db)


def _count(db: Session, spec: LogSpec, *, pruneable_only: bool = False) -> int:
    stmt = select(func.count(spec.model.id))
    if pruneable_only:
        stmt = stmt.where(spec.pruneable)
    return int(db.scalar(stmt) or 0)


def _iso(value) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        # SQLite 读回是 naive UTC；统一加 Z 标记，避免前端误读成本地时间
        return value.isoformat() + "Z"
    return str(value)


def stats(db: Session) -> list[dict]:
    """每类日志的总条数、可清理条数、时间跨度与保护规则。

    阶段三 A-12：每条 spec 用一条聚合 SQL 取 rows/min/max/pruneable，
    由 4×7=28 次查询降到 7 次；返回结构与字段口径完全不变。
    """
    out = []
    for spec in SPECS:
        row = db.execute(select(
            func.count(spec.model.id),
            func.min(spec.time_col),
            func.max(spec.time_col),
            func.sum(case((spec.pruneable, 1), else_=0)),
        )).one()
        rows, oldest, newest, pruneable = row
        out.append({
            "key": spec.key, "label": spec.label, "note": spec.note,
            "protected": spec.protected,
            "rows": int(rows or 0),
            "pruneable": int(pruneable or 0),
            "oldest": _iso(oldest), "newest": _iso(newest),
        })
    return out


def prune(db: Session, *, days: int | None = None, max_rows: int | None = None,
          mode: str = "prune") -> dict:
    """按策略清理。mode=prune 按「天数 + 条数上限」；mode=clear 清空所有可清理行。

    只做删除、不 commit，由调用方决定事务边界（端点与后台任务各自 commit）。
    返回每张表的 before/after/deleted，便于管理页展示与审计。
    """
    cfg = config(db)
    days = max(1, int(days if days is not None else cfg["retention_days"]))
    # 配置层已把下限夹到 100；这里只防非法值，便于测试用小上限验证条数兜底
    max_rows = max(1, int(max_rows if max_rows is not None else cfg["max_rows"]))
    # SQLite 的 DateTime(timezone=True) 存的是 naive UTC，比较参数也必须是 naive UTC
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=days)
    result = {"mode": mode, "days": days, "max_rows": max_rows,
              "cutoff": cutoff.isoformat() + "Z", "tables": [], "deleted": 0}
    for spec in SPECS:
        before = _count(db, spec)
        clauses = [spec.pruneable]
        if mode == "prune":
            clauses.append(spec.time_col < cutoff)
        deleted = db.query(spec.model).filter(*clauses).delete(synchronize_session=False)
        # autoflush=False：必须显式 flush，后续 count 才能看到删除结果
        db.flush()
        if mode == "prune":
            remaining = _count(db, spec)
            if remaining > max_rows:
                excess = remaining - max_rows
                ids = list(db.scalars(
                    select(spec.model.id).where(spec.pruneable)
                    .order_by(spec.time_col.asc(), spec.model.id.asc()).limit(excess)
                ).all())
                if ids:
                    deleted += db.query(spec.model).filter(
                        spec.model.id.in_(ids)).delete(synchronize_session=False)
                    db.flush()
        result["tables"].append({
            "key": spec.key, "label": spec.label,
            "before": before, "after": _count(db, spec), "deleted": int(deleted),
        })
        result["deleted"] += int(deleted)
    return result


def overview(db: Session) -> dict:
    tables = stats(db)
    return {
        "config": config(db),
        "tables": tables,
        "total_rows": sum(t["rows"] for t in tables),
        "total_pruneable": sum(t["pruneable"] for t in tables),
        "container_log": "compose 已限制 stdout 日志为 3 × 10MB，无需手工清理",
    }
