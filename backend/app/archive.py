"""v2.1.11：归档（archive）与复活（revive）语义的集中实现。

背景与红线
----------
「B 库被用户手动清空 → 清理现有日志/流程/关联任务/映射 → 等下一次镜像
同步任务自然重建」这条链路（需求1）要求：

  * 清理类操作**只归档不物理删除**——历史行与执行记录必须保留，
    事后要能追溯「当时推过什么、分享过什么」。
  * 归档行**不参与**任何业务查询（不出现在资源库面板、不被再次识别、
    不被再次推送）。
  * 归档行仍留在表里，唯一键（resources.file_id、push_queue 的
    resource_id+channel_id）依然被占用——重建时必须**复用归档行**，
    绝不能按唯一键 INSERT 新行（会撞 `UNIQUE constraint failed` 让整批扫描崩）。

三个概念
--------
archive_*   标记归档（行仍在表里，只是从所有业务查询中隐藏）
revive_*    取消归档标记（同一唯一键重新出现时复用旧行，而不是新建行）
find_*      按唯一键查找，命中归档行时自动复活

为什么复活而不是新建
--------------------
同一唯一键**永远只有一行**。历史靠 clean_state / clean_at / 状态与
时间戳区分，不靠重复行。这与 share_center.ensure_task 里「cancelled 的
first 任务要复活而不是新建」是同一条原则——否则 dedup_key 唯一约束会
挡住重建，作品永远无法重新分享推送。

真实触发路径（不只是理论）
--------------------------
B 库文件被删 → 归档 → 用户从网盘回收站恢复 → **fileId 不变** → 下一轮
扫描按 file_id 找 → 命中归档行 → 必须复活，否则撞唯一约束整批崩溃。
"""
from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import QueueItem, Resource, include_archived, revive_archived, utcnow

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 归档
# ---------------------------------------------------------------------------

def archive_resource(row: Resource, reason: str) -> bool:
    """归档一条资源行。返回是否发生了归档（已归档则跳过，不覆盖首次原因）。"""
    if getattr(row, "clean_state", "") == "cleaned":
        return False
    row.clean_state = "cleaned"
    row.cleaned_at = utcnow()
    row.clean_reason = (reason or "")[:64]
    return True


def archive_queue_item(row: QueueItem, reason: str) -> bool:
    """归档一条推送队列行。返回是否发生了归档。"""
    if getattr(row, "clean_state", "") == "cleaned":
        return False
    row.clean_state = "cleaned"
    row.cleaned_at = utcnow()
    row.clean_reason = (reason or "")[:64]
    return True


# ---------------------------------------------------------------------------
# 复活
# ---------------------------------------------------------------------------

def revive_resource(row: Resource | None) -> bool:
    """取消归档标记。返回是否发生了复活。"""
    return revive_archived(row)


def revive_queue_item(row: QueueItem | None) -> bool:
    """取消归档标记。返回是否发生了复活。"""
    return revive_archived(row)


# ---------------------------------------------------------------------------
# 按唯一键查找（命中归档行自动复活）
# ---------------------------------------------------------------------------

def find_resource_by_file_id(db: Session, file_id: str) -> Resource | None:
    """按 file_id 找资源行；只找到归档行时**复活并返回**，绝不返回 None 让上层 INSERT。

    这是资源库重建的关键入口：file_id 是唯一键，归档行仍占着它。
    返回 None 意味着这个 file_id 从未出现过，可以安全新建。
    """
    if not file_id:
        return None
    row = db.scalar(select(Resource).where(Resource.file_id == file_id).limit(1))
    if row is not None:
        return row
    with include_archived():
        row = db.scalar(select(Resource).where(Resource.file_id == file_id).limit(1))
    if row is not None and revive_resource(row):
        logger.info("[archive] 复用已归档资源行并复活：%s", file_id)
    return row


def find_queue_item(db: Session, resource_id: int,
                    channel_id: int) -> QueueItem | None:
    """按 (resource_id, channel_id) 找队列行；只找到归档行时**复活并返回**。

    push_queue 的唯一约束是 (resource_id, channel_id)。资源行被复活后
    id 不变，旧的归档队列行仍占着这个组合——直接 INSERT 会撞约束。
    """
    row = db.scalar(select(QueueItem).where(
        QueueItem.resource_id == resource_id,
        QueueItem.channel_id == channel_id).limit(1))
    if row is not None:
        return row
    with include_archived():
        row = db.scalar(select(QueueItem).where(
            QueueItem.resource_id == resource_id,
            QueueItem.channel_id == channel_id).limit(1))
    if row is not None and revive_queue_item(row):
        logger.info("[archive] 复用已归档队列行并复活：resource=%s channel=%s",
                    resource_id, channel_id)
    return row
