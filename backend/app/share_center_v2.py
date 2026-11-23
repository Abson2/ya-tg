"""分享中心 v2：全新数据体系的发布计划、清单、调度与节奏服务。

设计基线：SHARE_CENTER_REDESIGN_PHASE2_DESIGN.md。
红线：不读旧 ShareTask/ShareRecord；不迁移旧数据；AI 不直接推送；
频道/群发送间隔按目标类型可调且不得低于 Telegram 官方风控下限。
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import threading
import time
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
from .push_gate import evaluate_push_gate
from .models import (
    AuditLog,
    Channel,
    DiscoveredChat,
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

logger = logging.getLogger(__name__)

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

# v2.1.3 推送门禁：门禁未过的任务不建分享、不入队，等条件齐了再排期。
# 重试间隔取 60s——改名 / 补齐后最长等一分钟就能自动放行，不会「永远卡住」。
GATE_RETRY_SECONDS = 60

# ---------------------------------------------------------------------------
# v2.1.3 真机死锁根治（2026-11-22 黑豹2 事故后端修复）
#
# 事故现象：_job_share_center（5s 周期）与 process_queue（60s 周期）持续 24 小时
# 报 "maximum number of running instances reached (1)"，队列停滞但容器 healthy，
# 同时英文名解析链路整体停摆（07:23 后 56 部作品集体无英文名）。
#
# 根因链（源码逐环验证）：
#   1) 分享中心周期有**两个驱动入口**——main._job_share_center 与
#      publisher._process_queue_impl——两者都在独立线程里 asyncio.run 同一轮
#      run_cycle。无单飞锁时它们并发写同一批任务（**重复 dispatch**）并互抢
#      SQLite 写锁。
#   2) run_cycle 的前三步是同步写库（commit），第四步 dispatch 里有**网络 await**。
#      两个入口并发时，一个线程在等写锁、另一个线程在等网络，写锁持有时间被放大。
#   3) 单轮 dispatch 原本**没有任何时间预算**：一个建分享最坏 3 次重试 ×
#      (20s 超时 + 退避) ≈ 63s，limit=20 → 单轮最长 21 分钟。而 _job_share_center
#      周期只有 5 秒且 max_instances=1 → **上一轮永不结束**，之后每 5 秒都打
#      "maximum number of running instances reached"。这就是那条刷了 24 小时的日志。
#   4) publisher._process_queue_impl 在**主协程线程**里做同步 db.commit()，撞上
#      分享中心周期线程持有的写锁 → busy_timeout 30 秒**冻结整个 event loop**
#      → APScheduler 所有 job 一起饿死 → 定时镜像 / 后台改名复查全部不执行
#      → 新作品没有英文名、改名集体停摆（与 A-07 注释描述的是同一类故障）。
#
# 三道防线，缺一不可：
#   A. 进程级单飞锁：同一时刻只允许一轮 run_cycle，第二个入口直接跳过。
#   B. 单轮时间预算：建分享网络硬超时 + 整轮墙钟预算，单轮时长有界。
#   C. building 僵尸回收：建分享阶段卡死会让任务永远停在 building——dispatch 的
#      候选集不含 building，等于永久漏推（真机「推送停滞但容器 healthy」）。
# ---------------------------------------------------------------------------

# A. 进程级单飞锁。跨线程使用（两个入口各自在线程里跑），故用 threading.Lock
# 而非 asyncio.Lock——asyncio.Lock 绑定具体事件循环，跨循环会抛 RuntimeError。
_CYCLE_LOCK = threading.Lock()

# B. 单个建分享的网络硬超时（含 guangya.post 内部 3 次重试与退避）。
SHARE_BUILD_TIMEOUT_SECONDS = 45

# B. 单轮 dispatch 的墙钟预算：超预算就把剩余任务留到下一轮（它们仍是
# planned/ready，下一轮自然被捞起），绝不让一轮跑到十几分钟。
DISPATCH_ROUND_BUDGET_SECONDS = 60

# C. building 僵尸回收阈值（秒）。建分享阶段进程被杀 / 网络半开会让任务永远
# 停在 building，而 dispatch 候选集只含 planned/ready/failed → 永久漏推。
BUILDING_STALE_SECONDS = 600

# v2.1.2：归档态（清理后置位）。**不物理删除行**——任务与分享记录是推送历史与
# 链接生命周期的真相源，删除会连带毁掉执行记录与统计口径。归档后：
#   任务   → status='cleaned'
#   分享   → link_state='cleaned'
# 默认列表均排除归档态（可用 include_archived=true 查回），保证「清理」=
# 「从管理视图里移除」，而不是「抹掉历史」。
CLEANED_TASK_STATUS = "cleaned"
CLEANED_LINK_STATE = "cleaned"
# 永久性失败文案特征（真实来自 _RepResolver.resolve 与 dispatch 的错误分支）：
# 这些失败重试也不会成功——资源没了就是没了，属于「无效任务/无效链接」。
PERMANENT_ERROR_MARKERS = (
    "作品资源已从资源库移除",
    "资源版本快照缺少资源 id",
    "代表资源不存在",
    "清单项或资源版本不存在",
)

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


def _category_display_name(category_key: str) -> str:
    """把分类键翻译成**界面上可读的中文名**（不暴露后端 key 形态）。

    - 命中 TMDB 官方 genre → 官方中文名（如 `genre:动作` → 「动作」）；
    - `genre:unknown` → 「未分类」（资源未识别出 TMDB 类型时的兜底桶）；
    - 其它不在官方字典里的键 → 「未分类」，**不回显 key 本身**。
    """
    key = str(category_key or "").strip()
    if not key:
        return "未分类"
    for cat in _tmdb_genre_catalog():
        if cat["category_key"] == key:
            return str(cat["name"])
    if key == "genre:unknown":
        return "未分类"
    # 归一化后的中文名仍可能是有效 TMDB 分类（字典按 normalize 生成，此处兜底匹配）
    tail = key.split(":", 1)[-1]
    for cat in _tmdb_genre_catalog():
        if utils.normalize_work_name(str(cat["name"])) == tail:
            return str(cat["name"])
    return "未分类"


def _category_safe_name(category_key: str, names: dict[str, str]) -> str:
    """分类展示名：优先用户自定义名，但**拒绝回显后端 key 形态**。

    字典行的 name 可能仍是历史懒生成留下的 `genre:xxx`（与 category_key 相同），
    这类值直接渲染会把内部 key 暴露到界面，一律替换为可读中文名。
    """
    key = str(category_key or "").strip()
    raw = str(names.get(key) or "").strip()
    if raw and raw != key and not raw.startswith("genre:"):
        return raw
    return _category_display_name(key)


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

    v2.1.9 真机取证（线上库只有**一行** 8-24、段长 16 小时）：
      旧判定要求「段长 == 1 小时」，而线上库是一行 8-24 → pristine=False →
      **永远不迁移**，0-8 点全天封死。铁证：当日 push_sent 只有 3 条，
      时间全是 UTC 00:00-00:01（= 北京 08:00-08:01）——8 点才开工，8 点前零产出。

    v2.1.9 修正：不再用「段长 == 1 小时」判 pristine，改为**按形态判定**——
      ① 所有行都未绑定分类（category_key 空）且配额为 0（用户没手工配过）；
      ② 每一行的 (start_hour, end_hour) 都命中**系统曾分配过的形态**：
         v2.1.1 的 16 段整点模板（段长 1 小时）+ v2.1.2 之后的
         SYSTEM_ASSIGNED_WINDOWS（12 段高峰 / (8,24) / (0,8) / 全天窗口）；
      ③ 现有窗口**没覆盖满 0-24**（已覆盖满则已是目标形态，无需迁移）。
    命中上述即视为「系统旧模板」→ 重建为 12 段 0-24 模板。
    用户手工绑定过分类或改过配额的行保持原样，由页面「恢复默认模板」处理。

    v2.1.9 修正二（v2.1.9 全量门禁抓到的真实回归）：形态判定**不能只认
    SYSTEM_ASSIGNED_WINDOWS**——v2.1.1 的 16 段整点模板每段都是 (h, h+1)，
    不在那个集合里，于是老库模板被误判成「用户手工配过」→ 永不迁移。
    与 v2.1.1 的旧判定保持一致：段长 1 小时同样算系统形态。
    """
    rows = list(db.scalars(select(SCHourWindow).where(
        SCHourWindow.plan_version_id == version_id)).all())
    if rows:
        def _is_system_shape(r) -> bool:
            s, e = int(r.start_hour or 0), int(r.end_hour or 0)
            # v2.1.1 的 16 段整点模板（段长 1 小时）
            if e - s == 1:
                return True
            # v2.1.2 之后的系统形态全集
            return (s, e) in SYSTEM_ASSIGNED_WINDOWS

        pristine = all((not str(r.category_key or "").strip())
                        and int(r.quota or 0) == 0
                        and _is_system_shape(r)
                        for r in rows)
        if not pristine:
            return False
        # 已铺满 0-24 且是目标粒度 → 无需迁移（幂等，避免每次启动重写）。
        covered = set()
        for r in rows:
            s, e = int(r.start_hour or 0), int(r.end_hour or 0)
            if e > s:
                covered.update(range(s, e))
            else:
                covered.update(range(s, 24))
                covered.update(range(0, e))
        if covered >= set(range(24)) and all(
                int(r.end_hour or 0) - int(r.start_hour or 0) == HOUR_SEGMENT_HOURS
                for r in rows):
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


def _migrate_category_windows(db: Session, version_id: int) -> int:
    """按当前偏好分配**重排已存在的分类排期行的时段归属**（v2.1.9）。

    背景（真机 2026-11-22）：线上库的 27 行分类排期是按 v2.1.6 的
    「溢出分类只复用夜间段」口径落的——19 个分类被塞进 0-8 点，
    而全局总闸一旦封死 0-8，这 19 个分类（恐怖/科幻/悬疑/犯罪等）
    **配置上彻底死掉、永不出货**；12-16 黄金段只有 2 个分类覆盖。

    修法：溢出池改为全部可用段（见 _assign_category_windows），
    本函数负责把**存量行**的时段刷成新口径。

    与 _ensure_schedules 的分工（关键，别混用）：
      - _ensure_schedules：**补齐 + 回填**语义，会为不存在的分类**新建**行，
        并回填默认上限。仅在用户进「发布规则页」或点「一键恢复默认」时调用。
      - _migrate_category_windows（本作）：**只重排已存在的行**的时段，
        不新建行、不改上限、不动用户手工行。挂在 ensure_defaults 上，
        让存量库自动收敛。

    闸门（与 _ensure_schedules 同口径）：
      - window_auto=False → 用户手工设定过，一律不动；
      - 时段形态不在 SYSTEM_ASSIGNED_WINDOWS 内 → 用户手工时段，一律不动。
    """
    rows = list(db.scalars(select(SCCategorySchedule).where(
        SCCategorySchedule.plan_version_id == version_id)).all())
    if not rows:
        return 0
    windows = _preference_windows(db, _quiet_window(_cycle_plan_of(db, version_id)))
    if not windows:
        # 偏好读不到（分类字典尚未种下）→ 不动，避免把时段刷成 (0,24) 全天窗口。
        return 0
    changed = 0
    for row in rows:
        if not bool(row.window_auto):
            continue  # 用户手工设定过
        if not _is_system_managed_schedule(row):
            continue  # 用户手工时段（形态不在系统窗口集合内）
        win = windows.get(row.category_key)
        if not win:
            continue
        ws, we = win
        if (int(row.window_start or 0) != ws or int(row.window_end or 0) != we):
            row.window_start, row.window_end = ws, we
            row.updated_at = utcnow()
            changed += 1
    if changed:
        db.flush()
    return changed


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
    # v2.1.9：分类排期同步刷新——存量行按新分配口径重排。
    # 此前 _ensure_schedules 只在用户进「发布规则页」或点按钮时才跑，
    # 导致线上库的存量排期**永远不会自动迁移**：
    # 真机上 19 个分类被塞进 0-8 点（全局总闸封死 0-8 时彻底死掉）、
    # 12-16 黄金段只有 2 个分类覆盖。这里挂到 ensure_defaults（每轮调度都跑）
    # 上，升级后自动收敛，无需用户手动点按钮。
    #
    # **必须放在分类字典种子之后**：_category_preference 从 SCCategory 读偏好，
    # 字典还没种下时 windows 为空 → 全部退化成全天窗口 (0,24)，
    # 反而把「每分类 2 小时颗粒度」的口径破坏掉。
    #
    # v2.1.9 修正（v2.1.9 全量门禁抓到的真实副作用）：
    # 这里**不能**直接调 _ensure_schedules —— 那个函数是「补齐 + 回填」的语义，
    # 会为**尚未存在**的分类**新建** 27 行启用排期（window_start/window_end
    # 按偏好分配落成 2 小时段）。后果有两处：
    #   ① 新库/测试库里 plan_engine 的分类过滤立刻按排期窗口筛，
    #      每个分类只在自己那 2 小时里能产出 → 整轮 items 归零；
    #   ② 端点侧再插同一 category_key 时撞 UNIQUE 约束。
    # 排期行「存在与否」是用户侧语义（页面上能逐行编辑），
    # 系统只负责**重排已存在行的时段归属**，不替用户造行。
    try:
        _migrate_category_windows(db, version.id)
    except Exception:
        db.rollback()


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
# v2.1.2：高峰优先的时段顺序（**固定默认**，不随真实数据变化）。
# 用户口径：「观看用户多的倾向分类排在观影高峰时段」「每两个小时对应一个分类，
# 不要只分白天、夜间，太宽泛了」——因此由「昼夜两档」升级为「12 段高峰序列」。
# 顺序依据通用观影作息：20-22 黄金档 → 18-20 晚高峰 → 22-24 深夜档 →
# 12-14 午休档 → 傍晚 → 下午 → 上午 → 早间 → 凌晨依次递减。
# 这 12 段恰好覆盖 0-24 全天且互不重叠。
PEAK_SEGMENT_ORDER: list[tuple[int, int]] = [
    (20, 22), (18, 20), (22, 24), (12, 14),
    (16, 18), (14, 16), (10, 12), (8, 10),
    (0, 2), (2, 4), (4, 6), (6, 8),
]
# v2.1.2：第 13 名及以后保持**全天窗口**（不限时段），保证长尾分类仍有产出，
# 不因「前 12 名独占时段」把其余分类挤到完全推不出去。
FULL_DAY_WINDOW = (0, 24)
# ---------------------------------------------------------------------------
# v2.1.6：分类默认时间表（按分类观影习惯，2 小时一段）
# ---------------------------------------------------------------------------
# 用户口径：「配置时段，按照分类观影习惯排默认时间表，格式举例战争：0-2，
# 犯罪：2-4；战争：4-6，默认两个小时为一个时间段，不要全都是 0-8，
# 热门分类排在白天，冷门分类排在夜间，注意时间段和周期计划若管理配置的
# 静默时间启用避开」。
#
# 落地口径：
#   ① 粒度统一 2 小时一段（全天 12 段），不再出现 0-8 / 8-24 这种大颗粒；
#   ② **热门分类排白天、冷门分类排夜间**——与 v2.1.2 的「高峰优先」相反：
#      用户要的是把冷门分类挪到夜里，腾出白天给热门分类；
#   ③ 静默保护开启时，落在静默区间的段整体跳过并顺延，绝不静默冲突；
#   ④ 段用完（分类数 > 可用段数）后剩余分类保持全天窗口，保证都能产出。
#
# 白天段（热门优先占用）：10-12 / 12-14 / 14-16 / 16-18 / 8-10 / 18-20 / 20-22 / 22-24 / 6-8；
# 夜间段（冷门占用）：00-02 / 02-04 / 04-06。
# 段给哪个分类由偏好排名决定 → 排名靠前先拿白天段，即「热门白天、冷门夜间」。
#
# v2.1.6：白天段补上 (22,24) 黄金深夜档，段池共 12 段 × 2 小时 = **恰好铺满 0-24 全天**。
# 此前段池只有 11 段（8 白天 + 3 夜间 = 22 小时），22:00-24:00 是**空洞**——
# 没有任何分类的排期窗口覆盖这一小时，而它恰恰是晚间黄金档：
# 该小时 plan_engine 的分类过滤会把所有分类都筛掉 → **整个 22-24 点零产出**。
# 补齐后 12 段覆盖 0-24 互不重叠，与旧的 PEAK_SEGMENT_ORDER 段集合完全对齐。
CATEGORY_DAY_SEGMENTS: list[tuple[int, int]] = [
    (10, 12), (12, 14), (14, 16), (16, 18), (8, 10), (18, 20),
    (20, 22), (22, 24), (6, 8),
]
CATEGORY_NIGHT_SEGMENTS: list[tuple[int, int]] = [
    (0, 2), (2, 4), (4, 6),
]
# 分类排期可用的全部 2 小时段（白天优先 → 夜间兜底）。
CATEGORY_WINDOW_SEGMENTS: list[tuple[int, int]] = (
    CATEGORY_DAY_SEGMENTS + CATEGORY_NIGHT_SEGMENTS
)
# v2.1.2：**系统曾经分配过**的窗口形态全集（判定「这行的时段是系统给的，还是用户手改的」）。
# 含 v2.1.1 的昼夜两档（8-24 / 0-8）、v2.1.2 的 12 段高峰与全天窗口，
# 以及 v2.1.6 的热门白天/冷门夜间时段。
# 老库 ALTER 补 window_auto 列时 DEFAULT 1，用户手工改过时段但上限仍 0/0 的行
# 也会拿到 window_auto=1——用这张表做「系统窗口形态」白名单，避免误覆盖用户手工时段。
SYSTEM_ASSIGNED_WINDOWS: frozenset[tuple[int, int]] = frozenset(
    set(PEAK_SEGMENT_ORDER)
    | {FULL_DAY_WINDOW, (8, 24), (0, 8)}
    # v2.1.6：热门白天 / 冷门夜间的新时段形态也属于「系统分配过」，
    # 否则老行会被 _is_system_managed_schedule 判成用户手工行而不再跟随刷新。
    | set(CATEGORY_WINDOW_SEGMENTS)
)


# ---------------------------------------------------------------------------
# v2.1.6：冲突处理策略（三档，真实生效）
# ---------------------------------------------------------------------------
# 用户口径：「冲突处理矛盾，应该改成 a、新上映优先；b、新入库优先；c、存量排期优先」
# 旧值 pause_normal / prefer_new 语义重复，且**后端从未消费**（死配置）——
# 存进 app_config 后对调度毫无影响，用户以为能控优先级其实不能。
#
# v2.1.6 把冲突处理落到**同一批次发布任务的排序**上：三档互斥、语义清晰，
# 且都不改变任何配额上限（上限仍由周期计划管理统一约束）。
#
#   prefer_release —— 新上映优先：release_date 落在「近 N 天上映窗口」的作品先发。
#   prefer_new     —— 新入库优先：刚扫描入库（scanned_at 最新）的作品先发。
#   prefer_backlog —— 存量排期优先：已排期的存量作品先发，新入库的排到后面。
#
# 「新上映」的判定窗口 = 新品上映窗口天数（默认 30 天），与 TMDB release_date 比对。
CONFLICT_POLICIES: tuple[str, ...] = ("prefer_release", "prefer_new", "prefer_backlog")
CONFLICT_POLICY_LABELS: dict[str, str] = {
    "prefer_release": "新上映优先",
    "prefer_new": "新入库优先",
    "prefer_backlog": "存量排期优先",
}
# 兼容旧值：v2.1.6 之前写过 pause_normal / prefer_new，读到时映射到新口径。
# pause_normal（暂停普通排期优先上新）→ 新上映优先；prefer_new（上新优先）→ 新入库优先。
CONFLICT_POLICY_ALIASES: dict[str, str] = {
    "pause_normal": "prefer_release",
    "prefer_new": "prefer_new",
    "prefer_new_entry": "prefer_new",
    "prefer_release_first": "prefer_release",
    "backlog_first": "prefer_backlog",
}
# 新品上映判定窗口（天）：release_date 在最近 N 天内视为「新上映」。
NEW_RELEASE_WINDOW_DAYS = 30


def _normalize_conflict_policy(raw: str) -> str:
    """把配置里的冲突策略值归一化为三档之一（非法值回落默认档）。"""
    value = str(raw or "").strip().lower()
    value = CONFLICT_POLICY_ALIASES.get(value, value)
    return value if value in CONFLICT_POLICIES else "prefer_release"


def _hour_segments() -> list[tuple[int, int]]:
    """全天 2 小时一段的 (起, 止) 列表，共 12 段。"""
    return [(h, h + HOUR_SEGMENT_HOURS) for h in range(0, 24, HOUR_SEGMENT_HOURS)]


def _assign_category_windows(
    prefs: list[dict[str, Any]],
    quiet: tuple[int, int] | None = None,
) -> dict[str, tuple[int, int]]:
    """按偏好排名给分类分配 2 小时粒度的默认时段（热门白天、冷门夜间）。

    v2.1.6：取代 v2.1.2 的「高峰优先」口径。
    - 偏好排名靠前（热门）→ 先拿**白天**段；
    - 排名靠后（冷门）→ 拿**夜间**段；
    - 静默保护开启时，落进静默区间的段**整段跳过**（不分配给任何分类）；
    - **段用完后（分类数 > 可用段数）剩余分类循环复用夜间段**，
      绝不退回全天窗口 0-24。

    与 v2.1.2 的差异是刻意的：v2.1.2 把第 1 名放在 20-22 黄金档（夜里），
    v2.1.6 按用户口径把热门分类挪到白天、冷门挪到夜里。

    v2.1.6 修正一：兜底窗口**不得与静默区间重叠**。
    此前兜底用全天窗口 0-24，静默开启（如 0-7）时它必然覆盖静默区间——
    等于「一键恢复默认」把分类排到永远推不出去的小时。
    修正：兜底改为**静默区间之外的最大连续区间**；若全天都被静默占满，
    退回首个可用段（宁可让多个分类共享一段，也不要排到推不出去的时间）。

    v2.1.6 修正二：**每个分类都必须有 2 小时颗粒度的时段**。
    用户口径「配置时段按分类观影习惯排默认时间表，默认两个小时为一个时间段」
    +「推荐时段只推荐时间段（2 小时颗粒度）」——意味着**排期表里不能出现
    00:00–24:00 这种全天窗口**。此前段用完后直接退化为全天窗口，
    结果是 27 个分类里有 15 个的时段是全天，既不是 2 小时颗粒度，
    又让长尾分类全天与热门分类抢同一批小时。
    修正：段用完后**冷门优先循环复用夜间段**（复用而非独占），
    语义仍是「冷门排夜间」，但每个分类拿到的都是实打实的 2 小时段。

    v2.1.9 真机取证（线上库 27 个分类的实际分配）：
      上述「溢出全进夜间」造成 **19 个分类挤在 0-8 点**（用户报 17 个，
      量级一致），而白天 9 个段各只占 1 个分类。
      致命后果：全局总闸一旦封死 0-8（老库窗口正是 8-24），
      这 19 个分类（恐怖/科幻/悬疑/犯罪等）**配置上彻底死掉、永不出货**；
      同时 12-16 等白天黄金段只有极少数分类能产出。

    v2.1.9 修正三：**溢出分类不再全堆夜间，而是按段序循环复用全部可用段**。
      - 优先保证「每个段至少有一个分类」——段池不空转；
      - 热门分类仍拿白天段（用户口径「热门白天、冷门夜间」不变）；
      - 溢出分类按段顺序轮转分配，白天段同样参与复用，
        这样 0-8 不会堆积 19 个分类，12-16 也不会只有 2 个分类覆盖；
      - 分类数少于段数时，多出来的段自然空着（不硬塞），
        由用户后续新增分类或手工调整。
    """
    if not prefs:
        return {}
    segments = list(CATEGORY_WINDOW_SEGMENTS)
    if quiet:
        segments = [seg for seg in segments
                    if not _window_overlaps_quiet(seg, quiet)]
    if not segments:
        # 全天都被静默占满：退回首个段（此时任何时段都推不出去）
        only = CATEGORY_WINDOW_SEGMENTS[0]
        return {item["category_key"]: only for item in prefs}
    day_set = set(CATEGORY_DAY_SEGMENTS)
    night_set = set(CATEGORY_NIGHT_SEGMENTS)
    day = [seg for seg in segments if seg in day_set]
    night = [seg for seg in segments if seg in night_set]
    # 热门先占白天段，其后占夜间段（夜间段序里 (22,24) 黄金深夜档排最前）。
    primary = day + night
    # v2.1.9 修正三：溢出池改为**全部可用段**（白天段同样参与复用）。
    # 此前溢出池只有夜间 3 段 → 27 个分类里 19 个挤在 0-8 点，
    # 而全局总闸封死 0-8 时这些分类彻底死掉、且 12-16 黄金段产出极稀。
    overflow_pool = list(segments) or primary
    out: dict[str, tuple[int, int]] = {}
    for idx, item in enumerate(prefs):
        key = item["category_key"]
        if idx < len(primary):
            out[key] = primary[idx]
        else:
            out[key] = overflow_pool[(idx - len(primary)) % len(overflow_pool)]
    return out


def _fallback_window(
    quiet: tuple[int, int] | None,
    segments: list[tuple[int, int]],
) -> tuple[int, int]:
    """段用完后的兜底时段——必须**不与静默区间重叠**。"""
    if not quiet:
        return FULL_DAY_WINDOW
    # 静默区间之外的最大连续区间
    hours = [h for h in range(24) if not _window_overlaps_quiet((h, h + 1), quiet)]
    if not hours:
        # 全天都在静默里：退回首个可用段（此时任何时段都推不出去）
        return segments[0] if segments else FULL_DAY_WINDOW
    best_start, best_len = hours[0], 1
    cur_start, cur_len = hours[0], 1
    for prev, cur in zip(hours, hours[1:]):
        if cur == prev + 1:
            cur_len += 1
        else:
            cur_start, cur_len = cur, 1
        if cur_len > best_len:
            best_start, best_len = cur_start, cur_len
    return (best_start, min(24, best_start + best_len))


def _looks_like_title(value: str) -> bool:
    """判断字符串像作品名而不是文件名（带视频扩展名的按文件名处理）。"""
    text = str(value or "").strip()
    if not text:
        return False
    low = text.lower()
    for ext in (".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".ts", ".m2ts",
                ".rmvb", ".webm", ".mpg", ".mpeg", ".srt", ".ass", ".nfo"):
        if low.endswith(ext):
            return False
    return True


def _work_titles(db: Session, work_keys) -> dict[str, str]:
    """批量把作品聚合键翻译成**可读标题**（不暴露内部键形态）。

    聚合键是内部实现（展示、分享、推送三处共用），**不得**直接渲染到界面上。

    解析顺序（越靠前越准确）：
      ① `SCLibraryVersion.metadata_json.title` —— 资源库版本表里登记的真实作品名
         （识别成功时是 TMDB 中文名），按 work_key 精确命中；
      ② `work:<归一化作品名>` 直接剥前缀 —— 归一化会去掉空格与大小写，
         中文名无损，英文名会退化成 `thelastofus`，故仅作兜底；
      ③ `tmdb:<媒体类型>:<tmdb_id>` / `file:<file_id>` 回查资源库取真实作品名
         （media_title → share_dir_name → name 依次回退）。
    """
    out: dict[str, str] = {}
    keys: list[str] = []
    for raw in work_keys or []:
        key = str(raw or "")
        if not key or key in out:
            continue
        out[key] = ""
        keys.append(key)

    # ① 资源库版本表登记的真实标题（同一 work_key 取最新版本）
    if keys:
        rows = db.execute(
            select(SCLibraryVersion.work_key, SCLibraryVersion.version_no,
                   SCLibraryVersion.metadata_json)
            .where(SCLibraryVersion.work_key.in_(keys))
            .order_by(SCLibraryVersion.version_no.desc())).all()
        seen: set[str] = set()
        for wk, _ver, meta_json in rows:
            if wk in seen:
                continue
            seen.add(wk)
            meta = _jload(meta_json, {})
            if not isinstance(meta, dict):
                continue
            cand = str(meta.get("title") or "").strip()
            if not _looks_like_title(cand):
                cand = str(meta.get("share_dir_name") or "").strip()
                if not _looks_like_title(cand):
                    continue
            out[wk] = cand

    # ② work: 前缀直接剥
    tmdb_ids: set[int] = set()
    file_ids: set[str] = set()
    for key in keys:
        if out[key]:
            continue
        if key.startswith("work:"):
            name = key[5:].strip()
            if name:
                out[key] = name
        elif key.startswith("tmdb:"):
            parts = key.split(":")
            if len(parts) >= 3 and parts[2].isdigit():
                tmdb_ids.add(int(parts[2]))
        elif key.startswith("file:"):
            file_ids.add(key[5:])

    # ③ tmdb:/file: 回查资源库
    if tmdb_ids:
        for res in db.scalars(select(Resource).where(
                Resource.tmdb_id.in_(tmdb_ids))).all():
            title = (str(res.media_title or "").strip()
                     or str(res.share_dir_name or "").strip()
                     or str(res.name or "").strip())
            if not title:
                continue
            mt = str(res.media_type or "")
            # 注意：这些键在上面的初始化里已被置成 ""，不能用 setdefault（空串不是缺省），
            # 否则 tmdb: 形态永远解析不出标题。
            for cand in (f"tmdb:{mt}:{res.tmdb_id}", f"tmdb:{res.tmdb_id}"):
                if cand in out and not out[cand]:
                    out[cand] = title
    if file_ids:
        for res in db.scalars(select(Resource).where(
                Resource.file_id.in_(file_ids))).all():
            title = (str(res.media_title or "").strip()
                     or str(res.share_dir_name or "").strip()
                     or str(res.name or "").strip())
            if title:
                key = f"file:{res.file_id}"
                if key in out and not out[key]:
                    out[key] = title

    # 兜底：仍解析不出的键，至少给出「作品」这种中性占位，不暴露键形态
    for key in keys:
        if not out[key]:
            out[key] = "未知作品" if not key.startswith("work:") else (key[5:] or "未知作品")
    return out


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
    db.add(SCCategory(category_key=category_key,
                      name=name or _category_display_name(category_key),
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
        # v2.1.6 真实缺陷修复（泰迪熊2 双推·治触发源）：指纹**不含 utime**。
        #
        # 原指纹 = sha256(file_id:size:utime)。utime 是文件元数据的修改时间，
        # 任何一次重扫/重同步/触碰都可能让它跳动，而**内容完全没变**：
        # 真机证据——泰迪熊2 的文件 utime 1795345465→1795345530（+65 秒），
        # size 完全不变，指纹却变了 → 生成新版本 v3 → 重建 first 任务 → 二次首推。
        #
        # 指纹只应反映**内容身份**：file_id（文件名+大小即内容身份）
        # 与 size。utime 变化在 size 不变时一律视为伪变更，不生成新版本。
        # （真实内容变化必然伴随 file_id 或 size 变化，仍能被正确识别。）
        #
        # 注：老库已有的旧式指纹行与新指纹不同，首次运行会各生成一个新版本——
        # 这是**安全**的：首推幂等（已 sent 跳过）与 dedup 去指纹两道防线
        # 会拦下重复投递，不会因此多发消息。
        fingerprint_src = "|".join(sorted(
            f"{r.file_id}:{r.size or 0}" for r in rows
        ))
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


def _cycle_plan_of(db: Session, version_id: int = 0) -> dict[str, Any]:
    """取指定（或当前活动）版本的周期计划配置。

    v2.1.6：默认时间表必须避开静默保护时段，而静默配置住在
    计划版本的 cycle_plan 里——多处都要读它，故抽成公共函数，
    避免各处重复解析 config_json。
    """
    version = db.get(SCSharePlanVersion, version_id) if version_id else _active_version(db)
    if not version:
        return {}
    cfg = _jload(version.config_json, {})
    return (cfg.get("cycle_plan") or {}) if isinstance(cfg, dict) else {}


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


def _item_still_owns_first(db: Session, item: SCPushListItem) -> bool:
    """这条首推清单项是否仍「占用」该作品在该渠道的首推名额。

    v2.1.14 修的真机死锁：清理无效任务只把 `SCPublishTask` 标成 `cleaned`，
    对应的 `SCPushListItem` 仍是 `planned`；而建任务时的两道 dedup 都**不看任务状态**，
    于是系统永远认为「这个作品的首推已存在」→ 新入库作品再也建不出推送任务。
    真机实测：227 个最新作品快照被这种已清理旧项挡住。
    """
    statuses = list(db.scalars(select(SCPublishTask.status).where(
        SCPublishTask.list_item_id == item.id)).all())
    if not statuses:
        # 清单项没有任何任务：无法判断它是否推过，**保守视为仍占用**，
        # 宁可少建也不能造成重复首推。
        return True
    return any(str(s or "") != CLEANED_TASK_STATUS for s in statuses)


def _pushed_pairs_from_audit(db: Session) -> set[tuple[str, str]]:
    """从历史推送审计还原「(作品, 渠道 chat_id) 已经推送过」集合。

    为什么不能只看 `SCPublishTask.status='published'`：分享中心 v2 自己推送成功会写
    published，但**旧的推送队列**（publisher.py）成功时只写
    `audit_logs(action='push_sent', detail={resource_id, channel_id, message_id})`。
    真机库里有 233 条这类历史推送——只查 published 会漏掉它们，重建首推时就会把
    早已推过的作品再推一遍（正是用户明确担心的重复首推）。

    这里按 `resource_id → work_key`、`channel_id → chat_id` **精确归属**，
    不做「一律当作推过」的粗判：同一作品推到别的渠道不算重复。
    detail 解析不了或缺字段的行跳过（无法归属就不冒充结论），由调用方如实计数。
    """
    details = list(db.scalars(select(AuditLog.detail).where(
        AuditLog.action == "push_sent")).all())
    if not details:
        return set()
    parsed: list[tuple[int, int]] = []
    for raw in details:
        try:
            d = json.loads(raw or "{}")
        except Exception:
            continue
        rid, cid = d.get("resource_id"), d.get("channel_id")
        if rid in (None, "") or cid in (None, ""):
            continue
        try:
            parsed.append((int(rid), int(cid)))
        except (TypeError, ValueError):
            continue
    if not parsed:
        return set()
    res_ids = {rid for rid, _ in parsed}
    ch_ids = {cid for _, cid in parsed}
    res_map = {r.id: utils.work_group_key(r)
               for r in db.scalars(select(Resource).where(Resource.id.in_(res_ids))).all()}
    ch_map = {c.id: str(c.chat_id or "")
              for c in db.scalars(select(Channel).where(Channel.id.in_(ch_ids))).all()}
    return {(res_map[rid], ch_map[cid]) for rid, cid in parsed
            if rid in res_map and cid in ch_map}


def _ever_published(db: Session, *, work_key: str, chat_id: str,
                    pushed_pairs: set[tuple[str, str]] | None = None) -> bool:
    """该作品在该渠道是否曾**真正推送成功**。

    两个来源都要看（缺一不可）：
      ① 分享中心 v2 自己的任务：`SCPublishTask.status == 'published'`；
      ② 旧推送队列的历史：`audit_logs` 里的 `push_sent`（见 `_pushed_pairs_from_audit`）。

    这是「清理后重建首推」的防重复闸：清理只挑 `failed`/`cancelled`，理论上不会
    命中已推成功的；但历史数据里确实存在「任务被判失败、其实消息已送达」的情况，
    所以两道都查，宁可少推也不重复推。
    `pushed_pairs` 可由调用方预先算好传入，避免逐作品重复扫审计表。
    """
    row = db.scalar(
        select(SCPublishTask.id)
        .join(SCPushListItem, SCPushListItem.id == SCPublishTask.list_item_id)
        .where(SCPublishTask.work_key == work_key)
        .where(SCPushListItem.chat_id == chat_id)
        .where(SCPublishTask.status == "published")
        .limit(1))
    if row is not None:
        return True
    if pushed_pairs is None:
        pushed_pairs = _pushed_pairs_from_audit(db)
    return (work_key, chat_id) in pushed_pairs


def first_dedup_plan(db: Session, *, work_key: str, chat_id: str,
                     dedup: str,
                     pushed_pairs: set[tuple[str, str]] | None = None,
                     ) -> tuple[str, SCPushListItem | None]:
    """首推去重决策。返回 `(action, 可复用的旧清单项)`。

      - `"skip"`   ：已有仍有效的首推，或该作品在此渠道曾真正推送成功 → 不建任务；
      - `"revive"` ：旧首推项的任务**全部已清理** → 复活同一行再建任务；
      - `"create"` ：从来没有首推项 → 新建。

    为什么 revive 而不是新插一行：`SCPushListItem.dedup_key` 是**唯一约束**，
    旧行还在，直接按同一 dedup 另插一条会撞唯一键、整轮计划失败。
    复活只更新清单项本身，历史任务行与执行记录一条不动。
    """
    # 本项目 Session 是 autoflush=False：同一轮里刚建/刚改的任务状态必须先落库，
    # 否则下面的状态判定读到的是旧值（与 api_cleanup_tasks 处同一条坑）。
    db.flush()
    candidates = list(db.scalars(
        select(SCPushListItem)
        .where(or_(SCPushListItem.dedup_key == dedup,
                   (SCPushListItem.work_key == work_key)
                   & (SCPushListItem.chat_id == chat_id)
                   & (SCPushListItem.source_kind == "first")))
        .order_by(SCPushListItem.id)).all())
    if any(_item_still_owns_first(db, c) for c in candidates):
        return "skip", None
    if _ever_published(db, work_key=work_key, chat_id=chat_id,
                       pushed_pairs=pushed_pairs):
        return "skip", None
    if candidates:
        # 优先复用「新格式 dedup」那一行，其次才是历史格式行
        reuse = next((c for c in candidates if c.dedup_key == dedup), candidates[0])
        return "revive", reuse
    return "create", None


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
    # v2.1.15：历史推送审计只扫一次，供本轮所有作品复用（避免逐作品重复查表）。
    pushed_pairs = _pushed_pairs_from_audit(db)

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
    # v2.1.4：周维度配置生效——按**当天星期**取该分类配置的那一天时段。
    # 没有周维度配置的行走原来的单一 window_start/window_end 口径（向后兼容）。
    iso_weekday = datetime.now(shanghai_tz()).isoweekday()
    sched_map = {s.category_key: s for s in db.scalars(
        select(SCCategorySchedule).where(SCCategorySchedule.plan_version_id == version.id,
                                         SCCategorySchedule.enabled.is_(True))).all()}
    # v2.1.6：分类级「每小时上限 / 日上限」已从分类排期表移除，
    # 速率统一由**周期计划管理**约束：
    #   - 单分类每小时上限 = cycle.category_hour_max（此前是死配置，后端从未消费）；
    #   - 单分类每日上限 = cycle.category_day_max（周期计划新增项，0=不限）。
    # 分类排期行里残留的 hourly_limit / daily_limit 保留在库中（历史数据），
    # 但**不再参与调度判定**——否则用户在前台看不到、却在后台限流，无法排查。
    cycle_hour_max = max(0, int(cycle.get("category_hour_max") or 0))
    cycle_day_max = max(0, int(cycle.get("category_day_max") or 0))
    def _sched_allows(key: str, per_category_count: int) -> bool:
        s = sched_map.get(key)
        if not s:
            return True  # 未配置排期行的分类保持原轮转行为（兼容存量）
        day_window = _week_window_for(iso_weekday, s.week_windows or "")
        ws, we = day_window if day_window else (int(s.window_start or 0), int(s.window_end or 0))
        if we > ws:
            in_win = ws <= sh_hour < we
        elif we < ws:
            in_win = sh_hour >= ws or sh_hour < we  # 跨午夜
        else:
            in_win = True
        if not in_win:
            return False
        # v2.1.6：限流口径改为周期计划统一约束（分类行上的旧上限不再生效）
        if cycle_hour_max and per_category_count >= cycle_hour_max:
            return False
        if cycle_day_max and _category_today_count(db, key) >= cycle_day_max:
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
                # v2.1.6 真实缺陷修复（泰迪熊2 双推）：dedup **去掉 fingerprint**。
                #
                # 原 dedup = work_key:fingerprint:channel_id，而 fingerprint =
                # sha256(file_id:size:utime)。utime 抖动（文件被重写/重同步，
                # size 完全不变）就会生成新指纹 → 新 dedup → 绕过去重 →
                # 同一作品被重建 first 任务 → 再次首推。
                # 真机证据：泰迪熊2 的文件 utime 1795345465→1795345530（+65 秒），
                # size 完全不变，却生成了新版本 v3 与新指纹 55f429…，
                # 于是 12:25 又排了一批首推任务。
                #
                # 首推语义 = 「这个作品在这个渠道的第一次推送」——
                # 一旦发生就锁定，不该因为任何元数据抖动而重新排一次。
                # 更新推送（新集/新版本）走 source_kind=update 的独立事件，
                # 不依赖这条首推 dedup。
                chat_id = str(channel.chat_id or "")
                dedup = f"{lv.work_key}:{channel.id}:first"[:255]
                # v2.1.14：两道 dedup 合并成一次「看任务状态」的决策。
                # 旧写法是 dedup_key 查一次 + (work_key, chat_id, first) 兜底查一次，
                # 两次都**不看任务状态**——已清理（cleaned）的旧首推项把名额永久占死，
                # 新入库作品因此建不出任何推送任务（真机 227 个快照被挡）。
                action, reuse = first_dedup_plan(
                    db, work_key=lv.work_key, chat_id=chat_id, dedup=dedup,
                    pushed_pairs=pushed_pairs)
                if action == "skip":
                    continue
                if action == "revive" and reuse is not None:
                    # 复活旧行：dedup_key 是唯一约束，另插一条会撞唯一键。
                    # 只更新清单项本身，历史任务行与执行记录全部保留。
                    item = reuse
                    item.list_id = push_list.id
                    item.library_version_id = lv.id
                    item.category_key = category.category_key
                    item.target_type = target_type
                    item.bot_id = channel.bot_id or 0
                    item.status = "planned"
                    db.flush()
                else:
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
        self._members: dict[str, list[Resource]] | None = None

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
        self._members = grouped

    def work_members(self, lv: SCLibraryVersion) -> list[Resource]:
        """取作品的全部成员资源（推送门禁的作品级判定用）。

        优先用快照里的资源 id（那是建快照那一刻的真实成员）；快照失效时退回
        实时分组（与 sync_library_versions 同口径）。拿不到成员时退回代表资源
        单条——门禁仍能判定「至少这一条满足前三项」，不会凭空判失败。
        """
        out: list[Resource] = []
        for rid in _jload(lv.resource_ids_json, []) or []:
            try:
                res = self.db.get(Resource, int(rid))
            except (TypeError, ValueError):
                res = None
            if res is not None:
                out.append(res)
        if out:
            return out
        if self._members is None:
            self._build()
        live = list((self._members or {}).get(str(lv.work_key or "")) or [])
        if live:
            return live
        rep, _ = self.resolve(lv)
        return [rep] if rep is not None else []

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


def _conflict_sort_key(
    db: Session,
    task: SCPublishTask,
    policy: str,
    resolver: "_RepResolver",
    release_cutoff: datetime,
) -> tuple:
    """按冲突处理策略给同一批待发任务排序（v2.1.6）。

    旧实现里 conflict_policy 是**死配置**（只存不读），用户以为能控优先级
    其实完全无效。v2.1.6 让三档策略真正决定**同批次内的先后顺序**：

      prefer_release —— 新上映优先：release_date 在近 N 天上映窗口内的作品排前；
      prefer_new     —— 新入库优先：scanned_at 最新的作品排前；
      prefer_backlog —— 存量排期优先：已排期（scheduled_at 早）的存量作品排前，
                        新入库的排到后面。

    三档都**不改变任何配额上限**——上限仍由周期计划管理统一约束。

    返回值是「越小越先发」的排序键；无法判定时给中性值，保持原有相对顺序。
    """
    base = (-int(task.priority or 0), int(task.id or 0))
    if policy == "prefer_backlog":
        # 存量优先：scheduled_at 早的先发（早 = 已排期更久 = 存量）。
        scheduled = task.scheduled_at or task.created_at
        stamp = scheduled.timestamp() if scheduled else 0.0
        return (stamp,) + base
    lv = None
    try:
        item = db.get(SCPushListItem, task.list_item_id)
    except Exception:
        item = None
    if item is not None:
        try:
            lv = db.get(SCLibraryVersion, item.library_version_id)
        except Exception:
            lv = None
    rep = None
    if lv is not None:
        try:
            rep, _err = resolver.resolve(lv)
        except Exception:
            rep = None
    if rep is None:
        return (1, 0.0) + base
    if policy == "prefer_new":
        # 新入库优先：scanned_at 最新（时间戳大）排前 → 取负值使「越小越先」。
        scanned = rep.scanned_at
        stamp = scanned.timestamp() if scanned else 0.0
        return (-stamp,) + base
    # prefer_release：新上映优先——release_date 落在上映窗口内排前。
    release = str(rep.release_date or "").strip()[:10]
    is_new_release = False
    if release:
        try:
            rel_dt = datetime.fromisoformat(release).replace(tzinfo=shanghai_tz())
            is_new_release = rel_dt >= release_cutoff
        except (TypeError, ValueError):
            is_new_release = False
    return (0 if is_new_release else 1,) + base


async def dispatch_ready_tasks(db: Session, limit: int = 20) -> dict[str, int]:
    """建分享并提交到分发队列；发送核心仍由 publisher 负责。

    v2.1.3 真机死锁根治：单轮 dispatch 必须**时长有界**。
    原来一个建分享最坏 3 次重试 × (20s 超时 + 退避) ≈ 63s，limit=20 → 单轮最长
    21 分钟；而 _job_share_center 周期只有 5 秒且 max_instances=1 → 上一轮永不
    结束，之后每 5 秒都打 "maximum number of running instances reached"。现在：
      - 建分享有网络硬超时（SHARE_BUILD_TIMEOUT_SECONDS）；
      - 整轮有墙钟预算（DISPATCH_ROUND_BUDGET_SECONDS），超预算就把剩余任务
        留到下一轮（它们仍是 planned/ready，下轮自然被捞起）；
      - 开跑前先回收 building 僵尸（建分享阶段卡死 → 永久漏推）。
    """
    if str(runtime_config.get(db, "publish_paused") or "false").lower() in ("1", "true", "yes", "on"):
        return {"built": 0, "submitted": 0, "failed": 0, "paused": 1}
    now = utcnow()
    # v2.1.3：回收 building 僵尸。dispatch 的候选集只含 planned/ready/failed，
    # 一旦任务卡在 building（进程被杀 / 网络半开 / 旧版本崩溃）就**永远不会被
    # 再次捞起**——真机表现就是「推送停滞但容器 healthy」。这里把超过阈值的
    # building 任务放回 planned，让它重新走一轮（分享链接本来就没建成，
    # 放回 planned 是正确归位，不是重复推送）。
    stale_building = now - timedelta(seconds=BUILDING_STALE_SECONDS)
    try:
        revived = db.execute(
            update(SCPublishTask)
            .where(SCPublishTask.status == "building",
                   SCPublishTask.updated_at < stale_building)
            .values(status="planned", error="建分享阶段中断，已重新排期",
                    next_retry_at=now)).rowcount or 0
    except Exception:
        db.rollback()
        revived = 0
    if revived:
        db.commit()
        logger.warning("[share_center_v2] 回收卡在「建链中」的僵尸任务 %s 条", revived)
    tasks = list(db.scalars(
        select(SCPublishTask).where(SCPublishTask.status.in_({"planned", "ready", "failed"}))
        .where(or_(SCPublishTask.scheduled_at.is_(None), SCPublishTask.scheduled_at <= now))
        .where(or_(SCPublishTask.next_retry_at.is_(None), SCPublishTask.next_retry_at <= now))
        .order_by(SCPublishTask.priority.desc(), SCPublishTask.id).limit(limit)
    ).all())
    stats = {"built": 0, "submitted": 0, "failed": 0, "paused": 0, "throttled": 0,
             # v2.1.3：被推送门禁拦下的任务数（不算失败，等条件齐了自动重试）
             "gated": 0,
             # v2.1.3：因单轮预算耗尽而留给下一轮的任务数（不算失败）
             "deferred": 0,
             # v2.1.3：回收的 building 僵尸数
             "revived": int(revived),
             # v2.1.6：因「该资源在此渠道已推送过」而跳过重复投递的任务数
             # （泰迪熊2 双推修复：已 sent 的队列行不得被复活）
             "skipped": 0}
    # v2.1.3：单轮墙钟预算起点。
    round_started = time.monotonic()
    # v2.1.3：代表资源解析器在整轮内复用一次实时索引，避免每条任务全表扫描。
    _rep_resolver = _RepResolver(db)
    # v2.1.6：冲突处理策略真实消费——决定同一批待发任务的先后顺序。
    # 旧实现里这个配置只存不读（死配置），用户以为能控优先级其实无效。
    conflict_policy = _normalize_conflict_policy(
        runtime_config.get(db, "sc_conflict_policy") or "prefer_release")
    if tasks:
        release_cutoff = datetime.now(shanghai_tz()) - timedelta(days=NEW_RELEASE_WINDOW_DAYS)
        try:
            tasks = sorted(
                tasks,
                key=lambda t: _conflict_sort_key(
                    db, t, conflict_policy, _rep_resolver, release_cutoff))
        except Exception as exc:
            # 排序失败不阻断推送：保留数据库原顺序（priority desc, id）。
            logger.warning("[share_center_v2] 冲突处理排序失败，沿用默认顺序：%s", exc)
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
    for task_index, task in enumerate(tasks):
        # v2.1.3：单轮墙钟预算。超预算就把剩余任务留给下一轮（它们仍是
        # planned/ready，下一轮自然被捞起）——绝不让一轮跑到十几分钟，
        # 否则 _job_share_center（5s 周期 + max_instances=1）会永远卡在
        # "上一轮未结束"，持续刷 maximum number of running instances reached。
        if time.monotonic() - round_started > DISPATCH_ROUND_BUDGET_SECONDS:
            stats["deferred"] = len(tasks) - task_index
            break
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
        # v2.1.3 推送门禁（真机 2026-11-22 黑豹2 事故的直接成因）：建分享之前
        # 必须逐项核验「海报 / 资料 / 识别成功 / 目录已整理 / 文件名已英文」。
        # 门禁未过的任务**不建分享、不入队、不算失败**——它只是「还没准备好」，
        # 保持 planned + 下轮重试，等改名 / 补齐后再由计划引擎重新排期。
        # 中文原因写进 task.error（页面只展示中文，不出现任何后端字段名）。
        gate = evaluate_push_gate(db, str(task.work_key or ""), _rep_resolver.work_members(lv))
        if not gate["passed"]:
            task.status = "planned"
            task.error = gate["reason"]
            task.next_retry_at = now + timedelta(seconds=GATE_RETRY_SECONDS)
            db.commit()
            stats["gated"] += 1
            continue
        # item.bot_id 是目标 Bot，不是 Channel.id；用 chat_id 精确匹配目标频道/群。
        # v2.1.6：目标渠道的解析**前移到建分享之前**——下面的幂等检查要用它，
        # 而且「没有可用推送目标」这种问题也不该先白建一个分享链接才发现。
        channel = db.scalar(select(Channel).where(Channel.chat_id == item.chat_id))
        if not channel:
            channel = db.scalar(select(Channel).where(Channel.enabled.is_(True)).limit(1))
        if not channel:
            task.status = "failed"
            task.error = "无可用推送目标"
            db.commit()
            stats["failed"] += 1
            continue
        # v2.1.6 真实缺陷修复（泰迪熊2 双推）：**建分享之前**先做投递幂等检查。
        #
        # 原逻辑把「已 sent 的队列行」的重置放在建分享**之后**的 submit 阶段：
        #   ① 队列行被无条件重置回 pending/attempts=0 → 旧队列「复活」；
        #   ② publisher 每轮从 DB 重建的 sent_keys_by_channel 里不再有这一行
        #      → v0.6.5 的「同作品整剧去重」保险被绕过 → 第二次真实投递。
        # 真机证据：泰迪熊2（resource id=23）在 2026-11-22 11:06 与 12:26 各推一次，
        # 频道+群共 4 条消息，而 push_queue 全表只有 2 行——同一条队列被发了两遍。
        #
        # 现在口径统一为：**已 sent = 已投递过，永不重发**。
        # 检查前移到建分享之前，好处是「已投递过」的作品不会再白建一个
        # 光鸭分享链接（建链接是真实副作用：会在网盘里多出一条分享记录）。
        #
        # 注：更新推送（有新集/新版本要再推）走独立事件与新队列行，
        # 不依赖复活这条已 sent 的首推队列行；手动强制重推走 main 的独立通道。
        existing_q = db.scalar(select(QueueItem).where(
            QueueItem.resource_id == rep.id, QueueItem.channel_id == channel.id))
        if existing_q is not None and str(existing_q.status or "") == "sent":
            task.status = "skipped"
            task.error = "该资源在此渠道已推送过，跳过重复投递"
            db.add(SCPublishExecution(task_id=task.id, stage="submit", status="skipped",
                                      started_at=utcnow(), finished_at=utcnow(),
                                      response_json=_jdump({"reason": "already_sent",
                                                            "queue_id": existing_q.id})))
            db.commit()
            stats["skipped"] += 1
            continue
        task.status = "building"
        task.error = ""
        db.add(SCPublishExecution(task_id=task.id, stage="build", status="running",
                                  started_at=utcnow(), request_json=_jdump({"work_key": task.work_key})))
        db.commit()
        try:
            client = GuangYaShareClient(db)
            # v2.1.3：建分享网络硬超时。guangya.post 内部 3 次重试 + 退避最坏
            # ≈63s；20 个任务串起来 = 单轮 21 分钟 → 5 秒周期的 job 永远
            # "上一轮未结束"（真机刷了 24 小时的 maximum number of running
            # instances reached）。这里给单次建分享封顶 45s，超时按失败处理
            # （任务回到 planned，下轮重试），不再拖住整轮。
            share_id, share_url = await asyncio.wait_for(
                create_share_parsed(
                    client, str(metadata.get("share_dir_id") or rep.file_id),
                    str(metadata.get("title") or task.work_key), 0,
                ),
                timeout=SHARE_BUILD_TIMEOUT_SECONDS,
            )
            task.share_url = share_url
            task.link_state = "active"
            task.status = "ready"
            stats["built"] += 1
        except asyncio.TimeoutError:
            # 超时不算业务失败：链接本来就没建成，放回 planned 等下一轮重试。
            task.status = "planned"
            task.error = "建分享超时，已重新排期"
            task.next_retry_at = now + timedelta(seconds=GATE_RETRY_SECONDS)
            stats["deferred"] += 1
            execution = db.scalar(select(SCPublishExecution).where(
                SCPublishExecution.task_id == task.id,
                SCPublishExecution.stage == "build",
                SCPublishExecution.status == "running").order_by(SCPublishExecution.id.desc()))
            if execution:
                execution.status = "failed"
                execution.error = "建分享超时"
                execution.finished_at = utcnow()
            db.commit()
            continue
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

        # 队列行复用：同一 (resource_id, channel_id) 只有一行（唯一约束
        # uq_resource_channel）。上面已在建分享前拦掉「已 sent」的行，
        # 到这里仍需再查一次——建分享期间可能有并发轮次把同一行投出去了，
        # 这次重查是**并发兜底**，保证已投递的行永远不会被重置回 pending。
        q = db.scalar(select(QueueItem).where(QueueItem.resource_id == rep.id,
                                             QueueItem.channel_id == channel.id))
        if q is None:
            # v2.1.11：归档行也占着 (resource_id, channel_id) 唯一键，
            # 直接 INSERT 会撞约束。B 库清空重整后重建时命中归档行 → 复活复用。
            from .archive import find_queue_item
            q = find_queue_item(db, rep.id, channel.id)
        if not q:
            q = QueueItem(resource_id=rep.id, channel_id=channel.id)
            db.add(q)
        elif str(q.status or "") == "sent":
            # 并发兜底：建分享这段时间里另一轮已把同一行投递出去。
            # 同样按「已 sent = 永不重发」处理，绝不重置回 pending。
            task.status = "skipped"
            task.error = "该资源在此渠道已推送过，跳过重复投递"
            db.add(SCPublishExecution(task_id=task.id, stage="submit", status="skipped",
                                      started_at=utcnow(), finished_at=utcnow(),
                                      response_json=_jdump({"reason": "already_sent",
                                                            "queue_id": q.id})))
            db.commit()
            stats["skipped"] += 1
            continue
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
    """分享中心主周期：sync → plan → dispatch。

    v2.1.3 真机死锁根治：**进程级单飞**。
    本周期有两个驱动入口——main._job_share_center（5 秒一次）与
    publisher._process_queue_impl（队列 worker，每次也驱动一轮）——两者都在
    各自的线程里 asyncio.run 本函数。没有单飞锁时它们会：

      1) **重复 dispatch**：两个线程各自 select 同一批 planned 任务，都去建
         分享、都写 QueueItem —— 同一作品被建两次链接、入队两条；
      2) **互抢 SQLite 写锁**：一个线程持写锁时另一个在 busy_timeout 里等，
         写锁持有时间被放大，publisher 主协程的 db.commit() 撞上就冻结整个
         event loop → APScheduler 所有 job 饿死 → 英文名解析链路整体停摆。

    这里用**非阻塞**获取：拿不到锁就直接跳过本轮（返回 skipped），绝不让
    第二个入口排队等锁——排队等待就是真机"两轮同时跑、互相拖死"的成因。
    锁在 finally 里释放，异常路径也不会漏锁。
    """
    if not is_enabled(db):
        return {"enabled": False}
    if not _CYCLE_LOCK.acquire(blocking=False):
        # 上一轮还没跑完 → 本轮直接跳过（这是正常背压，不是故障）。
        return {"enabled": True, "skipped": True,
                "reason": "上一轮分享中心周期尚未结束，本轮跳过"}
    try:
        ensure_defaults(db)
        result: dict[str, Any] = {
            "enabled": True,
            "library": sync_library_versions(db),
            "update": merge_update_windows(db),
            "plan": plan_engine(db),
        }
        result["dispatch"] = await dispatch_ready_tasks(db, limit=limit)
        return result
    finally:
        _CYCLE_LOCK.release()


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
                      "name": _category_safe_name(str(key or ""), names),
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
        out = [{k: v for k, v in _model_dict(r).items() if k in (
            "id", "work_key", "kind", "status", "link_state", "share_url",
            "error", "attempts", "created_at", "published_at", "updated_at")} for r in rows]
        titles = _work_titles(db, [r.get("work_key") for r in out])
        for row in out:
            row["title"] = titles.get(str(row.get("work_key") or ""), "")
        return out

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
    total = int(db.scalar(select(func.count(SCPushListItem.id)).join(
        SCPublishTask, SCPublishTask.list_item_id == SCPushListItem.id).where(
        SCPublishTask.share_url.isnot(None), SCPublishTask.share_url != "",
        SCPublishTask.link_state != CLEANED_LINK_STATE)) or 0)
    rows = db.execute(
        select(SCPublishTask.link_state, SCPublishTask.status, func.count(SCPublishTask.id))
        .join(SCPushListItem, SCPublishTask.list_item_id == SCPushListItem.id)
        .where(SCPublishTask.share_url.isnot(None), SCPublishTask.share_url != "",
               SCPublishTask.link_state != CLEANED_LINK_STATE)
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


def _assign_peak_windows(prefs: list[dict[str, Any]]) -> dict[str, tuple[int, int]]:
    """按偏好排名把分类分配到**高峰独占时段**（每 2 小时一段，共 12 段）。

    用户口径（v2.1.2）：「举例 12 个分类，均匀地分布到 24 个小时里面，每两个小时
    对应一个分类，观看用户多的倾向分类排在观影高峰时段，不要只分白天、夜间，
    太宽泛了」——因此由「昼夜两档」升级为「12 段高峰序列」。

    规则：
      - 偏好排名第 1 名的分类占用**最高峰时段**（20:00–22:00 黄金档），
        第 2 名次高峰（18:00–20:00），依次映射到 PEAK_SEGMENT_ORDER；
      - 前 12 名**互不重叠**，恰好铺满 0–24 全天 12 段；
      - 第 13 名及以后保持**全天窗口** 0–24（不限时段），
        保证长尾分类仍能产出，不因前 12 名独占时段而完全推不出去。

    高峰顺序是**固定默认**（不随真实数据波动），观影作息是稳定的行业规律，
    真实推送量只决定「谁排前面」，不决定「什么时候是高峰」。
    """
    if not prefs:
        return {}
    out: dict[str, tuple[int, int]] = {}
    for idx, item in enumerate(prefs):
        key = item["category_key"]
        if idx < len(PEAK_SEGMENT_ORDER):
            out[key] = PEAK_SEGMENT_ORDER[idx]
        else:
            out[key] = FULL_DAY_WINDOW
    return out


@router.get("/preference")
def api_preference(db: Session = Depends(get_db)):
    """分类偏好排名（真实派生）+ 按偏好应分配的默认时段。

    前端用于展示「热门分类排白天、冷门分类排夜间」的分配依据。
    v2.1.6：时段分配改用 2 小时粒度（热门白天 / 冷门夜间），
    并在静默保护开启时避开静默区间。
    """
    prefs = _category_preference(db)
    version = _active_version(db)
    cycle = (_jload(version.config_json, {}) if version else {}).get("cycle_plan") or {}
    quiet = _quiet_window(cycle)
    windows = _assign_category_windows(prefs, quiet)
    items = []
    for idx, item in enumerate(prefs):
        ws, we = windows.get(item["category_key"], FULL_DAY_WINDOW)
        data = dict(item)
        data["window_start"] = ws
        data["window_end"] = we
        data["period"] = "peak" if (ws, we) != FULL_DAY_WINDOW else "full_day"
        # 分配序号：第几名的分类占用第几个时段（0 = 全天不限时段）
        data["peak_rank"] = idx + 1 if data["period"] == "peak" else 0
        data["cross_midnight"] = ws > we
        # v2.1.4：中性引用名，供前端做键匹配（页面不出现后端字段名）
        data["ref"] = item["category_key"]
        items.append(data)
    return {"items": items,
            "segments": _hour_segments(),
            "peak_segments": [list(s) for s in CATEGORY_WINDOW_SEGMENTS],
            "full_day_window": list(FULL_DAY_WINDOW),
            "segment_hours": HOUR_SEGMENT_HOURS,
            "day_segments": [list(s) for s in CATEGORY_DAY_SEGMENTS],
            "night_segments": [list(s) for s in CATEGORY_NIGHT_SEGMENTS],
            "quiet_avoided": bool(quiet),
            "source": "分类字典人工优先级（主）+ 该分类已成功推送任务数（次）"}


@router.post("/preference/assign")
def api_assign_preference_windows(body: Payload, db: Session = Depends(get_db)):
    """一键按用户偏好把分类排期重排到高峰独占时段。

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
    version_cfg = (_jload(version.config_json, {}) if version else {}).get("cycle_plan") or {}
    windows = _assign_category_windows(prefs, _quiet_window(version_cfg))
    rows = {r.category_key: r for r in db.scalars(
        select(SCCategorySchedule).where(
            SCCategorySchedule.plan_version_id == version.id)).all()}
    changed = 0
    skipped = 0
    # v2.1.4：新建行的上限档位按**媒体类型**取默认档（电视 5/30、电影 3/20），
    # 此前一律用电影档，导致电视类新行上限偏低。
    cat_media = {c["category_key"]: (c.get("media_type") or "")
                 for c in _tmdb_genre_catalog()}
    for item in prefs:
        ws, we = windows.get(item["category_key"], FULL_DAY_WINDOW)
        row = rows.get(item["category_key"])
        if row is None:
            hl, dl = _default_limits_for(cat_media.get(item["category_key"], ""))
            db.add(SCCategorySchedule(
                plan_version_id=version.id, category_key=item["category_key"],
                window_start=ws, window_end=we,
                hourly_limit=hl, daily_limit=dl,
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
def api_share_records(db: Session = Depends(get_db), limit: int = 200,
                      include_archived: bool = False):
    """分享管理记录列表（v2.1.1）。

    真实来源：sc_push_list_items JOIN sc_publish_tasks，
    每条代表一次「作品 × 目标」的分享记录。附带参考图所需的列：
    share_code（分享码，从 share_url 尾段解析）、share_title（作品标题，真实可读名）、
    last_checked_at（最后校验时间）、last_pushed_at（最近推送时间）。
    没有分享链接的记录不返回——分享管理管的是**已建链**的分享。
    v2.1.2：已归档（清理）的链接默认不返回，可用 include_archived=true 查回。
    v2.1.4：补 TMDB 海报（cover，中性字段名）+ 推送渠道名称（target_summary）。
    """
    q = (select(SCPushListItem, SCPublishTask)
         .join(SCPublishTask, SCPublishTask.list_item_id == SCPushListItem.id)
         .where(SCPublishTask.share_url.isnot(None), SCPublishTask.share_url != ""))
    if not include_archived:
        q = q.where(SCPublishTask.link_state != CLEANED_LINK_STATE)
    rows = db.execute(q.order_by(SCPublishTask.id.desc()).limit(min(limit, 500))).all()
    names = {c.category_key: c.name for c in db.scalars(select(SCCategory)).all()}
    titles = _work_titles(db, [item.work_key for item, _ in rows])
    posters = _work_posters(db, [item.work_key for item, _ in rows])
    channels = _task_channel_info(db, [int(task.id) for _item, task in rows])
    items = []
    for item, task in rows:
        data = _model_dict(task)
        title = titles.get(str(item.work_key or ""), "")
        info = channels.get(int(task.id)) or {}
        channel_names = info.get("channel_names") or []
        group_names = info.get("group_names") or []
        # v2.1.6：结构化渠道条目（名称 + 标识）——页面按
        # 「群:名称（群id或用户名）」「频道:名称（频道id或用户名）」展示。
        channel_targets = info.get("channel_targets") or []
        group_targets = info.get("group_targets") or []
        parts = []
        if channel_targets:
            parts.append("频道：" + "、".join(
                f"{t['label']}（{t['ref']}）" for t in channel_targets))
        if group_targets:
            parts.append("群：" + "、".join(
                f"{t['label']}（{t['ref']}）" for t in group_targets))
        items.append({
            "id": task.id,
            "list_item_id": item.id,
            "work_key": item.work_key,
            "share_title": title,
            # v2.1.4：TMDB 海报相对路径（中性名，不回显 poster_path 字段名）
            "cover": posters.get(str(item.work_key or ""), ""),
            "kind": data.get("kind") or "first",
            "category_key": item.category_key,
            "category_name": _category_safe_name(item.category_key, names),
            "share_url": data.get("share_url") or "",
            "share_code": _share_code_from_url(data.get("share_url") or ""),
            "link_state": data.get("link_state") or "unknown",
            # v2.1.4：建链状态中文标签 + 视觉态（页面不出现 link_state 取值）
            "link_status_label": _link_status_label(data.get("link_state") or "unknown"),
            "link_tone": _link_tone(data.get("link_state") or "unknown"),
            "status": data.get("status") or "",
            "chat_id": item.chat_id,
            "target_type": item.target_type,
            # v2.1.4：推送渠道可读汇总（频道 / 群名称）
            "channel_names": channel_names,
            "group_names": group_names,
            # v2.1.6：结构化渠道条目（名称 + 标识），供页面展示
            # 「群:名称（群id或用户名）」「频道:名称（频道id或用户名）」。
            "channel_targets": channel_targets,
            "group_targets": group_targets,
            "target_summary": "；".join(parts),
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


# v2.1.4：分享链接状态 → 中文标签 / 视觉态。
# 页面不得直出内部枚举码（active / suspicious / cleaned …），
# 这里在后端翻译成中性展示字段，前端只渲染 label 与 tone。
LINK_STATUS_LABELS: dict[str, str] = {
    "active": "有效",
    "unknown": "待校验",
    "suspicious": "疑似失效",
    "inactive": "已失效",
    "cleaned": "已清理",
    "manual_review": "待人工确认",
}
LINK_STATUS_TONES: dict[str, str] = {
    "active": "ok",
    "unknown": "muted",
    "suspicious": "warn",
    "inactive": "bad",
    "cleaned": "muted",
    "manual_review": "warn",
}


def _link_status_label(state: str) -> str:
    """链接状态 → 中文标签（未知态回落「待校验」，不暴露原始枚举）。"""
    return LINK_STATUS_LABELS.get(str(state or "unknown"), "待校验")


def _link_tone(state: str) -> str:
    """链接状态 → 视觉态（ok / muted / warn / bad），供前端上色。"""
    return LINK_STATUS_TONES.get(str(state or "unknown"), "muted")


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


def _task_channel_info(db: Session, task_ids: list[int]) -> dict[int, dict[str, Any]]:
    """按任务 id 汇总**推送渠道**（频道 / 群）信息。

    推送目标落在清单项上（SCPushListItem.chat_id / target_type / bot_id），
    任务通过 list_item_id 关联。渠道名称取 Channel 表（未登记的退化为群/频道 ID）。

    v2.1.6：**渠道类型以 Channel 表登记的 kind 为准**，不再盲信清单项的
    target_type。真机证据：channels 表把 -1003749901817 登记为 kind='channel'
    （名称「订阅」），但推送清单项的 target_type 全写成 'group'——旧实现
    「target_type 优先」会把频道误判成群，页面显示成「群 xxx」。
    判定优先级改为：① Channel.kind（登记过就以登记为准）→ ② 清单项 target_type
    → ③ chat_id 是否以 - 开头（-100… 是频道/超级群，数字正数是用户私聊）。
    """
    out: dict[int, dict[str, Any]] = {}
    if not task_ids:
        return out
    rows = db.execute(
        select(SCPublishTask.id, SCPushListItem.chat_id, SCPushListItem.target_type,
               SCPushListItem.bot_id)
        .join(SCPushListItem, SCPublishTask.list_item_id == SCPushListItem.id)
        .where(SCPublishTask.id.in_(task_ids))
    ).all()
    if not rows:
        return out
    chat_ids = {str(r[1] or "") for r in rows if r[1]}
    names: dict[str, str] = {}
    kinds: dict[str, str] = {}
    usernames: dict[str, str] = {}
    if chat_ids:
        for ch in db.scalars(select(Channel).where(Channel.chat_id.in_(chat_ids))).all():
            cid = str(ch.chat_id or "")
            names[cid] = str(ch.name or "")
            kinds[cid] = str(ch.kind or "channel")
        # v2.1.6：真实群名/频道名 + @用户名（Bot 发现台账）
        try:
            for dc in db.scalars(select(DiscoveredChat).where(
                    DiscoveredChat.chat_id.in_(chat_ids))).all():
                cid = str(dc.chat_id or "")
                title = str(dc.title or "").strip()
                if title:
                    names[cid] = title
                uname = str(dc.username or "").strip()
                if uname:
                    usernames[cid] = uname
        except Exception:
            pass
    for task_id, chat_id, target_type, _bot_id in rows:
        cid = str(chat_id or "")
        info = out.setdefault(int(task_id), {"channels": [], "groups": [],
                                             "channel_names": [], "group_names": [],
                                             "channel_targets": [], "group_targets": []})
        # v2.1.6：登记过的渠道以 Channel.kind 为准（修正 target_type 写错导致的误判）
        kind = kinds.get(cid, "")
        if kind:
            is_group = kind == "group"
        else:
            # 未登记：退回清单项声明的类型；再退回 chat_id 形态判定
            is_group = str(target_type or "") == "group" or (
                not target_type and cid.startswith("-"))
        raw_name = str(names.get(cid) or "").strip()
        # route: 前缀是路由自动补建 Channel 的内部标记，对用户无意义 → 退回 chat_id
        if raw_name.startswith("route:"):
            raw_name = ""
        label = raw_name or cid or "未知目标"
        # v2.1.6：结构化渠道条目（名称 + 标识），供页面展示
        # 「群:名称（群id或用户名）」/「频道:名称（频道id或用户名）」。
        entry = {"label": label, "ref": usernames.get(cid) or cid}
        if is_group:
            if label not in info["group_names"]:
                info["group_names"].append(label)
            if cid not in info["groups"]:
                info["groups"].append(cid)
            if entry not in info["group_targets"]:
                info["group_targets"].append(entry)
        else:
            if label not in info["channel_names"]:
                info["channel_names"].append(label)
            if cid not in info["channels"]:
                info["channels"].append(cid)
            if entry not in info["channel_targets"]:
                info["channel_targets"].append(entry)
    return out


def _work_posters(db: Session, work_keys: list[str]) -> dict[str, str]:
    """按作品聚合键取 TMDB 海报相对路径（供前端拼缩略图）。

    与 _work_titles 同口径（资源库版本 → 实时 Resource 回查），
    只取**第一条非空**海报，取不到返回空串（前端不渲染缩略图，不占位）。
    """
    out: dict[str, str] = {}
    keys: list[str] = []
    for raw in work_keys or []:
        key = str(raw or "")
        if not key or key in out:
            continue
        out[key] = ""
        keys.append(key)
    if not keys:
        return out
    # ① 资源库版本表登记的元数据
    rows = db.execute(
        select(SCLibraryVersion.work_key, SCLibraryVersion.version_no,
               SCLibraryVersion.metadata_json)
        .where(SCLibraryVersion.work_key.in_(keys))
        .order_by(SCLibraryVersion.version_no.desc())).all()
    seen: set[str] = set()
    for wk, _ver, meta_json in rows:
        if wk in seen or out.get(wk):
            continue
        seen.add(wk)
        meta = _jload(meta_json, {})
        if not isinstance(meta, dict):
            continue
        cand = str(meta.get("poster_path") or "").strip()
        if cand:
            out[wk] = cand
    # ② 回查实时资源表
    missing = [k for k, v in out.items() if not v]
    if missing:
        file_ids: list[str] = []
        tmdb_ids: list[str] = []
        for key in missing:
            if key.startswith("tmdb:"):
                tmdb_ids.append(key.split(":", 2)[-1] if key.count(":") >= 2 else "")
            elif key.startswith("file:"):
                file_ids.append(key.split(":", 1)[-1])
        if file_ids:
            for res in db.scalars(select(Resource).where(Resource.file_id.in_(file_ids))).all():
                for key in missing:
                    if key == f"file:{res.file_id}" and res.poster_path:
                        out[key] = str(res.poster_path)
        if tmdb_ids:
            wanted = {t for t in tmdb_ids if t}
            for res in db.scalars(select(Resource).where(
                    Resource.tmdb_id.in_(wanted))).all() if wanted else []:
                if not res.poster_path:
                    continue
                for key in missing:
                    if key.startswith("tmdb:") and key.endswith(f":{res.tmdb_id}"):
                        out[key] = str(res.poster_path)
                        break
    return out


@router.get("/tasks")
def api_tasks(status: str = "", limit: int = 200,
              include_archived: bool = False, db: Session = Depends(get_db)):
    q = select(SCPublishTask)
    if status:
        q = q.where(SCPublishTask.status == status)
    if not include_archived:
        # v2.1.2：已归档（清理）的任务默认不出现在列表里，可用 include_archived=true 查回。
        q = q.where(SCPublishTask.status != CLEANED_TASK_STATUS)
    rows = db.scalars(q.order_by(SCPublishTask.id.desc()).limit(min(limit, 1000))).all()
    items = [_model_dict(r) for r in rows]
    titles = _work_titles(db, [r.get("work_key") for r in items])
    # v2.1.4：补推送渠道与海报缩略图
    channels = _task_channel_info(db, [int(r.id) for r in rows])
    posters = _work_posters(db, [r.get("work_key") for r in items])
    for row in items:
        key = str(row.get("work_key") or "")
        row["title"] = titles.get(key, "")
        row["cover"] = posters.get(key, "")
        info = channels.get(int(row["id"])) or {}
        row["channel_names"] = info.get("channel_names") or []
        row["group_names"] = info.get("group_names") or []
        # v2.1.6：结构化渠道条目（名称 + 标识），页面按
        # 「群:名称（群id或用户名）」「频道:名称（频道id或用户名）」展示。
        row["channel_targets"] = info.get("channel_targets") or []
        row["group_targets"] = info.get("group_targets") or []
        # 汇总展示文案（频道 + 群），便于表格一列看清推送到哪
        parts = []
        if row["channel_targets"]:
            parts.append("频道：" + "、".join(
                f"{t['label']}（{t['ref']}）" for t in row["channel_targets"]))
        if row["group_targets"]:
            parts.append("群：" + "、".join(
                f"{t['label']}（{t['ref']}）" for t in row["group_targets"]))
        row["target_summary"] = "；".join(parts)
        # 中性展示文案：建链状态直接给中文，页面不出现内部枚举名
        row["link_label"] = "已建链" if str(row.get("link_state") or "") == "active" else "待建链"
        # 中性引用名（页面不出现后端字段名）
        row["ref"] = key
    return {"items": items}


@router.get("/tasks/{task_id}")
def api_task_detail(task_id: int, db: Session = Depends(get_db)):
    task = db.get(SCPublishTask, task_id)
    if not task:
        return {"ok": False, "reason": "任务不存在"}
    executions = db.scalars(select(SCPublishExecution).where(
        SCPublishExecution.task_id == task_id).order_by(SCPublishExecution.id)).all()
    item = _model_dict(task)
    titles = _work_titles(db, [item.get("work_key")])
    key = str(item.get("work_key") or "")
    item["title"] = titles.get(key, "")
    # v2.1.4：详情内页同样展示海报与渠道
    item["cover"] = _work_posters(db, [key]).get(key, "")
    info = _task_channel_info(db, [int(task_id)]).get(int(task_id)) or {}
    item["channel_names"] = info.get("channel_names") or []
    item["group_names"] = info.get("group_names") or []
    # v2.1.6：结构化渠道条目（名称 + 标识）
    item["channel_targets"] = info.get("channel_targets") or []
    item["group_targets"] = info.get("group_targets") or []
    parts = []
    if item["channel_targets"]:
        parts.append("频道：" + "、".join(
            f"{t['label']}（{t['ref']}）" for t in item["channel_targets"]))
    if item["group_targets"]:
        parts.append("群：" + "、".join(
            f"{t['label']}（{t['ref']}）" for t in item["group_targets"]))
    item["target_summary"] = "；".join(parts)
    item["link_label"] = "已建链" if str(item.get("link_state") or "") == "active" else "待建链"
    item["ref"] = key
    return {"item": item, "executions": [_model_dict(x) for x in executions]}


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


def _live_work_keys(db: Session) -> set[str]:
    """当前资源库中**真实存在**的作品聚合键集合（实时扫描派生）。

    用于判断一条历史任务/分享链接背后的作品是否已从资源库移除——
    这是「无效」判定的唯一事实依据，不依赖任何人工标记或示例值。
    """
    keys: set[str] = set()
    for res in db.scalars(select(Resource)).all():
        keys.add(utils.work_group_key(res))
    return keys


def _is_permanent_error(error: str) -> bool:
    """错误文案是否表明**重试也不会成功**（资源已不存在一类）。"""
    text = str(error or "")
    return any(marker in text for marker in PERMANENT_ERROR_MARKERS)


@router.post("/tasks/cleanup")
def api_cleanup_tasks(body: Payload, db: Session = Depends(get_db)):
    """清理无效推送任务：归档（status=cleaned），**不物理删除行**。

    「无效」判据（全部实时派生，不编造）：
      ① 任务已被用户取消（cancelled）——调度器不再捡起它；
      ② 任务失败，且失败原因表明**资源已从资源库移除**
         （重试也永远不会成功）；
      ③ 任务失败，且它背后的作品在当前资源库已查无此物。

    归档后默认列表不再展示（`include_archived=true` 可查回），
    历史行与执行记录保留，不影响推送历史与统计口径。
    """
    p = body.payload or {}
    only_ids = [int(x) for x in (p.get("ids") or []) if str(x).isdigit()]
    live = _live_work_keys(db)
    q = select(SCPublishTask).where(
        SCPublishTask.status.in_({"failed", "cancelled"}))
    if only_ids:
        q = q.where(SCPublishTask.id.in_(only_ids))
    rows = db.scalars(q.order_by(SCPublishTask.id.desc()).limit(5000)).all()
    cleaned = 0
    skipped = 0
    details = []
    for task in rows:
        reason = ""
        if str(task.status or "") == "cancelled":
            reason = "已取消"
        elif _is_permanent_error(task.error):
            reason = "资源已从资源库移除"
        elif str(task.work_key or "") not in live:
            reason = "作品已不在资源库"
        if not reason:
            skipped += 1
            continue
        task.status = CLEANED_TASK_STATUS
        task.error = f"已清理：{reason}"
        task.lease_owner = ""
        task.lease_expires_at = None
        task.updated_at = utcnow()
        cleaned += 1
        details.append({"id": task.id, "reason": reason})
    # v2.1.14：清单项状态跟着任务走。此前只把任务标 cleaned、清单项留 planned，
    # 「发布清单」里因此仍挂着一排「待推送」，首推名额也被这些僵尸项占死
    # （见 first_dedup_plan）。只在**该项下所有任务都已清理**时才标 cleaned，
    # 不误伤仍有在途任务的清单项；历史行一律保留，不做物理删除。
    # 注意：本项目 Session 是 autoflush=False，必须先 flush，否则下面的
    # 「还有没有未清理任务」查询读不到上面刚改的 cleaned，清单项永远不会被更新。
    db.flush()
    item_ids = {int(t.list_item_id or 0) for t in rows if int(t.list_item_id or 0) > 0}
    for iid in sorted(item_ids):
        left = db.scalar(select(SCPublishTask.id).where(
            SCPublishTask.list_item_id == iid,
            SCPublishTask.status != CLEANED_TASK_STATUS).limit(1))
        if left:
            continue
        it = db.get(SCPushListItem, iid)
        if it is not None:
            it.status = CLEANED_TASK_STATUS
    db.commit()
    return {"ok": True, "cleaned": cleaned, "skipped": skipped, "items": details}


def reset_plan_cursors(db: Session, version_id: int) -> int:
    """把该计划版本下所有分类轮转游标归零，让 plan_engine 重新扫一遍历史快照。

    v2.1.15：这是「第三道锁」的恢复通道。plan_engine 只取
    `SCLibraryVersion.id > cursor.last_library_version_id`，而游标在去重被卡死的那段
    时间里已经推进到**全部快照之后**——即使去重修好，也永远不再回头看那批快照。
    归零**不删任何数据**，也不放宽去重：已推送过 / 仍在途的作品照样被挡。
    """
    cursors = list(db.scalars(select(SCCategoryCursor).where(
        SCCategoryCursor.plan_version_id == version_id)).all())
    for c in cursors:
        c.last_library_version_id = 0
        c.last_work_key = ""
        c.updated_at = utcnow()
    db.flush()
    return len(cursors)


def preview_first_rebuild(db: Session) -> dict[str, Any]:
    """预演「重建首推」会怎么走：完全不写库，只按裁决计数。

    逐作品 × 逐渠道调用 `first_dedup_plan`（与 plan_engine 同一条裁决），
    因此这里给出的数字与真实执行后的结果同源，不是估算。
    """
    version = _active_version(db)
    if not version:
        return {"ok": False, "reason": "当前没有生效中的计划版本"}
    channels = list(db.scalars(select(Channel).where(Channel.enabled.is_(True))).all())
    if not channels:
        return {"ok": False, "reason": "还没有启用中的推送渠道"}
    pushed_pairs = _pushed_pairs_from_audit(db)
    versions = list(db.scalars(
        select(SCLibraryVersion).order_by(SCLibraryVersion.id)).all())
    out: dict[str, Any] = {"ok": True, "works": len(versions), "channels": len(channels),
                           "would_create": 0, "would_revive": 0,
                           "blocked_active": 0, "blocked_pushed": 0}
    for lv in versions:
        for channel in channels:
            chat_id = str(channel.chat_id or "")
            dedup = f"{lv.work_key}:{channel.id}:first"[:255]
            action, _reuse = first_dedup_plan(
                db, work_key=lv.work_key, chat_id=chat_id, dedup=dedup,
                pushed_pairs=pushed_pairs)
            if action == "create":
                out["would_create"] += 1
            elif action == "revive":
                out["would_revive"] += 1
            else:
                # 再判一次「是已推过还是仍在途」，只用于如实分类展示
                if _ever_published(db, work_key=lv.work_key, chat_id=chat_id,
                                   pushed_pairs=pushed_pairs):
                    out["blocked_pushed"] += 1
                else:
                    out["blocked_active"] += 1
    # 当前时段口径如实回报：未命中时段的分类到点才会建任务（不是缺陷）
    win = _current_hour_window(db, version)
    key = str(getattr(win, "category_key", "") or "")
    out["hour_allowed"] = bool(_current_hour_allowed(db, version))
    out["hour_category"] = key.split(":", 1)[-1] if key else ""
    return out


@router.post("/plan/rebuild-first")
def api_rebuild_first(body: Payload, db: Session = Depends(get_db)):
    """重建首推任务：把被卡住的首推重新排出来（幂等，绝不重复首推）。

    真机死锁是三道锁叠加，本端点处理的是**第三道**（前两道由 v2.1.14 修掉）：
      - 分类游标 `last_library_version_id` 已推进到全部快照之后 → plan_engine 只取
        `id > 游标`，永远不再回头看历史快照，所以「等一等」不会自愈。
    流程：`dry_run=true` 先只预演（不写库）→ 确认后把游标归零 → 立刻跑一轮
    plan_engine。已推送过（含旧队列 push_sent 历史）与仍在途的作品照样被挡，
    历史行一条不删。
    """
    p = body.payload or {}
    dry = bool(p.get("dry_run", True))
    if dry:
        return preview_first_rebuild(db)
    version = _active_version(db)
    if not version:
        return {"ok": False, "reason": "当前没有生效中的计划版本"}
    reset = reset_plan_cursors(db, version.id)
    db.commit()
    plan = plan_engine(db)
    return {"ok": True, "cursors_reset": reset, "plan": plan,
            "note": "游标已归零，本轮只处理当前时段命中的分类；其余分类到各自窗口自动继续。"}


@router.post("/share-records/cleanup")
def api_cleanup_share_records(body: Payload, db: Session = Depends(get_db)):
    """清理无效分享链接：归档（link_state=cleaned），**不物理删除行**。

    「无效」判据（全部实时派生，不编造）：
      ① 链接状态已被本地元数据状态机判为失效
         （suspicious / inactive）；
      ② 作品已从资源库移除——链接指向的目录已不存在，链接必然打不开；
      ③ 任务失败且失败原因表明资源已移除。

    只归档本地记录：**不删 B 库资源、不删远端分享、不删历史行**
    （与 share_link.cleanup_link 的既有语义一致）。
    """
    p = body.payload or {}
    only_ids = [int(x) for x in (p.get("ids") or []) if str(x).isdigit()]
    live = _live_work_keys(db)
    q = (select(SCPushListItem, SCPublishTask)
         .join(SCPublishTask, SCPublishTask.list_item_id == SCPushListItem.id)
         .where(SCPublishTask.share_url.isnot(None), SCPublishTask.share_url != ""))
    if only_ids:
        q = q.where(SCPublishTask.id.in_(only_ids))
    rows = db.execute(q.order_by(SCPublishTask.id.desc()).limit(5000)).all()
    cleaned = 0
    skipped = 0
    details = []
    for item, task in rows:
        state = str(task.link_state or "unknown")
        reason = ""
        if state in ("suspicious", "inactive"):
            reason = "链接已判失效"
        elif _is_permanent_error(task.error):
            reason = "资源已从资源库移除"
        elif str(task.work_key or item.work_key or "") not in live:
            reason = "作品已不在资源库"
        if not reason:
            skipped += 1
            continue
        task.link_state = CLEANED_LINK_STATE
        task.updated_at = utcnow()
        cleaned += 1
        details.append({"id": task.id, "reason": reason})
    db.commit()
    return {"ok": True, "cleaned": cleaned, "skipped": skipped, "items": details}


@router.post("/category-schedules/adopt-ai-all")
def api_adopt_schedule_ai_all(body: Payload, db: Session = Depends(get_db)):
    """批量采纳推荐时段：把建议窗口写入所选分类。

    v2.1.6：
      - 单一时间段（2 小时颗粒度）口径，不再写周维度网格；
      - **每个分类都有建议**（实测优先，无实测用默认时间表兜底），
        不再出现「无建议跳过」导致采纳率极低；
      - 支持传 category_keys 只处理勾选的分类；不传 = 全部。

    静默保护开启时，落在静默区间的建议时段会被跳过（而不是强行写入冲突时段）。
    """
    p = body.payload or {}
    version = _active_version(db)
    if not version:
        return {"ok": False, "reason": "无活动计划版本"}
    # v2.1.2：先补齐排期行（幂等），否则首次点击时表里没行，等于什么都没采纳。
    _ensure_schedules(db, version.id)
    rows = db.scalars(select(SCCategorySchedule).where(
        SCCategorySchedule.plan_version_id == version.id)).all()
    # v2.1.6：只处理勾选的分类（不传 = 全部）。
    wanted = {str(x).strip() for x in (p.get("category_keys") or []) if str(x).strip()}
    if wanted:
        rows = [r for r in rows if r.category_key in wanted]
    quiet = _quiet_window(_cycle_plan_of(db, version.id))
    fallback_map = _preference_windows(db, quiet)
    adopted = 0
    skipped = 0
    for row in rows:
        sug_start, sug_end, sug_source = _category_suggest_window(
            db, row.category_key, fallback_map.get(row.category_key, FULL_DAY_WINDOW))
        # v2.1.6：起点 0 合法，用来源标记判定有没有建议。
        if not sug_source:
            skipped += 1
            continue
        if quiet and _window_overlaps_quiet((sug_start, sug_end), quiet):
            skipped += 1
            continue
        row.window_start, row.window_end = sug_start, sug_end
        row.ai_window_start, row.ai_window_end = sug_start, sug_end
        row.week_windows = ""
        row.ai_adopted = True
        # 采纳是用户手工动作 → 该行不再被系统自动刷新覆盖。
        row.window_auto = False
        row.updated_at = utcnow()
        adopted += 1
    db.commit()
    return {"ok": True, "adopted": adopted, "skipped": skipped, "total": len(rows)}


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

    v2.1.2（用户口径：前 12 名独占高峰 2 小时段，其余保持全天窗口）：
    时段默认由**偏好分配**决定（window 参数传入高峰独占段或全天窗口），
    只有媒体类型决定速率上限。这样「一键应用默认建议」与
    「按偏好分配时段」两套动作口径一致，不会互相覆盖。
    未传 window 时兜底为全天窗口 0-24（不限时段，不掐断产出）。
    """
    hl, dl = _default_limits_for(media_type)
    ws, we = window if window else FULL_DAY_WINDOW
    return ws, we, hl, dl


def _preference_windows(db: Session,
                        quiet: tuple[int, int] | None = None) -> dict[str, tuple[int, int]]:
    """当前偏好排名对应的分类默认时段窗口（分类 key → (起, 止)）。

    v2.1.6：改用「热门白天 / 冷门夜间」的 2 小时粒度分配（见
    _assign_category_windows），不再是 v2.1.2 的「高峰独占段」。

    quiet 传入静默保护区间时，落进静默的段会被整段跳过——默认时间表
    **必须避开静默时段**，否则「一键恢复默认」会把分类排到推不出去的小时。
    """
    return _assign_category_windows(_category_preference(db), quiet)


def _is_untouched_schedule(row: SCCategorySchedule) -> bool:
    """判断一行分类排期是否「从未被用户配置过」。

    v2.1.2：指纹不再写死「时段 8-24」——时段默认值已改为按偏好分配的高峰段
    （前 12 名各占一个 2 小时段），写死 8-24 会漏判 v2.1.1 留下的 0-8 夜间行。

    新指纹 = **上限全 0** + **window_auto 为真** + **时段是系统曾用过的窗口形态**：
      - 用户在页面保存过任何一行，`api_upsert_category_schedule` 会置 window_auto=False
        并带上真实上限，指纹随之失效；
      - 系统托管行（window_auto=True）一旦被回填过默认上限，上限非 0，指纹失效，
        此时只刷新时段归属、不动上限；
      - 老库（v2.0.5 时代）ALTER 补 window_auto 列时 DEFAULT 1，
        用户手工改过时段但上限仍是 0/0 的行也会拿到 window_auto=1——
        加上「时段必须是系统窗口形态」这道闸，避免把用户手工时段覆盖掉。
    用途：只对「未配置过」的行回填默认建议值，不覆盖用户已改过的行。
    """
    if not bool(row.window_auto):
        return False
    if int(row.hourly_limit or 0) != 0 or int(row.daily_limit or 0) != 0:
        return False
    return (int(row.window_start or 0), int(row.window_end or 0)) in SYSTEM_ASSIGNED_WINDOWS


def _is_system_managed_schedule(row: SCCategorySchedule) -> bool:
    """判断一行的时段是否仍**归系统托管**（可被偏好分配刷新）。

    v2.1.2：光看 window_auto 不够——
      - window_auto 列是 v2.1.1 才加的，老库 ALTER 补列时 DEFAULT 1，
        **v2.1.0 时代用户手工改过的行也会拿到 window_auto=1**；
      - 因此追加一道「时段形态」闸：系统分配过的时段必定落在
        SYSTEM_ASSIGNED_WINDOWS（12 段高峰 + 全天 + v2.1.1 昼夜两档）之内，
        而用户手工设的任意时段（如 21:00–23:00）不在其中。

    v2.1.4：原判据是「上限恰为媒体类型默认档」，v2.1.4 改为「时段形态」——
    用户口径「配置时间段默认值应该与偏好档位推荐值一样」要求**时段**始终跟随
    偏好推荐，而**上限是否被用户改过与时段归属是两件事**，不该互相牵连：
    用户把上限从 5/30 改成 3/15，时段仍应是系统托管的、跟随偏好推荐值。
    「时段形态」这道闸精确覆盖了「老库手工行被误刷」的风险，且不牵连上限。
    """
    if not bool(row.window_auto):
        return False
    return (int(row.window_start or 0), int(row.window_end or 0)) in SYSTEM_ASSIGNED_WINDOWS


def _ensure_schedules(db: Session, version_id: int) -> list[SCCategorySchedule]:
    """为版本幂等补齐所有 TMDB 官方分类的排期行，并回填内置默认建议值。

    v2.1.1（用户口径：排期要内置默认建议值，避免流程跑不通）：
      ① 新建行直接写入按媒体类型分档的默认时段与上限，并默认启用——
         之前 enabled=False + 0/0，等于新建后调度完全不受排期约束，
         用户勾选启用才生效但参数仍是「不限」，配置形同虚设。
      ② **存量行回填**：已存在的行若是「从未被用户配置过」，回填默认建议值**并启用**，
         让排期规则开箱即用；用户已改过的行保持原样，不覆盖。

    v2.1.2（用户口径：前 12 名独占高峰 2 小时段，其余保持全天窗口）：
      ③ 时段不再写死 8-24，而是取**偏好分配**给出的高峰独占段，
         新建行一落库就落在正确的时段上；
      ④ 系统托管行（window_auto=True）的时段归属随偏好分配刷新——
         上限已由系统回填的，只改时段不动上限；上限全 0 的（从未配置过）
         连同默认上限一起回填。

    v2.1.6：偏好分配改为「热门白天 / 冷门夜间」的 2 小时粒度，
    且**避开静默保护时段**（静默开启时落进静默的段不分配给任何分类）。
    """
    rows = {r.category_key: r for r in db.scalars(
        select(SCCategorySchedule).where(SCCategorySchedule.plan_version_id == version_id)).all()}
    windows = _preference_windows(db, _quiet_window(_cycle_plan_of(db, version_id)))
    added = False
    changed = False
    for cat in _tmdb_genre_catalog():
        key = cat["category_key"]
        win = windows.get(key, FULL_DAY_WINDOW)
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
        # 从未配置过（上限全 0）：连同默认上限与时段一起回填。
        if _is_untouched_schedule(existing):
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
            continue
        # 系统托管行：只刷新时段归属（偏好档位推荐值），不动用户可见的上限。
        # v2.1.4：闸门从「上限恰为默认档」换成「时段形态仍在系统窗口集合内」——
        # 老库 ALTER 补 window_auto 时 DEFAULT 1，用户手工设的时段（如 21-23）
        # 不在 SYSTEM_ASSIGNED_WINDOWS 内，因此不会被误刷；而系统托管的行
        # 无论上限是否被用户改过，时段都始终跟随偏好档位推荐值。
        if not _is_system_managed_schedule(existing):
            continue
        if (int(existing.window_start or 0) != ws
                or int(existing.window_end or 0) != we):
            existing.window_start, existing.window_end = ws, we
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


# ---------------------------------------------------------------------------
# v2.1.6：分类推荐时段（档位偏好 + AI 建议合并，统一为「时间段」）
# ---------------------------------------------------------------------------
# 用户口径：「档位偏好和 ai 建议时段合并，只推荐时间段（尽量上 2 小时为颗粒度）
# 不用到周维度」「采纳操作有的按钮是灰色的（每个分类都要匹配对应的 ai 建议）」。
#
# v2.1.4 的周维度建议有两处问题：
#   ① 7 天网格信息量过大，用户要的是「一个推荐时间段」；
#   ② 采纳按钮按「是否有实测数据」置灰 → 大部分分类无实测，按钮永远点不了。
#
# v2.1.6 改为**单一时间段**（2 小时颗粒度），且**每个分类都必有建议**：
#   ① 真实优先——该分类已发布任务的小时分布，取最密集的连续 2 小时段；
#   ② 观影习惯兜底——无实测时用该分类的**默认时间表时段**
#      （热门白天 / 冷门夜间，已避开静默时段）。
#      这是「通用作息 + 偏好排名」派生，不是编造的历史数据，来源标注为「习惯基线」。
#
# 返回 (起, 止, 来源)：来源 "history"=实测 / "habit"=习惯基线 / ""=无建议。
# **起点 0 是合法值**（夜间段 00:00–02:00），不能用数值真伪判断有没有建议——
# v2.1.6 之前 `if start:` 会把这类建议当成「无建议」丢掉，
# 前端 `v-if="s.row.suggest_start"` 同样为假 → 采纳按钮置灰（用户实测反馈）。
def _category_suggest_window(
    db: Session,
    category_key: str,
    fallback: tuple[int, int] | None = None,
) -> tuple[int, int, str]:
    """该分类的推荐时间段（2 小时颗粒度）+ 来源标记。"""
    start, end = _ai_suggest_window(db, category_key)
    if end > start:
        return (start, end, "history")
    if fallback and fallback[0] != fallback[1]:
        return (int(fallback[0]), int(fallback[1]), "habit")
    return (0, 0, "")


# ---------------------------------------------------------------------------
# v2.1.4：周维度 AI 建议时段
# ---------------------------------------------------------------------------
# 用户口径：「AI 建议时间段按照国人观影时间习惯**周维度**生成」。
# 单一小时窗口无法表达工作日与周末的差异——工作日晚间是主力观影窗口，
# 周末白天同样有可观流量，而同一个分类的观影高峰在一周之内并不同步。
#
# 生成口径（两级，都不编造）：
#   ① **真实优先**：该分类在该星期几已发布任务的真实小时分布，
#      取最密集的连续 2 小时段。数据说话。
#   ② **习惯兜底**：该星期几没有真实样本时，用「国人观影作息」基线兜底
#      （周一至周五晚间黄金档、周六日白天+晚间双峰）。基线是**通用作息规律**，
#      不是编造的「本分类历史数据」——页面上明确标注来源，避免被误读为实测。
#
# 无任何真实样本且基线也不适用时返回空 dict → 前端显示「暂无建议」。
#
# ISO 星期：1=周一 … 7=周日（datetime.isoweekday()）。
WEEKDAY_NAMES: dict[int, str] = {
    1: "周一", 2: "周二", 3: "周三", 4: "周四",
    5: "周五", 6: "周六", 7: "周日",
}
# 国人观影作息基线（周一至周五 / 周末分开）。
# 工作日：晚间通勤后到睡前的连续观影窗口，主峰 20:00–22:00。
# 周末：白天（10:00–12:00）与晚间（20:00–22:00）双峰。
WEEKDAY_HABIT_BASELINE: dict[int, list[tuple[int, int]]] = {
    1: [(20, 22), (19, 21), (21, 23)],
    2: [(20, 22), (19, 21), (21, 23)],
    3: [(20, 22), (19, 21), (21, 23)],
    4: [(20, 22), (19, 21), (21, 23)],
    5: [(20, 22), (21, 23), (19, 21)],
    6: [(20, 22), (10, 12), (14, 16)],
    7: [(20, 22), (10, 12), (14, 16)],
}


def _week_windows_from_hours(hours: dict[int, int]) -> tuple[int, int]:
    """从「小时 → 次数」分布取最密集的连续 2 小时段（真实派生）。"""
    if not hours:
        return (0, 0)
    peak_hour = max(hours, key=lambda h: (hours[h], -h))
    return (peak_hour, min(24, peak_hour + 2))


def _ai_suggest_week_windows(db: Session, category_key: str) -> dict[str, Any]:
    """周维度 AI 建议时段：真实历史优先，通用观影作息兜底。

    返回形如：
        {"windows": {"1": [20, 22], ...}, "source": {"1": "history", ...}}
    - windows：ISO 星期 → [起, 止)
    - source ：每一天的来源（"history"=真实发布分布 / "habit"=通用作息基线）

    完全没有样本的星期几不出现在 windows 里——**不编造**。
    """
    rows = db.execute(
        select(SCPublishTask.published_at)
        .join(SCPushListItem, SCPublishTask.list_item_id == SCPushListItem.id)
        .where(SCPublishTask.status == "published",
               SCPushListItem.category_key == category_key)
    ).all()
    # 真实样本按「ISO 星期 → 小时分布」分组
    by_weekday: dict[int, dict[int, int]] = {}
    for (moment,) in rows:
        if not moment:
            continue
        local = moment.astimezone(shanghai_tz())
        bucket = by_weekday.setdefault(local.isoweekday(), {})
        bucket[local.hour] = bucket.get(local.hour, 0) + 1

    windows: dict[str, list[int]] = {}
    source: dict[str, str] = {}
    for weekday in range(1, 8):
        hours = by_weekday.get(weekday) or {}
        if hours:
            start, end = _week_windows_from_hours(hours)
            if start:
                windows[str(weekday)] = [start, end]
                source[str(weekday)] = "history"
                continue
        # 该星期几无真实样本 → 用通用观影作息基线兜底（明确标注来源）
        baseline = WEEKDAY_HABIT_BASELINE.get(weekday) or []
        if baseline:
            start, end = baseline[0]
            windows[str(weekday)] = [start, end]
            source[str(weekday)] = "habit"
    return {"windows": windows, "source": source}


def _week_window_for(weekday: int, raw: str) -> tuple[int, int] | None:
    """从周维度 JSON 里取某一天的配置时段；没有则返回 None。"""
    data = _jload(raw, {}) if raw else {}
    if not isinstance(data, dict):
        return None
    item = data.get(str(weekday))
    if not isinstance(item, (list, tuple)) or len(item) < 2:
        return None
    try:
        start = int(item[0])
        end = int(item[1])
    except (TypeError, ValueError):
        return None
    if start == end:
        return None
    return (start, end)


def _quiet_window(cycle: dict[str, Any]) -> tuple[int, int] | None:
    """静默保护区间（起, 止）；未启用或配置无效时返回 None。

    支持跨午夜：quiet_start=22 / quiet_end=6 表示 22:00–次日 6:00。
    """
    if not cycle.get("quiet_enabled"):
        return None
    try:
        start = int(cycle.get("quiet_start") or 0)
        end = int(cycle.get("quiet_end") or 0)
    except (TypeError, ValueError):
        return None
    if start == end:
        return None
    return (start, end)


def _window_overlaps_quiet(window: tuple[int, int], quiet: tuple[int, int]) -> bool:
    """配置的时段是否与静默区间有重叠。

    两个区间都支持跨午夜（止 < 起 表示跨午夜）。
    """
    ws, we = window
    qs, qe = quiet

    def _hours(win: tuple[int, int]) -> set[int]:
        s, e = win
        if s < e:
            return set(range(s, e))
        return set(range(s, 24)) | set(range(0, e))

    return bool(_hours((ws, we)) & _hours((qs, qe)))


def _quiet_conflict_reason(window: tuple[int, int], quiet: tuple[int, int]) -> str:
    """静默冲突的可读原因（中文，不出现任何后端字段名）。"""
    qs, qe = quiet
    return (f"配置时段 {window[0]}:00–{window[1]}:00 与静默保护时段 "
            f"{qs}:00–{qe}:00 重叠，静默保护开启期间不推送，请调整配置时段")


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
    # v2.1.6：支持只对**勾选的分类**应用默认（配合前端勾选语义：
    # 未勾选或全勾选 = 全部）。
    wanted = {str(x).strip() for x in (p.get("category_keys") or []) if str(x).strip()}
    # 先快照「从未配置过」的行（回填前取，否则指纹被回填动作抹掉）
    before = list(db.scalars(select(SCCategorySchedule).where(
        SCCategorySchedule.plan_version_id == version.id)).all())
    untouched_keys = {r.category_key for r in before if _is_untouched_schedule(r)}
    rows = _ensure_schedules(db, version.id)
    windows = _preference_windows(db, _quiet_window(_cycle_plan_of(db, version.id)))
    applied = 0
    updated = 0
    for row in rows:
        # v2.1.6：只处理勾选的分类（不传 = 全部）。
        if wanted and row.category_key not in wanted:
            continue
        cat = next((c for c in _tmdb_genre_catalog()
                    if c["category_key"] == row.category_key), None)
        win = windows.get(row.category_key, FULL_DAY_WINDOW)
        ws, we, hl, dl = _default_schedule_for((cat or {}).get("media_type") or "", win)
        if not force and row.category_key not in untouched_keys:
            continue
        changed = (int(row.window_start or 0) != ws
                   or int(row.window_end or 0) != we
                   or not row.enabled)
        row.window_start, row.window_end = ws, we
        # v2.1.6：分类级「每小时上限 / 日上限」已移除，统一由周期计划管理约束，
        # 因此这里不再回写分类级上限。
        row.enabled = True
        # v2.1.1：一键应用默认 = 系统托管 → 该行重新可被系统自动刷新。
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
    # v2.1.6：时段取**默认时间表**（热门白天 / 冷门夜间，避开静默），
    # 与「一键恢复默认」同一口径。
    win = _preference_windows(db, _quiet_window(_cycle_plan_of(db))).get(
        row.category_key, FULL_DAY_WINDOW)
    ws, we, hl, dl = _default_schedule_for(media_type, win)
    row.window_start, row.window_end = ws, we
    row.ai_adopted = False
    # v2.1.6：分类级上限已移除，统一由周期计划管理约束——
    # 恢复默认只改时段，不回写分类级 hourly/daily。
    row.updated_at = utcnow()
    db.commit()
    return {"ok": True, "item": _model_dict(row)}


@router.get("/category-schedules")
def api_category_schedules(version_id: int = 0, db: Session = Depends(get_db)):
    """分类-时间排期表：每分类一行，含**合并后的推荐时间段**。

    v2.1.6：档位偏好与 AI 建议合并为单一「推荐时间段」（2 小时颗粒度），
    每个分类都必有建议（实测优先，无实测用默认时间表兜底），
    不再输出周维度网格。
    """
    version = _active_version(db) if not version_id else db.get(SCSharePlanVersion, version_id)
    if not version:
        return {"items": [], "version_id": 0, "reason": "无活动计划版本"}
    rows = _ensure_schedules(db, version.id)
    db.commit()
    names = {c.category_key: c.name for c in db.scalars(select(SCCategory)).all()}
    cat_map = {c["category_key"]: c for c in _tmdb_genre_catalog()}
    # v2.1.4：静默保护区间（开启时用于校验配置时段是否冲突）
    cycle = _jload(version.config_json, {}).get("cycle_plan") or {}
    quiet = _quiet_window(cycle)
    # v2.1.6：默认时间表（热门白天 / 冷门夜间）——推荐时段的兜底来源，
    # 且已避开静默保护区间。
    default_windows = _preference_windows(db, quiet)
    # 偏好排名（档位）——与推荐时间段合并展示。
    pref_rank = {p["category_key"]: p.get("rank")
                 for p in _category_preference(db)}
    items = []
    for r in rows:
        d = _model_dict(r)
        d["name"] = _category_safe_name(r.category_key, names)
        # v2.1.4：给前端一个**中性引用名**（ref），避免页面源码里出现后端字段名。
        # 前端只用 ref 做键匹配，展示一律用 name。
        d["ref"] = r.category_key
        d["tmdb_genre_id"] = cat_map.get(r.category_key, {}).get("tmdb_genre_id", 0)
        # v2.1.6：合并后的推荐时间段（实测优先 → 默认时间表兜底）。
        fallback = default_windows.get(r.category_key, FULL_DAY_WINDOW)
        sug_start, sug_end, sug_source = _category_suggest_window(
            db, r.category_key, fallback)
        d["suggest_start"] = sug_start
        d["suggest_end"] = sug_end
        d["suggest_source"] = sug_source
        # 兼容旧字段：AI 建议时段仍写 ai_window_start/end，前端过渡期可继续用。
        if not r.ai_adopted:
            d["ai_window_start"], d["ai_window_end"] = sug_start, sug_end
        # v2.1.6：偏好档位（排名）并入推荐时段，不再单独占一列。
        d["pref_rank"] = pref_rank.get(r.category_key, 0)
        # v2.1.4：已采纳的周维度配置窗口（调度仍兼容读取）
        d["week_windows"] = _jload(r.week_windows, {}) if r.week_windows else {}
        # v2.1.4：静默冲突标记（前端据此提示，默认时段推荐也要避开静默区）
        conflict = False
        if quiet:
            ws, we = int(r.window_start or 0), int(r.window_end or 0)
            if ws != we and _window_overlaps_quiet((ws, we), quiet):
                conflict = True
            ww = d["week_windows"] if isinstance(d["week_windows"], dict) else {}
            for day, pair in ww.items():
                if isinstance(pair, (list, tuple)) and len(pair) >= 2:
                    try:
                        if int(pair[0]) != int(pair[1]) and _window_overlaps_quiet(
                                (int(pair[0]), int(pair[1])), quiet):
                            conflict = True
                            break
                    except (TypeError, ValueError):
                        continue
        d["quiet_conflict"] = conflict
        # v2.1.1：补媒体类型（前端不再展示「周期维度」列，仍随行下发备用）
        d["media_type"] = cat_map.get(r.category_key, {}).get("media_type", "")
        items.append(d)
    # v2.1.4：整表按**配置时间段**升序排列（0-2 → 2-4 → 4-6 …），
    # 同起点的按止点升序，再按分类键稳定排序。原先按 TMDB genre id 排，
    # 与用户看表时「按时间顺序巡检每个时段」的直觉不一致。
    items.sort(key=lambda x: (int(x.get("window_start") or 0),
                              int(x.get("window_end") or 0),
                              str(x.get("category_key") or "")))
    return {"items": items, "version_id": version.id,
            "quiet_enabled": bool(quiet),
            "quiet_start": quiet[0] if quiet else 0,
            "quiet_end": quiet[1] if quiet else 0}


@router.post("/category-schedules")
def api_upsert_category_schedule(body: Payload, db: Session = Depends(get_db)):
    p = body.payload
    active = _active_version(db)
    version_id = int(p.get("plan_version_id") or (active.id if active else 0))
    category_key = str(p.get("category_key") or "").strip()
    if not version_id or not category_key:
        return {"ok": False, "reason": "plan_version_id 与 category_key 必填"}
    ws = max(0, min(23, int(p.get("window_start") or 0)))
    we = max(ws + 1, min(24, int(p.get("window_end") or 24)))
    # v2.1.4：静默保护开启时，配置时段不得与静默区间重叠
    # （否则该分类在静默时段被推送，与「静默保护」的语义直接矛盾）。
    # **校验必须先于任何写库动作**——否则拒绝路径上残留的待提交行
    # （session autoflush=False，后续查询看不到它）会在下一次提交时
    # 触发唯一约束冲突。
    version = db.get(SCSharePlanVersion, version_id)
    cycle = (_jload(version.config_json, {}) if version else {}).get("cycle_plan") or {}
    quiet = _quiet_window(cycle)
    if quiet and _window_overlaps_quiet((ws, we), quiet):
        return {"ok": False, "reason": _quiet_conflict_reason((ws, we), quiet)}
    # v2.1.4：周维度配置窗口（采纳后写入，逐日生效）
    cleaned: dict[str, list[int]] | None = None
    week_windows = p.get("week_windows")
    if isinstance(week_windows, dict):
        cleaned = {}
        for day, pair in week_windows.items():
            if not isinstance(pair, (list, tuple)) or len(pair) < 2:
                continue
            try:
                d_start = max(0, min(23, int(pair[0])))
                d_end = max(d_start + 1, min(24, int(pair[1])))
            except (TypeError, ValueError):
                continue
            if quiet and _window_overlaps_quiet((d_start, d_end), quiet):
                return {"ok": False,
                        "reason": _quiet_conflict_reason((d_start, d_end), quiet)}
            cleaned[str(int(day))] = [d_start, d_end]
    row = db.scalar(select(SCCategorySchedule).where(
        SCCategorySchedule.plan_version_id == version_id,
        SCCategorySchedule.category_key == category_key))
    if not row:
        row = SCCategorySchedule(plan_version_id=version_id, category_key=category_key)
        db.add(row)
    row.window_start = ws
    row.window_end = we
    if cleaned is not None:
        row.week_windows = _jdump(cleaned)
    elif "week_windows" in p:
        row.week_windows = ""
    # v2.1.6：分类级「每小时上限 / 日上限」已从分类排期表移除，
    # 速率统一由**周期计划管理**约束（category_hour_max / day_max）。
    # 因此这里只在请求体**显式携带**时写入，缺失时保留库中原值——
    # 否则前端不再传这两个字段会把它们静默清零（0=不限），等于放开限流。
    if "hourly_limit" in p:
        row.hourly_limit = max(0, int(p.get("hourly_limit") or 0))
    if "daily_limit" in p:
        row.daily_limit = max(0, int(p.get("daily_limit") or 0))
    row.ai_window_start = max(0, min(23, int(p.get("ai_window_start") or 0)))
    row.ai_window_end = max(row.ai_window_start + 1, min(24, int(p.get("ai_window_end") or 0))) if p.get("ai_window_end") else 0
    row.ai_adopted = bool(p.get("ai_adopted", False))
    # v2.1.1：enabled 不传时**保持原值**。此前默认 False，导致前端保存单行排期
    # （未显式带 enabled）会把该分类意外停用，排期形同虚设。
    if "enabled" in p:
        row.enabled = bool(p.get("enabled"))
    # v2.1.4：只有当提交的时段**确实不等于偏好推荐值**时，才判定为「用户手工设定」。
    # 此前无条件置 window_auto=False，而前端「保存全部」会逐行提交**所有**行
    # （含用户根本没碰过的行），于是点一次保存全部，全表都变成「手工设定」，
    # 此后「按偏好分配时段」对所有行永久失效——配置时段与偏好档位推荐值从此脱钩，
    # 这正是用户反馈「配置时间段默认值应该与偏好档位推荐值一样」的真正根因。
    # v2.1.6：比较基准必须用**与默认时间表同口径**的窗口（含静默避让），
    # 否则用户保存的就是系统刚推的默认时段，却被判成「手工设定」。
    pref_win = _preference_windows(db, quiet).get(category_key, FULL_DAY_WINDOW)
    if (ws, we) != tuple(pref_win):
        row.window_auto = False
    row.updated_at = utcnow()
    db.commit()
    return {"ok": True, "item": _model_dict(row)}


@router.post("/category-schedules/adopt-ai")
def api_adopt_schedule_ai(body: Payload, db: Session = Depends(get_db)):
    """采纳推荐时段：把建议窗口写入当前配置并标记已采纳。

    v2.1.6：按**单一时间段**采纳（2 小时颗粒度），不再写周维度网格。
    每个分类都必有建议，因此不再出现「按钮置灰点不了」：
      ① 真实优先——该分类已发布任务的小时分布派生（来源：实测）；
      ② 兜底——该分类的默认时间表时段（来源：习惯基线）。
    兜底不是编造的历史数据，页面会明确标注来源。

    静默保护开启时，采纳的时段不得落在静默区间内。
    """
    p = body.payload
    row = db.get(SCCategorySchedule, int(p.get("id") or 0))
    if not row:
        return {"ok": False, "reason": "排期行不存在"}
    version = db.get(SCSharePlanVersion, int(row.plan_version_id or 0))
    cycle = (_jload(version.config_json, {}) if version else {}).get("cycle_plan") or {}
    quiet = _quiet_window(cycle)
    fallback = _preference_windows(db, quiet).get(row.category_key, FULL_DAY_WINDOW)
    sug_start, sug_end, sug_source = _category_suggest_window(
        db, row.category_key, fallback)
    # v2.1.6：起点 0 是合法值（夜间段 00:00–02:00），不能用 `not sug_start` 判无建议。
    if not sug_source:
        return {"ok": False, "reason": "该分类暂无可用的推荐时段"}
    if quiet and _window_overlaps_quiet((sug_start, sug_end), quiet):
        return {"ok": False,
                "reason": _quiet_conflict_reason((sug_start, sug_end), quiet)}
    row.window_start, row.window_end = sug_start, sug_end
    # v2.1.6：单一时间段口径，清掉历史周维度配置（否则调度仍按周维度走）。
    row.week_windows = ""
    row.ai_window_start = sug_start
    row.ai_window_end = sug_end
    row.ai_adopted = True
    # 采纳是用户手工动作 → 该行不再被系统自动刷新覆盖。
    row.window_auto = False
    row.updated_at = utcnow()
    db.commit()
    return {"ok": True, "window_start": sug_start, "window_end": sug_end,
            "source": sug_source, "week_windows": {}}


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
    # v2.1.6：单分类每日上限——分类排期表移除「日上限」列后，
    # 分类级日限流统一由周期计划约束（此前无处可配，分类级日限流是死配置）。
    "category_day_max": 0,
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
    for k in ("hour_max", "category_hour_max", "category_day_max",
              "day_max", "week_max", "month_max"):
        cycle[k] = max(0, int(cycle.get(k) or 0))
    for k in ("quiet_start", "window_start", "week_rest_hour"):
        cycle[k] = max(0, min(23, int(cycle.get(k) or 0)))
    for k in ("quiet_end", "window_end"):
        cycle[k] = max(1, min(24, int(cycle.get(k) or 24)))
    cfg["cycle_plan"] = cycle
    version.config_json = _jdump(cfg)
    db.commit()
    return {"ok": True, "cycle_plan": cycle}


def _display_time(value: datetime | None) -> str:
    """把库里的时间转成**给人看的**简洁字符串（v2.1.3）。

    真机问题：`_model_dict` 原先直接 `value.isoformat()`，输出
    `2026-11-22T01:00:17.123456` —— ①中间那个 `T` 是用户完全看不懂的"未知字符"；
    ②带微秒 + 偏移后缀，长度顶满表格列宽；③SQLite 读回的是 **naive UTC**，
    浏览器若直接 `new Date()` 会当本地时间解析，整体偏 8 小时。

    现在统一输出 **`YYYY-MM-DD HH:mm:ss`（Asia/Shanghai 本地时间）**：
      - naive 值按 UTC 解读（SQLite 的实际存法），再转上海时区；
      - 不带 `T`、不带微秒、不带 `+08:00` 后缀，列宽友好；
      - 空值返回空串，前端统一渲染为「—」。
    """
    dt = utils.as_utc(value)
    if dt is None:
        return ""
    return dt.astimezone(shanghai_tz()).strftime("%Y-%m-%d %H:%M:%S")


def _model_dict(row) -> dict[str, Any]:
    data = {}
    for column in row.__table__.columns:
        value = getattr(row, column.name)
        if isinstance(value, datetime):
            value = _display_time(value)
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
            "category_hour_max": int(cycle.get("category_hour_max") or 0),
            "category_day_max": int(cycle.get("category_day_max") or 0),
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
