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
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import and_, select, update, func, or_, delete, distinct
from sqlalchemy.orm import Session

from . import runtime_config, utils
from .db import get_db
from .guangya import GuangYaShareClient, create_share_parsed
from .push_gate import evaluate_push_gate
# v2.1.54：genre 名归一的**唯一入口**。资源库分组（main.py）与本文件的主分类
# 必须共用它 —— 此前各写一份，导致同一部作品在三个页面显示成三个分类。
from .tmdb import genre_name_cn as _genre_name_cn
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

# v2.1.16：建分享失败 → 任务落「无效推送」。此时名额会回收、作品回到待推送，
# 但绝不能每 5 秒就重建一次链接（会在网盘里刷出一堆废分享）。给一个退避窗口：
# 窗口内该作品的名额按「占用」处理，不重排；过了窗口才允许再试一次。
INVALID_RETRY_SECONDS = 30 * 60

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

# ---------------------------------------------------------------------------
# v2.1.16 推送记录有效性（把「已首推」从永久历史改为可回收状态）
#
# 用户口径：推送队列 = 资源库作品经发布规则处理后的队列。资源库有影视就该有队列；
# 重建资源库 / 清理资源库 / 取消推送任务 / 取消分享链接，都必须把对应任务归到
# 初始状态，**而不是一直在后台记着「已经首推过」**。
#
# 因此名额判定从「历史是否推过」改为「当前是否存在仍生效的推送记录」：
#   active  在途（planned/building/ready/submitted/…） → 占用名额，防重复建任务
#   valid   已推成功 且 分享链接仍有效 且 作品仍在资源库 → 占用名额，防重复首推
#   invalid 有历史行但无一生效（取消 / 清理 / 链接作废 / 资源已移除）→ 名额可回收
#   none    从未建过 → 新建
#
# 关键：永久闸（旧队列 push_sent 审计一律算「推过」）已被撤销——它会让「取消分享
# 链接」这种用户主动作废的动作永远无法恢复。旧队列的历史推送改为**看它自己的
# 队列行是否还有效**（未被归档、且分享未被取消），见 `_valid_legacy_push_pairs`。
# ---------------------------------------------------------------------------
INVALID_TASK_STATUS = "invalid"        # 无效推送：链接作废 / 建链失败 / 资源已移除
# 视为「链接已作废」的状态：记录仍在，但那份分享已经不能用了。
INVALID_LINK_STATES = frozenset({CLEANED_LINK_STATE, INVALID_TASK_STATUS})

# v2.1.47（用户口径）：「已作废不算任务条数」—— 作废的任务**不占任何额度**。
#
# 额度（每日总上限 / 单分类每日上限 / 均匀排期的每小时名额）问的是「今天已经推出去
# 多少」。下面三类任务从未真正投递：用户主动取消（cancelled）、清理归档（cleaned）、
# 判为无效推送（invalid：链接作废 / 资源已移除 / 建链失败）。让它们继续占额度，
# 等于让根本没发出去的任务吃掉配额，真实可推量凭空减少。
#
# `skipped`（已跳过投递）**故意不在这个集合里**：它有两种来源 ——
#   ① 同一部已在该渠道发过，这次按去重跳过（确实没发）；
#   ② 孤儿在途任务归档（进程中断，无法确认到底发没发）。
# 第 ② 种有真的发出去的可能，宁可让它继续占额度，也不凭猜疑多放一个名额。
VOID_TASK_STATUSES = frozenset({CLEANED_TASK_STATUS, INVALID_TASK_STATUS, "cancelled"})
# 计入「推送趋势」失败线的状态（v2.1.19）。
# 只算真正的失败：failed（重试中的临时失败也算失败尝试）+ invalid（判为无效推送）。
# 在途（ACTIVE_TASK_STATUS）、cancelled（用户主动取消）、cleaned（已归档）都**不是**
# 失败 —— 此前「非 published 一律算 failed」把这三类全算成失败，趋势图上因此出现
# 与真实故障无关的高峰。
TREND_FAILED_STATUSES = frozenset({"failed", INVALID_TASK_STATUS})
# 名额判定的三态取值
SLOT_ACTIVE = "active"
SLOT_VALID = "valid"
SLOT_INVALID = "invalid"
SLOT_NONE = "none"

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
    # v2.1.54：先把 tail 过一遍官方归一。`genre:scifi&fantasy` 这种脏键
    # 认得出是「科幻奇幻」就直接给中文名，不必等迁移跑完才显示对。
    cn = _genre_name_cn(tail)
    if cn and utils.normalize_work_name(cn) != tail:
        return cn
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
    # v2.1.54：**先**把历史脏分类键迁到官方中文键，**再**种子字典。
    # 顺序不能反：TMDB 漏翻时（线上实测 `Sci-Fi & Fantasy` 2438 条资源、
    # `War & Politics` 1408 条）会生成 `genre:scifi&fantasy` 这类不在 27 项
    # 官方字典里的键，界面上只能显示「未分类」、排期表里也没有对应的行。
    # 迁移是幂等的，每轮调度跑一次也只是几十行查询。
    try:
        migrate_legacy_category_keys(db)
    except Exception:
        db.rollback()
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
# v2.1.56：「仍在更新（returning）」的停更兜底窗口（天）。
# 真机反例：TMDB 把 2005 年就播完的《王保长后传》(tmdb 225764) 长期标成
# Returning Series（`finish_status=returning`、`last_air_date=2005-06-11`），
# 而 returning 分支原先「永不过期」，于是一部 21 年前的老剧被永久挂上「新上映」。
# 现在：声称仍在更新、但最后一集播出日距今超过这个天数 → 按存量处理。
RETURNING_STALE_DAYS = 30


# ---------------------------------------------------------------------------
# v2.1.22：推送记录三类标签（新入库 / 新上映 / 存量排期）
# ---------------------------------------------------------------------------
# 用户口径：推送记录要能一眼看出「这条为什么现在推」——是刚入库的新片、近期上映的
# 新作，还是老片补排（存量排期）。四个地方（今日推送排期 / 推送历史 / 推送队列 /
# 推送任务）共用同一套判定，绝不各算一套。
#
# **v2.1.23 起过期口径按用户口径重定义（每日翻转，不再用固定天数窗口）**：
#   new_library —— 「每日 0-24 小时入库在当日算新入库，24 时后转为存量」。
#                  判据 = 该作品**首次登记进资源库**的时间落在**上海自然日的今天**。
#                  取最早一条库快照的登记时间（SCLibraryVersion.created_at）；
#                  快照缺失时退到成员文件的网盘创建时间（Resource.ctime）。
#                  —— 不是「最近 7 天」，跨过今天 0 点就转存量。
#   new_release —— 「新上映至官方剧集更新完结（TMDB 数据）延后一天的 0 时转为存量」。
#                  判据按 TMDB 连载状态分两类：
#                    · 剧集（tv）：
#                        finish_status=returning（仍在更新）→ 需同时看最后一集播出日：
#                                                 距今未超过 RETURNING_STALE_DAYS 天才算新上映，
#                                                 超过即视为长期停更转存量（v2.1.56）；
#                                                 last_air_date 拿不到时不猜，维持算新上映；
#                        finish_status=ended     → 以「最后一集播出日 + 1 天 0 点」为界，
#                                                  过了就转存量（last_air_date 缺失时
#                                                  无法判定截止日，按已完结处理 → 不算新上映）；
#                        finish_status 空（老数据未回填）→ 退回下面的电影口径。
#                    · 电影（movie）：没有「更新完结」概念，沿用 NEW_RELEASE_WINDOW_DAYS
#                      天上映窗口，且必须**已经上映**（未来日期不算）。
#                  任何「还没上映」的作品都不算新上映——不能把没上映的标成上映了。
#   backlog     —— 前两条都不成立（且至少有一项可判定）时给「存量排期」，即老片补排。
# 前两个标签可同时出现（今日入库且仍在上映/更新）；「存量排期」只在无其他标签时出现。
# **拿不到就留空**：入库时间与上映信息都不可判定时，标签留空，不猜成「存量排期」。
PUSH_TAG_NEW_LIBRARY = "new_library"
PUSH_TAG_NEW_RELEASE = "new_release"
PUSH_TAG_BACKLOG = "backlog"
PUSH_TAG_LABELS: dict[str, str] = {
    PUSH_TAG_NEW_LIBRARY: "新入库",
    PUSH_TAG_NEW_RELEASE: "新上映",
    PUSH_TAG_BACKLOG: "存量排期",
}
# 「新入库」判定窗口（天）。v2.1.23 起口径改为「上海自然日当天」，
# 这个常量只保留给旧调用方/测试做兼容引用，判定不再用它。
NEW_LIBRARY_WINDOW_DAYS = 7
# 每个作品回查上映日期时最多取多少条成员资源（一页 200 条 × 上百集会把 SQL 参数打爆）。
PUSH_TAG_MEMBER_ID_LIMIT = 30


# ---------------------------------------------------------------------------
# v2.1.22：上新即时推送（开关 + 三档冲突策略的**真正语义**）
# ---------------------------------------------------------------------------
# 用户口径：
#   · 开关**不启用** → 资源库内影视全部当作存量排期，持续轮转推送（不插队）。
#   · 开关**启用**后按冲突处理策略三档：
#       prefer_release（默认）—— a. 新上映入库完 → 插队立即推；b. 其余按排期轮转。
#       prefer_new            —— a. 新上映 → 插队立即推；b. 新入库按其入库顺序
#                                 优先排队；c. 存量按排期轮转。
#       prefer_backlog        —— 全量按排期轮转（等价于开关关闭）。
#
# 为什么需要这个通道：`plan_engine` 只按「分类时段窗口 + 分类游标（入库顺序）」
# 生成任务，新上映的作品不看上映日期，前面排着几百部存量时，新片落地后要等
# 很久才推得出去。冲突策略此前只在 dispatch 里做**同批次排序**，救不了这个。
#
# 插队 = 不受**分类时段窗口**与分类级上限约束（那是"排期"的约束），但仍受
# 「上新即时推送每小时上限」（`sc_quick_publish_hourly_limit`，按分钟匀速放行）
# 与 TG 发送节奏约束 —— 否则真机会被 Telegram 限流（红线）。
INSTANT_TASK_PRIORITY = 900        # 新上映：插队，立即推
NEW_ENTRY_TASK_PRIORITY = 800      # 新入库优先：排在存量之前，按入库顺序
# 单轮插队上限：积压一夜后不至于一次性把队列灌爆（多出来的下一轮继续）。
INSTANT_ROUND_LIMIT = 20


def instant_push_mode(db: Session) -> str:
    """当前「上新即时推送」的生效档位：`off` / `prefer_release` / `prefer_new`。

    开关关闭、或策略选「存量排期优先」→ `off`（全量按排期轮转，等价于关闭）。
    非法/空值按默认档 `prefer_release` 处理（与 `_normalize_conflict_policy` 同口径）。
    """
    raw_switch = str(runtime_config.get(db, "sc_quick_publish_enabled") or "true").lower()
    if raw_switch not in ("1", "true", "yes", "on"):
        return "off"
    policy = _normalize_conflict_policy(
        runtime_config.get(db, "sc_conflict_policy") or "prefer_release")
    return "off" if policy == "prefer_backlog" else policy


def update_push_suppressed(db: Session) -> bool:
    """当前是否**整条停用追更即时推送**（v2.1.25）。

    用户口径（2026-11-24）：「追更应该是包含在新上映、新入库模式里，存量排期不应该
    推刚入库追更，存量排期下一次轮转检测到更新可重推」，并明确「全拦」——

      · 冲突策略 = `prefer_backlog`（存量排期优先）→ **所有追更一律不即时推**，
        作品按存量轮转的节奏走；轮转轮到它时自然会带上最新内容再推一次。
      · 其余两档（新上映优先 / 新入库优先）→ 追更照旧即时推。

    为什么需要这道闸门：追更走的是 `publish_resource` 之外的独立通道
    （`process_change_events` 把队列项复活成 pending），它**不经过 `dispatch`**，
    所以 `conflict_policy` 的排序与门禁都管不到它——存量模式下「说不推新片」，
    追更却照推，用户看到的就是「存量排期还推送刚入库的东西」。

    与 `instant_push_mode` 的关系：那个函数管「新片插队」，这个函数管「追更」，
    两者在 `prefer_backlog` 下都归零，语义一致（存量模式 = 只按排期轮转）。
    """
    policy = _normalize_conflict_policy(
        runtime_config.get(db, "sc_conflict_policy") or "prefer_release")
    return policy == "prefer_backlog"


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
    stock: dict[str, int] | None = None,
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
    # v2.1.54：给了库存信息就按新口径分（段数按库存、段位按热门）。
    # 这里只返回每个分类的**第一段**（主时段），完整多段见
    # `_assign_category_segments` —— 老调用方全都只认一个窗口，不能改签名。
    if stock:
        groups = _segments_by_stock(prefs, primary, stock)
        if groups:
            return {k: v[0] for k, v in groups.items()}
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


def _segments_by_stock(prefs: list[dict[str, Any]],
                       primary: list[tuple[int, int]],
                       stock: dict[str, int]) -> dict[str, list[tuple[int, int]]]:
    """v2.1.54：把时段分给分类 —— **段数按库存、段位按热门**。

    用户口径逐条落地：
      · 「结合资源库各分类的作品数量，均匀排期」→ 段数由库存占比决定；
      · 「现在空档的时段太多」→ **库存为 0 的分类一段都不给**，
        给了也是空的（它那 2 小时里没有任何作品可排）；
      · 「允许作品数量多的多时段推送」→ 同一分类拿多段，`_largest_remainder`
        保证每段都落到有货的分类上；
      · 「全天默认 12 个分类（根据库存情况允许重复）」→ 段池 12 段全部分完，
        有货分类不足 12 个时就重复占段。

    段**位**仍按偏好序（人工优先级 → 已推送热度）：热门分类先拿白天段，
    冷门分类拿夜间段（v2.1.6 口径不变）。段数与段位是两件事，不能互相顶替。

    返回 `{分类键: [段, 段, …]}`，主段排第一。
    """
    weights = {str(k): int(v or 0) for k, v in (stock or {}).items() if int(v or 0) > 0}
    if not weights or not primary:
        return {}
    # 名额分配直接复用「名额按部」的同一套最大余数法：
    # 每类至少 1 段（有货就不该被饿死），总额恰为段池大小（12 段铺满全天）。
    counts = _largest_remainder(weights, len(primary))
    if not counts:
        return {}
    order = [str(p.get("category_key") or "") for p in prefs]
    known = set(order)
    # 有货但不在偏好表里的键补到最后：它的库存不能凭空消失
    order += sorted(k for k in counts if k not in known)
    out: dict[str, list[tuple[int, int]]] = {}
    idx = 0
    for key in order:
        n = int(counts.get(key, 0) or 0)
        if n <= 0:
            continue
        picked: list[tuple[int, int]] = []
        for _ in range(n):
            picked.append(primary[idx % len(primary)])
            idx += 1
        out[key] = picked
    return out


def _assign_category_segments(prefs: list[dict[str, Any]],
                              quiet: tuple[int, int] | None = None,
                              stock: dict[str, int] | None = None,
                              ) -> dict[str, list[tuple[int, int]]]:
    """v2.1.54：每个分类应占的**全部**时段（主段在前）。

    只有 `_ensure_schedules` 用它 —— 主段写 `window_start/window_end`，
    其余写 `extra_windows`。其余调用方一律走 `_assign_category_windows`（单窗口）。
    """
    segments = list(CATEGORY_WINDOW_SEGMENTS)
    if quiet:
        segments = [seg for seg in segments if not _window_overlaps_quiet(seg, quiet)]
    if not segments:
        return {str(p.get("category_key") or ""): [CATEGORY_WINDOW_SEGMENTS[0]]
                for p in prefs}
    day_set = set(CATEGORY_DAY_SEGMENTS)
    night_set = set(CATEGORY_NIGHT_SEGMENTS)
    primary = ([s for s in segments if s in day_set]
               + [s for s in segments if s in night_set])
    if stock:
        groups = _segments_by_stock(prefs, primary, stock)
        if groups:
            return groups
    # 没有库存信息（老库、测试、全库空）→ 退回一分类一段的旧口径，
    # 但同样返回列表，让调用方只有一条代码路径。
    return {k: [v] for k, v in _assign_category_windows(prefs, quiet).items()}


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


def _share_target(db: Session, work_key: str, metadata: dict, rep: Resource,
                  resolver=None, lv=None) -> tuple[str, str]:
    """决定「建分享用哪个目录、叫什么名字」（v2.1.57）。

    真机事故（2026-11-28）：网盘「我的分享」里一条条全是单集名，同一部剧还建了
    两三条。根因有两个，都在这一个调用点上：
      ① 名字：直接取快照里的 `metadata.title` —— 那是扫描那一刻的**单集文件名**；
      ② 目录：直接取 `metadata.share_dir_id` —— 那时还指着**季目录**，
         于是分享出去的是「某一季」而不是整部剧。

    规则：
      · 目录：候选 = 快照目录 + 各成员目录；**排除等于任何成员 `parent_id` 的值**
              （那是季目录）；取第一个非空。都判不出才退回代表资源的文件 id。
      · 标题：`_work_titles` 能拒绝文件名并回落到作品名 → 清理过的目录名 →
              去掉扩展名的文件名（兜底也绝不把 `.mp4` 露给用户）。
    """
    members: list = []
    if resolver is not None and lv is not None:
        try:
            members = list(resolver.work_members(lv) or [])
        except Exception:      # 解析成员失败不影响建分享，退回代表资源单条
            members = []
    if not members:
        members = [rep]

    parents = {str(getattr(m, "parent_id", "") or "") for m in members}
    parents.discard("")
    cands = [str(metadata.get("share_dir_id") or "")]
    cands += [str(getattr(m, "share_dir_id", "") or "") for m in members]
    dir_id = next((c for c in cands if c and c not in parents), "")
    if not dir_id:
        dir_id = next((c for c in cands if c), "")

    title = (_work_titles(db, [work_key]) or {}).get(work_key) or ""
    if not _looks_like_title(title):
        title = utils.clean_work_title(str(metadata.get("share_dir_name") or ""))
    if not title:
        title = utils.clean_work_title(str(getattr(rep, "share_dir_name", "") or ""))
    if not title:
        title = re.sub(r"\.[A-Za-z0-9]{2,4}$", "", str(getattr(rep, "name", "") or ""))
    return dir_id, title.strip()


def _existing_share_on_dir(db: Session, dir_id: str) -> str:
    """同一目录下已经建过的有效分享链接（v2.1.57：建之前先复用，不重复建）。

    真机：235 部作品各有 ≥2 条独立分享，因为每个渠道一条任务、每条任务只认自己的
    `share_url`，建分享这条路径从不看「同目录是不是已经有链接了」。
    只复用 `share_status=active` 的行 —— 失效链接绝不能拿来顶。
    """
    if not dir_id:
        return ""
    try:
        url = db.scalar(select(Resource.share_url).where(
            Resource.share_dir_id == str(dir_id),
            Resource.share_status == "active",
            Resource.share_url != "").limit(1))
    except Exception:
        return ""
    return str(url or "")


def _existing_work_share_url(db: Session, work_key: str,
                             exclude_task_id: int = 0) -> str:
    """同一作品（work_key）任一渠道任务已建过的有效分享链接（v2.1.63）。

    用户口径：一部作品只生成一条分享链接，无论推到频道还是群都复用这一条。

    真机 2026-11-28：439 部作品每部都配 2 个群渠道（chat_id 只有两个），但
    v2.1.57 的 `_existing_share_on_dir` 只查 `Resource` 表，而作品级建链成功后
    只写 `task.share_url`、**从不写回 Resource**（git 确认 v2.1.56/57 都没有
    写回代码）→ 第二条渠道任务查不到同目录已有链接，又新建一条，26 部真出双链。

    这里按**作品维度跨 task** 查，取 id 最小的有效链接（即最早建、已发出去那条）；
    失效态（cleaned/invalid）一律不复用，与 `reuse_link` 的判据逐字一致。
    """
    wk = str(work_key or "").strip()
    if not wk:
        return ""
    try:
        stmt = (select(SCPublishTask.id, SCPublishTask.share_url,
                       SCPublishTask.link_state)
                .where(SCPublishTask.work_key == wk,
                       SCPublishTask.share_url.isnot(None),
                       SCPublishTask.share_url != "")
                .order_by(SCPublishTask.id).limit(64))
        if exclude_task_id:
            stmt = stmt.where(SCPublishTask.id != exclude_task_id)
        rows = db.execute(stmt).all()
    except Exception:
        return ""
    for _tid, url, link_state in rows:
        if str(link_state or "") in INVALID_LINK_STATES:
            continue
        candidate = str(url or "").strip()
        if candidate:
            return candidate
    return ""


def _primary_category(resource: Resource) -> tuple[str, str]:
    """资源的主分类键（`genre:<中文名>`）与媒体类型。

    v2.1.54：取到的 genre 名**先过 `tmdb.genre_name_cn()` 归一**再拼键。
    此前直接把 TMDB 返回的原文往键里塞，而 TMDB 的 zh-CN 对两个 TV 专用分类
    没有翻译 → 线上出现 `genre:scifi&fantasy`（2438 条资源）这种不在
    27 项官方字典里的脏键，界面上只能显示成「未分类」，排期表里也没有对应的行，
    这批作品永远排不出去。归一后统一落到 `genre:科幻奇幻`。
    """
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
    if primary:
        primary = _genre_name_cn(primary)
    category_key = f"genre:{utils.normalize_work_name(primary)}" if primary else "genre:unknown"
    return category_key, media_type


def _work_category(rows: list[Resource]) -> tuple[str, str]:
    """一部作品的主分类：**扫它的全部资源**，取第一个能判出非「未分类」的分类。

    v2.1.54：此前是拿 `ordered[0]` 这一条资源的 genres 做判据。一部剧有几十个
    文件，只要排在最前那条恰好没识别出 genres，整部作品就被判成「未分类」，
    而且后续资源识别回了类型也不会再纠正（见 `sync_library_versions` 的
    一次性写入 bug）。线上实测 586 部「未分类」里有 504 部的资源其实带着类型。

    顺序仍用调用方给的稳定序（file_id 升序），保证结果可复现：
    同一批资源无论扫几遍，判出来的分类都一样。
    """
    fallback_media = ""
    for res in rows:
        key, media_type = _primary_category(res)
        if not fallback_media and media_type:
            fallback_media = media_type
        if key != "genre:unknown":
            return key, media_type or fallback_media
    return "genre:unknown", fallback_media


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
    """按资源事实生成作品级版本快照与唯一主归属。

    v2.1.54（真机：资源库 586 部作品卡在「未分类」，其中 504 部的资源其实带类型）：
    归类从「首次创建时写一次」改为**每轮按最新资源重算**。两个旧写法共同把错误固化了：
      ① `if exists: continue` —— 版本指纹没变就整段跳过，连重算的机会都没有；
      ② `if not ownership: …` —— 归属只在第一轮写入，之后资源识别回填了 genres 也永不更新。
    实测证据：`朱雀堂` 资源识别 05:48:34、归类 05:48:38（识别早 4 秒，genres 当时已在库），
    仍被判成未分类 —— 说明不是时序问题，就是「只写一次、从不重算」。
    """
    resources = list(db.scalars(select(Resource)).all())
    grouped: dict[str, list[Resource]] = {}
    for res in resources:
        grouped.setdefault(utils.work_group_key(res), []).append(res)

    created = 0
    categories = 0
    updated = 0
    ensured: set[str] = set()   # 本轮已确保存在的分类键，省掉重复查询
    for work_key, rows in grouped.items():
        ordered = sorted(rows, key=lambda r: (str(r.file_id or ""), int(r.size or 0), int(r.utime or 0)))
        # v2.1.54：归类**每轮重算**，且扫全部资源而不是只看 ordered[0]。
        # 这两点必须放在指纹判断**之前** —— 指纹只反映内容身份（file_id:size），
        # 它不变不代表 genres 没被识别回填过。
        category_key, media_type = _work_category(ordered)
        if category_key not in ensured:
            _ensure_category(db, category_key, media_type)
            ensured.add(category_key)
        ownership = db.scalar(select(SCWorkCategory).where(SCWorkCategory.work_key == work_key))
        if ownership is None:
            db.add(SCWorkCategory(work_key=work_key, category_key=category_key,
                                  media_type=media_type, source="tmdb", confidence=1.0))
            categories += 1
        elif str(ownership.category_key or "") != category_key:
            # 只改真正变了的行。已经判对的归属一律不碰 —— 不刷新 assigned_at，
            # 让「这部作品是什么时候归到这个分类的」保持可追溯。
            ownership.category_key = category_key
            if media_type:
                ownership.media_type = media_type
            ownership.source = "tmdb"
            ownership.confidence = 1.0
            ownership.assigned_at = utcnow()
            updated += 1
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
        # v2.1.57（真机：推送标题、hashtag、分享目标全是单集信息）：元数据改成
        # **按整组行**解析，不再照抄 `ordered[0]` 那一行。扫描入库那一刻它的
        # `media_title` / `share_dir_name` 还是空、`share_dir_id` 还指着季目录，
        # 抄下来就等于把「单集文件名 + 季目录」定格成作品级信息（1166 个版本里
        # 873 个 title 是文件名、880 个 share_dir_name 是空）。
        # 解析规则与「判不出就留空」见 `utils.work_display_meta`。
        meta_work = utils.work_display_meta(ordered)
        metadata = {
            "title": meta_work["title"],
            "poster_path": meta_work["poster_path"] or getattr(rep, "poster_path", ""),
            "share_dir_id": meta_work["share_dir_id"],
            "share_dir_name": meta_work["share_dir_name"],
            "tmdb_id": meta_work["tmdb_id"] or getattr(rep, "tmdb_id", 0),
            "media_type": meta_work["media_type"] or media_type,
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
    db.commit()
    # `categories_created` / `categories_updated` 分开报：
    # 前者是首次入库的作品数，后者是这次真正被纠正的归属数（v2.1.54 新增）。
    # 两个数都要能对上明细，见 `recompute_work_categories()` 的同口径统计。
    return {"versions_created": created, "categories_created": categories,
            "categories_updated": updated, "works": len(grouped)}


def recompute_version_metadata(db: Session, *, dry_run: bool = False) -> dict[str, Any]:
    """按实时资源重算「最新版本快照」的展示元数据（v2.1.57，幂等）。

    为什么需要它：`sync_library_versions` 只在**指纹变化时**才写新版本，而指纹
    只由 `file_id:size` 集合决定。扫描入库那一刻作品级字段（`media_title` /
    `share_dir_name` / 作品目录 id）还没回填，快照就把单集文件名与季目录定格了；
    之后识别回填只改 `resources`，快照永远不重算 —— 真机 1166 个版本里
    873 个 title 是文件名、880 个 share_dir_name 为空、641 个目录 id 指着季目录。

    三条硬约束（都是既有红线）：
      ① **不造新版本**：只原地改 `metadata_json`，`fingerprint` / `version_no` /
         `change_type` / `created_at` 一个都不动 —— 造新版本等于伪造一次「追更」，
         会触发推送（`recompute_work_categories` 同一条红线）。
      ② **只改最新版本**：历史版本是当时的事实，改了就是把历史涂掉。
      ③ **判不出就不动**：`work_display_meta` 返回空标题时保留原值，绝不猜。

    资源侧只用 8 列投影，不实例化 `Resource` 实体（红线：2.4 万行实体 ≈ 120 MiB）。
    """
    from types import SimpleNamespace
    from sqlalchemy import func as _func

    rows = db.execute(select(
        Resource.file_id, Resource.path_hint, Resource.share_dir_name,
        Resource.media_title, Resource.tmdb_id, Resource.media_type,
        Resource.share_dir_id, Resource.parent_id, Resource.poster_path,
    )).all()
    grouped: dict[str, list] = {}
    for r in rows:
        proj = SimpleNamespace(file_id=r[0], path_hint=r[1], share_dir_name=r[2],
                               media_title=r[3], tmdb_id=r[4], media_type=r[5],
                               share_dir_id=r[6], parent_id=r[7], poster_path=r[8])
        grouped.setdefault(utils.work_group_key(proj), []).append(proj)

    latest: dict[str, SCLibraryVersion] = {}
    for v in db.scalars(select(SCLibraryVersion)).all():
        wk = str(v.work_key or "")
        cur = latest.get(wk)
        if cur is None or int(v.version_no or 0) > int(cur.version_no or 0):
            latest[wk] = v

    checked = fixed = skipped = 0
    samples: list[dict] = []
    for wk, ver in latest.items():
        checked += 1
        members = grouped.get(wk) or []
        if not members:
            # 资源已不在库（归档/换源）：保留原样，不猜、不抹掉历史标题
            skipped += 1
            continue
        meta = utils.work_display_meta(members)
        old = _jload(ver.metadata_json, {}) or {}
        new = dict(old)
        # v2.1.65：补 poster_path——此前漏了它，导致识别回填的海报永远进不了快照
        # metadata_json（真机 886/1186 部快照无海报）。仍守「判不出就不动」：
        # 空海报不写，保留旧值，绝不抹掉、绝不猜。
        for field in ("title", "share_dir_id", "share_dir_name",
                      "media_type", "tmdb_id", "poster_path"):
            val = meta.get(field)
            if field in ("tmdb_id",):
                if val:
                    new[field] = val
                continue
            if str(val or "").strip():
                new[field] = val
        if not new.get("title"):
            # 解析不出标题就整条不动（保留旧值，绝不写成空或文件名）
            skipped += 1
            continue
        if new == old:
            continue
        fixed += 1
        if len(samples) < 5:
            samples.append({"work_key": wk, "version_no": int(ver.version_no or 0),
                            "title": [str(old.get("title") or "")[:40],
                                      str(new.get("title") or "")[:40]],
                            "share_dir_id": [str(old.get("share_dir_id") or ""),
                                             str(new.get("share_dir_id") or "")]})
        if not dry_run:
            ver.metadata_json = _jdump(new)
    if not dry_run and fixed:
        db.commit()
    return {"checked": checked, "fixed": fixed, "skipped": skipped,
            "dry_run": bool(dry_run), "samples": samples}


# ---------------------------------------------------------------------------
# v2.1.54：归类修复的两件工具
# ---------------------------------------------------------------------------
def migrate_legacy_category_keys(db: Session, *, dry_run: bool = False) -> dict[str, Any]:
    """把历史脏分类键迁到官方中文键（幂等，可反复跑）。

    判定不看硬编码清单，而是**拿每个键过一遍 `genre_name_cn`**：
    归一后拼出来的键与自己不同 → 就是脏键。这样将来 TMDB 再漏翻别的分类，
    也自动被这一步收掉，不需要再加一张映射表。

    三张表都要迁，缺一张就会留下「分类字典有 A、排期表有 B、作品归属还是 A」的裂痕：
      · `sc_categories`        —— 分类字典（category_key 唯一）
      · `sc_category_schedules` —— 分类排期（(版本, 键) 唯一）
      · `sc_work_categories`   —— 作品归属（work_key 唯一）

    目标键已存在时**保留目标键那一行、删掉脏键行**：目标键是官方中文键，
    其排期/优先级才是页面在用的那一份；脏键行只是同一分类的影子。
    """
    moves: list[dict[str, str]] = []

    # ① 分类字典
    cats = list(db.scalars(select(SCCategory)).all())
    by_key = {str(r.category_key or ""): r for r in cats}
    for row in cats:
        key = str(row.category_key or "")
        if not key.startswith("genre:"):
            continue
        tail = key.split(":", 1)[1]
        cn = _genre_name_cn(tail)
        canon = f"genre:{utils.normalize_work_name(cn)}" if cn else ""
        if not canon or canon == key:
            continue
        moves.append({"table": "categories", "from": key, "to": canon})
        if dry_run:
            continue
        if canon in by_key and by_key[canon] is not row:
            db.delete(row)               # 目标行已在 → 删影子行
            by_key.pop(key, None)
        else:
            row.category_key = canon
            row.name = cn
            row.updated_at = utcnow()
            by_key.pop(key, None)
            by_key[canon] = row

    # ② 分类排期（按 (版本, 键) 唯一，逐版本处理）
    scheds = list(db.scalars(select(SCCategorySchedule)).all())
    sched_by = {(int(r.plan_version_id or 0), str(r.category_key or "")): r for r in scheds}
    for row in scheds:
        key = str(row.category_key or "")
        if not key.startswith("genre:"):
            continue
        cn = _genre_name_cn(key.split(":", 1)[1])
        canon = f"genre:{utils.normalize_work_name(cn)}" if cn else ""
        if not canon or canon == key:
            continue
        moves.append({"table": "schedules", "from": key, "to": canon})
        if dry_run:
            continue
        slot = (int(row.plan_version_id or 0), canon)
        if slot in sched_by and sched_by[slot] is not row:
            db.delete(row)               # 该版本下目标键已有排期行 → 删影子行
        else:
            row.category_key = canon
            row.updated_at = utcnow()
            sched_by[slot] = row
        sched_by.pop((int(row.plan_version_id or 0), key), None)

    # ③ 作品归属（work_key 唯一，直接改键，不会冲突）
    owners = list(db.scalars(select(SCWorkCategory).where(
        SCWorkCategory.category_key.like("genre:%"))).all())
    for row in owners:
        key = str(row.category_key or "")
        cn = _genre_name_cn(key.split(":", 1)[1])
        canon = f"genre:{utils.normalize_work_name(cn)}" if cn else ""
        if not canon or canon == key:
            continue
        moves.append({"table": "work_categories", "from": key, "to": canon})
        if not dry_run:
            row.category_key = canon
            row.assigned_at = utcnow()

    if not dry_run and moves:
        db.commit()
    return {"moved": len(moves), "dry_run": dry_run,
            "details": moves[:200], "truncated": max(0, len(moves) - 200)}


def recompute_work_categories(db: Session, *, dry_run: bool = False) -> dict[str, Any]:
    """按最新资源重算每部作品的主分类 —— **只动归属表，不生成版本快照**。

    为什么不直接跑 `sync_library_versions`：那个函数会为指纹变化的作品写
    `sc_library_versions` 新版本（`change_type=updated`），而新版本是追更推送的
    触发源。只想修归类的时候，绝不能顺手造出一批「更新」事件。

    `dry_run=True` 返回将要变更的明细（影响清单），一个字都不写库。
    """
    resources = list(db.scalars(select(Resource)).all())
    grouped: dict[str, list[Resource]] = {}
    for res in resources:
        grouped.setdefault(utils.work_group_key(res), []).append(res)

    owners = {str(r.work_key or ""): r for r in db.scalars(select(SCWorkCategory)).all()}
    ensured: set[str] = set()
    changes: list[dict[str, Any]] = []
    created = 0
    for work_key, rows in grouped.items():
        ordered = sorted(rows, key=lambda r: (str(r.file_id or ""), int(r.size or 0), int(r.utime or 0)))
        category_key, media_type = _work_category(ordered)
        row = owners.get(work_key)
        before = str(row.category_key or "") if row is not None else ""
        if before == category_key:
            continue
        changes.append({"work_key": work_key, "from": before or "(无)", "to": category_key,
                        "title": (str(getattr(ordered[0], "media_title", "") or "")
                                  or str(getattr(ordered[0], "name", "")))})
        if dry_run:
            continue
        if category_key not in ensured:
            _ensure_category(db, category_key, media_type)
            ensured.add(category_key)
        if row is None:
            db.add(SCWorkCategory(work_key=work_key, category_key=category_key,
                                  media_type=media_type, source="tmdb", confidence=1.0))
            created += 1
        else:
            row.category_key = category_key
            if media_type:
                row.media_type = media_type
            row.source = "tmdb"
            row.confidence = 1.0
            row.assigned_at = utcnow()
    if not dry_run:
        db.commit()
    # 迁移后库存分布：页面要用它解释「为什么排期变了」，所以一并回传
    stock: dict[str, int] = {}
    src = changes if dry_run else None
    if dry_run:
        for ch in changes:
            stock[ch["to"]] = stock.get(ch["to"], 0) + 1
    return {"works": len(grouped), "changed": len(changes), "created": created,
            "dry_run": dry_run, "items": changes[:300],
            "truncated": max(0, len(changes) - 300),
            "target_stock": stock if src is not None else {}}


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


# ---------------------------------------------------------------------------
# v2.1.19：分类排期判定的**纯函数内核**
#
# 为什么抽出来：`plan_engine` 里的 `_sched_allows` 闭包把 `sh_hour` 写死成「当前
# 小时」，只能回答「此刻能不能排」。而「今日排期」要回答「10 点能不能排、14 点能
# 不能排」，必须按任意小时求值。抽成参数化纯函数后，**实跑与预测共用同一份判据**，
# 预测才不会和真实调度算出差。
# ---------------------------------------------------------------------------
def _sched_window_for(sched: SCCategorySchedule, weekday: int) -> tuple[int, int]:
    """取该分类**某个星期几**生效的 (start_hour, end_hour)。周维度配置优先。"""
    day_window = _week_window_for(weekday, sched.week_windows or "")
    if day_window:
        return int(day_window[0]), int(day_window[1])
    return int(sched.window_start or 0), int(sched.window_end or 0)


def _sched_extra_windows(sched: SCCategorySchedule) -> list[tuple[int, int]]:
    """该分类的**额外时段**（v2.1.54：库存多的分类一天可以占多个时段）。

    读 `extra_windows`（JSON `[[起,止],…]`）。脏值一律丢掉 —— 宁可少一个时段，
    也不让一个写坏的窗口把分类放到推不出去的小时里。
    """
    raw = getattr(sched, "extra_windows", "") or ""
    if not raw:
        return []
    try:
        arr = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return []
    if not isinstance(arr, list):
        return []
    out: list[tuple[int, int]] = []
    for pair in arr:
        if not isinstance(pair, (list, tuple)) or len(pair) < 2:
            continue
        try:
            s, e = int(pair[0]), int(pair[1])
        except (TypeError, ValueError):
            continue
        if not (0 <= s <= 24 and 0 <= e <= 24):
            continue
        out.append((s, e))
    return out


def _hour_in_window(hour: int, ws: int, we: int) -> bool:
    """`hour` 是否落在 [ws, we) 内（支持跨午夜，起止相同视为全天）。"""
    if we > ws:
        return ws <= hour < we
    if we < ws:
        return hour >= ws or hour < we      # 跨午夜
    return True                             # 起止相同 = 全天


def _sched_allows_core(sched: SCCategorySchedule | None, *,
                       hour: int, weekday: int,
                       per_category_count: int,
                       cycle_hour_max: int,
                       cycle_day_max: int,
                       today_count: int = 0,
                       check_window: bool = True) -> bool:
    """纯判定：该分类在 `hour` 点这一刻是否允许排期。**不访问数据库**。

    判定顺序与 `plan_engine` 原闭包逐条一致：
      ① 没有排期行 → 放行（兼容存量，见 `_ensure_schedules` 只给 TMDB genre 建行）；
      ② 小时不在窗口内 → 拒绝（支持跨午夜窗口）；
      ③ 已达分类每小时上限 → 拒绝；
      ④ 已达分类当日上限 → 拒绝。

    v2.1.53（空档补位）：新增 `check_window`。补位时段里，方案是**跨分类时段窗口**
    指定的（否则「剧情」永远进不了 15 点，等于补不了），所以那几小时只跳过 ②，
    ③④ 两条上限照旧全部生效 —— 补位绝不能突破每小时 / 每日额度。

    v2.1.54：② 改为「主时段 + 额外时段，任一命中即放行」。库存多的分类一天占多个
    时段（`extra_windows`），只认主时段会让它多出来的那些段形同虚设。
    """
    if sched is None:
        return True
    if check_window:
        ws, we = _sched_window_for(sched, weekday)
        windows = [(ws, we)] + _sched_extra_windows(sched)
        if not any(_hour_in_window(hour, s, e) for s, e in windows):
            return False
    if cycle_hour_max and per_category_count >= cycle_hour_max:
        return False
    if cycle_day_max and today_count >= cycle_day_max:
        return False
    return True


def _hourly_due(cap: int, minute: int) -> int:
    """本小时到第 `minute` 分钟为止，累计**应放行**的任务数（匀速放行核心）。

    用户口径（v2.1.19）：每时段的推送任务应当**均匀分配**，不能时间一到就一股脑推完。

    原实现是「整点配额突发」——`submitted_this_hour >= cap` 一刀切：10:00:00 一到
    就把 cap 部全部推完，之后 59 分钟静默。用户看到的就是「一到点全推出去」。
    改成线性进度后，cap=3 落在第 0/20/40 分钟各 1 部，cap=60 每分钟 1 部。
    这不是新增节流，只是把同一个 cap 从「整点突发」摊成「匀速」。
    """
    if cap <= 0:
        return 10 ** 9          # 0/负数 = 不限，这里给个大数让调用方直接比较
    minute = max(0, min(59, int(minute)))
    return (cap * (minute + 1) + 59) // 60      # ceil(cap*(m+1)/60)，纯整数避免浮点


def _uniform_minute(cap: int, index: int) -> int:
    """匀速放行下，本小时第 `index` 部（1-based）预计落在第几分钟。"""
    if cap <= 0:
        return 0
    return min(59, max(0, int(60 * (max(1, int(index)) - 1) / cap)))


# ---------------------------------------------------------------------------
# v2.1.23：排期时段匀速（用户口径 4）
# ---------------------------------------------------------------------------
# 用户原话：「推送影视应该每小时内均匀排布，不要到时间排期一起蜂拥推送：
#            举例，本时段为 2 小时，有 4 部剧那么应该是 2 小时*60 分钟/4 部剧 = 30
#            分钟推送一部，除不尽时四舍五入。」
#
# 关键决定：**把「部数」而不是「条数」作为分母**。同一部剧推到频道 + 群是两条任务，
# 但用户认知里是「一部剧」，两部渠道任务同时出去才是对的；混在一起算会让间隔减半。
#
# 时段粒度取 `HOUR_SEGMENT_HOURS`（默认 2 小时），与分类时间排期的时段粒度一致 ——
# 排期本身按 2 小时分段，匀速也按同一段算，两处不会各说各的。

def _period_bounds(now_sh: datetime) -> tuple[datetime, datetime]:
    """当前排期时段的 [起, 止)（上海时间，按 HOUR_SEGMENT_HOURS 对齐整点）。"""
    start = now_sh.replace(minute=0, second=0, microsecond=0)
    start -= timedelta(hours=int(start.hour) % HOUR_SEGMENT_HOURS)
    return start, start + timedelta(hours=HOUR_SEGMENT_HOURS)


def _pace_interval_minutes(period_start: datetime, period_end: datetime,
                           count: int) -> int:
    """时段内均分的间隔分钟数 = 四舍五入(时段分钟 / 部数)，最小 1 分钟。"""
    if count <= 0:
        return 0
    total = max(1, int(round((period_end - period_start).total_seconds() / 60)))
    return max(1, int(round(total / count)))


def _pace_slot_times(period_start: datetime, period_end: datetime,
                     count: int) -> list[datetime]:
    """把 `count` 部作品均匀铺满一个时段，返回每部的预计时刻（升序）。

    例：2 小时（120 分钟）/ 4 部 = 30 分钟 → 起点、+30、+60、+90。
    除不尽按四舍五入（120/7 ≈ 17 分钟）；至少 1 分钟，避免挤在同一时刻。
    """
    if count <= 0:
        return []
    step = _pace_interval_minutes(period_start, period_end, count)
    return [period_start + timedelta(minutes=step * i) for i in range(count)]


# 「已经真的放出去了」的状态：这些任务会让作品的时段份额被消耗掉。
# invalid / cancelled 不算 —— 它们没发出去，不该占用本时段的匀速额度。
PACE_RELEASED_STATUS = frozenset(
    {"submitted", "accepted", "publishing", "published", "failed", "skipped"})


def _period_pace(db: Session, now: datetime) -> dict[str, Any]:
    """算本时段的匀速额度（dispatch 用）。

    返回：`period_start` / `period_end` / `n_works` / `interval` / `due`。
    `n_works` = 本时段已建任务覆盖的作品数（按 `scheduled_at` 落段内，distinct work_key）；
    `due`     = 到此刻应已放行的作品数 = floor(已过分钟 / interval) + 1。
    `n_works = 0`（本时段还没排任何任务）时 `due = None`，调用方不额外限流。
    """
    now_sh = utils.as_utc(now).astimezone(shanghai_tz())
    start_sh, end_sh = _period_bounds(now_sh)
    start_utc = start_sh.astimezone(timezone.utc)
    end_utc = end_sh.astimezone(timezone.utc)
    out: dict[str, Any] = {"period_start": start_sh, "period_end": end_sh,
                           "n_works": 0, "interval": 0, "due": None}
    try:
        n_works = int(db.scalar(
            select(func.count(func.distinct(SCPublishTask.work_key)))
            .where(SCPublishTask.status != CLEANED_TASK_STATUS,
                   SCPublishTask.scheduled_at.isnot(None),
                   SCPublishTask.scheduled_at >= start_utc,
                   SCPublishTask.scheduled_at < end_utc)) or 0)
    except Exception:
        db.rollback()
        return out
    if n_works <= 0:
        return out
    interval = _pace_interval_minutes(start_sh, end_sh, n_works)
    elapsed = max(0, int((now_sh - start_sh).total_seconds() // 60))
    out.update({"n_works": n_works, "interval": interval,
                "due": int(elapsed // interval) + 1})
    return out


def _period_released_works(db: Session, pace: dict[str, Any]) -> set[str]:
    """本时段**已经放出去**的作品集合（口径见 `PACE_RELEASED_STATUS`）。

    v2.1.33：原来只有一个返回「部数」的 `_period_released`，dispatch 只能拿它
    跟 `due` 比大小，回答不了「**这一条**任务的 work_key 是不是已经放过了」——
    于是同一部剧的第二个渠道被当成超额，整轮 `break`，固定多等一个 interval。
    改成返回集合，配额判断用 `len()`，同作品判断用 `in`。
    """
    start_sh = pace["period_start"]
    end_sh = pace["period_end"]
    try:
        rows = db.execute(
            select(SCPublishTask.work_key)
            .where(SCPublishTask.scheduled_at.isnot(None),
                   SCPublishTask.scheduled_at >= start_sh.astimezone(timezone.utc),
                   SCPublishTask.scheduled_at < end_sh.astimezone(timezone.utc),
                   SCPublishTask.work_key.isnot(None),
                   SCPublishTask.work_key != "",
                   SCPublishTask.status.in_(PACE_RELEASED_STATUS))).all()
        return {str(r[0]) for r in rows if r and r[0]}
    except Exception:
        db.rollback()
        return set()


def _period_released(db: Session, pace: dict[str, Any]) -> int:
    """本时段**已经放出去**的作品数（distinct work_key，口径见 `PACE_RELEASED_STATUS`）。"""
    return len(_period_released_works(db, pace))


def _tags_for_status(tags: list[dict], status: str) -> list[dict]:
    """按任务状态裁剪标签：**已结束**的记录不显示「存量排期」。

    用户口径 5：「新上映标签失效时，推送历史里该标签应消失」。
    标签是实时判定的，失效后自然会从「新上映」变成「存量排期」；但「存量排期」
    描述的是**还没推**的原因（老片补排），套在一条早就推完的历史记录上没有信息量。
    所以终态行只保留仍然有效的「新入库 / 新上映」，其余留空 → 页面显示「—」。
    """
    if str(status or "") in ACTIVE_TASK_STATUS:
        return tags
    return [t for t in tags if str(t.get("key") or "") != PUSH_TAG_BACKLOG]


# 排期类型的展示优先级（v2.1.26）。与「今日推送排期」按标签归列的顺序一致：
# 新上映 > 新入库 > 存量排期（一部作品只归一档，不重复占位）。
SCHEDULE_KIND_ORDER: tuple[str, ...] = (PUSH_TAG_NEW_RELEASE, PUSH_TAG_NEW_LIBRARY,
                                        PUSH_TAG_BACKLOG)


def _schedule_kind_pair(tags: list[dict] | None) -> tuple[str, str]:
    """「排期类型」的 (key, 中文标签)（v2.1.26，v2.1.28 起同时给 key）。

    为什么要把 key 也吐出去：前端要给「新入库」单独上绿色高亮（用户口径），
    但**绝不能靠中文文案去比对**——文案一改高亮就静默失效，而且中文散在前端就等于
    第二套口径。key 恒为 `new_release` / `new_library` / `backlog` / `""`，
    颜色映射放在前端只做「key → 样式」，判定仍然只在后端这一处。

    为什么单独成一个字段，而不是让前端从 `tags` 里挑：
      · `tags` 回答的是「**现在**还算不算新」——终态行被 `_tags_for_status`
        去掉「存量排期」后会变空（用户口径：失效就消失）；
      · `schedule_kind` 回答的是「这条**当时**是按哪一档排上的」——历史记录
        也能看出它是新上映来的还是存量轮转来的，所以用**未裁剪**的判定结果。
    两者都来自同一个 `_work_push_tags`，绝不在这里二次判定「新不新」。

    标签一条都没有（入库时间与上映日期都判定不出）→ 返回 `("", "")`，
    页面显示「—」，不猜成「存量排期」。
    """
    keys = {str((t or {}).get("key") or "") for t in (tags or [])}
    for key in SCHEDULE_KIND_ORDER:
        if key in keys:
            return key, PUSH_TAG_LABELS[key]
    return "", ""


def _schedule_kind_of(tags: list[dict] | None) -> str:
    """只要中文标签时用这个（等价 `_schedule_kind_pair(tags)[1]`）。"""
    return _schedule_kind_pair(tags)[1]


def _respread_forecast_times(hours: list[dict], now_sh: datetime) -> dict[str, Any]:
    """把「今日推送排期」里每部作品的预计时间改成**时段内均分**，并回写。

    为什么需要这一道后处理：模拟阶段是**逐小时**跑的，同一时段跨两个小时，
    逐小时各自摊分钟数会把「18:00-20:00 共 4 部」算成每小时各 4 部，
    间隔直接错一半。这里按「排期时段」重新归堆后再统一算，口径与
    `_pace_slot_times` / dispatch 完全一致（时段分钟 ÷ 部数，除不尽四舍五入）。

    返回该时段的节奏摘要（`interval_minutes` / `works`），前端拿去在面板顶部
    写「本时段每 30 分钟 1 部」，不用自己算。
    """
    period_counts: dict[int, int] = {}
    period_entries: dict[int, list[tuple[dict, dict]]] = {}
    for h in hours:
        hour = int(h.get("hour") or 0)
        psh = hour - hour % HOUR_SEGMENT_HOURS
        for it in h.get("items") or []:
            # v2.1.34：`kind=scheduled` 是**当天已经建好任务**的那批（真机反馈：
            # 排期表与推送队列不一致）。它们的时间是既成事实，绝不能参与均分重排——
            # 一旦被重写，排期表显示的时刻就又和队列对不上了，等于把刚修好的
            # 「同口径」重新打破。这类项也不计入时段部数，否则会把预测的间隔估大。
            if str(it.get("kind") or "") == "scheduled":
                continue
            # `works_total` 是未截断的作品数，均分必须按真实部数算间隔
            period_counts[psh] = period_counts.get(psh, 0) + int(
                it.get("works_total") or len(it.get("works") or []))
            for w in it.get("works") or []:
                if not str(w.get("work_key") or ""):
                    continue
                period_entries.setdefault(psh, []).append((it, w))
    pace_info: dict[str, Any] = {"period_hours": HOUR_SEGMENT_HOURS,
                                 "interval_minutes": 0, "works": 0}
    # 时段起点小时 → 该时段的间隔分钟数（回写到每个 hour 上，见函数末尾）
    period_interval: dict[int, int] = {}
    for psh, entries in period_entries.items():
        start = now_sh.replace(hour=psh, minute=0, second=0, microsecond=0)
        end = start + timedelta(hours=HOUR_SEGMENT_HOURS)
        base = now_sh.replace(second=0, microsecond=0)
        if base < start:
            base = start
        if base >= end:
            continue          # 这个时段已经过去，不留时间点
        count = max(len(entries), int(period_counts.get(psh) or 0))
        # v2.1.23：间隔按「**整个时段长度** ÷ 部数」算 —— 与 dispatch 的
        # `_period_pace` 同一个口径（那里 interval 也是全程算）。两处若一个按
        # 全程、一个按剩余时长，预测就会比实跑偏快，等于告诉用户错的时间
        # （v2.1.19 红线：预测与实跑算出差就是骗人）。
        # 但**站台锚在"现在"**而不是时段起点：时段起点可能已经过去（15:07 打开页面
        # 却显示 14:00 的站台），排期要回答的是「接下来几点推」。
        interval = _pace_interval_minutes(start, end, count) or 1
        slots = [base + timedelta(minutes=interval * i) for i in range(max(1, count))]
        objs: dict[int, dict] = {}
        item_times: dict[int, list[str]] = {}
        for idx, (it, w) in enumerate(entries):
            text = slots[idx].strftime("%H:%M") if idx < len(slots) else ""
            w["time"] = text
            objs[id(it)] = it
            if text:
                item_times.setdefault(id(it), []).append(text)
        for oid, texts in item_times.items():
            # 去重且保序：同一部剧推频道 + 群是同一个时间点，不该在行里重复铺 N 遍
            objs[oid]["times"] = list(dict.fromkeys(texts))
        if not pace_info["interval_minutes"]:
            pace_info["interval_minutes"] = interval
            pace_info["works"] = count
        period_interval[psh] = interval
    # v2.1.24（用户口径 1c）：把「每 N 分钟 1 部」回写到**每个小时**上。
    # 前端表头只有四列、时间列固定，节奏文案跟着左侧时间列走（不再报
    # 「18:24–18:42」这种跨小时的跨度，那串数字对用户没有信息量）。
    # 同一时段内的小时共用一个 interval —— 与 `_period_pace` 同源，不另算一套。
    for h in hours:
        hour = int(h.get("hour") or 0)
        h["pace_minutes"] = int(period_interval.get(hour - hour % HOUR_SEGMENT_HOURS) or 0)
    return pace_info


def _category_today_count(db: Session, category_key: str) -> int:
    """某分类当日已创建任务数（分类排期 daily_limit 用）。上海自然日。

    v2.1.47：作废任务（取消 / 清理 / 无效）不计入 —— 它们没真正投递过，不该吃额度。
    """
    now_sh = datetime.now(shanghai_tz())
    day_start = datetime(now_sh.year, now_sh.month, now_sh.day,
                         tzinfo=shanghai_tz()).astimezone(timezone.utc)
    day_end = day_start + timedelta(days=1)
    return int(db.scalar(
        select(func.count(SCPublishTask.id))
        .join(SCPushListItem, SCPublishTask.list_item_id == SCPushListItem.id)
        .where(SCPushListItem.category_key == category_key,
               SCPublishTask.status.notin_(VOID_TASK_STATUSES),
               SCPublishTask.created_at >= day_start,
               SCPublishTask.created_at < day_end)
    ) or 0)


def _category_today_counts(db: Session) -> dict[str, int]:
    """**批量**版：当日各分类已建任务数（条），省掉逐分类查库。

    v2.1.49：一键 AI 调度要按「今天这个分类还能推多少」算名额，逐个分类查会
    在 27 个分类 × 一次点击里打 27 次全表聚合。口径与 `_category_today_count`
    完全一致（作废任务不计、上海自然日、单位是条）。
    """
    now_sh = datetime.now(shanghai_tz())
    day_start = datetime(now_sh.year, now_sh.month, now_sh.day,
                         tzinfo=shanghai_tz()).astimezone(timezone.utc)
    day_end = day_start + timedelta(days=1)
    rows = db.execute(
        select(SCPushListItem.category_key, func.count(SCPublishTask.id))
        .join(SCPushListItem, SCPublishTask.list_item_id == SCPushListItem.id)
        .where(SCPublishTask.status.notin_(VOID_TASK_STATUSES),
               SCPublishTask.created_at >= day_start,
               SCPublishTask.created_at < day_end)
        .group_by(SCPushListItem.category_key)
    ).all()
    return {str(k): int(n or 0) for k, n in rows if k}


def _cycle_quota_gate(db: Session, cycle: dict[str, Any]) -> tuple[bool, str]:
    """周期计划**日**配额门禁（v2.1.0 真实消费，此前只落库）。

    v2.1.42（用户口径）：**周 / 月限额已下线，只保留日度限额**。
    原来 `week_max` / `month_max` 与日上限并排拦截，但周/月额度长期远未触顶、
    只会让「到底被哪一层拦了」变得难解释；用户明确要求砍掉这两层，只按天控量。

    v2.1.45（用户口径）：**周 / 月休止窗口也一并下线**，不留死代码。
    页面上的「周度」「月度」两个子页已整体撤掉，若后端还保留
    `week_rest_enabled` / `month_rest_enabled` 的判定，库里一条页面上再也看不见的
    旧开关就能让普通排期整天 / 整月不排——用户原话是「避免后续被规则卡住，而不知情」。
    所以这两个分支连同 `weekly_rest` / `monthly_rest` 两种原因码直接删除；
    历史遗留的这几个键在下次保存周期计划时清掉（见 `api_save_cycle_plan`）。
    想恢复成按周/月控量或休止，需要改代码，不是改配置。

    统计口径：当日已建任务数（按 created_at 落库时间），0 = 不限。
    **单位是「条」**——与页面文案（「单日推送总上限」按条计）一致，所以这里数的是
    任务行，不是作品部数。
    v2.1.47（用户口径）：作废任务（取消 / 清理 / 无效）不再计入 —— 它们从未真正
    投递，却一直在吃当日额度，用户看到的就是「明明没推出去几条，额度却满了」。
    quiet 已在调用方处理。
    返回 (是否拦截, 原因)。
    """
    now_sh = datetime.now(shanghai_tz())
    # 只剩「日」一层。周 / 月窗口与休止判断已随功能下线删除（v2.1.45）。
    checks = (("day_max", "day"),)
    for key, kind in checks:
        cap = int(cycle.get(key) or 0)
        if cap <= 0:
            continue
        # 统计窗口固定为上海自然日的一天（周窗口的 7 天 / 月窗口早已删掉，
        # 否则哪怕 checks 只剩日度，也留着一个能按周/月统计的死分支）。
        start = now_sh.replace(hour=0, minute=0, second=0, microsecond=0
                               ).astimezone(timezone.utc)
        end = start + timedelta(days=1)
        used = db.scalar(select(func.count(SCPublishTask.id)).where(
            SCPublishTask.status.notin_(VOID_TASK_STATUSES),
            SCPublishTask.created_at >= start, SCPublishTask.created_at < end)) or 0
        if int(used) >= cap:
            return True, f"{kind}_quota"
    return False, ""


# ---------------------------------------------------------------------------
# v2.1.46 一键 AI 调度 · 均匀排期
#
# 真机问题（用户原话）：「我们的作品排期可能不够均匀（比如每个分类下的作品数量
# 不一致，有的分类作品很多、有的很少，有些分类可能不全，而且我们的排期是排期
# 完成后又入库的）」。
#
# 根因不是时段配置，而是**同一小时内的名额分配**：`plan_engine` 按
# `SCCategory.priority desc` 遍历分类，每个分类只要在窗口内就一路建任务直到
# `hour_max` 用完。于是优先级最高的那个分类把整点额度吃光，排在后面的分类
# 每小时都拿 0 部——排期表上看就是「少数几个分类占满整天、其余分类整天没有」。
#
# 本版加一层**名额分配**（不是改时段）：
#   · 输入只有三样：每类在库待排作品数、每类自己的时段窗口、发布规则给的每小时上限；
#   · 用最大余额法把每小时额度按「待排存量权重」分给当小时有窗口的分类，
#     每类至少 1 部（额度够时），谁也不至于被前排吃光；
#   · 结果存进 `config_json.even_plan`，由 `plan_engine` 与 `forecast_schedule`
#     **同一份**数据消费（v2.1.19 红线：预测与实跑必须同口径）。
#
# 三条硬边界（用户口径：只调整每个时段排的数量，不能影响原有时段和时段 AI 建议）：
#   ① **不动** `SCCategorySchedule` 的任何字段（window_start/end、week_windows、
#      pref_rank、suggest_*）——时段是谁配的还是谁配的；
#   ② `even_plan` 为空时，`plan_engine` / `forecast_schedule` 全部短路走老路，
#      行为零变化（没点过按钮就等于没这个功能）；
#   ③ 额度值一律来自配置（每小时推送上限 → 周期计划小时上限），代码不写死数字；
#      两个都是 0（＝不限）时如实报「没有可分配的额度」，绝不编一个数出来。
#
# v2.1.47（用户口径第 1、3 条）再修三处，都只动「名额怎么算」，不动时段配置：
#   · **单位对齐**：名额是「部」，此前消费侧数的是任务行（条）。双渠道部署时一部
#     作品吃两个计数，配额 3 部实际只排 1 部 —— 改成按作品计数（`category_work_counts`
#     / `cat_works`），`_category_hour_counts` 也改 `count(distinct work_key)`。
#   · **余量不空转**：名额先夹到该分类「这一小时真能排的部数」，削下来的按剩余
#     余量回流给还有货的分类（`_fit_quota_to_backlog`），总额仍是发布规则给的上限。
#   · **作废不占额度**：取消 / 清理 / 无效的推送任务不计入任何额度（`VOID_TASK_STATUSES`）。
#   方案里另存 `backlog_snapshot`（生成时库存），页面据此提示「排期后又入库了」。
# ---------------------------------------------------------------------------
EVEN_PLAN_KEY = "even_plan"


def _in_quiet_hour(hour: int, quiet: tuple[int, int] | None) -> bool:
    if not quiet:
        return False
    qs, qe = int(quiet[0]), int(quiet[1])
    return (qs <= hour < qe) if qe > qs else (hour >= qs or hour < qe)


def _largest_remainder(weights: dict[str, int], total: int) -> dict[str, int]:
    """最大余额法：把 `total` 个名额按 `weights` 权重分成整数，总和恰为 min(total, ...)。

    - `total <= 0` 或没有候选 → 空；
    - `total < 候选数` → 按权重从大到小各给 1，其余 0（宁可空着几个分类，
      也不让任何一类超额——总额度是发布规则给的硬上限）；
    - 否则先取整数部分、把 0 抬到 1（每类至少 1 部，这是「均匀」的最低保证），
      余额按小数部分从大到小补，超了就从「高出理论值最多」的那类往下削。

    纯整数运算，不引入浮点误差；结果可复现（同输入同输出）。
    """
    if total <= 0 or not weights:
        return {}
    keys = list(weights)
    if total <= len(keys):
        order = sorted(keys, key=lambda k: (-weights[k], k))
        return {k: (1 if i < total else 0) for i, k in enumerate(order)}
    s = sum(weights.values()) or 1
    exact = {k: total * weights[k] / s for k in keys}
    out = {k: max(1, int(exact[k])) for k in keys}
    diff = total - sum(out.values())
    if diff > 0:
        order = sorted(keys, key=lambda k: (-(exact[k] - int(exact[k])), -weights[k], k))
        i = 0
        while diff > 0:
            out[order[i % len(order)]] += 1
            diff -= 1
            i += 1
    elif diff < 0:
        order = sorted(keys, key=lambda k: (exact[k] - out[k], -weights[k], k))
        i = 0
        while diff < 0 and i < 10000:
            k = order[i % len(order)]
            if out[k] > 1:
                out[k] -= 1
                diff += 1
            i += 1
    return out


def _fit_quota_to_backlog(quotas: dict[str, int],
                          room: dict[str, int]) -> dict[str, int]:
    """把每小时名额夹到「这个分类这一小时真能排几部」以内，削下来的回流给有货的分类。

    v2.1.47（真机反馈「有的分类排不上、有的分类排过期」）：
      最大余额法按存量权重分名额，一个只有 2 部作品的小分类，可能在 cap=6 时也拿到
      1–2 个名额；若它每小时名额 2 部、全天 15 个可排小时，方案就写着它能排 30 部，
      实际只有 2 部 —— 剩下的 28 个名额全天**空转**，作品多的分类被自己的配额卡住。

    做法（不发明数字，只是把「用不完的」转给「用得完的」）：
      · `room[k]` = 该分类这一小时最多能排几部（在库存量 与 单分类每小时上限取小），
        由调用方算好传进来；
      · 超额部分削掉，削掉的部数按「剩余余量」为权重用同一套最大余额法回流；
      · 回流总量 = 被削掉的部数，所以总和只会 ≤ 原总额（发布规则给的上限）——
        绝不会因为回流多排一部。
    """
    base = {k: max(0, int(v)) for k, v in (quotas or {}).items()}
    if not base:
        return {}
    out = {k: min(v, max(0, int(room.get(k, 0)))) for k, v in base.items()}
    freed = sum(base.values()) - sum(out.values())
    while freed > 0:
        spare = {k: max(0, int(room.get(k, 0)) - out[k]) for k in out}
        spare = {k: v for k, v in spare.items() if v > 0}
        if not spare:
            break                       # 所有分类都到顶了 → 剩下的名额只能空着
        add = _largest_remainder(spare, min(freed, sum(spare.values())))
        if not add:
            break
        for k, n in add.items():
            take = min(int(n), int(spare[k]))
            if take > 0:
                out[k] += take
                freed -= take
    return {k: v for k, v in out.items() if v > 0}


def _category_backlog(db: Session, live_keys: set[str]) -> dict[str, int]:
    """每个分类**当前在库待排**的作品数（均匀排期的权重来源）。

    与 `plan_engine` / `forecast_schedule` 同一份在库口径：只认最新快照 +
    `live_keys`。没有这批数字就没法按存量分配名额，所以宁可返回空也不编。
    """
    if not live_keys:
        return {}
    latest_ids = select(func.max(SCLibraryVersion.id)).group_by(SCLibraryVersion.work_key)
    rows = db.execute(
        select(SCWorkCategory.category_key, func.count(SCLibraryVersion.id))
        .join(SCLibraryVersion, SCLibraryVersion.work_key == SCWorkCategory.work_key)
        .where(SCLibraryVersion.id.in_(latest_ids))
        .where(SCLibraryVersion.work_key.in_(live_keys))
        .group_by(SCWorkCategory.category_key)
    ).all()
    return {str(k): int(n or 0) for k, n in rows if k}


def _schedule_open_hours(row: "SCCategorySchedule | None",
                         quiet: tuple[int, int] | None,
                         win: tuple[int, int] | None) -> list[int]:
    """某分类「一天里能排期」的小时列表（只读它自己的时段，不改任何东西）。

    顺序是：分类自己的时段窗口 → 发布规则的主推送窗口 → 去掉静默保护小时。
    一切仍以 `SCCategorySchedule` 为准；本函数只做「读出来」，绝不回写。
    """
    if row is None:
        hours = list(range(24))
    else:
        ws = int(row.window_start or 0)
        we = int(row.window_end or 0)
        hours = list(range(24)) if we <= ws else list(range(ws, min(24, we)))
        # v2.1.54：**额外时段**同样算「能排期的小时」。库存多的分类一天占多个时段，
        # 只认主时段会让它多出来的那几段在方案里凭空消失（配了却不生效）。
        extra = _sched_extra_windows(row)
        if extra:
            for s, e in extra:
                hours.extend(h for h in range(24) if _hour_in_window(h, s, e))
            hours = sorted(set(hours))
    if win:
        hours = [h for h in hours if int(win[0]) <= h < int(win[1])]
    if quiet:
        hours = [h for h in hours if not _in_quiet_hour(h, quiet)]
    return hours


def _even_cap_from_config(db: Session, cycle: dict[str, Any]) -> tuple[int, str]:
    """每小时可分配额度：先看「每小时推送上限」，没有再看周期计划的「小时」上限。

    两个都是 0＝不限 → 返回 (0, "")，调用方据此如实报错，**绝不编一个数**。
    """
    cap = 0
    try:
        on = str(runtime_config.get(db, "sc_quick_publish_enabled") or "true").lower() in (
            "1", "true", "yes", "on")
        if on:
            cap = max(0, int(runtime_config.get(db, "sc_quick_publish_hourly_limit") or 0))
    except (TypeError, ValueError):
        cap = 0
    if cap:
        return cap, "每小时推送上限"
    cap = max(0, int(cycle.get("hour_max") or 0))
    if cap:
        return cap, "周期计划的每小时上限"
    return 0, ""


def even_plan_of(db: Session, version: "SCSharePlanVersion | None" = None) -> dict[str, Any]:
    """取当前生效的均匀排期方案；没生成过 / 已被清空 → 空 dict（＝不生效）。"""
    v = version or _active_version(db)
    if v is None:
        return {}
    cfg = _jload(v.config_json, {})
    plan = cfg.get(EVEN_PLAN_KEY) if isinstance(cfg, dict) else None
    if not isinstance(plan, dict):
        return {}
    items = plan.get("items")
    return plan if isinstance(items, dict) and items else {}


def even_hour_quota(plan: dict[str, Any], hour: int) -> dict[str, int] | None:
    """某小时的「分类 → 部数」配额；方案里没有这个小时 → 空 dict（该小时不出货）。"""
    items = (plan or {}).get("items")
    if not isinstance(items, dict) or not items:
        return None
    row = items.get(str(int(hour)))
    if not isinstance(row, dict):
        return {}
    return {str(k): int(v) for k, v in row.items()}


def _even_allows(quota: dict[str, int] | None, category_key: str, used: int) -> bool:
    """均匀排期门禁：该分类在本小时还能不能再出。

    `quota is None` ＝ 没有方案，全部放行（老路径零变化）。
    """
    if quota is None:
        return True
    cap = int((quota or {}).get(category_key) or 0)
    return cap > 0 and int(used) < cap


# ---------------------------------------------------------------------------
# v2.1.53：空档时段补位（用户原话：「能否让 ai 一键调度后让无作品的分类让作品多的
# 分类做替补，弥补时段无作品可推的情况」）
#
# 真机（2026-11-26 15:04）：方案生成了、额度 60 也生效了，但 12–17 点在方案里**完全
# 没有配额** —— 那几小时的分类（未分类到上限 / 儿童 / 纪录 / 冒险 / 肥皂剧 / 西部 /
# 动作冒险）要么没货、要么已到日上限，`build_even_plan` 凑不出候选就 `continue`，
# 于是「额度再大也没用」：没有可分配的对象。
#
# 注意这不是「同段内让位」不够用 —— 同段内的让位 `_fit_quota_to_backlog` 早就做了。
# 缺的只有一种情况：**整个时段的分类一个都出不了货**，只能跨时段借人。
#
# 三条边界（改代码才动，不是配置）：
#   ① 只补**当前小时到当天 24 点**：已经过去的小时补了也没意义（用户拍板的口径）；
#   ② 绝不突破用户主动关掉的时间：主推送窗口之外、静默保护之内一律不补；
#      「可发布时间」是硬约束，「哪个分类发」才是可以商量的；
#   ③ 不发明额度：`cap` 仍只来自发布规则，「每小时 / 每日上限」照旧全部生效。
# ---------------------------------------------------------------------------
def _borrow_enabled(db: Session) -> bool:
    """空档补位开关（`sc_even_borrow_enabled`，默认开）。读不到按开处理。"""
    try:
        raw = str(runtime_config.get(db, "sc_even_borrow_enabled") or "true")
    except Exception:
        return True
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _hour_open_for_borrow(hour: int, quiet: tuple[int, int] | None,
                          win: tuple[int, int] | None) -> bool:
    """补位小时必须落在「用户明确允许发布的时间」内。

    主推送窗口之外 / 静默保护之内 → 一律不补。用户把某段时间关掉是明确的意思表示，
    不能用「补位」把它绕开。
    """
    hour = int(hour)
    if win and not (int(win[0]) <= hour < int(win[1])):
        return False
    if quiet and _in_quiet_hour(hour, quiet):
        return False
    return True


def _borrow_quota(room_works: dict[str, int], cap: int,
                  cycle_hour_max: int) -> dict[str, int]:
    """空档时段按「全库今天真能推的部数」分一份配额。

    用的是与正常小时**同一套** `_largest_remainder` + `_fit_quota_to_backlog`：
    额度只有 `cap` 一份，削下来的回流给还有货的分类 —— 不新增任何额度来源。
    作品多的分类多担、作品少的少担，正是用户说的「让作品多的分类做替补」。
    """
    weights = {k: max(1, int(v)) for k, v in (room_works or {}).items() if int(v) > 0}
    if not weights or int(cap) <= 0:
        return {}
    quotas = _largest_remainder(weights, int(cap))
    room = dict(weights)
    if cycle_hour_max:
        room = {k: min(v, int(cycle_hour_max)) for k, v in room.items()}
    return _fit_quota_to_backlog(quotas, room)


def _borrowed_hours(plan: dict[str, Any]) -> set[int]:
    """方案里的「补位时段」（方案没生成过 / 老方案没这个字段 → 空集，行为零变化）。"""
    raw = (plan or {}).get("borrowed_hours")
    if not isinstance(raw, list):
        return set()
    out: set[int] = set()
    for x in raw:
        try:
            h = int(x)
        except (TypeError, ValueError):
            continue
        if 0 <= h < 24:
            out.add(h)
    return out


def _category_hour_counts(db: Session, hour_start: datetime) -> dict[str, int]:
    """本小时**已经建好**的分类作品数（上海自然小时，按 created_at 落库时间）。

    v2.1.47（用户口径）：单位必须是**部**，不是条。
    均匀排期的名额是「每个时段排几部作品」，而同一部作品推频道 + 群是两条任务；
    此前这里数的是任务行数，于是「配额 2 部」遇到两个渠道时只排得出 1 部 ——
    配的额度和实排部数对不上，等于凭空少排一半。
    （与 `_pace_interval_minutes` 的分母口径一致：那处也是「部数」而不是「条数」。）

    作废任务（取消 / 清理 / 无效）不计入 —— 用户原话「已作废不算任务条数」。
    """
    start = hour_start.astimezone(timezone.utc)
    end = start + timedelta(hours=1)
    rows = db.execute(
        select(SCPushListItem.category_key,
               func.count(distinct(SCPublishTask.work_key)))
        .join(SCPushListItem, SCPublishTask.list_item_id == SCPushListItem.id)
        .where(SCPublishTask.status.notin_(VOID_TASK_STATUSES),
               SCPublishTask.created_at >= start, SCPublishTask.created_at < end)
        .group_by(SCPushListItem.category_key)
    ).all()
    return {str(k): int(n or 0) for k, n in rows if k}


def _stock_segment_pool(quiet: tuple[int, int] | None,
                        win: tuple[int, int] | None) -> list[tuple[int, int]]:
    """可用于分配的时段池：先按静默保护剔除，再按**主推送窗口**剔除。

    v2.1.54：主推送窗口外的小时本来就推不出去，把分类排过去等于让它整天零产出
    （v2.1.9 真机就是这么死的：19 个分类被塞进 0-8，而全局总闸封死 0-8）。
    顺序仍是「热门先拿白天段」——白天段在前、夜间段在后。
    """
    segs = []
    for seg in CATEGORY_WINDOW_SEGMENTS:
        if quiet and _window_overlaps_quiet(seg, quiet):
            continue
        segs.append(seg)
    if win:
        # 优先挑**完全落在主推送窗口内**的段：只覆盖一半的段会让分类有一半小时
        # 推不出去（例：主窗口 9-21，8-10 那个段的前一小时是废的）。
        inside = [s for s in segs if int(s[0]) >= int(win[0]) and int(s[1]) <= int(win[1])]
        if inside:
            segs = inside
        else:
            # 窗口比任何一段都窄（如 9-10）：退回有交集的那几段，至少还能推一部分
            segs = [s for s in segs if int(s[0]) < int(win[1]) and int(s[1]) > int(win[0])]
    day_set = set(CATEGORY_DAY_SEGMENTS)
    night_set = set(CATEGORY_NIGHT_SEGMENTS)
    return ([s for s in segs if s in day_set]
            + [s for s in segs if s in night_set])


def _retime_schedules_by_stock(db: Session, version: "SCSharePlanVersion",
                               quiet: tuple[int, int] | None,
                               win: tuple[int, int] | None,
                               stock: dict[str, int]) -> dict[str, Any]:
    """v2.1.54：按库存重排分类时段（「一键 AI 调度」与「批量采纳」共用）。

    用户口径：「一键 AI 调度：调整当前及后续排期分类、时间搭配，填充无排期的空白时段」。
    此前一键调度**只改每小时排几部**，分类时段一行都不动 —— 于是空档小时依然空着，
    只能靠 v2.1.53 的运行时补位打补丁。这里从配置层根治：

      · 段数按库存、段位按热门（与 `_ensure_schedules` 同一套算法）；
      · 库存 0 的分类**一段都不分**（分了也是空的），其额外时段同时收掉；
      · 库存 0 的分类**自动关闭排期**、有货时自动打开（v2.1.55 用户口径；
        只动系统自己关的行，用户手工关的不擅自打开）；
      · 只改时段 / `window_auto` / 启停，**不动**上限、不动已建任务。

    与推送队列的联动边界（v2.1.34 红线）：已建任务的推送时间是既成事实，
    本函数一个字都不改它们。时段改完后 `forecast_schedule` 立刻按新时段重算预测，
    而队列里在途的任务原样不动 —— 两边仍然对得上。
    """
    prefs = _category_preference(db)
    primary = _stock_segment_pool(quiet, win)
    if not primary:
        return {"moved": 0, "details": [], "truncated": 0,
                "reason": "主推送窗口与静默保护把全天时段都排除掉了，没有可分配的时段"}
    groups = _segments_by_stock(prefs, primary, stock)
    if not groups:
        return {"moved": 0, "details": [], "truncated": 0,
                "reason": "资源库里没有在库待排作品，时段无从分配"}
    rows = list(db.scalars(select(SCCategorySchedule).where(
        SCCategorySchedule.plan_version_id == version.id)).all())
    by_key = {r.category_key: r for r in rows}
    moved = 0
    details: list[dict[str, str]] = []
    for key, group in groups.items():
        row = by_key.get(key)
        if row is None:
            continue                      # 没排期行就不造行（造了页面会多出一行）
        main = group[0]
        extra = [tuple(s) for s in group[1:]]
        before_main = (int(row.window_start or 0), int(row.window_end or 0))
        before_extra = [tuple(x) for x in _sched_extra_windows(row)]
        if before_main == tuple(main) and before_extra == extra:
            continue
        row.window_start, row.window_end = int(main[0]), int(main[1])
        row.extra_windows = _jdump([list(s) for s in extra]) if extra else ""
        # v2.1.55：AI 定过的时段**不再交给系统自动刷新**（window_auto=False）。
        # 否则页面下一次刷新时 `_ensure_schedules` 会按内置单段把它抹回去，
        # 用户视角就是「一键调度跑完，一刷新就变回去了」。
        # 「一键恢复默认」会把它设回 True，那是用户明确要回默认的动作。
        row.window_auto = False
        row.updated_at = utcnow()
        moved += 1
        details.append({
            "name": _category_display_name(key),
            "before": "、".join([f"{before_main[0]}:00–{before_main[1]}:00"]
                                + [f"{s}:00–{e}:00" for s, e in before_extra]),
            "after": "、".join(f"{s}:00–{e}:00" for s, e in group),
        })
    # 库存 0 却还挂着额外时段的：收掉。主时段保留 —— 用户可能就想让它固定在那儿，
    # 等以后有货时能直接出货，不该被系统悄悄改掉。
    for key, row in by_key.items():
        if int(stock.get(key) or 0) > 0 or not str(row.extra_windows or ""):
            continue
        row.extra_windows = ""
        row.updated_at = utcnow()
    # v2.1.55（用户口径 3）：无作品 → 自动关闭让出时段；有作品 → 自动打开。
    # 段已经只分给有货的分类（`_segments_by_stock`），这里把「让位」这半个动作做完：
    # 关闭的行不再占任何小时，被让出来的小时就成了别的分类的时段。
    toggled = _apply_stock_enabled(rows, stock,
                                   known=sum(int(v or 0) for v in stock.values()) > 0)
    return {"moved": moved, "details": details[:60],
            "truncated": max(0, len(details) - 60),
            "closed": toggled["closed"], "opened": toggled["opened"]}


def build_even_plan(db: Session) -> dict[str, Any]:
    """算一份均匀排期方案并落库到活动版本的 `config_json.even_plan`。

    口径（不发明数字）：
      · 每小时额度 = 「每小时推送上限」（没配则周期计划的小时上限），两个都 0 → 报错不生成；
      · 分配权重 = 每个分类**在库待排作品数**，作品多的多给、作品少的少给；
      · 每类至少 1 部（额度够时），额度不够就按权重从大到小给，其余本小时为 0；
      · 还受「单分类每小时上限」与「单分类每日上限」的常识性约束（前者直接夹逼）；
      · 时段一律读分类现有配置，静默保护与主推送窗口之外的小时不参与分配。

    返回结构既给前端回显（分类名已转中文），也写进配置供调度消费。
    """
    version = _active_version(db)
    if version is None:
        return {"ok": False, "reason": "当前没有活动中的计划版本，先建一个发布规则再点一键调度。"}
    cfg = _jload(version.config_json, {})
    cycle = {**CYCLE_PLAN_DEFAULTS, **(cfg.get("cycle_plan") or {})}
    cap, cap_source = _even_cap_from_config(db, cycle)
    if cap <= 0:
        return {"ok": False,
                "reason": ("当前没有可分配的每小时额度（「每小时推送上限」与周期计划的"
                           "「小时」上限都是 0＝不限），先设一个上限再点一键调度。")}
    category_limit = max(1, int(cfg.get("category_limit") or 20))
    cats = list(db.scalars(
        select(SCCategory).where(SCCategory.enabled.is_(True))
        .order_by(SCCategory.priority.desc(), SCCategory.id)
    ).all())[:category_limit]
    if not cats:
        return {"ok": False, "reason": "没有启用中的分类，排期无从谈起。"}

    live_keys = _live_work_keys(db)
    backlog = _category_backlog(db, live_keys)
    if not live_keys or not any(int(v or 0) > 0 for v in backlog.values()):
        return {"ok": False,
                "reason": ("资源库里当前没有在库作品，没有可分配的名额——"
                           "先同步资源库，再来点一键调度。")}
    quiet = _quiet_window(cycle)
    win = None
    try:
        ws = int(cycle.get("window_start") or 0)
        we = int(cycle.get("window_end") or 24)
        if 0 <= ws < we <= 24:
            win = (ws, we)
    except (TypeError, ValueError):
        win = None
    cycle_hour_max = max(0, int(cycle.get("category_hour_max") or 0))
    # v2.1.54（用户口径）：一键 AI 调度**连分类时段一起调**，把没有排期的空白时段填上。
    # 此前这一步只分配「每小时几部」，分类时段一行不动 → 空档依然空着，
    # 只能靠 v2.1.53 的运行时补位打补丁。现在从配置层根治。
    retimed = _retime_schedules_by_stock(db, version, quiet, win, backlog)
    sched_map = {s.category_key: s for s in db.scalars(
        select(SCCategorySchedule).where(
            SCCategorySchedule.plan_version_id == version.id)).all()}
    open_hours = {c.category_key: _schedule_open_hours(
        sched_map.get(c.category_key), quiet, win) for c in cats}

    # v2.1.49（真机问题：方案把整点额度押在「今天根本排不出」的分类上）
    # ------------------------------------------------------------------
    # 真机表现（用户原话）：「20 点整条 0 任务，额度白白作废」。
    # 根因是名额分配只看「在库待排量」，没看「今天还能不能推」：
    #   ① 某分类当日已到 `category_day_max`，`plan_engine` 侧的 `_sched_allows_core`
    #      会直接拒掉它，可方案里照样给它排了名额 → 该整点 0 任务；
    #   ② 排期表里还可能有一行「动作冒险」这种**没有任何作品归属的分类键**
    #      （TMDB 官方分类是「动作」「冒险」两条，不存在合并的那种），它有排期行
    #      但库存恒为 0，同样白占名额。
    # 现在把「今天真能推多少部」作为唯一可分配量：
    #   · 可排部数 = min(在库待排量, 今日剩余条数 ÷ 启用渠道数)；
    #   · 到顶 / 无货的分类不进方案，改为带可读原因进 `skipped` 回显；
    #   · 权重与每小时上限都用这个可排部数 —— 同一小时里额度只会流向真能出货的分类。
    # **刻意不做逐小时扣减**：方案是「每小时最多排几部」的上限，不是全天预算切片。
    # 逐小时扣减会把下午之后的小时写成 0，而用户的排期本来就与入库交替进行
    # （排完一批又入库），那样只会造成「下午起再也排不出东西」的假象。
    cycle_day_max = max(0, int(cycle.get("category_day_max") or 0))
    day_cap = max(0, int(cycle.get("day_max") or 0))
    # 一部作品对每个启用的渠道各建一条任务 → 条数折部数要除以渠道数。
    # 单渠道部署时除数为 1，与旧行为完全一致。
    channels_n = max(1, int(db.scalar(
        select(func.count(Channel.id)).where(Channel.enabled.is_(True))) or 1))
    today_counts = _category_today_counts(db)
    room_works: dict[str, int] = {}
    skipped: list[dict[str, str]] = []
    for c in cats:
        key = c.category_key
        stock = int(backlog.get(key) or 0)
        if stock <= 0:
            skipped.append({"name": _category_display_name(key),
                            "reason": "当前没有在库待排作品"})
            continue
        if cycle_day_max:
            left_tasks = max(0, cycle_day_max - int(today_counts.get(key, 0)))
            left_works = left_tasks // channels_n
            if left_works <= 0:
                skipped.append({"name": _category_display_name(key),
                                "reason": "今日已达单分类每日上限"})
                continue
            stock = min(stock, left_works)
        room_works[key] = stock
    if not room_works:
        at_cap = sum(1 for s in skipped if "上限" in s["reason"])
        no_stock = len(skipped) - at_cap
        return {"ok": False,
                "reason": (f"今天所有分类都排不出作品了：{at_cap} 个已到当日上限、"
                           f"{no_stock} 个当前没有在库作品。把「单分类每日上限」调大，"
                           "或者明天再点一次。"),
                "skipped": skipped}

    # 全局每日总上限（条）也折成部数：已经用满就没必要再生成方案（调度侧
    # `_cycle_quota_gate` 本来就会整轮拒绝），如实拒绝比给一份假的分配更有用。
    if day_cap:
        now_sh = datetime.now(shanghai_tz())
        day_start = datetime(now_sh.year, now_sh.month, now_sh.day,
                             tzinfo=shanghai_tz()).astimezone(timezone.utc)
        day_end = day_start + timedelta(days=1)
        used_tasks = int(db.scalar(select(func.count(SCPublishTask.id)).where(
            SCPublishTask.status.notin_(VOID_TASK_STATUSES),
            SCPublishTask.created_at >= day_start,
            SCPublishTask.created_at < day_end)) or 0)
        if (day_cap - used_tasks) // channels_n <= 0:
            return {"ok": False,
                    "reason": ("今天的推送总上限已经用完了，再分配也排不出去。"
                               "明天再点，或者把「单日推送总上限」调大。"),
                    "skipped": skipped}

    items: dict[str, dict[str, int]] = {}
    borrowed: list[int] = []
    # v2.1.53：补位只算「当前小时 → 当天 24 点」。已经过去的小时补了也没意义，
    # 而且是用户拍板的口径（「只补当前和未来到 24 点的区间」）。
    now_hour = datetime.now(shanghai_tz()).hour
    borrow_on = _borrow_enabled(db)
    for h in range(24):
        cands = [c for c in cats
                 if c.category_key in room_works and h in open_hours.get(c.category_key, [])]
        if not cands:
            # 空档补位：本小时窗口内的分类全都没货 / 全到上限 → 从全库有货的分类里
            # 按「今天真能推的部数」分一份配额补上，记进 `borrowed_hours`。
            # 调度侧据此只跳过「分类自己的时段窗口」这一条，每小时/每日上限照旧生效。
            if (borrow_on and h >= now_hour
                    and _hour_open_for_borrow(h, quiet, win)):
                _bq = _borrow_quota(room_works, cap, cycle_hour_max)
                if _bq:
                    items[str(h)] = _bq
                    borrowed.append(h)
            continue
        # 权重 = 今天真能推的部数（不是原始库存）：临近当日上限的分类权重自然变小，
        # 额度会流向还有余量的分类。
        weights = {c.category_key: max(1, int(room_works[c.category_key])) for c in cands}
        quotas = _largest_remainder(weights, cap)
        # 名额夹到「这一小时真能排几部」（可排部数 与 单分类每小时上限取小），
        # 削下来的按剩余余量回流给还有货的分类。
        room = {c.category_key: int(room_works[c.category_key]) for c in cands}
        if cycle_hour_max:
            room = {k: min(v, cycle_hour_max) for k, v in room.items()}
        quotas = _fit_quota_to_backlog(quotas, room)
        if quotas:
            items[str(h)] = quotas

    if not items:
        return {"ok": False,
                "reason": ("按当前分类时段与静默保护，全天没有任何可排期的小时——"
                           "先检查分类时间排期与主推送窗口。"),
                "skipped": skipped}

    now_sh = datetime.now(shanghai_tz())
    plan = {
        "generated_at": now_sh.strftime("%Y-%m-%d %H:%M"),
        "base_cap": int(cap),
        "cap_source": cap_source,
        "category_limit": category_limit,
        "categories": len(cats),
        "hours": len(items),
        "slots_total": sum(sum(v.values()) for v in items.values()),
        # v2.1.47：把「生成这一刻的在库作品数」留成快照。
        # 页面据此对比当前库存，判断方案是不是已经跟不上入库进度
        # （用户原话：「我们的排期是排期完成后又入库的」）。
        "backlog_snapshot": {k: int(v) for k, v in backlog.items() if int(v) > 0},
        # v2.1.49：今天每个分类**真能推**的部数（在库待排量 与 当日剩余额度取小）。
        # 页面用它解释「为什么这个分类只分到这么多」，不参与调度。
        "room_works": {k: int(v) for k, v in room_works.items() if int(v) > 0},
        # 排不上的分类 + 可读原因（库存空 / 今日已到上限）。
        "skipped": skipped,
        # v2.1.54：这次调度顺手改了哪些分类时段（落库，「刷新页面也看得到」）。
        "retimed": retimed,
        # v2.1.53：这几个小时是「窗口内分类都没货」的空档，由库存最多的分类补位。
        # 调度侧只据此跳过「分类自己的时段窗口」，其余上限一律照旧。
        "borrowed_hours": list(borrowed),
        "items": items,
    }
    cfg[EVEN_PLAN_KEY] = plan
    version.config_json = _jdump(cfg)
    db.commit()

    # ---- 回显：全部转成业务语言（分类名 / 小时 / 部数），不外露后端键形态 ----
    per_cat: dict[str, dict[str, Any]] = {}
    for h in sorted(items, key=lambda x: int(x)):
        for key, n in items[h].items():
            row = per_cat.setdefault(key, {"name": _category_display_name(key),
                                           "hours": 0, "works": 0})
            row["hours"] += 1
            row["works"] += int(n)
    detail = []
    for key, row in per_cat.items():
        detail.append({
            "name": row["name"],
            "backlog": int(backlog.get(key) or 0),
            "hours": int(row["hours"]),
            "per_hour": round(row["works"] / row["hours"], 2) if row["hours"] else 0,
            "works": int(row["works"]),
        })
    detail.sort(key=lambda x: (-x["works"], x["name"]))
    return {
        "ok": True,
        "generated_at": plan["generated_at"],
        "base_cap": int(cap),
        "cap_source": cap_source,
        "categories": len(cats),
        "hours": len(items),
        "slots_total": plan["slots_total"],
        # 回显只用中文分类名，**绝不带 `genre:` 这类内部键**（页面红线）。
        "capacity": sorted(
            [{"name": _category_display_name(k), "works": int(v)}
             for k, v in room_works.items()],
            key=lambda x: (-x["works"], x["name"])),
        "detail": detail,
        "skipped": skipped,
        # v2.1.53：补位时段（回显用）。页面据此提示「哪几个小时是库存最多的分类补位」。
        "borrowed_hours": list(borrowed),
        "borrow_enabled": bool(borrow_on),
        # v2.1.54：这次调度顺手改了哪些分类的时段（不是只改数量）。
        # 前端要如实列出来，否则用户会以为「点了一键调度，分类时段没动」。
        "retimed": retimed,
    }


def clear_even_plan(db: Session) -> dict[str, Any]:
    """清空均匀排期方案 → 调度立刻回到「按分类优先级顺序吃满额度」的原行为。"""
    version = _active_version(db)
    if version is None:
        return {"ok": False, "reason": "当前没有活动中的计划版本。"}
    cfg = _jload(version.config_json, {})
    if EVEN_PLAN_KEY in cfg:
        cfg.pop(EVEN_PLAN_KEY, None)
        version.config_json = _jdump(cfg)
        db.commit()
    return {"ok": True}


@router.get("/even-plan")
def api_even_plan(db: Session = Depends(get_db)):
    """当前均匀排期方案（页面回显用）；未生成 → `active=false` 并说明怎么开。

    v2.1.47 追加「库存漂移」四个数字，回答用户那句「我们的排期是排期完成后又入库的」：
      · `snapshot_works`：生成方案那一刻的在库作品数（方案里存的快照）；
      · `current_works`：现在的在库作品数；
      · `drift`：差额（正数＝排期后又入库了，负数＝库变小了）；
      · `stale`：差额不为 0 就为真 —— 方案的名额是按旧库存分的，已经跟不上现状。
    只如实报差额，不猜「差多少部才该重算」，也不自动改方案（改不改由用户点按钮决定）。
    """
    plan = even_plan_of(db)
    if not plan:
        return {"active": False, "detail": [],
                "hint": "还没生成均匀排期。点「一键 AI 调度」按当前分类作品量重新分配每小时的排期数量。"}
    items = plan.get("items") or {}
    per_cat: dict[str, dict[str, Any]] = {}
    for h in items:
        for key, n in (items[h] or {}).items():
            row = per_cat.setdefault(key, {"hours": 0, "works": 0})
            row["hours"] += 1
            row["works"] += int(n)
    # v2.1.53：补上 `backlog`（在库待排作品数）。此前这里漏写这个字段，前端只能写
    # 「backlog != null ? 值 : '—'」→ 整列永远显示「—」，用户看到的是「方案生成了
    # 却一部待排作品都没有」的假象（真机 2026-11-26：那一列全是「—」）。
    # 口径与 `build_even_plan` 的 detail 一致：取方案生成那一刻的库存快照。
    _snap = plan.get("backlog_snapshot") or {}
    detail = [{"name": _category_display_name(k), "hours": v["hours"],
               "works": v["works"],
               "per_hour": round(v["works"] / v["hours"], 2) if v["hours"] else 0,
               "backlog": int(_snap.get(k) or 0)}
              for k, v in per_cat.items()]
    detail.sort(key=lambda x: (-x["works"], x["name"]))
    # v2.1.47：库存漂移（生成时 vs 现在）。取不到当前库存时如实报 0 差额并标不可比。
    snap = plan.get("backlog_snapshot") or {}
    snapshot_works = sum(int(v or 0) for v in snap.values())
    try:
        current_works = sum(_category_backlog(db, _live_work_keys(db)).values())
    except Exception:
        db.rollback()
        current_works = -1
    drift = (current_works - snapshot_works) if current_works >= 0 else 0
    return {
        "active": True,
        "generated_at": plan.get("generated_at") or "",
        "base_cap": int(plan.get("base_cap") or 0),
        "cap_source": plan.get("cap_source") or "",
        "categories": int(plan.get("categories") or 0),
        "hours": int(plan.get("hours") or 0),
        "slots_total": int(plan.get("slots_total") or 0),
        # v2.1.49：今天每类真能推的部数（中文名）+ 「排不上的分类 + 原因」。
        # 两者都只用于解释「为什么方案里没有它 / 只分到这么多」，不参与调度。
        "capacity": [{"name": _category_display_name(k), "works": int(v)}
                     for k, v in (plan.get("room_works") or {}).items()],
        "skipped": plan.get("skipped") or [],
        "snapshot_works": int(snapshot_works),
        "current_works": int(current_works),
        "drift": int(drift),
        "stale": bool(current_works >= 0 and drift != 0),
        # v2.1.53：哪几个小时是「窗口内分类都没货」的空档、由库存最多的分类补位。
        # 前端据此在这几个小时上加提示，别让人以为分类时段配置失效了。
        "borrowed_hours": sorted(_borrowed_hours(plan)),
        # v2.1.54：上次调度改了哪些分类时段（落库的快照，刷新页面也还在）
        "retimed": plan.get("retimed") or {"moved": 0, "details": [], "truncated": 0},
        "detail": detail,
    }


@router.post("/even-plan/apply")
def api_apply_even_plan(db: Session = Depends(get_db)):
    """一键 AI 调度：按当前分类作品量重新分配每小时的排期数量。"""
    return build_even_plan(db)


@router.post("/even-plan/clear")
def api_clear_even_plan(db: Session = Depends(get_db)):
    """撤销均匀排期，回到原有排期分配方式。"""
    return clear_even_plan(db)


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


def _valid_legacy_push_pairs(db: Session) -> set[tuple[str, str]]:
    """旧推送队列中**仍然生效**的推送记录 → `{(work_key, chat_id)}`。

    v2.1.16：本函数取代 v2.1.15 的 `_pushed_pairs_from_audit`。
    旧版把历史 `push_sent` 一律当作「已推送过」，是一座**永久闸**——用户取消分享
    链接后那条记录也无法作废，作品从此永远不能重推，与用户口径「取消分享链接 =
    该推送记录判为无效」直接冲突。

    现在改为看这条队列行**自身是否还有效**（审计只用来还原「资源 ↔ 渠道」关系）：
      ① `push_queue` 行未被归档（clean_state != 'cleaned'）；
      ② 该资源的分享**仍是 active 且确实还有链接**；
      ③ 资源与渠道都还存在。
    任一不满足 → 这条历史推送已作废，名额回收、允许重推。

    v2.1.17 判据收紧（**勿退回 `!= 'cancelled'`**）
    --------------------------------------------
    之前只排除 `cancelled`，于是 `not_created`（从没建过 / B 库清空后归零 /
    被清理过）与 `failed`（建链失败）都被当成「推送仍生效」。线上后果：
    资源库里明明有这部作品、分享也早就没了，名额却被这条历史记录**永久占着**，
    作品永远排不进队列 —— 用户看到的就是「资源库有影视，推送任务一个都没有」。
    真正决定「这条推送还有效吗」的是**分享链接还在不在**：状态必须 `active`
    且 `share_url` 非空。清空 B 库会把 share_status 归一为 not_created，
    于是这些历史记录自动作废，作品重新可排。
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
    res_map = {r.id: r for r in db.scalars(
        select(Resource).where(Resource.id.in_(res_ids))).all()}
    ch_map = {c.id: c for c in db.scalars(
        select(Channel).where(Channel.id.in_(ch_ids))).all()}
    # 队列行的归档态是「这条推送记录已作废」的直接证据；归档行在默认查询里不可见，
    # 这里必须显式放开才能读到，否则「已归档」会被误当成「仍有效」。
    from .models import include_archived
    clean_map: dict[tuple[int, int], str] = {}
    with include_archived():
        q_rows = db.scalars(select(QueueItem).where(
            QueueItem.resource_id.in_(res_ids))).all()
    for q in q_rows:
        clean_map[(int(q.resource_id), int(q.channel_id))] = str(q.clean_state or "")
    out: set[tuple[str, str]] = set()
    for rid, cid in parsed:
        res, ch = res_map.get(rid), ch_map.get(cid)
        if res is None or ch is None:
            continue
        if clean_map.get((rid, cid)) == "cleaned":
            continue
        # v2.1.17：只有「分享仍 active 且确实还有链接」才算这条历史推送还在生效。
        # 只排除 cancelled 是不够的——not_created / failed / 空链接同样意味着
        # 这条推送已经没有可用的分享，占着名额就等于把作品永久锁死。
        if str(getattr(res, "share_status", "") or "") != "active":
            continue
        if not str(getattr(res, "share_url", "") or "").strip():
            continue
        out.add((utils.work_group_key(res), str(ch.chat_id or "")))
    return out


def _is_future(now: datetime, value: datetime | None) -> bool:
    """`value` 是否晚于 `now`（容忍 SQLite 取出的 naive datetime）。

    SQLite 的 `DateTime(timezone=True)` 实际只存不带时区的字符串，读回来可能是
    naive——直接与 `utcnow()` 的 aware 值比较会抛 TypeError。这里统一降级成 naive
    比较，宁可少判一次「已到点」也不能让整轮计划崩掉。
    """
    if value is None:
        return False
    try:
        return value > now
    except TypeError:
        try:
            return value.replace(tzinfo=None) > now.replace(tzinfo=None)
        except Exception:
            return False


class _SlotIndex:
    """推送名额索引（v2.1.16）：把「已首推」从永久历史改成可回收状态。

    判定单位是 `(作品, 渠道)`——一个作品在一个渠道的名额相互独立。
    整轮只载入一次，避免 plan_engine 里「逐作品 × 逐渠道」反复查表
    （真机 486 部 × N 渠道、每 5 秒一轮，逐条查会把 SQLite 拖死）。

    四态（见模块顶部常量注释）：
      active  在途任务占用名额 → 不重复建任务；
      valid   已推成功且链接仍有效、作品仍在库 → 不重复首推；
      invalid 历史行全部作废 → 名额可回收，复活旧行重排；
      none    从未建过 → 新建。
    """

    def __init__(self, db: Session, *, live_keys: set[str] | None = None,
                 legacy_pairs: set[tuple[str, str]] | None = None) -> None:
        self.live_keys = live_keys
        self._now = utcnow()
        self.legacy_pairs = (
            legacy_pairs if legacy_pairs is not None
            else _valid_legacy_push_pairs(db))
        rows = db.execute(
            select(SCPushListItem, SCPublishTask)
            .outerjoin(SCPublishTask, SCPublishTask.list_item_id == SCPushListItem.id)
            .where(SCPushListItem.source_kind == "first")
        ).all()
        self._items: dict[tuple[str, str], dict[int, SCPushListItem]] = {}
        self._states: dict[int, list[tuple[str, str, datetime | None]]] = {}
        for item, task in rows:
            key = (str(item.work_key or ""), str(item.chat_id or ""))
            self._items.setdefault(key, {})[int(item.id)] = item
            if task is not None:
                self._states.setdefault(int(item.id), []).append(
                    (str(task.status or ""), str(task.link_state or ""),
                     task.next_retry_at))

    def state(self, work_key: str, chat_id: str,
              dedup: str = "") -> tuple[str, SCPushListItem | None]:
        key = (str(work_key or ""), str(chat_id or ""))
        items = self._items.get(key) or {}
        statuses: list[str] = []
        link_ok = False
        cooling = False
        for iid in items:
            for status, link, retry in self._states.get(iid, ()):
                statuses.append(status)
                # 「已推成功」只有在分享链接仍有效时才算数——链接被取消/作废后，
                # 群里那条消息已经点不开了，这条记录按用户口径判为无效。
                if status == "published" and link not in INVALID_LINK_STATES:
                    link_ok = True
                # 「无效推送」刚失败过、退避还没到点 → 按占用处理，
                # 否则每 5 秒重建一次链接、在网盘里刷出一堆废分享。
                elif status == INVALID_TASK_STATUS and _is_future(self._now, retry):
                    cooling = True
        # ① 在途优先：还有任务在跑，绝不重复建（否则每轮都新插一条）。
        #    `failed` 也算占用：dispatch 的候选集包含 failed（会原地重试），
        #    如果这里不占位，plan_engine 会另建一条新任务 → 同一作品两条在跑。
        if cooling or any(s in ACTIVE_TASK_STATUS or s == "failed" for s in statuses):
            return SLOT_ACTIVE, None
        # ② 已推成功且链接有效、作品仍在库 → 名额仍被占用。
        if link_ok and (self.live_keys is None or key[0] in self.live_keys):
            return SLOT_VALID, None
        # ③ 旧推送队列里仍生效的记录同样占用名额。
        if key in self.legacy_pairs:
            return SLOT_VALID, None
        # ③b 老库遗留：清单项自身带 published、但还没有对应任务行（v2.1 之前的写法）。
        #     按项状态判为有效，避免把「早已推过」的又推一遍。
        #     项上没有任务、状态也不是 published（如 planned）→ 说明从未真正推送过，
        #     落到下面的 invalid 分支，名额回收。
        if items and not statuses and any(
                str(c.status or "") == "published" for c in items.values()):
            return SLOT_VALID, None
        # ④ 有历史行但无一有效 → 作废，名额可回收。
        if items:
            ordered = [items[i] for i in sorted(items)]
            reuse = next((c for c in ordered if c.dedup_key == dedup), ordered[0])
            return SLOT_INVALID, reuse
        return SLOT_NONE, None


def first_dedup_plan(db: Session, *, work_key: str, chat_id: str,
                     dedup: str,
                     pushed_pairs: set[tuple[str, str]] | None = None,
                     index: "_SlotIndex | None" = None,
                     live_keys: set[str] | None = None,
                     ) -> tuple[str, SCPushListItem | None]:
    """首推去重决策。返回 `(action, 可复用的旧清单项)`。

      - `"skip"`   ：名额仍被占用（有在途任务，或已推成功且链接仍有效）→ 不建任务；
      - `"revive"` ：历史记录已作废（取消 / 清理 / 链接作废 / 资源已移除）→ 复活旧行；
      - `"create"` ：从未建过 → 新建。

    v2.1.16：判据从「历史是否推过」改为「当前是否存在仍生效的推送记录」。
    用户口径：取消推送任务 / 取消分享链接 / 清理资源库 / 重建资源库，都必须让该
    作品回到初始状态、可重新排队，而不是被永久记成「已经首推过」。
    具体四态判定见 `_SlotIndex`。

    为什么 revive 而不是新插一行：`SCPushListItem.dedup_key` 是**唯一约束**，
    旧行还在，直接按同一 dedup 另插一条会撞唯一键、整轮计划失败。
    复活只更新清单项本身，历史任务行与执行记录一条不动。
    """
    if index is None:
        # 本项目 Session 是 autoflush=False：刚改的任务状态必须先落库，
        # 否则建索引时读到的仍是旧值（与 api_cleanup_tasks 处同一条坑）。
        db.flush()
        index = _SlotIndex(db, live_keys=live_keys, legacy_pairs=pushed_pairs)
    state, reuse = index.state(work_key, chat_id, dedup)
    if state in (SLOT_ACTIVE, SLOT_VALID):
        return "skip", None
    if state == SLOT_INVALID:
        return "revive", reuse
    return "create", None


def _instant_candidates(db: Session, mode: str,
                        limit: int = INSTANT_ROUND_LIMIT, *,
                        now_sh: datetime | None = None) -> list[tuple[str, int, str]]:
    """上新即时推送的候选作品：返回 `[(work_key, 最新快照 id, lane)]`（已排好先后）。

    lane = `new_release`（新上映，插队立即推）或 `new_library`（新入库，优先排队）。
    排序口径：
      · 新上映按**上映日期倒序**（越新的越先推）；
      · 新入库按**首次入库时间升序**（先入库先推 —— 「按资源库入库的顺序」）。

    `now_sh` 仅供测试注入固定时刻：v2.1.23 起「新入库」按**上海自然日**判定
    （当天入库才算），不注入就没法稳定复现「同一天内先入库的先推」这条顺序。

    v2.1.22 性能约束：本函数跑在 `_job_share_center` 的 5 秒周期里，所以
      ① 只取判定要用的窄列，不加载整行 `Resource` / `SCLibraryVersion` 实体；
      ② 最新快照只对**命中窗口的候选键**回查，不做全库 group by 后全量物化。
    """
    if mode == "off":
        return []
    now_sh = now_sh or datetime.now(shanghai_tz())
    today = now_sh.strftime("%Y-%m-%d")
    rel_cutoff = (now_sh - timedelta(days=NEW_RELEASE_WINDOW_DAYS)).strftime("%Y-%m-%d")

    ordered: list[tuple[str, str]] = []   # [(work_key, lane)]，最终先后顺序
    seen: set[str] = set()

    # ① 新上映：与 `_work_push_tags` **同一判据**（`_new_release_state`）。
    # v2.1.23 起不再只看「上映 30 天窗口」——仍在更新的剧集首播日可能很早，
    # 但它确实属于新上映；刚完结的剧集则按「最后一集播出日次日 0 点」过期。
    newest: dict[str, str] = {}
    rows = db.execute(select(
        Resource.file_id, Resource.path_hint, Resource.share_dir_name,
        Resource.media_title, Resource.tmdb_id, Resource.media_type,
        Resource.release_date, Resource.last_air_date, Resource.finish_status,
    ).where(or_(
        # 电影 / 剧集状态未知：上映 30 天窗口内且已上映
        and_(Resource.release_date.isnot(None), Resource.release_date != "",
             Resource.release_date >= rel_cutoff,
             Resource.release_date <= today),
        # 剧集仍在更新（v2.1.56：下面这条**故意不按 last_air_date 收紧** ——
        # 停更兜底只在 `_new_release_state` 里判一份；SQL 再写一遍阈值就成了
        # 两套判据，改一处忘一处会出现「标签说存量、插队说新上映」的分裂）
        Resource.finish_status == "returning",
        # 剧集刚完结：最后一集播出日 >= 今天（播出日次日 0 点才过期）
        and_(Resource.finish_status == "ended",
             Resource.last_air_date.isnot(None), Resource.last_air_date != "",
             Resource.last_air_date >= today),
    ))).all()
    for res in rows:
        key = utils.work_group_key(res)
        if not key:
            continue
        if not _new_release_state(
                release_date=str(res.release_date or ""),
                last_air_date=str(res.last_air_date or ""),
                finish_status=str(res.finish_status or ""),
                media_type=str(res.media_type or ""),
                now_sh=now_sh):
            continue
        # 排序键 = 该作品最近的一个「有意义的日期」：
        # 仍在更新的剧集用最后一集播出日（更能代表「新」），其余用上映/首播日。
        day = max(str(res.release_date or "")[:10], str(res.last_air_date or "")[:10])
        if key not in newest or day > newest[key]:
            newest[key] = day
    for key, _day in sorted(newest.items(), key=lambda kv: kv[1], reverse=True):
        if key in seen:
            continue
        seen.add(key)
        ordered.append((key, "new_release"))
        if len(ordered) >= limit:
            break

    # ② 新入库（仅 prefer_new）：按**首次入库时间**升序优先排队。
    # 「首次入库」取该作品最早一条快照的登记时间（与其他标签判定同源），
    # 所以老片昨天更新了新版本不会被误判成新入库。
    # v2.1.23：窗口收紧到「上海自然日的今天」——与标签口径完全一致。
    if mode == "prefer_new" and len(ordered) < limit:
        today_start_sh = datetime.now(shanghai_tz()).replace(
            hour=0, minute=0, second=0, microsecond=0)
        lib_cutoff = today_start_sh.astimezone(timezone.utc)
        fresh = db.execute(
            select(SCLibraryVersion.work_key,
                   func.min(SCLibraryVersion.created_at).label("entered"),
                   func.max(SCLibraryVersion.id).label("lv_id"))
            .group_by(SCLibraryVersion.work_key)
            .having(func.min(SCLibraryVersion.created_at) >= lib_cutoff)
            .order_by("entered", "lv_id")).all()
        for row in fresh:
            key = str(row.work_key or "")
            if not key or key in seen:
                continue
            seen.add(key)
            ordered.append((key, "new_library"))
            if len(ordered) >= limit:
                break

    # ③ 回查候选作品的**最新快照 id**（同一作品多版本时只看最新那条）。
    # 只对候选键查询，候选数受 limit 约束，不会随库存量线性膨胀。
    keys = [k for k, _lane in ordered]
    lv_of: dict[str, int] = {}
    if keys:
        for wk, lv_id in db.execute(
            select(SCLibraryVersion.work_key, func.max(SCLibraryVersion.id))
            .where(SCLibraryVersion.work_key.in_(keys))
            .group_by(SCLibraryVersion.work_key)).all():
            lv_of[str(wk or "")] = int(lv_id)
    return [(k, lv_of[k], lane) for k, lane in ordered if k in lv_of]


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
    # v2.1.16：名额索引整轮只建一次。包含「作品×渠道」的当前推送状态
    # （在途 / 已推且链接有效 / 已作废 / 从未建过），以及旧队列里仍生效的历史推送。
    # 逐作品 × 逐渠道 → 一次性载入，避免每 5 秒一轮把 SQLite 拖死。
    live_keys = _live_work_keys(db)
    slots = _SlotIndex(db, live_keys=live_keys)
    # 环形轮转：本轮已经处理过的作品不再重复处理（回绕时可能二次命中）。
    seen_works: set[str] = set()

    categories = list(db.scalars(
        select(SCCategory).where(SCCategory.enabled.is_(True))
        .order_by(SCCategory.priority.desc(), SCCategory.id)
    ).all())[:category_limit]
    # v2.1.53：均匀排期方案整轮只读一次。下面两处都要用：
    # ① `even_quota`（本小时各分类还能出几部）；② `borrowed_hours`（空档补位时段）。
    # 补位时段里方案是**跨分类时段窗口**指定的 —— 所以那几小时要跳过「单分类缩表」
    # 和「分类自己的窗口」两条判定，否则补了也排不出来（用户要的就是别空着）。
    sh_hour = datetime.now(shanghai_tz()).hour
    even_plan = even_plan_of(db, version)
    borrowed_hours = _borrowed_hours(even_plan)
    # v2.0.4：小时分类排期真正驱动调度——当前时段绑定了真实分类时，只为该分类生成任务。
    # 绑定 key 不在启用分类字典中（如分类被停用）时退回全局轮转，不产出空清单。
    # v2.1.53：补位时段不缩表 —— 绑定分类正是「没货才需要补位」的那个，缩了就没得补。
    hour_window = _current_hour_window(db, version)
    if hour_window and hour_window.category_key and sh_hour not in borrowed_hours:
        scoped = [c for c in categories if c.category_key == hour_window.category_key]
        if scoped:
            categories = scoped
    # v2.1.0：SCCategorySchedule 分类-时间排期真实生效——每分类按自己的窗口时段与
    # hourly/daily 上限过滤。此前整表只落库不被消费，前端配的排期不产生调度效果。
    # 某分类未启用排期行/配了窗口但当前小时不在窗口内 → 跳过该分类；上限 0 = 不限。
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
    # v2.1.46：均匀排期配额（没生成过 → None，下面所有判定原样走老路）。
    # 只影响「本小时这个分类还能再出几部」，不改任何时段配置。
    even_quota = even_hour_quota(even_plan, sh_hour)
    even_used: dict[str, int] = {}
    if even_quota is not None:
        hour_start = datetime.now(shanghai_tz()).replace(
            minute=0, second=0, microsecond=0)
        even_used = _category_hour_counts(db, hour_start)

    # v2.1.47：**两个计数器，单位不同，别混用**。
    #   · `category_task_counts` 数「条」——任务行，供 `cycle_hour_max`
    #     （单分类每小时上限，页面按条配）与日上限比对；
    #   · `category_work_counts` 数「部」——作品，供均匀排期配额比对。
    # 同一部作品推频道 + 群是两条任务但只算一部；混用会让「配额 2 部」在双渠道
    # 部署下只排得出 1 部，实排永远只有配额的一半。
    category_task_counts: dict[str, int] = {}
    category_work_counts: dict[str, int] = {}

    def _sched_allows(key: str, per_category_count: int) -> bool:
        # v2.1.19：判定内核外移到 `_sched_allows_core`，与「今日排期」预测共用同一份
        # 判据——预测和实跑算出差，就等于骗用户。
        # `today_count` 保持惰性：只有配了日上限时才查库（原闭包同样是短路才算）。
        # v2.1.46：均匀排期配额再拦一道——「本小时库里已建 + 本轮已建」到顶就换下一个
        # 分类，把剩余名额让出去。没有方案时 `_even_allows` 恒为 True，零影响。
        # v2.1.47：这里比的是**部**（`category_work_counts`），不是条。
        if not _even_allows(even_quota, key,
                            int(even_used.get(key, 0))
                            + int(category_work_counts.get(key, 0))):
            return False
        today = _category_today_count(db, key) if cycle_day_max else 0
        return _sched_allows_core(sched_map.get(key), hour=sh_hour, weekday=iso_weekday,
                                  per_category_count=per_category_count,
                                  cycle_hour_max=cycle_hour_max,
                                  cycle_day_max=cycle_day_max,
                                  today_count=today,
                                  # v2.1.53：补位时段只跳过「分类自己的时段窗口」，
                                  # 每小时 / 每日上限仍然全部生效。
                                  check_window=sh_hour not in borrowed_hours)


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

    # v2.1.22：上新即时推送通道——在存量轮转**之前**插队生成任务。
    # 关闭（off）时整段跳过：资源库内影视全部当存量排期持续轮转。
    # 插队任务**不看分类时段窗口与分类上限**（那是排期的约束），但仍受
    # `hour_max` 单轮上限、每小时匀速额度（dispatch）与 TG 节奏约束。
    instant_mode = instant_push_mode(db)
    instant_lane_count = 0
    if instant_mode != "off":
        for work_key, lv_id, lane in _instant_candidates(db, instant_mode):
            if hour_max and task_count >= hour_max:
                break
            if instant_lane_count >= INSTANT_ROUND_LIMIT:
                break
            if work_key in seen_works or work_key not in live_keys:
                continue
            lv = db.get(SCLibraryVersion, lv_id)
            if lv is None:
                continue
            ownership = db.scalar(select(SCWorkCategory).where(
                SCWorkCategory.work_key == work_key))
            category_key = str(ownership.category_key if ownership else "") or "genre:unknown"
            # 新上映 = 插队（立即推）；新入库 = 排到存量之前（按入库顺序）
            priority = (INSTANT_TASK_PRIORITY if lane == "new_release"
                        else NEW_ENTRY_TASK_PRIORITY)
            built_any = False
            for channel in channels:
                target_type = "group" if str(channel.chat_id or "").startswith("-") else "channel"
                chat_id = str(channel.chat_id or "")
                dedup = f"{work_key}:{channel.id}:first"[:255]
                action, reuse = first_dedup_plan(
                    db, work_key=work_key, chat_id=chat_id, dedup=dedup, index=slots)
                if action == "skip":
                    continue
                if action == "revive" and reuse is not None:
                    item = reuse
                    item.list_id = push_list.id
                    item.library_version_id = lv.id
                    item.category_key = category_key
                    item.target_type = target_type
                    item.bot_id = channel.bot_id or 0
                    item.status = "planned"
                    db.flush()
                else:
                    item = SCPushListItem(
                        list_id=push_list.id, work_key=work_key,
                        library_version_id=lv.id, category_key=category_key,
                        target_type=target_type, bot_id=channel.bot_id or 0,
                        chat_id=chat_id, dedup_key=dedup,
                        source_kind="first", status="planned",
                    )
                    db.add(item)
                    db.flush()
                item_count += 1
                db.add(SCPublishTask(
                    list_item_id=item.id, work_key=work_key,
                    plan_version_id=version.id, kind="first", status="planned",
                    priority=priority, scheduled_at=utcnow()))
                db.flush()
                task_count += 1
                built_any = True
            if built_any:
                seen_works.add(work_key)
                instant_lane_count += 1
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
        # v2.1.16：候选 = 「按分类顺序排列的有序队列，依次生成」。
        #
        # 队列顺序：分类之间按 category.priority desc → category.id（外层循环），
        # 分类内部按 SCLibraryVersion.id 升序（= 作品入库/首次成为快照的先后）。
        # 每轮从游标位置往后续取 work_limit 部，建满就停——这就是「按发布规则限制的
        # 日 + 小时队列依次生成」：配额的夹逼在上面 `_sched_allows`（分类日/小时上限）
        # 与 `hour_max`（单轮上限）里，队列本身只负责**顺序**。
        #
        # 与「环形轮转」的区别（选型结论）：
        #   环形轮转每轮从「上次停的地方」开始，顺序由游标位置决定 —— 取消/清理释放
        #   出来的作品要等转完一圈才补得上，用户看到的顺序也说不清（有时推第 300 部、
        #   有时推第 20 部）。
        #   有序队列的游标只表示「队头已经消化到哪儿」，**不是水位线**：任何让作品失效
        #   的动作（取消任务 / 取消链接 / 清理任务 / 清理资源库）都会把它**回退**到该
        #   作品之前（见 `rewind_cursor_for_work`），于是它立刻回到队头优先补排 ——
        #   这才是用户要的「归到初始状态」。
        #
        # 只取**每部作品的最新快照**：同一作品有多个版本时，旧版本不该各占一个候选位。
        latest_ids = (select(func.max(SCLibraryVersion.id))
                      .group_by(SCLibraryVersion.work_key))
        # v2.1.35（真机：「全天排期很空」）：**在库过滤必须放在 SQL 层**。
        #
        # 根因：`SCLibraryVersion` / `SCWorkCategory` 不会随资源库清理而删——作品被
        # 移出资源库后，这两张表的记录还在，成了「死快照」。候选原先只按 id 升序
        # 无过滤地取 `work_limit` 条（这个数由**发布规则**配，缺省才回落 50），
        # 死快照照样占名额；而循环里的
        # `work_key not in live_keys` 判断在**这道 SQL 之后**才执行，于是只要前
        # 50 条恰好都是死快照，本轮就一个任务都建不出来，`task_count` 一直是 0，
        # `work_limit` 永远轮不到后面的活作品——用户看到的就是「排期很空」。
        #
        # 把过滤提到 SQL 层，`work_limit` 才真正落在在库作品上。`live_keys` 仍是
        # `_live_work_keys` 实时扫 Resource 派生的唯一事实依据，不新增记录状态字段，
        # 也**不碰游标回退**（红线：游标永不回退，`rewind_cursor_for_work` 原样保留）。
        # 参数规模：live_keys 千级，SQLite 3.50 的绑定上限 32766，余量充足。
        base_q = (select(SCLibraryVersion)
                  .join(SCWorkCategory, SCWorkCategory.work_key == SCLibraryVersion.work_key)
                  .where(SCWorkCategory.category_key == category.category_key)
                  .where(SCLibraryVersion.id.in_(latest_ids))
                  .where(SCLibraryVersion.work_key.in_(live_keys)))
        versions = list(db.scalars(
            base_q.where(SCLibraryVersion.id > cursor.last_library_version_id)
            .order_by(SCLibraryVersion.id).limit(work_limit)).all())
        from_head = False
        if not versions:
            # 队尾已消化完 → 从队头补扫一批做**自愈**。
            # 兜住不经过 rewind 的失效（外部删库、库被整体搬迁、旧版本遗留数据）：
            # 这批里全是「已完成」时不会建任何任务，游标原地不动，不产生抖动。
            versions = list(db.scalars(
                base_q.order_by(SCLibraryVersion.id).limit(work_limit)).all())
            from_head = True
        last_pos = cursor.last_library_version_id
        built_in_batch = 0
        for lv in versions:
            # v2.1.22：小时上限必须在**作品级**再拦一刀。此前只在渠道循环末尾
            # `break`，于是「一部作品建完就退出渠道循环、但外层作品循环继续」——
            # hour_max=1 时实际仍能建出 N 部，周期计划的小时上限形同虚设
            # （插队通道加入后两档叠加，超出更明显）。
            if hour_max and task_count >= hour_max:
                break
            # v2.1.46：均匀排期配额必须也在**作品级**拦一刀。
            # `_sched_allows` 是每轮每分类只判一次的，判的时候这个分类的计数还是 0，
            # 光靠它挡不住「一个分类在本轮把整点额度吃光」——那正是本版要修的病根。
            # 用「本小时库里已建 + 本轮已建」跟配额比，到顶就收手，把名额让给后面分类。
            # v2.1.47：两边都换成**部**（`category_work_counts`）。原来用任务条数，
            # 双渠道部署时一部作品吃两个计数，「配额 3 部」实际只排得出 1–2 部。
            if not _even_allows(even_quota, category.category_key,
                                int(even_used.get(category.category_key, 0))
                                + int(category_work_counts.get(category.category_key, 0))):
                break
            work_key = str(lv.work_key or "")
            # 自愈补扫时同一作品可能二次命中；本轮已处理过就不再重复。
            if work_key in seen_works:
                continue
            seen_works.add(work_key)
            if not from_head:
                last_pos = lv.id
            # v2.1.16：作品已从资源库移除（重建 / 手动清理）→ 不建队列。
            # 队列的事实依据是「资源库里当前有这部作品」，而不是历史快照还在不在。
            if work_key not in live_keys:
                continue
            built_before_work = built_in_batch
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
                dedup = f"{work_key}:{channel.id}:first"[:255]
                # v2.1.16：名额判定改为「当前是否仍有效」四态（见 _SlotIndex）。
                # 已推且链接有效 / 在途 → 跳过；取消·清理·链接作废 → 复活重排。
                action, reuse = first_dedup_plan(
                    db, work_key=work_key, chat_id=chat_id, dedup=dedup, index=slots)
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
                        list_id=push_list.id, work_key=work_key,
                        library_version_id=lv.id, category_key=category.category_key,
                        target_type=target_type, bot_id=channel.bot_id or 0,
                        chat_id=str(channel.chat_id or ""), dedup_key=dedup,
                        source_kind="first", status="planned",
                    )
                    db.add(item)
                    db.flush()
                item_count += 1
                task = SCPublishTask(
                    list_item_id=item.id, work_key=work_key,
                    plan_version_id=version.id, kind="first", status="planned",
                    priority=category.priority, scheduled_at=utcnow(),
                )
                db.add(task)
                db.flush()
                task_count += 1
                built_in_batch += 1
                cursor.last_success_at = utcnow()
                category_task_counts[category.category_key] = category_task_counts.get(
                    category.category_key, 0) + 1
            # v2.1.47：作品级计数（部）——一部作品建完它的全部渠道才算 1 部。
            # 均匀排期配额比的是这个数；`category_task_counts`（条）继续给
            # 「单分类每小时上限」用，两者单位不同，不许互相顶替。
            if built_in_batch > built_before_work:
                category_work_counts[category.category_key] = category_work_counts.get(
                    category.category_key, 0) + 1
            if hour_max and task_count >= hour_max:
                break
        # 游标 = 「队头已消化到哪儿」。正常推进时前移；自愈补扫（from_head）只有在
        # 真的建出任务时才前移——全是「已完成」时原地不动，避免每轮空转抖动。
        # 注意它不是水位线：任何让作品失效的动作都会把它回退（rewind_cursor_for_work）。
        if last_pos != cursor.last_library_version_id:
            cursor.last_library_version_id = last_pos
            cursor.last_work_key = str(versions[-1].work_key if versions else "")
            cursor.status = "idle"
        elif from_head and built_in_batch:
            cursor.last_library_version_id = max(int(lv.id) for lv in versions)
            cursor.last_work_key = str(versions[-1].work_key if versions else "")
            cursor.status = "idle"
        if hour_max and task_count >= hour_max:
            break
    db.commit()
    return {"lists": 1, "items": item_count, "tasks": task_count}


# ---------------------------------------------------------------------------
# v2.1.21 手动强推（force）改走 v2 任务表
#
# 真机问题（2026-11-24）：「手动推送的鹿鼎记没进推送任务、推送历史」。
# 推送本身是成功的（push_queue 13/14 已 sent、push_sent 审计 6191/6192 都在），
# 断在**记录写错了表**：`main.publish_resource` 调
# `share_center.shadow_register(kind="force")` 写旧链的 `share_tasks`，而
# 「推送任务 / 推送历史 / 推送队列」三个卡片读的是 `sc_publish_tasks` ——
# 双写期两张表没打通，手动推送的记录永远不出现在这三个模块里
# （自动推送走 v2，所以能在历史看到）。
#
# 现在 force 也写 `sc_push_list_items` + `sc_publish_tasks`，交给 v2 的
# dispatch / publisher 正常接管：三个模块与自动推送完全同源，链接状态、
# 失败重试、执行记录一并可用。旧表 `share_tasks` 只保留历史行，不再新增。
# ---------------------------------------------------------------------------
FORCE_TASK_KIND = "force"
# 手动强推优先级：dispatch 按 priority desc 取任务，手动动作必须排在最前。
FORCE_TASK_PRIORITY = 1000
# 追更（内容更新）任务：只记账、不建链——真实投递由更新事件那条队列项完成。
UPDATE_TASK_KIND = "update"


def _force_push_list(db: Session, version_id: int) -> SCPushList:
    """手动强推用的清单：按自然日复用一条，避免每次手动推送都新建空清单。"""
    day_key = datetime.now(shanghai_tz()).strftime("%Y%m%d")
    list_key = f"force:{day_key}"[:128]
    row = db.scalar(select(SCPushList).where(SCPushList.list_key == list_key))
    if row is None:
        row = SCPushList(list_key=list_key, name=f"手动推送 · {day_key}",
                         status="active", source=FORCE_TASK_KIND,
                         plan_version_id=version_id)
        db.add(row)
        db.flush()
    else:
        row.status = "active"
    return row


def _day_list(db: Session, prefix: str, name: str, version_id: int) -> SCPushList:
    """按自然日复用一条清单（同一天多轮追加，跨天才新建）。

    v2.1.22 从 `_force_push_list` 抽出：手动推送与追更都要「每天一条清单」，
    否则每次动作都新建空清单，列表页会被空清单淹掉。
    """
    day_key = datetime.now(shanghai_tz()).strftime("%Y%m%d")
    list_key = f"{prefix}:{day_key}"[:128]
    row = db.scalar(select(SCPushList).where(SCPushList.list_key == list_key))
    if row is None:
        row = SCPushList(list_key=list_key, name=f"{name} · {day_key}",
                         status="active", source=prefix, plan_version_id=version_id)
        db.add(row)
        db.flush()
    else:
        row.status = "active"
    return row


def register_update_task(db: Session, resource: Resource, *, channel_id: int,
                         event_id: int = 0) -> SCPublishTask | None:
    """把一次「内容更新（追更）」登记成 v2 任务，三个卡片才看得见（v2.1.22）。

    用户口径：「无论存量 / 新入库 / 新上映，有更新可以重复推送（不重复建分享链接），
    而且都需要在推送队列、今日推送排期、推送任务列表体现」。

    与手动推送（force）的区别：
      · 追更**复用已有分享链接**（share_url 直接抄过来、link_state=active），
        绝不重新建链；
      · 追更**不占首推名额**（`source_kind="update"`，`_SlotIndex` 只认 first），
        同一作品可以随更新一推再推；
      · 状态直接落 `submitted`：真实投递由更新事件里那条已经被置回 pending 的
        队列项完成（见 main 的更新事件消费），这里只负责把这次追更记进 v2 账本。
        **不落 planned**，否则 dispatch 会再发一次 → 双推。

    dedup 用 `update:<work_key>:<渠道>:<事件号>`，一次更新事件一条；拿不到事件号
    时退化为按渠道一条，仍然不会与首推/手动推送的 dedup 相撞。
    """
    work_key = utils.work_group_key(resource)
    lv = db.scalar(select(SCLibraryVersion).where(SCLibraryVersion.work_key == work_key)
                   .order_by(SCLibraryVersion.id.desc()))
    channel = db.get(Channel, int(channel_id or 0))
    if lv is None or channel is None:
        return None
    dedup = f"update:{work_key}:{int(channel_id)}:{int(event_id or 0)}"[:255]
    if db.scalar(select(SCPushListItem).where(SCPushListItem.dedup_key == dedup)):
        return None  # 同一事件重复消费：幂等，不重复记账
    version = _active_version(db)
    version_id = int(version.id) if version else 0
    push_list = _day_list(db, "update", "追更", version_id)
    ownership = db.scalar(select(SCWorkCategory).where(SCWorkCategory.work_key == work_key))
    category_key = str(ownership.category_key if ownership else "") or "genre:unknown"
    chat_id = str(channel.chat_id or "")
    # 复用该作品当前仍有效的分享链接（不重复建链）
    shared = db.scalar(select(SCPublishTask).where(
        SCPublishTask.work_key == work_key,
        SCPublishTask.share_url.isnot(None), SCPublishTask.share_url != "",
        SCPublishTask.link_state.notin_(INVALID_LINK_STATES),
    ).order_by(SCPublishTask.id.desc()))
    share_url = (str(getattr(shared, "share_url", "") or "")
                 or str(getattr(resource, "share_url", "") or ""))
    item = SCPushListItem(
        list_id=push_list.id, work_key=work_key, library_version_id=lv.id,
        category_key=category_key,
        target_type="group" if chat_id.startswith("-") else "channel",
        bot_id=channel.bot_id or 0, chat_id=chat_id, dedup_key=dedup,
        source_kind="update", status="planned",
    )
    db.add(item)
    db.flush()
    task = SCPublishTask(
        list_item_id=item.id, work_key=work_key, plan_version_id=version_id,
        kind="update", status="submitted", priority=0, scheduled_at=utcnow(),
        share_url=share_url, link_state="active" if share_url else "unknown",
    )
    db.add(task)
    db.flush()
    db.commit()
    return task


def register_force_tasks(db: Session, resource: Resource,
                         channel_ids: list[int]) -> list[SCPublishTask]:
    """把「手动强推」登记成 v2 的清单项 + 发布任务，返回新建的任务行。

    与 `plan_engine` 生成的首推任务走同一套表与同一条 dispatch 管道，区别只在：
      · `kind="force"`、`priority` 最高（手动动作排最前）；
      · **不占首推名额**：`_SlotIndex` 只载入 `source_kind="first"` 的清单项，
        force 项不进索引 —— 所以手动推过的作品之后仍会按发布规则正常排首推，
        不会被这次手动动作永久占掉名额；
      · 已在该渠道推过的作品要能重推：由**端点侧**保证——`publish_resource` 在建
        force 任务之前先把同 (资源, 渠道) 的队列行重置为 pending，dispatch 的
        「已 sent 永不重发」于是不会拦住手动动作（不给 force 开例外，否则卡住的
        force 任务会把已发出的消息再推一遍）。

    查不到快照（`sc_library_versions` 里没有该作品）时返回空列表：v2 的 dispatch
    必须有「清单项 + 快照」才能解析代表资源。调用方仍会把队列项直发出去兜底，
    只是这次不会出现在三个卡片里 —— 这是如实降级，不伪造记录。
    """
    if not channel_ids:
        return []
    work_key = utils.work_group_key(resource)
    lv = db.scalar(select(SCLibraryVersion)
                   .where(SCLibraryVersion.work_key == work_key)
                   .order_by(SCLibraryVersion.id.desc()))
    if lv is None:
        return []
    version = _active_version(db)
    version_id = int(version.id) if version else 0
    push_list = _force_push_list(db, version_id)
    category = db.scalar(select(SCWorkCategory).where(SCWorkCategory.work_key == work_key))
    category_key = str(category.category_key if category else "") or "genre:unknown"
    # 已有有效分享链接就复用，不重建：建链接有真实副作用（网盘里会多一条分享记录）。
    cached_url = str(getattr(resource, "share_url", "") or "")
    cached_ok = bool(cached_url
                     and str(getattr(resource, "share_status", "") or "") == "active")
    created: list[SCPublishTask] = []
    for channel_id in channel_ids:
        channel = db.get(Channel, int(channel_id))
        if not channel:
            continue
        chat_id = str(channel.chat_id or "")
        target_type = "group" if chat_id.startswith("-") else "channel"
        dedup = f"force:{work_key}:{channel_id}"[:255]
        item = db.scalar(select(SCPushListItem).where(SCPushListItem.dedup_key == dedup))
        if item is not None:
            # 复活旧清单项：`dedup_key` 是唯一约束，另插一条会撞唯一键。
            # 历史任务行与执行记录一条不动（清理只归档、不物理删除）。
            item.list_id = push_list.id
            item.library_version_id = lv.id
            item.category_key = category_key
            item.target_type = target_type
            item.bot_id = channel.bot_id or 0
            item.chat_id = chat_id
            item.status = "planned"
        else:
            item = SCPushListItem(
                list_id=push_list.id, work_key=work_key,
                library_version_id=lv.id, category_key=category_key,
                target_type=target_type, bot_id=channel.bot_id or 0,
                chat_id=chat_id, dedup_key=dedup,
                source_kind=FORCE_TASK_KIND, status="planned",
            )
            db.add(item)
        db.flush()
        task = SCPublishTask(
            list_item_id=item.id, work_key=work_key, plan_version_id=version_id,
            kind=FORCE_TASK_KIND, status="planned", priority=FORCE_TASK_PRIORITY,
            scheduled_at=utcnow(),
            share_url=cached_url if cached_ok else "",
            link_state="active" if cached_ok else "unknown",
        )
        db.add(task)
        db.flush()
        created.append(task)
    db.commit()
    return created


def _queue_works_by_hour(db: Session, now_sh: datetime) -> tuple[dict[int, dict[str, Any]], set[str]]:
    """当天「在途任务」按小时聚合，供排期表与推送队列同口径（v2.1.34）。

    真机反馈：「今日推送排期与推送队列影视不一致」——排期表上是蜘蛛侠 / 2001 太空
    漫游 / 阿凡达，队列里却是百变星君 / 望夫成龙。根因不是数据坏了，是**两张表在
    回答不同的问题**：`forecast_schedule` 只做预测（从分类游标往后模拟「还能排
    什么」），当天这批任务一旦建成、游标前移，它们就再也不会出现在预测里；而队列
    给的是真实任务。于是同一屏里，一边是「未来可能推的」，一边是「现在排着的」，
    用户第一反应必然是「排期丢了」。

    这里把队列那侧的数据反灌回排期表，两边从此同源：

      · 只取 `ACTIVE_TASK_STATUS`（在途）—— 与 `_aggregate_views` 的 queue 取数
        完全一致。已发布的归「推送历史」、失败的归「推送失败」，不重复进排期表。
      · 时间口径复用 `_mark_expect_release`：已发用实际发送时间，未发用按匀速节奏
        反推的预计放行时刻 —— 与队列时间列**同一个函数算出来的**，不可能对不上。
      · 同一部作品推群 + 频道合并成一条 —— 排期表按「部」说话，`count` 另外给出
        条数，与「N 条 · M 部」的写法一致。

    返回 `({小时: {"works": [...], "count": 条数}}, 该批全部 work_key)`。
    """
    today_sh = now_sh.strftime("%Y-%m-%d")
    try:
        rows = db.scalars(
            select(SCPublishTask).where(SCPublishTask.status.in_(ACTIVE_TASK_STATUS))
            .order_by(SCPublishTask.id.desc()).limit(400)).all()
    except Exception:
        db.rollback()
        return {}, set()
    out = [{k: v for k, v in _model_dict(r).items() if k in (
        "id", "work_key", "kind", "status", "created_at", "published_at",
        "scheduled_at")} for r in rows]
    # 当天口径与队列一致：按 `scheduled_at`（没有就退创建时间）判上海自然日
    out = [r for r in out
           if str(r.get("scheduled_at") or r.get("created_at") or "")[:10] == today_sh]
    if not out:
        return {}, set()

    # 预计放行时刻：与队列时间列同一个函数，已发布的行它不会设这个字段
    _mark_expect_release(db, out)

    def _at(r: dict[str, Any]) -> str:
        """这一行「什么时候推」：实际发送 > 预计放行 > 原口径兜底（同前端 scQueueClock）。"""
        return str(r.get("published_at") or r.get("expect_release_at")
                   or r.get("scheduled_at") or r.get("created_at") or "")

    first_at: dict[str, str] = {}
    task_count: dict[str, int] = {}
    for r in sorted(out, key=_at):
        wk = str(r.get("work_key") or "")
        if not wk:
            continue
        task_count[wk] = task_count.get(wk, 0) + 1
        if wk not in first_at:
            first_at[wk] = _at(r)
    keys = list(first_at)
    titles = _work_titles(db, keys)
    covers = _work_posters(db, keys)
    tags = _work_push_tags(db, keys)
    cats = _work_categories(db, keys)

    hour_map: dict[int, dict[str, Any]] = {}
    for wk in keys:
        at = first_at.get(wk) or ""
        try:
            hour = int(at[11:13])
        except (TypeError, ValueError):
            continue
        kind_key, kind_label = _schedule_kind_pair(tags.get(wk, []))
        slot = hour_map.setdefault(hour, {"works": [], "count": 0})
        slot["works"].append({
            "work_key": wk,
            "title": str(titles.get(wk) or ""),
            "cover": str(covers.get(wk) or ""),
            "tags": tags.get(wk, []),
            "category": str(cats.get(wk) or ""),
            "schedule_kind": kind_label,
            "schedule_kind_key": kind_key,
            # 真实时刻（不做均分回填），排期表直接透出，与队列显示一致
            "time": at[11:16],
            # 这部作品当前在途的任务条数（群 + 频道各一条）——分类行要写「N 条 · M 部」
            "tasks": int(task_count.get(wk) or 1),
        })
        slot["count"] += int(task_count.get(wk) or 1)
    return hour_map, set(keys)


def forecast_schedule(db: Session, *, scan_limit: int = 4000,
                      per_hour_top: int = 4) -> dict[str, Any]:
    """当天剩余时段的推送排期预测。**只读**：不写库、不建任务、不发消息。

    用户口径（v2.1.19）：发布规则不只是「分流规则」，而是调动资源库存量、有序推送的
    中枢。所以这里回答的不是「现在有哪些任务在跑」，而是「**今天接下来会推什么、
    几点推、还能推多少、排不上的卡在哪**」。

    复刻的真实链路（与 `plan_engine` / `dispatch_ready_tasks` 同源，不另写一套判据）：
      全局小时窗口 `SCHourWindow` → 小时窗口是否绑定单一分类 →
      `category_limit` 分类截断 → `_sched_allows_core` 分类时段/分类上限 →
      `_SlotIndex` 名额四态 → 在库过滤 `live_keys` → 游标有序推进 →
      `_hourly_due` 每小时匀速额度（cap 部均匀摊到 60 分钟）。

    与真实调度的**唯一区别**：这里不落库，游标推进与「已排过」都在内存里模拟。
    所以结果是「按当前配置的预测」，实际还会受网盘限流、建链失败重试影响。
    """
    now_sh = utcnow().astimezone(shanghai_tz())
    out: dict[str, Any] = {
        "ok": True,
        "forecast": True,
        "generated_at": now_sh.isoformat(timespec="seconds"),
        "now_hour": int(now_sh.hour),
        "now_minute": int(now_sh.minute),
        "hours": [],
        "today_total": 0,
        "works_total": 0,
        "backlog": 0,
        "overflow": 0,
        "blocked": [],
        "notes": [],
        "reason": "",
        # v2.1.22：插队档位与插队作品数（off 时为 0，前端据此说明"全量按排期轮转"）
        "instant_mode": "off",
        "instant_works": 0,
        # v2.1.24（用户口径 1a）：**取消分时段加载**，一次算出今天剩余全部时段，
        # 前端用固定高度的滚动区展示（打开时自动滚到当前时段）。
        # v2.1.23 的分段取数（`paged` / `from_hour` / `page`）已删除 ——
        # 用户实测「点一下才多两小时」不如直接给全天，滚动条本来就能解决长度问题。
    }

    version = _active_version(db)
    if not version:
        out["ok"] = False
        out["reason"] = "当前没有生效中的计划版本：发布规则未生效，今天不会排期。"
        return out
    cfg = _jload(version.config_json, {})
    cycle = cfg.get("cycle_plan") or {}
    category_limit = max(1, int(cfg.get("category_limit") or 20))
    work_limit = max(1, int(cfg.get("work_limit") or 50))
    hour_max = int(cycle.get("hour_max") or 0)
    cycle_hour_max = max(0, int(cycle.get("category_hour_max") or 0))
    cycle_day_max = max(0, int(cycle.get("category_day_max") or 0))

    # 每小时匀速额度（与 dispatch_ready_tasks 同一配置、同一算法）
    hourly_cap = 0
    try:
        if str(runtime_config.get(db, "sc_quick_publish_enabled") or "true").lower() in (
                "1", "true", "yes", "on"):
            hourly_cap = max(0, int(
                runtime_config.get(db, "sc_quick_publish_hourly_limit") or 0))
    except (TypeError, ValueError):
        hourly_cap = 0
    out["config"] = {"category_limit": category_limit, "work_limit": work_limit,
                     "hour_max": hour_max, "per_hour_cap": hourly_cap}
    # v2.1.23：不再提示「未设每小时上限就会一到位全推」——现在**无论如何**都按
    # 排期时段匀速放行（口径见 `_pace_slot_times`），每小时上限只作额外的绝对闸门。

    channels = list(db.scalars(select(Channel).where(Channel.enabled.is_(True))).all())
    if not channels:
        out["ok"] = False
        out["reason"] = "当前没有启用中的推送渠道：没有可推送目标，今天不会排期。"
        return out

    # 静默保护：命中静默区间的小时直接跳过，预测里也如实留空。
    quiet_on = bool(cycle.get("quiet_enabled"))
    quiet_start = int(cycle.get("quiet_start") or 0)
    quiet_end = int(cycle.get("quiet_end") or 0)

    # v2.1.35（用户口径）：时、日与分类的推送上限**一律由发布规则给**，
    # 代码里不写死任何一个数字。这里把 `plan_engine` 开头那道额度门禁
    # （`_cycle_quota_gate`，读的是发布规则的 `cycle_plan.day_max`）
    # 也接到预测侧来 —— 此前只有实跑消费它，于是额度吃满时出现
    # 「实跑 0 部、排期表照旧预测一堆」的同屏矛盾（v2.1.19 红线：预测与实跑同口径）。
    # v2.1.42：周 / 月限额下线；v2.1.45：周 / 月休止窗口也下线，只剩日度一层。
    quota_blocked, quota_code = _cycle_quota_gate(db, cycle)
    quota_reason = {
        "day_quota": "今日推送部数已达发布规则上限",
    }.get(quota_code, "已达发布规则上限")
    if quota_blocked:
        out["notes"].append(
            f"{quota_reason}，今天不再往下排；已建好的任务照常显示。")

    live_keys = _live_work_keys(db)
    slots = _SlotIndex(db, live_keys=live_keys)

    # ---- 分类字典：完全复刻 plan_engine 的选取（含 category_limit 截断）----
    all_cats = list(db.scalars(
        select(SCCategory).where(SCCategory.enabled.is_(True))
        .order_by(SCCategory.priority.desc(), SCCategory.id)
    ).all())
    kept = all_cats[:category_limit]
    dropped = all_cats[category_limit:]
    kept_keys = {c.category_key for c in kept}

    sched_map = {s.category_key: s for s in db.scalars(
        select(SCCategorySchedule).where(SCCategorySchedule.plan_version_id == version.id,
                                         SCCategorySchedule.enabled.is_(True))).all()}
    hour_windows = list(db.scalars(
        select(SCHourWindow).where(SCHourWindow.plan_version_id == version.id,
                                   SCHourWindow.enabled.is_(True))).all())
    cursors = {c.category_key: int(c.last_library_version_id or 0) for c in db.scalars(
        select(SCCategoryCursor).where(SCCategoryCursor.plan_version_id == version.id)).all()}
    today_counts = ({k: _category_today_count(db, k) for k in kept_keys}
                    if cycle_day_max else {})

    # 当前小时已经提交掉多少**部**（与 dispatch 同口径：status >= submitted、
    # 按 scheduled_at 落在本小时、distinct work_key）。
    # v2.1.33：原来数的是任务条数，同一部剧推群 + 频道会被算成两部，
    # 预测视图就会比实际少放一部——与 dispatch 改成同口径。
    hour_start_utc = utcnow().replace(minute=0, second=0, microsecond=0)
    submitted_keys_hour: set[str] = set()
    if hourly_cap:
        try:
            rows = db.execute(select(SCPublishTask.work_key).where(
                SCPublishTask.status.in_({"submitted", "accepted", "published", "failed"}),
                SCPublishTask.work_key.isnot(None),
                SCPublishTask.work_key != "",
                SCPublishTask.scheduled_at >= hour_start_utc)).all()
            submitted_keys_hour = {str(r[0]) for r in rows if r and r[0]}
        except Exception:
            db.rollback()
            submitted_keys_hour = set()

    # v2.1.34（真机反馈）：把「当天在途任务」按小时反灌进这张表 —— 排期表与推送队列
    # 从此同源。两者回答的本是同一个问题「今天几点推哪部」，此前一个只给预测、
    # 一个只给真实任务，于是同一屏里对不上。
    built_by_hour, built_keys = _queue_works_by_hour(db, now_sh)

    # v2.1.46：均匀排期方案（没生成过 → 空 dict，下面 `even_hour_quota` 返回 None，
    # 全部走老路径）。这里同时把「已建任务」按分类×小时统计出来，作为配额的已用部位，
    # 与 plan_engine 的 `_category_hour_counts` 同源，保证预测与实跑算同一笔账。
    even_plan = even_plan_of(db, version)
    # v2.1.53：补位时段。预测必须与 `plan_engine` 同步放行「分类自己的时段窗口」，
    # 否则同一个小时实跑排得出、预测却是 0 —— 那就是骗用户（v2.1.19 红线）。
    borrowed_hours = _borrowed_hours(even_plan)
    built_key_counts: dict[int, dict[str, int]] = {}
    if even_plan:
        for _h in (built_by_hour or {}):
            try:
                built_key_counts[int(_h)] = _category_hour_counts(
                    db, now_sh.replace(hour=int(_h), minute=0, second=0, microsecond=0))
            except Exception:
                db.rollback()
                built_key_counts[int(_h)] = {}

    def _quota(h: int) -> int:
        """该小时还剩多少推送额度（匀速口径，单位=部）。"""
        if not hourly_cap:
            return 10 ** 9
        if h == now_sh.hour:
            return max(0, _hourly_due(hourly_cap, now_sh.minute) - len(submitted_keys_hour))
        return hourly_cap

    # 候选基：只看每部作品的最新快照，同一作品不因多版本重复占位
    latest_ids = (select(func.max(SCLibraryVersion.id)).group_by(SCLibraryVersion.work_key))

    def _candidates(cat_key: str, after_id: int | None, limit: int | None):
        q = (select(SCLibraryVersion)
             .join(SCWorkCategory, SCWorkCategory.work_key == SCLibraryVersion.work_key)
             .where(SCWorkCategory.category_key == cat_key)
             .where(SCLibraryVersion.id.in_(latest_ids))
             # v2.1.35（真机「全天排期很空」）：**在库过滤必须放在 SQL 层**。
             # 快照表不随资源库清理删除，作品移出资源库后 SCLibraryVersion /
             # SCWorkCategory 记录仍在（「死快照」）。它们按 id 升序排在前面，
             # 发布规则里配的 `work_limit` 名额被它们吃掉 → picks 全空 → 排 0 部。
             # 循环内的 `wk not in live_keys` 校验在 SQL 之后才跑，救不回来。
             # 这里加过滤让 50 个名额全给在库作品，游标一次越过整个死区。
             # live_keys 为空时 in_([]) 生成恒假条件 → 返回空，是正确行为，勿特判。
             .where(SCLibraryVersion.work_key.in_(live_keys)))
        if after_id is not None:
            q = q.where(SCLibraryVersion.id > after_id)
        q = q.order_by(SCLibraryVersion.id)
        if limit:
            q = q.limit(limit)
        return list(db.scalars(q).all())

    planned_pairs: set[tuple[str, str]] = set()   # 预测中已排过的 (work_key, chat_id)
    scanned = 0

    def _picks_for(wk: str) -> list[str]:
        """该作品现在能排的渠道 chat_id 列表（空 = 名额已占 / 不在库 / 扫到上限）。"""
        nonlocal scanned
        if not wk or wk not in live_keys:
            return []
        # v2.1.34：已经建好任务的作品不再参与预测 —— 它已经在排期表里以
        # 「已排期任务」出现，再预测一遍就是同一部剧占两行、顶部数字也双计。
        if wk in built_keys:
            return []
        if scanned >= scan_limit:
            return []
        scanned += 1
        picks: list[str] = []
        for channel in channels:
            chat_id = str(channel.chat_id or "")
            if (wk, chat_id) in planned_pairs:
                continue
            dedup = f"{wk}:{channel.id}:first"[:255]
            action, _reuse = first_dedup_plan(db, work_key=wk, chat_id=chat_id,
                                              dedup=dedup, index=slots)
            if action == "skip":
                continue
            picks.append(chat_id)
        return picks

    # v2.1.22：上新即时推送的插队也要进预测。
    # 只模拟存量轮转的话，预测会说「今天推的都是老片」，而实跑会先把新上映推出去——
    # 预测与实跑算出差，就等于骗用户（v2.1.19 定下的红线）。这里把插队候选
    # 按 `plan_engine` 同一份 `_instant_candidates` 取出来，占当前小时的额度，
    # 同时把它们加进 `planned_pairs`，避免轮转模拟把它们再排一遍。
    instant_mode = instant_push_mode(db)
    instant_rows: list[dict[str, Any]] = []
    if instant_mode != "off":
        for wk, _lv_id, lane in _instant_candidates(db, instant_mode):
            picks = _picks_for(wk)
            if not picks:
                continue
            for chat_id in picks:
                planned_pairs.add((wk, chat_id))
            instant_rows.append({"work_key": wk, "lane": lane, "picks": len(picks)})
        if instant_rows:
            out["instant_mode"] = instant_mode
            out["instant_works"] = len(instant_rows)

    # ---- 逐小时模拟 ----
    # v2.1.24：一次性从当前小时算到 24 点。前端用滚动区承载（不再分批加载）。
    # v2.1.34：起点往前退到「最早一条在途任务所在小时」—— 队列里可能挂着本该更早
    # 发出、到现在还没出去的任务；不往前退，那些小时在排期表里就没有行，
    # 「队列有、排期表没有」会原样再来一次。
    start_hour = int(now_sh.hour)
    if built_by_hour:
        start_hour = min(start_hour, min(built_by_hour))
    end_hour = 24
    for h in range(start_hour, end_hour):
        # v2.1.34：本小时「已经建好任务」的那批 —— 板上钉钉的排期，排在最前。
        # 它们不占预测额度（额度已在 submitted_keys_hour 里扣过），只做展示。
        #
        # 按**作品自己的 TMDB 分类**拆成多项，绝不另造一个「已排期任务」的假分类：
        # 分类列只放题材（v2.1.25 已定的口径），这批与预测的区分靠 `kind=scheduled`
        # （前端加「已建」前缀 + 独立配色），不靠往分类格里塞状态词。
        built_slot = built_by_hour.get(h) or {}
        built_items: list[dict[str, Any]] = []
        by_cat: dict[str, list[dict[str, Any]]] = {}
        for w in (built_slot.get("works") or []):
            by_cat.setdefault(str(w.get("category") or ""), []).append(w)
        for cat_name, ws in by_cat.items():
            built_items.append({
                "category": cat_name or "未分类",
                "kind": "scheduled",
                "count": sum(int(x.get("tasks") or 1) for x in ws),
                "times": list(dict.fromkeys([x["time"] for x in ws if x.get("time")])),
                "titles": [x["title"] for x in ws if x.get("title")],
                "works": ws,
                "works_total": len(ws),
                "more": 0,
            })
        built_count = sum(int(i["count"]) for i in built_items)
        # 已经过去的小时：只如实交代「确实排着的任务」，不再编预测；
        # 连任务都没有的小时干脆不占一行，否则凌晨打开会多出一整屏空行。
        if h < now_sh.hour:
            if not built_items:
                continue
            out["today_total"] += built_count
            out["hours"].append({"hour": h, "total": built_count, "items": built_items})
            continue

        win = next((w for w in hour_windows
                    if int(w.start_hour or 0) <= h < int(w.end_hour or 0)), None)
        if hour_windows and win is None:
            out["hours"].append({"hour": h, "total": built_count,
                                 "items": built_items,
                                 "blocked": "" if built_items else "不在可发布时段"})
            continue
        if quiet_on:
            in_quiet = ((quiet_start <= h < quiet_end) if quiet_end > quiet_start
                        else (h >= quiet_start or h < quiet_end))
            if in_quiet:
                out["hours"].append({"hour": h, "total": built_count,
                                     "items": built_items,
                                     "blocked": "" if built_items else "静默保护时段"})
                continue

        # v2.1.35：日/周/月额度或休止窗口命中时，本小时不再预测（已建任务照常显示）。
        # 上限值全部来自发布规则，这里只做「如实留空 + 说明卡在哪」。
        if quota_blocked:
            out["today_total"] += built_count
            out["hours"].append({"hour": h, "total": built_count,
                                 "items": built_items,
                                 "blocked": "" if built_items else quota_reason})
            continue

        cats = kept
        bound_key = str(getattr(win, "category_key", "") or "") if win else ""
        # v2.1.53：补位时段不缩表（绑定分类正是没货才要补的那个），与 plan_engine 同源。
        if bound_key and h not in borrowed_hours:
            scoped = [c for c in kept if c.category_key == bound_key]
            if scoped:
                cats = scoped

        quota = _quota(h)
        hour_items: list[dict[str, Any]] = []
        hour_tasks = 0
        # v2.1.47：两个计数器，单位不同（`cat_counts`=条，计额度与单分类小时上限；
        # `cat_works`=部，计均匀排期名额）。与 plan_engine 一一对应，勿混用。
        cat_counts: dict[str, int] = {}
        cat_works: dict[str, int] = {}
        # v2.1.34：已建任务排在本小时最前（它比插队和轮转都确定），且不计入
        # `hour_tasks` —— 额度在 `submitted_keys_hour` 里已经扣过了，重复扣会把
        # 本小时的预测挤成 0，又变成「有队列、排期表空」。
        if built_items:
            hour_items.extend(built_items)
        # v2.1.22：插队项（新上映 / 新入库）排在本小时最前面，且**先占额度**。
        if h == now_sh.hour and instant_rows:
            taken: list[dict[str, Any]] = []
            taken_tasks = 0
            for row in instant_rows:
                if hour_tasks >= quota:
                    break
                allowed = min(int(row["picks"]), quota - hour_tasks)
                if allowed <= 0:
                    break
                hour_tasks += allowed
                taken_tasks += allowed
                taken.append(row)
            if taken:
                ikeys = [r["work_key"] for r in taken]
                ititles = _work_titles(db, ikeys)
                icovers = _work_posters(db, ikeys)
                itags = _work_push_tags(db, ikeys)
                # v2.1.25：每部作品带上自己的 TMDB 分类，「推送队列」的待排期行
                # 才有真实分类可显示（不能拿「上新即时推送」这个通道名去当分类）。
                icats = _work_categories(db, ikeys)
                works: list[dict[str, Any]] = []
                for wk in ikeys:
                    kind_key, kind_label = _schedule_kind_pair(itags.get(wk, []))
                    works.append({
                        "work_key": wk,
                        "title": str(ititles.get(wk) or ""),
                        "cover": str(icovers.get(wk) or ""),
                        "tags": itags.get(wk, []),
                        "category": str(icats.get(wk) or ""),
                        # v2.1.26：排期类型（新上映 / 新入库 / 存量排期）——前端
                        # 「推送队列」的待排期行直接用这个字段，不再自己从 tags 里挑。
                        # v2.1.28：同时给 key，供前端按 key 上色。
                        "schedule_kind": kind_label,
                        "schedule_kind_key": kind_key,
                        "time": "",
                    })
                hour_items.append({
                    "category": "上新即时推送",
                    # v2.1.25（用户口径 2）：这一项是**通道**不是 TMDB 分类 ——
                    # 它代表「按新上映 / 新入库插队进来的那批」，与分类轮转是两回事。
                    # 明确给 `kind`，前端据此加「插队」标记，别让人误读成题材分类。
                    "kind": "instant",
                    "count": taken_tasks,
                    # v2.1.23：预计时间统一由 `_respread_forecast_times` 按
                    # 「时段内均分」回填；这里先留空，绝不再吐一排相同的整点。
                    "times": [],
                    "titles": [t for t in ititles.values() if t],
                    "works": works,
                    "works_total": len(ikeys),
                    "more": max(0, len(instant_rows) - len(taken)),
                })
        # v2.1.46：均匀排期配额（与 plan_engine 同一份方案、同一个判据）。
        # `even_q is None` ＝ 没有方案 → 下面 `_even_allows` 恒 True，行为零变化。
        even_q = even_hour_quota(even_plan, h)
        even_base = built_key_counts.get(h, {})
        for cat in cats:
            if hour_tasks >= quota:
                break
            key = cat.category_key
            # 本小时「已建任务 + 本小时已预测」到该分类配额就换下一个分类，
            # 剩余名额留给排在后面的分类（这正是「均匀」的实现点）。
            # v2.1.47：比的是**部**（`cat_works`），与 plan_engine 同口径。
            if not _even_allows(even_q, key,
                               int(even_base.get(key, 0)) + int(cat_works.get(key, 0))):
                continue
            if not _sched_allows_core(sched_map.get(key), hour=h,
                                      weekday=now_sh.isoweekday(),
                                      per_category_count=cat_counts.get(key, 0),
                                      cycle_hour_max=cycle_hour_max,
                                      cycle_day_max=cycle_day_max,
                                      today_count=today_counts.get(key, 0),
                                      # v2.1.53：补位时段与实跑同一份放行口径
                                      check_window=h not in borrowed_hours):
                continue
            built = 0
            titles: list[str] = []
            for lv in _candidates(key, cursors.get(key, 0), work_limit):
                if hour_tasks >= quota:
                    break
                if hour_max and hour_tasks >= hour_max:
                    break
                # v2.1.46：与 plan_engine 同一处、同一判据——每部作品建之前都按
                # 「本小时该分类已排 已建」跟配额比一次，到顶就停手。
                # 只放在分类外层的话，入口计数为 0，一个分类照样能把额度吃满。
                # v2.1.47：比的是**部**（`cat_works`）。
                if not _even_allows(even_q, key,
                                   int(even_base.get(key, 0)) + int(cat_works.get(key, 0))):
                    break
                wk = str(lv.work_key or "")
                picks = _picks_for(wk)
                if not picks:
                    # 名额已占的作品也必须越过游标，否则预测会永远卡在同一部上
                    cursors[key] = max(cursors.get(key, 0), int(lv.id))
                    continue
                built_here = 0
                for chat_id in picks:
                    if hour_tasks >= quota:
                        break
                    planned_pairs.add((wk, chat_id))
                    hour_tasks += 1
                    built += 1
                    built_here += 1
                    cat_counts[key] = cat_counts.get(key, 0) + 1
                if built_here:
                    # 一部作品算一个名额（它的各渠道任务同属这一部）
                    cat_works[key] = cat_works.get(key, 0) + 1
                cursors[key] = max(cursors.get(key, 0), int(lv.id))
                if wk not in titles:
                    titles.append(wk)
            if built:
                # v2.1.20：额外给出结构化作品项（work_key + 标题 + 封面 + 预计时间），
                # 供「推送队列」直接复用 —— 三个模块（排期 / 队列 / 任务）的标题与封面
                # 必须同源（都走 _work_titles / _work_posters），否则同一部作品在两处
                # 显示不同名字或一处有封面一处没封面。`titles` 保留给旧调用方。
                top_keys = titles[:per_hour_top]
                wk_titles = _work_titles(db, top_keys)
                wk_covers = _work_posters(db, top_keys)
                # v2.1.22：排期里也要看得到「新入库 / 新上映 / 存量排期」，
                # 与推送历史 / 队列 / 任务同源（同一个 _work_push_tags）。
                wk_tags = _work_push_tags(db, top_keys)
                # v2.1.25：作品自带 TMDB 分类（供队列待排期行如实显示「分类」）
                wk_cats = _work_categories(db, top_keys)
                works: list[dict[str, Any]] = []
                for wk in top_keys:
                    kind_key, kind_label = _schedule_kind_pair(wk_tags.get(wk, []))
                    works.append({
                        "work_key": wk,
                        "title": str(wk_titles.get(wk) or ""),
                        "cover": str(wk_covers.get(wk) or ""),
                        "tags": wk_tags.get(wk, []),
                        "category": str(wk_cats.get(wk) or ""),
                        # v2.1.26：排期类型，与三视图同一口径（`_schedule_kind_pair`）
                        "schedule_kind": kind_label,
                        "schedule_kind_key": kind_key,
                        "time": "",
                    })
                hour_items.append({
                    "category": _category_display_name(key),
                    # v2.1.25：分类轮转产出的行，`category` 就是 TMDB 分类名
                    "kind": "category",
                    "category_key": key,
                    "count": built,
                    # v2.1.23：预计时间统一由 `_respread_forecast_times` 回填。
                    # `works_total` 是**未截断**的作品数——均分要按真实部数算间隔，
                    # 若按「只展示前 4 部」来算，间隔会被估大一倍。
                    "times": [],
                    "titles": [t for t in wk_titles.values() if t],
                    "works": works,
                    "works_total": len(titles),
                    "more": max(0, len(titles) - per_hour_top),
                })
        total = sum(i["count"] for i in hour_items)
        out["today_total"] += total
        out["hours"].append({"hour": h, "total": total, "items": hour_items})

    # v2.1.23：按「排期时段」把作品的时间均分回写（用户口径 4）。
    out["pace"] = _respread_forecast_times(out["hours"], now_sh)

    # v2.1.34：把「已经建好任务」的作品也算进总数 —— 它们不再进 planned_pairs
    # （_picks_for 已拦截），但确实是今天要推的，漏掉会让顶部数字比表里少。
    out["works_total"] = len(set(wk for wk, _ in planned_pairs) | built_keys)

    # ---- 存量盘点：今天推不完的还有多少（受 scan_limit 保护，超限则标注「至少」）----
    # 重新计数：上面的模拟阶段已经消耗过 scanned，若共用同一个计数器，
    # 作品多时会被提前耗尽 → overflow 恒为 0，等于告诉用户「全推得完」的假消息。
    scanned = 0
    backlog_pairs: set[tuple[str, str]] = set()
    seen_wk: set[str] = set()
    truncated = False
    for cat in kept:
        for lv in _candidates(cat.category_key, None, None):
            wk = str(lv.work_key or "")
            if not wk or wk in seen_wk:
                continue
            seen_wk.add(wk)
            if scanned >= scan_limit:
                truncated = True
                break
            picks = _picks_for(wk)
            for chat_id in picks:
                backlog_pairs.add((wk, chat_id))
        if truncated:
            break
    out["overflow"] = len(backlog_pairs)
    out["backlog"] = len(backlog_pairs) + len(planned_pairs)
    if truncated:
        out["notes"].append("存量作品过多，本次只盘点到 %d 部，实际待推量可能更多。" % scan_limit)

    # ---- 卡点如实上报：这才是「排不上」的真实原因 ----
    blocked: list[dict[str, Any]] = []
    for cat in dropped:
        n = 0
        for lv in _candidates(cat.category_key, None, None):
            if str(lv.work_key or "") in live_keys:
                n += 1
        if n:
            blocked.append({
                "reason": "分类未进入调度（启用分类数超过上限 %d）" % category_limit,
                "category": _category_display_name(cat.category_key),
                "count": n,
            })
    out["blocked"] = blocked
    if blocked:
        out["reason"] = ("有 %d 个分类、共 %d 部作品排不进队列：分类上限是 %d，但启用中的"
                         "分类有 %d 个——超出上限的分类不会被调度遍历，永远轮不到"
                         "（与时段窗口无关）。"
                         % (len(blocked), sum(b["count"] for b in blocked),
                            category_limit, len(all_cats)))
    elif not hour_windows:
        out["notes"].append("未配置发布时段：全天都允许排期。")
    return out

def _pick_representative(rows: list):
    """v2.1.65：从同一作品的资源行里挑「代表资源」——优先已识别且有海报的那条。

    真机事故（2026-11-28，李卫当官）：快照 resource_ids_json 第一个 id 指向的
    恰好是未识别空壳行（identify_status=unidentified / poster_path 空 / media_title
    空），`resolve()` 取它当代表 → publisher 用它的 poster_path 判 sendPhoto/sendMessage，
    海报空就退化成纯文字，TMDB/简介也全空，用户看到「只有标题和文件名」的半残推送。
    全库 1186 部里 365 部代表资源命中这类空壳行，其中 89 部组内其实有海报可救。

    选择口径（不改任何数据、不造版本、不动指纹）：
      ① 已识别 + 有海报  → 最优，直接发 sendPhoto + 完整信息；
      ② 已识别           → 次优（TMDB 确实无海报时仍要有正确标题/简介）；
      ③ 有海报           → 再次（未识别但海报是识别副产物，有则优先于空壳）；
      ④ 以上都没有       → 退回传入顺序第一条（老行为，绝不返回 None）。
    rows 用 getattr 读属性，Resource ORM 与 SimpleNamespace 都能用。
    """
    rows = list(rows or [])
    if not rows:
        return None

    def _identified(r):
        return str(getattr(r, "identify_status", "") or "").strip() == "identified"

    def _has_poster(r):
        return bool(str(getattr(r, "poster_path", "") or "").strip())

    for r in rows:
        if _identified(r) and _has_poster(r):
            return r
    for r in rows:
        if _identified(r):
            return r
    for r in rows:
        if _has_poster(r):
            return r
    return rows[0]


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
                # v2.1.65：实时回退也优先「已识别+有海报」，不再盲目取 file_id 最小
                # 那条（它可能正是未识别空壳行，导致推送无海报/TMDB/简介）。
                table[wk] = _pick_representative(ordered)
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
        """返回 (代表资源, 失败原因)。失败原因为空表示解析成功。

        v2.1.65：快照 id 命中的资源**不再取第一个存在的**，而是一次查出全部，
        优先挑「已识别+有海报」那条（见 `_pick_representative`）。病根：快照
        按 file_id 排序，第一条常是未识别空壳行（李卫当官 id 10669）， publisher
        按它的空 poster_path 退化成纯文字推送，用户看到无海报/无 TMDB/无简介。
        快照 id 全失效时仍退回实时分组（也已改为优先有海报）。
        """
        rids = _jload(lv.resource_ids_json, []) or []
        if rids:
            # 一次查询取回快照全部成员（避免逐条 db.get 的 N+1），再按优先级挑代表。
            try:
                ids = [int(r) for r in rids]
            except (TypeError, ValueError):
                ids = []
            if ids:
                rows = list(self.db.scalars(
                    select(Resource).where(Resource.id.in_(ids))).all())
                if rows:
                    return _pick_representative(rows), ""
        if self._by_key is None:
            self._build()
        live = (self._by_key or {}).get(str(lv.work_key or ""))
        if live is not None:
            return live, ""
        if rids:
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
    # v2.1.22：插队任务（新上映 900 / 新入库 800 / 手动推送 1000）的先后由计划
    # 引擎与用户动作定好了，冲突策略的排序**不得把它们洗回存量后面**——
    # 否则「上新即时推送」等于没开。
    if int(task.priority or 0) >= NEW_ENTRY_TASK_PRIORITY:
        return base
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
        # v2.1.22：追更任务不进 dispatch。它落库就是 submitted（只记账），真实
        # 投递由更新事件那条已置回 pending 的队列项完成；万一用户手动把一条追更
        # 任务重置成 planned，这里若不排除就会被当成首推再走一遍建链 → 双推 + 白建链接。
        .where(SCPublishTask.kind != UPDATE_TASK_KIND)
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
             "skipped": 0,
             # v2.1.16：复用已有有效分享链接的任务数（不重建链接，避免网盘堆废分享）
             "reused": 0,
             # v2.1.57：同目录已有分享 → 复用（与 reused 的区别：reused 是任务自己
             # 带着链接，linked 是同作品另一条任务/别的行建过的链接，属于「去重」）
             "linked": 0,
             # v2.1.16：建分享失败 → 标记为「无效推送」的任务数
             "invalid": 0,
             # v2.1.23：因「本时段匀速额度已用完」而留到下一轮的任务数
             # （不是失败、不是门禁，是节奏——下一轮会在下一个时间点放行）
             "paced": 0}
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
    # v2.1.33：整小时额度的计量单位从「条」改成「部」（distinct work_key）。
    #
    # 真机反馈：「同一部影视推送群和频道，时间差较大，没有同步」。根因就在这里——
    # 一部剧要推「频道 + 群」两条任务，旧口径按**条**算额度（默认每小时 3 条），
    # 于是第 1 条占掉一个名额，第 2 条立刻被判成超额：`stats["throttled"]` 之后
    # `break` 掉整轮，要等到下一个匀速放行点（cap=3 时下一个是第 20 分钟）
    # 才能出去 —— 用户看到的就是同一部剧群和频道差了半小时。
    #
    # 用户口径一直是「部」：`max_works_per_run` 按部、改名预算单位也是部。
    # 所以这里只算本小时已提交的作品数，同一部剧的第二条渠道不占额外名额。
    submitted_keys_hour: set[str] = set()
    if hourly_cap:
        try:
            rows = db.execute(select(SCPublishTask.work_key).where(
                SCPublishTask.status.in_({"submitted", "accepted", "published", "failed"}),
                SCPublishTask.work_key.isnot(None),
                SCPublishTask.work_key != "",
                SCPublishTask.scheduled_at >= hour_start)).all()
            submitted_keys_hour = {str(r[0]) for r in rows if r and r[0]}
        except Exception:
            db.rollback()
            submitted_keys_hour = set()
    # v2.1.23：时段匀速（用户口径 4）——「本时段 2 小时、4 部 → 30 分钟一部」。
    # 分母是**部数**（distinct work_key），不是任务数：同一部剧推频道 + 群是两条任务，
    # 但同时出去才对；按条算会把间隔砍半。
    # 与上面的每小时上限**并存取更严**：上限是绝对闸门，匀速是节奏。
    pace = _period_pace(db, now)
    # v2.1.33：本时段已放行的**作品集合**（不是条数）。
    # 用集合而不是数字，才能回答「这条任务的 work_key 是不是已经在里面了」——
    # 同一部剧的第二条渠道直接放行、不再占名额、也不会把整轮 break 掉。
    released_works: set[str] = (set() if pace["due"] is None
                               else _period_released_works(db, pace))
    # 已经在别处（别的渠道）本轮被挡下的作品：它的其余渠道同样不发，
    # 免得同一部剧一半出去一半留着。
    blocked_works: set[str] = set()
    for task_index, task in enumerate(tasks):
        # v2.1.3：单轮墙钟预算。超预算就把剩余任务留给下一轮（它们仍是
        # planned/ready，下一轮自然被捞起）——绝不让一轮跑到十几分钟，
        # 否则 _job_share_center（5s 周期 + max_instances=1）会永远卡在
        # "上一轮未结束"，持续刷 maximum number of running instances reached。
        if time.monotonic() - round_started > DISPATCH_ROUND_BUDGET_SECONDS:
            stats["deferred"] = len(tasks) - task_index
            break
        # v2.1.19：本小时额度按**分钟匀速**放行，不再整点一次性放完。
        # 原来 `submitted_this_hour >= hourly_cap` 会让整点一到就把 cap 部全推出去，
        # 后面 59 分钟静默——用户口径是「每时段均匀分配」，所以改成按进度放行：
        # cap=3 → 第 0/20/40 分钟各 1 部。cap=0 时不限（_hourly_due 返回大数）。
        #
        # v2.1.33：同作品豁免 —— 本小时已经提交过的作品，它的第二条渠道
        # （频道 / 群）不再走额度判断；被挡时从 `break` 改成 `continue`，
        # 否则后面同作品的那条也被一起停掉，正是「群和频道不同步」的形状。
        if hourly_cap and task.kind != FORCE_TASK_KIND:
            wk = str(task.work_key or "")
            if wk not in submitted_keys_hour:
                if len(submitted_keys_hour) >= _hourly_due(hourly_cap, now.minute):
                    # v2.1.21：手动强推例外——用户点按钮就是要在当下发出去，
                    # 不能被「上新即时推送每小时上限」卡住（自动调度照旧受限）。
                    stats["throttled"] += 1
                    continue
        item = db.get(SCPushListItem, task.list_item_id)
        lv = db.get(SCLibraryVersion, item.library_version_id) if item else None
        if not item or not lv:
            # v2.1.16：这类失败重试也不会成功（快照/清单项都没了），按用户口径
            # 落「无效推送」而不是普通失败——名额随之回收，作品回到待推送。
            task.status = INVALID_TASK_STATUS
            task.error = "清单项或资源版本不存在"
            db.commit()
            stats["invalid"] += 1
            continue
        metadata = _jload(lv.metadata_json, {})
        rep, rep_err = _rep_resolver.resolve(lv)
        if not rep:
            # 资源库已无该作品的文件 → 这次推送无效（用户口径：校验资源库有文件）。
            task.status = INVALID_TASK_STATUS
            task.error = rep_err or "代表资源不存在"
            db.commit()
            stats["invalid"] += 1
            continue
        # v2.1.3 推送门禁（真机 2026-11-22 黑豹2 事故的直接成因）：建分享之前
        # 必须逐项核验「海报 / 资料 / 识别成功 / 目录已整理 / 文件名已英文」。
        # 门禁未过的任务**不建分享、不入队、不算失败**——它只是「还没准备好」，
        # 保持 planned + 下轮重试，等改名 / 补齐后再由计划引擎重新排期。
        # 中文原因写进 task.error（页面只展示中文，不出现任何后端字段名）。
        if task.kind == FORCE_TASK_KIND:
            # v2.1.21：手动强推绕过推送门禁（海报 / 资料 / 识别成功 / 目录已整理 /
            # 文件名已英文）。这是 v0.9.11 起手动推送的既有口径——门禁只约束自动调度，
            # 手动是用户明确的动作，拦下来只会让人以为「点了没反应」。
            gate = {"passed": True, "reason": ""}
        else:
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
        # 注（v2.1.21）：「手动强推要能重推」由端点侧保证——`publish_resource` 在建
        # force 任务**之前**就把同 (资源, 渠道) 的队列行重置为 pending，于是这里的
        # 「已 sent 永不重发」天然不会拦住手动动作。**不在这里给 force 开例外**：
        # 一旦开了，任何卡在 planned 的 force 任务都会在下一次 run_cycle 把已发出的
        # 消息再推一遍（真机泰迪熊2 双推就是这个形状）。
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
        # v2.1.16：有仍有效的分享链接就**复用**，不再重建。
        #
        # 旧实现每次 dispatch 都调 create_share_parsed：重试一次就在网盘里多一条
        # 分享记录；而旧链接一旦作废，群里那条消息仍然指向它，等于白发一条。
        # 用户口径：「待推送时**若无推送链接**才重新触发分享链接」。
        # 判定「有链接」= share_url 非空 且 link_state 不是 cleaned/invalid。
        #
        # v2.1.23：时段匀速门禁放在**这里**——已经确认真要发条消息了才查节奏。
        # 此前它放在循环最前面，会把「跳过重复投递 / 无效推送 / 门禁未过」这些
        # **不消耗额度**的分支一起挡掉：用户看到的是 `paced` 而不是 `skipped`，
        # 统计口径就错了（真机表现为"明明该跳过却什么都没做"）。
        # 手动强推豁免：用户点按钮就要在当下发出去。
        # v2.1.33：**同一部作品多个目标同轮放行**（真机 #3「22 点推送的八仙，
        # 队列显示 22:00、实际 22:30」与 #4「群和频道时间差较大」同一个根因）。
        #
        # 旧写法是「`_period_released(db, pace) >= pace["due"]` 就 break」：
        #   · `_period_released` 数的是 distinct work_key（部数），口径没错；
        #   · 但判断落在**每一条任务**上——同一部剧的第 2 条渠道（群 / 频道）
        #     看着「部数已满」就被判超额，然后 `break` 掉整轮，
        #     固定要多等一个 interval 才能出去。
        #   实测 22:00、n_works=4 → interval=30：846 放行、847（同作品另一渠道）
        #   被 break，一直等到 22:30 才走。
        #
        # 现在改三步判断：
        #   ① 这个 work_key 本轮已被挡 → 同作品其余渠道一起跳过（不计重复额度）；
        #   ② 这个 work_key 本时段已放行 → 直接放行，不占名额（就是「同轮同步」）；
        #   ③ 否则按部数判额度，满了只把这个作品记进 blocked，不 break 整轮——
        #      后面若有别的作品早先已放行、它的第二渠道仍能跟上。
        if pace["due"] is not None and task.kind != FORCE_TASK_KIND:
            wk = str(task.work_key or "")
            if wk in blocked_works:
                stats["paced"] += 1
                continue
            if wk not in released_works:
                if len(released_works) >= pace["due"]:
                    blocked_works.add(wk)
                    stats["paced"] += 1
                    continue
                released_works.add(wk)
        reuse_link = (str(task.share_url or "").strip()
                      and str(task.link_state or "") not in INVALID_LINK_STATES)
        # v2.1.63：一部作品只生成一条分享链接，无论哪个渠道推送都复用这一条。
        # 同轮两条渠道任务：第一条建链后在这里 commit（见本函数末尾 db.commit()），
        # 第二条进来时它的 share_url 已在库里，但旧逻辑只认「自己这条 task」的
        # share_url（`reuse_link`），查不到 → 又建一条（真机 26 部双链）。
        # 现在自己的链接无效/为空时，先按作品维度跨 task 查同作品已建的有效链接：
        #   · 查到 → 复用，计入 linked（与 v2.1.57 的 linked 同义：去重）；
        #   · 查不到 → 才走下面的新建（含 _existing_share_on_dir 的目录级兜底）。
        work_link = ""
        if not reuse_link:
            work_link = _existing_work_share_url(
                db, str(task.work_key or ""), exclude_task_id=task.id)
        if reuse_link or work_link:
            if work_link:
                task.share_url = work_link
                task.link_state = "active"
            task.status = "ready"
            task.error = ""
            stats["reused" if reuse_link else "linked"] += 1
            db.commit()
        else:
            task.status = "building"
            task.error = ""
            db.add(SCPublishExecution(task_id=task.id, stage="build", status="running",
                                      started_at=utcnow(),
                                      request_json=_jdump({"work_key": task.work_key})))
            db.commit()
            try:
                client = GuangYaShareClient(db)
                # v2.1.3：建分享网络硬超时。guangya.post 内部 3 次重试 + 退避最坏
                # ≈63s；20 个任务串起来 = 单轮 21 分钟 → 5 秒周期的 job 永远
                # "上一轮未结束"（真机刷了 24 小时的 maximum number of running
                # instances reached）。这里给单次建分享封顶 45s，超时按失败处理
                # （任务回到 planned，下轮重试），不再拖住整轮。
                # v2.1.57（真机「我的分享」里一条条单集名 + 同剧多条链接）：
                # 名字用作品级标题、目录用作品目录（不是季目录），且**建之前先看
                # 同目录有没有现成链接**——有就复用，不再重复建。
                target_dir, share_title = _share_target(
                    db, str(task.work_key or ""), metadata, rep, _rep_resolver, lv)
                reuse_url = _existing_share_on_dir(db, target_dir)
                if reuse_url:
                    share_id, share_url = "", reuse_url
                    stats["linked"] += 1
                else:
                    share_id, share_url = await asyncio.wait_for(
                        create_share_parsed(
                            client, target_dir or rep.file_id,
                            share_title or str(task.work_key or ""), 0,
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
                # v2.1.16：建分享失败 → 该次推送判为**无效推送**（用户口径「若分享
                # 不成功则标记无效推送」）。名额随之回收，作品回到待推送；但给一个
                # 退避窗口，避免每 5 秒重建一次链接把网盘刷满废分享。
                task.status = INVALID_TASK_STATUS
                task.link_state = INVALID_TASK_STATUS
                task.share_url = ""
                task.error = f"分享链接创建失败：{str(exc)[:300]}"
                task.next_retry_at = now + timedelta(seconds=INVALID_RETRY_SECONDS)
                stats["invalid"] += 1
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
        # v2.1.33：按「部」记账 —— 同一部剧的第二条渠道（群 / 频道）落进同一个
        # work_key，不额外占本小时额度。
        submitted_keys_hour.add(str(task.work_key or ""))
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


def _orphan_submitted_tasks(db: Session, *, limit: int = 200) -> list[SCPublishTask]:
    """找出「已被同渠道后续任务顶掉、永远等不到回写」的 submitted 任务。

    v2.1.31 存量回收。判据（三条同时成立才算，宁可漏不可错杀）：
      ① 本任务还停在 `submitted`（已交给队列、但没拿到终态）；
      ② 同作品同渠道有一条 `published` 兄弟任务；
      ③ 那条兄弟任务的 `published_at` **不早于**本任务的 `created_at`
         —— 说明是本任务入队之后才投出去的，能代表「这次投递」已经发生。

    为什么不能只按「没有队列行指向它」判：队列行会被归档/复用，单看引用会误伤。
    命中后不物理删除，只归档为 `skipped`（页面显示「已跳过投递」），历史行与
    执行记录全保留 —— 与全站「清理一律归档」的口径一致。
    """
    rows = list(db.scalars(
        select(SCPublishTask).where(SCPublishTask.status == "submitted")
        .order_by(SCPublishTask.id).limit(int(limit))).all())
    out: list[SCPublishTask] = []
    for task in rows:
        item_id = int(task.list_item_id or 0)
        if not item_id:
            continue
        chat_id = db.scalar(select(SCPushListItem.chat_id).where(SCPushListItem.id == item_id))
        if not chat_id:
            continue
        done = db.scalar(
            select(func.count()).select_from(SCPublishTask)
            .join(SCPushListItem, SCPublishTask.list_item_id == SCPushListItem.id)
            .where(SCPushListItem.chat_id == str(chat_id),
                   SCPublishTask.work_key == str(task.work_key or ""),
                   SCPublishTask.status == "published",
                   SCPublishTask.id != int(task.id),
                   SCPublishTask.published_at.is_not(None),
                   SCPublishTask.published_at >= task.created_at))
        if done:
            out.append(task)
    return out


def _close_orphan_submitted(db: Session, tasks: list[SCPublishTask]) -> int:
    """把孤儿 submitted 任务归档为 `skipped`，返回条数。

    与 `mark_result` 成功收口兄弟任务**同一语义**（都是「这个作品在这个渠道已经
    发出去了，本条不该再投」），所以同样写一条 `skipped` 执行记录留痕 ——
    只改状态不写记录，事后就查不出是谁把它归档的、为什么归档。
    """
    now = utcnow()
    for task in tasks:
        task.status = "skipped"
        task.error = ("同作品同渠道已由另一条任务发出，本条重复投递跳过"
                      "（队列行被复用导致回写缺失，已由手动扫描回收）")[:1000]
        task.updated_at = now
        db.add(SCPublishExecution(task_id=task.id, stage="send", status="skipped",
                                  started_at=now, finished_at=now,
                                  response_json=_jdump({"reason": "orphan_submitted",
                                                        "recovered_by": "repair_cycle"})))
    return len(tasks)


async def repair_cycle(db: Session, *, dry_run: bool = True,
                       limit: int = 50) -> dict[str, Any]:
    """手动扫描（v2.1.18 升级）：体检 → 自愈 → 重排 → 推送。

    把用户口语里的四件事一次做完，不再要求他自己判断该点哪个按钮：

      ① 清理无效链接   —— 复用「清理分享链接」的**同一套判据**（`_scan_invalid_links`）；
      ② 清理失败任务   —— 复用「清理无效推送任务」的同一套判据（`_scan_invalid_tasks`）；
                          只清「重试也不会成功」的，临时失败（抖动/限流）留着重试；
      ③ 在库文件重排   —— 分类游标归零 + 被清作品的「已分享」标记归零，
                          下一轮 `plan_engine` 按发布规则把仍在库的作品重新排进队列；
      ④ 按发布规则推送 —— sync → plan → dispatch，与自动调度**同一条链路、同一把锁**。

    为什么游标要全量归零：用户要的是「资源库里现在有的都重新排一遍」。
    只 rewind 被清理的作品不够 —— 兜不住外部删库、旧版本遗留这类不经过 rewind
    入口的卡死（`api_rebuild_first` 也是这个口径）。
    归零**不会造成重复首推**：`_SlotIndex` 照样挡住「已推且链接仍有效」与在途任务，
    这一点有 `test_v2117`/`test_v2116` 的端到端用例守着。

    `dry_run=True`（默认）只体检、不写库、不推送 —— 前端必须先给用户看数字再确认。
    **不物理删除任何行**（红线）：清理一律是状态归档，历史行与执行记录全保留。
    """
    live = _live_work_keys(db)
    task_pairs, task_total = _scan_invalid_tasks(db)
    link_pairs, link_total = _scan_invalid_links(db)
    # v2.1.31：顺手把「被队列行顶掉、永远等不到回写」的 submitted 孤儿任务捞出来。
    # 存量库里这些行会一直显示「已提交」不动，用户看起来就像「没推成功」。
    orphans = _orphan_submitted_tasks(db, limit=limit)
    version = _active_version(db)
    channels = list(db.scalars(select(Channel).where(Channel.enabled.is_(True))).all())

    plannable = blocked_pushed = blocked_active = 0
    works: list[SCLibraryVersion] = []
    if version and channels:
        slots = _SlotIndex(db, live_keys=live)
        works = [lv for lv in db.scalars(
            select(SCLibraryVersion).order_by(SCLibraryVersion.id)).all()
            if str(lv.work_key or "") in live]
        for lv in works:
            for channel in channels:
                dedup = f"{lv.work_key}:{channel.id}:first"[:255]
                state, _reuse = slots.state(
                    str(lv.work_key or ""), str(channel.chat_id or ""), dedup)
                if state in (SLOT_NONE, SLOT_INVALID):
                    plannable += 1
                elif state == SLOT_ACTIVE:
                    blocked_active += 1
                else:
                    blocked_pushed += 1

    win = _current_hour_window(db, version) if version else None
    cat_key = str(getattr(win, "category_key", "") or "")
    report: dict[str, Any] = {
        "ok": True,
        "dry_run": bool(dry_run),
        "works_in_library": len(live),
        "works_total": len(works),
        "channels": len(channels),
        "links_invalid": len(link_pairs),
        "links_total": link_total,
        "tasks_invalid": len(task_pairs),
        "tasks_total": task_total,
        "tasks_orphan": len(orphans),
        "plannable": plannable,
        "blocked_pushed": blocked_pushed,
        "blocked_active": blocked_active,
        "hour_allowed": bool(_current_hour_allowed(db, version)) if version else False,
        "hour_category": cat_key.split(":", 1)[-1] if cat_key else "",
    }
    if not version:
        report["reason"] = "当前没有生效中的计划版本：本次可以清理，但无法按发布规则排期"
    elif not channels:
        report["reason"] = "当前没有启用中的推送渠道：本次可以清理，但无法按发布规则排期"
    report["plan_ready"] = bool(version and channels)
    if dry_run:
        report["note"] = "仅体检：未改动任何数据，也未发起推送。"
        return report
    if not _CYCLE_LOCK.acquire(blocking=False):
        return {"ok": False, "dry_run": False,
                "reason": "自动调度正在进行，请稍后重试（避免与自动推送重复建链）"}
    # v2.1.18：清理与排期**解耦**。清理无效链接 / 失败任务不依赖计划版本，
    # 没有生效版本时也要照做（否则用户没配计划就永远清不掉这些垃圾）。
    # 只有「归零游标 + 按发布规则排期 + 推送」这一段才需要版本与渠道。
    try:
        tasks_done = cleanup_invalid_tasks(db)
        links_done = cleanup_invalid_links(db)
        # v2.1.31：孤儿 submitted 一并归档（不删行，只改状态 + 留痕原因）
        orphans_closed = _close_orphan_submitted(db, orphans)
        if orphans_closed:
            db.commit()
        cursors_reset = 0
        sync: dict[str, int] = {}
        plan: dict[str, int] = {}
        dispatched: dict[str, Any] = {}
        if version:
            cursors_reset = reset_plan_cursors(db, version.id)
            db.commit()
            sync = sync_library_versions(db)
            plan = plan_engine(db)
            dispatched = await dispatch_ready_tasks(db, limit=limit)
        else:
            db.commit()
    finally:
        _CYCLE_LOCK.release()

    report.update({
        "dry_run": False,
        "cleaned_links": int(links_done.get("cleaned") or 0),
        "cleaned_tasks": int(tasks_done.get("cleaned") or 0),
        "orphans_closed": int(orphans_closed),
        "marks_reset": (int(links_done.get("marks_reset") or 0)
                        + int(tasks_done.get("marks_reset") or 0)),
        "cursors_reset": cursors_reset,
        "sync": sync,
        "queued": plan,
        "dispatched": dispatched,
    })
    return report


@router.post("/repair")
def api_repair(body: Payload, db: Session = Depends(get_db)):
    """手动扫描（端点）。

    `dry_run` 默认 **True**：先出体检报告，前端把数字摊给用户看过、确认后
    再带 `dry_run=false` 调一次执行。默认值故意选安全侧 —— 这个动作会写库并真的
    发消息，不能因为前端漏传参数就默默执行。
    """
    import asyncio
    p = body.payload or {}
    return asyncio.run(repair_cycle(
        db,
        dry_run=bool(p.get("dry_run", True)),
        limit=max(1, int(p.get("limit") or 50)),
    ))


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


def _sibling_tasks(db: Session, task: SCPublishTask, *, status: str) -> list[SCPublishTask]:
    """同一「作品 × 渠道」下、指定状态的**其它**任务。

    v2.1.31（地道战「看着没推成功」根治）：队列行是按 `(resource_id, channel_id)`
    唯一的（`uq_resource_channel`），一条队列行同时只能记住**一个** task.id
    （`q.event_id = task.id`）。同一作品在一轮里既有 first 又有 update、又指向
    同一个渠道时，后提交的那条会把 `q.event_id` 顶掉，前一条任务就永远等不到回写
    ——真机证据：地道战 6 条任务只对应 2 条队列项，115 带 event_id=804、
    116 带 event_id=656，于是 655 / 802 双双停在 submitted 永不终结。

    这里按 (work_key, chat_id) 归组：渠道用清单项的 chat_id 认，任务本身没有
    渠道字段（`sc_publish_tasks` 只存 list_item_id）。
    """
    item_id = int(task.list_item_id or 0)
    if not item_id:
        return []
    chat_id = db.scalar(select(SCPushListItem.chat_id).where(SCPushListItem.id == item_id))
    if not chat_id:
        return []
    return list(db.scalars(
        select(SCPublishTask)
        .join(SCPushListItem, SCPublishTask.list_item_id == SCPushListItem.id)
        .where(SCPushListItem.chat_id == str(chat_id),
               SCPublishTask.work_key == str(task.work_key or ""),
               SCPublishTask.status == status,
               SCPublishTask.id != int(task.id))).all())


def mark_result(db: Session, task_id: int, success: bool, error: str = "") -> dict[str, Any]:
    task = db.get(SCPublishTask, task_id)
    if not task:
        return {"ok": False, "reason": "任务不存在"}
    now = utcnow()
    task.status = "published" if success else "failed"
    task.error = "" if success else error[:1000]
    task.updated_at = now
    if success:
        task.published_at = now
        task.lease_owner = ""
        task.lease_expires_at = None
    execution = db.scalar(
        select(SCPublishExecution).where(SCPublishExecution.task_id == task_id,
                                         SCPublishExecution.status == "running")
        .order_by(SCPublishExecution.id.desc())
    )
    if not execution:
        execution = SCPublishExecution(task_id=task_id, stage="send", status="running",
                                       started_at=now)
        db.add(execution)
    execution.status = "success" if success else "failed"
    execution.error = "" if success else error[:1000]
    execution.finished_at = now
    # v2.1.31：**成功时把同组被顶掉的任务一起收口**。
    # 队列行被后来的任务复用后，先前那条任务收不到回执；而这条消息其实已经把
    # 「这个作品在这个渠道」投出去了，所以它不是失败、也不该再投一次 —— 按
    # v2.1.6 已有的语义标成 `skipped`（页面显示「已跳过投递」）。
    # 只在成功时做：失败时队列行会原地重试，兄弟任务要留着等重试结果。
    closed = 0
    if success:
        for peer in _sibling_tasks(db, task, status="submitted"):
            peer.status = "skipped"
            peer.error = f"同作品同渠道已由任务 #{task.id} 发出，本次重复投递跳过"[:1000]
            peer.updated_at = now
            db.add(SCPublishExecution(task_id=peer.id, stage="send", status="skipped",
                                      started_at=now, finished_at=now,
                                      response_json=_jdump({"reason": "superseded_by",
                                                            "task_id": int(task.id)})))
            closed += 1
    db.commit()
    return {"ok": True, "status": task.status, "siblings_closed": closed}


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
    """推送趋势：published_at 成功计数 + 失败计数，按日/周/月分桶。

    真实来源 sc_publish_tasks；无数据时 series 为空（前端显示空态，不画假曲线）。

    v2.1.1 修复两处真机异常：
    ① 粒度区分——此前 day 与 week 都返回 "%Y-%m-%d"，week 实际没按周聚合，
       且月视图只有 1 个桶时前端会画成三角形尖峰。现改为：
       day→按小时（HH:00）、week→按天、month→按天（近 30 天）。
    ② 补齐空桶——只补最近窗口内的零值桶，折线才有走势而不是孤立尖点。
       补齐的桶是真实计数 0，不是编造数据。

    v2.1.19 修 bug2（用户原话：「刚才有推送，但推送趋势里面没有数据变化」）：
    ① **时区**：库里是 `DateTime(timezone=True)`，但 SQLite 不保存 tzinfo，
       读回来是 naive 的 **UTC** 时刻。此前 `dt.replace(tzinfo=sh)` 直接给它贴上
       +8 标签，等于不转换 —— 北京时间 08:01 的 8 条推送全被画进 00:00 桶。
       现统一走 `utils.as_utc(x).astimezone(sh)`（naive 一律视为 UTC，与全仓口径
       一致，PG 的 aware 值也不受影响）。
    ② **可视窗口**：此前「日」视图把**全部历史**按小时混桶，`sorted()` 之后
       字符串最大的 8 个恒为 16:00~23:00，而前端只画 `slice(-8)` —— 早上、
       中午推的东西永远进不了画面（这才是「推了看不到」的另一半原因，光修时区
       仍不够）。现在「日」= 今天 00:00 到**当前小时**的连续桶，当前小时必在
       末尾，前端取最后 8 个必然包含它。
    ③ **失败口径**：此前除 published 外一律记 failed，把在途（planned/building/
       ready/submitted/accepted/publishing）、用户取消、已归档全算成失败，
       制造出与真实故障无关的高峰。现在只有真正的失败才计入失败线。
    """
    sh = shanghai_tz()
    now_sh = utcnow().astimezone(sh)
    buckets: dict[str, dict[str, int]] = {}

    def blank() -> dict[str, int]:
        return {"published": 0, "failed": 0, "total": 0}

    def key_of(dt: datetime) -> str:
        # v2.1.19：naive = UTC（SQLite 丢 tzinfo），不能再直接 replace 成 +8
        local = utils.as_utc(dt).astimezone(sh)
        if period == "month":
            return local.strftime("%Y-%m-%d")
        if period == "week":
            monday = local - timedelta(days=local.weekday())
            return monday.strftime("%Y-%m-%d")
        # day 视图按小时展开：一天的推送分布才有曲线形状
        return local.strftime("%H:00")

    # 「日」视图：今天 00:00 → 当前小时，连续预置空桶（真实计数 0，不是编造）
    day_start = now_sh.replace(hour=0, minute=0, second=0, microsecond=0)
    day_labels: list[str] = []
    if period == "day":
        day_labels = [(day_start + timedelta(hours=h)).strftime("%H:00")
                      for h in range(int(now_sh.hour) + 1)]
        for lbl in day_labels:
            buckets[lbl] = blank()
        last_label = day_labels[-1]
    else:
        last_label = ""

    for t in db.scalars(select(SCPublishTask)).all():
        status = str(t.status or "")
        if status == "published":
            field = "published"
            moment = t.published_at or t.updated_at or t.created_at
        elif status in TREND_FAILED_STATUSES:
            field = "failed"
            moment = t.updated_at or t.created_at
        else:
            # 在途 / 已取消 / 已归档：既不是成功也不是失败，不进趋势
            continue
        if not moment:
            continue
        if period == "day":
            local = utils.as_utc(moment).astimezone(sh)
            if local < day_start:
                continue                      # 只统计今天，历史不混进同一小时桶
            label = local.strftime("%H:00")
            if label not in buckets:
                # 落在「今天 00:00 ~ 当前小时」之外（时钟回拨 / 脏数据 / 未来时刻）
                # 直接不画，不塞进任何桶 —— 静默归并会把时区这类错误掩盖成正常。
                continue
        else:
            label = key_of(moment)
        b = buckets.setdefault(label, blank())
        b[field] += 1
        b["total"] += 1

    if period == "day":
        # 桶已连续预置（含全 0 的时段），不再走下面的「单点补齐」逻辑。
        # 但今天一条数据都没有时必须回到空 series —— 前端靠 `series.length`
        # 显示「暂无发布记录」，给一排 0 会把它画成一条贴底的假曲线。
        if not any(b["total"] for b in buckets.values()):
            return {"period": period, "series": []}
        return {"period": period,
                "series": [{"label": k, **buckets[k]} for k in day_labels]}

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


def _mark_expect_release(db: Session, rows: list[dict[str, Any]]) -> None:
    """给还**没发出去**的队列行补一个「预计放行时刻」（v2.1.33）。

    真机 #3：「22 点推送的八仙，超时未推送（推送队列显示 22 点，实际排期时间是
    22 点 30）」。根因不在调度，而在**时间列取错了字段**：`scheduled_at` 写的是
    `utcnow()`——建这条任务的那一刻，不是「计划什么时候推」。队列拿它当推送时间
    显示，看上去自然永远是「已到点却没发」。

    这里按匀速节奏反推一个可解释的时刻：
        预计 = 时段起点 + (本时段已放行部数 + 该作品在待发队列里的序号) × interval
    已发出去的行不设这个字段，前端直接用实际发送时间。
    """
    now = utcnow()
    try:
        pace = _period_pace(db, now)
    except Exception:
        db.rollback()
        return
    if pace.get("due") is None or not int(pace.get("interval") or 0):
        return
    released = _period_released_works(db, pace)
    start_sh = pace["period_start"]
    interval = int(pace["interval"])
    # 待发作品的先后：按队列时间排；同一作品只算一次（它的多条渠道一起走）
    pending: dict[str, int] = {}
    for r in sorted(rows, key=lambda x: str(x.get("scheduled_at") or x.get("created_at") or "")):
        wk = str(r.get("work_key") or "")
        if not wk or wk in released or wk in pending:
            continue
        pending[wk] = len(released) + len(pending)
    for r in rows:
        wk = str(r.get("work_key") or "")
        idx = pending.get(wk)
        if idx is None:
            continue
        at = start_sh + timedelta(minutes=interval * idx)   # type: ignore[operator]
        r["expect_release_at"] = at.strftime("%Y-%m-%d %H:%M:%S")


def _aggregate_views(db: Session, limit: int = 12) -> dict[str, Any]:
    """推送三视图：历史(published)/队列(active)/失败(failed)真实任务行。

    v2.1.20：补 `cover`。此前这里只给了 `title`，前端三个表都渲染 `row.cover`，
    于是一律显示「无封面」—— 与「推送任务」「分享管理」两个模块的口径不一致
    （它们走 `api_tasks` / 分享记录列表，都有 cover）。现在统一走同一个
    `_work_posters`，四个地方的海报来源完全一致。

    v2.1.26（用户口径 5）：三视图**只给当天（上海自然日 0-24 点）的行**。
    用户口径是「推送历史 / 队列 / 失败都只看今天发生了什么」——跨天的旧记录
    在这三张卡里既没有行动价值，还会把卡片的注意力从「今天」拉走。
    历史与失败的完整记录仍在「推送任务」「分享记录」两页，不丢数据。

    同时补 `scheduled_at`（队列时间列的「计划时间优先」此前一直拿不到字段，
    后端白名单漏了它，前端只能退回创建时间）与 `schedule_kind`（排期类型）。
    """
    # 当天口径：上海自然日。库里的时间统一由 `_model_dict` 转成
    # `YYYY-MM-DD HH:mm:ss`（上海本地），所以这里直接比字符串前缀，不做时区换算。
    today_sh = utcnow().astimezone(shanghai_tz()).strftime("%Y-%m-%d")
    # 预取窗口：当天行按时间倒序一定排在最前，给 8 倍余量足够（够取满 limit 即可）。
    # 取不满就**给多少算多少**——绝不拿昨天的行把表填满。
    fetch_n = max(int(limit) * 8, 60)
    # 队列时间口径 = 计划时间优先、没有才退创建时间（与前端 `scQueueClock` 一致）。
    queue_clock = func.coalesce(SCPublishTask.scheduled_at, SCPublishTask.created_at)

    def rows_for(statuses: set[str] | None, *, clock, day_of,
                 with_expect: bool = False) -> list[dict[str, Any]]:
        """按 `clock`（该表时间列的同一口径）倒序取，再裁到当天。

        为什么按时间列排序而不是按 id：失败任务重试后 id 很小、`updated_at` 却是
        今天，按 id 排就会把它漏在预取窗口之外。排序键与筛选键必须是同一个。
        """
        q = select(SCPublishTask)
        if statuses:
            q = q.where(SCPublishTask.status.in_(statuses))
        rows = db.scalars(q.order_by(clock.desc()).limit(fetch_n)).all()
        out = [{k: v for k, v in _model_dict(r).items() if k in (
            "id", "work_key", "kind", "status", "link_state", "share_url",
            "error", "attempts", "created_at", "published_at", "updated_at",
            "scheduled_at")} for r in rows]
        # 当天过滤：`day_of` 读的就是时间列显示的那个字段，页面看到几点就是按几点判日。
        out = [r for r in out if str(day_of(r))[:10] == today_sh][:int(limit)]
        if not out:
            return []
        keys = [r.get("work_key") for r in out]
        titles = _work_titles(db, keys)
        posters = _work_posters(db, keys)
        # v2.1.22：三类标签（新入库 / 新上映 / 存量排期）与「推送任务」同源
        tags = _work_push_tags(db, keys)
        # v2.1.25（用户口径 2）：TMDB 分类单独成字段，与 `kind`（推送类型）
        # 彻底分开——前端一列放类型、一列放分类，再不许拼在同一格。
        cats = _task_category_names(db, [int(r.get("id") or 0) for r in out])
        for row in out:
            key = str(row.get("work_key") or "")
            row["title"] = titles.get(key, "")
            row["cover"] = posters.get(key, "")
            cat_key, cat_name = cats.get(int(row.get("id") or 0), ("", ""))
            row["category_key"] = cat_key
            row["category_name"] = cat_name
            raw_tags = tags.get(key, [])
            # v2.1.26：排期类型用**未裁剪**的判定（历史行也能看出当时按哪档排上）
            # v2.1.28：同时给 key，前端给「新入库」上绿色高亮时按 key 映射样式，
            #           不靠中文文案比对（文案改了高亮就静默失效）。
            kind_key, kind_label = _schedule_kind_pair(raw_tags)
            row["schedule_kind_key"] = kind_key
            row["schedule_kind"] = kind_label
            # v2.1.23（用户口径 1）：**已结束**的记录不显示「存量排期」——
            # 标签失效（新上映过期）后自然会变成存量排期，但一条早就推完的历史记录
            # 上写「存量排期」没有信息量，用户要的是「失效就消失」。
            row["tags"] = _tags_for_status(raw_tags, str(row.get("status") or ""))
        # v2.1.33：只给「推送队列」补预计放行时刻（历史/失败两张表看的是既成事实）
        if with_expect:
            _mark_expect_release(db, out)
        return out

    return {
        # 历史：按「推送成功时间」判日（没有就退最后更新时间，与时间列一致）
        "history": rows_for({"published"},
                            clock=func.coalesce(SCPublishTask.published_at,
                                                SCPublishTask.updated_at),
                            day_of=lambda r: r.get("published_at") or r.get("updated_at")),
        # 队列：按「计划推送时间」判日（没有就退创建时间）
        "queue": rows_for(ACTIVE_TASK_STATUS, clock=queue_clock,
                          day_of=lambda r: r.get("scheduled_at") or r.get("created_at"),
                          with_expect=True),
        # 失败：按最后一次失败时间判日（时间列显示的就是它）
        "failed": rows_for({"failed"}, clock=SCPublishTask.updated_at,
                           day_of=lambda r: r.get("updated_at")),
    }


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

@router.get("/forecast")
def api_forecast(db: Session = Depends(get_db)):
    """今日排期预测（v2.1.19）。

    为什么单独开一个端点而不塞进 `/overview`：`overview` 会调 `ensure_defaults`
    并把六张表全量聚合，本身已经重；排期预测还要扫作品快照和名额索引，合并会把
    整个仪表盘首屏拖慢。这里保持只读、按需加载，前端先渲染已有卡片再补排期。

    v2.1.24：取消分时段加载（用户口径 1a），一次返回今天剩余全部时段，
    前端用固定高度滚动区承载。参数全无 —— 少一层状态就少一类「和实跑不一致」。
    """
    return forecast_schedule(db)


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


def _work_categories(db: Session, work_keys) -> dict[str, str]:
    """按作品聚合键取 **TMDB 分类**的可读名（v2.1.25，用户口径 2）。

    分类的唯一来源 = TMDB genres（`sync_library_versions` 写入 `SCWorkCategory` 的
    归属，经 `SCCategory` 转可读名）。它与另外两个维度互不替代：

      · 推送类型 —— 首次发布 / 追更 / 手动推送（`SCPublishTask.kind`，看「为什么推」）；
      · 排期标签 —— 新上映 / 新入库 / 存量排期（`_work_push_tags`，看「排期身份」）；
      · 分类     —— TMDB 分类（本函数，看「是什么片」）。

    查不到返回空串（页面显示 `—`），绝不回退成「推送类型」去冒充分类——那正是
    用户看到「分类列里出现追更」的由来。也不回显 `genre:xxx` 这类内部键形态。
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
    rows = db.execute(
        select(SCWorkCategory.work_key, SCWorkCategory.category_key)
        .where(SCWorkCategory.work_key.in_(keys))).all()
    wanted = {str(ck or "") for _wk, ck in rows if str(ck or "")}
    names: dict[str, str] = {}
    if wanted:
        for cat in db.scalars(select(SCCategory).where(
                SCCategory.category_key.in_(wanted))).all():
            names[str(cat.category_key or "")] = str(cat.name or "")
    for wk, cat_key in rows:
        key = str(cat_key or "")
        if key:
            out[str(wk)] = _category_safe_name(key, names)
    return out


def _task_category_names(db: Session, task_ids: list[int]) -> dict[int, tuple[str, str]]:
    """按任务 id 取该任务所属的 **TMDB 分类**（key + 可读名）。

    v2.1.25（用户口径 2）：页面上的「分类」只有一个来源 —— TMDB 分类，落在
    推送清单项上（`SCPushListItem.category_key`）。**推送类型**（首次发布 / 追更 /
    手动推送）是另一个维度（`SCPublishTask.kind`），此前前端把两者拼在同一个
    「分类」格子里（`追更 · 剧情`），用户看到的就是「分类列既有追更又有题材」。

    查不到（清单项已清理 / 从未归类）返回空串，页面显示「未分类」——不猜。
    """
    out: dict[int, tuple[str, str]] = {}
    ids = [int(x) for x in (task_ids or []) if x]
    if not ids:
        return out
    rows = db.execute(
        select(SCPublishTask.id, SCPushListItem.category_key)
        .join(SCPushListItem, SCPublishTask.list_item_id == SCPushListItem.id)
        .where(SCPublishTask.id.in_(ids))).all()
    if not rows:
        return out
    wanted = {str(k or "") for _tid, k in rows if str(k or "")}
    names: dict[str, str] = {}
    if wanted:
        for cat in db.scalars(select(SCCategory).where(
                SCCategory.category_key.in_(wanted))).all():
            names[str(cat.category_key or "")] = str(cat.name or "")
    for tid, cat_key in rows:
        key = str(cat_key or "")
        out[int(tid)] = (key, _category_safe_name(key, names) if key else "")
    return out


def _work_posters(db: Session, work_keys: list[str]) -> dict[str, str]:
    """按作品聚合键取 TMDB 海报相对路径（供前端拼缩略图）。

    与 `_work_titles` 同口径（资源库版本 → 实时 Resource 回查），取不到返回空串
    （前端不渲染缩略图，不占位）。

    v2.1.25（用户口径 3）：**取数优先级整体翻过来 —— 以资源库实时值为准**。
    真机证据（银河护卫队2）：资源库里当前资源行是 `/1vN…`，分享中心却显示 `/9RT…`。
    原因：v2.1.20 的顺序是「先快照 metadata（版本倒序取第一个非空）→ 只有全空才
    回查资源表」。B 库重建换过三轮资源 id，旧快照里登记的 `/9RT…` 早已不是当前
    资源的海报，但它是非空的，于是**一命中就停**，实时值永远排不上队 → 两处不一致。

    现在的优先级（与「资源库是唯一真相源」一致）：
      ① 版本倒序 → 每个版本登记的 `resource_ids_json` 反查**实时** `Resource.poster_path`，
         越新的快照越优先（最新快照指的就是当前资源）；取到即用；
      ② 实时资源一条都取不到（资源被归档/删除）→ 退回快照 metadata 里登记的海报，
         仍按版本倒序取第一个非空（v2.1.20 的兜底语义保留，不至于突然变「无封面」）；
      ③ `file:` / `tmdb:` 前缀键没有快照，直接按文件 id / TMDB 编号回查资源表。

    v2.1.20 的历史修复不要回退：`seen` 锁死第一条版本、`work:` 前缀完全不回查——
    这两个缺陷曾导致「资源表有海报、页面却显示无封面」。
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

    # 资源库版本表：一次查全（版本倒序），同时收集「每部作品 → 该版本登记的资源 id」
    # 与「快照自己登记的海报」，供 ① 实时优先 / ② 快照兜底两段使用。
    rows = db.execute(
        select(SCLibraryVersion.work_key, SCLibraryVersion.version_no,
               SCLibraryVersion.metadata_json, SCLibraryVersion.resource_ids_json)
        .where(SCLibraryVersion.work_key.in_(keys))
        .order_by(SCLibraryVersion.version_no.desc())).all()
    # work_key → [快照登记的海报, ...]（版本倒序）
    snapshot_posters: dict[str, list[str]] = {}
    # 资源 id → [work_key, ...]；want_ids 顺序 = 版本倒序 + 版本内资源顺序（越靠前越新）
    id_to_keys: dict[int, list[str]] = {}
    want_ids: list[int] = []
    for wk, _ver, meta_json, ids_json in rows:
        key = str(wk or "")
        if key not in out:
            continue        # 不在本次查询范围（防御：脏行 / 大小写差异）
        meta = _jload(meta_json, {})
        poster = str(meta.get("poster_path") or "").strip() if isinstance(meta, dict) else ""
        snapshot_posters.setdefault(key, []).append(poster)
        raw_ids = _jload(ids_json, [])
        if not isinstance(raw_ids, list):
            continue
        for x in raw_ids:
            try:
                rid = int(x)
            except (TypeError, ValueError):
                continue
            if rid not in id_to_keys:
                id_to_keys[rid] = []
                want_ids.append(rid)
            if key not in id_to_keys[rid]:
                id_to_keys[rid].append(key)

    # ① 实时资源优先：按「最新快照 → 更旧快照」的资源顺序取第一个非空 poster。
    #    走 ORM 查询，自动受全局归档过滤约束——已归档资源不会把旧海报带回来。
    if want_ids:
        res_map = {r.id: r for r in db.scalars(
            select(Resource).where(Resource.id.in_(set(want_ids)))).all()}
        for rid in want_ids:
            res = res_map.get(rid)
            if res is None or not res.poster_path:
                continue
            for key in id_to_keys.get(rid, []):
                if not out.get(key):
                    out[key] = str(res.poster_path)

    # ② 实时取不到（资源已归档/已删）→ 退回快照登记值，版本倒序第一个非空。
    for key in keys:
        if out.get(key):
            continue
        for poster in snapshot_posters.get(key, []):
            if poster:
                out[key] = poster
                break

    missing = [k for k in keys if not out.get(k)]
    if not missing:
        return out

    # ③ `file:` / `tmdb:` 前缀键（没有版本快照）按文件 id / TMDB 编号回查资源表。
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
        for res in (db.scalars(select(Resource).where(
                Resource.tmdb_id.in_(wanted))).all() if wanted else []):
            if not res.poster_path:
                continue
            for key in missing:
                if key.startswith("tmdb:") and key.endswith(f":{res.tmdb_id}"):
                    out[key] = str(res.poster_path)
                    break
    return out


def _work_push_tags(db: Session, work_keys) -> dict[str, list[dict[str, str]]]:
    """按作品聚合键批量算「新入库 / 新上映 / 存量排期」三类标签（v2.1.22）。

    四个模块（今日推送排期 / 推送历史 / 推送队列 / 推送任务）共用本函数，
    前端只渲染 `tags`（`[{key, label}]`），不各自再算一套口径。

    数据来源（全部真实字段，不编造）：
      入库时间 —— `SCLibraryVersion.created_at` 取该作品**最早**一条（首次进库）。
                  快照缺失时退到成员资源 `Resource.ctime`（文件进网盘的时间）。
      上映日期 —— 成员资源 `Resource.release_date`（TMDB 首播/上映日期）取第一个非空，
                  按 `NEW_RELEASE_WINDOW_DAYS` 窗口判定。
    两项都不可判定时返回空列表（页面显示「—」），绝不默认标成「存量排期」。
    """
    out: dict[str, list[dict[str, str]]] = {}
    keys: list[str] = []
    for raw in work_keys or []:
        key = str(raw or "")
        if not key or key in out:
            continue
        out[key] = []
        keys.append(key)
    if not keys:
        return out

    # ① 入库时间 + 成员资源 id（同一 work_key 的多版本里取最早登记时间；
    #    成员 id 只取**最新版本**那一份，代表当前内容）。
    rows = db.execute(
        select(SCLibraryVersion.work_key, SCLibraryVersion.version_no,
               SCLibraryVersion.created_at, SCLibraryVersion.resource_ids_json)
        .where(SCLibraryVersion.work_key.in_(keys))
        .order_by(SCLibraryVersion.version_no.desc())).all()
    entry_at: dict[str, datetime] = {}
    member_ids: dict[str, list[int]] = {}
    for wk, _ver, created, ids_json in rows:
        key = str(wk or "")
        if key not in out:
            continue
        stamp = utils.as_utc(created)
        if stamp is not None:
            cur = entry_at.get(key)
            if cur is None or stamp < cur:
                entry_at[key] = stamp
        if key in member_ids:
            continue  # 已按 version_no desc 取过最新版本
        ids: list[int] = []
        for raw_id in _jload(ids_json, []) or []:
            try:
                ids.append(int(raw_id))
            except (TypeError, ValueError):
                continue
            if len(ids) >= PUSH_TAG_MEMBER_ID_LIMIT:
                break
        member_ids[key] = ids

    # ② 回查资源表：上映/完结信息 + 网盘创建时间（入库时间兜底）
    id_to_keys: dict[int, list[str]] = {}
    for key, ids in member_ids.items():
        for rid in ids:
            id_to_keys.setdefault(rid, []).append(key)
    rel_raw: dict[str, str] = {}
    air_raw: dict[str, str] = {}
    finish_raw: dict[str, str] = {}
    type_raw: dict[str, str] = {}
    ctime_at: dict[str, datetime] = {}
    if id_to_keys:
        for rid, rel, last_air, finish, mtype, ctime in db.execute(
                select(Resource.id, Resource.release_date, Resource.last_air_date,
                       Resource.finish_status, Resource.media_type, Resource.ctime)
                .where(Resource.id.in_(set(id_to_keys)))).all():
            for key in id_to_keys.get(int(rid), []):
                rel_text = str(rel or "").strip()
                if rel_text and not rel_raw.get(key):
                    rel_raw[key] = rel_text
                air_text = str(last_air or "").strip()
                if air_text and not air_raw.get(key):
                    air_raw[key] = air_text
                finish_text = str(finish or "").strip()
                if finish_text and not finish_raw.get(key):
                    finish_raw[key] = finish_text
                type_text = str(mtype or "").strip()
                if type_text and not type_raw.get(key):
                    type_raw[key] = type_text
                try:
                    ctime_int = int(ctime or 0)
                except (TypeError, ValueError):
                    ctime_int = 0
                if ctime_int > 0:
                    stamp = datetime.fromtimestamp(ctime_int, timezone.utc)
                    cur = ctime_at.get(key)
                    if cur is None or stamp < cur:
                        ctime_at[key] = stamp

    now_utc = utcnow()
    now_sh = now_utc.astimezone(shanghai_tz())
    today_sh = now_sh.date()
    for key in keys:
        tags: list[str] = []
        entered = entry_at.get(key) or ctime_at.get(key)
        # v2.1.23：= 「上海自然日的今天入库」。跨过今天 0 点就转存量，
        # 不再用「最近 7 天」——用户口径是当日有效。
        is_new_lib = bool(entered is not None
                          and entered.astimezone(shanghai_tz()).date() == today_sh)
        if is_new_lib:
            tags.append(PUSH_TAG_NEW_LIBRARY)
        verdict = _new_release_state(
            release_date=rel_raw.get(key, ""),
            last_air_date=air_raw.get(key, ""),
            finish_status=finish_raw.get(key, ""),
            media_type=type_raw.get(key, ""),
            now_sh=now_sh,
        )
        if verdict:
            tags.append(PUSH_TAG_NEW_RELEASE)
        # 两项都判不出（没快照也没上映信息）→ 留空，不猜成存量排期。
        if not tags and (entered is not None or verdict is not None):
            tags.append(PUSH_TAG_BACKLOG)
        out[key] = [{"key": t, "label": PUSH_TAG_LABELS[t]} for t in tags]
    return out


def _parse_dt_date(text: str) -> date | None:
    """把 `YYYY-MM-DD` 文本解析成 date；解析不了返回 None（绝不猜一个日期）。"""
    raw = str(text or "").strip()[:10]
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        return None


def _new_release_state(*, release_date: str, last_air_date: str,
                       finish_status: str, media_type: str,
                       now_sh: datetime) -> bool | None:
    """该作品现在算不算「新上映」。返回值三态：

      True  —— 仍在映/更新期内，标「新上映」；
      False —— 已过映/更新期，转存量；
      None  —— 任何可判定的日期都拿不到（无从判断，调用方据此决定是否给存量标签）。

    过期口径（用户原话）：「新上映至官方剧集更新完结（TMDB 数据）延后一天的 0 时转为存量」。
      · 剧集 + returning + 最后一集播出日在 RETURNING_STALE_DAYS 天内 → True；
      · 剧集 + returning + 最后一集播出日已超过该窗口     → False（长期停更，见下）；
      · 剧集 + returning + 无最后一集播出日               → True（无从判断，不猜）；
      · 剧集 + ended + 有最后一集播出日   → 以「播出日 + 1 天 0 点」为界，之前 True；
      · 剧集 + ended + 无最后一集播出日   → 只知道已完结、不知何时完结 → False（不猜日期）；
      · 电影 / 剧集状态未知              → 沿用 30 天上映窗口（电影没有连载概念）。
    任何情况下「还没上映」（日期在未来）都不算新上映。

    v2.1.56（真机 2026-11-28）：**returning 不再永不过期**。
    原先 `returning` 分支只看上映日、不看 `last_air_date`，等于无条件信任 TMDB 的
    `status` 字段。真机上《王保长后传》(tmdb 225764) 2005 年就播完、最后一集
    `2005-06-11`，TMDB 仍把它标成 Returning Series，于是这部 21 年前的老剧
    被永久算作「新上映」，压着一堆真新片抢排期。
    同一库里《王保长歪传》因为是 `ended` → 走完结分支 → 老播出日直接转存量，
    两部的唯一差别就是 TMDB 这个字段。
    兜底：声称仍在更新、但最后一集播出日距今超过 `RETURNING_STALE_DAYS` 天 →
    视为长期停更，按存量处理。`last_air_date` 拿不到时保持原行为（不猜）。
    """
    today = now_sh.date()
    rel = _parse_dt_date(release_date)
    is_tv = str(media_type or "").strip().lower() == "tv"
    state = str(finish_status or "").strip().lower()
    if is_tv and state == "returning":
        air = _parse_dt_date(last_air_date)
        # v2.1.56：停更兜底。air 为 None 时无从判断，保持「算新上映」。
        if air is not None and (today - air).days > RETURNING_STALE_DAYS:
            return False
        return not (rel is not None and rel > today)
    if is_tv and state == "ended":
        air = _parse_dt_date(last_air_date)
        if air is None:
            return False
        # 「更新完结延后一天的 0 时」→ 播出日次日 00:00 起转存量。
        return now_sh.date() <= air
    # 电影，或剧集状态未知：退回上映窗口口径。
    if rel is None:
        return None
    if rel > today:
        return False          # 还没上映
    return rel >= (now_sh - timedelta(days=NEW_RELEASE_WINDOW_DAYS)).date()


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
    # v2.1.22：三类标签（新入库 / 新上映 / 存量排期），与总览三表同源
    tags = _work_push_tags(db, [r.get("work_key") for r in items])
    # v2.1.25（用户口径 2）：TMDB 分类（与推送类型 `kind` 分开输出）
    cats = _task_category_names(db, [int(r.id) for r in rows])
    for row in items:
        key = str(row.get("work_key") or "")
        row["title"] = titles.get(key, "")
        row["cover"] = posters.get(key, "")
        cat_key, cat_name = cats.get(int(row.get("id") or 0), ("", ""))
        row["category_key"] = cat_key
        row["category_name"] = cat_name
        # v2.1.23（用户口径 1）：终态行不显示「存量排期」，口径见 `_tags_for_status`
        row["tags"] = _tags_for_status(tags.get(key, []), str(row.get("status") or ""))
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
    # v2.1.22：详情与列表同源展示三类标签
    # v2.1.23：终态行不显示「存量排期」（口径见 `_tags_for_status`）
    item["tags"] = _tags_for_status(
        _work_push_tags(db, [key]).get(key, []), str(item.get("status") or ""))
    # v2.1.25（用户口径 2）：详情页同样把「推送类型」与「TMDB 分类」分开给
    cat_key, cat_name = _task_category_names(db, [int(task_id)]).get(
        int(task_id), ("", ""))
    item["category_key"] = cat_key
    item["category_name"] = cat_name
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
    """取消推送任务 / 取消分享链接：作废这一次推送，并**释放首推名额**。

    v2.1.16：此处以前只把 `status` 改成 `cancelled`，`link_state` 仍是
    `active`，`share_url` 也留着——于是名额判定照旧认为「已推成功、链接还有效」，
    作品从此再也排不进队列。用户口径是「取消推送任务 / 取消分享链接，就应该把
    对应任务归到初始状态，而不是一直在后台记着已经首推」。

    现在一并作废链接态：`link_state='cleaned'`（历史行与 share_url 保留，供追溯）。
    该次推送因此判为无效，名额回收；作品按排期轮到它时重新进队列（不是立刻原地
    重排——取消后 5 秒又冒出来等于没取消）。
    """
    task = db.get(SCPublishTask, task_id)
    if not task:
        return {"ok": False, "reason": "任务不存在"}
    task.status = "cancelled"
    # 这一份分享已被用户作废：群里那条消息指向的链接不再可用，记录判为无效。
    if str(task.share_url or "").strip():
        task.link_state = CLEANED_LINK_STATE
    task.next_retry_at = None
    task.lease_owner = ""
    task.lease_expires_at = None
    # 名额已释放 → 把队列游标回退到这部作品之前，让它下一轮优先补排
    # （否则取消要等自愈补扫才回来，用户看到的就是「取消了不生效」）。
    rewind_cursor_for_work(db, str(task.work_key or ""))
    db.commit()
    return {"ok": True, "status": task.status,
            "note": "该次推送已作废，名额已释放；作品会在下一轮排期轮到它时重新进队列。"}


def _live_work_keys(db: Session) -> set[str]:
    """当前资源库中**真实存在**的作品聚合键集合（实时扫描派生）。

    用于判断一条历史任务/分享链接背后的作品是否已从资源库移除——
    这是「无效」判定的唯一事实依据，不依赖任何人工标记或示例值。

    v2.1.16：只取 `work_group_key` 真正用到的 6 个窄列，不加载整行实体。
    本函数每个调度周期（5 秒）都会跑一次，实体全量加载会把 overview / genres /
    dir_chain_json 这些大文本字段白白读进内存。
    """
    rows = db.execute(select(
        Resource.file_id, Resource.path_hint, Resource.share_dir_name,
        Resource.media_title, Resource.tmdb_id, Resource.media_type)).all()
    keys: set[str] = set()
    for row in rows:
        keys.add(utils.work_group_key(row))
    return keys


def rewind_cursor_for_work(db: Session, work_key: str) -> int:
    """把队列游标回退到 `work_key` 这部作品之前，让它在下一轮**优先补排**。

    v2.1.16 的关键一环。队列是按 `SCLibraryVersion.id` 升序依次生成的，游标记录
    「队头已消化到哪儿」。如果用户取消任务 / 取消分享链接 / 清理了这部作品，
    名额虽然被释放（见 `_SlotIndex`），但游标已经越过它 —— 不主动回退的话，
    它要等很久才会被「自愈补扫」重新捡起，用户看到的现象就是「取消了也不回来」。

    回退到 `min(该作品所有快照 id) - 1`：下一轮 `id > 游标` 正好从它开始。
    只动游标，**不删任何记录**。
    """
    ids = list(db.scalars(select(SCLibraryVersion.id).where(
        SCLibraryVersion.work_key == work_key)).all())
    if not ids:
        return 0
    target = max(0, min(int(i) for i in ids) - 1)
    moved = 0
    for c in db.scalars(select(SCCategoryCursor)).all():
        if int(c.last_library_version_id or 0) > target:
            c.last_library_version_id = target
            c.updated_at = utcnow()
            moved += 1
    if moved:
        db.flush()
    return moved


def reset_share_marks_for_works(db: Session, work_keys: set[str], *,
                                reason: str = "") -> int:
    """把指定作品的资源行上的**已分享标记**归零，返回受影响行数。

    用户口径（原话）：「让清理推送任务等不只是清空列表，而是真正将任务和
    推送标记一并清理了」。

    「推送标记」实际分散在三处，此前 cleanup 只清了第一处：

      ① `SCPublishTask.status`   —— 清理端点已处理
      ② `SCPushListItem.status`  —— v2.1.14 起已联动
      ③ `Resource.share_status` / `share_id` / `share_url` —— **一直没清**

    第 ③ 处不清的后果：旧队列判据（`_valid_legacy_push_pairs`）看的就是它，
    `share_status` 停在 `active` 就认为「这条推送还生效」，名额释放了也排不回来。
    所以清理完必须把这里一并归零，否则用户看到的就是「清了列表还是没任务」。

    只改标记：不动 B 库文件、不删任何行（红线）。历史靠 `SCPublishTask`
    上保留的 `share_url` 与执行记录追溯，不受影响。
    """
    keys = {str(k) for k in work_keys if str(k)}
    if not keys:
        return 0
    touched = 0
    # 默认查询自动排除归档资源行，正好——已归档的作品不需要再动标记。
    for res in db.scalars(select(Resource)).all():
        if utils.work_group_key(res) not in keys:
            continue
        already_clean = (str(res.share_status or "") == "not_created"
                         and not str(res.share_id or "").strip()
                         and not str(res.share_url or "").strip())
        if already_clean:
            continue
        res.share_status = "not_created"
        res.share_id = ""
        res.share_url = ""
        touched += 1
    if touched:
        db.flush()
    return touched


def invalidate_for_works(
    db: Session,
    work_keys: set[str] | None = None,
    *,
    reason: str = "",
    reset_cursors: bool = True,
) -> dict[str, int]:
    """v2.1.17：B 库被清空 / 重整时，**联动作废**分享中心里在途的推送。

    为什么需要它
    ------------
    `/api/mirror/restructure`（v2.1.11）只联动了**旧版**分享中心
    （`ShareTask` / `ShareRecord`），完全没有碰 v2 的这四张表。于是 B 库清空之后，
    v2 这边还留着一批 planned / building 的任务 —— dispatch 会继续把它们捞起来，
    逐条去建分享链接，每一条都因「B 库文件不存在」失败。

    有了 v2.1.16 的 30 分钟退避，至少不再每 3 秒刷屏；但**存量任务仍会一波一波
    白跑**，把 dispatch 每轮那几个名额占满，真正能推的任务被挤到后面。这就是
    「有推送任务却不建链接」的最后一环。

    做法（**只改状态，绝不物理删除任何行**）
    ------------------------------------
      ① 未终结的任务（planned/building/ready/failed/…）→ `invalid`（无效推送）；
         有 `share_url` 的一律置 `link_state=cleaned`（链接确实坏了，页面要如实显示）。
         已经 `invalid` 的保留原状态与原失败原因，但**清掉退避与租约**——它们的退避
         理由是「建链暂时失败」，B 库清空后这个理由已不成立；不清会让 `_SlotIndex`
         判成「冷却中」继续占名额。
      ② 已 `published` 的**状态保持不动**（那是历史事实，「已推送」里还要看得到），
         但同样把 `link_state` 置 `cleaned` —— 它的分享指向的文件已经没了。
      ③ 已 `cancelled` 的跳过：那是用户主动取消，语义要留着。
      ④ 清单项**不动**：`_SlotIndex` 会把「有历史行但无一有效」判成 `SLOT_INVALID`，
         名额自动回收，作品恢复后走 revive 复用旧行。
      ⑤ 分类游标归零，让下一轮从队头完整重扫。重建时 `plan_engine` 的
         `live_keys` 会挡住「已从资源库移除」的作品，不会又造一批僵尸任务。

    `work_keys=None` = 全量（整个 B 库被清空）；`scope=dir` 时传受影响的作品键集合。
    返回各计数，供审计与页面回显。
    """
    now = utcnow()
    reason_text = (reason or "B 库内容已被手动清理，本次推送作废")[:300]

    stmt = select(SCPublishTask).where(
        SCPublishTask.status.notin_(("published", "cancelled")))
    if work_keys is not None:
        keys = [str(k) for k in work_keys if str(k)]
        if not keys:
            return {"tasks_invalidated": 0, "links_cleaned": 0,
                    "cursors_reset": 0, "marks_reset": 0}
        stmt = stmt.where(SCPublishTask.work_key.in_(keys))
    tasks = list(db.scalars(stmt).all())

    tasks_invalidated = 0
    links_cleaned = 0
    for task in tasks:
        if (str(task.share_url or "").strip()
                and str(task.link_state or "") != CLEANED_LINK_STATE):
            task.link_state = CLEANED_LINK_STATE
            links_cleaned += 1
        if str(task.status or "") != INVALID_TASK_STATUS:
            # 只在状态真的变化时改，`invalid` 的保留它原本的失败原因（可追溯）
            task.status = INVALID_TASK_STATUS
            task.error = reason_text
            tasks_invalidated += 1
        # 已经 invalid 的也要清退避与租约：它们的退避理由是「建链暂时失败」，
        # 而 B 库清空后这个理由已经不成立。留着未来的 next_retry_at 会让
        # _SlotIndex 判成「冷却中」继续占名额，作品恢复后还要白等一个窗口。
        task.next_retry_at = None
        task.lease_owner = ""
        task.lease_expires_at = None
        task.updated_at = now

    # published 的任务单独处理：状态不动，只把已失效的链接标出来。
    published_stmt = select(SCPublishTask).where(
        SCPublishTask.status == "published",
        SCPublishTask.share_url != "")
    if work_keys is not None:
        keys = [str(k) for k in work_keys if str(k)]
        if keys:
            published_stmt = published_stmt.where(SCPublishTask.work_key.in_(keys))
    for task in db.scalars(published_stmt).all():
        if str(task.link_state or "") != CLEANED_LINK_STATE:
            task.link_state = CLEANED_LINK_STATE
            task.updated_at = now
            links_cleaned += 1

    cursors_reset = 0
    if reset_cursors:
        for c in db.scalars(select(SCCategoryCursor)).all():
            if int(c.last_library_version_id or 0) != 0:
                c.last_library_version_id = 0
                c.last_work_key = ""
                c.status = "idle"
                c.updated_at = now
                cursors_reset += 1

    # 资源行上的「已分享」标记也要一并归零：旧队列判据看的就是它，
    # 留着 active 就等于把作品继续判成「已推送过」，名额释放了也排不回来。
    affected = {str(t.work_key or "") for t in tasks if str(t.work_key or "")}
    if work_keys is not None:
        affected |= {str(k) for k in work_keys if str(k)}
    marks_reset = reset_share_marks_for_works(db, affected)

    if tasks_invalidated or links_cleaned or cursors_reset or marks_reset:
        db.flush()
    return {"tasks_invalidated": tasks_invalidated, "links_cleaned": links_cleaned,
            "cursors_reset": cursors_reset, "marks_reset": marks_reset}


def _is_permanent_error(error: str) -> bool:
    """错误文案是否表明**重试也不会成功**（资源已不存在一类）。"""
    text = str(error or "")
    return any(marker in text for marker in PERMANENT_ERROR_MARKERS)


def _scan_invalid_tasks(db: Session, only_ids: list[int] | None = None
                        ) -> tuple[list[tuple[SCPublishTask, str]], int]:
    """扫描「可清理的无效推送任务」。**只读，不写库**。

    v2.1.18：把判据从端点里提出来 —— 「清理无效推送任务」按钮和「手动扫描」的
    体检必须用同一套判据，否则两个入口给出的数字会不一致（用户没法判断信谁）。

    返回 `(可清理项 [(task, 原因)], 候选总数)`。

    「无效」判据（全部实时派生，不编造）：
      ① 任务已被用户取消（cancelled）——调度器不再捡起它；
      ② 任务已被判为「无效推送」（invalid：建链失败 / 资源已移除 / 快照缺失）；
      ③ 任务失败，且失败原因表明**资源已从资源库移除**（重试也永远不会成功）；
      ④ 任务失败，且它背后的作品在当前资源库已查无此物。
    临时性失败（网络抖动 / 限流）**不在此列**，留着重试。
    """
    live = _live_work_keys(db)
    q = select(SCPublishTask).where(SCPublishTask.status.in_(
        {"failed", "cancelled", INVALID_TASK_STATUS}))
    if only_ids:
        q = q.where(SCPublishTask.id.in_(only_ids))
    rows = db.scalars(q.order_by(SCPublishTask.id.desc()).limit(5000)).all()
    out: list[tuple[SCPublishTask, str]] = []
    for task in rows:
        reason = ""
        if str(task.status or "") == "cancelled":
            reason = "已取消"
        elif str(task.status or "") == INVALID_TASK_STATUS:
            reason = "无效推送"
        elif _is_permanent_error(task.error):
            reason = "资源已从资源库移除"
        elif str(task.work_key or "") not in live:
            reason = "作品已不在资源库"
        if reason:
            out.append((task, reason))
    return out, len(rows)


def _scan_invalid_links(db: Session, only_ids: list[int] | None = None
                        ) -> tuple[list[tuple[SCPublishTask, SCPushListItem, str]], int]:
    """扫描「可清理的无效分享链接」。**只读，不写库**。与 `api_cleanup_share_records`
    共用同一套判据（理由同 `_scan_invalid_tasks`）。

    返回 `(可清理项 [(task, item, 原因)], 候选总数)`。

    **幂等性（v2.1.19 修 bug1）**：已被归档（`link_state=cleaned`）的记录直接跳过。
    少了这一步，判据 ③④ 会把同一批记录**每次**都重新扫出来 —— 清理只写
    `link_state`，不动 `status` / `error` / `work_key`，而 ③④ 恰好只看这三样。
    用户点一次「手动扫描」报 22 条、再点还是 22 条，就是这个原因：
    每点一次都白清一遍，还连带重复回退游标、重复清「已分享」标记。

    注意 `total` 语义**不变**（范围内带链接的任务总数，含历史归档行）——
    前端拿它当「扫描了 N 条链接」的分母；只在这里跳过判定，不缩小分母。
    """
    live = _live_work_keys(db)
    q = (select(SCPushListItem, SCPublishTask)
         .join(SCPublishTask, SCPublishTask.list_item_id == SCPushListItem.id)
         .where(SCPublishTask.share_url.isnot(None), SCPublishTask.share_url != ""))
    if only_ids:
        q = q.where(SCPublishTask.id.in_(only_ids))
    rows = db.execute(q.order_by(SCPublishTask.id.desc()).limit(5000)).all()
    out: list[tuple[SCPublishTask, SCPushListItem, str]] = []
    for item, task in rows:
        state = str(task.link_state or "unknown")
        if state == CLEANED_LINK_STATE:
            # 已归档：名额已回收、标记已归零，不重复清理。
            continue
        reason = ""
        if state in ("suspicious", "inactive"):
            reason = "链接已判失效"
        elif state == INVALID_TASK_STATUS:
            # link_state 也可能被写成 "invalid"（INVALID_LINK_STATES 含它）。
            reason = "无效推送"
        elif _is_permanent_error(task.error):
            reason = "资源已从资源库移除"
        elif str(task.work_key or item.work_key or "") not in live:
            reason = "作品已不在资源库"
        if reason:
            out.append((task, item, reason))
    return out, len(rows)


def cleanup_invalid_tasks(db: Session, only_ids: list[int] | None = None) -> dict[str, Any]:
    """清理无效推送任务：归档（status=cleaned），**不物理删除行**。

    归档后默认列表不再展示（`include_archived=true` 可查回），
    历史行与执行记录保留，不影响推送历史与统计口径。
    """
    pairs, total = _scan_invalid_tasks(db, only_ids)
    cleaned = 0
    details = []
    cleaned_works: set[str] = set()
    for task, reason in pairs:
        task.status = CLEANED_TASK_STATUS
        task.error = f"已清理：{reason}"
        task.lease_owner = ""
        task.lease_expires_at = None
        task.updated_at = utcnow()
        cleaned += 1
        if task.work_key:
            cleaned_works.add(str(task.work_key))
        details.append({"id": task.id, "reason": reason})
    # v2.1.14：清单项状态跟着任务走。此前只把任务标 cleaned、清单项留 planned，
    # 「发布清单」里因此仍挂着一排「待推送」，首推名额也被这些僵尸项占死
    # （见 first_dedup_plan）。只在**该项下所有任务都已清理**时才标 cleaned，
    # 不误伤仍有在途任务的清单项；历史行一律保留，不做物理删除。
    # 注意：本项目 Session 是 autoflush=False，必须先 flush，否则下面的
    # 「还有没有未清理任务」查询读不到上面刚改的 cleaned，清单项永远不会被更新。
    db.flush()
    item_ids = {int(t.list_item_id or 0) for t, _r in pairs if int(t.list_item_id or 0) > 0}
    for iid in sorted(item_ids):
        left = db.scalar(select(SCPublishTask.id).where(
            SCPublishTask.list_item_id == iid,
            SCPublishTask.status != CLEANED_TASK_STATUS).limit(1))
        if left:
            continue
        it = db.get(SCPushListItem, iid)
        if it is not None:
            it.status = CLEANED_TASK_STATUS
    # v2.1.16：清理掉的作品名额已释放 → 回退游标，让它下一轮优先回到队列。
    # v2.1.17：资源行上的「已分享」标记也一并归零。只清任务不清标记的话，
    # 旧队列判据仍把作品当成「已推送过」，名额释放了照样排不回来 ——
    # 用户说的「清了列表还是没任务」就是这一条。
    marks_reset = reset_share_marks_for_works(db, cleaned_works)
    for wk in sorted(cleaned_works):
        rewind_cursor_for_work(db, wk)
    db.commit()
    return {"ok": True, "cleaned": cleaned, "skipped": total - cleaned,
            "marks_reset": marks_reset, "items": details}


def cleanup_invalid_links(db: Session, only_ids: list[int] | None = None) -> dict[str, Any]:
    """清理无效分享链接：归档（link_state=cleaned），**不物理删除行**。

    只归档本地记录：**不删 B 库资源、不删远端分享、不删历史行**
    （与 share_link.cleanup_link 的既有语义一致）。
    """
    pairs, total = _scan_invalid_links(db, only_ids)
    cleaned = 0
    details = []
    affected_works: set[str] = set()
    for task, item, reason in pairs:
        # 作废链接态 → 该次推送判为无效、名额回收，作品可重新排队。
        task.link_state = CLEANED_LINK_STATE
        task.updated_at = utcnow()
        cleaned += 1
        wk = str(task.work_key or item.work_key or "")
        if wk:
            affected_works.add(wk)
        details.append({"id": task.id, "reason": reason})
    # v2.1.16：链接作废后名额已回收 → 回退游标，让作品下一轮优先回到队列。
    # v2.1.17：资源行的「已分享」标记同步归零，否则旧队列判据仍认为推送生效。
    marks_reset = reset_share_marks_for_works(db, affected_works)
    for wk in sorted(affected_works):
        rewind_cursor_for_work(db, wk)
    db.commit()
    return {"ok": True, "cleaned": cleaned, "skipped": total - cleaned,
            "marks_reset": marks_reset, "items": details}


@router.post("/tasks/cleanup")
def api_cleanup_tasks(body: Payload, db: Session = Depends(get_db)):
    """清理无效推送任务（端点）：判据与执行都见 `cleanup_invalid_tasks`。"""
    p = body.payload or {}
    only_ids = [int(x) for x in (p.get("ids") or []) if str(x).isdigit()]
    return cleanup_invalid_tasks(db, only_ids or None)


def reset_plan_cursors(db: Session, version_id: int) -> int:
    """把该计划版本下所有分类的队列游标归零，让队列从队头重新消化一遍。

    v2.1.16 起游标是「队头已消化到哪儿」，不是水位线：让作品失效的动作都会主动
    回退它（`rewind_cursor_for_work`），所以正常情况下不需要手动归零。
    本函数保留为**显式兜底**——用于兜住不经过那些入口的失效（外部删库、旧版本遗留
    数据、批量导入老片后想按入库顺序重排一次）。
    归零**不删任何数据**，也不放宽判定：已推且链接仍有效 / 仍在途的作品照样被挡。
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

    逐作品 × 逐渠道调用同一个 `_SlotIndex.state()`（与 plan_engine 同一条裁决），
    因此这里给出的数字与真实执行后的结果同源，不是估算。

    v2.1.16：口径随名额判定一起改——
      - `blocked_active`  ← 名额被**在途**任务占用（正在推送，本轮不该重排）；
      - `blocked_pushed`  ← 已推成功且分享链接仍有效（重排会重复首推）；
      - `would_create` ← 从未建过；
      - `would_revive` ← 历史记录已作废（取消 / 清理 / 链接作废 / 资源移除），可重排。
    """
    version = _active_version(db)
    if not version:
        return {"ok": False, "reason": "当前没有生效中的计划版本"}
    channels = list(db.scalars(select(Channel).where(Channel.enabled.is_(True))).all())
    if not channels:
        return {"ok": False, "reason": "还没有启用中的推送渠道"}
    db.flush()
    live_keys = _live_work_keys(db)
    slots = _SlotIndex(db, live_keys=live_keys)
    # 只统计**当前仍在资源库**的作品：已重建/已清理的作品不在候选池里，
    # 把它们算进「可重排」会让预演数字与真实执行结果对不上。
    versions = [lv for lv in db.scalars(
        select(SCLibraryVersion).order_by(SCLibraryVersion.id)).all()
        if str(lv.work_key or "") in live_keys]
    out: dict[str, Any] = {"ok": True, "works": len(versions), "channels": len(channels),
                           "would_create": 0, "would_revive": 0,
                           "blocked_active": 0, "blocked_pushed": 0}
    for lv in versions:
        for channel in channels:
            chat_id = str(channel.chat_id or "")
            dedup = f"{lv.work_key}:{channel.id}:first"[:255]
            state, _reuse = slots.state(str(lv.work_key or ""), chat_id, dedup)
            if state == SLOT_NONE:
                out["would_create"] += 1
            elif state == SLOT_INVALID:
                out["would_revive"] += 1
            elif state == SLOT_ACTIVE:
                out["blocked_active"] += 1
            else:
                out["blocked_pushed"] += 1
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
    """清理无效分享链接（端点）：判据与执行都见 `cleanup_invalid_links`。"""
    p = body.payload or {}
    only_ids = [int(x) for x in (p.get("ids") or []) if str(x).isdigit()]
    return cleanup_invalid_links(db, only_ids or None)


@router.post("/category-schedules/adopt-ai-all")
def api_adopt_schedule_ai_all(body: Payload, db: Session = Depends(get_db)):
    """批量采纳推荐时段：把建议窗口写入所选分类。

    v2.1.6：
      - 单一时间段（2 小时颗粒度）口径，不再写周维度网格；
      - **每个分类都有建议**（实测优先，无实测用默认时间表兜底），
        不再出现「无建议跳过」导致采纳率极低；
      - 支持传 category_keys 只处理勾选的分类；不传 = 全部。

    v2.1.54（用户口径：根据分类作品数量、结合热门分类的观影热门时段做优化，
    使排期不会大面积落空）：
      - **库存为 0 的分类不再参与采纳**。线上 27 个分类里 15 个零库存，
        给它们写时段等于制造空档 —— 采纳后那 15 段必然一个作品都排不出来。
        跳过的分类带可读原因回显，不是静默丢弃。
        前提是「库里有货」：全库库存都是 0（资源库没同步过）时不做这个过滤，
        照 v2.1.6 的口径全量采纳，否则点一次批量采纳会什么都不发生。
      - 库存多的分类除了主时段（实测观影时段优先）还拿**额外时段**
        （库存分配里除主段以外的段），一天可以推多个时段。
      - 采纳完做一次**覆盖校验**：算出哪些小时一个启用分类都盖不到，
        用库存最多的分类补上，并把补位结果如实回显。

    静默保护开启时，落在静默区间的建议时段会退回库存分配的段（已避开静默），
    而不是直接跳过这个分类 —— 跳过就等于让它彻底排不出东西。
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
    stock = _category_stock(db)
    # 库存过滤只在**库里确实有货**时生效。全库零库存有两种情况：
    # 资源库还没同步过（此时用户点采纳就是为了先有一张建议表，不能什么都不做），
    # 或者真的全空（那就更没有"让哪个分类让位"的依据）。
    # 这两种情况一律按 v2.1.6 口径全量采纳 —— 否则按钮点了没反应，
    # 又退回到当年"有的按钮是灰的"那种体验。
    stock_known = sum(int(v or 0) for v in stock.values()) > 0
    prefs = _category_preference(db)
    fallback_map = _preference_windows(db, quiet)
    cycle = _jload(version.config_json, {}).get("cycle_plan") or {}
    win = None
    try:
        _ws = int(cycle.get("window_start") or 0)
        _we = int(cycle.get("window_end") or 24)
        if 0 <= _ws < _we <= 24:
            win = (_ws, _we)
    except (TypeError, ValueError):
        win = None
    # 库存分配（含多段）——额外时段的来源，也用作实测窗口撞静默时的退路
    groups = _segments_by_stock(prefs, _stock_segment_pool(quiet, win), stock)
    adopted = 0
    skipped = 0
    closed = 0
    skipped_items: list[dict[str, str]] = []
    for row in rows:
        name = _category_display_name(row.category_key)
        st = int(stock.get(row.category_key) or 0)
        if stock_known and st <= 0:
            # v2.1.55（用户口径 3）：无作品的分类**自动关闭排期**而不是只跳过。
            # 关闭才是真的让位 —— 关闭的行不占任何小时，那几个小时会落到有货的分类上。
            if row.enabled:
                row.enabled = False
                row.auto_disabled = 1
                row.updated_at = utcnow()
            # 无论之前是不是已经关着，本次采纳之后它都是关闭状态 →
            # 都计进 closed，页面那句「N 个没有作品已自动关闭」才对得上账。
            closed += 1
            skipped += 1
            skipped_items.append({
                "name": name,
                "reason": ("资源库里这个分类没有待排作品，已自动关闭它的排期 —— "
                           "空出来的时段分给有作品的分类；等它有作品了会自动打开")})
            continue
        sug_start, sug_end, sug_source = _category_suggest_window(
            db, row.category_key, fallback_map.get(row.category_key, FULL_DAY_WINDOW))
        # v2.1.6：起点 0 合法，用来源标记判定有没有建议。
        if not sug_source:
            skipped += 1
            skipped_items.append({"name": name, "reason": "拿不到这个分类的观影时段"})
            continue
        grp = [tuple(s) for s in (groups.get(row.category_key) or [])]
        if quiet and _window_overlaps_quiet((sug_start, sug_end), quiet):
            # 实测时段撞静默 → 退回库存分配的段（那一套已经避开静默）
            if not grp:
                skipped += 1
                skipped_items.append({
                    "name": name, "reason": "实测时段落在静默保护里，库存分配也没给出可用时段"})
                continue
            sug_start, sug_end = grp[0]
            sug_source = "stock"
        # 额外时段 = 库存分配里除主段以外的段（一天多时段推送）
        extra = [s for s in grp if s != (int(sug_start), int(sug_end))]
        row.window_start, row.window_end = int(sug_start), int(sug_end)
        row.extra_windows = _jdump([list(s) for s in extra]) if extra else ""
        row.ai_window_start, row.ai_window_end = int(sug_start), int(sug_end)
        row.week_windows = ""
        row.ai_adopted = True
        # 采纳是用户手工动作 → 该行不再被系统自动刷新覆盖。
        row.window_auto = False
        row.updated_at = utcnow()
        adopted += 1

    # ---- 覆盖校验：采纳完还有哪些小时一个启用分类都盖不到 ----
    hours_all: set[int] = set(range(24))
    if win:
        hours_all = {h for h in hours_all if int(win[0]) <= h < int(win[1])}
    if quiet:
        hours_all = {h for h in hours_all if not _in_quiet_hour(h, quiet)}
    covered: set[int] = set()
    all_rows = db.scalars(select(SCCategorySchedule).where(
        SCCategorySchedule.plan_version_id == version.id)).all()
    for row in all_rows:
        if not row.enabled:
            continue
        covered |= set(_schedule_open_hours(row, quiet, win))
    gaps = sorted(hours_all - covered)
    filled_hours: list[int] = []
    filled_by = ""
    if gaps and stock_known:
        # 用库存最多的分类补：它的作品最多，补上去最不容易再次落空
        top_key = max((k for k, v in stock.items() if int(v or 0) > 0),
                      key=lambda k: int(stock[k]), default="")
        top_row = next((r for r in all_rows if r.category_key == top_key), None)
        if top_row is not None:
            extra = _sched_extra_windows(top_row)
            for h in gaps:
                seg = (h, min(24, h + HOUR_SEGMENT_HOURS))
                if seg not in extra:
                    extra.append(seg)
            top_row.extra_windows = _jdump([list(s) for s in extra])
            top_row.updated_at = utcnow()
            filled_hours = gaps
            filled_by = _category_display_name(top_key)
    db.commit()
    return {"ok": True, "adopted": adopted, "skipped": skipped, "closed": closed,
            "total": len(rows), "skipped_items": skipped_items[:60],
            # v2.1.54：采纳后仍有空档的小时 + 由谁补上，如实回显。
            # 空列表 = 这次采纳后每个可推小时都至少有一个有货的分类。
            "gaps": gaps, "filled_hours": filled_hours, "filled_by": filled_by}


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


def _category_stock(db: Session) -> dict[str, int]:
    """每个分类**在库待排**的作品数（时段分配的权重来源）。

    与 `build_even_plan` / `plan_engine` / `forecast_schedule` 同一份在库口径
    （`_category_backlog`）：只认每部作品的最新快照 + `live_keys`。
    拿不到就返回空 —— 绝不编一个库存出来，否则时段会被分给没货的分类。

    v2.1.54：这是「段数按库存」的唯一数据源。空档的根因就是分配完全不看它：
    线上 27 个分类里 15 个零库存，却各自占着一个 2 小时段。
    """
    try:
        return _category_backlog(db, _live_work_keys(db))
    except Exception:
        return {}


def _apply_stock_enabled(rows: list[SCCategorySchedule], stock: dict[str, int],
                         *, known: bool) -> dict[str, int]:
    """v2.1.55：按库存自动开关排期行 —— 无作品关闭、有作品打开。

    用户口径：「当分类无作品时，可通过不启用排期来减少空窗（如无作品的分类，
    或 ai 批量采纳自动关闭无作品的分类，有作品时自动打开，需将空余的时段
    分配到其他分类上去）」。空出来的小时怎么分，由 `_segments_by_stock`
    只给有货的分类分段来完成 —— 这两件事是一套：**先让零库存的让位，再把段分完**。

    三个必须守住的边界：

      ① `known=False`（全库零库存 = 资源库还没同步过）时**一个都不许关**。
         否则新装用户打开页面看到 27 个分类全关，等于排期功能整个是坏的。
      ② 只自动打开**系统自己关的**（`auto_disabled=1`）。用户手工关掉的行
         `auto_disabled=0`，即使有货也不许擅自打开 —— 那是用户的决定。
      ③ 只碰 `enabled` / `auto_disabled`，**不动时段、不动上限、不动已建任务**。

    返回 `{"closed": n, "opened": n}`，供端点如实回显。
    """
    out = {"closed": 0, "opened": 0}
    if not known:
        return out
    for row in rows:
        has = int(stock.get(row.category_key) or 0) > 0
        if not has:
            if row.enabled:
                row.enabled = False
                row.auto_disabled = 1
                row.updated_at = utcnow()
                out["closed"] += 1
            continue
        if int(getattr(row, "auto_disabled", 0) or 0) and not row.enabled:
            row.enabled = True
            row.auto_disabled = 0
            row.updated_at = utcnow()
            out["opened"] += 1
    return out


def _preference_windows(db: Session,
                        quiet: tuple[int, int] | None = None) -> dict[str, tuple[int, int]]:
    """当前偏好排名对应的分类默认时段窗口（分类 key → (起, 止)）。

    v2.1.6：改用「热门白天 / 冷门夜间」的 2 小时粒度分配（见
    _assign_category_windows），不再是 v2.1.2 的「高峰独占段」。

    v2.1.54：**把库存一起传下去**（`_category_stock`）。段数按库存算、
    段位按偏好序算 —— 只有一边看库存，空档就还在。库存为空（新库 / 全库没货）
    时自动退回旧口径（一分类一段），行为与改前一致。

    quiet 传入静默保护区间时，落进静默的段会被整段跳过——默认时间表
    **必须避开静默时段**，否则「一键恢复默认」会把分类排到推不出去的小时。
    """
    return _assign_category_windows(_category_preference(db), quiet, _category_stock(db))


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

    v2.1.54（用户口径：结合各分类作品数量均匀排期，空档太多，货多的多时段推送）：
      ⑤ 时段分配改为**段数按库存、段位按热门**：库存 0 的分类不再白占一个 2 小时段
         （线上 27 个分类里 15 个零库存，各占一段 → 那 15 段必然空档）；
         库存多的分类拿多段，多出来的段写进 `extra_windows`。
    """
    rows = {r.category_key: r for r in db.scalars(
        select(SCCategorySchedule).where(SCCategorySchedule.plan_version_id == version_id)).all()}
    quiet = _quiet_window(_cycle_plan_of(db, version_id))
    # v2.1.55（用户口径 5）：建行/回填一律用**内置单段**时段（热门白天 / 冷门夜间）。
    # 之前这里按库存把多出来的段也写进额外时段 —— 于是「恢复默认」刚把多时段清掉，
    # 页面下一次刷新（GET 会调本函数）又把它写回来了，用户视角就是「默认点了没用」。
    # 多时段只由「一键 AI 调度」和「批量采纳」产生，本函数不再制造它。
    win_map = _assign_category_windows(_category_preference(db), quiet)
    added = False
    changed = False
    for cat in _tmdb_genre_catalog():
        key = cat["category_key"]
        ws, we = win_map.get(key) or FULL_DAY_WINDOW
        extra_json = ""
        ws, we, hl, dl = _default_schedule_for(cat.get("media_type") or "", (ws, we))
        existing = rows.get(key)
        if existing is None:
            row = SCCategorySchedule(plan_version_id=version_id, category_key=key,
                                     window_start=ws, window_end=we, hourly_limit=hl,
                                     daily_limit=dl, ai_adopted=False, enabled=True,
                                     window_auto=True, extra_windows=extra_json)
            db.add(row)
            added = True
            continue
        # v2.1.55：带着额外时段的行是「AI 调度 / 批量采纳」的产物，本函数不碰它 ——
        # 否则每次打开页面都会把它抹回单段，AI 调的排期白调。
        if str(existing.extra_windows or "").strip():
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
                    or str(existing.extra_windows or "") != extra_json
                    or not existing.enabled):
                existing.window_start, existing.window_end = ws, we
                existing.extra_windows = extra_json
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
                or int(existing.window_end or 0) != we
                or str(existing.extra_windows or "") != extra_json):
            existing.window_start, existing.window_end = ws, we
            existing.extra_windows = extra_json
            existing.updated_at = utcnow()
            changed = True
    if added or changed:
        db.flush()
    all_rows = list(db.scalars(select(SCCategorySchedule).where(
        SCCategorySchedule.plan_version_id == version_id)).all())
    # v2.1.55：库存决定启停 —— 无作品的分类自动关闭、让出时段；有作品时自动打开。
    stock_now = _category_stock(db)
    if _apply_stock_enabled(all_rows, stock_now,
                            known=sum(int(v or 0) for v in stock_now.values()) > 0)["closed"]:
        changed = True
    if changed:
        db.flush()
    return all_rows


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
            h = utils.as_utc(moment).astimezone(shanghai_tz()).hour
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
        local = utils.as_utc(moment).astimezone(shanghai_tz())
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
    # v2.1.55（用户口径 5）：恢复默认 = 回到**内置的分类推送时段**（热门白天 / 冷门夜间，
    # 一分类一段），**不再**按库存把多出来的段写进额外时段。
    # 「同分类多时段」是 AI 调度/批量采纳的产物，不该由「恢复默认」制造出来 ——
    # 用户点默认，期待的就是那张熟悉的一张分类一个时段的老表。
    win_map = _assign_category_windows(
        _category_preference(db), _quiet_window(_cycle_plan_of(db, version.id)))
    applied = 0
    updated = 0
    for row in rows:
        # v2.1.6：只处理勾选的分类（不传 = 全部）。
        if wanted and row.category_key not in wanted:
            continue
        cat = next((c for c in _tmdb_genre_catalog()
                    if c["category_key"] == row.category_key), None)
        win = win_map.get(row.category_key) or FULL_DAY_WINDOW
        ws, we, hl, dl = _default_schedule_for((cat or {}).get("media_type") or "", win)
        if not force and row.category_key not in untouched_keys:
            continue
        changed = (int(row.window_start or 0) != ws
                   or int(row.window_end or 0) != we
                   or bool(str(row.extra_windows or ""))
                   or not row.enabled)
        row.window_start, row.window_end = ws, we
        row.extra_windows = ""      # v2.1.55：恢复默认不带多时段
        # v2.1.6：分类级「每小时上限 / 日上限」已移除，统一由周期计划管理约束，
        # 因此这里不再回写分类级上限。
        row.enabled = True
        # v2.1.1：一键应用默认 = 系统托管 → 该行重新可被系统自动刷新。
        row.window_auto = True
        row.updated_at = utcnow()
        applied += 1
        if changed:
            updated += 1
    # v2.1.55：恢复默认之后同样要按库存启停 —— 有货的打开，没货的关掉让出时段。
    stock_now = _category_stock(db)
    toggled = _apply_stock_enabled(rows, stock_now,
                                   known=sum(int(v or 0) for v in stock_now.values()) > 0)
    db.commit()
    return {"ok": True, "applied": applied, "updated": updated, "total": len(rows),
            "closed": toggled["closed"], "opened": toggled["opened"]}


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
    # v2.1.55（用户口径 5）：单行「默认」也只回到**内置单段**时段，
    # 之前按库存分给它的额外时段一并清掉 —— 「默认」不该等于「多时段」。
    quiet_now = _quiet_window(_cycle_plan_of(db))
    win = _assign_category_windows(
        _category_preference(db), quiet_now).get(row.category_key) or FULL_DAY_WINDOW
    ws, we, hl, dl = _default_schedule_for(media_type, win)
    row.window_start, row.window_end = ws, we
    row.extra_windows = ""
    # 恢复默认 = 交回系统托管，让后续分配能继续刷新它
    row.window_auto = True
    row.ai_adopted = False
    # v2.1.55：启停跟着库存走 —— 没货的恢复默认后仍是关闭状态（不白占时段），
    # 有货的打开。用户手工关掉的行（auto_disabled=0）此处不动。
    stock_now = _category_stock(db)
    _apply_stock_enabled([row], stock_now,
                         known=sum(int(v or 0) for v in stock_now.values()) > 0)
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
    # v2.1.55：库存（在库待排作品数）—— 表里那一列的数就取这里，与调度同源。
    stock_map = _category_stock(db)
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
        # v2.1.54：额外时段结构化下发（库存多的分类一天占多个时段）。
        # 覆盖 `_model_dict` 给的原始 JSON 字符串 —— 前端只认这个列表。
        d["extra_windows"] = [list(x) for x in _sched_extra_windows(r)]
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
        # v2.1.55：这一行是不是「因无作品被系统自动关闭」的（中性名，页面据此提示）。
        d["auto_closed"] = bool(int(getattr(r, "auto_disabled", 0) or 0)) and not r.enabled
        # v2.1.55（用户口径）：表里加一列「库存」，数字直接取资源库在库待排作品数。
        # 与「一键 AI 调度 / 批量采纳 / 均匀排期」**同一份判据**（`_category_backlog`），
        # 页面另给一份同谓词明细可点开（`/sc/category-works`），
        # 不允许只甩一个数字出来（v2.1.36 红线）。
        d["stock"] = int(stock_map.get(r.category_key) or 0)
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


@router.get("/category-works")
def api_category_works(category_key: str = "", limit: int = 200,
                       db: Session = Depends(get_db)):
    """某个分类「在库待排」的作品明细（v2.1.55）。

    红线（v2.1.36）：页面每个计数都必须有一份**同谓词**明细可点开。
    表里新加的「库存」列是个数字，所以这里用**与 `_category_backlog` 完全相同的
    谓词**（最新快照 + live_keys）把作品列出来 —— 两处对不上就说明有一处是错的。

    只读：不触发同步、不改任何状态。
    """
    key = str(category_key or "").strip()
    if not key:
        return {"ok": False, "reason": "缺少分类", "items": [], "total": 0}
    live = _live_work_keys(db)
    if not live:
        return {"ok": True, "items": [], "total": 0}
    latest_ids = select(func.max(SCLibraryVersion.id)).group_by(SCLibraryVersion.work_key)
    rows = db.execute(
        select(SCLibraryVersion.work_key, SCLibraryVersion.media_type)
        .join(SCWorkCategory, SCWorkCategory.work_key == SCLibraryVersion.work_key)
        .where(SCLibraryVersion.id.in_(latest_ids))
        .where(SCLibraryVersion.work_key.in_(live))
        .where(SCWorkCategory.category_key == key)
        .order_by(SCLibraryVersion.work_key)
    ).all()
    cap = max(1, min(int(limit or 200), 1000))
    items = [{"work": str(wk).split(":", 1)[-1],
              "media_type": str(mt or "")}
             for wk, mt in rows[:cap]]
    return {"ok": True, "items": items, "total": len(rows),
            "shown": len(items), "category": _category_display_name(key)}


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
    # v2.1.54：额外时段（同一分类一天可以占多个时段）。
    # 只接受 [[起,止],…]，逐对夹到 0-24、并逐对校验静默冲突 ——
    # 静默区间的时段绝不写进去（否则该分类会在用户关掉的时段被推送）。
    extra_cleaned: list[list[int]] | None = None
    if "extra_windows" in p:
        raw_extra = p.get("extra_windows")
        extra_cleaned = []
        if isinstance(raw_extra, (list, tuple)):
            for pair in raw_extra:
                if not isinstance(pair, (list, tuple)) or len(pair) < 2:
                    continue
                try:
                    e_ws = max(0, min(23, int(pair[0])))
                    e_we = max(e_ws + 1, min(24, int(pair[1])))
                except (TypeError, ValueError):
                    continue
                if quiet and _window_overlaps_quiet((e_ws, e_we), quiet):
                    return {"ok": False,
                            "reason": _quiet_conflict_reason((e_ws, e_we), quiet)}
                if [e_ws, e_we] not in extra_cleaned:
                    extra_cleaned.append([e_ws, e_we])
    row = db.scalar(select(SCCategorySchedule).where(
        SCCategorySchedule.plan_version_id == version_id,
        SCCategorySchedule.category_key == category_key))
    if not row:
        row = SCCategorySchedule(plan_version_id=version_id, category_key=category_key)
        db.add(row)
    row.window_start = ws
    row.window_end = we
    if extra_cleaned is not None:
        row.extra_windows = _jdump(extra_cleaned) if extra_cleaned else ""
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
        if row.enabled:
            # v2.1.55：用户手工把它打开了 → 清掉「系统自动关闭」标记，
            # 否则有货时系统会以为这还是它关的（虽然结果一样，但状态会说不清）。
            row.auto_disabled = 0
    # v2.1.4：只有当提交的时段**确实不等于偏好推荐值**时，才判定为「用户手工设定」。
    # 此前无条件置 window_auto=False，而前端「保存全部」会逐行提交**所有**行
    # （含用户根本没碰过的行），于是点一次保存全部，全表都变成「手工设定」，
    # 此后「按偏好分配时段」对所有行永久失效——配置时段与偏好档位推荐值从此脱钩，
    # 这正是用户反馈「配置时间段默认值应该与偏好档位推荐值一样」的真正根因。
    # v2.1.6：比较基准必须用**与默认时间表同口径**的窗口（含静默避让），
    # 否则用户保存的就是系统刚推的默认时段，却被判成「手工设定」。
    #
    # v2.1.54：基准同时包含**额外时段**。库存多的分类系统会给它 2-3 段，
    # 前端「保存全部」原样提交回来时必须仍判成「系统托管」，
    # 否则点一次保存就全表变手工，以后分配再也不刷新（v2.1.4 踩过的同一个坑）。
    #
    # v2.1.55：系统现在有**两种**合法形态，必须都认：
    #   口径 A —— 内置单段（建行 / 恢复默认的产物，无额外时段）
    #   口径 B —— 按库存多段（AI 调度 / 批量采纳的产物）
    # 只认一种的话，另一个入口刚写好的表一保存就整表变「手工设定」。
    prefs = _category_preference(db)
    candidates: list[tuple[tuple[int, int], list[list[int]]]] = [
        (tuple(_assign_category_windows(prefs, quiet).get(category_key)
               or FULL_DAY_WINDOW), [])]
    segs = _assign_category_segments(prefs, quiet, _category_stock(db)).get(category_key)
    if segs:
        candidates.append((tuple(segs[0]),
                           sorted([int(s), int(e)] for s, e in segs[1:])))
    extra_now = extra_cleaned if extra_cleaned is not None else _sched_extra_windows(row)
    extra_now_sorted = sorted([int(s), int(e)] for s, e in extra_now)
    if not any((ws, we) == main and extra_now_sorted == extra
               for main, extra in candidates):
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
    # v2.1.6：单分类每日上限——分类排期表移除「日上限」列后，
    # 分类级日限流统一由周期计划约束（此前无处可配，分类级日限流是死配置）。
    "category_day_max": 0,
}

# 已经彻底下线的周期计划键（v2.1.45 下线周/月休止窗口）。这些键**不再有默认值、
# 不再参与任何判定**；保留这份清单只为两件事：
#   ① 接口回显时把它们过滤掉，别让页面上看不见的旧值看起来还管用；
#   ② 保存周期计划时把它们从 config_json 里清掉，避免以后有人恢复部分代码时
#      被一条谁也不知道存在的旧开关卡住（用户口径：别被规则卡住而不自知）。
# 注意：周/月**限额**键（`week_max`/`month_max`）不在此列 —— v2.1.42 起它们就
# 已不接受写入、也没有任何判定分支，`test_v2135` 的契约是「代码里连字面量都别出现」，
# 这里不去碰它（它们是纯数字、不会拦任何东西，不属于「看不见的开关」）。
RETIRED_CYCLE_KEYS = (
    "week_rest_enabled", "week_rest_day", "week_rest_hour",
    "month_rest_enabled",
)


@router.get("/cycle-plan")
def api_cycle_plan(db: Session = Depends(get_db)):
    """周期计划（小时 / 日度配额 + 静默保护），持久化于活动版本 config_json.cycle_plan。

    v2.1.45：周度、月度两档已整体下线，回显里不再出现任何周/月键（历史值也不回显）。
    """
    version = _active_version(db)
    if not version:
        return {**CYCLE_PLAN_DEFAULTS, "version_id": 0}
    cfg = _jload(version.config_json, {})
    saved = {k: v for k, v in (cfg.get("cycle_plan") or {}).items()
             if k not in RETIRED_CYCLE_KEYS}
    return {**CYCLE_PLAN_DEFAULTS, **saved, "version_id": version.id}


@router.post("/cycle-plan")
def api_save_cycle_plan(body: Payload, db: Session = Depends(get_db)):
    version = _active_version(db)
    if not version:
        return {"ok": False, "reason": "无活动计划版本"}
    cfg = _jload(version.config_json, {})
    saved = {k: body.payload.get(k, v) for k, v in CYCLE_PLAN_DEFAULTS.items() if k in body.payload}
    cycle = {**CYCLE_PLAN_DEFAULTS, **(cfg.get("cycle_plan") or {}), **saved}
    # v2.1.45：周 / 月相关键（限额 + 休止窗口）已彻底下线，写回时一并清掉。
    # 它们的值已经不影响任何判定，留着只会让「为什么今天不排期」变成悬案。
    for _retired in RETIRED_CYCLE_KEYS:
        cycle.pop(_retired, None)
    # 数值夹逼，防非法输入破坏调度
    for k in ("hour_max", "category_hour_max", "category_day_max", "day_max"):
        cycle[k] = max(0, int(cycle.get(k) or 0))
    for k in ("quiet_start", "window_start"):
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
            # v2.1.42：周 / 月限额已下线，规则体检不再回显这两项（配了也不生效）。
            "quota_scope": "day_only",
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
