import asyncio
import contextvars
import html
import json
import logging
import re
import threading
import time
from contextlib import asynccontextmanager, contextmanager
from datetime import timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from . import (admin_session, ai_assist, async_scan_state, bot_listener,
               contribution_restore, guangya_auth, guangya_sms, log_retention,
               mirror, mirror_batch, runtime_config, share_center_v2, tg_stats,
               tg_stats_api, tg_stats_service, tgto, tmdb)
from .config import VALID_DURATIONS, settings
from .db import DEFAULT_TEMPLATE, TEMPLATE_PRESETS, Base, SessionLocal, get_db, json_dumps, migrate, engine
from .guangya import (GuangYaError, GuangYaOpenAPI, GuangYaShareClient, GuangYaWebFiles,
                      WebFileError, create_share_parsed)
from .identify_batch import count_pending, mark_attempted, select_identify_batch
from .models import (AuditLog, AuthToken, Bot, Channel, ContributionRestore, DiscoveredChat,
                     DistributionEvent, MediaRequest, MessageTemplate, MirrorDir, MirrorFile,
                     MirrorMedia, MirrorRule, MirrorTask, QueueItem, Resource, ResourceScanBatch,
                     ResourceVersion, Rule, ScanCursor, ScanTask, UserContribution)
from .publisher import ensure_share, process_queue
from .scanner import run_scan
from .security import decrypt_secret, encrypt_secret, is_masked_secret, mask_secret
logger = logging.getLogger(__name__)

from .utils import (as_utc, is_video_file, normalize_work_name, parse_media_name,
                    retry_after_seconds, scrub_upstream_internals, source_query_hint,
                    standard_media_name, star_media_name, strip_tmdb_tag, utcnow,
                    work_episode_numbers, work_group_key)

scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")

# v2.1.9：批量识别时的 TMDB 限流「分帧排队」阈值（秒）。
# 场景配额是 60 次/60 秒、最小间隔 0.2 秒。批量识别一轮最多 500 条，
# 一次性全打出去必然在几十条后撞限流——此前撞上就把整批写成 failed。
# 口径：等配额最多等 3 秒（够跨过最小间隔与窗口滑动），
# 超过则**中止本轮**，剩余资源留到下一轮（下一轮自动重选待重试资源）。
IDENTIFY_RATE_LIMIT_WAIT_MAX = 3.0


class _TimedMirrorLock:
    """v0.9.6：带持有者/时间戳的镜像互斥锁，防止「假持锁」误报 409。

    v0.8.9 起用 threading.Lock 互斥；正常路径 try/finally 都会释放，但有两种情况
    会让锁看起来「一直被占用」而界面上又看不到任务：
      1) 后台 `_job_rename_pending` 每 120 秒拿锁做长改名，用户点「立即同步」被 409，
         但后台任务不在界面显示，用户以为「没有任务在跑」；
      2) 进程异常 / 任务被强制中断时 finally 未执行，锁残留到下次请求。
    这里在 acquire 时记录 owner + 起始时间，超过 STALE_SECONDS 视为失效自动放行，
    并把占用信息暴露给 /api/mirror/status，前端可提示「后台改名任务运行中」。
    仍用 threading.Lock 做真正的互斥（定时 job 与手动端点上下文不同、测试会换事件循环）。

    v0.9.21：STALE_SECONDS 语义从「持有起点」改为「距最后心跳」。长任务（镜像长剧/
    批量改名）持有几十分钟属正常，旧实现只要超过 30 分钟就判残留、可能在真持有者
    仍推进时被错误放行（并发抢写）；而卡死任务又恰好因为没有心跳被当成「还在正常跑」
    直到 30 分钟才回收。改为持有方在推进阶段 heartbeat() 打点，超过 STALE_SECONDS
    无进展才回收——既保护正常长任务、又快速释放真卡死的锁。held_seconds 仍保留总
    持有时长（前端/契约不变），另暴露 idle_seconds 供诊断「多久没进展」。

    阶段四 B-03：v0.9.21 的残留接管只覆盖元数据、未真正取得底层锁所有权，且
    threading.Lock 无 owner 概念——误判残留（旧持有者其实还活着但无心跳）时，
    旧持有者 finally release() 会误放掉新持有者的锁。改为：接管分支先强制释放
    底层锁再阻塞式重新 acquire（新持有者真正持有）；并用 contextvars  fencing
    token 标记代次——每次 acquire 递增，release()/heartbeat() 只接受与当前代次
    一致的操作，过期持有者迟到的 release/heartbeat 自动失效（fencing token 语
    义）。ContextVar 按 asyncio Task 隔离，恰好区分不同任务里的新旧持有者；跨
    线程调用取不到 token 时退回旧的无条件行为，兼容 threading 场景与旧测试。
    """
    STALE_SECONDS = 30 * 60  # 距最后心跳超过 30 分钟无进展才判残留（长剧持有本身不受限）

    # 当前持锁代次：acquire 成功写入本 Task 上下文，release/heartbeat 校验。
    _fence_ctx: contextvars.ContextVar = contextvars.ContextVar("mirror_lock_fence", default=None)

    def __init__(self):
        self._lock = threading.Lock()
        self._owner = ""
        self._since = 0.0
        self._last_beat = 0.0
        self._fence = 0  # 代次计数，每次 acquire（含接管）递增
        # v2.1.11：等待队列（先来先得）。定时任务不再「抢不到就跳过」，
        # 而是排队等上一轮彻底结束再跑本轮（用户口径：上一轮没跑完，下一轮
        # 不启动；上一轮结束了才按新的定时启动新一轮）。
        self._meta = threading.Lock()
        self._waiters: dict[int, float] = {}  # 票据 → 开始等待的时刻
        self._wait_seq = 0

    def _register_waiter(self) -> int:
        with self._meta:
            self._wait_seq += 1
            self._waiters[self._wait_seq] = time.monotonic()
            return self._wait_seq

    def _unregister_waiter(self, ticket: int) -> None:
        with self._meta:
            self._waiters.pop(ticket, None)

    def _is_earliest_waiter(self, ticket: int) -> bool:
        """先来先得：只有等得最久的那个才去抢锁，避免后来者插队。"""
        with self._meta:
            if not self._waiters:
                return True
            mine = self._waiters.get(ticket)
            if mine is None:
                return True
            return all(t == ticket or v > mine for t, v in self._waiters.items())

    async def acquire_async(self, owner: str = "", timeout: float = 3600.0) -> bool:
        """等待式获取（定时任务用）：上一轮没跑完就排队等，等到了再跑本轮。

        用户口径（2026-11-23）：「复制、改名、翻译、识别直到入库资源库」算**一轮**，
        上一轮没跑完，下一轮定时任务不启动；上一轮彻底结束后，等着的这一轮
        继续跑（**不丢轮次**），按最新定时执行下一轮。

        实现要点：
          - 用 `await asyncio.sleep` 轮询，**绝不阻塞事件循环**——同步等待会占住
            loop 线程，让所有 job 与 API 一起饿死（六轮死锁教训）；
          - 等待者按先来先得排队，避免每次都是最后到的那个先抢到；
          - 超时放弃本轮（下一轮定时自然会重来），绝不无限堆积；
          - 锁被持有期间 `acquire()` 内部仍按「距最后心跳」判残留并强制接管，
            真卡死的上一轮不会把后续轮次永久堵死。
        """
        ticket = self._register_waiter()
        deadline = time.monotonic() + max(0.0, float(timeout))
        try:
            while True:
                if self._is_earliest_waiter(ticket):
                    if self.acquire(blocking=False, owner=owner):
                        return True
                if time.monotonic() >= deadline:
                    logger.warning("[pipeline] 等待上一轮任务结束超时（%.0fs），本轮放弃：%s",
                                   timeout, owner or "未命名任务")
                    return False
                await asyncio.sleep(1.0)
        finally:
            self._unregister_waiter(ticket)

    def _valid_fence(self) -> bool:
        """调用方是否当前持有者；跨线程无 token 时按旧语义放行。"""
        token = self._fence_ctx.get()
        return token is None or token == self._fence

    def acquire(self, blocking: bool = False, owner: str = "") -> bool:
        if self._lock.acquire(blocking=blocking):
            self._fence += 1
            self._fence_ctx.set(self._fence)
            self._owner = owner or "镜像任务"
            self._since = time.monotonic()
            self._last_beat = self._since
            return True
        # 拿不到锁：距最后心跳已超时才判残留、强制接管。用最后心跳而非持有起点，
        # 正常长任务持续打点不会被误回收；真卡死的锁按「无进展时长」快速释放。
        if self._idle_seconds() >= self.STALE_SECONDS:
            logger.warning("[mirror] 镜像锁距最后心跳超过 %ss 无进展（owner=%s，总持有 %ss），"
                           "判定残留并强制接管", self.STALE_SECONDS, self._owner,
                           int(self._held_seconds()))
            # B-03：v0.9.21 覆盖元数据但没拿到底层锁，旧持有者仍在 threading.Lock 层面
            # 拥有它，迟到的 finally release() 会误放新持有者的锁。先强制释放残留锁，
            # 再阻塞式重新 acquire 拿到真正所有权；blocking 兜住释放瞬间的并发抢锁。
            try:
                self._lock.release()
            except RuntimeError:
                pass  # 底层锁已释放（元数据残留）时也继续接管
            self._lock.acquire(blocking=True)
            self._fence += 1
            self._fence_ctx.set(self._fence)
            self._owner = "镜像任务(残留回收)"
            self._since = time.monotonic()
            self._last_beat = self._since
            return True
        return False

    def release(self) -> None:
        # B-03：过期持有者迟到 release 不得清掉新持有者元数据/误放其锁。
        if not self._valid_fence():
            logger.warning("[mirror] 忽略过期持有者的 release（fence %s != 当前 %s）",
                           self._fence_ctx.get(), self._fence)
            return
        self._owner, self._since, self._last_beat = "", 0.0, 0.0
        self._fence_ctx.set(None)
        try:
            self._lock.release()
        except RuntimeError:
            pass  # 未被持有时重复 release，忽略

    def set_owner(self, owner: str) -> None:
        """拿锁后补充持有者名称（保留 acquire(blocking=False) 原字面调用，兼容旧契约测试）。"""
        if self._since and self._valid_fence():
            self._owner = owner

    def heartbeat(self) -> None:
        """持有方在每个推进阶段打点，证明「还活着且在进展」，重置残留计时。

        B-03：过期持有者迟到的 heartbeat 不得重置新持有者的空闲计时。
        """
        if self._since and self._valid_fence():
            self._last_beat = time.monotonic()

    def _held_seconds(self) -> float:
        return max(0.0, time.monotonic() - self._since) if self._since else 0.0

    def _idle_seconds(self) -> float:
        """距最后心跳的秒数；无锁时 0。STALE_SECONDS<=0 时视为立即过期（兼容旧测试）。"""
        if not self._last_beat:
            return float("inf") if self.STALE_SECONDS <= 0 else 0.0
        return max(0.0, time.monotonic() - self._last_beat)

    def info(self) -> dict:
        held = self._lock.locked()
        with self._meta:
            waiting = len(self._waiters)
        return {"busy": held, "owner": self._owner if held else "",
                "held_seconds": int(self._held_seconds()) if held else 0,
                "idle_seconds": int(self._idle_seconds()) if held else 0,
                "fence": self._fence,
                # v2.1.11：正在排队等上一轮结束的任务数（>0 说明有轮次在等位）
                "waiting": waiting}


_mirror_lock = _TimedMirrorLock()

# v2.1.11 口径反转（用户 2026-11-23 明确）：镜像流水线**必须串行**。
# 「复制 → 改名 → 翻译 → 识别 → 入库资源库」合起来算**一轮**；上一轮没跑完，
# 下一轮定时任务不启动，等上一轮彻底结束后，才按（可能已经更新过的）定时
# 启动新一轮。目的是杜绝并发冲突：两个任务同时改同一批文件、同时抢 SQLite
# 写锁、同时建目录 / 改目录名。
#
# v2.1.3 曾经把「后台改名复查」拆成独立的 `_rename_lock` 与镜像锁并行，
# 因为真机上镜像长跑 119 部作品时，改名复查每 120 秒抢不到锁直接 return →
# 英文名解析链路整体停摆（56 部作品集体无英文名）。
# 那版的病灶是「**抢不到就跳过本轮**」，不是「并行」本身。
# v2.1.11 保留串行（并发才是真冲突），但把语义换成**排队等待**：
# 抢不到就等上一轮结束再跑本轮（不丢轮次）——既不并发，也不饿死。
_rename_lock = _mirror_lock  # 旧引用兼容：改名与复制 / 翻译共用同一把闸门

# 定时任务等待上一轮结束的最长秒数。超时只是放弃**本轮**（下一次定时会重来），
# 绝不无限堆积。给足余量是为了覆盖「镜像长跑 / 大批量识别」这类正常长任务。
PIPELINE_WAIT_SECONDS = 3600.0

# v2.1.11：改名门禁从「整轮拦截」收敛为「作品级拦截」。
#
# 起因（真机 2026-11-23）：同轮 20 部作品的镜像复制全部成功、英文名全部解析成功，
# 但资源库里一条都查不到——被 v2.1.0 的「改名硬门禁」整轮拦下了。触发点是**一部剧
# 的假失败**：光鸭回 code=160「文件名已存」，而那 63 个文件其实早已改名成功，
# 只是 DB 状态没跟上。1 部剧的 1 个假失败 → 同轮 19 部健康作品一起被拦。
#
# v2.1.0 门禁的**原意**只有一条：「杜绝英文名没改对就先推群/频道」。
# 这条原意在推送侧本来就有**作品级**闸门兜底：
#   `push_gate.evaluate_push_gate` 的 `file_english` 项 —— 作品所有已同步文件必须
#   都是 rename_status='done'（且有目标名），否则该作品不入队、不推送。
# 也就是说「整轮拦截」属于过度拦截：它连入库与识别一起拦了，而那部分没有任何风险。
#
# v2.1.11 起：改名没成功的作品自己不入队推送（作品级闸门），其余作品照常
# 入库 → 识别 → 推送。要恢复 v2.1.0 的整轮口径，把下面开关改回 True 即可。
RENAME_GATE_BLOCK_WHOLE_ROUND = False


# 阶段四 B-11：审计日志脱敏。异常 str(exc) 可能带 token / 签名 URL 片段，
# 原样落 audit_logs 会长期留存敏感信息。只替换明确的敏感 kv，不破坏诊断上下文。
_AUDIT_SECRET_RE = re.compile(
    r"(?i)(authorization|access[_-]?token|refresh[_-]?token|token|password|secret|api[_-]?key)"
    r"(\s*[=:]\s*)([^\s,;\"'}&]+)"
)


def _mask_audit_detail(detail: str) -> str:
    text = str(detail or "")
    text = _AUDIT_SECRET_RE.sub(r"\1\2***", text)
    # Bearer <token> 形式
    text = re.sub(r"(?i)(Bearer\s+)[A-Za-z0-9._\-]+", r"\1***", text)
    return text[:4000]


def add_audit(db: Session, action: str, target_type: str = "", target_id: str = "", detail: str = "") -> None:
    db.add(AuditLog(action=action, target_type=target_type,
                    target_id=str(target_id), detail=_mask_audit_detail(detail)))


_SESSION_EXPIRED_MSG = "登录已过期，请重新输入密码"
# 阶段四 B-12：拒绝示例/占位符口令——照抄 .env.example 部署会留下公开弱口令。
# 只拦截明显占位符，不强制长度（避免破坏存量短密码与测试环境 testpass）。
_WEAK_ADMIN_PASSWORDS = {
    "change_me", "请设置强密码", "your_password", "yourpassword",
    "admin123", "password", "123456", "admin", "test", "root",
}


def check_admin(
    x_admin_password: str | None = Header(default=None),
    x_admin_session: str | None = Header(default=None),
):
    """v0.9.20：双通道鉴权。浏览器走会话 token（带 idle 超时），脚本/API 兼容明文密码。

    - 带 X-Admin-Session：校验并刷新活跃时间；过期/invalid 一律 401 且给出可识别文案，
      前端据此清 token 回登录页。
    - 无 token 时回退 X-Admin-Password 明文比对（旧行为，向后兼容不破坏脚本/测试）。
    """
    if (settings.admin_password or "").strip().lower() in _WEAK_ADMIN_PASSWORDS:
        raise HTTPException(503, "请先在 .env 设置强 ADMIN_PASSWORD（当前为占位符/弱口令）")
    if x_admin_session:
        state = admin_session.touch(x_admin_session)
        if state == "ok":
            return
        raise HTTPException(401, _SESSION_EXPIRED_MSG if state == "expired" else "会话无效，请重新登录")
    if x_admin_password != settings.admin_password:
        raise HTTPException(401, "管理密码错误")


async def _job_token_refresh():
    db = SessionLocal()
    try:
        t = db.get(AuthToken, 1)
        if not t or not t.access_token_enc:
            return
        # 短信登录通道：用 refresh_token 经光鸭账号服务刷新
        if t.flow in ("sms", "tgto"):
            if not t.expires_at or as_utc(t.expires_at) <= utcnow() + timedelta(minutes=10):
                await guangya_sms.refresh(db)
                add_audit(db, "token_refreshed", target_type="auth", detail="短信登录通道定时刷新成功")
                db.commit()
            return
        # 扫码（webqr）通道：走网页端设备码 refresh_token 刷新（含专属 client_id/base）
        if not t.refresh_token_enc or not t.expires_at:
            return
        if as_utc(t.expires_at) <= utcnow() + timedelta(minutes=10):
            await guangya_auth.refresh(db)
            add_audit(db, "token_refreshed", target_type="auth", detail="定时刷新成功")
            db.commit()
    except Exception as exc:
        add_audit(db, "token_refresh_failed", target_type="auth", detail=str(exc)[:300])
        db.commit()
    finally:
        db.close()


_bot_poll_lock = asyncio.Lock()


def _bot_poll_in_thread() -> None:
    """v1.4.3：Bot 轮询整体挪出事件循环线程（线程内 asyncio.run + 独立 Session）。

    第五次死锁取证：_job_bot_poll 每 10 秒在主协程 await poll_once，其内部
    ProcessedUpdate 写 + 旧版 offset set_many 的 db.commit 撞写锁时在主线程等 30s，
    冻结 loop → 改名 job missed。挪到线程后，任何 SQLite 锁等待都不再冻结全站。
    """
    with SessionLocal() as db:
        try:
            asyncio.run(bot_listener.poll_once(db))
        except Exception:
            db.rollback()
            logger.warning("[bot] 消息轮询异常（线程内），本轮跳过", exc_info=True)


async def _job_bot_poll():
    """v0.8.0：投稿/我想看 Bot 消息轮询（Telegram 官方 getUpdates）。

    v0.9.11：加进程内非阻塞锁——真机偶发 `maximum number of running instances
    reached` 是上一轮 poll_once（getUpdates 网络等待）未结束、下一轮 10 秒已到，
    APScheduler max_instances=1 拒绝新实例并记 warning。这里抢不到锁直接跳过本轮，
    杜绝 warning，且下一轮仍会正常轮询，不影响收消息（与推送故障无关，顺手加固）。
    v1.4.3：整体走 to_thread，主线程零 SQLite 锁等待。
    """
    if _bot_poll_lock.locked():
        return
    async with _bot_poll_lock:
        await asyncio.to_thread(_bot_poll_in_thread)


_restore_lock = asyncio.Lock()


async def _job_contribution_restore():
    """v0.9.19：后台续跑未完成的投稿转存任务（重启后可恢复）。

    审核通过时已即时执行一次；本 job 兜底处理 pending / retry_wait（如进程
    重启、当次网络失败留待重试）。带进程内非阻塞锁，不与手动重试抢写。
    """
    if _restore_lock.locked():
        return
    async with _restore_lock:
        with SessionLocal() as db:
            try:
                if not contribution_restore.enabled(db):
                    return
                rows = contribution_restore.pick_pending(db, limit=20)
                for row in rows:
                    ok, info = await contribution_restore.execute_restore(db, row)
                    add_audit(db, "contribution_restore", target_type="contribution",
                              target_id=str(row.contribution_id),
                              detail=json_dumps({"ok": ok, "attempts": row.attempts,
                                                 "info": info[:160]}))
                    db.commit()
            except Exception:
                db.rollback()
                logger.warning("[contribution-restore] 后台转存异常", exc_info=True)


def untranslated_reason(lock_info: dict, tmdb_ok: bool, ai_ok: bool) -> str:
    """v0.9.21：待翻译项的原因文案按真实状态区分，纯函数便于测试。

    优先级：锁占用 > 配置缺失 > 部分配置 > 配置正常待重跑。真机排查发现配置
    其实正常、真正卡点是镜像锁被占，静态甩锅「补齐 TMDB/AI」会把排查带偏。
    """
    if lock_info.get("busy"):
        # v2.1.11：闸门改成「排队等待」后，用户会看到「上一轮在跑、下一轮在等」。
        # 把等待数量也说清楚，免得以为任务卡死。
        waiting = int(lock_info.get("waiting", 0) or 0)
        wait_hint = (f"；另有 {waiting} 个定时任务在排队等它结束" if waiting else "")
        return ("被镜像任务占用：同步 / 改名任务「%s」运行中（已 %s 秒，%s 秒无进展）%s，"
                "待其结束后点「按英文规范改名」重跑"
                % (lock_info.get("owner") or "未知任务", lock_info.get("held_seconds", 0),
                   lock_info.get("idle_seconds", 0), wait_hint))
    if not tmdb_ok and not ai_ok:
        return "配置缺失：TMDB 与 AI 均未配置；至少配其一后点「按英文规范改名」重跑（无配置时会走拼音兜底）"
    if not tmdb_ok:
        return "TMDB 未配置（AI 可用）：点「按英文规范改名」重跑，将走 AI 直译 / 拼音兜底"
    if not ai_ok:
        return "AI 未配置（TMDB 可用）：点「按英文规范改名」重跑；TMDB 查不到时走拼音兜底"
    return "待重跑：TMDB / AI 配置均正常，点「按英文规范改名」重跑即可"


def _rename_status_label(status: str) -> str:
    """把内部改名状态翻成中性中文（页面不出现内部状态词）。

    v2.1.11：待办清单此前直接把 `rename_status` 原始值（pending_visible /
    skipped_legacy / failed）塞进「状态」列——内部状态词直接漏给用户。
    这里统一走中文标签，与 `mirror.rename_error_label` 一起构成中性展示口径。
    """
    return ({"": "待处理", "pending_visible": "等待网盘可见", "failed": "未完成",
             "skipped_ascii": "无中文名", "skipped_legacy": "历史条目"}
            .get(str(status or ""), "待处理"))


# v2.1.11：镜像模块三类状态的中文标签（媒体 / 目录 / 任务）。
# 页面此前直接把 MirrorMedia.status / MirrorDir.status / MirrorTask.status
# 的英文码渲染到表格里（synced / failed / missing / pending / unknown…），
# 违反「页面不得体现后端代码名词」这条红线。这里统一在后端出文案，
# 前端只渲染 *_label 字段。
def _media_status_label(status: str) -> str:
    """影视映射状态中文标签。"""
    return ({"synced": "已同步", "failed": "同步失败", "blocked": "有阻断项",
             "pending": "待同步", "copying": "复制中", "syncing": "同步中",
             "unknown": "状态待确认", "": "待同步"}
            .get(str(status or "").strip().lower(), "待同步"))


def _dir_status_label(status: str) -> str:
    """目录映射状态中文标签。"""
    return ({"mapped": "已映射", "created": "已创建", "missing": "待创建",
             "created_fallback": "已创建（标准名）", "": "待创建"}
            .get(str(status or "").strip().lower(), "待创建"))


def _task_status_label(status: str, task_status: str = "") -> str:
    """复制任务状态中文标签。

    严格顺序引擎写 task_status（''/pending/submitting/submitted/verifying/
    completed/failed/skipped/cancelled），旧批量路径写 status
    （pending/running/done/failed/unknown）。两套并存时以 task_status 优先，
    这样「跳过本序列」与「已跳过投递」这类语义不会被混成同一个词。
    """
    ts = str(task_status or "").strip().lower()
    if ts:
        return ({"pending": "待处理", "submitting": "提交中", "submitted": "已提交",
                 "verifying": "确认中", "completed": "已完成", "failed": "失败",
                 "skipped": "跳过本序列", "cancelled": "已取消"}
                .get(ts, "进行中"))
    return ({"pending": "待处理", "running": "进行中", "done": "已完成",
             "failed": "失败", "unknown": "状态待确认"}
            .get(str(status or "").strip().lower(), "待处理"))


# v0.9.21：僵尸镜像任务自愈。进程崩溃/强中断时 mirror_tasks 的 running 行会
# 永久悬挂（polls=0、无 error、finished_at 为空），既不重试也不告警。后台定期
# 把超过阈值仍未终结的任务标 failed 并写明原因，纳入日志保留清理；同时释放其
# 持有的镜像锁（若锁恰好因残留卡住）。阈值默认 30 分钟，仅处理确实陈旧的行。
MIRROR_TASK_STALE_MINUTES = 30


def reap_stale_mirror_tasks(db: Session, *, stale_minutes: int | None = None) -> dict:
    """把超时未终结的镜像复制任务标 failed，返回统计。纯 DB 操作，供 job 与测试调用。"""
    from .models import MirrorTask

    minutes = MIRROR_TASK_STALE_MINUTES if stale_minutes is None else stale_minutes
    cutoff = utcnow() - timedelta(minutes=max(1, int(minutes)))
    stale = list(db.scalars(select(MirrorTask).where(
        MirrorTask.status.in_(("pending", "running", "unknown")),
        MirrorTask.created_at < cutoff,
    )).all())
    reaped = 0
    for t in stale:
        t.status = "failed"
        t.error = (f"后台自愈：任务超过 {minutes} 分钟仍未终结（polls={t.polls}），"
                   "判定僵尸并标记失败；如为真实残留请重新同步")[ :300]
        if not t.finished_at:
            t.finished_at = utcnow()
        reaped += 1
    if reaped:
        db.commit()
        logger.warning("[mirror] 僵尸任务自愈：标记 failed %s 条（阈值 %s 分钟）", reaped, minutes)
    return {"scanned": len(stale), "reaped": reaped}


def backfill_source_titles(db: Session, limit: int = 5000) -> int:
    """把 A 库映射回填到资源库：作品名 / 原文件名 / 影视号 / 类型 / 分类目录。

    v2.1.11 需求：资源库的**识别与展示**都要用映射回 A 库的正确影视名与
    TMDB 影视号。B 库改过名（单集名英文、作品目录带 · 标记），拿去 TMDB
    搜几乎必然零命中，用户看到的就是「资源库里识别不出来」。

    关联链：Resource.file_id == MirrorFile.target_file_id（B 库 fileId 一一对应），
    再经 MirrorFile.media_id 取 MirrorMedia 的 A 库作品名。

    类型来源见 `mirror._source_media_type`：**identity → 分类目录 → media_type**
    三级判定，判不出就留空——**宁可不锁类型，也不猜错类型**（猜错会把作品
    整个认错，比不认更糟）。

    实现要点：
      - **单条 JOIN 一次取齐**，不做逐行 N+1 查询（资源库动辄上千行）。
      - 只处理**还没有**映射的行（source_title 为空），避免每轮扫描都全表更新。
      - **只 flush 不 commit**：同一 Session 内后续识别立刻能读到新映射；
        跨 Session 场景（启动回填）由调用方显式提交。

    为什么放在 main.py 而不放在 mirror.py：mirror.py 是**红线模块**，
    其内不允许出现 `db.flush()`（会重新引入跨 await 写事务，见 test_v089）。
    本函数是纯同步 DB 操作、不跨 await，故放在 main.py 侧安全。

    返回回填行数。
    """
    try:
        joined = db.execute(
            select(Resource, MirrorFile, MirrorMedia)
            .join(MirrorFile, MirrorFile.target_file_id == Resource.file_id)
            .join(MirrorMedia, MirrorMedia.id == MirrorFile.media_id)
            .where(Resource.source_title == "")
            .limit(max(1, int(limit or 5000)))
        ).all()
    except Exception as exc:
        logger.warning("[library] A 库映射回填查询失败：%s", exc)
        return 0
    if not joined:
        return 0
    changed = 0
    seen_ids: set[int] = set()
    for x, mf, media in joined:
        if int(x.id) in seen_ids:
            continue
        seen_ids.add(int(x.id))
        # A 库作品名：MirrorMedia.title_standard 就是 A 库作品目录名（含年份、
        # 可能带 {tmdb-} 标记）。展示要「作品名 + 影视号」分开，故去掉标记。
        src_title = strip_tmdb_tag(standard_media_name(str(media.title_standard or ""))).strip()
        src_tmdb = 0
        ident_tid, _ident_type = mirror.identity_tmdb(str(mf.identity or ""))
        if ident_tid:
            src_tmdb = ident_tid
        if not src_tmdb:
            src_tmdb = mirror.tmdb_tag_id(str(media.title_standard or ""))
        cat = str(media.category_path or "")
        if cat == "__root__":
            cat = ""
        src_type = mirror.source_media_type(str(mf.identity or ""), media, cat)
        if not src_title:
            # 没有 A 库作品名 → 视为未命中，source_title 留空，下一轮再来。
            continue
        x.source_title = src_title
        x.source_name = str(mf.file_name or "")
        x.source_tmdb_id = src_tmdb or None
        x.source_media_type = src_type
        x.source_category = cat
        changed += 1
    if changed:
        # 只 flush 不 commit：同一 Session 内后续识别立刻可见；跨 Session 由调用方提交。
        db.flush()
    return changed


def _backfill_source_titles_in_thread() -> int:
    """v2.1.11：A 库映射回填（DB 密集，放线程池；Session 在线程内创建）。

    独立 Session → 独立事务 → 回填完自己 commit，与请求级会话互不干扰。
    """
    with SessionLocal() as db:
        try:
            n = backfill_source_titles(db)
            if n:
                db.commit()
            return n
        except Exception:
            db.rollback()
            logger.warning("[library] A 库映射回填异常", exc_info=True)
            return 0


async def _job_source_map_backfill():
    """v2.1.11：定时把「映射回 A 库」的作品名/影视号/类型回填到资源库。

    覆盖两种场景：
      1. 老库存量（升级前入库的资源，启动时先回填一次）；
      2. 新入库但当轮没匹配上映射的行（下一轮定时补）。
    幂等：只处理还没有映射的行，重复跑不会重复写。
    """
    await asyncio.to_thread(_backfill_source_titles_in_thread)


def _reap_stale_mirror_tasks_in_thread() -> None:
    """A-07：纯同步 DB 自愈扫描，放到线程池执行，Session 也在线程内创建（SQLite 安全）。"""
    with SessionLocal() as db:
        try:
            reap_stale_mirror_tasks(db)
        except Exception:
            db.rollback()
            logger.warning("[mirror] 僵尸任务自愈扫描异常", exc_info=True)


async def _job_mirror_task_reap():
    """每 5 分钟扫描一次僵尸镜像任务；镜像锁正被持有时跳过，避免误伤真在跑的任务。"""
    if _mirror_lock.info().get("busy"):
        return
    # A-07：DB 密集扫描不在事件循环线程做；to_thread 让它与 /api/health 等并发
    await asyncio.to_thread(_reap_stale_mirror_tasks_in_thread)


async def _job_mirror_task_poll():
    """阶段二路线B C5：定时轮询未确认的复制任务（含严格顺序批次遗留）。

    修复审计缺口：重启后 strict 批次停在 submitted/verifying 的任务此前只能手动
    /api/mirror/tasks/poll 才续跑。此 job 每 60s 自动确认远端状态。

    v2.1.11：纳入流水线闸门（非阻塞）。上一轮任务在跑就跳过本轮轮询；
    更关键的是本 job 自己**也持锁**——否则它正在轮询时，定时镜像轮次可能同时
    拿到闸门开跑，两条链路并发碰同一批文件。
    """
    if not _mirror_lock.acquire(blocking=False, owner="复制任务轮询"):
        return
    try:
        with SessionLocal() as db:
            try:
                r = _mirror_rule(db)
                if not r or not (r.source_root_id and r.target_root_id):
                    return
                out = await mirror.poll_pending_tasks(db, r, max_polls=3, interval=1.0)
                if out.get("done") or out.get("failed"):
                    logger.info("[mirror] 定时续跑：done=%s failed=%s",
                                out["done"], out["failed"])
            except Exception:
                db.rollback()
                logger.warning("[mirror] 定时轮询未确认任务异常", exc_info=True)
    finally:
        _mirror_lock.release()


def _log_prune_in_thread() -> None:
    """A-07：日志清理是纯同步 DB 批量操作，放线程池执行，避免阻塞事件循环。"""
    with SessionLocal() as db:
        try:
            cfg = log_retention.config(db)
            if not cfg["auto_prune"]:
                return
            out = log_retention.prune(db, days=cfg["retention_days"],
                                      max_rows=cfg["max_rows"])
            db.commit()
            if out["deleted"]:
                logger.info("[logs] 自动清理 %s 条（保留 %s 天 / 每表上限 %s）",
                            out["deleted"], out["days"], out["max_rows"])
        except Exception as exc:
            db.rollback()
            logger.warning("[logs] 自动清理失败：%s", exc)


async def _job_log_prune():
    """v0.8.3：每日按保留策略自动清理日志与历史留痕。失败只记日志，不影响其它任务。"""
    # A-07：DB 密集清理不在事件循环线程做；不降低 busy_timeout（v0.8.8 的 30s+WAL 是
    # 真实锁竞争修复，回退到 5s 会重现 database is locked），仅把阻塞挪出事件循环。
    await asyncio.to_thread(_log_prune_in_thread)


async def _job_rename_pending():
    """v0.9.2：后台复查「B 库暂未可见」的单文件改名，不复制、不入库、不推送。

    复制任务完成后网盘文件可见可能有秒级延迟。改名只认只读列目录命中的 fileId，
    每次仍走 file_idle_seconds 风控间隔与 rename 预算。

    v2.1.3 真机死锁根治（2026-11-22）：原来本 job 与「定时镜像 / 立即同步」抢**同一把**
    `_mirror_lock`。真机上镜像同步（119 部作品的大轮次）长时间持锁 → 本 job 每 120 秒
    都 `acquire(blocking=False)` 失败直接 return → 英文名解析链路整体停摆（07:23 后
    56 部作品集体无英文名、rename_status 56 条为空）。当时的修法是「拆两把锁并行」。

    v2.1.11（用户口径：复制 / 改名 / 翻译 / 识别直到入库资源库算**一轮**，上一轮没跑完
    下一轮不启动）：回到**同一把闸门**，但语义从「抢不到就跳过本轮」改成
    **排队等上一轮彻底结束再跑本轮**（不丢轮次）。
    既杜绝了并发冲突（两个任务同时改同一批文件 / 同时抢 SQLite 写锁），
    也不会像 v2.1.3 之前那样被饿死——等得到就一定会跑。
    """
    if not await _mirror_lock.acquire_async("待改名复查", timeout=PIPELINE_WAIT_SECONDS):
        logger.info("[mirror] 等待上一轮任务结束超时，本轮待改名复查放弃")
        return
    try:
        with SessionLocal() as db:
            r = db.scalar(select(MirrorRule).where(MirrorRule.enabled.is_(True)).limit(1))
            if not r or not bool(getattr(r, "rename_files", True)):
                return
            pending = db.scalar(select(func.count()).select_from(MirrorFile).where(
                MirrorFile.rule_id == r.id,
                MirrorFile.status == "synced",
                or_(MirrorFile.rename_status.is_(None),
                    MirrorFile.rename_status == "",
                    MirrorFile.rename_status == "pending_visible",
                    MirrorFile.rename_status == "failed"))) or 0
            if not pending:
                return
            # v1.4.4 按部收敛：不再传 max_files=10 截断单集（那会让被选中的整部剧
            # 只改 10 集）。每轮按 rule.max_works_per_run 选 N 部、部内全部改完。
            out = await _safe_rename(db, r, only_pending=True, max_media=5)
            db.commit()
            if out.get("renamed") or out.get("waiting_visible"):
                logger.info("[mirror] 待改名复查：改名 %s / 仍待可见 %s",
                            out.get("renamed", 0), out.get("waiting_visible", 0))
            # v2.1.3：改名完成后，之前被推送门禁挡下的作品可能已经满足「文件名已
            # 翻译为英文」——立即重新入队，避免门禁把它们永久拦下（漏推）。
            _recheck_gated_push(db)
    except Exception as exc:
        logger.warning("[mirror] 待改名复查失败：%s", exc)
    finally:
        _mirror_lock.release()


def _queue_trigger(db: Session):
    """v0.6.4：按管理页配置生成队列推送触发器。cron 优先（5 段），否则用间隔秒数。"""
    cron = (runtime_config.get(db, "queue_cron") or "").strip()
    if cron:
        try:
            return CronTrigger.from_crontab(cron, timezone="Asia/Shanghai")
        except Exception:
            pass  # 非法 cron 回退到间隔
    try:
        seconds = max(5, int(runtime_config.get(db, "queue_interval_seconds") or 30))
    except ValueError:
        seconds = 30
    return "interval", {"seconds": seconds}


def _schedule_queue_worker(trigger) -> None:
    """注册/热更新队列 worker；保存调度配置后立即生效，无需重启容器。"""
    if isinstance(trigger, tuple):
        scheduler.add_job(process_queue, trigger[0], **trigger[1], id="queue_worker",
                          max_instances=1, coalesce=True, replace_existing=True)
    else:
        scheduler.add_job(process_queue, trigger, id="queue_worker",
                          max_instances=1, coalesce=True, replace_existing=True)


async def _scheduled_scan(root: str) -> None:
    """v0.7.1：Cron 定时全量扫描 B 库 → 自动识别 → 完结回填（需求3-1）。

    与手动「重新扫描库」的区别：定时任务**不做对账删除**，避免断点/接口抖动
    造成误删；对账只由用户主动点击「重新扫描库」触发。全程独立 Session，
    不复用请求级 db，异常自行吞掉并由 run_scan 写入 ScanTask.last_status。

    v2.1.11：纳入**流水线闸门**。这一轮做的是「扫描入库 → 识别（翻译）→ 推送」，
    与镜像轮的「复制 → 改名」是同一条链路的两端；以前它**完全没有锁**，
    会与正在跑的镜像轮次并发（一边改 B 库文件名、一边按旧名入库），
    正是「资源库里名字对不上 / 这轮看不到」的成因之一。
    现在：上一轮没彻底结束就排队等，结束后再跑，绝不并发。
    """
    if not await _mirror_lock.acquire_async("定时扫描识别", timeout=PIPELINE_WAIT_SECONDS):
        logger.info("[scan] 等待上一轮任务结束超时，本轮定时扫描放弃")
        return
    try:
        with SessionLocal() as db:
            try:
                # v0.8.1：每次执行都重新取 B 镜像库目录，用户在镜像设置里改 ID 后立即生效
                effective, _ = _effective_scan_root(db)
                await run_scan(effective, resume=False)
                # v2.1.11：入库后回填「映射回 A 库」的作品名/影视号/类型，
                # 让本轮识别用 A 库口径（顺序必须在识别之前）。
                db.commit()
                backfill_source_titles(db)
                db.commit()
                await _post_scan_identify(db, publish_after_identify=True)
                process_change_events(db, publish=True)
                db.commit()
            except Exception:
                db.rollback()
                # A-09：run_scan 虽会写 ScanTask.last_status，但此前此处无应用日志，
                # 定时任务失败在日志侧不可见。补 exc_info，业务状态与应用日志双留痕。
                logger.warning("[scan] 定时全量扫描异常", exc_info=True)
    finally:
        _mirror_lock.release()


def _consume_resource_events_in_thread() -> None:
    """P0：资源事件消费是纯同步 DB 操作，放线程池 + 线程内独立 Session。

    对齐 A-07（_reap_stale_mirror_tasks_in_thread / _log_prune_in_thread）：
    SQLite 单写者 + busy_timeout=30s，若在 async job 的事件循环线程里同步等待写锁，
    30 秒冻结整个 loop → APScheduler 所有 job 与全部 API 一起饿死（2026-11-21 事故）。
    """
    from . import share_center as _sc
    with SessionLocal() as db:
        try:
            # 阶段三：先消费归档镜像事件（防未消费堆积），再消费资源事件。
            # 镜像事件不转资源事件——MirrorMedia.id 与 Resource.id 无可靠映射，
            # 恢复走 pipeline→_enqueue_ready 的 ever_cancelled 判定（不虚构关联）。
            # consume_mirror_events 纯短事务，不引入新持锁源（五轮死锁教训）。
            try:
                from . import events as _ev
                _ev.consume_mirror_events(db, limit=100)
            except Exception as exc:
                db.rollback()
                logger.warning("[events] 消费镜像事件周期异常：%s", exc)
            _sc.consume_resource_events(db, limit=100)
        except Exception as exc:
            db.rollback()
            logger.warning("[share_center] 消费资源事件周期异常：%s", exc)


def _run_share_cycle_in_thread(limit: int = 20) -> dict:
    """v1.4.2 洞2根治：分享调度周期整体挪出事件循环线程。

    2026-11-21 第四次复现：v1.4.0 只把「事件消费」挪进 to_thread，但 run_cycle
    仍在主协程——它内部 submit_to_dispatch / can_publish / _pace_after 的 db.commit()
    是同步写，撞上被占的写锁就在主线程等 30s，冻结整个 loop → 全 job missed + API 超时。
    这里在独立线程用 asyncio.run() 跑完整 run_cycle（含 build_share 网络 await 与全部
    commit），线程内自建 Session。主线程只等一个 to_thread，零 SQLite 锁等待。

    阶段六：share_run 手动端点复用本 helper（第六次死锁同源漏网修复），limit 参数化
    并返回统计 dict；_job_share_center 保持无参调用不变。
    """
    from . import share_center as _sc
    with SessionLocal() as db:
        try:
            return asyncio.run(_sc.run_cycle(db, limit=limit))
        except Exception as exc:
            db.rollback()
            logger.warning("[share_center] 调度周期异常（线程内）", exc_info=True)
            return {"built": 0, "submitted": 0, "skipped": 0, "failed": 0, "error": True}


def _run_share_center_v2_cycle_in_thread(limit: int = 20) -> dict:
    """分享中心 v2 调度周期：全部在 sc_* 新表上运行，线程内独立 Session。"""
    with SessionLocal() as db:
        try:
            share_center_v2.ensure_defaults(db)
            return asyncio.run(share_center_v2.run_cycle(db, limit=limit))
        except Exception as exc:
            db.rollback()
            logger.warning("[share_center_v2] 调度周期异常（线程内）", exc_info=True)
            return {"enabled": True, "error": True, "message": str(exc)[:500]}


def _run_sc_ai_weekly_in_thread() -> dict:
    with SessionLocal() as db:
        try:
            return share_center_v2.generate_weekly_suggestions(db)
        except Exception as exc:
            db.rollback()
            logger.warning("[share_center_v2] AI 周度建议异常", exc_info=True)
            return {"error": True, "message": str(exc)[:500]}


async def _job_sc_ai_weekly() -> None:
    await asyncio.to_thread(_run_sc_ai_weekly_in_thread)


# ---------------------------------------------------------------------------
# v2.1.8 #241：Telegram 阅读量定时采集
#
# 为什么必须有定时采集：阅读量是**累计值**，趋势必须由相邻快照求差得到。
# 只靠页面上的手动按钮，趋势图永远只有一个数据点（首次采集没有前值 → 不计增量），
# 「每日新增阅读」这类口径根本无法成立。所以这里按配置的间隔周期采集。
# ---------------------------------------------------------------------------

_tg_stats_lock = asyncio.Lock()

# 定时采集的唤醒节奏（固定 tick）。
# **间隔本身不写在这里**——用户在管理页改的间隔要能运行期生效，
# 所以固定每 10 分钟醒一次，由 due_for_collect() 按「距上次采集多久」判断是否到点。
_TG_STATS_TICK_SECONDS = 600


def _tg_stats_collect_in_thread() -> dict:
    """在**工作线程 + 线程内独立 Session** 里跑一轮采集。

    Session 必须在**线程内**创建（对齐 `_bot_poll_in_thread` /
    `_reap_stale_mirror_tasks_in_thread` / `_run_share_center_v2_cycle_in_thread`
    的既有做法）：SQLAlchemy Session 不是线程安全对象，跨线程复用同一实例
    是隐患；线程内自建会话则与「DB 密集 job 一律独立 Session」的红线一致。
    """
    with SessionLocal() as db:
        try:
            enabled = (runtime_config.get(db, tg_stats.CFG_ENABLED) or "").lower() == "true"
            interval = 0
            try:
                interval = int(runtime_config.get(db, tg_stats.CFG_COLLECT_INTERVAL_HOURS) or 0)
            except (TypeError, ValueError):
                interval = 0
            if not enabled:
                return {"ok": False, "collected": 0, "skipped": 0, "scanned": 0,
                        "reason": "disabled"}
            if interval <= 0:
                return {"ok": False, "collected": 0, "skipped": 0, "scanned": 0,
                        "reason": "interval_disabled"}
            if not tg_stats_service.due_for_collect(db, interval):
                return {"ok": False, "collected": 0, "skipped": 0, "scanned": 0,
                        "reason": "not_due"}
            # 采集内部还有同步 db.commit()，而 SQLite 只有单写者 +
            # busy_timeout=30000——写锁竞争时同步 commit 会在**事件循环线程**
            # 里干等最长 30 秒，把整个进程冻住。所以再隔一层：采集本体
            # 由 collect_once_threaded 在**它自己的线程 + 独立事件循环**里跑。
            #
            # 注意：这里**不能**提前 close 会话——collect_once_threaded 要用
            # 它写快照与执行记录。close 交给 with 语句在函数返回时统一做。
            return tg_stats_service.collect_once_threaded(db)
        except Exception as exc:
            try:
                db.rollback()
            except Exception:
                pass
            logger.warning("[tg_stats] 定时采集异常", exc_info=True)
            return {"ok": False, "collected": 0, "skipped": 0, "scanned": 0,
                    "reason": "unexpected_error", "error": str(exc)[:300]}


async def _job_tg_stats_collect() -> None:
    """按间隔自动跑一轮阅读量采集。

    - 采集过程有同步 `db.commit()`（SQLite 单写者 + busy_timeout 30s），
      **必须**放到线程里跑，否则写锁竞争会把主事件循环冻住整整 30 秒
      （六轮死锁教训）。collect_once_threaded() 在工作线程里起独立事件循环。
    - 进程内非阻塞锁防重入：上一轮没跑完时直接跳过本轮（不排队、不并发）。
    - 开关未开 / 间隔为 0 / 依赖缺失 / 未登录时直接跳过或由 collect_once 短路返回，
      **不抛异常**。
    - 任何异常都只记日志——**阅读量是辅助能力，绝不能拖累主链**。
    """
    if _tg_stats_lock.locked():
        return
    async with _tg_stats_lock:
        result = await asyncio.to_thread(_tg_stats_collect_in_thread)
    collected = int(result.get("collected") or 0)
    skipped = int(result.get("skipped") or 0)
    scanned = int(result.get("scanned") or 0)
    reason = str(result.get("reason") or "")
    if collected or skipped:
        logger.info("[tg_stats] 定时采集完成：扫描 %s 条，采集 %s 条，跳过 %s 条%s",
                    scanned, collected, skipped,
                    f"（{reason}）" if reason else "")
    elif reason and reason not in ("disabled", "interval_disabled", "not_due",
                                   "dependency_missing"):
        logger.info("[tg_stats] 定时采集未采集到数据（%s）", reason)


async def _job_share_center() -> None:
    """分享中心调度入口：v2 新数据体系默认接管，旧链保留为降级路径。"""
    with SessionLocal() as db:
        try:
            from . import share_center as _sc
            v2_on = share_center_v2.is_enabled(db)
            old_on = False if v2_on else _sc.is_enabled(db)
        except Exception as exc:
            db.rollback()
            logger.warning("[share_center] 开关读取异常：%s", exc)
            return
    if v2_on:
        await asyncio.to_thread(_run_share_center_v2_cycle_in_thread)
        return
    if not old_on:
        return
    from . import share_center as _sc
    await asyncio.to_thread(_consume_resource_events_in_thread)
    await asyncio.to_thread(_run_share_cycle_in_thread)


@asynccontextmanager
async def lifespan(app: FastAPI):
    migrate()
    # v0.9.21：把镜像锁心跳钩子注入 mirror 模块（长复制/改名轮询每轮打点），
    # 避免反向 import main 造成循环依赖；默认 no-op，钩子异常不影响同步主链路。
    mirror.set_progress_heartbeat(_mirror_lock.heartbeat)
    # v0.8.3：启动时先按保留策略清理一次，避免长期运行后 SQLite 被历史日志撑爆
    try:
        with SessionLocal() as db:
            cfg = log_retention.config(db)
            if cfg["auto_prune"]:
                log_retention.prune(db, days=cfg["retention_days"],
                                    max_rows=cfg["max_rows"])
                db.commit()
    except Exception as exc:
        logger.warning("[logs] 启动清理失败（不影响启动）：%s", exc)
    # 阶段五 B-04：重启恢复遗留的手动扫描状态。asyncio.Task 无法跨进程恢复，
    # 把上次未终结的 running 标为「已中断」，前端如实展示而非永远卡 running。
    try:
        with SessionLocal() as db:
            recovered = async_scan_state.recover_on_startup(db)
            if recovered.get("error"):
                logger.warning("[async-scan] 启动时恢复扫描状态：%s", recovered.get("stage"))
            _async_scan.update(recovered)
    except Exception as exc:
        logger.warning("[async-scan] 扫描状态恢复失败（不影响启动）：%s", exc)
    with SessionLocal() as db:
        guangya_auth.seed_from_config(db)
        task = db.scalar(select(ScanTask).limit(1))
        if not task:
            db.add(ScanTask(cron=settings.scan_cron, root_id=settings.scan_root_id))
            db.commit()
            task = db.scalar(select(ScanTask).limit(1))
        cron, root, enabled = task.cron, task.root_id, task.enabled
        queue_trigger = _queue_trigger(db)
        mr = db.scalar(select(MirrorRule).limit(1))
        mirror_minutes, mirror_enabled = ((mr.interval_minutes, mr.enabled)
                                          if mr else (60, False))
        # 必须在这里取出来：会话关掉后 ORM 实例是 detached，再读 last_run 会抛
        # DetachedInstanceError（属性没被加载过）。
        mirror_last_run = mr.last_run if mr else None
    # 镜像定时任务热注册；v0.8.5 起任务内部按 auto_identify 接「入库→识别→推送」，
    # 推送走分发规则与作品级去重，不单独注册群发任务
    #
    # v2.1.24：这里原来是 `try: ... except Exception: pass` —— 注册失败被**静默吞掉**，
    # 页面无任何痕迹，用户只能看到「同步没按间隔跑」。现在失败必落日志 + 审计。
    # 成功注册后还要判断「停机期间是否错过窗口」：错过就补跑一轮（镜像同步是幂等
    # 全量比对，补最新一轮即可，不必补 N 次）。
    if mirror_enabled:
        try:
            _schedule_mirror_job(mirror_minutes, True)
        except Exception as exc:
            logger.error("[mirror] 启动注册定时同步失败（定时同步不会运行）：%s", exc,
                         exc_info=True)
            try:
                with SessionLocal() as db:
                    add_audit(db, "mirror_schedule_failed", target_type="mirror",
                              detail=json_dumps({"phase": "startup",
                                                 "interval_minutes": mirror_minutes,
                                                 "error": f"{type(exc).__name__}: {exc}"}))
                    db.commit()
            except Exception:        # 审计写不进去也不能挡住启动
                pass
        else:
            if _mirror_missed_window(mirror_last_run, mirror_minutes):
                try:
                    scheduler.add_job(
                        _scheduled_mirror, "date",
                        run_date=utcnow() + timedelta(seconds=MIRROR_CATCHUP_DELAY_SECONDS),
                        id=MIRROR_CATCHUP_JOB_ID, replace_existing=True,
                        misfire_grace_time=3600)
                    logger.info("[mirror] 启动时发现已错过同步窗口 → %s 秒后补跑一轮",
                                MIRROR_CATCHUP_DELAY_SECONDS)
                except Exception as exc:
                    logger.warning("[mirror] 启动补跑注册失败（不影响定时任务本身）：%s", exc)
    if enabled:
        try:
            scheduler.add_job(_scheduled_scan, CronTrigger.from_crontab(cron, timezone="Asia/Shanghai"),
                              args=[root], id="global_scan", replace_existing=True,
                              max_instances=1, coalesce=True,
                              # v2.1.24：同 mirror_sync —— 默认 1 秒宽限会把「晚到」
                              # 的定时扫描直接丢掉。扫描是幂等的，给足 300 秒。
                              misfire_grace_time=300)
        except Exception as exc:
            logger.error("[scan] 启动注册定时扫描失败（定时扫描不会运行）：%s", exc)
    _schedule_queue_worker(queue_trigger)
    # 四模块重构：分享中心调度（v1.1 默认开启，5s 一轮缩短入队→建分享→提交的延迟窗口）
    scheduler.add_job(_job_share_center, "interval", seconds=5, id="share_center",
                      max_instances=1, coalesce=True)
    scheduler.add_job(_job_token_refresh, "interval", seconds=60, id="token_refresh",
                      max_instances=1, coalesce=True)
    scheduler.add_job(_job_bot_poll, "interval", seconds=10, id="bot_poll",
                      max_instances=1, coalesce=True)
    # v0.9.19：投稿转存续跑（重启后恢复 pending / retry_wait 任务）
    scheduler.add_job(_job_contribution_restore, "interval", seconds=30, id="contribution_restore",
                      max_instances=1, coalesce=True)
    scheduler.add_job(_job_rename_pending, "interval", seconds=120, id="rename_pending",
                      max_instances=1, coalesce=True)
    # v0.9.21：僵尸镜像任务自愈（每 5 分钟扫描一次超时未终结的 running/pending）
    scheduler.add_job(_job_mirror_task_reap, "interval", seconds=300, id="mirror_task_reap",
                      max_instances=1, coalesce=True)
    # 阶段二路线B C5：定时轮询未确认复制任务（strict 批次重启续跑，每 60s）
    scheduler.add_job(_job_mirror_task_poll, "interval", seconds=60, id="mirror_task_poll",
                      max_instances=1, coalesce=True)
    scheduler.add_job(_job_log_prune,
                      CronTrigger.from_crontab("30 3 * * *", timezone="Asia/Shanghai"),
                      id="log_prune", replace_existing=True,
                      max_instances=1, coalesce=True)
    # 分享中心 v2：AI 只做周度建议，默认只进审核队列，不直接推送。
    scheduler.add_job(_job_sc_ai_weekly,
                      CronTrigger.from_crontab("30 3 * * 1", timezone="Asia/Shanghai"),
                      id="sc_ai_weekly", replace_existing=True,
                      max_instances=1, coalesce=True)
    # v2.1.8 #241：阅读量定时采集。固定 tick 唤醒，实际是否采集由
    # due_for_collect() 按管理页配置的间隔判断（间隔改了下一轮就生效）。
    scheduler.add_job(_job_tg_stats_collect, "interval",
                      seconds=_TG_STATS_TICK_SECONDS, id="tg_stats_collect",
                      replace_existing=True, max_instances=1, coalesce=True)
    # v2.1.11：A 库映射回填。启动先补一次老库存量（升级前入库的资源），
    # 之后每 10 分钟补一次新入库未匹配上映射的行（幂等，只补空映射）。
    scheduler.add_job(_job_source_map_backfill, "interval", seconds=600,
                      id="source_map_backfill", replace_existing=True,
                      max_instances=1, coalesce=True)
    scheduler.start()
    # 启动即回填老库存量（放线程池，不阻塞事件循环启动）。
    try:
        await asyncio.to_thread(_backfill_source_titles_in_thread)
    except Exception as exc:
        logger.warning("[library] 启动时 A 库映射回填失败（不影响启动）：%s", exc)
    yield
    # 阶段四 A-11：wait=True 等待正在执行的 job 收尾，避免容器停止时腰斩
    # 队列推送 / 转存 / 扫描（各 job 网络调用自带超时，不会无限阻塞）
    scheduler.shutdown(wait=True)
    # v1.4.3：优雅退出时把内存态 Bot offset/error 低频落库一次（热路径已去 DB 化）。
    # 容器被强杀时内存态丢失也安全：ProcessedUpdate 幂等台账会跳过重复投递的 update。
    try:
        with SessionLocal() as db:
            bot_listener.flush_poll_state(db)
    except Exception as exc:
        logger.warning("[bot] 退出时轮询状态落库失败（不影响停止）：%s", exc)


app = FastAPI(title="Ya-TG", version="2.1.29", lifespan=lifespan)
ADMIN = [Depends(check_admin)]
# 分享中心 v2：所有端点走后台会话鉴权；前端只做管理入口。
app.include_router(share_center_v2.router, prefix="/api/sc", dependencies=ADMIN)
# v2.1.7 #241：Telegram 阅读量采集与趋势报表。同样走管理会话鉴权。
# v2.1.8 #241：阅读量报表口径修正 + 定时采集（详见 CHANGELOG）。
app.include_router(tg_stats_api.router, prefix="/api/sc", dependencies=ADMIN)


@app.get("/api/health")
def health():
    return {"ok": True, "timezone": "Asia/Shanghai", "version": "2.1.29", "app": "Ya-TG"}


# ---------- v0.9.20：管理后台会话登录（未活跃 idle 到期强制重新输入密码） ----------
class AdminLoginInput(BaseModel):
    password: str = ""


@app.post("/api/auth/login")
def admin_login(p: AdminLoginInput):
    """用管理员密码换取会话 token。浏览器只存 token，不再永久保存明文密码。"""
    import hmac
    if (settings.admin_password or "").strip().lower() in _WEAK_ADMIN_PASSWORDS:
        raise HTTPException(503, "请先在 .env 设置强 ADMIN_PASSWORD（当前为占位符/弱口令）")
    if not hmac.compare_digest(str(p.password or ""), settings.admin_password):
        raise HTTPException(401, "管理密码错误")
    token = admin_session.create()
    return {"ok": True, "token": token, "idle_seconds": admin_session.idle_seconds()}


@app.get("/api/auth/session")
def admin_session_status(x_admin_session: str | None = Header(default=None)):
    """只读会话状态（**不刷新活跃时间**，避免前端轮询续命导致永不超时）。"""
    state = admin_session.read(x_admin_session or "")
    if state != "ok":
        raise HTTPException(401, _SESSION_EXPIRED_MSG if state == "expired" else "会话无效，请重新登录")
    return {"active": True, "remaining_seconds": admin_session.remaining_seconds(x_admin_session),
            "idle_seconds": admin_session.idle_seconds()}


@app.post("/api/auth/logout", dependencies=ADMIN)
def admin_logout(x_admin_session: str | None = Header(default=None)):
    admin_session.destroy(x_admin_session or "")
    return {"ok": True}


@app.get("/api/overview", dependencies=ADMIN)
def overview(db: Session = Depends(get_db)):
    counts = {k: db.scalar(select(func.count(getattr(m, "id")))) or 0 for k, m in
              {"resources": Resource, "bots": Bot, "channels": Channel, "rules": Rule}.items()}
    for status in ("pending", "review", "retry", "failed", "sent"):
        counts[status] = db.scalar(select(func.count(QueueItem.id)).where(QueueItem.status == status)) or 0
    _auth = db.get(AuthToken, 1)  # 阶段三 A-05：取一次，避免重复 db.get
    counts["auth"] = "ok" if (_auth and _auth.access_token_enc) else "unauthorized"
    return counts


# ---------- v0.9.8：推送效果统计（只做后端真实可得指标） ----------
def _stat_bucket(dt, period: str) -> str:
    """把 UTC 时间按本地日/周/月归桶，返回桶标签。"""
    local = as_utc(dt).astimezone()  # 转本地时区
    if period == "month":
        return f"{local.year:04d}-{local.month:02d}"
    if period == "week":
        monday = local - timedelta(days=local.weekday())
        return f"{monday.year:04d}-{monday.month:02d}-{monday.day:02d}"
    return f"{local.year:04d}-{local.month:02d}-{local.day:02d}"


@app.get("/api/push-stats", dependencies=ADMIN)
def push_stats(period: str = "day", days: int = 30,
               db: Session = Depends(get_db)):
    """推送统计与图文报表数据源。

    真实可得：发送成功数、失败数、成功率、重试次数、分发事件数（均来自本地库）。
    不可得：阅读数 / 点击数 / 转存数——Telegram Bot API 不回传已读/点击，
    光鸭分享接口也没有转存回调；不造假数，明确在 unavailable 里说明。
    """
    if period not in ("day", "week", "month"):
        raise HTTPException(400, "period 仅支持 day/week/month")
    days = max(1, min(days, 365))
    cutoff = utcnow() - timedelta(days=days)

    sent_rows = db.scalars(select(QueueItem).where(
        QueueItem.sent_at.isnot(None), QueueItem.sent_at >= cutoff)).all()
    failed_rows = db.scalars(select(QueueItem).where(
        QueueItem.status == "failed", QueueItem.created_at >= cutoff)).all()
    dist_rows = db.scalars(select(DistributionEvent).where(
        DistributionEvent.created_at >= cutoff)).all()

    buckets: dict[str, dict] = {}

    def slot(label: str) -> dict:
        return buckets.setdefault(label, {"label": label, "sent": 0, "failed": 0,
                                          "retries": 0, "events": 0})

    for q in sent_rows:
        slot(_stat_bucket(q.sent_at, period))["sent"] += 1
        if q.attempts and q.attempts > 1:
            slot(_stat_bucket(q.sent_at, period))["retries"] += q.attempts - 1
    for q in failed_rows:
        slot(_stat_bucket(q.created_at, period))["failed"] += 1
    for e in dist_rows:
        slot(_stat_bucket(e.created_at, period))["events"] += 1

    series = sorted(buckets.values(), key=lambda x: x["label"])
    for row in series:
        total = row["sent"] + row["failed"]
        row["success_rate"] = round(row["sent"] / total * 100, 1) if total else 0.0

    totals = {
        "sent": sum(r["sent"] for r in series),
        "failed": sum(r["failed"] for r in series),
        "retries": sum(r["retries"] for r in series),
        "events": sum(r["events"] for r in series),
    }
    tt = totals["sent"] + totals["failed"]
    totals["success_rate"] = round(totals["sent"] / tt * 100, 1) if tt else 0.0

    return {
        "period": period, "days": days, "series": series, "totals": totals,
        "available": ["发送成功数", "发送失败数", "成功率", "重试次数", "分发更新次数"],
        "unavailable": [
            {"key": "reads", "label": "阅读数",
             "reason": "Telegram Bot API 不回传消息已读状态，无法统计"},
            {"key": "clicks", "label": "点击数",
             "reason": "Telegram inline 按钮点击不回流到本系统，无法统计"},
            {"key": "transfers", "label": "转存数",
             "reason": "光鸭分享接口未提供转存回调，无法统计"},
        ],
        "generated_at": utcnow().isoformat(),
    }


# ---------- 光鸭授权 ----------
@app.get("/api/auth/status", dependencies=ADMIN)
def auth_status(db: Session = Depends(get_db)):
    return guangya_auth.status(db)


@app.post("/api/auth/webqr/start", dependencies=ADMIN)
async def auth_webqr_start(db: Session = Depends(get_db)):
    """v0.9.2：网页端扫码登录——免开放平台应用凭证，光鸭 App 扫一扫授权。"""
    try:
        data = await guangya_auth.start_webqr(db)
        add_audit(db, "auth_webqr_started", target_type="auth",
                  detail=json_dumps({"user_code": data.get("user_code")}))
        db.commit()
        return data
    except guangya_auth.AuthError as exc:
        raise HTTPException(400, str(exc))


@app.post("/api/auth/webqr/poll", dependencies=ADMIN)
async def auth_webqr_poll(db: Session = Depends(get_db)):
    """轮询扫码结果：pending 继续等；authorized 落 Token。"""
    try:
        return await guangya_auth.poll_webqr(db)
    except guangya_auth.AuthError as exc:
        raise HTTPException(400, str(exc))


@app.post("/api/auth/refresh", dependencies=ADMIN)
async def auth_refresh(db: Session = Depends(get_db)):
    try:
        await guangya_auth.refresh(db, force=True)
        add_audit(db, "token_refreshed_manual", target_type="auth")
        db.commit()
        return {"ok": True, "status": guangya_auth.status(db)}
    except guangya_auth.AuthError as exc:
        raise HTTPException(400, str(exc))


@app.get("/api/share/status", dependencies=ADMIN)
def share_status(db: Session = Depends(get_db)):
    return GuangYaShareClient.config_status(db)


# ---------- 一键连接光鸭（自研短信登录，无外部依赖） ----------
class _PhoneIn(BaseModel):
    phone: str = ""


class _CodeIn(BaseModel):
    code: str = ""


@app.get("/api/sms/status", dependencies=ADMIN)
def sms_status(db: Session = Depends(get_db)):
    return guangya_sms.status(db)


@app.post("/api/sms/send", dependencies=ADMIN)
async def sms_send(payload: _PhoneIn, db: Session = Depends(get_db)):
    try:
        result = await guangya_sms.send_code(db, payload.phone)
        add_audit(db, "sms_code_sent", target_type="auth", detail=result.get("masked_phone", ""))
        db.commit()
        return result
    except guangya_sms.SmsError as exc:
        raise HTTPException(400, str(exc))


@app.post("/api/sms/verify", dependencies=ADMIN)
async def sms_verify(payload: _CodeIn, db: Session = Depends(get_db)):
    try:
        result = await guangya_sms.verify_code(db, payload.code)
        add_audit(db, "sms_login_success", target_type="auth")
        db.commit()
        return result
    except guangya_sms.SmsError as exc:
        raise HTTPException(400, str(exc))


@app.post("/api/sms/refresh", dependencies=ADMIN)
async def sms_refresh(db: Session = Depends(get_db)):
    try:
        await guangya_sms.refresh(db)
        add_audit(db, "sms_token_refreshed_manual", target_type="auth")
        db.commit()
        return {"ok": True, "status": guangya_sms.status(db)}
    except guangya_sms.SmsError as exc:
        raise HTTPException(400, str(exc))


@app.get("/api/auth/openapi/verify", dependencies=ADMIN)
async def verify_openapi(db: Session = Depends(get_db)):
    """验证连接按授权通道分流。

    短信登录（普通用户）拿的是网页端凭证，没有开放平台应用 ID，调官方
    /openapi/v1/user/get_user_info 必然 code=120 缺少clientId。这里改用网页端
    文件列表接口做一次轻量探测，真实反映「能不能扫描/建分享」；只有官方
    Device/PKCE 高级授权才走 get_user_info。
    """
    t = db.get(AuthToken, 1)
    if t and t.flow in ("sms", "tgto", "webqr"):
        try:
            data = await GuangYaWebFiles(db).list_files(page_size=1)
            items = data.get("list") or []
            is_webqr = t.flow == "webqr"
            return {"ok": True, "channel": "web", "flow": t.flow,
                    "user_id": guangya_sms.status(db).get("account") or "",
                    "root_items": len(items),
                    "message": f"光鸭网页端连接正常（{'扫码' if is_webqr else '短信'}登录），可扫描与建分享"}
        except Exception as exc:
            raise HTTPException(502, f"网页端连接失败：{exc}")
    try:
        info = await GuangYaOpenAPI(db).get_user_info()
        return {"ok": True, "channel": "openapi", "user_id": info.get("userId", ""),
                "vip_status": info.get("vipStatus"), "message": "官方 OpenAPI 授权可用"}
    except GuangYaError as exc:
        if exc.code == 120:
            raise HTTPException(400, "官方 OpenAPI 缺少开放平台应用 ID（x-client-id）。"
                                     "普通用户用短信登录即可，无需此接口；若坚持官方 Device/PKCE 授权，"
                                     "需到光鸭开放平台申请应用并配置 GUANGYA_CLIENT_ID。")
        raise HTTPException(502, str(exc))
    except Exception as exc:
        raise HTTPException(502, str(exc))


# ---------- 扫描 ----------
def _effective_scan_root(db: Session) -> tuple[str, str]:
    """v0.8.1：扫描根目录直接引用「镜像同步」里填过的 B 库目录 ID，不再重复填写。

    优先级：B 镜像库 > ScanTask.root_id（高级覆盖）> .env SCAN_ROOT_ID。
    返回 (root_id, 来源标记)，供前端只读展示。
    """
    mr = db.scalar(select(MirrorRule).limit(1))
    if mr and str(mr.target_root_id or "").strip():
        return str(mr.target_root_id).strip(), "mirror_target"
    t = db.scalar(select(ScanTask).limit(1))
    if t and str(t.root_id or "").strip():
        return str(t.root_id).strip(), "scan_task"
    return str(settings.scan_root_id or ""), "env"


# v0.8.0：文档要求扫描周期用「时间间隔按钮」，不再让用户手写 Cron。
# 仍保留高级自定义 Cron 输入（前端折叠），但默认与常用选项均由预设生成。
SCAN_INTERVAL_PRESETS = [10, 15, 30, 60, 120, 360, 720, 1440]
MIRROR_INTERVAL_PRESETS = [15, 30, 60, 120, 360, 720, 1440]


def _interval_to_cron(minutes: int) -> str:
    if minutes <= 0:
        return ""
    if minutes < 60:
        return f"*/{minutes} * * * *"
    if minutes < 1440 and minutes % 60 == 0:
        return f"0 */{minutes // 60} * * *"
    if minutes == 1440:
        return "0 0 * * *"
    raise ValueError("不支持的间隔，请选择预设值或使用自定义 Cron")


def _cron_to_interval(cron: str) -> int:
    parts = (cron or "").split()
    if len(parts) != 5:
        return 0
    m, h = parts[0], parts[1]
    if m.startswith("*/") and h == "*" and all(p == "*" for p in parts[2:]):
        try:
            return int(m[2:]) if int(m[2:]) in SCAN_INTERVAL_PRESETS else 0
        except ValueError:
            return 0
    if m == "0" and h.startswith("*/") and all(p == "*" for p in parts[2:]):
        try:
            v = int(h[2:]) * 60
            return v if v in SCAN_INTERVAL_PRESETS else 0
        except ValueError:
            return 0
    if m == "0" and h == "0" and all(p == "*" for p in parts[2:]):
        return 1440
    return 0


@app.get("/api/scan-task", dependencies=ADMIN)
def scan_task(db: Session = Depends(get_db)):
    t = db.scalar(select(ScanTask).limit(1))
    c = db.get(ScanCursor, 1)
    last = db.scalar(select(ResourceScanBatch).order_by(ResourceScanBatch.id.desc()).limit(1))
    eff_root, root_src = _effective_scan_root(db)
    return {k: getattr(t, k) for k in ["id", "name", "cron", "root_id", "enabled", "last_run", "last_status", "last_message"]} | {
        "effective_root_id": eff_root, "root_source": root_src,
        "interval_minutes": _cron_to_interval(t.cron),
        "interval_presets": SCAN_INTERVAL_PRESETS,
        "cursor_status": c.status if c else "never",
        "cursor_files": c.files_count if c else 0,
        "cursor_dirs": c.dirs_count if c else 0,
        "cursor_error": c.error if c else "",
        "last_batch": ({"id": last.id, "status": last.status, "complete": last.complete,
                        "reconcile_allowed": last.reconcile_allowed, "seen_count": last.seen_count}
                       if last else None),
    }


class ScanConfig(BaseModel):
    cron: str = ""                    # 高级自定义；interval_minutes>0 时由预设生成
    interval_minutes: int = 0
    enabled: bool = True              # v0.8.3：不再接收 root_id，根目录只引用镜像同步的 B 库 ID


@app.put("/api/scan-task", dependencies=ADMIN)
def save_scan_task(p: ScanConfig, db: Session = Depends(get_db)):
    try:
        cron = _interval_to_cron(p.interval_minutes) if p.interval_minutes else (p.cron or "").strip()
        if not cron:
            raise ValueError("请选择扫描间隔，或填写自定义 Cron")
        trigger = CronTrigger.from_crontab(cron, timezone="Asia/Shanghai")
    except Exception as e:
        raise HTTPException(400, f"扫描计划无效：{e}")
    t = db.scalar(select(ScanTask).limit(1))
    # v0.8.3：不再把根目录 ID 存进 ScanTask 形成第二份配置，避免 B 库 ID 改了以后
    # 旧值固化。每次扫描（含定时任务）都由 _effective_scan_root() 实时取 B 库 ID。
    root_id, _ = _effective_scan_root(db)
    t.cron, t.enabled = cron, p.enabled
    add_audit(db, "scan_task_updated", target_type="scan", detail=json_dumps({"cron": cron, "root_id": root_id, "enabled": p.enabled}))
    db.commit()
    if scheduler.get_job("global_scan"):
        scheduler.remove_job("global_scan")
    if p.enabled:
        scheduler.add_job(_scheduled_scan, trigger, args=[root_id], id="global_scan", replace_existing=True,
                          max_instances=1, coalesce=True,
                          # v2.1.24：默认 misfire_grace_time=1 秒会把晚到的定时扫描丢掉
                          misfire_grace_time=300)
    return {"ok": True, "cron": cron, "interval_minutes": _cron_to_interval(cron),
            "timezone": "Asia/Shanghai"}


# ---------- v0.8.0：A 源头库 → B 镜像库 ----------
def _mirror_rule(db: Session, create: bool = False) -> MirrorRule | None:
    r = db.scalar(select(MirrorRule).limit(1))
    if not r and create:
        r = MirrorRule(name="默认镜像", enabled=False)
        db.add(r)
        db.flush()
    return r


# v2.1.24：镜像定时任务的 job id 与「补跑」参数集中在这里。
# 真机问题（2026-11-24）：同步间隔设 15 分钟，但 14:02 起 last_run 一直停在 13:47，
# 后台明明在跑、镜像界面却「同步执行中」卡住，30 分钟没有任何一轮定时同步触发。
# 排查结论：注册点只有启动（main.py:1028）与保存配置（:1850）两处，且启动那处是
# `try: ... except Exception: pass` —— 注册失败被**静默吞掉**，页面也无从查看
# 「下次同步时间」，于是「定时没注册」和「刚跑过」在页面上长得一模一样。
MIRROR_JOB_ID = "mirror_sync"
MIRROR_CATCHUP_JOB_ID = "mirror_sync_catchup"
MIRROR_MIN_INTERVAL_MINUTES = 5
# 补跑延后 20 秒：让 lifespan 里其余 job 与 DB 迁移先稳定，别和启动抢写锁。
MIRROR_CATCHUP_DELAY_SECONDS = 20
# **本版最关键的根因修复**：APScheduler 3.11 的 `misfire_grace_time` 默认是
# **1 秒**（实测 job_defaults = {'misfire_grace_time': 1, ...}）。只要这次触发
# 晚到 1 秒以上，APScheduler 就把它当「过期触发」**直接丢掉**，只在日志里留一行
# "Run time of job ... was missed by ..."。本容器里镜像一轮要跑几十分钟、事件循环
# 还被同步 DB 调用占着，定时镜像晚到几秒是常态 —— 于是「定时从来不跑，但手动点
# 立即同步完全正常」（手动路径根本不经过调度器）。这就是 last_run 一直不动的真相。
# 镜像同步是幂等全量比对，晚一小时跑也完全无害，所以宽限给足（下限 300 秒，
# 与间隔同宽但封顶 1 小时；coalesce=True 保证堆积的多次触发只会补跑一轮）。
MIRROR_MISFIRE_GRACE_FLOOR_SECONDS = 300
MIRROR_MISFIRE_GRACE_CAP_SECONDS = 3600


def _mirror_misfire_grace(minutes: int) -> int:
    """该间隔下允许「晚多久仍然开跑」（秒）。见上面 misfire_grace_time 的说明。"""
    span = max(MIRROR_MIN_INTERVAL_MINUTES, int(minutes or 0)) * 60
    return int(min(MIRROR_MISFIRE_GRACE_CAP_SECONDS,
                   max(MIRROR_MISFIRE_GRACE_FLOOR_SECONDS, span)))


def _schedule_mirror_job(minutes: int, enabled: bool) -> None:
    if scheduler.get_job(MIRROR_JOB_ID):
        scheduler.remove_job(MIRROR_JOB_ID)
    if enabled:
        scheduler.add_job(_scheduled_mirror,
                          IntervalTrigger(minutes=max(MIRROR_MIN_INTERVAL_MINUTES, int(minutes)),
                                          timezone="Asia/Shanghai"),
                          id=MIRROR_JOB_ID, replace_existing=True,
                          max_instances=1, coalesce=True,
                          misfire_grace_time=_mirror_misfire_grace(minutes))


def _mirror_next_run_at() -> str | None:
    """镜像定时任务的**下次触发时间**（上海时间字符串，未注册返回 None）。

    v2.1.24：此前状态接口只有 `last_run`，用户没法区分「定时没注册」和「刚跑完」。
    现在页面直接显示「下次自动同步：...」；未注册时如实显示「未注册」而不是留空。
    取不到就返回 None，绝不编一个时间。
    """
    try:
        job = scheduler.get_job(MIRROR_JOB_ID)
        if not job:
            return None
        next_run = getattr(job, "next_run_time", None)
        if not next_run:
            return None
        return next_run.astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:                 # 状态接口不能因为一个时间字段整页 500
        return None


def _mirror_missed_window(last_run, minutes: int, *, now=None) -> bool:
    """启动时判断「上一轮到现在是否已经超过一个间隔」——超了就补跑一轮。

    v2.1.24：进程停机/重启期间错过的轮次此前**不会补**，IntervalTrigger 从启动时刻
    重新计时，于是「重启一次 = 往后顺延一个间隔」。对 15 分钟档还行，对 6 小时档
    就是半天不跑。这里补一次（不是补 N 次 —— 镜像同步是幂等全量比对，补最新一轮就够）。
    从未跑过（last_run 为空）且已启用 → 也该立刻跑一轮，否则新库永远等第一个间隔。
    """
    if not minutes:
        return False
    now = now or utcnow()
    last = as_utc(last_run)
    if last is None:
        return True
    return (now - last) >= timedelta(minutes=max(MIRROR_MIN_INTERVAL_MINUTES, int(minutes)))


def _write_pipeline_snapshot(db: Session, r: MirrorRule | None, *,
                             mode: str, copied: int = 0, renamed: int = 0,
                             identified: int = 0, queued: int = 0,
                             copy_tasks: int = 0, scan_files: int = 0,
                             works_total: int = 0, works_done: int = 0,
                             enqueue_diag: dict | None = None,
                             works: dict | None = None,
                             running: bool = False, stage: str = "",
                             total_files: int = 0) -> None:
    """v0.9.10：把本轮任务各节点处理量写入 MirrorRule.last_pipeline_json。

    「处理流程与进度」只读这个快照显示当前任务量，不再拿全库累计计数冒充进度。
    任何一轮任务完成都覆盖写；没有快照时前端显示「尚无任务记录」。
    v0.9.10.1：带 enqueue_diag——真机「识别成功却没入队」必须把拦截原因
    写进快照，前端节点直接显示，不再靠 last_message 字符串解析（会被截断/覆盖）。
    v0.9.10.2：带 works（按 TMDB 去重的剧集/电影部数）、running/stage（实时阶段），
    供「进度看板」实时轮询展示各阶段真实数量变化；进度条只有分享+推送全完成才 100%。

    v2.1.11：拆掉同名字段漂移。
      旧 `total_files` 在不同帧里塞过两种完全不同的东西——复制任务数（rename/identify 帧）
      与扫描文件数（done 帧），同一字段名两种口径，前端只能靠猜。
      现在改为两个互不重叠的字段：
        copy_tasks  本轮提交的复制任务数（分子分母同口径，进度条用它）
        scan_files  本轮 B 库扫描到的文件数（入库口径，只做展示）
      老快照仍带 total_files，读侧保留兼容回退，不让升级后历史快照显示空进度。

      **输入端保留 `total_files` 兼容别名**（映射到 copy_tasks）：既有调用与
      回归测试都按这个参数名传值，直接改名会破坏契约。输出端（快照 JSON）
      只有 copy_tasks / scan_files 两个字段，不再有同名漂移。

    v2.1.11：补部数口径 works_total / works_done——用户口径里「部」才是主单位
      （单轮上限 max_works_per_run 按部、分享/推送按 TMDB 作品去重按部），
      此前部数只存在于 last_message 文本里，前端拿不到，进度无法按部表达。
    """
    if not r:
        return
    # 兼容别名：老调用只传 total_files 时，按「复制任务数」解读
    if not copy_tasks and total_files:
        copy_tasks = int(total_files or 0)
    snap = {"mode": mode, "copied": int(copied or 0), "renamed": int(renamed or 0),
            "identified": int(identified or 0), "queued": int(queued or 0),
            "copy_tasks": int(copy_tasks or 0), "scan_files": int(scan_files or 0),
            "works_total": int(works_total or 0), "works_done": int(works_done or 0),
            "at": utcnow().isoformat(),
            "enqueue_diag": dict(enqueue_diag or {}),
            "works": dict(works or {}), "running": bool(running), "stage": str(stage or "")}
    try:
        r.last_pipeline_json = json_dumps(snap)
        db.flush()
    except Exception:
        pass


def _work_type_key(media_type, tmdb_id) -> tuple[str, str] | None:
    """按 (media_type, tmdb_id) 归一作品键——「剧集 N 部 / 电影 M 部」去重口径。

    只有拿得到 TMDB ID 且类型明确的才算一部作品；未识别 / 无 ID 的不计入，
    避免把「集数」误当「部数」（用户明确：入库识别/生成分享/推送入队显示部数）。
    """
    mt = str(media_type or "").strip().lower()
    tid = tmdb_id
    if mt not in ("tv", "movie") or not tid:
        return None
    try:
        return (mt, str(int(tid)))
    except (TypeError, ValueError):
        return None


def _work_breakdown(keys) -> dict:
    """作品键集合 → {tv, movie, total}，供进度看板节点显示「剧集/电影」部数。"""
    tv = sum(1 for k in keys if k[0] == "tv")
    movie = sum(1 for k in keys if k[0] == "movie")
    return {"tv": tv, "movie": movie, "total": tv + movie}


async def _mirror_post_sync_pipeline(db: Session, r: MirrorRule) -> dict:
    """v0.8.5：镜像完成 → B 库刷新入库 → 识别 → 只推本轮识别成功且有海报的影视。

    用户 2026-11-18 明确口径（务必保持）：「镜像完成不是群推送，镜像完成，
    触发资源库入库刷新，识别影视，识别成功的影视就推送」。

    本管线只做三件事：
      1. 扫 B 库刷新资源库（与「立即扫描」共用 _effective_scan_root 与 run_scan）；
      2. 自动识别待识别资源；
      3. 本轮识别成功且拿到海报的影视按启用的 TG 分发规则入队
         （_enqueue_after_identify → _enqueue_ready：已识别+有海报闸门、同作品同频道去重）。

    明确不做（防群推红线）：
      - 不调用 process_change_events(publish=True)。v0.8.0~v0.8.4 刻意积压了
        queue_id=0 的内容变化事件，在此放开会一次性批量补推历史更新。
      - 不做对账删除、不重建分享、不取消分享、不回溯推送历史已识别资源。
    内容变化更新仍由用户主动的「立即扫描 / 重新扫描库」按既有机制处理。
    """
    root, root_source = _effective_scan_root(db)
    if not root:
        # 定时路径已校验 target_root_id 非空；这里只是防止 _effective_scan_root 取到空值
        root, root_source = str(r.target_root_id or "").strip(), "mirror_target"
    scan = await run_scan(root, resume=False)
    out = {
        "root_source": root_source,
        "files": int(scan.get("files", 0) or 0),
        "directories": int(scan.get("directories", 0) or 0),
        "complete": bool(scan.get("complete", False)),
    }
    # v2.1.11：入库后立刻把「映射回 A 库」的影视名/影视号/类型回填到资源库，
    # 识别与展示都走 A 库口径（B 库改过名的英文译名拿去 TMDB 几乎必然零命中）。
    # 顺序很重要：必须在 _post_scan_identify **之前**，否则这一轮识别仍用旧线索。
    # run_scan 用的是独立 Session 且已提交，这里先 commit 结束本会话的旧读事务，
    # 保证下面的 JOIN 能读到本轮刚入库的行。
    db.commit()
    out["source_mapped"] = backfill_source_titles(db)
    db.commit()
    ident = await _post_scan_identify(db, publish_after_identify=True, limit=500)
    out["identify"] = ident
    out["queued"] = int(ident.get("queued", 0) or 0)
    # v0.9.9 #201/#206：把入队拦截原因显露出来——真机「识别了却一条队列都没有」
    # 就卡在这里，此前四道拦截全静默 continue，用户与排查都看不到。
    diag = dict(ident.get("enqueue_diag") or {})
    out["enqueue_diag"] = diag
    if diag:
        label_map = {"skip_unidentified": "未识别", "skip_no_poster": "无海报闸门",
                     "skip_route_no_match": "目录路由未匹配", "skip_no_channel": "目标无启用频道",
                     "skip_dedup_or_disabled": "去重/停用", "skip_no_target": "未配置推送目标",
                     "skip_rule_dir_mismatch": "规则目录不符", "skip_rule_keyword_mismatch": "规则关键词不符",
                     "enqueued": "已入队"}
        pretty = "、".join(f"{label_map.get(k, k)} {v}" for k, v in diag.items() if v)
        r.last_message = ((r.last_message or "") + f" · 入队诊断：{pretty}")[-1000:]
    # 镜像 → 资源库事件（设计文档 §2.9）：批次级完成通知，供资源库精确定位本轮刷新范围；
    # 资源库仍以 B 库全量扫描为准，事件只加快刷新、不替代核验。
    try:
        from . import events as _ev
        _ev.emit_mirror_event(
            db, work_key=f"mirror:{r.id}:{scan.get('batch_id') or ''}",
            rule_id=int(r.id or 0), batch_id=str(scan.get("batch_id") or ""),
            title_standard=f"镜像同步完成 files={out['files']}",
            scope=_ev.MIRROR_COMPLETED)
    except Exception:
        pass
    # 只读统计，绝不在这里处理：内容变化事件仍由常规扫描链路消化
    out["changes_pending"] = int(db.scalar(
        select(func.count()).select_from(DistributionEvent).where(
            DistributionEvent.queue_id == 0)) or 0)
    return out


async def _scheduled_mirror() -> None:
    """定时镜像：同步 + 补映射；按 auto_identify 决定是否刷新入库并推送本轮新识别影视。

    v0.8.5 起不再写死「镜像完成不推 TG」：识别成功（拿到海报）的影视按分发规则推送。
    仍不对全库群发，也不批量补推历史内容变化事件。
    v2.1.11：**等待式**进入流水线闸门 —— 上一轮（复制→改名→翻译→识别→入库）
    还没彻底结束，本轮定时镜像不并发启动，而是排队等它结束再跑（不丢轮次）。
    """
    if not await _mirror_lock.acquire_async("定时镜像", timeout=PIPELINE_WAIT_SECONDS):
        # v2.1.24：这条原来是 logger.info —— 「本轮定时镜像放弃」这种**真正丢轮次**
        # 的事件在默认 INFO 级别下和噪声没区别，排查时根本看不见。升为 WARNING 并
        # 带上占用者与等待时长，页面侧还能从 last_run 不往前走间接看出来。
        info = _mirror_lock.info() or {}
        logger.warning("[mirror] 等待上一轮任务结束超时（%s 秒），本轮定时镜像放弃；"
                       "当前占用者：%s，已占用 %s 秒",
                       PIPELINE_WAIT_SECONDS, info.get("owner") or "未知",
                       info.get("held_seconds"))
        return
    _mirror_lock.set_owner("定时镜像")
    try:
        with SessionLocal() as db:
            try:
                r = db.scalar(select(MirrorRule).where(MirrorRule.enabled.is_(True)).limit(1))
                if r and r.source_root_id and r.target_root_id:
                    # v2.1.11：定时路径补齐中间帧。此前定时路径只在整轮结束时写一帧
                    # 且 copied/renamed 硬编码 0，页面看定时同步时进度条全程停在
                    # 「复制 0 项」，与手动同步表现完全不同（同一套 UI 两套口径）。
                    # 现在定时路径与手动路径共用同一套快照字段与阶段语义。
                    _write_pipeline_snapshot(db, r, mode="定时同步", stage="copy", running=True)
                    db.commit()
                    stats = await mirror.sync_rule(db, r)
                    await mirror.backfill_target_file_ids(db, r)
                    # v0.8.6：改名必须赶在「扫描入库」之前，否则 Resource 记的是旧中文名。
                    # 只改 B 库单集/单电影文件名，A 库与 B 库目录名都不动。
                    # v0.8.9：TMDB 查不到英文名会快速直译兜底，不阻断下面的入库/推送。
                    # v2.1.0 改名硬门禁（与手动 mirror_run 同一口径）：改名任一文件失败
                    # 或整体异常 → 本轮不进入库识别推送，杜绝英文名没改对就先推群/频道。
                    # v2.1.11：门禁**收敛到作品级**（详见 RENAME_GATE_BLOCK_WHOLE_ROUND
                    # 处说明）：真机上一部剧的「假失败」（光鸭 160，实际早已改名成功）
                    # 把同轮另外 19 部作品全拦在门外、资源库里一条都没有。
                    # 「英文名没改对不推群/频道」这条原意由推送侧的作品级闸门兜底。
                    rename_failed = False
                    rename_stats: dict = {}
                    if bool(getattr(r, "rename_files", True)):
                        rn = await _safe_rename(db, r)
                        rename_stats = dict(rn or {})
                        rename_failed = bool((rn or {}).get("errors")) or int((rn or {}).get("failed") or 0) > 0
                        if rename_failed:
                            bad_works = [w for w in ((rn or {}).get("failed_works") or []) if w]
                            logger.warning(
                                "[mirror] 定时镜像：%s 部作品改名未完成（%s）——这些作品本轮"
                                "不入队推送，其余作品照常入库识别",
                                len(bad_works) or 1, "、".join(bad_works[:3]))
                            r.last_message = (
                                str(r.last_message or "")
                                + f" · 改名未完成 {len(bad_works) or 1} 部作品"
                                  "（这些作品本轮不入队推送，其余作品照常入库）")
                            db.commit()
                    # 改名完成 → 写「入库识别中」中间帧（与手动路径同语义）
                    _write_pipeline_snapshot(
                        db, r, mode="定时同步", stage="identify", running=True,
                        copied=int(stats.get("done", 0) or 0),
                        renamed=int(rename_stats.get("renamed", 0) or 0),
                        copy_tasks=int(stats.get("done", 0) or 0),
                        works_total=int(stats.get("batch_size", 0) or 0))
                    db.commit()
                    gate_block_round = rename_failed and RENAME_GATE_BLOCK_WHOLE_ROUND
                    if bool(getattr(r, "auto_identify", True)) and not gate_block_round:
                        p = await _mirror_post_sync_pipeline(db, r)
                        ident = p.get("identify") or {}
                        _write_pipeline_snapshot(
                            db, r, mode="定时同步", stage="done", running=False,
                            copied=int(stats.get("done", 0) or 0),
                            renamed=int(rename_stats.get("renamed", 0) or 0),
                            identified=int(ident.get("identified", 0) or 0),
                            queued=int(p.get("queued", 0) or 0),
                            copy_tasks=int(stats.get("done", 0) or 0),
                            scan_files=int(p.get("files", 0) or 0),
                            works_total=int(stats.get("batch_size", 0) or 0),
                            works_done=int(stats.get("media", 0) or 0),
                            enqueue_diag=dict(p.get("enqueue_diag") or {}),
                            works=dict(ident.get("work_stats") or {}))
                        db.commit()
                        # 追加而非覆盖：v0.8.4 的文件级复制错误就写在 last_message
                        tail = (f" | [自动入库] 扫 {p['files']} 文件 · 识别 "
                                f"{p['identify']['identified']} · 入队 {p['queued']}"
                                f" · 内容变化待处理 {p['changes_pending']}")
                        r.last_message = (str(r.last_message or "") + tail)[-1000:]
                        db.commit()
                    else:
                        # v2.1.11：定时路径关入库 / 改名门禁拦截时也要把 running 归位。
                        # 此前这条分支不写终态帧 → 进度条永远停在「入库识别中」，
                        # 页面看着像还在跑，实际早就结束了。
                        _write_pipeline_snapshot(
                            db, r, mode="定时同步", stage="done", running=False,
                            copied=int(stats.get("done", 0) or 0),
                            renamed=int(rename_stats.get("renamed", 0) or 0),
                            copy_tasks=int(stats.get("done", 0) or 0),
                            works_total=int(stats.get("batch_size", 0) or 0),
                            works_done=int(stats.get("media", 0) or 0))
                        db.commit()
            except Exception as exc:
                db.rollback()
                try:
                    r = db.scalar(select(MirrorRule).limit(1))
                    if r:
                        r.last_status, r.last_message = "failed", str(exc)[:1000]
                        db.commit()
                except Exception:
                    pass
    finally:
        _mirror_lock.release()


class MirrorConfigInput(BaseModel):
    name: str = "默认镜像"
    source_root_id: str = ""     # A 源头库目录（只读）
    target_root_id: str = ""     # B 镜像库目录
    interval_minutes: int = 60
    enabled: bool = False
    star_naming: bool = True
    auto_identify: bool = True
    # v0.8.1：文件操作空闲秒数（硬下限不可调低）、B 库缺目录自动创建
    file_idle_seconds: float = 1.0
    auto_create_dirs: bool = True
    # v0.8.6：B 库单集/单电影文件按英文剧名改名（只换中文片名段）
    rename_files: bool = True
    # v0.9.0：单次同步最多整理多少部整部剧/电影（0=不限量，到上限下轮接着做）
    max_works_per_run: int = 0
    # 镜像1.1：镜像模式 full（全镜像，默认：A 已删→删 B 库+清清单）/ half（半镜像：都保留）
    mirror_mode: str = "full"


class MirrorRetryInput(BaseModel):
    """v0.9.17.2：失败任务定向批量重试的勾选行 id。"""
    ids: list[int] = []


class MirrorCleanupInput(BaseModel):
    """v2.1.11：统一「清理无效」端点的入参。

    scope   清理范围：file（文件映射）/ dir（目录映射）/ pruned（僵尸目录行）
            / media（影视映射行）
    dry_run 只返回预览清单不写库
    rule_id 限定镜像规则（0 = 当前生效规则）
    ids     限定只处理这些行 id（空 = 该 scope 下全部）
    """
    scope: str = "file"
    dry_run: bool = False
    rule_id: int = 0
    ids: list[int] = []


@app.get("/api/mirror/config", dependencies=ADMIN)
def mirror_config_get(db: Session = Depends(get_db)):
    r = _mirror_rule(db)
    return {
        "name": r.name if r else "默认镜像",
        "source_root_id": r.source_root_id if r else "",
        "target_root_id": r.target_root_id if r else "",
        "interval_minutes": r.interval_minutes if r else 60,
        "enabled": bool(r.enabled) if r else False,
        "star_naming": bool(r.star_naming) if r else True,
        "auto_identify": bool(r.auto_identify) if r else True,
        "file_idle_seconds": float(r.file_idle_seconds or 1.0) if r else 1.0,
        "min_idle_seconds": mirror.MIN_FILE_IDLE_SECONDS,
        "auto_create_dirs": (True if getattr(r, "auto_create_dirs", None) is None
                              else bool(r.auto_create_dirs)) if r else True,
        # v0.8.6：改名开关；NULL 一律按默认开启（与 auto_create_dirs 同一防御口径）
        "rename_files": (True if getattr(r, "rename_files", None) is None
                         else bool(r.rename_files)) if r else True,
        # v0.9.0：单轮作品上限与当前轮转游标（0=不限量）
        "max_works_per_run": max(0, int(getattr(r, "max_works_per_run", 0) or 0)) if r else 0,
        "works_cursor": str(getattr(r, "works_cursor", "") or "") if r else "",
        # 镜像1.1：镜像模式（runtime_config 存储，默认 full 全镜像）
        "mirror_mode": str(runtime_config.get(db, "mirror_mode") or "full").lower().strip(),
        "interval_presets": MIRROR_INTERVAL_PRESETS,
        "last_run": r.last_run if r else None,
        "last_status": r.last_status if r else "never",
        "last_message": r.last_message if r else "",
        # v2.1.24：定时任务的下次触发时间（上海时间）。用户排查「间隔失效」时，
        # 原来只有 last_run 可看 —— 「定时没注册」和「刚跑过」在页面上长得一样。
        # 未注册返回 None，前端据此如实显示「定时未注册」，不留空也不编时间。
        "next_run_at": _mirror_next_run_at(),
        "schedule_registered": scheduler.get_job(MIRROR_JOB_ID) is not None,
        "confirmed_endpoints": mirror.CONFIRMED_WRITE_ENDPOINTS,
        "unverified_endpoints": mirror.UNVERIFIED_ENDPOINTS,
        "note": ("v0.8.6：A 库只读且目录名不变；B 库分类目录按原名 1:1 镜像、影视主目录按 · 约定命名，"
                 "括号年份与 TMDB 标记保持原样。单集/单电影文件按英文剧名改名（TMDB en-US 优先、"
                 "AI 翻译兜底），只替换中文片名段，季集号/年份/画质/压制组/扩展名原样保留；"
                 "A→B 映射靠 source_file_id + 指纹保持，改名不影响后期变量更新比对。"
                 "每个文件操作之间留可配置空闲时间，下限来自官方写接口 2 次/秒限频，不可调低。"
                 "同步完成后默认刷新入库、识别，并把本轮识别成功且有海报的影视按 TG 分发规则入队；"
                 "不做全库群发，也不批量补推历史内容变化事件。"),
    }


@app.post("/api/mirror/config", dependencies=ADMIN)
def mirror_config_save(p: MirrorConfigInput, db: Session = Depends(get_db)):
    if p.interval_minutes not in MIRROR_INTERVAL_PRESETS:
        raise HTTPException(400, "同步间隔请选择预设值")
    # v0.8.1：官方写接口 2 次/秒 → 0.5 秒是不可调低的硬下限，前端 min 只是第一道约束
    if float(p.file_idle_seconds) < mirror.MIN_FILE_IDLE_SECONDS:
        raise HTTPException(400, f"文件操作间隔不能低于 {mirror.MIN_FILE_IDLE_SECONDS} 秒："
                                "该下限来自官方写接口 2 次/秒限频，不可调整")
    if float(p.file_idle_seconds) > 60:
        raise HTTPException(400, "文件操作间隔不能超过 60 秒")
    r = _mirror_rule(db, create=True)
    r.name = (p.name or "默认镜像").strip()
    r.source_root_id, r.target_root_id = p.source_root_id.strip(), p.target_root_id.strip()
    r.interval_minutes, r.enabled = p.interval_minutes, p.enabled
    r.star_naming, r.auto_identify = p.star_naming, p.auto_identify
    r.file_idle_seconds, r.auto_create_dirs = float(p.file_idle_seconds), p.auto_create_dirs
    r.rename_files = p.rename_files
    # v0.9.0：单轮作品上限。改为 0（不限量）时清空轮转游标，避免残留；
    # 调整上限时游标保留，继续按上次进度轮转。
    new_cap = max(0, min(int(p.max_works_per_run or 0), 1000))
    if new_cap == 0:
        r.works_cursor = ""
    r.max_works_per_run = new_cap
    # 镜像1.1：镜像模式（full 全镜像 / half 半镜像），写入 runtime_config。
    mode = str(p.mirror_mode or "full").lower().strip()
    if mode not in ("full", "half"):
        raise HTTPException(400, "镜像模式只能是 full（全镜像）或 half（半镜像）")
    runtime_config.set_many(db, {"mirror_mode": mode})
    add_audit(db, "mirror_config_saved", target_type="mirror",
              detail=json_dumps({"source": r.source_root_id, "target": r.target_root_id,
                                 "interval": r.interval_minutes,
                                 "idle": r.file_idle_seconds, "auto_dirs": r.auto_create_dirs,
                                 "rename_files": r.rename_files,
                                 "max_works_per_run": r.max_works_per_run,
                                 "mirror_mode": mode}))
    db.commit()
    # v2.1.24：注册失败不能静默（原来直接抛 500，配置已提交但用户不知道定时没生效；
    # 更糟的是启动路径那边直接 pass）。现在配置照存，把失败原因**如实回给前端**。
    schedule_error = ""
    try:
        _schedule_mirror_job(r.interval_minutes, r.enabled)
    except Exception as exc:
        logger.error("[mirror] 保存配置后注册定时同步失败：%s", exc, exc_info=True)
        schedule_error = f"{type(exc).__name__}: {exc}"
    return {"ok": True, "enabled": r.enabled, "interval_minutes": r.interval_minutes,
            "next_run_at": _mirror_next_run_at(),
            "schedule_error": schedule_error}


@contextmanager
def _mirror_guard():
    """v0.8.9：镜像流水线互斥，拿不到闸门立刻 409，不让长任务排队堆积。

    用户等不及连点「立即同步」时，多个分钟级任务并发会互相抢 SQLite 写锁、
    成倍放大列目录与 rename 请求，管理页所有请求排队到 30 秒 busy_timeout，
    表现就是「整理剧集改名时后台页面卡死打不开」。宁可明确拒绝，也不排队。

    v2.1.11：闸门升级为「复制 / 改名 / 翻译 / 识别 / 入库」全链路共用一把，
    这里的 409 文案也随之更新——占用者可能是任意一段流水线，且后台定时任务
    现在是「排队等」而不是「跳过」，所以提示要说明「等它跑完即可」。
    """
    if not _mirror_lock.acquire(blocking=False, owner="手动同步 / 改名"):
        info = _mirror_lock.info()
        owner = info.get("owner") or "未知任务"
        held = info.get("held_seconds", 0)
        waiting = int(info.get("waiting", 0) or 0)
        wait_hint = (f"；另有 {waiting} 个定时任务正在排队等它结束"
                     if waiting else "")
        raise HTTPException(
            409, f"已有镜像同步 / 改名 / 识别任务「{owner}」正在运行"
                 f"（已持续约 {held} 秒）{wait_hint}；"
                 "本流程一轮要走完「复制 → 改名 → 翻译 → 识别 → 入库资源库」，"
                 "上一轮彻底结束后才会启动下一轮；"
                 f"若长时间（>30 分钟）仍报此错，重启容器即可清除残留锁。")
    try:
        yield
    finally:
        _mirror_lock.release()


async def _safe_rename(db: Session, r: MirrorRule, **kwargs) -> dict:
    """v0.8.6：改名是可选优化，整体失败绝不能拖垮已完成的复制与后续入库 / 推送。

    `rename_mirror_files()` 已逐文件捕获 `rename_file` 异常；这里再兜住「列 B 库 /
    解析英文名」等整体异常，把原因写进 `rename.errors` 与日志。故意**不 rollback**：
    `sync_rule()` / `backfill_target_file_ids()` 在此之前已各自提交，回滚反而可能
    让 session 状态与后续扫描管线互相干扰。
    """
    try:
        return await mirror.rename_mirror_files(db, r, **kwargs)
    except Exception as exc:
        logger.warning("[mirror] 文件改名整体失败，不阻断同步：%s", exc)
        # v2.1.12 #295：这条 errors 会透到前端 toast 与 last_message，
        # 先洗掉上游内部标识（服务名 / RPC 方法 / 接口路径 / 业务码）再拼文案。
        msg = f"{type(exc).__name__}: {scrub_upstream_internals(str(exc))}"[:300]
        return {"works": 0, "renamed": 0, "skipped": 0, "ascii": 0,
                "untranslated": 0, "failed": 0, "no_rows": 0,
                "literal_ai": 0, "literal_pinyin": 0,
                "errors": [f"文件改名未执行：{msg}"], "notices": [],
                "remaining": 0, "truncated_reason": "",
                "untranslated_works": [], "idle_seconds": 0}


async def _do_mirror_run(post_identify: bool | None, db: Session,
                         strict: bool = False) -> dict:
    """v0.8.5：立即同步；默认按 mirror.auto_identify 决定是否接「入库 → 识别 → 推送」管线。

    post_identify 不传（前端「立即同步」）= 遵循配置开关；
    False（「仅同步」）= 跳过管线；True（「强制入库识别」）= 即使开关关闭也执行。
    管线只推本轮识别成功且有海报的影视，不做全库群发、不补推历史内容变化事件。
    """
    r = _mirror_rule(db, create=True)
    if not r.source_root_id or not r.target_root_id:
        raise HTTPException(400, "请先在镜像设置里填写 A 源头库目录 ID 与 B 镜像库目录 ID")
    # auto_identify 在 v0.8.4 及之前只是「存了、回显了」的死配置，v0.8.5 起真正接线
    do_pipeline = (bool(getattr(r, "auto_identify", True))
                   if post_identify is None else bool(post_identify))
    try:
        # v0.9.10.2：任务开始即写「运行中·复制到 B 库」阶段快照并立即落盘，
        # 前端 2 秒轮询能立刻看到任务启动与各节点数量推进，不再等整轮结束才更新。
        _write_pipeline_snapshot(db, r, mode="镜像同步", stage="copy", running=True)
        db.commit()
        _mirror_lock.heartbeat()  # v0.9.21：阶段推进打点（复制开始）
        # v2.1.13：严格顺序引擎已移除，只有一套复制路径；strict 仅作为兼容入参透传（被忽略）
        stats = await mirror.sync_rule(db, r, strict=strict)
        stats["target_file_ids_backfilled"] = await mirror.backfill_target_file_ids(db, r)
        # 复制完成：先落一帧「复制 N / 改名待办」快照（网络 await 前不持写事务）
        # v2.1.11：copy_tasks 用复制任务数（分子分母同口径），不再复用 total_files。
        _write_pipeline_snapshot(db, r, mode="镜像同步", stage="rename", running=True,
                                 copied=int(stats.get("done", 0) or 0),
                                 copy_tasks=int(stats.get("done", 0) or 0),
                                 works_total=int(stats.get("batch_size", 0) or 0))
        db.commit()
        _mirror_lock.heartbeat()  # v0.9.21：阶段推进打点（复制完成→改名）
        # v0.8.6：改名赶在「扫描入库」之前，只动 B 库单集/单电影文件名；
        # A 库与 B 库目录名不变，A→B 映射靠 MirrorFile 保持。
        stats["rename_enabled"] = bool(getattr(r, "rename_files", True))
        if stats["rename_enabled"]:
            stats["rename"] = await _safe_rename(db, r)
            # v0.8.8：改名整体异常 / 单文件失败不能只留在本次响应里；
            # 追加到 last_message，刷新「同步状态」页仍能看到真实原因。
            rename_errors = (stats["rename"] or {}).get("errors") or []
            if rename_errors:
                # 新建规则 / 旧库缺列时 last_message 可能是 None，别让一条诊断把整次同步拖成 failed
                r.last_message = ((r.last_message or "")
                                  + " · 首个改名错误：" + str(rename_errors[0])[:300])
        stats["pipeline_enabled"] = do_pipeline
        # v2.1.0 改名硬门禁（用户决策：改名失败阻断后续入库识别推送）：
        # 改名任一文件失败或整体异常 → 本轮不进后置管线，等人工处理/下一轮重试，
        # 杜绝「英文名没改对就先推给群/频道」。仅 rename_files 开启时才校验。
        # v2.1.11：门禁**收敛到作品级**（见 RENAME_GATE_BLOCK_WHOLE_ROUND 处说明）：
        # 真机上一部剧的假失败（光鸭 160）把同轮 19 部健康作品一起拦在门外、
        # 资源库里一条都没有。「英文名没改对不推群/频道」由推送侧的作品级闸门兜底。
        rename_failed = False
        if stats["rename_enabled"]:
            rn = stats.get("rename") or {}
            rename_failed = bool(rn.get("errors")) or int(rn.get("failed") or 0) > 0
        gate_block_round = rename_failed and RENAME_GATE_BLOCK_WHOLE_ROUND
        stats["rename_gate_blocked"] = gate_block_round
        if rename_failed:
            bad_works = [w for w in ((stats.get("rename") or {}).get("failed_works") or []) if w]
            stats["rename_failed_works"] = bad_works
            logger.warning(
                "[mirror] %s 部作品改名未完成（%s）——这些作品本轮不入队推送，"
                "其余作品照常入库识别",
                len(bad_works) or 1, "、".join(bad_works[:3]))
            r.last_message = ((r.last_message or "")
                              + f" · 改名未完成 {len(bad_works) or 1} 部作品"
                                "（这些作品本轮不入队推送，其余作品照常入库）")
        if do_pipeline and not gate_block_round:
            # 改名完成 → 写「入库识别中」阶段帧
            _write_pipeline_snapshot(db, r, mode="镜像同步", stage="identify", running=True,
                                     copied=int(stats.get("done", 0) or 0),
                                     renamed=int((stats.get("rename") or {}).get("renamed", 0) or 0),
                                     copy_tasks=int(stats.get("done", 0) or 0),
                                     works_total=int(stats.get("batch_size", 0) or 0))
            db.commit()
            _mirror_lock.heartbeat()  # v0.9.21：阶段推进打点（改名完成→入库识别）
            # v0.8.9：找不到官方英文名的作品在改名环节已直接直译兜底，绝不在这里
            # 再补跑一次解析（会重复触发 TMDB / AI 网络等待）。复制 → 入库 →
            # 识别 → 分享/推送 主链路必须照常走完，改名只是可选优化。
            stats["pipeline"] = await _mirror_post_sync_pipeline(db, r)
            # v0.9.10：写本轮任务快照，进度节点只显示本轮量不显示历史累计
            pipe = stats["pipeline"] or {}
            ident = pipe.get("identify") or {}
            # v0.9.10.2：works 用识别/入队这两组 TMDB 去重作品数（剧集/电影部数）；
            # stage=done + running=False 表示本轮管线已跑完，进度条据此判断是否可到 100%。
            _write_pipeline_snapshot(
                db, r, mode="镜像同步", stage="done", running=False,
                copied=int(stats.get("done", 0) or 0),
                renamed=int((stats.get("rename") or {}).get("renamed", 0) or 0),
                identified=int(ident.get("identified", 0) or 0),
                queued=int(pipe.get("queued", 0) or 0),
                copy_tasks=int(stats.get("done", 0) or 0),
                scan_files=int(pipe.get("files", 0) or 0),
                works_total=int(stats.get("batch_size", 0) or 0),
                works_done=int(stats.get("media", 0) or 0),
                enqueue_diag=dict(pipe.get("enqueue_diag") or {}),
                works=dict(ident.get("work_stats") or {}))
        else:
            # 仅同步 / 定时关入库：也要把 running 归位，避免进度条永远显示「运行中」
            _write_pipeline_snapshot(db, r, mode="镜像同步", stage="done", running=False,
                                     copied=int(stats.get("done", 0) or 0),
                                     renamed=int((stats.get("rename") or {}).get("renamed", 0) or 0),
                                     copy_tasks=int(stats.get("done", 0) or 0),
                                     works_total=int(stats.get("batch_size", 0) or 0),
                                     works_done=int(stats.get("media", 0) or 0))
        add_audit(db, "mirror_run", target_type="mirror",
                  detail=json_dumps({"media": stats.get("media"), "new": stats.get("new"),
                                     "done": stats.get("done"), "unknown": stats.get("unknown"),
                                     "renamed": (stats.get("rename") or {}).get("renamed"),
                                     "pipeline": stats.get("pipeline")}))
        db.commit()
        return stats
    except WebFileError as exc:
        logger.warning("[mirror] 同步被接口错误中止：%s", exc)
        db.rollback()
        rr = db.scalar(select(MirrorRule).limit(1))
        if rr:
            rr.last_status, rr.last_message = "failed", str(exc)[:1000]
            # v0.9.10.2：失败也要让快照 running 归位，杜绝进度条永久「运行中」
            _write_pipeline_snapshot(db, rr, mode="镜像同步", stage="failed", running=False)
            db.commit()
        raise HTTPException(400, str(exc))
    except Exception as exc:
        # v0.8.1：不再把真实异常吞成笼统 502，保留类型与消息并落库，便于直接排查
        logger.exception("[mirror] 镜像同步异常")
        db.rollback()
        detail = f"{type(exc).__name__}: {exc}"
        if "database is locked" in detail.lower():
            detail += ("（数据库写冲突：请确认只运行一个容器实例、没有两个 compose 项目共用同一个 ./db；"
                       "v0.8.8 已启用 WAL 与 30 秒写等待，稍后重试即可）")
        rr = db.scalar(select(MirrorRule).limit(1))
        if rr:
            rr.last_status, rr.last_message = "failed", detail[:1000]
            _write_pipeline_snapshot(db, rr, mode="镜像同步", stage="failed", running=False)
            db.commit()
        raise HTTPException(502, f"镜像同步失败：{detail}")


@app.post("/api/mirror/run", dependencies=ADMIN)
async def mirror_run(post_identify: bool | None = None, strict: bool = False,
                     db: Session = Depends(get_db)):
    """v0.8.9：立即同步入口加互斥；重复点击直接 409，不让长任务排队堆积。

    v2.1.13：严格顺序引擎已移除，只剩一套复制路径。`strict` 参数保留仅为
    兼容旧前端/旧脚本（`?strict=1` 仍然能调通），传入后不再改变任何行为。
    """
    with _mirror_guard():
        return await _do_mirror_run(post_identify, db, strict=strict)


async def _do_mirror_retry_files(p: MirrorRetryInput, post_identify: bool | None,
                                 db: Session) -> dict:
    r = _mirror_rule(db)
    if not r:
        raise HTTPException(400, "请先在镜像设置里保存 A / B 库目录 ID")
    try:
        out = await mirror.retry_failed_files(db, r, p.ids)
        out["post_sync"] = None
        do_pipeline = bool(getattr(r, "auto_identify", True)) if post_identify is None else bool(post_identify)
        if do_pipeline and (out.get("done") or out.get("reused")):
            out["post_sync"] = await _mirror_post_sync_pipeline(db, r)
        add_audit(db, "mirror_retry_files", target_type="mirror",
                  detail=json_dumps({"selected": out.get("selected"), "done": out.get("done"),
                                     "reused": out.get("reused"), "failed": out.get("failed")}))
        db.commit()
        return out
    except WebFileError as exc:
        db.rollback()
        raise HTTPException(400, str(exc))
    except Exception as exc:
        db.rollback()
        raise HTTPException(502, f"定向重试文件失败：{type(exc).__name__}: {exc}")


@app.post("/api/mirror/retry/files", dependencies=ADMIN)
async def mirror_retry_files(p: MirrorRetryInput, post_identify: bool | None = None,
                             db: Session = Depends(get_db)):
    """v0.9.17.2：勾选失败文件定向重发复制；不扫全库。与同步/改名共用互斥锁。"""
    with _mirror_guard():
        return await _do_mirror_retry_files(p, post_identify, db)


@app.post("/api/mirror/retry/dirs", dependencies=ADMIN)
async def mirror_retry_dirs(p: MirrorRetryInput, db: Session = Depends(get_db)):
    """v0.9.17.2：勾选失败目录定向重建（create_dir）；不扫全库。"""
    r = _mirror_rule(db)
    if not r:
        raise HTTPException(400, "请先在镜像设置里保存 A / B 库目录 ID")
    with _mirror_guard():
        try:
            out = await mirror.retry_failed_dirs(db, r, p.ids)
            add_audit(db, "mirror_retry_dirs", target_type="mirror",
                      detail=json_dumps({"selected": out.get("selected"),
                                         "recreated": out.get("recreated"),
                                         "failed": out.get("failed")}))
            db.commit()
            return out
        except WebFileError as exc:
            db.rollback()
            raise HTTPException(400, str(exc))
        except Exception as exc:
            db.rollback()
            raise HTTPException(502, f"定向重建目录失败：{type(exc).__name__}: {exc}")


async def _do_mirror_rename_files(force: bool, db: Session) -> dict:
    """v0.8.6：手动对 B 库已同步的单集 / 单电影文件批量改名（存量一键处理）。

    不重新复制文件、不改 A 库、不改 B 库目录名；只按已存的 A→B 映射逐个定位 B fileId。
    `force=True` 会重新解析每部作品的英文剧名（TMDB en-US / AI 翻译缓存失效）。
    即使关闭「自动改名」开关，这个手动按钮仍然可用。
    """
    r = _mirror_rule(db)
    if not r:
        raise HTTPException(400, "请先在镜像设置里保存 A / B 库目录 ID")
    try:
        out = await mirror.rename_mirror_files(db, r, force=force)
        # v2.1.12 #295：手动改名的数量此前**从不进看板**。
        # 「本轮改名」读的是 last_pipeline_json.renamed，而写快照的只有
        # 立即同步 / 定时同步 / 手动扫描三条路径，手动改名一条都没写 →
        # 用户手动改完 50 项，任务中心还停在上一轮镜像同步的数字，看起来像「没生效」。
        # 这里补写一轮「只改名」快照，让看板如实反映这一轮。
        _write_pipeline_snapshot(
            db, r, mode="手动改名", stage="manual_rename", running=False,
            renamed=int(out.get("renamed") or 0))
        add_audit(db, "mirror_files_renamed", target_type="mirror",
                  detail=json_dumps({"renamed": out["renamed"], "failed": out["failed"],
                                     "untranslated": out["untranslated"], "force": force}))
        db.commit()
        return out
    except WebFileError as exc:
        db.rollback()
        # v2.1.12 #295：异常文案会直接进前端提示，转成中性标签（不带上游服务名/方法名）。
        raise HTTPException(400, mirror.rename_error_label(str(exc)))
    except Exception as exc:
        db.rollback()
        raise HTTPException(
            502, f"文件改名失败：{type(exc).__name__}: {scrub_upstream_internals(str(exc))}")


@app.post("/api/mirror/rename", dependencies=ADMIN)
async def mirror_rename_files(force: bool = False, db: Session = Depends(get_db)):
    """v0.8.9：与「立即同步」共用互斥锁，避免两个长任务并发拖死管理页。"""
    with _mirror_guard():
        return await _do_mirror_rename_files(force, db)


@app.post("/api/mirror/rename/untranslated/clear", dependencies=ADMIN)
def mirror_clear_untranslated(db: Session = Depends(get_db)):
    """v0.9.6：清空「英文改名待处理」清单——把当前英文名为空的作品标记 ignored。

    这些行多是已不在 A 库 / 已停止追更的历史映射，英文名为空却每次同步都进待处理清单，
    看着像「过期没清」。本端点不删映射、不动文件，只置 english_source=ignored 让其从
    清单消失。

    v2.1.4 真机修复（用户反馈：清空后这 10 部作品**永远不再翻译**）：
      - **加 rule_id 过滤**：此前查全表 MirrorMedia，多规则场景会误清别的规则的作品；
      - **ignored 不再是永久拉黑**：标记语义收敛为「该作品没有新内容时跳过解析」。
        同步侧一旦发现 A 库又出现新文件，会自动解除该标记并重新解析英文名
        （见 mirror.py sync_rule 的 has_new_source_file 分支），
        所以「清空清单」不会让这些作品永久失去英文名。
      - 仍可随时点「重新解析英文名并改名」(force=True) 立即全部恢复解析。
    """
    rule = _mirror_rule(db)
    if not rule:
        raise HTTPException(400, "请先在镜像设置里保存 A / B 库目录 ID")
    rows = list(db.scalars(select(MirrorMedia).where(
        MirrorMedia.rule_id == rule.id,
        or_(MirrorMedia.english_title.is_(None), MirrorMedia.english_title == ""),
        or_(MirrorMedia.english_source.is_(None), MirrorMedia.english_source != "ignored")
    )).all())
    cleared = len(rows)
    for m in rows:
        m.english_source = "ignored"
    if cleared:
        add_audit(db, "mirror_untranslated_cleared", target_type="mirror",
                  detail=json_dumps({"cleared": cleared}))
    db.commit()
    return {"ok": True, "cleared": cleared,
            "note": "已标记忽略的作品若后续在 A 库出现新文件，会自动恢复英文名解析；"
                    "也可点「重新解析英文名并改名」立即恢复"}


async def _do_mirror_poll_tasks(db: Session) -> dict:
    r = _mirror_rule(db)
    if not r:
        return {"tasks": 0, "done": 0, "still_unknown": 0}
    try:
        out = await mirror.poll_pending_tasks(db, r)
        if out["done"]:
            out["target_file_ids_backfilled"] = await mirror.backfill_target_file_ids(db, r)
            if bool(getattr(r, "rename_files", True)):
                out["rename"] = await _safe_rename(db, r)
        return out
    except Exception as exc:
        raise HTTPException(502, str(exc))


@app.post("/api/mirror/tasks/poll", dependencies=ADMIN)
async def mirror_poll_tasks(db: Session = Depends(get_db)):
    """v0.8.9：任务轮询也会触发改名，纳入同一把互斥锁。"""
    with _mirror_guard():
        return await _do_mirror_poll_tasks(db)


@app.post("/api/mirror/cleanup", dependencies=ADMIN)
async def mirror_cleanup(p: MirrorCleanupInput, db: Session = Depends(get_db)):
    """v2.1.11：统一的「清理无效」端点——把三个清单模块的清理动作收敛到一处。

    此前管理页有三个语义重叠的清理按钮（清理无效映射 / 清空已清理映射 /
    清理无效影视映射），各自的口径、是否联网、是否物理删除都不一致，用户点之前
    根本无法预判会发生什么。现在统一为一个端点，用 scope 区分范围：

      scope=file    清理失效的文件映射：只读核对 B 库，把已不存在的文件映射
                    重置为待同步（保留 A 库锚点，不删行）
      scope=dir     清理失效的目录映射：只读核对 B 库，把已不存在的目录映射
                    置为「待重建」（不删行，下轮同步自动重建）
      scope=pruned  彻底移除「已清理待重建」的僵尸目录行（物理删除，可按 ids 限定）
      scope=media   清理无效影视映射：只读核对 B 库，删除整部已无文件的映射行

    通用参数：
      dry_run=true  只返回预览清单，不写库（强烈建议先预览）
      rule_id       限定只处理某条镜像规则的行（0 = 当前规则）
      ids           限定只处理这些行 id（空 = 该 scope 下全部）

    三个老端点保留为薄封装转发到本端点，历史调用与断言不受影响。
    """
    scope = (p.scope or "file").strip().lower()
    # scope=all：文件段 + 目录段一起清（老端点 /mirror/mappings/prune 的原语义）
    if scope not in ("all", "file", "dir", "pruned", "media"):
        raise HTTPException(400, "清理范围只能是 all / file / dir / pruned / media")
    r = _mirror_rule(db)
    if not r:
        raise HTTPException(400, "请先在镜像设置里保存 A / B 库目录 ID")
    if p.rule_id and int(p.rule_id) != int(r.id):
        raise HTTPException(400, "指定的镜像规则与当前生效规则不一致")
    ids = [int(x) for x in (p.ids or []) if str(x).strip().isdigit()]

    if scope == "pruned":
        # 纯 DB 操作：物理移除带 [pruned] 标记的僵尸目录行
        # （与老端点 /mirror/mappings/clear-pruned 共用同一份同步实现）
        return _cleanup_pruned_dirs(db, r.id, ids, dry_run=bool(p.dry_run))

    if scope == "media":
        out = await mirror_media_clear_invalid_async(dry_run=p.dry_run, db=db)
        out["scope"] = scope
        return out

    # file / dir：联网只读核对 B 库后重置映射
    if not (r.target_root_id or "").strip():
        raise HTTPException(400, "请先在镜像设置里填写 B 镜像库目录 ID")
    with _mirror_guard():
        stats = await mirror.prune_stale_mappings(db, r, dry_run=p.dry_run, scope=scope)
        if not p.dry_run:
            add_audit(db, "mirror_cleanup", target_type="mirror",
                      detail=json_dumps({"scope": scope,
                                         "files_pruned": stats["files_pruned"],
                                         "dirs_pruned": stats["dirs_pruned"]}))
            db.commit()
    return stats


# ---------------------------------------------------------------------------
# v2.1.11 需求1：B 库被用户手动清空后的「重整」编排端点
# ---------------------------------------------------------------------------

class MirrorRestructureInput(BaseModel):
    """B 库清空重整的入参。

    scope:
      library  整个 B 库被清空（默认）
      dir      只有个别子目录被清空（按 A 库目录 ID 圈定）
    source_dir_ids: scope=dir 时要重整的 A 库目录 ID 列表
                     （= MirrorDir.source_dir_id，对应用户视角里的「子目录」）
    dry_run:  只出预览清单，不写库
    """
    scope: str = "library"
    source_dir_ids: list[str] = []
    dry_run: bool = False


def _restructure_scope_rows(db: Session, r: MirrorRule, scope: str,
                            src_ids: list[str]):
    """算出本次重整涉及的各层行。

    关联口径（**必须精确，不能靠作品名猜**）：
      资源库行 ↔ 文件映射行，用 **B 库文件 ID**：`Resource.file_id`
      等于 `MirrorFile.target_file_id`。这是唯一严格对得上的键。
      ——不能用作品名：`MirrorMedia.work_key` 是「A 库目录名」归一化，
      `work_group_key(resource)` 是「B 库资源名」归一化，两套口径，
      名字对不上时就漏掉整部作品（实测踩过）。

      分享中心行 ↔ 资源库行，用 `work_group_key(resource)`——
      分享任务本来就是从资源行建出来的，这套键在分享中心内部自洽。
    """
    medias = list(db.scalars(select(MirrorMedia).where(
        MirrorMedia.rule_id == r.id)).all())
    if scope == "dir" and src_ids:
        hit = set(src_ids)
        medias = [m for m in medias if str(m.source_dir_id or "") in hit]
    media_ids = {m.id for m in medias}
    files = list(db.scalars(select(MirrorFile).where(
        MirrorFile.media_id.in_(list(media_ids)))).all()) if media_ids else []
    dirs = list(db.scalars(select(MirrorDir).where(
        MirrorDir.rule_id == r.id)).all())
    if scope == "dir" and src_ids:
        hit = set(src_ids)
        dirs = [d for d in dirs if str(d.source_dir_id or "") in hit]

    # 受影响的 B 库文件 ID（精确键）
    b_file_ids = {str(f.target_file_id) for f in files if str(f.target_file_id or "")}
    res_rows = list(db.scalars(select(Resource)).all())
    if scope == "library":
        res_hit = list(res_rows)
    else:
        res_hit = [x for x in res_rows if str(x.file_id or "") in b_file_ids]
    res_ids = [x.id for x in res_hit]
    q_rows = list(db.scalars(select(QueueItem).where(
        QueueItem.resource_id.in_(res_ids))).all()) if res_ids else []
    # 分享中心按资源行的作品键圈定（与分享中心内部口径一致）
    work_keys = {work_group_key(x) for x in res_hit}
    return {"medias": medias, "files": files, "dirs": dirs,
            "res_hit": res_hit, "q_rows": q_rows, "work_keys": work_keys}


def _restructure_preview(db: Session, r: MirrorRule, scope: str,
                         src_ids: list[str]) -> dict:
    """统计本次重整会动到的各层行数（不写库）。"""
    from . import share_center as _sc

    g = _restructure_scope_rows(db, r, scope, src_ids)
    tasks = list(db.scalars(select(MirrorTask).where(
        MirrorTask.rule_id == r.id)).all())
    work_keys = g["work_keys"]
    st_hit = []
    for st in db.scalars(select(_sc.ShareTask)).all():
        if scope == "library" or str(st.work_key or "") in work_keys:
            if st.status not in (_sc.ST_PUBLISHED, _sc.ST_CANCELLED, _sc.ST_FAILED):
                st_hit.append(st)
    sr_hit = []
    for sr in db.scalars(select(_sc.ShareRecord)).all():
        if scope == "library" or str(sr.work_key or "") in work_keys:
            if int(sr.current_flag or 0) == 1:
                sr_hit.append(sr)
    # v2.1.17：v2 分享中心里会被一并作废的在途任务与游标。
    # 以前这里只统计旧版 ShareTask / ShareRecord，用户预览时完全看不到
    # v2 侧还有几百条任务会被留在原地继续重试——那正是「清空 B 库之后
    # 仍然一直在建链接失败」的存量来源。
    from . import share_center_v2 as _scv2
    v2_stmt = select(_scv2.SCPublishTask).where(
        _scv2.SCPublishTask.status.notin_(
            ("published", "cancelled", _scv2.INVALID_TASK_STATUS)))
    if scope != "library":
        v2_stmt = v2_stmt.where(_scv2.SCPublishTask.work_key.in_(list(work_keys) or [""]))
    v2_tasks = len(list(db.scalars(v2_stmt).all()))
    v2_cursors = sum(1 for c in db.scalars(select(_scv2.SCCategoryCursor)).all()
                     if int(c.last_library_version_id or 0) != 0)
    return {
        "media": len(g["medias"]), "files": len(g["files"]), "dirs": len(g["dirs"]),
        "tasks": len(tasks), "resources": len(g["res_hit"]),
        "queue": len(g["q_rows"]), "share_tasks": len(st_hit),
        "share_records": len(sr_hit),
        "sc_v2_tasks": v2_tasks, "sc_v2_cursors": v2_cursors,
        "media_titles": [m.title_standard for m in g["medias"][:10]],
    }


@app.post("/api/mirror/restructure", dependencies=ADMIN)
def mirror_restructure(p: MirrorRestructureInput, db: Session = Depends(get_db)):
    """v2.1.11 需求1：B 库被用户手动清空后的端到端重整。

    用户场景（原文）：「用户手动清理了存量文件，准备重新整理」——
    网盘上的 B 库文件被用户手工删掉/搬走，但系统里的镜像映射、资源库、
    推送队列、分享任务还停留在「已同步 / 已推送 / 已分享」的旧状态，
    于是页面显示一堆实际不存在的资源，推送时又报「文件不存在」。

    本端点把这条链路的**四层状态一次性归位**，让下一次定时镜像同步任务
    从干净的起点重新建目录、补复制、重新入库。

    ★ 硬红线：清理动作**不联动镜像同步**。
      绝不调用 sync_rule、绝不启动任何镜像任务、绝不碰 A 库。
      重建完全交给下一次定时镜像同步任务自然开始（用户原话：
      「重新镜像同步任务执行不联动，仍然等镜像同步任务开始」）。

    四层处理（每层都只归档/归零，不物理删除——历史行与执行记录必须保留）：

      ① 镜像映射层：MirrorFile 重置为待同步（**保留 A 库锚点**
         file_name / source_file_id / fingerprint）；MirrorMedia 状态归零为
         待同步；MirrorDir 置为「待重建」并打上 [pruned] 标记。
         ——下轮同步据此从 A 库原样重建 B 库目录与文件。

      ② 镜像任务层：本规则下的复制任务全部终态化（未终结的置 cancelled），
         避免旧任务被轮询领走后指向已不存在的 B 库文件。

      ③ 资源库层：命中的 Resource 行**归档**（clean_state='cleaned'），
         推送队列行同样归档并终态化，分享态归零。
         ——归档行不出现在资源库面板、不再被识别、不再被推送，但历史可查。

      ④ 分享中心层：未终结的分享任务置为已取消，当前有效分享记录作废
         （保留历史行）。——避免下一轮推送复用一个已失效的分享链接。

      ④b v2 分享中心层（**v2.1.17 补上，勿删**）：未终结的推送任务置为
         「无效推送」、已失效的分享链接标记作废、分类游标归零。
         ——此前这一段只覆盖旧版分享中心，而用户在用的是 v2 分享中心，
         于是清空 B 库后 v2 侧仍留着一批任务反复去建链接、反复因
         「文件不存在」失败，把每轮调度名额占满。

    ⑤ 日志层：镜像规则的 last_status / last_message / last_pipeline_json
         归零，进度条与状态标签回到「尚未同步」，不再显示上一轮的陈旧结果。

    参数：
      scope=library      整个 B 库被清空（默认）
      scope=dir          只有个别子目录被清空
      source_dir_ids     scope=dir 时的 A 库目录 ID 列表
      dry_run=true       只出预览清单，不写库（强烈建议先预览）
    """
    from .archive import archive_queue_item, archive_resource
    from . import share_center as _sc

    scope = (p.scope or "library").strip().lower()
    if scope not in ("library", "dir"):
        raise HTTPException(400, "重整范围只能是 library（整个 B 库）或 dir（个别子目录）")
    src_ids = [str(x).strip() for x in (p.source_dir_ids or []) if str(x).strip()]
    if scope == "dir" and not src_ids:
        raise HTTPException(400, "重整个别子目录时必须指定要重整的目录")
    r = _mirror_rule(db)
    if not r:
        raise HTTPException(400, "请先在镜像设置里保存 A / B 库目录 ID")

    # 正在跑的镜像批次不打断——用户点错了会毁掉正在进行的同步。
    if str(r.batch_status or "") in ("running", "paused"):
        raise HTTPException(409, "镜像批次正在运行或已暂停，请先等待本轮结束再重整")

    preview = _restructure_preview(db, r, scope, src_ids)
    if p.dry_run:
        return {"ok": True, "dry_run": True, "scope": scope, "preview": preview,
                "note": "仅预览，未改动任何数据"}

    # ---- ① 镜像映射层 ----
    g = _restructure_scope_rows(db, r, scope, src_ids)
    medias, files, dirs = g["medias"], g["files"], g["dirs"]
    for f in files:
        # 保留 A 库锚点（file_name / source_file_id / fingerprint）——
        # 下轮同步靠它判断「这个文件还没复制过」，清了就变成永远复制不回来。
        f.status = "pending"
        f.target_file_id = ""
        f.target_name = ""
        f.rename_status = ""
        f.rename_error = ""
        f.task_id = ""
        f.error = "B 库内容已被手动清理，映射已重置，等待下次镜像同步重建"
        f.updated_at = utcnow()
    for m in medias:
        m.status = "pending"
        m.last_synced_at = None
    for d in dirs:
        d.status = "missing"
        d.target_dir_id = ""
        d.error = mirror.PRUNED_DIR_MARK + "B 库内容已被手动清理，等待下次镜像同步重建"
        d.updated_at = utcnow()
    db.commit()

    # ---- ② 镜像任务层：未终结的复制任务终态化 ----
    tasks = list(db.scalars(select(MirrorTask).where(
        MirrorTask.rule_id == r.id)).all())
    task_done = 0
    for t in tasks:
        if t.task_status in ("pending", "submitting", "submitted", "verifying"):
            t.task_status = "cancelled"
            t.status = "cancelled"
            t.finished_at = utcnow()
            t.error = "B 库内容已被手动清理，任务已终止，等待下次镜像同步重建"
            task_done += 1
        elif str(t.status or "") in ("pending", "running"):
            t.status = "cancelled"
            t.finished_at = utcnow()
            task_done += 1
    db.commit()

    # ---- ③ 资源库层：资源行与推送队列行归档 ----
    res_hit = g["res_hit"]
    q_rows = g["q_rows"]
    work_keys = g["work_keys"]
    for x in res_hit:
        archive_resource(x, "B库内容已手动清理，归档资源库记录")
        x.share_status = "not_created"
        x.share_id = ""
        x.share_url = ""
    for q in q_rows:
        archive_queue_item(q, "B库内容已手动清理，归档推送队列记录")
        if q.status not in ("sent", "skipped", "failed"):
            q.status = "cancelled"
    db.commit()

    # ---- ④ 分享中心层：取消未终结的分享任务 + 作废当前分享记录 ----
    st_cancelled = 0
    sr_retired = 0
    try:
        st_rows = list(db.scalars(select(_sc.ShareTask)).all())
        for st in st_rows:
            if scope != "library" and str(st.work_key or "") not in work_keys:
                continue
            if st.status in (_sc.ST_PUBLISHED, _sc.ST_CANCELLED, _sc.ST_FAILED):
                continue
            st.status = _sc.ST_CANCELLED
            st.error = "B 库内容已被手动清理，分享任务已取消，等待重新入库后再分享"
            st_cancelled += 1
        sr_rows = list(db.scalars(select(_sc.ShareRecord)).all())
        for sr in sr_rows:
            if scope != "library" and str(sr.work_key or "") not in work_keys:
                continue
            if int(sr.current_flag or 0) != 1:
                continue
            _sc.retire_share_record(db, str(sr.work_key or ""),
                                    reason="B 库内容已手动清理，分享链接作废")
            sr_retired += 1
        db.commit()
    except Exception as exc:
        logger.exception("[mirror] 重整分享中心层失败：%s", exc)

    # ---- ④b v2 分享中心层：在途推送作废 + 链接置废 + 游标归零 ----
    # v2.1.17：上一段只覆盖**旧版**分享中心（ShareTask / ShareRecord）。用户实际在用的
    # 是 v2 分享中心，它自己的四张表（快照 / 清单项 / 任务 / 游标）完全没被联动——
    # 于是 B 库清空后 v2 这边仍留着几百条 planned/building 任务，dispatch 继续逐条
    # 去建分享链接、逐条因「文件不存在」失败，把每轮名额占满、拖慢真正能推的任务。
    # 这里补上联动：只改状态不删行，已 published 的保持历史（仅把失效链接标出来）。
    sc_v2 = {"tasks_invalidated": 0, "links_cleaned": 0, "cursors_reset": 0}
    try:
        from . import share_center_v2 as _scv2
        sc_v2 = _scv2.invalidate_for_works(
            db,
            None if scope == "library" else set(work_keys),
            reason="B 库内容已被手动清理，本次推送作废；镜像重建后重新入库再推送",
        )
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.exception("[mirror] 重整 v2 分享中心层失败：%s", exc)

    # ---- ⑤ 日志层：镜像规则的状态与进度归零 ----
    r.last_status = "never"
    r.last_message = ""
    r.last_pipeline_json = ""
    r.control_action = ""
    r.batch_status = "idle"
    r.batch_id = ""
    r.cursor_sequence = 0
    db.commit()

    add_audit(db, "mirror_restructured", target_type="mirror",
              detail=json_dumps({"scope": scope, "source_dir_ids": src_ids,
                                 "media": len(medias), "files": len(files),
                                 "dirs": len(dirs), "tasks": task_done,
                                 "resources": len(res_hit), "queue": len(q_rows),
                                 "share_tasks": st_cancelled,
                                 "share_records": sr_retired,
                                 # v2.1.17：v2 分享中心的联动作废计数
                                 "sc_v2": sc_v2}))
    db.commit()
    return {"ok": True, "dry_run": False, "scope": scope,
            "media": len(medias), "files": len(files), "dirs": len(dirs),
            "tasks": task_done, "resources": len(res_hit), "queue": len(q_rows),
            "share_tasks": st_cancelled, "share_records": sr_retired,
            # v2.1.17：v2 分享中心被一并作废的在途任务 / 失效链接 / 归零游标
            "sc_v2_tasks": sc_v2["tasks_invalidated"],
            "sc_v2_links": sc_v2["links_cleaned"],
            "sc_v2_cursors": sc_v2["cursors_reset"],
            "note": "已重置镜像映射、资源库与分享中心状态；"
                    "重建将由下一次定时镜像同步任务自然开始（本次清理不触发同步）"}


@app.post("/api/mirror/mappings/prune", dependencies=ADMIN)
async def mirror_mappings_prune(dry_run: bool = False, db: Session = Depends(get_db)):
    """v0.8.9：清理 B 库中已不存在的文件 / 目录映射（管理页「清理无效映射」）。

    只读核对 B 库实际内容，列目录不可信或整部作品为空时一律跳过，绝不误清。
    默认真正执行；`dry_run=true` 只返回清单不写库，供用户先预览。

    v2.1.11：薄封装转发到统一端点 /api/mirror/cleanup（scope=all），
    行为与旧端点完全一致（文件段 + 目录段一起清），保留端点是为了
    不破坏既有调用方与历史断言——**端点不删，只是实现换了地方**。
    """
    return await mirror_cleanup(MirrorCleanupInput(scope="all", dry_run=dry_run),
                                db=db)


def _cleanup_pruned_dirs(db: Session, rule_id: int, ids: list[int],
                         dry_run: bool) -> dict:
    """scope=pruned 的实现：彻底移除「已清理待重建」的僵尸目录行。

    抽成**同步**函数的原因：`/api/mirror/mappings/clear-pruned` 这个老端点
    一直是同步的（历史上有测试与内部调用方直接同步调用它），把它改成
    转发协程端点会导致「coroutine was never awaited」——函数返回一个
    协程对象却没人 await，实际什么都没删。

    所以：纯 DB 逻辑放在这个同步实现里，
    统一端点 /api/mirror/cleanup 与老端点都调它，两边行为完全一致。
    """
    stmt = select(MirrorDir).where(MirrorDir.status == "missing",
                                   MirrorDir.rule_id == rule_id)
    rows = list(db.scalars(stmt).all())
    targets = [d for d in rows if mirror.is_pruned_dir_error(getattr(d, "error", ""))]
    if ids:
        targets = [d for d in targets if d.id in ids]
    removed = len(targets)
    if not dry_run:
        for d in targets:
            db.delete(d)
        if targets:
            add_audit(db, "mirror_mappings_clear_pruned", target_type="mirror",
                      detail=json_dumps({"removed": removed}))
            db.commit()
    return {"ok": True, "scope": "pruned", "dry_run": bool(dry_run),
            "removed": removed,
            "removed_media": 0, "removed_files": 0, "files_pruned": 0,
            "dirs_pruned": 0, "media_checked": 0, "b_checked": 0,
            "b_deleted": 0, "skipped_has_files": 0, "skipped_unverified": 0,
            "samples": [d.name_standard for d in targets[:20]],
            "errors": []}


@app.post("/api/mirror/mappings/clear-pruned", dependencies=ADMIN)
def mirror_mappings_clear_pruned(db: Session = Depends(get_db)):
    """v0.9.6：彻底删除「已清理待重建」的僵尸目录映射行。

    这些行是之前「清理无效映射」(prune) 把 B 库已不存在的目录置 missing 留下的，
    error 带 [pruned] 标记。它们在「待创建目录」里已被排除，本端点供用户在确认后
    物理删除，让「目录映射清单」恢复干净。安全前提：A 库是权威源，若目录仍在，
    下轮「立即同步」会经 _record_dir 按 source_dir_id 原样重建；不会删资源 / 分享 / 队列。
    只删带 [pruned] 标记的 missing 行，不碰真正建目录失败的 missing 与任何正常映射。

    v2.1.11：与统一端点 /api/mirror/cleanup（scope=pruned）共用同一份实现，
    并补上此前缺失的 rule_id 过滤——此前查全表 missing 行，多规则场景会跨删。
    """
    r = _mirror_rule(db)
    if not r:
        raise HTTPException(400, "请先在镜像设置里保存 A / B 库目录 ID")
    return _cleanup_pruned_dirs(db, r.id, [], dry_run=False)


@app.post("/api/mirror/media/clear-invalid", dependencies=ADMIN)
async def mirror_media_clear_invalid_async(dry_run: bool = False,
                                           db: Session = Depends(get_db)):
    """v0.9.10：清理影视映射清单的无效行——口径改为「B 库网盘文件已删除」。

    用户 2026-11-19 明确：截图里全是 synced+有文件的历史行，按旧的「无 synced 文件」
    判定一条都清不掉，看起来「按钮没动作」。改为两步：
      ① 联网只读核对 B 库（复用 mirror.prune_missing_media_rows），整部已无文件的行删除；
      ② 再清本地无任何 synced 文件且非进行中的僵尸行（旧口径，覆盖 failed/blocked/unknown）。
    只删 A→B 映射登记（连同子 MirrorFile），不动资源 / 分享 / 队列 / B 库文件。
    列目录不可信时一律跳过该类，绝不误删；每步立即 commit，不跨 await 持写事务。
    dry_run=true 只返回预览清单不写库。
    """
    r = _mirror_rule(db)
    checked = {"checked": 0, "b_deleted": 0, "b_files": 0, "skipped_has_files": 0,
               "skipped_unverified": 0, "samples": [], "errors": []}
    if r and (r.target_root_id or "").strip():
        try:
            p = await mirror.prune_missing_media_rows(db, r, dry_run=dry_run)
            checked = {"checked": p.get("media_checked", 0),
                       "b_deleted": p.get("media_deleted", 0),
                       "b_files": p.get("files_deleted", 0),
                       "skipped_has_files": p.get("skipped_verified_has_files", 0),
                       "skipped_unverified": p.get("skipped_unverified", 0),
                       "samples": p.get("samples", []), "errors": p.get("errors", [])}
        except Exception as exc:
            checked["errors"].append(f"B 库核对失败：{str(exc)[:200]}")
            checked["skipped_unverified"] = -1  # 标记核对整体失败，不让本地清理冒充成功
    # ② 本地口径僵尸行（无 synced 文件）只在真正执行时删；dry_run 仅统计
    # v2.1.11：补 rule_id 过滤（此前查全表，多规则场景会跨删别的规则的映射行）
    _rule = _mirror_rule(db)
    targets = _invalid_media_rows(db, _rule.id if _rule else None)
    local_media = len(targets)
    local_files = sum(db.query(MirrorFile).filter(MirrorFile.media_id == m.id).count()
                      for m in targets)
    removed_media = checked["b_deleted"] + (0 if dry_run else local_media)
    removed_files = (checked["b_files"] if not dry_run else 0) + (0 if dry_run else local_files)
    if not dry_run and local_media:
        for m in targets:
            db.query(MirrorFile).filter(MirrorFile.media_id == m.id).delete(
                synchronize_session=False)
            db.delete(m)
        db.commit()
    if not dry_run and (removed_media or removed_files):
        add_audit(db, "mirror_media_clear_invalid", target_type="mirror",
                  detail=json_dumps({"b_deleted": checked["b_deleted"],
                                     "local_media": local_media,
                                     "media": removed_media, "files": removed_files}))
        db.commit()
    return {"ok": True, "removed_media": removed_media,
            "removed_files": removed_files,
            "b_checked": checked["checked"],
            "b_deleted": checked["b_deleted"],
            "skipped_has_files": checked["skipped_has_files"],
            "skipped_unverified": checked["skipped_unverified"],
            "samples": checked["samples"], "errors": checked["errors"],
            "dry_run": bool(dry_run)}


def mirror_media_clear_invalid(db: Session) -> dict:
    """v0.9.8 同步内部实现（本地口径：无 synced 文件的僵尸行），供内部/测试复用。

    HTTP 端点 v0.9.10 起走 `mirror_media_clear_invalid_async`（联网核对 B 库）。
    这里保留同步版只为兼容既有调用（不联网、不持锁），不影响前端行为。
    """
    targets = _invalid_media_rows(db)
    removed_media = len(targets)
    removed_files = 0
    for m in targets:
        removed_files += db.query(MirrorFile).filter(MirrorFile.media_id == m.id).count()
        db.query(MirrorFile).filter(MirrorFile.media_id == m.id).delete(
            synchronize_session=False)
        db.delete(m)
    if removed_media:
        add_audit(db, "mirror_media_clear_invalid", target_type="mirror",
                  detail=json_dumps({"media": removed_media, "files": removed_files}))
    db.commit()
    return {"ok": True, "removed_media": removed_media, "removed_files": removed_files}


def _invalid_media_rows(db: Session, rule_id: int | None = None) -> list:
    """v0.9.9 #205：判定「历史无效影视映射」——没有任何 synced 文件、且不在进行中的僵尸行。

    v0.9.8 口径只认 failed/blocked，真机上大量「status=synced 但 B 库文件已删」或
    「unknown/空状态」的历史遗留行清不掉，用户反馈「按钮点了没反应」。这里放宽为：
      - 没有任何 MirrorFile.status == "synced"（无已落地的 B 库文件锚点）；
      - 且状态不是进行中（copying/syncing/pending），避免误删正在同步的作品；
      - 且状态不是可信完成态 synced-with-files（已被上一条排除）。
    这样 failed/blocked/unknown/synced(无文件)/空状态 都归为可清理僵尸行。
    删除只清 A→B 映射登记，不删资源 / 分享 / 队列；A 库仍有文件时下轮同步自动重建。

    v2.1.11：加 rule_id 过滤——此前查全表 MirrorMedia，多规则场景下
    「清理无效影视映射」会跨规则删除别的规则的映射行。当前部署是单规则，
    但这是防御性缺口，补上以免将来多规则时误删。
    """
    in_progress = {"copying", "syncing", "pending"}
    stmt = select(MirrorMedia).order_by(MirrorMedia.id.desc())
    if rule_id:
        stmt = stmt.where(MirrorMedia.rule_id == int(rule_id))
    out = []
    for m in db.scalars(stmt).all():
        status = str(getattr(m, "status", "") or "").strip().lower()
        if status in in_progress:
            continue
        synced = db.query(MirrorFile).filter(
            MirrorFile.media_id == m.id, MirrorFile.status == "synced").count()
        if synced == 0:
            out.append(m)
    return out


@app.get("/api/mirror/status", dependencies=ADMIN)
def mirror_status(db: Session = Depends(get_db)):
    r = _mirror_rule(db)
    invalid_media = _invalid_media_rows(db)
    media = list(db.scalars(select(MirrorMedia).order_by(MirrorMedia.id.desc()).limit(200)).all())
    tasks = list(db.scalars(select(MirrorTask).order_by(MirrorTask.id.desc()).limit(30)).all())
    dirs = list(db.scalars(select(MirrorDir).order_by(MirrorDir.id.desc()).limit(200)).all())
    # v0.9.6：missing 分两类。带 [pruned] 的是「已清理待重建」僵尸行（下轮同步从 A 库
    # 自动重建，不需要用户手工建目录），从「待创建目录 / 待办清单」里排除，避免刷屏；
    # 真正建目录失败的 missing 才作为待办展示。用户可在映射清单里点「清空已清理映射」
    # 彻底删除僵尸行（需二次确认）。
    missing = [d for d in dirs if d.status == "missing"
               and not mirror.is_pruned_dir_error(getattr(d, "error", ""))]
    pruned_dirs = [d for d in dirs if d.status == "missing"
                   and mirror.is_pruned_dir_error(getattr(d, "error", ""))]
    # v0.8.4：文件复制失败的诊断不再只存在于「立即同步」那一次响应里。
    # MirrorFile.error / MirrorTask.error 落库持久化，刷新页面依然可查。
    failed_files = list(db.scalars(select(MirrorFile).where(
        MirrorFile.status == "failed").order_by(MirrorFile.id.desc()).limit(50)).all())
    titles = {m.id: m.title_standard for m in db.scalars(select(MirrorMedia)).all()}
    file_errors = [{"id": f.id, "work": titles.get(f.media_id, ""),
                    "file": f.file_name, "task_id": f.task_id or "",
                    "error": f.error or "", "status": f.status}
                   for f in failed_files]
    # v0.8.6：改名失败与「英文名待处理」都必须持久化可查，刷新页面仍在
    rename_failed = list(db.scalars(select(MirrorFile).where(
        MirrorFile.rename_status == "failed").order_by(MirrorFile.id.desc()).limit(50)).all())
    # v2.1.11：失败原因同时给「中性标签」——原始报错带 code=160 / code=127 这类
    # 后端口径，页面只展示中文标签（原始文本仍留在库里供排查）。
    rename_errors = [{"id": f.id, "work": titles.get(f.media_id, ""),
                      "file": f.file_name, "target": f.target_name or "",
                      "error_label": mirror.rename_error_label(f.rename_error),
                      "error": f.rename_error or ""} for f in rename_failed]
    # v0.9.6：排除用户已「清空 / 忽略」的待翻译项（english_source=ignored）。
    # 这些作品英文名为空但已被用户确认跳过，不再占用待处理清单；点「重新解析英文名并改名」
    # (force) 会清掉 ignored 标记重新走 TMDB→AI→拼音链路，可随时恢复。
    no_english = list(db.scalars(select(MirrorMedia).where(
        or_(MirrorMedia.english_title.is_(None), MirrorMedia.english_title == ""),
        or_(MirrorMedia.english_source.is_(None), MirrorMedia.english_source != "ignored")
    ).order_by(MirrorMedia.id.desc()).limit(50)).all())
    # v0.9.21：待翻译原因按真实状态区分，不再一律甩锅「补齐 TMDB/AI 配置」。
    # 真机排查发现配置其实正常、真正卡点是镜像锁被占；静态文案把排查方向带偏。
    _reason = untranslated_reason(_mirror_lock.info(),
                                  bool(tmdb.configured(db)), bool(ai_assist.enabled(db)))
    untranslated_works = [{"work": m.title_standard, "reason": _reason}
                          for m in no_english]
    # v0.9.0：待进行 / 进行中清单——把「缺失目录 + 待复制 / 复制中 / 待确认文件 + 改名待办 +
    # 未知任务」统一成一张待办表，管理页只保留「映射清单 + 待办清单」两个清单。
    pending_items: list[dict] = []
    for d in missing:
        pending_items.append({"kind": "目录待建", "work": d.name_standard,
                              "target": d.category_path or d.name_standard,
                              "status": d.status, "detail": getattr(d, "error", "") or d.parent_target_id})
    pending_files = list(db.scalars(select(MirrorFile).where(
        MirrorFile.status.in_(list(mirror.RETRYABLE_FILE_STATUS))
    ).order_by(MirrorFile.id).limit(200)).all())
    for f in pending_files:
        pending_items.append({"kind": "文件待复制", "work": titles.get(f.media_id, ""),
                              "target": f.file_name, "status": f.status,
                              "detail": (f.error or "")[:300]})
    rename_todo = list(db.scalars(select(MirrorFile).where(
        or_(MirrorFile.rename_status.is_(None), MirrorFile.rename_status == "",
            MirrorFile.rename_status == "pending_visible"),
        MirrorFile.status == "synced"
    ).order_by(MirrorFile.id).limit(200)).all())
    for f in rename_todo:
        pending_items.append({"kind": "文件待改名", "work": titles.get(f.media_id, ""),
                              "target": f.file_name,
                              "status": _rename_status_label(f.rename_status),
                              "detail": (mirror.rename_error_label(f.rename_error)
                                         if f.rename_error
                                         else "等待英文名解析 / 网盘文件可见")[:300]})
    for t in tasks:
        if t.status in ("pending", "running", "unknown"):
            pending_items.append({"kind": "复制任务待确认", "work": titles.get(t.media_id, ""),
                                  "target": t.task_id, "status": t.status,
                                  "detail": (t.error or "")[:300]})

    # v0.9.8 #195：入库→识别→分享→推送 全流程节点
    # v0.9.10：用户要求「只显示当前任务处理的数量和进度，不要历史累计」——
    #   优先读 MirrorRule.last_pipeline_json（本轮任务快照）；无快照时全 0 + 空闲提示。
    #   全库累计计数移到 counts.* 保留给其它面板，不再冒充本轮进度。
    # v2.1.11：这里原本算的 total_files / synced_files（全库累计）已删除——
    #   它们只被 progress.synced/total 使用，而那正是「进度条 100% 时文字显示 3/89」
    #   的病根（进度条按本轮、文字按全库）。全库累计统一走 counts.synced / counts.files。
    # v0.9.10.1：入队诊断优先读本轮快照 enqueue_diag（结构化、不会被截断）；
    # 老库无该字段时回退解析 last_message 字符串，保持升级兼容。
    snap = {}
    try:
        snap = json.loads(str(getattr(r, "last_pipeline_json", "") or "{}") or "{}")
    except Exception:
        snap = {}
    has_snap = bool(snap) and str(snap.get("at") or "")
    copied = int(snap.get("copied", 0) or 0) if has_snap else 0
    renamed = int(snap.get("renamed", 0) or 0) if has_snap else 0
    identified = int(snap.get("identified", 0) or 0) if has_snap else 0
    queued = int(snap.get("queued", 0) or 0) if has_snap else 0
    # v2.1.11：拆掉 total_files 同名字段漂移。
    #   copy_tasks —— 本轮提交的复制任务数（进度条分子分母同口径用它）
    #   scan_files —— 本轮 B 库扫描到的文件数（入库口径，只做展示）
    # 老快照只有 total_files：按「复制任务数」解读（与 v2.1.11 之前的 rename/identify 帧一致），
    # 保证升级后历史快照的进度不退化成 0。
    copy_tasks = int(snap.get("copy_tasks", snap.get("total_files", 0)) or 0) if has_snap else 0
    scan_files = int(snap.get("scan_files", 0) or 0) if has_snap else 0
    # v2.1.11：部数口径进结构化字段（此前只在 last_message 文本里，前端拿不到）
    works_total = int(snap.get("works_total", 0) or 0) if has_snap else 0
    works_done = int(snap.get("works_done", 0) or 0) if has_snap else 0
    snap_files = copy_tasks
    mode_label = str(snap.get("mode") or "上轮任务") if has_snap else ""
    idle_detail = "尚无任务记录，点「立即同步」或扫描后显示本轮处理量"
    # 诊断文案：快照 enqueue_diag 优先，回退 last_message 字符串
    diag = dict(snap.get("enqueue_diag") or {}) if has_snap else {}
    label_map = {"skip_unidentified": "未识别", "skip_no_poster": "无海报闸门",
                 "skip_route_no_match": "目录路由未匹配", "skip_no_channel": "目标无启用频道",
                 "skip_dedup_or_disabled": "去重/停用", "skip_no_target": "未配置推送目标",
                 "skip_rule_dir_mismatch": "规则目录不符", "skip_rule_keyword_mismatch": "规则关键词不符",
                 "enqueued": "已入队"}
    enqueue_diag_text = "、".join(f"{label_map.get(k, k)} {v}" for k, v in diag.items() if v)
    if not enqueue_diag_text:
        _lm = str(getattr(r, "last_message", "") or "") if r else ""
        if "入队诊断：" in _lm:
            enqueue_diag_text = _lm.split("入队诊断：", 1)[-1].split(" ·", 1)[0].strip()
    # v0.9.10.2：作品数口径——入库识别/生成分享/推送入队显示「剧集 N 部·电影 M 部」，
    # 按 (media_type, tmdb_id) 去重，不再是整体集数/文件数。
    works = dict(snap.get("works") or {}) if has_snap else {}
    ws_ident = dict(works.get("identified") or {})
    ws_queued = dict(works.get("queued") or {})
    # 老库兼容：v0.9.10 及以前快照没有 works/stage 字段，保留旧的「条数 + copied/files 进度」
    # 语义，避免升级后历史快照显示空部数 / 进度突变。
    legacy = has_snap and not works and not str(snap.get("stage") or "")

    def _works_text(w: dict) -> str:
        """剧集/电影部数文案；作品键缺失（老快照）时回退显示 '-'。"""
        tv, movie = int(w.get("tv", 0) or 0), int(w.get("movie", 0) or 0)
        if not tv and not movie:
            return ""
        parts = []
        if tv:
            parts.append(f"剧集 {tv} 部")
        if movie:
            parts.append(f"电影 {movie} 部")
        return " · ".join(parts)

    # 队列实时状态：按作品键去重统计待分享 / 已推送部数（同一剧集多集只算一部）
    queue_rows = list(db.execute(
        select(QueueItem.status, Resource.id, Resource.media_type, Resource.tmdb_id).join(
            Resource, Resource.id == QueueItem.resource_id
        ).where(QueueItem.status.in_(("pending", "retry", "sent")))).all())
    q_pending_works, q_sent_works = set(), set()
    for q_status, q_rid, q_mt, q_tid in queue_rows:
        wk = _work_type_key(q_mt, q_tid) or ("res", str(q_rid))  # 无 TMDB 时用资源 id 兜底
        (q_sent_works if q_status == "sent" else q_pending_works).add(wk)
    queue_pending = len(q_pending_works)
    queue_sent = len(q_sent_works)
    share_total = len(q_pending_works | q_sent_works)
    stage = str(snap.get("stage") or "") if has_snap else ""
    running = bool(snap.get("running", False)) if has_snap else False

    # 阶段 active 判定：任务运行中，当前节点尚未推进完即为 active
    copy_active = (copied < snap_files) if legacy else (running and stage in ("", "copy"))
    rename_active = running and stage == "rename"
    identify_active = running and stage == "identify"
    # ③入库识别：新快照用去重部数；老快照沿用 identified 条数
    ident_value = int(ws_ident.get("total", 0) or 0) if not legacy else identified
    ident_total = ident_value
    ident_disp = (_works_text(ws_ident) if not legacy else "") or f"{identified} 部"
    queued_value = int(ws_queued.get("total", 0) or 0) if not legacy else queued
    queued_disp = (_works_text(ws_queued) if not legacy else "") or f"{queued} 项"
    # v2.1.11：部数进度——分子分母都取快照的 works_done / works_total（同源）。
    # 此前进度条用「五节点综合百分比」，右侧文字用「全库累计 synced/total」，
    # 两套口径并排 → 进度条 100% 时文字可能显示「3/89」。现在两侧共用同一组数字。
    works_progress_total = works_total
    works_progress_done = works_done
    works_progress_label = ""
    if works_progress_total:
        works_progress_label = f"本轮已整理 {works_progress_done}/{works_progress_total} 部"
    elif has_snap and stage == "manual_rename":
        # v2.1.12 #295：只改名的轮次（手动「按英文规范改名」）。
        # 这一轮根本没有复制/识别节点，套「本轮复制 0 项 · 识别 0 部」会读成
        # 「什么都没干」，而用户实际改了 N 项 —— 按改名口径如实表达。
        works_progress_label = f"本轮改名 {renamed} 项"
    elif has_snap and not legacy:
        # 无部数口径（手动扫描 / 不写 works_total 的路径）：退回条数口径，
        # 仍与 percent 同一来源，不出现「一条口径算 percent、另一条算文字」。
        works_progress_label = f"本轮复制 {copied} 项 · 识别 {ident_value} 部"
    elif has_snap:
        works_progress_label = f"本轮复制 {copied}/{copy_tasks} 项"

    pipeline = [
        {"key": "copy", "label": "① 复制到 B 库", "value": copied, "total": copy_tasks,
         "status": ("active" if copy_active else "done") if has_snap else "idle",
         "detail": (f"{mode_label}：本轮复制 {copied} 项" if has_snap else idle_detail)},
        {"key": "rename", "label": "② 英文改名", "value": renamed, "total": copy_tasks,
         "status": ("active" if rename_active else ("done" if (renamed or stage) else "idle")) if has_snap else "idle",
         "detail": (f"本轮改名 {renamed} 项（可选步骤）" if has_snap else idle_detail)},
        {"key": "identify", "label": "③ 入库识别", "value": ident_value, "total": ident_total,
         "status": ("active" if identify_active else ("done" if (ws_ident or legacy and identified) else "idle")) if has_snap else "idle",
         "detail": ((f"本轮识别 {ident_disp}" + ("" if legacy else "（按 TMDB 作品去重）"))
                    if has_snap else idle_detail)},
        # v0.9.10.2：④生成分享按队列作品去重显示部数；worker 每 30 秒消费，实时变化靠前端轮询
        {"key": "share", "label": "④ 生成分享", "value": queue_sent, "total": share_total,
         "status": "idle" if share_total == 0 else ("active" if queue_pending else "done"),
         "detail": (f"已分享/推送 {queue_sent} 部 · 待处理 {queue_pending} 部（队列每 30 秒消费）"
                    if share_total else "队列空闲，无待处理分享")},
        {"key": "push", "label": "⑤ 推送入队", "value": queued_value, "total": queued_value,
         "status": ("active" if running and stage in ("", "copy", "rename", "identify")
                    else ("done" if (queued_value or queued) else "idle")),
         "detail": ((f"本轮入队 {queued_disp}" + ("" if legacy else "（按 TMDB 作品去重）")
                     + (f" · 拦截：{enqueue_diag_text}" if enqueue_diag_text else ""))
                    if has_snap else (idle_detail
                                      + (f" · 上次拦截：{enqueue_diag_text}" if enqueue_diag_text else "")))},
    ]
    # v2.1.11：进度百分比唯一口径。
    #   首选：部数口径（works_done / works_total）——用户主单位是「部」
    #         （单轮上限按部、分享/推送按 TMDB 作品去重也按部）。
    #         分享/推送未完成时压到 99，绝不显示 100%。
    #   次选：五节点综合完成度（手动扫描等不写部数口径的路径仍走这里）。
    #   兜底：老快照沿用 copied / copy_tasks（v2.1.11 之前口径），升级后不退化。
    # 进度条与右侧文字共用 progress 里的同一组数字（前端不再自己拼）。
    if not has_snap:
        progress = 0.0
    elif legacy:
        progress = round(copied / snap_files * 100, 1) if snap_files else 0.0
    elif stage == "manual_rename":
        # v2.1.12 #295：只改名轮次。这一轮没有复制/识别节点，套五节点综合百分比
        # 会被稀释成 20%，读起来像「只干了两成」；按该轮自身成败算。
        # 分享/推送还有待处理时沿用既有口径压到 99，不谎报全流程完成。
        progress = (99.0 if queue_pending else 100.0) if renamed else 0.0
    elif works_progress_total:
        progress = round(min(works_progress_done / works_progress_total, 1.0) * 100, 1)
        if progress >= 100 and (running or queue_pending):
            progress = 99.0  # 未完成分享/推送时不显示 100%
    else:
        all_done = (not running and queue_pending == 0)
        if all_done:
            progress = 100.0
        else:
            steps = []
            steps.append(min(copied / copy_tasks, 1.0) if copy_tasks else 0.0)
            steps.append(1.0 if (renamed or stage in ("identify", "done")) else 0.0)
            steps.append(1.0 if ws_ident else 0.0)
            steps.append((queue_sent / share_total) if share_total else 0.0)
            steps.append(1.0 if (queued_value or queued) else 0.0)
            progress = round(sum(steps) / len(steps) * 100, 1)
            if progress >= 100:
                progress = 99.0  # 未完成分享/推送时不显示 100%
    lock_info = _mirror_lock.info()

    # v2.1.11：单一权威状态源。
    # 此前 last_status（最近一轮结果）/ last_pipeline_json.running（实时）/
    # batch_status（严格引擎批次）/ lock.busy（互斥锁）四套并行，前端各取各的，
    # 于是出现「红字显示失败」与「进度条显示已完成」并存。
    # 现在由后端统一算出一个 state：
    #   实时态（running / stage）以快照 + 闸门为准；
    #   结果态（success / partial / blocked / failed）取自 last_status；
    #   页面只显示 state.label，不再直出 never/success/partial/blocked/failed 英文码。
    _last_status = str(getattr(r, "last_status", "") or "never") if r else "never"
    _state_label = {
        "never": "尚未同步", "success": "同步成功", "partial": "部分完成",
        "blocked": "有阻断项", "failed": "同步失败",
    }.get(_last_status, "尚未同步")
    _stage_label = {
        "copy": "复制到 B 库", "rename": "英文改名", "identify": "入库识别",
        "done": "收尾", "failed": "失败",
    }.get(stage, "处理中")
    _is_running = bool(running) or bool(lock_info.get("busy"))
    _state_code = "running" if _is_running else (
        "failed" if stage == "failed" else (_last_status or "never"))
    _state_label = f"同步中 · {_stage_label}" if _is_running else _state_label
    if _is_running and stage == "failed":
        _state_label = "同步失败"
    state = {
        "code": _state_code,
        "label": _state_label,
        "running": _is_running,
        "stage": stage,
        "stage_label": _stage_label,
        "owner": str(lock_info.get("owner") or ""),
        # 「本轮结果」与「实时状态」分开：结果态只在非运行时展示，避免运行中显示上一轮结果
        "result": ("" if _is_running else _state_label),
    }

    return {
        "rule": {"enabled": bool(r.enabled) if r else False,
                 "last_run": r.last_run if r else None,
                 "last_status": r.last_status if r else "never",
                 "last_message": r.last_message if r else ""},
        "counts": {
            "media": db.query(MirrorMedia).count(),
            "files": db.query(MirrorFile).count(),
            "synced": db.query(MirrorFile).filter(MirrorFile.status == "synced").count(),
            "superseded": db.query(MirrorFile).filter(MirrorFile.status == "superseded").count(),
            # v0.8.4：把「目录建好但 B 库空」的卡点量化，一眼看出是否卡在复制阶段
            "files_failed": db.query(MirrorFile).filter(MirrorFile.status == "failed").count(),
            "files_copying": db.query(MirrorFile).filter(MirrorFile.status == "copying").count(),
            "files_unknown": db.query(MirrorFile).filter(MirrorFile.status == "unknown").count(),
            "files_retryable": db.query(MirrorFile).filter(
                MirrorFile.status.in_(list(mirror.RETRYABLE_FILE_STATUS))).count(),
            # v0.8.6：改名进度与卡点
            "files_renamed": db.query(MirrorFile).filter(
                MirrorFile.rename_status == "done").count(),
            "files_rename_failed": db.query(MirrorFile).filter(
                MirrorFile.rename_status == "failed").count(),
            # v0.9.6.1：历史条目不回刷的单集改名行，默认不参与任何自动改名
            "files_rename_skipped_legacy": db.query(MirrorFile).filter(
                MirrorFile.rename_status == "skipped_legacy").count(),
            # v2.1.4：口径与「英文改名待处理」清单保持一致——排除已被用户标记忽略
            # （english_source='ignored'）的作品。此前这里不排除，导致用户点完
            # 「清空待处理清单」后清单已空、红字「待翻译作品」却仍显示旧数字，
            # 让人误以为「清了但没生效 / 翻译再也不会发生」。
            "works_without_english": db.query(MirrorMedia).filter(
                or_(MirrorMedia.english_title.is_(None),
                    MirrorMedia.english_title == ""),
                or_(MirrorMedia.english_source.is_(None),
                    MirrorMedia.english_source != "ignored")).count(),
            # 被标记忽略的作品数（透明可见，避免「静默丢弃」错觉）
            "works_english_ignored": db.query(MirrorMedia).filter(
                or_(MirrorMedia.english_title.is_(None),
                    MirrorMedia.english_title == ""),
                MirrorMedia.english_source == "ignored").count(),
            "tasks_unknown": db.query(MirrorTask).filter(MirrorTask.status == "unknown").count(),
            # v2.1.11：失败计数合并两套任务状态字段。
            # 严格顺序引擎写 task_status（failed），旧批量路径写 status（failed），
            # 此前只读 status → 严格引擎的失败任务一条都不计入，页面显示「失败 0」，
            # 但问题诊断里却挂着红色失败清单。两者都读，口径才与清单一致。
            "tasks_failed": db.query(MirrorTask).filter(
                or_(MirrorTask.status == "failed",
                    MirrorTask.task_status == "failed")).count(),
            # v2.1.11：严格引擎终态任务计数（跳过/取消/完成），供清单与按钮状态用
            "tasks_skipped": db.query(MirrorTask).filter(
                MirrorTask.task_status == "skipped").count(),
            "tasks_cancelled": db.query(MirrorTask).filter(
                MirrorTask.task_status == "cancelled").count(),
            "scan_batches": db.query(ResourceScanBatch).count(),
            "change_events": db.query(DistributionEvent).count(),
            "dirs_mapped": db.query(MirrorDir).filter(MirrorDir.status != "missing").count(),
            "dirs_created": db.query(MirrorDir).filter(MirrorDir.status == "created").count(),
            "dirs_missing": len(missing),
            "dirs_failed": len(missing),
            "dirs_pruned_zombie": len(pruned_dirs),  # v0.9.6：已清理待重建的僵尸行数
            "dirs_fallback": db.query(MirrorDir).filter(
                MirrorDir.status == "created_fallback").count(),
            # v0.9.8：历史无效影视映射（failed/blocked 且无 synced 文件）
            "media_invalid": len(invalid_media),
            # v2.1.11：给前端的中文口径别名。
            # 「页面不得体现后端代码名词」是硬红线，前端此前直接写
            # `counts.media_invalid` 取值——字段名本身就成了页面上的代码痕迹。
            # 这里后端出中性别名，前端只认这个别名；老键 media_invalid 保留
            # 给历史调用方，不删。
            "invalid_mappings": len(invalid_media),
            "unmapped_dirs": len(missing),
        },
        "media": [{"id": m.id, "title": m.title_standard, "mirror_name": m.title_mirror,
                   "media_type": m.media_type, "status": m.status,
                   "status_label": _media_status_label(m.status),
                   "english_title": getattr(m, "english_title", "") or "",
                   "english_source": getattr(m, "english_source", "") or "",
                   "source_dir_id": m.source_dir_id, "target_dir_id": m.target_dir_id,
                   "category_path": getattr(m, "category_path", "") or "",
                   "invalid": any(x.id == m.id for x in invalid_media),  # v0.9.8
                   "files": db.query(MirrorFile).filter(MirrorFile.media_id == m.id).count()}
                  for m in media],
        # v0.9.8：历史无效影视映射清单，供管理页「清理无效影视映射」按钮展示/确认
        # v2.1.11：补 files 计数 + 中文状态标签，供前端渲染这张表
        "invalid_media": [{"id": m.id, "title": m.title_standard,
                           "status": m.status,
                           "status_label": _media_status_label(m.status),
                           "category_path": getattr(m, "category_path", "") or "",
                           "files": db.query(MirrorFile).filter(
                               MirrorFile.media_id == m.id).count()}
                          for m in invalid_media],
        "dirs": [{"id": d.id, "name": d.name_standard, "kind": d.kind,
                  "category_path": d.category_path, "status": d.status,
                  "status_label": _dir_status_label(d.status),
                  "parent_target_id": d.parent_target_id, "target_dir_id": d.target_dir_id,
                  "pruned": mirror.is_pruned_dir_error(getattr(d, "error", "")),
                  "error": getattr(d, "error", "") or ""}
                 for d in dirs],
        # v2.1.11：「已清理待重建」的僵尸目录行单独列出，供前端勾选重建。
        # 此前这些行只能被物理清空（clear-pruned），用户无法对其中某几行
        # 单独触发重建——A 库还在、下轮同步会自动重建，但用户等不了下一轮。
        "pruned_dirs": [{"id": d.id, "name": d.name_standard, "kind": d.kind,
                         "category_path": d.category_path,
                         "parent_target_id": d.parent_target_id,
                         "error": getattr(d, "error", "") or ""}
                        for d in pruned_dirs],
        # v0.8.2：tip 不再硬编码成「请手工建立」，直接回显 create_dir 的真实报错
        # v0.9.17.2：补 id 供「待创建目录」批量勾选定向重建
        "missing_dirs": [{"id": d.id, "parent_id": d.parent_target_id, "name": d.name_standard,
                          "kind": d.kind, "star": d.kind == "work",
                          "error": getattr(d, "error", "") or "",
                          "tip": (getattr(d, "error", "") or "")
                                 or "在 B 库该父目录下建立此目录后重新同步"} for d in missing],
        # v2.1.11：任务清单补 task_status 与中文标签——严格顺序引擎的失败/跳过
        # 此前只体现在 counts 里，任务表本身看不到，用户无从判断该不该重试。
        "tasks": [{"id": t.id, "task_id": t.task_id, "status": t.status,
                   "task_status": t.task_status,
                   "status_label": _task_status_label(t.status, t.task_status),
                   "raw_status": t.raw_status, "polls": t.polls, "error": t.error,
                   "work": titles.get(t.media_id, "")} for t in tasks],
        # v0.8.4：文件复制失败清单（持久化，刷新页面仍在）
        "file_errors": file_errors,
        # v0.8.6：改名失败与英文名待处理清单
        "rename_errors": rename_errors,
        "untranslated_works": untranslated_works,
        # v0.9.0：统一待进行 / 进行中清单（管理页两个清单之一）
        "pending_items": pending_items,
        "unverified_endpoints": mirror.UNVERIFIED_ENDPOINTS,
        "confirmed_endpoints": mirror.CONFIRMED_WRITE_ENDPOINTS,
        "min_idle_seconds": mirror.MIN_FILE_IDLE_SECONDS,
        # v0.9.6：镜像互斥锁状态（谁在跑、跑了多久），前端据此提示「后台任务运行中」，
        # 避免用户点「立即同步」撞上静默的后台改名复查后只看到一句 409。
        "lock": _mirror_lock.info(),
        # v2.1.24：定时同步的调度可见性。用户排查「间隔 15 分钟却半天不跑」时，
        # 原来页面上只有 last_run ——「定时没注册」和「刚跑过」长得一模一样。
        # 这里给「下次触发时间」与「是否真的注册了」，前端据此三态如实展示。
        "schedule": {"registered": scheduler.get_job(MIRROR_JOB_ID) is not None,
                     "next_run_at": _mirror_next_run_at(),
                     "interval_minutes": int(r.interval_minutes or 0) if r else 0,
                     "enabled": bool(r.enabled) if r else False},
        # v0.9.8 #195：全流程节点 + 同步进度，首页/镜像页据此展示流程条与进度
        "pipeline": pipeline,
        # v2.1.11：进度唯一口径 —— 分子分母与右侧文字同源（部数口径）。
        #   此前 percent 走五节点综合，而右侧文字取全库累计 synced/total，
        #   进度条 100% 时文字可能显示「3/89」。现在 percent / done / total /
        #   label 全部来自同一组快照数字，前端只渲染 label，不再自己拼。
        "progress": {"percent": progress,
                     "done": works_progress_done,
                     "total": works_progress_total,
                     "unit": "部",
                     "label": (works_progress_label
                               or (f"本轮已整理 {works_progress_done}/{works_progress_total} 部"
                                   if works_progress_total else "")),
                     "running": _is_running,
                     "stage": stage,
                     "stage_label": _stage_label,
                     "owner": lock_info.get("owner", "")},
        # v2.1.11：单一权威状态（实时态 + 结果态），页面只显示 state.label
        "state": state,
        # v0.8.2：回显实际生效配置，一眼看出「自动创建是不是被关掉了」
        "config": {
            "auto_create_dirs": (True if getattr(r, "auto_create_dirs", None) is None
                                  else bool(r.auto_create_dirs)) if r else True,
            "star_naming": bool(r.star_naming) if r else True,
            # v0.8.5：auto_identify 从「死配置」变为真正生效的开关，状态页必须回显
            "auto_identify": (True if getattr(r, "auto_identify", None) is None
                              else bool(r.auto_identify)) if r else True,
            # v0.8.6：改名开关生效值
            "rename_files": (True if getattr(r, "rename_files", None) is None
                             else bool(r.rename_files)) if r else True,
            "source_root_id": r.source_root_id if r else "",
            "target_root_id": r.target_root_id if r else "",
            "file_idle_seconds": float(r.file_idle_seconds or 1.0) if r else 1.0,
            # v0.9.0：单轮作品上限生效值 + 当前轮转游标
            "max_works_per_run": max(0, int(getattr(r, "max_works_per_run", 0) or 0)) if r else 0,
            "works_cursor": str(getattr(r, "works_cursor", "") or "") if r else "",
        },
        # 阶段二路线B C4：严格顺序批次状态（前端据此显示暂停/恢复/跳过/取消按钮）
        "batch": (mirror_batch.batch_status(db, r) if r else
                  {"batch_id": "", "status": "idle", "control_action": "", "cursor_sequence": 0}),
    }


# ---- 阶段二路线B C4：严格顺序批次控制端点 ----
@app.get("/api/mirror/batch/status", dependencies=ADMIN)
def mirror_batch_status(db: Session = Depends(get_db)):
    """当前镜像批次状态 + 任务进度（暂停/恢复/跳过/取消按钮据此渲染）。"""
    r = _mirror_rule(db)
    if not r:
        return {"batch_id": "", "status": "idle", "total": 0, "done": 0,
                "pending": 0, "percent": 0, "current_sequence": 0, "status_count": {}}
    info = mirror_batch.batch_status(db, r)
    if r.batch_id:
        info.update(mirror_batch.batch_progress(db, r.batch_id))
    else:
        info.update({"total": 0, "done": 0, "pending": 0, "percent": 0,
                     "current_sequence": 0, "status_count": {}})
    return info


@app.post("/api/mirror/batch/control", dependencies=ADMIN)
def mirror_batch_control(action: str, db: Session = Depends(get_db)):
    """派发批次控制指令：pause / resume / skip / cancel / reopen。

    指令在"任务边界"生效，不打断正在进行的 copy_file/轮询。幂等。

    v2.1.11：
      - 返回 `applied` + `reason`，前端据此给真实反馈（此前非 running 时
        后端静默无操作，前端却弹「指令已下发」——有按钮无效果）。
      - 新增 `reopen`：把本批次被 skip 掉的序列任务恢复为待处理。
        skip 此前是不可逆终态，用户点错一次这批文件就再也不被复制。
    """
    r = _mirror_rule(db)
    if not r:
        raise HTTPException(400, "尚无镜像规则")
    act = (action or "").strip().lower()
    if act == "reopen":
        out = mirror_batch.reopen_skipped(db, r)
        add_audit(db, "mirror_batch_reopen", target_type="mirror",
                  detail=json_dumps({"batch_id": r.batch_id,
                                     "reopened": out.get("reopened")}))
        db.commit()
        return {"ok": bool(out.get("ok")), **out}
    try:
        out = mirror_batch.request_control(db, r, act)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    add_audit(db, "mirror_batch_control", target_type="mirror",
              detail=json_dumps({"action": act, "batch_id": r.batch_id,
                                 "applied": out.get("applied")}))
    db.commit()
    return {"ok": True, **out}


@app.get("/api/scan/batches", dependencies=ADMIN)
def scan_batches(limit: int = 20, db: Session = Depends(get_db)):
    rows = list(db.scalars(select(ResourceScanBatch).order_by(
        ResourceScanBatch.id.desc()).limit(min(limit, 100))).all())
    return [{"id": b.id, "status": b.status, "files": b.files, "directories": b.directories,
             "seen_count": b.seen_count, "complete": b.complete, "resumed": b.resumed,
             "reconcile_allowed": b.reconcile_allowed, "error": b.error,
             "started_at": b.started_at, "finished_at": b.finished_at} for b in rows]


async def _post_scan_identify(db: Session, *, publish_after_identify: bool = True,
                              limit: int = 200) -> dict:
    """v0.7.1：扫描完成后自动 TMDB 识别 + 完结/追更状态回填。

    需求3：扫描入库后自动识别；识别成功拿到海报才允许分发（沿用 v0.6.5 闸门）。
    v0.8.5：用户已反转 V2.0 第十章的「镜像完成不自动推 TG」红线——镜像链路现在
    传 publish_after_identify=True，只推本轮识别成功的影视；群推防护由
    _enqueue_ready 的作品级去重 + 镜像管线不调用 process_change_events 共同保证。

    完结标签：已识别但 finish_status 为空的历史资源（v0.7.0 之前识别的）用已有
    tmdb_id 回查 TMDB detail 补 status，否则角标永远不显示。
    """
    stats = {"identified": 0, "failed": 0, "queued": 0, "finish_backfilled": 0,
             # v2.1.9：限流单独计数——限流是「待重试」不是「识别失败」，
             # 单独暴露出来才能和真失败区分开（真机上二者曾混在一起）。
             "rate_limited": 0,
             # v0.9.9 #201/#206：入队拦截诊断，逐条累积后供镜像管线与前端进度节点展示
             "enqueue_diag": {},
             # v0.9.10.2：本轮识别 / 入队的作品键集合（按 TMDB 去重），
             # 供进度看板显示「剧集 N 部 / 电影 M 部」而非集数。
             "identified_work_keys": set(), "queued_work_keys": set()}
    diag = stats["enqueue_diag"]
    # v2.1.12：批次选取改为「按作品分散 + 轮次推进」（与手动「按目录重识别」同一套）。
    # 旧实现是 `order_by(id.desc()).limit(500)` 的固定窗口——真机上最新 500 条恰好
    # 全属同一部剧，窗口每次都被它的集数占满，其它剧从来没被自动识别过
    # （用户报障：只有手动重试的那部剧被识别了）。现在一批里按作品分散取，
    # 作品内部新内容优先；每条记一个识别轮次号，跨轮自动推进到没轮到过的那批。
    pending, batch_info = select_identify_batch(
        db, limit=min(limit, 500), force=False, prefer_new=True)
    for idx, x in enumerate(pending):
        try:
            # v2.1.9：分帧排队——发请求前先看配额还差多少。
            # 差得多就**中止本轮**（剩下的资源下一轮再识别），而不是继续撞限流
            # 把整批资源全写成失败。真机上一次爆发式全量扫描正是这样
            # 把上千个资源一次性标成 failed 的。
            wait = tmdb.rate_limit_wait_seconds(db, tmdb.SCAN)
            if wait > IDENTIFY_RATE_LIMIT_WAIT_MAX:
                # 本轮剩余资源都还没发过请求，不算「限流失败」，
                # 只记「本轮因配额不足提前结束」。
                stats["rate_limited"] += len(pending) - idx
                stats["rate_limit_deferred"] = len(pending) - idx
                break
            if wait > 0:
                await asyncio.sleep(min(wait, IDENTIFY_RATE_LIMIT_WAIT_MAX))
            # v2.1.12：即将真正尝试识别 → 记一次参与轮次。
            # 记在这里（而不是循环末尾）是为了保证：**任何**发过请求的行都算参与过，
            # 成功/失败/限流一视同仁；而配额不足直接中止时、根本没发请求的行不记，
            # 下一轮照样排前面。轮次因此是「尝试次数」，天然公平轮转。
            mark_attempted(x)
            r = await identify_with_a_source_fallback(x.name, x, db)
            if r.get("ok") and r.get("best"):
                _apply_identification(x, r["best"], source=r.get("source") or "tmdb",
                                      note=r.get("reason") or "", hint=r.get("hint"))
                stats["identified"] += 1
                wkey = _work_type_key(x.media_type, x.tmdb_id)
                if wkey:
                    stats["identified_work_keys"].add(wkey)
                # autoflush=False：同批次识别后立刻查队列去重，必须 flush
                db.flush()
                if publish_after_identify:
                    n_added = _after_identify_dispatch(db, x, diag=diag)
                    stats["queued"] += n_added
                    if n_added and wkey:
                        stats["queued_work_keys"].add(wkey)
            else:
                x.identify_status = r.get("status") or "none"
                x.identify_source = r.get("source") or ""
                x.identify_note = r.get("reason") or ""
                stats["failed"] += 1
        except Exception as exc:
            # TMDB 未配置/401/限流不得阻断扫描链路（项目既定降级约定）
            # v2.1.9：**限流单独捕获**——标记为「待重试」而不是 failed。
            # 真机上 1656 个资源因限流被永久写死 failed、后续再无重试机会，
            # 根因就是这里把限流和真失败一视同仁。
            if tmdb.is_rate_limited(exc):
                x.identify_status = "rate_limited"
                x.identify_note = str(exc)[:300]
                stats["rate_limited"] += 1
                # 限流说明本轮额度已尽，继续跑只会把后面所有资源也写成限流。
                # 中止本轮，剩下的资源保持原状，下一轮（有额度时）自动重试。
                break
            x.identify_status = "failed"
            x.identify_note = str(exc)[:300]
            stats["failed"] += 1
    # 完结状态 / 最后一集播出日回填：只处理已识别的剧集（限量，避免重复请求）
    # v2.1.23：把 `last_air_date` 也纳入回填——存量行都没有这一列（本次新增），
    # 缺了它「新上映」标签就无法按「更新完结次日 0 点」过期，只能退回 30 天窗口。
    need_status = or_(Resource.finish_status.is_(None), Resource.finish_status == "")
    need_air = or_(Resource.last_air_date.is_(None), Resource.last_air_date == "")
    stale = list(db.scalars(select(Resource).where(
        Resource.media_type == "tv", Resource.tmdb_id.isnot(None),
        Resource.identify_status == "identified",
        or_(need_status, need_air),
    ).limit(60)).all())
    seen_tmdb: dict[int, tuple[str, str]] = {}
    for x in stale:
        tid = int(x.tmdb_id)
        if tid not in seen_tmdb:
            try:
                d = await tmdb.detail(tid, "tv", db)
                seen_tmdb[tid] = (str(d.get("finish_status") or ""),
                                  str(d.get("last_air_date") or ""))
            except Exception:
                seen_tmdb[tid] = ("", "")
        status_val, air_val = seen_tmdb[tid]
        # v0.8.0：手动修正过（含手动清空）的完结状态不再被 TMDB 自动回填覆盖；
        # 但「最后一集播出日」是客观事实，不覆盖用户任何判断，照补。
        if status_val and not bool(getattr(x, "finish_locked", False)):
            x.finish_status = status_val
            stats["finish_backfilled"] += 1
        if air_val and not str(x.last_air_date or ""):
            x.last_air_date = air_val
            stats["last_air_backfilled"] = int(stats.get("last_air_backfilled") or 0) + 1
    # v0.9.10.2：作品键集合转成 JSON 安全的部数统计（进度看板按 TMDB 去重显示部数）
    stats["work_stats"] = {
        "identified": _work_breakdown(stats.pop("identified_work_keys")),
        "queued": _work_breakdown(stats.pop("queued_work_keys")),
    }
    return stats


async def _reconcile_library(db: Session, seen_file_ids: list[str],
                             cancel_shares: bool = True, scan_complete: bool = True,
                             resumed: bool = False, batch_id: int | None = None,
                             allow_empty: bool = False) -> dict:
    """v0.7.1：B 库对账——本轮没扫到的影视移除看板并取消分享（需求3-1）。

    V2.0 第 12.2 节扫描批次保护：必须同时满足
      ① 全量扫描（非断点续扫）、② 全部分页读取成功、③ seen 非空，
    否则一律跳过对账。宁可保留旧数据，也绝不能因接口抖动把整个资源库清空。

    v0.8.7：用户明确「刷新 = 读取最新 B 库内容，B 库已无该影视则清理出面板」。
    用户主动触发的刷新可传 `allow_empty=True`，允许**可信空批次**清库，但仍要
    满足硬门槛：批次 success + complete + 非断点 + **确实扫到了目录**（directories>0）
    + seen_count=0。B 库根目录都读不到（directories=0）时不信任，仍跳过。
    定时扫描与其它自动链路不传此参数，行为完全不变。
    """
    skipped = ""
    empty_ok = False
    # v0.8.0：批次表是「是否允许对账删除」的唯一权威信号。传入 batch_id 时，
    # 即使调用方声称 complete=True，只要批次未标记 reconcile_allowed 就一律跳过。
    if batch_id:
        b = db.get(ResourceScanBatch, batch_id)
        if not b:
            return {"removed": 0, "shares_cancelled": 0, "queue_cleared": 0,
                    "skipped": "batch_missing"}
        if b.status != "success" or not b.complete or b.resumed:
            return {"removed": 0, "shares_cancelled": 0, "queue_cleared": 0,
                    "skipped": "batch_not_allowed"}
        if not b.reconcile_allowed:
            if not allow_empty:
                return {"removed": 0, "shares_cancelled": 0, "queue_cleared": 0,
                        "skipped": "batch_not_allowed"}
            if not b.directories or b.seen_count:
                return {"removed": 0, "shares_cancelled": 0, "queue_cleared": 0,
                        "skipped": "empty_scan_untrusted"}
            empty_ok = True
        try:
            saved = json.loads(b.seen_json or "[]")
            if saved:
                seen_file_ids = saved
        except Exception:
            pass
    if resumed:
        skipped = "resumed_incomplete_seen"
    elif not scan_complete:
        skipped = "incomplete_pages"
    seen = set(str(f) for f in seen_file_ids)
    if not skipped and not seen and not empty_ok:
        skipped = "empty_seen"
    if skipped:
        return {"removed": 0, "shares_cancelled": 0, "queue_cleared": 0, "skipped": skipped}
    if empty_ok and not seen:
        logger.info("[library] 用户主动刷新：B 库可信空批次，按实际内容清空资源库面板")
    stale = [x for x in db.scalars(select(Resource).where(
        or_(Resource.clean_state.is_(None), Resource.clean_state != "cleaned"))).all()
        if x.file_id not in seen]
    if not stale:
        return {"removed": 0, "shares_cancelled": 0, "queue_cleared": 0}
    share_ids = {str(x.share_id) for x in stale if x.share_id}
    ids = [x.id for x in stale]
    cancelled = 0
    if cancel_shares and share_ids:
        for sid in share_ids:
            try:
                await GuangYaShareClient(db).delete(sid)
                cancelled += 1
            except Exception:
                pass  # 单个分享取消失败不阻断对账
    # v2.1.11：推送队列行**归档不物理删除**（红线：历史执行记录必须保留）。
    # 此前是 .delete()，整批行直接消失，事后无从追溯「当时推过什么」。
    q_rows = list(db.scalars(select(QueueItem).where(
        QueueItem.resource_id.in_(ids))).all())
    now = utcnow()
    for q in q_rows:
        q.clean_state = "cleaned"
        q.cleaned_at = now
        q.clean_reason = "B库内容已移除，归档推送队列记录"
        # 终态化，避免归档行仍被 worker 领走重发（文件已不存在）
        if q.status not in ("sent", "skipped", "failed"):
            q.status = "cancelled"
    q = len(q_rows)
    # 四模块重构 阶段C 规则2：B 库整部删除 → 分享中心取消原分享任务（出库解耦）。
    # 按 stale 资源作品键取消未终结的 ShareTask；A 库仍在时镜像下一轮补齐 B 库，
    # 新资源重新入库后走 _enqueue_ready 建新的 first 任务→重新分享推送（闭环）。
    try:
        from . import events, share_center
        stale_keys = {work_group_key(x) for x in stale}
        if stale_keys:
            active = [share_center.ST_PLANNED, share_center.ST_READY,
                      share_center.ST_SUBMITTED, share_center.ST_ACCEPTED,
                      share_center.ST_PUBLISHING]
            stale_tasks = db.scalars(select(share_center.ShareTask).where(
                share_center.ShareTask.work_key.in_(list(stale_keys)),
                share_center.ShareTask.status.in_(active))).all()
            for st in stale_tasks:
                st.status = share_center.ST_CANCELLED
            # 作废当前有效分享记录（保留历史），真相源与任务状态一致
            for wk in stale_keys:
                try:
                    share_center.retire_share_record(db, wk, reason="B库整部删除出库")
                except Exception:
                    pass
                # 产出资源出库事件（规则2：分享中心消费后确保取消分享；镜像补齐再重建）
                events.emit_resource_event(
                    db, event_type=events.RESOURCE_REMOVED, work_key=wk,
                    payload={"reason": "b_library_removed", "share_ids": sorted(share_ids)})
    except Exception:
        pass
    # v2.1.11：资源库行**归档不物理删除**（红线：历史行与执行记录必须保留）。
    # 此前是 db.delete(x) —— 一次 B 库重建空窗期的事故就把 4387 行彻底抹掉，
    # 事后完全无从追溯。归档后行还在（历史可查），只是不再出现在资源库面板、
    # 不再参与识别/分享/推送；A 库补齐后新一轮扫描会重新入库新的资源行。
    for x in stale:
        x.clean_state = "cleaned"
        x.cleaned_at = now
        x.clean_reason = "B库内容已移除，归档资源库记录"
        # 分享态一并归零，避免归档行还被当作「已分享」复用旧链接
        x.share_status = "not_created"
    add_audit(db, "library_reconciled", target_type="library",
              detail=json_dumps({"removed": len(ids), "shares_cancelled": cancelled,
                                 "queue_cleared": q, "empty_seen_allowed": empty_ok,
                                 "archived": True}))
    out = {"removed": len(ids), "shares_cancelled": cancelled, "queue_cleared": q,
           "archived": True}
    if empty_ok:
        out["empty_seen_allowed"] = True
    return out


# v2.1.25：被「存量排期优先」拦下的追更事件用这个哨兵值标记（queue_id 正常存
# 队列项 id，0 = 待处理）。置成 -1 之后该事件既不会反复被本函数捞起重算，也
# 不会被看板当成「待处理变更事件」计数，还会进入日志保留策略的清理范围。
DIST_EVENT_BLOCKED = -1


def process_change_events(db: Session, *, publish: bool = True) -> dict:
    """v0.8.0：把扫描批次产生的「内容更新」事件转成一次作品级更新推送。

    - 同一 (作品, 变化指纹) 只处理一次；一批多集同时变化只推一条。
    - 已分享影视内容变化**复用原分享链接**，不重建分享、不取消再创建。
    - 仅由「立即扫描 / 重新扫描库 / 定时扫描」调用。v0.8.5 的镜像同步管线刻意不调用
      本函数：历史 queue_id=0 事件一旦在这里放开，会一次性批量补推，属用户明确禁止的群推。
    - publish=False 时只保留事件不处理（保留给未来需要延期处理的调用方）。

    v2.1.25（用户口径 1）：**存量排期优先档下整条追更通道停用**。
    追更不走 `dispatch`，是这里把队列项复活成 pending 的独立通道，`conflict_policy`
    的排序与门禁本来都管不到它 —— 于是「存量模式不推新片」这个口径在追更上完全不成立。
    现在：`prefer_backlog` 下全部拦截、**不建任何任务也不记账**（不重复登 v2 追更
    任务、不动队列项、不占名额），只把事件标成已拦截。作品下次轮到存量轮转时，
    自然会带着最新内容再推一次 —— 这正是用户要的「下一次轮转检测到更新可重推」。
    """
    events = list(db.scalars(select(DistributionEvent).where(
        DistributionEvent.queue_id == 0)).all())
    stats = {"events": len(events), "queued": 0, "share_reused": 0, "skipped": 0,
             "deferred": 0, "blocked": 0}
    if not events:
        return stats
    if not publish:
        stats["deferred"] = len(events)
        return stats
    # v2.1.25：存量排期优先 → 追更全部拦下（判定层见 share_center_v2.update_push_suppressed）
    try:
        from . import share_center_v2 as _sc_v2
        suppressed = _sc_v2.update_push_suppressed(db)
    except Exception:
        # 配置读不到就按「不拦」处理：宁可多推一条，也不能因为一次读配置失败
        # 让真实追更整批消失（漏推比多推更难被发现）。
        logger.warning("[share] 读取冲突处理策略失败，追更通道按未拦截处理", exc_info=True)
        suppressed = False
    if suppressed:
        for ev in events:
            ev.queue_id = DIST_EVENT_BLOCKED
        db.flush()
        stats["blocked"] = len(events)
        logger.info("[share] 冲突策略为「存量排期优先」，拦截 %d 条追更事件（不建任务、不计账）",
                    len(events))
        return stats
    grouped: dict[str, list[Resource]] = {}
    for r in db.scalars(select(Resource)).all():
        grouped.setdefault(work_group_key(r), []).append(r)
    for ev in events:
        members = grouped.get(ev.work_key, [])
        if not members:
            stats["skipped"] += 1
            continue
        rep = next((m for m in members if m.identify_status == "identified"
                    and m.poster_path and m.share_url), None)
        rep = rep or next((m for m in members if m.identify_status == "identified"
                           and m.poster_path), None)
        if not rep:
            stats["skipped"] += 1
            continue
        if rep.share_url:
            ev.share_reused = True
            stats["share_reused"] += 1
        # 四模块重构 阶段B/C：更新事件登记独立分享任务（追更，复用原链接，独立事件 ID）
        try:
            from . import share_center
            sc_task = share_center.shadow_register(db, rep, kind="update", event_id=ev.id)
            if sc_task is not None:
                ev.share_task_id = sc_task.id
        except Exception:
            pass
        ids = [m.id for m in members]
        # v2.1.11：归档行也占着唯一键，命中则复活复用而不是新建。
        # 「内容更新」推送会重置队列项状态，归档行复活后正好承接这次更新推送。
        from .archive import find_queue_item, revive_queue_item
        from .models import include_archived
        q = db.scalar(select(QueueItem).where(
            QueueItem.resource_id.in_(ids)).order_by(QueueItem.id).limit(1))
        if q is None:
            with include_archived():
                q = db.scalar(select(QueueItem).where(
                    QueueItem.resource_id.in_(ids)).order_by(QueueItem.id).limit(1))
            if q is not None:
                revive_queue_item(q)
        if q:
            # 复用原队列项并重置为待发送 = 一次「内容更新」推送，不新增第二条
            q.status, q.attempts, q.error = "pending", 0, ""
            q.next_retry_at, q.sent_at = None, None
            ev.queue_id = q.id
            stats["queued"] += 1
        else:
            _after_identify_dispatch(db, rep)
            db.flush()
            q = db.scalar(select(QueueItem).where(
                QueueItem.resource_id.in_(ids)).order_by(QueueItem.id.desc()).limit(1))
            if q:
                ev.queue_id = q.id
                stats["queued"] += 1
            else:
                stats["skipped"] += 1
        # v2.1.22：把这次「追更」登记进 v2 账本（推送任务 / 推送队列 / 推送历史
        # 三个卡片才看得见），并把队列项标记成 v2 语义 —— 投递结果由 publisher
        # 回写到这条任务上。分享链接**复用**，不重复建链。
        if q is not None:
            try:
                from . import share_center_v2 as _sc_v2
                v2_task = _sc_v2.register_update_task(
                    db, rep, channel_id=int(q.channel_id), event_id=int(ev.id or 0))
                if v2_task is not None:
                    q.share_task_id = 0        # 不挂旧链，publisher 走 v2 回写分支
                    q.push_reason = "sc_v2"
                    q.event_id = int(v2_task.id)
                    db.flush()
            except Exception:
                # 记账失败绝不影响这次真实投递（队列项已置 pending）。
                logger.exception("[share] 追更登记 v2 任务失败，本次投递照常执行")
    return stats


async def _refresh_library_from_b(db: Session, *, reconcile: bool = True,
                                  auto_identify: bool = True,
                                  allow_empty_reconcile: bool = False) -> dict:
    """v0.8.7：三个「刷新」入口共用的「按 B 库实际内容重建资源库」。

    用户口径：「资源库里面刷新定义为读取最新的 B 库内容，B 库文件夹已无该影视
    则清理出面板」。顶部「刷新」、资源库「立即扫描」、「重新扫描库」统一走这里：
      ① run_scan(B 库, resume=False) 全量读取最新内容；
      ② _reconcile_library 按 seen 移除 B 库已不存在的影视并取消分享；
      ③ 识别新入库资源；④ 按既有机制处理内容变化。
    定时扫描与镜像后管线**不走这里**，继续保持「不自动删数据」。
    """
    root, root_source = _effective_scan_root(db)
    data = await run_scan(root, resume=False)
    out = {k: v for k, v in data.items() if k != "seen_file_ids"}
    out["root_source"] = root_source
    # v2.1.11：入库后回填「映射回 A 库」的作品名/影视号/类型（识别前的关键一步）。
    db.commit()
    out["source_mapped"] = backfill_source_titles(db)
    db.commit()
    if reconcile:
        out["reconcile"] = await _reconcile_library(
            db, data.get("seen_file_ids") or [],
            scan_complete=bool(data.get("complete", False)),
            resumed=bool(data.get("resumed", False)),
            batch_id=data.get("batch_id") or None,
            allow_empty=allow_empty_reconcile)
    if auto_identify:
        out["identify"] = await _post_scan_identify(db, publish_after_identify=True)
    try:
        out["changes"] = process_change_events(db, publish=True)
    except Exception as exc:
        out["changes"] = {"error": str(exc)[:200]}
    # v2.1.16：手动扫描本身就是「触发推送任务队列」的方式之一。
    # 三个「刷新」入口（顶部刷新 / 立即扫描 / 重新扫描库）都走本函数，扫描+识别
    # 结束后立刻跑一轮分享中心周期（sync → plan → dispatch），让新入库作品在本
    # 次操作内就进入队列，而不是等下一个 5 秒 tick。
    # 必须走线程 + 独立 Session：run_cycle 内含同步 commit，在主协程跑会冻结事件
    # 循环（v1.4.2 以来的既有红线）。先 commit 结束本会话的写事务，避免两个会话
    # 抢 SQLite 写锁。
    try:
        db.commit()
        out["share_cycle"] = await asyncio.to_thread(
            _run_share_center_v2_cycle_in_thread)
    except Exception as exc:
        out["share_cycle"] = {"error": str(exc)[:200]}
    return out


@app.post("/api/scan-now", dependencies=ADMIN)
async def scan_now(resume: bool = True, auto_identify: bool = True,
                   db: Session = Depends(get_db)):
    """v0.8.7：资源库「立即扫描」= 按最新 B 库内容全量重建（与顶部刷新 / 重新扫描库同口径）。

    `resume` 仅为兼容旧调用保留，统一为全量扫描，不再断点续扫。
    """
    try:
        out = await _refresh_library_from_b(db, auto_identify=auto_identify,
                                            allow_empty_reconcile=True)
        add_audit(db, "scan_manual", target_type="scan", detail=json_dumps(out))
        db.commit()
        return out
    except Exception as e:
        raise HTTPException(502, str(e))


# ---------- v0.9.10：手动扫描异步化（解决 Failed to fetch / 页面卡死） ----------
# 真机根因：手动扫描是「列整个 B 库 + 对账 + 识别 + 变更处理」的分钟级同步请求，
# 前端 fetch 等不到响应就被网关/浏览器断连 -> "Failed to fetch"，而后端仍在跑，
# 期间持有的 db 会话与其它 job 抢 SQLite 写锁，进一步触发 database is locked。
# 改为后台任务立即返回，前端轮询状态；原同步端点保留供内部/测试使用。
_async_scan: dict = {"running": False, "started_at": None, "finished_at": None,
                     "mode": "", "error": "", "result": {}, "stage": ""}
_async_scan_task: asyncio.Task | None = None


def _async_scan_reset(mode: str) -> None:
    _async_scan.update({"running": True, "started_at": utcnow().isoformat(),
                        "finished_at": None, "mode": mode, "error": "",
                        "result": {}, "stage": "启动"})
    # 阶段五 B-04：启动即持久化，进程重启后前端能查到「曾在跑」。
    try:
        with SessionLocal() as db:
            async_scan_state.save(db, _async_scan)
            db.commit()
    except Exception as exc:
        logger.warning("[async-scan] 持久化启动状态失败：%s", exc)


def _async_scan_persist() -> None:
    """把当前内存状态写回 DB（离散点调用，绝不跨 await 持写事务）。"""
    try:
        with SessionLocal() as db:
            async_scan_state.save(db, _async_scan)
            db.commit()
    except Exception as exc:
        logger.warning("[async-scan] 持久化进度失败：%s", exc)


async def _run_async_scan(mode: str) -> None:
    """后台执行扫描管线，全程独立 Session；状态写内存 + DB 快照（B-04）。"""
    global _async_scan_task
    try:
        with SessionLocal() as db:
            if mode == "refresh":
                _async_scan["stage"] = "扫描并重建资源库"
                out = await _refresh_library_from_b(db, auto_identify=True,
                                                    allow_empty_reconcile=True)
                add_audit(db, "library_refresh_async", target_type="scan", detail=json_dumps(out))
            elif mode == "rescan":
                _async_scan["stage"] = "重新扫描库"
                out = await _refresh_library_from_b(db, reconcile=True, auto_identify=True,
                                                    allow_empty_reconcile=True)
                add_audit(db, "library_rescan_async", target_type="scan", detail=json_dumps(out))
            else:  # scan
                _async_scan["stage"] = "立即扫描"
                out = await _refresh_library_from_b(db, auto_identify=True,
                                                    allow_empty_reconcile=True)
                add_audit(db, "scan_manual_async", target_type="scan", detail=json_dumps(out))
            db.commit()
            # v0.9.10：手动扫描也写本轮任务快照，进度节点反映本次扫描处理量
            if isinstance(out, dict):
                ident = out.get("identify") or {}
                _write_pipeline_snapshot(
                    db, _mirror_rule(db), mode="手动扫描", stage="done", running=False,
                    copied=0, renamed=0,
                    identified=int(ident.get("identified", 0) or 0),
                    queued=int(ident.get("queued", 0) or 0),
                    scan_files=int(out.get("files", 0) or 0),
                    enqueue_diag=dict(ident.get("enqueue_diag") or {}),
                    works=dict(ident.get("work_stats") or {}))
                db.commit()
        _async_scan["result"] = out if isinstance(out, dict) else {}
        _async_scan["stage"] = "完成"
    except Exception as exc:
        logger.exception("[async-scan] 后台扫描失败")
        _async_scan["error"] = f"{type(exc).__name__}: {exc}"[:1000]
        _async_scan["stage"] = "失败"
    finally:
        _async_scan["running"] = False
        _async_scan["finished_at"] = utcnow().isoformat()
        _async_scan_task = None
        # 阶段五 B-04：终结态（成功/失败/中断）持久化，前端重启后仍可查到本轮结果。
        _async_scan_persist()


@app.post("/api/scan/async", dependencies=ADMIN)
async def scan_async(mode: str = "scan"):
    """v0.9.10：启动后台扫描，立即返回；mode=scan|rescan|refresh。"""
    global _async_scan_task
    if _async_scan.get("running"):
        raise HTTPException(409, "已有扫描任务在运行，请等待完成")
    if mode not in ("scan", "rescan", "refresh"):
        raise HTTPException(400, "mode 只能是 scan / rescan / refresh")
    _async_scan_reset(mode)
    _async_scan_task = asyncio.create_task(_run_async_scan(mode))
    return {"started": True, "mode": mode}


@app.get("/api/scan/async", dependencies=ADMIN)
def scan_async_status():
    """轮询后台扫描状态：running/stage/result/error。"""
    return dict(_async_scan)


@app.post("/api/library/rescan", dependencies=ADMIN)
async def rescan_library(auto_identify: bool = True, reconcile: bool = True,
                         db: Session = Depends(get_db)):
    """v0.8.7：「重新扫描库」与顶部刷新、立即扫描共用同一重建管线。"""
    try:
        out = await _refresh_library_from_b(db, reconcile=reconcile,
                                            auto_identify=auto_identify,
                                            allow_empty_reconcile=True)
        add_audit(db, "library_rescan", target_type="library", detail=json_dumps(out))
        db.commit()
        return {"ok": True, **out}
    except Exception as e:
        raise HTTPException(502, str(e))


@app.post("/api/library/refresh", dependencies=ADMIN)
async def refresh_library(db: Session = Depends(get_db)):
    """v0.8.7：顶部「刷新」——读取最新 B 库内容并按实际内容重建资源库面板。

    B 库已无该影视时清出面板并取消分享；空批次只有「批次完整且确实扫到目录」
    才被信任，接口抖动不会误清整个资源库。
    """
    try:
        out = await _refresh_library_from_b(db, allow_empty_reconcile=True)
        add_audit(db, "library_refresh", target_type="library", detail=json_dumps(out))
        db.commit()
        return {"ok": True, **out}
    except Exception as e:
        raise HTTPException(502, str(e))


# ---------- 资源与分享 ----------
@app.get("/api/resources", dependencies=ADMIN)
def resources(q: str = "", limit: int = 100, media_type: str = "",
              identify_status: str = "", db: Session = Depends(get_db)):
    stmt = select(Resource).order_by(Resource.id.desc()).limit(min(limit, 500))
    if q:
        stmt = stmt.where(or_(Resource.name.ilike(f"%{q}%"), Resource.media_title.ilike(f"%{q}%")))
    if media_type in ("movie", "tv"):
        stmt = stmt.where(Resource.media_type == media_type)
    if identify_status:
        stmt = stmt.where(Resource.identify_status == identify_status)
    keys = ["id", "file_id", "parent_id", "path_hint", "name", "size", "ext", "tmdb_id", "media_type",
            "media_title", "original_title", "overview", "poster_path", "genres",
            "origin_country", "vote_average", "season", "episode", "identify_status",
            "identify_source", "identify_note", "share_url", "share_status",
            "share_duration", "revision", "utime",
            # v2.1.11：A 库映射（正确作品名 / 原文件名 / 影视号 / 类型）。
            # 前端手动修正窗口用这几个字段预填，避免拿 B 库英文名去搜。
            "source_title", "source_name", "source_tmdb_id", "source_media_type"]
    return [{k: getattr(x, k) for k in keys} for x in db.scalars(stmt).all()]


def _normalize_work_name(s: str) -> str:
    """兼容旧引用；v0.7.1 起实现统一收在 utils，供 main / publisher 共用。"""
    return normalize_work_name(s)


def _series_group_key(x: Resource) -> str:
    """v0.6.5/v0.7.1：单集 → 作品的聚合键。

    展示（library/series）、分享（series_dir）与推送（publisher 集数/大小）三处共用
    utils.work_group_key，且只用扫描阶段就有的字段，未识别单集不会被漏掉或裂成两张卡。
    """
    return work_group_key(x)


def _year_of(title: str) -> str:
    """从作品名里取年份（如「大军师司马懿 (2017)」→ 2017）。

    v2.1.11：展示改用 A 库正确作品名后，年份也要跟着从 A 库名里取
    （B 库英文名里往往没有年份）。
    """
    m = re.search(r"[\[\(\{]\s*((?:19|20)\d{2})\s*[\]\)\}]", str(title or ""))
    return m.group(1) if m else ""


_SEASON_DIR_RE = re.compile(r"^(?:season|series|s)\s*[._\- ]?\d{1,3}$|^第\s*\d{1,2}\s*[季部]$|^S\d{1,2}$", re.I)


@app.get("/api/library/series", dependencies=ADMIN)
def library_series(q: str = "", media_type: str = "", finish: str = "",
                   identify_status: str = "", share_status: str = "",
                   db: Session = Depends(get_db)):
    """v0.6.4：按作品聚合的资源库视图，参考 STRM 刮削海报墙。

    每个作品返回海报、标题、年份、类型、集数范围、识别/分享汇总，并带成员单集 id 列表，
    前端据此按「一部剧一张卡」呈现，操作（推送/识别/取消分享）落到代表资源。
    """
    stmt = select(Resource).order_by(Resource.id.desc())
    if media_type in ("movie", "tv"):
        stmt = stmt.where(Resource.media_type == media_type)
    rows = list(db.scalars(stmt).all())
    by_id = {x.id: x for x in rows}  # 阶段三 A-08：复用已取资源，避免筛选时逐条 db.get
    # 阶段六资源卡角标：一次性预加载「已推送资源集合」与「作品→链接状态」，
    # 避免逐卡查询（N+1）。两者均来自本地库，失败降级为空不阻断列表。
    #
    # v2.1.3：「已分享」与「已推送」标签同步（用户口径：这两块应该同步进行）。
    # 根因：此前「已分享」只看 legacy `Resource.share_status`，而**分享中心 v2
    # 建链从不回写该字段**——v2 建好的链接只落在 `sc_publish_tasks.share_url`，
    # 于是资源库卡片上会出现「已推」但没有「已分享」的矛盾状态。
    # 现在以**同一份真相**判定：
    #   - 已分享：任一成员有 share_url，或 v2 分享任务里存在已建链（active）且有链接；
    #   - 已推送：任一成员有实际发送记录（sent_at 非空），或 v2 任务已 published。
    # 两个标签由同一组数据派生，天然同步。
    try:
        from .models import ShareRecord as _SR
        sent_ids = {r[0] for r in db.execute(select(QueueItem.resource_id).where(
            QueueItem.sent_at.isnot(None))).all()}
        link_by_key = {r.work_key: str(r.link_state or "active") for r in db.scalars(
            select(_SR).where(_SR.current_flag == 1)).all()}
    except Exception:
        sent_ids, link_by_key = set(), {}
    # v2 分享中心：作品 → (是否已建链有链接, 是否已推送)
    try:
        from .models import SCPublishTask as _SPT
        v2_shared: set[str] = set()
        v2_pushed: set[str] = set()
        v2_url_by_key: dict[str, str] = {}
        for t in db.scalars(select(_SPT)).all():
            wk = str(t.work_key or "")
            if not wk:
                continue
            url = str(t.share_url or "").strip()
            if url and str(t.link_state or "") == "active":
                v2_shared.add(wk)
                if url not in v2_url_by_key.get(wk, ""):
                    v2_url_by_key[wk] = url
            if str(t.status or "") == "published":
                v2_pushed.add(wk)
    except Exception:
        v2_shared, v2_pushed, v2_url_by_key = set(), set(), {}
    groups: dict[str, dict] = {}
    for x in rows:
        key = _series_group_key(x)
        g = groups.get(key)
        if g is None:
            hint = parse_media_name(x.name)
            # v2.1.11：展示也走「映射回 A 库」口径。
            # 标题优先用识别成功的 TMDB 正名；未识别时用 A 库正确作品名
            # （B 库改过名的英文译名对用户没有意义）。
            src_title = str(getattr(x, "source_title", "") or "").strip()
            src_type = str(getattr(x, "source_media_type", "") or "").strip()
            src_tmdb = getattr(x, "source_tmdb_id", None)
            g = groups[key] = {
                "key": key,
                "title": x.media_title or src_title or hint.get("primary_title") or x.name,
                "year": str(hint.get("year") or "") or _year_of(src_title),
                "media_type": (x.media_type or src_type
                               or ("tv" if hint.get("is_tv_hint") else "")),
                "poster_path": x.poster_path or "",
                "overview": x.overview or "",
                "genres": x.genres or "",
                "vote_average": float(x.vote_average or 0),
                "tmdb_id": x.tmdb_id,
                # v2.1.11：A 库映射的影视号与类型一并透出，
                # 供前端手动修正窗口预填（不必拿 B 库英文名去搜）。
                "source_title": src_title,
                "source_tmdb_id": src_tmdb,
                "source_media_type": src_type,
                "identify_status": x.identify_status,
                "share_url": "",
                "share_status": "not_created",
                "share_duration": 0,
                "finish_status": x.finish_status or "",
                "finish_locked": bool(getattr(x, "finish_locked", False)),
                "count": 0, "identified_count": 0,
                "seasons": set(), "episodes": [],
                "ids": [], "representative_id": x.id,
                "unidentified_count": 0,
                # v2.1.6：作品级 TMDB 分类集合（资源库按分类分组展示用）。
                # primary_genre = genres 列表第一个（与分享中心同口径）；
                # genre_names 只含官方 genre 字典里的中文名（不编造分类）。
                "primary_genre": "",
                "genre_names": [],
            }
        g["count"] += 1
        g["ids"].append(x.id)
        # v2.1.11：组内成员可能识别状态不齐（A 库映射只在部分行回填成功），
        # 标题/影视号/类型取「组内已有的最好信息」，不因首个成员缺映射就定死。
        if not g.get("source_title") and str(getattr(x, "source_title", "") or "").strip():
            g["source_title"] = str(x.source_title or "").strip()
            g["source_tmdb_id"] = getattr(x, "source_tmdb_id", None)
            g["source_media_type"] = str(getattr(x, "source_media_type", "") or "").strip()
        # 标题：识别成功的 TMDB 正名 > A 库正确作品名 > 目录线索 > 文件名
        if x.media_title and g.get("title") == g.get("source_title"):
            g["title"] = x.media_title
        if not g.get("media_type") and x.media_type:
            g["media_type"] = x.media_type
        if x.identify_status == "identified":
            g["identified_count"] += 1
        else:
            g["unidentified_count"] += 1
        if x.poster_path and not g["poster_path"]:
            g["poster_path"] = x.poster_path
        if x.overview and not g["overview"]:
            g["overview"] = x.overview
        if x.tmdb_id and not g["tmdb_id"]:
            g["tmdb_id"] = x.tmdb_id
        if x.share_url and not g["share_url"]:
            g["share_url"] = x.share_url
        if x.share_status == "active":
            g["share_status"] = "active"
        elif g["share_status"] != "active":
            g["share_status"] = x.share_status
        # v0.7.0：效期/完结状态取任一成员的有效值（同一作品应一致）
        if getattr(x, "share_duration", 0) and not g["share_duration"]:
            g["share_duration"] = int(x.share_duration or 0)
        # v0.8.0：手动锁定的完结/追更优先于任何自动推断值
        locked = bool(getattr(x, "finish_locked", False))
        if locked and not g["finish_locked"]:
            g["finish_status"] = x.finish_status or ""
            g["finish_locked"] = True
        elif not g["finish_locked"] and getattr(x, "finish_status", "") and not g["finish_status"]:
            g["finish_status"] = x.finish_status
        if x.media_type == "tv" and (x.season or x.episode):
            if x.season:
                g["seasons"].add(int(x.season))
            g["episodes"].append((int(x.season or 0), int(x.episode or 0)))
        # v2.1.6：收集作品级 TMDB 分类（资源库按分类分组展示用）。
        # 只收**官方 genre 字典里存在的中文名**——tmdb.py::TMDB_GENRES 是唯一权威，
        # 非官方名（如接口偶发的其它写法）不入组，避免页面上出现自造分类。
        # 「主分类」另记：严格取 genres 列表的**第一个**（与 share_center_v2
        # ._primary_category 完全同口径）——即使它不在官方字典里，主分类也仍是它，
        # 避免与分享中心出现「同一作品两处归类不同」。
        try:
            from .tmdb import TMDB_GENRES as _TG
            from .tmdb import as_list as _as_list
            for _name in _as_list(x.genres):
                if not _name:
                    continue
                if not g["primary_genre"]:
                    g["primary_genre"] = _name
                if _name in _TG.values() and _name not in g["genre_names"]:
                    g["genre_names"].append(_name)
        except Exception:
            pass
    out = []
    for g in groups.values():
        seasons = sorted(g.pop("seasons"))
        eps = sorted(set(g.pop("episodes")))
        label = ""
        if seasons:
            label = f"第{seasons[0]}季" if len(seasons) == 1 else f"第{seasons[0]}-{seasons[-1]}季"
        # v0.6.5：只有已识别集数 >= 2 且连续时才显示 E 范围；单集或零散集数显示进度，
        # 避免只识别到 E25 时显示误导性的「E25-E25」。
        if len(eps) >= 2:
            first, last = eps[0][1], eps[-1][1]
            contiguous = (last - first + 1) == len([e for s, e in eps if s == seasons[0]]) if seasons else False
            if contiguous and last > first:
                label += f" · E{first:02d}-E{last:02d}"
            else:
                label += f" · 已识别 {len(eps)} 集"
        elif len(eps) == 1:
            label += f" · 已识别 E{eps[0][1]:02d}"
        g["episode_label"] = label
        g["season_count"] = len(seasons)
        # v2.1.6：作品级分类（资源库按 TMDB 分类分组展示用）。
        # primary_genre 已在收集阶段按「genres 列表第一个」定好（与分享中心同口径）；
        # genre_names 只含官方 genre 字典里的中文名，这里排序后返回（前端展示顺序稳定）。
        g["genre_names"] = sorted(g["genre_names"])
        # 阶段六资源卡角标：是否已推送（任一成员有 sent 记录）+ 当前链接状态（本地推断）
        #
        # v2.1.3：「已推送」与「已分享」由**同一份真相**派生，两者天然同步：
        #   - 已推送 = 任一成员有实际发送记录，或 v2 分享任务已 published；
        #   - 已分享 = 任一成员有 share_url，或 v2 分享任务已建链（active）且有链接。
        # v2 链建链**不回写** legacy Resource.share_status，所以必须显式合并，
        # 否则会出现「已推」标签亮着、却没有「已分享」的矛盾状态。
        g["pushed"] = any(i in sent_ids for i in g["ids"]) or (g["key"] in v2_pushed)
        v2_linked = g["key"] in v2_shared
        if v2_linked:
            g["share_status"] = "active"
            if not g["share_url"]:
                # v2 的链接挂在任务上，资源行可能还没回写；这里给出可点开的链接
                g["share_url"] = v2_url_by_key.get(g["key"], "")
        g["link_state"] = link_by_key.get(g["key"], "") or ("active" if v2_linked else "")
        # 「已推送」必然经过建链 → 分享状态至少是 active（消除「已推但未分享」的矛盾）
        if g["pushed"] and not str(g["share_url"] or "").strip():
            g["share_status"] = g["share_status"] if g["share_status"] != "not_created" else "active"
        # 筛选：标题/文件名关键词
        if q and q.lower() not in (str(g["title"]) + " " + " ".join(str(i) for i in g["ids"])).lower():
            # 关键词也匹配成员文件名
            member_hit = any(q.lower() in ((by_id[i].name if i in by_id else "") or "").lower()
                             for i in g["ids"])
            if not member_hit:
                continue
        # v0.7.0：一级状态栏筛选（完结/追更、识别状态、分享状态）
        if finish in ("ended", "returning") and g["finish_status"] != finish:
            continue
        if identify_status and g["identify_status"] != identify_status:
            continue
        if share_status and g["share_status"] != share_status:
            continue
        out.append(g)
    return out


@app.get("/api/library/finish-check", dependencies=ADMIN)
async def finish_check(limit: int = 30, db: Session = Depends(get_db)):
    """v0.8.0：追新/完结校验——TMDB 季数/已播出集数 vs B 库实际文件。

    只做只读核对，不自动改状态：TMDB 的 status 经常滞后或被误标，
    最终以用户手动打标为准（手动锁定的作品会标注出来）。
    """
    grouped: dict[str, list[Resource]] = {}
    for r in db.scalars(select(Resource)).all():
        grouped.setdefault(work_group_key(r), []).append(r)
    cache: dict[int, dict] = {}
    items: list[dict] = []
    for key, members in grouped.items():
        rep = next((m for m in members if m.tmdb_id and m.media_type == "tv"), None)
        if not rep:
            continue
        videos = [m for m in members if is_video_file(m)] or members
        eps = set()
        for m in videos:
            se, ep = work_episode_numbers(m)
            if ep is not None:
                eps.add((int(se or 0), int(ep)))
        local_count = len(eps) or len(videos)
        tid = int(rep.tmdb_id)
        if tid not in cache:
            try:
                cache[tid] = await tmdb.detail(tid, "tv", db)
            except Exception as exc:
                cache[tid] = {"error": str(exc)[:160]}
        d = cache[tid] or {}
        expected = int(d.get("number_of_episodes") or 0)
        seasons = int(d.get("number_of_seasons") or 0)
        tmdb_status = str(d.get("finish_status") or d.get("tmdb_status") or "")
        manual = any(bool(getattr(m, "finish_locked", False)) for m in members)
        current = next((m.finish_status for m in members if m.finish_status), "") or ""
        if d.get("error"):
            verdict = "unknown"
        elif not expected:
            verdict = "unknown"
        elif local_count >= expected:
            verdict = "complete"
        else:
            verdict = "missing"
        items.append({
            "work_key": key, "title": rep.media_title or rep.name,
            "tmdb_id": tid, "tmdb_status": tmdb_status,
            "expected_episodes": expected, "expected_seasons": seasons,
            "local_episodes": local_count, "missing": max(expected - local_count, 0),
            "current_finish": current, "manual_locked": manual,
            "verdict": verdict,
            "suggestion": ("TMDB 标完结但本地缺集，建议手动标追更" if verdict == "missing" and
                           tmdb_status == "ended" else
                           "本地集数已齐，可标记完结" if verdict == "complete" and
                           current != "ended" else ""),
            "error": d.get("error", ""),
        })
        if len(items) >= min(limit, 100):
            break
    return {"ok": True, "checked": len(items), "items": items}


@app.post("/api/library/clear", dependencies=ADMIN)
def clear_library(db: Session = Depends(get_db)):
    """v0.6.4：清空当前扫描结果（资源 + 队列 + 审核 + 扫描断点），保留 Bot/频道/规则/模板/凭证。

    用于重新扫描前清除旧的识别失败、重试残留数据。不触碰光鸭网盘文件、不删除已建分享。

    v2.1.11：计数改用 select(func.count())——`query().count()` 走的是
    SELECT COUNT(*) 不经过 do_orm_execute 事件钩子，拿不到归档过滤，
    会把已归档行也算进来（数字对不上实际删除量）。
    """
    resources = db.scalar(select(func.count()).select_from(Resource)) or 0
    queue = db.scalar(select(func.count()).select_from(QueueItem)) or 0
    cursors = db.query(ScanCursor).count()
    db.query(QueueItem).delete(synchronize_session=False)
    db.query(Resource).delete(synchronize_session=False)
    db.query(ScanCursor).delete(synchronize_session=False)
    # v0.8.0：变化留痕与未处理的更新事件一并清掉，避免清库后旧事件触发误推送
    db.query(ResourceVersion).delete(synchronize_session=False)
    db.query(DistributionEvent).delete(synchronize_session=False)
    add_audit(db, "library_cleared", detail=json_dumps({"resources": resources,
                                                          "queue": queue, "cursors": cursors}))
    db.commit()
    return {"ok": True, "resources": resources, "queue": queue, "cursors": cursors}


class BatchLibraryInput(BaseModel):
    ids: list[int] = []           # 作品代表资源 id
    action: str = ""              # set_duration / cancel_share / re_share / identify / set_finish
    duration: int = 0             # set_duration / re_share 使用
    finish: str = ""              # set_finish：ended / returning / 空串(清空并锁定) / auto(恢复自动)


async def _cancel_share_one(db: Session, x: Resource) -> bool:
    """取消单条分享并按 share_id 同步清空所有共用该分享的剧集单集。返回是否成功。"""
    if not x.share_id:
        return False
    try:
        await GuangYaShareClient(db).delete(x.share_id)
    except Exception:
        return False
    for item in db.scalars(select(Resource).where(Resource.share_id == x.share_id)).all():
        item.share_status = "cancelled"
        item.share_url, item.share_id = "", ""
    return True


@app.post("/api/library/batch", dependencies=ADMIN)
async def batch_library(p: BatchLibraryInput, db: Session = Depends(get_db)):
    """v0.7.0：批量管理勾选作品——设置效期 / 取消分享 / 重新分享 / 批量重识别。

    所有操作落到作品代表资源，剧集按整剧目录处理；逐个执行并汇总成功/失败，
    单个失败不影响其余。identify 走自动识别（TMDB 决策链），存疑的不落库。
    """
    action = (p.action or "").strip()
    if action not in ("set_duration", "cancel_share", "re_share", "identify", "set_finish"):
        raise HTTPException(400, "action 仅支持 set_duration/cancel_share/re_share/identify/set_finish")
    if action in ("set_duration", "re_share"):
        _check_duration(p.duration)
    if action == "set_finish" and p.finish not in ("ended", "returning", "", "auto"):
        raise HTTPException(400, "finish 仅支持 ended / returning / 空串(清空) / auto(恢复自动)")
    ok = fail = 0
    errors: list[str] = []
    for rid in p.ids:
        x = db.get(Resource, rid)
        if not x:
            fail += 1
            continue
        try:
            if action == "set_finish":
                # v0.8.0：完结剧识别有误时手动修正。按整部作品同步所有单集，
                # 并锁定状态，避免下次扫描/TMDB 回填又把人工判断改回去。
                key = work_group_key(x)
                targets = [r for r in db.scalars(select(Resource)).all()
                           if work_group_key(r) == key] or [x]
                if p.finish == "auto":
                    for r in targets:
                        r.finish_status, r.finish_locked = "", False
                else:
                    for r in targets:
                        r.finish_status, r.finish_locked = p.finish, True
                add_audit(db, "finish_marked", target_type="library", target_id=str(rid),
                          detail=json_dumps({"work_key": key, "finish": p.finish,
                                             "members": len(targets)}))
                ok += 1
            elif action == "identify":
                # v2.1.11：手动识别走「映射回 A 库」线索，且**绕过缓存**（手动=重新解析）。
                r = await _identify_with_source_hints(x.name, x, db)
                if r.get("ok") and r.get("best"):
                    _apply_identification(x, r["best"], source=r["source"],
                                          note=r["reason"], hint=r["hint"])
                    ok += 1
                else:
                    fail += 1
                    errors.append(f"#{rid} {r.get('status', '存疑')}")
            elif action == "cancel_share":
                if await _cancel_share_one(db, x):
                    ok += 1
                else:
                    fail += 1
                    errors.append(f"#{rid} 无分享或取消失败")
            elif action == "set_duration":
                if not x.share_id:
                    fail += 1
                    errors.append(f"#{rid} 尚无分享")
                    continue
                await GuangYaShareClient(db).update(x.share_id, x.media_title or x.name, p.duration)
                for item in db.scalars(select(Resource).where(Resource.share_id == x.share_id)).all():
                    item.share_duration = p.duration
                ok += 1
            elif action == "re_share":
                await _cancel_share_one(db, x)
                db.flush()
                dir_id = str(getattr(x, "share_dir_id", "") or "")
                is_series = bool(dir_id)
                target_id = dir_id if is_series else x.file_id
                title = x.media_title or x.name or ""
                if is_series and x.season:
                    title = f"{title} 第{int(x.season)}季"
                # 阶段四：统一走光鸭建分享解析适配器（三处散点口径收敛）
                share_id, share_url = await create_share_parsed(
                    GuangYaShareClient(db), target_id, title, p.duration)
                targets = list(db.scalars(select(Resource).where(Resource.share_dir_id == dir_id)).all()) if is_series else [x]
                for item in (targets or [x]):
                    item.share_id, item.share_url = share_id, share_url
                    item.share_status = "active" if share_url else "failed"
                    item.share_duration = p.duration
                ok += 1 if share_url else 0
                if not share_url:
                    fail += 1
        except Exception as exc:
            fail += 1
            errors.append(f"#{rid} {str(exc)[:80]}")
    add_audit(db, "library_batch", target_type="library",
              detail=json_dumps({"action": action, "ids": p.ids, "duration": p.duration,
                                 "ok": ok, "fail": fail}))
    db.commit()
    return {"ok": True, "action": action, "succeeded": ok, "failed": fail, "errors": errors[:20]}


def _check_duration(duration: int):
    if duration not in VALID_DURATIONS:
        raise HTTPException(400, "只支持永久(0)、1天(86400)、7天(604800)、30天(2592000)")


@app.post("/api/resources/{rid}/share", dependencies=ADMIN)
async def create_share(rid: int, duration: int = 0, db: Session = Depends(get_db)):
    _check_duration(duration)
    x = db.get(Resource, rid)
    if not x:
        raise HTTPException(404, "资源不存在")
    try:
        # v0.6.5：剧集一律按「剧集目录」分享，绝不按单集——单集分享是逐集推送的根因。
        # 与 publisher.ensure_share 同口径：目录 fileId + 标题带季，链接同步给同目录所有单集。
        dir_id = str(getattr(x, "share_dir_id", "") or "")
        scope = str(getattr(x, "share_scope", "") or "")
        is_series = bool(dir_id)
        target_id = dir_id if is_series else x.file_id
        title = x.media_title or x.name or ""
        if is_series and x.season:
            title = f"{title} 第{int(x.season)}季" if title else f"第{int(x.season)}季"
        # 同目录已有分享则直接复用，避免重复建链
        if is_series:
            sibling = db.scalar(select(Resource).where(
                Resource.share_dir_id == dir_id, Resource.share_url != "",
                Resource.id != x.id).limit(1))
            if sibling and sibling.share_url:
                x.share_id, x.share_url = sibling.share_id, sibling.share_url
                x.share_status, x.share_duration = "active", sibling.share_duration
                db.commit()
                return {"ok": True, "share_id": x.share_id, "share_url": x.share_url,
                        "scope": scope, "reused": True}
        # 阶段四：统一走光鸭建分享解析适配器（三处散点口径收敛）
        share_id, share_url = await create_share_parsed(
            GuangYaShareClient(db), target_id, title, duration)
        # 同步给同目录所有单集，一处分享整剧可见
        targets = list(db.scalars(select(Resource).where(Resource.share_dir_id == dir_id)).all()) if is_series else [x]
        targets = targets or [x]
        for item in targets:
            item.share_id, item.share_url = share_id, share_url
            item.share_status = "active" if share_url else "failed"
            item.share_duration = duration
        add_audit(db, "share_created_manual", target_type="resource", target_id=str(rid),
                  detail=json_dumps({"duration": duration, "has_url": bool(share_url),
                                     "target_id": target_id, "scope": scope, "members": len(targets)}))
        db.commit()
        return {"ok": True, "share_id": x.share_id, "share_url": x.share_url,
                "scope": scope, "members": len(targets)}
    except Exception as e:
        raise HTTPException(502, str(e))


@app.put("/api/resources/{rid}/share", dependencies=ADMIN)
async def update_share(rid: int, duration: int = 0, db: Session = Depends(get_db)):
    _check_duration(duration)
    x = db.get(Resource, rid)
    if not x or not x.share_id:
        raise HTTPException(404, "分享记录不存在")
    try:
        await GuangYaShareClient(db).update(x.share_id, x.media_title or x.name, duration)
        x.share_duration = duration
        add_audit(db, "share_updated", target_type="resource", target_id=str(rid))
        db.commit()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(502, str(e))


@app.delete("/api/resources/{rid}/share", dependencies=ADMIN)
async def delete_share(rid: int, db: Session = Depends(get_db)):
    x = db.get(Resource, rid)
    if not x or not x.share_id:
        raise HTTPException(404, "分享记录不存在")
    try:
        share_id = x.share_id
        await GuangYaShareClient(db).delete(share_id)
        # v0.6.4：清空本条及共用该分享的同目录剧集（v0.6.3 起整剧共用一个分享），
        # 否则 share_url 残留会让前端仍显示已分享、ensure_share 继续复用已删链接。
        for item in db.scalars(select(Resource).where(Resource.share_id == share_id)).all():
            item.share_status = "cancelled"
            item.share_url, item.share_id = "", ""
        add_audit(db, "share_cancelled", target_type="resource", target_id=str(rid),
                  detail=json_dumps({"share_id": share_id}))
        db.commit()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(502, str(e))


# ---------- TMDB 识别 ----------
class IdentifyInput(BaseModel):
    tmdb_id: int
    media_type: str  # movie / tv
    note: str = ""


def _apply_identification(x: Resource, d: dict, source: str = "manual",
                          note: str = "", hint: dict | None = None) -> None:
    """把 TMDB 详情写入资源：v0.6.0 一并落库分类标签、季集与识别来源。"""
    x.tmdb_id = d.get("tmdb_id")
    x.media_type = d.get("media_type") or ""
    x.media_title = d.get("title") or ""
    x.original_title = d.get("original_title") or ""
    x.overview = d.get("overview") or ""
    # v0.9.9 #202：TMDB 完整首播/上映日期，供消息模板 {{release_date}}
    x.release_date = d.get("release_date") or ""
    # v2.1.23：剧集最后一集播出日（电影为空）。「新上映」标签的过期判据靠它，
    # 不落库就只能退回「上映 30 天窗口」，跟用户口径（更新完结次日 0 点转存量）不符。
    x.last_air_date = d.get("last_air_date") or ""
    x.poster_path = d.get("poster_path") or ""
    x.genres = json_dumps(d.get("genres") or [])
    x.origin_country = json_dumps(d.get("origin_country") or [])
    try:
        x.vote_average = float(d.get("vote_average") or 0)
    except (TypeError, ValueError):
        x.vote_average = 0.0
    h = hint or {}
    if h.get("season") is not None:
        x.season = h["season"]
    if h.get("episode") is not None:
        x.episode = h["episode"]
    x.identify_status = "identified"
    x.identify_source = source
    x.identify_note = note or ""
    # v0.7.0：完结/追更状态落库（TMDB tv.status 推断，电影为空）
    # v0.8.0：用户在资源库手动修正过的状态锁定，重新识别/回填都不得覆盖人工判断
    if not bool(getattr(x, "finish_locked", False)):
        x.finish_status = str(d.get("finish_status") or "")
    # v0.8.9：TMDB 官方总集数。只取 tv 的 number_of_episodes，电影恒为 0；
    # 模板用它与「本地已收录集数」对比，才能显示「更新至 12 / 共 24 集」。
    try:
        x.total_episodes = (int(d.get("number_of_episodes") or 0)
                            if d.get("media_type") == "tv" else 0)
    except (TypeError, ValueError):
        x.total_episodes = 0
    # v0.8.1：电影与剧集统一按「作品目录」分享，绝不按单文件。
    # tv 已整理取上上层（剧名目录），movie 取直接父目录（电影主目录）；
    # 父目录是分类目录且无年份/TMDB 标记时退化为按文件，避免整类当一个作品。
    from .utils import series_dir_info
    info = series_dir_info(x.path_hint, getattr(x, "dir_chain_json", "[]"), x.parent_id,
                           x.media_type or "")
    x.share_scope = info["scope"]
    x.share_dir_id = info["dir_id"]
    x.share_dir_name = info["dir_name"]


def _work_key(x: Resource) -> str:
    """推送去重键：与资源库展示聚合键完全一致，保证「一张卡 = 一条推送」。"""
    return _series_group_key(x)


def _push_gate_passed(db: Session, x: Resource) -> bool:
    """推送门禁判定（旧链入队用）。True = 五项前置条件全部满足。

    门禁规则见 `push_gate.evaluate_push_gate`：海报 / 影片资料 / 识别成功 /
    影视目录已按命名约定整理 / 单文件已译为英文。判定是**作品级**的——推送的
    粒度是「一部作品一条推送」，因此按作品的全部成员判定，而不是只看当前这一集。

    这里把作品成员一次性取齐（同一作品键的所有 Resource），避免逐集重复全表扫。
    """
    from .push_gate import evaluate_push_gate
    key = _work_key(x)
    members = [s for s in db.scalars(select(Resource)).all() if _work_key(s) == key]
    if not members:
        members = [x]
    gate = evaluate_push_gate(db, key, members)
    return bool(gate.get("passed"))


def _enqueue_ready(db: Session, x: Resource, rule: Rule) -> bool:
    """v0.6.5：把「已识别 + 有海报」的资源按规则入队，同一作品同一频道只入一条。

    返回是否新增。扫描/识别/加规则三条路径都走这里，保证「一张卡 = 一条推送」
    在任何入口都成立。注意 Session 是 autoflush=False，入队后必须显式 flush，
    否则 identify_all 批量识别时前一条队列项对后续同剧单集不可见，36 集会全部入队。

    v2.1.3 推送门禁（真机 2026-11-22 黑豹2 事故）：在既有「识别成功 + 有海报」
    之外，补齐「影片资料 / 影视目录已整理 / 单文件已译为英文」三项，缺一不入队。
    门禁判定收敛在 `push_gate.evaluate_push_gate`，与分享中心 v2 的 dispatch 路径
    **共用同一处实现**，杜绝两条路径各写各的、其中一条漏检。

    门禁未过时返回 False（不入队）。这里不抛错、不写失败状态——被门禁拦下只是
    「还没准备好」，等镜像改名 / 补齐元数据后由 `recheck_gated_push` 重新入队，
    不会永久漏推。
    """
    if x.identify_status != "identified" or not (x.poster_path or "").strip():
        return False
    # v2.1.3 推送门禁：作品级判定，与 v2 dispatch 完全同一套规则。
    if not _push_gate_passed(db, x):
        return False
    # v0.8.1：旧数据（含电影）可能缺 share_dir_id，入队前现场补算作品目录，
    # 确保推送走整目录分享而非单文件。
    if not (getattr(x, "share_dir_id", "") or "").strip():
        from .utils import series_dir_info
        info = series_dir_info(x.path_hint or "", getattr(x, "dir_chain_json", "[]"),
                               x.parent_id, x.media_type or "")
        x.share_scope, x.share_dir_id, x.share_dir_name = info["scope"], info["dir_id"], info["dir_name"]
    ch = db.get(Channel, rule.channel_id)
    bot = db.get(Bot, ch.bot_id) if ch else None
    if not ch or not ch.enabled or not bot or not bot.enabled:
        return False  # 停用的频道/Bot 不入队，启用后由回溯补齐
    # 同一作品（含自身与所有兄弟单集）在该频道已有任意队列项就不再入队
    all_members = [s.id for s in db.scalars(select(Resource)).all()
                   if _work_key(s) == _work_key(x)]
    exists = db.scalar(select(QueueItem).where(
        QueueItem.resource_id.in_(all_members),
        QueueItem.channel_id == rule.channel_id).limit(1))
    if exists:
        return False
    auto_publish = bool(rule.auto_publish)
    # v2.1.11：归档行也占着 (resource_id, channel_id) 唯一键，直接 INSERT 会撞约束。
    # 命中归档行时复活复用（历史行保留，唯一键不重复）。
    from .archive import find_queue_item
    q_old = find_queue_item(db, x.id, rule.channel_id)
    if q_old is not None:
        q_old.status = "pending" if auto_publish else "review"
        q_old.attempts, q_old.error = 0, ""
        q_old.next_retry_at, q_old.sent_at = None, None
        db.flush()
        return True
    db.add(QueueItem(resource_id=x.id, channel_id=rule.channel_id,
                     status="pending" if auto_publish else "review"))
    db.flush()  # autoflush=False：立即落库，供同批次后续单集的去重查询看到
    # 四模块重构：登记到分享中心主链。只对 auto_publish 入队项登记——review（人工审核）
    # 项不能被分享中心 run_cycle 自动建分享并提交发送，否则绕过审核闸门。
    # 登记异常被内部吞掉，绝不影响主入队；未登记到的项 publisher 会走旧链兜底不漏发。
    if auto_publish:
        try:
            from . import share_center
            sc_task = share_center.shadow_register(db, x, kind="first",
                                                   channel_ids=[rule.channel_id])
            if sc_task is not None:
                q = db.scalar(select(QueueItem).where(
                    QueueItem.resource_id == x.id,
                    QueueItem.channel_id == rule.channel_id).limit(1))
                if q:
                    q.share_task_id = sc_task.id
                    q.push_reason = "first"
                    db.flush()
        except Exception:
            pass
    # 资源库 → 分享中心事件：新入库(identified)或镜像补齐后重建(restored)。
    # 有 cancelled 的旧 first 任务说明曾被 B 库删除，这次重新入库应视为 restored（规则2）。
    try:
        from . import events, share_center
        from .models import ShareTask
        wk = work_group_key(x)
        ever_cancelled = db.scalar(select(ShareTask.id).where(
            ShareTask.work_key == wk, ShareTask.kind == "first",
            ShareTask.status == share_center.ST_CANCELLED).limit(1))
        etype = events.RESOURCE_RESTORED if ever_cancelled else events.RESOURCE_NEW
        events.emit_resource_event(db, event_type=etype, work_key=wk, resource_id=x.id,
                                   payload={"share_dir_id": x.share_dir_id or "",
                                            "media_type": x.media_type or ""})
    except Exception:
        pass
    return True


def _ensure_route_channel(db: Session, route: dict):
    """v0.9.9 #201：路由目标 Channel 缺失时，用全局 Bot 现场补建并启用。

    真机根因：用户保存路由后 Channel 被清理（旧配置 / 手动改库 / 版本迁移），
    route_target_channel 找不到启用 Channel 就静默 continue，导致「识别成功却
    一条队列项都没有」。这里按 chat_id 现场补建，保证「配了路由就能推」。
    仍以路由表为配置真相源，不臆造未配置的 chat_id。
    """
    ch = bot_listener.route_target_channel(db, route)
    bot = _global_bot(db)
    if ch:
        # v0.9.10.2：真机「群聊自动推送仍收不到」的结构性根因——
        # v0.9.10 只在保存端点 push_routes_save 同步 bot_id，那是「保存时」的一次性动作；
        # 运行时入队命中已存在的路由 Channel 时直接 return，bot_id 仍指向换 token 前的
        # 旧 Bot（失效 / 不在群里），于是测试推送用新 Bot 能收、正式推送用旧 Bot 收不到。
        # 这里在运行时真相源强制校准到全局 Bot，存量数据无需用户重新点保存即自愈。
        if bot and ch.bot_id != bot.id:
            ch.bot_id = bot.id
            db.flush()
        return ch
    if not bot:
        return None
    kind = str(route.get("kind") or "channel")
    ch = Channel(bot_id=bot.id, name=f"{_ROUTE_CHANNEL_PREFIX}{kind}:{route['chat_id']}",
                 chat_id=route["chat_id"], kind=kind, enabled=True)
    db.add(ch)
    db.flush()
    return ch


# 阶段二：识别结果进程内缓存（纯内存，零 DB 写，避免高频识别打爆 TMDB）。
# key=(规范化 name, path_hint)；只缓存成功命中的结果，失败/存疑不缓存以便下次重试。
# 进程重启后自然失效（TMDB 元数据稳定，可接受）；force/手动 identify 不经本包装函数，
# 因此 force 永远重新解析，符合「force 才重解析」红线。
_IDENTIFY_CACHE: dict[tuple[str, str, int, str], dict] = {}
_IDENTIFY_CACHE_MAX = 2000


def _identify_cache_get(key: tuple[str, str, int, str]) -> dict | None:
    hit = _IDENTIFY_CACHE.get(key)
    return dict(hit) if hit is not None else None


def _identify_cache_put(key: tuple[str, str, int, str], value: dict) -> None:
    if len(_IDENTIFY_CACHE) >= _IDENTIFY_CACHE_MAX:
        # 简单有界清理：丢弃最早插入的 1/4（dict 保序），避免无界增长
        for k in list(_IDENTIFY_CACHE.keys())[:_IDENTIFY_CACHE_MAX // 4]:
            _IDENTIFY_CACHE.pop(k, None)
    _IDENTIFY_CACHE[key] = dict(value)


async def _identify_with_source_hints(name: str, x: Resource, db: Session) -> dict:
    """按「映射回 A 库」的线索做一次识别（**不经缓存**）。

    v2.1.11 抽出的核，供两处复用：
      - `identify_with_a_source_fallback`：带缓存的常规路径；
      - 手动识别 / force 批量识别：**必须绕过缓存**（红线：force 才重解析）。
    """
    q_name, q_hint, q_tmdb, q_type = source_query_hint(x)
    first = await tmdb.identify(q_name or name, db, path_hint=q_hint, use_ai=True,
                                forced_tmdb_id=q_tmdb, forced_type=q_type)
    if first and first.get("ok") and first.get("best"):
        return first
    # 没有 A 库映射、或映射线索未命中时：用 A 库原名再试一次（纯本地线索）
    a_name = a_source_name_hint(db, x)
    if not a_name or a_name in (q_name, name):
        return first
    retry = await tmdb.identify(a_name, db, path_hint=q_hint, use_ai=True,
                                forced_tmdb_id=q_tmdb, forced_type=q_type)
    if retry and retry.get("ok") and retry.get("best"):
        retry = dict(retry)
        retry["reason"] = (str(retry.get("reason") or "") + " · A库原名回溯命中").strip(" ·")
        return retry
    return first


async def identify_with_a_source_fallback(name: str, x: Resource, db: Session) -> dict:
    """识别结果缓存 + **映射回 A 库** 的识别线索（v2.1.11 起）。

    保持 tmdb.identify 的返回契约不变（ok/best/status/reason/...）。
    失败/存疑不缓存。

    v2.1.11 需求：资源库识别必须用 A 库的正确影视名 + TMDB 影视号，
    不能用改名后的 B 库名（B 库单集名是英文译名、作品目录带 · 标记，
    拿去 TMDB 搜几乎必然零命中）。
    线索来源（`utils.source_query_hint`）：
      - 文件名用 A 库原名（季集线索从中解析）；
      - 目录线索 = B 库链 + A 库作品名置于末尾（作品名用 A 库口径）；
      - TMDB 号 + 类型来自 A 库映射。**类型必须一起给**：TMDB 的 movie/tv
        是两套独立 ID 空间，只带号不带类型会跨命名空间串台（#247）。

    没有 A 库映射时退回资源自身的文件名与目录线索（行为与改造前一致），
    仍保留「A 库原名回溯」作为二次尝试。
    """
    q_name, q_hint, q_tmdb, q_type = source_query_hint(x)
    key = (str(q_name or name or "").strip().lower(), str(q_hint).strip(),
           int(q_tmdb or 0), str(q_type or ""))
    cached = _identify_cache_get(key)
    if cached is not None:
        return cached
    result = await _identify_with_source_hints(name, x, db)
    if result and result.get("ok") and result.get("best"):
        _identify_cache_put(key, result)
        return result
    return result


def a_source_name_hint(db: Session, x: Resource) -> str:
    """阶段二（资源库解耦）：从本地 MirrorFile.file_name 取 A 库原名作为识别线索。

    MirrorFile.file_name 永远是 A 库原名（改名不覆盖，见 models.py:389），因此无需
    访问 A 库、无需网络——纯本地查表。B 库 fileId 通过 target_file_id 关联。
    当 B 库改过名/目录线索失效导致识别失败时，用 A 原名重试可显著提升识别率。
    查不到返回空串，调用方据此决定是否重试。
    """
    try:
        row = db.scalar(select(MirrorFile.file_name).where(
            MirrorFile.target_file_id == str(x.file_id or "")).limit(1))
        return str(row or "").strip()
    except Exception as exc:
        logger.warning("[resource-decouple] A库原名回溯失败 file_id=%s：%s",
                       x.file_id, exc)
        return ""


def compute_resource_channel_targets(db: Session, x: Resource) -> list[dict]:
    """阶段二（资源库解耦）：算出某个已识别资源应推送到的频道目标快照。

    [解耦设计] 把「决定推给谁」从 _enqueue_after_identify 里抽成纯计算函数：
    只按现有路由表（push_routes/match_push_routes/规则匹配）算出目标 channel_id，
    并现场补建缺失的路由 Channel（_ensure_route_channel，与旧链等价），但**不写
    QueueItem、不建 ShareTask**。结果写进资源事件的 payload.channel_targets，供
    分享中心 consume_resource_events 独立接管推送，无需回调资源库的入队函数。

    返回值：[{"channel_id": int, "kind": str, "chat_id": str}]，可能为空（无目标）。
    语义必须与旧链 _enqueue_after_identify 完全一致，否则切换开关会漏推/错推：
      - 路由表非空：只走匹配到的路由（不再叠加 Rule）；
      - 路由表为空：走启用的 Rule（按 directory_id/keyword 过滤）。
    现场补建的 Channel 会 flush 落库（autoflush=False），保证随后 submit 能按 id 取到。
    """
    targets: list[dict] = []
    try:
        routes = bot_listener.push_routes(db)
        if routes:
            matched = bot_listener.match_push_routes(
                db, x, root_ids=bot_listener.scan_root_ids(db))
            for route in matched:
                ch = _ensure_route_channel(db, route)
                if ch:
                    targets.append({"channel_id": ch.id,
                                    "kind": str(route.get("kind") or "channel"),
                                    "chat_id": str(route.get("chat_id") or "")})
            return targets
        rules = list(db.scalars(select(Rule).where(Rule.enabled.is_(True))).all())
        for rule in rules:
            if rule.directory_id and rule.directory_id != x.parent_id:
                continue
            if rule.keyword and rule.keyword.lower() not in (x.name or "").lower():
                continue
            ch = db.get(Channel, rule.channel_id)
            bot = db.get(Bot, ch.bot_id) if ch else None
            if not ch or not ch.enabled or not bot or not bot.enabled:
                continue
            targets.append({"channel_id": ch.id, "kind": "channel", "chat_id": ch.chat_id or ""})
        return targets
    except Exception as exc:
        # 路由计算失败不阻断识别主链；返回空目标，由调用方决定是否回退旧链兜底。
        logger.warning("[resource-decouple] 计算频道目标失败 work_key=%s：%s",
                       work_group_key(x), exc)
        return []


def _identify_emit_only(db: Session, x: Resource, diag: dict | None = None) -> int:
    """阶段二（解耦开关开启时）识别入口的替代动作：只发资源事件，不直接入队/建分享。

    与 _enqueue_after_identify 的关键区别：本函数**不写 QueueItem**，推送决策完全交给
    分享中心 consume_resource_events → ensure_task → submit_to_dispatch 新链。为保证新链
    能独立算出推送目标，事件 payload 携带 channel_targets（由 compute_resource_channel_targets
    按与旧链完全一致的路由/规则逻辑算出）。

    返回 int（算出的目标数），语义对齐 _enqueue_after_identify 的返回值，调用方统计无需改。
    灰度开关关闭时识别入口仍走 _enqueue_after_identify，本函数不会被调用。
    """
    if x.identify_status != "identified" or not (x.poster_path or "").strip():
        return 0
    targets = compute_resource_channel_targets(db, x)
    try:
        from . import events, share_center
        from .models import ShareTask
        wk = work_group_key(x)
        ever_cancelled = db.scalar(select(ShareTask.id).where(
            ShareTask.work_key == wk, ShareTask.kind == "first",
            ShareTask.status == share_center.ST_CANCELLED).limit(1))
        etype = events.RESOURCE_RESTORED if ever_cancelled else events.RESOURCE_NEW
        payload = {"share_dir_id": x.share_dir_id or "",
                   "media_type": x.media_type or "",
                   "channel_targets": targets,
                   "decoupled": True}
        ev = events.emit_resource_event(db, event_type=etype, work_key=wk,
                                        resource_id=x.id, payload=payload)
        # 同一资源同一类型已存在未消费事件时 emit 返回旧事件（幂等）；
        # 用 targets 刷新其 payload，避免「开关切换后旧事件无 targets」导致断推。
        if ev is not None and not json.loads(ev.payload_json or "{}").get("channel_targets"):
            ev.payload_json = json.dumps(payload, ensure_ascii=False)
            db.commit()
        return len(targets)
    except Exception as exc:
        logger.warning("[resource-decouple] 只发事件失败 work_key=%s：%s",
                       work_group_key(x), exc)
        return 0


def _after_identify_dispatch(db: Session, x: Resource, diag: dict | None = None) -> int:
    """识别入口统一分发：按 resource_decoupled_enabled 灰度开关选新链/旧链。

    所有识别入口（扫描后识别、手动 identify、auto_identify、identify_all、更新事件兜底）
    都走这里，保证「一处开关、全局一致」。默认走旧链，零回归；开关开启且分享中心
    主链启用时才走「只发事件」新链，避免在 share_center_enabled=false 时切到无消费者的
    事件导致漏推。返回 int 语义与两条链一致。
    """
    decoupled = str(runtime_config.get(db, "resource_decoupled_enabled") or "false").lower() in (
        "1", "true", "yes", "on")
    share_on = str(runtime_config.get(db, "share_center_enabled") or "true").lower() in (
        "1", "true", "yes", "on")
    if decoupled and share_on:
        return _identify_emit_only(db, x, diag=diag)
    return _enqueue_after_identify(db, x, diag=diag)


def _enqueue_after_identify(db: Session, x: Resource, diag: dict | None = None) -> int:
    """v0.6.5：识别成功且拿到 TMDB 海报后才入队推送。

    v0.9.9 #201/#206：新增 diag 收集每道拦截原因，解决「识别了却没入队、
    用户完全看不到卡在哪」。不改海报闸门红线（无海报仍不入队），但把拦截
    量化到镜像管线输出，前端进度节点可直接显示。
    """
    d = diag if diag is not None else {}
    def bump(key: str, n: int = 1) -> None:
        d[key] = int(d.get(key, 0)) + n

    if x.identify_status != "identified":
        bump("skip_unidentified")
        return 0
    if not (x.poster_path or "").strip():
        bump("skip_no_poster")
        return 0
    added = 0
    # v0.9.5：推送白名单改为目录路由表——同一 Bot 按资源所在 B 库目录推到不同频道/群。
    # 路由表非空时以它为准（不同目录→不同目标）；为空时回退旧分发规则，保持升级兼容。
    from types import SimpleNamespace
    routes = bot_listener.push_routes(db)
    if routes:
        # v0.9.11：把 B 库根 / 扫描根 ID 传给匹配器——填了根目录 ID 的路由视为整库通配，
        # 修复真机「dir_id 是 B 库根、资源在季子目录、32 条全 skip_route_no_match」。
        matched = bot_listener.match_push_routes(db, x, root_ids=bot_listener.scan_root_ids(db))
        if not matched:
            bump("skip_route_no_match")
            return 0
        for route in matched:
            ch = _ensure_route_channel(db, route)
            if not ch:
                bump("skip_no_channel")
                continue
            pseudo_rule = SimpleNamespace(channel_id=ch.id, auto_publish=True)
            if _enqueue_ready(db, x, pseudo_rule):
                added += 1
            else:
                bump("skip_dedup_or_disabled")
        if added:
            bump("enqueued", added)
        return added
    # 没有任何启用规则 / 路由：明确记为「未配置推送目标」，不再静默
    rules = list(db.scalars(select(Rule).where(Rule.enabled.is_(True))).all())
    if not rules:
        bump("skip_no_target")
        return 0
    for rule in rules:
        if rule.directory_id and rule.directory_id != x.parent_id:
            bump("skip_rule_dir_mismatch")
            continue
        if rule.keyword and rule.keyword.lower() not in (x.name or "").lower():
            bump("skip_rule_keyword_mismatch")
            continue
        if _enqueue_ready(db, x, rule):
            added += 1
        else:
            bump("skip_dedup_or_disabled")
    if added:
        bump("enqueued", added)
    return added


def _recheck_gated_push(db: Session) -> int:
    """推送门禁补偿：改名完成后，把被门禁挡下、现已满足条件的作品重新入队。

    v2.1.3（真机 2026-11-22 黑豹2 事故）：`_enqueue_ready` 的门禁未过时**直接不入队**，
    而「单文件改名为英文」这件事只有镜像改名 job 会做——改名完成后若不补这一跳，
    作品就永久停在门禁之外（漏推）。本函数由 `_job_rename_pending` 在改名完成后调用，
    是门禁与改名链路之间的**闭环回边**。

    只走**旧链**入队（`_enqueue_after_identify`）：解耦开关（resource_decoupled_enabled）
    开启时推送由分享中心 v2 负责，其 `dispatch_ready_tasks` 在门禁未过时把任务留在
    `planned` 并设 `next_retry_at`，**每分钟自动重试**——这里再补事件反而有重复建任务、
    造成重复推送的风险，故直接跳过。

    判定口径与 `_enqueue_ready` 完全一致（作品级、同一作品同一频道只入一条）：
      - 已有 QueueItem 的作品 → 跳过（已推过，绝不重复推）；
      - 未识别 / 无海报 → 跳过（那是识别链的职责，不是改名链能解决的）；
      - 门禁未过 → 跳过（继续等下一轮改名）；
      - 门禁已过 → 重新入队。

    本函数**绝不抛异常**：调用方 `_job_rename_pending` 的 except 会把改名成功误报为
    失败。异常一律记日志后吞掉——补偿失败只是少推一次，下轮 120 秒还会再来。

    返回重新入队的作品数。
    """
    try:
        from .push_gate import evaluate_push_gate
        decoupled = str(runtime_config.get(db, "resource_decoupled_enabled") or "false").lower() in (
            "1", "true", "yes", "on")
        if decoupled:
            # 分享中心 v2 自带按分钟重试，这里不重复入队。
            return 0
        rows = list(db.scalars(select(Resource)).all())
        if not rows:
            return 0
        groups: dict[str, list[Resource]] = {}
        for res in rows:
            groups.setdefault(_work_key(res), []).append(res)
        # 已入队的作品一律跳过（QueueItem 存在即视为已进推送流程，含已取消的——
        # 用户主动取消过的不该被补偿重新推出去）。
        queued_ids = {int(i) for i in db.scalars(select(QueueItem.resource_id)).all() if i}
        enqueued = 0
        for wk, members in groups.items():
            if any(int(getattr(m, "id", 0) or 0) in queued_ids for m in members):
                continue
            # 代表资源：必须已识别且有海报（与 _enqueue_ready 的前置一致）。
            rep = None
            for m in members:
                if m.identify_status == "identified" and (m.poster_path or "").strip():
                    rep = m
                    break
            if rep is None:
                continue
            gate = evaluate_push_gate(db, wk, members)
            if not gate.get("passed"):
                continue
            try:
                added = _enqueue_after_identify(db, rep)
            except Exception as exc:
                logger.warning("[push-gate] 门禁补偿入队失败 work_key=%s：%s", wk, exc)
                continue
            if added:
                enqueued += 1
                queued_ids.update(int(getattr(m, "id", 0) or 0) for m in members)
        if enqueued:
            # 必须在这里 commit：调用方 `_job_rename_pending` 用的是
            # `with SessionLocal() as db:`，会话退出时 close() 会回滚未提交的写入，
            # 而 `_enqueue_ready` 只做 db.flush()。不提交则补偿等于没做。
            db.commit()
            logger.info("[push-gate] 改名后补偿入队 %s 部作品", enqueued)
        return enqueued
    except Exception as exc:
        logger.warning("[push-gate] 门禁补偿异常：%s", exc)
        return 0


def _clear_identification(x: Resource, status: str = "none", source: str = "manual",
                          note: str = "") -> None:
    """LitePan「无匹配」：清空元数据并标记，不再被自动识别误配。"""
    x.tmdb_id = None
    x.media_type = ""
    x.media_title = ""
    x.original_title = ""
    x.overview = ""
    x.poster_path = ""
    x.genres = ""
    x.origin_country = ""
    x.vote_average = 0.0
    x.identify_status = status
    x.identify_source = source
    x.identify_note = note


def _tmdb_error(exc: Exception) -> HTTPException:
    """TMDB 凭证问题返回 400（可操作的中文提示），其它才是 502。"""
    if isinstance(exc, tmdb.TmdbAuthError):
        return HTTPException(400, str(exc))
    return HTTPException(502, str(exc))


# ---------- TMDB 设置（管理页配置，无需改 .env / 重启） ----------
class TmdbConfigInput(BaseModel):
    api_key: str = ""
    bearer_token: str = ""
    language: str = ""
    # v0.9.8：按场景独立限流（bot 默认更保守，防 Bot 被刷导致 TMDB 账号异常）
    scan_min_interval: str = ""
    scan_max_calls: str = ""
    bot_min_interval: str = ""
    bot_max_calls: str = ""
    rate_window_seconds: str = ""


@app.get("/api/tmdb/status", dependencies=ADMIN)
def tmdb_status(db: Session = Depends(get_db)):
    out = tmdb.auth_status(db)
    out["rate_limit"] = {
        "scan_min_interval": runtime_config.get(db, "tmdb_scan_min_interval"),
        "scan_max_calls": runtime_config.get(db, "tmdb_scan_max_calls"),
        "bot_min_interval": runtime_config.get(db, "tmdb_bot_min_interval"),
        "bot_max_calls": runtime_config.get(db, "tmdb_bot_max_calls"),
        "rate_window_seconds": runtime_config.get(db, "tmdb_rate_window_seconds"),
    }
    return out


@app.post("/api/tmdb/config", dependencies=ADMIN)
def tmdb_config_save(p: TmdbConfigInput, db: Session = Depends(get_db)):
    values = {
        "tmdb_api_key": p.api_key.strip(),
        "tmdb_bearer_token": p.bearer_token.strip(),
        "tmdb_language": (p.language.strip() or "zh-CN"),
    }
    # 仅当用户填了值才覆盖限流配置（留空保持原值/默认）
    for field, key in (("scan_min_interval", "tmdb_scan_min_interval"),
                       ("scan_max_calls", "tmdb_scan_max_calls"),
                       ("bot_min_interval", "tmdb_bot_min_interval"),
                       ("bot_max_calls", "tmdb_bot_max_calls"),
                       ("rate_window_seconds", "tmdb_rate_window_seconds")):
        raw = getattr(p, field, "").strip()
        if raw:
            try:
                num = float(raw) if "interval" in field or "window" in field else int(raw)
                if num >= 0:
                    values[key] = str(int(num) if num == int(num) else num)
            except ValueError:
                pass  # 非法输入忽略，保持原值
    runtime_config.set_many(db, values)
    tmdb.reset_rate_limiters()  # 配置变更后清空滑动窗口，立即生效
    tgto.clear_tmdb_meta_cache()  # 阶段五 B-07：清空求片元数据缓存，新语言/密钥立即生效
    add_audit(db, "tmdb_config_saved", target_type="config", detail="TMDB 密钥/限流已更新（密文存储）")
    db.commit()
    return {"ok": True, "status": tmdb_status(db)}


@app.post("/api/tmdb/test", dependencies=ADMIN)
async def tmdb_test(db: Session = Depends(get_db)):
    return await tmdb.test_connection(db)


@app.get("/api/tmdb/candidates/{rid}", dependencies=ADMIN)
async def tmdb_candidates(rid: int, media_type: str = "", db: Session = Depends(get_db)):
    """手动匹配窗口打开时先带出候选；指定类型时只搜该类型（不回退）。

    v2.1.11：候选线索改用「映射回 A 库」的正确作品名 + TMDB 影视号 + 类型，
    不用改名后的 B 库名（B 库单集名是英文译名，搜不到）。
    """
    x = db.get(Resource, rid)
    if not x:
        raise HTTPException(404, "资源不存在")
    mt = media_type if media_type in ("movie", "tv") else None
    q_name, q_hint, q_tmdb, q_type = source_query_hint(x)
    try:
        if mt:
            hint = parse_media_name(q_name or x.name, q_hint)
            q = hint.get("primary_title") or hint.get("clean_title") or q_name or x.name
            items = await tmdb.search(q, mt, db)
            label = "电影" if mt == "movie" else "剧集"
            return {"ok": bool(items), "status": "identified" if items else "failed",
                    "source": "tmdb", "reason": f"按{label}搜索「{q}」得到 {len(items)} 个候选",
                    "hint": hint, "ai": None, "candidates": tmdb.rank(items, hint), "best": None}
        return await tmdb.identify(q_name or x.name, db, path_hint=q_hint,
                                   forced_tmdb_id=q_tmdb, forced_type=q_type)
    except Exception as e:
        raise _tmdb_error(e)


@app.get("/api/tmdb/search", dependencies=ADMIN)
async def tmdb_search(q: str, media_type: str = "", db: Session = Depends(get_db)):
    """手动匹配：片名或 TMDB ID，可限定 movie / tv。"""
    raw = (q or "").strip()
    if not raw:
        return []
    mt = media_type if media_type in ("movie", "tv") else None
    try:
        m = re.fullmatch(r"(?:tmdb[-_:= ]?)?(\d{2,12})", raw, re.I)
        if m:
            d, _ = await tmdb.detail_by_id_any(int(m.group(1)), mt or "movie", db)
            return [d] if d else []
        return await tmdb.search(raw, mt, db)
    except Exception as e:
        raise _tmdb_error(e)


@app.post("/api/resources/{rid}/identify", dependencies=ADMIN)
async def identify(rid: int, p: IdentifyInput, db: Session = Depends(get_db)):
    """手动「应用匹配」：按指定 TMDB ID + 类型拉详情并补全分类标签。"""
    if p.media_type not in ("movie", "tv"):
        raise HTTPException(400, "media_type 仅支持 movie / tv")
    x = db.get(Resource, rid)
    if not x:
        raise HTTPException(404, "资源不存在")
    try:
        d = await tmdb.detail(p.tmdb_id, p.media_type, db)
        _apply_identification(x, d, source="manual", note=p.note or "手动匹配",
                              hint=parse_media_name(x.name, x.path_hint or ""))
        queued = _after_identify_dispatch(db, x)  # 阶段二：灰度开关选新链/旧链
        add_audit(db, "resource_identified", target_type="resource", target_id=str(rid),
                  detail=json_dumps({"tmdb_id": p.tmdb_id, "media_type": p.media_type,
                                     "title": x.media_title, "source": "manual", "queued": queued}))
        db.commit()
        return {"ok": True, "media_title": x.media_title, "poster": tmdb.poster_url(x.poster_path),
                "type": x.media_type, "genres": tmdb.as_list(x.genres),
                "origin_country": tmdb.country_label(x.origin_country), "queued": queued}
    except Exception as e:
        raise _tmdb_error(e)


@app.post("/api/resources/{rid}/match-none", dependencies=ADMIN)
def match_none(rid: int, db: Session = Depends(get_db)):
    """LitePan 风格「无匹配」：标记后不再自动识别，等人工处理。"""
    x = db.get(Resource, rid)
    if not x:
        raise HTTPException(404, "资源不存在")
    _clear_identification(x, note="人工标记无匹配，不再自动识别")
    add_audit(db, "resource_match_none", target_type="resource", target_id=str(rid))
    db.commit()
    return {"ok": True}


@app.post("/api/resources/{rid}/auto-identify", dependencies=ADMIN)
async def auto_identify(rid: int, db: Session = Depends(get_db)):
    """自动识别：只有决策明确才落库；匹配存疑/失败时标记状态并给出候选。"""
    x = db.get(Resource, rid)
    if not x:
        raise HTTPException(404, "资源不存在")
    try:
        r = await _identify_with_source_hints(x.name, x, db)
        if r["ok"] and r.get("best"):
            _apply_identification(x, r["best"], source=r["source"], note=r["reason"], hint=r["hint"])
            queued = _after_identify_dispatch(db, x)  # 阶段二：灰度开关选新链/旧链
            add_audit(db, "resource_auto_identified", target_type="resource", target_id=str(rid),
                      detail=json_dumps({"tmdb_id": x.tmdb_id, "media_type": x.media_type,
                                         "source": r["source"], "reason": r["reason"], "queued": queued}))
            db.commit()
            return {"ok": True, "status": "identified", "candidates": r["candidates"][:8],
                    "applied": r["best"], "reason": r["reason"], "source": r["source"],
                    "hint": r["hint"], "ai": r.get("ai"), "queued": queued}
        x.identify_status = r["status"]
        x.identify_source = r["source"]
        x.identify_note = r["reason"]
        add_audit(db, "resource_identify_pending", target_type="resource", target_id=str(rid),
                  detail=json_dumps({"status": r["status"], "reason": r["reason"]}))
        db.commit()
        return {"ok": False, "status": r["status"], "reason": r["reason"],
                "candidates": r["candidates"][:8], "hint": r["hint"], "ai": r.get("ai")}
    except Exception as e:
        raise _tmdb_error(e)


@app.post("/api/resources/identify-all", dependencies=ADMIN)
async def identify_all(force: bool = False, limit: int = 50, db: Session = Depends(get_db)):
    """按目录名批量重识别。升级到 v0.6.1 后旧资源缺 path_hint，先重新扫描一次补全线索。

    force=true 会连已经识别过的资源一起重算（用于修正「寂静之噪」这类误配）。

    v2.1.12：批次选取改为「按作品分散 + 轮次推进」（`identify_batch.select_identify_batch`）。
    旧实现是 `order_by(id.desc()).limit(N)` 的固定窗口——真机上最新 50 条恰好全属
    同一部剧（封神英雄榜），窗口每次都被它的集数占满，其余 1215 条从未被覆盖，
    看起来就像「按目录识别没生效」。现在：
      ① 一批里按作品分散取（每部作品先出 1 条），窗口不被单部剧吃满；
      ② 每条记一个识别轮次号，跨轮自动推进到还没轮到过的那批；
      ③ 发请求前先看配额，超阈值就中止本轮，剩下的留到下一轮（不写成失败）。
    """
    limit = max(1, min(int(limit or 1), 200))
    rows, info = select_identify_batch(db, limit=limit, force=force,
                                       prefer_new=False)
    done = pending = queued = 0
    deferred = 0
    for x in rows:
        # v2.1.12：发请求前先满足 TMDB 的**最小调用间隔**。
        # 此前手动路径完全没等，于是上一条请求刚发出、下一条立刻打过去，
        # 客户端按间隔判定直接限流 → 真机上 40 条资源被一次性标成「待重试」，
        # 看起来像「这批剧全都识别不出来」。
        wait = tmdb.rate_limit_wait_seconds(db, tmdb.SCAN)
        if wait > 0:
            await asyncio.sleep(min(wait, IDENTIFY_RATE_LIMIT_WAIT_MAX))
        # v2.1.12：即将真正尝试识别 → 记一次参与轮次（成功/失败/限流一视同仁）。
        mark_attempted(x)
        try:
            # v2.1.11：批量重识别同样走「映射回 A 库」线索；手动触发**绕过缓存**。
            r = await _identify_with_source_hints(x.name, x, db)
            if r["ok"] and r.get("best"):
                # v2.1.12：字段一律用 .get() 取——上游少给一个键不该把
                # 「识别成功」变成「识别失败」（KeyError 会落进 except 分支）。
                _apply_identification(x, r["best"], source=r.get("source") or "tmdb",
                                      note=r.get("reason") or "", hint=r.get("hint"))
                queued += _after_identify_dispatch(db, x)  # 阶段二：灰度开关选新链/旧链
                done += 1
            else:
                x.identify_status = r.get("status") or "none"
                x.identify_source = r.get("source") or ""
                x.identify_note = r.get("reason") or ""
                pending += 1
        except Exception as exc:
            # v2.1.9：限流单独标记为「待重试」，不写死 failed（口径同 _post_scan_identify）。
            if tmdb.is_rate_limited(exc):
                x.identify_status = "rate_limited"
                x.identify_note = str(exc)[:500]
                pending += 1
                # v2.1.12：限流即**中止本轮**，不再继续往下打。
                # 此前是 continue —— 配额已耗尽还继续发请求，
                # 后面每一条都被判限流、整批被写成「待重试」，
                # 真正的原因（配额窗口没到）反而被淹没在结果里。
                # 中止后剩余条目保持原状，下一轮（有额度时）自动继续。
                deferred = len(rows) - (done + pending)
                break
            x.identify_status = "failed"
            x.identify_note = str(exc)[:500]
            pending += 1
    add_audit(db, "identify_all", target_type="resource",
              detail=json_dumps({"scanned": len(rows), "identified": done,
                                 "pending": pending, "force": force, "queued": queued,
                                 "works": int(info.get("works") or 0),
                                 "deferred": deferred}))
    db.commit()
    # 配额恢复还需要的秒数：前端「连续补跑」据此决定等多久再发起下一轮，
    # 而不是盲目重试把剩余资源全打成待重试。
    retry_after = round(tmdb.rate_limit_wait_seconds(db, tmdb.SCAN), 1) if deferred else 0.0
    return {"ok": True, "scanned": len(rows), "identified": done, "pending": pending,
            "queued": queued,
            # v2.1.12：如实回报批次口径——覆盖了几部作品、本轮因配额推迟了多少、
            # 库里还剩多少没认出来（前端要显示，避免用户误以为「已全库重识别」）。
            "works": int(info.get("works") or 0),
            "deferred": deferred,
            "remaining": count_pending(db),
            "retry_after": retry_after}


# ---------- v2.1.12：批量重识别（后台补跑全库） ----------
# 真机诉求：「想快速补跑全库：分批调高 limit 并对未识别资源按作品遍历，
# 同时把 tmdb_scan_max_calls 分批控制好，避免 again 触发限流。」
#
# 同步端点一次只跑一批（200 条），用户要自己反复点。这里做后台任务：
# 循环「取一批 → 识别 → 提交」，直到待识别存量清零或触发 TMDB 限流闸门。
# 关键点：
#   ① 每批仍走 select_identify_batch（按作品分散 + 轮次推进），
#      跨批自动覆盖到没轮到过的那批，不会反复命中同一部剧；
#   ② 限流不是失败：等 rate_limit_wait_seconds 给出的窗口再继续，
#      被推迟的条目保持原状，下一批自然轮到它们；
#   ③ 全程独立 Session + 每批提交，绝不跨 await 持写事务。
_identify_all_state: dict = {"running": False, "started_at": None, "finished_at": None,
                              "rounds": 0, "scanned": 0, "identified": 0,
                              "pending": 0, "queued": 0, "works": 0,
                              "remaining": 0, "stage": "", "error": ""}
_identify_all_task: asyncio.Task | None = None

# 每批条数：一次请求的合理上限（TMDB 单次搜索也要走配额，批太大反而容易撞限流）
IDENTIFY_ALL_BATCH = 200
# 单轮任务最多跑多少批（安全阀，避免极端情况下无限跑）
IDENTIFY_ALL_MAX_ROUNDS = 200
# 限流时最长等待多少秒再继续（超出则本轮收工，剩余交给下一次触发）
IDENTIFY_ALL_MAX_WAIT = 60.0


async def _run_identify_all_async(force: bool) -> None:
    """后台循环补跑：按作品分散分批识别，直到存量清零或限流闸门拦下。"""
    global _identify_all_task
    try:
        with SessionLocal() as db:
            _identify_all_state["stage"] = "识别中"
            while _identify_all_state["rounds"] < IDENTIFY_ALL_MAX_ROUNDS:
                # 限流闸门：配额不够就先等窗口，避免继续发请求把资源打成「待重试」
                wait = tmdb.rate_limit_wait_seconds(db, tmdb.SCAN)
                if wait > IDENTIFY_ALL_MAX_WAIT:
                    _identify_all_state["stage"] = (
                        f"访问频次受限，约 {int(wait)} 秒后可继续（已处理部分保留）")
                    break
                if wait > 0:
                    _identify_all_state["stage"] = f"等待访问间隔 {wait:.1f} 秒"
                    await asyncio.sleep(min(wait, IDENTIFY_ALL_MAX_WAIT))
                rows, info = select_identify_batch(db, limit=IDENTIFY_ALL_BATCH,
                                                   force=force, prefer_new=False)
                if not rows:
                    _identify_all_state["stage"] = "没有待识别的资源"
                    break
                done = pending = queued = 0
                stopped = False
                for x in rows:
                    w = tmdb.rate_limit_wait_seconds(db, tmdb.SCAN)
                    if w > IDENTIFY_ALL_MAX_WAIT:
                        stopped = True
                        break
                    if w > 0:
                        await asyncio.sleep(min(w, IDENTIFY_ALL_MAX_WAIT))
                    mark_attempted(x)
                    try:
                        r = await _identify_with_source_hints(x.name, x, db)
                        if r.get("ok") and r.get("best"):
                            _apply_identification(x, r["best"],
                                                  source=r.get("source") or "tmdb",
                                                  note=r.get("reason") or "",
                                                  hint=r.get("hint"))
                            queued += _after_identify_dispatch(db, x)
                            done += 1
                        else:
                            x.identify_status = r.get("status") or "none"
                            x.identify_source = r.get("source") or ""
                            x.identify_note = r.get("reason") or ""
                            pending += 1
                    except Exception as exc:
                        if tmdb.is_rate_limited(exc):
                            x.identify_status = "rate_limited"
                            x.identify_note = str(exc)[:300]
                            pending += 1
                            stopped = True
                        else:
                            x.identify_status = "failed"
                            x.identify_note = str(exc)[:300]
                            pending += 1
                # 每批独立提交：绝不跨 await 持写事务，也不怕中途崩掉丢进度
                db.commit()
                st = _identify_all_state
                st["rounds"] += 1
                st["scanned"] += len(rows)
                st["identified"] += done
                st["pending"] += pending
                st["queued"] += queued
                st["works"] = int(info.get("works") or 0)
                st["remaining"] = count_pending(db)
                st["stage"] = f"第 {st['rounds']} 批完成（覆盖 {st['works']} 部作品）"
                if stopped:
                    w = tmdb.rate_limit_wait_seconds(db, tmdb.SCAN)
                    if w > IDENTIFY_ALL_MAX_WAIT:
                        st["stage"] = (
                            f"访问频次受限，约 {int(w)} 秒后可继续（已处理部分保留）")
                        break
                if st["remaining"] <= 0:
                    st["stage"] = "全部识别完成"
                    break
    except Exception as exc:
        logger.exception("[identify-all] 后台补跑失败")
        _identify_all_state["error"] = f"{type(exc).__name__}: {exc}"[:1000]
        _identify_all_state["stage"] = "失败"
    finally:
        _identify_all_state["running"] = False
        _identify_all_state["finished_at"] = utcnow().isoformat()
        _identify_all_task = None


@app.post("/api/resources/identify-all/async", dependencies=ADMIN)
async def identify_all_async(force: bool = True):
    """启动后台批量重识别，立即返回；前端轮询 /api/resources/identify-all/async。"""
    global _identify_all_task
    if _identify_all_state.get("running"):
        raise HTTPException(409, "已有批量重识别任务在运行，请等待完成")
    _identify_all_state.update({
        "running": True, "started_at": utcnow().isoformat(), "finished_at": None,
        "rounds": 0, "scanned": 0, "identified": 0, "pending": 0, "queued": 0,
        "works": 0, "remaining": 0, "stage": "启动", "error": ""})
    _identify_all_task = asyncio.create_task(_run_identify_all_async(force))
    return {"started": True}


@app.get("/api/resources/identify-all/async", dependencies=ADMIN)
def identify_all_async_status():
    """轮询后台批量重识别状态。"""
    return dict(_identify_all_state)


# ---------- AI 辅助识别（OpenAI 兼容接口，可选） ----------
class AiConfigInput(BaseModel):
    enabled: bool = False
    base_url: str = ""
    api_key: str = ""
    model: str = ""
    prompt: str = ""
    timeout: int = 30


class AiParseInput(BaseModel):
    name: str = ""


@app.get("/api/ai/status", dependencies=ADMIN)
def ai_status(db: Session = Depends(get_db)):
    return ai_assist.status(db)


@app.post("/api/ai/config", dependencies=ADMIN)
def ai_config_save(p: AiConfigInput, db: Session = Depends(get_db)):
    runtime_config.set_many(db, {
        "ai_enabled": "true" if p.enabled else "false",
        "ai_base_url": p.base_url.strip() or "https://api.openai.com/v1",
        "ai_api_key": p.api_key.strip(),
        "ai_model": p.model.strip() or "gpt-4o-mini",
        "ai_prompt": p.prompt.strip(),
        "ai_timeout": str(max(5, min(120, p.timeout))),
    })
    add_audit(db, "ai_config_saved", target_type="config", detail="AI 辅助识别配置已更新（密文存储）")
    db.commit()
    return {"ok": True, "status": ai_assist.status(db)}


@app.post("/api/ai/test", dependencies=ADMIN)
async def ai_test(db: Session = Depends(get_db)):
    return await ai_assist.test_connection(db)


@app.post("/api/ai/parse", dependencies=ADMIN)
async def ai_parse(p: AiParseInput, db: Session = Depends(get_db)):
    """识别测试：让 AI 解析一个文件名，不写库。未启用时返回提示而非报错。"""
    if not p.name.strip():
        return {"ok": False, "message": "请输入文件名"}
    if not ai_assist.enabled(db):
        return {"ok": False, "message": "AI 辅助识别未启用或尚未配置 API Key"}
    try:
        return await ai_assist.parse_filename(p.name.strip(), db)
    except Exception as exc:
        return {"ok": False, "message": str(exc)}


# ---------- Bot / 频道 / 规则 ----------
class BotInput(BaseModel):
    name: str
    token: str
    enabled: bool = True


def _mask_stored_token(stored: str) -> str:
    try:
        return mask_secret(decrypt_secret(stored))
    except Exception:
        return mask_secret(stored)


@app.get("/api/bots", dependencies=ADMIN)
def bots(db: Session = Depends(get_db)):
    return [{"id": x.id, "name": x.name, "enabled": x.enabled,
             "token_masked": _mask_stored_token(x.token)}
            for x in db.scalars(select(Bot)).all()]


@app.post("/api/bots", dependencies=ADMIN)
def add_bot(p: BotInput, db: Session = Depends(get_db)):
    try:
        token_enc = encrypt_secret(p.token)
    except RuntimeError as e:
        raise HTTPException(400, str(e))
    x = Bot(name=p.name, token=token_enc, enabled=p.enabled)
    db.add(x)
    add_audit(db, "bot_added", target_type="bot", detail=p.name)
    db.commit()
    db.refresh(x)
    return {"id": x.id, "name": x.name}


async def _telegram_call(token: str, method: str, payload: dict,
                         db: Session | None = None) -> dict:
    """直接调 Telegram Bot API，不写队列、不建分享。异常由调用方转成结构化提示。"""
    base = (runtime_config.telegram_base(db) if db is not None
            else (settings.telegram_api_base or "https://api.telegram.org").rstrip("/"))
    url = f"{base}/bot{token}/{method}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(url, json=payload)
    except httpx.TimeoutException as exc:
        raise RuntimeError(
            f"连接 Telegram 超时（{base}）。若在国内网络，请在「分发规则」页把 "
            "Telegram API 地址改成可用的反向代理后重试") from exc
    except Exception as exc:
        raise RuntimeError(f"无法连接 Telegram（{base}）：{exc}") from exc
    try:
        data = r.json()
    except Exception:
        data = {"ok": False, "description": r.text[:300]}
    return {"http_status": r.status_code, "data": data,
            "retry_after": r.headers.get("Retry-After")}


def _cascade_channel(db: Session, channel_id: int) -> tuple[int, int]:
    """删除频道前清理引用它的分发规则与队列项，返回清理数量供前端确认框展示。"""
    rules = list(db.scalars(select(Rule).where(Rule.channel_id == channel_id)).all())
    queue = list(db.scalars(select(QueueItem).where(QueueItem.channel_id == channel_id)).all())
    for x in rules:
        db.delete(x)
    for x in queue:
        db.delete(x)
    return len(rules), len(queue)


@app.post("/api/bots/{bid}/test", dependencies=ADMIN)
async def test_bot(bid: int, db: Session = Depends(get_db)):
    """验证 Bot Token 是否有效（Telegram getMe），不发消息。"""
    b = db.get(Bot, bid)
    if not b:
        raise HTTPException(404, "Bot 不存在")
    try:
        token = decrypt_secret(b.token)
    except Exception as exc:
        return {"ok": False, "message": f"Bot Token 解密失败：{exc}"}
    if not token:
        return {"ok": False, "message": "Bot Token 为空，请重新添加"}
    try:
        r = await _telegram_call(token, "getMe", {}, db)
    except Exception as exc:
        return {"ok": False, "message": str(exc)}
    if r["http_status"] in (401, 403) or not r["data"].get("ok"):
        return {"ok": False, "message": "Bot Token 无效或已被撤销（Telegram 401/403），请重新添加 Bot"}
    u = r["data"].get("result") or {}
    add_audit(db, "bot_tested", target_type="bot", target_id=str(bid), detail=str(u.get("username", "")))
    db.commit()
    return {"ok": True, "username": u.get("username", ""), "name": u.get("first_name", ""),
            "message": (f"Bot Token 有效：@{u.get('username') or u.get('first_name') or bid}。"
                        "验证只检查 Token、不会发消息；要收到测试消息请点「测试推送」")}


@app.delete("/api/bots/{bid}", dependencies=ADMIN)
def delete_bot(bid: int, db: Session = Depends(get_db)):
    """删除 Bot，并级联清理其下频道、关联规则与队列项。"""
    b = db.get(Bot, bid)
    if not b:
        raise HTTPException(404, "Bot 不存在")
    channels_ = list(db.scalars(select(Channel).where(Channel.bot_id == bid)).all())
    removed_rules = removed_queue = 0
    for ch in channels_:
        nr, nq = _cascade_channel(db, ch.id)
        removed_rules += nr
        removed_queue += nq
        db.delete(ch)
    name = b.name
    db.delete(b)
    add_audit(db, "bot_deleted", target_type="bot", target_id=str(bid),
              detail=json_dumps({"name": name, "removed_channels": len(channels_),
                                 "removed_rules": removed_rules, "removed_queue": removed_queue}))
    db.commit()
    return {"ok": True, "removed_channels": len(channels_),
            "removed_rules": removed_rules, "removed_queue": removed_queue}


class ChannelInput(BaseModel):
    bot_id: int
    name: str
    chat_id: str
    enabled: bool = True
    kind: str = "channel"   # v0.9.4：channel/group，仅菜单分类展示用


@app.get("/api/channels", dependencies=ADMIN)
def channels(db: Session = Depends(get_db)):
    return [{"id": x.id, "bot_id": x.bot_id, "name": x.name, "chat_id": x.chat_id, "enabled": x.enabled,
             "kind": getattr(x, "kind", "channel") or "channel"}
            for x in db.scalars(select(Channel)).all()]


@app.post("/api/channels", dependencies=ADMIN)
def add_channel(p: ChannelInput, db: Session = Depends(get_db)):
    if not db.get(Bot, p.bot_id):
        raise HTTPException(404, "Bot 不存在")
    data = p.model_dump()
    data["kind"] = "group" if str(data.get("kind") or "").lower() == "group" else "channel"
    x = Channel(**data)
    db.add(x)
    add_audit(db, "channel_added", target_type="channel", detail=p.name)
    db.commit()
    db.refresh(x)
    return {"id": x.id}


@app.post("/api/channels/{cid}/test", dependencies=ADMIN)
async def test_channel(cid: int, db: Session = Depends(get_db)):
    """向该频道发一条真实测试消息，验证 Bot Token + 频道 ID + 网络是否联通。"""
    ch = db.get(Channel, cid)
    if not ch:
        raise HTTPException(404, "频道不存在")
    bot = db.get(Bot, ch.bot_id)
    if not bot:
        return {"ok": False, "message": "关联 Bot 不存在，请重新添加"}
    if not bot.enabled:
        return {"ok": False, "message": "关联 Bot 已禁用，请先启用后再测试"}
    if not ch.enabled:
        return {"ok": False, "message": "该频道已禁用，请先启用后再测试"}
    try:
        token = decrypt_secret(bot.token)
    except Exception as exc:
        return {"ok": False, "message": f"Bot Token 解密失败：{exc}"}
    if not token:
        return {"ok": False, "message": "Bot Token 为空，请重新添加 Bot"}
    now_bj = (utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
    text = (f"<b>Ya-TG 测试推送</b>\n"
            f"频道：{html.escape(ch.name, quote=False)}\n"
            f"Bot：{html.escape(bot.name, quote=False)}\n"
            f"时间：{now_bj}（北京时间）\n\n"
            "收到这条消息，说明 Bot Token、频道 ID 与网络均正常，可以正式分发。")
    try:
        r = await _telegram_call(token, "sendMessage",
                                 {"chat_id": ch.chat_id, "text": text, "parse_mode": "HTML",
                                  "disable_web_page_preview": True}, db)
    except Exception as exc:
        return {"ok": False, "message": str(exc)}
    if r["http_status"] == 429:
        wait = retry_after_seconds(r["retry_after"]) or 60
        return {"ok": False, "message": f"Telegram 限流（429），请约 {wait} 秒后重试"}
    if r["http_status"] >= 400 or not r["data"].get("ok"):
        desc = str(r["data"].get("description") or r["data"])[:300]
        if "chat not found" in desc.lower() or "chat_id" in desc.lower() or "chat" in desc.lower():
            desc += "（请检查频道 ID 是否正确、Bot 是否已加入频道并有发送权限）"
        elif "unauthorized" in desc.lower() or r["http_status"] in (401, 403):
            desc += "（Bot Token 可能无效，请先点 Bot 的「验证」）"
        add_audit(db, "channel_test_failed", target_type="channel", target_id=str(cid), detail=desc[:300])
        db.commit()
        return {"ok": False, "message": f"测试推送失败：{desc}"}
    message_id = str((r["data"].get("result") or {}).get("message_id", ""))
    add_audit(db, "channel_test_sent", target_type="channel", target_id=str(cid), detail=message_id)
    db.commit()
    return {"ok": True, "message_id": message_id, "message": "测试消息已发送，请到频道查看"}


@app.delete("/api/channels/{cid}", dependencies=ADMIN)
def delete_channel(cid: int, db: Session = Depends(get_db)):
    """删除频道，并级联清理引用它的分发规则与队列项。"""
    ch = db.get(Channel, cid)
    if not ch:
        raise HTTPException(404, "频道不存在")
    removed_rules, removed_queue = _cascade_channel(db, cid)
    name = ch.name
    db.delete(ch)
    add_audit(db, "channel_deleted", target_type="channel", target_id=str(cid),
              detail=json_dumps({"name": name, "removed_rules": removed_rules,
                                 "removed_queue": removed_queue}))
    db.commit()
    return {"ok": True, "removed_rules": removed_rules, "removed_queue": removed_queue}


class RuleInput(BaseModel):
    name: str
    directory_id: str = ""
    keyword: str = ""
    channel_id: int | None = None  # 允许缺省，后端给中文提示而不是 422 对象错误
    auto_publish: bool = True     # 默认自动发布；关闭时队列项停在「人工审核」
    enabled: bool = True


@app.get("/api/rules", dependencies=ADMIN)
def rules(db: Session = Depends(get_db)):
    return [{"id": x.id, "name": x.name, "directory_id": x.directory_id, "keyword": x.keyword,
             "channel_id": x.channel_id, "auto_publish": x.auto_publish, "enabled": x.enabled}
            for x in db.scalars(select(Rule)).all()]


@app.post("/api/rules", dependencies=ADMIN)
def add_rule(p: RuleInput, db: Session = Depends(get_db)):
    if not p.channel_id:
        raise HTTPException(400, "请先添加频道并选择「目标频道」")
    if not db.get(Channel, p.channel_id):
        raise HTTPException(404, "频道不存在，请刷新页面重新选择")
    x = Rule(**p.model_dump())
    db.add(x)
    db.flush()
    queued = _enqueue_existing(db, x)
    add_audit(db, "rule_added", target_type="rule",
              detail=json_dumps({"name": p.name, "channel_id": p.channel_id,
                                 "auto_publish": p.auto_publish, "queued": queued}))
    db.commit()
    mode = "自动发布" if p.auto_publish else "人工审核（需到审核中心逐条通过）"
    return {"id": x.id, "queued": queued,
            "message": f"规则已保存（{mode}），已把 {queued} 个已扫描资源补入分发队列"}


@app.delete("/api/rules/{rid}", dependencies=ADMIN)
def delete_rule(rid: int, db: Session = Depends(get_db)):
    r = db.get(Rule, rid)
    if not r:
        raise HTTPException(404, "分发规则不存在")
    name = r.name
    db.delete(r)
    add_audit(db, "rule_deleted", target_type="rule", target_id=str(rid), detail=name)
    db.commit()
    return {"ok": True}


# ---------- 队列与审核 ----------
@app.get("/api/queue", dependencies=ADMIN)
def queue(status: str = "", limit: int = 100, db: Session = Depends(get_db)):
    stmt = select(QueueItem).order_by(QueueItem.id.desc()).limit(min(limit, 500))
    if status:
        stmt = stmt.where(QueueItem.status == status)
    qs = list(db.scalars(stmt).all())
    # 阶段三 A-02：批量取资源/频道，消除逐条 db.get 的 1+2N 查询
    res_map = {x.id: x for x in db.scalars(select(Resource).where(
        Resource.id.in_([q.resource_id for q in qs]))).all()}
    ch_map = {x.id: x for x in db.scalars(select(Channel).where(
        Channel.id.in_([q.channel_id for q in qs]))).all()}
    out = []
    for q in qs:
        r, c = res_map.get(q.resource_id), ch_map.get(q.channel_id)
        out.append({"id": q.id, "resource": r.name if r else "", "channel": c.name if c else "",
                    "status": q.status, "attempts": q.attempts, "error": q.error,
                    "next_retry_at": q.next_retry_at.isoformat() if q.next_retry_at else None})
    return out


@app.get("/api/review", dependencies=ADMIN)
def review(db: Session = Depends(get_db)):
    qs = list(db.scalars(select(QueueItem).where(QueueItem.status == "review")).all())
    # 阶段三 A-03：批量取资源/频道，消除逐条 db.get
    res_map = {x.id: x for x in db.scalars(select(Resource).where(
        Resource.id.in_([q.resource_id for q in qs]))).all()}
    ch_map = {x.id: x for x in db.scalars(select(Channel).where(
        Channel.id.in_([q.channel_id for q in qs]))).all()}
    out = []
    for q in qs:
        r, c = res_map.get(q.resource_id), ch_map.get(q.channel_id)
        out.append({"id": q.id, "resource": r.name if r else "", "channel_id": q.channel_id,
                    "channel": c.name if c else "", "identified": r.identify_status if r else ""})
    return out


@app.post("/api/review/{qid}/approve", dependencies=ADMIN)
def approve(qid: int, db: Session = Depends(get_db)):
    q = db.get(QueueItem, qid)
    if not q or q.status != "review":
        raise HTTPException(404, "审核项不存在")
    q.status, q.attempts, q.error, q.next_retry_at = "pending", 0, "", None
    add_audit(db, "review_approved", target_type="queue", target_id=str(qid))
    db.commit()
    return {"ok": True}


@app.post("/api/review/{qid}/reject", dependencies=ADMIN)
def reject(qid: int, db: Session = Depends(get_db)):
    q = db.get(QueueItem, qid)
    if not q or q.status != "review":
        raise HTTPException(404, "审核项不存在")
    q.status = "rejected"
    add_audit(db, "review_rejected", target_type="queue", target_id=str(qid))
    db.commit()
    return {"ok": True}


@app.post("/api/queue/{qid}/retry", dependencies=ADMIN)
def retry(qid: int, db: Session = Depends(get_db)):
    q = db.get(QueueItem, qid)
    if not q:
        raise HTTPException(404, "队列项不存在")
    q.status, q.attempts, q.error, q.next_retry_at = "pending", 0, "", None
    # v1.5.1 P0：分享中心项(share_task_id>0)若 status_ext 为空/accepted/paused
    # 等不可消费态，手动 retry 必须把它重新置回 submitted 才能进交接消费链——
    # 否则真机 q82/83/84 retry 后 ext 仍空、永远不被 worker 领取（用户实测无效）。
    if q.share_task_id:
        q.status_ext = "submitted"
        q.accept_worker = ""
        q.accepted_at = None
    add_audit(db, "queue_retried", target_type="queue", target_id=str(qid))
    db.commit()
    return {"ok": True}


# ---------- 消息模板 / 审计 ----------
class TemplateInput(BaseModel):
    name: str
    content: str
    enabled: bool = True
    is_default: bool = False


@app.get("/api/templates/presets", dependencies=ADMIN)
def template_presets():
    """v0.9.6：内置模板预设列表（不落库），供管理页下拉一键载入后再改 / 另存。"""
    return [{"name": p["name"], "content": p["content"]} for p in TEMPLATE_PRESETS]


@app.get("/api/templates", dependencies=ADMIN)
def templates(db: Session = Depends(get_db)):
    # v2.1.22：带上 builtin 标记——前端据此把内置「默认图文」标成不可删除，
    # 用户自定义模板才允许删。此前没有该字段，前端只能"猜"哪条是内置。
    rows = list(db.scalars(select(MessageTemplate).order_by(MessageTemplate.id)).all())
    # v2.1.23：**读时也归一**。写路径（新增 / 编辑 / 恢复 / 启动迁移）都已经收敛，
    # 但历史库里仍可能残留多条 is_default（旧版本写入、手工改库、并发写）。
    # 只要接口把两条都报成 True，界面上就是两个「默认」标签 —— 用户的验收口径是
    # 「只能出现一条默认设置」，所以这里按与写路径**同一套规则**兜底挑出唯一 keeper：
    # 只给它报 True，并把库里多余的顺手纠正（已经合规时不写库）。
    keeper = _pick_default_keeper(rows)
    if keeper is not None and sum(1 for x in rows if x.is_default) != 1:
        for x in rows:
            x.is_default = (x.id == keeper.id)
        try:
            db.commit()
        except Exception as exc:      # 写不进去也不能让"看模板列表"整个 500
            db.rollback()
            logger.warning("[template] 纠正多条默认模板失败（本次仍按唯一默认返回）：%s",
                           str(exc)[:200])
    return [{"id": x.id, "name": x.name, "content": x.content, "enabled": x.enabled,
             "is_default": bool(keeper is not None and x.id == keeper.id),
             "builtin": bool(x.builtin)}
            for x in rows]


def _pick_default_keeper(rows: list[MessageTemplate]) -> MessageTemplate | None:
    """从模板列表里挑出「唯一默认」该归谁——读路径与写路径共用这一套规则。

    规则（与 v2.1.22 写路径一致，不要各写一份）：
      恰好一条默认 → 就是它（用户把自定义模板设为默认是合法状态，不抢回内置）；
      多条默认     → 优先内置那条，其次最早的一条；
      一条都没有   → 优先内置，其次第一条启用的，最后第一条。
    """
    if not rows:
        return None
    flagged = [x for x in rows if x.is_default]
    if len(flagged) == 1:
        return flagged[0]
    if flagged:
        return next((x for x in flagged if x.builtin), flagged[0])
    return (next((x for x in rows if x.builtin), None)
            or next((x for x in rows if x.enabled), rows[0]))


def _ensure_single_default_template(db: Session) -> int:
    """保证「恰好一条默认模板」——用户口径：默认配置只能配置一条（v2.1.22）。

    任何写操作（新增 / 编辑 / 恢复内置 / 删除）之后都调用：把 is_default 归一到一条，
    优先保留已是默认的那条（多条时优先内置），一条都没有就把内置的设为默认。
    这样 publisher._template() 永远不会落到「零条默认→随手取一条启用模板」的歧义上，
    也不会出现界面上两个「默认」标签。
    返回被清掉的默认条数（0 表示本来就合规）。
    """
    db.flush()
    rows = list(db.scalars(select(MessageTemplate).order_by(MessageTemplate.id)).all())
    if not rows:
        return 0
    flagged = [x for x in rows if x.is_default]
    if len(flagged) == 1:
        return 0
    # v2.1.23：挑 keeper 的规则外移到 `_pick_default_keeper`，与读路径共用一份，
    # 避免「接口报一条、库里改另一条」这种两套规则打起来的情况。
    keeper = _pick_default_keeper(rows)
    cleared = 0
    for x in rows:
        want = (keeper is not None and x.id == keeper.id)
        if bool(x.is_default) != want:
            if x.is_default and not want:
                cleared += 1
            x.is_default = want
    return cleared


@app.post("/api/templates", dependencies=ADMIN)
def add_template(p: TemplateInput, db: Session = Depends(get_db)):
    if p.is_default:
        for x in db.scalars(select(MessageTemplate).where(MessageTemplate.is_default.is_(True))).all():
            x.is_default = False
    # v2.1.22：用户新建的一律是自定义模板（builtin 只能由内置恢复端点产生）。
    x = MessageTemplate(**p.model_dump(), builtin=False)
    db.add(x)
    add_audit(db, "template_saved", target_type="template", detail=p.name)
    _ensure_single_default_template(db)
    db.commit()
    return {"id": x.id}


@app.post("/api/templates/default", dependencies=ADMIN)
def reset_default_template(db: Session = Depends(get_db)):
    """一键恢复内置「默认图文」并设为默认（v2.1.22 版式）。"""
    # v0.9.8：first() 防止历史库多条同名时 scalar() 抛 MultipleResultsFound（500）
    x = db.scalars(select(MessageTemplate).where(MessageTemplate.name == "默认图文模板")).first()
    if not x:
        x = MessageTemplate(name="默认图文模板", content=DEFAULT_TEMPLATE,
                            enabled=True, is_default=True, builtin=True)
        db.add(x)
    else:
        x.content, x.enabled, x.is_default, x.builtin = DEFAULT_TEMPLATE, True, True, True
    db.flush()
    for y in db.scalars(select(MessageTemplate).where(MessageTemplate.id != x.id)).all():
        y.is_default = False
    add_audit(db, "template_default_restored", target_type="template")
    db.commit()
    return {"ok": True, "content": DEFAULT_TEMPLATE, "id": x.id}


@app.put("/api/templates/{tid}", dependencies=ADMIN)
def edit_template(tid: int, p: TemplateInput, db: Session = Depends(get_db)):
    x = db.get(MessageTemplate, tid)
    if not x:
        raise HTTPException(404, "模板不存在")
    if p.is_default:
        for y in db.scalars(select(MessageTemplate).where(MessageTemplate.is_default.is_(True))).all():
            y.is_default = False
    for k, v in p.model_dump().items():
        setattr(x, k, v)
    # v2.1.22：编辑后同样强制「恰好一条默认」（用户可能把唯一的默认取消勾选）。
    _ensure_single_default_template(db)
    add_audit(db, "template_updated", target_type="template", target_id=str(tid))
    db.commit()
    return {"ok": True}


@app.delete("/api/templates/{tid}", dependencies=ADMIN)
def delete_template(tid: int, db: Session = Depends(get_db)):
    """v0.9.8：删除模板；删掉默认模板后自动补位，保证 publisher 始终有模板可用。

    旧版直接禁删默认模板（400），真机上历史库存在多条默认/同名模板时，点删除偶发
    Internal Server Error。这里统一在事务内完成「删除→补默认」，任何异常转成明确
    中文 HTTPException，绝不让底层异常裸抛成 500。

    v2.1.22：内置「默认图文」改为**不可删除**（用户口径：内置只保留默认图文，
    默认配置只能一条）。删了它系统就没有可恢复的默认版式了——要换样式请改内容或
    另存自定义模板；要还原点「载入默认模板」。用户自定义模板照旧可删。
    """
    try:
        x = db.get(MessageTemplate, tid)
        if not x:
            raise HTTPException(404, "模板不存在")
        if x.builtin:
            raise HTTPException(
                400, "内置「默认图文」不可删除（系统要靠它兜底渲染）。"
                     "需要换样式请直接改内容，或另存为自定义模板；要还原点「载入默认模板」。")
        was_default = bool(x.is_default)
        db.delete(x)
        db.flush()
        # 删除后必须还有一条启用模板，且恰好有一条默认模板
        remain = db.scalars(select(MessageTemplate).where(MessageTemplate.enabled.is_(True))).all()
        defaults = [r for r in remain if r.is_default]
        if was_default or not defaults:
            if remain:
                # v2.1.22：补位优先内置那条，其次原本的默认，最后第一条
                keeper = (next((r for r in remain if r.builtin), None)
                          or (defaults[0] if defaults else remain[0]))
                for r in remain:
                    r.is_default = (r.id == keeper.id)
            else:
                # 最后一条模板也被删：重建内置默认，避免 publisher 无模板可用
                db.add(MessageTemplate(name="默认图文模板", content=DEFAULT_TEMPLATE,
                                       enabled=True, is_default=True, builtin=True))
        add_audit(db, "template_deleted", target_type="template", target_id=str(tid))
        db.commit()
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as exc:
        db.rollback()
        raise HTTPException(400, f"删除模板失败：{str(exc)[:120]}")


@app.get("/api/audit", dependencies=ADMIN)
def audit(limit: int = 200, db: Session = Depends(get_db)):
    rows = db.scalars(select(AuditLog).order_by(AuditLog.id.desc()).limit(min(limit, 1000))).all()
    return [{"id": x.id, "ts": x.ts.isoformat() if x.ts else "", "action": x.action,
             "target_type": x.target_type, "target_id": x.target_id, "detail": x.detail, "actor": x.actor}
            for x in rows]


# ---------- v0.8.3：日志保留策略与清理 ----------
class LogConfigInput(BaseModel):
    retention_days: int = 30
    max_rows: int = 5000
    auto_prune: bool = True


class LogClearInput(BaseModel):
    confirm: bool = False


@app.get("/api/logs/overview", dependencies=ADMIN)
def logs_overview(db: Session = Depends(get_db)):
    return log_retention.overview(db)


@app.post("/api/logs/config", dependencies=ADMIN)
def logs_config_save(p: LogConfigInput, db: Session = Depends(get_db)):
    cfg = log_retention.save_config(db, retention_days=p.retention_days,
                                    max_rows=p.max_rows, auto_prune=p.auto_prune)
    add_audit(db, "logs_config_updated", target_type="logs",
              detail=json_dumps({"retention_days": cfg["retention_days"],
                                 "max_rows": cfg["max_rows"],
                                 "auto_prune": cfg["auto_prune"]}))
    db.commit()
    return {"ok": True, "config": cfg}


@app.post("/api/logs/prune", dependencies=ADMIN)
def logs_prune(db: Session = Depends(get_db)):
    out = log_retention.prune(db, mode="prune")
    add_audit(db, "logs_pruned", target_type="logs",
              detail=json_dumps({"deleted": out["deleted"], "days": out["days"],
                                 "max_rows": out["max_rows"]}))
    db.commit()
    return out


@app.post("/api/logs/clear", dependencies=ADMIN)
def logs_clear(p: LogClearInput, db: Session = Depends(get_db)):
    if not p.confirm:
        raise HTTPException(400, "清空日志需要显式确认 confirm=true")
    out = log_retention.prune(db, mode="clear")
    add_audit(db, "logs_cleared", target_type="logs",
              detail=json_dumps({"deleted": out["deleted"]}))
    db.commit()
    return out


# ---------- v0.6.1：分发链路修复 ----------
class ToggleInput(BaseModel):
    enabled: bool


class TestMessageInput(BaseModel):
    chat_id: str = ""


class BroadcastInput(BaseModel):
    text: str = ""
    chat_ids: list[str] | None = None   # 不传=全部推送目标（频道/群），永不私聊用户


class TelegramConfigInput(BaseModel):
    base_url: str = ""


class QueueScheduleInput(BaseModel):
    interval_seconds: int = 30
    cron: str = ""
    send_delay: int = 2   # v0.6.5：每条消息之间的节流秒数，避免 TG 风控


class TranslateSearchInput(BaseModel):
    title: str
    media_type: str = ""
    year: str = ""


def _enqueue_existing(db: Session, rule: Rule) -> int:
    """保存规则后回溯已入库资源。

    用户常「先扫描、再识别、最后才加规则」，识别时还没有规则、这些资源没有队列项。
    v0.6.5：回溯同样遵守「已识别 + 有海报 + 同一作品同一频道只入一条」，
    绝不把未识别的单集也补进去，否则扫描→识别→加规则后仍会一集一集推。
    """
    rows = list(db.scalars(select(Resource)).all())
    added = 0
    for r in rows:
        if rule.directory_id and rule.directory_id != r.parent_id:
            continue
        if rule.keyword and rule.keyword.lower() not in (r.name or "").lower():
            continue
        if _enqueue_ready(db, r, rule):
            added += 1
    return added


async def _send_test_message(token: str, chat_id: str, bot_name: str,
                             channel_label: str, db: Session | None = None) -> dict:
    """发一条真实的 Telegram 测试消息，错误转成可操作的中文提示。"""
    now_bj = (utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
    text = (f"<b>Ya-TG 测试推送</b>\n"
            f"频道：{html.escape(channel_label, quote=False)}\n"
            f"Bot：{html.escape(bot_name, quote=False)}\n"
            f"时间：{now_bj}（北京时间）\n\n"
            "收到这条消息，说明 Bot Token、频道 ID 与网络均正常，可以正式分发。")
    try:
        r = await _telegram_call(token, "sendMessage",
                                 {"chat_id": chat_id, "text": text, "parse_mode": "HTML",
                                  "disable_web_page_preview": True}, db)
    except Exception as exc:
        return {"ok": False, "message": str(exc)}
    if r["http_status"] == 429:
        wait = retry_after_seconds(r["retry_after"]) or 60
        return {"ok": False, "message": f"Telegram 限流（429），请约 {wait} 秒后重试"}
    if r["http_status"] >= 400 or not r["data"].get("ok"):
        desc = str(r["data"].get("description") or r["data"])[:300]
        if "chat not found" in desc.lower() or "chat_id" in desc.lower() or "chat" in desc.lower():
            desc += "（请检查频道 ID 是否正确、Bot 是否已加入频道并有发送权限）"
        elif "unauthorized" in desc.lower() or r["http_status"] in (401, 403):
            desc += "（Bot Token 可能无效，请先点 Bot 的「验证 Token」）"
        return {"ok": False, "message": f"测试推送失败：{desc}"}
    return {"ok": True, "message_id": str((r["data"].get("result") or {}).get("message_id", "")),
            "message": "测试消息已发送，请到频道查看"}


@app.post("/api/queue/run", dependencies=ADMIN)
async def queue_run():
    """立即处理一次队列（默认 worker 每 30 秒一轮），配置完可马上验证。"""
    return await process_queue(limit=50)


@app.post("/api/resources/{rid}/publish", dependencies=ADMIN)
async def publish_resource(rid: int, db: Session = Depends(get_db)):
    """把单个资源按所有启用规则 + 目录路由（频道/群）入队并立即推送一次。

    v0.9.11：真机根因「手动立即推送只到频道、不到群」——旧实现只读 Rule 表，
    而群目标只存在于 push_routes（目录路由），Rule 表里根本没有群 Channel。
    这里补上目录路由分支，让立即推送和自动识别走同一套目标口径（频道 + 群）。
    """
    x = db.get(Resource, rid)
    if not x:
        raise HTTPException(404, "资源不存在")
    rules = list(db.scalars(select(Rule).where(Rule.enabled.is_(True))).all())
    routes = bot_listener.push_routes(db)
    if not rules and not routes:
        raise HTTPException(400, "还没有启用的分发规则或目录路由：请先在「分发与互动中心」完成 Bot → 频道 / 群 → 路由配置")
    added = 0
    skipped = []
    # v2.1.18：同一请求内按「目标渠道」去重。
    #
    # 真机根因（线上手动推送 500，已用探针复现）：本 Session 是 autoflush=False，
    # routes 分支 `db.add` 之后没有 flush，rules 分支的 `db.scalar` 就看不见刚加的
    # 行 → 同一对 (resource_id, channel_id) 被 add 两次，到函数末尾统一 flush 时
    # 撞 UNIQUE(resource_id, channel_id) → 手动推送直接 500。
    #
    # 实测触发条件（变体反证，不是推断）：
    #   · 目录路由(频道+群) + 规则指向某个路由渠道 → 撞键（线上正是这套配置）
    #   · 只有路由、无规则                      → 正常（入队 2 条）
    #   · 规则指向路由之外的第三渠道            → 正常（入队 3 条）
    #   · 两条启用规则指向同一渠道              → 也撞键（rules 循环内部同样漏查）
    # 所以去重必须横跨两个分支 + 循环内的重复，只在某一个分支里补 flush 治不干净。
    seen_channels: set[int] = set()

    def _ensure_pending_q(channel_id: int) -> bool:
        """把 (资源, 渠道) 置为待推送；返回 True 表示这个渠道本次新纳入。"""
        if channel_id in seen_channels:
            return False
        seen_channels.add(channel_id)
        q = db.scalar(select(QueueItem).where(
            QueueItem.resource_id == x.id, QueueItem.channel_id == channel_id))
        if not q:
            # v2.1.11：归档行也占着唯一键，命中则复活复用而不是 INSERT（否则撞约束）。
            from .archive import find_queue_item
            q = find_queue_item(db, x.id, channel_id)
        if q:
            # v0.6.3：「立即推送」是手动强制动作，ANY 状态（含 sent/retry）都重置为
            # pending 立即重推。旧逻辑只重置 review/failed，用户第一次推成功后反复点
            # 都显示「已入队 0 条」，误以为按钮无反应。
            q.status, q.error, q.next_retry_at, q.attempts = "pending", "", None, 0
            # v2.1.21：手动推送不再挂旧链 share_tasks。残留的 share_task_id 会让
            # publisher 把它当「分享中心项」、要求 status_ext=accepted 才发
            # ——手动强推就永远发不出去（v1.5.1 真机同款卡点）。
            q.share_task_id = 0
        else:
            db.add(QueueItem(resource_id=x.id, channel_id=channel_id,
                             status="pending", share_task_id=0))
        return True

    # v0.9.11：目录路由（频道 + 群）——手动强制动作，绕过海报闸门但尊重目录匹配。
    if routes:
        matched = bot_listener.match_push_routes(db, x, root_ids=bot_listener.scan_root_ids(db))
        for route in matched:
            ch = _ensure_route_channel(db, route)
            if not ch:
                skipped.append(f"路由目标 {route['chat_id']} 找不到可用 Bot")
                continue
            if _ensure_pending_q(ch.id):
                added += 1
        if not matched:
            skipped.append("资源目录未匹配任何推送路由（可在「目录路由」把 dir_id 留空做整库推送）")
    for r in rules:
        ch = db.get(Channel, r.channel_id)
        if not ch or not ch.enabled:
            skipped.append(f"规则「{r.name}」的目标频道不存在或已停用")
            continue
        bot = db.get(Bot, ch.bot_id) if ch else None
        if not bot or not bot.enabled:
            skipped.append(f"规则「{r.name}」的 Bot 不存在或已停用")
            continue
        if r.directory_id and r.directory_id != x.parent_id:
            skipped.append(f"规则「{r.name}」限定目录 {r.directory_id}，与资源目录 {x.parent_id} 不符")
            continue
        if r.keyword and r.keyword.lower() not in x.name.lower():
            skipped.append(f"规则「{r.name}」限定关键词「{r.keyword}」，文件名不含该词")
            continue
        if _ensure_pending_q(r.channel_id):
            added += 1
    # v1.1：autoflush=False，上面新 add 的 QueueItem 必须先 flush 才能被下面查询看到，
    # 否则 force 任务 channel_targets=[] → submit_to_dispatch 返回 0 → 强推消息发不出去。
    db.flush()
    ch_ids = sorted({q.channel_id for q in db.scalars(select(QueueItem).where(
        QueueItem.resource_id == x.id, QueueItem.status == "pending")).all()})
    # v2.1.21：手动强推改登记 **v2 的 force 任务**（写 sc_publish_tasks）。
    #
    # 真机问题：「手动推送的鹿鼎记没进推送任务、推送历史」。推送其实成功了
    # （push_queue 13/14 已 sent、push_sent 审计 6191/6192 都在），断在记录写错了表：
    # 旧实现调 share_center.shadow_register(kind="force") 写旧链的 share_tasks，
    # 而这三个卡片读的是 sc_publish_tasks —— 双写期两张表没打通，手动推送的记录
    # 永远不出现在这三个模块里（自动推送走 v2，所以能在历史看到）。
    #
    # 现在 force 也写 v2 表，交给 v2 的 dispatch / publisher 接管：链接状态、失败重试、
    # 执行记录一并可用，三个模块与自动推送完全同源。
    force_tasks: list = []
    force_error = ""
    try:
        from . import share_center_v2 as _sc_v2
        force_tasks = _sc_v2.register_force_tasks(db, x, ch_ids)
    except Exception as exc:
        # 登记失败不吞掉：如实回给前端；下面仍会把队列项直发出去，消息不丢。
        force_error = f"登记推送任务失败：{str(exc)[:200]}"
        logger.exception("[publish] 手动强推登记 v2 任务失败")
    db.commit()
    result = await process_queue(limit=50)
    # v0.6.2：把本次入队项的即时结果带回前端，避免"点了没反应"的错觉。
    # 用户常在资源未建分享或 Telegram 未通时点推送，队列项立即进入 retry，
    # 若不回显错误，界面只显示"已入队 1 条"却收不到消息。
    recent = list(db.scalars(select(QueueItem).where(
        QueueItem.resource_id == x.id).order_by(QueueItem.id.desc()).limit(max(added, 1))).all())
    detail = [{"channel_id": q.channel_id, "status": q.status,
               "error": str(q.error or "")[:300]} for q in recent if q.error]
    # v1.5.2 P0：process_queue 的 result 绝不能覆盖本端点的 skipped（字符串数组）
    # 契约。旧写法把 skipped 写在 **result 展开之前，result 带同名键时后者覆盖前者
    # ——真机锁冲突那轮 result["skipped"]=True(bool)，前端
    # `[...(d.skipped||[])]` 展开布尔抛 TypeError，手动推送直接报错。
    # 处理：先展开 result 再写自身字段；pop 掉内部 skipped 标志（内部统计不该
    # 污染给前端的数组契约）；锁冲突 reason 并进 detail 作为普通提示回显。
    payload = dict(result)
    reason = str(payload.pop("reason", "") or "")
    payload.pop("skipped", None)
    if reason:
        detail = [*detail, {"channel_id": 0, "status": "skipped", "error": reason}]
    if force_error:
        detail = [*detail, {"channel_id": 0, "status": "failed", "error": force_error}]
    # v2.1.21：如实回显本次登记了几条推送任务（= 本次会出现在「推送任务」里的条数）。
    # 查不到作品快照时为 0，此时只能走队列直发，不伪造任务行。
    return {**payload, "ok": True, "queued": added, "detail": detail, "skipped": skipped,
            "tasks_registered": len(force_tasks)}


@app.put("/api/bots/{bid}", dependencies=ADMIN)
def toggle_bot(bid: int, p: ToggleInput, db: Session = Depends(get_db)):
    """Bot 独立启停：停用只影响它自己的频道链路，不拖累其它 Bot。"""
    b = db.get(Bot, bid)
    if not b:
        raise HTTPException(404, "Bot 不存在")
    b.enabled = bool(p.enabled)
    add_audit(db, "bot_toggled", target_type="bot", target_id=str(bid),
              detail=json_dumps({"enabled": b.enabled}))
    db.commit()
    return {"ok": True, "id": bid, "enabled": b.enabled}


@app.put("/api/channels/{cid}", dependencies=ADMIN)
def toggle_channel(cid: int, p: ToggleInput, db: Session = Depends(get_db)):
    """频道独立启停：停用只影响该频道自己的队列项。"""
    ch = db.get(Channel, cid)
    if not ch:
        raise HTTPException(404, "频道不存在")
    ch.enabled = bool(p.enabled)
    add_audit(db, "channel_toggled", target_type="channel", target_id=str(cid),
              detail=json_dumps({"enabled": ch.enabled}))
    db.commit()
    return {"ok": True, "id": cid, "enabled": ch.enabled}


@app.post("/api/bots/{bid}/test-message", dependencies=ADMIN)
async def test_bot_message(bid: int, p: TestMessageInput, db: Session = Depends(get_db)):
    """Bot 行「测试推送」：真正发一条 Telegram 消息。

    用户反馈：原来的「验证」只调 getMe，频道里收不到任何消息。chat_id 留空时
    自动取该 Bot 下第一个频道；没有频道时要求用户手填 chat_id。
    """
    b = db.get(Bot, bid)
    if not b:
        raise HTTPException(404, "Bot 不存在")
    if not b.enabled:
        return {"ok": False, "message": "该 Bot 已停用，请先点「启用」"}
    chat_id = (p.chat_id or "").strip()
    label = chat_id
    if not chat_id:
        ch = db.scalar(select(Channel).where(Channel.bot_id == bid)
                       .order_by(Channel.id).limit(1))
        if ch:
            chat_id, label = ch.chat_id, ch.name
    if not chat_id:
        return {"ok": False, "message": "请填写频道 chat_id（如 -1001234567890），或先在「02 添加频道」里添加频道"}
    try:
        token = decrypt_secret(b.token)
    except Exception as exc:
        return {"ok": False, "message": f"Bot Token 解密失败：{exc}"}
    if not token:
        return {"ok": False, "message": "Bot Token 为空，请重新添加 Bot"}
    r = await _send_test_message(token, chat_id, b.name, label or chat_id, db)
    add_audit(db, "bot_test_message", target_type="bot", target_id=str(bid),
              detail=json_dumps({"ok": r["ok"], "chat_id": chat_id,
                                 "message": r.get("message", "")[:200]}))
    db.commit()
    return r


@app.post("/api/broadcast/notify", dependencies=ADMIN)
async def broadcast_notify(p: BroadcastInput, db: Session = Depends(get_db)):
    """v0.9.6.1：管理员自定义群发通知。

    边界（用户明确口径）：TG Bot 面向普通用户，私聊只由用户指令触发；
    这里是管理员侧的「通知到推送目标」，只发目录路由表里的频道 / 群聊，
    **绝不私聊用户、不写队列、不改资源或分享**。chat_ids 不传 = 全部路由目标。
    """
    text = (p.text or "").strip()
    if not text:
        raise HTTPException(400, "通知内容不能为空")
    bot = _global_bot(db)
    if not bot or not bot.enabled:
        return {"ok": False, "sent": 0, "message": "Bot 未配置或已停用"}
    try:
        token = decrypt_secret(bot.token)
    except Exception as exc:
        return {"ok": False, "sent": 0, "message": f"Bot Token 解密失败：{exc}"}
    if not token:
        return {"ok": False, "sent": 0, "message": "Bot Token 为空，请重新配置"}

    routes = bot_listener.push_routes(db)
    allowed = {str(r.get("chat_id") or "").strip() for r in routes
               if str(r.get("chat_id") or "").strip()}
    if p.chat_ids is not None:
        picked = {str(x).strip() for x in p.chat_ids if str(x).strip()}
        targets = sorted(x for x in picked if x in allowed)
    else:
        targets = sorted(allowed)
    if not targets:
        return {"ok": False, "sent": 0,
                "message": "没有可发送的频道 / 群：请先在「推送白名单（目录路由）」配置目标"}

    sent, failures = 0, []
    for chat_id in targets:
        try:
            r = await _telegram_call(token, "sendMessage",
                                     {"chat_id": chat_id, "text": text,
                                      "disable_web_page_preview": True}, db)
            if r["http_status"] == 429:
                wait = retry_after_seconds(r["retry_after"]) or 60
                failures.append(f"{chat_id}：限流 429，约 {wait} 秒后重试")
            elif r["http_status"] >= 400 or not r["data"].get("ok"):
                desc = str(r["data"].get("description") or r["data"])[:160]
                failures.append(f"{chat_id}：{desc}")
            else:
                sent += 1
        except Exception as exc:
            failures.append(f"{chat_id}：{str(exc)[:160]}")
    add_audit(db, "broadcast_notify", target_type="config",
              detail=json_dumps({"sent": sent, "targets": len(targets),
                                 "content": text[:200], "failures": failures[:5]}))
    db.commit()
    msg = f"已发送 {sent}/{len(targets)} 个推送目标"
    if failures:
        msg += "；失败：" + "；".join(failures[:3])
    return {"ok": sent > 0, "sent": sent, "targets": len(targets), "message": msg}


@app.get("/api/telegram/config", dependencies=ADMIN)
def telegram_config_get(db: Session = Depends(get_db)):
    return {"base_url": runtime_config.get(db, "telegram_api_base"),
            "default": settings.telegram_api_base}


@app.post("/api/telegram/config", dependencies=ADMIN)
def telegram_config_save(p: TelegramConfigInput, db: Session = Depends(get_db)):
    runtime_config.set_many(db, {"telegram_api_base": p.base_url.strip()})
    add_audit(db, "telegram_config_saved", target_type="config", detail=p.base_url.strip()[:200])
    db.commit()
    return {"ok": True, "base_url": runtime_config.get(db, "telegram_api_base")}


class TgExtraConfigInput(BaseModel):
    push_mode: str = "channel"
    custom_bot_token: str = ""
    custom_chat_id: str = ""
    fubao_bot: str = ""
    link_style: str = "button"
    detail_level: str = "compact"


_TG_EXTRA_KEYS = {
    "push_mode": "tg_push_mode",
    "custom_bot_token": "tg_custom_bot_token",
    "custom_chat_id": "tg_custom_chat_id",
    "fubao_bot": "tg_fubao_bot",
    "link_style": "tg_link_style",
    "detail_level": "tg_detail_level",
}


@app.get("/api/tg/extra-config", dependencies=ADMIN)
def tg_extra_config_get(db: Session = Depends(get_db)):
    """v0.9.2：推送模式统一为「由 Bot 推送」——频道/群聊都作为一条 Channel，
    走同一 token + chat_id 路径。旧值（channel/custom_bot_group）归一回显为 bot_channel。"""
    out = {k: runtime_config.get(db, key) for k, key in _TG_EXTRA_KEYS.items()}
    out["push_mode"] = "bot_channel"  # 唯一口径：由 Bot 推送（频道/群聊统一）
    return out


@app.post("/api/tg/extra-config", dependencies=ADMIN)
def tg_extra_config_save(p: TgExtraConfigInput, db: Session = Depends(get_db)):
    # v0.9.2：无论前端传什么旧值，一律归一为 bot_channel；custom_bot_* 不再参与发送，
    # 保存时清空以免遗留"绕过 Channel"的隐患（存量用户升级后自动迁移）。
    if p.link_style not in ("button", "plain"):
        raise HTTPException(400, "链接样式仅支持 button/plain")
    if p.detail_level not in ("compact", "full"):
        raise HTTPException(400, "详情级别仅支持 compact/full")
    vals = {key: str(getattr(p, k) or "").strip() for k, key in _TG_EXTRA_KEYS.items()}
    vals["tg_push_mode"] = "bot_channel"
    vals["tg_custom_bot_token"] = ""
    vals["tg_custom_chat_id"] = ""
    runtime_config.set_many(db, vals)
    add_audit(db, "tg_extra_config_saved", target_type="config",
              detail=json_dumps({"push_mode": "bot_channel", "link_style": p.link_style}))
    db.commit()
    return {"ok": True, **{k: runtime_config.get(db, key) for k, key in _TG_EXTRA_KEYS.items()},
            "push_mode": "bot_channel"}


class RewardConfigInput(BaseModel):
    code: str = ""


@app.get("/api/reward/config", dependencies=ADMIN)
def reward_config_get(db: Session = Depends(get_db)):
    """v0.7.0：赞赏码配置（图片 URL 或文本）。"""
    return {"code": runtime_config.get(db, "reward_code")}


@app.post("/api/reward/config", dependencies=ADMIN)
def reward_config_save(p: RewardConfigInput, db: Session = Depends(get_db)):
    runtime_config.set_many(db, {"reward_code": (p.code or "").strip()})
    add_audit(db, "reward_config_saved", target_type="config")
    db.commit()
    return {"ok": True, "code": runtime_config.get(db, "reward_code")}


@app.get("/api/queue/schedule", dependencies=ADMIN)
def queue_schedule_get(db: Session = Depends(get_db)):
    """v0.6.4：读取队列推送调度配置。"""
    try:
        seconds = int(runtime_config.get(db, "queue_interval_seconds") or 30)
    except ValueError:
        seconds = 30
    try:
        delay = int(runtime_config.get(db, "telegram_send_delay") or 2)
    except ValueError:
        delay = 2
    return {"interval_seconds": max(5, seconds),
            "cron": runtime_config.get(db, "queue_cron") or "",
            "send_delay": max(0, min(delay, 60)),
            "default_interval": 30}


@app.post("/api/queue/schedule", dependencies=ADMIN)
def queue_schedule_save(p: QueueScheduleInput, db: Session = Depends(get_db)):
    """保存推送间隔/Cron 并立即热更新定时任务，无需重启容器。"""
    cron = (p.cron or "").strip()
    if cron:
        try:
            CronTrigger.from_crontab(cron, timezone="Asia/Shanghai")
        except Exception as e:
            raise HTTPException(400, f"Cron 表达式无效：{e}。示例 */1 * * * * 表示每分钟推送一次")
    seconds = max(5, min(int(p.interval_seconds or 30), 86400))
    delay = max(0, min(int(p.send_delay or 0), 60))
    runtime_config.set_many(db, {"queue_interval_seconds": str(seconds), "queue_cron": cron,
                                 "telegram_send_delay": str(delay)})
    add_audit(db, "queue_schedule_saved", target_type="config",
              detail=json_dumps({"interval_seconds": seconds, "cron": cron, "send_delay": delay}))
    db.commit()
    trigger = _queue_trigger(db)
    _schedule_queue_worker(trigger)
    return {"ok": True, "interval_seconds": seconds, "cron": cron, "send_delay": delay,
            "mode": "cron" if cron else "interval"}


@app.post("/api/tmdb/translate-search", dependencies=ADMIN)
async def tmdb_translate_search(p: TranslateSearchInput, db: Session = Depends(get_db)):
    """手动识别：中文标题自动翻译成英/日/韩标题后多语搜索，返回带海报候选。"""
    try:
        return await tmdb.search_multilingual(
            p.title.strip(), p.media_type if p.media_type in ("movie", "tv") else None,
            p.year, None, db)
    except Exception as e:
        raise _tmdb_error(e)


@app.get("/api/diagnostics", dependencies=ADMIN)
def diagnostics(db: Session = Depends(get_db)):
    """链路自检：一眼看出卡在 Bot / 频道 / 规则 / 审核 / 队列哪一步。"""
    def count(model, *conds):
        stmt = select(func.count(model.id))
        for cond in conds:
            stmt = stmt.where(cond)
        return int(db.scalar(stmt) or 0)

    bots_on = count(Bot, Bot.enabled.is_(True))
    bots_all = count(Bot)
    chs_on = count(Channel, Channel.enabled.is_(True))
    chs_all = count(Channel)
    rules_on = count(Rule, Rule.enabled.is_(True))
    pending = count(QueueItem, QueueItem.status == "pending")
    review = count(QueueItem, QueueItem.status == "review")
    retry = count(QueueItem, QueueItem.status == "retry")
    failed = count(QueueItem, QueueItem.status == "failed")
    sent = count(QueueItem, QueueItem.status == "sent")
    blockers = []
    if not bots_on:
        blockers.append("还没有启用的 Bot：" + ("已添加但全部停用" if bots_all else "请先添加 Bot"))
    if not chs_on:
        blockers.append("还没有启用的频道：" + ("已添加但全部停用" if chs_all else "请先添加频道"))
    if not rules_on:
        blockers.append("还没有启用的分发规则，扫描到的资源不会入队")
    if review:
        blockers.append(f"{review} 条待人工审核，需到「审核中心」点「通过」才会推送")
    if retry:
        blockers.append(f"{retry} 条等待重试（建分享或 Telegram 失败），到「推送队列」看错误列")
    if failed:
        blockers.append(f"{failed} 条推送失败，请到「推送队列」查看错误列")
    # v0.6.2：最近失败原因直接暴露，避免用户看到「链路完整」却收不到消息
    last_error = ""
    for q in db.scalars(select(QueueItem).where(QueueItem.error != "")
                        .order_by(QueueItem.id.desc()).limit(1)).all():
        last_error = str(q.error or "")[:300]
    return {"ok": not blockers, "blockers": blockers,
            "bots": {"total": bots_all, "enabled": bots_on},
            "channels": {"total": chs_all, "enabled": chs_on},
            "rules_enabled": rules_on, "queue": {"pending": pending, "review": review,
                                                  "retry": retry, "failed": failed, "sent": sent},
            "last_error": last_error,
            "telegram_base": runtime_config.get(db, "telegram_api_base")}


# ---------- v0.8.0：用户互动 / TgtoDrive / 打赏 ----------
class InteractionConfigInput(BaseModel):
    contribution_enabled: bool = False
    donation_enabled: bool = False
    # 以下字段传 None 表示「保持原值」。
    # v2.1.16：凭证类 key（tgto_api_key / contribution_bot_token）**不回显任何值**，
    # 只回 *_configured / *_needs_reset 标志；保存时掩码串会被直接丢弃（见 is_masked_secret）。
    tgto_base_url: str | None = None
    tgto_scene: str | None = None
    tgto_api_key: str | None = None
    # v0.9.13 #254：TgtoDrive 白名单规则 a/b/c（求片联动，与投稿转存无关）
    tgto_rule_resolution: str | None = None
    tgto_rule_effect: str | None = None
    tgto_rule_required_keyword: str | None = None
    # v0.9.19：投稿转存——直接调光鸭 API 转存到指定目录（不经 TgtoDrive）
    contribution_restore_enabled: bool | None = None
    contribution_restore_target_dir_id: str | None = None
    contribution_bot_token: str | None = None
    interaction_bot_id: str | None = None       # v0.9.4：交互 Bot 引用 bots 表 id（token 去重）；"" = 不选用
    interaction_bot_username: str | None = None  # v0.9.15 #276：Bot @用户名（startgroup 链接兜底；getMe 会自动缓存）
    donation_image_url: str | None = None
    donation_text: str | None = None
    donation_button_text: str | None = None
    # v0.9.0：Bot 推送白名单 / 使用限制 / 自定义指令
    bot_push_channels: list[str] | None = None       # 需管理员的频道 ID
    bot_push_groups: list[str] | None = None         # 无需管理员的群聊 ID
    bot_restrict_enabled: bool | None = None
    bot_restrict_cache_seconds: int | None = None
    bot_required_channels: list[str] | None = None   # 必须关注频道
    bot_required_groups: list[str] | None = None     # 必须加入群聊
    bot_commands: list[dict] | None = None           # 自定义指令 [{command,description,reply}]
    # v0.9.2：使用限制与推送白名单二选一 whitelist/membership/off
    bot_gate_mode: str | None = None
    # v0.9.5：使用限制跳转链接 + 推送消息频道/群跳转（与打赏一起维护，模板变量）
    bot_required_channel_link: str | None = None
    bot_required_group_link: str | None = None
    tg_channel_link: str | None = None
    tg_group_link: str | None = None
    tg_bot_link: str | None = None       # v0.9.10：推送消息正文里的机器人跳转链接（指向推送 Bot 本身）


# v2.1.16：配置页里**只能由人工填写**的凭证 key。这类 key 一律不回显（连掩码都不回），
# 保存时也拒绝任何形如 mask_secret() 掩码的提交——掩码回写会让鉴权永久 401。
SECRET_CONFIG_KEYS = ["tgto_api_key", "contribution_bot_token"]


@app.get("/api/interaction/config", dependencies=ADMIN)
def interaction_config_get(db: Session = Depends(get_db)):
    plain = ["tgto_base_url", "tgto_scene", "donation_image_url", "donation_text",
             "donation_button_text", "bot_required_channel_link", "bot_required_group_link",
             "tg_channel_link", "tg_group_link", "tg_bot_link",
             "tgto_rule_resolution", "tgto_rule_effect", "tgto_rule_required_keyword",
             "contribution_restore_target_dir_id"]
    out = {k: runtime_config.get(db, k) for k in plain}
    # v2.1.16：凭证类 key 只回「状态」，不回任何值——包括掩码。
    # 事故链：以前这里回显 mask_secret() 的掩码（tgto******4ifI），输入框被填满星号，
    # 用户以为没写入就又点一次保存，掩码被 set_many 加密写库 → TgtoDrive OpenAPI 恒 401，
    # 且「重新生成真 Token 再填」也会被下一次误保存打回原形。值一律置空，只给标志。
    out.update({k: "" for k in SECRET_CONFIG_KEYS})
    for key, state in runtime_config.secret_state(db, SECRET_CONFIG_KEYS).items():
        out[f"{key}_configured"] = state["configured"]   # 存储的是真凭证，可直接用
        out[f"{key}_needs_reset"] = state["needs_reset"]  # 存储的是掩码，必须重填
    out["contribution_enabled"] = (runtime_config.get(db, "contribution_enabled") or "").lower() == "true"
    out["donation_enabled"] = (runtime_config.get(db, "donation_enabled") or "").lower() == "true"
    out["contribution_restore_enabled"] = contribution_restore.enabled(db)
    # v0.9.0：推送白名单 / 使用限制 / 指令清单回显
    out["bot_push_channels"] = bot_listener.push_whitelist(db)[0]
    out["bot_push_groups"] = bot_listener.push_whitelist(db)[1]
    out["bot_restrict_enabled"] = (runtime_config.get(db, "bot_restrict_enabled") or "").lower() == "true"
    try:
        out["bot_restrict_cache_seconds"] = int(runtime_config.get(db, "bot_restrict_cache_seconds") or "120")
    except ValueError:
        out["bot_restrict_cache_seconds"] = 120
    out["bot_required_channels"] = bot_listener._json_list("bot_required_channels_json", db)
    out["bot_required_groups"] = bot_listener._json_list("bot_required_groups_json", db)
    out["bot_commands"] = bot_listener.builtin_and_custom(db)
    out["bot_gate_mode"] = bot_listener.gate_mode(db)
    # v0.9.4：交互 Bot 去重——回显选中的 bot id 与可选 bot 列表（不含 token 明文）
    out["interaction_bot_id"] = (runtime_config.get(db, "interaction_bot_id") or "").strip()
    out["interaction_bot_username"] = (runtime_config.get(db, "interaction_bot_username") or "").strip().lstrip("@")
    out["bot_options"] = [{"id": b.id, "name": b.name, "enabled": b.enabled,
                           "token_masked": _mask_stored_token(b.token)}
                          for b in db.scalars(select(Bot).order_by(Bot.id)).all()]
    out["scene_default"] = "ENV_FILTER_GUANGYA"
    out["unverified_endpoints"] = ["跨账号他人分享转存成功路径（无可信他人分享样本，未验证）"]
    return out


@app.post("/api/interaction/config", dependencies=ADMIN)
def interaction_config_save(p: InteractionConfigInput, db: Session = Depends(get_db)):
    values = {"contribution_enabled": "true" if p.contribution_enabled else "false",
              "donation_enabled": "true" if p.donation_enabled else "false"}
    for k in ("tgto_base_url", "tgto_scene", "tgto_api_key", "contribution_bot_token",
              "donation_image_url", "donation_text", "donation_button_text",
              "bot_required_channel_link", "bot_required_group_link",
              "tg_channel_link", "tg_group_link", "tg_bot_link",
              "tgto_rule_resolution", "tgto_rule_effect", "tgto_rule_required_keyword",
              "contribution_restore_target_dir_id"):
        v = getattr(p, k)
        if v is not None:
            values[k] = v.strip()
    # v2.1.16：拒绝掩码回写。GET 现在不回显掩码了，这里再兜一层——旧版前端、浏览器
    # 缓存的旧 JS、或任何人手工调 API 把 mask_secret() 的产物（如 tgto******4ifI）
    # 提交上来时，一律丢弃不写库，保持库里原值不动，并回告给页面。
    ignored_masked = [k for k in SECRET_CONFIG_KEYS if is_masked_secret(values.get(k, ""))]
    for k in ignored_masked:
        values.pop(k, None)
    # v0.9.19：投稿转存开关（bool → runtime_config 字符串）
    if p.contribution_restore_enabled is not None:
        values["contribution_restore_enabled"] = "true" if p.contribution_restore_enabled else "false"
    # v0.9.4：交互 Bot 引用（token 去重）。传 "" 表示清空引用、回退 contribution_bot_token。
    if p.interaction_bot_id is not None:
        bid = str(p.interaction_bot_id).strip()
        if bid:
            if not bid.isdigit() or not db.get(Bot, int(bid)):
                raise HTTPException(400, "interaction_bot_id 必须指向一个已存在的 Bot")
        values["interaction_bot_id"] = bid
    # v0.9.15 #276：Bot 用户名兜底值（getMe 成功会自动覆盖缓存，这里只存人工填写值）
    if p.interaction_bot_username is not None:
        values["interaction_bot_username"] = str(p.interaction_bot_username).strip().lstrip("@")
    # v0.9.0：Bot 交互配置。列表序列化为 JSON，缺省 None 表示保持原值。
    if p.bot_push_channels is not None:
        values["bot_push_channels_json"] = json_dumps([str(x).strip() for x in p.bot_push_channels if str(x).strip()])
    if p.bot_push_groups is not None:
        values["bot_push_groups_json"] = json_dumps([str(x).strip() for x in p.bot_push_groups if str(x).strip()])
    if p.bot_required_channels is not None:
        values["bot_required_channels_json"] = json_dumps([str(x).strip() for x in p.bot_required_channels if str(x).strip()])
    if p.bot_required_groups is not None:
        values["bot_required_groups_json"] = json_dumps([str(x).strip() for x in p.bot_required_groups if str(x).strip()])
    if p.bot_restrict_enabled is not None:
        values["bot_restrict_enabled"] = "true" if p.bot_restrict_enabled else "false"
    if p.bot_restrict_cache_seconds is not None:
        values["bot_restrict_cache_seconds"] = str(max(0, min(int(p.bot_restrict_cache_seconds), 86400)))
    if p.bot_gate_mode is not None:
        if p.bot_gate_mode not in ("whitelist", "membership", "off"):
            raise HTTPException(400, "bot_gate_mode 仅支持 whitelist/membership/off")
        values["bot_gate_mode"] = p.bot_gate_mode
    if p.bot_commands is not None:
        cmds = []
        for c in p.bot_commands:
            if not isinstance(c, dict):
                continue
            cmd = str(c.get("command") or "").strip().lstrip("/")
            if not cmd:
                continue
            cmds.append({"command": cmd[:64], "description": str(c.get("description") or "")[:200],
                         "reply": str(c.get("reply") or "")[:2000]})
        values["bot_commands_json"] = json_dumps(cmds)
    runtime_config.set_many(db, values)
    add_audit(db, "interaction_config_saved", target_type="interaction",
              detail=json_dumps({"keys": sorted(values),
                                 "ignored_masked": ignored_masked}))
    db.commit()
    return {"ok": True, "saved": sorted(values), "ignored_masked": ignored_masked}


# ---------- v0.9.5：推送白名单 = 目录路由表（全局单 Bot） ----------
class PushTargetInput(BaseModel):
    kind: str = "channel"  # channel/group
    chat_id: str = ""
    name: str = ""         # 展示名（群名/频道名），纯展示


class PushRouteInput(BaseModel):
    dir_id: str = ""       # 影视库 B 的下属目录 ID；空=根目录通配
    kind: str = "channel"  # 老字段：channel/group；v0.9.8 起前端改用双字段，保留兼容
    chat_id: str = ""      # 老字段：单一目标；v0.9.8 起前端改用双字段，保留兼容
    # v0.9.8：频道 ID 与群 ID 两个独立可填字段，可只填其一或两个都填
    channel_id: str = ""
    group_id: str = ""
    # v0.9.14 #257：一个目录可绑定多个频道/群目标（与双字段并存，targets 优先）
    targets: list[PushTargetInput] = []
    note: str = ""         # v0.9.6：备注，便于肉眼区分不同目录（纯展示，不参与匹配）


class PushRoutesInput(BaseModel):
    routes: list[PushRouteInput] = []
    bot_token: str | None = None   # None/空/掩码=不改；新值=更新全局单 Bot token
    bot_name: str = "Ya-TG Bot"


_ROUTE_CHANNEL_PREFIX = "route:"  # 由路由表自动维护的 Channel 标记，清理时不碰用户旧数据


def _global_bot(db: Session) -> Bot | None:
    """全局单 Bot：优先 interaction_bot_id 指向的，否则第一个启用的，否则第一个。"""
    raw = (runtime_config.get(db, "interaction_bot_id") or "").strip()
    if raw.isdigit():
        b = db.get(Bot, int(raw))
        if b:
            return b
    return (db.scalar(select(Bot).where(Bot.enabled.is_(True)).order_by(Bot.id).limit(1))
            or db.scalar(select(Bot).order_by(Bot.id).limit(1)))


@app.get("/api/push/routes", dependencies=ADMIN)
def push_routes_get(db: Session = Depends(get_db)):
    bot = _global_bot(db)
    return {
        # v0.9.8：管理页回填用原始路由（频道 / 群两个独立字段），不要用展开后的目标列表
        "routes": bot_listener.push_routes_raw(db),
        "bot": ({"id": bot.id, "name": bot.name, "enabled": bot.enabled,
                 "token_masked": _mask_stored_token(bot.token)} if bot else None),
        "max_routes": 10,
    }


@app.post("/api/push/routes", dependencies=ADMIN)
def push_routes_save(p: PushRoutesInput, db: Session = Depends(get_db)):
    """保存目录路由表 + 全局单 Bot token，并同步维护推送目标 Channel。

    路由表是配置真相源（runtime_config JSON）；Channel 是队列/发送层依赖的记录，
    这里为每条路由确保一条启用的 Channel（按 chat_id 复用），删除不再使用的路由 Channel。
    """
    routes = []
    for item in p.routes[:10]:
        channel_id = str(getattr(item, "channel_id", "") or "").strip()
        group_id = str(getattr(item, "group_id", "") or "").strip()
        # 兼容老前端：只传 kind/chat_id 时归入对应字段
        if not channel_id and not group_id:
            legacy = str(item.chat_id or "").strip()
            if legacy:
                if str(item.kind or "").lower() == "group":
                    group_id = legacy
                else:
                    channel_id = legacy
        # v0.9.14 #257：一目录多目标。targets 优先；归一化并按 (kind,chat_id) 去重。
        targets = []
        seen = set()
        for t in (getattr(item, "targets", None) or []):
            t_chat = str(t.chat_id or "").strip()
            t_kind = "group" if str(t.kind or "").lower() == "group" else "channel"
            if not t_chat or (t_kind, t_chat) in seen:
                continue
            seen.add((t_kind, t_chat))
            targets.append({"kind": t_kind, "chat_id": t_chat,
                            "name": str(t.name or "").strip()[:128]})
        if targets:
            # 同步双字段（取各 kind 第一个）供老前端/下游兼容
            channel_id = channel_id or next((t["chat_id"] for t in targets if t["kind"] == "channel"), "")
            group_id = group_id or next((t["chat_id"] for t in targets if t["kind"] == "group"), "")
        else:
            # 无 targets：从双字段合成（兼容老前端只传 channel_id/group_id）
            if channel_id:
                targets.append({"kind": "channel", "chat_id": channel_id, "name": ""})
            if group_id:
                targets.append({"kind": "group", "chat_id": group_id, "name": ""})
        if not targets:
            continue  # 无任何目标的路由跳过
        routes.append({"dir_id": str(item.dir_id or "").strip(),
                       "channel_id": channel_id, "group_id": group_id,
                       "targets": targets,
                       "note": str(item.note or "").strip()[:120]})
    if len(routes) > 10:
        raise HTTPException(400, "推送白名单最多 10 条")
    # 展开成目标列表供 Channel 同步：一条路由的每个目标各算一个目标（跨路由按 chat_id 复用）
    channel_targets = []
    seen_targets = set()
    for r in routes:
        for t in r["targets"]:
            key = (t["kind"], t["chat_id"])
            if key in seen_targets:
                continue
            seen_targets.add(key)
            channel_targets.append({"kind": t["kind"], "chat_id": t["chat_id"], "name": t.get("name", "")})

    bot = _global_bot(db)
    # token 有实质新值（非掩码/非占位）时更新或创建全局单 Bot
    token = (p.bot_token or "").strip()
    current_masked = _mask_stored_token(bot.token) if bot else ""
    is_new_token = bool(token) and "•" not in token and "*" not in token and token != current_masked
    if is_new_token:
        try:
            token_enc = encrypt_secret(token)
        except RuntimeError as exc:
            raise HTTPException(400, str(exc))
        if bot:
            bot.token = token_enc
            bot.name = str(p.bot_name or bot.name).strip() or bot.name
            bot.enabled = True
        else:
            bot = Bot(name=str(p.bot_name or "Ya-TG Bot").strip(), token=token_enc, enabled=True)
            db.add(bot)
            db.flush()
        runtime_config.set_many(db, {"interaction_bot_id": str(bot.id)})

    # 同步路由 Channel：按 chat_id 复用已有启用 Channel；没有则用全局 Bot 新建
    keep_ids: set[int] = set()
    for r in channel_targets:
        ch = db.scalar(select(Channel).where(Channel.chat_id == r["chat_id"]).limit(1))
        if not ch:
            if not bot:
                raise HTTPException(400, "请先在「交互 Bot 与推送配置」填写 Bot Token")
            ch = Channel(bot_id=bot.id, name=f"{_ROUTE_CHANNEL_PREFIX}{r['kind']}:{r['chat_id']}",
                         chat_id=r["chat_id"], kind=r["kind"], enabled=True)
            db.add(ch)
            db.flush()
        else:
            ch.enabled = True
            ch.kind = r["kind"]
            # v0.9.10：推送 Bot 换 token/重建后，路由 Channel 的 bot_id 必须同步到
            # 全局 Bot，否则测试用新 Bot（能发）、正常推送用旧 bot_id（失效/不在群）
            # 造成「测试能收、正常推送收不到」。原条件跳过 route: 前缀 Channel，
            # 恰恰把路由自动建的群/频道排除在外，是真机错配根因之一；这里统一同步。
            if bot:
                ch.bot_id = bot.id
        keep_ids.add(ch.id)
    # 清理路由自动创建、但已不再引用的 Channel（连带其规则/队列交由数据库外键与 worker 处理）
    for ch in db.scalars(select(Channel).where(Channel.name.startswith(_ROUTE_CHANNEL_PREFIX))).all():
        if ch.id not in keep_ids:
            nr, nq = _cascade_channel(db, ch.id)
            db.delete(ch)

    runtime_config.set_many(db, {"push_routes_json": json_dumps(routes)})
    add_audit(db, "push_routes_saved", target_type="config",
              detail=json_dumps({"count": len(routes)}))
    db.commit()
    return {"ok": True, "routes": routes, "count": len(routes)}


# ---------- v0.9.14 #255-#258：自动发现群组 + 统一推送目标管理 ----------
_TYPE_LABELS = {"group": "普通群", "supergroup": "超级群", "channel": "频道", "private": "私聊"}
# v0.9.15 #276：Bot 成员状态中文标签（left/kicked=已退出，保留历史不删）
_MEMBER_LABELS = {
    "creator": "群主", "administrator": "管理员", "member": "已加入",
    "restricted": "受限", "left": "已退出", "kicked": "已被移出", "unknown": "未知",
}



def _discovered_kind(d: DiscoveredChat) -> str:
    """只有 channel 归为频道；group/supergroup/其它一律按群处理（决定 Channel.kind）。"""
    return "channel" if str(d.chat_type or "").lower() == "channel" else "group"


def _inactive_discover_reason(d: DiscoveredChat) -> str:
    """v0.9.17.1 #283：目标已被确认不在群/被移出时的拒绝原因；unknown 不拦截。

    只对明确证据（left/kicked 成员状态、not_member 权限、active=False）拒绝加入
    推送白名单。member_status=unknown（如从白名单配置落库、尚未检查权限的目标）
    放行，避免误伤新目标/手工兜底。
    """
    member = str(getattr(d, "member_status", "") or "").lower()
    perm = str(getattr(d, "perm_status", "") or "").lower()
    if member in ("left", "kicked") or perm == "not_member":
        label = {"channel": "频道", "group": "群"}.get(_discovered_kind(d), "目标")
        return (f"Bot 已不在该{label}（成员状态={member or '已退出'}、权限={perm or 'not_member'}），"
                f"无法加入推送白名单。请先用「生成授权链接」把 Bot 加回{label}"
                f"{'并设为管理员授予发布权限' if _discovered_kind(d) == 'channel' else ''}，"
                f"收到成员变化事件后再试。")
    return ""


def _whitelist_configured_chats(db: Session) -> list[tuple[str, str, str]]:
    """v0.9.16 #279：列出管理员已在白名单/目录路由里配置的真实推送目标。

    返回 [(chat_type, chat_id, title), ...]，去重。这些不是"扫描发现"的结果，
    而是管理员手工确认过、正在使用的 chat_id——用于在发现台账为空时兜底展示，
    让用户看到实际在推送的群/频道。绝不虚构数量。
    """
    out: list[tuple[str, str, str]] = []
    seen: set[str] = set()

    def add(ctype: str, chat_id: str, title: str = ""):
        cid = str(chat_id or "").strip()
        if not cid or cid in seen:
            return
        seen.add(cid)
        out.append((ctype, cid, str(title or "").strip()))

    # 1) channels 表（推送目标真相源）
    for c in db.scalars(select(Channel).where(Channel.enabled.is_(True))).all():
        ctype = "channel" if (getattr(c, "kind", "channel") or "channel") != "group" else "supergroup"
        add(ctype, c.chat_id, getattr(c, "name", ""))
    # 2) 目录路由 targets（一个目录可绑多个目标）
    for r in bot_listener.push_routes(db):
        add(r.get("kind", "channel"), r.get("chat_id", ""), r.get("target_name", ""))
    # 3) 旧版 runtime_config 白名单兼容
    chans, groups = bot_listener.push_whitelist(db)
    for cid in chans:
        add("channel", cid)
    for cid in groups:
        add("group", cid)
    return out


def _discovered_row(d: DiscoveredChat, bot_name: str = "") -> dict:
    # v0.9.17 热修：权限检查已判 not_member 时，成员状态列以它为准展示，
    # 避免「成员状态=已加入、权限=Bot 不在群」自相矛盾（不改库，事件更新后自然恢复）。
    raw_member = str(getattr(d, "member_status", "") or "")
    perm = str(getattr(d, "perm_status", "") or "")
    eff_member = "left" if (perm == "not_member" and raw_member not in ("left", "kicked")) else (raw_member or "unknown")
    return {
        "id": d.id, "bot_id": d.bot_id, "bot_name": bot_name,
        "chat_id": d.chat_id, "title": d.title, "username": d.username,
        "chat_type": d.chat_type, "type_label": _TYPE_LABELS.get(d.chat_type, d.chat_type or "未知"),
        "source": d.source, "in_whitelist": bool(d.in_whitelist),
        "perm_status": perm or "unknown", "perm_can_send": bool(d.perm_can_send),
        "perm_detail": d.perm_detail or "",
        "perm_checked_at": d.perm_checked_at.isoformat() if d.perm_checked_at else None,
        "member_status": eff_member,
        "member_label": _MEMBER_LABELS.get(eff_member, eff_member or "未知"),
        "active": eff_member not in ("left", "kicked"),
        "bot_username": getattr(d, "bot_username", "") or "",
        "last_update_id": int(getattr(d, "last_update_id", 0) or 0),
        "first_seen_at": d.first_seen_at.isoformat() if d.first_seen_at else None,
        "last_seen_at": d.last_seen_at.isoformat() if d.last_seen_at else None,
        "updated_at": d.updated_at.isoformat() if getattr(d, "updated_at", None) else None,
        "message_count": int(d.message_count or 0),
    }


@app.get("/api/discover/chats", dependencies=ADMIN)
def discover_chats(db: Session = Depends(get_db), chat_type: str = "",
                   bot_id: int | None = None, whitelisted: bool | None = None,
                   q: str = "", include_inactive: bool = True):
    """统一推送目标列表：发现的群 + 手动添加的频道/群。

    同时校正 in_whitelist——以目录路由表实际引用的 chat_id 为准（路由表是真相源），
    避免发现台账与白名单状态不一致。老库没有发现记录但路由里有目标的，这里不动
    （它们仍在「推送白名单（目录路由）」卡片里管理）。

    v0.9.15 #278：新增 chat_type / bot_id / whitelisted / q / include_inactive 筛选。
    """
    rows = db.scalars(select(DiscoveredChat).order_by(DiscoveredChat.last_seen_at.desc())).all()
    whitelisted_ids = {t["chat_id"] for t in bot_listener.push_routes(db)}
    bot_names = {b.id: b.name for b in db.scalars(select(Bot)).all()}
    out = []
    seen_chat_ids = set()
    # v0.9.18.1：「从列表移除」的 chat_id 集合——既不再展示，也绝不被白名单兜底
    # 重新落库（否则用户删除白名单里的频道后又被兜底加回，表现为"删不掉"）。
    removed_chat_ids = {str(d.chat_id).strip() for d in rows if getattr(d, "hidden", False)}
    for d in rows:
        # 被移除的目标：不展示、不校正白名单状态（白名单配置原样保留，推送不受影响）
        if getattr(d, "hidden", False):
            seen_chat_ids.add(str(d.chat_id).strip())
            continue
        actual = d.chat_id in whitelisted_ids
        if bool(d.in_whitelist) != actual:
            d.in_whitelist = actual
        # 默认隐藏已退出/被移出的目标；include_inactive=True 时全部返回由前端标记
        row = _discovered_row(d, bot_names.get(d.bot_id, ""))
        out.append(row)
        seen_chat_ids.add(str(d.chat_id).strip())
    # v0.9.16 #279：把「已在白名单/目录路由里配置、但发现台账里还没有记录」的目标
    # 真正落库为 DiscoveredChat（source=whitelist），赋真实主键——这样「检查权限 /
    # 测试推送 / 加入移出白名单 / 删除」都能按 id 操作，不再因虚拟 id=0 报「目标不存在」。
    # Telegram 无列群接口、历史消息不可回溯，这些 chat_id 是管理员已确认真实存在的目标；
    # 只补真实配置里的 chat_id，不虚构任何"扫描发现"数量。
    known_chats = _whitelist_configured_chats(db)
    main_bot_id = bot_listener.effective_bot_id(db)
    appended = False
    for ctype, chat_id, title in known_chats:
        if chat_id in seen_chat_ids:
            continue
        seen_chat_ids.add(chat_id)
        now = utcnow()
        # 目录路由的 target_name 缺省是 "route-<kind>:<chat_id>" 占位串，不当展示名
        clean_title = str(title or "").strip()
        if not clean_title or clean_title.startswith("route-") or clean_title == chat_id:
            clean_title = ""
        row_obj = DiscoveredChat(
            bot_id=main_bot_id, chat_id=chat_id, chat_type=ctype,
            title=clean_title[:256], username="", source="whitelist",
            in_whitelist=True, perm_status="unknown", perm_can_send=False,
            member_status="unknown", message_count=0,
            first_seen_at=now, last_seen_at=now, updated_at=now)
        db.add(row_obj)
        db.flush()  # 取真实主键，供本轮出参立即返回
        appended = True
        out.append(_discovered_row(row_obj, bot_names.get(main_bot_id, "")))
    if rows or appended:
        db.commit()
    # 筛选
    kw = (q or "").strip().lower()
    filtered = []
    for row in out:
        if chat_type and row["chat_type"] != chat_type:
            continue
        if bot_id is not None and int(row["bot_id"] or 0) != int(bot_id):
            continue
        if whitelisted is not None and bool(row["in_whitelist"]) != bool(whitelisted):
            continue
        if kw and kw not in (row["title"] or "").lower() and kw not in (row["chat_id"] or "").lower() \
                and kw not in (row["username"] or "").lower():
            continue
        if not include_inactive and not row["active"]:
            continue
        filtered.append(row)
    # v0.9.15 #276：附带主 Bot 轮询错误状态（409 冲突 / 429 限流），供前端提示。
    # v1.4.3：错误状态改内存态（轮询热路径不再写 app_config 抢锁），内存优先、DB 兜底。
    _main_bot_id = bot_listener.effective_bot_id(db)
    _poll_st = bot_listener.get_poll_state(_main_bot_id)
    if _poll_st.get("_seeded"):
        poll_err = _poll_st.get("error", "") or ""
    else:
        poll_err = runtime_config.get(db, f"bot_poll_error_{_main_bot_id or 'global'}") or ""
    return {"chats": filtered, "total": len(filtered), "all_count": len(out),
            "poll_error": poll_err, "main_bot_id": _main_bot_id}


class DiscoverRefreshInput(BaseModel):
    manual_chat_id: str = ""      # 手动输入 ID 或 @用户名
    manual_name: str = ""         # 展示名
    manual_kind: str = "group"    # group / channel


@app.post("/api/discover/refresh", dependencies=ADMIN)
async def discover_refresh(p: DiscoverRefreshInput = DiscoverRefreshInput(),
                           db: Session = Depends(get_db)):
    """触发一次「发现」：

    1) 主动拉一次 getUpdates（Telegram 无「列出已加入群」接口，只能从更新里看到群）；
       复用 poll_once 的 offset 与处理链，不丢消息、不重复处理。
    2) 若填了 manual_chat_id，手动登记一个目标（ID 或 @用户名/公开链接）。

    自动发现的前提：Bot 已在群里 + 群里产生新消息 + （若开了隐私模式）Bot 被设为管理员
    或有人 @Bot 发消息。否则 Telegram 不会把这些消息下发给 Bot。
    """
    token = bot_listener.effective_token(db)
    polled = {"enabled": False}
    if token:
        try:
            polled = await bot_listener.poll_once(db)
        except Exception as exc:
            polled = {"enabled": True, "error": str(exc)[:200]}
    added = None
    add_error = ""
    chat_id = str(p.manual_chat_id or "").strip()
    if chat_id:
        parsed = bot_listener.parse_chat_input(chat_id)
        if parsed["kind"] == "invite":
            add_error = parsed["error"]  # 私有邀请链接：明确提示，不解析不写入
        elif parsed["kind"] in ("invalid", "empty"):
            add_error = parsed["error"] or "输入格式无法识别。"
        elif token:
            # v0.9.15 #277：调 getChat 校验并取真实元数据（数字 ID / @用户名 / t.me 链接）
            resolved = await bot_listener.resolve_chat_via_getchat(db, token, parsed["chat_id"])
            if not resolved.get("ok"):
                add_error = resolved.get("error") or "Bot 无法访问该目标，请确认 Bot 已加入或用户名正确。"
            else:
                real = resolved["chat"]
                ctype = str(real.get("type") or "").strip()
                if ctype not in ("group", "supergroup", "channel"):
                    add_error = f"getChat 返回类型「{ctype or '未知'}」不是群组或频道，不能作为推送目标。"
                else:
                    bot_id = bot_listener.effective_bot_id(db)
                    row = db.scalar(select(DiscoveredChat).where(
                        DiscoveredChat.bot_id == bot_id, DiscoveredChat.chat_id == real["id"]))
                    if row is None:
                        now = utcnow()
                        row = DiscoveredChat(bot_id=bot_id, chat_id=real["id"],
                                             chat_type=ctype,
                                             title=(str(p.manual_name or "").strip() or real["title"])[:256],
                                             username=real["username"][:128], source="manual",
                                             first_seen_at=now, last_seen_at=now,
                                             message_count=0, member_status="member",
                                             updated_at=now)
                        db.add(row)
                    else:
                        row.source = "manual"
                        # v0.9.18.1：手动重新加入 → 恢复列表显示
                        row.hidden = False
                        row.last_seen_at = utcnow()
                        row.updated_at = utcnow()
                        row.chat_type = ctype
                        if real["username"]:
                            row.username = real["username"][:128]
                        if real["title"]:
                            row.title = real["title"][:256]
                        if str(row.member_status or "") in ("", "unknown"):
                            row.member_status = "member"
                    db.flush()
                    added = _discovered_row(row)
                    add_audit(db, "discover_manual_added", target_type="chat",
                              target_id=real["id"], detail=ctype)
        else:
            add_error = "未配置交互 Bot，无法通过 getChat 校验目标；请先在 Bot 管理配置并选用 Bot。"
    db.commit()
    chats = discover_chats(db)
    discovered_count = int(polled.get("discovered") or 0) if isinstance(polled, dict) else 0
    # v0.9.16 #279：诊断为什么"新增发现 0"——不虚构、不误导用户反复点刷新。
    poll_diagnosis = ""
    poll_err_kind = (polled.get("error_kind") or "") if isinstance(polled, dict) else ""
    poll_err = (polled.get("error") or "") if isinstance(polled, dict) else ""
    if discovered_count == 0 and not add_error:
        if not token:
            poll_diagnosis = "未配置交互 Bot，无法接收群消息事件。"
        elif poll_err_kind == "conflict":
            poll_diagnosis = "另一个实例正用同一 Token 取更新（409 冲突），本轮未拉取到事件。"
        elif poll_err:
            poll_diagnosis = f"轮询异常：{poll_err}"
        else:
            poll_diagnosis = ("Telegram 只下发「未确认的新事件」，后台每 10 秒已自动取走并处理；"
                              "历史群消息无法重放。要让 Bot 发现某群，请在群内 @Bot 发一条消息"
                              "（或把 Bot 设为管理员），随后等待约 10 秒；私有群请在群内发 /id。"
                              "已在白名单配置的目标会直接出现在下方列表。")
    return {"ok": True, "added": added, "add_error": add_error, "polled": polled,
            "chats": chats["chats"], "total": chats["total"],
            "diagnosis": poll_diagnosis,
            "hint": ("自动发现需 Bot 已入群且群内有新消息；若 Bot 开了隐私模式，"
                     "请设为群管理员或在群里 @Bot 发一条消息后再点发现。私有群请在群内发 /id 登记。")}


@app.post("/api/discover/chats/{cid}/permission", dependencies=ADMIN)
async def discover_check_permission(cid: int, db: Session = Depends(get_db)):
    """检查 Bot 在该群/频道是否有发送消息权限，并回写缓存。"""
    d = db.get(DiscoveredChat, cid)
    if not d:
        raise HTTPException(404, "目标不存在")
    token = bot_listener.effective_token(db)
    if not token:
        return {"ok": False, "status": "unknown", "can_send": False,
                "detail": "未配置交互 Bot Token，无法检查权限"}
    res = await bot_listener.check_chat_permission(db, token, d.chat_id, d.chat_type)
    d.perm_status = res["status"]
    d.perm_can_send = bool(res["can_send"])
    d.perm_detail = res.get("detail", "")
    d.perm_checked_at = utcnow()
    # v0.9.17 热修：权限检查发现 Bot 不在群时，同步成员状态为已退出，
    # 避免「成员状态=已加入、权限=Bot 不在群」自相矛盾；拿到真实 ChatMember.status 时回写。
    real_member_status = res.get("member_status") or ""
    if res["status"] == "not_member":
        d.member_status = "left"
    elif real_member_status in ("creator", "administrator", "member", "restricted", "left", "kicked"):
        d.member_status = real_member_status
    db.commit()
    return {"ok": res["ok"], "status": res["status"], "can_send": res["can_send"],
            "detail": res.get("detail", ""), "row": _discovered_row(d)}


class DiscoverWhitelistInput(BaseModel):
    in_whitelist: bool = True
    dir_id: str = ""      # 加入时绑定到的目录；空=整库通配


@app.post("/api/discover/chats/{cid}/whitelist", dependencies=ADMIN)
def discover_set_whitelist(cid: int, p: DiscoverWhitelistInput,
                           db: Session = Depends(get_db)):
    """勾选/取消勾选把目标加入或移出推送白名单（目录路由表）。

    加入：确保有启用 Channel（发送层落点），并向路由表追加一条 dir_id 目标；
    移出：从路由表移除该 chat_id 的所有目标、停用对应 Channel，并同步受影响记录。
    """
    d = db.get(DiscoveredChat, cid)
    if not d:
        raise HTTPException(404, "目标不存在")
    routes = bot_listener.push_routes_raw(db)
    chat_id = d.chat_id
    if p.in_whitelist:
        # v0.9.17.1 #283：明确已退出/不在群的目标禁止加入白名单（unknown 放行）
        inactive_reason = _inactive_discover_reason(d)
        if inactive_reason:
            raise HTTPException(400, inactive_reason)
        bot = _global_bot(db)
        if not bot:
            raise HTTPException(400, "请先在「交互 Bot 与推送配置」填写 Bot Token")
        # 已有则不重复追加
        kind = _discovered_kind(d)
        if not any(t["chat_id"] == chat_id for r in routes for t in r["targets"]):
            # v0.9.17 热修：优先把「整库通配（dir_id=""）」目标并入已有通配路由的
            # targets，而不是总 append 新行——路由表硬上限 10 条（push_routes_raw
            # 只 raw[:10]），满额时新增的第 11 条会被静默截断，导致白名单开关弹回。
            wildcard = next((r for r in routes if not str(r.get("dir_id") or "").strip()), None)
            new_target = {"kind": kind, "chat_id": chat_id, "name": d.title or ""}
            if wildcard is not None:
                wildcard.setdefault("targets", []).append(new_target)
                # 同步双字段（兼容旧前端），只在空时补
                if kind == "group" and not wildcard.get("group_id"):
                    wildcard["group_id"] = chat_id
                if kind == "channel" and not wildcard.get("channel_id"):
                    wildcard["channel_id"] = chat_id
            else:
                # 没有通配路由可并入时才会新增行；push_routes_raw 硬上限 10 条，
                # 满额新增会被静默截断致开关弹回——这里明确拒绝并给出可操作提示。
                if len(routes) >= 10:
                    raise HTTPException(400, "推送路由已达 10 条上限，无法新增；请先在「推送白名单（目录路由）」删除一条，或把某条路由的目录 ID 留空改为整库通配后再加入")
                routes.append({"dir_id": str(p.dir_id or "").strip(),
                               "channel_id": chat_id if kind == "channel" else "",
                               "group_id": chat_id if kind == "group" else "",
                               "targets": [new_target],
                               "note": d.title or "发现目标"})
        # 确保 Channel 落点
        ch = db.scalar(select(Channel).where(Channel.chat_id == chat_id).limit(1))
        if not ch:
            ch = Channel(bot_id=bot.id, name=f"{_ROUTE_CHANNEL_PREFIX}{kind}:{chat_id}",
                         chat_id=chat_id, kind=kind, enabled=True)
            db.add(ch)
        else:
            ch.enabled = True
            ch.bot_id = bot.id
        d.in_whitelist = True
        msg = "已加入推送白名单"
    else:
        # 从所有路由移除该 chat_id
        for r in routes:
            r["targets"] = [t for t in r["targets"] if t["chat_id"] != chat_id]
            if r["channel_id"] == chat_id:
                r["channel_id"] = next((t["chat_id"] for t in r["targets"] if t["kind"] == "channel"), "")
            if r["group_id"] == chat_id:
                r["group_id"] = next((t["chat_id"] for t in r["targets"] if t["kind"] == "group"), "")
        routes = [r for r in routes if r["targets"]]
        for ch in db.scalars(select(Channel).where(Channel.chat_id == chat_id)).all():
            ch.enabled = False
        d.in_whitelist = False
        msg = "已移出推送白名单"
    runtime_config.set_many(db, {"push_routes_json": json_dumps(routes)})
    add_audit(db, "discover_whitelist_toggled", target_type="chat", target_id=chat_id,
              detail=json_dumps({"in_whitelist": bool(p.in_whitelist)}))
    db.commit()
    return {"ok": True, "in_whitelist": bool(p.in_whitelist), "message": msg,
            "route_count": len(routes)}


@app.post("/api/discover/chats/{cid}/test", dependencies=ADMIN)
async def discover_test_push(cid: int, db: Session = Depends(get_db)):
    """向发现的群/频道直接发一条测试推送（复用现有 Bot 测试链）。"""
    d = db.get(DiscoveredChat, cid)
    if not d:
        raise HTTPException(404, "目标不存在")
    # v0.9.17.1 #283：明确不在群的目标直接给可操作提示，不发起注定失败的 Telegram 调用
    inactive_reason = _inactive_discover_reason(d)
    if inactive_reason:
        return {"ok": False, "message": inactive_reason}
    bot = _global_bot(db)
    if not bot:
        return {"ok": False, "message": "未配置交互 Bot，无法测试推送"}
    try:
        token = decrypt_secret(bot.token)
    except Exception as exc:
        return {"ok": False, "message": f"Bot Token 解密失败：{exc}"}
    if not token:
        return {"ok": False, "message": "Bot Token 为空，请先配置"}
    label = d.title or d.username or d.chat_id
    return await _send_test_message(token, d.chat_id, bot.name, label, db)


def _remove_discovered_chat(db: Session, cid: int) -> tuple[bool, str]:
    """v0.9.18.1：仅从「发现列表」移除目标——打 hidden 标记，绝不触碰三样东西：

      1) 白名单/目录路由（本项目推送是否加入）——原样保留；
      2) Channel（发送层落点）——不禁用、不删除；
      3) Telegram 里 Bot 实际加入的群/频道——本就无任何 Bot API 调用。

    同一 chat_id 可能有多行（不同 Bot，或 auto + whitelist 重复落库），全部一起
    hidden，避免删一行另一行又在列表里出现。有新事件/手动重新加入时清标记恢复显示。
    """
    d = db.get(DiscoveredChat, cid)
    if not d:
        return False, "目标不存在"
    chat_id = str(d.chat_id or "").strip()
    affected = 0
    for row in db.scalars(select(DiscoveredChat).where(
            DiscoveredChat.chat_id == chat_id)).all():
        if not getattr(row, "hidden", False):
            row.hidden = True
            affected += 1
    db.flush()
    add_audit(db, "discover_chat_removed_from_list", target_type="chat", target_id=chat_id,
              detail=json_dumps({"affected_rows": affected, "whitelist_untouched": True}))
    return True, chat_id


@app.delete("/api/discover/chats/{cid}", dependencies=ADMIN)
def discover_delete_chat(cid: int, db: Session = Depends(get_db)):
    """从发现列表移除目标（仅隐藏本条台账，不动白名单/Channel/Telegram）。"""
    ok, info = _remove_discovered_chat(db, cid)
    if not ok:
        raise HTTPException(404, info)
    db.commit()
    return {"ok": True, "chat_id": info,
            "message": "已从列表移除；不影响推送白名单与 Bot 已加入的群/频道，可刷新或手动重新加入"}


class DiscoverBatchDeleteInput(BaseModel):
    ids: list[int] = []


@app.post("/api/discover/chats/batch-delete", dependencies=ADMIN)
def discover_batch_delete(p: DiscoverBatchDeleteInput, db: Session = Depends(get_db)):
    """批量从发现列表移除（仅隐藏台账，不动白名单/Channel/Telegram）。"""
    removed, missing = 0, 0
    for cid in p.ids[:500]:
        ok, _info = _remove_discovered_chat(db, int(cid))
        if ok:
            removed += 1
        else:
            missing += 1
    db.commit()
    add_audit(db, "discover_batch_remove_from_list", target_type="chat",
              detail=json_dumps({"removed": removed, "missing": missing}))
    return {"ok": True, "removed": removed, "missing": missing}


class DiscoverBatchInput(BaseModel):
    ids: list[int] = []
    in_whitelist: bool = True
    dir_id: str = ""


@app.post("/api/discover/chats/batch-whitelist", dependencies=ADMIN)
def discover_batch_whitelist(p: DiscoverBatchInput, db: Session = Depends(get_db)):
    """v0.9.15 #278：批量加入/移出推送白名单，逐条复用单目标逻辑，单条失败不影响其它。"""
    results = []
    for cid in p.ids[:500]:
        try:
            res = discover_set_whitelist(
                cid, DiscoverWhitelistInput(in_whitelist=p.in_whitelist, dir_id=p.dir_id), db)
            results.append({"id": cid, "ok": bool(res.get("ok"))})
        except HTTPException as exc:
            results.append({"id": cid, "ok": False, "error": str(exc.detail)[:120]})
        except Exception as exc:
            results.append({"id": cid, "ok": False, "error": str(exc)[:120]})
    ok_count = sum(1 for r in results if r["ok"])
    add_audit(db, "discover_batch_whitelist", target_type="chat",
              detail=json_dumps({"count": len(results), "ok": ok_count,
                                 "in_whitelist": bool(p.in_whitelist)}))
    return {"ok": True, "total": len(results), "succeeded": ok_count, "results": results}


class DiscoverInviteInput(BaseModel):
    bot_id: int | None = None   # 指定 Bot id（多 Bot 独立生成）；留空=主交互 Bot


@app.post("/api/discover/invite-link", dependencies=ADMIN)
async def discover_invite_link(p: DiscoverInviteInput = DiscoverInviteInput(),
                               db: Session = Depends(get_db)):
    """v0.9.15 #276：生成 Telegram 官方 startgroup 深链，让用户把 Bot 加进群。

    Bot API 没有「让 Bot 主动加群」接口，邀请链接只对真人账号有效；唯一合规机制是
    https://t.me/<bot_username>?startgroup=<payload>，由用户在 Telegram 里选群添加。
    Bot 加入后由主 Bot 的 getUpdates 收到 my_chat_member 事件自动登记（不自动加白名单）。
    频道不走 startgroup，返回「添加为管理员」引导。

    payload 为短期随机标识（yg_<时间>_<随机>），不含 token 也不含敏感信息。
    """
    token = ""
    bot_label = ""
    bot_id = 0
    if p.bot_id:
        bot = db.get(Bot, int(p.bot_id))
        if not bot:
            raise HTTPException(404, "指定的 Bot 不存在")
        bot_id = bot.id
        bot_label = bot.name or f"Bot {bot.id}"
        try:
            token = decrypt_secret(bot.token) if bot.token else ""
        except Exception:
            token = ""
    else:
        token = bot_listener.effective_token(db)
        bot_id = bot_listener.effective_bot_id(db)
        bot_label = "主交互 Bot"
    if not token:
        return {"ok": False, "error": "未配置交互 Bot Token，无法生成授权链接。请先在 Bot 管理中添加并选用 Bot。"}

    username, err = await bot_listener.get_bot_username(db, token)
    if username:
        # v0.9.16 #278：getMe 成功即回写配置，前端「Bot 用户名」输入框自动回填，
        # 免管理员手工查抄；这是权威值（BotFather 设置），优先于人工填写兜底。
        runtime_config.set_many(db, {"interaction_bot_username": username})
    if not username:
        # 人工填写的 username 作为兜底（getMe 网络异常时仍可出链接）
        fallback = (runtime_config.get(db, "interaction_bot_username") or "").strip().lstrip("@")
        if fallback:
            username = fallback
            err = ""
    if not username:
        return {"ok": False, "error": err or "无法获取 Bot 用户名"}

    payload = bot_listener.new_startgroup_payload()
    url = bot_listener.build_startgroup_url(username, payload)
    if not url:
        return {"ok": False, "error": f"Bot 用户名「{username}」格式非法，无法生成授权链接。"}
    add_audit(db, "discover_invite_link", target_type="bot", target_id=str(bot_id or 0),
              detail=json_dumps({"bot_username": username}))
    db.commit()
    return {
        "ok": True, "bot_id": bot_id, "bot_name": bot_label, "bot_username": username,
        "startgroup_url": url, "payload": payload,
        "expires_at": (utcnow() + timedelta(hours=24)).isoformat(),
        "hint": ("点「加入群组」后在 Telegram 选择目标群即可把该 Bot 加入；成功后系统通过 "
                 "my_chat_member 事件自动登记群 ID / 名称 / 类型，不会自动加入推送白名单，"
                 "请确认后再勾选。若列表未刷新，进群发 /id 或在群里发一条消息后点「发现 / 刷新」。"),
        "channel_guide": [
            "在 Telegram 打开目标频道 → 频道右上角进入管理 → 管理员 → 添加管理员",
            f"搜索 @{username} 并添加，至少勾选「发布消息」权限（否则无法推送）",
            "添加后频道有新版时由 my_chat_member / channel_post 事件自动登记；需要推送时再勾选加入白名单",
        ],
    }


@app.post("/api/push/bot/token/clear", dependencies=ADMIN)
def push_bot_token_clear(db: Session = Depends(get_db)):
    """v0.9.6.1：清除全局单 Bot Token。

    留空保存 = 不改 Token（历史语义），所以删除必须走独立端点。
    只清空凭证并停用 Bot，保留 Bot 记录、目录路由、Channel、队列与互动配置；
    下次填入新 Token 会复用同一条 Bot，不会重复建记录或丢历史。
    """
    bot = _global_bot(db)
    if not bot:
        return {"ok": True, "cleared": False}
    bot.token = ""
    bot.enabled = False
    add_audit(db, "push_bot_token_cleared", target_type="config", target_id=str(bot.id))
    db.commit()
    return {"ok": True, "cleared": True,
            "bot": {"id": bot.id, "name": bot.name, "enabled": bot.enabled,
                    "token_masked": _mask_stored_token(bot.token)}}


class BotCommandsMenuInput(BaseModel):
    clear: bool = False


@app.post("/api/bot/commands/menu", dependencies=ADMIN)
async def bot_commands_menu(p: BotCommandsMenuInput = BotCommandsMenuInput(),
                            db: Session = Depends(get_db)):
    """v0.9.8：把 Telegram 私聊指令栏同步为当前项目指令，或置空（清除其它项目旧残留）。"""
    bot = _global_bot(db)
    if not bot or not bot.token:
        raise HTTPException(400, "尚未配置 Bot Token，请先填写并保存")
    try:
        token = decrypt_secret(bot.token)
    except Exception:
        token = bot.token
    out = await bot_listener.sync_bot_command_menu(db, token, clear=bool(p.clear))
    if not out.get("ok"):
        raise HTTPException(400, out.get("description") or "同步指令栏失败，请检查 Token 与网络")
    add_audit(db, "bot_commands_menu_synced", target_type="config", target_id=str(bot.id),
              detail="clear" if p.clear else "sync")
    db.commit()
    return {"ok": True, "cleared": bool(p.clear), "count": out.get("count", 0)}


@app.post("/api/tgto/test", dependencies=ADMIN)
async def tgto_test(db: Session = Depends(get_db)):
    try:
        return await tgto.test_connection(db)
    except Exception as exc:
        raise HTTPException(400, str(exc))


@app.post("/api/bot/poll", dependencies=ADMIN)
async def bot_poll_now(db: Session = Depends(get_db)):
    return await bot_listener.poll_once(db)


@app.get("/api/contributions", dependencies=ADMIN)
def contributions(status: str = "", limit: int = 100, db: Session = Depends(get_db)):
    stmt = select(UserContribution).order_by(UserContribution.id.desc()).limit(min(limit, 500))
    if status:
        stmt = stmt.where(UserContribution.status == status)
    rows = list(db.scalars(stmt).all())
    # 阶段三 A-01：一次批量取转存任务，替代逐条 restore_for（1+N → 2 次 SQL）
    restore_by_cid = contribution_restore.restore_map_for(db, [c.id for c in rows])
    return [{"id": c.id, "title": c.title, "content": c.content, "share_url": c.share_url,
             "tg_user_id": c.tg_user_id, "tg_username": c.tg_username,
             "tmdb_id": getattr(c, "tmdb_id", None), "media_type": getattr(c, "media_type", "") or "",
             "year": getattr(c, "year", "") or "",
             # v0.9.17 #281：同作品去重合并计数字段（老库迁移默认 1/空）
             "submit_count": int(getattr(c, "submit_count", 1) or 1),
             "dedup_key": getattr(c, "dedup_key", "") or "",
             "last_share_url": getattr(c, "last_share_url", "") or "",
             "last_tg_user_id": getattr(c, "last_tg_user_id", "") or "",
             "last_submitted_at": c.last_submitted_at.isoformat() if getattr(c, "last_submitted_at", None) else None,
             "status": c.status, "note": c.note,
             "created_at": c.created_at, "handled_at": c.handled_at,
             # v0.9.19：转存态与审核态分离——列表带出光鸭转存快照供后台展示
             "restore_status": r.status if r else None,
             "restore_status_label": contribution_restore.status_label(r.status) if r else "未转存",
             "restore_attempts": int(r.attempts or 0) if r else 0,
             "restore_error": r.error if r else "",
             "restore_verify_note": r.verify_note if r else "",
             "restored_at": r.restored_at.isoformat() if r and r.restored_at else None}
            for c in rows
            for r in [restore_by_cid.get(c.id)]]


class ContributionHandleInput(BaseModel):
    status: str = "approved"     # approved / rejected / transferred
    note: str = ""


@app.post("/api/contributions/{cid}/handle", dependencies=ADMIN)
async def handle_contribution(cid: int, p: ContributionHandleInput, db: Session = Depends(get_db)):
    """v0.9.19：审核态与转存态分离（光鸭原生 API，不经 TgtoDrive）。

    审核通过：先落审核状态并 commit（转存失败绝不回退审核态），再幂等建一条
    转存任务并即时执行一次；转存结果只更新转存任务，不改投稿 status。
    重复点「通过」不会新建任务、已成功不自动重跑；失败可手动重试。
    """
    if p.status not in ("approved", "rejected", "transferred", "pending"):
        raise HTTPException(400, "status 仅支持 approved/rejected/transferred/pending")
    c = db.get(UserContribution, cid)
    if not c:
        raise HTTPException(404, "投稿不存在")
    c.status, c.note, c.handled_at = p.status, p.note[:1000], utcnow()
    add_audit(db, "contribution_handled", target_type="contribution", target_id=str(cid),
              detail=json_dumps({"status": p.status, "note": p.note[:200]}))
    db.commit()  # 审核结论先独立落盘，后续转存成败与此无关
    notice = ""
    restore_result = ""         # 转存结果只回显给管理员，不写审核备注、不发给投稿用户
    restore_ok = False
    restore_status = None
    if p.status == "approved":
        notice = f"你的投稿「{c.title}」已通过审核，感谢支持！"
        # 幂等建任务 + 即时执行一次；失败留任务供后台或手动重试
        try:
            cfg_err = contribution_restore.config_error(db)
            if cfg_err:
                restore_result = cfg_err
                restore_ok, restore_status = False, "unconfigured"
            elif _restore_lock.locked():
                # 阶段四 B-02：后台 job / 手动重试正占用转存，审核态已落盘不受影响，
                # 跳过即时执行，留待后台续跑，避免同分享被并发提交两次。
                restore_result = "另一次转存正在进行，本次跳过即时执行，后台将自动续跑"
                restore_ok, restore_status = False, "deferred"
            else:
                async with _restore_lock:
                    restore_ok, restore_result, rrow = await contribution_restore.run_restore_for_contribution(db, cid)
                restore_status = rrow.status if rrow else None
                db.commit()
        except Exception as exc:
            db.rollback()
            restore_result = f"转存执行异常：{type(exc).__name__}: {str(exc)[:160]}"
            restore_ok, restore_status = False, "failed"
        add_audit(db, "contribution_restore_approved", target_type="contribution",
                  target_id=str(cid), detail=json_dumps({"ok": restore_ok, "info": restore_result[:200]}))
        db.commit()
    elif p.status == "rejected":
        notice = f"你的投稿「{c.title}」未通过审核。" + (f"原因：{p.note}" if p.note else "")
    elif p.status == "transferred":
        notice = f"你的投稿「{c.title}」已转存入库，感谢支持！"
    notified = False
    if notice and c.tg_user_id:
        notified = await bot_listener.notify_user(db, c.tg_user_id, notice)
    # 兼容旧字段 forwarded/forward_info，语义改为「光鸭转存」
    return {"ok": True, "notified": notified,
            "forwarded": restore_ok, "forward_info": restore_result,
            "restore_ok": restore_ok, "restore_result": restore_result,
            "restore_status": restore_status}


@app.get("/api/contributions/{cid}/restore", dependencies=ADMIN)
def contribution_restore_detail(cid: int, db: Session = Depends(get_db)):
    """查看某投稿的光鸭转存任务与历史记录（当前一条任务 + 审计留痕）。"""
    if not db.get(UserContribution, cid):
        raise HTTPException(404, "投稿不存在")
    row = contribution_restore.restore_for(db, cid)
    if not row:
        return {"exists": False}
    history = db.scalars(select(AuditLog).where(
        AuditLog.target_type == "contribution", AuditLog.target_id == str(cid),
        AuditLog.action.like("contribution_restore%")).order_by(AuditLog.id.desc()).limit(50)).all()
    return {"exists": True,
            "status": row.status, "status_label": contribution_restore.status_label(row.status),
            "attempts": int(row.attempts or 0), "error": row.error,
            "verify_note": row.verify_note, "last_response": row.last_response,
            "created_at": row.created_at, "restored_at": row.restored_at,
            "updated_at": row.updated_at,
            "history": [{"action": a.action, "detail": a.detail,
                         "ts": a.ts.isoformat() if getattr(a, "ts", None) else None} for a in history]}


@app.post("/api/contributions/{cid}/restore/retry", dependencies=ADMIN)
async def contribution_restore_retry(cid: int, db: Session = Depends(get_db)):
    """管理员手动重试失败的转存任务；已成功的不自动重复转存。"""
    c = db.get(UserContribution, cid)
    if not c:
        raise HTTPException(404, "投稿不存在")
    cfg_err = contribution_restore.config_error(db)
    if cfg_err:
        raise HTTPException(400, cfg_err)
    if not contribution_restore.is_guangya_share_url(c.share_url or ""):
        raise HTTPException(400, "投稿链接非光鸭分享链接，无法重试转存")
    # 阶段四 B-02：与后台 job / 审核通过入口共用同一把锁，防同分享被重复提交转存
    if _restore_lock.locked():
        raise HTTPException(409, "另一次转存正在进行，请稍后重试")
    try:
        async with _restore_lock:
            ok, info, row = await contribution_restore.retry_restore(db, cid)
        db.commit()
    except HTTPException:
        raise
    except Exception as exc:
        db.rollback()
        raise HTTPException(502, f"重试转存异常：{type(exc).__name__}: {str(exc)[:160]}")
    add_audit(db, "contribution_restore_retry", target_type="contribution",
              target_id=str(cid), detail=json_dumps({"ok": ok, "info": info[:200]}))
    db.commit()
    return {"ok": ok, "info": info,
            "restore_status": row.status if row else None,
            "restore_status_label": contribution_restore.status_label(row.status) if row else None}


@app.delete("/api/contributions/{cid}", dependencies=ADMIN)
def delete_contribution(cid: int, db: Session = Depends(get_db)):
    """v0.9.10：删除投稿登记行（不动用户、不动 TgtoDrive，纯管理侧清理）。"""
    c = db.get(UserContribution, cid)
    if not c:
        raise HTTPException(404, "投稿不存在")
    # v0.9.19：连带清理转存任务（审核/转存都属这条投稿）
    rrow = contribution_restore.restore_for(db, cid)
    if rrow:
        db.delete(rrow)
    db.delete(c)
    add_audit(db, "contribution_deleted", target_type="contribution", target_id=str(cid))
    db.commit()
    return {"ok": True}


@app.get("/api/media-requests", dependencies=ADMIN)
def media_requests(limit: int = 100, db: Session = Depends(get_db)):
    rows = db.scalars(select(MediaRequest).order_by(
        MediaRequest.id.desc()).limit(min(limit, 500))).all()
    return [{"id": r.id, "title": r.title, "tmdb_id": r.tmdb_id, "media_type": r.media_type,
             "year": getattr(r, "year", "") or "",
             "season": r.season, "keyword_id": r.keyword_id,
             "status": r.status,  # TgtoDrive 同步状态（管理员内部排查用，不当业务态展示）
             "review_status": getattr(r, "review_status", "pending") or "pending",
             "review_note": getattr(r, "review_note", "") or "",
             "sync_note": r.note or "",  # 后台可见的 TgtoDrive 失败原因
             "tg_user_id": r.tg_user_id, "tg_username": r.tg_username,
             # v0.9.17.1 #284：同作品求片合并计数字段（老库迁移默认 1/空）
             "submit_count": int(getattr(r, "submit_count", 1) or 1),
             "last_tg_user_id": getattr(r, "last_tg_user_id", "") or "",
             "last_tg_username": getattr(r, "last_tg_username", "") or "",
             "last_submitted_at": r.last_submitted_at.isoformat() if getattr(r, "last_submitted_at", None) else None,
             "created_at": r.created_at, "handled_at": getattr(r, "handled_at", None)}
            for r in rows]


class MediaRequestInput(BaseModel):
    title: str
    tmdb_id: int | None = None
    media_type: str = "tv"
    season: int | None = None


@app.post("/api/media-requests", dependencies=ADMIN)
async def add_media_request(p: MediaRequestInput, db: Session = Depends(get_db)):
    title = (p.title or "").strip()
    if not title:
        raise HTTPException(400, "请填写影视名称")
    # v0.9.17.1 #284：同作品去重合并——后台重复加入时不再重复写白名单，只累加计数。
    row, created = bot_listener.upsert_media_request(
        db, tg_user_id="", tg_username="", title=title,
        tmdb_id=p.tmdb_id, media_type=p.media_type, season=p.season)
    if created:
        try:
            r = await tgto.upsert_keyword(db, title=title, tmdb_id=p.tmdb_id,
                                          media_type=p.media_type, season=p.season)
            row.status = "synced" if r.get("ok") else "failed"
            row.note = (r.get("detail") or r.get("message") or r.get("status") or "")[:300]
            if r.get("ok"):
                row.synced_at = utcnow()
        except Exception as exc:
            row.status, row.note = "failed", str(exc)[:300]
    db.commit()
    return {"ok": True, "id": row.id, "status": row.status, "keyword_id": row.keyword_id,
            "note": row.note, "created": created, "submit_count": int(row.submit_count or 1)}


class MediaRequestHandleInput(BaseModel):
    review_status: str = "adopted"   # adopted / rejected / pending
    note: str = ""                    # 拒绝原因，可选，会反馈给提交用户


@app.post("/api/media-requests/{rid}/handle", dependencies=ADMIN)
async def handle_media_request(rid: int, p: MediaRequestHandleInput,
                               db: Session = Depends(get_db)):
    """v0.9.8：求片后台审核——采纳 / 拒绝（可选填原因反馈用户）/ 重置待处理。

    审核态 review_status 与 TgtoDrive 同步态 status 解耦，列表不再把 failed 当业务态。
    """
    if p.review_status not in ("adopted", "rejected", "pending"):
        raise HTTPException(400, "review_status 仅支持 adopted/rejected/pending")
    r = db.get(MediaRequest, rid)
    if not r:
        raise HTTPException(404, "求片记录不存在")
    r.review_status = p.review_status
    r.review_note = p.note[:1000]
    r.handled_at = utcnow()
    add_audit(db, "media_request_handled", target_type="media_request", target_id=str(rid),
              detail=json_dumps({"review_status": p.review_status, "note": p.note[:200]}))
    db.commit()  # 先落审核态，再做外部网络调用，绝不跨 await 持写事务

    # v0.9.17 #280：审核通过才把影视信息 + a/b/c 字段增量写入 TgtoDrive 白名单；
    # 拒绝时若此前已同步，则联动移除本项目规则。任何失败/冲突都把真实原因写进 note
    # 并随回执返回，杜绝"前端说已通知、用户却发现参数没传进 TgtoDrive"的黑洞。
    sync_info = ""
    sync_ok = False
    applied = None
    if p.review_status == "adopted":
        try:
            res = await tgto.upsert_keyword(
                db, title=r.title, tmdb_id=r.tmdb_id,
                media_type=r.media_type or "tv", season=r.season, year=r.year or "")
            sync_ok = bool(res.get("ok"))
            applied = res.get("applied")
            r.status = "synced" if sync_ok else "failed"
            detail = res.get("detail") or res.get("message") or res.get("status") or ""
            r.note = (str(detail) + (f" | 规则ID={res.get('rule_id')}" if res.get("rule_id") else ""))[:500]
            if sync_ok:
                r.synced_at = utcnow()
            sync_info = (f"白名单{res.get('status', 'ok')}" if sync_ok
                         else f"白名单{res.get('status', 'failed')}未写入：{detail}")
        except Exception as exc:
            sync_ok = False
            r.status, r.note = "failed", f"白名单同步异常：{str(exc)[:300]}"
            sync_info = r.note
        db.commit()
    elif p.review_status == "rejected" and r.status == "synced":
        try:
            await tgto.remove_keyword(db, tmdb_id=r.tmdb_id, season=r.season, title=r.title)
            r.status, r.note = "pending", "审核拒绝，已移除白名单规则"
        except Exception as exc:
            r.note = f"拒绝后移除白名单失败：{str(exc)[:200]}"
        db.commit()

    notice = ""
    if p.review_status == "adopted":
        notice = f"你求片的「{r.title}」已采纳，我们会尽快安排，感谢支持！"
    elif p.review_status == "rejected":
        notice = f"你求片的「{r.title}」暂不满足条件。" + (f"原因：{p.note}" if p.note else "")
    notified = False
    if notice and r.tg_user_id:
        notified = await bot_listener.notify_user(db, r.tg_user_id, notice)
    return {"ok": True, "notified": notified, "review_status": r.review_status,
            "sync_ok": sync_ok, "sync": sync_info or "ok",
            "sync_note": r.note or "", "applied": applied}


@app.delete("/api/media-requests/{rid}", dependencies=ADMIN)
async def delete_media_request(rid: int, db: Session = Depends(get_db)):
    """v0.9.13 #254：删除求片记录，并联动移除本项目写入的白名单规则。

    只删本项目 source=ya-tg 且 id 匹配的规则，绝不覆盖/清空 TgtoDrive 原有关键词；
    联动失败只记日志，不阻断删除主流程。
    """
    r = db.get(MediaRequest, rid)
    if not r:
        raise HTTPException(404, "求片记录不存在")
    tmdb_id, season, title, synced = r.tmdb_id, r.season, r.title, r.status == "synced"
    db.delete(r)
    add_audit(db, "media_request_deleted", target_type="media_request", target_id=str(rid))
    db.commit()
    removed = False
    if synced:
        try:
            res = await tgto.remove_keyword(db, tmdb_id=tmdb_id, season=season, title=title)
            removed = bool(res.get("changed"))
        except Exception:
            pass  # 联动失败只记日志，删除主流程不受影响
    return {"ok": True, "keyword_removed": removed}


@app.get("/api/interaction/status", dependencies=ADMIN)
def interaction_status(db: Session = Depends(get_db)):
    return {
        "contributions": db.query(UserContribution).count(),
        "contributions_pending": db.query(UserContribution).filter(
            UserContribution.status == "pending").count(),
        "media_requests": db.query(MediaRequest).count(),
        "keyword_synced": db.query(MediaRequest).filter(MediaRequest.status == "synced").count(),
        "contribution_enabled": (runtime_config.get(db, "contribution_enabled") or "").lower() == "true",
        "tgto_configured": bool(runtime_config.get(db, "tgto_base_url") and
                                runtime_config.get(db, "tgto_api_key")),
        "unverified_endpoints": ["投稿分享转存（光鸭接口，8.6 待验证）"],
    }


def safe_static_file(path: str, root: Path) -> Path | None:
    """C-01 修复：返回 root 目录内且真实存在的文件；越界/不存在返回 None。

    SPA catch-all 必须把请求路径 realpath 规范化后校验仍落在静态目录内，
    否则 /../db/ya-tg.db、/%2e%2e/... 等未规范化原始路径可读取静态目录外的文件。
    抽成模块级纯函数以便单元测试直接覆盖（无需 /app/static 存在）。
    """
    if not path:
        return None
    try:
        resolved_root = root.resolve()
        candidate = (root / path).resolve()
    except (OSError, RuntimeError):
        return None
    # realpath 后必须仍在静态根内（含根本身），阻断 .. / 符号链接穿越
    if candidate != resolved_root and resolved_root not in candidate.parents:
        return None
    return candidate if candidate.is_file() else None


static = Path("/app/static")
if static.exists():
    assets = static / "assets"
    if assets.exists():
        app.mount("/assets", StaticFiles(directory=assets), name="assets")

    @app.get("/{path:path}")
    def spa(path: str):
        return FileResponse(safe_static_file(path, static) or static / "index.html")
