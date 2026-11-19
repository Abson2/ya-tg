import html
import json
import logging
import re
import threading
import time
from contextlib import asynccontextmanager, contextmanager
from datetime import timedelta
from pathlib import Path

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

from . import (ai_assist, bot_listener, guangya_auth, guangya_sms, log_retention,
               mirror, runtime_config, tgto, tmdb)
from .config import VALID_DURATIONS, settings
from .db import DEFAULT_TEMPLATE, TEMPLATE_PRESETS, Base, SessionLocal, get_db, json_dumps, migrate, engine
from .guangya import GuangYaError, GuangYaOpenAPI, GuangYaShareClient, GuangYaWebFiles, WebFileError
from .models import (AuditLog, AuthToken, Bot, Channel, DistributionEvent,
                     MediaRequest, MessageTemplate, MirrorDir, MirrorFile, MirrorMedia, MirrorRule,
                     MirrorTask, QueueItem, Resource, ResourceScanBatch, ResourceVersion, Rule,
                     ScanCursor, ScanTask, UserContribution)
from .publisher import ensure_share, process_queue
from .scanner import run_scan
from .security import decrypt_secret, encrypt_secret, mask_secret
logger = logging.getLogger(__name__)

from .utils import (as_utc, is_video_file, normalize_work_name, parse_media_name,
                    retry_after_seconds, standard_media_name, star_media_name, utcnow,
                    work_episode_numbers, work_group_key)

scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")


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
    """
    STALE_SECONDS = 30 * 60  # 镜像长剧 + 改名最多几分钟；30 分钟仍持锁必属异常残留

    def __init__(self):
        self._lock = threading.Lock()
        self._owner = ""
        self._since = 0.0

    def acquire(self, blocking: bool = False, owner: str = "") -> bool:
        if self._lock.acquire(blocking=blocking):
            self._owner = owner or "镜像任务"
            self._since = time.monotonic()
            return True
        # 拿不到锁：若当前持有已超时，判定为残留，强制解绑（不 release 原锁，避免
        # 真持有者稍后 release 时误放别人的锁；用代数版本号隔离，见 release）。
        if self._held_seconds() > self.STALE_SECONDS:
            logger.warning("[mirror] 镜像锁持有超过 %ss（owner=%s），判定残留并强制放行",
                           self.STALE_SECONDS, self._owner)
            self._owner, self._since = "", 0.0
            self._lock.release()  # 残留场景：原持有者已不存在，安全释放
            self._owner = "镜像任务(残留回收)"
            self._since = time.monotonic()
            return True
        return False

    def release(self) -> None:
        self._owner, self._since = "", 0.0
        try:
            self._lock.release()
        except RuntimeError:
            pass  # 未被持有时重复 release，忽略

    def set_owner(self, owner: str) -> None:
        """拿锁后补充持有者名称（保留 acquire(blocking=False) 原字面调用，兼容旧契约测试）。"""
        if self._since:
            self._owner = owner

    def _held_seconds(self) -> float:
        return max(0.0, time.monotonic() - self._since) if self._since else 0.0

    def info(self) -> dict:
        held = self._lock.locked()
        return {"busy": held, "owner": self._owner if held else "",
                "held_seconds": int(self._held_seconds()) if held else 0}


_mirror_lock = _TimedMirrorLock()


def add_audit(db: Session, action: str, target_type: str = "", target_id: str = "", detail: str = "") -> None:
    db.add(AuditLog(action=action, target_type=target_type, target_id=str(target_id), detail=detail))


def check_admin(x_admin_password: str | None = Header(default=None)):
    if settings.admin_password == "CHANGE_ME":
        raise HTTPException(503, "请先在 .env 设置 ADMIN_PASSWORD")
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


async def _job_bot_poll():
    """v0.8.0：投稿/我想看 Bot 消息轮询（Telegram 官方 getUpdates）。"""
    with SessionLocal() as db:
        try:
            await bot_listener.poll_once(db)
        except Exception:
            db.rollback()


async def _job_log_prune():
    """v0.8.3：每日按保留策略自动清理日志与历史留痕。失败只记日志，不影响其它任务。"""
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


async def _job_rename_pending():
    """v0.9.2：后台复查「B 库暂未可见」的单文件改名，不复制、不入库、不推送。

    复制任务完成后网盘文件可见可能有秒级延迟。改名只认只读列目录命中的 fileId，
    每次仍走 file_idle_seconds 风控间隔与 rename 预算；拿不到镜像互斥锁就跳过，
    不与「立即同步 / 手动改名」抢 SQLite 写锁。
    """
    if not _mirror_lock.acquire(blocking=False):
        return
    _mirror_lock.set_owner("后台改名复查")
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
            out = await _safe_rename(db, r, only_pending=True, max_media=5,
                                     max_files=10, max_seconds=20)
            db.commit()
            if out.get("renamed") or out.get("waiting_visible"):
                logger.info("[mirror] 待改名复查：改名 %s / 仍待可见 %s",
                            out.get("renamed", 0), out.get("waiting_visible", 0))
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
    """
    with SessionLocal() as db:
        try:
            # v0.8.1：每次执行都重新取 B 镜像库目录，用户在镜像设置里改 ID 后立即生效
            effective, _ = _effective_scan_root(db)
            await run_scan(effective, resume=False)
            await _post_scan_identify(db, publish_after_identify=True)
            process_change_events(db, publish=True)
            db.commit()
        except Exception:
            db.rollback()


@asynccontextmanager
async def lifespan(app: FastAPI):
    migrate()
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
    # 镜像定时任务热注册；v0.8.5 起任务内部按 auto_identify 接「入库→识别→推送」，
    # 推送走分发规则与作品级去重，不单独注册群发任务
    if mirror_enabled:
        try:
            _schedule_mirror_job(mirror_minutes, True)
        except Exception:
            pass
    if enabled:
        try:
            scheduler.add_job(_scheduled_scan, CronTrigger.from_crontab(cron, timezone="Asia/Shanghai"),
                              args=[root], id="global_scan", replace_existing=True,
                              max_instances=1, coalesce=True)
        except Exception:
            pass
    _schedule_queue_worker(queue_trigger)
    scheduler.add_job(_job_token_refresh, "interval", seconds=60, id="token_refresh",
                      max_instances=1, coalesce=True)
    scheduler.add_job(_job_bot_poll, "interval", seconds=10, id="bot_poll",
                      max_instances=1, coalesce=True)
    scheduler.add_job(_job_rename_pending, "interval", seconds=120, id="rename_pending",
                      max_instances=1, coalesce=True)
    scheduler.add_job(_job_log_prune,
                      CronTrigger.from_crontab("30 3 * * *", timezone="Asia/Shanghai"),
                      id="log_prune", replace_existing=True,
                      max_instances=1, coalesce=True)
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="Ya-TG", version="0.9.7", lifespan=lifespan)
ADMIN = [Depends(check_admin)]


@app.get("/api/health")
def health():
    return {"ok": True, "timezone": "Asia/Shanghai", "version": "0.9.7", "app": "Ya-TG"}


@app.get("/api/overview", dependencies=ADMIN)
def overview(db: Session = Depends(get_db)):
    counts = {k: db.scalar(select(func.count(getattr(m, "id")))) or 0 for k, m in
              {"resources": Resource, "bots": Bot, "channels": Channel, "rules": Rule}.items()}
    for status in ("pending", "review", "retry", "failed", "sent"):
        counts[status] = db.scalar(select(func.count(QueueItem.id)).where(QueueItem.status == status)) or 0
    counts["auth"] = "ok" if (db.get(AuthToken, 1) and db.get(AuthToken, 1).access_token_enc) else "unauthorized"
    return counts


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
                          max_instances=1, coalesce=True)
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


def _schedule_mirror_job(minutes: int, enabled: bool) -> None:
    if scheduler.get_job("mirror_sync"):
        scheduler.remove_job("mirror_sync")
    if enabled:
        scheduler.add_job(_scheduled_mirror,
                          IntervalTrigger(minutes=max(5, int(minutes)), timezone="Asia/Shanghai"),
                          id="mirror_sync", replace_existing=True, max_instances=1, coalesce=True)


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
    ident = await _post_scan_identify(db, publish_after_identify=True, limit=500)
    out["identify"] = ident
    out["queued"] = int(ident.get("queued", 0) or 0)
    # 只读统计，绝不在这里处理：内容变化事件仍由常规扫描链路消化
    out["changes_pending"] = int(db.scalar(
        select(func.count()).select_from(DistributionEvent).where(
            DistributionEvent.queue_id == 0)) or 0)
    return out


async def _scheduled_mirror() -> None:
    """定时镜像：同步 + 补映射；按 auto_identify 决定是否刷新入库并推送本轮新识别影视。

    v0.8.5 起不再写死「镜像完成不推 TG」：识别成功（拿到海报）的影视按分发规则推送。
    仍不对全库群发，也不批量补推历史内容变化事件。
    v0.8.9：与手动同步共用互斥锁，手动任务在跑时直接跳过本轮，不叠加长任务。
    """
    if not _mirror_lock.acquire(blocking=False):
        logger.info("[mirror] 已有同步 / 改名任务运行，跳过本轮定时镜像")
        return
    _mirror_lock.set_owner("定时镜像")
    try:
        with SessionLocal() as db:
            try:
                r = db.scalar(select(MirrorRule).where(MirrorRule.enabled.is_(True)).limit(1))
                if r and r.source_root_id and r.target_root_id:
                    await mirror.sync_rule(db, r)
                    await mirror.backfill_target_file_ids(db, r)
                    # v0.8.6：改名必须赶在「扫描入库」之前，否则 Resource 记的是旧中文名。
                    # 只改 B 库单集/单电影文件名，A 库与 B 库目录名都不动。
                    # v0.8.9：TMDB 查不到英文名会快速直译兜底，不阻断下面的入库/推送。
                    if bool(getattr(r, "rename_files", True)):
                        await _safe_rename(db, r)
                    if bool(getattr(r, "auto_identify", True)):
                        p = await _mirror_post_sync_pipeline(db, r)
                        db.commit()
                        # 追加而非覆盖：v0.8.4 的文件级复制错误就写在 last_message
                        tail = (f" | [自动入库] 扫 {p['files']} 文件 · 识别 "
                                f"{p['identify']['identified']} · 入队 {p['queued']}"
                                f" · 内容变化待处理 {p['changes_pending']}")
                        r.last_message = (str(r.last_message or "") + tail)[-1000:]
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
        "interval_presets": MIRROR_INTERVAL_PRESETS,
        "last_run": r.last_run if r else None,
        "last_status": r.last_status if r else "never",
        "last_message": r.last_message if r else "",
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
    add_audit(db, "mirror_config_saved", target_type="mirror",
              detail=json_dumps({"source": r.source_root_id, "target": r.target_root_id,
                                 "interval": r.interval_minutes,
                                 "idle": r.file_idle_seconds, "auto_dirs": r.auto_create_dirs,
                                 "rename_files": r.rename_files,
                                 "max_works_per_run": r.max_works_per_run}))
    db.commit()
    _schedule_mirror_job(r.interval_minutes, r.enabled)
    return {"ok": True, "enabled": r.enabled, "interval_minutes": r.interval_minutes}


@contextmanager
def _mirror_guard():
    """v0.8.9：镜像同步 / 改名互斥，拿不到锁立刻 409，不让长任务排队堆积。

    用户等不及连点「立即同步」时，多个分钟级任务并发会互相抢 SQLite 写锁、
    成倍放大列目录与 rename 请求，管理页所有请求排队到 30 秒 busy_timeout，
    表现就是「整理剧集改名时后台页面卡死打不开」。宁可明确拒绝，也不排队。
    """
    if not _mirror_lock.acquire(blocking=False):
        info = _mirror_lock.info()
        owner = info.get("owner") or "未知任务"
        held = info.get("held_seconds", 0)
        # v0.9.6：把占用者说清楚——多数「假 409」是后台改名复查（每 120 秒静默跑）
        # 正持锁，界面看不到它，用户误以为「没有任务在跑」。给出占用者与时长，
        # 超过阈值的残留由 _TimedMirrorLock.acquire 自动回收，这里只报真实在跑的。
        raise HTTPException(
            409, f"已有镜像同步 / 改名任务「{owner}」正在运行（已持续约 {held} 秒）；"
                 "后台改名复查已限制为短任务（最多约 20 秒），稍候重试即可；"
                 f"若长时间（>30 分钟）仍报此错，重启容器即可清除残留锁。")
    _mirror_lock.set_owner("手动同步 / 改名")
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
        msg = f"{type(exc).__name__}: {exc}"[:300]
        return {"works": 0, "renamed": 0, "skipped": 0, "ascii": 0,
                "untranslated": 0, "failed": 0, "no_rows": 0,
                "literal_ai": 0, "literal_pinyin": 0,
                "errors": [f"文件改名未执行：{msg}"], "notices": [],
                "remaining": 0, "truncated_reason": "",
                "untranslated_works": [], "idle_seconds": 0}


async def _do_mirror_run(post_identify: bool | None, db: Session) -> dict:
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
        stats = await mirror.sync_rule(db, r)
        stats["target_file_ids_backfilled"] = await mirror.backfill_target_file_ids(db, r)
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
        if do_pipeline:
            # v0.8.9：找不到官方英文名的作品在改名环节已直接直译兜底，绝不在这里
            # 再补跑一次解析（会重复触发 TMDB / AI 网络等待）。复制 → 入库 →
            # 识别 → 分享/推送 主链路必须照常走完，改名只是可选优化。
            stats["pipeline"] = await _mirror_post_sync_pipeline(db, r)
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
            db.commit()
        raise HTTPException(502, f"镜像同步失败：{detail}")


@app.post("/api/mirror/run", dependencies=ADMIN)
async def mirror_run(post_identify: bool | None = None, db: Session = Depends(get_db)):
    """v0.8.9：立即同步入口加互斥；重复点击直接 409，不让长任务排队堆积。"""
    with _mirror_guard():
        return await _do_mirror_run(post_identify, db)


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
        add_audit(db, "mirror_files_renamed", target_type="mirror",
                  detail=json_dumps({"renamed": out["renamed"], "failed": out["failed"],
                                     "untranslated": out["untranslated"], "force": force}))
        db.commit()
        return out
    except WebFileError as exc:
        db.rollback()
        raise HTTPException(400, str(exc))
    except Exception as exc:
        db.rollback()
        raise HTTPException(502, f"文件改名失败：{type(exc).__name__}: {exc}")


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
    清单消失；日后点「重新解析英文名并改名」(force=True) 会清掉标记重新解析，可随时恢复。
    """
    rows = list(db.scalars(select(MirrorMedia).where(
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
    return {"ok": True, "cleared": cleared}


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


@app.post("/api/mirror/mappings/prune", dependencies=ADMIN)
async def mirror_mappings_prune(dry_run: bool = False, db: Session = Depends(get_db)):
    """v0.8.9：清理 B 库中已不存在的文件 / 目录映射（管理页「清理无效映射」）。

    只读核对 B 库实际内容，列目录不可信或整部作品为空时一律跳过，绝不误清。
    默认真正执行；`dry_run=true` 只返回清单不写库，供用户先预览。
    """
    with _mirror_guard():
        r = _mirror_rule(db)
        if not r or not (r.target_root_id or "").strip():
            raise HTTPException(400, "请先在镜像设置里填写 B 镜像库目录 ID")
        stats = await mirror.prune_stale_mappings(db, r, dry_run=dry_run)
        if not dry_run:
            add_audit(db, "mirror_mappings_prune", target_type="mirror",
                      detail=json_dumps({
                          "files_pruned": stats["files_pruned"],
                          "dirs_pruned": stats["dirs_pruned"],
                          "media_skipped_unverified": stats["media_skipped_unverified"]}))
            db.commit()
        return stats


@app.post("/api/mirror/mappings/clear-pruned", dependencies=ADMIN)
def mirror_mappings_clear_pruned(db: Session = Depends(get_db)):
    """v0.9.6：彻底删除「已清理待重建」的僵尸目录映射行。

    这些行是之前「清理无效映射」(prune) 把 B 库已不存在的目录置 missing 留下的，
    error 带 [pruned] 标记。它们在「待创建目录」里已被排除，本端点供用户在确认后
    物理删除，让「目录映射清单」恢复干净。安全前提：A 库是权威源，若目录仍在，
    下轮「立即同步」会经 _record_dir 按 source_dir_id 原样重建；不会删资源 / 分享 / 队列。
    只删带 [pruned] 标记的 missing 行，不碰真正建目录失败的 missing 与任何正常映射。
    """
    rows = list(db.scalars(select(MirrorDir).where(MirrorDir.status == "missing")).all())
    targets = [d for d in rows if mirror.is_pruned_dir_error(getattr(d, "error", ""))]
    removed = len(targets)
    for d in targets:
        db.delete(d)
    if removed:
        add_audit(db, "mirror_mappings_clear_pruned", target_type="mirror",
                  detail=json_dumps({"removed": removed}))
    db.commit()
    return {"ok": True, "removed": removed}


@app.get("/api/mirror/status", dependencies=ADMIN)
def mirror_status(db: Session = Depends(get_db)):
    r = _mirror_rule(db)
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
    rename_errors = [{"id": f.id, "work": titles.get(f.media_id, ""),
                      "file": f.file_name, "target": f.target_name or "",
                      "error": f.rename_error or ""} for f in rename_failed]
    # v0.9.6：排除用户已「清空 / 忽略」的待翻译项（english_source=ignored）。
    # 这些作品英文名为空但已被用户确认跳过，不再占用待处理清单；点「重新解析英文名并改名」
    # (force) 会清掉 ignored 标记重新走 TMDB→AI→拼音链路，可随时恢复。
    no_english = list(db.scalars(select(MirrorMedia).where(
        or_(MirrorMedia.english_title.is_(None), MirrorMedia.english_title == ""),
        or_(MirrorMedia.english_source.is_(None), MirrorMedia.english_source != "ignored")
    ).order_by(MirrorMedia.id.desc()).limit(50)).all())
    untranslated_works = [{"work": m.title_standard,
                           "reason": "尚未解析到英文剧名；补齐 TMDB 密钥或 AI 配置后点「按英文规范改名」重跑"}
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
                              "target": f.file_name, "status": f.rename_status or "pending",
                              "detail": (f.rename_error or "等待英文名解析 / B 库可见")[:300]})
    for t in tasks:
        if t.status in ("pending", "running", "unknown"):
            pending_items.append({"kind": "复制任务待确认", "work": titles.get(t.media_id, ""),
                                  "target": t.task_id, "status": t.status,
                                  "detail": (t.error or "")[:300]})
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
            "works_without_english": db.query(MirrorMedia).filter(
                or_(MirrorMedia.english_title.is_(None),
                    MirrorMedia.english_title == "")).count(),
            "tasks_unknown": db.query(MirrorTask).filter(MirrorTask.status == "unknown").count(),
            "tasks_failed": db.query(MirrorTask).filter(MirrorTask.status == "failed").count(),
            "scan_batches": db.query(ResourceScanBatch).count(),
            "change_events": db.query(DistributionEvent).count(),
            "dirs_mapped": db.query(MirrorDir).filter(MirrorDir.status != "missing").count(),
            "dirs_created": db.query(MirrorDir).filter(MirrorDir.status == "created").count(),
            "dirs_missing": len(missing),
            "dirs_failed": len(missing),
            "dirs_pruned_zombie": len(pruned_dirs),  # v0.9.6：已清理待重建的僵尸行数
            "dirs_fallback": db.query(MirrorDir).filter(
                MirrorDir.status == "created_fallback").count(),
        },
        "media": [{"id": m.id, "title": m.title_standard, "mirror_name": m.title_mirror,
                   "media_type": m.media_type, "status": m.status,
                   "english_title": getattr(m, "english_title", "") or "",
                   "english_source": getattr(m, "english_source", "") or "",
                   "source_dir_id": m.source_dir_id, "target_dir_id": m.target_dir_id,
                   "category_path": getattr(m, "category_path", "") or "",
                   "files": db.query(MirrorFile).filter(MirrorFile.media_id == m.id).count()}
                  for m in media],
        "dirs": [{"id": d.id, "name": d.name_standard, "kind": d.kind,
                  "category_path": d.category_path, "status": d.status,
                  "parent_target_id": d.parent_target_id, "target_dir_id": d.target_dir_id,
                  "pruned": mirror.is_pruned_dir_error(getattr(d, "error", "")),
                  "error": getattr(d, "error", "") or ""}
                 for d in dirs],
        # v0.8.2：tip 不再硬编码成「请手工建立」，直接回显 create_dir 的真实报错
        "missing_dirs": [{"parent_id": d.parent_target_id, "name": d.name_standard,
                          "kind": d.kind, "star": d.kind == "work",
                          "error": getattr(d, "error", "") or "",
                          "tip": (getattr(d, "error", "") or "")
                                 or "在 B 库该父目录下建立此目录后重新同步"} for d in missing],
        "tasks": [{"id": t.id, "task_id": t.task_id, "status": t.status,
                   "raw_status": t.raw_status, "polls": t.polls, "error": t.error} for t in tasks],
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
    }


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
    stats = {"identified": 0, "failed": 0, "queued": 0, "finish_backfilled": 0}
    pending = list(db.scalars(select(Resource).where(
        Resource.identify_status.in_(["unidentified", "ambiguous", "failed", "none", ""])
    ).order_by(Resource.id.desc()).limit(min(limit, 500))).all())
    for x in pending:
        try:
            r = await tmdb.identify(x.name, db, path_hint=x.path_hint or "", use_ai=True)
            if r.get("ok") and r.get("best"):
                _apply_identification(x, r["best"], source=r.get("source") or "tmdb",
                                      note=r.get("reason") or "", hint=r.get("hint"))
                stats["identified"] += 1
                # autoflush=False：同批次识别后立刻查队列去重，必须 flush
                db.flush()
                if publish_after_identify:
                    stats["queued"] += _enqueue_after_identify(db, x)
            else:
                x.identify_status = r.get("status") or "none"
                x.identify_source = r.get("source") or ""
                x.identify_note = r.get("reason") or ""
                stats["failed"] += 1
        except Exception as exc:
            # TMDB 未配置/401/限流不得阻断扫描链路（项目既定降级约定）
            x.identify_status = "failed"
            x.identify_note = str(exc)[:300]
            stats["failed"] += 1
    # 完结状态回填：只处理已识别的剧集且缺 finish_status（限量，避免重复请求）
    stale = list(db.scalars(select(Resource).where(
        Resource.media_type == "tv", Resource.tmdb_id.isnot(None),
        Resource.identify_status == "identified",
        or_(Resource.finish_status.is_(None), Resource.finish_status == ""),
        # v0.8.0：手动修正过（含手动清空）的资源不再被 TMDB 自动回填覆盖
        or_(Resource.finish_locked.is_(None), Resource.finish_locked.is_(False))
    ).limit(60)).all())
    seen_tmdb: dict[int, str] = {}
    for x in stale:
        tid = int(x.tmdb_id)
        if tid not in seen_tmdb:
            try:
                d = await tmdb.detail(tid, "tv", db)
                seen_tmdb[tid] = str(d.get("finish_status") or "")
            except Exception:
                seen_tmdb[tid] = ""
        if seen_tmdb[tid]:
            x.finish_status = seen_tmdb[tid]
            stats["finish_backfilled"] += 1
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
    stale = [x for x in db.scalars(select(Resource)).all() if x.file_id not in seen]
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
    q = db.query(QueueItem).filter(QueueItem.resource_id.in_(ids)).delete(
        synchronize_session=False)
    for x in stale:
        db.delete(x)
    add_audit(db, "library_reconciled", target_type="library",
              detail=json_dumps({"removed": len(ids), "shares_cancelled": cancelled,
                                 "queue_cleared": q, "empty_seen_allowed": empty_ok}))
    out = {"removed": len(ids), "shares_cancelled": cancelled, "queue_cleared": q}
    if empty_ok:
        out["empty_seen_allowed"] = True
    return out


def process_change_events(db: Session, *, publish: bool = True) -> dict:
    """v0.8.0：把扫描批次产生的「内容更新」事件转成一次作品级更新推送。

    - 同一 (作品, 变化指纹) 只处理一次；一批多集同时变化只推一条。
    - 已分享影视内容变化**复用原分享链接**，不重建分享、不取消再创建。
    - 仅由「立即扫描 / 重新扫描库 / 定时扫描」调用。v0.8.5 的镜像同步管线刻意不调用
      本函数：历史 queue_id=0 事件一旦在这里放开，会一次性批量补推，属用户明确禁止的群推。
    - publish=False 时只保留事件不处理（保留给未来需要延期处理的调用方）。
    """
    events = list(db.scalars(select(DistributionEvent).where(
        DistributionEvent.queue_id == 0)).all())
    stats = {"events": len(events), "queued": 0, "share_reused": 0, "skipped": 0,
             "deferred": 0}
    if not events:
        return stats
    if not publish:
        stats["deferred"] = len(events)
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
        ids = [m.id for m in members]
        q = db.scalar(select(QueueItem).where(
            QueueItem.resource_id.in_(ids)).order_by(QueueItem.id).limit(1))
        if q:
            # 复用原队列项并重置为待发送 = 一次「内容更新」推送，不新增第二条
            q.status, q.attempts, q.error = "pending", 0, ""
            q.next_retry_at, q.sent_at = None, None
            ev.queue_id = q.id
            stats["queued"] += 1
        else:
            _enqueue_after_identify(db, rep)
            db.flush()
            q = db.scalar(select(QueueItem).where(
                QueueItem.resource_id.in_(ids)).order_by(QueueItem.id.desc()).limit(1))
            if q:
                ev.queue_id = q.id
                stats["queued"] += 1
            else:
                stats["skipped"] += 1
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
            "share_duration", "revision", "utime"]
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
    groups: dict[str, dict] = {}
    for x in rows:
        key = _series_group_key(x)
        g = groups.get(key)
        if g is None:
            hint = parse_media_name(x.name)
            g = groups[key] = {
                "key": key,
                "title": x.media_title or hint.get("primary_title") or x.name,
                "year": str(hint.get("year") or ""),
                "media_type": x.media_type or ("tv" if hint.get("is_tv_hint") else ""),
                "poster_path": x.poster_path or "",
                "overview": x.overview or "",
                "genres": x.genres or "",
                "vote_average": float(x.vote_average or 0),
                "tmdb_id": x.tmdb_id,
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
            }
        g["count"] += 1
        g["ids"].append(x.id)
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
        # 筛选：标题/文件名关键词
        if q and q.lower() not in (str(g["title"]) + " " + " ".join(str(i) for i in g["ids"])).lower():
            # 关键词也匹配成员文件名
            member_hit = any(q.lower() in (db.get(Resource, i).name or "").lower() for i in g["ids"])
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
    """
    resources = db.query(Resource).count()
    queue = db.query(QueueItem).count()
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
                r = await tmdb.identify(x.name, db, path_hint=x.path_hint or "", use_ai=True)
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
                result = await GuangYaShareClient(db).create(target_id, title, p.duration)
                data = result.get("data") or {}
                share_id = str(data.get("shareId") or data.get("share_id") or data.get("id") or "")
                share_url = str(data.get("shareUrl") or data.get("share_url") or data.get("url") or "")
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
        result = await GuangYaShareClient(db).create(target_id, title, duration)
        data = result.get("data") or {}
        share_id = str(data.get("shareId") or data.get("share_id") or data.get("id") or "")
        share_url = str(data.get("shareUrl") or data.get("share_url") or data.get("url") or "")
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


def _enqueue_ready(db: Session, x: Resource, rule: Rule) -> bool:
    """v0.6.5：把「已识别 + 有海报」的资源按规则入队，同一作品同一频道只入一条。

    返回是否新增。扫描/识别/加规则三条路径都走这里，保证「一张卡 = 一条推送」
    在任何入口都成立。注意 Session 是 autoflush=False，入队后必须显式 flush，
    否则 identify_all 批量识别时前一条队列项对后续同剧单集不可见，36 集会全部入队。
    """
    if x.identify_status != "identified" or not (x.poster_path or "").strip():
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
    db.add(QueueItem(resource_id=x.id, channel_id=rule.channel_id,
                     status="pending" if rule.auto_publish else "review"))
    db.flush()  # autoflush=False：立即落库，供同批次后续单集的去重查询看到
    return True


def _enqueue_after_identify(db: Session, x: Resource) -> int:
    """v0.6.5：识别成功且拿到 TMDB 海报后才入队推送。

    扫描只负责入库，不再触发推送；未识别/无海报的单集永不入队，
    保证用户看到的每条消息都有完整海报与元数据，且同一作品只推一条。
    """
    if x.identify_status != "identified" or not (x.poster_path or "").strip():
        return 0
    added = 0
    # v0.9.5：推送白名单改为目录路由表——同一 Bot 按资源所在 B 库目录推到不同频道/群。
    # 路由表非空时以它为准（不同目录→不同目标）；为空时回退旧分发规则，保持升级兼容。
    from types import SimpleNamespace
    routes = bot_listener.push_routes(db)
    if routes:
        for route in bot_listener.match_push_routes(db, x):
            ch = bot_listener.route_target_channel(db, route)
            if not ch:
                continue  # 路由目标未落成启用 Channel，跳过（保存路由时会同步维护）
            pseudo_rule = SimpleNamespace(channel_id=ch.id, auto_publish=True)
            if _enqueue_ready(db, x, pseudo_rule):
                added += 1
        return added
    for rule in db.scalars(select(Rule).where(Rule.enabled.is_(True))).all():
        if rule.directory_id and rule.directory_id != x.parent_id:
            continue
        if rule.keyword and rule.keyword.lower() not in (x.name or "").lower():
            continue
        if _enqueue_ready(db, x, rule):
            added += 1
    return added


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


@app.get("/api/tmdb/status", dependencies=ADMIN)
def tmdb_status(db: Session = Depends(get_db)):
    return tmdb.auth_status(db)


@app.post("/api/tmdb/config", dependencies=ADMIN)
def tmdb_config_save(p: TmdbConfigInput, db: Session = Depends(get_db)):
    runtime_config.set_many(db, {
        "tmdb_api_key": p.api_key.strip(),
        "tmdb_bearer_token": p.bearer_token.strip(),
        "tmdb_language": (p.language.strip() or "zh-CN"),
    })
    add_audit(db, "tmdb_config_saved", target_type="config", detail="TMDB 密钥已更新（密文存储）")
    db.commit()
    return {"ok": True, "status": tmdb.auth_status(db)}


@app.post("/api/tmdb/test", dependencies=ADMIN)
async def tmdb_test(db: Session = Depends(get_db)):
    return await tmdb.test_connection(db)


@app.get("/api/tmdb/candidates/{rid}", dependencies=ADMIN)
async def tmdb_candidates(rid: int, media_type: str = "", db: Session = Depends(get_db)):
    """手动匹配窗口打开时先带出候选；指定类型时只搜该类型（不回退）。"""
    x = db.get(Resource, rid)
    if not x:
        raise HTTPException(404, "资源不存在")
    mt = media_type if media_type in ("movie", "tv") else None
    try:
        if mt:
            hint = parse_media_name(x.name, x.path_hint or "")
            q = hint.get("primary_title") or hint.get("clean_title") or x.name
            items = await tmdb.search(q, mt, db)
            label = "电影" if mt == "movie" else "剧集"
            return {"ok": bool(items), "status": "identified" if items else "failed",
                    "source": "tmdb", "reason": f"按{label}搜索「{q}」得到 {len(items)} 个候选",
                    "hint": hint, "ai": None, "candidates": tmdb.rank(items, hint), "best": None}
        return await tmdb.identify(x.name, db, path_hint=x.path_hint or "")
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
        queued = _enqueue_after_identify(db, x)  # v0.6.5：识别成功且有海报才推送
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
        r = await tmdb.identify(x.name, db, path_hint=x.path_hint or "", use_ai=True)
        if r["ok"] and r.get("best"):
            _apply_identification(x, r["best"], source=r["source"], note=r["reason"], hint=r["hint"])
            queued = _enqueue_after_identify(db, x)  # v0.6.5
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
    """
    stmt = select(Resource).order_by(Resource.id.desc()).limit(min(limit, 200))
    if not force:
        stmt = stmt.where(Resource.identify_status.in_(
            ["unidentified", "ambiguous", "failed", "none", ""]))
    rows = list(db.scalars(stmt).all())
    done = pending = queued = 0
    for x in rows:
        try:
            r = await tmdb.identify(x.name, db, path_hint=x.path_hint or "", use_ai=True)
            if r["ok"] and r.get("best"):
                _apply_identification(x, r["best"], source=r["source"],
                                      note=r["reason"], hint=r["hint"])
                queued += _enqueue_after_identify(db, x)  # v0.6.5：识别成功且有海报才推
                done += 1
            else:
                x.identify_status = r["status"]
                x.identify_source = r["source"]
                x.identify_note = r["reason"]
                pending += 1
        except Exception as exc:
            x.identify_status = "failed"
            x.identify_note = str(exc)[:500]
            pending += 1
    add_audit(db, "identify_all", target_type="resource",
              detail=json_dumps({"scanned": len(rows), "identified": done,
                                 "pending": pending, "force": force, "queued": queued}))
    db.commit()
    return {"ok": True, "scanned": len(rows), "identified": done, "pending": pending,
            "queued": queued}


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
    out = []
    for q in db.scalars(stmt).all():
        r, c = db.get(Resource, q.resource_id), db.get(Channel, q.channel_id)
        out.append({"id": q.id, "resource": r.name if r else "", "channel": c.name if c else "",
                    "status": q.status, "attempts": q.attempts, "error": q.error,
                    "next_retry_at": q.next_retry_at.isoformat() if q.next_retry_at else None})
    return out


@app.get("/api/review", dependencies=ADMIN)
def review(db: Session = Depends(get_db)):
    out = []
    for q in db.scalars(select(QueueItem).where(QueueItem.status == "review")).all():
        r = db.get(Resource, q.resource_id)
        c = db.get(Channel, q.channel_id)
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
    return [{"id": x.id, "name": x.name, "content": x.content, "enabled": x.enabled,
             "is_default": x.is_default} for x in db.scalars(select(MessageTemplate)).all()]


@app.post("/api/templates", dependencies=ADMIN)
def add_template(p: TemplateInput, db: Session = Depends(get_db)):
    if p.is_default:
        for x in db.scalars(select(MessageTemplate).where(MessageTemplate.is_default.is_(True))).all():
            x.is_default = False
    x = MessageTemplate(**p.model_dump())
    db.add(x)
    add_audit(db, "template_saved", target_type="template", detail=p.name)
    db.commit()
    return {"id": x.id}


@app.post("/api/templates/default", dependencies=ADMIN)
def reset_default_template(db: Session = Depends(get_db)):
    """一键恢复 v0.6.4 整剧风格的默认模板（emoji + 海报，不含单集文件名）。"""
    x = db.scalar(select(MessageTemplate).where(MessageTemplate.name == "默认图文模板"))
    if not x:
        x = MessageTemplate(name="默认图文模板", content=DEFAULT_TEMPLATE,
                            enabled=True, is_default=True)
        db.add(x)
    else:
        x.content, x.enabled, x.is_default = DEFAULT_TEMPLATE, True, True
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
    add_audit(db, "template_updated", target_type="template", target_id=str(tid))
    db.commit()
    return {"ok": True}


@app.delete("/api/templates/{tid}", dependencies=ADMIN)
def delete_template(tid: int, db: Session = Depends(get_db)):
    """v0.9.6：删除模板；默认模板禁删（避免 publisher 无模板可用）。"""
    x = db.get(MessageTemplate, tid)
    if not x:
        raise HTTPException(404, "模板不存在")
    if x.is_default:
        raise HTTPException(400, "默认模板不能删除，请先把别的模板设为默认")
    db.delete(x)
    add_audit(db, "template_deleted", target_type="template", target_id=str(tid))
    db.commit()
    return {"ok": True}


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
    """把单个资源按所有启用规则入队并立即推送一次。"""
    x = db.get(Resource, rid)
    if not x:
        raise HTTPException(404, "资源不存在")
    rules = list(db.scalars(select(Rule).where(Rule.enabled.is_(True))).all())
    if not rules:
        raise HTTPException(400, "还没有启用的分发规则：请先在「分发规则」页完成 Bot → 频道 → 规则")
    added = 0
    skipped = []
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
        q = db.scalar(select(QueueItem).where(
            QueueItem.resource_id == x.id, QueueItem.channel_id == r.channel_id))
        if q:
            # v0.6.3：「立即推送」是手动强制动作， ANY 状态（含 sent/retry）都重置为 pending
            # 立即重推。旧逻辑只重置 review/failed，用户第一次推成功后反复点都显示
            # 「已入队 0 条」，误以为按钮无反应。
            q.status, q.error, q.next_retry_at, q.attempts = "pending", "", None, 0
            added += 1
        else:
            db.add(QueueItem(resource_id=x.id, channel_id=r.channel_id, status="pending"))
            added += 1
    db.commit()
    result = await process_queue(limit=50)
    # v0.6.2：把本次入队项的即时结果带回前端，避免"点了没反应"的错觉。
    # 用户常在资源未建分享或 Telegram 未通时点推送，队列项立即进入 retry，
    # 若不回显错误，界面只显示"已入队 1 条"却收不到消息。
    recent = list(db.scalars(select(QueueItem).where(
        QueueItem.resource_id == x.id).order_by(QueueItem.id.desc()).limit(len(rules) or 1)).all())
    detail = [{"channel_id": q.channel_id, "status": q.status,
               "error": str(q.error or "")[:300]} for q in recent if q.error]
    return {"ok": True, "queued": added, "detail": detail, "skipped": skipped, **result}


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
    # 以下字段传 None 表示「保持原值」；返回给前端的是掩码值，避免把掩码写回凭证
    tgto_base_url: str | None = None
    tgto_scene: str | None = None
    tgto_api_key: str | None = None
    contribution_bot_token: str | None = None
    interaction_bot_id: str | None = None       # v0.9.4：交互 Bot 引用 bots 表 id（token 去重）；"" = 不选用
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


@app.get("/api/interaction/config", dependencies=ADMIN)
def interaction_config_get(db: Session = Depends(get_db)):
    plain = ["tgto_base_url", "tgto_scene", "donation_image_url", "donation_text",
             "donation_button_text", "bot_required_channel_link", "bot_required_group_link",
             "tg_channel_link", "tg_group_link"]
    out = {k: runtime_config.get(db, k) for k in plain}
    out.update(runtime_config.masked(db, ["tgto_api_key", "contribution_bot_token"]))
    out["contribution_enabled"] = (runtime_config.get(db, "contribution_enabled") or "").lower() == "true"
    out["donation_enabled"] = (runtime_config.get(db, "donation_enabled") or "").lower() == "true"
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
    out["bot_options"] = [{"id": b.id, "name": b.name, "enabled": b.enabled,
                           "token_masked": _mask_stored_token(b.token)}
                          for b in db.scalars(select(Bot).order_by(Bot.id)).all()]
    out["scene_default"] = "ENV_FILTER_GUANGYA"
    out["unverified_endpoints"] = ["投稿分享转存（光鸭接口，8.6 待验证）"]
    return out


@app.post("/api/interaction/config", dependencies=ADMIN)
def interaction_config_save(p: InteractionConfigInput, db: Session = Depends(get_db)):
    values = {"contribution_enabled": "true" if p.contribution_enabled else "false",
              "donation_enabled": "true" if p.donation_enabled else "false"}
    for k in ("tgto_base_url", "tgto_scene", "tgto_api_key", "contribution_bot_token",
              "donation_image_url", "donation_text", "donation_button_text",
              "bot_required_channel_link", "bot_required_group_link",
              "tg_channel_link", "tg_group_link"):
        v = getattr(p, k)
        if v is not None:
            values[k] = v.strip()
    # v0.9.4：交互 Bot 引用（token 去重）。传 "" 表示清空引用、回退 contribution_bot_token。
    if p.interaction_bot_id is not None:
        bid = str(p.interaction_bot_id).strip()
        if bid:
            if not bid.isdigit() or not db.get(Bot, int(bid)):
                raise HTTPException(400, "interaction_bot_id 必须指向一个已存在的 Bot")
        values["interaction_bot_id"] = bid
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
              detail=json_dumps({"keys": sorted(values)}))
    db.commit()
    return {"ok": True, "saved": sorted(values)}


# ---------- v0.9.5：推送白名单 = 目录路由表（全局单 Bot） ----------
class PushRouteInput(BaseModel):
    dir_id: str = ""       # 影视库 B 的下属目录 ID；空=根目录通配
    kind: str = "channel"  # channel=频道 / group=群聊，任填其一即可
    chat_id: str = ""      # 频道 ID 或群 ID
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
        "routes": bot_listener.push_routes(db),
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
        chat = str(item.chat_id or "").strip()
        if not chat:
            continue  # 频道/群任填其一，空路由跳过
        kind = "group" if str(item.kind or "").lower() == "group" else "channel"
        routes.append({"dir_id": str(item.dir_id or "").strip(), "kind": kind, "chat_id": chat,
                       "note": str(item.note or "").strip()[:120]})
    if len(routes) > 10:
        raise HTTPException(400, "推送白名单最多 10 条")

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
    for r in routes:
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
            if bot and not ch.name.startswith(_ROUTE_CHANNEL_PREFIX):
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
    return [{"id": c.id, "title": c.title, "content": c.content, "share_url": c.share_url,
             "tg_username": c.tg_username, "status": c.status, "note": c.note,
             "created_at": c.created_at, "handled_at": c.handled_at}
            for c in db.scalars(stmt).all()]


class ContributionHandleInput(BaseModel):
    status: str = "approved"     # approved / rejected / transferred
    note: str = ""


@app.post("/api/contributions/{cid}/handle", dependencies=ADMIN)
async def handle_contribution(cid: int, p: ContributionHandleInput, db: Session = Depends(get_db)):
    if p.status not in ("approved", "rejected", "transferred", "pending"):
        raise HTTPException(400, "status 仅支持 approved/rejected/transferred/pending")
    c = db.get(UserContribution, cid)
    if not c:
        raise HTTPException(404, "投稿不存在")
    c.status, c.note, c.handled_at = p.status, p.note[:1000], utcnow()
    add_audit(db, "contribution_handled", target_type="contribution", target_id=str(cid),
              detail=json_dumps({"status": p.status, "note": p.note[:200]}))
    db.commit()
    # v0.9.2：审核结论主动回复投稿用户一次（事务性通知，不刷屏）
    notice = ""
    if p.status == "approved":
        notice = f"你的投稿「{c.title}」已通过审核，感谢支持！"
    elif p.status == "rejected":
        notice = f"你的投稿「{c.title}」未通过审核。" + (f"原因：{p.note}" if p.note else "")
    elif p.status == "transferred":
        notice = f"你的投稿「{c.title}」已转存入库，感谢支持！"
    notified = False
    if notice and c.tg_user_id:
        notified = await bot_listener.notify_user(db, c.tg_user_id, notice)
    return {"ok": True, "notified": notified}


@app.get("/api/media-requests", dependencies=ADMIN)
def media_requests(limit: int = 100, db: Session = Depends(get_db)):
    rows = db.scalars(select(MediaRequest).order_by(
        MediaRequest.id.desc()).limit(min(limit, 500))).all()
    return [{"id": r.id, "title": r.title, "tmdb_id": r.tmdb_id, "media_type": r.media_type,
             "season": r.season, "keyword_id": r.keyword_id, "status": r.status,
             "note": r.note, "tg_username": r.tg_username, "created_at": r.created_at}
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
    kid = tgto.keyword_id(p.tmdb_id, p.season, title)
    row = MediaRequest(title=title, tmdb_id=p.tmdb_id, media_type=p.media_type,
                       season=p.season, keyword_id=kid, status="pending")
    db.add(row)
    db.flush()
    try:
        r = await tgto.upsert_keyword(db, title=title, tmdb_id=p.tmdb_id,
                                      media_type=p.media_type, season=p.season)
        row.status = "synced" if r.get("ok") else "failed"
        row.note = r.get("message") or r.get("status") or ""
        if r.get("ok"):
            row.synced_at = utcnow()
    except Exception as exc:
        row.status, row.note = "failed", str(exc)[:300]
    db.commit()
    return {"ok": True, "id": row.id, "status": row.status, "keyword_id": kid,
            "note": row.note}


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


static = Path("/app/static")
if static.exists():
    assets = static / "assets"
    if assets.exists():
        app.mount("/assets", StaticFiles(directory=assets), name="assets")

    @app.get("/{path:path}")
    def spa(path: str):
        f = static / path
        return FileResponse(f if path and f.is_file() else static / "index.html")
