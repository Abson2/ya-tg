import html
import json
import logging
import re
import time
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from . import (ai_assist, bot_listener, guangya_auth, guangya_sms, mirror,
               runtime_config, tgto, tmdb)
from .config import VALID_DURATIONS, settings
from .db import DEFAULT_TEMPLATE, Base, SessionLocal, get_db, json_dumps, migrate, engine
from .guangya import GuangYaError, GuangYaOpenAPI, GuangYaShareClient, GuangYaWebFiles, WebFileError
from .models import (AuditLog, AuthSession, AuthToken, Bot, Channel, DistributionEvent,
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
_last_device_poll = 0.0


def add_audit(db: Session, action: str, target_type: str = "", target_id: str = "", detail: str = "") -> None:
    db.add(AuditLog(action=action, target_type=target_type, target_id=str(target_id), detail=detail))


def check_admin(x_admin_password: str | None = Header(default=None)):
    if settings.admin_password == "CHANGE_ME":
        raise HTTPException(503, "请先在 .env 设置 ADMIN_PASSWORD")
    if x_admin_password != settings.admin_password:
        raise HTTPException(401, "管理密码错误")


async def _job_device_poll():
    global _last_device_poll
    db = SessionLocal()
    try:
        s = db.get(AuthSession, 1)
        if not s or s.purpose != "device" or not s.device_code:
            return
        now = time.monotonic()
        if now - _last_device_poll < max(2, s.interval or 3):
            return
        _last_device_poll = now
        await guangya_auth.poll_device(db)
    except Exception:
        pass  # 后台轮询失败不打断调度，管理端可见 pending_message
    finally:
        db.close()


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
        # 官方 Device/PKCE 通道：refresh_token 刷新
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
    # v0.8.0：镜像定时任务热注册；只同步，不注册任何自动推送任务
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
    scheduler.add_job(_job_device_poll, "interval", seconds=3, id="device_poll",
                      max_instances=1, coalesce=True)
    scheduler.add_job(_job_token_refresh, "interval", seconds=60, id="token_refresh",
                      max_instances=1, coalesce=True)
    scheduler.add_job(_job_bot_poll, "interval", seconds=10, id="bot_poll",
                      max_instances=1, coalesce=True)
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="Ya-TG", version="0.8.2", lifespan=lifespan)
ADMIN = [Depends(check_admin)]


@app.get("/api/health")
def health():
    return {"ok": True, "timezone": "Asia/Shanghai", "version": "0.8.2", "app": "Ya-TG"}


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


@app.post("/api/auth/device/start", dependencies=ADMIN)
async def auth_device_start(db: Session = Depends(get_db)):
    try:
        data = await guangya_auth.start_device(db)
        add_audit(db, "auth_device_started", target_type="auth", detail=json_dumps({"user_code": data["user_code"]}))
        db.commit()
        return data
    except guangya_auth.AuthError as exc:
        raise HTTPException(400, str(exc))


@app.post("/api/auth/device/poll", dependencies=ADMIN)
async def auth_device_poll_now(db: Session = Depends(get_db)):
    try:
        return await guangya_auth.poll_device(db)
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


@app.get("/api/auth/pkce/start", dependencies=ADMIN)
def auth_pkce_start(db: Session = Depends(get_db)):
    try:
        data = guangya_auth.start_pkce(db)
        add_audit(db, "auth_pkce_started", target_type="auth")
        db.commit()
        return data
    except guangya_auth.AuthError as exc:
        raise HTTPException(400, str(exc))


@app.get("/api/auth/pkce/callback", response_class=HTMLResponse)
async def auth_pkce_callback(code: str = "", state: str = "", error: str = "",
                             error_description: str = ""):
    db = SessionLocal()
    try:
        result = await guangya_auth.exchange_pkce(db, code, state, error, error_description)
        add_audit(db, "auth_pkce_success", target_type="auth", detail=result.get("sub", ""))
        db.commit()
        message = "授权成功，可关闭本窗口"
    except Exception as exc:
        message = f"授权失败：{str(exc)[:300]}"
    finally:
        db.close()
    safe = message.replace("\\", "\\\\").replace("'", "\\'")
    return HTMLResponse(f"<!doctype html><meta charset='utf-8'><body style='font-family:sans-serif;padding:40px'>"
                        f"<h3>{safe}</h3><script>window.close()</script></body>")


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
    if t and t.flow in ("sms", "tgto"):
        try:
            data = await GuangYaWebFiles(db).list_files(page_size=1)
            items = data.get("list") or []
            return {"ok": True, "channel": "web", "flow": "sms",
                    "user_id": guangya_sms.status(db).get("account") or "",
                    "root_items": len(items),
                    "message": "光鸭网页端连接正常（短信登录），可扫描与建分享"}
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
    root_id: str = ""
    enabled: bool = True


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
    # v0.8.1：前端不再让用户填扫描根目录；留空时自动引用镜像同步里的 B 库目录 ID
    root_id = (p.root_id or "").strip() or _effective_scan_root(db)[0]
    t.cron, t.root_id, t.enabled = cron, root_id, p.enabled
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


async def _scheduled_mirror() -> None:
    """定时镜像：只同步 + 补映射，绝不触发 Telegram 推送。"""
    with SessionLocal() as db:
        try:
            r = db.scalar(select(MirrorRule).where(MirrorRule.enabled.is_(True)).limit(1))
            if r and r.source_root_id and r.target_root_id:
                await mirror.sync_rule(db, r)
                await mirror.backfill_target_file_ids(db, r)
        except Exception as exc:
            db.rollback()
            try:
                r = db.scalar(select(MirrorRule).limit(1))
                if r:
                    r.last_status, r.last_message = "failed", str(exc)[:1000]
                    db.commit()
            except Exception:
                pass


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
        "interval_presets": MIRROR_INTERVAL_PRESETS,
        "last_run": r.last_run if r else None,
        "last_status": r.last_status if r else "never",
        "last_message": r.last_message if r else "",
        "confirmed_endpoints": mirror.CONFIRMED_WRITE_ENDPOINTS,
        "unverified_endpoints": mirror.UNVERIFIED_ENDPOINTS,
        "note": ("v0.8.1：A 库分类目录会在 B 库按原名 1:1 自动创建（不改名），"
                 "只有影视主目录按 * 约定命名，括号里的年份与 TMDB 标记保持原样。"
                 "每次文件复制之间留可配置空闲时间，下限来自官方写接口 2 次/秒限频，不可调低。"
                 "镜像完成不会自动推送 Telegram。"),
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
    add_audit(db, "mirror_config_saved", target_type="mirror",
              detail=json_dumps({"source": r.source_root_id, "target": r.target_root_id,
                                 "interval": r.interval_minutes,
                                 "idle": r.file_idle_seconds, "auto_dirs": r.auto_create_dirs}))
    db.commit()
    _schedule_mirror_job(r.interval_minutes, r.enabled)
    return {"ok": True, "enabled": r.enabled, "interval_minutes": r.interval_minutes}


@app.post("/api/mirror/run", dependencies=ADMIN)
async def mirror_run(post_identify: bool = False, db: Session = Depends(get_db)):
    """立即同步一次。post_identify=True 时额外做 B 库扫描+识别，但强制不推 TG。"""
    r = _mirror_rule(db, create=True)
    if not r.source_root_id or not r.target_root_id:
        raise HTTPException(400, "请先在镜像设置里填写 A 源头库目录 ID 与 B 镜像库目录 ID")
    try:
        stats = await mirror.sync_rule(db, r)
        stats["target_file_ids_backfilled"] = await mirror.backfill_target_file_ids(db, r)
        if post_identify:
            scan = await run_scan(r.target_root_id, resume=False)
            stats["scan"] = {k: v for k, v in scan.items() if k != "seen_file_ids"}
            # 关键：镜像链路的识别绝不入队推送（V2.0 第十章）
            stats["identify"] = await _post_scan_identify(db, publish_after_identify=False)
        add_audit(db, "mirror_run", target_type="mirror",
                  detail=json_dumps({"media": stats.get("media"), "new": stats.get("new"),
                                     "done": stats.get("done"), "unknown": stats.get("unknown")}))
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
        rr = db.scalar(select(MirrorRule).limit(1))
        if rr:
            rr.last_status, rr.last_message = "failed", detail[:1000]
            db.commit()
        raise HTTPException(502, f"镜像同步失败：{detail}")


@app.post("/api/mirror/tasks/poll", dependencies=ADMIN)
async def mirror_poll_tasks(db: Session = Depends(get_db)):
    r = _mirror_rule(db)
    if not r:
        return {"tasks": 0, "done": 0, "still_unknown": 0}
    try:
        out = await mirror.poll_pending_tasks(db, r)
        if out["done"]:
            out["target_file_ids_backfilled"] = await mirror.backfill_target_file_ids(db, r)
        return out
    except Exception as exc:
        raise HTTPException(502, str(exc))


@app.get("/api/mirror/status", dependencies=ADMIN)
def mirror_status(db: Session = Depends(get_db)):
    r = _mirror_rule(db)
    media = list(db.scalars(select(MirrorMedia).order_by(MirrorMedia.id.desc()).limit(200)).all())
    tasks = list(db.scalars(select(MirrorTask).order_by(MirrorTask.id.desc()).limit(30)).all())
    dirs = list(db.scalars(select(MirrorDir).order_by(MirrorDir.id.desc()).limit(200)).all())
    missing = [d for d in dirs if d.status == "missing"]
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
            "tasks_unknown": db.query(MirrorTask).filter(MirrorTask.status == "unknown").count(),
            "scan_batches": db.query(ResourceScanBatch).count(),
            "change_events": db.query(DistributionEvent).count(),
            "dirs_mapped": db.query(MirrorDir).filter(MirrorDir.status != "missing").count(),
            "dirs_created": db.query(MirrorDir).filter(MirrorDir.status == "created").count(),
            "dirs_missing": len(missing),
            "dirs_failed": len(missing),
            "dirs_fallback": db.query(MirrorDir).filter(
                MirrorDir.status == "created_fallback").count(),
        },
        "media": [{"id": m.id, "title": m.title_standard, "mirror_name": m.title_mirror,
                   "media_type": m.media_type, "status": m.status,
                   "source_dir_id": m.source_dir_id, "target_dir_id": m.target_dir_id,
                   "category_path": getattr(m, "category_path", "") or "",
                   "files": db.query(MirrorFile).filter(MirrorFile.media_id == m.id).count()}
                  for m in media],
        "dirs": [{"id": d.id, "name": d.name_standard, "kind": d.kind,
                  "category_path": d.category_path, "status": d.status,
                  "parent_target_id": d.parent_target_id, "target_dir_id": d.target_dir_id,
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
        "unverified_endpoints": mirror.UNVERIFIED_ENDPOINTS,
        "confirmed_endpoints": mirror.CONFIRMED_WRITE_ENDPOINTS,
        "min_idle_seconds": mirror.MIN_FILE_IDLE_SECONDS,
        # v0.8.2：回显实际生效配置，一眼看出「自动创建是不是被关掉了」
        "config": {
            "auto_create_dirs": (True if getattr(r, "auto_create_dirs", None) is None
                                  else bool(r.auto_create_dirs)) if r else True,
            "star_naming": bool(r.star_naming) if r else True,
            "source_root_id": r.source_root_id if r else "",
            "target_root_id": r.target_root_id if r else "",
            "file_idle_seconds": float(r.file_idle_seconds or 1.0) if r else 1.0,
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
    需求2/V2.0 第十章：镜像同步复用本管线时必须传 publish_after_identify=False，
    避免 B 库复制完成就自动发 Telegram——这是「独立调用模式」的隔离点。

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
                             resumed: bool = False, batch_id: int | None = None) -> dict:
    """v0.7.1：B 库对账——本轮没扫到的影视移除看板并取消分享（需求3-1）。

    V2.0 第 12.2 节扫描批次保护：必须同时满足
      ① 全量扫描（非断点续扫）、② 全部分页读取成功、③ seen 非空，
    否则一律跳过对账。宁可保留旧数据，也绝不能因接口抖动把整个资源库清空。
    """
    skipped = ""
    # v0.8.0：批次表是「是否允许对账删除」的唯一权威信号。传入 batch_id 时，
    # 即使调用方声称 complete=True，只要批次未标记 reconcile_allowed 就一律跳过。
    if batch_id:
        b = db.get(ResourceScanBatch, batch_id)
        if not b:
            return {"removed": 0, "shares_cancelled": 0, "queue_cleared": 0,
                    "skipped": "batch_missing"}
        if b.status != "success" or not b.complete or b.resumed or not b.reconcile_allowed:
            return {"removed": 0, "shares_cancelled": 0, "queue_cleared": 0,
                    "skipped": "batch_not_allowed"}
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
    if not skipped and not seen:
        skipped = "empty_seen"
    if skipped:
        return {"removed": 0, "shares_cancelled": 0, "queue_cleared": 0, "skipped": skipped}
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
                                 "queue_cleared": q}))
    return {"removed": len(ids), "shares_cancelled": cancelled, "queue_cleared": q}


def process_change_events(db: Session, *, publish: bool = True) -> dict:
    """v0.8.0：把扫描批次产生的「内容更新」事件转成一次作品级更新推送。

    - 同一 (作品, 变化指纹) 只处理一次；一批多集同时变化只推一条。
    - 已分享影视内容变化**复用原分享链接**，不重建分享、不取消再创建。
    - publish=False 时只保留事件不处理（镜像链路禁止完成后自动推 TG）。
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


@app.post("/api/scan-now", dependencies=ADMIN)
async def scan_now(resume: bool = True, auto_identify: bool = True,
                   db: Session = Depends(get_db)):
    root, _ = _effective_scan_root(db)
    try:
        data = await run_scan(root, resume=resume)
        out = {k: v for k, v in data.items() if k != "seen_file_ids"}
        if auto_identify:
            out["identify"] = await _post_scan_identify(db, publish_after_identify=True)
        try:
            out["changes"] = process_change_events(db, publish=True)
        except Exception as exc:
            out["changes"] = {"error": str(exc)[:200]}
        add_audit(db, "scan_manual", target_type="scan", detail=json_dumps(out))
        db.commit()
        return out
    except Exception as e:
        raise HTTPException(502, str(e))


@app.post("/api/library/rescan", dependencies=ADMIN)
async def rescan_library(auto_identify: bool = True, reconcile: bool = True,
                         db: Session = Depends(get_db)):
    """资源库「重新扫描库」：全量重扫 B 库 → 对账移除已删除影视并取消分享 → 自动识别。

    全量扫描（resume=False）才允许对账，避免断点续扫 seen 不完整导致误删。
    返回统计供前端一次展示，扫描过程中资源逐目录 commit，看板可边扫边刷新。
    """
    root, _ = _effective_scan_root(db)
    try:
        data = await run_scan(root, resume=False)
        out = {k: v for k, v in data.items() if k != "seen_file_ids"}
        if reconcile:
            out["reconcile"] = await _reconcile_library(
                db, data.get("seen_file_ids") or [],
                scan_complete=bool(data.get("complete", False)),
                resumed=bool(data.get("resumed", False)),
                batch_id=data.get("batch_id") or None)
        if auto_identify:
            out["identify"] = await _post_scan_identify(db, publish_after_identify=True)
        try:
            out["changes"] = process_change_events(db, publish=True)
        except Exception as exc:
            out["changes"] = {"error": str(exc)[:200]}
        add_audit(db, "library_rescan", target_type="library", detail=json_dumps(out))
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


@app.get("/api/channels", dependencies=ADMIN)
def channels(db: Session = Depends(get_db)):
    return [{"id": x.id, "bot_id": x.bot_id, "name": x.name, "chat_id": x.chat_id, "enabled": x.enabled}
            for x in db.scalars(select(Channel)).all()]


@app.post("/api/channels", dependencies=ADMIN)
def add_channel(p: ChannelInput, db: Session = Depends(get_db)):
    if not db.get(Bot, p.bot_id):
        raise HTTPException(404, "Bot 不存在")
    x = Channel(**p.model_dump())
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


@app.get("/api/audit", dependencies=ADMIN)
def audit(limit: int = 200, db: Session = Depends(get_db)):
    rows = db.scalars(select(AuditLog).order_by(AuditLog.id.desc()).limit(min(limit, 1000))).all()
    return [{"id": x.id, "ts": x.ts.isoformat() if x.ts else "", "action": x.action,
             "target_type": x.target_type, "target_id": x.target_id, "detail": x.detail, "actor": x.actor}
            for x in rows]


# ---------- v0.6.1：分发链路修复 ----------
class ToggleInput(BaseModel):
    enabled: bool


class TestMessageInput(BaseModel):
    chat_id: str = ""


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
    """v0.7.0：TG 分发扩展配置（推送模式/自定义 bot/福袋入口/消息样式）。"""
    return {k: runtime_config.get(db, key) for k, key in _TG_EXTRA_KEYS.items()}


@app.post("/api/tg/extra-config", dependencies=ADMIN)
def tg_extra_config_save(p: TgExtraConfigInput, db: Session = Depends(get_db)):
    if p.push_mode not in ("channel", "bot_channel", "custom_bot_group"):
        raise HTTPException(400, "推送模式仅支持 channel/bot_channel/custom_bot_group")
    if p.link_style not in ("button", "plain"):
        raise HTTPException(400, "链接样式仅支持 button/plain")
    if p.detail_level not in ("compact", "full"):
        raise HTTPException(400, "详情级别仅支持 compact/full")
    vals = {key: str(getattr(p, k) or "").strip() for k, key in _TG_EXTRA_KEYS.items()}
    runtime_config.set_many(db, vals)
    add_audit(db, "tg_extra_config_saved", target_type="config",
              detail=json_dumps({"push_mode": p.push_mode, "link_style": p.link_style}))
    db.commit()
    return {"ok": True, **{k: runtime_config.get(db, key) for k, key in _TG_EXTRA_KEYS.items()}}


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
    donation_image_url: str | None = None
    donation_text: str | None = None
    donation_button_text: str | None = None


@app.get("/api/interaction/config", dependencies=ADMIN)
def interaction_config_get(db: Session = Depends(get_db)):
    plain = ["tgto_base_url", "tgto_scene", "donation_image_url", "donation_text",
             "donation_button_text"]
    out = {k: runtime_config.get(db, k) for k in plain}
    out.update(runtime_config.masked(db, ["tgto_api_key", "contribution_bot_token"]))
    out["contribution_enabled"] = (runtime_config.get(db, "contribution_enabled") or "").lower() == "true"
    out["donation_enabled"] = (runtime_config.get(db, "donation_enabled") or "").lower() == "true"
    out["scene_default"] = "ENV_FILTER_GUANGYA"
    out["unverified_endpoints"] = ["投稿分享转存（光鸭接口，8.6 待验证）"]
    return out


@app.post("/api/interaction/config", dependencies=ADMIN)
def interaction_config_save(p: InteractionConfigInput, db: Session = Depends(get_db)):
    values = {"contribution_enabled": "true" if p.contribution_enabled else "false",
              "donation_enabled": "true" if p.donation_enabled else "false"}
    for k in ("tgto_base_url", "tgto_scene", "tgto_api_key", "contribution_bot_token",
              "donation_image_url", "donation_text", "donation_button_text"):
        v = getattr(p, k)
        if v is not None:
            values[k] = v.strip()
    runtime_config.set_many(db, values)
    add_audit(db, "interaction_config_saved", target_type="interaction",
              detail=json_dumps({"keys": sorted(values)}))
    db.commit()
    return {"ok": True, "saved": sorted(values)}


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
def handle_contribution(cid: int, p: ContributionHandleInput, db: Session = Depends(get_db)):
    if p.status not in ("approved", "rejected", "transferred", "pending"):
        raise HTTPException(400, "status 仅支持 approved/rejected/transferred/pending")
    c = db.get(UserContribution, cid)
    if not c:
        raise HTTPException(404, "投稿不存在")
    c.status, c.note, c.handled_at = p.status, p.note[:1000], utcnow()
    add_audit(db, "contribution_handled", target_type="contribution", target_id=str(cid),
              detail=json_dumps({"status": p.status, "note": p.note[:200]}))
    db.commit()
    return {"ok": True}


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
