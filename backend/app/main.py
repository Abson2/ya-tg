import json
import time
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from . import guangya_auth, guangya_sms, runtime_config, tmdb
from .config import VALID_DURATIONS, settings
from .db import Base, SessionLocal, get_db, json_dumps, migrate, engine
from .guangya import GuangYaError, GuangYaOpenAPI, GuangYaShareClient
from .models import (AuditLog, AuthSession, AuthToken, Bot, Channel, MessageTemplate,
                     QueueItem, Resource, Rule, ScanCursor, ScanTask)
from .publisher import ensure_share, process_queue
from .scanner import run_scan
from .security import decrypt_secret, encrypt_secret, mask_secret
from .utils import as_utc, utcnow

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
    if enabled:
        try:
            scheduler.add_job(run_scan, CronTrigger.from_crontab(cron, timezone="Asia/Shanghai"),
                              args=[root], id="global_scan", replace_existing=True,
                              max_instances=1, coalesce=True)
        except Exception:
            pass
    scheduler.add_job(process_queue, "interval", seconds=30, id="queue_worker",
                      max_instances=1, coalesce=True)
    scheduler.add_job(_job_device_poll, "interval", seconds=3, id="device_poll",
                      max_instances=1, coalesce=True)
    scheduler.add_job(_job_token_refresh, "interval", seconds=60, id="token_refresh",
                      max_instances=1, coalesce=True)
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="Ya-TG", version="0.5.4", lifespan=lifespan)
ADMIN = [Depends(check_admin)]


@app.get("/api/health")
def health():
    return {"ok": True, "timezone": "Asia/Shanghai", "version": "0.5.4", "app": "Ya-TG"}


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
    """用公开文档中的 get_user_info 验证 OpenAPI 授权是否真的可用。"""
    try:
        info = await GuangYaOpenAPI(db).get_user_info()
        return {"ok": True, "user_id": info.get("userId", ""), "vip_status": info.get("vipStatus")}
    except Exception as exc:
        raise HTTPException(502, str(exc))


# ---------- 扫描 ----------
@app.get("/api/scan-task", dependencies=ADMIN)
def scan_task(db: Session = Depends(get_db)):
    t = db.scalar(select(ScanTask).limit(1))
    c = db.get(ScanCursor, 1)
    return {k: getattr(t, k) for k in ["id", "name", "cron", "root_id", "enabled", "last_run", "last_status", "last_message"]} | {
        "cursor_status": c.status if c else "never",
        "cursor_files": c.files_count if c else 0,
        "cursor_dirs": c.dirs_count if c else 0,
        "cursor_error": c.error if c else "",
    }


class ScanConfig(BaseModel):
    cron: str
    root_id: str = ""
    enabled: bool = True


@app.put("/api/scan-task", dependencies=ADMIN)
def save_scan_task(p: ScanConfig, db: Session = Depends(get_db)):
    try:
        trigger = CronTrigger.from_crontab(p.cron, timezone="Asia/Shanghai")
    except Exception as e:
        raise HTTPException(400, f"Cron 表达式无效：{e}")
    t = db.scalar(select(ScanTask).limit(1))
    t.cron, t.root_id, t.enabled = p.cron, p.root_id, p.enabled
    add_audit(db, "scan_task_updated", target_type="scan", detail=json_dumps({"cron": p.cron, "root_id": p.root_id, "enabled": p.enabled}))
    db.commit()
    if scheduler.get_job("global_scan"):
        scheduler.remove_job("global_scan")
    if p.enabled:
        scheduler.add_job(run_scan, trigger, args=[p.root_id], id="global_scan", replace_existing=True,
                          max_instances=1, coalesce=True)
    return {"ok": True, "timezone": "Asia/Shanghai"}


@app.post("/api/scan-now", dependencies=ADMIN)
async def scan_now(resume: bool = True, db: Session = Depends(get_db)):
    task = db.scalar(select(ScanTask).limit(1))
    root = task.root_id if task else settings.scan_root_id
    try:
        data = await run_scan(root, resume=resume)
        add_audit(db, "scan_manual", target_type="scan", detail=json_dumps(data))
        db.commit()
        return data
    except Exception as e:
        raise HTTPException(502, str(e))


# ---------- 资源与分享 ----------
@app.get("/api/resources", dependencies=ADMIN)
def resources(q: str = "", limit: int = 100, db: Session = Depends(get_db)):
    stmt = select(Resource).order_by(Resource.id.desc()).limit(min(limit, 500))
    if q:
        stmt = stmt.where(or_(Resource.name.ilike(f"%{q}%"), Resource.media_title.ilike(f"%{q}%")))
    keys = ["id", "file_id", "name", "size", "ext", "tmdb_id", "media_type", "media_title",
            "overview", "poster_path", "identify_status", "share_url", "share_status",
            "share_duration", "revision", "utime"]
    return [{k: getattr(x, k) for k in keys} for x in db.scalars(stmt).all()]


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
        result = await GuangYaShareClient(db).create(x.file_id, x.media_title or x.name, duration)
        data = result.get("data") or {}
        x.share_id = str(data.get("shareId") or data.get("share_id") or data.get("id") or "")
        x.share_url = str(data.get("shareUrl") or data.get("share_url") or data.get("url") or "")
        x.share_status = "active" if x.share_url else "failed"
        x.share_duration = duration
        add_audit(db, "share_created_manual", target_type="resource", target_id=str(rid),
                  detail=json_dumps({"duration": duration, "has_url": bool(x.share_url)}))
        db.commit()
        return {"ok": True, "share_id": x.share_id, "share_url": x.share_url}
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
        await GuangYaShareClient(db).delete(x.share_id)
        x.share_status = "cancelled"
        add_audit(db, "share_cancelled", target_type="resource", target_id=str(rid))
        db.commit()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(502, str(e))


# ---------- TMDB 识别 ----------
class IdentifyInput(BaseModel):
    tmdb_id: int
    media_type: str  # movie / tv


def _apply_identification(x: Resource, d: dict) -> None:
    x.tmdb_id = d["tmdb_id"]
    x.media_type = d["media_type"]
    x.media_title = d["title"]
    x.overview = d["overview"]
    x.poster_path = d["poster_path"]
    x.identify_status = "identified"


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
async def tmdb_candidates(rid: int, db: Session = Depends(get_db)):
    x = db.get(Resource, rid)
    if not x:
        raise HTTPException(404, "资源不存在")
    try:
        return await tmdb.identify_by_filename(x.name, db)
    except Exception as e:
        raise _tmdb_error(e)


@app.post("/api/resources/{rid}/identify", dependencies=ADMIN)
async def identify(rid: int, p: IdentifyInput, db: Session = Depends(get_db)):
    if p.media_type not in ("movie", "tv"):
        raise HTTPException(400, "media_type 仅支持 movie / tv")
    x = db.get(Resource, rid)
    if not x:
        raise HTTPException(404, "资源不存在")
    try:
        d = await tmdb.detail(p.tmdb_id, p.media_type, db)
        _apply_identification(x, d)
        add_audit(db, "resource_identified", target_type="resource", target_id=str(rid),
                  detail=json_dumps(d))
        db.commit()
        return {"ok": True, "media_title": x.media_title, "poster": tmdb.poster_url(x.poster_path)}
    except Exception as e:
        raise _tmdb_error(e)


@app.post("/api/resources/{rid}/auto-identify", dependencies=ADMIN)
async def auto_identify(rid: int, db: Session = Depends(get_db)):
    x = db.get(Resource, rid)
    if not x:
        raise HTTPException(404, "资源不存在")
    try:
        candidates = await tmdb.identify_by_filename(x.name, db)
        if not candidates:
            return {"ok": False, "candidates": []}
        _apply_identification(x, candidates[0])
        add_audit(db, "resource_auto_identified", target_type="resource", target_id=str(rid),
                  detail=json_dumps({"tmdb_id": x.tmdb_id, "media_type": x.media_type}))
        db.commit()
        return {"ok": True, "candidates": candidates, "applied": candidates[0]}
    except Exception as e:
        raise _tmdb_error(e)


@app.get("/api/tmdb/search", dependencies=ADMIN)
async def tmdb_search(q: str, db: Session = Depends(get_db)):
    try:
        return await tmdb.search(q, None, db)
    except Exception as e:
        raise _tmdb_error(e)


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


class RuleInput(BaseModel):
    name: str
    directory_id: str = ""
    keyword: str = ""
    channel_id: int
    auto_publish: bool = False
    enabled: bool = True


@app.get("/api/rules", dependencies=ADMIN)
def rules(db: Session = Depends(get_db)):
    return [{"id": x.id, "name": x.name, "directory_id": x.directory_id, "keyword": x.keyword,
             "channel_id": x.channel_id, "auto_publish": x.auto_publish, "enabled": x.enabled}
            for x in db.scalars(select(Rule)).all()]


@app.post("/api/rules", dependencies=ADMIN)
def add_rule(p: RuleInput, db: Session = Depends(get_db)):
    if not db.get(Channel, p.channel_id):
        raise HTTPException(404, "频道不存在")
    x = Rule(**p.model_dump())
    db.add(x)
    add_audit(db, "rule_added", target_type="rule", detail=p.name)
    db.commit()
    db.refresh(x)
    return {"id": x.id}


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


static = Path("/app/static")
if static.exists():
    assets = static / "assets"
    if assets.exists():
        app.mount("/assets", StaticFiles(directory=assets), name="assets")

    @app.get("/{path:path}")
    def spa(path: str):
        f = static / path
        return FileResponse(f if path and f.is_file() else static / "index.html")
