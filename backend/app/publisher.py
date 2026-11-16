import html
import json
from datetime import timedelta

import httpx
from sqlalchemy import select, or_
from sqlalchemy.orm import Session

from . import tmdb
from .config import settings
from .db import SessionLocal, json_dumps
from .guangya import GuangYaError, GuangYaShareClient
from .models import AuditLog, Bot, Channel, MessageTemplate, QueueItem, Resource, utcnow
from .security import decrypt_secret
from .utils import backoff_seconds, parse_media_name, plus_seconds, render_template, retry_after_seconds

MAX_ATTEMPTS = 8
_FALLBACK_TEMPLATE = "<b>{{title}}</b>\n\n{{overview}}\n\n{{filename}}\n\n{{share_url}}"


def human_size(size: int) -> str:
    value = float(size or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.1f}{unit}" if unit != "B" else f"{int(value)}B"
        value /= 1024
    return f"{value:.1f}TB"


async def ensure_share(db: Session, res: Resource) -> str:
    """幂等创建分享；返回 share_url。失败抛出 GuangYaError。"""
    if res.share_url:
        return res.share_url
    if not settings.auto_create_share:
        raise GuangYaError("AUTO_CREATE_SHARE=false，且该资源尚无分享链接")
    client = GuangYaShareClient()
    result = await client.create(res.file_id, res.name or res.media_title or "", settings.share_duration)
    data = result.get("data") or {}
    share_id = str(data.get("shareId") or data.get("share_id") or data.get("id") or "")
    share_url = str(data.get("shareUrl") or data.get("share_url") or data.get("url") or data.get("link") or "")
    res.share_id = share_id or res.share_id
    res.share_url = share_url
    res.share_status = "active" if share_url else "failed"
    res.share_duration = settings.share_duration
    db.add(AuditLog(action="share_created" if share_url else "share_failed",
                    target_type="resource", target_id=str(res.id),
                    detail=json_dumps({"file_id": res.file_id, "duration": settings.share_duration,
                                       "share_id": share_id, "has_url": bool(share_url)})))
    db.commit()
    if not share_url:
        raise GuangYaError("分享接口返回成功但未找到 shareUrl/share_url，请核对抓包响应字段")
    return share_url


def _template(db: Session) -> MessageTemplate | None:
    return (db.scalar(select(MessageTemplate).where(MessageTemplate.is_default.is_(True),
                                                    MessageTemplate.enabled.is_(True)))
            or db.scalar(select(MessageTemplate).where(MessageTemplate.enabled.is_(True))))


def _build_message(db: Session, res: Resource, share_url: str) -> tuple[str, str]:
    title = res.media_title or res.name
    hint = parse_media_name(res.name)
    tpl = _template(db)
    ctx = {
        "title": html.escape(title, quote=False),
        "overview": html.escape((res.overview or "").strip(), quote=False),
        "filename": html.escape(res.name, quote=False),
        "share_url": share_url,
        "year": html.escape(str(hint.get("year") or ""), quote=False),
        "resolution": html.escape(str(hint.get("resolution") or ""), quote=False),
        "size": human_size(res.size),
        "tmdb_id": res.tmdb_id or "",
        "tmdb_url": f"https://www.themoviedb.org/{res.media_type}/{res.tmdb_id}" if res.tmdb_id and res.media_type else "",
    }
    text = render_template(tpl.content if tpl else _FALLBACK_TEMPLATE, ctx).strip()
    return text[:1000], text[:1000]


async def _send_telegram(db: Session, q: QueueItem, res: Resource, channel: Channel, bot: Bot) -> tuple[bool, str, str]:
    token = decrypt_secret(bot.token)
    if not token:
        return False, "", "Bot Token 为空", None
    caption, _ = _build_message(db, res, res.share_url)
    poster = tmdb.poster_url(res.poster_path) if res.poster_path else ""
    base = settings.telegram_api_base.rstrip("/")
    method = "sendPhoto" if poster else "sendMessage"
    payload = {"chat_id": channel.chat_id, "caption" if poster else "text": caption,
               "parse_mode": "HTML", "disable_web_page_preview": False}
    if poster:
        payload["photo"] = poster
    retry_after = None
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(f"{base}/bot{token}/{method}", json=payload)
    try:
        data = r.json()
    except Exception:
        data = {"ok": False, "description": r.text[:300]}
    if r.status_code == 429:
        retry_after = retry_after_seconds((data.get("parameters") or {}).get("retry_after"))
    if r.status_code >= 400 or not data.get("ok"):
        return False, "", f"Telegram {method} 失败 HTTP {r.status_code}：{str(data.get('description') or data)[:300]}", retry_after
    return True, str((data.get("result") or {}).get("message_id", "")), "", retry_after


async def process_queue(limit: int = 20) -> dict:
    db: Session = SessionLocal()
    sent = failed = shared = 0
    try:
        now = utcnow()
        rows = list(db.scalars(
            select(QueueItem).where(
                or_(QueueItem.status == "pending", QueueItem.status == "retry"),
                or_(QueueItem.next_retry_at.is_(None), QueueItem.next_retry_at <= now),
            ).order_by(QueueItem.id).limit(limit)
        ).all())
        for q in rows:
            res = db.get(Resource, q.resource_id)
            channel = db.get(Channel, q.channel_id)
            bot = db.get(Bot, channel.bot_id) if channel else None
            if not res or not channel or not bot or not bot.enabled or not channel.enabled:
                q.status, q.error = "failed", "资源、频道或 Bot 不可用/已禁用"
                failed += 1
                continue
            need_share = not res.share_url
            try:
                await ensure_share(db, res)
                if need_share:
                    shared += 1
            except Exception as exc:
                q.attempts += 1
                q.error = f"建分享失败：{str(exc)[:600]}"
                q.next_retry_at = plus_seconds(utcnow(), backoff_seconds(q.attempts))
                q.status = "retry" if q.attempts < MAX_ATTEMPTS else "failed"
                failed += 1
                continue
            ok, message_id, error, retry_after = await _send_telegram(db, q, res, channel, bot)
            if ok:
                q.status = "sent"
                q.telegram_message_id = message_id
                q.sent_at = utcnow()
                q.error = ""
                q.next_retry_at = None
                sent += 1
                db.add(AuditLog(action="push_sent", target_type="queue", target_id=str(q.id),
                                detail=json_dumps({"resource_id": res.id, "channel_id": channel.id,
                                                   "message_id": message_id})))
            else:
                q.attempts += 1
                q.error = error[:1500]
                delay = retry_after or backoff_seconds(q.attempts)
                q.next_retry_at = plus_seconds(utcnow(), delay)
                q.status = "retry" if q.attempts < MAX_ATTEMPTS else "failed"
                failed += 1
        db.commit()
        return {"sent": sent, "failed": failed, "shares_created": shared}
    finally:
        db.close()
