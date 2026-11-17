import html
import json
from datetime import timedelta

import httpx
from sqlalchemy import select, or_
from sqlalchemy.orm import Session

from . import runtime_config, tmdb
from .config import settings
from .db import DEFAULT_TEMPLATE, SessionLocal, json_dumps
from .guangya import GuangYaError, GuangYaShareClient
from .models import AuditLog, Bot, Channel, MessageTemplate, QueueItem, Resource, utcnow
from .security import decrypt_secret
from .utils import backoff_seconds, parse_media_name, plus_seconds, render_template, retry_after_seconds, version_spec

MAX_ATTEMPTS = 8
_FALLBACK_TEMPLATE = DEFAULT_TEMPLATE


def human_size(size: int) -> str:
    value = float(size or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.1f}{unit}" if unit != "B" else f"{int(value)}B"
        value /= 1024
    return f"{value:.1f}TB"


async def ensure_share(db: Session, res: Resource) -> str:
    """幂等创建分享；返回 share_url。失败抛出 GuangYaError。

    v0.6.3：剧集按「剧集目录」分享（未整理=父目录，已整理=剧名目录），
    同一目录只建一个分享，share_url 同步给同目录所有单集，避免每集一条链接。
    """
    if res.share_url:
        return res.share_url
    if not settings.auto_create_share:
        raise GuangYaError("AUTO_CREATE_SHARE=false，且该资源尚无分享链接")
    # 剧集目录级分享：找同目录已有分享，直接复用
    dir_id = str(getattr(res, "share_dir_id", "") or "")
    scope = str(getattr(res, "share_scope", "") or "")
    if scope == "series_dir" and dir_id:
        from sqlalchemy import select as _select
        sibling = db.scalar(_select(Resource).where(
            Resource.share_dir_id == dir_id, Resource.share_url != "",
            Resource.id != res.id).limit(1))
        if sibling and sibling.share_url:
            res.share_id, res.share_url = sibling.share_id, sibling.share_url
            res.share_status, res.share_duration = "active", sibling.share_duration
            db.commit()
            return res.share_url
    client = GuangYaShareClient(db)
    # 剧集分享整目录，标题用「剧名 S01」；电影/无目录线索仍按单集
    target_id = dir_id if (scope == "series_dir" and dir_id) else res.file_id
    title = res.media_title or res.name or ""
    if scope == "series_dir" and res.season:
        title = f"{title} 第{int(res.season)}季" if title else f"第{int(res.season)}季"
    result = await client.create(target_id, title, settings.share_duration)
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
    """构造 Telegram 消息。v0.6.0 增加 TMDB 分类打标变量。

    可用：type / type_label / origin_country / origin_country_code / genres / tags，
    以及原有 title/overview/filename/year/resolution/size/tmdb_id/tmdb_url/share_url，
    另有 vote_average/original_title/season/episode/episode_tag/media_type。
    """
    esc = lambda v: html.escape(str(v if v is not None else ""), quote=False)
    title = res.media_title or res.name
    hint = parse_media_name(res.name)
    tpl = _template(db)

    genres = tmdb.as_list(res.genres)
    countries = tmdb.as_list(res.origin_country)
    genres_text = "、".join(genres)
    country_text = tmdb.country_label(res.origin_country)
    type_text = tmdb.type_label(res.media_type) if res.media_type else ""
    season = res.season if res.season is not None else hint.get("season")
    episode = res.episode if res.episode is not None else hint.get("episode")
    episode_tag = f"S{int(season):02d}E{int(episode):02d}" if season and episode else ""
    # v0.6.4：整剧展示用「第N季」而非单集信息；版本规格从文件名提取
    season_label = f"第{int(season)}季" if season else ""
    version = version_spec(res.name)

    # 标签串按「片名 · 类型 · 地区 · 分类」打标（示例：#保镖恋人 #电影 #美国 #爱情 #惊悚）
    tag_parts = [title, type_text]
    if country_text:
        tag_parts.append(country_text)
    tag_parts.extend(genres[:3])
    tags = " ".join("#" + str(p).replace(" ", "") for p in dict.fromkeys(tag_parts) if p)

    ctx = {
        "title": esc(title),
        "overview": esc((res.overview or "").strip()),
        "filename": esc(res.name),
        "share_url": share_url,
        "year": esc(hint.get("year") or ""),
        "resolution": esc(hint.get("resolution") or ""),
        "size": human_size(res.size),
        "tmdb_id": res.tmdb_id or "",
        "tmdb_url": tmdb.tmdb_url(res.media_type, res.tmdb_id) if res.tmdb_id and res.media_type else "",
        "media_type": res.media_type or "",
        "type": res.media_type or "",
        "type_label": esc(type_text),
        "origin_country": esc(country_text),
        "origin_country_code": esc(", ".join(countries)),
        "genres": esc(genres_text),
        "tags": esc(tags),
        "vote_average": f"{float(res.vote_average or 0):.1f}" if res.vote_average else "",
        "original_title": esc(res.original_title),
        "season": season or "",
        "episode": episode or "",
        "episode_tag": episode_tag,
        "season_label": esc(season_label),
        "version": esc(version),
    }
    text = render_template(tpl.content if tpl else _FALLBACK_TEMPLATE, ctx).strip()
    return text[:1000], text[:1000]


async def _send_telegram(db: Session, q: QueueItem, res: Resource, channel: Channel, bot: Bot) -> tuple[bool, str, str]:
    token = decrypt_secret(bot.token)
    if not token:
        return False, "", "Bot Token 为空", None
    caption, _ = _build_message(db, res, res.share_url)
    poster = tmdb.poster_url(res.poster_path) if res.poster_path else ""
    base = runtime_config.telegram_base(db)
    method = "sendPhoto" if poster else "sendMessage"
    payload = {"chat_id": channel.chat_id, "caption" if poster else "text": caption,
               "parse_mode": "HTML", "disable_web_page_preview": False}
    if poster:
        payload["photo"] = poster
    retry_after = None
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(f"{base}/bot{token}/{method}", json=payload)
    except httpx.TimeoutException:
        return False, "", (f"连接 Telegram 超时（{base}）。若在国内网络，请在「分发规则」页"
                           "把 Telegram API 地址改成可用的反向代理"), None
    except Exception as exc:
        return False, "", f"无法连接 Telegram（{base}）：{exc}", None
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
            if not res or not channel or not bot:
                q.status, q.error = "failed", "资源、频道或 Bot 已不存在（可能被删除）"
                failed += 1
                continue
            if not bot.enabled or not channel.enabled:
                # v0.6.1：Bot 与频道各自独立启停。停用不算失败、不消耗重试次数，
                # 重新启用后下一轮 worker 会自动继续，且不影响其它链路。
                q.error = "Bot 或频道已停用，暂不推送；启用后自动继续"
                q.next_retry_at = None
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
