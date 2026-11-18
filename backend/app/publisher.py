import asyncio
import html
import json
import re
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
from .utils import (backoff_seconds, is_video_file, parse_media_name, plus_seconds,
                    render_template, retry_after_seconds, version_spec, work_episode_numbers,
                    work_group_key)

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
    v0.8.9 剧集状态组：episode_count（本地已收录集数）/ total_episodes（TMDB 官方总集数）
    / updated_to_episode（已更新到第几集）/ current_episode（当前集）
    / episode_progress（如 12/24）/ episode_status（如「更新至第 12 集 / 共 24 集 · 连载中」）。
    """
    esc = lambda v: html.escape(str(v if v is not None else ""), quote=False)
    title = res.media_title or res.name
    hint = parse_media_name(res.name)
    tpl = _template(db)
    # v0.7.0：精简详情——compact 模式把简介压到 110 字，缩短消息篇幅
    detail_level = (runtime_config.get(db, "tg_detail_level") or "compact").strip()
    overview_text = (res.overview or "").strip()
    if detail_level == "compact" and len(overview_text) > 110:
        overview_text = overview_text[:110].rstrip("，。、；：,;: ") + "…"

    genres = tmdb.as_list(res.genres)
    countries = tmdb.as_list(res.origin_country)
    genres_text = "、".join(genres)
    country_text = tmdb.country_label(res.origin_country)
    type_text = tmdb.type_label(res.media_type) if res.media_type else ""
    season = res.season if res.season is not None else hint.get("season")
    episode = res.episode if res.episode is not None else hint.get("episode")
    episode_tag = f"S{int(season):02d}E{int(episode):02d}" if season and episode else ""
    # v0.7.1：整剧统计改用「与展示/分享完全相同的作品键」圈定同剧全部单集。
    # 旧实现用 share_dir_id / tmdb_id 查成员，这两个字段只有识别成功才写入，
    # 未识别单集被整批漏掉，且只读 DB 的 season/episode（同样只有识别才有），
    # 于是 TG 里集数与总大小都偏小——这是「推送的集数显示不对」的直接根因。
    total_size = int(res.size or 0)
    whole_season_label = f"第{int(season)}季" if season else ""
    episode_count = ""
    # v0.8.9：剧集状态与集数进度。episode_count 一直是「本地已收录集数」，
    # 这里补上 TMDB 官方总集数、已更新到第几集，追更剧才能显示「更新至 12/24 集」。
    total_episodes = 0
    updated_ep = 0
    cur_ep = 0
    episode_progress = ""
    episode_status = ""
    if res.media_type == "tv":
        key = work_group_key(res)
        sibs = [s for s in db.scalars(select(Resource)).all()
                if work_group_key(s) == key]
        videos = [s for s in sibs if is_video_file(s)] or sibs
        if videos:
            total_size = sum(int(s.size or 0) for s in videos)
            parsed = [work_episode_numbers(s) for s in videos]
            seasons = sorted({se for se, _ in parsed if se is not None})
            if seasons:
                whole_season_label = (f"第{seasons[0]}季" if len(seasons) == 1
                                      else f"第{seasons[0]}-{seasons[-1]}季")
            # 按 (季, 集) 去重；文件名解析不出集号时（如未识别单集）用视频文件数兜底
            eps = {(se or 0, ep) for se, ep in parsed if ep is not None}
            n = len(eps) if eps else len(videos)
            if n > 0:
                episode_count = str(n)
            # 官方总集数：同作品任一条识别结果里取最大非零值（单集都写了同一值）。
            total_episodes = max((int(getattr(s, "total_episodes", 0) or 0)
                                  for s in sibs), default=0)
            # 已更新到第几集：多季剧取最后一季的最大集号；文件名解析不出集号时
            # （未识别单集）退回本地视频数，绝不显示 0/N 这种误导值。
            if eps:
                last_season = max(se for se, _ in eps)
                updated_ep = max(ep for se, ep in eps if se == last_season)
            else:
                updated_ep = n
            cur_ep = int(episode) if episode else (updated_ep or 0)
            if total_episodes > 0 and updated_ep > 0:
                episode_progress = f"{updated_ep}/{total_episodes}"
            elif updated_ep > 0:
                episode_progress = f"第 {updated_ep} 集 / 已收录 {n} 集"
            finish = (str(res.finish_status or "")
                      or next((str(s.finish_status or "") for s in sibs
                               if str(s.finish_status or "")), ""))
            if total_episodes > 0:
                if finish == "ended" and updated_ep >= total_episodes:
                    episode_status = f"全 {total_episodes} 集 · 已完结"
                else:
                    episode_status = f"更新至第 {updated_ep} 集 / 共 {total_episodes} 集"
                    if finish == "returning":
                        episode_status += " · 连载中"
            elif updated_ep > 0:
                episode_status = f"更新至第 {updated_ep} 集 · 已收录 {n} 集"
                if finish == "ended":
                    episode_status += " · 已完结"
    season_label = whole_season_label  # v0.6.4：整剧展示用「第N季」而非单集信息
    version = version_spec(res.name)

    # 标签串按「片名 · 类型 · 地区 · 分类」打标（示例：#保镖恋人 #电影 #美国 #爱情 #惊悚）
    tag_parts = [title, type_text]
    if country_text:
        tag_parts.append(country_text)
    tag_parts.extend(genres[:3])
    tags = " ".join("#" + str(p).replace(" ", "") for p in dict.fromkeys(tag_parts) if p)

    ctx = {
        "title": esc(title),
        "overview": esc(overview_text),
        "filename": esc(res.name),
        "share_url": share_url,
        "year": esc(hint.get("year") or ""),
        "resolution": esc(hint.get("resolution") or ""),
        "size": human_size(total_size),
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
        # v0.8.9：本地已收录集数 / TMDB 官方总集数 / 已更新集号 / 当前集 / 进度 / 状态文案
        "episode_count": esc(episode_count),
        "total_episodes": str(total_episodes) if total_episodes else "",
        "updated_to_episode": str(updated_ep) if updated_ep else "",
        "current_episode": str(cur_ep) if cur_ep else "",
        "episode_progress": esc(episode_progress),
        "episode_status": esc(episode_status),
        "reward_code": esc(runtime_config.get(db, "reward_code") or ""),  # v0.7.0：赞赏码
        "version": esc(version),
    }
    text = render_template(tpl.content if tpl else _FALLBACK_TEMPLATE, ctx).strip()
    return text[:1000], text[:1000]


async def _send_telegram(db: Session, q: QueueItem, res: Resource, channel: Channel, bot: Bot,
                         override_token: str = "", override_chat: str = "") -> tuple[bool, str, str]:
    # v0.7.0：custom_bot_group 模式下用配置的自定义 bot token / 群聊覆盖
    token = override_token or decrypt_secret(bot.token)
    chat_id = override_chat or channel.chat_id
    if not token:
        return False, "", "Bot Token 为空", None
    caption, _ = _build_message(db, res, res.share_url)
    poster = tmdb.poster_url(res.poster_path) if res.poster_path else ""
    base = runtime_config.telegram_base(db)
    method = "sendPhoto" if poster else "sendMessage"
    payload = {"chat_id": chat_id, "caption" if poster else "text": caption,
               "parse_mode": "HTML", "disable_web_page_preview": False}
    if poster:
        payload["photo"] = poster
    # v0.7.0：分享链接按钮化 + 福袋 bot 跳转入口（inline keyboard）
    link_style = (runtime_config.get(db, "tg_link_style") or "button").strip()
    if link_style == "button" and res.share_url:
        buttons = [{"text": "📥 获取资源", "url": res.share_url}]
        fubao = (runtime_config.get(db, "tg_fubao_bot") or "").strip().lstrip("@")
        if fubao:
            buttons.append({"text": "🎁 福袋/入库", "url": f"https://t.me/{fubao}?start=library"})
        payload["reply_markup"] = {"inline_keyboard": [buttons]}
        # 按钮模式下去掉正文末行的纯文本链接，避免重复
        caption = re.sub(r"\n?https?://\S+\s*$", "", caption).rstrip()
        if poster:
            payload["caption"] = caption
        else:
            payload["text"] = caption
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


def _work_group_key(res: Resource) -> str:
    """运行时整剧去重键（与后端展示/入队口径一致）：目录 > TMDB > 单文件。"""
    dir_id = str(getattr(res, "share_dir_id", "") or "")
    if dir_id:
        return f"dir:{dir_id}"
    if res.tmdb_id:
        return f"tmdb:{res.media_type or ''}:{res.tmdb_id}"
    return f"file:{res.id}"


async def process_queue(limit: int = 20) -> dict:
    db: Session = SessionLocal()
    sent = failed = shared = 0
    # v0.6.5：逐条发送节流，避免一次轮询连发多条触发 Telegram 风控
    try:
        delay = max(0, min(int(runtime_config.get(db, "telegram_send_delay") or 2), 60))
    except ValueError:
        delay = 2
    # v0.7.0：推送模式。custom_bot_group 时用自定义 bot/群聊覆盖，其余用频道自身 bot
    push_mode = (runtime_config.get(db, "tg_push_mode") or "channel").strip()
    ov_token = ov_chat = ""
    if push_mode == "custom_bot_group":
        ov_token = (runtime_config.get(db, "tg_custom_bot_token") or "").strip()
        ov_chat = (runtime_config.get(db, "tg_custom_chat_id") or "").strip()
    try:
        now = utcnow()
        rows = list(db.scalars(
            select(QueueItem).where(
                or_(QueueItem.status == "pending", QueueItem.status == "retry"),
                or_(QueueItem.next_retry_at.is_(None), QueueItem.next_retry_at <= now),
            ).order_by(QueueItem.id).limit(limit)
        ).all())
        for idx, q in enumerate(rows):
            if idx and delay:
                await asyncio.sleep(delay)  # v0.6.5：与上一条消息保持间隔，规避 TG 风控
            res = db.get(Resource, q.resource_id)
            channel = db.get(Channel, q.channel_id)
            bot = db.get(Bot, channel.bot_id) if channel else None
            if not res or not channel or not bot:
                q.status, q.error = "failed", "资源、频道或 Bot 已不存在（可能被删除）"
                failed += 1
                continue
            # v0.6.5 兜底：同一作品同一频道只要已有 sent 项（整剧已推过），
            # 其余同作品残留项一律跳过，绝不逐集再推。即便旧队列/手动入队漏收敛也拦得住。
            wkey = _work_group_key(res)
            dup_sent = None
            for other_id, other_rid in db.execute(
                select(QueueItem.id, QueueItem.resource_id).where(
                    QueueItem.channel_id == q.channel_id,
                    QueueItem.status == "sent",
                    QueueItem.id != q.id)
            ).all():
                other_res = db.get(Resource, other_rid)
                if other_res and _work_group_key(other_res) == wkey:
                    dup_sent = other_id
                    break
            if dup_sent:
                q.status = "sent"
                q.error = f"同作品已由队列项 #{dup_sent} 整剧推送，本单集跳过（去重）"
                q.sent_at = utcnow()
                continue
            if not bot.enabled or not channel.enabled:
                # v0.6.1：Bot 与频道各自独立启停。停用不算失败、不消耗重试次数，
                # 重新启用后下一轮 worker 会自动继续，且不影响其它链路。
                q.error = "Bot 或频道已停用，暂不推送；启用后自动继续"
                q.next_retry_at = None
                continue
            # v0.9.0：推送白名单闸门。Bot 只向管理员配置的频道 / 群聊主动推送，
            # 不私聊刷屏。白名单为空时保持兼容不拦截；非空时只放行列出的 chat_id。
            from . import bot_listener
            allowed, deny_reason = bot_listener.is_push_allowed(db, channel.chat_id)
            if not allowed:
                q.error = deny_reason
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
            use_token = ov_token if (ov_token and ov_chat) else ""
            use_chat = ov_chat if (ov_token and ov_chat) else ""
            ok, message_id, error, retry_after = await _send_telegram(
                db, q, res, channel, bot, override_token=use_token, override_chat=use_chat)
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
                # v0.6.5：用独立变量，勿覆盖逐条发送节流的 delay，否则下一条的间隔会被退避秒数篡改
                retry_delay = retry_after or backoff_seconds(q.attempts)
                q.next_retry_at = plus_seconds(utcnow(), retry_delay)
                q.status = "retry" if q.attempts < MAX_ATTEMPTS else "failed"
                failed += 1
        db.commit()
        return {"sent": sent, "failed": failed, "shares_created": shared}
    finally:
        db.close()
