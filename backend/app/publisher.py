import asyncio
import html
import json
import logging
import re
from datetime import timedelta

import httpx
from sqlalchemy import select, or_, case
from sqlalchemy.orm import Session

from . import runtime_config, tmdb
from .config import settings
from .db import DEFAULT_TEMPLATE, SessionLocal, json_dumps
from .guangya import GuangYaError, GuangYaShareClient
from .models import AuditLog, Bot, Channel, MessageTemplate, QueueItem, Resource, utcnow
from .security import decrypt_secret
from .utils import (backoff_seconds, is_video_file, parse_media_name, parse_tech_spec,
                    plus_seconds, render_template, retry_after_seconds, version_spec,
                    work_episode_numbers, work_group_key)

MAX_ATTEMPTS = 8
_FALLBACK_TEMPLATE = DEFAULT_TEMPLATE
logger = logging.getLogger(__name__)


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
    # 阶段四：统一走光鸭建分享解析适配器（三处散点字段口径收敛，发送逻辑不变）
    from .guangya import create_share_parsed
    share_id, share_url = await create_share_parsed(client, target_id, title, settings.share_duration)
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
    # v0.9.8：用 scalars().first() 而非 scalar()。历史库可能残留多条 is_default=True
    # 或一条 enabled 模板都有的情况，scalar() 遇多行会抛 MultipleResultsFound（表现为
    # 推送 Internal Server Error）；first() 取第一条，绝不抛异常。
    return (db.scalars(select(MessageTemplate).where(
        MessageTemplate.is_default.is_(True), MessageTemplate.enabled.is_(True))).first()
            or db.scalars(select(MessageTemplate).where(
                MessageTemplate.enabled.is_(True))).first())


def _build_message(db: Session, res: Resource, share_url: str) -> tuple[str, str]:
    """构造 Telegram 消息。v0.6.0 增加 TMDB 分类打标变量。

    可用：type / type_label / origin_country / origin_country_code / genres / tags，
    以及原有 title/overview/filename/year/resolution/size/tmdb_id/tmdb_url/share_url，
    另有 vote_average/original_title/season/episode/episode_tag/media_type。
    v0.8.9 剧集状态组：episode_count（本地已收录集数）/ total_episodes（TMDB 官方总集数）
    / updated_to_episode（已更新到第几集）/ current_episode（当前集）
    / episode_progress（如 12/24）/ episode_status（v0.9.0 改为季集标签格式：
      追更「更新至 S01E12 · 连载中」/ 收齐「S01E24 · 完结」）。
    """
    esc = lambda v: html.escape(str(v if v is not None else ""), quote=False)
    title = res.media_title or res.name
    hint = parse_media_name(res.name)
    # v0.9.9 #202：技术规格分项（来源/画质/编码/音频），纯文件名解析、不联网
    spec = parse_tech_spec(res.name)
    # 季集范围变量默认值（tv 分支内会覆盖）
    episode_range = ""
    season_count = ""
    _parsed_eps: list = []
    tpl = _template(db)
    # v0.7.0：精简详情——compact 模式压缩简介；v0.9.10.2 按真机反馈从 110 字再压到 70 字
    detail_level = (runtime_config.get(db, "tg_detail_level") or "compact").strip()
    overview_text = (res.overview or "").strip()
    overview_limit = 70
    if detail_level == "compact" and len(overview_text) > overview_limit:
        overview_text = overview_text[:overview_limit].rstrip("，。、；：,;: ") + "…"

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
    # 这里补上 TMDB 官方总集数、已更新到第几集。v0.9.0：episode_status 改用
    # 季集标签——追更「更新至 S01E07 · 连载中」、收齐「S01E24 · 完结」，
    # 进度与总集数仍由 episode_progress / total_episodes 单独承载，不重复。
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
            # v0.9.9 #202：季集范围串——「S01 E01-E13」「共 N 季」供新模板分项展示
            _parsed_eps = parsed
            if seasons:
                season_count = str(len(seasons))
            if eps:
                rng_season = max(se for se, _ in eps)
                rng_eps = sorted(ep for se, ep in eps if se == rng_season)
                if rng_eps:
                    ep_part = (f"E{rng_eps[0]:02d}" if len(rng_eps) == 1
                               else f"E{rng_eps[0]:02d}-E{rng_eps[-1]:02d}")
                    episode_range = f"S{rng_season:02d} {ep_part}"
            if total_episodes > 0 and updated_ep > 0:
                episode_progress = f"{updated_ep}/{total_episodes}"
            elif updated_ep > 0:
                episode_progress = f"第 {updated_ep} 集 / 已收录 {n} 集"
            finish = (str(res.finish_status or "")
                      or next((str(s.finish_status or "") for s in sibs
                               if str(s.finish_status or "")), ""))
            # v0.9.0：按用户要求改为季集标签格式。
            # 季号取「本地已收录的最后一集」所在季（多季剧用最后一季），
            # 集号取该季最大集号；总集数/进度仍由 episode_progress 单独承载。
            status_season = (int(last_season) if eps and last_season
                             else int(season or 1) or 1)
            status_ep_tag = f"S{status_season:02d}E{int(updated_ep):02d}"
            ended = finish == "ended" and (total_episodes <= 0
                                           or updated_ep >= total_episodes)
            if ended:
                episode_status = f"{status_ep_tag} · 完结"
            elif finish == "returning":
                episode_status = f"更新至 {status_ep_tag} · 连载中"
            elif total_episodes > 0 and updated_ep > 0:
                # 官方有总集数但未标连载：只报进度标签，不臆断连载/完结
                episode_status = f"更新至 {status_ep_tag}"
            elif updated_ep > 0:
                episode_status = f"更新至 {status_ep_tag} · 已收录 {n} 集"
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
        # v0.9.5：频道/群跳转链接（与打赏配置一起维护，模板可用 {{channel_link}}/{{group_link}}）
        "channel_link": esc(runtime_config.get(db, "tg_channel_link") or ""),
        "group_link": esc(runtime_config.get(db, "tg_group_link") or ""),
        "version": esc(version),
        # v0.9.9 #202：用户新样式分项变量（来源/画质/编码/音频从文件名解析，上映时间取 TMDB）
        "source": esc(spec.get("source")),
        "quality": esc(spec.get("quality")),
        "video_codec": esc(spec.get("codec")),
        "audio_codec": esc(spec.get("audio")),
        "release_date": esc(getattr(res, "release_date", "") or ""),
        # 上映时间优先 TMDB 完整日期，回退文件名年份；渲染器不支持 {{else}}，故在 ctx 算好
        "air_date": esc(getattr(res, "release_date", "") or hint.get("year") or ""),
        # 评分整数态：TMDB 9.0 → 9，与用户样式「⭐️ 评分: 9」一致
        "vote_plain": (str(int(float(res.vote_average))) if res.vote_average
                       and float(res.vote_average) == int(float(res.vote_average))
                       else (f"{float(res.vote_average):.1f}" if res.vote_average else "")),
        # 季集范围：共 N 季 / S01 E01-E13（按本地已收录最后一季算）
        "season_count": esc(season_count),
        "episode_range": esc(episode_range),
        # v0.9.10.1：入口文字由渲染层统一追加（频道 / 群聊 / 投稿·求片 三个标签式
        # 超链接），对所有模板生效；模板里的 {{buttons_text}} 变量保持置空避免重复。
        "buttons_text": "",
    }
    # v0.9.10.1：所有模板统一在正文追加三个标签式文字超链接，替代 v0.9.11 的
    # 灰底 inline 按钮。链接取自消息模板模块配置，留空的那一项不输出。
    # 「标签形式」= Telegram HTML <a href> 可点击文字（与截图 #标签 同类观感），
    # 不是 reply_markup 按钮；bot 链接指向推送 Bot 本身。
    from . import bot_listener
    jump_lines: list[str] = []
    for label, raw in (
        ("频道", runtime_config.get(db, "tg_channel_link")),
        ("群聊", runtime_config.get(db, "tg_group_link")),
        ("投稿·求片", runtime_config.get(db, "tg_bot_link")),
    ):
        link = bot_listener._normalize_tme_link(raw)
        if link:
            jump_lines.append(f'<a href="{html.escape(link, quote=True)}">{label}</a>')
    text = render_template(tpl.content if tpl else _FALLBACK_TEMPLATE, ctx).strip()
    if jump_lines:
        # 插在末尾 share_url 之前；无 share_url 则直接追加到末尾。
        # 用 「 · 」把三标签排在同一行，避免三个独占三行显得冗长。
        tags_line = " · ".join(jump_lines)
        lines = text.split("\n")
        if lines and re.match(r"^https?://\S+$", lines[-1].strip()):
            share_line = lines.pop()
            body = "\n".join(lines).rstrip()
            text = (body + "\n" + tags_line + "\n" + share_line).strip()
        else:
            text = (text.rstrip() + "\n" + tags_line).strip()
    return text[:1000], text[:1000]


async def _send_telegram(db: Session, q: QueueItem, res: Resource, channel: Channel, bot: Bot) -> tuple[bool, str, str]:
    """v0.9.2：统一发送入口——群聊与频道都走同一条路径。

    目标恒为该 Channel 的 chat_id，发送身份恒为该 Channel 所属 Bot 的 token。
    群聊只是一条 chat_id 为群号的 Channel，与频道在发送层无任何差别；
    收发配置集中在「Bot 管理 / 频道管理」，不再有绕过 Channel 的全局覆盖分支。
    """
    token = decrypt_secret(bot.token)
    chat_id = channel.chat_id
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
    # v0.9.10.1：频道/群聊/投稿·求片 三个入口已改为正文标签式超链接（_build_message
    # 对所有模板统一追加），不再是灰底 inline 按钮；inline 键盘只保留「获取资源」。
    keyboard_rows: list[list[dict]] = []
    link_style = (runtime_config.get(db, "tg_link_style") or "button").strip()
    if link_style == "button" and res.share_url:
        keyboard_rows.append([{"text": "📥 获取资源", "url": res.share_url}])
        # 按钮模式下去掉正文末行的纯文本链接，避免重复
        caption = re.sub(r"\n?https?://\S+\s*$", "", caption).rstrip()
        if poster:
            payload["caption"] = caption
        else:
            payload["text"] = caption
    if keyboard_rows:
        payload["reply_markup"] = {"inline_keyboard": keyboard_rows}
    retry_after = None
    # v1.5.1 P0 第七次：超时可配置（默认8s，旧硬编码20s），避免单条慢请求拖垮整轮。
    try:
        http_timeout = max(1, min(60, int(runtime_config.get(
            db, "telegram_http_timeout_seconds") or 8)))
    except (TypeError, ValueError):
        http_timeout = 8
    try:
        async with httpx.AsyncClient(timeout=http_timeout) as client:
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
        desc = str(data.get("description") or data)[:300]
        # v0.9.10：sendPhoto 失败（多为 Telegram 拉不到 image.tmdb.org 的图片，
        # 国内网络常见）时自动回退 sendMessage——海报优先保留，但图拉不到也要让
        # 消息文字送达，避免「测试能收、正常推送收不到」。回退只做一次；
        # 429 限流不走回退（立即再打一次只会加剧限流），交给上层退避重试。
        # v0.9.10.2：补 HTTP 200 + ok=false（Telegram 业务失败也走回退），
        # 但仍排除 429（限流回退只会加剧）。原条件只认 4xx，漏掉 200+ok=false。
        photo_failed = (400 <= r.status_code < 500 or not data.get("ok"))
        if method == "sendPhoto" and photo_failed and r.status_code != 429:
            fallback = {"chat_id": chat_id, "text": caption,
                        "parse_mode": "HTML", "disable_web_page_preview": False}
            if payload.get("reply_markup"):
                fallback["reply_markup"] = payload["reply_markup"]
            try:
                async with httpx.AsyncClient(timeout=http_timeout) as client:
                    r2 = await client.post(f"{base}/bot{token}/sendMessage", json=fallback)
                d2 = r2.json()
            except Exception:
                d2 = {"ok": False}
            if r2.status_code < 400 and d2.get("ok"):
                return True, str((d2.get("result") or {}).get("message_id", "")), "", None
        return False, "", f"Telegram {method} 失败 HTTP {r.status_code}：{desc}", retry_after
    return True, str((data.get("result") or {}).get("message_id", "")), "", retry_after


def _work_group_key(res: Resource) -> str:
    """运行时整剧去重键（与后端展示/入队口径一致）：目录 > TMDB > 单文件。"""
    dir_id = str(getattr(res, "share_dir_id", "") or "")
    if dir_id:
        return f"dir:{dir_id}"
    if res.tmdb_id:
        return f"tmdb:{res.media_type or ''}:{res.tmdb_id}"
    return f"file:{res.id}"


def _run_share_center_v2_in_thread(limit: int = 20) -> None:
    """v2.1.0 分流层唯一生产者：在独立线程跑 v2 的 sync→plan→dispatch 一轮。

    v2 模式下的资源事件消费（含 removed/deleted 链接失效联动）由 v2.run_cycle 内的
    merge_update_windows 承担；本 helper 与 _run_share_center_in_thread 互斥调用，
    杜绝新旧两条链同时向 push_queue 写造成 push_reason/share_task_id 互相覆盖。
    同样走 to_thread + 线程内独立 Session（第六次死锁同源防护）。
    """
    from . import share_center_v2 as _sc2
    with SessionLocal() as sdb:
        try:
            _sc2.ensure_defaults(sdb)
            asyncio.run(_sc2.run_cycle(sdb, limit=max(limit, 20)))
        except Exception as exc:
            try:
                sdb.rollback()
            except Exception:
                pass
            logger.warning("[publisher] v2 分享中心周期异常（线程内已隔离）", exc_info=True)


def _run_share_center_in_thread(limit: int = 20) -> None:
    """P0 第六次死锁根治：分享中心的事件消费 + run_cycle 整体挪出事件循环线程。

    2026-11-21 真机：queue_worker 的 process_queue 每 60s 在**主协程**里直接
    `await run_cycle(db)`（publisher.py:419），其内部 build_share/submit_to_dispatch
    的同步 db.commit() 撞写锁就在主线程等 30s → 冻结整个 event loop →
    process_queue 的 _queue_lock 一直不释放 → 后续所有调度 skipped、队列停滞但
    容器 healthy（第六次锁故障，新持锁点 build_share，与前五轮不同位置）。

    main.py:_job_share_center 早在 v1.4.2 已用同款 to_thread 保护，但 queue_worker
    这条调用路径漏了。这里在独立线程用 asyncio.run 跑完整 run_cycle（含网络 await
    与全部 commit），线程内自建 Session；主线程只等一个 to_thread，零 SQLite 锁等待。
    事件消费也一并挪进来（它同样是同步 DB 写）。
    """
    from . import events as _ev
    from . import share_center as _sc
    with SessionLocal() as sdb:
        try:
            _sc.consume_resource_events(sdb, limit=100)
            asyncio.run(_sc.run_cycle(sdb, limit=max(limit, 20)))
        except Exception as exc:
            try:
                sdb.rollback()
            except Exception:
                pass
            logger.warning("[publisher] 分享中心周期异常（线程内已隔离）", exc_info=True)


async def _process_queue_impl(limit: int = 20) -> dict:
    db: Session = SessionLocal()
    sent = failed = shared = 0
    # v0.6.5：逐条发送节流，避免一次轮询连发多条触发 Telegram 风控
    try:
        delay = max(0, min(int(runtime_config.get(db, "telegram_send_delay") or 2), 60))
    except ValueError:
        delay = 2
    # v0.9.2：推送口径统一——所有目标（频道/群聊）都是一条 Channel，
    # 一律用该 Channel 所属 Bot 的 token 发到它的 chat_id，并经过白名单闸门。
    # 旧 tg_push_mode / custom_bot_* 不再参与发送（字段保留仅为兼容存量配置读取，不报错）。
    try:
        now = utcnow()
        rows = list(db.scalars(
            select(QueueItem).where(
                or_(QueueItem.status == "pending", QueueItem.status == "retry"),
                or_(QueueItem.next_retry_at.is_(None), QueueItem.next_retry_at <= now),
            ).order_by(QueueItem.id).limit(limit)
        ).all())
        # 四模块重构：分享中心主链接收确认（v1.1 默认开启）。
        # 开关关闭：rows 原样处理（旧链排障用）。开关开启：
        #   ① 先原子领取 submitted→accepted；
        #   ② 对「分享中心管理的项」（share_task_id>0）只处理已 accepted 的，未交接留下一轮，
        #      杜绝「未确认就发送」（强制项4）；
        #   ③ 对「非分享中心项」（share_task_id=0，如人工审核后放行、历史残留、临时回退期
        #      直接入队的项）照常处理走 ensure_share 旧路径，避免漏发——这些项不属分享中心，
        #      不受交接确认约束。数据已清空的新部署下几乎全是①，③只是稳健兜底。
        from . import share_center as _sc
        from . import share_center_v2 as _sc2
        # v2.1.1 用户口径「前后端仅用一套最新重构后的分享中心」：
        # 新体系（sc_* 新表 + 四级计划引擎）是唯一驱动方。旧链不再作为
        # 可切换的"另一套分享中心"——它只在总开关关闭时作为排障兜底，
        # 正常运行时绝不与新体系并行，避免双写 push_queue。
        sc_enabled = _sc.is_enabled(db)
        v2_enabled = _sc2.is_enabled(db)
        active_sc = "v2" if v2_enabled else ("off" if not sc_enabled else "legacy")
        if active_sc != "off":
            # 先驱动本轮分流周期，不依赖独立 5s job 的时序：
            # 「立即处理队列」/手动 queue/run 必须也能把 planned 任务推到 submitted，
            # 否则入队项 status_ext 为空会一直卡住不被本函数消费（真机漏发风险）。
            try:
                # P0 第六次死锁根治：事件消费 + run_cycle 含同步 commit，必须挪出
                # 事件循环线程（线程内自建 Session），否则撞写锁会冻结全站并让
                # _queue_lock 永不释放（队列停滞但容器 healthy）。
                if v2_enabled:
                    await asyncio.to_thread(_run_share_center_v2_in_thread, max(limit, 20))
                else:
                    await asyncio.to_thread(_run_share_center_in_thread, max(limit, 20))
            except Exception as exc:
                try:
                    db.rollback()
                except Exception:
                    pass
                logger.exception("[publisher] 分享中心提交周期异常（主线程兜底）")
            # 重新读取 rows（run_cycle 可能刚新建/重置了队列项的 status_ext）
            # v1.5.1 P0 队头解阻塞：原样取前 limit(20) 条时，status_ext 空的僵尸项
            # （有 share_task_id 但从未被领取）占满名额，后面的新项永远排不到（真机
            # 群聊40条pending只1条发出）。改为：
            #   ① 取样窗口放大到 sample（默认200），但发送上限仍是 limit；
            #   ② 按「可消费优先」排序——非分享中心项或已 accepted 的排前，僵尸排后；
            #   ③ 过滤后只取前 limit 条真正可消费的，绝不一次多发触发 Telegram 风控。
            sample = max(limit * 10, 200)
            consumable = or_(
                QueueItem.share_task_id == 0,
                QueueItem.share_task_id.is_(None),
                QueueItem.status_ext == _sc.ST_ACCEPTED,
            )
            rows = list(db.scalars(
                select(QueueItem).where(
                    or_(QueueItem.status == "pending", QueueItem.status == "retry"),
                    or_(QueueItem.next_retry_at.is_(None), QueueItem.next_retry_at <= now),
                ).order_by(case((consumable, 0), else_=1), QueueItem.id).limit(sample)
            ).all())
            _sc.claim_tasks(db, worker="publisher", limit=sample)
            # claim_tasks 可能把本轮窗口内 submitted 项领成 accepted，刷新可消费判定：
            rows = [q for q in rows
                    if (q.share_task_id and q.status_ext == _sc.ST_ACCEPTED)
                    or not q.share_task_id][:limit]
        # v0.9.11.1：一次性预构建「本轮涉及频道」的 sent 作品键映射，消除原逐条
        # O(N·M) 全表反查（每条 pending 都按 channel_id 扫全部 sent，再逐条
        # db.get(Resource) + 算键）。作品键 _work_group_key 只依赖 share_dir_id /
        # tmdb_id / id，这三项在识别阶段固定、发送阶段（ensure_share 只写 share_url）
        # 不再变化，可安全缓存。本轮新 sent 的项在发送成功后即时写回映射，保持
        # 「同一轮内后续同作品单集能看到前面已发送项」的原语义，避免重复推送回归。
        involved_channels = {q.channel_id for q in rows}
        sent_keys_by_channel: dict[int, dict[str, int]] = {}
        if involved_channels:
            for sid, sch, srid in db.execute(
                select(QueueItem.id, QueueItem.channel_id, QueueItem.resource_id).where(
                    QueueItem.status == "sent",
                    QueueItem.channel_id.in_(involved_channels))
            ).all():
                sres = db.get(Resource, srid)
                if sres:
                    sent_keys_by_channel.setdefault(int(sch), {})[_work_group_key(sres)] = int(sid)
        for idx, q in enumerate(rows):
            # v0.9.10：进入每条前先落盘上一条的写——绝不让未提交写事务跨下面的
            # asyncio.sleep / ensure_share(光鸭网络) / _send_telegram(TG 网络) await。
            # 真机根因：此前整轮 for 只在本末尾 commit 一次，20 条 × (2s 间隔 + 建分享 +
            # 发 TG) 可让一个写事务持锁几十秒，别的请求 DELETE push_queue 撞
            # `database is locked` 500、后台 job 被 missed。每条处理完立即 commit。
            db.commit()
            if idx and delay:
                await asyncio.sleep(delay)  # v0.6.5：与上一条消息保持间隔，规避 TG 风控
            res = db.get(Resource, q.resource_id)
            channel = db.get(Channel, q.channel_id)
            bot = db.get(Bot, channel.bot_id) if channel else None
            if not res or not channel or not bot:
                q.status, q.error = "failed", "资源、频道或 Bot 已不存在（可能被删除）"
                failed += 1
                db.commit()
                continue
            # v0.6.5 兜底：同一作品同一频道只要已有 sent 项（整剧已推过），
            # 其余同作品残留项一律跳过，绝不逐集再推。即便旧队列/手动入队漏收敛也拦得住。
            # v0.9.11.1：命中从预构建映射取（含本轮刚写回的项），不再逐条扫库。
            wkey = _work_group_key(res)
            dup_sent = sent_keys_by_channel.get(q.channel_id, {}).get(wkey)
            if dup_sent:
                q.status = "sent"
                q.error = f"同作品已由队列项 #{dup_sent} 整剧推送，本单集跳过（去重）"
                q.sent_at = utcnow()
                db.commit()
                continue
            if not bot.enabled or not channel.enabled:
                # v0.6.1：Bot 与频道各自独立启停。停用不算失败、不消耗重试次数，
                # 重新启用后下一轮 worker 会自动继续，且不影响其它链路。
                q.error = "Bot 或频道已停用，暂不推送；启用后自动继续"
                q.next_retry_at = None
                db.commit()
                continue
            # v0.9.0：推送白名单闸门。Bot 只向管理员配置的频道 / 群聊主动推送，
            # 不私聊刷屏。白名单为空时保持兼容不拦截；非空时只放行列出的 chat_id。
            from . import bot_listener
            allowed, deny_reason = bot_listener.is_push_allowed(db, channel.chat_id)
            if not allowed:
                # v0.9.10.2：不再每轮原样取出永久卡死（旧逻辑 next_retry_at=None +
                # status 仍 pending，队列看似有项却永不推进）。改为计一次退避重试，
                # 到上限转 failed 并带可诊断原因，前端队列页能直接看到卡在哪。
                q.attempts += 1
                q.error = deny_reason
                q.next_retry_at = plus_seconds(utcnow(), backoff_seconds(q.attempts))
                q.status = "retry" if q.attempts < MAX_ATTEMPTS else "failed"
                db.commit()
                continue
            # v0.9.10：下面两个 await 是网络请求。进网络前先把本条已写的字段落盘，
            # 确保等待期间不持有 SQLite 写锁。
            db.commit()
            need_share = not res.share_url
            share_error = ""
            try:
                # 四模块重构 阶段C：开关开启且队列项由分享中心管理时，只读交接的
                # ShareTask.share_url（分享中心已建好），分发中心绝不反建分享。
                sc_url = _sc.share_url_for_queue_item(db, q) if sc_enabled else None
                if sc_url is not None:
                    res.share_url = sc_url  # 可能为空=降级无链接推送
                else:
                    await ensure_share(db, res)
                    if need_share:
                        shared += 1
            except Exception as exc:
                # v0.9.10：建分享失败不再跳过发送——分享只是消息里的「获取资源」入口，
                # 失败时仍要把资源通知送出去（正文无链接、无按钮），否则群/频道一条都
                # 收不到，正好命中真机「测试能收、正常推送收不到」。错误记为 q.error，
                # 发送成功后再清掉，仅保留为本次诊断痕迹。
                share_error = f"建分享失败：{str(exc)[:600]}"
                q.error = share_error
            # v0.9.10：ensure_share 内部已 commit；发 TG 前再确保无悬挂写事务。
            db.commit()
            # 分享中心 v2：按频道/群分型取发送间隔；只读计算，等待期间绝不持写事务。
            try:
                from . import share_center_v2 as _sc2
                pacing_wait = _sc2.pacing_wait_seconds(db, q)
            except Exception:
                pacing_wait = 0.0
            if pacing_wait > 0:
                await asyncio.sleep(min(pacing_wait, 60.0))
            # v1.5.1 P0 第七次：给单条发送加硬上限，杜绝「慢请求+sendPhoto回退」
            # 把一轮拖到数分钟。超时按可重试失败计退避，不破坏逐条保序与去重。
            try:
                hard_limit = max(3, min(120, int(runtime_config.get(
                    db, "telegram_send_hard_limit_seconds") or 12)))
            except (TypeError, ValueError):
                hard_limit = 12
            try:
                ok, message_id, error, retry_after = await asyncio.wait_for(
                    _send_telegram(db, q, res, channel, bot), timeout=hard_limit)
            except asyncio.TimeoutError:
                ok, message_id, error, retry_after = (
                    False, "", f"Telegram 发送超过硬上限 {hard_limit}s（网络慢），本条退避重试", None)
            except Exception as exc:
                ok, message_id, error, retry_after = (
                    False, "", f"Telegram 发送异常：{str(exc)[:300]}", None)
            if ok:
                q.status = "sent"
                q.telegram_message_id = message_id
                q.sent_at = utcnow()
                # v0.9.11.1：写回映射——本轮后续同作品单集要能看到这条刚发的 sent，
                # 与旧逻辑「每条实时查库」语义一致，否则同剧多集会在同一轮重复推送。
                sent_keys_by_channel.setdefault(q.channel_id, {}).setdefault(wkey, int(q.id))
                # 分享失败但消息送达：保留分享诊断（带前缀），便于前端查「为何没链接」
                q.error = (f"[已送达·无资源链接] {share_error}" if share_error else "")
                q.next_retry_at = None
                sent += 1
                db.add(AuditLog(action="push_sent", target_type="queue", target_id=str(q.id),
                                detail=json_dumps({"resource_id": res.id, "channel_id": channel.id,
                                                   "message_id": message_id})))
            else:
                q.attempts += 1
                q.error = error[:1500]
                # 阶段五 #6：429（限流）/网络错误可退避重试；401/403（Token 失效、
                # Bot 被移出群、无发送权限）是永久错误，重试 MAX_ATTEMPTS 次只会浪费
                # 配额、让队列看似「在重试」实则永不成功。分级：永久错误直接 failed，
                # 状态只落在本条 QueueItem（channel_id 独立），不影响其它目标。
                err_low = str(error or "").lower()
                permanent = ("http 401" in err_low or "http 403" in err_low or
                             "bot was blocked" in err_low or "chat not found" in err_low or
                             "not enough rights" in err_low or "kicked" in err_low)
                if permanent:
                    q.status = "failed"
                    q.next_retry_at = None
                    q.error = (error + "（永久错误，不重试；请检查 Bot Token / 目标权限）")[:1500]
                else:
                    # v0.6.5：用独立变量，勿覆盖逐条发送节流的 delay，否则下一条的间隔会被退避秒数篡改
                    retry_delay = retry_after or backoff_seconds(q.attempts)
                    q.next_retry_at = plus_seconds(utcnow(), retry_delay)
                    q.status = "retry" if q.attempts < MAX_ATTEMPTS else "failed"
                failed += 1
            # 四模块重构 阶段C：发送结果回执给分享中心（终态 published/failed）
            if sc_enabled and q.share_task_id:
                try:
                    if ok:
                        _sc.mark_result(db, q.share_task_id, ok=True, message_id=message_id)
                    elif q.status == "failed":
                        _sc.mark_result(db, q.share_task_id, ok=False, error=q.error)
                except Exception:
                    pass
            # 分享中心 v2：节奏状态与任务终态回写新表；429 retry_after 覆盖静态间隔。
            if getattr(q, "push_reason", "") == "sc_v2" and getattr(q, "event_id", 0):
                try:
                    from . import share_center_v2 as _sc2
                    _sc2.mark_pacing_attempt(db, q, retry_after=float(retry_after or 0), success=ok)
                    if ok or q.status == "failed":
                        _sc2.mark_result(db, q.event_id, success=ok, error=q.error)
                except Exception:
                    db.rollback()
                    logger.warning("[publisher] 分享中心 v2 回写异常（不影响主链）", exc_info=True)
            # v0.9.10：本条发送结果立即落盘，绝不把写事务带进下一轮的 sleep / 网络 await。
            db.commit()
        return {"sent": sent, "failed": failed, "shares_created": shared}
    finally:
        db.close()


# 阶段四 B-01：定时 worker 与 HTTP「立即处理一次」互斥。
# 原实现无进程内锁，二者可并发操作同一批 pending 项，重复建分享 / 重复发 TG。
# 非阻塞拿锁：正有一轮在跑时直接返回 skipped，不排队堆积。
_queue_lock = asyncio.Lock()


async def process_queue(limit: int = 20) -> dict:
    if _queue_lock.locked():
        # v1.5.2 P0：绝不能用 "skipped" 作锁冲突标志——手动推送端点
        # main.publish_resource 会把 result 展开进响应，而它与本地 skipped=[]
        # （字符串数组）同名。bool True 覆盖数组后前端 [...d.skipped] 抛
        # TypeError: 'bool' object is not iterable（真机「道士下山」手动推送报错）。
        # 改用独立语义键 queue_busy，由调用方决定如何回显。
        return {"sent": 0, "failed": 0, "shares_created": 0,
                "queue_busy": True, "reason": "另一次队列处理进行中，本轮跳过"}
    async with _queue_lock:
        return await _process_queue_impl(limit=limit)
