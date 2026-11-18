"""v0.8.0：Telegram 用户互动（Bot 接收消息）。

Bot 此前只有发送能力，本项目新增 getUpdates 轮询，支持：
  /want <剧名> [TMDB ID] [季]   用户想看 → 登记 + TgtoDrive 关键词监控
  /contribute <标题> [分享链接]  用户投稿 → 登记待处理
  /donate                        打赏信息
  /help                          指令说明

说明：getUpdates / sendMessage 是 Telegram 官方 Bot API，不依赖光鸭抓包。
「投稿转存到光鸭」所用的光鸭接口在需求文档 8.6 仍标记为待验证，因此投稿只登记
为 pending，**不会**自动转存、不会写入 B 库、不会触发任何推送。
"""
from __future__ import annotations

import re

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from . import runtime_config, tgto
from .models import MediaRequest, UserContribution, utcnow

HELP = ("Ya-TG 用户互动指令\n\n"
        "/want 剧名 [TMDB ID] [季]  — 提交想看的影视，自动加入关键词监控\n"
        "/contribute 标题 [分享链接] — 投稿资源（登记后由管理员处理）\n"
        "/donate — 查看打赏信息\n"
        "/help — 查看本说明")

_SHARE_URL = re.compile(r"https?://[^\s]+", re.I)


async def _api(db: Session, token: str, method: str, payload: dict) -> dict:
    base = runtime_config.telegram_base(db)
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(f"{base}/bot{token}/{method}", json=payload)
    try:
        data = r.json()
    except Exception:
        return {"ok": False, "description": r.text[:200]}
    return data if isinstance(data, dict) else {"ok": False}


async def _reply(db: Session, token: str, chat_id: str, text: str) -> None:
    try:
        await _api(db, token, "sendMessage",
                   {"chat_id": str(chat_id), "text": text[:4000],
                    "disable_web_page_preview": True})
    except Exception:
        pass  # 回复失败不影响主链路与 offset 推进


async def handle_text(db: Session, text: str, *, tg_user_id: str = "",
                      tg_username: str = "") -> tuple[str, str]:
    """处理一条用户文本，返回 (动作, 回复文本)。写库但不在此处 commit。"""
    raw = (text or "").strip()
    if not raw:
        return "ignored", ""
    cmd, *rest = raw.split(None, 1)
    arg = (rest[0] if rest else "").strip()
    low = cmd.lower().split("@")[0]

    if low in ("/help", "/start"):
        return "help", HELP

    if low == "/donate":
        if (runtime_config.get(db, "donation_enabled") or "").lower() != "true":
            return "donate", "暂未开启打赏，感谢支持。"
        parts = [runtime_config.get(db, "donation_text") or "感谢支持！"]
        img = runtime_config.get(db, "donation_image_url") or ""
        if img:
            parts.append(f"打赏码：{img}")
        return "donate", "\n".join(parts)

    if low == "/want":
        if not arg:
            return "want_invalid", "用法：/want 剧名 [TMDB ID] [季]\n例：/want 伟大的长征 297253 1"
        bits = arg.split()
        tmdb_id = None
        season = None
        tail = []
        for b in bits:
            if b.isdigit() and len(b) >= 5 and tmdb_id is None:
                tmdb_id = int(b)
            elif b.isdigit() and season is None and tmdb_id is not None:
                season = int(b)
            else:
                tail.append(b)
        title = " ".join(tail).strip() or arg
        kid = tgto.keyword_id(tmdb_id, season, title)
        req = MediaRequest(tg_user_id=str(tg_user_id), tg_username=tg_username,
                           title=title, tmdb_id=tmdb_id, media_type="tv" if season else "",
                           season=season, keyword_id=kid, status="pending")
        db.add(req)
        db.flush()
        try:
            r = await tgto.upsert_keyword(db, title=title, tmdb_id=tmdb_id,
                                          media_type=req.media_type or "tv", season=season)
            req.status = "synced" if r.get("ok") else "failed"
            req.note = r.get("message") or r.get("status") or ""
            if r.get("ok"):
                req.synced_at = utcnow()
            reply = (f"已加入监控：{title}（规则 {kid}）" if r.get("ok")
                     else f"登记成功，但关键词同步未生效：{req.note}")
        except Exception as exc:
            req.status, req.note = "failed", str(exc)[:300]
            reply = f"已登记「{title}」，但 TgtoDrive 未配置或不可达：{req.note}"
        return "want", reply

    if low == "/contribute":
        urls = _SHARE_URL.findall(arg)
        share_url = urls[-1] if urls else ""
        title = _SHARE_URL.sub("", arg).strip() or (share_url or "未命名投稿")
        db.add(UserContribution(tg_user_id=str(tg_user_id), tg_username=tg_username,
                                title=title[:512], content=raw[:4000],
                                share_url=share_url, status="pending",
                                note="投稿转存接口待验证，当前仅登记，不自动入库"))
        return "contribute", (f"投稿已登记：{title}\n管理员审核后处理。"
                              "（转存接口尚未验证，暂不自动入库）")

    # 普通消息里带链接也按投稿登记，降低用户使用门槛
    if _SHARE_URL.search(raw):
        share_url = _SHARE_URL.findall(raw)[-1]
        title = _SHARE_URL.sub("", raw).strip()[:512] or "未命名投稿"
        db.add(UserContribution(tg_user_id=str(tg_user_id), tg_username=tg_username,
                                title=title, content=raw[:4000], share_url=share_url,
                                status="pending", note="自动识别为投稿；转存接口待验证"))
        return "contribute", f"已收到资源链接并登记投稿（{title}），等待管理员处理。"
    return "ignored", ""


async def poll_once(db: Session) -> dict:
    """拉取一次 Bot 更新。返回统计；关闭或未配置时直接跳过，不产生网络请求。"""
    if (runtime_config.get(db, "contribution_enabled") or "").lower() != "true":
        return {"enabled": False, "messages": 0}
    token = runtime_config.get(db, "contribution_bot_token") or ""
    if not token:
        return {"enabled": False, "messages": 0, "error": "未配置投稿 Bot Token"}
    try:
        offset = int(runtime_config.get(db, "contribution_update_offset") or 0)
    except ValueError:
        offset = 0
    base = runtime_config.telegram_base(db)
    try:
        async with httpx.AsyncClient(timeout=25) as client:
            r = await client.get(f"{base}/bot{token}/getUpdates",
                                 params={"offset": offset, "timeout": 2,
                                         "allowed_updates": '["message"]'})
        data = r.json()
    except Exception as exc:
        return {"enabled": True, "messages": 0, "error": str(exc)[:200]}
    updates = data.get("result") or [] if isinstance(data, dict) else []
    stats = {"enabled": True, "messages": len(updates), "handled": 0, "actions": []}
    for upd in updates:
        uid = int(upd.get("update_id") or 0)
        offset = max(offset, uid + 1)
        msg = upd.get("message") or upd.get("channel_post") or {}
        text = str(msg.get("text") or msg.get("caption") or "")
        chat = msg.get("chat") or {}
        user = msg.get("from") or {}
        try:
            action, reply = await handle_text(
                db, text, tg_user_id=str(user.get("id") or chat.get("id") or ""),
                tg_username=str(user.get("username") or ""))
            if action != "ignored":
                stats["handled"] += 1
                stats["actions"].append(action)
            if reply and chat.get("id"):
                await _reply(db, token, str(chat["id"]), reply)
        except Exception as exc:
            stats.setdefault("errors", []).append(str(exc)[:120])
    runtime_config.set_many(db, {"contribution_update_offset": str(offset)})
    db.commit()
    return stats
