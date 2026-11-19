"""v0.9.0：Telegram Bot 交互（接收消息）。

能力：
  内置指令固定四项：/求片  /投稿  /搜索  /打赏（另 /start /help 帮助）
  后台自定义指令：管理页配置 command → 说明 + 固定回复文本
  使用限制：可选「必须关注频道 / 必须加入群聊」，结果按秒缓存，未满足时拒绝执行指令
  推送白名单：Bot 只向管理员配置的频道 / 群聊主动推送，绝不私聊刷屏

说明：getUpdates / getChatMember / sendMessage 是 Telegram 官方 Bot API，不依赖光鸭抓包。
「投稿转存到光鸭」接口在需求文档 8.6 仍待验证，投稿只登记为 pending，不自动转存。
"""
from __future__ import annotations

import json
import re
import time

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from . import runtime_config, tgto
from .models import Bot, Channel, MediaRequest, Resource, UserContribution, utcnow
from .security import decrypt_secret

# v0.9.0：内置指令固定，不允许后台删除；后台只能追加自定义指令。
BUILTIN_COMMANDS = [
    {"command": "求片", "aliases": ["want", "request"], "description": "提交想看的影视，自动加入关键词监控"},
    {"command": "投稿", "aliases": ["contribute"], "description": "投稿资源，登记后由管理员审核"},
    {"command": "搜索", "aliases": ["search"], "description": "搜索资源库中的影视（接关键词）"},
    {"command": "打赏", "aliases": ["donate"], "description": "查看打赏支持方式"},
]
HELP = ("Ya-TG 指令说明\n\n"
        "/求片 剧名 [TMDB ID] [季]  — 提交想看，自动加入关键词监控\n"
        "/投稿 标题 [分享链接]      — 投稿资源，等待管理员审核\n"
        "/搜索 关键词               — 搜索资源库中的影视\n"
        "/打赏                     — 查看打赏方式\n"
        "/help                     — 查看本说明\n\n"
        "也可用下方按钮直达频道、群组、投稿求片与搜索。\n"
        "其它指令以管理员在后台配置的为准。")

_SHARE_URL = re.compile(r"https?://[^\s]+", re.I)
# 使用限制检查结果缓存：{tg_user_id: (checked_ts, ok, reason)}
_restrict_cache: dict[str, tuple[float, bool, str]] = {}


def _json_list(key: str, db: Session) -> list:
    try:
        v = json.loads(runtime_config.get(db, key) or "[]")
        return v if isinstance(v, list) else []
    except Exception:
        return []


def builtin_and_custom(db: Session) -> list[dict]:
    """给管理页「内置指令速查」用：固定四项 + 后台自定义。"""
    out = [{"command": c["command"], "description": c["description"], "builtin": True}
           for c in BUILTIN_COMMANDS]
    for item in _json_list("bot_commands_json", db):
        if not isinstance(item, dict):
            continue
        cmd = str(item.get("command") or "").strip().lstrip("/")
        if not cmd:
            continue
        out.append({"command": cmd, "description": str(item.get("description") or "").strip(),
                    "builtin": False, "reply": str(item.get("reply") or "").strip()})
    return out


def push_whitelist(db: Session) -> tuple[list[str], list[str]]:
    """v0.9.4：白名单与推送目标去重——直接读 channels 表已启用的目标。

    返回 (频道 ID 列表, 群聊 ID 列表)，按 Channel.kind 分类（default=channel）。
    老数据若仍只填了 runtime_config 的 bot_push_*_json 且 channels 表为空，
    则回退读旧配置，保持升级兼容。
    """
    rows = db.scalars(select(Channel).where(Channel.enabled.is_(True))).all()
    chans = [str(c.chat_id).strip() for c in rows
             if (getattr(c, "kind", "channel") or "channel") != "group" and str(c.chat_id or "").strip()]
    groups = [str(c.chat_id).strip() for c in rows
              if (getattr(c, "kind", "channel") or "channel") == "group" and str(c.chat_id or "").strip()]
    if chans or groups:
        return chans, groups
    # 兼容旧配置：channels 为空时仍认 runtime_config 白名单
    return ([str(x).strip() for x in _json_list("bot_push_channels_json", db) if str(x).strip()],
            [str(x).strip() for x in _json_list("bot_push_groups_json", db) if str(x).strip()])


def effective_token(db: Session) -> str:
    """v0.9.4：交互 Bot token 与 Bot 管理去重。

    优先用 interaction_bot_id 指向的 Bot（从 bots 表解密）的 token；
    未配置 / Bot 不存在 / 未启用时回退旧 contribution_bot_token。
    """
    raw_id = (runtime_config.get(db, "interaction_bot_id") or "").strip()
    if raw_id.isdigit():
        bot = db.get(Bot, int(raw_id))
        if bot and bot.enabled:
            try:
                return decrypt_secret(bot.token) or ""
            except Exception:
                return ""
        return ""
    return runtime_config.get(db, "contribution_bot_token") or ""


def gate_mode(db: Session) -> str:
    """v0.9.2：使用限制与推送白名单二选一。

    whitelist（默认）= 只按白名单过滤推送目标，用户使用指令不受关注/加群限制；
    membership = 只校验用户关注频道/加入群聊，推送不按白名单过滤；
    off = 两者都不启用。
    空值兼容旧配置：bot_restrict_enabled=true → membership，否则 whitelist。
    """
    mode = (runtime_config.get(db, "bot_gate_mode") or "").strip()
    if mode in ("whitelist", "membership", "off"):
        return mode
    if (runtime_config.get(db, "bot_restrict_enabled") or "").lower() == "true":
        return "membership"
    return "whitelist"


def is_push_allowed(db: Session, chat_id: str) -> tuple[bool, str]:
    """推送白名单闸门：仅 whitelist 模式生效；其余模式不拦截。

    白名单为空时保持兼容不拦截；非空时只放行列出的 chat。
    """
    if gate_mode(db) != "whitelist":
        return True, ""
    channels, groups = push_whitelist(db)
    allowed = set(channels) | set(groups)
    if not allowed:
        return True, ""
    chat = str(chat_id or "").strip()
    if chat in allowed:
        return True, ""
    return False, (f"目标 {chat or '空'} 不在 Bot 推送白名单；"
                   "请到「分发与互动中心 → 推送目标」添加并启用该频道 / 群聊")


async def _api(db: Session, token: str, method: str, payload: dict) -> dict:
    base = runtime_config.telegram_base(db)
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(f"{base}/bot{token}/{method}", json=payload)
    try:
        data = r.json()
    except Exception:
        return {"ok": False, "description": r.text[:200]}
    return data if isinstance(data, dict) else {"ok": False}


async def _reply(db: Session, token: str, chat_id: str, text: str,
                 reply_markup: dict | None = None) -> None:
    """只回复指令来源的 chat（频道 / 群聊 / 私聊由 poll_once 决定），不主动私推用户。"""
    payload = {"chat_id": str(chat_id), "text": text[:4000],
               "disable_web_page_preview": True}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        await _api(db, token, "sendMessage", payload)
    except Exception:
        pass  # 回复失败不影响主链路与 offset 推进


async def check_restriction(db: Session, token: str, tg_user_id: str) -> tuple[bool, str]:
    """使用限制：仅 membership 模式生效，用户需关注指定频道 / 加入指定群聊。

    v0.9.2：只校验管理员实际填写的项目（频道、群聊不必两类都配）；
    与「推送白名单」二选一——由 gate_mode 决定本机制是否生效。结果按秒数缓存。
    """
    if gate_mode(db) != "membership":
        return True, ""
    req_channels = [str(x).strip() for x in _json_list("bot_required_channels_json", db) if str(x).strip()]
    req_groups = [str(x).strip() for x in _json_list("bot_required_groups_json", db) if str(x).strip()]
    required = req_channels + req_groups
    if not required:
        return True, ""
    uid = str(tg_user_id or "")
    try:
        ttl = max(0, int(runtime_config.get(db, "bot_restrict_cache_seconds") or "120"))
    except ValueError:
        ttl = 120
    now = time.time()
    cached = _restrict_cache.get(uid)
    if cached and ttl > 0 and now - cached[0] < ttl:
        return cached[1], cached[2]
    if not token:
        return False, "机器人尚未配置 Token，无法校验关注状态"
    for chat in required:
        try:
            data = await _api(db, token, "getChatMember",
                              {"chat_id": chat, "user_id": uid})
        except Exception:
            data = {}
        status = str(((data.get("result") or {}) if data.get("ok") else {}).get("status") or "")
        # kicked / left / restricted 均视为未满足；restricted 仅在可发消息时通过
        if status not in ("creator", "administrator", "member"):
            reason = (f"请关注频道 / 加入群聊后再使用指令：{chat}"
                      "（管理员可把频道 @用户名 或 ID 填到使用限制里）")
            _restrict_cache[uid] = (now, False, reason)
            return False, reason
    _restrict_cache[uid] = (now, True, "")
    return True, ""


# ---------- v0.9.2：Bot 真交互菜单（inline keyboard + callback 路由） ----------
# callback_data 全部用英文短码，控制在 64 字节内（Telegram 硬限制）。
CB_HOME = "m.home"
CB_CHANNELS = "m.ch"
CB_GROUPS = "m.grp"
CB_ACTIONS = "m.act"
CB_WANT = "m.want"
CB_CONTRIB = "m.contrib"
CB_SEARCH = "m.search"
CB_DONATE = "m.donate"
async def notify_user(db: Session, tg_user_id: str, text: str) -> bool:
    """v0.9.2：审核投稿 / 求片后主动回复用户一次（非刷屏，属事务性通知）。

    交互 Bot Token 由管理页配置；失败只返回 False，不影响审核主链路。
    """
    token = effective_token(db)
    if not token or not tg_user_id or not text:
        return False
    try:
        await _api(db, token, "sendMessage",
                   {"chat_id": str(tg_user_id), "text": text[:4000],
                    "disable_web_page_preview": True})
        return True
    except Exception:
        return False


def main_keyboard() -> dict:
    """两列主菜单：频道 / 群组 / 投稿求片 / 搜索 / 打赏。"""
    return {"inline_keyboard": [
        [{"text": "📺 官方频道", "callback_data": CB_CHANNELS},
         {"text": "👥 交流群组", "callback_data": CB_GROUPS}],
        [{"text": "📤 投稿 / 求片", "callback_data": CB_ACTIONS},
         {"text": "🔍 搜索资源", "callback_data": CB_SEARCH}],
        [{"text": "💝 打赏支持", "callback_data": CB_DONATE}],
    ]}


def actions_keyboard() -> dict:
    return {"inline_keyboard": [
        [{"text": "🎬 我要求片", "callback_data": CB_WANT},
         {"text": "📤 我要投稿", "callback_data": CB_CONTRIB}],
        [{"text": "« 返回主菜单", "callback_data": CB_HOME}],
    ]}


def back_keyboard() -> dict:
    return {"inline_keyboard": [[{"text": "« 返回主菜单", "callback_data": CB_HOME}]]}


def _chat_ref(raw: str) -> tuple[str, str]:
    """把一个 chat 引用转成 (显示名, t.me 链接或空)。兼容 @用户名 / 数字 ID。"""
    s = str(raw or "").strip()
    if not s:
        return "", ""
    if s.startswith("@"):
        uname = s.lstrip("@")
        return s, f"https://t.me/{uname}"
    # 数字频道/群 ID（-100...）无法生成可直接加入的 t.me 链接，展示为可复制文本
    return s, ""


def _donate_text(db: Session) -> str:
    if (runtime_config.get(db, "donation_enabled") or "").lower() != "true":
        return "暂未开启打赏，感谢支持。"
    parts = [runtime_config.get(db, "donation_text") or "感谢支持！"]
    img = runtime_config.get(db, "donation_image_url") or ""
    if img:
        parts.append(f"打赏码：{img}")
    return "\n".join(parts)


def _menu_text(db: Session, data: str) -> tuple[str, dict | None]:
    """根据 callback_data 生成菜单回复文本与键盘。"""
    if data == CB_HOME:
        return ("请选择功能：\n\n求片/投稿/搜索用下方按钮；"
                "也可直接发送 /求片 剧名、/投稿 标题 链接、/搜索 关键词。", main_keyboard())
    if data == CB_ACTIONS:
        return "求片：提交想看的影视，自动加入关键词监控。\n投稿：登记你的资源分享链接，等待管理员审核。", actions_keyboard()
    if data == CB_CHANNELS:
        channels, _ = push_whitelist(db)
        if not channels:
            return "管理员尚未配置官方频道。", back_keyboard()
        lines = ["📺 官方频道（需管理员权限加入）："]
        for c in channels:
            name, link = _chat_ref(c)
            lines.append(f"· {name}" + (f"  {link}" if link else ""))
        return "\n".join(lines), back_keyboard()
    if data == CB_GROUPS:
        _, groups = push_whitelist(db)
        if not groups:
            return "管理员尚未配置交流群组。", back_keyboard()
        lines = ["👥 交流群组（可直接加入）："]
        for g in groups:
            name, link = _chat_ref(g)
            lines.append(f"· {name}" + (f"  {link}" if link else ""))
        return "\n".join(lines), back_keyboard()
    if data == CB_WANT:
        return ("🎬 求片用法：\n/求片 剧名 [TMDB ID] [季]\n"
                "例：/求片 伟大的长征 297253 1\n提交后自动加入关键词监控。", back_keyboard())
    if data == CB_CONTRIB:
        return ("📤 投稿用法：\n/投稿 标题 分享链接\n"
                "也可直接把分享链接发给我，我会自动登记，管理员审核后处理。", back_keyboard())
    if data == CB_SEARCH:
        return "🔍 搜索用法：\n/搜索 关键词\n例：/搜索 保镖恋人", back_keyboard()
    if data == CB_DONATE:
        return _donate_text(db), back_keyboard()
    return "未知操作，请重试。", main_keyboard()


async def _handle_callback(db: Session, token: str, cb: dict) -> str:
    """处理一个 callback_query：先 answer 消掉按钮 loading，再编辑原消息。"""
    cb_id = str(cb.get("id") or "")
    data = str(cb.get("data") or "")
    msg = cb.get("message") or {}
    chat_id = str((msg.get("chat") or {}).get("id") or "")
    message_id = msg.get("message_id")
    user = cb.get("from") or {}
    user_id = str(user.get("id") or chat_id)
    if cb_id:
        await _api(db, token, "answerCallbackQuery", {"callback_query_id": cb_id})
    ok, reason = await check_restriction(db, token, user_id)
    if not ok:
        await _api(db, token, "sendMessage",
                   {"chat_id": chat_id, "text": reason[:4000]})
        return "blocked"
    text, keyboard = _menu_text(db, data)
    payload = {"chat_id": chat_id, "message_id": message_id,
               "text": text[:4000], "disable_web_page_preview": True}
    if keyboard:
        payload["reply_markup"] = keyboard
    if message_id:
        r = await _api(db, token, "editMessageText", payload)
        if r.get("ok"):
            return "menu"
    # 编辑失败（消息太旧/未找到）则新发一条
    payload.pop("message_id", None)
    await _api(db, token, "sendMessage",
               {"chat_id": chat_id, "text": text[:4000],
                "reply_markup": keyboard or {}, "disable_web_page_preview": True})
    return "menu"


def _match_builtin(low: str) -> str:
    for c in BUILTIN_COMMANDS:
        if low == "/" + c["command"] or low == c["command"]:
            return c["command"]
        for a in c["aliases"]:
            if low == "/" + a or low == a:
                return c["command"]
    return ""


def _custom_command(db: Session, low: str):
    for item in _json_list("bot_commands_json", db):
        if not isinstance(item, dict):
            continue
        cmd = str(item.get("command") or "").strip().lstrip("/").lower()
        if cmd and low in ("/" + cmd, cmd):
            return item
    return None


async def _do_search(db: Session, keyword: str) -> str:
    kw = (keyword or "").strip()
    if not kw:
        return "用法：/搜索 关键词\n例：/搜索 保镖恋人"
    rows = list(db.scalars(select(Resource).where(
        Resource.name.like(f"%{kw}%")).order_by(Resource.id.desc()).limit(10)).all())
    if not rows:
        return f"资源库中没有匹配「{kw}」的影视。"
    lines = [f"搜索「{kw}」："]
    for r in rows:
        label = r.media_title or r.name
        lines.append(f"· {label}（{r.media_type or '未知'}）")
    return "\n".join(lines)


async def handle_text(db: Session, text: str, *, tg_user_id: str = "",
                      tg_username: str = "") -> tuple[str, str]:
    """处理一条用户文本，返回 (动作, 回复文本)。写库但不在此处 commit。"""
    raw = (text or "").strip()
    if not raw:
        return "ignored", ""
    cmd, *rest = raw.split(None, 1)
    arg = (rest[0] if rest else "").strip()
    low = cmd.lower().split("@")[0]

    if low in ("/help", "/start", "/帮助"):
        return "help", HELP

    builtin = _match_builtin(low)
    custom = None if builtin else _custom_command(db, low)
    if not builtin and not custom and not _SHARE_URL.search(raw):
        return "ignored", ""

    if builtin == "打赏" or (custom and str(custom.get("command") or "").lower() in ("donate", "打赏")):
        if (runtime_config.get(db, "donation_enabled") or "").lower() != "true":
            return "donate", "暂未开启打赏，感谢支持。"
        parts = [runtime_config.get(db, "donation_text") or "感谢支持！"]
        img = runtime_config.get(db, "donation_image_url") or ""
        if img:
            parts.append(f"打赏码：{img}")
        return "donate", "\n".join(parts)

    if builtin == "搜索":
        return "search", await _do_search(db, arg)

    if builtin == "求片":
        if not arg:
            return "want_invalid", "用法：/求片 剧名 [TMDB ID] [季]\n例：/求片 伟大的长征 297253 1"
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

    if builtin == "投稿":
        urls = _SHARE_URL.findall(arg)
        share_url = urls[-1] if urls else ""
        title = _SHARE_URL.sub("", arg).strip() or (share_url or "未命名投稿")
        db.add(UserContribution(tg_user_id=str(tg_user_id), tg_username=tg_username,
                                title=title[:512], content=raw[:4000],
                                share_url=share_url, status="pending",
                                note="投稿转存接口待验证，当前仅登记，不自动入库"))
        return "contribute", (f"投稿已登记：{title}\n管理员审核后处理。"
                              "（转存接口尚未验证，暂不自动入库）")

    if custom:
        return "custom", str(custom.get("reply") or custom.get("description") or "已收到指令。")

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
    """拉取一次 Bot 更新。返回统计；关闭或未配置时直接跳过，不产生网络请求。

    v0.9.2：新增 callback_query（inline 菜单按钮点击路由）。
    """
    if (runtime_config.get(db, "contribution_enabled") or "").lower() != "true":
        return {"enabled": False, "messages": 0}
    token = effective_token(db)
    if not token:
        return {"enabled": False, "messages": 0, "error": "未配置交互 Bot（请在 Bot 管理添加并选用，或配置投稿 Bot Token）"}
    try:
        offset = int(runtime_config.get(db, "contribution_update_offset") or 0)
    except ValueError:
        offset = 0
    base = runtime_config.telegram_base(db)
    try:
        async with httpx.AsyncClient(timeout=25) as client:
            r = await client.get(f"{base}/bot{token}/getUpdates",
                                 params={"offset": offset, "timeout": 2,
                                         "allowed_updates": '["message","callback_query"]'})
        data = r.json()
    except Exception as exc:
        return {"enabled": True, "messages": 0, "error": str(exc)[:200]}
    updates = data.get("result") or [] if isinstance(data, dict) else []
    stats = {"enabled": True, "messages": len(updates), "handled": 0,
             "blocked": 0, "callbacks": 0, "actions": []}
    for upd in updates:
        uid = int(upd.get("update_id") or 0)
        offset = max(offset, uid + 1)
        # v0.9.2：inline 菜单按钮点击
        cb = upd.get("callback_query") or {}
        if cb:
            try:
                out = await _handle_callback(db, token, cb)
                if out == "blocked":
                    stats["blocked"] += 1
                else:
                    stats["callbacks"] += 1
            except Exception as exc:
                stats.setdefault("errors", []).append(str(exc)[:120])
            continue
        msg = upd.get("message") or upd.get("channel_post") or {}
        text = str(msg.get("text") or msg.get("caption") or "")
        chat = msg.get("chat") or {}
        user = msg.get("from") or {}
        chat_id = str(chat.get("id") or "")
        user_id = str(user.get("id") or chat_id)
        try:
            # v0.9.0：使用限制——未关注频道 / 未加群的用户拒绝执行指令
            ok, reason = await check_restriction(db, token, user_id)
            if not ok:
                stats["blocked"] = stats.get("blocked", 0) + 1
                if chat_id and text.strip().startswith("/"):
                    await _reply(db, token, chat_id, reason)
                continue
            action, reply = await handle_text(
                db, text, tg_user_id=user_id,
                tg_username=str(user.get("username") or ""))
            if action != "ignored":
                stats["handled"] += 1
                stats["actions"].append(action)
            if reply and chat_id:
                # /start、/help 附带 inline 主菜单，让用户真正看到可点的功能入口
                if action == "help" and text.strip().lower().split("@")[0] in ("/start", "/help"):
                    await _reply(db, token, chat_id, reply, reply_markup=main_keyboard())
                else:
                    await _reply(db, token, chat_id, reply)
        except Exception as exc:
            stats.setdefault("errors", []).append(str(exc)[:120])
    runtime_config.set_many(db, {"contribution_update_offset": str(offset)})
    db.commit()
    return stats
