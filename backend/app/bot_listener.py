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

import hashlib
import json
import re
import time

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from . import runtime_config, tgto, tmdb
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
        "/求片 剧名               — 列出 TMDB 候选，点选提交（推荐）\n"
        "/求片 剧名 TMDB_ID 季    — 精确提交，跳过候选\n"
        "/投稿 剧名               — 选 TMDB 候选后补光鸭链接\n"
        "/投稿 标题 分享链接      — 一步提交投稿\n"
        "/搜索 关键词             — 搜索资源库中的影视\n"
        "/打赏                    — 查看打赏方式\n"
        "/help                    — 查看本说明\n\n"
        "也可直接点输入框上方的菜单按钮：求片 / 投稿 / 搜索 / 打赏。\n"
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


def push_routes_raw(db: Session) -> list[dict]:
    """v0.9.8：目录路由表的原始配置（供管理页回填）。

    每条同时可填 channel_id / group_id 两个独立目标（任选其一或两个都填）：
    {dir_id, channel_id, group_id, note}。老数据只有 {kind, chat_id} 时归一到对应字段。
    """
    try:
        raw = json.loads(runtime_config.get(db, "push_routes_json") or "[]")
    except Exception:
        raw = []
    out = []
    if not isinstance(raw, list):
        return out
    for item in raw[:10]:
        if not isinstance(item, dict):
            continue
        channel_id = str(item.get("channel_id") or "").strip()
        group_id = str(item.get("group_id") or "").strip()
        # 兼容老数据：只有 kind/chat_id 时归入对应字段
        if not channel_id and not group_id:
            legacy_chat = str(item.get("chat_id") or "").strip()
            if legacy_chat:
                if str(item.get("kind") or "").lower() == "group":
                    group_id = legacy_chat
                else:
                    channel_id = legacy_chat
        if not channel_id and not group_id:
            continue
        out.append({"dir_id": str(item.get("dir_id") or "").strip(),
                    "channel_id": channel_id, "group_id": group_id,
                    "note": str(item.get("note") or "").strip()[:120]})
    return out


def push_routes(db: Session) -> list[dict]:
    """把原始路由展开成「一个目标一项」的列表，供匹配 / 推送 / 群发消费。

    v0.9.8：一条路由可同时含频道与群两个目标，展开后返回两条 {dir_id, kind, chat_id, note}；
    下游 match_push_routes / route_target_channel / 群发逻辑保持不变。
    """
    out = []
    for r in push_routes_raw(db):
        for kind, chat in (("channel", r["channel_id"]), ("group", r["group_id"])):
            if chat:
                out.append({"dir_id": r["dir_id"], "kind": kind, "chat_id": chat,
                            "note": r["note"]})
    return out


def match_push_routes(db: Session, resource) -> list[dict]:
    """返回与资源目录匹配的路由（可多条）。

    匹配规则：dir_id 为空 = 根目录通配（匹配所有资源，即整库推送）；
    否则精确匹配 resource.parent_id，或出现在 resource.dir_chain_json 目录链中。
    """
    routes = push_routes(db)
    if not routes:
        return []
    parent = str(getattr(resource, "parent_id", "") or "").strip()
    try:
        chain = set(str(x) for x in json.loads(getattr(resource, "dir_chain_json", "[]") or "[]"))
    except Exception:
        chain = set()
    matched = []
    for r in routes:
        dir_id = r["dir_id"]
        if not dir_id or dir_id == parent or dir_id in chain:
            matched.append(r)
    return matched


def route_target_channel(db: Session, route: dict):
    """把路由项的 chat_id 解析为可用的启用 Channel（频道/群聊统一记录）。

    路由表是配置真相源；Channel 记录由保存路由时同步维护。若记录缺失（老数据/手工改库），
    这里按 chat_id 现场匹配一条启用 Channel，避免路由配了却推不出去。
    """
    rows = db.scalars(select(Channel).where(Channel.enabled.is_(True))).all()
    for c in rows:
        if str(c.chat_id or "").strip() == route["chat_id"]:
            return c
    return None


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


async def sync_bot_command_menu(db: Session, token: str, *, clear: bool = False) -> dict:
    """v0.9.9 #207：同步 / 清空 Telegram 私聊指令栏（setMyCommands）。

    真机上曾残留其它项目的旧指令栏（/bind /unbind /profile /register /code /renew，
    用户截图反馈）。这些是 Telegram 服务端的持久状态，只设一次就一直在，除非被
    覆盖/清空——所以 v0.9.9 起在 poll_once 里按 token 指纹自动清空一次，无需用户手动点。

    两种策略：
      - clear=True：传空数组彻底置空。用户已转向「点 Reply 键盘 + 直接输入文字」，
        不需要 "/" 下拉指令栏，置空可永久消除旧项目残留。
      - clear=False：注册当前项目 6 个英文指令（Telegram 指令只能英文）。
    与 Reply 键盘（main_reply_keyboard）互不冲突：键盘在输入框上方，
    指令栏是输入法 "/" 触发的下拉，两者可共存。
    """
    commands = [] if clear else [
        {"command": "start", "description": "开始使用 / 打开菜单"},
        {"command": "help", "description": "查看指令说明"},
        {"command": "want", "description": "求片：提交想看的影视"},
        {"command": "contribute", "description": "投稿：登记影视资源"},
        {"command": "search", "description": "搜索资源库影视"},
        {"command": "donate", "description": "打赏支持方式"},
    ]
    data = await _api(db, token, "setMyCommands", {"commands": commands})
    ok = bool(data.get("ok"))
    return {"ok": ok, "count": len(commands),
            "description": str(data.get("description") or "")[:200]}


# v0.9.9 #207：指令栏自动清理。setMyCommands 是 Telegram 服务端持久状态，旧项目经
# BotFather 设过的指令会一直挂在 "/" 下拉里，用户不手动清就永远显示。用 token 指纹
# 记录「已自动清过」的 bot，进程内 + runtime_config 双保险，每个 token 只清一次，
# 避免 poll_once 每 10 秒都打一次 setMyCommands 被 TG 限流。
_AUTO_CLEARED_TOKENS: set[str] = set()


async def auto_clear_stale_commands(db: Session, token: str) -> bool:
    """每个 Bot token 首次进入轮询时自动清空一次 "/" 指令栏，覆盖旧项目残留。

    返回是否本次执行了清空。失败不抛异常（登录/网络问题不阻断消息轮询），
    下次进程启动会重试；成功则写 runtime_config 持久标记 + 进程内集合。
    用户仍可用管理页「同步指令栏」按钮注册当前项目指令，或再次「清空」。
    """
    if not token:
        return False
    fp = hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]
    if fp in _AUTO_CLEARED_TOKENS:
        return False
    marked = set()
    try:
        marked = set(x for x in (runtime_config.get(db, "bot_commands_cleared_fp") or "").split(",") if x)
    except Exception:
        marked = set()
    if fp in marked:
        _AUTO_CLEARED_TOKENS.add(fp)
        return False
    try:
        out = await sync_bot_command_menu(db, token, clear=True)
    except Exception:
        return False
    if not out.get("ok"):
        return False
    _AUTO_CLEARED_TOKENS.add(fp)
    marked.add(fp)
    try:
        runtime_config.set_many(db, {"bot_commands_cleared_fp": ",".join(sorted(marked))})
        db.flush()
    except Exception:
        pass
    return True


def _normalize_tme_link(raw: str) -> str:
    """把 @用户名 / t.me/用户名 / 完整 URL 归一成可点的 https://t.me/... 链接。"""
    s = str(raw or "").strip()
    if not s:
        return ""
    if s.startswith("http://") or s.startswith("https://"):
        return s
    s = s.lstrip("@")
    return f"https://t.me/{s}" if s else ""


def restriction_keyboard(db: Session) -> dict | None:
    """v0.9.5：未满足使用限制时，附上官方频道 / 交流群跳转按钮（t.me/@ 链接）。"""
    buttons = []
    ch_link = _normalize_tme_link(runtime_config.get(db, "bot_required_channel_link"))
    grp_link = _normalize_tme_link(runtime_config.get(db, "bot_required_group_link"))
    if ch_link:
        buttons.append({"text": "📺 关注官方频道", "url": ch_link})
    if grp_link:
        buttons.append({"text": "👥 加入交流群", "url": grp_link})
    return {"inline_keyboard": [buttons]} if buttons else None


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
    """使用限制（v0.9.5 独立功能）：用户须关注指定频道 / 加入指定群聊之一。

    由 bot_restrict_enabled 独立开关控制，与「推送白名单」相互独立、不再二选一。
    兼容旧配置 gate_mode=membership 作为兜底。结果按秒数缓存。
    """
    restrict_on = (runtime_config.get(db, "bot_restrict_enabled") or "").lower() == "true" \
        or gate_mode(db) == "membership"
    if not restrict_on:
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
    # v0.9.5：「关注的频道 ID / 加入的群 ID 任填其一即可」= OR 语义——
    # 管理员可填多个候选，用户只需满足其中任意一个即可使用 Bot（不再要求全部满足）。
    reason = "请先关注官方频道或加入交流群，再使用机器人功能。"
    for chat in required:
        try:
            data = await _api(db, token, "getChatMember",
                              {"chat_id": chat, "user_id": uid})
        except Exception:
            data = {}
        status = str(((data.get("result") or {}) if data.get("ok") else {}).get("status") or "")
        # kicked / left / restricted 均视为未满足；restricted 仅在可发消息时通过
        if status in ("creator", "administrator", "member"):
            _restrict_cache[uid] = (now, True, "")
            return True, ""  # 任一满足即通过
    _restrict_cache[uid] = (now, False, reason)
    return False, reason


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

# ---------- v0.9.8：求片/投稿 TMDB 候选多轮交互 ----------
# 会话按 tg_user_id 存内存（进程内，重启即失效，符合「一次性选择」场景）。
# 结构: {uid: {"flow": "want"|"contribute", "query": str, "candidates": [dict],
#              "secret": str(可选, 投稿先收链接), "created": float}}
_pick_sessions: dict[str, dict] = {}
_PICK_TTL = 600  # 10 分钟未选完则丢弃
# callback_data 中候选选择的短码（Telegram 限制 64 字节）。格式 pick.<flow>.<idx>
CB_PICK = "pk"
FLOW_WANT = "want"
FLOW_CONTRIB = "contribute"

# ---------- v0.9.9 #209：点 Reply 键盘进入会话态，后续直接输入文字即可 ----------
# 与候选会话 _pick_sessions 分开：这里只记录「用户当前处于求片/投稿模式」，
# 让用户点「求片」「投稿」按钮后不必每条都加 /命令，直接发剧名/链接。
_flow_sessions: dict[str, dict] = {}       # {uid: {"flow": want|contribute, "created": ts}}
_FLOW_TTL = 600                            # 10 分钟无操作自动退出会话
CB_END = "m.end"                           # inline「结束交互」按钮短码


def _flow_gc() -> None:
    now = time.time()
    for uid in [k for k, v in _flow_sessions.items()
                if now - float(v.get("created") or 0) > _FLOW_TTL]:
        _flow_sessions.pop(uid, None)


def _flow_set(uid: str, flow: str) -> None:
    _flow_gc()
    if str(uid):
        _flow_sessions[str(uid)] = {"flow": flow, "created": time.time()}


def _flow_get(uid: str) -> str:
    _flow_gc()
    s = _flow_sessions.get(str(uid))
    return str(s.get("flow") or "") if s else ""


def _flow_clear(uid: str) -> None:
    _flow_sessions.pop(str(uid), None)


def low_end(raw: str) -> bool:
    """识别用户主动结束交互的文字（中英文/斜杠命令都认）。"""
    s = str(raw or "").strip().lower().lstrip("/")
    return s in ("结束", "退出", "取消", "返回", "end", "exit", "cancel", "stop", "home")


def end_flow_keyboard() -> dict:
    """会话态的 inline 键盘：一个「结束交互」按钮，让用户随时退出文字模式。"""
    return {"inline_keyboard": [
        [{"text": "✅ 结束交互 / 返回主菜单", "callback_data": CB_END}]]}


def _pick_gc() -> None:
    """清理过期候选会话，避免长期运行内存泄漏。"""
    now = time.time()
    for uid in [k for k, v in _pick_sessions.items()
                if now - float(v.get("created") or 0) > _PICK_TTL]:
        _pick_sessions.pop(uid, None)


def _pick_set(uid: str, flow: str, query: str, candidates: list[dict],
              secret: str = "") -> None:
    _pick_gc()
    _pick_sessions[str(uid)] = {"flow": flow, "query": query,
                                "candidates": candidates[:8], "secret": secret,
                                "created": time.time()}


def _pick_get(uid: str) -> dict | None:
    _pick_gc()
    return _pick_sessions.get(str(uid))


def _pick_clear(uid: str) -> None:
    _pick_sessions.pop(str(uid), None)


def _candidates_keyboard(flow: str, candidates: list[dict]) -> dict:
    """候选选择 inline 键盘：每题一行，最多 8 个，附「跳过/取消」。"""
    rows = []
    for i, c in enumerate(candidates[:8]):
        title = str(c.get("title") or "?")
        year = str(c.get("year") or "")
        kind = "剧" if c.get("media_type") == "tv" else "影"
        label = f"{i + 1}. {title}" + (f" ({year})" if year else "") + f" · {kind}"
        rows.append([{"text": label[:60],
                      "callback_data": f"{CB_PICK}.{flow}.{i}"}])
    rows.append([{"text": "跳过 TMDB，手动填写", "callback_data": f"{CB_PICK}.{flow}.skip"},
                 {"text": "取消", "callback_data": f"{CB_PICK}.{flow}.cancel"}])
    return {"inline_keyboard": rows}


def _candidate_caption(flow: str, query: str, candidates: list[dict]) -> str:
    verb = "求片" if flow == FLOW_WANT else "投稿"
    lines = [f"🔍 按「{query}」找到以下 TMDB 候选，请点选{verb}对应作品：", ""]
    for i, c in enumerate(candidates[:8]):
        title = str(c.get("title") or "?")
        year = str(c.get("year") or "")
        kind = "剧集" if c.get("media_type") == "tv" else "电影"
        score = c.get("vote_average")
        rating = f" ⭐{float(score):.1f}" if score else ""
        lines.append(f"{i + 1}. {title}" + (f"（{year}）" if year else "")
                     + f" · {kind}{rating} · ID {c.get('tmdb_id')}")
    return "\n".join(lines) + "\n\n选错可点「跳过 TMDB，手动填写」。"

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
    """inline 主菜单：供已有 inline 消息与 callback 路由使用。"""
    return {"inline_keyboard": [
        [{"text": "📺 官方频道", "callback_data": CB_CHANNELS},
         {"text": "👥 交流群组", "callback_data": CB_GROUPS}],
        [{"text": "📤 投稿 / 求片", "callback_data": CB_ACTIONS},
         {"text": "🔍 搜索资源", "callback_data": CB_SEARCH}],
        [{"text": "💝 打赏支持", "callback_data": CB_DONATE}],
    ]}


def main_reply_keyboard() -> dict:
    """v0.9.6.1：/start 后弹出的 Telegram 私聊 Reply 键盘。

    用户明确要的是 Telegram 软件里 Bot 界面的菜单按钮（输入框上方的键盘），
    不是 setMyCommands 注册的底部指令栏，也不是 inline callback 按钮。
    按钮文字用中文指令名（不带斜杠），`_match_builtin()` 已同时接受两种写法。
    只回复 /start 的来源 chat，不主动私推；one_time_keyboard=False 保持常驻。
    """
    return {"keyboard": [["求片", "投稿"], ["搜索", "打赏"]],
            "resize_keyboard": True, "one_time_keyboard": False,
            "selective": True}


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
        return "求片：提交想看的影视，自动加入关键词监控。\n投稿：登记你的资源分享链接，我们会尽快处理。", actions_keyboard()
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
        return ("🎬 求片：直接发送剧名，我会列出 TMDB 候选让你点选（推荐）。\n"
                "也可精确提交：/求片 剧名 TMDB_ID 季\n例：/求片 伟大的长征 297253 1", back_keyboard())
    if data == CB_CONTRIB:
        return ("📤 投稿：先发剧名选 TMDB 候选，再补光鸭分享链接。\n"
                "也可一步提交：/投稿 标题 分享链接\n"
                "也可直接把分享链接发给我，我会自动为你登记，感谢支持！", back_keyboard())
    if data == CB_SEARCH:
        return "🔍 搜索用法：\n/搜索 关键词\n例：/搜索 保镖恋人", back_keyboard()
    if data == CB_DONATE:
        return _donate_text(db), back_keyboard()
    return "未知操作，请重试。", main_keyboard()


async def _edit_or_send(db, token, chat_id, message_id, text: str,
                        keyboard: dict | None) -> str:
    """编辑原消息；失败则新发一条。供候选交互复用。"""
    payload = {"chat_id": chat_id, "text": text[:4000],
               "disable_web_page_preview": True}
    if keyboard:
        payload["reply_markup"] = keyboard
    if message_id:
        r = await _api(db, token, "editMessageText", {**payload, "message_id": message_id})
        if r.get("ok"):
            return "pick"
    await _api(db, token, "sendMessage", payload)
    return "pick"


async def _handle_pick(db: Session, token: str, data: str, *, uid: str, chat_id: str,
                       message_id, tg_username: str = "") -> str:
    """处理 TMDB 候选点选：pick.<flow>.<idx|skip|cancel>。"""
    # v0.9.9 #209：进入候选点选即退出「文字会话态」，后续由 _pick_sessions 管理，
    # 避免用户点选后发下一条无关消息仍被当成求片/投稿输入。
    _flow_clear(uid)
    parts = data.split(".")
    flow = parts[1] if len(parts) > 1 else ""
    token_part = parts[2] if len(parts) > 2 else ""
    sess = _pick_get(uid)
    if token_part == "cancel":
        _pick_clear(uid)
        return await _edit_or_send(db, token, chat_id, message_id,
                                   "已取消，未提交任何内容。", main_keyboard())
    if not sess or sess.get("flow") != flow:
        return await _edit_or_send(db, token, chat_id, message_id,
                                   "候选已过期，请重新输入剧名发起求片/投稿。", main_keyboard())
    if token_part == "skip":
        _pick_clear(uid)
        hint = ("请手动发送：/求片 剧名 TMDB_ID 季" if flow == FLOW_WANT else
                "请手动发送：/投稿 标题 光鸭链接")
        return await _edit_or_send(db, token, chat_id, message_id,
                                   f"已跳过 TMDB 候选。{hint}", main_keyboard())
    try:
        idx = int(token_part)
    except ValueError:
        return await _edit_or_send(db, token, chat_id, message_id,
                                   "选择无效，请重试。", main_keyboard())
    candidates = sess.get("candidates") or []
    if idx < 0 or idx >= len(candidates):
        return await _edit_or_send(db, token, chat_id, message_id,
                                   "选择超出范围，请重试。", main_keyboard())
    chosen = candidates[idx]
    title = _fmt_title(chosen)
    tmdb_id = int(chosen["tmdb_id"]) if str(chosen.get("tmdb_id") or "").isdigit() else None
    media_type = str(chosen.get("media_type") or "") or "tv"
    year = str(chosen.get("year") or "")

    if flow == FLOW_WANT:
        kid = tgto.keyword_id(tmdb_id, None, title)
        req = MediaRequest(tg_user_id=str(uid), tg_username=tg_username,
                           title=title, tmdb_id=tmdb_id, media_type=media_type,
                           year=year, season=None, keyword_id=kid, status="pending",
                           review_status="pending")
        db.add(req)
        db.flush()
        try:
            r = await tgto.upsert_keyword(db, title=title, tmdb_id=tmdb_id,
                                          media_type=media_type, season=None)
            req.status = "synced" if r.get("ok") else "failed"
            req.note = r.get("message") or r.get("status") or ""
            if r.get("ok"):
                req.synced_at = utcnow()
        except Exception as exc:
            req.status, req.note = "failed", str(exc)[:300]
        _pick_clear(uid)
        return await _edit_or_send(db, token, chat_id, message_id,
                                   f"已登记「{title}」，我们会尽快处理，感谢支持！", main_keyboard())

    # 投稿：若会话里已存链接（先发链接再选片）则直接落库，否则提示补链接
    secret = str(sess.get("secret") or "").strip()
    if secret:
        db.add(UserContribution(tg_user_id=str(uid), tg_username=tg_username,
                                title=title[:512], content=sess.get("query", "")[:4000],
                                share_url=secret, status="pending",
                                tmdb_id=tmdb_id, media_type=media_type, year=year,
                                note="TMDB 候选选定；投稿转存接口待验证，当前仅登记"))
        _pick_clear(uid)
        return await _edit_or_send(db, token, chat_id, message_id,
                                   f"投稿已登记：{title}\n感谢支持，我们会尽快处理。", main_keyboard())
    # 记住已选候选，等下一条消息补链接
    sess["picked"] = {"title": title, "tmdb_id": tmdb_id,
                      "media_type": media_type, "year": year}
    _pick_sessions[str(uid)] = sess
    return await _edit_or_send(db, token, chat_id, message_id,
                               f"已选「{title}」，请发送该资源的光鸭分享链接（http/https）。",
                               back_keyboard())


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
        payload = {"chat_id": chat_id, "text": reason[:4000]}
        kb = restriction_keyboard(db)
        if kb:
            payload["reply_markup"] = kb
        await _api(db, token, "sendMessage", payload)
        return "blocked"
    # v0.9.9 #209：inline「结束交互」按钮——清掉文字会话与候选会话
    if data == CB_END:
        _flow_clear(user_id)
        _pick_clear(user_id)
        return await _edit_or_send(db, token, chat_id, message_id,
                                   "已结束本次交互，随时可点下方「求片 / 投稿」重新开始。",
                                   main_keyboard())
    # v0.9.8：TMDB 候选选择回调 pick.<flow>.<idx|skip|cancel>
    if data.startswith(f"{CB_PICK}."):
        return await _handle_pick(db, token, data, uid=user_id,
                                  chat_id=chat_id, message_id=message_id,
                                  tg_username=str(user.get("username") or ""))
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


async def search_candidates(db: Session, query: str, media_type: str = "") -> list[dict]:
    """TMDB 中文候选搜索：默认双路（剧+影）并按查询词相关性排序。

    TMDB 未配置 / 网络异常时返回空列表，调用方据此降级到旧的直接登记流程，
    保证「TMDB 挂了也能投稿求片」。
    """
    q = (query or "").strip()
    if not q:
        return []
    try:
        if not tmdb.configured(db):
            return []
        if media_type in ("movie", "tv"):
            rows = await tmdb.search(q, media_type, db, scenario=tmdb.BOT)
        else:
            # BOT 场景用 multi 单路：一次配额拿到剧+影，避免 search_both 双路
            # 各扣一次配额、且被 bot 间隔挡掉第二路导致只返回剧集。
            rows = await tmdb.search(q, None, db, scenario=tmdb.BOT)
    except Exception:
        return []
    if not rows:
        return []
    ranked = tmdb.rank(rows, {"clean_title": q})
    # 去重（同一 tmdb_id + type 只留一条），保中文名
    seen: set[tuple] = set()
    out = []
    for it in ranked:
        key = (str(it.get("tmdb_id") or ""), str(it.get("media_type") or ""))
        if key in seen:
            continue
        seen.add(key)
        out.append({"tmdb_id": it.get("tmdb_id"), "title": it.get("title") or "",
                    "year": str(it.get("year") or ""), "media_type": it.get("media_type") or "",
                    "vote_average": it.get("vote_average") or 0})
        if len(out) >= 8:
            break
    return out


def _fmt_title(c: dict) -> str:
    """候选落库展示名：TMDB 中文名 + 年份。"""
    title = str(c.get("title") or "").strip()
    year = str(c.get("year") or "").strip()
    return f"{title}（{year}）" if title and year else (title or year)


async def handle_text(db: Session, text: str, *, tg_user_id: str = "",
                      tg_username: str = "") -> tuple[str, str]:
    """处理一条用户文本，返回 (动作, 回复文本)。写库但不在此处 commit。

    v0.9.9 #209：点「求片 / 投稿」Reply 键盘后进入会话态，后续直接输入文字即可，
    不必每条都加 /命令；发「结束」或点 inline「结束交互」按钮退出。
    """
    raw = (text or "").strip()
    if not raw:
        return "ignored", ""

    # v0.9.9 #209：结束词优先——任何会话（文字模式 / TMDB 候选）都清掉
    if low_end(raw):
        _flow_clear(tg_user_id)
        _pick_clear(tg_user_id)
        return "flow_end", "已结束本次交互，随时可点下方「求片 / 投稿」重新开始。"

    # v0.9.8：投稿候选已选、正在等光鸭链接 —— 本次消息作为链接落库
    pending = _pick_get(tg_user_id)
    if pending and pending.get("flow") == FLOW_CONTRIB and pending.get("picked"):
        urls = _SHARE_URL.findall(raw)
        if urls:
            picked = pending["picked"]
            db.add(UserContribution(tg_user_id=str(tg_user_id), tg_username=tg_username,
                                    title=str(picked.get("title") or "")[:512],
                                    content=raw[:4000], share_url=urls[-1], status="pending",
                                    tmdb_id=picked.get("tmdb_id"),
                                    media_type=picked.get("media_type") or "",
                                    year=str(picked.get("year") or ""),
                                    note="TMDB 候选选定；投稿转存接口待验证，当前仅登记"))
            _pick_clear(tg_user_id)
            _flow_clear(tg_user_id)
            # v0.9.9 #208：用户回复只给友好文案，管理员侧状态只在后台 note 可见
            return "contribute", (f"投稿已登记：{picked.get('title')}\n"
                                  "感谢支持，我们会尽快处理。")
        return "contribute_await_url", "请发送该资源的光鸭分享链接（http/https 开头），或回复「结束」退出。"

    cmd, *rest = raw.split(None, 1)
    arg = (rest[0] if rest else "").strip()
    low = cmd.lower().split("@")[0]

    if low in ("/help", "/start", "/帮助"):
        return "help", HELP

    builtin = _match_builtin(low)
    custom = None if builtin else _custom_command(db, low)

    # v0.9.9 #209：会话态下，用户直接输入的文字（不含命令）当作当前 flow 的输入。
    # 这样点「求片」后发「凡人修仙传」= 求片剧名，点「投稿」后发剧名 = 投稿候选。
    active_flow = _flow_get(tg_user_id)
    if active_flow and not builtin and not custom and not low.startswith("/"):
        builtin = "求片" if active_flow == FLOW_WANT else "投稿"
        arg = raw
        low = builtin

    if not builtin and not custom and not _SHARE_URL.search(raw):
        return "ignored", ""

    # v0.9.9 #209：裸命令（点 Reply 键盘触发，无参数）→ 进入会话态，提示直接输入文字。
    # 不再回「用法：/求片 …」逼用户加命令，也不再让投稿落「未命名投稿」。
    if builtin in ("求片", "投稿") and not arg:
        if builtin == "求片":
            _flow_set(tg_user_id, FLOW_WANT)
            return "flow_want", ("已进入求片模式。请直接输入剧名，我会列出 TMDB 候选让你点选。\n"
                                 "随时回复「结束」或点下方按钮退出。")
        _flow_set(tg_user_id, FLOW_CONTRIB)
        return "flow_contribute", ("已进入投稿模式。请直接输入剧名（我会列 TMDB 候选），"
                                   "或直接粘贴光鸭分享链接。\n随时回复「结束」或点下方按钮退出。")

    # 用户带了内容 → 该轮交互完成，退出文字会话态（后续走正常候选/落库流程）
    if builtin in ("求片", "投稿") and active_flow:
        _flow_clear(tg_user_id)

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
            return "want_invalid", "用法：/求片 剧名 [TMDB ID] [季]\n例：/求片 伟大的长征 297253 1\n也可直接输入剧名，我会列出 TMDB 候选让你选。"
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
        # v0.9.8：只给了剧名、没带 TMDB ID → 走候选选择；用户已给 ID 时直接登记（保持兼容）。
        if tmdb_id is None:
            candidates = await search_candidates(db, title)
            if candidates:
                _pick_set(tg_user_id, FLOW_WANT, title, candidates)
                return "want_pick", _candidate_caption(FLOW_WANT, title, candidates)
        kid = tgto.keyword_id(tmdb_id, season, title)
        req = MediaRequest(tg_user_id=str(tg_user_id), tg_username=tg_username,
                           title=title, tmdb_id=tmdb_id, media_type="tv" if season else "",
                           season=season, keyword_id=kid, status="pending")
        db.add(req)
        db.flush()
        # v0.9.8：TgtoDrive 同步是管理员侧的事，失败原因只写 note 供后台排查，
        # 绝不能发给用户（用户反馈第 4 项）。用户只看到「已登记，等待审核」。
        req.review_status = "pending"
        try:
            r = await tgto.upsert_keyword(db, title=title, tmdb_id=tmdb_id,
                                          media_type=req.media_type or "tv", season=season)
            req.status = "synced" if r.get("ok") else "failed"
            req.note = r.get("message") or r.get("status") or ""
            if r.get("ok"):
                req.synced_at = utcnow()
        except Exception as exc:
            req.status, req.note = "failed", str(exc)[:300]
        reply = f"已登记「{title}」，我们会尽快处理，感谢支持！"
        return "want", reply

    if builtin == "投稿":
        urls = _SHARE_URL.findall(arg)
        share_url = urls[-1] if urls else ""
        title = _SHARE_URL.sub("", arg).strip()
        # v0.9.9 #208：有剧名先走 TMDB 候选，选完补链接，后台拿到中文名/年份/影号。
        # TMDB 未配置/无候选时仍可用用户剧名登记，但绝不再造「未命名投稿」。
        if not share_url and title:
            candidates = await search_candidates(db, title)
            if candidates:
                _pick_set(tg_user_id, FLOW_CONTRIB, title, candidates)
                return "contribute_pick", _candidate_caption(FLOW_CONTRIB, title, candidates)
        if not title and not share_url:
            return "contribute_need_title", "请告诉我影视名称，或直接粘贴光鸭分享链接。"
        final_title = (title or share_url)[:512]
        db.add(UserContribution(tg_user_id=str(tg_user_id), tg_username=tg_username,
                                title=final_title, content=raw[:4000],
                                share_url=share_url, status="pending",
                                note="投稿转存接口待验证，当前仅登记，不自动入库"))
        # v0.9.9 #208：用户侧只给友好确认，「转存接口待验证」等管理员信息只在后台 note
        return "contribute", (f"投稿已登记：{final_title}\n"
                              "感谢支持，我们会尽快处理。")

    if custom:
        return "custom", str(custom.get("reply") or custom.get("description") or "已收到指令。")

    # 普通消息里带链接也按投稿登记，降低用户使用门槛（如会话态里直接发纯链接）
    if _SHARE_URL.search(raw):
        return await _register_link_contribution(db, raw, tg_user_id, tg_username)
    # 会话态下走到这里说明是一段文字但没匹配命令——按投稿剧名处理，先走 TMDB 候选
    if active_flow == FLOW_CONTRIB and raw:
        candidates = await search_candidates(db, raw)
        if candidates:
            _pick_set(tg_user_id, FLOW_CONTRIB, raw, candidates)
            return "contribute_pick", _candidate_caption(FLOW_CONTRIB, raw, candidates)
        db.add(UserContribution(tg_user_id=str(tg_user_id), tg_username=tg_username,
                                title=raw[:512], content=raw[:4000], share_url="",
                                status="pending", note="文字投稿（TMDB 无候选）；转存接口待验证"))
        _flow_clear(tg_user_id)
        return "contribute", f"投稿已登记：{raw[:512]}\n感谢支持，我们会尽快处理。"
    return "ignored", ""


async def _register_link_contribution(db: Session, raw: str, tg_user_id: str,
                                      tg_username: str) -> tuple[str, str]:
    """纯光鸭链接投稿：链接即内容，标题从候选/文本拿，用户回复不含管理员信息。"""
    share_url = _SHARE_URL.findall(raw)[-1]
    text_title = _SHARE_URL.sub("", raw).strip()[:512]
    title = text_title
    if not title:
        candidates = await search_candidates(db, share_url)
        if candidates:
            # 有候选也走点选，拿到规范中文名；链接先存进会话 secret 供选完落库
            _pick_set(tg_user_id, FLOW_CONTRIB, "分享链接", candidates, secret=share_url)
            return "contribute_pick", _candidate_caption(FLOW_CONTRIB, "分享链接", candidates)
        title = share_url  # 无候选兜底用链接本身，仍不造「未命名投稿」
    db.add(UserContribution(tg_user_id=str(tg_user_id), tg_username=tg_username,
                            title=title, content=raw[:4000], share_url=share_url,
                            status="pending", note="自动识别为投稿；转存接口待验证"))
    _flow_clear(tg_user_id)
    return "contribute", f"已收到资源并登记投稿（{title}），感谢支持，我们会尽快处理。"


async def poll_once(db: Session) -> dict:
    """拉取一次 Bot 更新。返回统计；关闭或未配置时直接跳过，不产生网络请求。

    v0.9.2：新增 callback_query（inline 菜单按钮点击路由）。
    """
    if (runtime_config.get(db, "contribution_enabled") or "").lower() != "true":
        return {"enabled": False, "messages": 0}
    token = effective_token(db)
    if not token:
        return {"enabled": False, "messages": 0, "error": "未配置交互 Bot（请在 Bot 管理添加并选用，或配置投稿 Bot Token）"}
    # v0.9.9 #207：每个 token 首次轮询自动清空一次旧项目残留的 "/" 指令栏。
    # 指纹去重在 auto_clear_stale_commands 内部，正常轮询不产生额外请求。
    menu_cleared = await auto_clear_stale_commands(db, token)
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
             "blocked": 0, "callbacks": 0, "actions": [],
             "menu_cleared": menu_cleared}
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
                    await _reply(db, token, chat_id, reason,
                                 reply_markup=restriction_keyboard(db))
                continue
            action, reply = await handle_text(
                db, text, tg_user_id=user_id,
                tg_username=str(user.get("username") or ""))
            if action != "ignored":
                stats["handled"] += 1
                stats["actions"].append(action)
            if reply and chat_id:
                # /start、/help 附带 Telegram 私聊 Reply 键盘（输入框上方菜单）。
                # 不调用 setMyCommands，不改 Telegram 底部指令栏；只回来源 chat。
                if action == "help" and text.strip().lower().split("@")[0] in ("/start", "/help"):
                    await _reply(db, token, chat_id, reply, reply_markup=main_reply_keyboard())
                elif action in ("want_pick", "contribute_pick"):
                    # v0.9.8：TMDB 候选列表附 inline 选择键盘
                    sess = _pick_get(user_id)
                    kb = _candidates_keyboard(sess["flow"], sess["candidates"]) if sess else None
                    await _reply(db, token, chat_id, reply, reply_markup=kb)
                elif action == "flow_end":
                    # v0.9.9 #209：退出文字会话，回主 Reply 键盘
                    await _reply(db, token, chat_id, reply, reply_markup=main_reply_keyboard())
                elif action in ("flow_want", "flow_contribute", "contribute_await_url"):
                    # v0.9.9 #209：刚进入会话 / 等光鸭链接，附 inline「结束交互」按钮
                    await _reply(db, token, chat_id, reply, reply_markup=end_flow_keyboard())
                elif _flow_get(user_id) or _pick_get(user_id):
                    # v0.9.9 #209：会话态下其它回复也带结束按钮
                    await _reply(db, token, chat_id, reply, reply_markup=end_flow_keyboard())
                else:
                    await _reply(db, token, chat_id, reply)
        except Exception as exc:
            stats.setdefault("errors", []).append(str(exc)[:120])
    runtime_config.set_many(db, {"contribution_update_offset": str(offset)})
    db.commit()
    return stats
