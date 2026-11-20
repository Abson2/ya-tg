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
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from . import runtime_config, tgto, tmdb
from .models import (Bot, Channel, DiscoveredChat, MediaRequest, ProcessedUpdate,
                     QueueItem, Resource, UserContribution, utcnow)
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
        "/搜索 关键词             — 转发已推送、仍在库的资源卡片给你\n"
        "/打赏                    — 直接收赞赏码图片\n"
        "/help                    — 查看本说明\n\n"
        "也可直接点输入框上方的菜单按钮：求片 / 投稿 / 搜索 / 打赏，\n"
        "点后再直接输入文字即可（不必加 /），回复「结束」退出交互。\n"
        "其它指令以管理员在后台配置的为准。")

# v0.9.16 #277：未进入指令/会话态时，对用户随意发送内容的统一纠正提示。
# 投稿/求片/搜索只走指令或 Reply 菜单进入的会话态，裸链接/裸文字不再自动登记。
_UNRECOGNIZED_HINT = ("当前内容未识别为指令。请点下方菜单，或发送 "
                      "/求片 /投稿 /搜索 /打赏 /help；\n"
                      "如正在交互中，请按提示输入，或回复「结束」退出。")

_SHARE_URL = re.compile(r"https?://[^\s]+", re.I)


# v0.9.17 #281：投稿按作品去重合并——同 dedup_key 只保留一行、累加 submit_count，
# 后台审批只显示一条。键优先 tmdb:ID:类型，无 TMDB 回退规范化标题（去空白/标点/大小写）。
def _norm_contrib_title(title: str) -> str:
    s = str(title or "").strip().lower()
    s = re.sub(r"[\s\W_]+", "", s, flags=re.UNICODE)  # 仅保留字母/数字/中日韩文字
    return s[:120]


def contribution_dedup_key(*, tmdb_id: int | None, media_type: str,
                           title: str, share_url: str) -> str:
    if tmdb_id:
        return f"tmdb:{int(tmdb_id)}:{str(media_type or '').strip().lower()}"
    norm = _norm_contrib_title(title) or _norm_contrib_title(share_url)
    return f"title:{norm}" if norm else ""


def upsert_contribution(db: Session, *, tg_user_id: str, tg_username: str,
                        title: str, content: str, share_url: str,
                        tmdb_id: int | None = None, media_type: str = "",
                        year: str = "", note: str = "") -> tuple[UserContribution, bool]:
    """投稿入库：同作品去重累加计数（返回 (行, 是否新建)）。

    v0.9.17 #281：重复投稿不新增行——submit_count+1，覆盖"最新链接/最新投稿人"，
    原 status 保持不变（已通过/已拒绝的审批结论不被重复投稿冲掉）；dedup_key 为空
    （既无标题又无链接的异常输入）时不合并，按新行登记避免误并。
    """
    key = contribution_dedup_key(tmdb_id=tmdb_id, media_type=media_type,
                                 title=title, share_url=share_url)
    now = utcnow()
    existing = None
    if key:
        existing = db.scalar(select(UserContribution).where(
            UserContribution.dedup_key == key).order_by(UserContribution.id).limit(1))
    if existing is None:
        row = UserContribution(
            tg_user_id=str(tg_user_id), tg_username=str(tg_username or ""),
            title=str(title)[:512], content=str(content)[:4000], share_url=share_url,
            status="pending", note=note, tmdb_id=tmdb_id, media_type=media_type or "",
            year=str(year or ""), dedup_key=key, submit_count=1,
            last_share_url=share_url, last_tg_user_id=str(tg_user_id),
            last_submitted_at=now)
        db.add(row)
        db.flush()  # autoflush=False：写完即查/取主键必须显式 flush
        return row, True
    # 命中已有：累加计数、刷新最新投稿信息；保留原 status（审批结论不被冲掉）
    existing.submit_count = int(existing.submit_count or 1) + 1
    existing.last_share_url = share_url
    existing.last_tg_user_id = str(tg_user_id)
    existing.last_submitted_at = now
    existing.tg_username = str(tg_username or "")
    # pending 行同步最新链接/标题，让管理员审批到的是当前资源；已结案行不动原字段
    if existing.status == "pending":
        if share_url:
            existing.share_url = share_url
        if tmdb_id:
            existing.tmdb_id = tmdb_id
            existing.media_type = media_type or existing.media_type
            existing.year = str(year or "") or existing.year
    db.flush()
    return existing, False


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
        # v0.9.14 #257：一条路由可绑定多个目标 {kind, chat_id, name}，合并去重后回填
        # 到 channel_id/group_id（取每个 kind 的第一个），保留老前端双字段兼容。
        targets = []
        seen_t = set()
        for t in (item.get("targets") if isinstance(item.get("targets"), list) else []):
            if not isinstance(t, dict):
                continue
            t_chat = str(t.get("chat_id") or "").strip()
            t_kind = "group" if str(t.get("kind") or "").lower() == "group" else "channel"
            if not t_chat or (t_kind, t_chat) in seen_t:
                continue
            seen_t.add((t_kind, t_chat))
            targets.append({"kind": t_kind, "chat_id": t_chat,
                            "name": str(t.get("name") or "").strip()[:128]})
        if targets:
            if not channel_id:
                channel_id = next((t["chat_id"] for t in targets if t["kind"] == "channel"), "")
            if not group_id:
                group_id = next((t["chat_id"] for t in targets if t["kind"] == "group"), "")
        if not channel_id and not group_id:
            continue
        out.append({"dir_id": str(item.get("dir_id") or "").strip(),
                    "channel_id": channel_id, "group_id": group_id,
                    "targets": targets,
                    "note": str(item.get("note") or "").strip()[:120]})
    return out


def push_routes(db: Session) -> list[dict]:
    """把原始路由展开成「一个目标一项」的列表，供匹配 / 推送 / 群发消费。

    v0.9.8：一条路由可同时含频道与群两个目标，展开后返回两条 {dir_id, kind, chat_id, note}；
    下游 match_push_routes / route_target_channel / 群发逻辑保持不变。
    """
    out = []
    for r in push_routes_raw(db):
        # v0.9.14 #257：优先用 targets 列表（一个目录绑多个频道/群），老数据回退双字段。
        targets = r.get("targets") or []
        if targets:
            for t in targets:
                out.append({"dir_id": r["dir_id"], "kind": t["kind"], "chat_id": t["chat_id"],
                            "note": r["note"], "target_name": t.get("name", "")})
        else:
            for kind, chat in (("channel", r["channel_id"]), ("group", r["group_id"])):
                if chat:
                    out.append({"dir_id": r["dir_id"], "kind": kind, "chat_id": chat,
                                "note": r["note"]})
    return out


def match_push_routes(db: Session, resource, root_ids=None) -> list[dict]:
    """返回与资源目录匹配的路由（可多条）。

    匹配规则：dir_id 为空 = 根目录通配（匹配所有资源，即整库推送）；
    否则精确匹配 resource.parent_id，或出现在 resource.dir_chain_json 目录链中。

    v0.9.11：新增 root_ids——真机根因「路由 dir_id 填了 B 库目标根目录 ID 却
    一个都匹配不上」。扫描入库时 dir_chain 从直接父目录往上，但**扫描根本身
    不在链里**（scanner 把 root 的链初始化成 []），所以填 B 库根目录 ID 的路由
    永远匹配不到任何季子目录资源。这里把「dir_id == 任一扫描根 ID」也视为通配，
    等价用户直觉「我填的是 B 库根，就是要整库都推」。root_ids 由调用方从
    MirrorRule.target_root_id / ScanTask.root_id 传入，避免本模块耦合镜像配置。
    """
    routes = push_routes(db)
    if not routes:
        return []
    parent = str(getattr(resource, "parent_id", "") or "").strip()
    try:
        chain = set(str(x) for x in json.loads(getattr(resource, "dir_chain_json", "[]") or "[]"))
    except Exception:
        chain = set()
    roots = {str(x).strip() for x in (root_ids or []) if str(x).strip()}
    matched = []
    for r in routes:
        dir_id = r["dir_id"]
        if (not dir_id or dir_id == parent or dir_id in chain
                or (roots and dir_id in roots)):
            matched.append(r)
    return matched


def scan_root_ids(db: Session) -> list[str]:
    """收集「视为整库根」的目录 ID：镜像 B 库根 + 扫描计划根。

    供 match_push_routes 的 root_ids 参数使用。只读，不改变任何配置语义。
    """
    ids: list[str] = []
    try:
        from .models import MirrorRule, ScanTask
        for r in db.scalars(select(MirrorRule)).all():
            if getattr(r, "target_root_id", ""):
                ids.append(str(r.target_root_id).strip())
        for t in db.scalars(select(ScanTask)).all():
            if getattr(t, "root_id", ""):
                ids.append(str(t.root_id).strip())
    except Exception:
        pass
    return ids


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


def effective_bot_id(db: Session) -> int:
    """v0.9.14：当前交互 Bot 对应的 bots.id（发现目标归属），取不到返回 0。

    与 effective_token 同源：优先 interaction_bot_id，否则 contribution_bot_token 场景
    没有 Bot 记录时返回 0（发现的群仍会记录 bot_id=0，供无 Bot 管理页场景兜底展示）。
    """
    raw = (runtime_config.get(db, "interaction_bot_id") or "").strip()
    if raw.isdigit():
        return int(raw)
    return 0


def bot_user_id_from_token(token: str) -> str:
    """Telegram Bot token 形如 `123456789:AAxx...`，冒号前即 Bot 的数字 user_id。

    用于 getChatMember 查询 Bot 自己在群里的身份/权限。解析失败返回空串。
    """
    t = str(token or "").strip()
    head = t.split(":", 1)[0]
    return head if head.isdigit() else ""


# v0.9.15 #276：startgroup 官方授权链接（让用户把 Bot 加进群；Bot API 无主动加群接口）
async def get_bot_username(db: Session, token: str) -> tuple[str, str]:
    """调 Telegram getMe 取当前 Bot 的 @username（startgroup 链接必需）。

    返回 (username 不含 @, error)。绝不输出 token；getMe 失败返回空 username + 分类原因。
    官方约束：Bot 必须先在 BotFather 设置 username 才能生成 t.me/<bot> 深链。
    """
    t = str(token or "").strip()
    if not t:
        return "", "未配置交互 Bot Token"
    try:
        data = await _api(db, t, "getMe", {})
    except Exception as exc:
        return "", f"getMe 请求失败：{str(exc)[:120]}"
    if not isinstance(data, dict) or not data.get("ok"):
        kind, hint = _tg_error_hint(data if isinstance(data, dict) else {}, 200)
        return "", hint or "getMe 调用失败，无法读取 Bot 用户名"
    me = data.get("result") or {}
    username = str(me.get("username") or "").strip().lstrip("@")
    if not username:
        return "", "该 Bot 未在 BotFather 设置 username，无法生成授权链接；请先设置后重试。"
    return username, ""


def build_startgroup_url(bot_username: str, payload: str) -> str:
    """拼 Telegram 官方 startgroup 深链。payload 为短期随机标识，绝不含 token。"""
    name = str(bot_username or "").strip().lstrip("@")
    if not name or not re.match(r"^[A-Za-z0-9_]{5,32}$", name):
        return ""
    p = re.sub(r"[^A-Za-z0-9_\-]", "", str(payload or ""))[:64]
    return f"https://t.me/{name}?startgroup={p}" if p else f"https://t.me/{name}?startgroup"


def new_startgroup_payload() -> str:
    """生成短期随机 payload：yg_<unix秒36进制>_<8位随机>，不含 token 也不含敏感信息。"""
    import secrets
    import string
    alphabet = string.ascii_letters + string.digits
    rand = "".join(secrets.choice(alphabet) for _ in range(8))
    return f"yg_{int(time.time()):x}_{rand}"


def record_discovered_chat(db: Session, chat: dict, *, source: str = "auto",
                           flush: bool = True, member_status: str = "",
                           bot_username: str = "", bot_id_override: int | None = None,
                           last_update_id: int = 0,
                           count_message: bool = True) -> DiscoveredChat | None:
    """v0.9.14 #256：把一条 Telegram chat 对象 upsert 进发现池。

    只记录 group / supergroup / channel（私聊无需作为推送目标发现）。
    同一 bot+chat_id 幂等，累加消息数与最后出现时间。

    v0.9.15 #276：
    - member_status 非空时写入 ChatMember.status（left/kicked 只标记状态、不删记录）；
    - bot_id_override 用于 my_chat_member 事件按消息携带的 bot 归属（缺省用 effective_bot_id）；
    - bot_username 记录 /id@BotUsername 等场景的 Bot 用户名；last_update_id 记幂等审计。

    flush：默认 True（API / 测试「写完即查」需要）。poll_once 里传 False——
    轮询链路后面还有多次 await，flush 会持写事务跨 await，触发 database is locked；
    那里由 poll_once 末尾统一 commit。
    """
    if not isinstance(chat, dict):
        return None
    chat_id = str(chat.get("id") or "").strip()
    ctype = str(chat.get("type") or "").strip()
    if not chat_id or ctype not in ("group", "supergroup", "channel"):
        return None
    bot_id = bot_id_override if bot_id_override is not None else effective_bot_id(db)
    row = db.scalar(select(DiscoveredChat).where(
        DiscoveredChat.bot_id == bot_id, DiscoveredChat.chat_id == chat_id))
    title = str(chat.get("title") or "").strip()
    username = str(chat.get("username") or "").strip()
    now = utcnow()
    if row is None:
        row = DiscoveredChat(bot_id=bot_id, chat_id=chat_id, chat_type=ctype,
                             title=title[:256], username=username[:128], source=source,
                             first_seen_at=now, last_seen_at=now,
                             member_status=(member_status or "unknown")[:24],
                             bot_username=(bot_username or "")[:128],
                             last_update_id=int(last_update_id or 0),
                             message_count=1 if count_message else 0,
                             updated_at=now)
        db.add(row)
    else:
        row.last_seen_at = now
        if count_message:
            row.message_count = int(row.message_count or 0) + 1
        row.updated_at = now
        if title:
            row.title = title[:256]
        if username:
            row.username = username[:128]
        if member_status:
            row.member_status = member_status[:24]
            # Bot 被移出：发送权限随之失效，避免白名单仍显示可发送
            if member_status in ("left", "kicked"):
                row.perm_status = "not_member"
                row.perm_can_send = False
            elif row.perm_status == "not_member":
                row.perm_status = "unknown"
        if bot_username:
            row.bot_username = bot_username[:128]
        if last_update_id:
            row.last_update_id = int(last_update_id)
        # 手动添加后又被自动发现：保留 manual 来源标记，避免被自动流水覆盖语义
        if row.source != "manual":
            row.source = source
    if flush:
        db.flush()
    return row


async def check_chat_permission(db: Session, token: str, chat_id: str,
                                chat_type: str = "group") -> dict:
    """v0.9.14 #256：用 getChatMember 检查 Bot 在该群/频道是否有发送消息权限。

    返回 {ok, status, can_send, detail}：
      - status: ok / no_permission / not_member / unknown
      - can_send: 能否发消息（频道需 can_post_messages；群需 can_send_messages 或管理员）
    不抛异常，失败以 unknown 返回，便于前端展示「未检测到」而非误判为有权限。
    """
    result = {"ok": False, "status": "unknown", "can_send": False,
              "detail": "", "member_status": ""}
    bot_uid = bot_user_id_from_token(token)
    if not token or not chat_id:
        result["detail"] = "缺少 Bot Token 或 chat_id"
        return result
    payload = {"chat_id": str(chat_id).strip()}
    if bot_uid:
        payload["user_id"] = int(bot_uid)
    else:
        # token 无法解析 Bot ID 时退化为 getMe 取 id（多一次请求，仅在异常 token 场景）
        me = await _api(db, token, "getMe", {})
        me_res = me.get("result") or {}
        if not me.get("ok") or not me_res.get("id"):
            result["detail"] = "无法获取 Bot 自身 ID（getMe 失败）"
            return result
        payload["user_id"] = int(me_res["id"])
    try:
        data = await _api(db, token, "getChatMember", payload)
    except Exception as exc:
        result["detail"] = f"权限检查请求失败：{str(exc)[:120]}"
        return result
    if not data.get("ok"):
        desc = str(data.get("description") or "")[:160]
        low = desc.lower()
        if "not found" in low or "member" in low or "kicked" in low:
            result["status"] = "not_member"
            result["member_status"] = "left"
            result["detail"] = "Bot 不在该群/频道，或已被移出；请先把 Bot 拉进去"
        else:
            result["detail"] = desc or "getChatMember 调用失败"
        return result
    member = data.get("result") or {}
    status = str(member.get("status") or "")
    result["member_status"] = status
    if status in ("left", "kicked"):
        result.update(status="not_member", detail=f"Bot 已离开或被移出（{status}）")
        return result
    # 频道：只有管理员/创建者且 can_post_messages 才能发言；普通成员（订阅者）不能发
    if chat_type == "channel":
        can = status in ("creator", "administrator") and bool(member.get("can_post_messages"))
        result.update(ok=True, status="ok" if can else "no_permission", can_send=can,
                      detail=("Bot 可在该频道发消息" if can else
                              "Bot 需在频道中为管理员且开启「发布消息」权限"))
        return result
    # 群 / 超级群：creator 必可发；administrator 看 can_send_messages（老 API 无该字段时默认可发）；
    # member（普通成员）默认就能在群里发言——Telegram ChatMemberMember 不带 can_send_messages 字段，
    # 不能因为没有该字段就判为不可发；restricted 才严格看 can_send_messages；其余视为无权限。
    if status == "creator":
        can = True
    elif status == "administrator":
        can = bool(member.get("can_send_messages", True))
    elif status == "member":
        can = bool(member.get("can_send_messages", True))
    elif status == "restricted":
        can = bool(member.get("can_send_messages"))
    else:
        can = False
    result.update(ok=True, status="ok" if can else "no_permission", can_send=can,
                  detail=("Bot 可在该群发消息" if can else
                          f"Bot 在该群无发送权限（身份：{status or '未知'}）"))
    return result


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
                 reply_markup: dict | None = None,
                 parse_mode: str | None = None) -> None:
    """只回复指令来源的 chat（频道 / 群聊 / 私聊由 poll_once 决定），不主动私推用户。"""
    payload = {"chat_id": str(chat_id), "text": text[:4000],
               "disable_web_page_preview": True}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        await _api(db, token, "sendMessage", payload)
    except Exception:
        pass  # 回复失败不影响主链路与 offset 推进


async def _send_photo(db: Session, token: str, chat_id: str, photo: str,
                      caption: str = "", parse_mode: str = "") -> None:
    """v0.9.12：给用户发一张图片（打赏码 / 资源海报），失败静默不影响 offset。

    v0.9.13 #249：搜索卡片复用 publisher._build_message() 渲染出的是 HTML（<b>/<a>），
    必须带 parse_mode=HTML 才会被 Telegram 解析；否则标签会以纯文本外泄。打赏 caption
    是纯文本，保持默认不带 parse_mode，避免文案里的 < > 触发 Telegram 解析错误。
    无海报降级 sendMessage 时同样要带 parse_mode，否则正文标签仍会外泄。
    """
    if not photo:
        await _reply(db, token, chat_id, caption, parse_mode=parse_mode or None)
        return
    payload = {"chat_id": str(chat_id), "photo": photo}
    if caption:
        payload["caption"] = caption[:1000]
    if parse_mode:
        payload["parse_mode"] = parse_mode
    try:
        await _api(db, token, "sendPhoto", payload)
    except Exception:
        # 图片发送失败降级为纯文字，至少把说明送到（保留 parse_mode）
        await _reply(db, token, chat_id, caption or "图片发送失败，请稍后重试。",
                     parse_mode=parse_mode or None)


async def _send_search_cards(db: Session, token: str, chat_id: str,
                             resource_ids: list[int]) -> None:
    """v0.9.12：把已推送过的资源卡片逐个发给发起搜索的用户（私聊）。

    卡片正文复用 publisher 的统一渲染（海报 + 简介 + 分享），保证与频道推送的
    消息一致；这是「转发已推送卡片」的实现——不重建分享、不改队列、不私聊刷屏
    （只在用户主动搜索时回他自己的 chat）。
    """
    from . import publisher  # 延迟 import，避免 publisher → bot_listener 循环依赖
    for rid in resource_ids:
        res = db.get(Resource, rid)
        if not res or not (res.share_url or "").strip():
            continue
        caption, _ = publisher._build_message(db, res, res.share_url)
        poster = tmdb.poster_url(res.poster_path) if res.poster_path else ""
        # v0.9.13 #249：卡片正文是 HTML，必须带 parse_mode=HTML，否则 <b>/<a> 外泄成代码
        await _send_photo(db, token, chat_id, poster, caption, parse_mode="HTML")


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
FLOW_SEARCH = "search"   # v0.9.12：搜索也走会话态，与求片/投稿一致

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


async def forward_contribution_to_tgto(db: Session, share_url: str, title: str = "") -> tuple[bool, str]:
    """v0.9.13 #254：投稿审核通过后，把分享链接转发到配置的 TgtoDrive 绑定 Bot chat。

    由该 Bot 监控链接自动转存。目标 chat_id 由管理页配置（tgto_contribute_bot_chat_id），
    发送用交互 Bot 的 token；未配置或失败只返回状态供后台 note，绝不影响审核主流程。
    返回 (是否发送, 说明)。
    """
    chat_id = (runtime_config.get(db, "tgto_contribute_bot_chat_id") or "").strip()
    if not chat_id:
        return False, "未配置 TgtoDrive 投稿监控 chat_id"
    share_url = (share_url or "").strip()
    if not share_url:
        return False, "投稿无分享链接，跳过转发"
    token = effective_token(db)
    if not token:
        return False, "未配置交互 Bot Token，无法转发"
    text = f"📥 投稿转存\n{title}\n{share_url}" if title else f"📥 投稿转存\n{share_url}"
    try:
        data = await _api(db, token, "sendMessage",
                          {"chat_id": chat_id, "text": text[:4000],
                           "disable_web_page_preview": False})
    except Exception as exc:
        return False, f"转发请求失败：{str(exc)[:160]}"
    # v0.9.17 热修：_api 返回 {"ok":false,...} 不抛异常，必须判 ok，
    # 否则 Telegram 拒绝（chat not found / bot 被禁）会被误报为"已转发"。
    if isinstance(data, dict) and not data.get("ok"):
        desc = str(data.get("description") or "")[:160]
        return False, f"转发被 Telegram 拒绝：{desc or '未知原因'}（请确认 Bot 已在该 chat 且有发送权限）"
    return True, "已转发至 TgtoDrive 监控 Bot"


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
    # v0.9.12：打赏图片由 poll_once / callback 用 sendPhoto 直接发，这里只回说明文字，
    # 不再把图片网址当纯文本塞给用户。
    return runtime_config.get(db, "donation_text") or "感谢支持！"


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
                "v0.9.16 起裸链接不再自动登记，请先点「投稿」进入流程再按提示操作。", back_keyboard())
    if data == CB_SEARCH:
        return ("🔍 搜索：直接输入剧名 / 关键词，我会把你之前推送过、仍在库的"
                "资源卡片转发给你。\n也可发 /搜索 关键词。", back_keyboard())
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
        # v0.9.13 #254：白名单写入后移到「管理员审核通过」时执行（见 main.handle_media_request），
        # 提交阶段只登记，避免未审核的求片占用频道监控。
        req = MediaRequest(tg_user_id=str(uid), tg_username=tg_username,
                           title=title, tmdb_id=tmdb_id, media_type=media_type,
                           year=year, season=None, keyword_id=kid, status="pending",
                           review_status="pending", note="待管理员审核通过后同步白名单")
        db.add(req)
        db.flush()  # autoflush=False：写完即查必须显式 flush
        _pick_clear(uid)
        return await _edit_or_send(db, token, chat_id, message_id,
                                   f"已登记「{title}」，我们会尽快处理，感谢支持！", main_keyboard())

    # 投稿：若会话里已存链接（先发链接再选片）则直接落库，否则提示补链接
    secret = str(sess.get("secret") or "").strip()
    if secret:
        row, created = upsert_contribution(
            db, tg_user_id=str(uid), tg_username=tg_username,
            title=title[:512], content=sess.get("query", "")[:4000], share_url=secret,
            tmdb_id=tmdb_id, media_type=media_type, year=year,
            note="TMDB 候选选定；投稿转存接口待验证，当前仅登记")
        _pick_clear(uid)
        thanks = (f"投稿已登记：{title}\n感谢支持，我们会尽快处理。"
                  if created else
                  f"「{title}」已有其它用户投稿，已合并登记（累计 {row.submit_count} 次），感谢支持！")
        return await _edit_or_send(db, token, chat_id, message_id, thanks, main_keyboard())
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
    # v0.9.12：点「搜索资源」inline 按钮也进入会话态，后续直接输关键词即可
    if data == CB_SEARCH:
        _flow_set(user_id, FLOW_SEARCH)
    # v0.9.12：点「打赏支持」inline 按钮——编辑菜单为说明，再直接发打赏码图片
    if data == CB_DONATE and (runtime_config.get(db, "donation_enabled") or "").lower() == "true":
        img = runtime_config.get(db, "donation_image_url") or ""
        if img:
            await _send_photo(db, token, chat_id, img,
                              runtime_config.get(db, "donation_text") or "感谢支持！")
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


async def _do_search(db: Session, keyword: str) -> tuple[str, list[int]]:
    """搜索已推送且仍在库的资源，返回 (提示文本, 命中资源 id 列表)。

    v0.9.12：搜索不再只列文件名，而是返回「之前推送过、仍在库」的资源，
    由 poll_once 把这些资源卡片转发给发起搜索的用户。
    """
    kw = (keyword or "").strip()
    if not kw:
        return "请直接输入剧名 / 关键词搜索。", []
    # 只取已成功推送（QueueItem.sent）且仍有分享链接的资源，按作品去重、
    # 优先最近推送；用「作品键」收敛同一部剧的多个单集为一部。
    rows = list(db.scalars(select(Resource).join(
        QueueItem, QueueItem.resource_id == Resource.id).where(
        QueueItem.status == "sent",
        Resource.share_url != "",
        or_(Resource.media_title.like(f"%{kw}%"),
            Resource.name.like(f"%{kw}%"),
            Resource.path_hint.like(f"%{kw}%")),
    ).order_by(QueueItem.id.desc())).all())
    # 去重：同一作品只保留一条（按 share_dir_id 或 tmdb_id+type 或文件 id）
    seen: set[str] = set()
    picked: list[Resource] = []
    for r in rows:
        key = (str(getattr(r, "share_dir_id", "") or "")
               or (f"tmdb:{r.media_type}:{r.tmdb_id}" if r.tmdb_id else f"file:{r.id}"))
        if key in seen:
            continue
        seen.add(key)
        picked.append(r)
        if len(picked) >= 5:
            break
    if not picked:
        return f"没有找到已推送、仍在库且匹配「{kw}」的资源。", []
    return f"找到 {len(picked)} 部与「{kw}」相关的资源，正在转发给你：", [int(r.id) for r in picked]


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
            row, created = upsert_contribution(
                db, tg_user_id=str(tg_user_id), tg_username=tg_username,
                title=str(picked.get("title") or "")[:512], content=raw[:4000],
                share_url=urls[-1], tmdb_id=picked.get("tmdb_id"),
                media_type=picked.get("media_type") or "",
                year=str(picked.get("year") or ""),
                note="TMDB 候选选定；投稿转存接口待验证，当前仅登记")
            _pick_clear(tg_user_id)
            _flow_clear(tg_user_id)
            # v0.9.9 #208：用户回复只给友好文案，管理员侧状态只在后台 note 可见
            thanks = (f"投稿已登记：{picked.get('title')}\n感谢支持，我们会尽快处理。"
                      if created else
                      f"「{picked.get('title')}」已有其它用户投稿，已合并登记（累计 {row.submit_count} 次），感谢支持！")
            return "contribute", thanks
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
    _flow_to_builtin = {FLOW_WANT: "求片", FLOW_CONTRIB: "投稿", FLOW_SEARCH: "搜索"}
    if active_flow and not builtin and not custom and not low.startswith("/"):
        builtin = _flow_to_builtin.get(active_flow, "求片")
        arg = raw
        low = builtin

    # v0.9.16 #277：投稿/求片/搜索只走「指令」或「Reply 键盘进入的会话态」。
    # 非会话态下任意内容（含裸链接 / 裸文字）一律不落库、不响应业务，回纠正提示。
    # 仅 /help /start /自定义指令 在上面已返回，裸分享链接不再触发任何投稿。
    if not builtin and not custom and not active_flow:
        return "unrecognized", _UNRECOGNIZED_HINT

    # v0.9.9 #209：裸命令（点 Reply 键盘触发，无参数）→ 进入会话态，提示直接输入文字。
    # 不再回「用法：/求片 …」逼用户加命令，也不再让投稿落「未命名投稿」。
    if builtin in ("求片", "投稿", "搜索") and not arg:
        if builtin == "求片":
            _flow_set(tg_user_id, FLOW_WANT)
            return "flow_want", ("已进入求片模式。请直接输入剧名，我会列出 TMDB 候选让你点选。\n"
                                 "随时回复「结束」或点下方按钮退出。")
        if builtin == "搜索":
            _flow_set(tg_user_id, FLOW_SEARCH)
            return "flow_search", ("已进入搜索模式。请直接输入剧名 / 关键词，我会把你之前"
                                   "推送过、仍在库的资源卡片转发给你。\n随时回复「结束」退出。")
        _flow_set(tg_user_id, FLOW_CONTRIB)
        return "flow_contribute", ("已进入投稿模式。请直接输入剧名（我会列 TMDB 候选），"
                                   "或直接粘贴光鸭分享链接。\n随时回复「结束」或点下方按钮退出。")

    # 用户带了内容 → 该轮交互完成，退出文字会话态（后续走正常候选/落库流程）
    if builtin in ("求片", "投稿", "搜索") and active_flow:
        _flow_clear(tg_user_id)

    if builtin == "搜索":
        text, ids = await _do_search(db, arg)
        # v0.9.12：把命中资源 id 随 action 交给 poll_once 发卡片（避免此处无 token/chat_id）
        return ("search_cards:" + ",".join(str(i) for i in ids)), text

    if builtin == "打赏" or (custom and str(custom.get("command") or "").lower() in ("donate", "打赏")):
        if (runtime_config.get(db, "donation_enabled") or "").lower() != "true":
            return "donate", "暂未开启打赏，感谢支持。"
        text = runtime_config.get(db, "donation_text") or "感谢支持！"
        img = runtime_config.get(db, "donation_image_url") or ""
        if img:
            # v0.9.12：直接把打赏图片发给用户，而不是把图片网址当文字输出。
            # action 携带图片 URL，poll_once 用 sendPhoto 发送。
            return "donate_photo:" + img, text
        return "donate", text

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
        # v0.9.13 #254：白名单写入后移到「审核通过」时执行；提交只登记待审核。
        # 同步细节（成功/失败）只写后台 note，绝不发给用户。
        req = MediaRequest(tg_user_id=str(tg_user_id), tg_username=tg_username,
                           title=title, tmdb_id=tmdb_id, media_type="tv" if season else "",
                           season=season, keyword_id=kid, status="pending",
                           review_status="pending", note="待管理员审核通过后同步白名单")
        db.add(req)
        db.flush()  # autoflush=False：写完即查必须显式 flush
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
        row, created = upsert_contribution(
            db, tg_user_id=str(tg_user_id), tg_username=tg_username,
            title=final_title, content=raw[:4000], share_url=share_url,
            note="投稿转存接口待验证，当前仅登记，不自动入库")
        # v0.9.9 #208：用户侧只给友好确认，「转存接口待验证」等管理员信息只在后台 note
        thanks = (f"投稿已登记：{final_title}\n感谢支持，我们会尽快处理。"
                  if created else
                  f"「{final_title}」已有其它用户投稿，已合并登记（累计 {row.submit_count} 次），感谢支持！")
        return "contribute", thanks

    if custom:
        return "custom", str(custom.get("reply") or custom.get("description") or "已收到指令。")

    # v0.9.16 #277：删除「普通消息里带链接也按投稿登记」分支——正是把用户发的
    # startgroup 授权链接误登记投稿的根因。合法投稿仅三条路径，均已在上面处理：
    #   ① /投稿 剧名 [链接]      → builtin=投稿 分支
    #   ② 点「投稿」进会话态后发剧名/链接 → 1259 转为 builtin=投稿 分支
    #   ③ TMDB 候选点选后补链接  → pending 等链接分支
    # 走到这里只剩会话态下发了未知 /命令（守卫已拦截非会话态裸内容），回纠正提示。
    return "unrecognized", _UNRECOGNIZED_HINT


ALLOWED_UPDATES = ["message", "callback_query", "my_chat_member", "channel_post"]


def _is_update_processed(db: Session, uid: int) -> bool:
    """v0.9.15 #276：update_id 幂等判断，防止重启/重复投递导致重复登记与重复回命令。"""
    if not uid:
        return False
    return db.get(ProcessedUpdate, uid) is not None


def _mark_update_processed(db: Session, uid: int, bot_id: int) -> None:
    if uid and not _is_update_processed(db, uid):
        db.add(ProcessedUpdate(update_id=uid, bot_id=bot_id, processed_at=utcnow()))


def _tg_error_hint(data: dict, http_status: int) -> tuple[str, str]:
    """把 Telegram 错误归一为 (短类目, 用户可读说明)。绝不返回 token/完整请求 URL。"""
    desc = str((data or {}).get("description") or "")
    low = desc.lower()
    if http_status == 409 or "conflict" in low:
        return "conflict", ("getUpdates 冲突：可能有另一个进程/实例正在用同一个 Bot Token 拉取更新，"
                            "或本机有多余的轮询任务。请检查并停止重复实例；本轮已跳过，将退避重试。")
    if http_status == 429 or "too many requests" in low or "retry after" in low:
        retry = 0
        try:
            params = (data or {}).get("parameters") or {}
            retry = int(params.get("retry_after") or 0)
        except (ValueError, TypeError):
            retry = 0
        return "rate_limited", f"Telegram 限流 429，已按 retry_after≈{retry}s 退避，本轮跳过。"
    if "not found" in low or "unauthorized" in low or "invalid token" in low:
        return "auth", "Bot Token 无效或已被撤销，请在 Bot 管理更新 Token。"
    if http_status >= 500:
        return "server", f"Telegram 服务暂时不可用（HTTP {http_status}），本轮跳过，下轮重试。"
    return "unknown", (desc[:120] or f"Telegram 请求失败（HTTP {http_status}）")


def _handle_my_chat_member(db: Session, token: str, upd: dict, bot_id: int,
                           stats: dict) -> None:
    """v0.9.15 #276：Bot 自身成员状态变化——被拉入群/频道即登记，被移出只标记不删。

    my_chat_member 与 chat_member 不同：前者是 Bot 自身状态，Telegram 无需管理员权限即可收到。
    """
    mc = upd.get("my_chat_member") or {}
    chat = mc.get("chat") or {}
    new_member = mc.get("new_chat_member") or mc.get("new_chat_participant") or {}
    status = str(new_member.get("status") or "").strip()
    chat_id = str(chat.get("id") or "")
    if not chat_id or not status:
        return
    user_obj = new_member.get("user") or {}
    bot_username = str(user_obj.get("username") or "") if isinstance(user_obj, dict) else ""
    if not bot_username:
        bot_username = str(new_member.get("username") or "")
    row = record_discovered_chat(
        db, chat, source="auto", flush=False, member_status=status,
        bot_username=bot_username, bot_id_override=bot_id,
        last_update_id=int(upd.get("update_id") or 0), count_message=False)
    if row is not None:
        stats["member_events"] = stats.get("member_events", 0) + 1
        stats["actions"].append("member_joined" if status not in ("left", "kicked") else "member_left")


def _handle_id_command(db: Session, token: str, chat: dict, *, bot_id: int,
                       update_id: int = 0) -> str | None:
    """v0.9.15 #276：群内 /id（含 /id@BotUsername）自动登记并回复群组信息。

    只在 group/supergroup 生效；只登记、不加入白名单；即使 Bot 无发送权限也先落库。
    返回给用户的回复文本；非 /id 命令返回 None。
    """
    ctype = str(chat.get("type") or "").strip()
    if ctype not in ("group", "supergroup"):
        return None
    row = record_discovered_chat(db, chat, source="auto", flush=False,
                                 bot_id_override=bot_id, last_update_id=int(update_id or 0))
    if row is None:
        return None
    type_label = "超级群组" if ctype == "supergroup" else "群组"
    bot_label = str(getattr(row, "bot_username", "") or "") or f"Bot#{bot_id}"
    return ("群组信息：\n"
            f"· 群组名称：{row.title or chat.get('title', '')}\n"
            f"· 群组 ID：{row.chat_id}\n"
            f"· 群组类型：{type_label}\n"
            f"· Bot：{bot_label}\n\n"
            "已登记，可以在 Ya-TG 管理后台选择加入推送白名单。")


async def poll_once(db: Session) -> dict:
    """拉取一次 Bot 更新（主交互 Bot）。返回统计；关闭或未配置时跳过，不产生网络请求。

    v0.9.2：新增 callback_query（inline 菜单按钮点击路由）。
    v0.9.15 #276：
      - allowed_updates 补 my_chat_member / channel_post；
      - update_id 幂等台账 + offset「处理成功后才推进」，重启不丢、重复不重；
      - my_chat_member 入群登记/退群标记、channel_post 频道发现、群内 /id 登记；
      - 409 冲突 / 429 限流分类处理，退避且不输出 token。
    """
    if (runtime_config.get(db, "contribution_enabled") or "").lower() != "true":
        return {"enabled": False, "messages": 0}
    token = effective_token(db)
    if not token:
        return {"enabled": False, "messages": 0, "error": "未配置交互 Bot（请在 Bot 管理添加并选用，或配置投稿 Bot Token）"}
    bot_id = effective_bot_id(db)
    # v0.9.9 #207：每个 token 首次轮询自动清空一次旧项目残留的 "/" 指令栏。
    # 指纹去重在 auto_clear_stale_commands 内部，正常轮询不产生额外请求。
    menu_cleared = await auto_clear_stale_commands(db, token)
    try:
        offset = int(runtime_config.get(db, "contribution_update_offset") or 0)
    except ValueError:
        offset = 0
    base = runtime_config.telegram_base(db)
    payload = {"offset": offset, "timeout": 2, "allowed_updates": ALLOWED_UPDATES}
    err_key = f"bot_poll_error_{bot_id or 'global'}"
    try:
        async with httpx.AsyncClient(timeout=25) as client:
            r = await client.get(f"{base}/bot{token}/getUpdates", params=payload)
        try:
            data = r.json()
        except Exception:
            data = {}
    except Exception as exc:
        runtime_config.set_many(db, {err_key: f"network: {str(exc)[:160]}"})
        db.commit()
        return {"enabled": True, "messages": 0, "error": str(exc)[:200]}
    # 兼容 mock/异常响应缺 status_code：按 200 处理，交由 ok=false 分支识别错误
    http_status = getattr(r, "status_code", 200)
    # Telegram 返回 ok=false 或 409/429：分类记录 + 退避，不推进 offset
    if http_status == 409 or (isinstance(data, dict) and data.get("ok") is False and
                              "conflict" in str(data.get("description", "")).lower()):
        kind, hint = _tg_error_hint(data if isinstance(data, dict) else {}, http_status)
        runtime_config.set_many(db, {err_key: f"{kind}: {hint}"})
        db.commit()
        return {"enabled": True, "messages": 0, "error": hint, "error_kind": kind}
    if http_status == 429 or http_status >= 500:
        kind, hint = _tg_error_hint(data if isinstance(data, dict) else {}, http_status)
        runtime_config.set_many(db, {err_key: f"{kind}: {hint}"})
        db.commit()
        return {"enabled": True, "messages": 0, "error": hint, "error_kind": kind}
    if isinstance(data, dict) and data.get("ok") is False:
        kind, hint = _tg_error_hint(data, http_status)
        runtime_config.set_many(db, {err_key: f"{kind}: {hint}"})
        db.commit()
        return {"enabled": True, "messages": 0, "error": hint, "error_kind": kind}
    updates = data.get("result") or [] if isinstance(data, dict) else []
    stats = {"enabled": True, "messages": len(updates), "handled": 0,
             "blocked": 0, "callbacks": 0, "discovered": 0, "member_events": 0,
             "actions": [], "menu_cleared": menu_cleared}
    # 处理成功后才推进的最高 update_id；失败/跳过的事件也记账，避免卡死在同一条
    max_committed = offset
    for upd in updates:
        uid = int(upd.get("update_id") or 0)
        if _is_update_processed(db, uid):
            continue  # 幂等：重启或重复投递已处理过的 update
        try:
            # 1) Bot 自身成员状态变化（入群/退群），独立于普通消息
            if upd.get("my_chat_member"):
                _handle_my_chat_member(db, token, upd, bot_id, stats)

            # 2) inline 菜单按钮点击
            cb = upd.get("callback_query") or {}
            if cb:
                out = await _handle_callback(db, token, cb)
                if out == "blocked":
                    stats["blocked"] += 1
                else:
                    stats["callbacks"] += 1

            # 3) 普通消息 / 频道新消息
            msg = upd.get("message") or upd.get("channel_post") or {}
            if msg:
                text = str(msg.get("text") or msg.get("caption") or "")
                chat = msg.get("chat") or {}
                user = msg.get("from") or {}
                chat_id = str(chat.get("id") or "")
                user_id = str(user.get("id") or chat_id)
                is_channel = bool(upd.get("channel_post"))
                # v0.9.15 #276：群内 /id 登记（拦截，不进指令分发）
                cmd_head = text.strip().split(None, 1)[0].lower().split("@")[0] if text.strip() else ""
                is_id_cmd = cmd_head == "/id" and str(chat.get("type") or "") in ("group", "supergroup")
                # 自动发现：群/频道消息即记录（放在限制检查之前）。
                # 注意：/id 群消息由 _handle_id_command 统一登记——它内部也会调
                # record_discovered_chat。若这里再登记一次，autoflush=False 下会留下
                # 两条 pending 行、提交时触发 (bot_id,chat_id) UNIQUE 冲突。故 /id 跳过通用登记。
                if chat and chat_id and not is_id_cmd:
                    record_discovered_chat(db, chat, flush=False, last_update_id=uid)
                    if is_channel:
                        stats["discovered"] = stats.get("discovered", 0) + 1
                id_reply = _handle_id_command(db, token, chat, bot_id=bot_id,
                                              update_id=uid) if is_id_cmd else None
                if id_reply and chat_id:
                    await _reply(db, token, chat_id, id_reply)
                    stats["handled"] += 1
                    stats["actions"].append("id")
                elif not is_channel and text:
                    try:
                        ok, reason = await check_restriction(db, token, user_id)
                        if not ok:
                            stats["blocked"] = stats.get("blocked", 0) + 1
                            if chat_id and text.strip().startswith("/"):
                                await _reply(db, token, chat_id, reason,
                                             reply_markup=restriction_keyboard(db))
                        else:
                            action, reply = await handle_text(
                                db, text, tg_user_id=user_id,
                                tg_username=str(user.get("username") or ""))
                            if action != "ignored":
                                stats["handled"] += 1
                                stats["actions"].append(
                                    action.split(":", 1)[0] if ":" in action else action)
                            if reply and chat_id:
                                await _dispatch_reply(db, token, chat_id, text, action, reply, user_id)
                    except Exception as exc:
                        stats.setdefault("errors", []).append(str(exc)[:120])
            _mark_update_processed(db, uid, bot_id)
            if uid > max_committed:
                max_committed = uid
        except Exception as exc:
            stats.setdefault("errors", []).append(str(exc)[:120])
            # 该 update 处理失败也记账，避免每次重启都卡在同一条；错误已进 stats 可见
            _mark_update_processed(db, uid, bot_id)
            if uid > max_committed:
                max_committed = uid
    new_offset = max(offset, max_committed + 1)
    runtime_config.set_many(db, {"contribution_update_offset": str(new_offset),
                                 err_key: ""})  # 成功轮询清空上一轮错误状态
    db.commit()
    stats["offset"] = new_offset
    return stats


async def _dispatch_reply(db: Session, token: str, chat_id: str, text: str,
                          action: str, reply: str, user_id: str) -> None:
    """把 handle_text 的回复按 action 分派到合适的发送方式（从 poll_once 抽出，保持原语义）。"""
    if action.startswith("search_cards:"):
        await _reply(db, token, chat_id, reply,
                     reply_markup=end_flow_keyboard() if _flow_get(user_id) else None)
        ids = [int(x) for x in action.split(":", 1)[1].split(",") if x.isdigit()]
        await _send_search_cards(db, token, chat_id, ids)
    elif action.startswith("donate_photo:"):
        await _send_photo(db, token, chat_id, action.split(":", 1)[1], reply)
    elif action == "help" and text.strip().lower().split("@")[0] in ("/start", "/help"):
        await _reply(db, token, chat_id, reply, reply_markup=main_reply_keyboard())
    elif action in ("want_pick", "contribute_pick"):
        sess = _pick_get(user_id)
        kb = _candidates_keyboard(sess["flow"], sess["candidates"]) if sess else None
        await _reply(db, token, chat_id, reply, reply_markup=kb)
    elif action == "flow_end":
        await _reply(db, token, chat_id, reply, reply_markup=main_reply_keyboard())
    elif action == "unrecognized":
        # v0.9.16 #277：非指令内容纠正提示，附主菜单引导用户走正确入口
        await _reply(db, token, chat_id, reply, reply_markup=main_reply_keyboard())
    elif action in ("flow_want", "flow_contribute", "flow_search", "contribute_await_url"):
        await _reply(db, token, chat_id, reply, reply_markup=end_flow_keyboard())
    elif _flow_get(user_id) or _pick_get(user_id):
        await _reply(db, token, chat_id, reply, reply_markup=end_flow_keyboard())
    else:
        await _reply(db, token, chat_id, reply)


# ==================== v0.9.15 #277：智能添加（getChat 解析） ====================
# 需求：数字 ID / @username / Telegram 公开链接 → 调 getChat 拿真实信息；
# 私有邀请链接（t.me/+xxx 或 t.me/joinchat/xxx）不能当 Chat ID，给明确提示。

_INVITE_MARKERS = ("t.me/+", "t.me/joinchat/", "telegram.me/+", "telegram.me/joinchat/")


def parse_chat_input(raw: str) -> dict:
    """纯解析：把一个输入框内容归一为 {kind, chat_id, error}。

    kind: id / username / invite / empty / invalid
    - 数字（含 -100 开头的超级群/频道）→ id
    - @xxx 或 t.me/xxx / telegram.me/xxx → username（归一为 @xxx）
    - 私有邀请链接 → invite（不解析 ID）
    """
    s = str(raw or "").strip()
    if not s:
        return {"kind": "empty", "chat_id": "", "error": ""}
    low = s.lower()
    if any(m in low for m in _INVITE_MARKERS):
        return {"kind": "invite", "chat_id": "", "error": (
            "这是私有群组邀请链接，无法直接解析群组 ID。请先将 Bot 添加到群组，"
            "再在群内发送 /id 自动获取群组信息。")}
    # 去掉协议与域名前缀
    if "t.me/" in low or "telegram.me/" in low:
        tail = re.split(r"t\.me/|telegram\.me/", s, maxsplit=1, flags=re.I)[-1]
        tail = tail.strip().strip("/").split("?", 1)[0].split("#", 1)[0]
        if not tail or "/" in tail:
            return {"kind": "invalid", "chat_id": "", "error": "链接格式无法识别，请检查后重试。"}
        if tail.lstrip("-").isdigit():
            return {"kind": "id", "chat_id": tail, "error": ""}
        return {"kind": "username", "chat_id": "@" + tail.lstrip("@"), "error": ""}
    # 裸 @用户名
    if s.startswith("@"):
        name = s.lstrip("@").strip()
        if not re.match(r"^[A-Za-z0-9_]{5,32}$", name):
            return {"kind": "invalid", "chat_id": "", "error": "用户名格式错误（应为 @字母数字下划线，5–32 位）。"}
        return {"kind": "username", "chat_id": "@" + name, "error": ""}
    # 裸数字 ID（可为负；频道/超级群 -100 开头）
    if re.match(r"^-?\d{5,20}$", s):
        return {"kind": "id", "chat_id": s, "error": ""}
    return {"kind": "invalid", "chat_id": "", "error": (
        "无法识别的输入。请填数字 Chat ID（如 -1001234567890）、"
        "@用户名 或公开链接（https://t.me/xxx）。")}


async def resolve_chat_via_getchat(db: Session, token: str, chat_ref: str) -> dict:
    """调 Telegram getChat 校验并取真实 chat 元数据，供智能添加落库。

    返回 {ok, chat:{id,title,type,username}, error_kind, error}。绝不输出 token。
    """
    data = await _api(db, token, "getChat", {"chat_id": chat_ref})
    if not isinstance(data, dict) or not data.get("ok"):
        kind, hint = _tg_error_hint(data if isinstance(data, dict) else {}, 200)
        return {"ok": False, "error_kind": kind, "error": hint}
    chat = data.get("result") or {}
    if not isinstance(chat, dict) or not chat.get("id"):
        return {"ok": False, "error_kind": "unknown", "error": "getChat 未返回有效 chat 信息。"}
    return {"ok": True, "chat": {
        "id": str(chat.get("id")),
        "title": str(chat.get("title") or chat.get("first_name") or ""),
        "type": str(chat.get("type") or ""),
        "username": str(chat.get("username") or ""),
    }}
