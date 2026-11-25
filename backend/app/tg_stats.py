"""v2.1.7 #241：Telegram 阅读量采集（MTProto 接入）。

v2.1.50 改口径：**用推送 Bot 自己的令牌登录，不再要手机号 + 验证码**
---------------------------------------------------------------
v2.1.7 最初用的是「用户账号 + 手机号 + 一次性验证码」。真机上这条路极其难走：
换号要重新人工收码、验证码超时就失败、还要在服务器上维护一个真人账号的会话。

关键事实（用户实测 + 本轮核对）：**Bot 走 MTProto 一样能读到 `Message.views`**。
`getHistory`（拉历史消息）对 Bot 会被服务端拒（`BotMethodInvalidError`），
但我们本来就只需要「按 message_id 精确取一条」——`get_messages(ids=...)` 对 Bot 是放行的。
所以整条链路可以只用 Bot 令牌，与推送用的是同一个 Bot：
- 会话文件 `tg_bot_<bot_id>.session`，令牌取 `bots.token`（Fernet 密文，用时解密）；
- **首次采集自动登录**（`_ensure_authorized` 里发现未授权就 `sign_in(bot_token=...)`），
  人工零操作；管理页那个按钮退化成「检测一下通不通」。

为什么不能用 Bot API
-------------------
Bot API **不暴露消息浏览量**——`getChat` / `getFile` 等方法拿不到「这条帖子被看过几次」。
只有 MTProto 的 `messages.GetHistory` / `messages.GetMessages` 返回的 `Message`
对象里才有 `views` 字段。Bot 令牌可以通过 MTProto 协议登录，所以两者不冲突。

口径边界（务必记住，报表不得越界）
--------------------------------
1. **群聊不暴露浏览量**：Telegram 只对**频道**（channel）帖计数，群聊/超级群的
   消息没有 `views` 概念（返回 None 或 0）。所以群聊目标只能采集
   转发数与表情回应，**不得**在页面上承诺群消息的「浏览量」。
2. **阅读量是累计值**：Telegram 只给当前总数，不给增量。趋势必须由
   相邻两次快照求差得到——所以本模块只负责把「当前总数」如实写入快照表，
   差值计算放在报表层。
3. **Bot 只能读到它自己看得见的目标**：Bot 必须是该频道/群的管理员或成员，
   否则拿到「消息不存在」，此时如实标记为不可采集，**不编造 0**。

依赖
----
`telethon`（纯 Python MTProto 客户端）。未安装时本模块的所有对外能力
一律返回「未启用」而不是抛异常——**绝不让阅读量功能拖垮主链**。

凭证管理
--------
`api_id` / `api_hash` 走 runtime_config（Fernet 加密落库 + 管理页配置）；
Bot 令牌复用「Bot 管理」里那一条（同一份密文，不另存）。
会话：本地 `.session` 文件（目录由 `tg_session_dir` 配置，默认 `./data/tg_session`）。
**会话文件等同于账号凭据**，绝不通过接口返回给前端，只报告「是否已连接」。
"""
from __future__ import annotations

import asyncio
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from . import runtime_config
from .models import Bot, Channel
from .security import decrypt_secret

# ---------------------------------------------------------------------------
# 配置键（走 runtime_config → app_config 加密落库 / .env 兜底）
# ---------------------------------------------------------------------------

# MTProto 应用的 api_id（my.telegram.org 申请的数字）。
CFG_API_ID = "tg_api_id"
# MTProto 应用的 api_hash（my.telegram.org 申请的 32 位十六进制）。
CFG_API_HASH = "tg_api_hash"
# v2.1.50：用哪个 Bot 的令牌登录（bots.id 的字符串形式）。
# 留空则自动挑「有启用推送目标且启用」的第一个 Bot —— 即推送在用的那个。
CFG_BOT_ID = "tg_stats_bot_id"
# 会话文件所在目录。
CFG_SESSION_DIR = "tg_session_dir"
# 采集总开关。
CFG_ENABLED = "tg_stats_enabled"
# 采集回溯窗口（小时）：只回溯这个时间窗内推送过的消息。
CFG_LOOKBACK_HOURS = "tg_stats_lookback_hours"
# 单轮采集的最大消息数（防接口风暴护栏）。
CFG_MAX_PER_RUN = "tg_stats_max_per_run"
# 定时采集间隔（小时）。阅读量是累计值，趋势必须靠**周期采集**才能形成，
# 只靠手动点按钮永远只有一条数据点。0 表示关闭定时采集（仅手动）。
CFG_COLLECT_INTERVAL_HOURS = "tg_stats_collect_interval_hours"

# 已建立的客户端会话缓存：key = 会话标识，value = Telethon Client。
_CLIENTS: dict[str, Any] = {}
_CLIENT_LOCK = threading.Lock()

# 采集失败计数（连续失败达上限即停止本轮，不再空转打接口）。
_CONSECUTIVE_FAILURES: dict[str, int] = {}


class MTProtoUnavailable(RuntimeError):
    """MTProto 能力不可用（依赖缺失 / 凭证未配置 / 未登录）。

    对外一律转成「未启用」状态返回，**不向上抛**——阅读量是辅助能力，
    不能因为它让主链（扫描 / 识别 / 推送）失败。
    """


@dataclass
class MessageStat:
    """一条消息的采集结果。available=False 时其余字段无意义。"""

    message_id: str = ""
    views: int = 0
    forwards: int = 0
    reactions: int = 0
    available: bool = True
    note: str = ""
    # 目标类型：channel / group。决定 views 是否有意义。
    target_kind: str = "channel"
    extra: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# 依赖与凭证
# ---------------------------------------------------------------------------

def telethon_available() -> bool:
    """telethon 是否已安装。未安装时**不报错**，只让功能处于未启用状态。"""
    try:
        import telethon  # noqa: F401
        return True
    except Exception:
        return False


def _require_telethon():
    try:
        from telethon import TelegramClient
        return TelegramClient
    except Exception as exc:  # pragma: no cover - 依赖缺失路径
        raise MTProtoUnavailable(f"未安装 telethon，无法使用阅读量采集：{exc}") from exc


def _session_dir(db: Session) -> Path:
    """会话文件所在目录（**只算路径，不建目录**）。

    只读的状态查询（status → session_exists）不能有写盘副作用——
    否则「看一眼状态」就会在服务器上凭空造出目录。
    真正需要落盘的地方（登录 / 建客户端）显式调 _ensure_session_dir()。
    """
    raw = runtime_config.get(db, CFG_SESSION_DIR) or os.environ.get("TG_SESSION_DIR") or ""
    if not raw:
        raw = "./data/tg_session"
    return Path(raw).expanduser()


def _ensure_session_dir(db: Session) -> Path:
    """确保会话目录存在（登录 / 建客户端前调用）。"""
    path = _session_dir(db)
    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return path


def _stats_bot(db: Session) -> Bot | None:
    """采集用哪个 Bot：运行时指定优先，否则挑「有启用推送目标」的第一个。

    为什么要挑「有启用推送目标」的：采集能读到的只能是 Bot 自己看得见的频道/群。
    推送在用的那个 Bot 一定在目标里，所以默认跟推送对齐，不需要用户再选一次。
    """
    raw = str(runtime_config.get(db, CFG_BOT_ID) or "").strip()
    if raw.isdigit():
        bot = db.get(Bot, int(raw))
        if bot is not None and bool(bot.enabled):
            return bot
    ids = [int(x) for x in db.scalars(
        select(Channel.bot_id).where(Channel.enabled.is_(True)).distinct()).all() if x]
    if ids:
        bot = db.scalars(select(Bot).where(Bot.id.in_(ids), Bot.enabled.is_(True))
                         .order_by(Bot.id)).first()
        if bot is not None:
            return bot
    return db.scalars(select(Bot).where(Bot.enabled.is_(True))
                      .order_by(Bot.id)).first()


def _bot_token(db: Session) -> str:
    """取采集用 Bot 的明文令牌（解密失败按空处理，不抛——不能拖垮主链）。"""
    bot = _stats_bot(db)
    if bot is None:
        return ""
    try:
        return str(decrypt_secret(bot.token) or "").strip()
    except Exception:
        return ""


def _session_path(db: Session) -> Path:
    """会话文件路径。文件名由**推送 Bot** 派生（同一个 Bot 复用同一会话）。

    v2.1.50：此前按手机号派生（`tg_<手机号>.session`）。bot 直连后没有手机号了，
    改成 `tg_bot_<bot_id>.session`——换 Bot 等于换会话，不会串号。
    """
    bot = _stats_bot(db)
    safe = f"bot_{int(bot.id)}" if bot is not None else "bot_default"
    return _session_dir(db) / f"tg_{safe}.session"


def _api_credentials(db: Session) -> tuple[int, str]:
    api_id_raw = (runtime_config.get(db, CFG_API_ID) or "").strip()
    api_hash = (runtime_config.get(db, CFG_API_HASH) or "").strip()
    if not api_id_raw or not api_hash:
        raise MTProtoUnavailable("尚未配置 MTProto 凭证（api_id / api_hash）")
    try:
        api_id = int(api_id_raw)
    except ValueError as exc:
        raise MTProtoUnavailable(f"api_id 必须是数字，当前值非法：{api_id_raw[:16]}") from exc
    if not api_hash:
        raise MTProtoUnavailable("尚未配置 MTProto 凭证（api_id / api_hash）")
    return api_id, api_hash


def is_configured(db: Session) -> bool:
    """是否已具备采集所需的凭证配置（不含登录态）。"""
    try:
        _api_credentials(db)
    except MTProtoUnavailable:
        return False
    return True


def session_exists(db: Session) -> bool:
    """本地会话文件是否已存在（= 大概率已登录过）。"""
    try:
        return _session_path(db).exists()
    except Exception:
        return False


def status(db: Session) -> dict[str, Any]:
    """采集能力的当前状态（供管理页展示）。**不返回任何明文凭证**。"""
    bot = _stats_bot(db)
    out: dict[str, Any] = {
        "dependency_ready": telethon_available(),
        "configured": is_configured(db),
        # v2.1.50：`logged_in` 的含义变了 —— 不再有「人工登录」这一步，
        # 它只表示「本地已有会话文件」（= 之前自动签过）。没有也不影响使用：
        # 下一轮采集会自动签一次，所以前端不把它当门槛。
        "logged_in": session_exists(db),
        "bot_ready": bool(bot is not None and _bot_token(db)),
        "bot_name": str(getattr(bot, "name", "") or ""),
        "enabled": (runtime_config.get(db, CFG_ENABLED) or "false").lower() == "true",
        "lookback_hours": _int_cfg(db, CFG_LOOKBACK_HOURS, 72),
        "max_per_run": _int_cfg(db, CFG_MAX_PER_RUN, 200),
        "interval_hours": _int_cfg(db, CFG_COLLECT_INTERVAL_HOURS, 0),
    }
    if not out["dependency_ready"]:
        out["reason"] = "未安装 telethon 依赖"
    elif not out["configured"]:
        out["reason"] = "未配置 api_id / api_hash"
    elif not out["bot_ready"]:
        out["reason"] = "还没有可用的 Bot（先到「Bot 管理」添加并启用一个）"
    elif not out["logged_in"]:
        out["reason"] = "首次采集时会自动用该 Bot 连接，无需手动登录"
    return out


def _int_cfg(db: Session, key: str, default: int) -> int:
    try:
        return int(runtime_config.get(db, key) or default)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# 连接（bot 直连，无需人工登录）
# ---------------------------------------------------------------------------

async def ensure_bot_login(db: Session) -> dict[str, Any]:
    """用推送 Bot 的令牌建立 MTProto 会话（人工零操作）。

    v2.1.50：这是「登录」的全部内容了——不再有手机号、验证码两步。
    已经有有效会话时直接返回 `already=True`，不重复签入。
    """
    TelegramClient = _require_telethon()
    api_id, api_hash = _api_credentials(db)
    bot = _stats_bot(db)
    if bot is None:
        raise MTProtoUnavailable("还没有可用的 Bot，请先在「Bot 管理」里添加并启用一个")
    token = _bot_token(db)
    if not token:
        raise MTProtoUnavailable(f"Bot「{bot.name}」的令牌为空或无法解密，请重新填写")
    _ensure_session_dir(db)
    client = TelegramClient(str(_session_path(db)), api_id, api_hash)
    try:
        await client.connect()
        if await client.is_user_authorized():
            return {"ok": True, "already": True, "bot": bot.name}
        await client.sign_in(bot_token=token)
        return {"ok": True, "already": False, "bot": bot.name}
    except MTProtoUnavailable:
        raise
    except Exception as exc:
        raise MTProtoUnavailable(f"用 Bot「{bot.name}」连接失败：{str(exc)[:300]}") from exc
    finally:
        # 只做「签一次」这一件事；后续采集由 _get_client 统一建连（跨事件循环自己重建）。
        try:
            await client.disconnect()
        except Exception:
            pass


def logout(db: Session) -> dict[str, Any]:
    """断开并删除本地会话文件（下次采集会自动重新签一次，无需人工登录）。"""
    try:
        path = _session_path(db)
        removed = False
        for suffix in ("", "-shm", "-journal"):
            p = Path(str(path) + suffix)
            if p.exists():
                p.unlink()
                removed = True
        return {"ok": True, "removed": removed}
    except Exception as exc:
        raise MTProtoUnavailable(f"退出登录失败：{str(exc)[:300]}") from exc


# ---------------------------------------------------------------------------
# 客户端
# ---------------------------------------------------------------------------

def _get_client(db: Session):
    """取（或建立）绑定了本地 session 的 Telethon 客户端。

    客户端是**模块级缓存**：MTProto 客户端持有长连接，重建代价高。
    但必须避免多事件循环复用同一客户端——因此在检测到客户端所属循环与当前
    循环不同时，主动断开重建。
    """
    TelegramClient = _require_telethon()
    api_id, api_hash = _api_credentials(db)
    key = str(_session_path(db))
    with _CLIENT_LOCK:
        client = _CLIENTS.get(key)
        if client is not None:
            loop = getattr(client, "_loop", None)
            try:
                running = asyncio.get_running_loop()
            except RuntimeError:
                running = None
            if loop is not None and running is not None and loop is not running:
                # 跨事件循环：断开丢弃，下一轮重建。
                _CLIENTS.pop(key, None)
                try:
                    client.disconnect()
                except Exception:
                    pass
                client = None
            else:
                return client
        # 建客户端要落 session 文件 → 这里才确保目录存在。
        _ensure_session_dir(db)
        # v2.1.50：**不再传 api_base**。telethon 1.44 的 TelegramClient 没有这个
        # 参数（历史上传了就是 TypeError，整条采集链一进来就炸）。
        # 需要自建 DC / 反代时走 telethon 自己的 connection 参数，不在这里拼。
        client = TelegramClient(key, api_id, api_hash)
        _CLIENTS[key] = client
        return client


async def _ensure_authorized(client, db: Session | None = None) -> None:
    """确保客户端已连上且已授权；未授权就**当场用 Bot 令牌签一次**。

    v2.1.50：这是「登录太费劲」的真正解法——人工不需要再点任何东西。
    只要 api_id/api_hash 与 Bot 令牌在，首次采集会自己完成签入，
    会话落在 `tg_bot_<id>.session`，之后直接复用。
    """
    await client.connect()
    if await client.is_user_authorized():
        return
    token = _bot_token(db) if db is not None else ""
    if not token:
        raise MTProtoUnavailable("采集会话尚未建立，且没有可用的 Bot 令牌"
                                 "（请到「Bot 管理」确认 Bot 已启用、令牌已填写）")
    try:
        await client.sign_in(bot_token=token)
    except Exception as exc:
        raise MTProtoUnavailable(f"用 Bot 令牌自动登录失败：{str(exc)[:200]}") from exc


async def close_client(db: Session) -> None:
    """关闭并丢弃缓存客户端（应用退出 / 换号时调用）。"""
    key = str(_session_path(db))
    with _CLIENT_LOCK:
        client = _CLIENTS.pop(key, None)
    if client is None:
        return
    try:
        await client.disconnect()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# 采集
# ---------------------------------------------------------------------------

async def collect_message_stat(client, chat_id: str, message_id: str) -> MessageStat:
    """采集单条消息的阅读量 / 转发 / 表情回应。

    口径处理：
    - 消息读不到（已删除 / 无权限）→ available=False + note，**不返回 0**；
    - 群聊消息没有 views → views 记 0 且 available 仍为 True，
      但调用方据 target_kind=group 知道**不能**展示浏览量。
    """
    stat = MessageStat(message_id=str(message_id))
    try:
        entity = await client.get_entity(chat_id)
    except Exception as exc:
        stat.available = False
        stat.note = f"无法访问该推送目标：{str(exc)[:120]}"
        return stat
    try:
        msg = await client.get_messages(entity, ids=int(message_id))
    except (TypeError, ValueError):
        # message_id 非数字（历史脏数据 / 外部写入）→ 无法定位消息，如实标记不可采集。
        stat.available = False
        stat.note = "该条消息的编号格式无效，无法定位"
        return stat
    except Exception as exc:
        stat.available = False
        stat.note = f"无法读取该条消息：{str(exc)[:120]}"
        return stat
    if msg is None or getattr(msg, "empty", False):
        stat.available = False
        stat.note = "消息已不存在或不可见"
        return stat

    views = getattr(msg, "views", None)
    forwards = getattr(msg, "forwards", None)
    reactions = getattr(msg, "reactions", None)
    stat.views = int(views) if isinstance(views, int) else 0
    stat.forwards = int(forwards) if isinstance(forwards, int) else 0
    stat.reactions = _count_reactions(reactions)
    # 判断目标类型：Telegram 实体是 Channel 且不是 megagroup → 频道（有浏览量）。
    stat.target_kind = "channel" if _is_channel(entity) else "group"
    if stat.target_kind == "group" and not stat.views:
        stat.note = "群聊不提供浏览量统计"
    return stat


def _count_reactions(reactions) -> int:
    """表情回应总数。Telegram 返回 ReactionCount 列表，取各项 count 之和。"""
    if not reactions:
        return 0
    total = 0
    try:
        for item in reactions.results if hasattr(reactions, "results") else reactions:
            count = getattr(item, "count", 0)
            if isinstance(count, int):
                total += count
    except Exception:
        return 0
    return total


def _is_channel(entity) -> bool:
    """判定实体是否为**频道**（有浏览量）。

    Telethon 用同一个 `Channel` 类型表示频道与超级群，用 `megagroup` 属性区分：
    `megagroup=True` 是群（无浏览量），`False` 是频道（有浏览量）。

    实现要点：**优先看 `megagroup` 属性**，而不是比较类名——
    类名比较对测试替身与未来 Telethon 版本改名都脆弱；
    且实体本身带 `broadcast` / `megagroup` 属性时，语义最明确。
    """
    if entity is None:
        return False
    # 有 megagroup 属性时按它判定（Channel 实体的权威依据）。
    megagroup = getattr(entity, "megagroup", None)
    if isinstance(megagroup, bool):
        return not megagroup
    # 无 megagroup 属性：退而看 broadcast（频道/广播标记）。
    broadcast = getattr(entity, "broadcast", None)
    if isinstance(broadcast, bool):
        return broadcast
    # 两者都没有 → 不是 Channel 实体（User / Chat），按群处理。
    return False
