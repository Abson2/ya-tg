"""v2.1.7 #241：Telegram 阅读量采集（MTProto user account）。

为什么需要 MTProto 而不是 Bot API
--------------------------------
Bot API **不暴露消息浏览量**——`getChat` / `getFile` 等方法拿不到「这条帖子被看过几次」。
只有 MTProto 的 `messages.GetHistory` 返回的 `Message` 对象里才有 `views` 字段。
因此要统计频道帖的阅读量，必须用**用户账号**（user account）以 MTProto 协议接入。

口径边界（务必记住，报表不得越界）
--------------------------------
1. **群聊不暴露浏览量**：Telegram 只对**频道**（channel）帖计数，群聊/超级群的
   消息没有 `views` 概念（返回 None 或 0）。所以群聊目标只能采集
   转发数与表情回应，**不得**在页面上承诺群消息的「浏览量」。
2. **阅读量是累计值**：Telegram 只给当前总数，不给增量。趋势必须由
   相邻两次快照求差得到——所以本模块只负责把「当前总数」如实写入快照表，
   差值计算放在报表层。
3. **用户账号权限边界**：普通用户账号只能读到它**自己有权查看**的频道。
   私有频道若该账号不是订阅者就读不到——采集会拿到「消息不存在」，
   此时如实标记为不可采集，**不编造 0**。

依赖
----
`telethon`（纯 Python MTProto 客户端）。未安装时本模块的所有对外能力
一律返回「未启用」而不是抛异常——**绝不让阅读量功能拖垮主链**。
镜像构建时通过 `backend/requirements.txt` 安装。

登录与会话
----------
首次登录需要 `api_id` + `api_hash` + 手机号 + 验证码（验证码是**一次性**的，
由用户在管理页输入，**不落库**）。登录成功后 Telethon 会在本地写一个
`.session` 文件，之后无需重复认证。

凭证管理
--------
`api_id` / `api_hash` / 手机号 走 runtime_config（Fernet 加密落库 + 管理页配置），
与会话文件路径分离：
- 凭证：数据库 `app_config`（加密），可通过管理页随时改；
- 会话：本地 `.session` 文件（目录由 `tg_session_dir` 配置，默认 `./data/tg_session`）。
  **会话文件等同于账号凭据**，绝不通过接口返回给前端，只报告「是否已登录」。
"""
from __future__ import annotations

import asyncio
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from . import runtime_config

# ---------------------------------------------------------------------------
# 配置键（走 runtime_config → app_config 加密落库 / .env 兜底）
# ---------------------------------------------------------------------------

# MTProto 应用的 api_id（my.telegram.org 申请的数字）。
CFG_API_ID = "tg_api_id"
# MTProto 应用的 api_hash（my.telegram.org 申请的 32 位十六进制）。
CFG_API_HASH = "tg_api_hash"
# 登录用的手机号（含国际区号，如 +86...）。
CFG_PHONE = "tg_phone"
# 会话文件所在目录。
CFG_SESSION_DIR = "tg_session_dir"
# 采集总开关。
CFG_ENABLED = "tg_stats_enabled"
# 采集回溯窗口（小时）：只回溯这个时间窗内推送过的消息。
CFG_LOOKBACK_HOURS = "tg_stats_lookback_hours"
# 单轮采集的最大消息数（防接口风暴护栏）。
CFG_MAX_PER_RUN = "tg_stats_max_per_run"
# MTProto 接入点（默认官方；支持自建 DC / 反代）。
CFG_API_BASE = "tg_api_base"

# 用户码登录（首次 / 换号）时临时保存的登录态。
# 只在**内存**里活到验证码校验完成，不落库——验证码是一次性的。
_PENDING_LOGIN: dict[str, Any] = {}
_PENDING_LOCK = threading.Lock()

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
    raw = runtime_config.get(db, CFG_SESSION_DIR) or os.environ.get("TG_SESSION_DIR") or ""
    if not raw:
        raw = "./data/tg_session"
    path = Path(raw).expanduser()
    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return path


def _session_path(db: Session) -> Path:
    """会话文件路径。文件名由手机号派生（同号复用同一会话）。"""
    phone = runtime_config.get(db, CFG_PHONE) or ""
    safe = "".join(ch for ch in phone if ch.isdigit()) or "default"
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
    out: dict[str, Any] = {
        "dependency_ready": telethon_available(),
        "configured": is_configured(db),
        "logged_in": session_exists(db),
        "enabled": (runtime_config.get(db, CFG_ENABLED) or "false").lower() == "true",
        "lookback_hours": _int_cfg(db, CFG_LOOKBACK_HOURS, 72),
        "max_per_run": _int_cfg(db, CFG_MAX_PER_RUN, 200),
    }
    if not out["dependency_ready"]:
        out["reason"] = "未安装 telethon 依赖"
    elif not out["configured"]:
        out["reason"] = "未配置 api_id / api_hash"
    elif not out["logged_in"]:
        out["reason"] = "尚未完成登录"
    return out


def _int_cfg(db: Session, key: str, default: int) -> int:
    try:
        return int(runtime_config.get(db, key) or default)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# 登录
# ---------------------------------------------------------------------------

async def request_login_code(db: Session) -> dict[str, Any]:
    """第一步：向手机号请求登录验证码。

    返回中性信息给前端（不含验证码本身）——验证码由 Telegram 直接发给用户，
    用户再填回页面。验证码**不落库**。
    """
    TelegramClient = _require_telethon()
    api_id, api_hash = _api_credentials(db)
    phone = (runtime_config.get(db, CFG_PHONE) or "").strip()
    if not phone:
        raise MTProtoUnavailable("未配置登录手机号")
    client = TelegramClient(
        str(_session_path(db)), api_id, api_hash,
        api_base=_api_base(db),
    )
    try:
        await client.connect()
        if await client.is_user_authorized():
            return {"ok": True, "sent": False, "reason": "already_authorized"}
        sent = await client.send_code_request(phone)
    except MTProtoUnavailable:
        raise
    except Exception as exc:
        raise MTProtoUnavailable(f"发送验证码失败：{str(exc)[:300]}") from exc
    finally:
        # connect 成功但流程未完成时断开——避免连接泄漏。
        if not _pending_exists():
            try:
                await client.disconnect()
            except Exception:
                pass
    with _PENDING_LOCK:
        _PENDING_LOGIN["phone"] = phone
        _PENDING_LOGIN["phone_code_hash"] = getattr(sent, "phone_code_hash", "")
        _PENDING_LOGIN["client"] = client
    return {"ok": True, "sent": True}


def _pending_exists() -> bool:
    with _PENDING_LOCK:
        return bool(_PENDING_LOGIN.get("client"))


async def complete_login(db: Session, code: str) -> dict[str, Any]:
    """第二步：提交验证码完成登录。

    验证码是**一次性**的，仅用于这一步，**绝不落库**。
    """
    TelegramClient = _require_telethon()
    api_id, api_hash = _api_credentials(db)
    code = (code or "").strip()
    if not code:
        raise MTProtoUnavailable("验证码为空")
    with _PENDING_LOCK:
        client = _PENDING_LOGIN.get("client")
        phone = _PENDING_LOGIN.get("phone", "")
        phone_code_hash = _PENDING_LOGIN.get("phone_code_hash", "")
    if client is None:
        raise MTProtoUnavailable("没有进行中的登录流程，请先获取验证码")
    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
    except Exception as exc:
        raise MTProtoUnavailable(f"验证码校验失败：{str(exc)[:300]}") from exc
    finally:
        with _PENDING_LOCK:
            _PENDING_LOGIN.clear()
    # 登录成功后关闭这个临时连接；后续采集用带 session 的客户端。
    try:
        await client.disconnect()
    except Exception:
        pass
    return {"ok": True}


def logout(db: Session) -> dict[str, Any]:
    """退出登录：删除本地会话文件（下次采集需重新登录）。"""
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


def _api_base(db: Session) -> str | None:
    raw = (runtime_config.get(db, CFG_API_BASE) or "").strip()
    return raw or None


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
        client = TelegramClient(key, api_id, api_hash, api_base=_api_base(db))
        _CLIENTS[key] = client
        return client


async def _ensure_authorized(client) -> None:
    await client.connect()
    if not await client.is_user_authorized():
        raise MTProtoUnavailable("账号未登录或登录已失效，请重新登录")


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
