"""管理后台会话（v0.9.20）：替代「浏览器永久明文存密码」的登录态。

设计红线：
  - 浏览器不再保存管理员明文密码；登录时用 ADMIN_PASSWORD 换一个随机会话 token。
  - 会话是**进程内存态**：last_active 每次带 token 的请求刷新；超过 idle_minutes
    未活跃即失效，服务端强制 401（前端清 token 回登录页重新输密码）。
  - 进程重启 / 多实例：内存会话不共享，重启后需重新登录（安全预期），不做持久化。
  - 明文密码 header（X-Admin-Password）仅作脚本/API 兼容通道保留，浏览器日常不用。
  - token 只经 HTTPS/本机使用；不写库、不写日志、不返回给第三方。
"""
from __future__ import annotations

import secrets
import threading
import time

from .config import settings

# token -> 最后活跃时间（time.monotonic，不受系统时钟回拨影响）
_sessions: dict[str, float] = {}
_lock = threading.RLock()


def idle_seconds() -> int:
    """未活跃有效期（秒）。ADMIN_IDLE_MINUTES 可配，默认 120 分钟，范围 1~1440。"""
    try:
        minutes = int(getattr(settings, "admin_idle_minutes", 120))
    except (TypeError, ValueError):
        minutes = 120
    return max(1, min(minutes, 1440)) * 60


def create() -> str:
    """登录成功后签发会话 token。密码已由调用方校验通过。"""
    token = secrets.token_urlsafe(32)
    with _lock:
        _sessions[token] = time.monotonic()
    return token


def read(token: str) -> str:
    """只读校验会话，返回 'ok' / 'expired' / 'invalid'。**不刷新活跃时间**。

    供「会话状态」轮询端点使用——若轮询也刷新，用户挂着页面不操作就永不过期，
    违背 idle 语义。真实业务请求走 touch() 才刷新。
    """
    tok = str(token or "")
    if not tok:
        return "invalid"
    with _lock:
        last = _sessions.get(tok)
        if last is None:
            return "invalid"
        if time.monotonic() - last >= idle_seconds():
            _sessions.pop(tok, None)
            return "expired"
        return "ok"


def touch(token: str) -> str:
    """业务请求带 token 时调用：校验并刷新活跃时间。返回同 read()。"""
    tok = str(token or "")
    with _lock:
        state = read(tok)  # RLock 可重入
        if state == "ok":
            _sessions[tok] = time.monotonic()
        return state


def remaining_seconds(token: str) -> int:
    """会话剩余未活跃秒数（只读）；无效/过期返回 0。"""
    tok = str(token or "")
    with _lock:
        last = _sessions.get(tok)
        if last is None:
            return 0
        left = idle_seconds() - (time.monotonic() - last)
        if left <= 0:
            _sessions.pop(tok, None)
            return 0
        return int(left)


def destroy(token: str) -> None:
    """登出：立即失效该会话。"""
    with _lock:
        _sessions.pop(str(token or ""), None)


def clear_all() -> None:
    """清空所有会话（管理员改密码时调用，强制所有已登录端重新输入）。"""
    with _lock:
        _sessions.clear()
