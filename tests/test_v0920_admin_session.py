"""v0.9.20：管理后台会话登录 + 未活跃 idle 到期强制重新输入密码。

覆盖：
  1 密码换 token（成功/失败）        2 token 访问受保护端点
  3 idle 到期后 token 401（服务端强制） 4 只读会话轮询不刷新活跃时间（不续命）
  5 登出即失效                       6 明文密码 header 兼容（脚本/旧测试不破坏）
  7 改密码/重启等价 clear_all         8 idle 配置边界
  9 前端不再保存明文密码、401 回登录页
"""
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import admin_session
from app.db import migrate
from app.main import app

migrate()  # overview 端点要读真实表；模块级建表一次

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


# 模块级单例：避免每个用例 with TestClient 反复触发 lifespan 导致 scheduler
# 事件循环关闭（工作记忆记录的坑：单进程一次 lifespan）。
_client = TestClient(app)


@pytest.fixture
def client():
    admin_session.clear_all()
    yield _client
    admin_session.clear_all()


def _login(c, pwd="testpass"):
    return c.post("/api/auth/login", json={"password": pwd})


# ---------- 1 登录换 token ----------
def test_login_success_returns_token(client):
    r = _login(client)
    assert r.status_code == 200
    d = r.json()
    assert d["ok"] is True and len(d["token"]) >= 20
    assert d["idle_seconds"] == 120 * 60  # 默认 2 小时


def test_login_wrong_password_rejected(client):
    r = _login(client, "wrong")
    assert r.status_code == 401
    assert "密码错误" in r.json()["detail"]


def test_login_default_change_me_blocked(client, monkeypatch):
    from app import main as main_mod
    monkeypatch.setattr(main_mod.settings, "admin_password", "CHANGE_ME")
    r = _login(client, "CHANGE_ME")
    assert r.status_code == 503


# ---------- 2 token 访问受保护端点 ----------
def test_token_grants_access(client):
    token = _login(client).json()["token"]
    r = client.get("/api/health", headers={"X-Admin-Session": token})  # health 本就公开
    assert r.status_code == 200
    r = client.get("/api/overview", headers={"X-Admin-Session": token})
    assert r.status_code == 200
    r = client.get("/api/overview", headers={"X-Admin-Session": "bad-token"})
    assert r.status_code == 401


# ---------- 3 idle 到期服务端强制 401 ----------
def test_idle_expiry_forces_relogin(client):
    token = _login(client).json()["token"]
    # 把该会话的 last_active 回拨到超过 idle 阈值
    with admin_session._lock:
        admin_session._sessions[token] = time.monotonic() - admin_session.idle_seconds() - 10
    r = client.get("/api/overview", headers={"X-Admin-Session": token})
    assert r.status_code == 401
    assert "登录已过期" in r.json()["detail"]
    # token 已失效，再次访问仍 401
    assert client.get("/api/overview", headers={"X-Admin-Session": token}).status_code == 401


def test_invalid_session_message_distinct(client):
    r = client.get("/api/overview", headers={"X-Admin-Session": "not-a-token"})
    assert r.status_code == 401
    assert "会话无效" in r.json()["detail"]


# ---------- 4 只读轮询不续期 ----------
def test_session_poll_does_not_extend_idle(client):
    token = _login(client).json()["token"]
    with admin_session._lock:
        admin_session._sessions[token] = time.monotonic() - admin_session.idle_seconds() + 30  # 剩 30s
    # 轮询只读：返回活跃但剩余时间 ≈ 30s，不刷新
    s1 = client.get("/api/auth/session", headers={"X-Admin-Session": token})
    assert s1.status_code == 200 and s1.json()["active"] is True
    assert 0 < s1.json()["remaining_seconds"] <= 30
    with admin_session._lock:
        last_after_poll = admin_session._sessions[token]
    # 再轮一次，剩余不应增加（仍在 30s 附近）
    s2 = client.get("/api/auth/session", headers={"X-Admin-Session": token})
    assert s2.json()["remaining_seconds"] <= s1.json()["remaining_seconds"]
    # 真实业务请求才 touch：此时调一次 overview，会话活跃时间被刷新到当前
    client.get("/api/overview", headers={"X-Admin-Session": token})
    with admin_session._lock:
        assert admin_session._sessions[token] > last_after_poll
    assert client.get("/api/auth/session", headers={"X-Admin-Session": token}).json()["remaining_seconds"] > 60


def test_session_status_expired_returns_401(client):
    token = _login(client).json()["token"]
    admin_session.destroy(token)
    r = client.get("/api/auth/session", headers={"X-Admin-Session": token})
    assert r.status_code == 401


# ---------- 5 登出即失效 ----------
def test_logout_invalidates_token(client):
    token = _login(client).json()["token"]
    assert client.get("/api/overview", headers={"X-Admin-Session": token}).status_code == 200
    r = client.post("/api/auth/logout", headers={"X-Admin-Session": token})
    assert r.status_code == 200
    assert client.get("/api/overview", headers={"X-Admin-Session": token}).status_code == 401


# ---------- 6 明文密码兼容 ----------
def test_plaintext_header_still_works(client):
    # 脚本/旧测试依赖的 X-Admin-Password 通道不能被破坏
    r = client.get("/api/overview", headers={"X-Admin-Password": "testpass"})
    assert r.status_code == 200
    assert client.get("/api/overview", headers={"X-Admin-Password": "wrong"}).status_code == 401


# ---------- 7 clear_all（改密码/重启强制下线） ----------
def test_clear_all_forces_all_relogin(client):
    t1 = _login(client).json()["token"]
    t2 = _login(client).json()["token"]
    assert t1 != t2
    admin_session.clear_all()
    assert client.get("/api/overview", headers={"X-Admin-Session": t1}).status_code == 401
    assert client.get("/api/overview", headers={"X-Admin-Session": t2}).status_code == 401


# ---------- 8 idle 配置边界 ----------
def test_idle_seconds_config_bounds(monkeypatch):
    from app import main as main_mod
    monkeypatch.setattr(main_mod.settings, "admin_idle_minutes", 99999)
    assert admin_session.idle_seconds() == 1440 * 60
    monkeypatch.setattr(main_mod.settings, "admin_idle_minutes", 0)
    assert admin_session.idle_seconds() == 60
    monkeypatch.setattr(main_mod.settings, "admin_idle_minutes", "bad")
    assert admin_session.idle_seconds() == 120 * 60


# ---------- 9 前端静态 ----------
def test_frontend_no_plaintext_password_storage():
    assert "ya_tg_session" in APP_VUE
    assert "X-Admin-Session" in APP_VUE
    assert "/api/auth/login" in APP_VUE
    assert "forceSessionExpired" in APP_VUE
    assert "登录已过期" in APP_VUE
    # 明文密码写入 localStorage 的代码必须消失（只保留清理历史遗留的 removeItem）
    assert "localStorage.setItem('ya_tg_password'" not in APP_VUE
    assert "localStorage.setItem(\"ya_tg_password\"" not in APP_VUE
