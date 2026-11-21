"""分享中心路径收敛回归：旧 /api/share/* 分享中心端点已删除，统一走 /api/sc/*。

背景：v2.0.2 移除旧分享中心 15 个 HTTP 路径（前端卡片化后零调用），
仅保留 /api/share/status（光鸭分享会话状态，前端「光鸭授权」页在用）。
本测试锁定：旧路径返回 404、新体系路径 200、光鸭 status 端点不被误删/覆盖。
复用 conftest 全局 app（不改环境、不建新 engine）。
"""
import asyncio
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))


def test_old_share_routes_removed_new_sc_routes_active():
    from app.main import app
    from app.config import settings
    from app.db import migrate
    from httpx import ASGITransport, AsyncClient
    migrate()  # ASGITransport 不触发 lifespan，显式建表（幂等）

    async def scenario():
        transport = ASGITransport(app=app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.post("/api/auth/login", json={"password": settings.admin_password})
            assert r.status_code == 200
            h = {"x-admin-session": r.json()["token"]}
            # 旧分享中心路径全部移除（GET 路径返回 404）
            for old in ("/api/share/center/status", "/api/share/tasks",
                        "/api/share/records", "/api/share/followups",
                        "/api/share/schedule", "/api/share/publish-overview",
                        "/api/share/rules"):
                r = await c.get(old, headers=h)
                assert r.status_code == 404, f"旧路径未删除：{old} -> {r.status_code}"
            # 新体系路径正常
            r = await c.get("/api/sc/overview", headers=h)
            assert r.status_code == 200, f"新路径异常：{r.text}"
            assert "counts" in r.json() and "paused" in r.json()
            # 光鸭分享会话状态端点保留（与分享中心路径同名前缀，不可误删）
            r = await c.get("/api/share/status", headers=h)
            assert r.status_code == 200
            assert "configured" in r.json(), f"光鸭 status 端点被误删：{r.json()}"

    asyncio.run(scenario())
