"""四模块重构 真机验证发现的回归：分享中心 API 路由契约。

背景：新增的分享中心状态端点原用 /api/share/status，与既有「光鸭分享会话状态」
端点路径冲突（FastAPI 先注册优先），前端拿到光鸭配置对象而非 {enabled,counts}。
本测试锁定两端点路径与返回结构，防复发。复用 conftest 全局 app（不改环境、不建新 engine）。
"""
import asyncio
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))


def test_share_center_status_route_contract():
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
            # 新端点：分享中心状态，必须是 {enabled, counts}
            r = await c.get("/api/share/center/status", headers=h)
            assert r.status_code == 200
            body = r.json()
            assert "enabled" in body and "counts" in body, f"路由冲突回归：{body}"
            assert body["enabled"] is False, "灰度默认应关闭"
            # 旧端点：光鸭分享会话状态，保持原结构不被覆盖
            r = await c.get("/api/share/status", headers=h)
            assert r.status_code == 200
            assert "configured" in r.json(), f"旧端点被覆盖：{r.json()}"
            # tasks 列表结构
            r = await c.get("/api/share/tasks", headers=h)
            assert r.status_code == 200
            assert "items" in r.json() and "enabled" in r.json()

    asyncio.run(scenario())
