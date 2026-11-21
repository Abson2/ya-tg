import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

# 测试环境隔离：在任何 app 模块导入前设置
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")
os.environ.setdefault("GUANGYA_SHARE_AUTHORIZATION", "fake-web-session")
os.environ.setdefault("GUANGYA_CLIENT_ID", "test-client")
_db_path = os.path.join(tempfile.gettempdir(), "guangya_pytest.db")
if os.path.exists(_db_path):
    os.remove(_db_path)
os.environ.setdefault("DATABASE_URL", f"sqlite:///{_db_path}")


import pytest


def ensure_full_day_hour_window(db, version_id: int) -> None:
    """把当前小时纳入可调度窗口，使 plan_engine 相关测试不依赖真实墙钟小时。

    背景：默认小时窗口模板是 8-24，落在窗口外时 plan_engine 整轮空转
    （这是**生产行为**：夜间不排期）。若测试直接断言「生成了任务/清单」，
    就会在 8-24 之外（例如 CI 的 UTC 夜间）随机失败——v2.1.1 全量跑时
    在上海时间 00:02 复现了 3 个 plan_engine 假红。

    做法：额外插入一条 0-24 的整日窗口（category_key 留空 = 不绑定分类，
    不影响分类轮转），让「当前小时是否允许排期」恒为真。
    生产代码不依赖此 helper。
    """
    from sqlalchemy import select
    from app.models import SCHourWindow
    exists = db.scalar(select(SCHourWindow.id).where(
        SCHourWindow.plan_version_id == version_id,
        SCHourWindow.start_hour == 0, SCHourWindow.end_hour == 24))
    if exists is None:
        db.add(SCHourWindow(plan_version_id=version_id, start_hour=0, end_hour=24,
                            category_key="", quota=0, enabled=True))
        db.commit()


@pytest.fixture(autouse=True)
def _clear_module_caches():
    """阶段二：识别缓存/内存态是模块级的，必须每用例清理，避免跨用例串扰假失败。

    与 bot_listener._poll_state / share_center._EVENT_FAIL_COUNT 的清理同理：
    任何新增的模块级内存态都要在这里登记清理，否则 patch 掉 tmdb.identify 的测试
    会命中上一个用例缓存的成功结果（v085 两个用例曾因此假失败）。
    """
    try:
        from app import main as _main
        _main._IDENTIFY_CACHE.clear()
    except Exception:
        pass
    yield
    try:
        from app import main as _main
        _main._IDENTIFY_CACHE.clear()
    except Exception:
        pass
