import os
import sys
import tempfile
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
