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
