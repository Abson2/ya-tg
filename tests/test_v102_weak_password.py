"""阶段四 B-12：拒绝占位符/弱管理员口令。

.env.example 原写「请设置强密码」，照抄部署即有公开弱口令。
验证 check_admin / 登录端点对已知占位符返回 503，正常口令不受影响。
"""
import pytest
from fastapi import HTTPException

from app import main
from app.config import settings


@pytest.mark.parametrize("weak", [
    "CHANGE_ME", "change_me", "请设置强密码", "your_password",
    "admin123", "password", "123456", "admin", "test", "root",
    "  CHANGE_ME  ",
])
def test_weak_passwords_rejected(weak, monkeypatch):
    monkeypatch.setattr(settings, "admin_password", weak)
    with pytest.raises(HTTPException) as e:
        main.check_admin(x_admin_password=weak, x_admin_session=None)
    assert e.value.status_code == 503


def test_normal_password_accepted(monkeypatch):
    monkeypatch.setattr(settings, "admin_password", "Str0ng-P@ssw0rd-2026")
    # 明文通道正常比对通过（不进入会话分支）
    main.check_admin(x_admin_password="Str0ng-P@ssw0rd-2026", x_admin_session=None)


def test_env_example_uses_rejected_placeholder():
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    env = (root / ".env.example").read_text(encoding="utf-8")
    assert "ADMIN_PASSWORD=CHANGE_ME" in env
