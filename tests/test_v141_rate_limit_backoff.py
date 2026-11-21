"""v1.4.1 验收：光鸭 code=127「操作过于频繁」改名限流根治。

真机现象（v1.4.0）：force 改名 15 成功 / 25 全因 code=127 失败 / 82 未轮到。
根因：默认 rename_idle_seconds=0.3 + rename_concurrency=3 ≈ 10 req/s 触发光鸭风控；
旧代码对 127 只有「连续失败 5 次熔断停一轮」，无专门退避、更无同集自动重试，
失败标 failed 后干等下一轮 120s job。

修复（本测试覆盖）：
  1. _is_rate_limit_error 精确识别 127（结构化 code + 文本兜底）；
  2. _rename_one_with_backoff 遇 127 指数退避 1/2/4/8/8 + 自动重试同集，成功即 done；
  3. 重试耗尽仍失败带 RENAME_RATE_LIMIT_MARK 标记；
  4. 127 不计入「连续失败熔断」，不误伤后续正常文件；
  5. 默认 idle 上调 0.5（风控余量），并发仍 3（不消极降吞吐）；
  6. 数值型配置明文（如 file_idle_seconds="2"）不再触发「凭证解密失败」告警；
  7. 版本号四处一致。
"""
import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")


# ---------- ① 127 识别 ----------

def test_is_rate_limit_error_by_code_and_text():
    from app import mirror, guangya

    # 结构化 code（guangya.py 已让 127 带 code 抛出）
    assert mirror._is_rate_limit_error(guangya.WebFileError("网页端 rename 失败", 127))
    # 文本兜底（兼容旧版 code=None 的存量错误对象）
    assert mirror._is_rate_limit_error(RuntimeError("网页端 rename 失败 code=127 msg=操作过于频繁"))
    assert mirror._is_rate_limit_error(Exception("code=127"))
    assert mirror._is_rate_limit_error(Exception("too many requests"))
    # 真实失败不得误判为限流
    assert not mirror._is_rate_limit_error(guangya.WebFileError("登录失效", 401))
    assert not mirror._is_rate_limit_error(RuntimeError("database is locked"))
    assert not mirror._is_rate_limit_error(RuntimeError("rename 429 风控"))


def test_post_web_attaches_code_for_127():
    """护栏：guangya._post_web 必须把 code=127 结构化带出，不能只留文本。"""
    import inspect
    from app import guangya
    src = inspect.getsource(guangya.GuangYaWebFiles._post_web)
    assert "code == 127" in src, "127 须显式抛出带 code 的 WebFileError"
    assert "WebFileError(last_error, code)" in src


# ---------- ② 退避重试 ----------

def test_rename_one_retries_127_then_succeeds():
    """遇 127 退避后重试同集成功：返回成功，且重试次数与退避序列正确。"""
    from app import mirror, guangya

    calls = {"n": 0}

    class API:
        async def rename_file(self, fid, final):
            calls["n"] += 1
            if calls["n"] <= 2:
                raise guangya.WebFileError("网页端 rename 失败 code=127 msg=操作过于频繁", 127)
            return None

    delays = []
    real_sleep = asyncio.sleep

    async def fake_sleep(d):
        delays.append(d)
        await real_sleep(0)

    with patch.object(mirror.asyncio, "sleep", fake_sleep):
        ok, err = asyncio.run(mirror._rename_one_with_backoff(API(), "F1", "Ep01"))

    assert ok is True and err == ""
    assert calls["n"] == 3, "两次 127 后第 3 次成功"
    assert delays == [1.0, 2.0], "指数退避 1→2 秒，成功即止"


def test_rename_one_backoff_is_capped():
    """退避上限 8 秒，防止单窗口阻塞过久。"""
    from app import mirror, guangya

    class API:
        async def rename_file(self, fid, final):
            raise guangya.WebFileError("操作过于频繁 code=127", 127)

    delays = []
    real_sleep = asyncio.sleep

    async def fake_sleep(d):
        delays.append(d)
        await real_sleep(0)

    with patch.object(mirror.asyncio, "sleep", fake_sleep):
        ok, err = asyncio.run(mirror._rename_one_with_backoff(API(), "F1", "Ep01"))

    assert ok is False
    assert err.startswith(mirror.RENAME_RATE_LIMIT_MARK)
    assert len(delays) == mirror.RENAME_RATE_LIMIT_MAX_RETRIES
    assert max(delays) == mirror.RENAME_RATE_LIMIT_BACKOFF_CAP
    assert delays == [1.0, 2.0, 4.0, 8.0, 8.0]


def test_rename_one_does_not_retry_real_failure():
    """真实失败立即返回，不重试、不加限流标记。"""
    from app import mirror

    calls = {"n": 0}

    class API:
        async def rename_file(self, fid, final):
            calls["n"] += 1
            raise RuntimeError("database is locked")

    ok, err = asyncio.run(mirror._rename_one_with_backoff(API(), "F1", "Ep01"))
    assert ok is False
    assert calls["n"] == 1, "真实失败不重试"
    assert mirror.RENAME_RATE_LIMIT_MARK not in err


# ---------- ③ 默认值与常量 ----------

def test_safe_defaults_and_constants():
    from app import mirror, runtime_config
    assert runtime_config.DEFAULTS["rename_idle_seconds"][1] == "0.5"
    assert runtime_config.DEFAULTS["rename_concurrency"][1] == "3"
    assert mirror.MIN_RENAME_IDLE_SECONDS == 0.2
    assert mirror.MAX_RENAME_CONCURRENCY == 8
    assert mirror.RENAME_RATE_LIMIT_MAX_RETRIES == 5
    assert mirror.RENAME_RATE_LIMIT_BACKOFF_BASE == 1.0
    assert mirror.RENAME_RATE_LIMIT_BACKOFF_CAP == 8.0


def test_no_retry_on_127_when_disabled_via_zero():
    """护栏：重试次数为 0 时 127 立即返回带标记，不 sleep（供主循环预算收紧场景）。"""
    from app import mirror, guangya

    calls = {"n": 0}

    class API:
        async def rename_file(self, fid, final):
            calls["n"] += 1
            raise guangya.WebFileError("code=127", 127)

    old = mirror.RENAME_RATE_LIMIT_MAX_RETRIES
    try:
        mirror.RENAME_RATE_LIMIT_MAX_RETRIES = 0
        ok, err = asyncio.run(mirror._rename_one_with_backoff(API(), "F1", "Ep01"))
    finally:
        mirror.RENAME_RATE_LIMIT_MAX_RETRIES = old
    assert ok is False and calls["n"] == 1
    assert err.startswith(mirror.RENAME_RATE_LIMIT_MARK)


# ---------- ④ 数值明文不告警 ----------

def test_plain_number_config_skips_decrypt_warning():
    """数值型配置明文（file_idle_seconds="2" 等）不得触发凭证解密失败告警。"""
    from app import security

    for raw in ("2", "0.5", "1.0", "-3", "  1.5  "):
        assert security.decrypt_secret(raw) == raw.strip()

    # 真正的非法密文（非纯数字、非 Fernet）仍按明文回退（兼容存量），且触发 warning
    with patch.object(security.logger, "warning") as w:
        assert security.decrypt_secret("gAAAAA_broken_ciphertext") == "gAAAAA_broken_ciphertext"
        assert w.called, "疑似损坏密文仍须告警"


# ---------- ⑤ 版本一致性 ----------

def test_version_is_141_everywhere():
    import json
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    main_src = (root / "backend/app/main.py").read_text(encoding="utf-8")
    assert 'version="2.0.2"' in main_src
    assert '"version": "2.0.2"' in main_src
    assert "v2.0.2 · Asia/Shanghai" in (root / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert json.loads((root / "frontend/package.json").read_text(encoding="utf-8"))["version"] == "2.0.2"
    assert "当前版本：v2.0.2" in (root / "README.md").read_text(encoding="utf-8")
