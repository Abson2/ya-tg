"""C-01：SPA catch-all 路径穿越防护（阶段四安全项，动态验证发现后修复）。

审计期静态定级 C（Starlette 通常规范化 ..），隔离环境用真实 uvicorn +
`curl --path-as-is` 打未规范化原始路径，证实 /../、/..%2f、/%2e%2e 均能
读取静态目录外文件（HTTP 200 + 泄露内容），故升级为已确认并修复。

本文件覆盖修复函数 main.safe_static_file，并做 uvicorn 端到端验证（若环境可用）。
"""
import sys
import shutil
import tempfile
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
from app.main import safe_static_file  # noqa: E402


@pytest.fixture
def static_env():
    """隔离静态目录：static/ 内有 index.html + assets/，同级 db/ 放敏感文件。

    用 tempfile.mkdtemp(dir="/tmp") 而非 pytest tmp_path：后者在系统临时根建
    pytest-of-* 目录，沙箱下会触发 PermissionError/EEXIST（环境限制，非被测逻辑）。
    """
    root = Path(tempfile.mkdtemp(prefix="c01_test_", dir="/tmp"))
    static = root / "static"
    static.mkdir()
    (static / "index.html").write_text("SPA_INDEX", encoding="utf-8")
    (static / "assets").mkdir()
    (static / "assets" / "app.js").write_text("console.log(1)", encoding="utf-8")
    db = root / "db"
    db.mkdir()
    secret = db / "ya-tg.db"
    secret.write_text("SECRET_DB", encoding="utf-8")
    try:
        yield static, secret
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_traversal_payloads_are_rejected(static_env):
    """所有穿越 payload 一律返回 None，绝不落到 static 外文件。"""
    static, secret = static_env
    payloads = [
        "../db/ya-tg.db",
        "..%2fdb%2fya-tg.db",
        "%2e%2e/db/ya-tg.db",
        "%2e%2e%2fdb%2fya-tg.db",
        "./../db/ya-tg.db",
        "assets/../db/ya-tg.db",
        "assets/%2e%2e/db/ya-tg.db",
        "../../etc/passwd",
        "/etc/passwd",  # 绝对路径：pathlib 中 root / "/etc/passwd" 会覆盖 root
        "",  # 空路径
    ]
    for p in payloads:
        got = safe_static_file(p, static)
        assert got is None or (got != secret and static.resolve() in got.parents), p


def test_normal_static_files_still_served(static_env):
    """正常 SPA 文件（index.html / assets/app.js）仍可被服务，不误伤。"""
    static, _ = static_env
    # safe_static_file 返回 realpath（macOS /tmp -> /private/tmp），故比较 resolve 后路径
    assert safe_static_file("index.html", static) == (static / "index.html").resolve()
    assert safe_static_file("assets/app.js", static) == (static / "assets" / "app.js").resolve()
    assert safe_static_file("does-not-exist.js", static) is None


def test_symlink_escape_is_rejected(static_env):
    """静态目录内的符号链接指向外部文件时也必须拦截（realpath 后越界）。"""
    static, secret = static_env
    link = static / "leak.db"
    try:
        link.symlink_to(secret)
    except (OSError, NotImplementedError):
        pytest.skip("当前平台不支持符号链接")
    assert safe_static_file("leak.db", static) is None


def test_end_to_end_uvicorn_path_as_is(static_env):
    """真实 ASGI 栈端到端：未规范化原始路径不得泄露静态目录外文件。"""
    pytest.importorskip("uvicorn")
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.responses import FileResponse
    import uvicorn

    static, _ = static_env
    app = FastAPI()

    @app.get("/{path:path}")
    def spa(path: str):
        return FileResponse(safe_static_file(path, static) or static / "index.html")

    import socket

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()

    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    try:
        for _ in range(50):
            if server.started:
                break
            time.sleep(0.1)
        assert server.started

        import subprocess

        for raw in ["/../db/ya-tg.db", "/..%2fdb%2fya-tg.db",
                    "/%2e%2e/db/ya-tg.db", "/./../db/ya-tg.db"]:
            r = subprocess.run(
                ["curl", "-sS", "--path-as-is",
                 f"http://127.0.0.1:{port}{raw}"],
                capture_output=True, text=True, timeout=10,
            )
            assert "SECRET_DB" not in r.stdout, raw
            assert "SPA_INDEX" in r.stdout, raw
    finally:
        server.should_exit = True
        t.join(timeout=5)
