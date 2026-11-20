"""阶段四 A-09：后台 job / 启动迁移 / 扫描失败路径不得静默吞异常。

回归护栏：用 AST 扫描 backend/app 全部模块，任何 `except Exception` 的处理体
若只有 `db.rollback()`（可选 pass）而无任何日志/审计/上抛，视为回归。
"""
import ast
from pathlib import Path

import pytest


APP_DIR = Path(__file__).resolve().parents[1] / "backend" / "app"


def _silent_handlers(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    out = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ExceptHandler):
            continue
        calls = []
        for st in node.body:
            if isinstance(st, ast.Expr) and isinstance(st.value, ast.Call):
                f = st.value.func
                calls.append(getattr(f, "attr", None) or getattr(f, "id", None))
            elif isinstance(st, ast.Pass):
                calls.append("pass")
            else:
                calls.append(type(st).__name__)
        if calls and set(calls) <= {"rollback", "pass"} and "rollback" in calls:
            out.append(node.lineno)
    return out


@pytest.mark.parametrize("module", [
    "main.py", "db.py", "scanner.py", "contribution_restore.py",
    "publisher.py", "bot_listener.py", "mirror.py", "mirror_batch.py",
])
def test_no_silent_rollback(module):
    path = APP_DIR / module
    assert path.exists(), f"缺少模块 {module}"
    assert _silent_handlers(path) == [], (
        f"{module} 存在静默 rollback 的 except（行号 {_silent_handlers(path)}），"
        "A-09 要求所有静默吞异常点留日志"
    )


def test_target_sites_have_logging():
    """四个历史静默点必须各自带 logger 调用。"""
    expectations = {
        "main.py": ["[bot] 消息轮询异常", "[scan] 定时全量扫描异常"],
        "db.py": ["[db] 旧队列收敛失败"],
        "scanner.py": ["[scan] 写入失败状态失败"],
    }
    for module, needles in expectations.items():
        src = (APP_DIR / module).read_text(encoding="utf-8")
        for needle in needles:
            assert needle in src, f"{module} 缺少日志：{needle}"
