"""阶段四 B-10/B-12：CI 最小权限 + Docker 非 root 运行的静态护栏。

供应链与容器逃逸是运行时难测的风险点，用配置静态检查固化：任何把顶层权限
改回 write、去掉 public 开关、或移除非 root entrypoint 的改动都会被拦下。
"""
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def workflow():
    yaml = pytest.importorskip("yaml")
    return yaml.safe_load((ROOT / ".github/workflows/docker-publish.yml").read_text(encoding="utf-8"))


def test_top_level_permissions_read_only(workflow):
    assert workflow.get("permissions") == {"contents": "read"}, (
        "顶层 permissions 必须只读，写权限仅 release job 单独授予"
    )


def test_release_job_has_write_permission(workflow):
    assert workflow["jobs"]["release"].get("permissions") == {"contents": "write"}


def test_force_public_is_opt_in(workflow):
    steps = workflow["jobs"]["build-and-push"]["steps"]
    pub = [s for s in steps if "public" in s.get("name", "").lower()]
    assert pub, "保留可选公开步骤，但不默认强制公开"
    assert "DOCKERHUB_FORCE_PUBLIC" in (pub[0].get("if") or ""), (
        "强制公开必须显式 opt-in，避免私有仓库被意外公开"
    )


def test_dockerfile_non_root_entrypoint():
    src = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "gosu" in src, "Dockerfile 需 gosu 供 entrypoint 降权"
    assert "useradd -u 10001" in src, "需创建固定 UID 10001 的非特权用户"
    assert 'ENTRYPOINT ["docker-entrypoint.sh"]' in src, "需经 entrypoint 处理挂载卷属主后降权"
    assert "chown -R yatg:yatg /app/db /app/logs" in src, "需把 db/logs 授权给运行用户"


def test_entrypoint_drops_privileges():
    src = (ROOT / "scripts/docker-entrypoint.sh").read_text(encoding="utf-8")
    assert "chown -R yatg:yatg /app/db /app/logs" in src
    assert "exec gosu yatg" in src, "entrypoint 最终必须降权到非 root"
