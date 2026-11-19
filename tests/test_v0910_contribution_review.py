"""v0.9.10 投稿审核模块：真实状态展示 + 删除按钮。

真机反馈：投稿列表状态列直接显示英文原始值（approved），且没有删除按钮
（求片已有）。修复：后端加 DELETE 端点；前端状态列改中文标签带颜色，操作列加删除。
"""
from pathlib import Path

import pytest

from app import main
from app.db import SessionLocal, migrate
from app.models import UserContribution

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as s:
        s.query(UserContribution).delete()
        s.commit()
    yield
    with SessionLocal() as s:
        s.query(UserContribution).delete()
        s.commit()


def test_delete_contribution_removes_row():
    with SessionLocal() as s:
        c = UserContribution(title="投稿件", tg_user_id="u1", status="approved")
        s.add(c); s.commit(); cid = c.id
    with SessionLocal() as s:
        out = main.delete_contribution(cid, db=s)
    assert out["ok"] is True
    with SessionLocal() as s:
        assert s.get(UserContribution, cid) is None


def test_delete_contribution_missing_returns_404():
    from fastapi import HTTPException
    with SessionLocal() as s:
        with pytest.raises(HTTPException) as e:
            main.delete_contribution(999999, db=s)
    assert e.value.status_code == 404


def test_frontend_status_label_and_delete_button():
    # 用户投稿表格段：状态列不再用 prop="status" 显示英文，改中文标签函数
    seg = APP.split("<h3>用户投稿</h3>", 1)[1].split("</el-table>", 1)[0]
    assert 'prop="status"' not in seg
    assert "contributionStatusLabel(s.row.status)" in seg
    assert "function contributionStatusLabel" in APP
    assert "已通过" in APP and "已转存" in APP and "已拒绝" in APP and "待处理" in APP
    assert "contributionStatusTag(s.row.status)" in APP
    # 删除按钮 + 端点调用存在
    assert "deleteContribution(s.row)" in APP
    assert "`/contributions/${row.id}`" in APP
    assert "method: 'DELETE'" in APP
    # 投稿标题即资源链接：有 share_url 时剧名渲染为可点击超链接，且带协议白名单
    assert "function safeHttpUrl" in APP
    assert "title-link" in seg
    assert ':href="safeHttpUrl(s.row.share_url)"' in seg
    assert "target=\"_blank\"" in seg
