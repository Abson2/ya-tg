"""v0.9.19：用户投稿 → 光鸭原生转存（替代 v0.9.18 TGtoDrive 分发通道）。

覆盖：
  1 真实分享链接（含下划线 shareId）强校验与解析
  2 审核通过 → 调用光鸭转存 → 按名称+大小核对落库
  3 转存失败不改审核状态（审核态/转存态分离）
  4 重复点通过不重复建任务、已成功不重跑
  5 拒绝 / 关闭开关不建转存任务
  6 失败可手动重试；SHARE_SELF_RESTORE 明确不可重试提示
  7 后台 job 可续处理（pick_pending）
  8 删除投稿连带清理转存任务
  9 转存 API 请求体与光鸭技能真实契约一致（非猜测）
  10 前端已移除 TGtoDrive 分发 UI，新增投稿转存模块
"""
import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import select

from app import contribution_links as cl
from app import contribution_restore as cr
from app import main as main_mod
from app import runtime_config
from app.db import SessionLocal, migrate
from app.guangya import WebFileError
from app.models import AppConfig, ContributionRestore, UserContribution

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")

GY = "https://www.guangyapan.com/s/1970558251383148604_aey8CVbCzY9joZbY"
GY_SIMPLE = "https://www.guangyapan.com/s/7kqj"
TARGET = "1943905243358466080"
SHARED = [{"file_id": "f1", "name": "测试剧.mkv", "size": 12345, "is_dir": False}]
TARGET_HIT = [{"fileName": "测试剧.mkv", "fileSize": 12345, "isDir": False}]


class FakeGW:
    """假光鸭客户端：绕过真实登录/网络，保留可执行链路的调用层次。"""
    instances: list = []

    def __init__(self, db):
        self.db = db
        self.list_calls = 0
        self.restore_share_calls = 0
        FakeGW.instances.append(self)

    shared = SHARED
    target_rows = None      # None → 转存前空、转存后命中
    restore_error = None
    token_error = None
    token_code = None

    async def share_access_token(self, share_id, code=""):
        if self.token_error:
            raise WebFileError(self.token_error, self.token_code)
        return "AT"

    async def share_root_files(self, token):
        return self.shared

    async def restore_share_to_dir(self, share_url, target_dir_id, code=""):
        self.restore_share_calls += 1
        if self.restore_error:
            raise self.restore_error
        return {"raw": {"code": 0, "msg": "success"}}

    async def list_files(self, parent_id="", page=0, page_size=100):
        self.list_calls += 1
        rows = self.target_rows
        if rows is None:
            rows = TARGET_HIT if self.list_calls >= 2 else []
        return {"list": rows, "total": len(rows)}


CFG_KEYS = ("contribution_restore_enabled", "contribution_restore_target_dir_id")


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    FakeGW.instances.clear()
    FakeGW.shared = SHARED
    FakeGW.target_rows = None
    FakeGW.restore_error = None
    FakeGW.token_error = None
    FakeGW.token_code = None
    with SessionLocal() as db:
        for k in CFG_KEYS:
            row = db.get(AppConfig, k)
            if row:
                db.delete(row)
        db.query(ContributionRestore).delete()
        db.query(UserContribution).delete()
        db.commit()
    yield
    with SessionLocal() as db:
        db.query(ContributionRestore).delete()
        db.query(UserContribution).delete()
        db.commit()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as s:
        yield s


def _seed(db, *, status="pending", url=GY, title="测试剧"):
    c = UserContribution(title=title, tg_user_id="u1", tg_username="tester",
                         share_url=url, status=status, note="")
    db.add(c)
    db.commit()
    return c


def _cfg(db, *, enabled=True, target=TARGET):
    runtime_config.set_many(db, {
        "contribution_restore_enabled": "true" if enabled else "false",
        "contribution_restore_target_dir_id": target,
    })
    db.commit()


def _fake_gw():
    return patch.object(cr, "GuangYaWebFiles", FakeGW)


# ---------- 1 强校验 / 解析 ----------
def test_real_share_link_supported_and_parsed():
    assert cl.is_guangya_share_url(GY)          # 含下划线 shareId，旧正则会误拒
    assert cl.is_guangya_share_url(GY_SIMPLE)
    assert cl.is_guangya_share_url("https://m.guangyapan.com/s/xyz?code=abcd")
    assert not cl.is_guangya_share_url("https://www.guangyapan.com/folder/123")
    assert not cl.is_guangya_share_url("https://www.baidu.com/s/abc")
    sid, code = cl.parse_share_reference(GY)
    assert sid == "1970558251383148604_aey8CVbCzY9joZbY" and code == ""
    sid, code = cl.parse_share_reference("https://www.guangyapan.com/s/12345?code=7kqj")
    assert sid == "12345" and code == "7kqj"


def test_extract_link_rules():
    url, err = cl.extract_guangya_share_url(f"凡人修仙传 {GY}")
    assert url == GY and err == ""
    url, err = cl.extract_guangya_share_url("凡人修仙传")
    assert url == "" and err == ""
    url, err = cl.extract_guangya_share_url("https://t.me/foo?start=yg_x 加群")
    assert url == "" and "非光鸭" in err


# ---------- 2 审核通过 → 转存 + 核对 ----------
def test_approve_restores_and_verifies(db):
    c = _seed(db)
    _cfg(db)
    with _fake_gw():
        out = asyncio.run(main_mod.handle_contribution(
            c.id, main_mod.ContributionHandleInput(status="approved"), db))
    assert out["ok"] is True and out["restore_ok"] is True
    assert out["restore_status"] == "success"
    db.refresh(c)
    assert c.status == "approved"                 # 审核态不受转存影响
    row = cr.restore_for(db, c.id)
    assert row.status == "success" and row.attempts == 1
    assert "全部 1 项" in row.verify_note           # 必须按名称+大小核对
    assert row.restored_at is not None
    assert FakeGW.instances[0].restore_share_calls == 1


def test_approve_without_config_fails_but_keeps_status(db):
    c = _seed(db)
    _cfg(db, enabled=False)
    out = asyncio.run(main_mod.handle_contribution(
        c.id, main_mod.ContributionHandleInput(status="approved"), db))
    assert out["ok"] is True and out["restore_ok"] is False
    assert out["restore_status"] == "unconfigured"
    db.refresh(c)
    assert c.status == "approved"
    assert cr.restore_for(db, c.id) is None       # 未启用不建任务


# ---------- 3 转存失败不改审核态 ----------
def test_restore_failure_keeps_contribution_approved(db):
    c = _seed(db)
    _cfg(db)
    FakeGW.restore_error = WebFileError("网页端失败 code=999", 999)
    with _fake_gw():
        out = asyncio.run(main_mod.handle_contribution(
            c.id, main_mod.ContributionHandleInput(status="approved"), db))
    assert out["ok"] is True and out["restore_ok"] is False
    db.refresh(c)
    assert c.status == "approved"
    row = cr.restore_for(db, c.id)
    assert row.status == "failed" and "999" in row.error


def test_self_restore_is_explicit_non_retryable(db):
    c = _seed(db)
    _cfg(db)
    FakeGW.restore_error = WebFileError("不能转存当前账号创建的分享", 204)
    with _fake_gw():
        asyncio.run(main_mod.handle_contribution(
            c.id, main_mod.ContributionHandleInput(status="approved"), db))
    row = cr.restore_for(db, c.id)
    assert row.status == "failed"
    assert "SHARE_SELF_RESTORE" in row.error and "retryable=false" in row.error


def test_expired_share_gives_readable_error(db):
    c = _seed(db)
    _cfg(db)
    FakeGW.token_error = "分享已失效"
    FakeGW.token_code = 202
    with _fake_gw():
        asyncio.run(main_mod.handle_contribution(
            c.id, main_mod.ContributionHandleInput(status="approved"), db))
    row = cr.restore_for(db, c.id)
    assert row.status == "failed" and "分享已失效" in row.error


# ---------- 4 幂等 ----------
def test_repeat_approve_no_duplicate_restore(db):
    c = _seed(db)
    _cfg(db)
    with _fake_gw():
        asyncio.run(main_mod.handle_contribution(
            c.id, main_mod.ContributionHandleInput(status="approved"), db))
        row = cr.restore_for(db, c.id)
        assert row.status == "success" and row.attempts == 1
        asyncio.run(main_mod.handle_contribution(
            c.id, main_mod.ContributionHandleInput(status="approved"), db))
    rows = db.scalars(select(ContributionRestore).where(
        ContributionRestore.contribution_id == c.id)).all()
    assert len(rows) == 1
    db.refresh(row)
    assert row.attempts == 1                      # 已成功不自动重跑


# ---------- 5 拒绝不建任务 ----------
def test_reject_does_not_restore(db):
    c = _seed(db)
    _cfg(db)
    asyncio.run(main_mod.handle_contribution(
        c.id, main_mod.ContributionHandleInput(status="rejected", note="版权"), db))
    assert cr.restore_for(db, c.id) is None
    db.refresh(c)
    assert c.status == "rejected"


# ---------- 6 重试 ----------
def test_retry_failed_restore(db):
    c = _seed(db)
    _cfg(db)
    FakeGW.restore_error = WebFileError("临时失败", 999)
    with _fake_gw():
        asyncio.run(main_mod.handle_contribution(
            c.id, main_mod.ContributionHandleInput(status="approved"), db))
        row = cr.restore_for(db, c.id)
        assert row.status == "failed"
        FakeGW.restore_error = None
        FakeGW.target_rows = None                 # 转存前空、转存后命中
        FakeGW.instances[0].list_calls = 0
        ok, info, row2 = asyncio.run(cr.retry_restore(db, c.id))
    assert ok is True and row2.status == "success" and row2.attempts == 2


# ---------- 7 重启续处理 ----------
def test_pick_pending_for_restart(db):
    c = _seed(db)
    row, created = cr.ensure_restore(db, c.id)
    db.commit()
    assert created is True and row.status == "pending"
    assert any(r.contribution_id == c.id for r in cr.pick_pending(db))
    row.status = "success"
    db.commit()
    assert not any(r.contribution_id == c.id for r in cr.pick_pending(db))


# ---------- 8 删除连带清理 ----------
def test_delete_cleans_restore(db):
    c = _seed(db)
    cr.ensure_restore(db, c.id)
    db.commit()
    main_mod.delete_contribution(c.id, db=db)
    assert db.get(UserContribution, c.id) is None
    assert cr.restore_for(db, c.id) is None


# ---------- 9 真实 API 请求体契约（不猜测参数） ----------
def test_restore_api_payload_matches_real_contract(db):
    from app.guangya import GuangYaWebFiles

    gw = object.__new__(GuangYaWebFiles)
    gw._header_override = {"Authorization": "Bearer x", "did": "d", "dt": "4",
                           "Content-Type": "application/json"}
    gw.base = "https://api.guangyapan.com"
    captured = []

    def body_for(url):
        if url.endswith("/userres/v1/get_share_access_token"):
            return {"code": 0, "msg": "success", "data": {"accessToken": "AT"}}
        if url.endswith("/userres/v1/get_share_page_files_list"):
            return {"code": 0, "msg": "success", "data": {
                "list": [{"fileId": "f1", "fileName": "测试剧.mkv", "fileSize": 12345,
                          "isDir": False}], "hasMore": False}}
        if url.endswith("/userres/v1/restore_share"):
            return {"code": 0, "msg": "success", "data": {}}
        return {"code": 0, "msg": "success", "data": {}}

    class Resp:
        status_code = 200
        text = ""

        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    async def fake_post(self, url, **kw):
        captured.append((url, kw.get("json")))
        return Resp(body_for(url))

    with patch("httpx.AsyncClient.post", new=fake_post):
        result = asyncio.run(gw.restore_share_to_dir(GY, TARGET))

    token_call = [b for u, b in captured if u.endswith("get_share_access_token")][0]
    restore_call = [b for u, b in captured if u.endswith("restore_share")][0]
    assert token_call == {"shareId": "1970558251383148604_aey8CVbCzY9joZbY"}
    assert restore_call["accessToken"] == "AT"
    assert restore_call["fileIds"] == ["f1"]
    assert restore_call["parentId"] == TARGET
    assert "shareCode" in restore_call
    assert result["shared_files"][0]["name"] == "测试剧.mkv"


# ---------- 10 配置端点 / 前端静态 ----------
def test_config_roundtrip(db):
    p = main_mod.InteractionConfigInput(
        contribution_restore_enabled=True,
        contribution_restore_target_dir_id=TARGET)
    main_mod.interaction_config_save(p, db=db)
    out = main_mod.interaction_config_get(db=db)
    assert out["contribution_restore_enabled"] is True
    assert out["contribution_restore_target_dir_id"] == TARGET


def test_frontend_restore_module_replaces_dispatch():
    seg = APP.split("<h3>用户投稿</h3>", 1)[1].split("</el-table>", 1)[0]
    assert "转存状态" in seg
    assert "restoreStatusTag(s.row.restore_status)" in seg
    assert "retryContributionRestore(s.row)" in seg
    assert "showContributionRestore(s.row)" in seg
    assert "投稿转存配置" in APP
    assert "contribution_restore_enabled" in APP
    assert "contribution_restore_target_dir_id" in APP
    # 旧 TGtoDrive 分发通道 / 旧监控 Bot chat_id 彻底移除
    assert "contribution_dispatch_mode" not in APP
    assert "contribution_relay_chat_id" not in APP
    assert "tgto_contribute_bot_chat_id" not in APP
    assert "USER_BOT_TO_BOT_DISABLED" not in APP
