"""v2.1.12 #295 验收：rename 的 RPC 超时被误判为「鉴权失效」。

真机报障（2026-11-23）：
    网页端 /userres/v1/file/rename 失败
    msg=rpc timeout: timeout=6s, to=nd.svcuserres.s, method=CheckSafeBoxAccessToken

根因链（已实测）：
  1. `_post_web` 的鉴权启发式用**裸子串** "token" 判登录态失效，而上游方法名
     `CheckSafeBoxAccessToken` 里正好含 "token" → 一个**服务端内部 RPC 超时**
     被当成「掉登录」，直接 raise、零重试；
  2. 单次抖动因此算「真实失败」，计入 5 次连续失败熔断 → **整轮改名停摆**；
  3. 报错原文（上游服务名 + RPC 方法名 + 接口路径）直接透到页面，违反红线；
  4. 用户手动重试后看板数字不动 —— 那一轮压根没有一次改名成功。

本文件锁死修复后的四条契约：
  A. 鉴权判定收紧：只有业务码 401/116 与**明确**登录文案才算失效，
     `rpc timeout` 属于瞬时故障；
  B. 瞬时故障 → 退避重试，**耗尽时显式打标** `TRANSIENT_FAULT_MARK`
     （不再靠错误文案关键词猜，上游回 `internal error` 也能正确分类）；
  C. 瞬时故障**不计熔断、不计失败、不进失败清单**，文件退回「待处理」等下一轮；
  D. 页面展示层中性化：不出现服务名 / RPC 方法名 / 接口路径 / 业务码。
"""
import asyncio
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

A_ROOT, B_ROOT = "A_ROOT", "B_ROOT"
A_WORK, B_WORK = "A_WORK", "B_WORK"
WORK = "大醉侠 (2002) {tmdb-135646}"
ENG = "The Drunk Hero"

# 真机原文（逐字保留，作为分类器的回归样本）
REAL_MSG = ("rpc timeout: timeout=6s, to=nd.svcuserres.s, "
            "method=CheckSafeBoxAccessToken")
REAL_ERR = f"网页端 /userres/v1/file/rename 失败 code=None msg={REAL_MSG}"


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v2112t_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'd.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _noop_sleep():
    async def _sleep(*_a, **_kw):
        return None
    return _sleep


class FakeAPI:
    """假光鸭客户端。

    `mode`：
      - "transient"（默认）→ 一律抛瞬时故障（服务端 RPC 超时）；
      - "real"            → 抛非瞬时的真失败；
      - "ok"              → 改名成功并把 B 库文件真名改掉。
    `b_files` 的 fileName 必须与 DB 里的 file_name 一致，否则会被准备阶段的
    「B 库可见性」幂等闸判成 waiting_visible 直接跳过（不是本轮要测的东西）。
    """

    b_files: list = []
    rename_calls: list = []
    list_calls = 0
    payload: str = REAL_ERR
    mode = "transient"

    def __init__(self, db):
        self.db = db

    @classmethod
    def reset(cls, files=None, *, payload=REAL_ERR, mode="transient"):
        cls.b_files = [dict(f) for f in (files or [])]
        cls.rename_calls = []
        cls.list_calls = 0
        cls.payload = payload
        cls.mode = mode

    async def list_files(self, parent_id="", page=0, page_size=50):
        type(self).list_calls += 1
        if page == 0:
            return {"list": [dict(f) for f in self.b_files],
                    "total": len(self.b_files)}
        return {"list": [], "total": len(self.b_files)}

    async def rename_file(self, file_id, new_name):
        from app import guangya
        type(self).rename_calls.append((file_id, new_name))
        if type(self).mode == "ok":
            for f in type(self).b_files:
                if str(f.get("fileId")) == str(file_id):
                    f["fileName"] = new_name
            return None
        raise guangya.WebFileError(type(self).payload)


def _b_files(n, name_of):
    return [{"fileId": f"BF{i}", "fileName": name_of(i), "fileSize": 1024,
             "resType": 1} for i in range(1, n + 1)]


def _zh(i):
    return f"大醉侠 第0{i}集 2160p.WEB-DL.H.265.mkv"


def _seed(db, n=1, *, rename_status="", rename_error="", target_name=""):
    """建 1 部作品 + n 个已同步文件（默认 rename_status='' 即待处理）。"""
    from app.models import MirrorFile, MirrorMedia, MirrorRule
    from app.utils import normalize_work_name
    rule = MirrorRule(name="v2112t", source_root_id=A_ROOT, target_root_id=B_ROOT,
                      enabled=True, star_naming=True, auto_identify=False,
                      rename_files=True, file_idle_seconds=0.5, auto_create_dirs=True)
    db.add(rule)
    db.commit()
    media = MirrorMedia(rule_id=rule.id,
                        work_key="work:" + normalize_work_name(WORK),
                        title_standard=WORK, title_mirror="大·醉·侠 (2002) {tmdb-135646}",
                        source_dir_id=A_WORK, target_dir_id=B_WORK,
                        english_title=ENG, english_source="tmdb", status="synced")
    db.add(media)
    db.commit()
    rows = []
    for i in range(1, n + 1):
        rows.append(MirrorFile(
            media_id=media.id, rule_id=rule.id,
            identity=f"tmdb:135646:tv:1:{i}", source_file_id=f"AF{i}",
            target_file_id=f"BF{i}",
            file_name=f"大醉侠 第0{i}集 2160p.WEB-DL.H.265.mkv",
            target_name=target_name, size=1024, utime=1, fingerprint=f"{1024}:{i}",
            status="synced", rename_status=rename_status, rename_error=rename_error))
    db.add_all(rows)
    db.commit()
    return rule, media, rows


def _run_rename(db, rule):
    from app import mirror
    with patch("app.mirror.GuangYaWebFiles", FakeAPI), \
            patch("app.mirror.tmdb.english_title", new=AsyncMock(return_value="")), \
            patch("app.mirror.ai_assist.translate_title",
                  new=AsyncMock(return_value={"ok": False, "titles": {}, "queries": []})), \
            patch("app.mirror.asyncio.sleep", new=_noop_sleep()):
        return asyncio.run(mirror.rename_mirror_files(db, rule))


# ---------- A. 鉴权判定收紧 ----------

def test_rpc_timeout_is_transient_not_auth_expiry():
    """真机原文不得再被判成「登录态失效」。"""
    from app import guangya
    assert guangya.is_auth_expired_guangya(REAL_MSG) is False, \
        "上游方法名 CheckSafeBoxAccessToken 里的 token 不等于掉登录"
    assert guangya.is_transient_guangya_fault(REAL_MSG) is True
    # 反面：真正的登录失效文案仍要认出来（别把收紧做过头）
    assert guangya.is_auth_expired_guangya("未授权，请重新登录") is True
    assert guangya.is_auth_expired_guangya("invalid token") is True
    assert guangya.is_auth_expired_guangya("token expired") is True


class _FakeResp:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload, ensure_ascii=False)

    def json(self):
        return self._payload


def _web_client(payload, status_code=200):
    """绕开登录态初始化，构造一个只回固定响应的网页端客户端。"""
    from app import guangya
    obj = object.__new__(guangya.GuangYaWebFiles)
    obj.db = None
    obj.base = "https://example.invalid"
    obj._header_override = {"Authorization": "Bearer t"}
    calls = {"n": 0}

    class _Client:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def post(self, url, json=None, headers=None):
            calls["n"] += 1
            return _FakeResp(status_code, payload)

    return obj, _Client, calls


def test_post_web_marks_transient_when_retries_exhausted():
    """瞬时故障重试耗尽 → 显式打标，code 保持 None（不是鉴权失效）。"""
    from app import guangya
    obj, Client, calls = _web_client({"code": 500, "msg": REAL_MSG})
    with patch("app.guangya.httpx.AsyncClient", Client), \
            patch("app.guangya.asyncio.sleep", new=_noop_sleep()):
        with pytest.raises(guangya.WebFileError) as ei:
            asyncio.run(obj._post_web("/userres/v1/file/rename",
                                      {"fileId": "F1", "newName": "Ep01"}))
    assert str(ei.value).startswith(guangya.TRANSIENT_FAULT_MARK)
    assert ei.value.code is None, "瞬时故障不是鉴权失效，不能带 401/116"
    assert calls["n"] == 3, "内层应退避重试 3 次才放弃"


def test_post_web_keeps_auth_error_immediate():
    """真正的鉴权失效仍然立即抛、不重试（收紧不能改成放行）。"""
    from app import guangya
    obj, Client, calls = _web_client({"code": 116, "msg": "账号状态异常"})
    with patch("app.guangya.httpx.AsyncClient", Client), \
            patch("app.guangya.asyncio.sleep", new=_noop_sleep()):
        with pytest.raises(guangya.WebFileError) as ei:
            asyncio.run(obj._post_web("/userres/v1/file/rename",
                                      {"fileId": "F1", "newName": "Ep01"}))
    assert ei.value.code == 116
    assert not str(ei.value).startswith(guangya.TRANSIENT_FAULT_MARK)
    assert calls["n"] == 1


def test_post_web_real_failure_is_unmarked():
    """非瞬时的真失败不打标记 —— 否则真失败会被当成抖动无限重试。"""
    from app import guangya
    obj, Client, calls = _web_client({"code": 999, "msg": "参数错误"})
    with patch("app.guangya.httpx.AsyncClient", Client), \
            patch("app.guangya.asyncio.sleep", new=_noop_sleep()):
        with pytest.raises(guangya.WebFileError) as ei:
            asyncio.run(obj._post_web("/userres/v1/file/rename",
                                      {"fileId": "F1", "newName": "Ep01"}))
    assert not str(ei.value).startswith(guangya.TRANSIENT_FAULT_MARK)
    assert calls["n"] == 1, "真失败不重试"


# ---------- B. 单文件执行器：瞬时故障退避后打标 ----------

def test_rename_one_retries_transient_then_marks():
    """一次抖动要能自愈；仍失败则带标记返回，而不是当真实失败。"""
    from app import guangya, mirror
    calls = {"n": 0}

    class API:
        async def rename_file(self, fid, final):
            calls["n"] += 1
            raise guangya.WebFileError(REAL_ERR)

    delays = []
    real_sleep = asyncio.sleep

    async def fake_sleep(d):
        delays.append(d)
        await real_sleep(0)

    with patch.object(mirror.asyncio, "sleep", fake_sleep):
        ok, err = asyncio.run(mirror._rename_one_with_backoff(API(), "F1", "Ep01"))

    assert ok is False
    assert calls["n"] == 2, "1 次原请求 + 1 次退避重试"
    assert delays == [mirror.RENAME_TRANSIENT_BACKOFF_BASE]
    assert err.startswith(mirror.RENAME_TRANSIENT_MARK)
    # 三种标记互斥，绝不能串台（串台会让主循环走错分支）
    assert mirror.RENAME_RATE_LIMIT_MARK not in err
    assert mirror.RENAME_NAME_EXISTS_MARK not in err


def test_rename_one_transient_recovers_within_retry():
    """抖动只发生一次时，重试直接成功 —— 不必等下一轮。"""
    from app import guangya, mirror
    calls = {"n": 0}

    class API:
        async def rename_file(self, fid, final):
            calls["n"] += 1
            if calls["n"] == 1:
                raise guangya.WebFileError(REAL_ERR)
            return None

    with patch.object(mirror.asyncio, "sleep", _noop_sleep()):
        ok, err = asyncio.run(mirror._rename_one_with_backoff(API(), "F1", "Ep01"))
    assert ok is True and err == ""
    assert calls["n"] == 2


# ---------- C. 主循环：不计熔断、不计失败、退回待处理 ----------

def test_transient_does_not_trip_circuit_breaker(env):
    """6 个连续瞬时故障：不许熔断整轮，不许记成失败。"""
    S, _ = env
    from app import mirror
    from app.models import MirrorFile
    with S() as db:
        rule, _media, rows = _seed(db, n=6)
        FakeAPI.reset(_b_files(6, _zh))
        out = _run_rename(db, rule)

        assert out["transient"] == 6
        assert out["failed"] == 0, "瞬时故障不是改名失败"
        assert out["renamed"] == 0
        assert out["truncated_reason"] in ("", None), "不许因抖动熔断整轮"
        assert out["failed_works"] == [], "不得进失败作品清单（否则会被门禁拦推）"
        assert len(FakeAPI.rename_calls) == 12, "6 文件 × (1 原请求 + 1 退避重试)"
        # 用户可见文案：走 notices（正常进度提示），不是 errors（失败）
        assert out["errors"] == []
        assert any("临时不可用" in n for n in out["notices"])
        # 落库：退回「待处理」→ 下一轮 only_pending / rename_todo 三个集合都会选中
        db.expire_all()
        for r in rows:
            fresh = db.get(MirrorFile, r.id)
            assert fresh.rename_status == "", "退回待处理，下一轮自动重试"
            assert fresh.rename_error == mirror.RENAME_TRANSIENT_LABEL


def test_transient_falls_into_next_round_pending_sets(env):
    """退回的「待处理」必须真的被下一轮选中 —— 否则等于丢了。"""
    S, _ = env
    from sqlalchemy import select
    from app.models import MirrorFile
    from app.push_gate import _RENAME_OK_STATUS
    with S() as db:
        rule, _media, rows = _seed(db, n=2)
        FakeAPI.reset(_b_files(2, _zh))
        _run_rename(db, rule)
        db.expire_all()
        pend = list(db.scalars(select(MirrorFile).where(
            MirrorFile.status == "synced",
            MirrorFile.rename_status.in_(["", "pending_visible", "failed"]))))
        assert {r.id for r in pend} == {r.id for r in rows}
        # 推送门禁：'' 不在放行集合里 → 该作品本轮不会被推出去
        assert "" not in _RENAME_OK_STATUS


def test_real_failure_still_counts_as_failed(env):
    """回归保护：真失败（非瞬时）仍要计失败、仍要进失败清单。"""
    S, _ = env
    from app.models import MirrorFile
    with S() as db:
        rule, _media, rows = _seed(db, n=1)
        FakeAPI.reset(_b_files(1, _zh), mode="real",
                      payload="网页端 /userres/v1/file/rename 失败 "
                              "code=500 msg=存储节点拒绝写入")
        out = _run_rename(db, rule)
        assert out["transient"] == 0
        assert out["failed"] == 1
        assert len(out["errors"]) == 1
        db.expire_all()
        assert db.get(MirrorFile, rows[0].id).rename_status == "failed"


# ---------- D. 展示层中性化 ----------

def _assert_no_upstream_internals(text: str):
    low = (text or "").lower()
    for bad in ("nd.svcuserres.s", "checksafeboxaccesstoken", "/userres/v1",
                "svcuserres", "code=", "rpc timeout"):
        assert bad not in low, f"页面文案泄露上游内部标识：{bad} → {text}"


def test_transient_label_hides_upstream_internals():
    from app import mirror
    label = mirror.rename_error_label(f"{mirror.RENAME_TRANSIENT_MARK}{REAL_ERR}")
    assert label == mirror.RENAME_TRANSIENT_LABEL
    _assert_no_upstream_internals(label)


def test_transient_label_from_mark_only_text():
    """只带标记、不含任何关键词（上游只回 internal error）也要正确分类。"""
    from app import mirror
    label = mirror.rename_error_label(
        f"{mirror.RENAME_TRANSIENT_MARK}网页端 /userres/v1/file/rename 失败 code=None "
        "msg=internal error")
    assert label == mirror.RENAME_TRANSIENT_LABEL
    _assert_no_upstream_internals(label)


def test_fallback_label_never_leaks_upstream_internals():
    """兜底分支也不能把服务名 / 方法名 / 接口路径透出去。"""
    from app import mirror
    label = mirror.rename_error_label(
        "网页端 /userres/v1/file/rename 失败 code=None "
        "msg=upstream nd.svcuserres.s CheckSafeBoxAccessToken 返回异常")
    _assert_no_upstream_internals(label)


def test_rename_errors_payload_is_neutralized(env):
    """真失败时 stats['errors'] 会进 toast 与 last_message → 必须已中性化。"""
    S, _ = env
    with S() as db:
        rule, _media, _rows = _seed(db, n=1)
        FakeAPI.reset(_b_files(1, _zh), mode="real",
                      payload="网页端 /userres/v1/file/rename 失败 code=None "
                              "msg=internal error nd.svcuserres.s CheckSafeBoxAccessToken")
        out = _run_rename(db, rule)
        assert out["errors"], "真失败仍要有可追溯的提示"
        _assert_no_upstream_internals(out["errors"][0])


# ---------- E. 手动改名的数量要进看板 ----------

def test_manual_rename_writes_pipeline_snapshot(env):
    """手动「按英文规范改名」此前从不写快照 → 任务中心「本轮改名」永远停在上轮。"""
    S, _ = env
    from app import main
    with S() as db:
        rule, _media, _rows = _seed(db, n=1)
        FakeAPI.reset(_b_files(1, _zh), mode="ok")
        with patch("app.mirror.GuangYaWebFiles", FakeAPI), \
                patch("app.mirror.tmdb.english_title", new=AsyncMock(return_value="")), \
                patch("app.mirror.ai_assist.translate_title",
                      new=AsyncMock(return_value={"ok": False, "titles": {}, "queries": []})), \
                patch("app.mirror.asyncio.sleep", new=_noop_sleep()):
            out = asyncio.run(main._do_mirror_rename_files(False, db))
        assert out["renamed"] == 1
        db.expire_all()
        snap = json.loads(db.get(type(rule), rule.id).last_pipeline_json or "{}")
        assert snap.get("renamed") == 1
        assert snap.get("stage") == "manual_rename"
        assert snap.get("running") is False


def test_manual_rename_round_label_and_progress(env):
    """只改名的一轮：看板文字要说「本轮改名 N 项」，不能读成「本轮复制 0 项」。"""
    S, _ = env
    from app import main
    with S() as db:
        rule, _media, _rows = _seed(db, n=1)
        main._write_pipeline_snapshot(db, rule, mode="手动改名",
                                      stage="manual_rename", running=False, renamed=3)
        db.commit()
        st = main.mirror_status(db=db)
    assert st["progress"]["label"] == "本轮改名 3 项"
    assert st["progress"]["percent"] in (100.0, 99.0)
    pipe = {n["key"]: n for n in st["pipeline"]}
    assert pipe["rename"]["value"] == 3
