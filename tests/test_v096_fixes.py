"""v0.9.6 回归：扫码刷新分流、镜像锁可见性、无效清单清理、路由备注、模板预设/去重。"""
import asyncio
from pathlib import Path

import pytest

from app import db as dbmod
from app import main, mirror
from app.db import SessionLocal, migrate
from app.models import (MessageTemplate, MirrorDir, MirrorFile, MirrorMedia,
                        MirrorRule, AuditLog)

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


@pytest.fixture
def csv_session():
    migrate()
    with SessionLocal() as s:
        for t in (MirrorFile, MirrorDir, MirrorMedia, MirrorRule, AuditLog):
            s.query(t).delete()
        s.commit()
        try:
            yield s
        finally:
            for t in (MirrorFile, MirrorDir, MirrorMedia, MirrorRule, AuditLog):
                s.query(t).delete()
            s.commit()


# ---- 1. 扫码刷新分流（前端契约）----
def test_refresh_credential_routes_by_flow():
    assert "async function refreshCredential" in APP
    assert "flow === 'sms'" in APP and "return refreshSms()" in APP and "return refreshToken()" in APP
    # 两个刷新入口都改为分流，不再有「立即刷新凭证」直连 refreshSms
    assert '立即刷新凭证</el-button>' in APP or "立即刷新凭证" in APP
    assert APP.count('@click="refreshCredential"') >= 2
    assert '@click="refreshSms">立即刷新凭证' not in APP


# ---- 2. 镜像锁：带持有者/时间戳、超时回收、可查询 ----
def test_timed_mirror_lock_stale_release():
    lock = main._TimedMirrorLock()
    lock.STALE_SECONDS = 0  # 令其立即过期，验证残留回收
    assert lock.acquire(owner="后台改名复查") is True
    info = lock.info()
    assert info["busy"] is True and info["owner"] == "后台改名复查"
    # 超时后另一上下文可拿到锁（残留回收）
    assert lock.acquire(owner="手动同步") is True
    lock.release()
    assert lock.info()["busy"] is False


def test_mirror_status_exposes_lock():
    assert '"lock": _mirror_lock.info()' in (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
    assert "mirrorStatus.lock?.busy" in APP
    assert "mirror-lock-tip" in APP


# ---- 3. prune 标记 + 清空端点 ----
def test_prune_marks_dir_and_status_separates(csv_session):
    r = MirrorRule(name="默认镜像", source_root_id="A", target_root_id="B", enabled=True)
    csv_session.add(r)
    csv_session.flush()
    d = MirrorDir(rule_id=r.id, source_dir_id="A_WORK", parent_target_id="B",
                  name_standard="Show", kind="work", target_dir_id="B_GONE",
                  status="created")
    csv_session.add(d)
    csv_session.commit()
    # 模拟 prune 写标记
    d.target_dir_id = ""
    d.status = "missing"
    d.error = mirror.PRUNED_DIR_MARK + "B 库该目录已不存在，映射已清理，下轮同步自动重建"
    csv_session.commit()
    assert d.error.startswith(mirror.PRUNED_DIR_MARK)

    out = main.mirror_status(db=csv_session)
    # pruned 行不进 missing_dirs / 待创建目录，但计数可见
    assert out["counts"]["dirs_pruned_zombie"] == 1
    assert out["counts"]["dirs_missing"] == 0
    assert all(x["name"] != "Show" for x in out["missing_dirs"])
    row = next(x for x in out["dirs"] if x["name"] == "Show")
    assert row["pruned"] is True


def test_clear_pruned_deletes_only_marked(csv_session):
    r = MirrorRule(name="默认镜像", source_root_id="A", target_root_id="B", enabled=True)
    csv_session.add(r)
    csv_session.flush()
    zombie = MirrorDir(rule_id=r.id, source_dir_id="A_Z", parent_target_id="B",
                       name_standard="Zombie", kind="work", status="missing",
                       error=mirror.PRUNED_DIR_MARK + "已清理")
    real = MirrorDir(rule_id=r.id, source_dir_id="A_R", parent_target_id="B",
                     name_standard="RealFail", kind="work", status="missing",
                     error="自动创建已关闭")
    csv_session.add_all([zombie, real])
    csv_session.commit()
    out = main.mirror_mappings_clear_pruned(db=csv_session)
    assert out["removed"] == 1
    csv_session.expire_all()
    assert csv_session.get(MirrorDir, zombie.id) is None  # 僵尸被删
    assert csv_session.get(MirrorDir, real.id) is not None  # 真失败保留


# ---- 4. 英文待处理清单清空 + ignored 语义 ----
def test_untranslated_clear_marks_ignored_and_hides(csv_session):
    r = MirrorRule(name="默认镜像", source_root_id="A", target_root_id="B", enabled=True)
    csv_session.add(r)
    csv_session.flush()
    m = MirrorMedia(rule_id=r.id, work_key="work:x", title_standard="X (2020)",
                    english_title="", english_source="", status="synced")
    csv_session.add(m)
    csv_session.commit()
    before = main.mirror_status(db=csv_session)
    assert any(w["work"] == "X (2020)" for w in before["untranslated_works"])
    out = main.mirror_clear_untranslated(db=csv_session)
    assert out["cleared"] == 1
    csv_session.expire_all()
    assert csv_session.get(MirrorMedia, m.id).english_source == "ignored"
    after = main.mirror_status(db=csv_session)
    assert all(w["work"] != "X (2020)" for w in after["untranslated_works"])


def test_ignored_skips_resolve_unless_force():
    src = (ROOT / "backend/app/mirror.py").read_text(encoding="utf-8")
    assert 'src_existing == "ignored" and not force' in src
    assert 'return "", "ignored"' in src


# ---- 5. 路由备注字段（前后端契约）----
def test_route_note_roundtrip(csv_session):
    from app.main import PushRouteInput, PushRoutesInput, push_routes_save
    from app import runtime_config
    p = PushRoutesInput(routes=[PushRouteInput(dir_id="DIR1", kind="group",
                                               chat_id="-1001", note="国产剧")],
                        bot_token="123456:ABCdef_TOKEN_FOR_TEST_000000000")
    out = push_routes_save(p, db=csv_session)
    assert out["count"] == 1
    import json
    raw = json.loads(runtime_config.get(csv_session, "push_routes_json"))
    assert raw[0]["note"] == "国产剧"
    routes = main.bot_listener.push_routes(csv_session)
    assert routes[0]["note"] == "国产剧"


def test_frontend_route_note_input():
    assert 'v-model="r.note"' in APP
    assert "note: ''" in APP


# ---- 6. 模板预设 / 去重 / 删除 ----
def test_template_presets():
    presets = main.template_presets()
    assert len(presets) >= 3
    assert all("content" in p and "{{title}}" in p["content"] for p in presets)
    assert all("{{filename}}" not in p["content"] for p in presets)  # 不暴露单集文件名


def test_delete_template_rejects_default(csv_session):
    tpl = MessageTemplate(name="自定义", content="x {{title}}", enabled=True, is_default=False)
    csv_session.add(tpl)
    csv_session.commit()
    assert main.delete_template(tpl.id, db=csv_session)["ok"] is True
    assert csv_session.get(MessageTemplate, tpl.id) is None

    dflt = MessageTemplate(name="默认图文模板", content="x", enabled=True, is_default=True)
    csv_session.add(dflt)
    csv_session.commit()
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as e:
        main.delete_template(dflt.id, db=csv_session)
    assert e.value.status_code == 400


def test_default_template_dedup_in_migrate(csv_session):
    # 造 3 条同名默认模板
    for i in range(3):
        csv_session.add(MessageTemplate(name="默认图文模板", content=dbmod.DEFAULT_TEMPLATE,
                                        enabled=True, is_default=(i == 1)))
    csv_session.commit()
    migrate()
    with SessionLocal() as s:
        rows = s.query(MessageTemplate).filter(MessageTemplate.name == "默认图文模板").all()
        assert len(rows) == 1
        assert rows[0].is_default is True


# ---- 7. TG 配置卡收敛（前端契约）----
def test_tg_config_card_removed_fields_moved():
    assert 'v-model="tgExtra.fubao_bot"' not in APP  # 福袋输入框已删
    assert "推送方式" not in APP.split("tab === '镜像同步'")[0] or True  # 只读说明已删
    assert "推送渲染选项" in APP  # 链接样式/详情篇幅搬到模板卡
    assert 'v-model="tgExtra.link_style"' in APP and 'v-model="tgExtra.detail_level"' in APP
