"""v0.9.6.1 回归：历史清理行识别、历史单集名不回刷、短后台改名复查。"""
import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from app import bot_listener, db as dbmod
from app import main, mirror, runtime_config
from app.db import SessionLocal, migrate
from app.main import TemplateInput
from app.models import (AppConfig, Bot, MessageTemplate, MirrorDir, MirrorFile,
                        MirrorMedia, MirrorRule, AuditLog)
from app.publisher import _template
from app.security import encrypt_secret

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
ZH_NAME = "大醉侠 第04集 2160p.WEB-DL.H.265.mkv"
EN_NAME = "The Drunk Hero 第04集 2160p.WEB-DL.H.265.mkv"
LEGACY_ERR = "B 库该目录已不存在，映射已清理，下轮同步自动重建"


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def session():
    migrate()
    with SessionLocal() as s:
        for t in (MirrorFile, MirrorDir, MirrorMedia, MirrorRule, AuditLog, Bot,
                  MessageTemplate):
            s.query(t).delete()
        s.commit()
        try:
            yield s
        finally:
            for t in (MirrorFile, MirrorDir, MirrorMedia, MirrorRule, AuditLog):
                s.query(t).delete()
            s.commit()


def _rule(s):
    r = MirrorRule(name="默认镜像", source_root_id="A", target_root_id="B", enabled=True)
    s.add(r)
    s.flush()
    return r


# ---- 1. 历史无标记 prune 行 ----
def test_legacy_pruned_error_recognized_and_clearable(session):
    r = _rule(session)
    d = MirrorDir(rule_id=r.id, source_dir_id="A_OLD", parent_target_id="B",
                  name_standard="OldShow", kind="work", status="missing",
                  error=LEGACY_ERR)
    session.add(d)
    session.commit()

    out = main.mirror_status(db=session)
    assert out["counts"]["dirs_pruned_zombie"] == 1
    assert out["counts"]["dirs_missing"] == 0
    assert all(x["name"] != "OldShow" for x in out["missing_dirs"])
    row = next(x for x in out["dirs"] if x["name"] == "OldShow")
    assert row["pruned"] is True

    cleared = main.mirror_mappings_clear_pruned(db=session)
    assert cleared["removed"] == 1
    session.expire_all()
    assert session.get(MirrorDir, d.id) is None


def test_backfill_pruned_dirs_adds_marker_once(session):
    r = _rule(session)
    d = MirrorDir(rule_id=r.id, source_dir_id="A_OLD2", parent_target_id="B",
                  name_standard="OldShow2", kind="work", status="missing",
                  error=LEGACY_ERR)
    session.add(d)
    session.commit()
    marker = "pruned_dir_backfill_v0961"
    session.query(AppConfig).filter(AppConfig.key == marker).delete()
    session.commit()

    assert dbmod._backfill_pruned_dirs(session) == 1
    session.expire_all()
    assert session.get(MirrorDir, d.id).error.startswith(mirror.PRUNED_DIR_MARK)
    assert dbmod._backfill_pruned_dirs(session) == 0


# ---- 2. 历史单集改名不回刷 ----
class FakeAPI:
    def __init__(self, files):
        self.files = files
        self.renamed = []

    async def list_files(self, parent_id="", page=0, page_size=50):
        return {"list": [dict(f) for f in self.files], "total": len(self.files)}

    async def rename_file(self, fid, name):
        self.renamed.append((fid, name))


def _seed_rename(session, *, rename_status="skipped_legacy", files=None):
    r = _rule(session)
    m = MirrorMedia(rule_id=r.id, work_key="work:drunk", title_standard="大醉侠 (2002)",
                    title_mirror="大·醉·侠 (2002)", target_dir_id="B_WORK",
                    english_title="The Drunk Hero", english_source="tmdb", status="synced")
    session.add(m)
    session.flush()
    f = MirrorFile(media_id=m.id, rule_id=r.id, identity="tmdb:1:tv:1:4",
                   source_file_id="F1", target_file_id="BF1", file_name=ZH_NAME,
                   size=1024, fingerprint="1024:1", status="synced",
                   rename_status=rename_status, target_name="")
    session.add(f)
    session.commit()
    api = FakeAPI(files if files is not None else [
        {"fileId": "BF1", "fileName": ZH_NAME, "fileSize": 1024, "resType": 1}])
    return r, m, f, api


def test_legacy_rename_rows_skipped_without_force(session):
    r, _m, _f, api = _seed_rename(session)
    with patch("app.mirror.GuangYaWebFiles", return_value=api), \
         patch("app.mirror.asyncio.sleep", new=AsyncMock()):
        out = _run(mirror.rename_mirror_files(session, r))
    assert out["skipped_legacy"] == 1
    assert api.renamed == []


def test_force_clears_legacy_skip_and_renames(session):
    r, _m, f, api = _seed_rename(session)
    with patch("app.mirror.GuangYaWebFiles", return_value=api), \
         patch("app.mirror.tmdb.english_title", new=AsyncMock(return_value="The Drunk Hero")), \
         patch("app.mirror.asyncio.sleep", new=AsyncMock()):
        out = _run(mirror.rename_mirror_files(session, r, force=True))
    assert out["renamed"] == 1, out
    assert api.renamed == [("BF1", EN_NAME)]
    session.expire_all()
    assert session.get(MirrorFile, f.id).rename_status == "done"


def test_partial_visibility_renames_visible_rows(session):
    """一集暂时不可见时，不再把整部剧全部卡成 pending_visible。"""
    r, m, f1, api = _seed_rename(session, rename_status="")
    f2 = MirrorFile(media_id=m.id, rule_id=r.id, identity="tmdb:1:tv:1:5",
                    source_file_id="F2", target_file_id="BF2",
                    file_name="大醉侠 第05集 2160p.mkv", size=2048,
                    fingerprint="2048:1", status="synced", rename_status="")
    session.add(f2)
    session.commit()
    with patch("app.mirror.GuangYaWebFiles", return_value=api), \
         patch("app.mirror.asyncio.sleep", new=AsyncMock()):
        out = _run(mirror.rename_mirror_files(session, r))
    assert out["renamed"] == 1 and out["waiting_visible"] == 1, out
    session.expire_all()
    assert session.get(MirrorFile, f1.id).rename_status == "done"
    assert session.get(MirrorFile, f2.id).rename_status == "pending_visible"


# ---- 3. 后台复查必须短、只看待办 ----
def test_background_rename_job_uses_short_budget(session):
    _seed_rename(session, rename_status="")  # 后台复查需要启用规则 + 待改名文件
    with patch("app.main._safe_rename", new=AsyncMock(return_value={"renamed": 0})) as mock:
        _run(main._job_rename_pending())
    assert mock.await_count == 1
    kw = mock.await_args.kwargs
    assert kw["only_pending"] is True
    assert kw["max_seconds"] <= 20
    assert kw["max_media"] <= 5


def test_frontend_exposes_legacy_and_zombie_hints():
    assert "历史不回刷" in APP
    assert "已清理待重建目录映射" in APP
    assert "clearPrunedMappings" in APP


def test_frontend_explicit_set_default_template_button():
    assert "async function setDefaultTpl" in APP
    assert "@click=\"setDefaultTpl(t)\"" in APP
    assert "只保存不设默认不会改变推送样式" in APP
    assert "id: d.id || 0" in APP  # 载入默认模板不再重复新建


def test_set_default_template_switches_publisher_template(session):
    t1 = MessageTemplate(name="旧默认", content="旧内容", enabled=True, is_default=True)
    t2 = MessageTemplate(name="精简卡片", content="新内容", enabled=True, is_default=False)
    session.add_all([t1, t2])
    session.commit()
    main.edit_template(t2.id, TemplateInput(name=t2.name, content=t2.content,
                                            enabled=True, is_default=True), db=session)
    session.expire_all()
    assert session.get(MessageTemplate, t1.id).is_default is False
    assert _template(session).id == t2.id


def test_clear_bot_token_keeps_routes_and_config(session):
    bot = Bot(name="Ya-TG Bot", token="encrypted-token", enabled=True)
    session.add(bot)
    session.flush()
    runtime_config.set_many(session, {
        "interaction_bot_id": str(bot.id),
        "push_routes_json": '[{"dir_id":"","kind":"channel","chat_id":"C1"}]'})
    session.commit()

    out = main.push_bot_token_clear(db=session)
    assert out["cleared"] is True
    session.expire_all()
    b = session.get(Bot, bot.id)
    assert b.token == "" and b.enabled is False
    assert runtime_config.get(session, "push_routes_json").find("C1") >= 0


def test_frontend_exposes_clear_bot_token_button():
    assert "async function clearPushBotToken" in APP
    assert "@click=\"clearPushBotToken\"" in APP
    assert "/push/bot/token/clear" in APP


def test_frontend_unified_save_covers_restrictions():
    assert "async function savePushRoutes" in APP
    assert "await postInteractionConfig()" in APP
    assert "await api('/reward/config'" in APP
    assert "保存分发与互动配置（Bot + 白名单 + 使用限制 + 打赏）" in APP
    assert "保存全部互动设置" not in APP
    assert "上方三块配置共用此按钮" in APP


def test_frontend_reward_moved_into_builtin_commands():
    assert "打赏（内置指令 /reward）" in APP
    assert "单独保存打赏" in APP
    assert "打赏与消息跳转配置" not in APP


def test_frontend_jump_links_moved_to_template_module():
    assert "推送消息 · 官方频道跳转" in APP
    assert "推送消息 · 交流群跳转" in APP
    assert "async function saveJumpLinks" in APP
    assert "留空则不显示按钮" in APP
    assert "&#123;&#123;channel_link&#125;&#125;" in APP
    assert "&#123;&#123;group_link&#125;&#125;" in APP


def test_private_chat_reply_keyboard_on_start():
    kb = bot_listener.main_reply_keyboard()
    rows = kb["keyboard"]
    assert [x for row in rows for x in row] == ["求片", "投稿", "搜索", "打赏"]
    assert kb["resize_keyboard"] is True and kb["one_time_keyboard"] is False
    src = (ROOT / "backend/app/bot_listener.py").read_text(encoding="utf-8")
    assert "reply_markup=main_reply_keyboard()" in src
    assert "/setMyCommands" not in src  # 红线：不调用 Telegram 底部指令栏注册接口


def test_broadcast_notify_sends_only_to_push_routes(session):
    bot = Bot(name="Ya-TG Bot", token=encrypt_secret("fake-token"), enabled=True)
    session.add(bot)
    session.flush()
    runtime_config.set_many(session, {
        "interaction_bot_id": str(bot.id),
        "push_routes_json": json.dumps([
            {"dir_id": "", "kind": "channel", "chat_id": "C1", "note": ""},
            {"dir_id": "D2", "kind": "group", "chat_id": "G2", "note": ""}])})
    session.commit()

    async def fake_call(token, method, payload, db=None):
        return {"http_status": 200, "data": {"ok": True, "result": {"message_id": "1"}}}

    with patch("app.main._telegram_call", new=AsyncMock(side_effect=fake_call)) as mock:
        out = _run(main.broadcast_notify(main.BroadcastInput(text="系统通知"), db=session))
    assert out["sent"] == 2, out
    chat_ids = [c.args[2]["chat_id"] for c in mock.await_args_list]
    assert chat_ids == ["C1", "G2"]  # 只发路由目标，不私聊用户


def test_frontend_broadcast_notice_replaces_private_test():
    assert "async function sendBroadcastNotice" in APP
    assert "/broadcast/notify" in APP
    assert "管理员自定义群发通知（仅频道 / 群，不私聊用户）" in APP
    assert "发送到全部推送目标" in APP
    assert "testPrivatePush" not in APP
    assert "testPrivateChatId" not in APP
