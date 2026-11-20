"""v0.9.16 #277-#279 三项真机反馈回归。

锁死以下红线：
  - #277：非指令/非会话态内容（含 startgroup 授权链接、任意裸链接、裸文字）不得登记投稿，
          统一回「未识别指令」纠正提示；仅 /投稿、Reply 会话态、TMDB 候选补链接三条路径可投稿。
  - #278：getMe 成功后自动回写 interaction_bot_username；前端初次加载同步 bot 下拉；
          copyText 带 execCommand fallback（HTTP 局域网可复制）。
  - #279：发现台账为空时，把白名单/目录路由里已配置的真实目标补进列表；刷新返回诊断字段，
          不再用「新增发现 0」误导用户反复点刷新。不虚构任何扫描数量。
"""
import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from app import bot_listener, runtime_config
from app.db import SessionLocal, migrate
from app.models import (AppConfig, Bot, Channel, DiscoveredChat, MediaRequest,
                        UserContribution)

TABLES = (UserContribution, MediaRequest, DiscoveredChat, Channel, Bot)
CFG_KEYS = ("contribution_enabled", "interaction_bot_id", "interaction_bot_username",
            "push_routes_json", "bot_push_channels_json", "bot_push_groups_json",
            "contribution_bot_token", "tgto_rule_resolution", "tgto_rule_effect",
            "tgto_rule_required_keyword")

CANDIDATES = [{"tmdb_id": 261315, "title": "十万狂花入梦来", "year": "2026",
               "media_type": "tv", "vote_average": 9.0}]


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    bot_listener._flow_sessions.clear()
    bot_listener._pick_sessions.clear()
    with SessionLocal() as db:
        for k in CFG_KEYS:
            row = db.get(AppConfig, k)
            if row:
                db.delete(row)
        for t in TABLES:
            db.query(t).delete()
        db.commit()
    yield
    bot_listener._flow_sessions.clear()
    bot_listener._pick_sessions.clear()
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        db.commit()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as s:
        yield s


def _fake_search(cands):
    async def fake(*a, **kw):
        return list(cands)
    return fake


# ---------------- #277 非指令内容不投稿 ----------------
def test_bare_startgroup_link_not_registered(db):
    """用户误发的 startgroup 授权链接不得被登记为投稿。"""
    before = db.query(UserContribution).count()
    action, reply = asyncio.run(bot_listener.handle_text(
        db, "https://t.me/yyds_embty_bot?startgroup=yg_abc123_XyZ", tg_user_id="u1"))
    assert action == "unrecognized"
    assert "未识别为指令" in reply
    assert db.query(UserContribution).count() == before


def test_bare_share_link_not_registered(db):
    """裸光鸭分享链接在非会话态下也不得自动投稿。"""
    before = db.query(UserContribution).count()
    action, _ = asyncio.run(bot_listener.handle_text(
        db, "https://www.alipan.com/s/abc123", tg_user_id="u2"))
    assert action == "unrecognized"
    assert db.query(UserContribution).count() == before


def test_bare_text_not_registered(db):
    action, reply = asyncio.run(bot_listener.handle_text(db, "随便一句话", tg_user_id="u3"))
    assert action == "unrecognized"
    assert "未识别为指令" in reply


def test_unrecognized_hint_has_menu_guidance():
    h = bot_listener._UNRECOGNIZED_HINT
    assert "/求片" in h and "/投稿" in h and "/搜索" in h and "结束" in h


def test_session_contribute_still_works(db):
    """点「投稿」进入会话态后，输入剧名仍走候选，不被纠正提示拦截。"""
    bot_listener._flow_set("u4", bot_listener.FLOW_CONTRIB)
    with patch.object(bot_listener, "search_candidates", new=_fake_search(CANDIDATES)):
        action, _ = asyncio.run(bot_listener.handle_text(db, "十万狂花入梦来", tg_user_id="u4"))
    assert action == "contribute_pick"


def test_picked_then_link_still_registers(db):
    """TMDB 候选点选后补链接仍是合法投稿路径。"""
    sess = {"flow": bot_listener.FLOW_CONTRIB, "query": "x", "candidates": CANDIDATES,
            "secret": "", "created": time.time(), "picked": {
                "title": "十万狂花入梦来", "tmdb_id": 261315, "media_type": "tv", "year": "2026"}}
    bot_listener._pick_sessions["u5"] = sess
    action, reply = asyncio.run(bot_listener.handle_text(
        db, "https://www.guangyapan.com/s/xyz789", tg_user_id="u5"))
    assert action == "contribute" and "登记" in reply
    db.flush()
    row = db.query(UserContribution).order_by(UserContribution.id.desc()).first()
    assert row.tmdb_id == 261315


# ---------------- #278 Bot 用户名自动回写 ----------------
def test_invite_link_writes_back_username(db):
    """getMe 成功 → invite-link 端点把 username 写回配置。"""
    from app import main
    b = Bot(name="Ya-TG Bot", token="enc-fake", enabled=True)
    db.add(b)
    db.flush()
    runtime_config.set_many(db, {"interaction_bot_id": str(b.id)})

    async def fake_get_me(d, token):
        return "yyds_embty_bot", ""

    with patch.object(bot_listener, "get_bot_username", new=fake_get_me), \
         patch.object(bot_listener, "auto_clear_stale_commands", new=AsyncMock(return_value=False)):
        out = asyncio.run(main.discover_invite_link(main.DiscoverInviteInput(), db))

    assert out["ok"] is True and out["bot_username"] == "yyds_embty_bot"
    assert "startgroup=y" in out["startgroup_url"]
    assert (runtime_config.get(db, "interaction_bot_username") or "") == "yyds_embty_bot"


def test_get_bot_username_strips_at():
    async def fake_api(d, t, method, payload):
        return {"ok": True, "result": {"username": "@yyds_embty_bot"}}
    with patch.object(bot_listener, "_api", new=fake_api):
        name, err = asyncio.run(bot_listener.get_bot_username(None, "tok"))
    assert name == "yyds_embty_bot" and err == ""


def test_copy_fallback_source_present():
    """前端 copyText 必须带 execCommand fallback 与 clipboard 兼容分支。"""
    import pathlib
    root = pathlib.Path(__file__).resolve().parents[1]
    src = (root / "frontend" / "src" / "App.vue").read_text(encoding="utf-8")
    assert "document.execCommand('copy')" in src
    assert "navigator.clipboard.writeText" in src
    assert "syncDiscBots()" in src


# ---------------- #279 白名单目标补进发现列表 ----------------
def test_whitelist_configured_chats_from_channels(db):
    from app import main
    b = Bot(name="b", token="x", enabled=True)
    db.add(b); db.flush()
    db.add(Channel(bot_id=b.id, name="影视交流群", chat_id="-1001234567890",
                   enabled=True, kind="group"))
    db.add(Channel(bot_id=b.id, name="官方频道", chat_id="-1009999999999",
                   enabled=True, kind="channel"))
    db.commit()
    rows = main._whitelist_configured_chats(db)
    ids = {r[1] for r in rows}
    assert "-1001234567890" in ids and "-1009999999999" in ids


def test_discover_chats_merges_whitelist_when_pool_empty(db):
    """发现台账为空时，白名单里已配置的目标落库为真实记录（有 id），可后续操作。"""
    from app import main
    b = Bot(name="b", token="x", enabled=True)
    db.add(b); db.flush()
    runtime_config.set_many(db, {"interaction_bot_id": str(b.id),
                                 "push_routes_json": '[{"dir_id":"","targets":['
                                                     '{"kind":"channel","chat_id":"-1008888888","name":"route-group:-1008888888"}]}]'})
    out = main.discover_chats(db)
    chats = out["chats"]
    assert any(c["chat_id"] == "-1008888888" for c in chats)
    merged = [c for c in chats if c["chat_id"] == "-1008888888"][0]
    assert merged["source"] == "whitelist" and merged["in_whitelist"] is True
    # 真机 bug：虚拟 id=0 导致点「检查权限/测试推送/删除」报「目标不存在」
    assert isinstance(merged["id"], int) and merged["id"] > 0
    assert db.get(DiscoveredChat, merged["id"]) is not None
    # 目录路由占位名 route-group:<id> 不当展示标题
    assert not str(merged["title"]).startswith("route-")


def test_whitelist_merge_is_idempotent(db):
    """重复发现不再重复落库（bot_id+chat_id 唯一），标题不覆盖真实名。"""
    from app import main
    b = Bot(name="b", token="x", enabled=True)
    db.add(b); db.flush()
    db.add(Channel(bot_id=b.id, name="官方频道", chat_id="-1007777777",
                   enabled=True, kind="channel"))
    db.commit()
    first = main.discover_chats(db)
    id1 = [c for c in first["chats"] if c["chat_id"] == "-1007777777"][0]["id"]
    second = main.discover_chats(db)
    matches = [c for c in second["chats"] if c["chat_id"] == "-1007777777"]
    assert len(matches) == 1 and matches[0]["id"] == id1


def test_discover_refresh_returns_diagnosis(db):
    """无新发现时不虚构数量，返回诚实诊断。"""
    from app import main
    runtime_config.set_many(db, {"contribution_enabled": "false"})
    out = asyncio.run(main.discover_refresh(main.DiscoverRefreshInput(), db))
    assert out["ok"] is True
    assert out["diagnosis"], "应返回诊断说明，而非静默的'新增发现 0'"


# ---------------- v0.9.17 成员状态/权限一致性 ----------------
def test_check_permission_returns_member_status():
    """check_chat_permission 返回真实 ChatMember.status，供成员状态列同步。"""
    from app import bot_listener as bl
    async def fake_left(d, token, method, payload):
        return {"ok": True, "result": {"status": "left"}}
    with patch.object(bl, "_api", new=fake_left):
        res = asyncio.run(bl.check_chat_permission(None, "123:AA", "-1001", "group"))
    assert res["status"] == "not_member" and res["member_status"] == "left"


def test_discovered_row_reconciles_member_status():
    """权限已判 not_member 时，成员状态展示为已退出，避免两列矛盾（不改库）。"""
    from app import main
    d = DiscoveredChat(bot_id=1, chat_id="-1001", chat_type="supergroup",
                       title="测试群", source="auto",
                       member_status="member", perm_status="not_member")
    row = main._discovered_row(d)
    assert row["member_status"] == "left" and row["member_label"] == "已退出"
    assert row["active"] is False
    # 原始持久状态未被改动
    assert d.member_status == "member"


def test_discovered_row_keeps_real_left_status():
    """已经是 left 的不被误改。"""
    from app import main
    d = DiscoveredChat(bot_id=1, chat_id="-1002", chat_type="supergroup",
                       member_status="left", perm_status="not_member")
    row = main._discovered_row(d)
    assert row["member_status"] == "left"


# ---------------- v0.9.17 白名单开关弹回（路由满额） ----------------
def test_whitelist_toggle_no_rebound_when_routes_full(db):
    """路由满 10 条时，从发现页加入白名单并入通配路由，重载后不弹回。"""
    import json
    from app import main
    b = Bot(name="b", token="x", enabled=True); db.add(b); db.flush()
    routes = [{"dir_id": "", "channel_id": "-1000", "group_id": "",
               "targets": [{"kind": "channel", "chat_id": "-1000", "name": "全局频道"}], "note": ""}]
    routes += [{"dir_id": f"dir{i}", "channel_id": f"-100{i}", "group_id": "",
                "targets": [{"kind": "channel", "chat_id": f"-100{i}", "name": f"c{i}"}], "note": ""}
               for i in range(1, 10)]
    runtime_config.set_many(db, {"interaction_bot_id": str(b.id),
                                 "push_routes_json": json.dumps(routes)})
    d = DiscoveredChat(bot_id=b.id, chat_id="-1003702445011", chat_type="supergroup",
                       title="光鸭云盘资源分享群", source="auto", in_whitelist=False)
    db.add(d); db.commit()
    r = main.discover_set_whitelist(d.id, main.DiscoverWhitelistInput(in_whitelist=True), db)
    assert r["ok"] is True and r["route_count"] == 10  # 并入，不新增第 11 条
    out = main.discover_chats(db)
    row = [x for x in out["chats"] if x["id"] == d.id][0]
    assert row["in_whitelist"] is True  # 重载后仍开，不弹回


def test_whitelist_toggle_rejects_when_no_wildcard_and_full(db):
    """无通配路由且已满 10 条时明确拒绝，不静默写入被截断。"""
    import json
    from fastapi import HTTPException
    from app import main
    b = Bot(name="b", token="x", enabled=True); db.add(b); db.flush()
    routes = [{"dir_id": f"dir{i}", "channel_id": f"-100{i}", "group_id": "",
               "targets": [{"kind": "channel", "chat_id": f"-100{i}", "name": f"c{i}"}], "note": ""}
              for i in range(10)]
    runtime_config.set_many(db, {"interaction_bot_id": str(b.id),
                                 "push_routes_json": json.dumps(routes)})
    d = DiscoveredChat(bot_id=b.id, chat_id="-1003702445011", chat_type="supergroup",
                       title="群", source="auto", in_whitelist=False)
    db.add(d); db.commit()
    with pytest.raises(HTTPException) as ei:
        main.discover_set_whitelist(d.id, main.DiscoverWhitelistInput(in_whitelist=True), db)
    assert ei.value.status_code == 400 and "上限" in ei.value.detail


# ---------------- v0.9.19 投稿转存失败诊断（不再静默吞） ----------------
def test_handle_contribution_records_restore_failure(db):
    """v0.9.19：光鸭转存异常也不影响审核通过，原因如实回显（不写投稿 note）。"""
    from app import main
    from app import contribution_restore as cr_mod

    runtime_config.set_many(db, {"contribution_restore_enabled": "true",
                                 "contribution_restore_target_dir_id": "1943905243358466080"})
    db.commit()
    c = UserContribution(tg_user_id="u1", title="剧",
                         share_url="https://www.guangyapan.com/s/x",
                         status="pending", note="")
    db.add(c); db.commit()
    async def boom(*a, **kw):
        raise RuntimeError("模拟转存异常")
    with patch.object(cr_mod, "run_restore_for_contribution", new=boom), \
         patch.object(bot_listener, "notify_user", new=AsyncMock(return_value=False)):
        out = asyncio.run(main.handle_contribution(
            c.id, main.ContributionHandleInput(status="approved", note=""), db))
    assert out["ok"] is True and out["restore_ok"] is False
    assert "转存执行异常" in out["restore_result"]
    assert db.get(UserContribution, c.id).status == "approved"  # 转存失败不改审核态


# ---------------- v0.9.17 #280 求片审核参数传入 TgtoDrive ----------------
def test_tmdb_meta_fills_year_for_command_want(db):
    """命令式 /求片 不带年份时，_tmdb_meta 从 TMDB 详情补 year。"""
    from app import tgto
    import app.tmdb as tmdb_mod
    async def fake_detail(tid, mtype, db_=None):
        return {"year": "2026", "poster_path": "/p.jpg", "overview": "简介",
                "original_title": "OT", "backdrop_path": ""}
    with patch.object(tmdb_mod, "configured", return_value=True), \
         patch.object(tmdb_mod, "detail", new=fake_detail):
        meta = asyncio.run(tgto._tmdb_meta(db, 297253, "tv"))
    assert meta["year"] == "2026" and meta["poster_url"]


def test_upsert_keyword_uses_tmdb_year_and_reports_applied(db):
    """year 入参为空时用 TMDB 补全，且回执带出真正写入的参数快照。"""
    from app import tgto
    runtime_config.set_many(db, {"tgto_rule_resolution": "4k", "tgto_rule_effect": "all",
                                 "tgto_rule_required_keyword": "265"})
    store = {"current": {"key": "ENV_FILTER_GUANGYA", "rules": []},
             "scenes": {"ENV_FILTER_GUANGYA": {"key": "ENV_FILTER_GUANGYA", "rules": []}}}
    captured = {}
    async def fake_fetch(db_):
        return store, "ENV_FILTER_GUANGYA"
    async def fake_save(db_, data, scene, obj):
        captured["rules"] = list(obj["rules"])
    async def fake_meta(db_, tmdb_id, media_type):
        return {"year": "2026", "original_title": "", "overview": "", "poster_path": "",
                "poster_url": "", "backdrop_path": "", "backdrop_url": ""}
    with patch.object(tgto, "fetch_config", fake_fetch), \
         patch.object(tgto, "save_config", fake_save), \
         patch.object(tgto, "_tmdb_meta", new=fake_meta):
        out = asyncio.run(tgto.upsert_keyword(db, title="伟大的长征", tmdb_id=297253))
    assert out["ok"] and out["status"] == "created"
    assert captured["rules"][0]["year"] == "2026"
    assert captured["rules"][0]["include_year"] is True
    assert out["applied"]["year"] == "2026" and out["applied"]["resolution"] == "4k"


def test_media_request_conflict_is_visible(db):
    """采纳遇他人规则 id 冲突时，回执 sync_ok=False + 真实原因，不再伪装成功。"""
    from app import main
    from app import tgto
    req = MediaRequest(title="冲突剧", tmdb_id=666, media_type="tv", year="2025",
                       keyword_id=tgto.keyword_id(666, 1, "冲突剧"),
                       status="pending", review_status="pending")
    db.add(req); db.commit()
    async def conflict(db_, **kw):
        return {"ok": False, "status": "conflict", "rule_id": "kw_666_1",
                "message": "该规则 id 已存在且不是本项目创建",
                "detail": "TgtoDrive 现有规则 source=tmdb，为防覆盖他人规则未写入",
                "applied": {}}
    with patch.object(tgto, "upsert_keyword", conflict), \
         patch.object(bot_listener, "notify_user", new=AsyncMock(return_value=False)):
        out = asyncio.run(main.handle_media_request(
            req.id, main.MediaRequestHandleInput(review_status="adopted"), db))
    assert out["ok"] is True and out["sync_ok"] is False
    assert "conflict" in out["sync"] and "source=tmdb" in out["sync_note"]
    row = db.get(MediaRequest, req.id)
    assert row.status == "failed" and "source=tmdb" in (row.note or "")


# ---------------- v0.9.17 #281 投稿去重合并计数 ----------------
def test_contribution_dedup_key_logic(db):
    """去重键：优先 tmdb:ID:类型，无 TMDB 回退规范化标题。"""
    from app import bot_listener as bl
    k1 = bl.contribution_dedup_key(tmdb_id=123, media_type="tv", title="任意名", share_url="")
    k2 = bl.contribution_dedup_key(tmdb_id=123, media_type="tv", title="另一个名", share_url="")
    assert k1 == k2 == "tmdb:123:tv"
    a = bl.contribution_dedup_key(tmdb_id=None, media_type="", title="伟大的 长征！", share_url="")
    b = bl.contribution_dedup_key(tmdb_id=None, media_type="", title="伟大 的长征", share_url="")
    assert a == b and a.startswith("title:")


def test_upsert_contribution_merges_duplicate_and_counts(db):
    """同作品重复投稿只保留一行、累加计数、保留 pending、刷新最新链接。"""
    from app import bot_listener as bl
    row1, new1 = bl.upsert_contribution(
        db, tg_user_id="u1", tg_username="alice", title="同一部剧", content="", 
        share_url="https://s/one", tmdb_id=900, media_type="tv")
    row2, new2 = bl.upsert_contribution(
        db, tg_user_id="u2", tg_username="bob", title="同一部剧别称", content="",
        share_url="https://s/two", tmdb_id=900, media_type="tv")
    assert new1 is True and new2 is False
    assert row1.id == row2.id and row2.submit_count == 2
    assert row2.share_url == "https://s/two" and row2.last_tg_user_id == "u2"
    assert row2.last_share_url == "https://s/two" and row2.status == "pending"
    assert db.query(UserContribution).count() == 1


def test_upsert_contribution_keeps_closed_status_and_bumps_count(db):
    """已结案投稿被重复投时不重开审批，只累加计数并记最新链接。"""
    from app import bot_listener as bl
    row1, _ = bl.upsert_contribution(
        db, tg_user_id="u1", tg_username="", title="已通过剧", content="",
        share_url="https://s/a", tmdb_id=901, media_type="tv")
    row1.status = "approved"
    db.commit()
    row2, new2 = bl.upsert_contribution(
        db, tg_user_id="u3", tg_username="", title="已通过剧", content="",
        share_url="https://s/b", tmdb_id=901, media_type="tv")
    assert new2 is False and row2.status == "approved"
    assert row2.submit_count == 2 and row2.share_url == "https://s/a"
    assert row2.last_share_url == "https://s/b"
    assert db.query(UserContribution).count() == 1


def test_contributions_api_exposes_merge_count(db):
    """后台列表 API 回显合并计数字段。"""
    from app import main
    from app import bot_listener as bl
    bl.upsert_contribution(db, tg_user_id="u1", tg_username="", title="计数剧",
                           content="", share_url="https://s/x", tmdb_id=902, media_type="tv")
    bl.upsert_contribution(db, tg_user_id="u2", tg_username="", title="计数剧",
                           content="", share_url="https://s/y", tmdb_id=902, media_type="tv")
    rows = main.contributions("", 100, db)
    assert len(rows) == 1 and rows[0]["submit_count"] == 2


# ---------------- v0.9.17.1 #283 不在群目标禁止加入白名单/推送 ----------------
def test_inactive_discover_reason_guard(db):
    """明确 left/kicked/not_member 拦截；unknown 放行，避免误伤新目标。"""
    from app import main
    left = DiscoveredChat(bot_id=1, chat_id="-100A", chat_type="supergroup",
                          member_status="left", perm_status="not_member")
    unknown = DiscoveredChat(bot_id=1, chat_id="-100B", chat_type="supergroup",
                             member_status="unknown", perm_status="unknown")
    assert "不在" in main._inactive_discover_reason(left)
    assert main._inactive_discover_reason(unknown) == ""


def test_whitelist_rejects_inactive_target(db):
    """后端白名单开关：已退出目标返回 400，不落路由。"""
    from app import main
    from fastapi import HTTPException
    d = DiscoveredChat(bot_id=1, chat_id="-100LEFT", chat_type="supergroup",
                       member_status="left", perm_status="not_member", in_whitelist=False)
    db.add(d); db.commit()
    with pytest.raises(HTTPException) as ei:
        main.discover_set_whitelist(
            d.id, main.DiscoverWhitelistInput(in_whitelist=True), db)
    assert ei.value.status_code == 400 and "不在" in ei.value.detail
    assert db.get(DiscoveredChat, d.id).in_whitelist is False


def test_test_push_rejects_inactive_target(db):
    """测试推送对已退出目标直接返回 ok=False + 提示，不发起 Telegram 调用。"""
    from app import main
    d = DiscoveredChat(bot_id=1, chat_id="-100LEFT2", chat_type="supergroup",
                       member_status="kicked", perm_status="not_member")
    db.add(d); db.commit()
    out = asyncio.run(main.discover_test_push(d.id, db))
    assert out["ok"] is False and ("不在" in out["message"] or "移出" in out["message"])


# ---------------- v0.9.17.1 #284 求片同作品去重合并 ----------------
def test_upsert_media_request_merges_and_counts(db):
    """同作品重复求片只留一行、累加计数、保留 pending。"""
    from app import bot_listener as bl
    r1, new1 = bl.upsert_media_request(
        db, tg_user_id="u1", tg_username="a", title="同一部剧",
        tmdb_id=1234, media_type="tv", season=1)
    r2, new2 = bl.upsert_media_request(
        db, tg_user_id="u2", tg_username="b", title="同一部剧",
        tmdb_id=1234, media_type="tv", season=1)
    assert new1 is True and new2 is False
    assert r1.id == r2.id and r2.submit_count == 2
    assert r2.last_tg_user_id == "u2" and r2.review_status == "pending"
    assert db.query(MediaRequest).count() == 1


def test_upsert_media_request_keeps_adopted_status(db):
    """已采纳的求片被重复提交时保留 review/status，只计数。"""
    from app import bot_listener as bl
    r1, _ = bl.upsert_media_request(
        db, tg_user_id="u1", tg_username="", title="已采纳剧", tmdb_id=1235, season=1)
    r1.review_status = "adopted"; r1.status = "synced"
    db.commit()
    r2, new2 = bl.upsert_media_request(
        db, tg_user_id="u3", tg_username="", title="已采纳剧", tmdb_id=1235, season=1)
    assert new2 is False and r2.review_status == "adopted" and r2.status == "synced"
    assert r2.submit_count == 2 and db.query(MediaRequest).count() == 1


def test_media_requests_api_exposes_merge_count(db):
    """求片后台列表 API 回显合并计数字段。"""
    from app import main
    from app import bot_listener as bl
    bl.upsert_media_request(db, tg_user_id="u1", tg_username="", title="计数求片", tmdb_id=1236)
    bl.upsert_media_request(db, tg_user_id="u2", tg_username="", title="计数求片", tmdb_id=1236)
    rows = main.media_requests(100, db)
    assert len(rows) == 1 and rows[0]["submit_count"] == 2


# ---------------- v0.9.17.1 前端源码断言（开关受控 / row-key / 下拉过滤 / 求片×N） ----------------
def test_frontend_discover_switch_and_filter_fixes():
    """锁死三个真机交互修复，防止回归。"""
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    src = (root / "frontend" / "src" / "App.vue").read_text(encoding="utf-8")
    # 开关受控绑定：@change 显式传新值，不再 v-model 双翻转
    assert ':model-value="!!row.in_whitelist"' in src
    assert '@change="v => toggleDiscoverWhitelist(row, v)"' in src
    assert 'toggleDiscoverWhitelist(c, newValue)' in src
    assert 'v-model="row.in_whitelist"' not in src
    # 表格加 row-key，删除/筛选重排不再按索引复用组件导致开关错位
    assert ':data="discoverChats" row-key="id"' in src
    # 推送目标下拉只列可推送目标（已退出/不在群被过滤）
    assert 'v-for="c in pushableDiscoverChats"' in src
    assert 'function isPushableDisc' in src
    # 求片合并计数 ×N 标记
    assert "累计 ' + s.row.submit_count + ' 次求片" in src
