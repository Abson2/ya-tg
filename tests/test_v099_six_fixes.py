"""v0.9.9：6 项真机反馈回归（卡片并列/镜像自动推送诊断/新模板/频道群路由/影视清理/进度节点）。"""
from pathlib import Path

import pytest

from app import main, tmdb, utils
from app.db import DEFAULT_TEMPLATE, SessionLocal, _needs_template_upgrade, migrate
from app.models import (Bot, Channel, MirrorFile, MirrorMedia, MirrorRule,
                        QueueItem, Resource, Rule)

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


@pytest.fixture
def session():
    migrate()
    with SessionLocal() as s:
        for t in (QueueItem, MirrorFile, MirrorMedia, MirrorRule,
                  Resource, Channel, Bot, Rule):
            s.query(t).delete()
        s.commit()
        try:
            yield s
        finally:
            for t in (QueueItem, MirrorFile, MirrorMedia, MirrorRule,
                      Resource, Channel, Bot, Rule):
                s.query(t).delete()
            s.commit()


# ---------- #200 卡片并列 ----------
def test_mirror_cards_side_by_side():
    assert 'class="two-col mirror-two-col"' in APP
    assert APP.count('class="panel mirror-col"') >= 2
    assert ".mirror-col{max-width:none" in APP


# ---------- #202 消息模板新样式 ----------
def test_default_template_new_style_fields():
    t = DEFAULT_TEMPLATE
    for token in ("🖼️", "类型：{{type_label}}", "🆔 TMDB ID: {{tmdb_id}}",
                  "📅 上映时间: {{air_date}}", "💿 来源: {{source}}",
                  "📺 画质: {{quality}}", "🎞️ 编码: {{video_codec}}",
                  "🔊 音频: {{audio_codec}}", "📦 大小：{{size}}",
                  "🏷️ 标签：", "{{buttons_text}}", "{{share_url}}"):
        assert token in t, token
    assert "{{else}}" not in t  # 渲染器不支持 else


def test_render_template_matches_requested_sample():
    ctx = dict(title="十万狂花入梦来", year="2026", type_label="国产剧",
               tmdb_id="261315", air_date="2026-11-15", vote_plain="9",
               source="Web-DL", quality="2160p | 10bit | 60fps | SDR",
               video_codec="H265", audio_codec="AAC", season_count="1",
               episode_count="13", episode_status="更新中", episode_range="S01 E01-E13",
               size="70.09 GB", overview="剧情简介", tags="#十万狂花入梦来 #2026年 #国产剧",
               buttons_text="📢 频道　👥 群组　🔍 投稿/搜索", share_url="https://s/1")
    out = utils.render_template(DEFAULT_TEMPLATE, ctx)
    assert "<b>十万狂花入梦来 (2026) 🖼️</b>" in out
    assert "🆔 TMDB ID: 261315" in out
    assert "📅 上映时间: 2026-11-15" in out
    assert "🎞️ 编码: H265 | 🔊 音频: AAC" in out
    assert "📋 季集：共1季 | 共 13 集（更新中） S01 E01-E13" in out


def test_parse_tech_spec_from_filename():
    spec = utils.parse_tech_spec("十万狂花入梦来.S01E01.2160p.10bit.60fps.HDR.WEB-DL.H265.AAC.mkv")
    assert spec["source"] == "Web-DL"
    assert "2160p" in spec["quality"] and "10bit" in spec["quality"] and "60fps" in spec["quality"]
    assert spec["codec"] == "H265"
    assert spec["audio"] == "AAC"
    assert utils.parse_tech_spec("") == {"source": "", "quality": "", "codec": "", "audio": ""}


def test_template_upgrade_detection():
    old_default = ("<b>🎬 {{title}}{{#if year}}（{{year}}）{{/if}}</b>\n"
                   "{{#if episode_status}}📡 {{episode_status}}\n{{/if}}"
                   "{{#if tmdb_url}}🔗 TMDB：#{{tmdb_id}}\n{{/if}}{{share_url}}")
    assert _needs_template_upgrade(old_default) is True      # v0.8.9 旧默认 → 升级
    assert _needs_template_upgrade(DEFAULT_TEMPLATE) is False  # 已是新版 → 不动
    assert _needs_template_upgrade("<b>{{title}}</b> {{episode_status}} 我的自定义") is False


def test_release_date_normalized_from_tmdb():
    d = tmdb._normalize({"id": 1, "title": "X", "release_date": "2026-11-15",
                         "vote_average": 9}, "movie")
    assert d["release_date"] == "2026-11-15"


# ---------- #201/#206 入队诊断与 Channel 补建 ----------
def test_enqueue_diag_no_target(session):
    # 清空可能由其它测试残留的路由配置，确保走「无目标」分支
    main.runtime_config.set_many(session, {"push_routes_json": "[]"})
    res = Resource(file_id="f1", name="剧", parent_id="root",
                   identify_status="identified", poster_path="/p.jpg")
    session.add(res); session.flush()
    diag = {}
    n = main._enqueue_after_identify(session, res, diag=diag)
    assert n == 0
    assert diag.get("skip_no_target") == 1


def test_enqueue_diag_no_poster(session):
    res = Resource(file_id="f2", name="剧", parent_id="root",
                   identify_status="identified", poster_path="")
    session.add(res); session.flush()
    diag = {}
    assert main._enqueue_after_identify(session, res, diag=diag) == 0
    assert diag.get("skip_no_poster") == 1


def test_ensure_route_channel_recreates_missing(session):
    bot = Bot(name="b", token="x", enabled=True)
    session.add(bot); session.flush()
    main.runtime_config.set_many(session, {"interaction_bot_id": str(bot.id)})
    route = {"kind": "group", "chat_id": "-100999"}
    ch = main._ensure_route_channel(session, route)
    assert ch is not None and ch.enabled is True and ch.chat_id == "-100999"
    # 幂等：再调一次复用同一条，不重复建
    ch2 = main._ensure_route_channel(session, dict(route))
    assert ch2.id == ch.id


def test_pipeline_push_node_shows_diag(session):
    rule = main._mirror_rule(session, create=True)
    rule.last_message = "上次同步 · 入队诊断：无海报闸门 3、目录路由未匹配 2"
    session.flush()
    st = main.mirror_status(db=session)
    push = next(x for x in st["pipeline"] if x["key"] == "push")
    assert "上次拦截" in push["detail"] and "无海报闸门" in push["detail"]


# ---------- #205 影视映射清理口径与不持锁 ----------
def test_media_clear_includes_synced_without_files(session):
    r = MirrorRule(name="r", source_root_id="A", target_root_id="B", enabled=True)
    session.add(r); session.flush()
    # synced 状态但没有任何文件 = 历史遗留僵尸行，应可清理
    m = MirrorMedia(rule_id=r.id, work_key="wk:zombie", title_standard="僵尸剧", status="synced")
    session.add(m); session.flush()
    # pending 进行中 = 不能清理
    m2 = MirrorMedia(rule_id=r.id, work_key="wk:running", title_standard="进行中", status="pending")
    session.add(m2); session.flush()
    invalid = main._invalid_media_rows(session)
    ids = {x.id for x in invalid}
    assert m.id in ids and m2.id not in ids


def test_media_clear_source_does_not_hold_mirror_lock():
    src = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
    start = src.index("def mirror_media_clear_invalid(")
    end = src.index("def _invalid_media_rows(", start)
    body = src[start:end]
    assert "_mirror_guard()" not in body  # 不占用镜像锁


def test_frontend_invalid_media_controls_kept():
    assert "async function clearInvalidMedia" in APP
    assert "清理无效影视映射" in APP
