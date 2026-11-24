"""v0.8.9 回归 / v0.9.0 改版：推送模板「剧集状态 / 季集标签 / 当前集·总集数」变量。

用户反馈：推送模板希望增加剧集状态，比如更新至多少集、当前集/总集数。
v0.9.0 用户进一步要求季集标签格式：追更「更新至 S01E07 · 连载中」、
收齐「S01E24 · 完结」；总集数/进度仍由 episode_progress 单独承载。
本组测试锁死三件事：
  1. TMDB 官方总集数与本地已收录集数分开，追更剧能显示 12/24；
  2. 完结剧收齐后显示「S01E24 · 完结」，电影不输出任何剧集状态；
  3. 存量默认模板自动升级，用户自定义模板绝不被覆盖。
"""
from pathlib import Path

from sqlalchemy import select

import pytest

from app.db import DEFAULT_TEMPLATE, SessionLocal, _needs_template_upgrade, migrate
from app.main import _apply_identification
from app.models import MessageTemplate, QueueItem, Resource
from app.publisher import _build_message


def _clean():
    with SessionLocal() as db:
        db.query(QueueItem).delete()
        db.query(Resource).delete()
        # 模板是跨测试共享的：本文件会临时改成自定义短模板，必须每个用例
        # 前后都恢复默认，否则会污染后面测试文件对「🎬 / 集数」的断言。
        tpl = db.query(MessageTemplate).filter(
            MessageTemplate.name == "默认图文模板").first()
        if tpl:
            tpl.content = DEFAULT_TEMPLATE
        db.commit()


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    _clean()
    yield
    _clean()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as session:
        try:
            yield session
        finally:
            session.rollback()


def _episode(fid, i, *, total=0, finish="", identified=False, episode=None):
    return Resource(
        file_id=fid, name=f"追更剧.2026.S01E{i:02d}.2160p.mkv", ext=".mkv",
        size=1024 ** 3, utime=1,
        path_hint="影视/电视剧/国产剧/追更剧 (2026)/Season 1",
        share_dir_name="追更剧", media_title="追更剧",
        media_type="tv" if identified else "", tmdb_id=7788 if identified else None,
        total_episodes=total, finish_status=finish,
        identify_status="identified" if identified else "unidentified",
        poster_path="/p.jpg" if identified else "",
        season=1 if identified else None,
        episode=episode if identified else None)


def _render(db, fid="v089-e01"):
    rep = db.scalar(select(Resource).where(Resource.file_id == fid))
    return _build_message(db, rep, "https://share/x")[0]


# ---------- 1. 默认模板与升级 ----------
def test_default_template_exposes_episode_status_vars():
    # v2.1.22：默认图文用「📡 分集: {{episode_status}}」承载季集；
    # episode_count / episode_progress / episode_range 等细粒度变量仍保留在变量面板
    # 供用户自行插入（默认模板不再逐行列全，避免消息过长）。
    assert "{{episode_status}}" in DEFAULT_TEMPLATE
    assert _needs_template_upgrade(DEFAULT_TEMPLATE) is False
    # 变量面板必须把 5 个新变量都列出来，否则用户根本不知道能插入
    app_vue = (Path(__file__).resolve().parents[1]
               / "frontend/src/App.vue").read_text(encoding="utf-8")
    for var in ("episode_status", "episode_progress", "updated_to_episode",
                "total_episodes", "current_episode"):
        assert f"'{var}'" in app_vue, var


def test_v088_default_template_is_upgraded_but_custom_is_untouched():
    v088 = ("<b>🎬 {{title}}{{#if year}}（{{year}}）{{/if}}</b>\n"
            "{{#if season_label}}📺 {{season_label}}\n{{/if}}"
            "{{#if episode_count}}📼 集数：共 {{episode_count}} 集\n{{/if}}"
            "{{#if tmdb_url}}🔗 TMDB：<a href=\"{{tmdb_url}}\">#{{tmdb_id}}</a>\n{{/if}}"
            "{{share_url}}")
    assert _needs_template_upgrade(v088) is True
    # 自定义模板：同样用了 episode_count，但不带默认骨架，绝不覆盖
    custom = "<b>我的模板 {{title}}</b>\n📼 集数：共 {{episode_count}} 集\n{{share_url}}"
    assert _needs_template_upgrade(custom) is False
    assert _needs_template_upgrade("<b>{{title}}</b> · {{share_url}}") is False


def test_migrate_upgrades_stored_v088_default(db):
    tpl = db.query(MessageTemplate).filter(MessageTemplate.name == "默认图文模板").first()
    assert tpl is not None
    tpl.content = ("<b>🎬 {{title}}</b>\n"
                   "{{#if episode_count}}📼 集数：共 {{episode_count}} 集\n{{/if}}"
                   "🔗 TMDB：{{tmdb_url}}\n{{share_url}}")
    db.commit()
    migrate()
    db.expire_all()
    tpl = db.query(MessageTemplate).filter(MessageTemplate.name == "默认图文模板").first()
    assert "{{episode_status}}" in tpl.content


# ---------- 2. 追更剧：官方 24 集 / 本地已收录 12 集 ----------
def test_returning_series_shows_updated_to_and_progress(db):
    for i in range(1, 13):
        db.add(_episode(f"v089-e{i:02d}", i, total=24, finish="returning",
                        identified=(i == 1), episode=1 if i == 1 else None))
    db.commit()
    text = _render(db)
    # v0.9.0：剧集状态改为季集标签「更新至 S01E12 · 连载中」
    assert "更新至 S01E12 · 连载中" in text, text
    # v2.1.23：版式与用户参考图对齐——季集标签整行用【】包住（参考图【S01E04 · 完结】），
    # 不再带「📡 分集:」这个行头。
    assert "【更新至 S01E12 · 连载中】" in text, text
    assert "📡 分集:" not in text, text


def test_ended_series_shows_full_episode_count(db):
    for i in range(1, 25):
        db.add(_episode(f"v089-f{i:02d}", i, total=24, finish="ended",
                        identified=(i == 1), episode=1 if i == 1 else None))
    db.commit()
    text = _render(db, "v089-f01")
    # v0.9.0：收齐显示「S01E24 · 完结」
    assert "S01E24 · 完结" in text, text


def test_no_tmdb_total_falls_back_to_local_count(db):
    """没识别到官方总集数时也不能显示 0/N，用「已收录 N 集」兜底。"""
    for i in (1, 2, 3):
        db.add(_episode(f"v089-g{i}", i, total=0, finish="",
                        identified=(i == 1), episode=1 if i == 1 else None))
    db.commit()
    text = _render(db, "v089-g1")
    assert "更新至 S01E03 · 已收录 3 集" in text, text
    assert "0/" not in text, text


# ---------- 3. 电影与变量可用性 ----------
def test_movie_has_no_episode_status_lines(db):
    db.add(Resource(file_id="v089-movie", name="大醉侠 (2002).mkv", ext=".mkv",
                    size=1024, utime=1, media_type="movie", media_title="大醉侠",
                    identify_status="identified", poster_path="/p.jpg"))
    db.commit()
    text = _render(db, "v089-movie")
    assert "📡" not in text and "🔢" not in text, text
    assert "进度：" not in text, text


def test_episode_vars_render_with_custom_template(db):
    """用户可直接写 {{current_episode}}/{{total_episodes}}，变量必须可用。"""
    # 两集且集号与文件名一致：E01 / E07，代表「已更新到第 7 集」
    db.add(_episode("v089-h1", 1, total=24, finish="returning",
                    identified=True, episode=1))
    db.add(_episode("v089-h7", 7, total=24, finish="returning",
                    identified=True, episode=7))
    db.commit()
    tpl = db.query(MessageTemplate).filter(MessageTemplate.name == "默认图文模板").first()
    tpl.content = ("{{episode_status}} | {{episode_progress}} | "
                   "{{current_episode}}/{{total_episodes}} | {{updated_to_episode}}")
    db.commit()
    text = _render(db, "v089-h7")
    assert "7/24" in text, text
    assert "更新至 S01E07 · 连载中" in text, text
    assert text.rstrip().endswith("| 7"), text


# ---------- 4. 官方总集数落库 ----------
def test_apply_identification_stores_tmdb_total_episodes(db):
    tv = Resource(file_id="v089-i1", name="Show.S01E01.mkv")
    _apply_identification(tv, {"tmdb_id": 7788, "media_type": "tv", "title": "Show",
                               "number_of_episodes": 24, "finish_status": "returning"})
    assert tv.total_episodes == 24
    movie = Resource(file_id="v089-i2", name="Movie.mkv")
    _apply_identification(movie, {"tmdb_id": 99, "media_type": "movie",
                                  "title": "Movie", "number_of_episodes": 24})
    assert movie.total_episodes == 0
