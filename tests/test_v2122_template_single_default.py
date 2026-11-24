"""v2.1.22：消息模板收敛为「默认图文 + 自定义」，且默认唯一（#309）。

需求原话：「消息模版：优化内置模版，仅保留默认图文与自定义，默认配置只能配置一条，
但用户可自定义不同模版供保存」。

本文件锁死：
  ① 内置预设只剩「默认图文」一条，且内容 = DEFAULT_TEMPLATE；
  ② 默认图文版式按参考图逐行给出（TMDB / 评分 / 画质 / 版本 / 大小 / 分享 / 分集 /
     简介 / 标签），每行都带 {{#if}}，缺字段整行消失；
  ③ 存量 v0.9.9 旧默认图文启动时自动升级；用户自定义模板绝不被覆盖；
  ④ 「默认只能一条」：启动迁移与所有写操作后都归一为恰好一条；
  ⑤ 内置那条不可删除（后端 400 + 中文说明），自定义照旧可删；
  ⑥ 用户新建的模板永远是自定义（builtin 不会被前端带进来）。
"""
from pathlib import Path

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from app import db as dbmod
from app import main, utils
from app.db import DEFAULT_TEMPLATE, TEMPLATE_PRESETS, SessionLocal, migrate
from app.models import MessageTemplate

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")

# v0.9.9 的旧默认图文（三特征 🖼️ + {{buttons_text}} + 🆔 TMDB ID 同时命中才会被升级）
OLD_V099_DEFAULT = (
    "<b>{{title}}{{#if year}} ({{year}}){{/if}} 🖼️</b>\n"
    "{{#if type_label}}类型：{{type_label}}\n{{/if}}"
    "{{#if tmdb_id}}🆔 TMDB ID: {{tmdb_id}}\n{{/if}}"
    "{{#if vote_plain}}⭐️ 评分: {{vote_plain}}{{/if}}"
    "{{#if quality}}📺 画质: {{quality}}\n{{/if}}"
    "{{#if size}}📦 大小：{{size}}\n{{/if}}"
    "{{#if tags}}🏷️ 标签： {{tags}}\n{{/if}}"
    "{{buttons_text}}\n"
    "{{share_url}}"
)


@pytest.fixture
def session():
    migrate()
    with SessionLocal() as s:
        s.query(MessageTemplate).delete()
        s.commit()
        try:
            yield s
        finally:
            s.rollback()
            s.query(MessageTemplate).delete()
            s.commit()


# ---- ① 内置只剩一条 ----
def test_only_one_builtin_preset():
    assert len(TEMPLATE_PRESETS) == 1
    assert TEMPLATE_PRESETS[0]["name"] == "默认图文"
    assert TEMPLATE_PRESETS[0]["content"] == DEFAULT_TEMPLATE


def test_preset_endpoint_returns_single_builtin():
    presets = main.template_presets()
    assert [p["name"] for p in presets] == ["默认图文"]


# ---- ② 版式逐行 ----
def test_default_template_matches_reference_layout():
    t = DEFAULT_TEMPLATE
    for token in ("🆔 TMDB: {{tmdb_id}}", "⭐ TMDB评分: {{vote_plain}}/10",
                  "🖥 画质: {{quality}}", "🎞 版本: {{version}}",
                  "📦 大小: {{size}}", "🔗 分享: {{share_code}}",
                  "📡 分集: {{episode_status}}", "📝 简介: {{overview}}",
                  "🏷 {{tags}}"):
        assert token in t, token
    # 参考图里的「豆瓣评分」系统没有数据源，写进去只能永久显示「暂无」→ 不放
    assert "豆瓣" not in t


def test_missing_fields_drop_whole_line():
    """缺字段必须整行消失，不能留「分享: 」这种空壳行。"""
    out = utils.render_template(DEFAULT_TEMPLATE,
                                {"title": "只有标题", "share_url": "https://s/1"})
    assert out == "<b>只有标题</b>\nhttps://s/1", out


# ---- ③ 存量升级 ----
def test_old_default_upgrades_to_new_layout(session):
    row = MessageTemplate(name="默认图文模板", content=OLD_V099_DEFAULT,
                          enabled=True, is_default=True, builtin=True)
    session.add(row)
    session.commit()
    migrate()
    with SessionLocal() as s:
        got = s.query(MessageTemplate).filter(
            MessageTemplate.name == "默认图文模板").first()
        assert got is not None
        assert dbmod.CURRENT_DEFAULT_MARK in got.content
        assert "🆔 TMDB ID" not in got.content


def test_custom_template_is_never_upgraded(session):
    """用户自己写的模板（只借用了 🖼️ 但没全套旧默认特征）不动。"""
    mine = MessageTemplate(name="我的模板", content="🖼️ 我自己的 {{title}}",
                           enabled=True, is_default=False, builtin=False)
    session.add(mine)
    session.commit()
    assert dbmod._needs_template_upgrade(mine.content) is False
    migrate()
    with SessionLocal() as s:
        got = s.query(MessageTemplate).filter(MessageTemplate.name == "我的模板").first()
        assert got.content == "🖼️ 我自己的 {{title}}"


# ---- ④ 默认唯一 ----
def test_migrate_collapses_multiple_defaults(session):
    for i in range(3):
        session.add(MessageTemplate(name=f"T{i}", content="x", enabled=True,
                                    is_default=True, builtin=(i == 1)))
    session.commit()
    migrate()
    with SessionLocal() as s:
        rows = s.scalars(select(MessageTemplate)).all()
        flagged = [r for r in rows if r.is_default]
        assert len(flagged) == 1, [r.name for r in flagged]
        # 多条时优先保留内置那条
        assert flagged[0].builtin is True


def test_migrate_restores_missing_default(session):
    session.add(MessageTemplate(name="自定义", content="x", enabled=True,
                                is_default=False, builtin=False))
    session.commit()
    migrate()
    with SessionLocal() as s:
        rows = s.scalars(select(MessageTemplate)).all()
        assert len([r for r in rows if r.is_default]) == 1


def test_single_default_is_left_alone(session):
    """用户把自定义模板设为默认是合法状态，迁移不得抢回内置。"""
    session.add(MessageTemplate(name="默认图文模板", content=DEFAULT_TEMPLATE,
                                enabled=True, is_default=False, builtin=True))
    session.add(MessageTemplate(name="我的默认", content="x", enabled=True,
                                is_default=True, builtin=False))
    session.commit()
    migrate()
    with SessionLocal() as s:
        flagged = [r for r in s.scalars(select(MessageTemplate)).all() if r.is_default]
        assert len(flagged) == 1
        assert flagged[0].name == "我的默认"


def test_ensure_single_default_helper(session):
    session.add(MessageTemplate(name="A", content="x", enabled=True, is_default=True))
    session.add(MessageTemplate(name="B", content="y", enabled=True, is_default=True))
    session.commit()
    cleared = main._ensure_single_default_template(session)
    session.commit()
    assert cleared == 1
    flagged = [r for r in session.scalars(select(MessageTemplate)).all() if r.is_default]
    assert len(flagged) == 1


# ---- ⑤ 内置不可删 ----
def test_builtin_template_cannot_be_deleted(session):
    builtin = MessageTemplate(name="默认图文模板", content=DEFAULT_TEMPLATE,
                              enabled=True, is_default=True, builtin=True)
    session.add(builtin)
    session.commit()
    with pytest.raises(HTTPException) as ei:
        main.delete_template(builtin.id, db=session)
    assert ei.value.status_code == 400
    assert "不可删除" in str(ei.value.detail)
    # 反证：把 builtin 标记去掉就能删（证明保护确实挂在标记上，不是挂在名字上）
    builtin.builtin = False
    session.commit()
    assert main.delete_template(builtin.id, db=session)["ok"] is True


def test_custom_template_can_still_be_deleted(session):
    session.add(MessageTemplate(name="默认图文模板", content=DEFAULT_TEMPLATE,
                                enabled=True, is_default=True, builtin=True))
    mine = MessageTemplate(name="我的", content="x", enabled=True, is_default=False,
                           builtin=False)
    session.add(mine)
    session.commit()
    assert main.delete_template(mine.id, db=session)["ok"] is True


# ---- ⑥ 新建永远是自定义 ----
def test_new_template_is_never_builtin(session):
    body = main.TemplateInput(name="我的新模板", content="{{title}}", is_default=False)
    main.add_template(body, db=session)
    got = session.query(MessageTemplate).filter(
        MessageTemplate.name == "我的新模板").first()
    assert got is not None and got.builtin is False


def test_restore_endpoint_marks_builtin(session):
    out = main.reset_default_template(db=session)
    with SessionLocal() as s:
        rows = s.scalars(select(MessageTemplate)).all()
        assert len([r for r in rows if r.is_default]) == 1
        got = s.get(MessageTemplate, out["id"])
        assert got.builtin is True
        assert got.content == DEFAULT_TEMPLATE


# ---- 前端契约（页面不得出现后端枚举名，且默认只能一条要说清）----
def test_frontend_template_page_contract():
    assert "仅「默认图文」一条" in APP_VUE
    assert "默认配置**只能有一条**" in APP_VUE
    assert "t.builtin" in APP_VUE          # 列表按内置标记禁用删除
    assert "newTplDraft" in APP_VUE        # 另存为自定义模板的入口
    assert "'share_code'" in APP_VUE       # 新变量进变量面板
