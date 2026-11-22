"""v2.1.4 分享管理页专项验收（用户口径）：
  （1）右边留空较多、内容拥挤在左侧 → 页面满宽；
  （2）分享任务最前面和分享详情增加 TMDB 海报缩略图。
"""
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = ROOT / "frontend" / "src" / "App.vue"

# 后端内部枚举码：页面上一律不得出现，必须由后端翻译成中文标签
INTERNAL_LINK_STATES = ("active", "unknown", "suspicious", "inactive",
                         "cleaned", "manual_review")


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v216share_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'share.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    from sqlalchemy.orm import sessionmaker
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _seed(db, *, chat_id="-1001", target_type="group", kind=None, channel_name="影迷群",
          poster="/poster.jpg", work_key="work:测试剧", link_state="active",
          share_url="https://pan.quark.cn/s/abc123"):
    """造一条分享记录：清单项（渠道）+ 发布任务（分享链接非空）+ 资源库版本（海报）。

    渠道归类由**清单项的 target_type** 决定（后端 _task_channel_info 口径），
    Channel.kind 默认与之保持一致，避免口径漂移。
    """
    from app.models import (Bot, Channel, SCLibraryVersion, SCPublishTask,
                            SCPushList, SCPushListItem)
    bot = Bot(name="b", token="t")
    db.add(bot)
    db.flush()
    db.add(Channel(bot_id=bot.id, name=channel_name, chat_id=chat_id, enabled=True,
                   kind=kind or ("channel" if target_type == "channel" else "group")))
    pl = SCPushList(list_key="pl:1", name="清单", status="active")
    db.add(pl)
    db.flush()
    item = SCPushListItem(list_id=pl.id, work_key=work_key, library_version_id=0,
                          category_key="genre:剧情", target_type=target_type,
                          bot_id=bot.id, chat_id=chat_id, dedup_key="d:1")
    db.add(item)
    db.flush()
    task = SCPublishTask(list_item_id=item.id, work_key=work_key, kind="first",
                         status="published", link_state=link_state,
                         share_url=share_url)
    db.add(task)
    db.add(SCLibraryVersion(work_key=work_key, version_no=1,
                            metadata_json='{"title": "测试剧", "poster_path": "%s"}' % poster))
    db.commit()
    return task


# ---------------------------------------------------------------------------
# 1. 后端：分享记录返回海报 cover
# ---------------------------------------------------------------------------

def test_share_records_returns_poster_cover(env):
    """分享记录列表返回 TMDB 海报相对路径（前端拼缩略图地址）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        _seed(db, poster="/poster.jpg")
        res = sc2.api_share_records(db=db)
        assert res["items"][0]["cover"] == "/poster.jpg"


def test_share_records_cover_empty_when_missing(env):
    """没有海报时返回空串（前端不渲染缩略图）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        _seed(db, poster="")
        res = sc2.api_share_records(db=db)
        assert res["items"][0]["cover"] == ""


# ---------------------------------------------------------------------------
# 2. 后端：链接状态中文化
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("state,label,tone", [
    ("active", "有效", "ok"),
    ("unknown", "待校验", "muted"),
    ("suspicious", "疑似失效", "warn"),
    ("inactive", "已失效", "bad"),
    ("cleaned", "已清理", "muted"),
])
def test_share_records_link_status_localized(env, state, label, tone):
    """链接状态翻译成中文标签 + 视觉态，页面不再直出内部枚举码。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        _seed(db, link_state=state)
        # cleaned 是归档态，默认列表排除，需 include_archived 才能查回
        res = sc2.api_share_records(db=db, include_archived=True)
        item = res["items"][0]
        assert item["link_status_label"] == label, f"{state} 状态标签不对"
        assert item["link_tone"] == tone, f"{state} 视觉态不对"


def test_link_status_helpers_fallback():
    """未知 / 空状态回落为「待校验」+ muted，不泄漏原始值。"""
    from app import share_center_v2 as sc2
    assert sc2._link_status_label("") == "待校验"
    assert sc2._link_tone("") == "muted"
    assert sc2._link_status_label("不存在的状态") == "待校验"
    assert sc2._link_tone("不存在的状态") == "muted"


def test_link_status_labels_have_no_internal_enum():
    """标签集合里不含任何内部英文枚举名。"""
    from app import share_center_v2 as sc2
    labels = list(sc2.LINK_STATUS_LABELS.values())
    assert labels, "缺少链接状态标签"
    for label in labels:
        for state in INTERNAL_LINK_STATES:
            assert state not in label, f"标签直出了内部枚举名：{label}"


# ---------------------------------------------------------------------------
# 3. 后端：推送渠道
# ---------------------------------------------------------------------------

def test_share_records_returns_group_target(env):
    """群目标出现在 group_names，汇总文案含「群」与群名。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        _seed(db, chat_id="-1001", target_type="group", kind="group", channel_name="影迷群")
        res = sc2.api_share_records(db=db)
        item = res["items"][0]
        assert item["group_names"] == ["影迷群"]
        assert item["channel_names"] == []
        assert "群" in item["target_summary"]
        assert "影迷群" in item["target_summary"]


def test_share_records_returns_channel_target(env):
    """频道目标只出现在 channel_names，汇总文案含「频道」。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        _seed(db, chat_id="-1001", target_type="channel", kind="channel", channel_name="影迷群")
        res = sc2.api_share_records(db=db)
        item = res["items"][0]
        assert item["channel_names"] == ["影迷群"]
        assert item["group_names"] == []
        assert "频道" in item["target_summary"]


# ---------------------------------------------------------------------------
# 4. 前端契约
# ---------------------------------------------------------------------------

def test_frontend_share_table_has_poster():
    """分享管理表格首列是作品列，带 TMDB 海报缩略图。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert 'label="作品"' in app, "表格首列应为作品列"
    assert "sc-media-thumb" in app, "缺少缩略图样式"
    assert "image.tmdb.org/t/p/w185" in app, "缩略图未使用 TMDB 图片地址"
    assert "s.row.cover" in app, "未消费封面字段"
    # 分享管理表格内：作品列排在分享码列之前（首列）
    start = app.index("<!-- 设计稿分享管理：搜索 + 状态筛选 + 批量 + 统计卡 + 记录表（详情为隐藏式弹窗） -->")
    end = app.index("<!-- v2.1.1：按设计稿收敛——分享管理页只保留", start)
    block = app[start:end]
    assert block.index('label="作品"') < block.index('label="分享码"'), "作品列不在首列"


def test_frontend_share_detail_has_poster():
    """分享详情弹窗展示 TMDB 海报缩略图。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "sc-share-poster" in app, "分享详情缺少海报"
    assert "image.tmdb.org/t/p/w342" in app, "详情海报未使用 TMDB 图片地址"
    assert "scSelectedDetail.cover" in app, "详情未消费封面字段"


def test_frontend_share_detail_no_backend_field_leak():
    """分享管理模板区不得直出**内部枚举 / 内部键**。

    ① 注释里的字段名不算泄漏（不渲染到页面），先剥掉 HTML 注释；
    ② ``scOverview.link_state_counts`` 这类是**前端聚合对象的属性访问**，
       属既有合法模式，不算泄漏（与 v215 推送任务页同一口径）；
    ③ 描述性字段（cover / channel_names / group_names / target_summary）
       是给页面用的中性展示字段，允许出现；
    ④ 但**后端行对象**上的内部字段一律算泄漏——尤其 ``scSelectedDetail.chat_id``，
       它会被上面的豁免规则一并放过，所以这里对详情对象单独加一道断言。
    """
    import re
    app = APP_VUE.read_text(encoding="utf-8")
    start = app.index("<!-- 设计稿分享管理：搜索 + 状态筛选 + 批量 + 统计卡 + 记录表（详情为隐藏式弹窗） -->")
    end = app.index("<!-- v2.1.1：按设计稿收敛——分享管理页只保留", start)
    block = app[start:end]
    stripped = re.sub(r"<!--.*?-->", "", block, flags=re.S)
    banned = ("link_state", "work_key", "poster_path", "chat_id",
              "target_type", "list_item_id", "dedup_key")
    for word in banned:
        hits = [m for m in re.finditer(re.escape(word), stripped)
                if not re.search(r"sc[A-Za-z]+\." + re.escape(word),
                                 stripped[max(0, m.start() - 24):m.end()])]
        assert not hits, f"分享管理页模板直出内部字段：{word}"
    # 详情对象（后端整行）不得暴露任何内部字段——通用豁免规则盖不住，这里单独卡
    for word in banned:
        assert f"scSelectedDetail.{word}" not in stripped, \
            f"分享详情直出内部字段：scSelectedDetail.{word}"
    # 列表行对象同理（s.row.<内部字段>）
    for word in banned:
        assert f"s.row.{word}" not in stripped, f"分享列表直出内部字段：s.row.{word}"


def test_frontend_link_state_tones_css():
    """链接状态视觉态四档 CSS 齐全（ok / muted / warn / bad）。"""
    app = APP_VUE.read_text(encoding="utf-8")
    for tone in ("ok", "muted", "warn", "bad"):
        assert f".sc-dot-state.{tone}" in app, f"缺少链接状态视觉态：{tone}"


def test_frontend_share_page_full_width():
    """分享管理页满宽（不再挤在 .sc-board 的半格子里）。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "sc-board-full" in app, "缺少满宽容器"
    assert ".sc-board-full{grid-template-columns:minmax(0,1fr)}" in app, "满宽样式缺失"
