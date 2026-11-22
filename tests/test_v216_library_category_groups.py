"""v2.1.6 资源库按 TMDB 分类展示与折叠专项验收。

用户口径（Task #244）：资源库按 TMDB 分类展示，分类可折叠，
且要能一键折叠 / 展开所有。

事实基础：
  - `Resource.genres` 存 JSON 数组字符串，元素是 **TMDB 官方中文名**
    （如 `["动作","惊悚"]`，tmdb.py::_normalize 写入）；
  - TMDB 官方 genre 全集是影视分类唯一权威（tmdb.py::TMDB_GENRES，27 项）；
  - 作品级主分类口径与分享中心一致：genres 列表第一个
    （share_center_v2._primary_category 同款）；
  - 资源库页现有筛选（关键词 / 分享状态 / 识别状态 / 类型 tab）与批量操作
    **必须原样保留**，分组只是把同一批卡片重新分桶。

覆盖：
  1. 后端：作品聚合返回 primary_genre（主分类）与 genre_names（全部分类）；
  2. 后端：只收官方 genre 字典里的中文名，非官方名不入组（不编造分类）；
  3. 后端：无分类作品不丢，仍在列表里（前端进「未分类」桶）；
  4. 后端：主分类口径 = genres 第一个（与分享中心一致）；
  5. 前端：分组视图 + 平铺视图并存，卡片操作（推送/分享/识别/完结）两视图一致；
  6. 前端：分类可单独折叠，且支持一键折叠/展开所有；
  7. 前端：「未分类」永远排最后，不与真实分类抢位置；
  8. 页面不得出现后端内部字段名。
"""
import json
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


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v216lib_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'lib.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    from sqlalchemy.orm import sessionmaker
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _seed(db, *, name, genres, tmdb_id=1, media_type="movie",
          share_status="not_created", file_id=None):
    """造一条资源（代表一部作品）。

    注意 `Resource.file_id` 是**非空且唯一**的列（models.py），
    每条资源必须给一个不同的 file_id，否则 INSERT 直接报 NOT NULL 约束失败。
    """
    from app.models import Resource
    r = Resource(
        file_id=file_id or f"f_{name}",
        name=name,
        path_hint=name,
        parent_id="p1",
        size=1024,
        ext=".mkv",
        tmdb_id=tmdb_id,
        media_type=media_type,
        media_title=name,
        poster_path="/p.jpg",
        genres=json.dumps(genres, ensure_ascii=False) if genres else "",
        identify_status="identified",
        share_status=share_status,
        vote_average=7.5,
    )
    db.add(r)
    db.flush()
    return r


def _series(db):
    """调用真实的 library_series 端点函数（按真实签名传参）。"""
    from app.main import library_series
    return library_series(q="", media_type="", finish="",
                          identify_status="", share_status="", db=db)


# ---------------------------------------------------------------------------
# 1~4. 后端
# ---------------------------------------------------------------------------

def test_series_returns_primary_genre(env):
    """作品聚合返回主分类与全部分类。"""
    S, _ = env
    db = S()
    _seed(db, name="A", genres=["动作", "惊悚"])
    _seed(db, name="B", genres=["喜剧"])
    rows = _series(db)
    by_title = {r["title"]: r for r in rows}
    assert by_title["A"]["primary_genre"] == "动作", "主分类应取 genres 第一个"
    assert set(by_title["A"]["genre_names"]) == {"动作", "惊悚"}, "全部分类应保留"
    assert by_title["B"]["primary_genre"] == "喜剧"
    db.close()


def test_series_genre_names_only_official_tmdb_genres(env):
    """只收官方 genre 字典里的中文名——不编造分类。"""
    S, _ = env
    db = S()
    _seed(db, name="A", genres=["动作", "自造分类名"])
    rows = _series(db)
    row = next(r for r in rows if r["title"] == "A")
    assert "动作" in row["genre_names"], "官方分类应保留"
    assert "自造分类名" not in row["genre_names"], \
        "非官方分类名不得入组（页面会显示自造分类）"
    db.close()


def test_series_without_genres_still_returned(env):
    """无分类作品不丢（前端进「未分类」桶）。"""
    S, _ = env
    db = S()
    _seed(db, name="未识别剧", genres=[])
    rows = _series(db)
    row = next(r for r in rows if r["title"] == "未识别剧")
    assert row["primary_genre"] == "", "无分类时主分类为空"
    assert row["genre_names"] == [], "无分类时分类集合为空"
    db.close()


def test_series_primary_genre_matches_share_center_priority(env):
    """主分类口径与分享中心一致：genres 列表第一个（不是排序后第一个）。"""
    S, _ = env
    db = S()
    # 故意让字典序与列表序不同：「惊悚」字典序在「动作」之后，
    # 若实现里对分类做了排序，主分类会变成「惊悚」——那与分享中心不一致。
    _seed(db, name="A", genres=["惊悚", "动作"])
    rows = _series(db)
    row = next(r for r in rows if r["title"] == "A")
    assert row["primary_genre"] == "惊悚", \
        "主分类必须取 genres 的第一个元素（与 _primary_category 同口径）"
    db.close()


def test_series_primary_genre_takes_first_even_if_not_official(env):
    """主分类**严格取 genres 列表第一个**，即使它不在官方字典里。

    这条锁的是「资源库与分享中心归类一致」：分享中心的 _primary_category
    也是取 genres 第一个（不查官方字典）。若资源库这边偷偷跳到第二个官方项，
    同一作品在两页会显示成不同分类。
    """
    S, _ = env
    db = S()
    _seed(db, name="A", genres=["自造分类名", "动作"])
    rows = _series(db)
    row = next(r for r in rows if r["title"] == "A")
    assert row["primary_genre"] == "自造分类名", \
        "主分类必须严格取列表第一个（与 _primary_category 同口径）"
    assert row["genre_names"] == ["动作"], \
        "全部分类只收官方名"
    db.close()


def test_series_multi_episode_same_work_merges_genres(env):
    """同一作品的多集合并成分类并集（不丢分类）。"""
    S, _ = env
    db = S()
    from app.models import Resource
    for ep in (1, 2):
        db.add(Resource(
            file_id=f"f_ep{ep}",
            name=f"剧集E{ep}", path_hint="剧集", parent_id="p1", size=100,
            ext=".mkv", tmdb_id=99, media_type="tv", media_title="同一部剧",
            poster_path="/p.jpg",
            genres=json.dumps(["动作"] if ep == 1 else ["惊悚"], ensure_ascii=False),
            identify_status="identified", share_status="not_created",
        ))
    db.flush()
    rows = _series(db)
    assert len(rows) == 1, "同一部剧应聚合成一张卡"
    assert set(rows[0]["genre_names"]) == {"动作", "惊悚"}, \
        "多集的分类应合并成并集"
    db.close()


# ---------------------------------------------------------------------------
# 5~8. 前端
# ---------------------------------------------------------------------------

def _strip_comments(src: str) -> str:
    """剥掉 HTML 注释与 **CSS 注释**。

    注意：不能无脑用 ``re.sub(r"/\\*.*?\\*/", ...)`` 剥全文件——
    JS 里的路径字符串（``/api/sc/*``）会被当成 CSS 注释起点，
    一路吞掉整个 ``<script setup>``，把要断言的逻辑一起删掉。
    因此只对 ``<style>`` 块内的内容剥 CSS 注释。
    """
    import re
    src = re.sub(r"<!--.*?-->", "", src, flags=re.S)

    def _strip_style(m: re.Match) -> str:
        return re.sub(r"/\*.*?\*/", "", m.group(0), flags=re.S)

    return re.sub(r"<style[^>]*>.*?</style>", _strip_style, src, flags=re.S)


def test_frontend_has_grouped_and_flat_views():
    """分组视图与平铺视图并存，切换开关在工具条。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert 'class="lib-groups"' in app, "缺少分类分组容器"
    assert 'v-if="groupByCategory"' in app, "分组视图未按开关切换"
    assert 'v-else class="media-grid"' in app, "缺少平铺视图"
    assert "按 TMDB 分类分组" in app, "缺少分组开关文案"


def test_frontend_group_toggle_all():
    """支持一键折叠 / 展开所有分类。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "toggleAllGroups" in app, "缺少一键折叠/展开函数"
    assert "折叠所有" in app and "展开所有" in app, "缺少一键折叠/展开按钮文案"
    assert "allGroupsCollapsed" in app, "缺少全部折叠态判定"


def test_frontend_single_group_collapse():
    """单个分类可独立折叠。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "toggleGroup(grp.name)" in app, "分类头未绑定折叠切换"
    assert 'v-show="!grp.collapsed"' in app, "折叠未控制内容显隐"
    assert "collapsedGroups" in app, "缺少折叠状态存储"


def test_frontend_ungrouped_bucket_last():
    """「未分类」永远排最后。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "UNGROUPED_NAME" in app, "缺少未分类常量"
    code = _strip_comments(app)
    # 排序函数里必须对未分类做特殊处理（置后）
    assert "if (a.name === UNGROUPED_NAME) return 1" in code, \
        "未分类未排到最后"
    assert "if (b.name === UNGROUPED_NAME) return -1" in code, \
        "未分类未排到最后"


def test_frontend_no_backend_field_leak():
    """资源库分组不得暴露后端内部字段名。

    口径说明（与 v216 分享管理页**不同**，勿混用）：
      分享管理页拿的是**后端整行对象**，`link_state` / `poster_path` 出现在模板里
      就算泄漏；资源库页的 `g` 是前端聚合对象，`g.poster_path` / `g.link_state`
      是 v2.1.4 之前就有的合法消费（用于取海报图与链接角标），**不算泄漏**。
      本页真正要防的是新引入的**后端内部键形态**（如 `category_key` / `work_key` /
      `chat_id` / `dedup_key`）与内部枚举直出。
    """
    app = APP_VUE.read_text(encoding="utf-8")
    code = _strip_comments(app)
    start = code.index('class="lib-groups"')
    end = code.index('v-else class="media-grid"')
    block = code[start:end]
    for word in ("work_key", "category_key", "chat_id",
                 "dedup_key", "target_type", "list_item_id"):
        assert word not in block, f"资源库分组区直出内部字段：{word}"
    # 不得出现 genre:<中文名> 形态的内部键（页面只展示中文分类名）
    assert "genre:" not in block, "页面不得出现 genre:<中文名> 形态的内部键"


def test_frontend_consumes_primary_genre_in_script():
    """分组视图确实消费后端主分类字段（消费点在 script 的 computed 里）。

    模板区块只渲染**已分好组**的 `grp.name / grp.items / grp.count / grp.collapsed`，
    `primary_genre` 的读取在 `<script setup>` 的 `seriesGroupsByCategory` computed 内
    —— 断言要查 script 段，不能只查模板区块。
    """
    app = APP_VUE.read_text(encoding="utf-8")
    code = _strip_comments(app)
    assert "g.primary_genre" in code, "分组 computed 未消费后端主分类字段"
    # computed 必须是模板 v-for 的数据源
    assert 'v-for="grp in seriesGroupsByCategory"' in code, \
        "模板未消费分组 computed"
    assert "const seriesGroupsByCategory = computed(" in code, \
        "分组 computed 缺失"


def test_frontend_group_actions_identical_to_flat():
    """两视图的操作按钮完全一致（分组不得改功能）。"""
    app = APP_VUE.read_text(encoding="utf-8")
    code = _strip_comments(app)
    g_start = code.index('class="lib-groups"')
    g_end = code.index('v-else class="media-grid"')
    # 平铺视图的结束边界：平铺容器之后紧邻的是「扫描设置」面板（原文 L3510 的
    # v-if="showScanPanel"）。这里必须用**代码内可见**的锚点，且三个索引统一在
    # code 坐标系里——若改用 HTML 注释作锚点，_strip_comments 会先把注释删掉，
    # 锚点根本找不到，flat 切片就一路延伸到文件末尾（含整个 <style> 块），
    # 把「按钮被删了」这类真实回退稀释掉而侥幸通过。
    if 'v-if="showScanPanel"' not in code[g_end:]:
        pytest.fail("未找到平铺视图结束锚点 v-if=\"showScanPanel\"——锚点已失效，"
                    "请同步更新本测试的视图边界")
    flat_end = code.index('v-if="showScanPanel"', g_end)
    grouped = code[g_start:g_end]
    flat = code[g_end:flat_end]
    for token in ("publishResource", "createShare", "cancelShare",
                  "identify(", "openMatch", "setFinish"):
        assert token in grouped, f"分组视图缺少操作：{token}"
        assert token in flat, f"平铺视图缺少操作：{token}"
