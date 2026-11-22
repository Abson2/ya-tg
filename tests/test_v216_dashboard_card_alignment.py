"""v2.1.6 发布总览卡片尺寸对齐专项验收。

用户口径（Task #243）：发布总览页的卡片尺寸要对齐——
同一行卡片上下边缘齐平、KPI 卡与页面其它卡片视觉体量一致。

事实基础：
  ① `.sc-dash-three` 此前显式 `align-items:start` → 同一行三张卡各自按内容高度收缩，
     上下边缘参差不齐（趋势图固定 160px / 占比图 130px / 热门候选随行数变）；
  ② 六张 KPI 卡此前被单独压小（padding:14px / ico 38px / 数字 21px），
     与页面其它卡片的体量不一致 —— 用户口中的「尺寸与页面不符、比例失调」。

覆盖：
  1. 三栏行不再用 align-items:start（必须 stretch 等高）；
  2. 卡片改纵向 flex、内容区撑满（趋势 / 占比 / 热门候选三区各自撑满）；
  3. 热门候选底部按钮贴卡片底部（margin-top:auto）；
  4. 三表行空态占满剩余高度；
  5. KPI 六卡不再被压成「缩小版」（padding / 图标 / 数字与基础档一致）；
  6. 不得给 el-table 加 flex:1（el-table 依赖显式 height，强行撑高会塌陷）。
"""
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = ROOT / "frontend" / "src" / "App.vue"


def _css() -> str:
    """取源码（保留 CSS 注释，供规则定位用）。"""
    return APP_VUE.read_text(encoding="utf-8")


def _strip_comments(src: str) -> str:
    """剥掉 HTML 注释与 **CSS 注释**。

    注意：不能无脑用 ``re.sub(r"/\\*.*?\\*/", ...)`` 剥全文件——
    JS 里的路径字符串（``/api/sc/*``）会被当成 CSS 注释起点，
    一路吞掉整个 ``<script setup>``。因此只对 ``<style>`` 块内的内容剥 CSS 注释。
    """
    src = re.sub(r"<!--.*?-->", "", src, flags=re.S)

    def _strip_style(m: re.Match) -> str:
        return re.sub(r"/\*.*?\*/", "", m.group(0), flags=re.S)

    return re.sub(r"<style[^>]*>.*?</style>", _strip_style, src, flags=re.S)


# ---------------------------------------------------------------------------
# 1. 三栏行等高
# ---------------------------------------------------------------------------

def test_dash_three_rows_are_stretch_not_start():
    """三栏行必须 stretch（同行卡片等高），不得再 align-items:start。"""
    code = _strip_comments(_css())
    m = re.search(r"\.sc-dash-three\{([^}]*)\}", code)
    assert m, ".sc-dash-three 定义缺失"
    body = m.group(1)
    assert "align-items:start" not in body, \
        ".sc-dash-three 又回到 align-items:start，同行卡片高度会参差"
    assert "align-items:stretch" in body, ".sc-dash-three 未启用 stretch 等高"


def test_dash_cards_are_flex_column():
    """三栏卡片改纵向 flex，标题头固定高、内容区吃掉剩余空间。"""
    code = _strip_comments(_css())
    assert ".sc-dash-three>.sc-card{display:flex;flex-direction:column}" in code, \
        "三栏卡片未改纵向 flex（内容区无法撑满）"
    assert ".sc-dash-three>.sc-card>.sc-card-head{flex:none}" in code, \
        "卡片标题头应固定高度（flex:none）"


# ---------------------------------------------------------------------------
# 2. 三个内容区各自撑满
# ---------------------------------------------------------------------------

def test_three_content_areas_fill_card_height():
    """趋势图 / 占比图 / 热门候选三区各自撑满卡片剩余高度。"""
    code = _strip_comments(_css())
    assert ".sc-dash-three .sc-trend-wrap{flex:1" in code, "趋势图区未撑满"
    assert ".sc-dash-three .sc-trend-svg{flex:1" in code, "趋势图绘图区未随卡片等高"
    assert ".sc-dash-three .sc-ratio-wrap{flex:1" in code, "占比图区未撑满"
    assert ".sc-dash-three .sc-hot-list{flex:1" in code, "热门候选列表未撑满"


def test_hot_more_button_pinned_to_card_bottom():
    """「查看候选详情」贴卡片底部（与同排卡片底部对齐）。"""
    code = _strip_comments(_css())
    assert ".sc-dash-three .sc-hot-more{margin-top:auto" in code, \
        "热门候选底部按钮未贴底（margin-top:auto 缺失）"
    # 基础档不应再带固定 margin-top（会与 auto 冲突）。
    # 注意：正则里的 '.' 会通配前缀，`.sc-hot-more{` 会匹配到
    # `.sc-dash-three .sc-hot-more{` —— 必须用负向断言锁定「基础档」这一条，
    # 且前驱字符集要**排除空格**（否则三栏档的前驱空格会让 lookbehind 通过）。
    m = re.search(r"(?<![\w.\- ])\.sc-hot-more\{([^}]*)\}", code)
    assert m, ".sc-hot-more 基础样式缺失"
    assert "margin-top" not in m.group(1), \
        ".sc-hot-more 基础档仍带固定 margin-top，会盖掉贴底规则"


def test_three_tables_empty_state_fills_remaining_height():
    """三表行空态提示占满剩余高度（与表格底部对齐）。"""
    code = _strip_comments(_css())
    assert ".sc-three-tables>.sc-card>.empty{flex:1" in code, \
        "三表行空态未撑满剩余高度"


# ---------------------------------------------------------------------------
# 3. KPI 六卡不再缩小版
# ---------------------------------------------------------------------------

def test_kpi_six_not_shrunk_version():
    """KPI 六卡不再被压成「缩小版」。"""
    code = _strip_comments(_css())
    m = re.search(r"\.sc-kpi-six \.sc-kpi\{([^}]*)\}", code)
    assert m, ".sc-kpi-six .sc-kpi 规则缺失"
    body = m.group(1)
    assert "padding:14px" not in body, \
        "KPI 六卡仍是缩小版（padding:14px）"
    m2 = re.search(r"\.sc-kpi-six \.sc-kpi-ico\{([^}]*)\}", code)
    assert m2, ".sc-kpi-six .sc-kpi-ico 规则缺失"
    assert "width:38px" not in m2.group(1), \
        "KPI 六卡图标仍是缩小版（38px）"
    m3 = re.search(r"\.sc-kpi-six \.sc-kpi-body b\{([^}]*)\}", code)
    assert m3, ".sc-kpi-six .sc-kpi-body b 规则缺失"
    assert "font-size:21px" not in m3.group(1), \
        "KPI 六卡数字仍是缩小版（21px）"


def test_kpi_six_has_min_height():
    """KPI 六卡设最小高度，六卡高度天然齐平（grid 行内 stretch）。"""
    code = _strip_comments(_css())
    m = re.search(r"\.sc-kpi-six \.sc-kpi\{([^}]*)\}", code)
    assert m and "min-height" in m.group(1), \
        "KPI 六卡缺少 min-height（六卡高度无法齐平）"


# ---------------------------------------------------------------------------
# 4. 反向守卫：不得给 el-table 强行 flex:1
# ---------------------------------------------------------------------------

def test_no_flex_on_el_table():
    """不得给 el-table 加 flex:1。

    el-table 的内部滚动区依赖显式 height；用 flex:1 撑高会让行数少的表格塌陷，
    反而制造新的尺寸不一致。
    """
    code = _strip_comments(_css())
    assert not re.search(r"\.el-table\s*\{[^}]*flex:1", code), \
        "不得给 .el-table 加 flex:1"
    assert not re.search(r"\.sc-dash-three>\.sc-card>\.el-table\{[^}]*flex:1", code), \
        "不得给三表行的 el-table 加 flex:1"


# ---------------------------------------------------------------------------
# 5. 产出物校验
# ---------------------------------------------------------------------------

def test_built_css_keeps_stretch():
    """若构建产物存在且比源码**新**，产出 CSS 必须保留 align-items:stretch。

    注意：``frontend/dist`` 已被 .gitignore 忽略，是本地构建产物。
    产物可能早于源码（改了源码还没重新 build）——那不是源码缺陷，
    此时跳过断言，只在「产物已是最新」时才校验，避免假红。
    """
    dist = ROOT / "frontend" / "dist" / "assets"
    if not dist.exists():
        pytest.skip("未构建，跳过产出 CSS 校验")
    css_files = sorted(dist.glob("index-*.css"))
    if not css_files:
        pytest.skip("未找到构建产物 CSS")
    css = css_files[-1]
    src_mtime = APP_VUE.stat().st_mtime
    if css.stat().st_mtime < src_mtime:
        pytest.skip("构建产物比源码旧，需重新 build 后再校验")
    text = css.read_text(encoding="utf-8")
    m = re.search(r"\.sc-dash-three\{([^}]*)\}", text)
    assert m, "产出 CSS 缺 .sc-dash-three"
    assert "align-items:start" not in m.group(1), \
        "产出 CSS 的 .sc-dash-three 又变回 align-items:start"
