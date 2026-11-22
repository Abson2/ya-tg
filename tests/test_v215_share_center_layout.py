"""v2.1.5 分享中心「缩小版」布局回归专项验收。

用户真机反馈（v2.1.4 发布后）：
  「2.14 版本分享中心里面，除开总览整体页面是铺满了，其余页面均是缩小版，
    尺寸与页面不符，比例失调」

根因（两层叠加）：
  ① **层叠顺序错误**：满宽类 .sc-board-full{grid-template-columns:minmax(0,1fr)}
     定义在 4516 行，而 .sc-board{display:grid;grid-template-columns:repeat(2,...)}
     定义在 4576 行——两者同特异度（单类选择器 0,1,0），**后者胜出**
     → 满宽规则完全失效，三页仍被塞进两列 Grid 的半格子里；
  ② **嵌套 Grid 二次收缩**：发布规则页在半幅容器内又套 .sc-rule-layout
     （两列）→ 每列只剩 1/4 页宽，卡片被压成「缩小版」。

为何总览页正常：总览用 .sc-dash{display:flex;flex-direction:column}
直接挂在 .sc-workspace 下，根本不经过 Grid。

v2.1.5 修法：三页移出 Grid，与总览页平级，由 .sc-subpage 独占整行满宽。

覆盖：
  1. DOM：三页挂在 .sc-subpage 下，不再挂在 .sc-board 两列 Grid 里；
  2. CSS：.sc-subpage 满宽定义存在；.sc-board 不再默认两列；
  3. CSS：**层叠顺序守卫**——若有人重新引入 .sc-board-full，
     必须保证它排在 .sc-board **之后**或提高特异度，否则满宽会再次失效；
  4. CSS：产出物中不得残留 .sc-board-full（与 CSS 层叠顺序守卫互补）；
  5. 发布规则页内部两列布局保留（基础规则 / 周期计划并列 + 排期表整行）。
"""
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = ROOT / "frontend" / "src" / "App.vue"


# ---------------------------------------------------------------------------
# 1. DOM 结构：三页必须挂在满宽容器下
# ---------------------------------------------------------------------------

def test_three_pages_mounted_on_full_width_container():
    """分享管理 / 发布规则 / 推送任务三页挂在 .sc-subpage 满宽容器下。"""
    app = APP_VUE.read_text(encoding="utf-8")
    # 剥掉 HTML 注释，避免把注释里的示例文本当成真实挂载点
    code = re.sub(r"<!--.*?-->", "", app, flags=re.S)
    assert '<div class="sc-subpage"' in code, "缺少满宽子页容器 .sc-subpage"
    assert 'class="sc-board sc-board-full"' not in code, \
        "三页仍挂在 .sc-board 两列 Grid 里（满宽会失效）"
    # 三页的挂载点必须都在 .sc-subpage 之后（同一容器内）
    idx_sub = code.index('<div class="sc-subpage"')
    for marker in ('class="sc-share-layout"', "scSubNav === '发布规则'",
                   'class="sc-task-layout"'):
        assert marker in code, f"缺少子页挂载点：{marker}"
        assert code.index(marker) > idx_sub, \
            f"子页不在满宽容器内：{marker}"


def test_subpage_is_sibling_of_dashboard():
    """三页容器与总览页 .sc-dash 平级（都挂在 .sc-workspace 下）。

    这是本次修复的**结构本质**：总览页本来就是满宽的，
    三页必须和它同级，不能被任何 Grid 容器再切一刀。
    """
    app = APP_VUE.read_text(encoding="utf-8")
    # 剥掉 HTML 注释，避免把注释里的示例文本当成真实挂载点
    code = re.sub(r"<!--.*?-->", "", app, flags=re.S)
    i_workspace = code.index('<div class="sc-workspace">')
    i_dash = code.index('<div class="sc-dash">')
    i_sub = code.index('<div class="sc-subpage"')
    assert i_workspace < i_dash < i_sub, \
        "总览页与三页容器必须同为 .sc-workspace 的直接子节点"


# ---------------------------------------------------------------------------
# 2. CSS 定义
# ---------------------------------------------------------------------------

def test_subpage_css_is_full_width():
    """.sc-subpage 必须满宽（display:block + width:100% + min-width:0）。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert ".sc-subpage{display:block;width:100%;min-width:0}" in app, \
        "满宽子页容器样式缺失或被改小"


def test_sc_board_no_longer_two_columns_by_default():
    """.sc-board 不得再默认两列（那正是「缩小版」的来源）。"""
    app = APP_VUE.read_text(encoding="utf-8")
    code = re.sub(r"/\*.*?\*/", "", app, flags=re.S)
    code = re.sub(r"<!--.*?-->", "", code, flags=re.S)
    m = re.search(r"\.sc-board\{([^}]*)\}", code)
    assert m, ".sc-board 定义缺失"
    body = m.group(1)
    assert "grid-template-columns:repeat(2" not in body, \
        ".sc-board 又变回两列 Grid，子页会被切半"
    assert "display:block" in body, ".sc-board 应为单列块级"


# ---------------------------------------------------------------------------
# 3. 层叠顺序守卫（本次回归的核心陷阱）
# ---------------------------------------------------------------------------

def test_no_sc_board_full_modifier_remains():
    """产出 CSS 中不得再出现 .sc-board-full。

    它的存在本身不是错，但历史上它定义在 .sc-board **之前**导致满宽失效。
    v2.1.5 已改用「三页移出 Grid」的修法，满宽修饰类一并清除，
    避免有人再把它挂回 Grid 而重蹈覆辙。

    同样必须先剥注释——源码注释里多处出现 ``.sc-board-full`` 作为历史说明。
    """
    app = APP_VUE.read_text(encoding="utf-8")
    code = re.sub(r"/\*.*?\*/", "", app, flags=re.S)
    code = re.sub(r"<!--.*?-->", "", code, flags=re.S)
    assert not re.search(r"\.sc-board-full\s*\{", code), \
        ".sc-board-full 满宽修饰类仍在（层叠顺序会再次失效）"


def test_if_full_width_modifier_reintroduced_it_must_win_cascade():
    """**层叠顺序守卫**：若将来重新引入满宽修饰类，必须排在 .sc-board 之后。

    同一特异度（单类选择器）下，后定义者胜出——这正是 v2.1.4 满宽失效的原因。
    本测试把这条隐性知识固化为可执行断言。

    注意：必须先剥掉 CSS/HTML 注释再匹配——注释里大量出现
    ``.sc-board-full`` 这个词（历史根因说明），正则贪婪匹配会误判成选择器。
    """
    app = APP_VUE.read_text(encoding="utf-8")
    # 剥掉注释，避免把说明文字当成选择器定义
    code = re.sub(r"/\*.*?\*/", "", app, flags=re.S)
    code = re.sub(r"<!--.*?-->", "", code, flags=re.S)
    mods = list(re.finditer(r"\.sc-board-full\s*\{", code))
    if not mods:
        return  # 当前无满宽修饰类，无需校验顺序
    for m in mods:
        # 提高特异度也算通过（.sc-board.sc-board-full / .sc-board-full.sc-board）
        line_start = code.rfind("\n", 0, m.start()) + 1
        selector = code[line_start:m.start()].strip()
        if selector.count(".") > 1:
            continue
        pos_board = code.find(".sc-board{")
        assert pos_board != -1 and m.start() > pos_board, \
            ".sc-board-full 与 .sc-board 同特异度，必须定义在 .sc-board **之后**，" \
            "否则满宽规则会被后者覆盖（v2.1.4 真机「缩小版」的根因）"


# ---------------------------------------------------------------------------
# 4. 页内布局：发布规则页改为竖排单列
# ---------------------------------------------------------------------------

def test_rule_page_keeps_inner_two_column_layout():
    """发布规则页内部改为**单列竖排**（基础规则 / 周期计划上下依次 + 排期表整行）。

    v2.1.5 时是 2 列 Grid 并排；v2.1.6 改为单列竖排——真机反馈两卡并排时
    宽度被压到半格、高度不齐，视觉上「不对齐」。单列后每张卡独占整行宽度，
    两卡上下依次排布、高度自然对齐。

    保留 ``.sc-rule-full{grid-column:1/-1}`` 这条：分类排期表在单列下
    仍应显式声明独占整行，避免将来改回多列时排期表被塞进半格。
    """
    app = APP_VUE.read_text(encoding="utf-8")
    assert ".sc-rule-layout{display:grid;grid-template-columns:minmax(0,1fr)" in app, \
        "发布规则页应为单列竖排"
    assert ".sc-rule-layout .sc-rule-full{grid-column:1/-1}" in app, \
        "分类排期表应独占整行"
    assert "repeat(2,minmax(0,1fr))" not in app.split(".sc-rule-layout{")[1][:80], \
        "发布规则页内部不应回退为两列并排"


def test_share_and_task_pages_single_column():
    """分享管理 / 推送任务两页单栏满宽（配合表格横向滚动）。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert ".sc-share-layout,.sc-task-layout{display:block}" in app, \
        "两页应单栏满宽"
    assert ".sc-table-scroll{overflow-x:auto;max-width:100%}" in app, \
        "表格容器应允许横向滚动，避免列被压扁"


# ---------------------------------------------------------------------------
# 5. 构建产出校验（若 dist 存在）
# ---------------------------------------------------------------------------

def test_built_css_has_no_two_column_trap():
    """若已构建，产出 CSS 不得含 .sc-board-full。

    这是**最终交付物**层面的守卫：源码改对了但构建产物没同步的情况会被抓住。
    """
    dist = ROOT / "frontend" / "dist" / "assets"
    if not dist.exists():
        pytest.skip("未构建，跳过产出 CSS 校验")
    css_files = sorted(dist.glob("index-*.css"))
    if not css_files:
        pytest.skip("未找到构建产物 CSS")
    text = css_files[-1].read_text(encoding="utf-8")
    assert "sc-board-full" not in text, \
        "构建产物仍含 .sc-board-full（满宽会再次失效）"
    assert ".sc-subpage" in text, "构建产物缺少 .sc-subpage 满宽容器"
    m = re.search(r"\.sc-board\{([^}]*)\}", text)
    assert m and "repeat(2" not in m.group(1), \
        "构建产物的 .sc-board 仍是两列"
