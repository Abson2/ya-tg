"""v2.1.30：全局 UI 统一（基准 = 分享中心），只改样式、不动功能。

用户口径（原话）：
  「保证100%功能的前提下，对全局ui参考分享中心的ui进行重构，使整体交互统一
    （布局、按钮、展示方式可以优化，不动功能）」

为什么是「一套样式」而不是「重排 10 个页面的模板」：
  `.panel` 42 处、`.panel-head` 38 处、`.field` 54 处、`.hint` 91 处 —— 这些原语
  是全站共用的，改一次样式 10 个页面同时对齐；而重排 2000 行模板是碰坏功能的主要
  风险来源。所以本版的做法是：**在样式表末尾追加一个统一层**，把旧原语与分享中心
  原语收敛到同一套 token，模板与脚本一行不动。

本文件锁死：
  ① 统一层存在，且页标题/卡片头/统计数字/筛选条/文字按钮五类原语被收敛；
  ② 页标题与卡片头的关键数值与分享中心**完全相等**（不是「差不多」）——以后谁把
     分享中心改了而没同步旧页面，这里会红；
  ③ 统一层写在所有媒体查询之后，必须自己重申窄屏值（否则等于把手机端改回 PC 值）；
  ④ 空态（.empty）**明确不在统一范围内**：它本来就是全站唯一一份，重复定义会触发
     v2.1.6 的「.empty 只许定义一次」守卫，还会把基准一起改了；
  ⑤ 反证：功能与结构没被顺手删——10 个 section 都在、旧原语仍在使用、
     分享中心自己的原语没被改坏。
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _app() -> str:
    return (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


def _css() -> str:
    app = _app()
    return app[app.index("<style>"):]


def _strip_media(css: str) -> str:
    """去掉所有 @media 块，只留基础层规则。

    为什么必须这么做：同一个选择器在媒体查询里还有一份窄屏值（例如
    `.sc-card-head h3` 在 ≤430 是 14px），直接「取最后一次出现」会拿到手机端
    的值，把「两处字号是否相等」这类断言比错对象。
    """
    out, i = [], 0
    pat = re.compile(r"@media[^{]*\{")
    while True:
        m = pat.search(css, i)
        if not m:
            out.append(css[i:])
            break
        out.append(css[i:m.start()])
        depth, j = 1, m.end()
        while depth:
            if css[j] == "{":
                depth += 1
            elif css[j] == "}":
                depth -= 1
            j += 1
        i = j
    return "".join(out)


def _tpl() -> str:
    app = _app()
    return app[app.index("<template>"):app.rindex("</template>")]


def _rule(css: str, selector: str) -> str:
    """取基础层某选择器块的声明（取**最后一次**出现，即后写生效的那条）。

    只认「行首 / } 之后 / ; 之后」出现的完整选择器：`.sc-kpi-body b` 不能匹配到
    `.sc-kpi-six .sc-kpi-body b`（那是另一档值），否则比出来的数是错的。
    """
    base = _strip_media(css)
    pat = re.compile(r"(?:^|[}\n;])\s*" + re.escape(selector) + r"\{")
    hits = list(pat.finditer(base))
    assert hits, f"找不到规则 {selector}"
    start = hits[-1].end()
    end = base.index("}", start)
    return base[start:end]


def _px(block: str, prop: str) -> float:
    """取声明的数值。px 可省 —— font-weight / line-height 是无单位数。"""
    m = re.search(prop + r":(-?[\d.]+)", block)
    assert m, f"{prop} 里没有数值：{block[:120]}"
    return float(m.group(1))


# ==========================================================================
# ① 统一层存在
# ==========================================================================
def test_unify_layer_exists():
    css = _css()
    assert "v2.1.30 全局 UI 统一（基准 = 分享中心）" in css, (
        "缺少统一层 —— 各页又会回到「分享中心一套、旧页面一套」")
    # 统一层必须是**最后**一段（否则被后面的旧定义覆盖，等于白写）
    base = _strip_media(css)
    layer = base.index("v2.1.30 全局 UI 统一")
    for legacy in (".panel-head h3{", ".page-head h2,.sc-topbar h2{",
                   ".stat-num{", ".status-tabs{"):
        assert base.rfind(legacy) > layer, f"{legacy} 的生效定义不在统一层里"


# ==========================================================================
# ② 与分享中心数值完全相等（不是「差不多」）
# ==========================================================================
def test_panel_head_equals_sc_card_head():
    css = _css()
    panel = _rule(css, ".panel-head h3")
    card = _rule(css, ".sc-card-head h3")
    assert _px(panel, "font-size") == _px(card, "font-size"), (
        "卡片头标题字号两套了 —— 分享中心变了这里没跟上")
    assert _px(panel, "font-weight") == _px(card, "font-weight")
    # 旧的 24px 深紫渐变方块必须换掉（分享中心图标块是 30px 软底）
    icon = _rule(css, ".panel-head h3::before")
    assert _px(icon, "width") == 30
    assert "linear-gradient" not in icon, "图标块还在用深紫渐变，与分享中心的软底方块不是一家人"
    assert "--yg-tone-violet-bg" in icon


def test_page_head_equals_sc_topbar():
    css = _css()
    # 两个选择器写在同一条规则里，从根上避免以后各自长歪
    assert ".page-head,.sc-topbar{" in css
    head = _rule(css, ".page-head,.sc-topbar")
    assert _px(head, "padding-bottom") == 14
    h2 = _rule(css, ".page-head h2,.sc-topbar h2")
    assert _px(h2, "font-size") == 21
    assert _px(h2, "font-weight") == 770
    # 旧页标题左侧那条 5px 竖条撤掉（分享中心是基准，它没有）
    assert "display:none" in _rule(css, ".page-head h2::before")


def test_stat_num_equals_sc_kpi():
    css = _css()
    assert _px(_rule(css, ".stat-num"), "font-size") == \
           _px(_rule(css, ".sc-kpi-body b"), "font-size"), (
        "概览 6 卡与分享中心 6 KPI 的数字字号不一致，两页并排看就是两套设计")


# ==========================================================================
# ③ 筛选条 / 文字按钮 / 空态
# ==========================================================================
def test_status_tabs_become_pill_group():
    css = _css()
    box = _rule(css, ".status-tabs")
    assert _px(box, "padding") == 6, "筛选条要像 .sc-tabbar 那样「一条浅槽里装胶囊」"
    on = _rule(css, ".status-tab-group button.on")
    assert "linear-gradient" in on, "选中态要用主色软底渐变（.sc-tab.active 同款）"
    assert "--yg-primary" in on
    assert "background:var(--yg-primary)" not in on, "别再回到「实心主色」那种旧观感"


def test_link_buttons_merged():
    css = _css()
    assert ".link-btn,.sc-link-btn{" in css, (
        "任务中心的「查看明细」与分享中心的文字按钮要同一套")


def test_empty_not_redefined():
    """空态**故意不动** —— 这是一条反向断言。

    `.empty` 本来就是全站唯一一份（分享中心三表用的也是它，v2.1.6 已做过空态统一）。
    统一层再补一条同选择器会：① 触发 v2.1.6 的「.empty 只许定义一次」守卫
    （防「后定义胜出」的隐性覆盖）；② 把基准（分享中心）一起改了 —— 统一层不该
    反向影响基准。第一版就是这么写错的，全量被 test_v216 抓了个正着。
    """
    css = _css()
    hits = re.findall(r"(?<![\w>.-])\.empty\{", css)
    assert len(hits) == 1, (
        f".empty 被定义了 {len(hits)} 次 —— 统一层不许重复定义它（会与 v2.1.6 守卫冲突）")
    assert "font-size:13px" in _rule(css, ".empty"), "空态基础口径不应被改动"


# ==========================================================================
# ④ 表格/控件 token
# ==========================================================================
def test_el_table_header_uses_theme_token():
    css = _css()
    block = _rule(css, ".el-table")
    assert "--el-table-header-bg-color:var(--yg-bg)" in block, (
        "表头底色要跟主题走，否则深色主题下表头是白的")
    assert "--el-table-row-hover-bg-color" in block


def test_control_radius_unified():
    css = _css()
    assert ".el-input__wrapper,.el-select__wrapper,.el-textarea__inner{border-radius:10px}" in css
    assert ".el-button{border-radius:10px" in css


# ==========================================================================
# ⑤ 窄屏不能被统一层改回去
# ==========================================================================
def test_unify_layer_repeats_narrow_screen():
    css = _css()
    layer = css.index("v2.1.30 全局 UI 统一")
    tail = css[layer:]
    assert "@media(max-width:680px){" in tail, (
        "统一层写在所有媒体查询之后，不重申窄屏值就等于把手机端改回 PC 值")
    narrow = tail[tail.index("@media(max-width:680px){"):]
    assert ".panel{padding:15px}" in narrow
    assert ".page-head h2,.sc-topbar h2{font-size:19px}" in narrow


# ==========================================================================
# ⑥ 反证：功能与结构一个都没少
# ==========================================================================
def test_all_sections_still_present():
    tpl = _tpl()
    for tab in ("概览", "任务中心", "光鸭授权", "镜像同步", "资源库", "推送队列",
                "分发与互动中心", "TMDB 设置", "消息模板", "操作审计"):
        assert f"tab === '{tab}'" in tpl, f"{tab} 页面不见了"


def test_legacy_primitives_still_in_use():
    """统一是「改外观」，不是「删原语改模板」——反证模板没被大改。

    阈值取当前实际数量往下留余量（现在 class="panel" 29 处、panel-head 38 处）：
    掉到阈值以下说明有人在「重构 UI」时顺手重排了模板，那属于功能风险。
    """
    tpl = _tpl()
    assert tpl.count('class="panel"') >= 25, "面板数量骤减，疑似模板被重排"
    assert tpl.count("panel-head") >= 30
    assert 'class="status-tabs"' in tpl
    assert 'class="info-chips"' in tpl


def test_share_center_primitives_intact():
    css = _css()
    for sel in (".sc-card{", ".sc-card-head{", ".sc-kpi{", ".sc-tabbar{",
                ".sc-tab.active{", ".sc-three-tables"):
        assert sel in css, f"分享中心原语 {sel} 被改没了"
    # 分享中心的卡片头仍是它自己的字号（没有被统一层顺手改掉）
    assert "font-size:15px" in _rule(css, ".sc-card-head h3")
