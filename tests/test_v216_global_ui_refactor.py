"""v2.1.6 #245：全局 UI 按分享中心风格重构 —— 门禁。

背景（审计结论）：
- 分享中心有独立的一套设计 token（--yg-radius / --yg-radius-sm / --yg-shadow /
  --yg-surface / --yg-line），其余 9 个页面却各自为政：5 套主体容器
  （.panel / .bot-card / .media-card / .method-card / 裸 div）、4 套数据卡、
  4 种空态写法、三套间距 → 同一屏里卡片体量参差。
- 分享中心 .sc-* 体系**零条** html.dark 覆盖，且散落 20 处硬编码浅色
  （#eef0ff / #e7f8f0 / #fff3e2 / #fdecef / #f0ebff / #e6f7f7）→ 深色下整页失配。
- `--yg-panel` 变量被 .sc-week-cell / .sc-inline-detail 引用却**从未定义**，
  背景声明不生效。

本文件把这些结论固化成**会失败**的断言，防止后续回退。
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = ROOT / "frontend/src/App.vue"


def _read() -> str:
    return APP_VUE.read_text(encoding="utf-8")


def _style_block(src: str) -> str:
    """取 <style> 块内容（CSS 注释保留，便于断言注释里写的口径说明）。"""
    m = re.search(r"<style[^>]*>(.*?)</style>", src, flags=re.S)
    assert m, "App.vue 缺少 <style> 块"
    return m.group(1)


def _strip_css_comments(src: str) -> str:
    """剥掉 CSS 注释，避免把注释里的历史说明误判成选择器定义。

    注意：不能对整份 App.vue 无脑用 ``re.sub(r"/\\*.*?\\*/", ...)``——
    JS 里的路径字符串（``/api/sc/*``）会被当成 CSS 注释起点，
    一路吞掉整个 ``<script setup>``。因此只对 ``<style>`` 块内的内容剥 CSS 注释。

    同时注意**调用顺序**：``_style_block()`` 返回的已经是纯 CSS（没有 ``<style>`` 标签），
    此时再用 ``<style...>...</style>`` 去包一层会匹配不到任何东西 → 注释根本没被剥掉。
    所以本函数要能直接处理「纯 CSS 片段」：先尝试按 style 块替换，
    匹配不到就退化为「直接剥 CSS 注释」。
    """
    src = re.sub(r"<!--.*?-->", "", src, flags=re.S)

    def _strip_style(m: re.Match) -> str:
        return re.sub(r"/\*.*?\*/", "", m.group(0), flags=re.S)

    stripped = re.sub(r"<style[^>]*>.*?</style>", _strip_style, src, flags=re.S)
    if stripped == src and "<style" not in src:
        # 传入的是纯 CSS 片段（已取出的 style 块）——直接剥 CSS 注释
        stripped = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return stripped


def _style_code() -> str:
    """取 <style> 块并剥掉 CSS 注释（供「规则体 / 字面量」类断言使用）。"""
    return _strip_css_comments(_style_block(_read()))


def _rule_body(code: str, selector: str) -> str:
    """取某选择器的规则体。

    ``selector`` **必须带结尾的 ``{``**（调用方直接写 ``".panel{"``），
    这里只补规则体部分——不要在这里再拼一个 ``\\{``，
    否则会变成 ``\\.panel\\{\\{…\\}`` 而永远匹配不到。

    注意：不能用 ``code.find(selector)`` 截断到下一个 ``}``——
    同一选择器可能在文件里出现多次（例如 ``.panel{`` 与媒体查询里的 ``.panel{``），
    截断位置与真实规则体可能错位。正则要求「选择器紧跟 ``{``」再取到配对的 ``}``。
    """
    m = re.search(re.escape(selector) + r"([^}]*)\}", code)
    assert m, f"未找到选择器 {selector} 的规则体"
    return m.group(1)


def _rule_bodies(code: str, selector: str) -> list:
    """返回某选择器的**全部**规则体（用于「至少一条满足」类断言）。

    有些选择器在文件里有多条定义（例如 ``.panel{`` 既在基础档出现，
    也会以后代选择器形式出现，如 ``.link-modules .panel{``）——只取首条会误判，
    此时应断言「至少一条含目标声明」而不是「首条含目标声明」。
    """
    return re.findall(re.escape(selector) + r"([^}]*)\}", code)


# ---------- ① token 体系 ----------

def test_semantic_tone_tokens_defined_in_light_root():
    """浅色 :root 必须定义全部 7 组语义色 token（底色 + 字色成对）。

    注意：文件里只有一个 :root 块（主变量块）——v2.1.6 #245 已把原先放在
    深色主题段里的浅色兜底变量（--yg-border / --yg-panel-soft）上移回主 :root，
    深色段只保留 html.dark 覆盖值。这里取**第一个**（也是唯一那个）主变量块。
    """
    src = _read()
    roots = re.findall(r":root\{(.*?)\}", src, flags=re.S)
    assert roots, "缺少 :root 变量块"
    root = roots[0]
    for tone in ("blue", "green", "amber", "red", "violet", "teal", "slate"):
        assert f"--yg-tone-{tone}-bg:" in root, f"浅色根缺少 --yg-tone-{tone}-bg"
        assert f"--yg-tone-{tone}-fg:" in root, f"浅色根缺少 --yg-tone-{tone}-fg"


def test_semantic_tone_tokens_defined_in_dark_root():
    """深色 html.dark 必须覆盖同一组 token，否则深色下语义色仍是浅色值。"""
    src = _read()
    m = re.search(r"html\.dark\{(.*?)\n\}", src, flags=re.S)
    assert m, "缺少 html.dark 变量块"
    dark = m.group(1)
    for tone in ("blue", "green", "amber", "red", "violet", "teal", "slate"):
        assert f"--yg-tone-{tone}-bg:" in dark, f"深色根缺少 --yg-tone-{tone}-bg"
        assert f"--yg-tone-{tone}-fg:" in dark, f"深色根缺少 --yg-tone-{tone}-fg"


def test_yg_panel_variable_actually_defined():
    """--yg-panel 被 .sc-week-cell / .sc-inline-detail 引用，此前从未定义。"""
    src = _read()
    roots = re.findall(r":root\{(.*?)\}", src, flags=re.S)
    assert roots, "缺少 :root 变量块"
    assert "--yg-panel:" in roots[0], "浅色根缺少 --yg-panel"
    m = re.search(r"html\.dark\{(.*?)\n\}", src, flags=re.S)
    assert m and "--yg-panel:" in m.group(1), "深色根缺少 --yg-panel"


def test_light_background_variable_preserved():
    """深色是附加层：浅色默认变量 --yg-bg:#f5f6fb 必须逐字保留。"""
    assert "--yg-bg:#f5f6fb" in _read(), "浅色默认变量必须保留"


# ---------- ② 硬编码浅色 token 化 ----------

def test_hardcoded_tint_literals_removed_from_share_center():
    """分享中心 .sc-* 体系不得再出现硬编码浅色底。"""
    code = _style_code()
    sc_rules = re.findall(r"([^{}]*\.sc-[^{}]*)\{([^{}]*)\}", code)
    assert sc_rules, "未取到 .sc-* 规则"
    for literal in ("#eef0ff", "#e7f8f0", "#fff3e2", "#fdecef", "#f0ebff", "#e6f7f7"):
        offenders = [sel for sel, body in sc_rules if literal in body]
        assert not offenders, f"分享中心仍有硬编码浅色 {literal}：{offenders[:2]}"


def test_hardcoded_tint_literals_removed_from_global_cards():
    """全局数据卡 / 状态标签不得再出现硬编码浅色底。

    断言范围是**页面规则体**，需先剔除两块变量定义块——
    ``:root{…}`` 与 ``html.dark{…}`` 里当然含这些色值（它们是 token 的「值」），
    但那不是页面里的硬编码。若不剔除，通用正则会把变量块当普通规则捕获而假红。
    """
    code = _style_code()
    code = re.sub(r":root\{[^}]*\}", "", code)
    code = re.sub(r"html\.dark\{[^}]*\}", "", code)
    rules = re.findall(r"([^{}]*)\{([^{}]*)\}", code)
    assert rules, "未取到 CSS 规则"
    for literal in ("#eef0ff", "#e7f8f0", "#fff3e2", "#fdecef", "#f0ebff", "#e6f7f7"):
        offenders = [sel for sel, body in rules if literal in body]
        assert not offenders, f"仍有硬编码浅色 {literal}（应走 --yg-tone-* token）：{offenders[:2]}"


def test_key_cards_use_surface_variable():
    """主体容器底色一律走 var(--yg-surface)，不再写死 #fff。

    用「全部规则体 + 至少一条满足」而非「首条满足」——
    同一选择器可能在多处被定义（如 ``.panel{`` 基础档 + ``.link-modules .panel{``），
    取首条会误判。
    """
    code = _style_code()
    for selector in (".panel{", ".bot-card{", ".media-card{", ".method-card{",
                     ".auth-summary-item{", ".mini-stat{", ".cmd-item{",
                     ".cstep{", ".webqr-box{", ".preview-box{", ".row-btn{"):
        bodies = _rule_bodies(code, selector)
        assert bodies, f"缺少选择器 {selector}"
        assert any("var(--yg-surface)" in b for b in bodies), \
            f"{selector} 底色未走 var(--yg-surface)"
        assert not any("background:#fff" in b for b in bodies), \
            f"{selector} 仍写死 #fff"


# ---------- ③ 主体容器 / 数据卡 / 空态统一 ----------

def test_main_containers_share_sc_card_baseline():
    """主体容器统一到 .sc-card 基准：圆角 var(--yg-radius) + 阴影 var(--yg-shadow)。"""
    code = _style_code()
    for selector in (".panel{", ".bot-card{", ".method-card{", ".media-card{"):
        bodies = _rule_bodies(code, selector)
        assert bodies, f"缺少选择器 {selector}"
        assert any("var(--yg-radius" in b for b in bodies), \
            f"{selector} 圆角未走 --yg-radius* token"
        assert any("var(--yg-shadow)" in b for b in bodies), \
            f"{selector} 阴影未走 var(--yg-shadow)"


def test_panel_padding_matches_sc_card_baseline():
    """.panel 内边距与 .sc-card 对齐（20px 22px）。"""
    code = _style_code()
    bodies = _rule_bodies(code, ".panel{")
    assert bodies, "缺少 .panel"
    assert any("padding:20px 22px" in b for b in bodies), ".panel 内边距未与 .sc-card 对齐"


def test_empty_states_unified():
    """空态只有一套基础口径：.empty 定义基础态，.empty.big 只放大留白。

    注意计数用**负向后视**排除后代选择器——``.sc-three-tables>.sc-card>.empty{``
    里也含 ``.empty{`` 子串，但那是「三表行里的空态撑满」修饰规则（更高特异度），
    不是重复定义。裸 ``re.findall(r"\\.empty\\{", code)`` 会把它算进去而假红。
    """
    code = _style_code()
    body = _rule_body(code, ".empty{")
    assert "font-size:13px" in body, ".empty 基础态未统一字号"
    big_body = _rule_body(code, ".empty.big{")
    assert "padding:60px" in big_body, ".empty.big 留白口径变化"
    # 不允许同一选择器被定义两次（旧定义残留会导致「后定义胜出」的隐性 bug）
    assert len(re.findall(r"(?<![\w>.-])\.empty\{", code)) == 1, ".empty 被重复定义"
    assert len(re.findall(r"(?<![\w>.-])\.empty-hint\{", code)) == 1, ".empty-hint 被重复定义"
    assert len(re.findall(r"(?<![\w>.-])\.empty\.big\{", code)) == 1, ".empty.big 被重复定义"


def test_min_width_zero_literal_still_present():
    """全仓保留至少一处字面 min-width:0（既有契约断言是裸串匹配）。"""
    assert "min-width:0" in _read(), "字面 min-width:0 丢失"


# ---------- ④ 深色模式覆盖补齐 ----------

def test_share_center_has_dark_overrides():
    """分享中心 .sc-* 此前零条 html.dark 覆盖，深色下整页失配。"""
    code = _style_code()
    # 用 ``[^{}]`` 排除 ``}``，避免一条匹配把后续规则的选择器部分也吞进来
    dark_rules = re.findall(r"([^{}]*html\.dark[^{}]*)\{([^{}]*)\}", code)
    assert dark_rules, "未取到 html.dark 规则"
    hit = [sel for sel, _ in dark_rules if ".sc-" in sel]
    assert len(hit) >= 5, f"分享中心深色覆盖过少（{len(hit)} 条），深色下仍会失配"


def test_dark_overrides_cover_share_center_structure():
    """分享中心的卡片容器、表格、徽标在深色下必须有对应覆盖。"""
    code = _style_code()
    for token in ("html.dark .sc-card", "html.dark .sc-tab",
                  "html.dark .sc-detail-kv>div", "html.dark .sc-week-cell"):
        assert token in code, f"缺少深色覆盖：{token}"


def test_dark_overrides_cover_tinted_containers():
    """其余页面的带底色容器在深色下必须有对应覆盖。"""
    code = _style_code()
    for token in ("html.dark .connect-hero", "html.dark .credential-bar",
                  "html.dark .restrict-card", "html.dark .device-panel",
                  "html.dark .match-path", "html.dark .match-card"):
        assert token in code, f"缺少深色覆盖：{token}"


def test_dark_theme_toggle_still_present():
    """深色主题是附加层：切换逻辑与 html.dark 类不得被删。"""
    src = _read()
    assert "html.dark{" in src, "需保留 html.dark 变量块"
    assert "classList.toggle('dark'" in src, "需保留主题切换逻辑"
    assert "ya_tg_theme" in src, "主题偏好需持久化"


# ---------- ⑤ 功能零回归（红线） ----------

def test_all_tabs_preserved():
    """全部 tab 一个不能少 + 分享中心 4 个子页。

    注意：主导航实际有 **10 个** tab（概览 / 任务中心 / 光鸭授权 / 镜像同步 /
    资源库 / 推送队列(显示为「分享中心」) / 分发与互动中心 / TMDB 设置 /
    消息模板 / 操作审计）——「任务中心」也在其中，别漏。
    """
    src = _read()
    for label in ["概览", "任务中心", "光鸭授权", "镜像同步", "资源库",
                  "推送队列", "分发与互动中心", "TMDB 设置", "消息模板", "操作审计"]:
        assert label in src, f"tab 丢失：{label}"
    for label in ["发布总览", "发布规则", "推送任务", "分享管理"]:
        assert label in src, f"分享中心子页丢失：{label}"


def test_key_action_entries_preserved():
    """危险操作二次确认 / 镜像批次控制 / 分享中心核心入口不得被删。"""
    src = _read()
    assert "ElMessageBox.confirm" in src, "危险操作二次确认丢失"
    for action in ("pause", "resume", "skip", "cancel"):
        assert f"batchControl('{action}')" in src, f"镜像批次控制丢失：{action}"
    for token in ("sc-tabbar", "sc-card", "sc-board", "sc-subpage"):
        assert token in src, f"分享中心结构类名丢失：{token}"


def test_share_center_layout_baseline_not_regressed():
    """v2.1.5 的满宽布局基线不得在本次重构中被破坏。"""
    src = _read()
    assert ".sc-subpage{display:block;width:100%;min-width:0}" in src, "满宽子页容器样式缺失"
    assert ".sc-share-layout,.sc-task-layout{display:block}" in src, "两页单栏满宽样式缺失"
    assert not re.search(r"\.sc-board-full\s*\{", _strip_css_comments(src)), \
        ".sc-board-full 满宽修饰类又被加回（层叠顺序会再次失效）"


def test_build_artifact_keeps_new_tokens():
    """若构建产物存在且比源码**新**，产出 CSS 里必须带上新 token。

    注意：``frontend/dist`` 已被 .gitignore 忽略，是本地构建产物。
    产物可能早于源码（改了源码还没重新 build）——那不是源码缺陷，
    此时跳过断言，只在「产物已是最新」时才校验，避免假红。
    """
    assets = ROOT / "frontend/dist/assets"
    if not assets.exists():
        return
    css_files = sorted(assets.glob("index-*.css"))
    if not css_files:
        return
    css = css_files[-1]
    src_mtime = APP_VUE.stat().st_mtime
    if css.stat().st_mtime < src_mtime:
        # 产物比源码旧 → 需要重新构建，本断言跳过（由构建步骤保证一致性）
        return
    text = css.read_text(encoding="utf-8")
    assert "--yg-tone-blue-bg" in text, "构建产物缺少语义色 token（需重新构建）"
    assert "sc-board-full" not in text, "构建产物仍有 .sc-board-full"
