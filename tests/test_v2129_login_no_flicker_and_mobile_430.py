"""v2.1.29：① 刷新不再闪登录页 ② 以 iPhone 15 Pro（393×852）为基准的手机适配。

用户口径（原话）：
  「1、刷新前端页面瞬间调登录页面，第一次进入页面会先进界面再调整登录页
    2、全局手机以 iphone15pro（浏览器 edg 手机版）为参考优化手机显示界面」

① 闪烁的根因：`authed` 初值恒为 `false`，首帧必然渲染登录页分支，
   等 `onMounted` 里 `await load()` 回来才切主界面 —— 用户看到的就是「闪一下再跳」。
   修法：`authed` 改三态（`null` 恢复中 / `true` 已登录 / `false` 未登录），
   恢复期只渲染中性骨架屏。本文件锁死：
     · 初值必须「有 token 才给 null」，没 token 直接 false（不抖）；
     · 三个分支的渲染顺序：骨架 → 登录页 → 主界面；
     · 恢复失败分支**必须显式**落 false（否则永远停在骨架屏，比闪烁更糟）；
     · 反证：登录页与主界面的内容一个都没少（别为了不闪把登录框砍了）。

② 手机适配：≤430 一档覆盖 iPhone 15 Pro / 15 Pro Max，只调布局不动结构。
   本文件锁死：
     · viewport 带 `viewport-fit=cover`（不带则 `env(safe-area-inset-*)` 恒为 0）；
     · 不写 `user-scalable=no`（允许放大，可访问性底线）；
     · `100vh` 后面必须跟 `100dvh` 兜底（移动端地址栏收放会截断底部）；
     · 三表在手机档**仍然 6 列**（不许用「列太多」当理由删列）。
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _app() -> str:
    return (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


def _index_html() -> str:
    return (ROOT / "frontend/index.html").read_text(encoding="utf-8")


def _script(app: str) -> str:
    """只取 <script setup> 段，避免把模板/样式里的片断当成逻辑判断。"""
    return app[:app.index("<template>")]


def _mobile_block(app: str) -> str:
    """取 ≤430 手机档的整段 CSS。"""
    anchor = "@media (max-width:430px){"
    assert anchor in app, "缺少 ≤430 手机档 —— iPhone 15 Pro（393px）没有专门适配"
    seg = app[app.index(anchor):]
    return seg[:seg.index("\n}\n")]


# ==========================================================================
# ① 登录页不再闪
# ==========================================================================
def test_authed_starts_as_tri_state():
    app = _script(_app())
    assert "const authed = ref(sessionToken.value ? null : false)" in app, (
        "authed 必须以「有没有 token」决定初值："
        "没 token → false（首帧就是登录页，不抖）；有 token → null（恢复中，先渲染骨架屏）")


def test_boot_screen_rendered_before_login_and_app():
    app = _app()
    tpl = app[app.index("<template>"):]
    boot = tpl.index('v-if="authed === null"')
    login = tpl.index('v-else-if="authed === false"')
    shell = tpl.index('v-else class="app-shell"')
    assert boot < login < shell, "三个分支的顺序必须是 骨架屏 → 登录页 → 主界面"
    # 骨架屏必须有自己的容器类（不是复用登录页，否则等于没修）
    assert tpl[boot:login].count("boot-screen") == 1
    assert "正在恢复会话" in tpl[boot:login]


def test_restore_failure_falls_to_login():
    """改三态后必须自己收口：旧布尔版靠初值 false 兜底，三态版漏写就卡在骨架屏。"""
    app = _script(_app())
    seg = app[app.index("if (sessionToken.value) {"):]
    seg = seg[:seg.index("pollSession")] if "pollSession" in seg else seg[:600]
    assert "authed.value = true" in seg, "恢复成功要落 true"
    assert "authed.value = false" in seg, (
        "恢复失败必须显式落 false —— 否则 authed 永远停在 null，页面卡在骨架屏")


def test_login_and_app_content_not_removed():
    """反证：修闪烁不等于删页面。登录框与主界面骨架都必须在。"""
    tpl = _app()[_app().index("<template>"):]
    assert 'placeholder="ADMIN_PASSWORD"' in tpl, "登录页的密码框被删了"
    assert "进入工作台" in tpl
    assert 'class="app-shell"' in tpl
    assert 'class="sidebar"' in tpl
    assert 'class="topbar"' in tpl


def test_boot_screen_has_dvh_fallback():
    app = _app()
    seg = app[app.index(".boot-screen{"):]
    seg = seg[:seg.index("\n")]
    assert "100vh" in seg and "100dvh" in seg, "骨架屏高度要有 vh 兜底再上 dvh"
    assert seg.index("100vh") < seg.index("100dvh"), "必须先写 100vh 再写 100dvh（回落顺序）"


# ==========================================================================
# ② 手机适配（iPhone 15 Pro 393×852）
# ==========================================================================
def test_viewport_enables_safe_area():
    html = _index_html()
    # 只看 meta 本体：注释里提到这些词不算违规（注释是在说明「为什么要避开」）
    line = [l for l in html.splitlines() if 'name="viewport"' in l]
    assert len(line) == 1, "viewport meta 缺失或重复"
    tag = line[0]
    assert "viewport-fit=cover" in tag, (
        "不加 viewport-fit=cover，env(safe-area-inset-*) 恒为 0，灵动岛/Home 指示条照样压内容")
    assert "user-scalable" not in tag, "禁止缩放属于可访问性倒退"
    assert "maximum-scale" not in tag


def test_mobile_430_block_covers_key_breakages():
    block = _mobile_block(_app())
    assert "100dvh" in block, "移动端地址栏收放：app-shell 要 100dvh"
    assert "env(safe-area-inset-bottom)" in block, "底部要给 Home 指示条留安全区"
    assert "env(safe-area-inset-top)" in block, "顶部要让开灵动岛"
    assert ".sc-kpi-six" in block and "repeat(2,minmax(0,1fr))" in block, (
        "六 KPI 在 393px 会压成窄条，必须两列")
    assert ".sc-three-tables .el-table" in block, "三表要单独收字号/内边距，否则剧名只剩省略号"
    assert "min-height:34px" in block, "触控目标：small 按钮 24px 高，手指点不准"


def test_mobile_does_not_drop_three_table_columns():
    """反证：手机上「列太多」不是删列的理由 —— 三表各 6 列，一列都不许少。"""
    app = _app()
    tpl = app[app.index("<template>"):]
    expect = ["时间", "资源名称", "分类", "推送类型", "排期类型", "状态"]
    for anchor, name in (('scOverview.views?.history || []', "推送历史"),
                         ('<el-table :data="scQueueRows"', "推送队列"),
                         ('scOverview.views?.failed || []', "推送失败")):
        seg = tpl[tpl.index(anchor):]
        seg = seg[:seg.index("</el-table>")]
        labels = []
        for part in seg.split("<el-table-column")[1:]:
            head = part[:part.index("</el-table-column>")]
            if 'label="' in head:
                labels.append(head[head.index('label="') + 7:].split('"')[0])
        assert labels[:6] == expect, f"{name}表头变了：{labels}"


def test_topbar_not_double_sticky_on_mobile():
    """≤900 侧栏已改成吸顶横条，顶栏再 sticky 就叠成两条，竖向白吃 100+px。"""
    block = _mobile_block(_app())
    assert ".topbar{position:static" in block, (
        "手机上顶栏要随页面滚走，只留导航吸顶，否则首屏可用高度被吃掉一大截")


def test_narrow_screen_overrides_inline_width():
    """分享管理里内联写了 width:280px / 130px 的控件，393px 屏会横向溢出。"""
    block = _mobile_block(_app())
    assert "width:100%!important" in block
    assert ".sc-form-row" in block
