"""阶段六：深色主题（附加层）+ 全部现有 tab 入口保护。

红线：UI 升级不得删任何 tab / 操作入口（FEATURE_PRESERVATION 第 8 节）。
深色主题用 html.dark 类 + 官方暗色变量，浅色默认不变。
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
MAIN = (ROOT / "frontend/src/main.js").read_text(encoding="utf-8")


def test_all_tab_entries_preserved():
    """9 个现有 tab 全部保留（一个都不能少）。"""
    for label in ["概览", "光鸭授权", "镜像同步", "资源库", "分发与互动中心",
                  "推送队列", "TMDB 设置", "消息模板", "操作审计"]:
        assert f"'{label}'" in APP or f'"{label}"' in APP or label in APP, f"tab 丢失：{label}"
        assert f"tab === '{label}'" in APP or f'tab === "{label}"' in APP or f"setTab('{label}')" in APP, \
            f"tab 渲染/跳转丢失：{label}"


def test_mirror_batch_control_buttons_preserved():
    """阶段二 C4 的批次控制四按钮不得被 UI 升级删掉。"""
    for action in ["pause", "resume", "skip", "cancel"]:
        assert f"batchControl('{action}')" in APP, f"批次控制按钮丢失：{action}"
    assert "mirrorStrict" in APP and "batchInfo" in APP


def test_dark_theme_toggle_present():
    assert "ya_tg_theme" in APP, "主题偏好需持久化"
    assert "toggleTheme" in APP and "isDark" in APP, "需有主题切换逻辑"
    assert "classList.toggle('dark'" in APP, "需切换 html.dark 类"
    assert "element-plus/theme-chalk/dark/css-vars.css" in MAIN, "需引入官方暗色变量"


def test_dark_is_additive_layer():
    """深色是附加层：html.dark 覆盖规则存在，且不删除原 :root 浅色变量。"""
    assert "html.dark{" in APP
    assert "--yg-bg:#f5f6fb" in APP, "浅色默认变量必须保留"
    assert "html.dark .el-table" in APP or "html.dark .panel" in APP, "需覆盖硬编码白底面板"


def test_mobile_breakpoints_preserved():
    """阶段六要求保留并增强现有移动端断点。"""
    for bp in ["1100px", "900px", "680px", "400px", "820px"]:
        assert bp in APP, f"移动端断点丢失：{bp}"


def test_danger_confirm_and_anti_dup_preserved():
    """危险操作二次确认 + 按钮防重机制不得退化。"""
    assert "ElMessageBox.confirm" in APP, "危险操作需二次确认"
    assert "mirrorBusy" in APP and ":loading=\"mirrorBusy\"" in APP, "镜像按钮防重需保留"
