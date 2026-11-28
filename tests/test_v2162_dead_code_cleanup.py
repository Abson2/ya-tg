"""v2.1.62 回归测试：清理 App.vue 里定义后从未被引用的死代码。

背景（2026-11-28，v2.1.61 上线后）：
`filteredResources` 与另外 5 个标识符在 `<script setup>` 里定义、在模板与脚本里
**一次都没有被引用**，其中 `filteredResources` 还是按 `resources`（只有最新 100 行）
过滤的单集列表——真要用起来就会踩 v2.1.61 那个「代表资源查不到」的同款坑。

删除清单（全部只删定义，无行为变化）：
  - `viewMode`：视图切换入口早已不存在，值永远停在 'series'
  - `scGenreOptions`：分类下拉已不存在
  - `scLinkStateLabel`：分享管理页改消费后端下发的 `link_status_label`，卡片走 `mediaLinkLabel`
  - `filteredResources`：单集列表视图自 v0.6.4 起已取消（只有作品视图）
  - `repIdOf`：已被 `selectedRepIds` 取代
  - `scSchedName`：排期表直接用目录里的 name

替代物必须还在：**中文映射函数与「代表资源」入口一个都不能少**。
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
MAIN = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")

REMOVED = ("viewMode", "scGenreOptions", "scLinkStateLabel",
           "filteredResources", "repIdOf", "scSchedName")


def test_dead_identifiers_are_gone():
    for name in REMOVED:
        assert f"const {name} = " not in APP, f"{name} 的定义又回来了（它没有任何引用）"
        assert f"function {name}(" not in APP, f"{name} 的定义又回来了（它没有任何引用）"


def test_replacements_still_present():
    # 中文映射：资源库卡片的状态角标靠它（红线：不许渲染裸状态码）
    assert "const mediaLinkLabel = (state) => ({" in APP
    # 作品级批量操作的代表 id 入口
    assert "function selectedRepIds() {" in APP
    # 作品级筛选（唯一还在用的筛选链）
    assert "const filteredSeries = computed(" in APP


def test_share_center_uses_backend_labels_not_raw_codes():
    """删掉脚本侧兜底映射后，分享中心仍不许渲染裸 link_state。"""
    for line in APP.splitlines():
        if "{{" in line and "link_state" in line:
            # 只允许「取值后过映射函数」或「计数展示」，不允许原样插值
            assert "mediaLinkLabel" in line or "link_state_counts" in line, line.strip()
            assert "{{ scOverview.link_state" not in line or "_counts" in line, line.strip()


def test_version_locked_to_2162():
    assert 'app = FastAPI(title="Ya-TG", version="2.1.65"' in MAIN
    assert "v2.1.65 · Asia/Shanghai" in APP
