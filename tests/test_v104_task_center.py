"""阶段六：任务中心分类聚合视图（纯前端，零新增 API、零假数）。

红线：
- 9 个原 tab 一个不删（FEATURE_PRESERVATION 第 8 节）；
- 任务中心只用 load()/pollMirrorStatus() 已加载的 ref，不调用任何新接口；
- 8 类任务（镜像/复制/改名/翻译/分享/推送/投稿/求片）全覆盖，且每类标注数据来源。
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")

TABS = ["概览", "光鸭授权", "镜像同步", "资源库", "分发与互动中心",
        "推送队列", "TMDB 设置", "消息模板", "操作审计"]


def test_all_original_tabs_still_preserved():
    """新增任务中心后，原 9 个 tab 渲染入口全部保留。"""
    for label in TABS:
        assert (f"tab === '{label}'" in APP or f'tab === "{label}"' in APP), f"原 tab 渲染丢失：{label}"


def test_task_center_entry_added():
    """任务中心作为第 10 个入口加入导航，且有独立渲染 section。"""
    assert "key: '任务中心'" in APP, "导航缺少任务中心入口"
    assert "tab === '任务中心'" in APP, "缺少任务中心渲染区块"


def test_eight_categories_covered():
    """8 类任务全覆盖：镜像/复制/改名/翻译/分享/推送/投稿/求片。"""
    for key in ["mirror", "copy", "rename", "translate", "share", "push", "contribution", "request"]:
        assert f"key: '{key}'" in APP, f"任务中心缺少分类：{key}"


def test_uses_only_loaded_refs_no_new_api():
    """任务中心计算只引用已加载 ref；不新增任何 api() 请求。"""
    # 任务中心 computed 片段（从 const taskCenter 到 syntaxHint）
    start = APP.index("const taskCenter = computed")
    end = APP.index("const syntaxHint = computed", start)
    block = APP[start:end]
    # 已加载 ref
    for ref in ["mirrorStatus.value", "queue.value", "contributions.value",
                "mediaRequests.value", "interactionStatus.value", "batchInfo.value", "mirrorStrict.value"]:
        assert ref in block, f"任务中心未使用预期已加载数据：{ref}"
    # 不新增 API
    assert "api(" not in block, "任务中心不得新增任何 API 调用"
    assert "fetch(" not in block, "任务中心不得直接 fetch"


def test_no_fake_numbers_and_source_notes():
    """每类都标注数据来源，且明确不造假数的说明存在。"""
    assert block_note_count() >= 8, "8 类任务每类都应有 note 数据来源"
    assert "不造假数" in APP or "不做估算" in APP, "需明确不造假数说明"


def block_note_count():
    start = APP.index("const taskCenter = computed")
    end = APP.index("const syntaxHint = computed", start)
    return APP[start:end].count("note:")


def test_task_center_has_detail_drilldown():
    """每类任务可跳到对应原始页（setTab(c.target)），不新增操作入口。"""
    assert "setTab(c.target)" in APP, "任务中心需提供查看明细跳转"
    for target in ["镜像同步", "推送队列", "分发与互动中心"]:
        assert f"target: '{target}'" in APP, f"任务中心缺少跳转目标：{target}"


def test_task_center_responsive_and_dark():
    """任务中心栅格纳入既有响应式断点 + 深色主题覆盖。"""
    assert ".task-grid{display:grid;grid-template-columns:repeat(4,1fr)" in APP, "任务中心默认栅格缺失"
    assert ".task-grid{grid-template-columns:repeat(2,1fr)" in APP, "任务中心 1100px 断点缺失"
    assert "html.dark .task-notice" in APP, "任务中心深色主题覆盖缺失"
