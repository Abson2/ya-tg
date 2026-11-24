"""v2.1.22：推送任务的状态筛选去重 + 全中文（#308）。

需求原话：「推送任务中状态筛选优化，现在重复且存在无效按钮」。

现场（2026-11-24 截图）：下拉里同时有
  「待发布（待发布 + 待推送）」/「推送中（提交/领取/推送/建链）」两个**阶段档**
  和「已提交(85)」/「已成功(25)」/「已跳过投递(2)」/「cleaned(532)」四个**单项**。
两个毛病：
  ① 语义重复：`已提交` 既属于阶段档「推送中」，又单独列一项；
  ② 无效按钮：`cleaned(532)` 是归档态，`/sc/tasks` 默认就把归档行过滤掉
     （include_archived=false），点进去必然空表；标签还是后端枚举名。

本文件锁死修好后的契约：
  ① 每个状态只出现一次（阶段档成员不再单独列）；
  ② 全中文，`cleaned` 有中文兜底且不作为筛选项；
  ③ 0 条的项不挂入口（避免又一个"点了空表"的无效按钮）；
  ④ KPI 仍然能用 `pending` / `published` / `failed` 跳转筛选。
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


def _items_block() -> str:
    start = APP_VUE.index("const SC_TASK_STATUS_ITEMS = [")
    end = APP_VUE.index("\n]\n", start)
    return APP_VUE[start:end]


def _members() -> list[list[str]]:
    return [re.findall(r"'([a-z_]+)'", m) for m in
            re.findall(r"members: \[([^\]]*)\]", _items_block())]


def test_status_select_has_no_hardcoded_duplicates():
    """下拉里不再写死「待发布」/「推送中」两个阶段 option。"""
    assert '<el-option label="待发布（待发布 + 待推送）" value="pending"/>' not in APP_VUE
    assert '<el-option label="推送中（提交/领取/推送/建链）" value="inflight"/>' not in APP_VUE
    # 状态下拉只保留一个循环渲染的选项来源
    seg_start = APP_VUE.index('v-model="scTaskStatusFilter"')
    seg_end = APP_VUE.index("</el-select>", seg_start)
    seg = APP_VUE[seg_start:seg_end]
    assert seg.count("<el-option") == 1
    assert "v-for=\"opt in scTaskStatusOptions\"" in seg


def test_each_status_belongs_to_exactly_one_item():
    """核心回归：同一个状态不得出现在两个筛选项里（这就是"重复"）。"""
    seen: dict[str, int] = {}
    for group in _members():
        for st in group:
            seen[st] = seen.get(st, 0) + 1
    dupes = {k: v for k, v in seen.items() if v > 1}
    assert not dupes, f"状态在多个筛选项里重复：{dupes}"
    # 覆盖六个在途态 + 四个终态
    for st in ("planned", "ready", "building", "submitted", "accepted", "publishing",
               "published", "failed", "cancelled", "skipped", "invalid"):
        assert st in seen, f"缺少状态 {st}"


def test_archived_status_is_not_a_filter_option():
    """cleaned 是归档态：列表默认不返回它，挂筛选入口等于无效按钮。"""
    assert "cleaned" not in _items_block()
    # 但中文兜底必须有——否则任何地方漏出这个值就又是裸枚举名
    assert "cleaned: '已清理'" in APP_VUE


def test_empty_counts_are_not_offered():
    """0 条不挂入口。"""
    assert "if (!n) continue" in APP_VUE


def test_kpi_phase_values_still_resolve():
    """KPI 点「待提交」传的是 pending，必须仍是可选项，否则下拉会显示裸值。"""
    assert "'pending'" in _items_block()
    assert "'inflight'" in _items_block()
    assert APP_VUE.count("scFilterQueue('pending')") == 1
    assert "scFilterQueue('published')" in APP_VUE
    assert "scFilterQueue('failed')" in APP_VUE


def test_members_are_words_not_backend_field_names():
    """筛选项文案里不得出现后端字段名（红线：页面不体现内部名词）。"""
    block = _items_block()
    for bad in ("work_key", "link_state", "chat_id", "queue_id"):
        assert bad not in block, bad
