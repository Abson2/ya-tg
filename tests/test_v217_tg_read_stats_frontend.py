"""v2.1.7 #241：阅读量统计页前端契约验收。

红线：
  1. 页面**不得出现后端字段名**（chat_id / message_id / work_key / views / queue_id）；
  2. **群聊不提供浏览量** → 群聊条目的阅读量显示「不适用」，不是 0；
  3. 展示的是**期间新增**（累计值相邻两次采集求差），不是累计总数；
  4. 读不到的消息标记为「暂不可采集」，不显示 0；
  5. 深色模式覆盖存在（否则切主题后又是浅底浅字）。
"""
import os
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = ROOT / "frontend" / "src" / "App.vue"


@pytest.fixture(scope="module")
def app() -> str:
    return APP_VUE.read_text(encoding="utf-8")


def _stats_block(app: str) -> str:
    """阅读量子页的**模板区**。

    锚点必须是**模板区特有**的标记：「v2.1.7 #241：阅读量统计」这串字在文件里
    出现 5 次（脚本区注释 / 样式注释 / 模板注释各若干），用 `index()` 会命中
    **脚本区的第一处** → 切片完全错位，断言要么恒真要么恒假。
    这里用模板区独有的 `sc-stats-layout`（只有模板里用这个 class 名）定位起点，
    用 `/sc-workspace` 定位终点——两个锚点都唯一且可见。
    """
    start = app.index('class="sc-stats-layout"')
    # 往前退到该块的起始注释，便于把说明性注释也纳入检查
    head = app.rfind("<!--", 0, start)
    tail = app.index("/sc-workspace", start)
    assert head > 0 and tail > start, "阅读量模板区边界锚点失效"
    return app[head:tail]


# ---------------------------------------------------------------------------
# 1. 子页挂载
# ---------------------------------------------------------------------------

def test_reads_subpage_registered(app):
    """分享中心新增「阅读量」子页。"""
    assert "key: '阅读量'" in app
    assert "scSubNav === '阅读量'" in app


def test_reads_subpage_switch_loads_data(app):
    """切到阅读量子页时才加载数据（不拖慢其它子页）。"""
    assert "scSwitchSubNav" in app
    assert "if (key === '阅读量')" in app
    assert "loadScStats()" in app


def test_original_subpages_not_removed(app):
    """**功能零删减**：原有 4 个子页一个都不能少。"""
    for key in ("发布总览", "发布规则", "推送任务", "分享管理"):
        assert f"key: '{key}'" in app, f"子页被删：{key}"


def test_original_nine_tabs_intact(app):
    """**功能零删减**：9 个原 tab 一个不删。"""
    for key in ("概览", "任务中心", "光鸭授权", "镜像同步", "资源库",
                "推送队列", "分发与互动中心", "TMDB 设置", "消息模板", "操作审计"):
        assert f"key: '{key}'" in app, f"tab 被删：{key}"


# ---------------------------------------------------------------------------
# 2. 页面不得出现后端字段名
# ---------------------------------------------------------------------------

def test_page_has_no_backend_field_names(app):
    """阅读量页面**不得出现后端字段名**。"""
    block = _stats_block(app)
    # 这些词只在**模板区**出现才算违规（脚本区里的变量名不算展示给用户的）
    for banned in ("chat_id", "message_id", "work_key", "queue_id",
                   "telegram_message_id", "target_kind", "reads_available",
                   "views_available", "suggest_source",
                   "suggest_source"):
        assert banned not in block, f"页面泄露后端字段名：{banned}"


def test_page_does_not_show_raw_views_key(app):
    """不得直接绑定后端的 views 字段名。"""
    block = _stats_block(app)
    assert ".views" not in block
    assert "row.views" not in block


# ---------------------------------------------------------------------------
# 3. 口径表达
# ---------------------------------------------------------------------------

def test_group_reads_shows_not_applicable(app):
    """群聊的阅读量显示「不适用」，**不是 0**。"""
    block = _stats_block(app)
    assert "不适用" in block
    # 群聊条目用 reads_available / kind 判断
    assert "reads_shown" in block or "s.row.kind_label" in block


def test_items_reads_null_rendered_as_not_applicable(app):
    """明细页里空值渲染成「不适用」而不是 0。"""
    block = _stats_block(app)
    assert "s.row.reads !== null" in block


def test_unavailable_message_marked_not_zero(app):
    """不可采集的消息标记为「暂不可采集」，不显示 0。"""
    block = _stats_block(app)
    assert "暂不可采集" in block
    assert "已采集" in block


def test_page_states_group_limitation(app):
    """页面明确告知群聊不提供浏览量。"""
    assert "群聊不提供浏览量统计" in app


def test_page_states_delta_semantics(app):
    """页面说明展示的是**期间新增**（差值），不是累计总数。"""
    block = _stats_block(app)
    assert "新增阅读" in block
    assert "相邻两次采集的差值" in block


def test_period_new_labels(app):
    """四个统计数都用「期间新增」口径命名。"""
    block = _stats_block(app)
    for label in ("期间新增阅读", "期间新增转发", "期间新增回应"):
        assert label in block, f"缺少口径标签：{label}"


# ---------------------------------------------------------------------------
# 4. 配置与操作
# ---------------------------------------------------------------------------

def test_config_area_has_credentials_and_switch(app):
    """配置区含三项凭证 + 启用开关 + 回溯窗口 + 单轮上限。"""
    block = _stats_block(app)
    for token in ("scStatsConfig.api_id", "scStatsConfig.api_hash",
                  "scStatsConfig.phone", "scStatsConfig.enabled",
                  "scStatsConfig.lookback_hours", "scStatsConfig.max_per_run"):
        assert token in block, f"配置项缺失：{token}"


def test_credential_input_is_password_type(app):
    """密钥输入框是密码类型（不肩窥）。

    注意方向：`type="password"` 写在 `v-model` **之后**
    （`<el-input v-model="..." type="password" .../>`），
    所以必须取 `idx` **之后**的窗口，取前面的会永远找不到。
    """
    block = _stats_block(app)
    idx = block.index("scStatsConfig.api_hash")
    seg = block[idx:idx + 200]
    assert 'type="password"' in seg, "api_hash 输入框必须是密码类型"


def test_credentials_cleared_after_save(app):
    """保存后清空输入框——明文不留存在页面状态里。"""
    assert "scStatsConfig.value.api_id = ''" in app
    assert "scStatsConfig.value.api_hash = ''" in app


def test_login_flow_present(app):
    """登录两步流程都在（取验证码 → 提交验证码）。"""
    assert "scStatsSendLoginCode" in app
    assert "scStatsSubmitLoginCode" in app
    assert "/sc/stats/login/code" in app
    assert "/sc/stats/login/verify" in app


def test_collect_button_calls_endpoint(app):
    """手动采集按钮调真实端点。"""
    assert "/sc/stats/collect" in app
    assert "scStatsCollect" in app


def test_reads_disabled_without_ready_state(app):
    """未就绪时采集按钮禁用（避免点了没反应）。"""
    block = _stats_block(app)
    assert "!scStatsStatus.ready" in block


def test_error_handling_independent(app):
    """阅读量加载失败**不影响**其它功能（独立 loader + 静默降级）。"""
    assert "async function loadScStats()" in app
    # loadSc() 的函数体里不含阅读量端点。
    # 边界必须用**紧邻的下一个函数定义**收口——loadScStats 定义在 scSwitchTrend
    # 之后，若用 scSwitchTrend 当终点会把阅读量代码也切进来 → 断言恒假。
    i = app.index("async function loadSc()")
    j = app.index("\n}", i)          # loadSc 的函数体结束（顶格右花括号）
    load_sc = app[i:j]
    assert "/sc/stats/" not in load_sc, "阅读量端点不得混进主加载"
    # 阅读量必须走**独立**的 loader，且失败时静默降级
    assert "try {" in app[app.index("async function loadScStats()"):][:400]


# ---------------------------------------------------------------------------
# 5. 深色模式
# ---------------------------------------------------------------------------

def test_reads_page_has_dark_overrides(app):
    """阅读量页有深色覆盖（否则切主题后失配）。"""
    for sel in (".sc-stats-state", ".sc-state-dot", ".sc-stats-config"):
        assert f"html.dark {sel}" in app, f"深色覆盖缺失：{sel}"


def test_dark_overrides_defined_after_light_rules(app):
    """深色覆盖必须**在浅色规则之后**（同特异度后者胜出）。"""
    light = app.index(".sc-stats-state{")
    dark = app.index("html.dark .sc-stats-state{")
    assert dark > light, "深色覆盖必须定义在浅色规则之后"


# ---------------------------------------------------------------------------
# 6. 脚本侧状态与函数
# ---------------------------------------------------------------------------

def test_script_state_refs_declared(app):
    """页面状态 ref 都有声明。"""
    for name in ("scStatsStatus", "scStatsTrend", "scStatsItems",
                 "scStatsRuns", "scStatsDays", "scStatsConfig"):
        assert f"const {name} = ref(" in app, f"状态未声明：{name}"


def test_script_helpers_defined(app):
    """辅助函数都有定义。"""
    for fn in ("loadScStats", "saveScStatsConfig", "scStatsSendLoginCode",
               "scStatsSubmitLoginCode", "scStatsCollect", "scStatsRunLabel"):
        assert f"function {fn}" in app, f"函数未定义：{fn}"


def test_run_status_label_is_chinese(app):
    """采集轮次状态显示中文，不暴露后端状态码。"""
    assert "scStatsRunLabel" in app
    i = app.index("function scStatsRunLabel")
    block = app[i:i+400]
    for label in ("成功", "进行中", "失败"):
        assert label in block, f"状态中文标签缺失：{label}"
