"""v2.1.62 回归测试：资源库卡片「推送」点了没反应。

真机背景（2026-11-28，v2.1.60 上线后）：
- 资源库 678 部作品，`resources` 来自 `/api/resources`，**默认只带最新 100 行**
  （上限 500）。
- 卡片按钮拿「代表资源」走的是 `seriesRepresentative(g)` =
  `resources.value.find(r => r.id === g.representative_id)`，查不到就 `return null`。
- 模板里「分享 / 取消分享 / 识别 / 手动修正」都写了 `|| {id, name}` 兜底，还能用；
  唯独「推送」写的是 `seriesRepresentative(g) && publishResource(...)` —— null 被
  短路，**点了什么都不发生、也没有任何提示**。受影响的是 678 部里除了最新那批
  以外的绝大多数作品。

修法：`seriesRepresentative` **永不返回 null**，查不到就用作品组上的字段拼一个
（四个操作都只依赖 `row.id`；手动修正需要的剧名/类型/影视号组上本来就有），
模板里所有 `|| {...}` / `&&` 兜底随之去掉，只剩一个数据来源。
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


def _rep_fn_seg():
    seg = APP[APP.index("function seriesRepresentative(g) {"):]
    return seg[:seg.index("// ---------- LitePan 风格「重新匹配元数据」 ----------")]


def test_representative_never_returns_null():
    seg = _rep_fn_seg()
    # 反证：查不到就 return null 是本次真机故障的根因，不许回来
    assert "return null" not in seg
    # 必须有兜底对象（查不到时用作品组字段拼）
    assert "const hit = resources.value.find(r => r.id === g.representative_id)" in seg
    assert "if (hit) {" in seg
    # 兜底对象必须带对 id —— 四个操作全都只靠它
    assert "id: g.representative_id" in seg


def test_representative_fallback_carries_match_clues():
    """手动修正窗口要的字段：剧名、类型、影视号（组上已有），不许缺。"""
    seg = _rep_fn_seg()
    for field in ("name: g.title", "media_title: g.title", "media_type: g.media_type",
                  "poster_path: g.poster_path", "tmdb_id: g.tmdb_id"):
        assert field in seg, f"兜底对象缺 {field}"
    # 源映射兜底仍在（v2.1.11 口径：代表资源没回填时用作品组的映射）
    assert "source_title: g.source_title || ''" in seg
    assert "source_tmdb_id: g.source_tmdb_id || 0" in seg
    assert "source_media_type: g.source_media_type || ''" in seg
    # 拿不到路径就不编造
    assert "path_hint: ''" in seg


def test_publish_button_not_short_circuited():
    # 反证：`seriesRepresentative(g) && publishResource(...)` 会让按钮静默失效
    assert '@click="seriesRepresentative(g) &&' not in APP
    assert '@click="publishResource(seriesRepresentative(g))"' in APP


def test_all_card_actions_use_single_source():
    """四个操作统一走 seriesRepresentative(g)，模板里不再各写一份兜底。"""
    assert '@click="createShare(seriesRepresentative(g))"' in APP
    assert '@click="cancelShare(seriesRepresentative(g))"' in APP
    assert '@click="identify(seriesRepresentative(g), true)"' in APP
    assert '@click="openMatch(seriesRepresentative(g))"' in APP
    # 反证：模板内联兜底对象不许回来（两处卡片墙都要干净）
    assert "seriesRepresentative(g) ||" not in APP


def test_all_card_buttons_still_present():
    """用户红线：所有按钮一个不删。两组卡片墙各 4 个操作入口 + 1 个下拉。"""
    assert APP.count(">推送</el-button>") == 2
    assert APP.count(">分享</el-button>") == 2
    # 「取消分享」在分享中心/队列里也有，这里只要求两组卡片墙各留一个
    assert APP.count(">取消分享</el-button>") >= 2
    assert APP.count(">识别</el-button>") == 2
    assert APP.count(">手动修正</el-button>") == 2
    assert APP.count("完结状态 ▾") == 2


def test_version_locked_to_current():
    assert "v2.1.62 · Asia/Shanghai" in APP
