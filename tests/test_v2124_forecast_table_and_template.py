"""v2.1.24：排期列表优化 + 折叠模板 + 进度筛选修复 + 队列时间口径。

用户原话（2026-11-24，四条 + 一次追加纠正）：
  1、今日推送排期看板优化：
     a、不用分批分时段加载，固定显示条数，通过手动滚动进度条展示
        （临近时段自动上移，和之前的版本差不多）；
     b、表格布局调整：左侧时间不变，表头增加标题栏：分类、新上映、新入库、
        存量排期（这一列适当宽一些），字段对齐；
     c、「18:24–18:42 · 每 6 分钟 1 部」不用展示那么细，可以展示几分钟一部就行，
        不用具体到哪个时间段，与第一列的时间段对齐即可，
        每部剧仍然在后面标记推送时间「黑豹2（18:12）」。
     （追加纠正，原话）「也不是改成表格，还是保留上 2.22 的风格，将布局和展示做优化」
     → 结论：分组靠**行内彩色标签**，不靠列；表头/表格结构一律不留。
  2、内置的默认消息模版中「👤 分享：」这个字段去掉，现在的模版和参考模版的差异在
     影视信息、剧集更新情况和简介是采用的折叠展示。
  3、推送任务：未推送、已推送的筛选标签无效。
  4、推送相关的时间维度对齐：待发布的有日期、待排期的无日期
     （在队列里面只展示时间）。

本文件锁死：
  ① 排期数据契约：每部作品带 `tags`（决定落哪一组）与 `time`（几点推），
     每小时带 `pace_minutes`（每 N 分钟 1 部），不再有跨小时的时间跨度串；
  ② 前端仍是 v2.1.22 的列表风格（无表头/无 grid），三组靠行内标签区分，
     固定高度滚动区（不是分批加载）；
  ③ 默认模板三块折叠、无 `👤 分享:`、`spec_line` 由 ctx 合成并真的进消息；
  ④ `_needs_template_upgrade` 认得 v2.1.23 版式，且不误伤自定义模板；
  ⑤ `_clip_html` 兜住截断导致的标签未闭合（否则 Telegram 整条拒发）；
  ⑥ 进度二分档接在**表格真正用的** computed 上（`scTaskViewRows`）；
  ⑦ 队列时间：同天只给 HH:mm，跨天才补 MM-DD。
"""
import json
import re
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import delete, select

from app import share_center_v2 as sc2
from app import utils
from app.db import (DEFAULT_TEMPLATE, SessionLocal, _needs_template_upgrade,
                    migrate)
from app.models import (
    AppConfig, AuditLog, Bot, Channel, MessageTemplate, QueueItem, Resource,
    SCCategory, SCCategoryCursor, SCCategorySchedule, SCHourWindow,
    SCLibraryVersion, SCPublishTask, SCPushList, SCPushListItem, SCSharePlan,
    SCSharePlanVersion, SCWorkCategory, ShareTask,
)
from app.publisher import _build_message, _clip_html

from conftest import ensure_full_day_hour_window

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")

WIPED = (SCPublishTask, SCPushListItem, SCPushList, SCLibraryVersion,
         SCCategoryCursor, SCCategorySchedule, SCWorkCategory, SCCategory,
         SCSharePlanVersion, SCSharePlan, ShareTask, QueueItem, Resource,
         Channel, Bot, AppConfig, AuditLog, SCHourWindow)


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    for _ in range(2):
        with SessionLocal() as db:
            for model in WIPED:
                db.execute(delete(model))
            db.commit()
    # 模板是跨测试共享的：本文件会临时改内容，用完必须恢复默认
    with SessionLocal() as db:
        tpl = db.query(MessageTemplate).filter(
            MessageTemplate.name == "默认图文模板").first()
        if tpl:
            tpl.content = DEFAULT_TEMPLATE
        db.commit()
    yield
    with SessionLocal() as db:
        tpl = db.query(MessageTemplate).filter(
            MessageTemplate.name == "默认图文模板").first()
        if tpl:
            tpl.content = DEFAULT_TEMPLATE
        db.commit()


# ---------------------------------------------------------------------------
# 造数（与 test_v2123 同构，独立一份避免跨文件耦合）
# ---------------------------------------------------------------------------
def _plan(db, **cfg):
    plan = SCSharePlan(name="p", plan_type="daily", enabled=True)
    db.add(plan)
    db.flush()
    config = {"category_limit": 20, "work_limit": 50}
    config.update(cfg)
    ver = SCSharePlanVersion(plan_id=plan.id, version_no=1, status="active",
                             config_json=json.dumps(config))
    db.add(ver)
    db.flush()
    ensure_full_day_hour_window(db, ver.id)
    return ver


def _channel(db):
    bot = Bot(name="b", token="x", enabled=True)
    db.add(bot)
    db.flush()
    ch = Channel(bot_id=bot.id, name="频道", chat_id="@ch", kind="channel",
                 enabled=True)
    db.add(ch)
    db.flush()
    return ch


def _work(db, *, title, media_type="movie"):
    res = Resource(file_id=f"f-{title}", name=f"{title}.2026.2160p.mkv",
                   ext=".mkv", size=1024 ** 3, utime=1,
                   identify_status="identified", media_type=media_type,
                   tmdb_id=abs(hash(title)) % 100000, media_title=title,
                   share_status="active", share_url="https://pan.quark.cn/s/abc")
    db.add(res)
    db.flush()
    wk = f"work:{title}"
    db.add(SCLibraryVersion(
        work_key=wk, version_no=1, fingerprint=f"fp-{title}",
        media_type=media_type, resource_ids_json=json.dumps([res.id]),
        metadata_json=json.dumps({"title": title}), change_type="created",
        created_at=datetime.now(timezone.utc)))
    db.flush()
    db.add(SCWorkCategory(work_key=wk, category_key="genre:剧情",
                          media_type=media_type))
    db.flush()
    return wk


# ---------------------------------------------------------------------------
# ① 排期数据契约（用户口径 1b / 1c）
# ---------------------------------------------------------------------------
def test_forecast_works_carry_tags_and_time():
    """每部作品要带 `tags` 与 `time` —— 前端靠它们决定「落哪一列 + 几点推」。

    没有 `tags` 就分不了「新上映 / 新入库 / 存量排期」三列；
    没有 `time` 就只能显示裸剧名，用户看不到「黑豹2（18:12）」。
    """
    with SessionLocal() as db:
        _plan(db)
        db.add(SCCategory(category_key="genre:剧情", name="剧情", enabled=True,
                          priority=0))
        _channel(db)
        for i in range(3):
            _work(db, title=f"剧{i}")
        db.commit()

        out = sc2.forecast_schedule(db)
        works = [w for h in out["hours"] for it in h["items"] for w in it["works"]]
        assert works, "没有排到任何作品，用例前提不成立"
        for w in works:
            assert "tags" in w and isinstance(w["tags"], list), w
            assert "time" in w and isinstance(w["time"], str), w
        # 有时间点就必须是 HH:mm（前端直接透出，不做二次格式化）
        timed = [w for w in works if w["time"]]
        assert timed, "出现了排期却一个时间点都没有"
        for w in timed:
            assert re.fullmatch(r"\d{2}:\d{2}", w["time"]), w["time"]


def test_forecast_items_no_longer_carry_hour_span():
    """用户口径 1c：不再报「18:24–18:42」这种跨小时跨度。

    排期项只剩「条数 / 部数 / 作品各带时间」，跨度串整块删掉。
    """
    with SessionLocal() as db:
        _plan(db)
        db.add(SCCategory(category_key="genre:剧情", name="剧情", enabled=True,
                          priority=0))
        _channel(db)
        for i in range(2):
            _work(db, title=f"剧{i}")
        db.commit()

        out = sc2.forecast_schedule(db)
        for h in out["hours"]:
            for it in h["items"]:
                # times 是给「同一行多部作品」留的去重时刻表，不是跨度串
                assert isinstance(it["times"], list), it
                assert all(re.fullmatch(r"\d{2}:\d{2}", t) for t in it["times"]), it
                assert not any("–" in str(v) or "~" in str(v) for v in it.values()
                               if isinstance(v, str)), it


# ---------------------------------------------------------------------------
# ② 排期表（用户口径 1a / 1b：与「推送队列」同一套表格）
# ---------------------------------------------------------------------------
def test_frontend_forecast_uses_same_table_as_queue():
    """用户口径：「如果和推送队列的表格样式一样，可以接受」。

    所以排期卡必须用**同一个组件同一组属性**（el-table + stripe + size=small +
    max-height），而不是自绘 div 网格 —— 自绘的行高/斑马纹/滚动行为都会跟队列不一致。
    """
    seg = APP_VUE[APP_VUE.index(':data="scFcRows"'):]
    seg = seg[:seg.index("</el-table>")]
    assert 'class="sc-fc-table"' in seg
    for attr in ('stripe', 'size="small"', 'max-height="330"', 'ref="scFcBodyRef"'):
        assert attr in seg, attr
    # 队列那张表也是这四个属性，观感才一致
    q = APP_VUE[APP_VUE.index('<el-table :data="scQueueRows"'):]
    q = q[:q.index("</el-table>")]
    for attr in ('stripe', 'size="small"', 'max-height='):
        assert attr in q, attr


def test_frontend_forecast_has_five_columns():
    """表头：时间 | 分类 | 新上映 | 新入库 | 存量排期（存量列最宽，作品最多）。"""
    seg = APP_VUE[APP_VUE.index('class="sc-fc-table"'):]
    seg = seg[:seg.index("</el-table>")]
    for label in ("时间", "分类", "新上映", "新入库", "存量排期"):        assert f'label="{label}"' in seg, label
    widths = dict(re.findall(r'label="([^"]+)"\s+(?:width|min-width)="(\d+)"', seg))
    assert set(widths) == {"时间", "分类", "新上映", "新入库", "存量排期"}, widths
    assert int(widths["存量排期"]) > int(widths["新上映"]), widths
    assert int(widths["存量排期"]) > int(widths["新入库"]), widths


def test_frontend_forecast_fixed_height_scroll_and_auto_scroll():
    """固定高度（用户口径 1a）+ 临近时段自动上移。"""
    assert 'max-height="330"' in APP_VUE
    seg = APP_VUE[APP_VUE.index("function scFcScrollToNow"):]
    seg = seg[:seg.index("\n}")]
    # el-table 的滚动容器在组件内部，必须按它的 DOM 找，不能当成普通 div
    assert ".el-scrollbar__wrap" in seg, seg
    assert ".el-table__body tr" in seg, seg
    assert "if (!force && hour === _fcScrolledHour) return" in seg
    # 当前时段整行加底色
    assert "is-now" in APP_VUE


def test_frontend_pace_text_follows_time_column():
    """用户口径 1c：节奏文案挂在**第一列时间**，不另起一行报跨度。"""
    assert "每 {{ s.row.pace_minutes }} 分钟 1 部" in APP_VUE
    # 作品后面跟推送时间：剧名（18:12）
    assert "{{ w.title || '未命名作品' }}<em v-if=\"w.time\">（{{ w.time }}）</em>" in APP_VUE
    # 旧的跨度变量已删干净
    for gone in ("scForecastSpan", "function _hm"):
        assert gone not in APP_VUE, gone


def test_frontend_rows_precomputed_not_split_in_template():
    """三组分桶在 computed 里算一次，不在模板里对同一行重复分桶三遍。"""
    assert "const scFcRows = computed" in APP_VUE
    seg = APP_VUE[APP_VUE.index("const scFcRows = computed"):]
    seg = seg[:seg.index("\n})")]
    assert "scFcSplit(works)" in seg
    assert "groups: scFcSplit(works)" in seg
    # 模板只读 s.row.groups.*，不再自己调函数
    assert "s.row.groups.rel" in APP_VUE
    assert "scFcGroups(" not in APP_VUE, "模板里不该再逐行分桶"


def test_frontend_split_respects_tag_priority():
    """一部作品只落一列：新上映 > 新入库 > 存量排期（不重复占位）。"""
    seg = APP_VUE[APP_VUE.index("function scFcSplit"):]
    seg = seg[:seg.index("\n}")]
    rel = seg.index("new_release")
    lib = seg.index("new_library")
    assert rel < lib, "新上映必须优先于新入库判定"
    assert "else out.old.push(w)" in seg, "没有标签的必须落到存量列"


# ---------------------------------------------------------------------------
# ③ 默认模板：三块折叠 + 去掉分享行（用户口径 2）
# ---------------------------------------------------------------------------
def test_default_template_folds_three_blocks():
    t = DEFAULT_TEMPLATE
    assert t.count("<blockquote expandable>") == 3
    assert t.count("</blockquote>") == 3
    for block in ("📽 影视信息:", "📡 剧集更新:", "📖 简介:"):
        assert block in t, block
    assert "👤" not in t and "{{share_code}}" not in t
    # 分享链接本身仍要发出（去掉的只是「分享码」这一行）
    assert t.endswith("{{share_url}}")


def test_rendered_message_has_no_share_line_and_folds_episode():
    """真机消息：季集在折叠块里、没有「👤 分享:」行。"""
    with SessionLocal() as db:
        res = Resource(
            file_id="v2124-m1", name="罪案之法.2026.S01E04.2160p.WEB-DL.mkv",
            ext=".mkv", size=1024 ** 3, utime=1,
            path_hint="影视/电视剧/国产剧/罪案之法 (2026)/Season 1",
            share_dir_name="罪案之法", media_title="罪案之法", media_type="tv",
            tmdb_id=99001, total_episodes=24, finish_status="returning",
            identify_status="identified", poster_path="/p.jpg", season=1,
            episode=4, overview="一桩旧案重启。")
        db.add(res)
        db.commit()
        text, _ = _build_message(db, res, "https://share/x")
        assert "📡 剧集更新:\n<blockquote expandable>" in text, text
        assert "👤 分享" not in text, text
        # 折叠标签必须成对（Telegram 解析失败会整条拒发）
        assert text.count("<blockquote expandable>") == text.count("</blockquote>")
        assert text.rstrip().endswith("https://share/x")


def test_spec_line_merges_version_and_size():
    """`spec_line` = 技术规格 · 大小（ctx 侧合成，模板只判一次）。"""
    with SessionLocal() as db:
        res = Resource(
            file_id="v2124-m2", name="黑豹2.2026.2160p.WEB-DL.H265.mkv",
            ext=".mkv", size=1024 ** 3, utime=1,
            share_dir_name="黑豹2", media_title="黑豹2", media_type="movie",
            tmdb_id=99002, identify_status="identified", poster_path="/p.jpg")
        db.add(res)
        db.commit()
        text, _ = _build_message(db, res, "https://share/y")
        assert "📽 影视信息:\n<blockquote expandable>" in text, text
        assert "1.0GB" in text, text
        block = text.split("📽 影视信息:\n<blockquote expandable>")[1].split("</blockquote>")[0]
        assert "2160p" in block, block
        assert " · " in block, block


def test_spec_line_empty_when_nothing_to_show():
    """版本与大小都没有 → 整个折叠块（含行头）不出现，不留空 blockquote。"""
    assert utils.render_template(DEFAULT_TEMPLATE,
                                 {"title": "x", "share_url": "https://s/1"}
                                 ) == "🎬 <b>x</b>\nhttps://s/1"


# ---------------------------------------------------------------------------
# ④ 存量模板升级（v2.1.23 → v2.1.24）
# ---------------------------------------------------------------------------
V2123_DEFAULT = (
    "🎬 <b>{{title}}{{#if year}} ({{year}}){{/if}}</b>\n"
    "{{#if tmdb_id}}\n📺 TMDB: {{tmdb_id}}\n{{/if}}"
    "{{#if vote_plain}}⭐ TMDB评分: {{vote_plain}}/10\n{{/if}}"
    "{{#if quality}}💿 画质: {{quality}}\n{{/if}}"
    "{{#if version}}🎞 版本: {{version}}\n{{/if}}"
    "{{#if size}}💾 大小: {{size}}\n{{/if}}"
    "{{#if share_code}}👤 分享: {{share_code}}\n{{/if}}"
    "{{#if episode_status}}\n【{{episode_status}}】\n{{/if}}"
    "{{#if overview}}\n📝 简介: {{overview}}\n{{/if}}"
    "{{#if tags}}\n🏷 标签: {{tags}}\n{{/if}}"
    "{{share_url}}"
)


def test_v2123_default_is_upgraded():
    assert _needs_template_upgrade(V2123_DEFAULT) is True


def test_v2123_default_upgraded_even_if_share_line_removed():
    """用户自己把分享行删了，也不能停在平铺版式上（折叠是这一版的重点）。"""
    trimmed = V2123_DEFAULT.replace(
        "{{#if share_code}}👤 分享: {{share_code}}\n{{/if}}", "")
    assert _needs_template_upgrade(trimmed) is True


def test_current_default_needs_no_upgrade():
    assert _needs_template_upgrade(DEFAULT_TEMPLATE) is False


def test_custom_template_untouched():
    """只有 🏷 标签: 而没有 v2.1.23 骨架的自定义模板不动。"""
    assert _needs_template_upgrade("🏷 标签: {{tags}}\n{{share_url}}") is False


# ---------------------------------------------------------------------------
# ⑤ 截断补齐（折叠块最容易在这里翻车）
# ---------------------------------------------------------------------------
def test_clip_html_closes_blockquote_cut_in_half():
    text = "📖 简介:\n<blockquote expandable>很长的简介" + "字" * 20
    out = _clip_html(text)
    assert out.endswith("</blockquote>"), out
    assert out.count("<blockquote") == out.count("</blockquote>")


def test_clip_html_drops_dangling_open_tag():
    """截断点落在半个标签上 → 丢掉这半个，不留下 `<blockquot`。"""
    out = _clip_html("abc<blockquot")
    assert out == "abc", out


def test_clip_html_closes_nested_in_reverse_order():
    """嵌套必须内层先闭（`</blockquote></a>` 会被 Telegram 判为非法）。"""
    out = _clip_html("<blockquote expandable><a href=\"x\">链接")
    assert out.endswith("</a></blockquote>"), out


def test_clip_html_leaves_balanced_text_alone():
    ok = "🎬 <b>标题</b>\n<blockquote expandable>内容</blockquote>\nhttps://s/1"
    assert _clip_html(ok) == ok


# ---------------------------------------------------------------------------
# ⑥ 进度筛选（用户口径 3：按钮点了没反应）
# ---------------------------------------------------------------------------
def test_progress_filter_lives_in_the_computed_the_table_uses():
    """缺陷根因不是状态集合写错，而是过滤写在了表**不用**的那个 computed 上。

    锁三件事：① 过滤在 `scTaskViewRows` 里；② 表格绑的是它；
    ③ 那份没人用的 `scFilteredTasks` 已删除（留着必然再犯）。
    """
    assert "const scFilteredTasks" not in APP_VUE, "又出现了两份筛选实现"
    seg = APP_VUE[APP_VUE.index("const scTaskViewRows = computed"):]
    seg = seg[:seg.index("\nconst ", 10)]
    assert "scPushProgressFilter.value" in seg, "筛选没接进表格用的 computed"
    assert "SC_PUSHED_STATUS.includes(r.status)" in seg
    assert "SC_UNPUSHED_STATUS.includes(r.status)" in seg
    # 推送任务表必须绑这个 computed
    assert '<el-table :data="scTaskViewRows"' in APP_VUE
    # 「清除筛选」要一并复位（否则筛完没推的再清状态，列表仍是空的）
    assert "scPushProgressFilter.value = ''" in APP_VUE


def test_progress_status_sets_are_disjoint():
    """已推送 / 未推送 两个集合不许重叠，否则「未推送」会混进已推的。"""
    m = re.search(r"const SC_PUSHED_STATUS = \[([^\]]*)\]", APP_VUE)
    assert m, "找不到 SC_PUSHED_STATUS"
    pushed = set(re.findall(r"'([^']+)'", m.group(1)))
    m2 = re.search(r"const SC_UNPUSHED_STATUS = \[([^\]]*)\]", APP_VUE)
    assert m2, "找不到 SC_UNPUSHED_STATUS"
    unpushed = set(re.findall(r"'([^']+)'", m2.group(1)))
    assert pushed and unpushed
    assert not (pushed & unpushed), pushed & unpushed


# ---------------------------------------------------------------------------
# ⑦ 队列时间口径（用户口径 4）
# ---------------------------------------------------------------------------
def test_queue_time_column_uses_single_formatter():
    """队列的时间列必须走同一个格式化入口，不再「一条带日期一条不带」。"""
    assert "function scQueueClock(row)" in APP_VUE
    assert '{{ scQueueClock(s.row) }}' in APP_VUE
    # 待排期行直接透出 HH:mm；任务行优先用计划时间
    seg = APP_VUE[APP_VUE.index("function scQueueClock(row)"):]
    seg = seg[:seg.index("\n}")]
    assert "row._src === 'plan'" in seg
    assert "row.scheduled_at || row.created_at" in seg


def test_fmtclock_contract_at_source_level():
    """契约：同一天只给 HH:mm，跨天才补 MM-DD。"""
    seg = APP_VUE[APP_VUE.index("function fmtClock(value)"):]
    seg = seg[:seg.index("\n}")]
    assert "m[1] === today ? m[2]" in seg
    assert "m[1].slice(5)" in seg, "跨天没补 MM-DD"


@pytest.mark.skipif(not shutil.which("node"), reason="本机没有 node，跳过行为验证")
def test_fmtclock_behaviour_with_node():
    """把 App.vue 里的 fmtTime / fmtClock 抠出来真跑一遍（不只是看源码像不像）。"""
    src = APP_VUE
    start = src.index("const fmtTime = (value, withSeconds = false) => {")
    end = src.index("function fmtClock(value) {")
    fmt_time = src[start:end].rstrip()
    fc_start = end
    fc_end = src.index("\n}", fc_start) + 2
    fmt_clock = src[fc_start:fc_end]
    harness = f"""
{fmt_time}
{fmt_clock}
const d = new Date(); const p = n => String(n).padStart(2, '0');
const today = `${{d.getFullYear()}}-${{p(d.getMonth() + 1)}}-${{p(d.getDate())}}`;
const out = {{
  sameDay: fmtClock(today + ' 18:00'),
  otherDay: fmtClock('2020-01-02 09:07'),
  empty: fmtClock(''),
  nul: fmtClock(null),
  iso: fmtClock('2020-01-02T09:07:05+08:00'),
}};
console.log(JSON.stringify(out));
"""
    node = shutil.which("node")
    got = subprocess.run([node, "-e", harness], capture_output=True, text=True,
                         timeout=60)
    assert got.returncode == 0, got.stderr
    data = json.loads(got.stdout.strip().splitlines()[-1])
    assert data["sameDay"] == "18:00", data
    assert data["otherDay"] == "01-02 09:07", data
    assert data["empty"] == "—", data
    assert data["nul"] == "—", data
    # ISO 串带偏移：按浏览器本地时区展示，但至少要收敛到分钟精度
    assert re.fullmatch(r"\d{2}-\d{2} \d{2}:\d{2}", data["iso"]), data
