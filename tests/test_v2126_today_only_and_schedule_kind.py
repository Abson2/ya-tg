"""v2.1.26：三表只看当天 + 表头统一六列 + 排期类型独立成字段。

用户口径（原话）：
  「推送历史中时间去掉日期，推送历史、推送队列、推送失败均只展示当天（0-24 小时）的数据」
  「三个表表头统一为：时间、资源名称、分类、推送类型、排期类型、状态」

本文件锁死四件事：

① **当天口径**（后端）：三视图按各自「时间列显示的那个字段」判日，跨天的行不进表。
   历史=推送成功时间（没有退最后更新）、队列=计划时间（没有退创建时间）、失败=最后更新时间。
   ⚠️ 排序键与筛选键必须是同一个字段 —— 一条昨天的失败任务今天重试又失败，id 很小但
   `updated_at` 是今天，按 id 排序就会把它漏在预取窗口之外（本文件有反证用例）。

② **预取窗口不吃昨天的行**：当天只有 1 条时，表里就只有 1 行，绝不用旧行把表填满。

③ **排期类型（schedule_kind）**：新上映 > 新入库 > 存量排期，来自**未裁剪**的
   `_work_push_tags` 结果 —— 与「资源名称」旁的标签**不是同一件事**：
   标签回答「现在还算不算新」（终态行去掉「存量排期」会变空），
   排期类型回答「这条当时按哪一档排上的」（历史行照样看得到）。

④ **前端契约**：三表表头顺序一致（时间 / 资源名称 / 分类 / 推送类型 / 排期类型 / 状态）、
   时间列只给 `HH:mm`、失败表的原因没丢、重试按钮还在。
"""
import json

import pytest
from sqlalchemy import delete, select

from app import share_center_v2 as sc2
from app import utils
from app.db import SessionLocal, migrate
from app.models import (
    AppConfig,
    Bot,
    Channel,
    DistributionEvent,
    QueueItem,
    Resource,
    ResourceVersion,
    Rule,
    SCCategory,
    SCLibraryVersion,
    SCPublishTask,
    SCPushList,
    SCPushListItem,
    SCWorkCategory,
    ShareTask,
)
from datetime import datetime, timedelta, timezone

WIPED = (DistributionEvent, ResourceVersion, SCPublishTask, SCPushListItem,
         SCPushList, SCLibraryVersion, SCWorkCategory, SCCategory,
         QueueItem, ShareTask, Rule, Resource, Channel, Bot, AppConfig)

HEADERS = ("时间", "资源名称", "分类", "推送类型", "排期类型", "状态")


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    for _ in range(2):
        with SessionLocal() as db:
            for model in WIPED:
                db.execute(delete(model))
            db.commit()
    yield


def _now():
    return datetime.now(timezone.utc)


def _work(db, *, title="银河护卫队2", file_id="f-1", release_date="",
          media_type="movie", finish_status="", entry_days_ago=0, with_snapshot=True):
    """造一部作品：资源 + （可选）库快照 + 分类归属。"""
    res = Resource(file_id=file_id, name=f"{title}.mkv", ext=".mkv", size=100,
                   utime=1, identify_status="identified", media_type=media_type,
                   tmdb_id=1, media_title=title, release_date=release_date,
                   finish_status=finish_status, share_dir_name=title,
                   share_scope="series_dir", share_status="created",
                   share_url="https://share/x")
    db.add(res)
    db.flush()
    wk = utils.work_group_key(res)
    if with_snapshot:
        db.add(SCLibraryVersion(
            work_key=wk, version_no=1, fingerprint=f"fp-{file_id}",
            media_type=media_type, resource_ids_json=json.dumps([res.id]),
            metadata_json=json.dumps({"title": title}, ensure_ascii=False),
            created_at=_now() - timedelta(days=entry_days_ago),
            change_type="created"))
    db.add(SCWorkCategory(work_key=wk, category_key="genre:动作"))
    # 分类字典本文件会造多部作品，只建一次（category_key 有唯一约束）
    if db.scalar(select(SCCategory).where(
            SCCategory.category_key == "genre:动作")) is None:
        db.add(SCCategory(category_key="genre:动作", name="动作", enabled=True,
                          priority=0))
    db.commit()
    return res, wk


def _task(db, wk, *, kind="first", status="planned", published_at=None,
          updated_at=None, scheduled_at=None, error="", category_key="genre:动作"):
    suffix = f"{kind}-{status}-{int(_now().timestamp() * 1000)}-{id(wk)}"
    lst = SCPushList(list_key=f"l-{suffix}", name="t", status="active")
    db.add(lst)
    db.flush()
    item = SCPushListItem(list_id=lst.id, work_key=wk, chat_id="-1001234567890",
                          dedup_key=f"{wk}:{suffix}", category_key=category_key,
                          status="planned")
    db.add(item)
    db.flush()
    task = SCPublishTask(list_item_id=item.id, work_key=wk, kind=kind, status=status,
                         link_state="unknown", error=error,
                         published_at=published_at, scheduled_at=scheduled_at)
    db.add(task)
    db.flush()
    if updated_at is not None:
        task.updated_at = updated_at
    db.commit()
    return task


# ==========================================================================
# ① 三视图只给当天
# ==========================================================================
def test_history_only_shows_today():
    """昨天的成功推送不进「推送历史」——这张卡只看今天推了什么。"""
    with SessionLocal() as db:
        _res, wk_old = _work(db, title="老片", file_id="f-old")
        _res2, wk_new = _work(db, title="新片", file_id="f-new")
        _task(db, wk_old, status="published",
              published_at=_now() - timedelta(days=1, hours=2))
        _task(db, wk_new, status="published", published_at=_now())
        views = sc2._aggregate_views(db)
    titles = [r["title"] for r in views["history"]]
    assert titles == ["新片"], f"推送历史必须只有今天的记录，实际 {titles}"


def test_failed_only_shows_today():
    """按「最后失败时间」判日：昨天失败、今天没再失败 → 不进失败表。"""
    with SessionLocal() as db:
        _res, wk_old = _work(db, title="老失败", file_id="f-fo")
        _res2, wk_new = _work(db, title="新失败", file_id="f-fn")
        _task(db, wk_old, status="failed", error="超时",
              updated_at=_now() - timedelta(days=2))
        _task(db, wk_new, status="failed", error="超时", updated_at=_now())
        views = sc2._aggregate_views(db)
    titles = [r["title"] for r in views["failed"]]
    assert titles == ["新失败"], f"失败表必须只有今天的记录，实际 {titles}"


def test_failed_sorted_by_updated_at_not_id():
    """反证：昨天的失败任务今天重试又失败 —— id 小但 `updated_at` 在今天，必须进表。

    按 id 排序的旧写法会把它漏在预取窗口之外（这正是本用例要钉住的缺陷）。
    """
    with SessionLocal() as db:
        _res, wk_old = _work(db, title="老任务重试", file_id="f-retry")
        _res2, wk_new = _work(db, title="今天新建", file_id="f-today")
        # 先建老任务（id 小），更新时间为「两小时前」；再建一条 id 更大的无关任务
        old = _task(db, wk_old, status="failed", error="重试仍失败",
                    updated_at=_now() - timedelta(hours=2))
        old_id = old.id
        _task(db, wk_new, status="failed", error="x",
              updated_at=_now() - timedelta(days=1))
        # 老任务今天重试过 → 刷新 updated_at（id 仍然最小）
        old = db.get(SCPublishTask, old_id)
        old.updated_at = _now()
        db.commit()
        views = sc2._aggregate_views(db)
    rows = [r for r in views["failed"] if r["id"] == old_id]
    assert rows, "id 最小的失败任务今天重试过，必须按 updated_at 被捞进失败表"


def test_queue_uses_scheduled_at_and_shows_today_only():
    """队列按「计划时间」判日，且 `scheduled_at` 必须真的传给前端。

    v2.1.24 起前端就是「计划时间优先、没有才退创建时间」，但后端白名单里一直漏了
    `scheduled_at` —— 这个字段没出去，前端只能退回创建时间。本用例同时钉住两件事。
    """
    with SessionLocal() as db:
        _res, wk_y = _work(db, title="昨天排的", file_id="f-qy")
        _res2, wk_t = _work(db, title="今天排的", file_id="f-qt")
        _task(db, wk_y, status="planned", scheduled_at=_now() - timedelta(days=1))
        _task(db, wk_t, status="planned", scheduled_at=_now() + timedelta(hours=1))
        views = sc2._aggregate_views(db)
    titles = [r["title"] for r in views["queue"]]
    assert titles == ["今天排的"], f"队列只给今天的计划，实际 {titles}"
    row = views["queue"][0]
    assert "scheduled_at" in row and row["scheduled_at"], (
        "队列行必须带计划时间，否则前端「计划时间优先」永远拿不到值")
    assert row["scheduled_at"][:10] == _now().astimezone(
        sc2.shanghai_tz()).strftime("%Y-%m-%d")


def test_stale_rows_never_pad_the_table():
    """当天只有 1 条时，表里就 1 行 —— 绝不用昨天的行把卡片填满。"""
    with SessionLocal() as db:
        for i in range(5):
            _res, wk = _work(db, title=f"旧{i}", file_id=f"f-pad-{i}")
            _task(db, wk, status="published",
                  published_at=_now() - timedelta(days=3, minutes=i))
        _res, wk = _work(db, title="今天唯一", file_id="f-pad-today")
        _task(db, wk, status="published", published_at=_now())
        views = sc2._aggregate_views(db, limit=12)
    assert [r["title"] for r in views["history"]] == ["今天唯一"]


def test_today_rows_beyond_limit_are_capped():
    """当天行多于 limit 时按时间倒序取前 limit 条（不是把旧的排前面）。"""
    with SessionLocal() as db:
        base = _now()
        for i in range(4):
            _res, wk = _work(db, title=f"今日{i}", file_id=f"f-cap-{i}")
            _task(db, wk, status="published",
                  published_at=base - timedelta(minutes=10 * i))
        views = sc2._aggregate_views(db, limit=2)
    titles = [r["title"] for r in views["history"]]
    assert titles == ["今日0", "今日1"], f"应按时间倒序取最近 2 条，实际 {titles}"


# ==========================================================================
# ② 排期类型
# ==========================================================================
def _kind_of(db, wk):
    return sc2._schedule_kind_of(sc2._work_push_tags(db, [wk]).get(wk, []))


def test_schedule_kind_survives_terminal_tag_trim():
    """历史行标签被裁掉「存量排期」，但排期类型仍要显示「存量排期」。

    这是「排期类型」必须独立成字段的理由：`tags` 回答「现在还算不算新」，
    `schedule_kind` 回答「当时按哪一档排上的」。
    """
    today = _now().astimezone(sc2.shanghai_tz()).date()
    with SessionLocal() as db:
        # 10 天前入库 + 60 天前上映 → 只判得出「存量」（新入库/新上映都过期）
        res, wk = _work(db, title="老片", file_id="f-backlog", entry_days_ago=10,
                        release_date=(today - timedelta(days=60)).isoformat())
        task = _task(db, wk, status="published", published_at=_now())
        views = sc2._aggregate_views(db)
    row = [r for r in views["history"] if r["id"] == task.id][0]
    assert row["tags"] == [], "终态行不显示「存量排期」标签（用户口径：失效就消失）"
    assert row["schedule_kind"] == "存量排期", (
        "排期类型不能被终态裁剪影响——否则历史表这一列永远是空的")


def test_schedule_kind_new_release_beats_new_library():
    """同时命中「新上映」与「新入库」时，排期类型归「新上映」（与排期归列同序）。"""
    today = _now().astimezone(sc2.shanghai_tz()).date()
    with SessionLocal() as db:
        res, wk = _work(db, title="新上映新入库", file_id="f-both",
                        release_date=(today - timedelta(days=1)).isoformat(),
                        entry_days_ago=0)
        got = _kind_of(db, wk)
        tags = [t["key"] for t in sc2._work_push_tags(db, [wk])[wk]]
    assert "new_release" in tags and "new_library" in tags, tags
    assert got == "新上映", f"排期类型优先级应为 新上映 > 新入库，实际 {got!r}"


def test_schedule_kind_new_library_when_only_today_entry():
    with SessionLocal() as db:
        res, wk = _work(db, title="今天刚入库", file_id="f-lib",
                        release_date="", entry_days_ago=0)
        assert _kind_of(db, wk) == "新入库"


def test_schedule_kind_blank_when_undecidable():
    """入库时间与上映日期都判不出 → 空串（页面显示「—」），不猜成存量排期。"""
    with SessionLocal() as db:
        res, wk = _work(db, title="啥都没有", file_id="f-none", media_type="",
                        release_date="", with_snapshot=False)
        res.ctime = 0
        db.commit()
        assert _kind_of(db, wk) == ""
        task = _task(db, wk, status="published", published_at=_now())
        views = sc2._aggregate_views(db)
    row = [r for r in views["history"] if r["id"] == task.id][0]
    assert row["schedule_kind"] == "", "判不出就留空，绝不默认标成存量"
    assert row["tags"] == []


def test_forecast_works_carry_schedule_kind():
    """今日排期的作品项也带排期类型，「推送队列」的待排期行直接透传，不自己再算。"""
    from app.models import SCSharePlan, SCSharePlanVersion
    from conftest import ensure_full_day_hour_window
    today = _now().astimezone(sc2.shanghai_tz()).date()
    with SessionLocal() as db:
        bot = Bot(name="b", token="x", enabled=True)
        db.add(bot)
        db.flush()
        db.add(Channel(bot_id=bot.id, name="频道", chat_id="-1001234567890",
                       kind="channel", enabled=True))
        db.commit()
        plan = SCSharePlan(name="p", plan_type="daily", enabled=True)
        db.add(plan)
        db.flush()
        ver = SCSharePlanVersion(plan_id=plan.id, version_no=1, status="active",
                                 config_json=json.dumps({"category_limit": 20,
                                                         "work_limit": 50}))
        db.add(ver)
        db.flush()
        ensure_full_day_hour_window(db, ver.id)
        db.commit()
        _work(db, title="黑豹2", file_id="f-fc",
              release_date=(today - timedelta(days=1)).isoformat())
        out = sc2.forecast_schedule(db)
    works = [w for h in out["hours"] for it in h["items"] for w in it.get("works", [])]
    assert works, "排期必须真的排出作品，否则本用例空转"
    for w in works:
        assert "schedule_kind" in w, "作品项要带排期类型"
        assert w["schedule_kind"] in ("新上映", "新入库", "存量排期", "")
        # v2.1.28：连 key 一起给，前端给「新入库」上绿色高亮时按 key 映射
        assert "schedule_kind_key" in w, "作品项要带排期类型的 key"
        assert w["schedule_kind_key"] in ("new_release", "new_library", "backlog", "")


# ==========================================================================
# ③ 前端：三表表头统一 + 时间只给时刻
# ==========================================================================
def _app_source() -> str:
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    return (root / "frontend/src/App.vue").read_text(encoding="utf-8")


def _table_headers(app: str, anchor: str) -> list[str]:
    """从锚点（三张表的 data 表达式）往后取第一张表的表头文字。"""
    seg = app[app.index(anchor):]
    seg = seg[:seg.index("</el-table>")]
    import re
    labels = re.findall(r'label="([^"]+)"', seg)
    return labels


def test_three_tables_share_the_same_headers():
    """三表表头必须一模一样（顺序也一致）：时间 / 资源名称 / 分类 / 推送类型 / 排期类型 / 状态。"""
    app = _app_source()
    for anchor, name in (
            ('scOverview.views?.history || []', "推送历史"),
            ('<el-table :data="scQueueRows"', "推送队列"),
            ('scOverview.views?.failed || []', "推送失败")):
        labels = _table_headers(app, anchor)
        head = [x for x in labels if x in HEADERS]
        assert head[:len(HEADERS)] == list(HEADERS), (
            f"{name}表头不对：{labels} —— 三表必须都是 {list(HEADERS)}")


def test_failed_table_keeps_reason_and_retry_button():
    """失败原因不能因为「表头统一」就丢了，重试按钮也不许删。"""
    app = _app_source()
    seg = app[app.index('scOverview.views?.failed || []'):]
    seg = seg[:seg.index("</el-table>")]
    assert "s.row.error" in seg, "失败原因要并入「状态」列展示，不能整列删掉"
    assert "scTaskAction(s.row.id, 'retry')" in seg, "重试按钮不得删除"


def test_time_column_shows_clock_only():
    """三表时间列都走 `scDayClock` / `scQueueClock`，且不再出现带日期的 `fmtTime`。"""
    app = _app_source()
    assert "function scDayClock(value)" in app
    for anchor in ('scOverview.views?.history || []', 'scOverview.views?.failed || []'):
        seg = app[app.index(anchor):]
        seg = seg[:seg.index("</el-table>")]
        assert "scDayClock(" in seg
        assert "fmtTime(s.row." not in seg, "时间列不许再直出带日期的完整时间"
    seg = app[app.index('<el-table :data="scQueueRows"'):]
    seg = seg[:seg.index("</el-table>")]
    assert "scQueueClock(s.row)" in seg


def test_schedule_kind_label_single_entry():
    """排期类型收敛成一个函数，且只认后端字段（不回退成 tags / kind）。"""
    app = _app_source()
    assert "function scScheduleKindLabel(row)" in app
    seg = app[app.index("function scScheduleKindLabel(row)"):]
    seg = seg[:seg.index("\n}")]
    assert "row.schedule_kind" in seg
    assert "tags" not in seg, (
        "前端不许自己从 tags 里挑排期类型，口径只在后端 `_schedule_kind_of`")


@pytest.mark.skipif(__import__("shutil").which("node") is None, reason="本机没有 node")
def test_scDayClock_behaviour_with_node():
    """真跑一遍：同天只给 HH:mm，跨天也只给 HH:mm（不冒出 MM-DD）。"""
    import shutil
    import subprocess
    app = _app_source()
    start = app.index("const fmtTime = (value, withSeconds = false) => {")
    end = app.index("function fmtClock(value) {")
    fmt_time = app[start:end].rstrip()
    fc_start = end
    fc_end = app.index("\n}", fc_start) + 2
    fmt_clock = app[fc_start:fc_end]
    sd_start = app.index("function scDayClock(value) {")
    sd_end = app.index("\n}", sd_start) + 2
    day_clock = app[sd_start:sd_end]
    harness = f"""
{fmt_time}
{fmt_clock}
{day_clock}
const d = new Date(); const p = n => String(n).padStart(2, '0');
const today = `${{d.getFullYear()}}-${{p(d.getMonth() + 1)}}-${{p(d.getDate())}}`;
const out = {{
  today: scDayClock(today + ' 18:00'),
  cross: scDayClock('2020-01-02 09:07'),
  empty: scDayClock(''),
  nul: scDayClock(null),
}};
console.log(JSON.stringify(out));
"""
    got = subprocess.run([shutil.which("node"), "-e", harness],
                         capture_output=True, text=True, timeout=60)
    assert got.returncode == 0, got.stderr
    data = json.loads(got.stdout.strip().splitlines()[-1])
    assert data["today"] == "18:00", data
    assert data["cross"] == "09:07", f"跨天也只给时刻，实际 {data['cross']!r}"
    assert data["empty"] == "—" and data["nul"] == "—", data
