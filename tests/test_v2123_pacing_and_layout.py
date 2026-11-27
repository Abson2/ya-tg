"""v2.1.23：时段匀速推送 + 标签过期口径 + 版面收敛。

用户原话（2026-11-24）：
  3、「新上映至官方剧集更新完结（tmdb 数据）延后一天的 0 时转为存量，
      每日 0-24 小时入库在当日算新入库，24 时后转为存量」
  4、「推送影视应该每小时内均匀排布，不要到时间排期一起蜂拥推送：举例，
      本时段为 2 小时，有 4 部剧那么应该是 2 小时*60 分钟/4 部剧 = 30 分钟
      推送一部，除不尽时四舍五入」
  1（追加）「新上映标签失效时，推送历史里该标签应消失」

本文件锁死：
  ① `_new_release_state` 三态：returning 在映 / ended 按 last_air_date 次日 0 点过期 /
     电影按 30 天窗口 / 数据不足返回 None（绝不猜）；
     v2.1.56 起 returning 也要看 `last_air_date`（长期停更转存量），
     本文件那条「拿不到播出日」的用例仍按原行为算新上映，
     「播出日太老」的边界由 `test_v2156_new_release_returning.py` 锁；
  ② 时段匀速：间隔 = 四舍五入(时段分钟 ÷ 部数)，站台锚定时段起点；
  ③ `_period_pace` / `_period_released` 与预测同一口径（预测不许比实跑偏快）；
  ④ 终态记录不显示「存量排期」；
  ⑤ forecast 一次给全天（v2.1.24 起取消 `paged` / `from_hour` 分页取数）；
  ⑥ 前端契约：标签在剧名左侧、已推送/未推送可筛、预览带示例数据。
"""
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import delete

from app import share_center_v2 as sc2
from app.db import SessionLocal, migrate
from app.models import (
    AppConfig, AuditLog, Bot, Channel, QueueItem, Resource, SCCategory,
    SCCategoryCursor, SCCategorySchedule, SCHourWindow, SCLibraryVersion,
    SCPublishTask, SCPushList, SCPushListItem, SCSharePlan, SCSharePlanVersion,
    SCWorkCategory, ShareTask,
)

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
    yield


def _sh(dt: datetime) -> datetime:
    return dt.astimezone(sc2.shanghai_tz())


# ---------------------------------------------------------------------------
# ① 「新上映」过期口径（用户口径 3）
# ---------------------------------------------------------------------------
def test_new_release_state_returning_stays_valid():
    """returning 但**拿不到**最后一集播出日 → 无从判断，仍算「新上映」。

    v2.1.56：returning 不再是「无条件永不过期」——播出日超过
    `RETURNING_STALE_DAYS` 天会转存量（见 `test_v2156_new_release_returning.py`）。
    本条锁的是「拿不到日期时不猜」这一半：宁可继续算新上映，也不编一个日期。
    """
    now = _sh(datetime.now(timezone.utc))
    assert sc2._new_release_state(
        release_date="2020-01-01", last_air_date="", finish_status="returning",
        media_type="tv", now_sh=now) is True


def test_new_release_state_ended_expires_after_last_air_date():
    """ended → 「最后一集播出日 + 1 天 0 点」转存量（用户口径原话）。"""
    now = _sh(datetime.now(timezone.utc)).replace(hour=12, minute=0, second=0, microsecond=0)
    air = now.date()
    assert sc2._new_release_state(
        release_date="2020-01-01", last_air_date=str(air), finish_status="ended",
        media_type="tv", now_sh=now) is True
    # 最后一集是昨天 → 今天 0 点已过期
    assert sc2._new_release_state(
        release_date="2020-01-01", last_air_date=str(air - timedelta(days=1)),
        finish_status="ended", media_type="tv", now_sh=now) is False


def test_new_release_state_ended_without_air_date_is_not_guessed():
    """ended 但拿不到最后一集播出日 → 不猜「还在映」，直接判过期。"""
    now = _sh(datetime.now(timezone.utc))
    assert sc2._new_release_state(
        release_date="2020-01-01", last_air_date="", finish_status="ended",
        media_type="tv", now_sh=now) is False


def test_new_release_state_movie_uses_window():
    """电影没有连载概念 → 沿用 30 天窗口。"""
    now = _sh(datetime.now(timezone.utc))
    recent = str((now - timedelta(days=5)).date())
    late = str((now - timedelta(days=sc2.NEW_RELEASE_WINDOW_DAYS + 5)).date())
    assert sc2._new_release_state(
        release_date=recent, last_air_date="", finish_status="",
        media_type="movie", now_sh=now) is True
    assert sc2._new_release_state(
        release_date=late, last_air_date="", finish_status="",
        media_type="movie", now_sh=now) is False


def test_new_release_state_missing_data_is_none():
    """什么数据都没有 → None（调用方据此留空，不许编一个结论）。"""
    now = _sh(datetime.now(timezone.utc))
    assert sc2._new_release_state(
        release_date="", last_air_date="", finish_status="",
        media_type="movie", now_sh=now) is None


def test_parse_dt_date_rejects_garbage():
    assert sc2._parse_dt_date("2026-11-18") is not None
    assert sc2._parse_dt_date("") is None
    assert sc2._parse_dt_date("not-a-date") is None
    assert sc2._parse_dt_date(None) is None


# ---------------------------------------------------------------------------
# ② 时段匀速：2 小时 4 部 = 30 分钟一部（用户口径 4）
# ---------------------------------------------------------------------------
def test_pace_interval_matches_user_example():
    """用户原话：本时段 2 小时、4 部 → 120/4 = 30 分钟一部。"""
    start = datetime(2026, 11, 24, 18, 0, tzinfo=sc2.shanghai_tz())
    end = start + timedelta(hours=sc2.HOUR_SEGMENT_HOURS)
    assert sc2._pace_interval_minutes(start, end, 4) == 30


def test_pace_interval_rounds_when_not_divisible():
    """除不尽时四舍五入：120/7 = 17.14 → 17；120/9 = 13.3 → 13；120/8 = 15。"""
    start = datetime(2026, 11, 24, 18, 0, tzinfo=sc2.shanghai_tz())
    end = start + timedelta(hours=2)
    assert sc2._pace_interval_minutes(start, end, 7) == 17
    assert sc2._pace_interval_minutes(start, end, 9) == 13
    assert sc2._pace_interval_minutes(start, end, 8) == 15
    # 极端：部数多于分钟数也至少 1 分钟，不能挤在同一时刻
    assert sc2._pace_interval_minutes(start, end, 500) == 1


def test_pace_slot_times_anchor_at_period_start():
    """站台锚定**时段起点**（与 dispatch 的 due 公式同源），不是锚在"现在"。"""
    start = datetime(2026, 11, 24, 18, 0, tzinfo=sc2.shanghai_tz())
    end = start + timedelta(hours=2)
    slots = sc2._pace_slot_times(start, end, 4)
    assert [s.strftime("%H:%M") for s in slots] == ["18:00", "18:30", "19:00", "19:30"]


def test_period_bounds_align_to_segment():
    """时段按 HOUR_SEGMENT_HOURS 对齐整点：18:37 落在 18:00-20:00 这一段。"""
    now = datetime(2026, 11, 24, 18, 37, tzinfo=sc2.shanghai_tz())
    start, end = sc2._period_bounds(now)
    assert (start.hour, start.minute) == (18, 0)
    assert end.hour == 20 and end.minute == 0
    # 21 点整落在 20:00-22:00
    start2, end2 = sc2._period_bounds(now.replace(hour=21, minute=0))
    assert (start2.hour, end2.hour) == (20, 22)


# ---------------------------------------------------------------------------
# ③ 终态不显示「存量排期」（用户追加口径 1）
# ---------------------------------------------------------------------------
def test_tags_for_status_strips_backlog_on_finished_rows():
    tags = [{"key": "backlog", "label": "存量排期"}]
    # 在途 → 原样返回（存量排期解释的是「还没推的原因」）
    assert sc2._tags_for_status(tags, "planned") == tags
    # 终态 → 存量排期消失
    assert sc2._tags_for_status(tags, "published") == []
    assert sc2._tags_for_status(tags, "failed") == []


def test_tags_for_status_keeps_valid_tags_on_finished_rows():
    """新入库 / 新上映这类**仍然有效**的标签，终态行照旧显示。"""
    tags = [{"key": "new_release", "label": "新上映"},
            {"key": "backlog", "label": "存量排期"}]
    assert sc2._tags_for_status(tags, "published") == [
        {"key": "new_release", "label": "新上映"}]


# ---------------------------------------------------------------------------
# ④ 排期时段均分后回写（用户口径 4 的呈现侧）
# ---------------------------------------------------------------------------
def test_respread_forecast_times_evens_out_within_segment():
    """跨两个小时的同一时段，时间必须按「时段」均分，而不是每小时各摊一遍。"""
    now = datetime(2026, 11, 24, 18, 5, tzinfo=sc2.shanghai_tz())
    works = [{"work_key": f"work:{i}", "time": ""} for i in range(4)]
    hours = [
        {"hour": 18, "total": 2, "items": [
            {"category": "剧情", "count": 2, "times": [], "works": works[:2],
             "works_total": 2}]},
        {"hour": 19, "total": 2, "items": [
            {"category": "剧情", "count": 2, "times": [], "works": works[2:],
             "works_total": 2}]},
    ]
    pace = sc2._respread_forecast_times(hours, now)
    times = [w["time"] for w in works]
    # 间隔按「整个时段 120 分钟 ÷ 4 部 = 30 分钟」算，站台锚在"现在"（18:05）：
    # 18:05 / 18:35 / 19:05 / 19:35。跨小时不影响间隔。
    assert all(times), times
    absmin = [int(t.split(":")[0]) * 60 + int(t.split(":")[1]) for t in times]
    gaps = [b - a for a, b in zip(absmin, absmin[1:])]
    assert all(g == 30 for g in gaps), (times, gaps)
    assert pace["interval_minutes"] == 30
    assert pace["works"] == 4
    assert pace["period_hours"] == sc2.HOUR_SEGMENT_HOURS


def test_respread_dedupes_same_time_for_multi_channel():
    """同一部剧推频道 + 群是同一个时刻，行里不该重复铺 N 遍。"""
    now = datetime(2026, 11, 24, 18, 0, tzinfo=sc2.shanghai_tz())
    works = [{"work_key": "work:a", "time": ""}, {"work_key": "work:b", "time": ""}]
    hours = [{"hour": 18, "items": [
        {"category": "剧情", "count": 2, "times": [], "works": works,
         "works_total": 2}]}]
    sc2._respread_forecast_times(hours, now)
    assert hours[0]["items"][0]["times"] == ["18:00", "18:00"][:1] + [
        t for t in hours[0]["items"][0]["times"] if t != "18:00"]
    # 去重后不得有重复值
    got = hours[0]["items"][0]["times"]
    assert len(got) == len(set(got)), got


def test_respread_skips_past_segments():
    """已经过去的时段不再报时间（排期回答的是「接下来推什么」）。"""
    now = datetime(2026, 11, 24, 21, 0, tzinfo=sc2.shanghai_tz())
    works = [{"work_key": "work:a", "time": ""}]
    hours = [{"hour": 18, "items": [
        {"category": "剧情", "count": 1, "times": [], "works": works,
         "works_total": 1}]}]
    sc2._respread_forecast_times(hours, now)
    assert works[0]["time"] == ""


# ---------------------------------------------------------------------------
# ⑤ forecast 全天一次返回（v2.1.24 起取消分页）
#
# v2.1.23 曾按「时段」分批取数（`paged` / `from_hour` / `page.next_hour`），
# 用户实测后否掉了：「点一下才多两小时」比滚动条更麻烦。分页整套删除后，
# 这里只锁「一次给全天」「签名里没有分页参数」「节奏回写到每小时」三件事。
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
    ch = Channel(bot_id=bot.id, name="频道", chat_id="@ch", kind="channel", enabled=True)
    db.add(ch)
    db.flush()
    return ch


def _work(db, *, title, entry_at=None, release_date="", media_type="movie",
          last_air_date="", finish_status=""):
    res = Resource(file_id=f"f-{title}", name=f"{title}.mkv", ext=".mkv", size=100,
                   utime=1, identify_status="identified", media_type=media_type,
                   tmdb_id=abs(hash(title)) % 100000, media_title=title,
                   release_date=release_date, last_air_date=last_air_date,
                   finish_status=finish_status, share_status="active",
                   share_url="https://pan.quark.cn/s/abc")
    db.add(res)
    db.flush()
    wk = f"work:{title}"
    db.add(SCLibraryVersion(
        work_key=wk, version_no=1, fingerprint=f"fp-{title}", media_type=media_type,
        resource_ids_json=json.dumps([res.id]),
        metadata_json=json.dumps({"title": title}), change_type="created",
        created_at=entry_at or (datetime.now(timezone.utc) - timedelta(days=100))))
    db.flush()
    # 排期模拟走 `_candidates`，它是 join SCWorkCategory 取候选的 —— 不挂分类，
    # 作品就永远进不了预测（表现是「造了 5 部剧、排期却全空」，极易误判成 bug）。
    db.add(SCWorkCategory(work_key=wk, category_key="genre:剧情", media_type=media_type))
    db.flush()
    return wk


def test_forecast_returns_whole_day_in_one_call():
    """v2.1.24（用户口径 1a）：一次给出今天剩余全部时段，不再有 `page` 字段。

    只读：预测不建任务（分页删了，但「预测 ≠ 建任务」这条更要守住）。
    """
    with SessionLocal() as db:
        _plan(db)
        db.add(SCCategory(category_key="genre:剧情", name="剧情", enabled=True, priority=0))
        _channel(db)
        for i in range(3):
            _work(db, title=f"剧{i}")
        db.commit()

        out = sc2.forecast_schedule(db)
        assert "page" not in out, "分页字段已删除，别再吐半个 page"
        assert out["hours"], "至少要返回当前小时"
        assert out["hours"][0]["hour"] == out["now_hour"], "不从已经过去的整点开始预测"
        assert out["hours"][-1]["hour"] == 23, "必须一路算到 24 点前最后一个小时"
        # 连续、不跳空（否则前端滚动列表会缺行）
        assert [h["hour"] for h in out["hours"]] == list(range(out["now_hour"], 24))
        # 只读：两次调用前后库内任务数不变
        assert db.query(SCPublishTask).count() == 0


def test_forecast_has_no_paging_parameters():
    """分页参数要**整套**删掉，不留半套（留半套 = 前端还会试着传）。"""
    import inspect
    fn_params = inspect.signature(sc2.forecast_schedule).parameters
    for gone in ("paged", "from_hour"):
        assert gone not in fn_params, f"forecast_schedule 还留着 {gone}"
    api_params = inspect.signature(sc2.api_forecast).parameters
    assert set(api_params) == {"db"}, f"api_forecast 还有多余参数：{api_params}"


def test_forecast_every_hour_carries_pace_minutes():
    """v2.1.24（用户口径 1c）：每小时都带「每 N 分钟 1 部」。

    用户原话：「18:24–18:42 · 每 6 分钟 1 部」不用展示那么细，展示几分钟一部就行，
    与第一列的时间段对齐 —— 所以节奏挂在**时间列**上，同一时段共用一个值。
    """
    with SessionLocal() as db:
        _plan(db)
        db.add(SCCategory(category_key="genre:剧情", name="剧情", enabled=True, priority=0))
        _channel(db)
        for i in range(4):
            _work(db, title=f"剧{i}")
        db.commit()

        out = sc2.forecast_schedule(db)
        for h in out["hours"]:
            assert "pace_minutes" in h, f"{h['hour']} 点缺 pace_minutes"
            assert isinstance(h["pace_minutes"], int), h["pace_minutes"]
        paced = [h for h in out["hours"] if h["total"]]
        assert paced, "造了 4 部剧却没有排期，用例前提不成立"
        assert all(h["pace_minutes"] >= 1 for h in paced), \
            [(h["hour"], h["pace_minutes"]) for h in paced]
        # 同一时段（HOUR_SEGMENT_HOURS）内的小时共用一个值，不各算一套
        seg = int(sc2.HOUR_SEGMENT_HOURS)
        by_seg: dict[int, set[int]] = {}
        for h in out["hours"]:
            by_seg.setdefault(h["hour"] - h["hour"] % seg, set()).add(h["pace_minutes"])
        for start, vals in by_seg.items():
            assert len(vals) == 1, f"{start} 点起的时段出现了多个间隔：{vals}"


def test_forecast_pace_minutes_zero_for_empty_segment():
    """整段没排到东西 → 0（前端据此不显示节奏文案），不编一个假间隔。

    注意这一条**不是**「0 冒充读不到」：0 的语义就是「本时段没有推送」，
    与 pacing 的 interval 无值可算是两件事。
    """
    with SessionLocal() as db:
        _plan(db)
        _channel(db)
        db.commit()
        out = sc2.forecast_schedule(db)
        assert all(h["pace_minutes"] == 0 for h in out["hours"]), \
            [(h["hour"], h["pace_minutes"]) for h in out["hours"]]


# ---------------------------------------------------------------------------
# ⑥ 前端契约
# ---------------------------------------------------------------------------
def test_frontend_tags_render_left_of_title():
    """用户口径：三类标签放在剧名左侧（不是剧名下面一行）。

    v2.1.28 起：**「推送历史 / 队列 / 失败」三张卡不再挂标签** —— 它们有了独立的
    「排期类型」列，剧名前再挂一枚「新入库」就是同一信息同一行出现两次
    （用户截图：`新入库 杨… | 新入库`）。三类标签功能没删，仍保留在「推送任务」列表
    与「今日推送排期」里，所以这里的结构断言从「四处」收紧为「仍在用的人必须用同一个
    结构」，并补一条反证钉死三表不再回归。
    """
    assert ".sc-title-row" in APP_VUE
    assert ".sc-title-text" in APP_VUE
    # 仍在使用标签的地方（推送任务列表）必须还是「标签在剧名左侧」的同一行结构
    assert APP_VUE.count('class="sc-title-row"') >= 1, "标签未统一到剧名左侧"
    # 不允许再出现「剧名一行、标签另起一行」的老结构
    assert '<div>{{ s.row.title }}</div>' not in APP_VUE
    # v2.1.28 反证：三张卡的「资源名称」列不得再出现标签渲染（否则又重复）
    for anchor, name in (('scOverview.views?.history || []', "推送历史"),
                         ('<el-table :data="scQueueRows"', "推送队列"),
                         ('scOverview.views?.failed || []', "推送失败")):
        seg = APP_VUE[APP_VUE.index(anchor):]
        seg = seg[:seg.index("</el-table>")]
        assert "sc-rec-tags" not in seg, (
            f"{name}的「资源名称」列又挂回了三类标签 —— 与「排期类型」列重复")


def test_frontend_push_progress_filter_exists():
    """用户口径：推送任务要能筛「已推送 / 未推送」。"""
    assert "scPushProgressFilter" in APP_VUE
    assert "SC_PUSH_PROGRESS_ITEMS" in APP_VUE
    assert "SC_PUSHED_STATUS" in APP_VUE
    assert "SC_UNPUSHED_STATUS" in APP_VUE
    # 二分档是独立控件，不塞进状态下拉（否则又变成"同一状态两个选项"）
    for label in ("未推送", "已推送"):
        assert label in APP_VUE
    # 清除筛选要一并复位
    assert "scPushProgressFilter.value = ''" in APP_VUE


def test_frontend_preview_uses_series_sample():
    """预览要有示例数据，且按参考图那部剧集来（否则季集/标签行看不出来）。"""
    assert "previewDemo" in APP_VUE
    seg = APP_VUE[APP_VUE.index("const previewDemo"):APP_VUE.index("const previewText")]
    assert "episode_status" in seg and "S01E04 · 完结" in seg
    assert "tags" in seg
    # 预览必须照真机语义把空字段的整行拿掉，不能留空壳行
    assert "previewJumpLine" in APP_VUE
    assert "⚡ " in APP_VUE


def test_frontend_forecast_no_longer_pages():
    """v2.1.24（用户口径 1a）：分批加载整套删除，改固定高度滚动区。

    这条是**反证**：v2.1.23 的分段取数痕迹必须一处不剩。后端已经全量返回，
    前端若还挂着「加载下一时段」按钮，用户会以为排期没排完（这就是缺陷）。
    """
    for gone in ("/sc/forecast?paged=1", "scForecastLoadMore", "scFcLoadMore",
                 "scFcLoadedCount", "scFcCount", "scFcMoreLabel", "scFcHasMore",
                 "loadScForecastFull", "scForecastFull"):
        assert gone not in APP_VUE, f"分批加载残留：{gone}"
    # 正向：一次性请求 + 与「推送队列」同一套 el-table + 自动滚到当前时段
    assert "api('/sc/forecast')" in APP_VUE
    assert 'class="sc-fc-table"' in APP_VUE
    assert "ref=\"scFcBodyRef\"" in APP_VUE
    assert "scFcScrollToNow" in APP_VUE
    # 「临近时段自动上移」靠每分钟 tick 复算当前小时（不是定时器另起一套）
    assert "if (authed.value) scFcScrollToNow()" in APP_VUE

