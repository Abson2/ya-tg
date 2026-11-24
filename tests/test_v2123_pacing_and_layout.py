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
  ② 时段匀速：间隔 = 四舍五入(时段分钟 ÷ 部数)，站台锚定时段起点；
  ③ `_period_pace` / `_period_released` 与预测同一口径（预测不许比实跑偏快）；
  ④ 终态记录不显示「存量排期」；
  ⑤ forecast 支持按段取数（from_hour），且**只影响取数范围、不影响推送**；
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
    """returning（连载中）→ 一直是「新上映」，与上映多久无关。"""
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
# ⑤ forecast 分页取数：只影响「看」，不影响「推」
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
    return wk


def test_forecast_from_hour_narrows_window_only():
    """`from_hour` 只缩小「模拟范围」，不改写入——推送本身不分页。

    v2.1.23 用户口径 5 的第一句是「排期完成后再加载下一页」，但同一轮对话里
    用户明确纠正过「推送不是分页推送」。所以这里锁死：分页只影响**取数**，
    `plan_engine` 照旧一次把当日任务建满。
    """
    with SessionLocal() as db:
        ver = _plan(db)
        db.add(SCCategory(category_key="genre:剧情", name="剧情", enabled=True, priority=0))
        _channel(db)
        for i in range(3):
            _work(db, title=f"剧{i}")
        db.commit()

        full = sc2.forecast_schedule(db)
        assert full["page"]["from_hour"] == full["now_hour"]
        assert full["page"]["to_hour"] == 24

        later = min(23, full["now_hour"] + 3)
        part = sc2.forecast_schedule(db, from_hour=later)
        assert part["page"]["from_hour"] == later
        assert all(h["hour"] >= later for h in part["hours"])
        assert len(part["hours"]) <= len(full["hours"])

        # 只读：两次调用前后库内任务数不变（分页不是"分批建任务"）
        assert db.query(SCPublishTask).count() == 0


def test_forecast_from_hour_never_rewinds_before_now():
    """传过去的小时不能"倒回去算"——起点永远不早于当前小时。"""
    with SessionLocal() as db:
        _plan(db)
        db.add(SCCategory(category_key="genre:剧情", name="剧情", enabled=True, priority=0))
        _channel(db)
        _work(db, title="剧0")
        db.commit()
        out = sc2.forecast_schedule(db, from_hour=0)
        assert out["page"]["from_hour"] == out["now_hour"]


def test_forecast_paged_returns_one_segment():
    """`paged=1` 一次只排**一个时段**（用户口径 6：排完 14-16 再加载 16-18）。"""
    seg = int(sc2.HOUR_SEGMENT_HOURS)
    with SessionLocal() as db:
        _plan(db)
        db.add(SCCategory(category_key="genre:剧情", name="剧情", enabled=True, priority=0))
        _channel(db)
        for i in range(3):
            _work(db, title=f"剧{i}")
        db.commit()

        out = sc2.forecast_schedule(db, paged=True)
        page = out["page"]
        assert page["to_hour"] % seg == 0, "时段末尾必须落在整段边界上"
        assert 0 < page["to_hour"] - page["from_hour"] <= seg
        assert page["from_hour"] == out["now_hour"], "不从已经过去的整点开始预测"
        assert len(out["hours"]) <= seg
        assert all(page["from_hour"] <= h["hour"] < page["to_hour"]
                   for h in out["hours"])

        # 全天口径（不传 paged）保持原样，不能被分页改坏
        full = sc2.forecast_schedule(db)
        assert full["page"]["to_hour"] == 24
        assert full["page"]["partial"] is False
        assert full["page"]["has_more"] is False
        assert full["page"]["next_hour"] is None


def test_forecast_paged_next_hour_advances_by_segment():
    """`next_hour` 必须是下一段的起点，前端拿它去取「加载下一时段」。"""
    seg = int(sc2.HOUR_SEGMENT_HOURS)
    with SessionLocal() as db:
        _plan(db)
        _channel(db)
        db.commit()
        first = sc2.forecast_schedule(db, paged=True)
        nxt = first["page"]["next_hour"]
        if first["page"]["has_more"]:
            assert nxt == first["page"]["to_hour"]
            assert nxt % seg == 0
            second = sc2.forecast_schedule(db, paged=True, from_hour=nxt)
            assert second["page"]["from_hour"] == nxt
            assert second["page"]["to_hour"] == min(24, nxt + seg)
            # 两段不重叠、不跳空
            assert second["page"]["from_hour"] == first["page"]["to_hour"]


def test_forecast_paged_last_segment_closes_the_day():
    """最后一段（22:00-24:00）要收尾：`has_more=False` 且不再给 `next_hour`。"""
    seg = int(sc2.HOUR_SEGMENT_HOURS)
    last = 24 - seg
    with SessionLocal() as db:
        _plan(db)
        _channel(db)
        db.commit()
        out = sc2.forecast_schedule(db, paged=True, from_hour=last)
        assert out["page"]["from_hour"] == last
        assert out["page"]["to_hour"] == 24
        assert out["page"]["has_more"] is False
        assert out["page"]["next_hour"] is None
        assert out["page"]["partial"] is False
        assert isinstance(out["overflow"], int)
        assert isinstance(out["backlog"], int)


def test_forecast_paged_does_not_report_partial_overflow():
    """还没排到的时段不能拿半个模拟结果冒充「今天排不进 N 条」。"""
    with SessionLocal() as db:
        _plan(db)
        db.add(SCCategory(category_key="genre:剧情", name="剧情", enabled=True, priority=0))
        _channel(db)
        for i in range(5):
            _work(db, title=f"剧{i}")
        db.commit()
        out = sc2.forecast_schedule(db, paged=True)
        if out["page"]["partial"]:
            assert out["overflow"] is None, "分页时不得报一个只算了一段的 overflow"
            assert out["backlog"] is None
            assert any("分批加载" in n for n in out["notes"])
        # 只读：分页不是「分批建任务」
        assert db.query(SCPublishTask).count() == 0


# ---------------------------------------------------------------------------
# ⑥ 前端契约
# ---------------------------------------------------------------------------
def test_frontend_tags_render_left_of_title():
    """用户口径：三类标签放在推送记录的开头、剧名左侧（不是剧名下面一行）。"""
    assert ".sc-title-row" in APP_VUE
    assert ".sc-title-text" in APP_VUE
    # 四处（历史 / 队列 / 失败 / 任务）都用同一个行结构
    assert APP_VUE.count('class="sc-title-row"') >= 4, "标签未统一到剧名左侧"
    # 不允许再出现「剧名一行、标签另起一行」的老结构
    assert '<div>{{ s.row.title }}</div>' not in APP_VUE


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


def test_frontend_forecast_loads_segment_by_segment():
    """排期按「时段」分批加载（用户口径 6），不是一次性重排全天。"""
    # 请求必须显式带 paged，后端才会只排一段
    assert "/sc/forecast?paged=1" in APP_VUE
    # 追加逻辑：按小时去重合并 + 累加已加载段数/条数
    assert "scForecastLoadMore" in APP_VUE
    assert "scFcLoadedCount" in APP_VUE
    assert "scFcCount" in APP_VUE
    # 按钮文案要给出下一段的钟点，用户知道点一下会多出哪两小时
    assert "加载下一时段" in APP_VUE
    assert "scFcMoreLabel" in APP_VUE
    # 按钮与「全部排完」不能同时出现
    assert "scFcHasMore" in APP_VUE
    assert "今天所有时段已排完" in APP_VUE
    # 排期卡片分段，但「推送队列」要全天（上一版刚把队列改成「在途 + 今日待排期」，
    # 只给一段会让队列莫名变短）→ 首屏之后后台静默补拉一次全天，只喂队列。
    assert "loadScForecastFull" in APP_VUE
    assert "scForecastFull" in APP_VUE
    q = APP_VUE[APP_VUE.index("const scQueueRows"):]
    q = q[:q.index("// ----------")]
    assert "scForecastFull" in q, "队列没用全天预测"
    assert "planHours" in q, "队列缺少「全天优先、已加载段兜底」的取数"

