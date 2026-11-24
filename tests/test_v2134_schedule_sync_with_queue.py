"""v2.1.34：今日推送排期与推送队列同源（真机反馈）。

用户原话（2026-11-24，附截图）：
  「今日推送排期与推送队列影视不一致，内容需要实时同步推送队列的数据
    （或取同口径数据源）」

截图里对不上的那两批：
  排期表 02:00 → 蜘蛛侠：崭新之日 / 2001 太空漫游 / 阿凡达火与烬
  队列   00:09 → 百变星君 / 00:18 → 望夫成龙

根因不是数据坏了，是**两张表回答不同的问题**：
  · `forecast_schedule` 只做预测 —— 从分类游标往后模拟「还能排什么」；
    当天这批任务一旦建成、游标前移，它们就再也不出现在预测里；
  · `_aggregate_views` 的 queue 给的是**真实在途任务**。
于是同一屏里一边是「未来可能推的」、一边是「现在排着的」，用户只能读成「排期丢了」。

修法：`_queue_works_by_hour` 把当天在途任务按小时反灌进排期表，时间口径复用
`_mark_expect_release`（与队列时间列**同一个函数**），两边从此不可能对不上。

本文件锁死四件事：
  ① 只取在途状态，已发 / 失败不重复进排期表（它们各有自己的表）；
  ② 同一部作品推群 + 频道合并成**一部**（排期表按部说话），条数另算；
  ③ 已建任务不再参与预测（否则同一部剧占两行、顶部数字双计）；
  ④ 已建任务的时间是既成事实，绝不被 `_respread_forecast_times` 均分重写。
"""
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import delete

from app import runtime_config, share_center_v2 as sc2
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

# 固定「上海 2026-11-24 12:00」= UTC 04:00 —— 绝不依赖真实运行时刻。
# 深夜跑全量时用真实 now 会让「过去小时 / 跨天」这类断言随钟点变红（v2.1.32、
# v2.1.33 各踩过一次），这里一律冻结。
FIXED_UTC = datetime(2026, 11, 24, 4, 0, tzinfo=timezone.utc)
DAY_SH = "2026-11-24"


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    migrate()
    for _ in range(2):
        with SessionLocal() as db:
            for model in WIPED:
                db.execute(delete(model))
            db.commit()
    monkeypatch.setattr(sc2, "utcnow", lambda: FIXED_UTC)
    yield


def _sh(hour, minute=5):
    """上海某时某分 → 存库用的 UTC datetime。"""
    return datetime(2026, 11, 24, hour, minute, tzinfo=sc2.shanghai_tz()
                    ).astimezone(timezone.utc)


def _seed_env(db):
    """最小可跑链路：计划版本 / 全时段窗口 / 两个渠道 / 一个启用分类。"""
    plan = SCSharePlan(name="p", plan_type="daily", enabled=True)
    db.add(plan)
    db.flush()
    ver = SCSharePlanVersion(plan_id=plan.id, version_no=1, status="active",
                             config_json=json.dumps({"category_limit": 20,
                                                     "work_limit": 50}))
    db.add(ver)
    db.flush()
    ensure_full_day_hour_window(db, ver.id)
    cat = SCCategory(category_key="genre:剧情", name="剧情", enabled=True)
    db.add(cat)
    db.flush()
    lst = SCPushList(list_key="k", name="清单", status="active")
    db.add(lst)
    db.flush()
    bot = Bot(name="b", token="x", enabled=True)
    db.add(bot)
    db.flush()
    channels = []
    for chat in ("@chan", "@group"):
        ch = Channel(bot_id=bot.id, name=chat, chat_id=chat, kind="channel", enabled=True)
        db.add(ch)
        channels.append(ch)
    db.flush()
    return ver, lst, channels, cat


def _task(db, *, work_key, scheduled_at, status="planned", published_at=None,
          list_item_id=0, kind="first"):
    t = SCPublishTask(work_key=work_key, kind=kind, status=status,
                      scheduled_at=scheduled_at, published_at=published_at,
                      list_item_id=list_item_id,
                      share_url="https://pan.quark.cn/s/keep", link_state="active")
    db.add(t)
    db.flush()
    return t


def _library_work(db, cat, *, work_key, order):
    """让这部作品进得了预测候选（资源 + 快照 + 分类映射）。"""
    res = Resource(file_id=f"f-{work_key}-{order}", name=f"{work_key}.mkv",
                   ext=".mkv", size=100, utime=1, identify_status="identified",
                   media_type="movie", tmdb_id=2000 + order, media_title=work_key,
                   share_status="active", share_url="https://pan.quark.cn/s/keep")
    db.add(res)
    db.flush()
    lv = SCLibraryVersion(work_key=work_key, version_no=1, fingerprint=f"fp-{work_key}",
                          media_type="movie",
                          resource_ids_json=json.dumps([res.id]),
                          metadata_json=json.dumps({"title": work_key}),
                          change_type="created", created_at=FIXED_UTC - timedelta(days=3))
    db.add(lv)
    db.flush()
    db.add(SCWorkCategory(work_key=work_key, category_key=cat.category_key))
    db.flush()
    return lv


# ---------------------------------------------------------------------------
# ① 只取在途状态；已发 / 失败不进排期表
# ---------------------------------------------------------------------------
def test_built_only_takes_active_statuses():
    now_sh = FIXED_UTC.astimezone(sc2.shanghai_tz())
    with SessionLocal() as db:
        _task(db, work_key="work:LIVE", scheduled_at=_sh(12, 5), status="planned")
        _task(db, work_key="work:DONE", scheduled_at=_sh(12, 6), status="published",
              published_at=_sh(12, 6))
        _task(db, work_key="work:FAIL", scheduled_at=_sh(12, 7), status="failed")
        db.commit()
        hour_map, keys = sc2._queue_works_by_hour(db, now_sh)
    assert keys == {"work:LIVE"}, f"只应取在途任务，实际 {keys}"
    assert "work:DONE" not in keys and "work:FAIL" not in keys


def test_built_skips_other_days():
    """队列只给当天，排期表也必须只给当天 —— 否则两张表的日期范围不一致。"""
    now_sh = FIXED_UTC.astimezone(sc2.shanghai_tz())
    with SessionLocal() as db:
        _task(db, work_key="work:OLD",
              scheduled_at=_sh(12, 5) - timedelta(days=1), status="planned")
        db.commit()
        hour_map, keys = sc2._queue_works_by_hour(db, now_sh)
    assert keys == set(), f"昨天的在途任务不该进今天的排期表，实际 {keys}"
    assert hour_map == {}


# ---------------------------------------------------------------------------
# ② 同一部作品多目标合并成「一部」，条数另算
# ---------------------------------------------------------------------------
def test_built_merges_two_channels_into_one_work():
    """群 + 频道是两条任务，但排期表按**部**说话：一部作品只出现一次。"""
    now_sh = FIXED_UTC.astimezone(sc2.shanghai_tz())
    with SessionLocal() as db:
        _task(db, work_key="work:W1", scheduled_at=_sh(12, 5), status="planned")
        _task(db, work_key="work:W1", scheduled_at=_sh(12, 6), status="planned")
        db.commit()
        hour_map, keys = sc2._queue_works_by_hour(db, now_sh)
    assert keys == {"work:W1"}
    slot = hour_map[12]
    assert len(slot["works"]) == 1, "同一部剧两条渠道必须合并成一行"
    assert slot["works"][0]["tasks"] == 2, "条数要如实给 2"
    assert slot["count"] == 2, "该小时的条数应含两个渠道"


def test_built_uses_published_at_when_present():
    """行上若有实际发送时间，就按它归小时，不按建任务时刻。

    注：`published` 不在在途集合里（它归「推送历史」），所以这里用 `submitted`
    这种「已提交、可能已落实际发送时间」的中间态来覆盖这条分支。
    """
    now_sh = FIXED_UTC.astimezone(sc2.shanghai_tz())
    with SessionLocal() as db:
        # 任务建在 09 点，但 11:40 已记下实际发送 → 应归 11 点
        _task(db, work_key="work:W1", scheduled_at=_sh(9, 5), status="submitted",
              published_at=_sh(11, 40))
        db.commit()
        hour_map, _keys = sc2._queue_works_by_hour(db, now_sh)
    assert 11 in hour_map, f"应按实际发送时刻归到 11 点，实际 {sorted(hour_map)}"
    assert hour_map[11]["works"][0]["time"] == "11:40"


def test_built_time_matches_queue_clock_exactly():
    """**同源契约**：排期表的时刻必须与队列时间列是**同一个函数**算出来的。

    队列在 `_aggregate_views.rows_for` 里调 `_mark_expect_release` 拿
    `expect_release_at`，前端 `scQueueClock` 按「实际 > 预计 > 原口径」取值。
    这里对同一行单独跑一遍那个函数，两边给出的时刻必须一模一样 ——
    只要有一处改了口径，这条断言立刻红。
    """
    now_sh = FIXED_UTC.astimezone(sc2.shanghai_tz())
    with SessionLocal() as db:
        _task(db, work_key="work:W1", scheduled_at=_sh(12, 9), status="planned")
        db.commit()
        hour_map, _keys = sc2._queue_works_by_hour(db, now_sh)

        # 队列侧：同一行、同一个函数
        probe = [{"work_key": "work:W1",
                  "scheduled_at": _sh(12, 9).astimezone(sc2.shanghai_tz()
                                                        ).strftime("%Y-%m-%d %H:%M:%S")}]
        sc2._mark_expect_release(db, probe)
        want = str(probe[0].get("expect_release_at") or probe[0]["scheduled_at"])[11:16]

    works = [w for slot in hour_map.values() for w in slot["works"]]
    assert len(works) == 1
    assert re.fullmatch(r"\d{2}:\d{2}", works[0]["time"]), works[0]["time"]
    assert works[0]["time"] == want, (
        f"排期表 {works[0]['time']} 与队列 {want} 不是同口径")


# ---------------------------------------------------------------------------
# ③ 排期表带上已建任务，且与预测不重复
# ---------------------------------------------------------------------------
def test_forecast_includes_built_tasks_in_its_hour():
    """**核心回归**：排期表那一小时不再是空的，里面就是队列里那几条。"""
    with SessionLocal() as db:
        ver, lst, channels, cat = _seed_env(db)
        lv = _library_work(db, cat, work_key="work:W1", order=1)
        item = SCPushListItem(list_id=lst.id, work_key="work:W1",
                              library_version_id=lv.id, chat_id=channels[0].chat_id,
                              category_key=cat.category_key,
                              dedup_key="W1-@chan", status="planned")
        db.add(item)
        db.flush()
        _task(db, work_key="work:W1", scheduled_at=_sh(12, 5),
              list_item_id=item.id)
        db.commit()

        out = sc2.forecast_schedule(db)

    hours = {int(h["hour"]): h for h in out["hours"]}
    assert 12 in hours, "当前小时必须在表里"
    kinds = [it.get("kind") for it in hours[12]["items"]]
    assert "scheduled" in kinds, f"已建任务没进排期表，实际 items={kinds}"
    built = next(it for it in hours[12]["items"] if it.get("kind") == "scheduled")
    assert [w["work_key"] for w in built["works"]] == ["work:W1"]
    assert built["category"] == "剧情", "分类必须取作品真实 TMDB 分类"
    assert built["count"] == 1 and built["works_total"] == 1


def test_built_work_is_not_predicted_again():
    """反证：已建任务的作品**不能**再被预测排一遍（否则一部剧占两行、数字双计）。"""
    with SessionLocal() as db:
        ver, lst, channels, cat = _seed_env(db)
        lv = _library_work(db, cat, work_key="work:W1", order=1)
        item = SCPushListItem(list_id=lst.id, work_key="work:W1",
                              library_version_id=lv.id, chat_id=channels[0].chat_id,
                              category_key=cat.category_key,
                              dedup_key="W1-@chan", status="planned")
        db.add(item)
        db.flush()
        _task(db, work_key="work:W1", scheduled_at=_sh(12, 5), list_item_id=item.id)
        db.commit()

        out = sc2.forecast_schedule(db)

    predicted = []
    for h in out["hours"]:
        for it in h["items"]:
            if it.get("kind") == "scheduled":
                continue
            predicted.extend(w.get("work_key") for w in (it.get("works") or []))
    assert "work:W1" not in predicted, "已建任务的作品又出现在预测里了"


def test_built_category_is_never_a_fake_bucket():
    """分类列只放 TMDB 题材：绝不许造「已排期任务」这种状态词当分类（v2.1.25 口径）。"""
    src = (ROOT / "backend/app/share_center_v2.py").read_text(encoding="utf-8")
    assert '"category": "已排期任务"' not in src, "又把状态词塞进分类格了"
    assert '"category": cat_name or "未分类"' in src, "已建批次没有按作品真实分类拆项"


def test_forecast_works_total_counts_built():
    """顶部「N 部作品」要把已建任务算进去，否则比表里少。"""
    with SessionLocal() as db:
        ver, lst, channels, cat = _seed_env(db)
        lv = _library_work(db, cat, work_key="work:W1", order=1)
        item = SCPushListItem(list_id=lst.id, work_key="work:W1",
                              library_version_id=lv.id, chat_id=channels[0].chat_id,
                              category_key=cat.category_key,
                              dedup_key="W1-@chan", status="planned")
        db.add(item)
        db.flush()
        _task(db, work_key="work:W1", scheduled_at=_sh(12, 5), list_item_id=item.id)
        db.commit()

        out = sc2.forecast_schedule(db)
    assert out["works_total"] >= 1
    assert out["today_total"] >= 1


# ---------------------------------------------------------------------------
# ④ 过去的小时：有任务才占一行
# ---------------------------------------------------------------------------
def test_past_hour_shows_built_and_vanishes_when_empty():
    """10 点（已过去）挂着一条在途任务 → 出行；11 点什么都没有 → 不占行。"""
    with SessionLocal() as db:
        ver, lst, channels, cat = _seed_env(db)
        _library_work(db, cat, work_key="work:W1", order=1)
        _task(db, work_key="work:LATE", scheduled_at=_sh(10, 5), status="planned")
        db.commit()

        out = sc2.forecast_schedule(db)

    hours = {int(h["hour"]): h for h in out["hours"]}
    assert 10 in hours, "过去的小时若有在途任务，必须出行交代"
    assert [it.get("kind") for it in hours[10]["items"]] == ["scheduled"]
    assert 11 not in hours, "既过去又没任务的小时不该占一行（否则一屏空行）"


def test_respread_never_rewrites_built_times():
    """已建任务的时间是既成事实：均分回填必须跳过，否则刚对齐的又错开。"""
    hours = [{"hour": 12, "items": [
        {"kind": "scheduled", "count": 1, "works_total": 1,
         "works": [{"work_key": "work:BUILT", "time": "12:09"}]},
        {"kind": "category", "category": "剧情", "count": 2, "works_total": 2,
         "works": [{"work_key": "work:P1", "time": ""},
                   {"work_key": "work:P2", "time": ""}]},
    ]}]
    now_sh = FIXED_UTC.astimezone(sc2.shanghai_tz())
    sc2._respread_forecast_times(hours, now_sh)
    built = hours[0]["items"][0]["works"][0]
    assert built["time"] == "12:09", f"已建任务的时间被均分覆盖了：{built['time']}"
    predicted = hours[0]["items"][1]["works"]
    assert all(w["time"] for w in predicted), "预测行的时间应当被回填"
    # 已建任务不计入时段部数：本时段只有 2 部预测
    pace = sc2._respread_forecast_times(hours, now_sh)
    assert pace["works"] == 2, f"部数把已建任务也算进去了：{pace}"


# ---------------------------------------------------------------------------
# ⑤ 前端：已建批次要看得出是「已建」，且不新造分类词
# ---------------------------------------------------------------------------
def _strip_vue_comments(text):
    """剥掉注释再断言，避免「解释旧写法为什么错」的注释被当成残留代码。

    ⚠️ 不能用 `re.sub(r"/\\*.*?\\*/", ...)` 剥块注释：App.vue 第 48 行附近有一句
    `// 所有状态来自后端 /api/*，前端不承载计划…`，里面的 `/api/*` 含 `/*`，
    正则会把此后 20 万字符一路吞到 CSS 段的 `*/` —— 断言随之假红（已踩）。
    这里只剥 HTML 注释 + 整行注释，不做跨行的块注释匹配。
    """
    text = re.sub(r"<!--.*?-->", "", text, flags=re.S)
    out = []
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("//") or s.startswith("/*") or s.startswith("*"):
            continue
        out.append(line)
    return "\n".join(out)


def test_frontend_marks_built_items():
    src = _strip_vue_comments(APP_VUE)
    assert "it.kind === 'scheduled'" in src, "模板没识别已建批次"
    assert ">已建<" in src, "缺少「已建」前缀"
    assert ".sc-fc-cat.is-built" in src, "缺少已建批次配色"


def test_frontend_built_prefix_is_not_a_category():
    """「已建」只能是前缀 `<i>`，不能写进分类名里 —— 那是两回事。"""
    src = _strip_vue_comments(APP_VUE)
    assert "已排期任务" not in src, "分类列里出现了状态词"


def test_frontend_keeps_empty_text_branch():
    """空态提示保留（v2.1.33 加的），别在改同源时把它删了。"""
    src = _strip_vue_comments(APP_VUE)
    assert "scForecastEmptyText" in src
    assert "见下方「推送队列」" in src
