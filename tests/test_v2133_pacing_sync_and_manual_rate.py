"""v2.1.33：同作品多目标同轮放行 + 队列预计放行时刻 + 手动搜索独立限流档。

真机反馈（用户原话，2026-11-24）：
  1、「22 点时段有排期队列，为什么在今日推送排期中任务为空」
  2、「排查 TMDB 限流的原因」（后台手动搜索被批量识别挤掉）
  3、「22 点推送的八仙，超时未推送（推送队列显示是 22 点推送，实际上排期时间是
      22 点 30，推送队列的时间不精准）」
  4、「同一部影视推送群和频道，时间差较大，没有同步」

本文件锁死（按用户拍板的「节奏 + 时间口径 + 手动不限流」三项）：
  ① 一部作品的多个目标（频道 / 群）算**一个**份额，同轮一起出去；
  ② 队列时间列对未发的行给「预计放行时刻」，不再拿建任务时刻冒充推送时间；
  ③ 手动搜索 / 重新匹配走独立限流档，不与批量识别共桶；
  ④ 限流文案不把内部场景键（scan / bot / manual）露给用户；
  ⑤ 排期表为空时要解释「任务已建成，见推送队列」。

用户已明确 **不碰 identify 的类型锁定**（红线 #247），故本文件不涉及换类型逻辑。
"""
import asyncio
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import delete

from app import runtime_config, share_center_v2 as sc2, tmdb
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
MAIN_PY = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
TMDB_PY = (ROOT / "backend/app/tmdb.py").read_text(encoding="utf-8")

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
    tmdb.reset_rate_limiters()
    yield
    tmdb.reset_rate_limiters()


# ---------------------------------------------------------------------------
# ① TMDB 手动搜索独立限流档
# ---------------------------------------------------------------------------
def test_manual_scenario_exists_and_is_distinct():
    """手动档必须是一个**独立键**，不能借道 SCAN 或 BOT。"""
    assert tmdb.MANUAL == "manual"
    assert len({tmdb.SCAN, tmdb.BOT, tmdb.MANUAL}) == 3


def test_manual_defaults_registered():
    """两个新键必须进 DEFAULTS —— 否则 `runtime_config.get` 拿不到兜底值。"""
    d = runtime_config.DEFAULTS
    assert "tmdb_manual_min_interval" in d
    assert "tmdb_manual_max_calls" in d
    assert float(d["tmdb_manual_min_interval"][1]) > 0
    assert int(d["tmdb_manual_max_calls"][1]) >= 1


def test_rate_cfg_reads_three_independent_slots():
    """三档各读各的键，互不串值。"""
    with SessionLocal() as db:
        runtime_config.set_many(db, {
            "tmdb_scan_min_interval": "0.5", "tmdb_scan_max_calls": "11",
            "tmdb_bot_min_interval": "7", "tmdb_bot_max_calls": "22",
            "tmdb_manual_min_interval": "2", "tmdb_manual_max_calls": "33",
            "tmdb_rate_window_seconds": "60"})
        db.commit()
        assert tmdb._rate_cfg(db, tmdb.SCAN) == (0.5, 11, 60)
        assert tmdb._rate_cfg(db, tmdb.BOT) == (7.0, 22, 60)
        assert tmdb._rate_cfg(db, tmdb.MANUAL) == (2.0, 33, 60)


def test_manual_bucket_is_not_drained_by_scan_bucket():
    """**反证**：把批量识别那桶打满，手动搜索仍必须放行。

    这正是真机现象——批量识别在跑，管理员在后台点一次搜索就报「调用过于频繁」。
    桶共用的实现里，下面第二个 assert 会是 False。
    """
    with SessionLocal() as db:
        runtime_config.set_many(db, {
            "tmdb_scan_min_interval": "0", "tmdb_scan_max_calls": "1",
            "tmdb_manual_min_interval": "0", "tmdb_manual_max_calls": "1",
            "tmdb_rate_window_seconds": "60"})
        db.commit()
        assert tmdb.check_rate_limit(db, tmdb.SCAN) is True
        assert tmdb.check_rate_limit(db, tmdb.SCAN) is False, "scan 桶应已满"
        assert tmdb.check_rate_limit(db, tmdb.MANUAL) is True, (
            "手动档被打量识别拖累 → 共桶了，改动没生效")
        assert tmdb.check_rate_limit(db, tmdb.MANUAL) is False, "手动档自己也要能挡住"


def test_manual_bucket_does_not_drain_scan_bucket():
    """反向也要成立：手动搜索刷满，不影响批量识别。"""
    with SessionLocal() as db:
        runtime_config.set_many(db, {
            "tmdb_scan_min_interval": "0", "tmdb_scan_max_calls": "2",
            "tmdb_manual_min_interval": "0", "tmdb_manual_max_calls": "1",
            "tmdb_rate_window_seconds": "60"})
        db.commit()
        assert tmdb.check_rate_limit(db, tmdb.MANUAL) is True
        assert tmdb.check_rate_limit(db, tmdb.MANUAL) is False
        assert tmdb.check_rate_limit(db, tmdb.SCAN) is True


def _strip_comments(text: str, marker: str = "#") -> str:
    """去掉注释行/行尾注释后再做「有没有这段代码」的断言。

    为什么必须先剥：解释「旧写法为什么错」的注释里，必然原样引用旧代码，
    直接对全文做 `in` 断言会把自己的注释当成残留代码（本轮就踩了）。
    """
    out = []
    for line in text.splitlines():
        if marker in line:
            line = line.split(marker, 1)[0]
        out.append(line)
    return "\n".join(out)


def test_scenario_labels_are_chinese():
    """限流文案要给人看，内部场景键绝不能原样出现在页面上。

    注意口径：禁的是**内部键原文**（`scan` / `manual` 这种），不是英文字母。
    `BOT` 单独豁免——「Bot」是用户自己的常用词，管理页本来就有「Bot 场景」，
    但标签不能只是一个裸的 `bot`。
    """
    for sc in (tmdb.SCAN, tmdb.MANUAL):
        label = tmdb.scenario_label(sc)
        assert label and label != sc, f"{sc} 没有独立的中文标签"
        assert sc not in label.lower(), f"{sc} 的标签里还嵌着场景键原文：{label}"
    bot_label = tmdb.scenario_label(tmdb.BOT)
    assert bot_label and bot_label.lower().strip() != "bot", "Bot 档标签不能是裸键名"
    assert "交互" in bot_label, f"Bot 档标签要看得出是人跟 Bot 说话：{bot_label}"
    # 取不到的键要给中性说法，不能原样吐出去
    fallback = tmdb.scenario_label("weird-key")
    assert fallback and fallback != "weird-key" and "weird" not in fallback.lower()


def test_rate_limit_error_text_uses_label_not_raw_key():
    """`_get` 的限流异常文案必须是「TMDB 手动搜索调用过于频繁…」，不是 `manual`。"""
    src = TMDB_PY
    assert "scenario_label(scenario)" in src, "报错文案没走中文标签"
    assert 'f"TMDB {scenario} 场景调用过于频繁' not in src, "旧文案（直接拼场景键）还在"


def test_manual_endpoints_pass_manual_scenario():
    """后台手动搜索 / 重新识别 / 应用匹配 / 多语搜索都必须显式传 MANUAL。"""
    assert MAIN_PY.count("scenario=tmdb.MANUAL") >= 5, (
        "手动路径没全部切到独立档，仍有端点落回批量档")
    # 反证：不能再出现「不传 scenario」的手动调用
    assert "await tmdb.search(raw, mt, db)" not in MAIN_PY
    assert "await tmdb.search(q, mt, db)" not in MAIN_PY


def test_identify_accepts_scenario_kwarg():
    """`identify` / `detail` / `detail_by_id_any` / `search_multilingual` 都要能收 scenario。"""
    import inspect
    for fn in (tmdb.identify, tmdb.detail, tmdb.detail_by_id_any,
               tmdb.search_multilingual, tmdb._search_with_retry):
        assert "scenario" in inspect.signature(fn).parameters, f"{fn.__name__} 缺 scenario"


# ---------------------------------------------------------------------------
# ② 同作品多目标同轮放行
# ---------------------------------------------------------------------------
def _period():
    now = sc2.utcnow()
    now_sh = now.astimezone(sc2.shanghai_tz())
    start_sh, end_sh = sc2._period_bounds(now_sh)
    return now, start_sh, end_sh


def _task(db, *, work_key, scheduled_at, status="planned", link_state="active",
          share_url="https://pan.quark.cn/s/keep", list_item_id=0):
    t = SCPublishTask(work_key=work_key, kind="first", status=status,
                      scheduled_at=scheduled_at, list_item_id=list_item_id,
                      share_url=share_url, link_state=link_state)
    db.add(t)
    db.flush()
    return t


def test_period_released_works_returns_a_set_of_works():
    """按**作品**去重：同一部剧两条渠道，只算一个已放行作品。"""
    now, start_sh, _end_sh = _period()
    at = now - timedelta(seconds=30)
    with SessionLocal() as db:
        _task(db, work_key="work:W1", scheduled_at=at, status="submitted")
        _task(db, work_key="work:W1", scheduled_at=at, status="published")
        _task(db, work_key="work:W2", scheduled_at=at, status="planned")
        db.commit()
        pace = {"period_start": start_sh, "period_end": sc2._period_bounds(
            start_sh)[1], "n_works": 2, "interval": 30, "due": 1}
        got = sc2._period_released_works(db, pace)
        assert got == {"work:W1"}, f"应只统计已发出的作品，实际 {got}"
        assert sc2._period_released(db, pace) == 1


def test_old_pace_gate_shape_is_gone():
    """旧的「部数满就 break 整轮」写法必须消失——它是 #3/#4 的直接成因。"""
    src = _strip_comments(
        (ROOT / "backend/app/share_center_v2.py").read_text(encoding="utf-8"))
    assert '_period_released(db, pace) >= pace["due"]' not in src, (
        "旧闸门还在：同一部剧的第二条渠道会被判超额并 break 整轮")
    assert "blocked_works" in src and "released_works" in src, "同作品豁免逻辑没落地"


# --- 端到端：真的跑一轮 dispatch -------------------------------------------
def _seed_publish_env(db):
    """最小可跑链路：清单 / 快照 / 资源 / 两个渠道 / 全时段窗口。"""
    plan = SCSharePlan(name="p", plan_type="daily", enabled=True)
    db.add(plan)
    db.flush()
    ver = SCSharePlanVersion(plan_id=plan.id, version_no=1, status="active",
                             config_json=json.dumps({"category_limit": 20,
                                                     "work_limit": 50}))
    db.add(ver)
    db.flush()
    ensure_full_day_hour_window(db, ver.id)
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
    return ver, lst, channels


def _work_row(db, ver, lst, *, work_key, channels, order):
    res = Resource(file_id=f"f-{work_key}-{order}", name=f"{work_key}-{order}.mkv",
                   ext=".mkv", size=100, utime=1, identify_status="identified",
                   media_type="movie", tmdb_id=1000 + order, media_title=work_key,
                   share_status="active", share_url="https://pan.quark.cn/s/keep")
    db.add(res)
    db.flush()
    lv = SCLibraryVersion(work_key=work_key, version_no=1, fingerprint=f"fp-{order}",
                          media_type="movie",
                          resource_ids_json=json.dumps([res.id]),
                          metadata_json=json.dumps({"title": work_key}),
                          change_type="created",
                          created_at=datetime.now(timezone.utc) - timedelta(days=3))
    db.add(lv)
    db.flush()
    tasks = []
    for i, ch in enumerate(channels):
        item = SCPushListItem(list_id=lst.id, work_key=work_key,
                              library_version_id=lv.id, chat_id=ch.chat_id,
                              category_key="genre:剧情",
                              dedup_key=f"{work_key}-{ch.chat_id}",
                              status="planned")
        db.add(item)
        db.flush()
        t = SCPublishTask(work_key=work_key, kind="first", status="planned",
                          list_item_id=item.id,
                          scheduled_at=datetime.now(timezone.utc) - timedelta(seconds=60),
                          share_url="https://pan.quark.cn/s/keep", link_state="active")
        db.add(t)
        tasks.append(t)
    db.flush()
    return tasks


def test_same_work_two_channels_go_out_in_the_same_round(monkeypatch):
    """**核心回归**：W1 推频道 + 群，只占一个作品名额，同一轮都出去。

    反证：把闸门改回「部数满就 break」，W1 的第二条会停在 planned 等下一个
    interval —— 真机表现就是「同一部剧群和频道差半小时」。
    """
    with SessionLocal() as db:
        ver, lst, channels = _seed_publish_env(db)
        tasks = {}
        tasks["W1"] = _work_row(db, ver, lst, work_key="work:W1",
                                channels=channels, order=1)
        for i, wk in enumerate(("work:W2", "work:W3", "work:W4"), start=2):
            tasks[wk] = _work_row(db, ver, lst, work_key=wk,
                                  channels=channels[:1], order=i)
        # 隔离：关掉小时上限，只留时段匀速；gate 与本用例无关，直接放行
        runtime_config.set_many(db, {"sc_quick_publish_hourly_limit": "0",
                                     "publish_paused": "false"})
        db.commit()
        monkeypatch.setattr(sc2, "evaluate_push_gate",
                            lambda *a, **k: {"passed": True, "blocked": [],
                                             "reason": "", "items": {}})
        pace = {"period_start": sc2._period_bounds(
                    sc2.utcnow().astimezone(sc2.shanghai_tz()))[0],
                "period_end": sc2._period_bounds(
                    sc2.utcnow().astimezone(sc2.shanghai_tz()))[1],
                "n_works": 4, "interval": 30, "due": 1}
        monkeypatch.setattr(sc2, "_period_pace", lambda db_, now_: dict(pace))
        stats = asyncio.run(sc2.dispatch_ready_tasks(db, limit=20))
        db.commit()
        got = {t.id: t.status for t in db.scalars(
            __import__("sqlalchemy").select(SCPublishTask)).all()}
    w1_status = [got[t.id] for t in tasks["W1"]]
    assert w1_status == ["submitted", "submitted"], (
        f"同一部作品的两条渠道没有同轮出去：{w1_status}")
    assert stats["submitted"] == 2, f"两部都该提交，实际 {stats}"
    # 其余作品只占名额 1 已用完 → 全部保持 planned（不再「break 掉但也不计数」）
    for wk in ("work:W2", "work:W3", "work:W4"):
        assert all(got[t.id] == "planned" for t in tasks[wk]), (
            f"{wk} 不该在 due=1 时出去")
    assert stats["paced"] >= 3


def test_hourly_cap_counts_works_not_rows():
    """整小时额度按「部」计：一部剧两条渠道不该算两份额度。"""
    src = (ROOT / "backend/app/share_center_v2.py").read_text(encoding="utf-8")
    assert "submitted_this_hour" not in src.replace(
        "`submitted_this_hour >= cap`", "").replace(
        "`submitted_this_hour >= hourly_cap`", ""), (
        "代码里还残留按「条」计数的变量")
    assert "submitted_keys_hour" in src


def test_hourly_gate_uses_continue_not_break():
    """被小时额度挡下时不能 `break` —— 否则同作品的第二条渠道被一起停掉。"""
    src = (ROOT / "backend/app/share_center_v2.py").read_text(encoding="utf-8")
    block = src.split("if hourly_cap and task.kind != FORCE_TASK_KIND:", 1)
    assert len(block) == 2, "小时额度的判断块没找到"
    head = block[1][:900]
    assert "continue" in head, "被挡后应 continue（放过同作品后续行）"
    assert "break" not in head.split("item = db.get")[0], "还在 break 整轮"


# ---------------------------------------------------------------------------
# ③ 队列显示「预计放行时刻」
# ---------------------------------------------------------------------------
def test_mark_expect_release_paces_pending_works():
    """未发的作品按 interval 递推：已放行 1 部、interval=30 →
    待发第 1 部落在时段起点 +30 分钟，第 2 部 +60 分钟。"""
    now, start_sh, end_sh = _period()
    at = now - timedelta(seconds=30)
    with SessionLocal() as db:
        _task(db, work_key="work:DONE", scheduled_at=at, status="submitted")
        _task(db, work_key="work:A", scheduled_at=at, status="planned")
        _task(db, work_key="work:B", scheduled_at=at, status="planned")
        db.commit()
        rows = [{"work_key": "work:A", "scheduled_at": str(at)},
                {"work_key": "work:B", "scheduled_at": str(at)}]
        pace = {"period_start": start_sh, "period_end": end_sh,
                "n_works": 3, "interval": 30, "due": 1}
        import unittest.mock as mock
        with mock.patch.object(sc2, "_period_pace", lambda d, n: dict(pace)):
            sc2._mark_expect_release(db, rows)
    a = datetime.strptime(rows[0]["expect_release_at"], "%Y-%m-%d %H:%M:%S")
    b = datetime.strptime(rows[1]["expect_release_at"], "%Y-%m-%d %H:%M:%S")
    start_naive = start_sh.replace(tzinfo=None)
    assert a - start_naive == timedelta(minutes=30), f"第 1 部应在 +30 分，实际 {a}"
    assert b - start_naive == timedelta(minutes=60), f"第 2 部应在 +60 分，实际 {b}"


def test_mark_expect_release_skips_already_released_work():
    """已放行的作品不设预计时刻（它的其余渠道随时跟着走，不该被排到后面）。"""
    now, start_sh, end_sh = _period()
    at = now - timedelta(seconds=30)
    with SessionLocal() as db:
        _task(db, work_key="work:DONE", scheduled_at=at, status="submitted")
        db.commit()
        rows = [{"work_key": "work:DONE", "scheduled_at": str(at)}]
        pace = {"period_start": start_sh, "period_end": end_sh,
                "n_works": 1, "interval": 30, "due": 1}
        import unittest.mock as mock
        with mock.patch.object(sc2, "_period_pace", lambda d, n: dict(pace)):
            sc2._mark_expect_release(db, rows)
    assert "expect_release_at" not in rows[0]


def test_mark_expect_release_noop_without_pace():
    """本时段还没排任务（due=None）时不许瞎编一个时刻。"""
    now, start_sh, end_sh = _period()
    with SessionLocal() as db:
        rows = [{"work_key": "work:A", "scheduled_at": str(now)}]
        pace = {"period_start": start_sh, "period_end": end_sh,
                "n_works": 0, "interval": 0, "due": None}
        import unittest.mock as mock
        with mock.patch.object(sc2, "_period_pace", lambda d, n: dict(pace)):
            sc2._mark_expect_release(db, rows)
    assert "expect_release_at" not in rows[0]


def test_frontend_queue_clock_prefers_real_and_expected_time():
    """队列时间列：已发 → 实际发送时间；未发 → 预计放行时刻；都没有才退回旧口径。"""
    assert "row.published_at" in APP_VUE
    assert "row.expect_release_at" in APP_VUE
    block = APP_VUE.split("function scQueueClock(row) {", 1)
    assert len(block) == 2
    body = _strip_comments(block[1].split("\n}", 1)[0], marker="//")
    assert body.index("published_at") < body.index("expect_release_at") < body.index("scheduled_at"), (
        f"取值优先级错了：实际时间 > 预计放行时刻 > 建任务时间\n{body}")


# ---------------------------------------------------------------------------
# ④ 排期表空态要解释清楚
# ---------------------------------------------------------------------------
def test_forecast_empty_text_points_to_queue():
    """排期为空但在途有任务时，文案必须指向「推送队列」并带上条数。"""
    assert "scForecastEmptyText" in APP_VUE
    block = APP_VUE.split("const scForecastEmptyText", 1)[1][:600]
    assert "views?.queue" in block, "没读在途任务数"
    assert "推送队列" in block, "空态文案没把用户导向正确的表"
    assert ':empty-text="scForecastEmptyText"' in APP_VUE, "表格没绑这个文案"


def test_forecast_empty_text_has_no_backend_jargon():
    """空态文案不得出现后端名词（views / queue / scheduled_at 之类）。"""
    block = APP_VUE.split("const scForecastEmptyText", 1)[1][:600]
    # 只看会显示给用户的那几个字符串字面量
    shown = re.findall(r"return `([^`]*)`|return '([^']*)'", block)
    texts = [a or b for a, b in shown]
    assert texts
    for t in texts:
        for bad in ("views", "queue", "work_key", "scheduled_at", "planned", "due"):
            assert bad not in t, f"空态文案露了后端名词 {bad}：{t}"


# ---------------------------------------------------------------------------
# ⑥ Reply 键盘（用户追加反馈：「反馈建议的选项在 TG Bot 的 Reply 里没有显示出来」）
# ---------------------------------------------------------------------------
def test_is_private_chat_detection():
    """私聊 = chat_id 与 user_id 相同；群/频道是负数或不等。"""
    from app import bot_listener as bl
    assert bl._is_private_chat("555", "555") is True
    assert bl._is_private_chat("555", "-1001234567") is False
    assert bl._is_private_chat("555", "999") is False
    assert bl._is_private_chat("", "555") is False


def _capture_dispatch(monkeypatch):
    from app import bot_listener as bl
    calls = []

    async def fake_reply(db, token, chat_id, text, reply_markup=None, parse_mode=None):
        calls.append({"chat_id": chat_id, "markup": reply_markup})

    monkeypatch.setattr(bl, "_reply", fake_reply)
    return bl, calls


def test_help_action_carries_main_keyboard_even_for_chinese_help(monkeypatch):
    """`/帮助` 与 `/start xxx` 也要拿到键盘 —— 旧实现按原文精确比较，两者都漏。"""
    bl, calls = _capture_dispatch(monkeypatch)
    asyncio.run(bl._dispatch_reply(None, "t", "555", "/帮助", "help", "帮助文本", "555"))
    markup = calls[-1]["markup"]
    assert markup, "/帮助 没带 Reply 键盘"
    assert markup["keyboard"] == [["求片", "投稿", "搜索"], ["打赏", "建议/反馈"]], (
        f"Reply 键盘不是 3+2 布局：{markup['keyboard']}")


def test_plain_private_reply_refreshes_keyboard(monkeypatch):
    """**核心**：任意一条私聊普通回复都顺手把最新主菜单刷回去。

    Telegram 的 Reply 键盘是客户端本地状态，只认该 chat 最后一次收到的
    reply_markup。Bot 升级加了一个按钮，老会话不重新下发就永远看不到——
    这就是「反馈建议的选项没显示出来」的真因。
    """
    bl, calls = _capture_dispatch(monkeypatch)
    asyncio.run(bl._dispatch_reply(None, "t", "555", "随便一句话", "ignored", "文本", "555"))
    markup = calls[-1]["markup"]
    assert markup and markup["keyboard"][-1] == ["打赏", "建议/反馈"], (
        "私聊普通回复没刷新主菜单，老用户升级后看不到第 5 个按钮")


def test_group_reply_never_carries_reply_keyboard(monkeypatch):
    """群聊绝不能带 Reply 键盘：会把群的输入区顶开，且 selective 只对 @Bot 的人生效。"""
    bl, calls = _capture_dispatch(monkeypatch)
    asyncio.run(bl._dispatch_reply(None, "t", "-1001234", "随便一句话",
                                   "ignored", "文本", "555"))
    assert calls[-1]["markup"] is None, "群聊带上了 Reply 键盘"


def test_reply_keyboard_layout_is_three_plus_two():
    """5 个按钮排 3+2 两行；4+1 会剩一个按钮拉满整行宽。"""
    from app import bot_listener as bl
    kb = bl.main_reply_keyboard()["keyboard"]
    assert len(kb) == 2, f"应为两行，实际 {len(kb)} 行"
    assert len(kb[0]) == 3 and len(kb[1]) == 2
    assert sum(len(r) for r in kb) == 5
    assert "建议/反馈" in kb[1]


def test_feedback_button_text_matches_builtin_alias():
    """键盘上的「建议/反馈」必须能被指令匹配器认出来，否则点了没反应。"""
    from app import bot_listener as bl
    kb = bl.main_reply_keyboard()["keyboard"]
    label = [b for r in kb for b in r if "建议" in b][0]
    assert bl._match_builtin(label.lower()) == "建议", (
        f"点「{label}」匹配不到内置指令，用户会以为按钮坏了")
