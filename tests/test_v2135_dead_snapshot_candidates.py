"""v2.1.35：死快照不再占满每轮候选名额（排期「很空」的根因）。

现象（用户真机反馈，2026-11-25）：
  「今日推送排期和推送队列都显示到晚上了，而且全天排期很空」

根因不在调度节奏，而在**候选集被死快照占满**：

  · `plan_engine` / `forecast_schedule` 的候选是按 `SCWorkCategory` 关联
    `SCLibraryVersion` 取的，而这两张表都不会随资源库清理而删——作品被移出
    资源库后，`SCLibraryVersion` 与 `SCWorkCategory` 还在，成了「死快照」；
  · 候选查询只按 `id` 升序无过滤地取 `work_limit` 条（这个数由**发布规则**配，
    缺省才回落 50），死快照照样占名额；
  · 循环里的 `if work_key not in live_keys: continue` **在这道 SQL 之后**才执行。
    于是只要前 50 条恰好都是死快照，本轮就是**一个任务都不建**，`task_count`
    一直 0，`work_limit` 永远轮不到后面的活作品；
  · 预测侧同构：`_candidates` 一样无过滤，预测取到的 50 部死快照全被
    `_picks_for` 判空 → 排期表未来时段空白 → 用户看到「排期很空」。

修法：候选查询加 `SCLibraryVersion.work_key.in_(live_keys)`，把死快照挡在
SQL 层，让 `work_limit` 真正落在**在库作品**上。`live_keys` 仍是唯一事实依据
（`_live_work_keys` 实时扫 `Resource` 派生），不新增任何记录状态字段，
**不碰游标回退**（红线），也不碰首推三道锁与分类三分离。

本文件的断言在修复前会红（就是下面两个 `test_*_not_crowded_out_by_dead_snapshots`），
修复后转绿。
"""
import json
from datetime import datetime, timezone

import pytest
from sqlalchemy import delete, select

from app import share_center_v2 as sc2, utils
from app.db import SessionLocal, migrate
from app.models import (
    AppConfig, AuditLog, Bot, Channel, QueueItem, Resource, SCCategory,
    SCCategoryCursor, SCCategorySchedule, SCHourWindow, SCLibraryVersion,
    SCPublishTask, SCPushList, SCPushListItem, SCSharePlan, SCSharePlanVersion,
    SCWorkCategory, ShareTask,
)

from conftest import ensure_full_day_hour_window

WIPED = (SCPublishTask, SCPushListItem, SCPushList, SCLibraryVersion,
         SCCategoryCursor, SCCategorySchedule, SCWorkCategory, SCCategory,
         SCSharePlanVersion, SCSharePlan, ShareTask, QueueItem, Resource,
         Channel, Bot, AppConfig, AuditLog, SCHourWindow)

CAT = "genre:剧情"


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    for _ in range(2):
        with SessionLocal() as db:
            for model in WIPED:
                db.execute(delete(model))
            db.commit()
    yield


def _plan(db, work_limit=50):
    plan = SCSharePlan(name="p", plan_type="daily", enabled=True)
    db.add(plan)
    db.flush()
    ver = SCSharePlanVersion(plan_id=plan.id, version_no=1, status="active",
                             config_json=json.dumps({"category_limit": 20,
                                                     "work_limit": work_limit}))
    db.add(ver)
    db.flush()
    ensure_full_day_hour_window(db, ver.id)
    db.add(SCCategory(category_key=CAT, name="剧情", enabled=True, priority=0))
    db.commit()
    return ver


def _channel(db, chat_id="@chan"):
    bot = Bot(name="b", token="x", enabled=True)
    db.add(bot)
    db.flush()
    ch = Channel(bot_id=bot.id, name=chat_id, chat_id=chat_id,
                 kind="channel", enabled=True)
    db.add(ch)
    db.commit()
    return ch


def _dead_snapshot(db, *, work_key, order):
    """只有快照与分类映射、**没有资源**的作品 —— 资源库清理后留下的死记录。"""
    db.add(SCLibraryVersion(
        work_key=work_key, version_no=1, fingerprint=f"fp-dead-{order}",
        media_type="movie", resource_ids_json=json.dumps([999000 + order]),
        metadata_json=json.dumps({"title": work_key}, ensure_ascii=False),
        change_type="first"))
    db.add(SCWorkCategory(work_key=work_key, category_key=CAT))
    db.flush()


def _live_work(db, *, title, file_id):
    """真实在库作品：有 `Resource`，`work_key` 由同一套 `work_group_key` 派生。

    ⚠️ `created_at` 必须设成**老日期**（40 天前）。默认是「现在」，那样这部作品会
    带上「新入库」标签，走「上新即时推送」插队通道（`_instant_candidates`），
    整条测试就绕过了候选查询而假绿 —— 本文件已踩过一次。
    """
    old = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
    res = Resource(file_id=file_id, name=f"{title}.2024.mkv", ext=".mkv",
                   size=100, utime=1, identify_status="identified",
                   media_type="movie", tmdb_id=42, media_title=title,
                   share_dir_name=title, share_scope="series_dir",
                   share_status="not_created")
    db.add(res)
    db.flush()
    wk = utils.work_group_key(res)
    db.add(SCLibraryVersion(
        work_key=wk, version_no=1, fingerprint=f"fp-live-{file_id}",
        media_type="movie", resource_ids_json=json.dumps([res.id]),
        metadata_json=json.dumps({"title": title}, ensure_ascii=False),
        change_type="first", created_at=old))
    db.add(SCWorkCategory(work_key=wk, category_key=CAT))
    db.flush()
    return res, wk


def _seed_crowded(db, *, work_limit=50, dead=60):
    """前 N 条全是死快照，最后一条才是在库作品 —— 复刻真机的形状。"""
    _plan(db, work_limit=work_limit)
    _channel(db)
    for i in range(dead):
        _dead_snapshot(db, work_key=f"work:dead{i:03d}", order=i)
    res, wk = _live_work(db, title="活的作品", file_id="f-live-1")
    db.commit()
    return res, wk


# ---------------------------------------------------------------------------
# ① 主链路：plan_engine 不能被死快照挤掉在库作品
# ---------------------------------------------------------------------------
def test_plan_engine_not_crowded_out_by_dead_snapshots():
    """**核心反证**：60 条死快照 + 1 部在库作品，本轮必须为那部作品建任务。

    修复前：候选按 id 升序取前 50 条全是死快照 → 循环里全部 `continue` →
    一个任务都不建，用户看到的就是「排期很空」。
    """
    with SessionLocal() as db:
        _res, wk = _seed_crowded(db)
        live = sc2._live_work_keys(db)
        assert wk in live, "构造有误：目标作品必须在 live_keys 里"
        assert len(live) == 1, f"构造有误：只有一部在库作品，实际 {live}"

        sc2.plan_engine(db)
        tasks = list(db.scalars(select(SCPublishTask)).all())

    assert tasks, "一个任务都没建 —— 候选名额被死快照占满了"
    assert any(str(t.work_key) == wk for t in tasks), \
        f"在库作品 {wk} 没被排上，实际建了 {[t.work_key for t in tasks]}"


def test_dead_snapshots_get_no_tasks():
    """反证的另一半：死快照本身绝不能建任务（资源库已没有它）。"""
    with SessionLocal() as db:
        _res, wk = _seed_crowded(db)
        sc2.plan_engine(db)
        tasks = list(db.scalars(select(SCPublishTask)).all())

    dead_keys = [str(t.work_key) for t in tasks if str(t.work_key).startswith("work:dead")]
    assert dead_keys == [], f"给死快照建了任务：{dead_keys}"


# ---------------------------------------------------------------------------
# ② 预测侧：同样不能被死快照占满
# ---------------------------------------------------------------------------
def test_forecast_not_crowded_out_by_dead_snapshots():
    """预测侧：死快照把候选吃满时，**第一个小时**就该看到在库作品。

    为什么断言「第一个小时」而不是「某个小时里出现过」：`forecast_schedule` 是
    逐小时模拟、每小时都会推进游标，所以哪怕候选里全是死快照，**第二个小时**也能
    爬过去（`id > 50` 就取到后面的活作品）。真机上死快照上千条时，「每小时爬 50」
    要十几小时——用户看到的就是「全天排期很空」。所以只有「当前小时就能排出东西」
    才算修好；否则只是把空窗往后挪。
    """
    with SessionLocal() as db:
        _res, wk = _seed_crowded(db)
        out = sc2.forecast_schedule(db)

    first = out["hours"][0]
    first_works = set()
    for it in first["items"]:
        if it.get("kind") == "scheduled":
            continue
        for w in (it.get("works") or []):
            first_works.add(str(w.get("work_key") or ""))

    all_predicted = set()
    for h in out["hours"]:
        for it in h["items"]:
            if it.get("kind") == "scheduled":
                continue
            for w in (it.get("works") or []):
                all_predicted.add(str(w.get("work_key") or ""))

    dead = {k for k in all_predicted if k.startswith("work:dead")}
    assert dead == set(), f"预测里出现了死快照：{sorted(dead)[:5]}"
    assert wk in first_works, (
        f"第一个小时（{first['hour']} 点）没排出在库作品 {wk}，"
        f"实际只有 {sorted(first_works)} —— 候选名额还在被死快照吃")


# ---------------------------------------------------------------------------
# ③ 源码契约：过滤必须在 SQL 层（循环里 continue 那刀救不了名额浪费）
# ---------------------------------------------------------------------------
def _strip_comments(text: str) -> str:
    out = []
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("#"):
            continue
        out.append(line.split("  #")[0] if "  #" in line else line)
    return "\n".join(out)


def test_candidate_query_filters_by_live_keys_at_sql_level():
    src = _strip_comments(
        (__import__("pathlib").Path(__file__).resolve().parents[1]
         / "backend/app/share_center_v2.py").read_text(encoding="utf-8"))
    assert src.count("SCLibraryVersion.work_key.in_(live_keys)") >= 2, (
        "两处候选查询（plan_engine 的 base_q 与 forecast 的 _candidates）"
        "都必须加上在库过滤，否则 work_limit 会被死快照吃掉")


def test_cursor_rewind_still_intact():
    """红线提醒：这次只加过滤，绝不能顺手改游标回退逻辑。"""
    src = (__import__("pathlib").Path(__file__).resolve().parents[1]
           / "backend/app/share_center_v2.py").read_text(encoding="utf-8")
    assert "def rewind_cursor_for_work" in src
    assert "cursor.last_library_version_id = max" in src or \
           "last_library_version_id = max" in src or \
           "rewind_cursor_for_work" in src


# ---------------------------------------------------------------------------
# ④ 附带项评估（用户要求单独评估「回退目标已不在 live_keys」）
# ---------------------------------------------------------------------------
def test_rewind_into_dead_zone_does_not_stall_or_duplicate():
    """游标被回退到**死区**后，下一轮必须照常走完：不卡死、也不重复建任务。

    用户提的隐患：`rewind_cursor_for_work` 把游标退到「该作品最小快照 id - 1」，
    若这部作品已经不在库（死快照），回退目标落在死区里，担心「退回去又卡死」。

    评估结论：**加了 SQL 层在库过滤之后，这个隐患自动消失** —— 候选 SQL 直接
    跳过整片死区，回退到哪儿都只会命中在库作品。已建的不会重复建（`_SlotIndex`
    仍按名额判占）。所以不改 `rewind_cursor_for_work`（它要判在库就得全表扫
    `Resource`，而它在批量清理里被循环调用，会把轻量 UPDATE 变成重查询）。

    本用例就是把这个结论锁成回归：回退到死区后任务集合**一模一样**。
    """
    with SessionLocal() as db:
        _plan(db)
        _channel(db)
        for i in range(60):
            _dead_snapshot(db, work_key=f"work:dead{i:03d}", order=i)
        _res_a, wk_a = _live_work(db, title="活A", file_id="f-live-a")
        _res_b, wk_b = _live_work(db, title="活B", file_id="f-live-b")
        db.commit()

        sc2.plan_engine(db)
        first = sorted(str(t.work_key) for t in db.scalars(select(SCPublishTask)).all())
        assert first == sorted([wk_a, wk_b]), f"首轮没把两部在库作品排上：{first}"

        # 回退一部**已不在库**的作品：游标退回死区头部
        moved = sc2.rewind_cursor_for_work(db, "work:dead000")
        db.commit()
        assert moved >= 1, "回退没生效，用例没测到目标路径"

        sc2.plan_engine(db)
        second = sorted(str(t.work_key) for t in db.scalars(select(SCPublishTask)).all())

    assert second == first, f"回退到死区后任务集合变了（重复建/丢任务）：{second}"
    assert not [k for k in second if k.startswith("work:dead")], "给死快照建了任务"
