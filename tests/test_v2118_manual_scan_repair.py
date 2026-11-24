"""v2.1.18：分享中心「手动扫描」升级为 体检 → 自愈 → 重排 → 推送。

用户要的一句话：**为清理无效链接、失败任务，将仍然在库的文件重新排队，按发布规则推送。**

以前「手动扫描」只是调 `/sc/run` 跑一轮 `run_cycle` —— 建计划、建任务、派发。
它不体检、不清无效、不动游标，所以「有东西卡着」的时候点它没用，用户还得自己
去猜该点「清理无效推送任务」还是「重建首推」还是「清理分享链接」。

本版把这几件事串成一条流水线（`repair_cycle`），并且**判据只有一套**：
体检与清理都走 `_scan_invalid_tasks` / `_scan_invalid_links`，不存在
「手动扫描说 3 条、清理按钮说 5 条」这种自相矛盾。

本文件锁死：
  ① dry_run 默认开、且零写入（前端必须先给用户看数字）；
  ② 执行后无效链接 / 永久失败任务被归档，**临时失败不许误清**；
  ③ 游标归零、资源行「已分享」标记归零 → 在库作品真能重新排进队列；
  ④ 一行都不删；无生效计划版本时不崩、如实说明原因。
"""
import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import delete, func, select

from app import share_center_v2 as sc2
from app.db import SessionLocal, migrate
from app.models import (
    Bot,
    Channel,
    Resource,
    SCCategory,
    SCCategoryCursor,
    SCLibraryVersion,
    SCPublishTask,
    SCPushList,
    SCPushListItem,
    SCSharePlan,
    SCSharePlanVersion,
    SCWorkCategory,
)

from conftest import ensure_full_day_hour_window

WIPED = (SCPublishTask, SCPushListItem, SCPushList, SCLibraryVersion,
         SCCategoryCursor, SCWorkCategory, SCCategory, SCSharePlanVersion,
         SCSharePlan, Resource, Channel, Bot)


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    for _ in range(2):
        with SessionLocal() as db:
            for model in WIPED:
                db.execute(delete(model))
            db.commit()
    yield


# --------------------------------------------------------------------------
# 造数据
# --------------------------------------------------------------------------
def _resource(db, *, file_id="f-1", title="某剧", share_status="not_created",
              share_url=""):
    r = Resource(file_id=file_id, name=f"{title}.S01E01.mkv", ext=".mkv",
                 size=100, utime=1, identify_status="identified", media_type="tv",
                 tmdb_id=1, media_title=title, share_dir_name=title,
                 share_scope="series_dir", share_status=share_status,
                 share_url=share_url, share_id="sid" if share_url else "")
    db.add(r)
    db.flush()
    return r


def _channel(db):
    bot = Bot(name="b", token="x", enabled=True)
    db.add(bot)
    db.flush()
    ch = Channel(bot_id=bot.id, name="频道", chat_id="-1001234567890",
                 kind="channel", enabled=True)
    db.add(ch)
    db.flush()
    return ch


def _task(db, *, work_key, status="failed", share_url="",
          link_state="unknown", error="", chat_id="-1001234567890"):
    lst = SCPushList(list_key=f"l-{work_key}-{status}-{link_state}",
                     name="t", status="active")
    db.add(lst)
    db.flush()
    item = SCPushListItem(list_id=lst.id, work_key=work_key, chat_id=chat_id,
                          dedup_key=f"{work_key}:{chat_id}:first", status="planned")
    db.add(item)
    db.flush()
    t = SCPublishTask(list_item_id=item.id, work_key=work_key, kind="first",
                      status=status, share_url=share_url, link_state=link_state,
                      error=error)
    db.add(t)
    db.commit()
    return t


def _active_plan(db, *, work_key, category_key="genre:剧情"):
    """造一套能真跑 plan_engine 的最小环境：计划版本 + 分类 + 快照 + 分类映射。"""
    plan = SCSharePlan(name="p", plan_type="daily", enabled=True)
    db.add(plan)
    db.flush()
    ver = SCSharePlanVersion(plan_id=plan.id, version_no=1, status="active",
                             config_json=json.dumps({"category_limit": 20,
                                                     "work_limit": 50}))
    db.add(ver)
    db.flush()
    ensure_full_day_hour_window(db, ver.id)
    db.add(SCCategory(category_key=category_key, name="剧情", enabled=True, priority=1))
    db.add(SCWorkCategory(work_key=work_key, category_key=category_key))
    db.commit()
    return ver


def _run(**kw):
    with SessionLocal() as db:
        return asyncio.run(sc2.repair_cycle(db, **kw))


# --------------------------------------------------------------------------
# ① dry_run：默认开、零写入
# --------------------------------------------------------------------------
def test_dry_run_reports_without_writing_anything():
    with SessionLocal() as db:
        _channel(db)
        _task(db, work_key="work-a", status="failed", share_url="https://t.me/c/1/2",
              link_state="suspicious", error="share_file 失败：文件不存在")
    out = _run(dry_run=True)
    assert out["ok"] is True and out["dry_run"] is True
    assert out["links_invalid"] == 1
    assert out["tasks_invalid"] >= 1
    # 体检不得写库
    with SessionLocal() as db:
        t = db.scalar(select(SCPublishTask))
        assert t.status == "failed", "dry_run 不得改任务状态"
        assert t.link_state == "suspicious", "dry_run 不得改链接状态"
        assert db.scalar(select(func.count(SCPublishTask.id))) == 1


def test_dry_run_does_not_create_any_task():
    with SessionLocal() as db:
        ch = _channel(db)
        res = _resource(db)
        wk = sc2.utils.work_group_key(res)
        _active_plan(db, work_key=wk)
    out = _run(dry_run=True)
    assert out["dry_run"] is True
    with SessionLocal() as db:
        assert db.scalar(select(func.count(SCPublishTask.id))) == 0, \
            "dry_run 不得建任务"


def test_endpoint_defaults_to_dry_run():
    """前端漏传 dry_run 时必须走体检，不能默默执行。"""
    with SessionLocal() as db:
        _channel(db)
        _task(db, work_key="work-a", status="failed",
              error="share_file 失败：文件不存在")
    with SessionLocal() as db:
        out = sc2.api_repair(sc2.Payload(payload={}), db)
    assert out["dry_run"] is True
    with SessionLocal() as db:
        assert db.scalar(select(SCPublishTask)).status == "failed"


# --------------------------------------------------------------------------
# ② 执行：清无效链接 + 清永久失败，临时失败不许误清
# --------------------------------------------------------------------------
def test_repair_cleans_invalid_links_and_permanent_failures():
    with SessionLocal() as db:
        _channel(db)
        _task(db, work_key="work-link", status="published",
              share_url="https://t.me/c/1/2", link_state="suspicious",
              error="")
        _task(db, work_key="work-fail", status="failed",
              error="share_file 失败：文件不存在")
    with patch.object(sc2, "dispatch_ready_tasks",
                      new=AsyncMock(return_value={"dispatched": 0})), \
         patch.object(sc2, "sync_library_versions", return_value={}):
        out = _run(dry_run=False)
    assert out["ok"] is True and out["dry_run"] is False
    assert out["cleaned_links"] == 1
    assert out["cleaned_tasks"] >= 1
    with SessionLocal() as db:
        rows = {t.work_key: t for t in db.scalars(select(SCPublishTask)).all()}
        assert rows["work-link"].link_state == "cleaned"
        assert rows["work-fail"].status == "cleaned"


def test_repair_keeps_transient_failure_for_retry():
    """作品还在库、错误也不是永久性的（网络抖动）→ 必须留着重试，不许清。"""
    with SessionLocal() as db:
        _channel(db)
        res = _resource(db, file_id="f-keep", title="还在库")
        wk = sc2.utils.work_group_key(res)
        _task(db, work_key=wk, status="failed", error="Telegram 限流（429）")
    with patch.object(sc2, "dispatch_ready_tasks",
                      new=AsyncMock(return_value={"dispatched": 0})), \
         patch.object(sc2, "sync_library_versions", return_value={}):
        out = _run(dry_run=False)
    assert out["cleaned_tasks"] == 0, "临时失败不该被当成无效任务清掉"
    with SessionLocal() as db:
        assert db.scalar(select(SCPublishTask)).status == "failed"


def test_repair_never_deletes_rows():
    with SessionLocal() as db:
        _channel(db)
        _task(db, work_key="work-a", status="failed", share_url="https://t.me/c/1/2",
              link_state="inactive", error="share_file 失败：文件不存在")
        before = (db.scalar(select(func.count(SCPublishTask.id))),
                  db.scalar(select(func.count(SCPushListItem.id))),
                  db.scalar(select(func.count(SCPushList.id))))
    with patch.object(sc2, "dispatch_ready_tasks",
                      new=AsyncMock(return_value={"dispatched": 0})), \
         patch.object(sc2, "sync_library_versions", return_value={}):
        _run(dry_run=False)
    with SessionLocal() as db:
        after = (db.scalar(select(func.count(SCPublishTask.id))),
                 db.scalar(select(func.count(SCPushListItem.id))),
                 db.scalar(select(func.count(SCPushList.id))))
    assert before == after, "清理只能改状态，一行都不许删"


# --------------------------------------------------------------------------
# ③ 重排：在库文件真的被重新排进队列
# --------------------------------------------------------------------------
def test_repair_resets_cursors():
    with SessionLocal() as db:
        _channel(db)
        res = _resource(db, file_id="f-cur", title="游标剧")
        wk = sc2.utils.work_group_key(res)
        ver = _active_plan(db, work_key=wk)
        db.add(SCCategoryCursor(plan_version_id=ver.id, category_key="genre:剧情",
                                last_library_version_id=999, last_work_key="x",
                                status="busy"))
        db.commit()
        ver_id = ver.id
    with patch.object(sc2, "dispatch_ready_tasks",
                      new=AsyncMock(return_value={"dispatched": 0})), \
         patch.object(sc2, "sync_library_versions", return_value={}):
        out = _run(dry_run=False)
    assert out["cursors_reset"] == 1
    with SessionLocal() as db:
        cur = db.scalar(select(SCCategoryCursor).where(
            SCCategoryCursor.plan_version_id == ver_id))
        assert cur.last_library_version_id == 0


def test_repair_queues_work_back_into_plan():
    """端到端：作品还在库、历史任务已被清 → 手动扫描后必须重新排出任务。"""
    with SessionLocal() as db:
        _channel(db)
        res = _resource(db, file_id="f-queue", title="要重排的剧")
        wk = sc2.utils.work_group_key(res)
        db.add(SCLibraryVersion(work_key=wk, version_no=1, fingerprint="fp-1",
                                resource_ids_json=json.dumps([res.id]),
                                metadata_json="{}"))
        _active_plan(db, work_key=wk)
        # 一条历史「已清理」任务：名额已释放，但没有主动重排
        _task(db, work_key=wk, status="cleaned", share_url="https://t.me/c/1/2",
              link_state="cleaned")
    with patch.object(sc2, "dispatch_ready_tasks",
                      new=AsyncMock(return_value={"dispatched": 0})), \
         patch.object(sc2, "sync_library_versions", return_value={}):
        out = _run(dry_run=False)
    assert out["ok"] is True
    assert out["queued"]["tasks"] >= 1, f"应重新排出任务，实际：{out['queued']}"
    with SessionLocal() as db:
        planned = db.scalars(select(SCPublishTask).where(
            SCPublishTask.work_key == wk,
            SCPublishTask.status.in_({"planned", "ready"}))).all()
        assert planned, "清理后在库作品必须重新回到队列"


def test_repair_does_not_duplicate_first_push_for_valid_link():
    """已推成功且链接仍有效 → 手动扫描不得再造一条首推。"""
    with SessionLocal() as db:
        _channel(db)
        res = _resource(db, file_id="f-valid", title="已推好的剧",
                        share_status="active", share_url="https://t.me/c/9/9")
        wk = sc2.utils.work_group_key(res)
        db.add(SCLibraryVersion(work_key=wk, version_no=1, fingerprint="fp-2",
                                resource_ids_json=json.dumps([res.id]),
                                metadata_json="{}"))
        _active_plan(db, work_key=wk)
        _task(db, work_key=wk, status="published",
              share_url="https://t.me/c/9/9", link_state="ok")
        before = db.scalar(select(func.count(SCPublishTask.id)))
    with patch.object(sc2, "dispatch_ready_tasks",
                      new=AsyncMock(return_value={"dispatched": 0})), \
         patch.object(sc2, "sync_library_versions", return_value={}):
        _run(dry_run=False)
    with SessionLocal() as db:
        after = db.scalar(select(func.count(SCPublishTask.id)))
    assert after == before, "链接仍有效时不得重复首推"


# --------------------------------------------------------------------------
# ④ 边界：无计划版本 / 计数契约
# --------------------------------------------------------------------------
def test_repair_without_active_version_returns_reason_not_crash():
    with SessionLocal() as db:
        _channel(db)
        _task(db, work_key="work-a", status="failed",
              error="share_file 失败：文件不存在")
    out = _run(dry_run=True)
    assert out["ok"] is True
    assert out["reason"], "没有生效计划版本时必须如实说明"
    assert out["hour_allowed"] is False
    assert out["plannable"] == 0


def test_repair_still_cleans_when_no_plan_version():
    """反证：没有生效计划版本时，清理**仍要生效**。

    首版 repair_cycle 把「无版本」写成了提前 return，导致没配计划的用户
    永远清不掉无效链接与失败任务 —— 清理不该被排期条件卡住。
    """
    with SessionLocal() as db:
        _channel(db)
        _task(db, work_key="work-a", status="failed",
              error="share_file 失败：文件不存在")
    with patch.object(sc2, "dispatch_ready_tasks", new=AsyncMock(return_value={})):
        out = _run(dry_run=False)
    assert out["ok"] is True
    assert out["plan_ready"] is False
    assert out["cleaned_tasks"] == 1, "没有计划版本也必须清理"
    assert out["queued"] == {}
    with SessionLocal() as db:
        assert db.scalar(select(SCPublishTask)).status == "cleaned"


def test_report_counts_are_real_not_guessed():
    """体检数字必须与库里实际可清理项一致（不编造）。"""
    with SessionLocal() as db:
        _channel(db)
        _task(db, work_key="w1", status="failed", share_url="https://t.me/c/1/1",
              link_state="inactive")
        _task(db, work_key="w2", status="cancelled")
        _task(db, work_key="w3", status="failed",
              error="share_file 失败：文件不存在")
    out = _run(dry_run=True)
    assert out["links_invalid"] == 1, "只有 w1 有链接且已失效"
    assert out["tasks_invalid"] == 3, "w1 无永久错误但作品不在库；w2 已取消；w3 永久错误"
    assert out["links_total"] == 1
    assert out["tasks_total"] == 3


def test_frontend_uses_repair_endpoint():
    """前端「手动扫描」接入体检端点，且必须先 dry_run 预览再执行。

    只检查 `scRun` 这一段函数体：`link_state` 这类词在别的段落作为后端返回的
    数据键本来就存在（不是给用户看的文案），全文搜索会误伤。
    """
    from pathlib import Path
    app = (Path(__file__).resolve().parents[1] / "frontend/src/App.vue").read_text(
        encoding="utf-8")
    start = app.index("async function scRun")
    block = app[start:start + 2800]
    assert "/sc/repair" in block, "手动扫描未接入新端点"
    assert "dry_run: true" in block, "必须先做一次体检预览"
    assert "dry_run: false" in block, "确认后要能真正执行"
    for term in ("link_state", "work_key", "SCPublishTask", "dedup_key",
                 "next_retry_at", "SCPushListItem"):
        assert term not in block, f"界面文案泄了后端名词：{term}"
