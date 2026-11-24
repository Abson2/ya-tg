"""v2.1.19 bug1：手动扫描每次点都报同一个「清理无效链接 N 条」。

用户原话：
    bug1:手动扫描按钮每次都显示手动扫描完成：清理无效链接 22 条 ·
         清理失败任务 0 个 · 清除已推标记 0 条 · 重排 0 项 / 0 个任务

根因（`_scan_invalid_links` 候选集少了「已归档」排除）：

    q = (select(SCPushListItem, SCPublishTask)
         .join(...)
         .where(SCPublishTask.share_url.isnot(None), SCPublishTask.share_url != ""))

候选集只按 `share_url` 非空取，**不看 `link_state`**。而清理只写
`task.link_state = "cleaned"`，不改 `status` / `error` / `work_key`。
于是四条判据里：

    ① link_state in (suspicious, inactive)  → 清理后变 cleaned，**不再命中**（正常）
    ② link_state == invalid                  → 同上（正常）
    ③ _is_permanent_error(task.error)        → **照样命中**（bug）
    ④ work_key not in live                   → **照样命中**（bug）

即：凡是被 ③④ 扫出来的任务，清理一次之后 `link_state` 已是 `cleaned`，
下次扫描仍被 ③④ 重新判定为「可清理」→ 再清一次，数字恒定不变。
`rewind_cursor_for_work` 也被重复调用，每点一次扫描都白白回退一次游标。

本文件锁死：
  ① 清理是幂等的 —— 第二次扫描必须 0 条，不再报同一个数字；
  ② 已归档（cleaned）的记录不得被任何判据重新捞回来；
  ③ 归档后若链接真的又坏了（新任务 / 状态被显式改回），仍应能被扫出
     —— 幂等不等于「永久忽略」，不能把未来的真失效一起闷掉。
"""
import asyncio

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


_seq = {"n": 0}


def _task(db, *, work_key, status="failed", share_url="",
          link_state="unknown", error="", chat_id="-1001234567890"):
    _seq["n"] += 1
    lst = SCPushList(list_key=f"l-{_seq['n']}-{work_key}", name="t", status="active")
    db.add(lst)
    db.flush()
    item = SCPushListItem(list_id=lst.id, work_key=work_key, chat_id=chat_id,
                          dedup_key=f"{work_key}:{chat_id}:{_seq['n']}",
                          status="planned")
    db.add(item)
    db.flush()
    t = SCPublishTask(list_item_id=item.id, work_key=work_key, kind="first",
                      status=status, share_url=share_url, link_state=link_state,
                      error=error)
    db.add(t)
    db.commit()
    return t


def _plan(db):
    plan = SCSharePlan(name="p", plan_type="daily", enabled=True)
    db.add(plan)
    db.flush()
    ver = SCSharePlanVersion(plan_id=plan.id, version_no=1, status="active",
                             config_json='{"category_limit": 20, "work_limit": 50}')
    db.add(ver)
    db.flush()
    ensure_full_day_hour_window(db, ver.id)
    db.add(SCCategory(category_key="genre:剧情", name="剧情", enabled=True, priority=1))
    db.commit()
    return ver


def _scan_links():
    with SessionLocal() as db:
        pairs, total = sc2._scan_invalid_links(db)
        return [(t.id, reason) for t, _item, reason in pairs], total


def _scan_tasks():
    with SessionLocal() as db:
        pairs, total = sc2._scan_invalid_tasks(db)
        return [(t.id, reason) for t, reason in pairs], total


def _cleanup_links():
    with SessionLocal() as db:
        return sc2.cleanup_invalid_links(db)


def _run(dry_run=False):
    with SessionLocal() as db:
        return asyncio.run(sc2.repair_cycle(db, dry_run=dry_run))


# --------------------------------------------------------------------------
# ① 三条判据各造一条，验证「清理后不再被扫出」
# --------------------------------------------------------------------------
@pytest.mark.parametrize("case", [
    # (link_state, error, 是否在库, 期望首次命中原因)
    ("suspicious", "", True, "链接已判失效"),          # 判据 ①
    ("active", "作品资源已从资源库移除", True, "资源已从资源库移除"),  # 判据 ③
    ("active", "", False, "作品已不在资源库"),          # 判据 ④
    ("active", "网络超时", True, ""),                   # 临时失败，不许命中
])
def test_cleanup_links_is_idempotent_per_reason(case):
    link_state, error, in_library, want_reason = case
    with SessionLocal() as db:
        _channel(db)
        _plan(db)
        if in_library:
            _resource(db, file_id="f-1", title="在库剧")
            wk = sc2.utils.work_group_key(db.scalar(select(Resource)))
        else:
            wk = "work:已删剧"
        _task(db, work_key=wk, status="failed", link_state=link_state,
              share_url="https://t.me/c/1/2", error=error)

    first, _total = _scan_links()
    if not want_reason:
        assert first == [], f"临时失败不该被扫出，实际 {first}"
        return
    assert len(first) == 1, f"首次应扫出 1 条，实际 {first}"
    assert first[0][1] == want_reason

    out = _cleanup_links()
    assert out["cleaned"] == 1
    assert _scan_links()[0] == [], \
        f"清理后仍被扫出（原因 {want_reason}）→ 每次点扫描都会报同一个数字"

    # 再清一次也不该动任何东西
    again = _cleanup_links()
    assert again["cleaned"] == 0, "第二次清理必须 0 条"
    assert _scan_links()[0] == []


# --------------------------------------------------------------------------
# ② 手动扫描连点两次：第二次的数字必须归零（bug1 的直接复现）
# --------------------------------------------------------------------------
def test_repair_cycle_reports_same_count_twice_before_fix():
    """这是 bug 的原始现象：22 条恒定 → 这里用 3 条同构数据复现。"""
    with SessionLocal() as db:
        _channel(db)
        _plan(db)
        _resource(db, file_id="f-1", title="在库剧")
        wk = sc2.utils.work_group_key(db.scalar(select(Resource)))
        # 三条都是「永久失败 + 有链接」→ 命中判据 ③
        for i in range(3):
            _task(db, work_key=f"{wk}-{i}", status="failed", link_state="active",
                  share_url=f"https://t.me/c/1/{i}",
                  error="作品资源已从资源库移除")

    first = _run(dry_run=False)
    assert first["cleaned_links"] == 3
    second = _run(dry_run=False)
    assert second["cleaned_links"] == 0, (
        f"第二次手动扫描仍清理 {second['cleaned_links']} 条 —— "
        "这就是用户看到的「每次都显示 22 条」")
    third = _run(dry_run=False)
    assert third["cleaned_links"] == 0


def test_repair_cycle_reports_same_task_count_twice():
    """失败任务那条路径本来就是幂等的（status 改 cleaned 后退出候选），守住别回退。"""
    with SessionLocal() as db:
        _channel(db)
        _plan(db)
        for i in range(2):
            _task(db, work_key=f"work:无{i}", status="failed",
                  share_url="", error="作品资源已从资源库移除")
    first = _run(dry_run=False)
    assert first["cleaned_tasks"] == 2
    second = _run(dry_run=False)
    assert second["cleaned_tasks"] == 0


# --------------------------------------------------------------------------
# ③ 幂等 ≠ 永久忽略：归档后出现新的真失效，仍必须被扫出
# --------------------------------------------------------------------------
def test_archived_record_with_new_invalid_state_is_scanned_again():
    """把 link_state 显式改回 suspicious（例如复检发现链接又坏了）→ 必须重新命中。"""
    with SessionLocal() as db:
        _channel(db)
        _plan(db)
        _resource(db, file_id="f-1", title="在库剧")
        wk = sc2.utils.work_group_key(db.scalar(select(Resource)))
        t = _task(db, work_key=wk, status="published",
                  share_url="https://t.me/c/9/9", link_state="suspicious")
        tid = t.id

    assert _cleanup_links()["cleaned"] == 1
    assert _scan_links()[0] == []

    # 链接复检又判失效：状态被显式写回 suspicious
    with SessionLocal() as db:
        row = db.get(SCPublishTask, tid)
        row.link_state = "suspicious"
        row.updated_at = sc2.utcnow()
        db.commit()

    got = _scan_links()[0]
    assert [r for _i, r in got] == ["链接已判失效"], \
        f"归档记录被显式改回失效态后应能重新扫出，实际 {got}"


def test_repair_cycle_rewinds_cursor_only_once():
    """重复清理会重复 rewind 游标 —— 用 marks_reset / rewind 计数确认不再重复。"""
    with SessionLocal() as db:
        _channel(db)
        _plan(db)
        r = _resource(db, file_id="f-1", title="在库剧", share_status="shared",
                      share_url="https://t.me/c/1/1")
        wk = sc2.utils.work_group_key(r)
        _task(db, work_key=wk, status="published",
              share_url="https://t.me/c/1/1", link_state="active",
              error="作品资源已从资源库移除")

    first = _run(dry_run=False)
    assert first["cleaned_links"] == 1
    second = _run(dry_run=False)
    assert second["cleaned_links"] == 0
    # 第二次没有任何东西被清 → 不该再回退游标 / 再清已推标记
    with SessionLocal() as db:
        assert second["marks_reset"] == 0, "第二次不该再动已推标记"


# --------------------------------------------------------------------------
# ④ 归档仍是「状态归档」，不物理删行
# --------------------------------------------------------------------------
def test_cleaned_link_is_archived_not_deleted():
    """只走清理，不带后续 plan/dispatch —— 后者会 revive 同一行，看不出归档态。"""
    with SessionLocal() as db:
        _channel(db)
        _plan(db)
        _resource(db, file_id="f-1", title="在库剧")
        wk = sc2.utils.work_group_key(db.scalar(select(Resource)))
        _task(db, work_key=wk, status="failed", share_url="https://t.me/c/1/1",
              link_state="active", error="作品资源已从资源库移除")
    assert _cleanup_links()["cleaned"] == 1
    with SessionLocal() as db:
        t = db.scalar(select(SCPublishTask))
        assert t.link_state == sc2.CLEANED_LINK_STATE
        assert db.scalar(select(func.count(SCPublishTask.id))) == 1, \
            "归档不得物理删除行"
