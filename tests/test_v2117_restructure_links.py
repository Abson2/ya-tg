"""v2.1.17 验收：清空/清理时，v2 分享中心的**推送标记必须真正清干净**。

现场（2026-11-23 线上）两段症状，其实是同一条链上的两处没打通：

  A. 「资源库有影视，推送任务一个都没有」
     旧队列判据 `_valid_legacy_push_pairs` 只排除 `cancelled`，于是
     `not_created`（B 库清空后归零 / 被清理过）与 `failed`（建链失败）都被
     当成「推送仍生效」。资源库里明明有这部作品、分享早已不存在，名额却被
     一条历史 `push_sent` 永久占死 → `_SlotIndex` 判 `valid` → 永不排任务。

  B. 「清理推送任务只是清空了列表」
     cleanup 只动 `SCPublishTask` / `SCPushListItem`，**没动**
     `Resource.share_status / share_id / share_url`。这三处标记不清，
     旧队列判据照样认为推送生效，清完还是排不回来。

  另有 C：`mirror_restructure` 的分享中心段只联动旧版 `ShareTask` /
  `ShareRecord`，v2 四张表完全没碰，清空 B 库后在途任务继续逐条建链失败。

本文件锁死：
  ① `_valid_legacy_push_pairs` 判据 = `share_status == active` **且** 有链接；
  ② 两个清理端点 + `invalidate_for_works` 都把资源行标记一并归零；
  ③ 清空重整时在途任务 → invalid、published 只标链接失效、游标归零；
  ④ 一行都不删，`work_keys` 为空时不得误伤全量（安全阀）。
"""
import pytest
from sqlalchemy import delete, select

from app import share_center_v2 as sc2
from app.db import SessionLocal, migrate
from app.models import (
    Channel,
    Resource,
    SCCategoryCursor,
    SCLibraryVersion,
    SCPublishTask,
    SCPushList,
    SCPushListItem,
)

WK = "work-alpha"
WK2 = "work-beta"


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as db:
        for model in (SCPublishTask, SCPushListItem, SCPushList,
                      SCLibraryVersion, SCCategoryCursor):
            db.execute(delete(model))
        db.execute(delete(Resource))
        db.execute(delete(Channel))
        db.commit()
    yield
    with SessionLocal() as db:
        for model in (SCPublishTask, SCPushListItem, SCPushList,
                      SCLibraryVersion, SCCategoryCursor):
            db.execute(delete(model))
        db.execute(delete(Resource))
        db.execute(delete(Channel))
        db.commit()


def _item(db, work_key=WK, status="planned", chat_id="-1001"):
    lst = SCPushList(list_key=f"l-{work_key}-{chat_id}", name="t", status="active")
    db.add(lst)
    db.flush()
    it = SCPushListItem(list_id=lst.id, work_key=work_key, chat_id=chat_id,
                        dedup_key=f"{work_key}:{chat_id}:first", status=status)
    db.add(it)
    db.commit()
    return it


def _task(db, item, *, status="planned", share_url="", link_state="unknown",
          retry=None, lease_owner="", work_key=None):
    t = SCPublishTask(list_item_id=item.id, work_key=work_key or item.work_key,
                      kind="first", status=status, share_url=share_url,
                      link_state=link_state, next_retry_at=retry,
                      lease_owner=lease_owner)
    db.add(t)
    db.commit()
    return t


def _cursor(db, key="genre:剧情", pos=99):
    c = SCCategoryCursor(plan_version_id=1, category_key=key,
                         last_library_version_id=pos, last_work_key="x", status="busy")
    db.add(c)
    db.commit()
    return c


# ---------- ① 在途任务作废 ----------
def test_inflight_tasks_become_invalid():
    with SessionLocal() as db:
        it = _item(db)
        _task(db, it, status="planned")
        _task(db, it, status="building", share_url="https://x/1", link_state="active")
        _task(db, it, status="failed", retry=None)
        out = sc2.invalidate_for_works(db, {WK}, reason="B 库清空")
        db.commit()
        assert out["tasks_invalidated"] == 3
        rows = db.scalars(select(SCPublishTask)).all()
        assert all(t.status == sc2.INVALID_TASK_STATUS for t in rows)
        assert all("B 库清空" in (t.error or "") for t in rows)


def test_published_task_keeps_status_but_link_marked_dead():
    """已推过是历史事实：状态保留，「已推送」里还要看得到，但链接确实坏了。"""
    with SessionLocal() as db:
        it = _item(db)
        _task(db, it, status="published", share_url="https://x/old", link_state="active")
        out = sc2.invalidate_for_works(db, {WK})
        db.commit()
        t = db.scalars(select(SCPublishTask)).one()
        assert t.status == "published"                  # 历史不篡改
        assert t.link_state == sc2.CLEANED_LINK_STATE   # 但链接标失效
        assert out["links_cleaned"] == 1
        assert out["tasks_invalidated"] == 0


def test_cancelled_task_untouched():
    """用户手动取消的语义要留着，不能被重整改写成「无效推送」。"""
    with SessionLocal() as db:
        it = _item(db)
        _task(db, it, status="cancelled", share_url="https://x/c",
              link_state=sc2.CLEANED_LINK_STATE)
        out = sc2.invalidate_for_works(db, {WK})
        db.commit()
        t = db.scalars(select(SCPublishTask)).one()
        assert t.status == "cancelled"
        assert out["tasks_invalidated"] == 0


# ---------- ② 退避清空 + 名额释放 ----------
def test_retry_backoff_cleared_so_slot_is_released():
    """invalid 不在 dispatch 候选集里，留着未来的 next_retry_at 反而会占名额。"""
    from datetime import timedelta

    from app.models import utcnow
    with SessionLocal() as db:
        it = _item(db)
        _task(db, it, status="invalid", share_url="https://x/2",
              link_state=sc2.INVALID_TASK_STATUS,
              retry=utcnow() + timedelta(hours=1), lease_owner="worker-1")
        sc2.invalidate_for_works(db, {WK})
        db.commit()
        t = db.scalars(select(SCPublishTask)).one()
        assert t.next_retry_at is None
        assert t.lease_owner == ""
        # 名额必须可回收，否则这部作品在 B 库恢复后永远排不回来
        state, _ = sc2._SlotIndex(db, live_keys={WK}).state(WK, "-1001")
        assert state == sc2.SLOT_INVALID


def test_after_revive_work_is_plannable_again():
    """作废后名额回收：作品回到资源库 → 判定不再占位，可以重新建队列。"""
    with SessionLocal() as db:
        it = _item(db)
        _task(db, it, status="published", share_url="https://x/3", link_state="active")
        sc2.invalidate_for_works(db, {WK}, reset_cursors=False)
        db.commit()
        state, reuse = sc2._SlotIndex(db, live_keys={WK}).state(WK, "-1001")
        assert state == sc2.SLOT_INVALID
        assert reuse is not None and reuse.id == it.id   # 复用旧行，不新插（dedup_key 唯一）


# ---------- ③ 游标归零 ----------
def test_cursors_reset_to_zero():
    with SessionLocal() as db:
        _cursor(db, "genre:剧情", 99)
        _cursor(db, "genre:喜剧", 42)
        out = sc2.invalidate_for_works(db, None, reset_cursors=True)
        db.commit()
        assert out["cursors_reset"] == 2
        rows = db.scalars(select(SCCategoryCursor)).all()
        assert all(int(c.last_library_version_id or 0) == 0 for c in rows)
        assert all(c.status == "idle" for c in rows)
        assert all(c.last_work_key == "" for c in rows)


def test_reset_cursors_can_be_disabled():
    with SessionLocal() as db:
        _cursor(db, "genre:剧情", 99)
        out = sc2.invalidate_for_works(db, None, reset_cursors=False)
        db.commit()
        assert out["cursors_reset"] == 0
        assert int(db.scalars(select(SCCategoryCursor)).one().last_library_version_id) == 99


# ---------- ④ 范围与安全阀 ----------
def test_scope_dir_only_touches_given_works():
    with SessionLocal() as db:
        it1 = _item(db, WK)
        it2 = _item(db, WK2)
        _task(db, it1, status="planned")
        _task(db, it2, status="planned")
        out = sc2.invalidate_for_works(db, {WK})
        db.commit()
        assert out["tasks_invalidated"] == 1
        by_wk = {t.work_key: t.status for t in db.scalars(select(SCPublishTask)).all()}
        assert by_wk[WK] == sc2.INVALID_TASK_STATUS
        assert by_wk[WK2] == "planned"          # 没被清空的作品不受影响


def test_empty_work_keys_is_noop_not_global():
    """安全阀：`scope=dir` 却圈出空集合时，**绝不能**当成全量清空。"""
    with SessionLocal() as db:
        it = _item(db)
        _task(db, it, status="planned")
        out = sc2.invalidate_for_works(db, set())
        db.commit()
        assert out == {"tasks_invalidated": 0, "links_cleaned": 0,
                       "cursors_reset": 0, "marks_reset": 0}
        assert db.scalars(select(SCPublishTask)).one().status == "planned"


def test_no_rows_are_deleted():
    """红线：清理类操作只归档/改状态，历史行与执行记录必须保留。"""
    with SessionLocal() as db:
        it = _item(db)
        _task(db, it, status="planned")
        _task(db, it, status="published", share_url="https://x/4")
        _task(db, it, status="cancelled")
        before = len(db.scalars(select(SCPublishTask)).all())
        sc2.invalidate_for_works(db, {WK})
        db.commit()
        assert len(db.scalars(select(SCPublishTask)).all()) == before == 3


# ---------- ⑤ 清理要「真正清干净」：任务 + 链接 + 资源标记一起清 ----------
def _resource(db, *, file_id="f1", title="Show", share_status="active",
              share_url="https://t.me/c/1/2", share_id="sid-1"):
    r = Resource(file_id=file_id, name=f"{title}.S01E01.mkv", ext=".mkv",
                 size=100, utime=1, identify_status="identified", media_type="tv",
                 tmdb_id=1, media_title=title, share_dir_name=title,
                 share_scope="series_dir", share_status=share_status,
                 share_url=share_url, share_id=share_id)
    db.add(r)
    db.commit()
    return r, sc2.utils.work_group_key(r)


def test_reset_share_marks_clears_resource_flags():
    with SessionLocal() as db:
        res, wk = _resource(db)
        assert sc2.reset_share_marks_for_works(db, {wk}) == 1
        db.commit()
        r = db.get(Resource, res.id)
        assert r.share_status == "not_created"
        assert r.share_id == "" and r.share_url == ""


def test_reset_share_marks_is_idempotent():
    with SessionLocal() as db:
        _res, wk = _resource(db, share_status="not_created", share_url="", share_id="")
        assert sc2.reset_share_marks_for_works(db, {wk}) == 0


def test_reset_share_marks_does_not_touch_other_works():
    with SessionLocal() as db:
        _r1, wk1 = _resource(db, file_id="f1", title="Show")
        _resource(db, file_id="f2", title="Other")
        assert sc2.reset_share_marks_for_works(db, {wk1}) == 1
        marks = {r.media_title: r.share_status
                 for r in db.scalars(select(Resource)).all()}
        assert marks["Show"] == "not_created"
        assert marks["Other"] == "active"


def test_cleanup_tasks_resets_resource_share_marks():
    """清理推送任务必须把资源行的「已分享」标记一并清掉（用户原话）。"""
    with SessionLocal() as db:
        res, wk = _resource(db)
        it = _item(db, wk)
        _task(db, it, status=sc2.INVALID_TASK_STATUS,
              share_url="https://t.me/c/1/2", work_key=wk)
        out = sc2.api_cleanup_tasks(sc2.Payload(payload={}), db=db)
        assert out["cleaned"] == 1
        assert out["marks_reset"] == 1
        r = db.get(Resource, res.id)
        assert r.share_status == "not_created" and r.share_url == ""


def test_cleanup_share_records_resets_resource_share_marks():
    with SessionLocal() as db:
        res, wk = _resource(db)
        it = _item(db, wk)
        _task(db, it, status="published", share_url="https://t.me/c/1/2",
              link_state="suspicious", work_key=wk)
        out = sc2.api_cleanup_share_records(sc2.Payload(payload={}), db=db)
        assert out["cleaned"] == 1
        assert out["marks_reset"] == 1
        assert db.get(Resource, res.id).share_status == "not_created"


def test_invalidate_for_works_resets_resource_share_marks():
    with SessionLocal() as db:
        res, wk = _resource(db)
        it = _item(db, wk)
        _task(db, it, status="planned", work_key=wk)
        out = sc2.invalidate_for_works(db, {wk})
        db.commit()
        assert out["marks_reset"] == 1
        assert db.get(Resource, res.id).share_status == "not_created"


def test_after_cleanup_legacy_push_no_longer_blocks():
    """端到端：清理前旧队列把作品判成「已推送过」，清理后不再挡。"""
    import json as _json

    from app.models import AuditLog, Channel, QueueItem

    with SessionLocal() as db:
        res, wk = _resource(db)      # 关键：share_status 初值 active + 有链接
        ch = Channel(bot_id=0, name="g", chat_id="-1001", enabled=True, kind="group")
        db.add(ch)
        db.flush()
        db.add(QueueItem(resource_id=res.id, channel_id=ch.id, status="sent",
                         clean_state=""))
        db.add(AuditLog(action="push_sent", target_type="queue",
                        detail=_json.dumps({"resource_id": res.id,
                                            "channel_id": ch.id,
                                            "message_id": 1})))
        it = _item(db, wk)
        _task(db, it, status=sc2.INVALID_TASK_STATUS, work_key=wk)
        db.commit()
        # 清理前：这条历史推送仍被算作生效
        assert (wk, "-1001") in sc2._valid_legacy_push_pairs(db)
        sc2.api_cleanup_tasks(sc2.Payload(payload={}), db=db)
        # 清理后：任务 + 链接 + 资源标记一起清，历史推送作废、名额回收
        assert (wk, "-1001") not in sc2._valid_legacy_push_pairs(db)
        assert sc2._SlotIndex(db, live_keys={wk}).state(wk, "-1001")[0] == sc2.SLOT_INVALID


# ---------- ⑥ 端点接线（源码级守卫，与仓库既有做法一致） ----------
def _main_src() -> str:
    from pathlib import Path
    return (Path(__file__).resolve().parents[1] / "backend/app/main.py").read_text(encoding="utf-8")


def _app_src() -> str:
    from pathlib import Path
    return (Path(__file__).resolve().parents[1] / "frontend/src/App.vue").read_text(encoding="utf-8")


def test_restructure_endpoint_wires_v2_invalidation():
    src = _main_src()
    assert "invalidate_for_works" in src
    assert "④b v2 分享中心层" in src
    # 预览也要报 v2 侧数字，否则用户 dry_run 时看不到这批任务会被一起作废
    assert "sc_v2_tasks" in src and "sc_v2_cursors" in src


def test_restructure_endpoint_reports_counts():
    src = _main_src()
    for field in ("sc_v2_tasks", "sc_v2_links", "sc_v2_cursors"):
        assert f'"{field}"' in src, field


def test_frontend_confirm_explains_v2_cleanup():
    src = _app_src()
    assert "无效推送" in src
    assert "sc_v2_tasks" in src
    assert "队列从头重扫" in src


def test_frontend_does_not_leak_backend_terms_in_restructure_copy():
    src = _app_src()
    start = src.index("async function restructureBLibrary")
    block = src[start:start + 2200]
    for term in ("SCPublishTask", "SCPushListItem", "link_state", "work_key",
                 "SCCategoryCursor", "next_retry_at"):
        assert term not in block, term
