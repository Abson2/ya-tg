"""v2.1.20：有海报却显示「无封面」——`_work_posters` 两条取数路径同时落空。

现场（小气鬼，推送任务 533/534 引用的快照）：
  - `Resource.poster_path = '/sJVh2U27BXwnfT05kpLOmo0ZUop.jpg'`  ← 资源表里有海报
  - LV29（v1）metadata.poster_path 有值                          ← 旧快照也有
  - LV495（v2，最新，被任务引用）metadata.poster_path = ''        ← 最新快照恰好空
  - `work_key = 'work:小气鬼'`

旧实现两个缺陷叠在一起：
  ① `seen` 在读到**第一条版本**（哪怕 poster 为空）后就把该 key 锁死，
     旧版本的非空海报再也读不到 → 第一条路径落空；
  ② 实时资源兜底只对 `file:` / `tmdb:` 前缀回查 `Resource`，
     `work:` 前缀**一个都不查** → 资源表里那张海报用不上。

本文件锁死：
  ① 最新快照海报为空时，必须退到旧快照（**反证**：旧实现返回空串）；
  ② 快照全空时，必须用快照登记的 resource_ids 回查资源表（**反证**：旧实现跳过 work:）；
  ③ 最新快照有海报时直接用，不被旧版本覆盖；
  ④ `file:` / `tmdb:` 前缀的原有路径不回归；
  ⑤ 取不到就是空串，不编造；资源已归档时不拿归档行的海报。
"""
import json

import pytest
from sqlalchemy import delete

from app import share_center_v2 as sc2
from app import utils
from app.db import SessionLocal, migrate
from app.models import (
    Bot,
    Channel,
    Resource,
    SCCategoryCursor,
    SCLibraryVersion,
    SCPublishTask,
    SCPushList,
    SCPushListItem,
)

WIPED = (SCPublishTask, SCPushListItem, SCPushList, SCLibraryVersion,
         SCCategoryCursor, Resource, Channel, Bot)

POSTER = "/sJVh2U27BXwnfT05kpLOmo0ZUop.jpg"


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    for _ in range(2):
        with SessionLocal() as db:
            for model in WIPED:
                db.execute(delete(model))
            db.commit()
    yield


def _res(db, *, title="小气鬼", poster=POSTER, file_id="f-1", tmdb_id=1):
    r = Resource(file_id=file_id, name=f"{title}.S01E01.mkv", ext=".mkv",
                 size=100, utime=1, identify_status="identified", media_type="tv",
                 tmdb_id=tmdb_id, media_title=title, share_dir_name=title,
                 share_scope="series_dir", share_status="not_created",
                 poster_path=poster)
    db.add(r)
    db.flush()
    db.commit()
    return r


def _lv(db, work_key, *, version_no, poster="", title=None, resource_ids=None):
    meta = {"title": title if title is not None else work_key.split(":", 1)[-1]}
    meta["poster_path"] = poster
    lv = SCLibraryVersion(
        work_key=work_key, version_no=version_no, fingerprint=f"fp-{version_no}",
        media_type="tv",
        resource_ids_json=json.dumps(resource_ids or []),
        metadata_json=json.dumps(meta, ensure_ascii=False),
        change_type="first")
    db.add(lv)
    db.commit()
    return lv


def _posters(*keys):
    with SessionLocal() as db:
        return sc2._work_posters(db, list(keys))


# --------------------------------------------------------------------------
# ① 最新快照海报为空 → 退到旧快照（现场场景，反证核心）
# --------------------------------------------------------------------------
def test_falls_back_to_older_snapshot_when_latest_poster_empty():
    with SessionLocal() as db:
        r = _res(db)
        wk = utils.work_group_key(r)
    assert wk == "work:小气鬼", f"聚合键形态变了，用例前提失效：{wk}"
    with SessionLocal() as db:
        _lv(db, wk, version_no=1, poster=POSTER, title="小气鬼",
            resource_ids=[r.id])
        _lv(db, wk, version_no=2, poster="", title="小气鬼.S01E01.mkv",
            resource_ids=[r.id])

    got = _posters(wk)
    assert got[wk] == POSTER, (
        "最新快照 poster 为空时必须退到旧快照；返回空串就是「有海报却显示无封面」")


def test_picks_first_non_empty_across_many_versions():
    """快照兜底路径：实时资源取不到海报时，仍按版本倒序取第一个非空。

    v2.1.25 起「实时资源优先」，所以这里把资源本身的海报清空，专测快照兜底
    这条路的顺序（否则会被实时值抢先命中，测不到兜底）。
    """
    with SessionLocal() as db:
        r = _res(db, poster="")
        wk = utils.work_group_key(r)
    with SessionLocal() as db:
        _lv(db, wk, version_no=1, poster="/old.jpg", resource_ids=[r.id])
        _lv(db, wk, version_no=2, poster="", resource_ids=[r.id])
        _lv(db, wk, version_no=3, poster="", resource_ids=[r.id])
        _lv(db, wk, version_no=4, poster="/newest.jpg", resource_ids=[r.id])

    assert _posters(wk)[wk] == "/newest.jpg"


# --------------------------------------------------------------------------
# ② 快照全空 → 用 resource_ids 回查资源表（work: 前缀兜底）
# --------------------------------------------------------------------------
def test_work_prefix_falls_back_to_resource_table():
    """旧实现只对 file:/tmdb: 回查，work: 直接被跳过 → 这条必红。"""
    with SessionLocal() as db:
        r = _res(db)
        wk = utils.work_group_key(r)
    with SessionLocal() as db:
        _lv(db, wk, version_no=1, poster="", resource_ids=[r.id])

    got = _posters(wk)
    assert got[wk] == POSTER, (
        "快照海报为空时应用快照登记的 resource_ids 回查资源表；"
        "work: 前缀不回查就是资源表有海报也不显示")


def test_fallback_uses_latest_version_resource_ids_first():
    """多版本登记的 resource 不同 → 按版本倒序，先试最新版本登记的资源。"""
    with SessionLocal() as db:
        r1 = _res(db, title="小气鬼", file_id="f-1", poster="")
        r2 = _res(db, title="小气鬼", file_id="f-2", poster="/second.jpg")
        # commit 会让属性过期，会话关闭后读就是 DetachedInstanceError → 这里先取好
        wk, rid1, rid2 = utils.work_group_key(r1), r1.id, r2.id
    with SessionLocal() as db:
        _lv(db, wk, version_no=1, poster="", resource_ids=[rid2])
        _lv(db, wk, version_no=2, poster="", resource_ids=[rid1])

    assert _posters(wk)[wk] == "/second.jpg"


def test_fallback_does_not_leak_poster_between_works():
    with SessionLocal() as db:
        r1 = _res(db, title="小气鬼", file_id="f-1", poster="/a.jpg")
        r2 = _res(db, title="另一部", file_id="f-2", poster="/b.jpg")
        wk1 = utils.work_group_key(r1)
        wk2 = utils.work_group_key(r2)
    with SessionLocal() as db:
        _lv(db, wk1, version_no=1, poster="", resource_ids=[r1.id])
        _lv(db, wk2, version_no=1, poster="", resource_ids=[r2.id])

    got = _posters(wk1, wk2)
    assert got[wk1] == "/a.jpg" and got[wk2] == "/b.jpg", f"不许串台：{got}"


# --------------------------------------------------------------------------
# ③ 最新快照有海报 → 直接用
# --------------------------------------------------------------------------
def test_latest_snapshot_poster_wins_over_older():
    """快照兜底内部：多版本都有海报时取最新那一版（实时资源无海报的场景）。"""
    with SessionLocal() as db:
        r = _res(db, poster="")
        wk = utils.work_group_key(r)
    with SessionLocal() as db:
        _lv(db, wk, version_no=1, poster="/old.jpg", resource_ids=[r.id])
        _lv(db, wk, version_no=2, poster="/new.jpg", resource_ids=[r.id])

    assert _posters(wk)[wk] == "/new.jpg"


# --------------------------------------------------------------------------
# ④ file: / tmdb: 原有路径不回归
# --------------------------------------------------------------------------
def test_file_prefix_still_resolves_from_resource():
    with SessionLocal() as db:
        r = _res(db, file_id="f-77")
        got = _posters(f"file:{r.file_id}")
    assert got["file:f-77"] == POSTER


def test_tmdb_prefix_still_resolves_from_resource():
    with SessionLocal() as db:
        r = _res(db, tmdb_id=4242)
        got = _posters(f"tmdb:tv:{r.tmdb_id}")
    assert got[f"tmdb:tv:4242"] == POSTER


# --------------------------------------------------------------------------
# ⑤ 取不到就是空串；归档行不参与兜底
# --------------------------------------------------------------------------
def test_no_poster_anywhere_returns_empty_string():
    with SessionLocal() as db:
        r = _res(db, poster="")
        wk = utils.work_group_key(r)
    with SessionLocal() as db:
        _lv(db, wk, version_no=1, poster="", resource_ids=[r.id])
    assert _posters(wk)[wk] == ""


def test_unknown_key_returns_empty_string():
    assert _posters("work:从未存在")["work:从未存在"] == ""


def test_archived_resource_poster_not_used():
    """资源已归档 → 全局归档过滤看不到它 → 兜底拿不到，返回空串（不编造）。"""
    with SessionLocal() as db:
        r = _res(db, poster=POSTER)
        wk = utils.work_group_key(r)
        rid = r.id
    with SessionLocal() as db:
        _lv(db, wk, version_no=1, poster="", resource_ids=[rid])
        row = db.get(Resource, rid)
        row.clean_state = "cleaned"
        row.clean_reason = "测试归档"
        db.commit()

    assert _posters(wk)[wk] == ""


# --------------------------------------------------------------------------
# ⑥ 空入参不炸
# --------------------------------------------------------------------------
def test_empty_input():
    assert _posters() == {}
    assert _posters("", "") == {}   # 空串键直接跳过，不登记
