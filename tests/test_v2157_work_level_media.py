"""v2.1.57：作品级口径（分享 / 推送 / 快照都不许按季按集）。

真机事故（2026-11-28，用户两次贴图）：
  ① 推送出来的标题与 hashtag 都是单集文件名
     （`#TheLegendofCrazyMonk.2010.S01E07….mp4`）；
  ② 网盘「我的分享」里一条条全是单集名，同一部剧还建了 2–4 条链接。

根因是三处各自抄了「代表资源行」的字段，而扫描入库那一刻它还是空的：
  · `sync_library_versions` 把 `media_title='' / share_dir_name='' /
    share_dir_id=季目录` 定格进快照（1166 个版本里 873 个 title 是文件名、
    880 个 share_dir_name 为空、641 个目录 id 指着季目录）；
  · `publisher._build_message` 用 `res.media_title or res.name`；
  · 建分享用 `metadata.title` + `metadata.share_dir_id`。

本文件锁死（用户口径：**分享、推送都不能按季、集，必须作品级**）：
  ① `utils.work_display_meta` 的解析优先级与「判不出就留空」；
  ② 目录必须排除季目录（成员 `parent_id`），同分按 TMDB 编号取；
  ③ `recompute_version_metadata` 只改最新版本的 metadata_json —— 不造版本、
     不动指纹、幂等、dry_run 不写库；
  ④ 建分享取作品目录 + 作品名，同目录已有有效分享就复用（不重复建）；
  ⑤ `publisher._work_title` 绝不把 `.mp4` 当标题；
  ⑥ 多集作品的单集行若还没识别 → `ShareNotReady`，本轮不推、不按集建分享。
"""
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import delete

from app import publisher, share_center_v2 as sc2, utils
from app.db import SessionLocal, migrate
from app.models import (
    AppConfig, AuditLog, Bot, Channel, QueueItem, Resource, SCCategory,
    SCCategoryCursor, SCCategorySchedule, SCHourWindow, SCLibraryVersion,
    SCPublishTask, SCPushList, SCPushListItem, SCSharePlan, SCSharePlanVersion,
    SCWorkCategory, ShareTask,
)

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


def _row(**kw):
    """一行资源的最小投影（`work_display_meta` 只按属性读，不需要 ORM 实体）。"""
    base = dict(file_id="f1", path_hint="影视/电视剧/剧/Season 1", share_dir_name="",
                media_title="", tmdb_id=0, media_type="", share_dir_id="",
                parent_id="", poster_path="", name="剧.S01E01.mkv", season=1,
                episode=1, share_scope="file")
    base.update(kw)
    return SimpleNamespace(**base)


# ---------------------------------------------------------------------------
# ① work_display_meta：标题
# ---------------------------------------------------------------------------
def test_title_prefers_identified_name_over_file_name():
    """识别出的 TMDB 名优先；单集文件名永远不能当作品名。"""
    meta = utils.work_display_meta([
        _row(media_title="活佛济公", name="The Legend of Crazy Monk.2010.S01E07.mp4"),
        _row(media_title="活佛济公"),
    ])
    assert meta["title"] == "活佛济公"


def test_title_falls_back_to_cleaned_dir_name():
    """没有识别名 → 用目录名，但要剥掉 `{tmdb-…}`、防和谐分隔符与年份。"""
    meta = utils.work_display_meta([
        _row(media_title="", share_dir_name="活·佛·济·公 (2010) {tmdb-111097}"),
    ])
    assert meta["title"] == "活佛济公"


def test_title_never_returns_a_file_name():
    """什么作品级信息都没有时**留空**，绝不把 `.mp4` 当标题。"""
    meta = utils.work_display_meta([_row(name="Show.S01E07.1080p.mkv")])
    assert meta["title"] == ""
    assert utils.looks_like_file_name(meta["title"]) is False


def test_majority_wins_for_title():
    """同一部剧里两种识别名并存 → 取多数派，不受行顺序影响。"""
    rows = [_row(media_title="封神榜")] * 3 + [_row(media_title="封神英雄榜")]
    assert utils.work_display_meta(rows)["title"] == "封神榜"


# ---------------------------------------------------------------------------
# ② work_display_meta：目录（必须作品目录，不能是季目录）
# ---------------------------------------------------------------------------
def test_dir_excludes_season_dirs():
    """成员 `parent_id` 指向的是季目录 → 绝不能当作品目录用。"""
    season = "1973331655262064654"
    meta = utils.work_display_meta([
        _row(share_dir_id=season, parent_id=season),          # 快照当时就是这个
        _row(share_dir_id="1973331643253776389", parent_id=season),
        _row(share_dir_id="1973331643253776389", parent_id=season),
    ])
    assert meta["share_dir_id"] == "1973331643253776389"


def test_dir_prefers_matching_tmdb_when_name_collides():
    """同名不同剧（封神榜 99113 / 封神英雄榜 68680）混在一组时，按 TMDB 编号取目录。"""
    rows = [
        _row(tmdb_id=99113, share_dir_id="dir-fengshen", parent_id="s1"),
        _row(tmdb_id=99113, share_dir_id="dir-fengshen", parent_id="s1"),
        _row(tmdb_id=68680, share_dir_id="dir-hero", parent_id="s2"),
    ]
    meta = utils.work_display_meta(rows)
    assert meta["tmdb_id"] == 99113
    assert meta["share_dir_id"] == "dir-fengshen"


def test_empty_rows_give_empty_meta():
    meta = utils.work_display_meta([])
    assert meta["title"] == "" and meta["share_dir_id"] == "" and meta["tmdb_id"] == 0


# ---------------------------------------------------------------------------
# ③ recompute_version_metadata：只改最新版本、不造版本、幂等
# ---------------------------------------------------------------------------
def _seed_work(db, *, title_in_meta, dir_in_meta, media_title="活佛济公",
               dir_id="dir-work", parent_id="dir-season", n=3):
    res_ids = []
    for i in range(n):
        r = Resource(file_id=f"v2157-{i}", name=f"The Legend of Crazy Monk.S01E0{i+1}.mp4",
                     ext=".mp4", size=100, utime=1, identify_status="identified",
                     media_type="tv", tmdb_id=111097, media_title=media_title,
                     share_dir_name="活·佛·济·公 (2010) {tmdb-111097}",
                     share_scope="series_dir", share_dir_id=dir_id,
                     parent_id=parent_id, path_hint="影视/电视剧/活·佛·济·公 (2010)/Season 1",
                     season=1, episode=i + 1)
        db.add(r)
        db.flush()
        res_ids.append(r.id)
    wk = "work:活佛济公"
    v = SCLibraryVersion(work_key=wk, version_no=1, fingerprint="fp-x", media_type="tv",
                         resource_ids_json=json.dumps(res_ids),
                         metadata_json=json.dumps({"title": title_in_meta,
                                                   "share_dir_name": "",
                                                   "share_dir_id": dir_in_meta,
                                                   "tmdb_id": 0, "media_type": "",
                                                   "resource_count": n}),
                         change_type="created")
    db.add(v)
    db.commit()
    return wk, v.id


def test_recompute_fixes_title_and_dir_without_creating_versions():
    with SessionLocal() as db:
        wk, vid = _seed_work(db, title_in_meta="活佛济公.2010.S01E07.第7集.1080p.mp4",
                             dir_in_meta="dir-season")
        before = db.query(SCLibraryVersion).count()
        out = sc2.recompute_version_metadata(db)
        assert out["fixed"] == 1, out
        db.expire_all()
        v = db.get(SCLibraryVersion, vid)
        meta = json.loads(v.metadata_json)
        assert meta["title"] == "活佛济公", meta
        assert meta["share_dir_id"] == "dir-work", meta
        assert meta["share_dir_name"] == "活·佛·济·公 (2010) {tmdb-111097}"
        # 红线：不造新版本、不动指纹与版本号
        assert db.query(SCLibraryVersion).count() == before
        assert v.fingerprint == "fp-x" and int(v.version_no) == 1
        assert v.change_type == "created"


def test_recompute_is_idempotent():
    with SessionLocal() as db:
        _seed_work(db, title_in_meta="x.mp4", dir_in_meta="dir-season")
        assert sc2.recompute_version_metadata(db)["fixed"] == 1
        assert sc2.recompute_version_metadata(db)["fixed"] == 0


def test_recompute_dry_run_does_not_write():
    with SessionLocal() as db:
        _, vid = _seed_work(db, title_in_meta="x.mp4", dir_in_meta="dir-season")
        out = sc2.recompute_version_metadata(db, dry_run=True)
        assert out["fixed"] == 1 and out["dry_run"] is True
        db.expire_all()
        assert json.loads(db.get(SCLibraryVersion, vid).metadata_json)["title"] == "x.mp4"


def test_recompute_skips_work_without_live_rows():
    """作品资源已归档（实时无行）→ 保留原样，不猜、不抹掉历史标题。"""
    with SessionLocal() as db:
        wk, vid = _seed_work(db, title_in_meta="旧标题", dir_in_meta="d")
        for r in db.query(Resource).all():
            db.delete(r)
        db.commit()
        out = sc2.recompute_version_metadata(db)
        assert out["skipped"] >= 1
        db.expire_all()
        assert json.loads(db.get(SCLibraryVersion, vid).metadata_json)["title"] == "旧标题"


# ---------------------------------------------------------------------------
# ④ 建分享：作品目录 + 作品名 + 同目录复用
# ---------------------------------------------------------------------------
def test_share_target_replaces_season_dir_and_file_title():
    with SessionLocal() as db:
        season, work = "dir-season", "dir-work"
        rep = Resource(file_id="v2157-rep", name="The Legend of Crazy Monk.S01E07.mp4",
                       ext=".mp4", size=1, utime=1, media_type="tv", tmdb_id=111097,
                       media_title="活佛济公", share_dir_name="活·佛·济·公 (2010) {tmdb-111097}",
                       share_scope="series_dir", share_dir_id=work, parent_id=season,
                       season=1, episode=7)
        db.add(rep)
        db.commit()
        metadata = {"title": "活佛济公.2010.S01E07.第7集.1080p.mp4",
                    "share_dir_id": season, "share_dir_name": ""}
        dir_id, title = sc2._share_target(db, "work:活佛济公", metadata, rep)
        assert dir_id == work, dir_id          # 季目录被换掉
        assert title == "活佛济公", title       # 不是文件名


def test_existing_share_on_dir_only_reuses_active():
    with SessionLocal() as db:
        db.add(Resource(file_id="v2157-a", name="a.mkv", ext=".mkv", size=1, utime=1,
                        share_dir_id="dir-1", share_url="https://s/ok",
                        share_status="active"))
        db.add(Resource(file_id="v2157-b", name="b.mkv", ext=".mkv", size=1, utime=1,
                        share_dir_id="dir-2", share_url="https://s/dead",
                        share_status="failed"))
        db.commit()
        assert sc2._existing_share_on_dir(db, "dir-1") == "https://s/ok"
        assert sc2._existing_share_on_dir(db, "dir-2") == ""
        assert sc2._existing_share_on_dir(db, "") == ""


# ---------------------------------------------------------------------------
# ⑤ 推送标题：永不把文件名当作品名
# ---------------------------------------------------------------------------
def test_build_message_title_is_work_level():
    with SessionLocal() as db:
        r = Resource(file_id="v2157-msg", name="The Legend of Crazy Monk.2010.S01E07.第7集.mp4",
                     ext=".mp4", size=1024 ** 3, utime=1, media_type="tv", tmdb_id=111097,
                     media_title="", share_dir_name="活·佛·济·公 (2010) {tmdb-111097}",
                     share_dir_id="dir-work", parent_id="dir-season", season=1, episode=7,
                     identify_status="unidentified")
        db.add(r)
        db.commit()
        text, _tags = publisher._build_message(db, r, "https://share/x")
        assert "活佛济公" in text, text[:200]
        assert ".mp4" not in text.split("\n")[0], text.split("\n")[0]


# ---------------------------------------------------------------------------
# ⑥ 未识别的单集行：不按集建分享
# ---------------------------------------------------------------------------
def test_ensure_share_refuses_per_episode_share():
    with SessionLocal() as db:
        r = Resource(file_id="v2157-ep", name="某剧.S01E05.1080p.mkv", ext=".mkv",
                     size=1, utime=1, media_type="", media_title="", share_scope="file",
                     share_dir_id="dir-season", parent_id="dir-season", season=1, episode=5)
        db.add(r)
        db.commit()
        with pytest.raises(publisher.ShareNotReady):
            import asyncio
            asyncio.run(publisher.ensure_share(db, r))


def test_manual_share_is_work_level_only():
    """手动分享也只认 series_dir，且目录不能是季目录、标题不带季集。"""
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "backend/app/main.py").read_text(encoding="utf-8")
    start = src.index('@app.post("/api/resources/{rid}/share"')
    end = src.index('@app.put("/api/resources/{rid}/share"')
    seg = src[start:end]
    assert 'scope == "series_dir"' in seg
    assert "dir_id != parent_id" in seg
    assert "第{int(x.season)}季" not in seg
    assert "_work_title(db, x)" in seg
