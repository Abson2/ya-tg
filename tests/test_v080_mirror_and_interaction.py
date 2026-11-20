"""v0.8.0 回归：A/B 镜像、扫描批次、手动完结打标、内容变化、用户互动。

锁死以下红线（最终版需求文档）：
  - B 库带 * 目录名与 A 库标准名归一到同一作品键。
  - 身份不明确禁止自动替换；旧 B 文件只标 superseded，不删除。
  - 镜像完成绝不写 push_queue。
  - 手动修正的完结/追更锁定，TMDB 自动回填与重新识别均不得覆盖。
  - 只有批次表标记 reconcile_allowed 才允许对账删除。
  - 同批文件变化合并为一次作品级更新事件，复用原分享。
  - TgtoDrive 关键词只增/改本项目规则，绝不覆盖原有规则。
"""
import asyncio
from unittest.mock import patch

import pytest
from sqlalchemy import select

from app import bot_listener, mirror, tgto
from app.db import SessionLocal, json_dumps, migrate
from app.main import (BatchLibraryInput, DistributionEvent, MirrorFile, MirrorMedia,
                      MirrorRule, MirrorTask, QueueItem, Resource, ResourceScanBatch,
                      _apply_identification, _cron_to_interval, _interval_to_cron,
                      _post_scan_identify, _reconcile_library, batch_library,
                      process_change_events)
from app.models import (Bot, Channel, MediaRequest, UserContribution,
                        ResourceVersion, MirrorDir)
from app.scanner import _record_change_events
from app.utils import (NAME_MARK, media_identity, normalize_work_name, star_media_name,
                        work_group_key)

TABLES = [QueueItem, ResourceVersion, DistributionEvent, MirrorFile, MirrorTask,
          MirrorDir, MirrorMedia, MirrorRule, ResourceScanBatch, UserContribution, MediaRequest,
          Resource, Channel, Bot]


def _clean():
    with SessionLocal() as db:
        for t in TABLES:
            db.query(t).delete()
        db.commit()


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    _clean()
    # v0.8.7：类变量跨用例累加会污染「B 库是否为空」的判定，每个用例重置
    FakeMirrorAPI.copies = []
    FakeMirrorAPI.b_files = []
    FakeMirrorAPI.fp = 1
    FakeMirrorAPI.task_status = 2
    yield
    _clean()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as session:
        try:
            yield session
        finally:
            session.rollback()


def _res(fid, name, **kw):
    base = dict(file_id=fid, name=name, ext=".mkv", size=1, utime=1)
    base.update(kw)
    return Resource(**base)


# ---------- 1. B 库带 · 命名与身份保护（v0.8.3 起，* 已被网盘拒绝） ----------
def test_star_naming_maps_to_same_work_key():
    a = "侠客行 (2001) {tmdb-135646}"
    b = "侠*客*行 *(2001) {tmdb-135646}"
    assert normalize_work_name(a) == normalize_work_name(b) == "侠客行"
    assert NAME_MARK in star_media_name(a) and "*" not in star_media_name(a)
    x = _res("f1", "X.S01E01.mkv", path_hint="影视/" + a + "/Season 1")
    y = _res("f2", "X.S01E02.mkv", path_hint="影视/" + b + "/Season 1")
    assert work_group_key(x) == work_group_key(y)


def test_media_identity_confidence():
    confident, ok = media_identity(135646, "tv", 1, 4, "X.S01E04.mkv")
    assert ok and confident == "tmdb:135646:tv:S1:E4"
    weak, ok2 = media_identity(135646, "", None, None, "X.2001.mkv")
    assert not ok2 and weak.startswith("tmdb:")
    name_only, ok3 = media_identity(None, "", None, None, "Some.Movie.2001.mkv")
    assert not ok3 and name_only.startswith("name:")


def test_scan_interval_presets_roundtrip():
    for m in (10, 15, 30, 60, 120, 360, 720, 1440):
        assert _cron_to_interval(_interval_to_cron(m)) == m
    assert _cron_to_interval("0 3 * * *") == 0  # 自定义表达式不当作预设
    with pytest.raises(ValueError):
        _interval_to_cron(77)


def test_identify_does_not_change_work_key():
    """识别后 series_dir_info 兜底可能写入「Season 1」，作品键必须保持不变。"""
    x = _res("v080-key", "剧.S01E01.mkv", path_hint="影视/剧/Season 1")
    before = work_group_key(x)
    _apply_identification(x, {"tmdb_id": 135646, "media_type": "tv", "title": "剧",
                              "finish_status": "ended"}, source="manual")
    assert work_group_key(x) == before


# ---------- 2. 手动完结打标与锁定 ----------
def test_manual_finish_marks_whole_work_and_blocks_auto_overwrite(db):
    for i in (1, 2):
        db.add(_res(f"v080-e{i}", f"剧.S01E0{i}.mkv", media_type="tv", tmdb_id=135646,
                    identify_status="identified", poster_path="/p.jpg", media_title="剧",
                    path_hint="影视/剧/Season 1", finish_status="returning"))
    db.commit()
    rep = db.scalar(select(Resource).where(Resource.file_id == "v080-e1"))
    out = asyncio.run(batch_library(BatchLibraryInput(
        ids=[rep.id], action="set_finish", finish="ended"), db=db))
    assert out["succeeded"] == 1
    rows = list(db.scalars(select(Resource)).all())
    assert all(r.finish_status == "ended" and r.finish_locked for r in rows)

    async def fake_detail(tid, mt, db=None):
        return {"finish_status": "returning"}

    async def fake_identify(*a, **k):
        return {"ok": False, "status": "none", "best": None, "source": "", "reason": "", "hint": {}}

    # ① 扫描后自动回填不得覆盖人工判断
    with patch("app.main.tmdb.detail", fake_detail), \
         patch("app.main.tmdb.identify", fake_identify):
        asyncio.run(_post_scan_identify(db, publish_after_identify=False))
    assert all(r.finish_status == "ended" for r in db.scalars(select(Resource)).all())

    # ② 手动重新识别也不得覆盖
    _apply_identification(rep, {"tmdb_id": 135646, "media_type": "tv",
                                "finish_status": "returning", "title": "剧"}, source="manual")
    assert rep.finish_status == "ended" and rep.finish_locked

    # ③ 只有显式「恢复自动」才解锁
    asyncio.run(batch_library(BatchLibraryInput(
        ids=[rep.id], action="set_finish", finish="auto"), db=db))
    assert all(not r.finish_locked and r.finish_status == "" for r in
               db.scalars(select(Resource)).all())


def test_finish_check_compares_tmdb_and_local(db):
    """TMDB 标完结但本地缺集 → 给出建议，且不自动改状态。"""
    from app.main import finish_check
    for i in (1, 2):
        db.add(_res(f"v080-fc{i}", f"剧.S01E0{i}.mkv", media_type="tv", tmdb_id=135646,
                    identify_status="identified", poster_path="/p.jpg", media_title="剧",
                    path_hint="影视/剧/Season 1", finish_status="ended"))
    db.commit()

    async def fake_detail(tid, mt, db=None):
        return {"finish_status": "ended", "number_of_episodes": 36,
                "number_of_seasons": 1, "tmdb_status": "Ended"}

    with patch("app.main.tmdb.detail", fake_detail):
        out = asyncio.run(finish_check(limit=10, db=db))
    assert out["checked"] == 1
    item = out["items"][0]
    assert item["expected_episodes"] == 36 and item["local_episodes"] == 2
    assert item["verdict"] == "missing" and "手动标追更" in item["suggestion"]
    assert all(r.finish_status == "ended" for r in db.scalars(select(Resource)).all())


# ---------- 3. 批次表是对账的唯一权威 ----------
def test_reconcile_requires_batch_authorization(db):
    db.add(_res("v080-keep", "Keep.mkv"))
    db.add(_res("v080-gone", "Gone.mkv"))
    db.commit()
    b = ResourceScanBatch(status="success", complete=True, resumed=False,
                          reconcile_allowed=False, seen_count=1,
                          seen_json=json_dumps(["v080-keep"]))
    db.add(b)
    db.commit()
    r = asyncio.run(_reconcile_library(db, ["v080-keep"], scan_complete=True,
                                       resumed=False, batch_id=b.id))
    assert r["skipped"] == "batch_not_allowed"
    assert db.scalar(select(Resource).where(Resource.file_id == "v080-gone")) is not None
    b.reconcile_allowed = True
    db.commit()
    r = asyncio.run(_reconcile_library(db, ["v080-keep"], scan_complete=True,
                                       resumed=False, batch_id=b.id))
    assert r["removed"] == 1
    db.commit()  # autoflush=False，删除需提交后才对查询可见
    assert db.scalar(select(Resource).where(Resource.file_id == "v080-gone")) is None


# ---------- 4. 内容变化：合并一次事件 + 复用分享 + 不重建 ----------
def test_change_events_merge_and_reuse_share(db):
    db.add(Bot(name="b", token="t"))
    db.flush()
    bid = db.scalar(select(Bot.id))
    ch = Channel(bot_id=bid, name="c", chat_id="1")
    db.add(ch)
    db.flush()
    cid = ch.id
    rep = _res("v080-ch1", "剧.S01E01.mkv", media_type="tv", tmdb_id=1,
               identify_status="identified", poster_path="/p.jpg", media_title="剧",
               path_hint="影视/剧/Season 1", share_url="https://s/1",
               share_id="sid-1", share_status="active")
    db.add(rep)
    db.flush()
    q = QueueItem(resource_id=rep.id, channel_id=cid, status="sent", attempts=1)
    db.add(q)
    db.commit()
    changes = [
        {"resource_id": rep.id, "file_id": rep.file_id, "work_key": work_group_key(rep),
         "old": "1:1", "new": "2:2", "size": 2, "utime": 2},
        {"resource_id": rep.id, "file_id": rep.file_id + "-b", "work_key": work_group_key(rep),
         "old": "1:1", "new": "3:3", "size": 3, "utime": 3},
    ]
    assert _record_change_events(db, changes) == 1
    assert _record_change_events(db, changes) == 0  # 同一批指纹幂等
    db.commit()
    assert db.query(DistributionEvent).count() == 1
    stats = process_change_events(db, publish=True)
    assert stats["queued"] == 1 and stats["share_reused"] == 1, stats
    db.commit()
    q2 = db.get(QueueItem, q.id)
    assert q2.status == "pending" and q2.attempts == 0
    assert rep.share_url == "https://s/1" and rep.share_id == "sid-1"  # 未重建分享
    ev = db.scalar(select(DistributionEvent))
    assert ev.queue_id == q.id and ev.share_reused
    assert process_change_events(db, publish=False)["events"] == 0


# ---------- 5. A/B 镜像增量链路 ----------
A_ROOT, B_ROOT, A_WORK, B_WORK = "A_ROOT", "B_ROOT", "A_W1", "B_W1"


class FakeMirrorAPI:
    """可控的 A/B 目录树；记录复制请求，验证 status=2 之外的状态不猜含义。"""

    fp = 1
    copies = []
    task_status = 2
    # v0.8.7：模拟「复制任务落地后 B 库作品目录里真的有文件」。旧假 API 永远返回
    # 空目录，v0.8.7 起 sync_rule 会据此判定文件缺失并重新复制，故必须补齐。
    b_files = []

    def __init__(self, db):
        self.db = db

    async def list_files(self, parent_id, page=0, page_size=100):
        if page > 0:
            return {"list": [], "total": 0}
        if parent_id == A_ROOT:
            return {"list": [{"fileId": A_WORK, "resType": 2,
                              "fileName": "侠客行 (2001) {tmdb-135646}"}], "total": 1}
        if parent_id == A_WORK:
            return {"list": [{"fileId": "F1", "resType": 1, "fileName": "X.S01E01.mkv",
                              "fileSize": 100, "utime": self.fp, "parentId": A_WORK}], "total": 1}
        if parent_id == B_ROOT:
            return {"list": [{"fileId": B_WORK, "resType": 2,
                              "fileName": "侠*客*行 *(2001) {tmdb-135646}"}], "total": 1}
        if parent_id == B_WORK:
            return {"list": [dict(f) for f in self.b_files], "total": len(self.b_files)}
        return {"list": [], "total": 0}

    async def copy_file(self, file_ids, parent_id):
        self.copies.append((list(file_ids), parent_id))
        self.b_files.append({"fileId": f"BF{len(self.b_files) + 1}", "resType": 1,
                             "fileName": "X.S01E01.mkv", "fileSize": 100,
                             "utime": self.fp, "parentId": parent_id})
        return "TASK1"

    async def get_task_status(self, task_id):
        return {"status": self.task_status, "raw": {"status": self.task_status}}


def _mirror_rule(db):
    r = MirrorRule(name="测试镜像", source_root_id=A_ROOT, target_root_id=B_ROOT,
                   interval_minutes=60, enabled=True)
    db.add(r)
    db.commit()
    return r


def test_mirror_incremental_and_never_pushes_tg(db):
    r = _mirror_rule(db)
    with patch("app.mirror.GuangYaWebFiles", FakeMirrorAPI):
        first = asyncio.run(mirror.sync_rule(db, r))
        assert first["new"] == 1 and first["done"] == 1, first
        # 只锁「复制函数本身不写队列」；v0.8.5 起上层镜像后管线会按分发规则
        # 推送本轮识别成功且有海报的影视，那是 main.py 的职责，不在这里断言
        assert db.query(QueueItem).count() == 0
        media = db.scalar(select(MirrorMedia))
        assert media.title_standard == "侠客行 (2001) {tmdb-135646}"
        assert NAME_MARK in media.title_mirror and "*" not in media.title_mirror
        assert media.target_dir_id == B_WORK

        second = asyncio.run(mirror.sync_rule(db, r))
        assert second["new"] == 0 and second["unchanged"] == 1, second  # 无变化不重复复制
        assert len(FakeMirrorAPI.copies) == 1

        FakeMirrorAPI.fp = 9  # A 文件内容变化
        third = asyncio.run(mirror.sync_rule(db, r))
        assert third["new"] == 1 and third["changed"] == 1, third
        rows = list(db.scalars(select(MirrorFile)).all())
        assert {x.status for x in rows} == {"superseded", "synced"}  # 旧文件不删除
        assert all(x.identity.startswith("tmdb:135646:tv:") for x in rows)
        assert db.query(QueueItem).count() == 0


def test_mirror_unknown_task_status_is_not_guessed(db):
    r = _mirror_rule(db)
    FakeMirrorAPI.fp = 1
    FakeMirrorAPI.task_status = 99  # 文档只确认 status=2，其余不得判成功/失败
    with patch("app.mirror.GuangYaWebFiles", FakeMirrorAPI):
        out = asyncio.run(mirror.sync_rule(db, r))
    assert out["unknown"] == 1 and out["done"] == 0, out
    t = db.scalar(select(MirrorTask))
    assert t.status == "unknown" and t.raw_status == "99"
    FakeMirrorAPI.task_status = 2
    with patch("app.mirror.GuangYaWebFiles", FakeMirrorAPI):
        polled = asyncio.run(mirror.poll_pending_tasks(db, r))
    assert polled["done"] == 1
    assert db.scalar(select(MirrorFile)).status == "synced"


def test_mirror_missing_target_dir_is_skipped_when_auto_create_off(db):
    """v0.8.1：关闭「自动创建 B 库目录」后，缺目录只登记清单不猜测、不误复制。"""
    r = _mirror_rule(db)
    r.auto_create_dirs = False
    db.commit()

    class NoB(FakeMirrorAPI):
        async def list_files(self, parent_id, page=0, page_size=100):
            if parent_id == B_ROOT:
                return {"list": [], "total": 0}
            return await super().list_files(parent_id, page, page_size)

    with patch("app.mirror.GuangYaWebFiles", NoB):
        out = asyncio.run(mirror.sync_rule(db, r))
    assert out["skipped"] == 1 and out["new"] == 0, out
    assert out["missing_dirs"] and out["missing_dirs"][0]["kind"] == "work", out
    assert db.scalar(select(MirrorMedia)).status == "blocked"


def test_web_copy_endpoints_match_confirmed_contract(db):
    """文档第八章已确认的两个接口：路径与请求体必须与原文一致，不得推测。"""
    from app.guangya import GuangYaWebFiles
    from app.models import AuthToken
    from app.security import encrypt_secret

    old = db.get(AuthToken, 1)
    if old:
        db.delete(old)
        db.flush()
    db.add(AuthToken(id=1, flow="sms", access_token_enc=encrypt_secret("tok"),
                     device_id="did-1"))
    db.commit()
    calls = []

    class _Resp:
        status_code = 200

        def __init__(self, data):
            self.data = data

        def json(self):
            return {"msg": "success", "data": self.data}

    class _Client:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None, headers=None):
            calls.append((url, json))
            if url.endswith("copy_file"):
                return _Resp({"taskId": "T1"})
            return _Resp({"status": 2})

    with patch("app.guangya.httpx.AsyncClient", _Client):
        api = GuangYaWebFiles(db)
        task_id = asyncio.run(api.copy_file(["1941911362609643616"], "1969396537043607645"))
        state = asyncio.run(api.get_task_status(task_id))
    assert task_id == "T1" and state["status"] == 2
    assert calls[0][0].endswith("/userres/v1/file/copy_file")
    assert calls[0][1] == {"fileIds": ["1941911362609643616"],
                           "parentId": "1969396537043607645"}
    assert calls[1][0].endswith("/userres/v1/get_task_status")
    assert calls[1][1] == {"taskId": "T1"}


# ---------- 6. TgtoDrive 关键词：保留原规则、不覆盖他人 ----------
def test_tgto_keyword_merge_preserves_existing_rules(db):
    existing = {"id": "manual_rule", "keywords": ["请勿覆盖"], "source": "admin"}
    store = {"scene": "ENV_FILTER_GUANGYA",
             "current": {"key": "ENV_FILTER_GUANGYA", "rules": [existing]},
             "scenes": {"ENV_FILTER_GUANGYA": {"key": "ENV_FILTER_GUANGYA", "rules": [existing]}}}

    async def fake_fetch(db_):
        return store, "ENV_FILTER_GUANGYA"

    writes = []

    async def fake_save(db_, data, scene, obj):
        writes.append((scene, [r["id"] for r in obj["rules"]]))

    with patch("app.tgto.fetch_config", fake_fetch), \
         patch("app.tgto.save_config", fake_save):
        out = asyncio.run(tgto.upsert_keyword(db, title="新剧", tmdb_id=135646, season=1))
    assert out["ok"] and out["status"] == "created"
    assert out["rules_before"] == 1 and out["rules_after"] == 2
    assert writes and writes[0][1] == ["manual_rule", out["rule_id"]]  # 原规则仍在
    # 同 id 再次提交只更新，不新增
    with patch("app.tgto.fetch_config", fake_fetch), \
         patch("app.tgto.save_config", fake_save):
        again = asyncio.run(tgto.upsert_keyword(db, title="新剧 改名", tmdb_id=135646, season=1))
    assert again["status"] == "updated"


def test_tgto_conflict_does_not_overwrite_foreign_rule(db):
    foreign = {"id": "kw_135646_1", "keywords": ["别人的规则"], "source": "other-project"}
    store = {"current": {"key": "ENV_FILTER_GUANGYA", "rules": [foreign]},
             "scenes": {"ENV_FILTER_GUANGYA": {"key": "ENV_FILTER_GUANGYA", "rules": [foreign]}}}

    async def fake_fetch(db_):
        return store, "ENV_FILTER_GUANGYA"

    writes = []

    async def fake_save(db_, data, scene, obj):
        writes.append(scene)

    with patch("app.tgto.fetch_config", fake_fetch), \
         patch("app.tgto.save_config", fake_save):
        out = asyncio.run(tgto.upsert_keyword(db, title="剧", tmdb_id=135646, season=1))
    assert out["status"] == "conflict" and not writes  # 一次都没写回
    assert store["current"]["rules"][0]["keywords"] == ["别人的规则"]


# ---------- 7. Bot 消息：投稿只登记、想看走关键词 ----------
def test_bot_listener_want_and_contribute(db):
    async def fake_keyword(db_, **kw):
        return {"ok": True, "status": "created", "rule_id": "kw_135646_1"}

    with patch("app.bot_listener.tgto.upsert_keyword", fake_keyword):
        act, reply = asyncio.run(bot_listener.handle_text(
            db, "/want 伟大的长征 297253 1", tg_user_id="42", tg_username="u"))
        # v0.9.8：用户回复只报「已登记」，不暴露 TgtoDrive 同步细节（管理员侧信息）
        assert act == "want" and "已登记" in reply and "TgtoDrive" not in reply
        act2, reply2 = asyncio.run(bot_listener.handle_text(
            db, "/contribute 某剧 https://www.guangyapan.com/s/x", tg_user_id="42"))
        assert act2 == "contribute" and "登记" in reply2
    db.commit()
    req = db.scalar(select(MediaRequest))
    # v0.9.13 #254：提交只登记待审核，白名单写入后移到管理员审核通过时执行
    assert req.status == "pending" and "审核" in req.note
    assert req.title == "伟大的长征" and req.tmdb_id == 297253
    assert req.season == 1 and req.keyword_id == "kw_297253_1"
    c = db.scalar(select(UserContribution))
    assert c.status == "pending" and "待验证" in c.note  # 转存接口未验证，不自动入库
    assert db.query(QueueItem).count() == 0
