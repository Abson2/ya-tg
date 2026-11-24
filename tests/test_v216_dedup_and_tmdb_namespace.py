"""v2.1.6 两个真机缺陷的专项回归（泰迪熊2 双推 + 银河护卫队2 串成剧集）。

bug1：泰迪熊2 被连续推送两次（resource id=23 / work:泰迪熊2 / TMDB 214756）
  2026-11-22 11:06 与 12:26 各推一次，频道+群共 4 条消息，
  而 push_queue 全表只有 2 行——同一条队列被发了两遍。
  根因是「指纹过敏 + 队列复活」两级缺陷叠加：
    ① sync_library_versions 的指纹 = sha256(file_id:size:utime)，utime 抖动
       （文件被重写/重同步，size 完全不变）就生成新指纹 → 新版本；
    ② plan_engine 的首推 dedup = work_key:fingerprint:channel_id，指纹一变
       就绕过去重 → 重建 first 任务；
    ③ submit 阶段无条件把已 sent 的队列行重置回 pending/attempts=0 →
       旧队列「复活」；而 publisher 每轮从 DB 重建的 sent_keys_by_channel
       里不再有这一行 → v0.6.5「同作品整剧去重」保险被绕过 → 第二次真实投递。
  本轮修法（三层防线，缺一不可）：
    ① 指纹去掉 utime（只留 file_id:size）——治触发源；
    ② 首推 dedup 去掉 fingerprint（work_key:channel_id:first）——首推一次即锁；
    ③ submit 幂等：同 (resource_id, channel_id) 已有 sent 记录则跳过，
       绝不重置回 pending——治本，掐断复活。

bug2：银河护卫队2 (2017) 被识别成印度剧《Indrani》（265 集）
  TMDB 的 movie / tv 是**两套完全独立的 ID 命名空间**：
      movie/283995 → 银河护卫队2 (2017)      ← 正确的
      tv/283995    → Indrani (2022, 印度剧)   ← 串台后的错值
  原实现 detail_by_id_any 是「每类型 try…except Exception: continue」，
  首选 movie 那次调用若抛任何异常（限流 / 网络超时 / HTTP 5xx）就被静默吞掉，
  自动回退到 tv/283995 命中 Indrani 并以「剧集」写死。
  修正后的语义：
    ① 404 → 该类型命名空间里没有这个 ID，允许换另一种类型试；
    ② 限流 / 网络 / 5xx → 同类型重试后**向上抛**，绝不换类型；
    ③ identify() 在 type_hint 明确（movie/tv）时**类型锁定**，只查单类型。

覆盖：
  1. 指纹忽略 utime：只改 utime 时不生成新版本；
  2. dedup 去 fingerprint：utime 抖动后重跑计划引擎不新建第二条 first 清单项；
  3. submit 幂等：已 sent 的队列行不得被重置回 pending（治本）；
  4. 幂等检查前移到建分享之前：已投递过的作品不再白建一个分享链接；
  5. publisher 的整剧去重保险不再被绕过；
  6. TMDB 命名空间隔离：瞬态失败不得换类型、404 才允许换类型、
     type_hint 明确时 identify 只查单类型；
  7. 后端新增的 skipped 状态不得在页面上直出英文枚举。
"""
import asyncio
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def env():
    """独立临时库：与全局测试库隔离，避免并发跑批时被 conftest 删库波及。"""
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v216dedup_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'v216dedup.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# 构造辅助
# ---------------------------------------------------------------------------

WORK_KEY = "work:泰迪熊2"
TMDB_ID = 214756


def _teddy_resource(db, *, file_id="teddy-file", size=100, utime=1795345465):
    """泰迪熊2 的资源行：已识别、有海报、有资料（与真机 resources id=23 一致）。"""
    from app.models import Resource
    r = Resource(
        file_id=file_id, name="Teddy.Bear.2.2017.mkv", ext=".mkv",
        size=size, utime=utime,
        identify_status="identified", media_type="movie", tmdb_id=TMDB_ID,
        media_title="泰迪熊2", original_title=" teddy Bear 2",
        path_hint="泰迪熊2 (2017) {tmdb-214756}",
        share_dir_id="teddy-dir", share_scope="series_dir",
        share_dir_name="泰迪熊2", poster_path="/poster.jpg",
        genres=json.dumps([{"id": 35, "name": "喜剧"}]),
    )
    db.add(r)
    db.commit()
    return r


def _bot_channel(db, chat_id="-1001"):
    from app.models import Bot, Channel
    from app.security import encrypt_secret
    bot = Bot(name="v216-bot", token=encrypt_secret("1:FAKE"), enabled=True)
    db.add(bot)
    db.flush()
    ch = Channel(name="v216-ch", bot_id=bot.id, chat_id=chat_id, enabled=True)
    db.add(ch)
    db.commit()
    return ch


def _publish_task(db, work_key=WORK_KEY, resource_ids=None, *, chat_id="-1001",
                  dedup_key=None, list_key="v216-list", fingerprint="v216-fp",
                  version_no=1):
    """构造一条 planned 的发布任务 + 其清单项 / 资源版本。"""
    from app.models import (SCPublishTask, SCPushList, SCPushListItem,
                            SCLibraryVersion)
    lst = db.scalar(select(SCPushList).where(SCPushList.list_key == list_key))
    if lst is None:
        lst = SCPushList(list_key=list_key, name="v216", status="active")
        db.add(lst)
        db.flush()
    lv = SCLibraryVersion(
        work_key=work_key, version_no=version_no, fingerprint=fingerprint,
        media_type="movie",
        resource_ids_json=str(resource_ids if resource_ids is not None else [1]),
        metadata_json=json.dumps({"title": "泰迪熊2", "share_dir_id": "teddy-dir"}),
        change_type="created",
    )
    db.add(lv)
    db.flush()
    item = SCPushListItem(
        list_id=lst.id, work_key=work_key, library_version_id=lv.id,
        category_key="genre:喜剧", target_type="group", bot_id=1,
        chat_id=chat_id,
        dedup_key=dedup_key or f"{work_key}:1:first",
        source_kind="first", status="planned",
    )
    db.add(item)
    db.flush()
    task = SCPublishTask(
        list_item_id=item.id, work_key=work_key, plan_version_id=1,
        kind="first", status="planned", priority=0,
        scheduled_at=datetime.now(timezone.utc),
    )
    db.add(task)
    db.commit()
    return task, item, lv


# ---------------------------------------------------------------------------
# 1. 指纹忽略 utime（治触发源）
# ---------------------------------------------------------------------------

def test_fingerprint_ignores_utime_jitter(env):
    """只改 utime（size 不变）不得生成新版本——这是泰迪熊2 双推的触发源。"""
    from app import share_center_v2 as sc2
    from app.models import SCLibraryVersion
    S, _ = env
    with S() as db:
        r = _teddy_resource(db)
        first = sc2.sync_library_versions(db)
        assert first["versions_created"] == 1, first
        lv = db.scalar(select(SCLibraryVersion))
        fp_before = lv.fingerprint

        # 真机证据：utime 1795345465 → 1795345530（+65 秒），size 完全不变
        r.utime = 1795345530
        db.commit()

        again = sc2.sync_library_versions(db)
        assert again["versions_created"] == 0, (
            "utime 抖动不得生成新版本（否则会重建 first 任务）")
        rows = db.scalars(select(SCLibraryVersion)).all()
        assert len(rows) == 1
        assert rows[0].fingerprint == fp_before, "指纹不得随 utime 变化"


def test_fingerprint_still_changes_on_real_content_change(env):
    """真实内容变化（size 变）仍必须被识别为新版本——不能把去 utime 做成漏更。"""
    from app import share_center_v2 as sc2
    from app.models import SCLibraryVersion
    S, _ = env
    with S() as db:
        r = _teddy_resource(db)
        assert sc2.sync_library_versions(db)["versions_created"] == 1
        r.size = 200
        db.commit()
        again = sc2.sync_library_versions(db)
        assert again["versions_created"] == 1, "size 变化必须生成新版本"
        assert len(db.scalars(select(SCLibraryVersion)).all()) == 2


# ---------------------------------------------------------------------------
# 2. 首推 dedup 去掉 fingerprint（首推一次即锁）
# ---------------------------------------------------------------------------

def test_plan_engine_dedup_has_no_fingerprint(env):
    """首推 dedup = work_key:channel_id:first，不得含指纹。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        _teddy_resource(db)
        sc2.ensure_defaults(db)
        ch = _bot_channel(db)
        # plan_engine 靠「作品版本 + 分类归属」产出清单项
        sc2.sync_library_versions(db)
        assert sc2.plan_engine(db)["items"] >= 1
        from app.models import SCPushListItem
        items = db.scalars(select(SCPushListItem)).all()
        assert items, "应生成首推清单项"
        for it in items:
            assert it.source_kind == "first"
            assert it.dedup_key == f"{it.work_key}:{ch.id}:first", it.dedup_key


def test_plan_engine_does_not_recreate_first_item_after_utime_jitter(env):
    """utime 抖动（新版本）后重跑计划引擎，不得新建第二条 first 清单项。"""
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask, SCPushListItem
    S, _ = env
    with S() as db:
        r = _teddy_resource(db)
        sc2.ensure_defaults(db)
        _bot_channel(db)
        sc2.sync_library_versions(db)

        first = sc2.plan_engine(db)
        assert first["items"] >= 1, first
        items_before = db.scalars(select(SCPushListItem)).all()
        tasks_before = db.scalars(select(SCPublishTask)).all()
        assert len(items_before) == 1
        assert len(tasks_before) == 1

        # utime 抖动 → 生成新版本（模拟指纹变化的极端情形，
        # 即使指纹已去 utime，这里也验证 dedup 本身独立生效）
        r.utime = 1795345530
        db.commit()
        sc2.sync_library_versions(db)
        from app.models import SCLibraryVersion
        versions = db.scalars(select(SCLibraryVersion)).all()
        assert len(versions) >= 1

        # 手工给该作品再造一个「不同指纹」的新版本，模拟老库里
        # 已存在旧式指纹行的情况——dedup 必须独立于指纹生效。
        extra = SCLibraryVersion(
            work_key=WORK_KEY, version_no=99, fingerprint="legacy-old-fp",
            media_type="movie", resource_ids_json=json.dumps([r.id]),
            metadata_json=json.dumps({"title": "泰迪熊2", "share_dir_id": "teddy-dir"}),
            change_type="updated")
        db.add(extra)
        db.commit()

        second = sc2.plan_engine(db)
        assert second["items"] == 0, (
            f"首推已存在，不得因新版本/新指纹再排一条 first：{second}")
        assert len(db.scalars(select(SCPushListItem)).all()) == 1
        assert len(db.scalars(select(SCPublishTask)).all()) == 1


def test_plan_engine_legacy_dedup_row_blocks_duplicate(env):
    """老库 first 项 dedup 是 work_key:fingerprint:channel_id，
    格式变了但语义不变——必须被历史兼容兜底查出来，不得重复首推。"""
    from app import share_center_v2 as sc2
    from app.models import SCPushList, SCPushListItem
    S, _ = env
    with S() as db:
        r = _teddy_resource(db)
        sc2.ensure_defaults(db)
        ch = _bot_channel(db)
        sc2.sync_library_versions(db)
        # 预置一条**老格式**的 first 清单项
        lst = SCPushList(list_key="legacy-list", name="legacy", status="active")
        db.add(lst)
        db.flush()
        db.add(SCPushListItem(
            list_id=lst.id, work_key=WORK_KEY, library_version_id=1,
            category_key="genre:喜剧", target_type="group", bot_id=1,
            chat_id="-1001", dedup_key=f"{WORK_KEY}:old-fp:{ch.id}",
            source_kind="first", status="published"))
        db.commit()

        out = sc2.plan_engine(db)
        assert out["items"] == 0, f"老格式 first 项必须被兼容兜底拦住：{out}"
        rows = db.scalars(select(SCPushListItem)).all()
        assert len(rows) == 1


# ---------------------------------------------------------------------------
# 3. submit 幂等（治本：掐断队列复活）
# ---------------------------------------------------------------------------

def test_dispatch_skips_when_queue_already_sent(env, monkeypatch):
    """同 (resource_id, channel_id) 已有 sent 队列行 →
    任务走 skipped、队列行**不得**被重置回 pending。"""
    from app import share_center_v2 as sc2
    from app.models import QueueItem, SCPublishExecution, SCPublishTask
    S, _ = env

    built = {"n": 0}

    async def fake_create(client, file_id, title, duration):
        built["n"] += 1
        return "sid", "https://www.guangyapan.com/s/x"

    monkeypatch.setattr(sc2, "create_share_parsed", fake_create)

    with S() as db:
        r = _teddy_resource(db)
        ch = _bot_channel(db)
        task, _, _ = _publish_task(db, resource_ids=[r.id])
        # 预置一条**已投递**的队列行（真机：11:06 已推过一次）
        q = QueueItem(resource_id=r.id, channel_id=ch.id, status="sent",
                      attempts=2, push_reason="sc_v2")
        db.add(q)
        db.commit()

        stats = asyncio.run(sc2.dispatch_ready_tasks(db, limit=20))

        assert stats["skipped"] == 1, stats
        assert stats["submitted"] == 0, stats

        db.expire_all()
        t = db.get(SCPublishTask, task.id)
        assert t.status == "skipped", t.status
        assert "已推送过" in (t.error or ""), t.error

        row = db.scalar(select(QueueItem).where(QueueItem.resource_id == r.id))
        assert row is not None
        assert row.status == "sent", "已投递的队列行绝不能被重置回 pending"
        assert row.attempts == 2, "attempts 不得被清零"

        ex = db.scalars(select(SCPublishExecution).where(
            SCPublishExecution.task_id == task.id,
            SCPublishExecution.stage == "submit")).all()
        assert len(ex) == 1, [e.status for e in ex]
        assert ex[0].status == "skipped", ex[0].status


def test_dispatch_does_not_build_share_for_already_sent(env, monkeypatch):
    """幂等检查必须**前移到建分享之前**——已投递过的作品不再白建分享链接。"""
    from app import share_center_v2 as sc2
    from app.models import QueueItem
    S, _ = env

    built = {"n": 0}

    async def fake_create(client, file_id, title, duration):
        built["n"] += 1
        return "sid", "https://www.guangyapan.com/s/x"

    monkeypatch.setattr(sc2, "create_share_parsed", fake_create)

    with S() as db:
        r = _teddy_resource(db)
        ch = _bot_channel(db)
        _publish_task(db, resource_ids=[r.id])
        db.add(QueueItem(resource_id=r.id, channel_id=ch.id, status="sent"))
        db.commit()

        stats = asyncio.run(sc2.dispatch_ready_tasks(db, limit=20))
        assert stats["skipped"] == 1, stats
        assert built["n"] == 0, "已投递过的资源不得再建分享（会在网盘多出分享记录）"


def test_dispatch_new_resource_is_submitted_normally(env, monkeypatch):
    """回归：未投递过的资源照常建分享并入队。"""
    from app import share_center_v2 as sc2
    from app.models import QueueItem, SCPublishTask
    S, _ = env

    async def fake_create(client, file_id, title, duration):
        return "sid", "https://www.guangyapan.com/s/ok"

    monkeypatch.setattr(sc2, "create_share_parsed", fake_create)

    with S() as db:
        r = _teddy_resource(db)
        _bot_channel(db)
        task, _, _ = _publish_task(db, resource_ids=[r.id])

        stats = asyncio.run(sc2.dispatch_ready_tasks(db, limit=20))
        assert stats["skipped"] == 0, stats
        assert stats["submitted"] == 1, stats

        db.expire_all()
        t = db.get(SCPublishTask, task.id)
        assert t.status == "submitted", t.status
        q = db.scalar(select(QueueItem))
        assert q is not None and q.push_reason == "sc_v2"
        assert q.status == "pending"


# ---------------------------------------------------------------------------
# 4. publisher 的整剧去重保险不再被绕过
# ---------------------------------------------------------------------------

def test_publisher_dedup_survives_second_dispatch(env, monkeypatch):
    """回归：同作品第二次进 dispatch 时不得产生第二条可投递队列项。

    真实链路：publisher 每轮从 DB 重建 sent_keys_by_channel（status=="sent"）。
    只要「已 sent 的行不被复活」，这层保险就依然拦得住同作品的其它单集。
    """
    from app import share_center_v2 as sc2
    from app.models import QueueItem
    S, _ = env

    async def fake_create(client, file_id, title, duration):
        return "sid", "https://www.guangyapan.com/s/ok"

    monkeypatch.setattr(sc2, "create_share_parsed", fake_create)
    # v2.1.19：固定在本小时第 59 分，让匀速额度放满（cap=3）。
    # 本用例验证的是**去重**（第二次进 dispatch 应 skipped），不该被分钟级节流
    # 干扰：整点刚过时额度只放 1 部，dispatch 会先 throttled 退出，
    # 根本走不到去重判定，stats["skipped"] 就恒为 0（假红）。
    _frozen = sc2.utcnow().replace(minute=59, second=0, microsecond=0)
    monkeypatch.setattr(sc2, "utcnow", lambda: _frozen)

    with S() as db:
        r = _teddy_resource(db)
        ch = _bot_channel(db)
        _publish_task(db, resource_ids=[r.id])
        # 第一轮：入队
        asyncio.run(sc2.dispatch_ready_tasks(db, limit=20))
        q = db.scalar(select(QueueItem))
        assert q is not None and q.status == "pending"

        # publisher 发完 → 队列行变 sent
        q.status = "sent"
        db.commit()

        # 第二轮：又来一个 first 任务（模拟 utime 抖动重建任务）。
        # 注意 dedup_key 唯一约束本身也是一道防线——这里给一个**不同**的
        # dedup_key，模拟「绕开唯一约束也要拦住」的场景（历史库里
        # 老格式 dedup 与新格式并存的情形）。
        _publish_task(db, resource_ids=[r.id], list_key="v216-list-2",
                      fingerprint="v216-fp-2", version_no=2,
                      dedup_key=f"{WORK_KEY}:v216-fp-2:1:first")
        stats = asyncio.run(sc2.dispatch_ready_tasks(db, limit=20))
        assert stats["skipped"] == 1, stats

        rows = db.scalars(select(QueueItem)).all()
        assert len(rows) == 1, "不得为同一 (资源, 渠道) 产生第二条队列行"
        assert rows[0].status == "sent", "已投递的行不得被复活"
        # publisher 的 sent_keys_by_channel 依据：status=="sent" 的行仍在
        sent_rows = db.scalars(select(QueueItem).where(
            QueueItem.status == "sent")).all()
        assert len(sent_rows) == 1, "publisher 的整剧去重依据必须仍然存在"


def test_publisher_dedup_guard_is_still_present():
    """静态守卫：publisher 的「同作品整剧去重」保险不得被删掉。"""
    src = (ROOT / "backend/app/publisher.py").read_text(encoding="utf-8")
    assert "sent_keys_by_channel" in src
    assert "整剧推送" in src, "publisher 的整剧去重保险必须保留"
    assert 'QueueItem.status == "sent"' in src


# ---------------------------------------------------------------------------
# 5. TMDB 命名空间隔离（bug2）
# ---------------------------------------------------------------------------

def _item(title, mt, tmdb_id, year="", genres=None, countries=None):
    return {
        "tmdb_id": tmdb_id, "media_type": mt, "type": mt,
        "type_label": "电影" if mt == "movie" else "剧集",
        "title": title, "original_title": title,
        "year": year, "poster_path": "", "backdrop_path": "", "overview": "简介",
        "vote_average": 7.2, "popularity": 90,
        "genres": genres or ["剧情"], "origin_country": countries or ["US"], "score": 0.0,
    }


def test_detail_by_id_any_does_not_fallback_to_tv_on_transient_error():
    """movie 调用抛瞬态错误（限流 / 网络 / 5xx）时**不得**回退到 tv。

    这是「银河护卫队2 → Indrani」的根因：movie 首调被静默吞掉后回退 tv。
    """
    from app import tmdb

    calls = []

    async def fake_detail(tmdb_id, media_type, db=None):
        calls.append(media_type)
        if media_type == "movie":
            raise RuntimeError("TMDB 请求失败 HTTP 503")
        return _item("Indrani", "tv", tmdb_id, "2022")

    with patch.object(tmdb, "detail", fake_detail):
        with pytest.raises(RuntimeError):
            asyncio.run(tmdb.detail_by_id_any(283995, preferred="movie", db=None))

    assert calls, "movie 必须被调用"
    assert "tv" not in calls, (
        f"瞬态失败时绝不允许换类型命名空间（会把电影串成剧集）：{calls}")
    assert calls.count("movie") >= 1


def test_detail_by_id_any_does_not_fallback_on_rate_limit():
    """限流是瞬态失败，但换类型只会再撞一次限流——必须直接上抛。"""
    from app import tmdb

    calls = []

    async def fake_detail(tmdb_id, media_type, db=None):
        calls.append(media_type)
        raise tmdb.TmdbRateLimited("限流")

    with patch.object(tmdb, "detail", fake_detail):
        with pytest.raises(tmdb.TmdbRateLimited):
            asyncio.run(tmdb.detail_by_id_any(283995, preferred="movie", db=None))
    assert "tv" not in calls, calls


def test_detail_by_id_any_may_fallback_to_tv_on_404():
    """404 = 该类型命名空间里确实没有这个 ID → 允许换类型继续试。"""
    from app import tmdb

    calls = []

    async def fake_detail(tmdb_id, media_type, db=None):
        calls.append(media_type)
        if media_type == "movie":
            raise tmdb.TmdbNotFound("TMDB 无此资源（HTTP 404）：/movie/283995")
        return _item("Indrani", "tv", tmdb_id, "2022")

    with patch.object(tmdb, "detail", fake_detail):
        data, actual = asyncio.run(
            tmdb.detail_by_id_any(283995, preferred="movie", db=None))
    assert actual == "tv", actual
    assert data["title"] == "Indrani"
    assert calls == ["movie", "tv"], calls


def test_get_raises_not_found_only_on_404():
    """_get 必须把 404 分流成 TmdbNotFound，其它 4xx/5xx 走普通异常。"""
    import httpx
    from app import tmdb

    class FakeResp:
        def __init__(self, code):
            self.status_code = code
            self.text = "err"

        def json(self):
            return {}

    async def fake_get(self, url, **kwargs):
        return FakeResp(404)

    with patch.object(httpx.AsyncClient, "get", fake_get):
        with patch.object(tmdb, "credentials",
                          lambda db=None: {"bearer": "x", "key": "", "language": "zh-CN"}):
            with pytest.raises(tmdb.TmdbNotFound):
                asyncio.run(tmdb._get("/movie/283995", {}, None))

    async def fake_get_500(self, url, **kwargs):
        return FakeResp(500)

    with patch.object(httpx.AsyncClient, "get", fake_get_500):
        with patch.object(tmdb, "credentials",
                          lambda db=None: {"bearer": "x", "key": "", "language": "zh-CN"}):
            with pytest.raises(RuntimeError) as exc:
                asyncio.run(tmdb._get("/movie/283995", {}, None))
    assert not isinstance(exc.value, tmdb.TmdbNotFound)


def test_identify_type_locked_to_movie():
    """type_hint=movie 时只查 movie 命名空间 → 银河护卫队2，不会变成 Indrani。"""
    from app import tmdb

    calls = []

    async def fake_detail(tmdb_id, media_type, db=None):
        calls.append((tmdb_id, media_type))
        if media_type == "movie":
            return _item("银河护卫队2", "movie", tmdb_id, "2017",
                         genres=["动作", "冒险"], countries=["US"])
        return _item("Indrani", "tv", tmdb_id, "2022")

    with patch.object(tmdb, "detail", fake_detail):
        r = asyncio.run(tmdb.identify(
            "银河护卫队2 (2017) {tmdb-283995}.mkv",
            None, path_hint="/影视/电影/欧美电影/银河护卫队2 (2017) {tmdb-283995}",
            use_ai=False))

    assert r["status"] == "identified", r
    assert r["best"]["media_type"] == "movie", r["best"]
    assert r["best"]["title"] == "银河护卫队2", r["best"]
    assert all(mt == "movie" for _tid, mt in calls), calls


def test_identify_type_locked_movie_404_does_not_return_tv():
    """type_hint=movie 且 movie 404 → 不得返回 tv 的 Indrani，
    应落到名称搜索路径（找不到就 failed）。"""
    from app import tmdb

    calls = []

    async def fake_detail(tmdb_id, media_type, db=None):
        calls.append(media_type)
        raise tmdb.TmdbNotFound("TMDB 无此资源（HTTP 404）")

    async def fake_search(title, media_type=None, db=None, scenario=None):
        calls.append(f"search:{media_type}")
        return []

    with patch.object(tmdb, "detail", fake_detail):
        with patch.object(tmdb, "search", fake_search):
            r = asyncio.run(tmdb.identify(
                "银河护卫队2 (2017) {tmdb-283995}.mkv",
                None, path_hint="/影视/电影/欧美电影/银河护卫队2 (2017) {tmdb-283995}",
                use_ai=False))

    assert r["status"] != "identified", r
    assert not r.get("best"), r
    assert "tv" not in calls, f"类型锁定后不得查询 tv 命名空间：{calls}"


def test_identify_unknown_type_still_may_cross_namespace():
    """类型未知（老标记不带类型）时仍允许跨类型尝试——不破坏既有能力。"""
    from app import tmdb

    calls = []

    async def fake_detail(tmdb_id, media_type, db=None):
        calls.append(media_type)
        if media_type == "movie":
            raise tmdb.TmdbNotFound("404")
        return _item("Indrani", "tv", tmdb_id, "2022")

    with patch.object(tmdb, "detail", fake_detail):
        r = asyncio.run(tmdb.identify("某剧 (2022) {tmdb-283995}.mkv",
                                      None, use_ai=False))
    assert r["status"] == "identified", r
    assert calls == ["movie", "tv"], calls


# ---------------------------------------------------------------------------
# 6. 页面不得直出后端枚举（skipped）
# ---------------------------------------------------------------------------

def test_frontend_maps_skipped_status_to_chinese():
    """后端新增 skipped 状态 → 前端必须有中文映射，不得直出英文枚举。

    v2.1.11：措辞按用户口径收敛——「跳过」在页面里原本有 5 种含义，
    全部叫「已跳过」会让用户以为是同一件事。现在区分成：
      忽略翻译 / 改名跳过 / 跳过本序列 / 已跳过投递
    """
    src = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "skipped: '已跳过投递'" in src, "任务状态缺少 skipped 的中文映射"
    # 执行记录表的阶段 / 状态同样要中文化
    assert "scExecStageLabel" in src and "scExecStatusLabel" in src
    assert "scExecStageLabel(s.row.stage)" in src
    assert "scExecStatusLabel(s.row.status)" in src
    # 筛选下拉的顺序数组必须包含 skipped
    assert "'skipped'" in src


def test_backend_skipped_status_is_not_counted_as_unfinished():
    """skipped 是终态（已跳过），不得计入「未完成」口径。"""
    from app import share_center_v2 as sc2
    src = (ROOT / "backend/app/share_center_v2.py").read_text(encoding="utf-8")
    unfinished_line = [ln for ln in src.splitlines()
                       if ln.strip().startswith("unfinished = (")]
    assert unfinished_line, "未找到 unfinished 口径定义"
    line = unfinished_line[0]
    assert "skipped" not in line, f"skipped 是终态，不得计入未完成：{line}"
    # 统计字典必须含 skipped 键，前端 KPI 才对得上
    assert '"skipped": 0' in src
