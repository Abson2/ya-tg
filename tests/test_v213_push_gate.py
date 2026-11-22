"""v2.1.3 推送门禁 + 真机死锁根治验收（2026-11-22 黑豹2 事故）。

真机事实：
  - 「黑·豹·2 (2022) {tmdb-505642}」在**未取到英文名、未改名**的状态下被推送到
    频道与群聊（q13/q14 均 sent，push_reason=sc_v2）。
  - `_job_share_center`（5s 周期）与 `process_queue`（60s 周期）连续 24 小时报
    "maximum number of running instances reached (1)"。
  - 07:23 之后入库的 56 部作品集体没有英文名（解析链路停摆）。

覆盖：
  1. 门禁五项逐项判定（非镜像链路放行「目录 / 文件名」两项）；
  2. 黑豹2 回归：MirrorMedia 无英文名 → 门禁不通过；
  3. v2 dispatch：门禁未过 → 任务保持 planned + 60 秒后重试，不建分享不失败；
     门禁通过 → 建分享并提交队列；
  4. 旧链 `_enqueue_ready`：门禁未过 → 不入队；
  5. `_recheck_gated_push`：改名完成后把被门禁挡下的作品重新入队（闭环回边）；
  6. 死锁防线静态守卫：单飞锁 / 网络硬超时 / 单轮预算 / 僵尸回收 /
     改名锁与镜像锁分离 / publisher 整轮线程化。
"""
import asyncio
import os
import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

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
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v213_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'v213.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# 构造辅助
# ---------------------------------------------------------------------------

BP_TITLE = "黑豹2 (2022) {tmdb-505642}"


def _bp_resource(db, *, poster="/poster.jpg", title="黑豹2", identified="identified"):
    """黑豹2 的资源行：已识别、有海报、有资料（与真机 resources id=146 一致）。"""
    from app.models import Resource
    r = Resource(
        file_id="v213-bp", name="Black.Panther.Wakanda.Forever.2022.mkv",
        ext=".mkv", size=100, utime=1,
        identify_status=identified, media_type="movie", tmdb_id=505642,
        media_title=title, original_title="Black Panther: Wakanda Forever",
        path_hint=BP_TITLE, share_dir_id="v213-bp-dir",
        share_scope="series_dir", share_dir_name=BP_TITLE,
        poster_path=poster,
    )
    db.add(r)
    db.commit()
    return r


def _bp_media(db, *, english_title="", title_mirror="黑·豹·2 (2022) {tmdb-505642}",
              rule_id=1):
    """黑豹2 的镜像记录。真机镜像行 english_title='' → 解析链路一次都没跑。"""
    from app.models import MirrorMedia, MirrorRule
    rule = db.get(MirrorRule, rule_id)
    if rule is None:
        rule = MirrorRule(id=rule_id, name="v213-rule", enabled=True)
        db.add(rule)
        db.flush()
    m = MirrorMedia(
        rule_id=rule_id, work_key="work:黑豹2",
        title_standard="黑豹2 (2022) {tmdb-505642}",
        title_mirror=title_mirror, tmdb_id=505642, media_type="movie",
        source_dir_id="v213-a-dir", target_dir_id="v213-b-dir",
        english_title=english_title, english_source="" if not english_title else "tmdb",
        status="synced",
    )
    db.add(m)
    db.commit()
    return m


def _bp_file(db, media_id, *, status="synced", rename_status="done", target_name=""):
    from app.models import MirrorFile
    f = MirrorFile(
        media_id=media_id, rule_id=1, identity="tmdb:505642:movie:S0:E0",
        source_file_id="v213-a-file", target_file_id="v213-b-file",
        file_name="Black.Panther.Wakanda.Forever.2022.mkv", size=100, utime=1,
        status=status, rename_status=rename_status, target_name=target_name,
    )
    db.add(f)
    db.commit()
    return f


def _bot_channel(db, chat_id="-1001"):
    from app.models import Bot, Channel
    from app.security import encrypt_secret
    bot = Bot(name="v213-bot", token=encrypt_secret("1:FAKE"), enabled=True)
    db.add(bot)
    db.flush()
    ch = Channel(name="v213-ch", bot_id=bot.id, chat_id=chat_id, enabled=True)
    db.add(ch)
    db.commit()
    return ch


def _publish_task(db, work_key="work:黑豹2", resource_ids=None):
    """构造一条 planned 的发布任务 + 其清单项 / 资源版本。"""
    from app.models import (SCPublishTask, SCPushList, SCPushListItem,
                            SCLibraryVersion)
    lst = SCPushList(list_key="v213-list", name="v213", status="active")
    db.add(lst)
    db.flush()
    lv = SCLibraryVersion(
        work_key=work_key, version_no=1, fingerprint="v213-fp", media_type="movie",
        resource_ids_json=str(resource_ids if resource_ids is not None else [1]),
        metadata_json='{"title": "黑豹2", "share_dir_id": "v213-bp-dir"}',
        change_type="created",
    )
    db.add(lv)
    db.flush()
    item = SCPushListItem(
        list_id=lst.id, work_key=work_key, library_version_id=lv.id,
        category_key="genre:动作", target_type="group", bot_id=1,
        chat_id="-1001", dedup_key=f"v213:{work_key}", source_kind="first",
        status="planned",
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
# 1. 门禁五项逐项判定
# ---------------------------------------------------------------------------

def test_gate_non_mirror_work_only_needs_first_three(env):
    """非镜像链路作品（无 mirror_media）只校验海报 / 资料 / 识别成功。"""
    from app.push_gate import evaluate_push_gate
    S, _ = env
    with S() as db:
        r = _bp_resource(db)
        gate = evaluate_push_gate(db, "work:黑豹2", [r])
        assert gate["passed"] is True, gate
        assert gate["blocked"] == []
        # 非镜像链路：目录 / 文件名两项自动放行
        assert gate["items"]["dir_marked"] is True
        assert gate["items"]["file_english"] is True


def test_gate_missing_poster_blocks(env):
    from app.push_gate import evaluate_push_gate
    S, _ = env
    with S() as db:
        r = _bp_resource(db, poster="")
        gate = evaluate_push_gate(db, "work:黑豹2", [r])
        assert gate["passed"] is False
        assert "poster" in gate["blocked"]
        assert "缺少影片海报" in gate["reason"]


def test_gate_missing_metadata_blocks(env):
    from app.push_gate import evaluate_push_gate
    S, _ = env
    with S() as db:
        r = _bp_resource(db, title="")
        r.original_title = ""
        db.commit()
        gate = evaluate_push_gate(db, "work:黑豹2", [r])
        assert gate["passed"] is False
        assert "metadata" in gate["blocked"]
        assert "缺少影片资料" in gate["reason"]


def test_gate_not_identified_blocks(env):
    from app.push_gate import evaluate_push_gate
    S, _ = env
    with S() as db:
        r = _bp_resource(db, identified="unidentified")
        gate = evaluate_push_gate(db, "work:黑豹2", [r])
        assert gate["passed"] is False
        assert "identified" in gate["blocked"]
        assert "影片尚未识别完成" in gate["reason"]


def test_gate_mirror_dir_not_marked_blocks(env):
    """镜像链路：影视主目录未按 · 约定整理 → 不通过。"""
    from app.push_gate import evaluate_push_gate
    S, _ = env
    with S() as db:
        r = _bp_resource(db)
        _bp_media(db, english_title="Black Panther: Wakanda Forever",
                  title_mirror="黑豹2 (2022) {tmdb-505642}")
        _bp_file(db, 1, rename_status="done", target_name="Black Panther 2.mkv")
        gate = evaluate_push_gate(db, "work:黑豹2", [r])
        assert gate["passed"] is False
        assert "dir_marked" in gate["blocked"]
        assert "影片目录尚未按命名约定整理" in gate["reason"]


def test_gate_mirror_created_fallback_dir_is_ok(env):
    """网盘拒绝带 · 时的降级（created_fallback）不算整理遗漏。"""
    from app.models import MirrorDir
    from app.push_gate import evaluate_push_gate
    S, _ = env
    with S() as db:
        r = _bp_resource(db)
        m = _bp_media(db, english_title="Black Panther: Wakanda Forever",
                      title_mirror="黑豹2 (2022) {tmdb-505642}")
        db.add(MirrorDir(rule_id=1, source_dir_id="v213-a-dir",
                         name_standard="黑豹2 (2022) {tmdb-505642}",
                         kind="work", status="created_fallback"))
        db.commit()
        _bp_file(db, m.id, rename_status="done", target_name="Black Panther 2.mkv")
        gate = evaluate_push_gate(db, "work:黑豹2", [r])
        assert gate["passed"] is True, gate


def test_gate_mirror_no_english_title_blocks(env):
    """黑豹2 回归：镜像行 english_title 为空 → 门禁不通过。"""
    from app.push_gate import evaluate_push_gate
    S, _ = env
    with S() as db:
        r = _bp_resource(db)
        m = _bp_media(db, english_title="")
        _bp_file(db, m.id, rename_status="", target_name="")
        gate = evaluate_push_gate(db, "work:黑豹2", [r])
        assert gate["passed"] is False
        assert "file_english" in gate["blocked"]
        assert "影片文件名尚未翻译为英文" in gate["reason"]


def test_gate_mirror_file_not_renamed_blocks(env):
    """英文名已解析但单文件还没改名 → 仍然不通过。"""
    from app.push_gate import evaluate_push_gate
    S, _ = env
    with S() as db:
        r = _bp_resource(db)
        m = _bp_media(db, english_title="Black Panther: Wakanda Forever")
        _bp_file(db, m.id, rename_status="", target_name="")
        gate = evaluate_push_gate(db, "work:黑豹2", [r])
        assert gate["passed"] is False
        assert "file_english" in gate["blocked"]


def test_gate_mirror_file_done_but_empty_target_blocks(env):
    """改名状态是 done 但目标名为空（改名半途失败）→ 不通过。"""
    from app.push_gate import evaluate_push_gate
    S, _ = env
    with S() as db:
        r = _bp_resource(db)
        m = _bp_media(db, english_title="Black Panther: Wakanda Forever")
        _bp_file(db, m.id, rename_status="done", target_name="")
        gate = evaluate_push_gate(db, "work:黑豹2", [r])
        assert gate["passed"] is False
        assert "file_english" in gate["blocked"]


def test_gate_mirror_all_ready_passes(env):
    """目录已整理 + 英文名已解析 + 单文件已改名 → 门禁通过。"""
    from app.push_gate import evaluate_push_gate
    S, _ = env
    with S() as db:
        r = _bp_resource(db)
        m = _bp_media(db, english_title="Black Panther: Wakanda Forever")
        _bp_file(db, m.id, rename_status="done", target_name="Black Panther 2.mkv")
        gate = evaluate_push_gate(db, "work:黑豹2", [r])
        assert gate["passed"] is True, gate
        assert gate["blocked"] == []


def test_gate_skipped_ascii_counts_as_english(env):
    """原文件名本就是英文（skipped_ascii）视为已具备英文名。"""
    from app.push_gate import evaluate_push_gate
    S, _ = env
    with S() as db:
        r = _bp_resource(db)
        m = _bp_media(db, english_title="Black Panther: Wakanda Forever")
        _bp_file(db, m.id, rename_status="skipped_ascii", target_name="")
        gate = evaluate_push_gate(db, "work:黑豹2", [r])
        assert gate["passed"] is True, gate


def test_gate_is_work_level_any_member_satisfies(env):
    """作品级判定：同一部剧只要有一集满足前三项即可。"""
    from app.models import Resource
    from app.push_gate import evaluate_push_gate
    S, _ = env
    with S() as db:
        a = _bp_resource(db)
        b = Resource(file_id="v213-bp2", name="ep2.mkv", ext=".mkv", size=1, utime=2,
                     identify_status="unidentified", media_type="movie",
                     media_title="黑豹2", share_dir_name=BP_TITLE)
        db.add(b)
        db.commit()
        gate = evaluate_push_gate(db, "work:黑豹2", [a, b])
        assert gate["passed"] is True, gate


# ---------------------------------------------------------------------------
# 2. v2 dispatch：门禁接入
# ---------------------------------------------------------------------------

def test_dispatch_gate_blocks_black_panther_and_retries_in_60s(env, monkeypatch):
    """黑豹2 回归（v2 链）：门禁未过 → 不建分享、不入队、保持 planned 并 60 秒后重试。"""
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env

    built = {"n": 0}

    async def fake_create(client, file_id, title, duration):
        built["n"] += 1
        return "sid", "https://www.guangyapan.com/s/x"

    monkeypatch.setattr(sc2, "create_share_parsed", fake_create)

    with S() as db:
        r = _bp_resource(db)
        m = _bp_media(db, english_title="")
        _bp_file(db, m.id, rename_status="", target_name="")
        task, _, _ = _publish_task(db, resource_ids=[r.id])
        _bot_channel(db)

        stats = asyncio.run(sc2.dispatch_ready_tasks(db, limit=20))

        assert stats["gated"] == 1, stats
        assert stats["built"] == 0 and stats["submitted"] == 0
        assert built["n"] == 0, "门禁未过不得建分享"

        db.expire_all()
        t = db.get(SCPublishTask, task.id)
        assert t.status == "planned", t.status
        assert "影片文件名尚未翻译为英文" in (t.error or "")
        assert t.next_retry_at is not None
        # 重试间隔 = 60 秒（允许时钟微小偏差）。SQLite 存的是 naive 时间，
        # 必须先统一成 UTC 才能与 now 比较。
        from app.utils import as_utc
        delta = (as_utc(t.next_retry_at) - datetime.now(timezone.utc)).total_seconds()
        assert 0 < delta <= 60 + 5, delta


def test_dispatch_gate_passes_builds_and_submits(env, monkeypatch):
    """门禁通过 → 建分享 + 提交队列，任务状态 submitted。"""
    from app import share_center_v2 as sc2
    from app.models import QueueItem, SCPublishTask
    S, _ = env

    async def fake_create(client, file_id, title, duration):
        return "sid", "https://www.guangyapan.com/s/ok"

    monkeypatch.setattr(sc2, "create_share_parsed", fake_create)

    with S() as db:
        r = _bp_resource(db)
        m = _bp_media(db, english_title="Black Panther: Wakanda Forever")
        _bp_file(db, m.id, rename_status="done", target_name="Black Panther 2.mkv")
        task, _, _ = _publish_task(db, resource_ids=[r.id])
        _bot_channel(db)

        stats = asyncio.run(sc2.dispatch_ready_tasks(db, limit=20))

        assert stats["gated"] == 0, stats
        assert stats["built"] == 1 and stats["submitted"] == 1, stats

        db.expire_all()
        t = db.get(SCPublishTask, task.id)
        assert t.status == "submitted", t.status
        assert t.share_url == "https://www.guangyapan.com/s/ok"
        q = db.scalar(select(QueueItem))
        assert q is not None and q.push_reason == "sc_v2"


def test_dispatch_revives_stale_building_zombie(env, monkeypatch):
    """卡在「建链中」的僵尸任务必须被回收重排（否则永久漏推）。"""
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env

    async def fake_create(client, file_id, title, duration):
        return "sid", "https://www.guangyapan.com/s/ok"

    monkeypatch.setattr(sc2, "create_share_parsed", fake_create)

    with S() as db:
        r = _bp_resource(db)
        m = _bp_media(db, english_title="Black Panther: Wakanda Forever")
        _bp_file(db, m.id, rename_status="done", target_name="Black Panther 2.mkv")
        task, _, _ = _publish_task(db, resource_ids=[r.id])
        _bot_channel(db)
        # 人为把任务卡在 building 且 updated_at 很久以前
        task.status = "building"
        task.updated_at = datetime.now(timezone.utc) - timedelta(seconds=sc2.BUILDING_STALE_SECONDS + 60)
        db.commit()

        stats = asyncio.run(sc2.dispatch_ready_tasks(db, limit=20))

        assert stats["revived"] == 1, stats
        db.expire_all()
        t = db.get(SCPublishTask, task.id)
        assert t.status == "submitted", t.status


def test_run_cycle_single_flight_skips_second_round(env):
    """进程级单飞：上一轮未结束时第二轮直接跳过（正常背压，不是故障）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        assert sc2._CYCLE_LOCK.acquire(blocking=False) is True
        try:
            out = asyncio.run(sc2.run_cycle(db, limit=5))
            assert out.get("skipped") is True, out
            assert out.get("enabled") is True
            assert "跳过" in (out.get("reason") or "")
        finally:
            sc2._CYCLE_LOCK.release()


# ---------------------------------------------------------------------------
# 3. 旧链 _enqueue_ready 门禁 + 补偿闭环
# ---------------------------------------------------------------------------

def _global_env_setup():
    """旧链相关用例走全局 SessionLocal（main 模块用的是全局会话工厂）。"""
    from app.db import Base, SessionLocal, migrate
    from app import models  # noqa: F401
    Base.metadata.create_all(bind=SessionLocal.kw["bind"])
    migrate()


def _clean_legacy():
    """清理本文件在**全局测试库**里留下的数据。

    注意 LIKE 必须带尾部通配符：写成 "v213-" 只精确匹配 "v213-" 这个串，
    上一条用例的资源会残留下来污染下一条（作品分组 / 已入队判定都会被带偏）。
    """
    from app.db import SessionLocal
    from app.models import (Bot, Channel, MirrorFile, MirrorMedia, MirrorRule,
                            QueueItem, Resource, Rule)
    db = SessionLocal()
    db.query(QueueItem).delete(synchronize_session=False)
    db.query(Rule).delete(synchronize_session=False)
    db.query(Channel).delete(synchronize_session=False)
    db.query(Bot).delete(synchronize_session=False)
    db.query(MirrorFile).delete(synchronize_session=False)
    db.query(MirrorMedia).delete(synchronize_session=False)
    db.query(MirrorRule).delete(synchronize_session=False)
    db.query(Resource).filter(Resource.file_id.like("v213-%")).delete(synchronize_session=False)
    db.commit()
    db.close()


def test_legacy_enqueue_ready_blocked_by_gate():
    """旧链入队同样受门禁约束：黑豹2 未改名 → 不入队。"""
    _global_env_setup()
    _clean_legacy()
    from app.db import SessionLocal
    from app.main import _enqueue_ready
    from app.models import (Bot, Channel, MirrorFile, MirrorMedia, MirrorRule,
                            QueueItem, Resource, Rule)
    from app.security import encrypt_secret
    db = SessionLocal()
    try:
        bot = Bot(name="v213-bot", token=encrypt_secret("1:FAKE"), enabled=True)
        db.add(bot)
        db.flush()
        ch = Channel(name="v213-ch", bot_id=bot.id, chat_id="-1001", enabled=True)
        db.add(ch)
        db.flush()
        rule = Rule(name="v213-rule", channel_id=ch.id, auto_publish=True, enabled=True)
        db.add(rule)
        db.add(MirrorRule(id=9001, name="v213-mirror-rule", enabled=True))
        r = Resource(file_id="v213-legacy", name="bp.mkv", ext=".mkv", size=1, utime=1,
                     identify_status="identified", media_type="movie", tmdb_id=505642,
                     media_title="黑豹2", original_title="Black Panther: Wakanda Forever",
                     path_hint=BP_TITLE, share_dir_name=BP_TITLE,
                     share_dir_id="v213-bp-dir", poster_path="/p.jpg")
        db.add(r)
        db.commit()
        m = MirrorMedia(rule_id=9001, work_key="work:黑豹2",
                        title_standard="黑豹2 (2022) {tmdb-505642}",
                        title_mirror="黑·豹·2 (2022) {tmdb-505642}", tmdb_id=505642,
                        media_type="movie", source_dir_id="v213-a-dir",
                        english_title="", status="synced")
        db.add(m)
        db.commit()
        db.add(MirrorFile(media_id=m.id, rule_id=9001, identity="tmdb:505642:movie:S0:E0",
                          source_file_id="v213-a-file", file_name="bp.mkv",
                          size=1, utime=1, status="synced", rename_status=""))
        db.commit()

        assert _enqueue_ready(db, r, rule) is False, "门禁未过不得入队"
        mine = db.scalars(select(QueueItem).where(QueueItem.resource_id == r.id)).all()
        assert len(mine) == 0, "门禁未过不得产生队列项"
    finally:
        db.close()
        _clean_legacy()


def test_recheck_gated_push_reenqueues_after_rename():
    """补偿闭环：改名完成后，被门禁挡下的作品重新入队。"""
    _global_env_setup()
    _clean_legacy()
    from app.db import SessionLocal
    from app.main import _recheck_gated_push
    from app.models import (Bot, Channel, MirrorFile, MirrorMedia, MirrorRule,
                            QueueItem, Resource, Rule)
    from app.security import encrypt_secret
    db = SessionLocal()
    try:
        bot = Bot(name="v213-bot", token=encrypt_secret("1:FAKE"), enabled=True)
        db.add(bot)
        db.flush()
        ch = Channel(name="v213-ch", bot_id=bot.id, chat_id="-1001", enabled=True)
        db.add(ch)
        db.flush()
        db.add(Rule(name="v213-rule", channel_id=ch.id, auto_publish=True, enabled=True))
        db.add(MirrorRule(id=9002, name="v213-mirror-rule", enabled=True))
        r = Resource(file_id="v213-comp", name="bp.mkv", ext=".mkv", size=1, utime=1,
                     identify_status="identified", media_type="movie", tmdb_id=505642,
                     media_title="黑豹2", original_title="Black Panther: Wakanda Forever",
                     path_hint=BP_TITLE, share_dir_name=BP_TITLE,
                     share_dir_id="v213-bp-dir", poster_path="/p.jpg")
        db.add(r)
        db.commit()
        m = MirrorMedia(rule_id=9002, work_key="work:黑豹2",
                        title_standard="黑豹2 (2022) {tmdb-505642}",
                        title_mirror="黑·豹·2 (2022) {tmdb-505642}", tmdb_id=505642,
                        media_type="movie", source_dir_id="v213-a-dir",
                        english_title="Black Panther: Wakanda Forever", status="synced")
        db.add(m)
        db.commit()
        db.add(MirrorFile(media_id=m.id, rule_id=9002, identity="tmdb:505642:movie:S0:E0",
                          source_file_id="v213-a-file", file_name="bp.mkv",
                          size=1, utime=1, status="synced", rename_status="done",
                          target_name="Black Panther 2.mkv"))
        db.commit()

        added = _recheck_gated_push(db)
        db.commit()
        # 注意：_recheck_gated_push 遍历**全库**资源（全局测试库被其它测试文件
        # 留有残留数据），所以断言必须按本用例的资源 id 精确限定，不能断言总数。
        q = db.scalar(select(QueueItem).where(QueueItem.resource_id == r.id))
        assert q is not None, "改名完成后应重新入队"
        assert added >= 1
    finally:
        db.close()
        _clean_legacy()


def test_recheck_gated_push_skips_already_queued():
    """已经入队的作品不得被补偿重复入队。"""
    _global_env_setup()
    _clean_legacy()
    from app.db import SessionLocal
    from app.main import _recheck_gated_push
    from app.models import Bot, Channel, QueueItem, Resource, Rule
    from app.security import encrypt_secret
    db = SessionLocal()
    try:
        bot = Bot(name="v213-bot", token=encrypt_secret("1:FAKE"), enabled=True)
        db.add(bot)
        db.flush()
        ch = Channel(name="v213-ch", bot_id=bot.id, chat_id="-1001", enabled=True)
        db.add(ch)
        db.flush()
        db.add(Rule(name="v213-rule", channel_id=ch.id, auto_publish=True, enabled=True))
        r = Resource(file_id="v213-dup", name="bp.mkv", ext=".mkv", size=1, utime=1,
                     identify_status="identified", media_type="movie",
                     media_title="某片", original_title="Some Movie",
                     path_hint="某片 (2020)", share_dir_name="某片 (2020)",
                     share_dir_id="v213-x-dir", poster_path="/p.jpg")
        db.add(r)
        db.commit()
        db.add(QueueItem(resource_id=r.id, channel_id=ch.id, status="pending"))
        db.commit()

        _recheck_gated_push(db)
        db.commit()
        # 已入队的作品不得被补偿重复入队（本资源仍只有最初那一条）
        mine = db.scalars(select(QueueItem).where(QueueItem.resource_id == r.id)).all()
        assert len(mine) == 1, [q.id for q in mine]
    finally:
        db.close()
        _clean_legacy()


def test_recheck_gated_push_noop_when_decoupled():
    """解耦开关开启时由 v2 自行重试，补偿函数不得重复入队。"""
    _global_env_setup()
    _clean_legacy()
    from app import runtime_config
    from app.db import SessionLocal
    from app.main import _recheck_gated_push
    from app.models import Bot, Channel, QueueItem, Resource, Rule
    from app.security import encrypt_secret
    db = SessionLocal()
    try:
        runtime_config.set_many(db, {"resource_decoupled_enabled": "true"})
        db.commit()
        bot = Bot(name="v213-bot", token=encrypt_secret("1:FAKE"), enabled=True)
        db.add(bot)
        db.flush()
        ch = Channel(name="v213-ch", bot_id=bot.id, chat_id="-1001", enabled=True)
        db.add(ch)
        db.flush()
        db.add(Rule(name="v213-rule", channel_id=ch.id, auto_publish=True, enabled=True))
        r = Resource(file_id="v213-dec", name="bp.mkv", ext=".mkv", size=1, utime=1,
                     identify_status="identified", media_type="movie",
                     media_title="某片", original_title="Some Movie",
                     path_hint="某片 (2020)", share_dir_name="某片 (2020)",
                     share_dir_id="v213-y-dir", poster_path="/p.jpg")
        db.add(r)
        db.commit()

        assert _recheck_gated_push(db) == 0
        q = db.scalar(select(QueueItem).where(QueueItem.resource_id == r.id))
        assert q is None, "解耦开启时补偿函数不得入队（v2 自行按分钟重试）"
    finally:
        runtime_config.set_many(db, {"resource_decoupled_enabled": "false"})
        db.commit()
        db.close()
        _clean_legacy()


# ---------------------------------------------------------------------------
# 4. 死锁防线静态守卫
# ---------------------------------------------------------------------------

def test_gate_is_checked_before_building_in_dispatch():
    """门禁必须发生在建分享（status=building）之前。"""
    src = (ROOT / "backend/app/share_center_v2.py").read_text(encoding="utf-8")
    body = src[src.index("async def dispatch_ready_tasks"):]
    body = body[:body.index("async def run_cycle")]
    gate_at = body.index("evaluate_push_gate(")
    building_at = body.index('task.status = "building"')
    assert gate_at < building_at, "门禁判定必须早于建分享"


def test_share_center_has_single_flight_lock():
    """单飞锁：非阻塞获取 + finally 释放。"""
    src = (ROOT / "backend/app/share_center_v2.py").read_text(encoding="utf-8")
    assert "_CYCLE_LOCK = threading.Lock()" in src
    assert "_CYCLE_LOCK.acquire(blocking=False)" in src
    body = src[src.index("async def run_cycle"):]
    body = body[:body.index("def claim_tasks")]
    assert "finally:" in body and "_CYCLE_LOCK.release()" in body


def test_share_build_has_hard_timeout_and_round_budget():
    """建分享网络硬超时 + 单轮墙钟预算。"""
    src = (ROOT / "backend/app/share_center_v2.py").read_text(encoding="utf-8")
    assert "SHARE_BUILD_TIMEOUT_SECONDS" in src
    assert "asyncio.wait_for(" in src
    assert "DISPATCH_ROUND_BUDGET_SECONDS" in src
    assert "BUILDING_STALE_SECONDS" in src


def test_rename_lock_is_separate_from_mirror_lock():
    """改名复查不得再与镜像同步抢同一把锁。"""
    src = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
    assert "_rename_lock = threading.Lock()" in src
    body = src[src.index("async def _job_rename_pending"):]
    body = body[:body.index("def _queue_trigger")]
    assert "_rename_lock.acquire(blocking=False)" in body
    # 只查「加锁动作」，函数文档里会提到 _mirror_lock 作为背景说明。
    assert "_mirror_lock.acquire" not in body, "改名复查不得再抢镜像锁"
    assert "_mirror_lock.release" not in body, "改名复查不得再动镜像锁"
    assert "_recheck_gated_push(db)" in body, "改名完成后必须走门禁补偿"


def test_publisher_queue_round_runs_in_worker_thread():
    """publisher 整轮队列处理挪到工作线程（主协程零写库）。"""
    src = (ROOT / "backend/app/publisher.py").read_text(encoding="utf-8")
    assert "def _process_queue_in_thread" in src
    assert "asyncio.run(_process_queue_core(db, limit=limit))" in src
    assert "async def _process_queue_core" in src
    impl = src[src.index("async def _process_queue_impl"):]
    impl = impl[:impl.index("async def _process_queue_core")]
    assert "asyncio.to_thread(_process_queue_in_thread" in impl
    assert "SEND_ROUND_BUDGET_SECONDS" in src


def test_push_gate_module_has_no_backend_field_names_in_labels():
    """门禁原因必须是中文业务语，不得暴露后端字段名/状态码。"""
    from app.push_gate import GATE_ITEMS, GATE_LABELS
    assert set(GATE_ITEMS) == {"poster", "metadata", "identified",
                                "dir_marked", "file_english"}
    for label in GATE_LABELS.values():
        for forbidden in ("rename_status", "english_title", "identify_status",
                          "poster_path", "link_state", "work_key", "genre:"):
            assert forbidden not in label, (forbidden, label)
