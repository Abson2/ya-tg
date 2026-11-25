"""v2.1.9 三项真机报障的门禁测试。

本轮修的是三个**真机上真实存在**的缺陷，全部有数据佐证，不是预防性加固：

【问题一】TMDB 限流被写死 identify_status="failed"（永久放弃重试）
  真机：3714 个资源里 identified 344 / failed 1656 / unidentified 1714，
  1656 个 failed 的 identify_note 100% 是限流文案。
  根因：main.py 识别循环用 `except Exception` 把限流与真失败一视同仁。
  修法：
    ① 新增 identify_status="rate_limited"（前端显示「待重试」）；
    ② 该状态加入 pending 查询集合 → 下一轮自动重选；
    ③ 分帧排队（rate_limit_wait_seconds + 等待阈值）→ 超阈值中止本轮，
       而不是继续撞限流把整批写成失败；
    ④ db.py 一次性回滚迁移：把 1656 个限流误标救回待重试队列。

【问题二】小时窗口迁移判定写错 + 分类时段死区/空洞
  真机：线上库 sc_hour_windows 只有**一行** 8-24（段长 16 小时），
  旧判定要求「段长 == 1 小时」→ pristine=False → **永远不迁移**，
  0-8 点全天封死（当日 push_sent 仅 3 条，时间全是 UTC 00:00-00:01）。
  同时分类时段分配把 19 个分类挤在 0-8 点，白天 9 段各只占 1 个分类，
  12-16 黄金段只有 2 个分类覆盖。
  修法：
    ① pristine 改为**按形态判定**（SYSTEM_ASSIGNED_WINDOWS 白名单 + 幂等保护）；
    ② 溢出池改为**全部可用段**（白天段同样参与复用）；
    ③ _ensure_schedules 挂到 ensure_defaults（存量排期才会自动迁移）。

【问题三】copy_file 遇 code=127 直接写 failed + 作品循环无异常隔离
  真机：196 个 failed 的 error 100% 是 copy_file code=127「操作过于频繁」，
  失败集中在 6 部长剧（包青天 50 / 吸血鬼日记 44 / …），每部同一秒批量建失败行；
  另有 89 条 MirrorFile 卡在 pending，镜像 last_run 停在 9.5 小时前。
  根因：
    ① copy_file 是写接口，却没有像改名路径那样做 127 指数退避；
    ② sync_rule 的作品循环**整段没有 try/except** → 任一部作品抛异常，
       整轮直接崩 → rule.last_run 永不更新 → 看起来像定时任务停摆。
  修法：
    ① _copy_file_with_backoff：127 → 指数退避 1/2/4/8/8；耗尽标 pending 不标 failed；
    ② 作品循环整体 try/except：单部失败只跳过这一部；
    ③ 待重试作品优先于轮转游标，不再等游标转到。

覆盖：
  A. 限流判定与「待重试」状态（含一次性回滚迁移）
  B. 分帧排队（发请求前先看配额，超阈值中止本轮）
  C. 小时窗口迁移（形态判定 + 幂等）与分类时段均衡分配
  D. copy_file 127 退避重试（含 pending/failed 的区分）
  E. 作品级异常隔离 + 待重试作品优先
  F. 前端契约（中文标签，不露后端状态码）
"""
import asyncio
import os
import shutil
import sys
import tempfile
import time
from collections import deque
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = ROOT / "frontend" / "src" / "App.vue"


@pytest.fixture
def env():
    """独立临时库（库名带进程号，避免并发跑时互相删库）。"""
    from app.db import Base
    from app import models  # noqa: F401  必须先 import 才能建全表
    d = tempfile.mkdtemp(prefix="v219fix_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'fix.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    from sqlalchemy.orm import sessionmaker
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _noop_sleep():
    async def _sleep(*_a, **_kw):
        return None
    return _sleep


# ===========================================================================
# A. 限流判定与「待重试」状态
# ===========================================================================

def test_is_rate_limited_detects_type_and_text():
    """限流判别：异常类型优先，文案兜底；真失败不得误判。"""
    from app import tmdb

    # 类型优先
    assert tmdb.is_rate_limited(tmdb.TmdbRateLimited("已被限流"))
    # 文案兜底（存量库里 note 就是这么写的）
    assert tmdb.is_rate_limited(RuntimeError("调用过于频繁，请稍后重试"))
    assert tmdb.is_rate_limited(RuntimeError("rate limit exceeded"))
    assert tmdb.is_rate_limited(RuntimeError("Too Many Requests"))
    # 真失败一律不判成限流
    assert not tmdb.is_rate_limited(RuntimeError("未找到候选"))
    assert not tmdb.is_rate_limited(RuntimeError("404 Not Found"))
    assert not tmdb.is_rate_limited(ValueError("数据库锁"))


def test_rate_limited_is_in_pending_set_and_not_failed():
    """核心口径：限流必须是独立的「待重试」状态，且会被下一轮自动重选。

    反证点：旧实现写死 failed —— 而 pending 查询集合本来就包含 "failed"，
    写死 failed 看似「还会重试」，实则每次重试都立刻再撞限流再写 failed，
    等于永久放弃；且前端把限流的资源显示成「识别失败」，误导用户。
    """
    from app import main, tmdb
    from app.models import Resource

    src = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")
    # 待识别集合必须含 rate_limited
    assert '"rate_limited"' in src, "pending 集合必须包含 rate_limited"
    # 限流分支不得写死 failed
    assert 'x.identify_status = "rate_limited"' in src, "限流必须标记为 rate_limited"
    assert tmdb.is_rate_limited(tmdb.TmdbRateLimited("x"))


def test_identify_loop_marks_rate_limited_and_stops_round(env):
    """端到端：识别遇限流 → 标 rate_limited（不是 failed），且中止本轮。

    中止是必须的：继续跑只会把后面所有资源也写成限流。
    """
    S, _ = env
    from app import main
    from app.models import Resource

    with S() as db:
        for i in range(3):
            db.add(Resource(file_id=f"F{i}", name=f"作品{i}", path_hint="",
                            identify_status="unidentified"))
        db.commit()

        calls = {"n": 0}

        async def fake_identify(name, x, db_):
            calls["n"] += 1
            from app.tmdb import TmdbRateLimited
            raise TmdbRateLimited("调用过于频繁")

        with patch("app.main.identify_with_a_source_fallback", fake_identify):
            stats = asyncio.run(main._post_scan_identify(db, publish_after_identify=False))
        # _post_scan_identify 不自行提交（由调用方 commit），测试里显式提交后再读
        db.commit()

        db.expire_all()
        rows = list(db.scalars(select(Resource).order_by(Resource.id)).all())
        assert calls["n"] == 1, f"限流后必须中止本轮，实际调用 {calls['n']} 次"
        # v2.1.12：待识别集合改为「按作品分散 + 轮次推进」（identify_batch.select_identify_batch），
        # 不再是 id.desc() 固定窗口。本用例的 3 条资源分属 3 部作品（file_id 各异），
        # 自动管线优先新内容 → 先处理 id 最大的那条；限流的那一条是 rate_limited。
        limited = [r for r in rows if r.identify_status == "rate_limited"]
        assert len(limited) == 1, f"应有且仅有 1 条标为待重试，实际 {len(limited)}"
        assert limited[0].identify_status != "failed"
        # 后面未被处理的资源保持原状，不得被污染成限流/失败
        untouched = [r for r in rows if r.identify_status == "unidentified"]
        assert len(untouched) == 2, \
            f"未被处理的 2 条必须保持未识别，实际状态 {[(r.file_id, r.identify_status) for r in rows]}"
        assert int(stats.get("rate_limited") or 0) >= 1


def test_identify_all_endpoint_also_marks_rate_limited(env):
    """identify_all 端点同样不得把限流写成 failed。"""
    S, _ = env
    from app import main
    from app.models import Resource

    with S() as db:
        db.add(Resource(file_id="FX", name="限流作品", path_hint="",
                        identify_status="unidentified"))
        db.commit()

        async def fake_identify(name, db_, path_hint="", use_ai=True,
                                 *, forced_tmdb_id=0, forced_type=""):
            from app.tmdb import TmdbRateLimited
            raise TmdbRateLimited("调用过于频繁")

        with patch("app.tmdb.identify", fake_identify):
            out = asyncio.run(main.identify_all(force=False, limit=10, db=db))

        db.expire_all()
        row = db.scalar(select(Resource))
        assert row.identify_status == "rate_limited", "端点也必须标待重试"
        assert out["ok"] is True


def test_rate_limit_wait_seconds_blocks_before_sending(env):
    """分帧排队：发请求前先算「还要等多久」，超阈值直接中止本轮。"""
    S, _ = env
    from app import tmdb

    with S() as db:
        tmdb.reset_rate_limiters()
        try:
            # 没有历史调用 → 不需要等
            assert tmdb.rate_limit_wait_seconds(db, tmdb.SCAN) == 0.0
            # 把滑动窗口塞满 → 必须算出等待时间
            interval, max_calls, window = tmdb._rate_cfg(db, tmdb.SCAN)
            with tmdb._RATE_LOCK:
                st = tmdb._RATE_STATE.setdefault(tmdb.SCAN, {"last": 0.0, "calls": deque()})
                # 全部落在窗口内（用 monotonic 基准点，保证 now - t < window）
                base = time.monotonic()
                st["calls"] = deque([base] * max_calls)
            wait = tmdb.rate_limit_wait_seconds(db, tmdb.SCAN)
            assert wait > 0, "配额打满时必须算出等待时间"
            assert wait <= window + 1, "等待时间不应超过一个窗口"
        finally:
            tmdb.reset_rate_limiters()


def test_recover_rate_limited_identify_rolls_back_and_is_idempotent(env):
    """一次性回滚迁移：限流误标的 failed 救回待重试；真失败不动；只跑一次。"""
    S, _ = env
    from app.db import _recover_rate_limited_identify
    from app.models import AppConfig, Resource

    with S() as db:
        db.add(Resource(file_id="R1", name="限流作品", path_hint="",
                        identify_status="failed",
                        identify_note="调用过于频繁，请稍后重试"))
        db.add(Resource(file_id="R2", name="限流作品2", path_hint="",
                        identify_status="failed",
                        identify_note="TMDB 已被限流"))
        db.add(Resource(file_id="R3", name="真失败作品", path_hint="",
                        identify_status="failed",
                        identify_note="未找到候选"))
        db.commit()

        changed = _recover_rate_limited_identify(db)
        assert changed == 2, f"应回滚 2 条，实际 {changed}"

        db.expire_all()
        rows = {r.file_id: r for r in db.scalars(select(Resource)).all()}
        assert rows["R1"].identify_status == "rate_limited"
        assert rows["R2"].identify_status == "rate_limited"
        assert rows["R3"].identify_status == "failed", "真失败不得被误改"

        # 幂等：第二次不再改（marker 生效）
        again = _recover_rate_limited_identify(db)
        assert again == 0, "回滚迁移必须只跑一次"
        assert db.get(AppConfig, "identify_rate_limit_recover_v219") is not None


# ===========================================================================
# B. 分帧排队
# ===========================================================================

def test_identify_loop_aborts_when_quota_exhausted(env):
    """配额不足时必须**中止本轮**，不得继续发请求把整批写成限流。"""
    S, _ = env
    from app import main
    from app.models import Resource

    with S() as db:
        for i in range(4):
            db.add(Resource(file_id=f"Q{i}", name=f"配额作品{i}", path_hint="",
                            identify_status="unidentified"))
        db.commit()

        calls = {"n": 0}

        async def fake_identify(name, x, db_):
            calls["n"] += 1
            return {"ok": True, "best": {"tmdb_id": 1, "media_type": "movie",
                                         "title": name, "overview": "",
                                         "poster_path": "", "vote_average": 0,
                                         "genres": [], "origin_country": []},
                    "source": "tmdb", "reason": ""}

        # 让配额等待时间远超阈值
        with patch("app.main.tmdb.rate_limit_wait_seconds", return_value=99.0), \
             patch("app.main.identify_with_a_source_fallback", fake_identify):
            stats = asyncio.run(main._post_scan_identify(db, publish_after_identify=False))

        assert calls["n"] == 0, "配额不足时不得发出任何识别请求"
        assert int(stats.get("rate_limit_deferred") or 0) == 4, stats

        db.expire_all()
        rows = list(db.scalars(select(Resource)).all())
        assert all(r.identify_status == "unidentified" for r in rows), \
            "本轮未发请求的资源不得被改成限流/失败"


def test_identify_wait_threshold_constant_exists():
    """等待阈值常量必须存在且为正（等太久就中止本轮）。"""
    from app import main
    assert main.IDENTIFY_RATE_LIMIT_WAIT_MAX > 0, "等待阈值必须为正"
    assert main.IDENTIFY_RATE_LIMIT_WAIT_MAX <= 10, "阈值过大等于没设闸"


# ===========================================================================
# C. 小时窗口迁移 + 分类时段分配
# ===========================================================================

def _seed_hour_window(db, *, version_id, start, end, category_key="", quota=0):
    from app.models import SCHourWindow
    row = SCHourWindow(plan_version_id=version_id, start_hour=start, end_hour=end,
                       category_key=category_key, quota=quota, enabled=True)
    db.add(row)
    db.commit()
    return row


def test_single_row_8_24_is_migrated_to_12_segments(env):
    """真机形态：库里只有一行 8-24（段长 16 小时）→ 必须迁移为 12 段 0-24。

    反证点：旧判定要求「段长 == 1 小时」，对 8-24 这一行永远 False，
    于是**永远不迁移**，0-8 点全天封死。
    """
    S, _ = env
    from app import share_center_v2 as sc
    from app.models import SCHourWindow

    with S() as db:
        _seed_hour_window(db, version_id=1, start=8, end=24)

        changed = sc._upgrade_hour_windows(db, 1)
        assert changed is True, "单行 8-24 必须被判为系统旧模板并迁移"

        rows = list(db.scalars(select(SCHourWindow).where(
            SCHourWindow.plan_version_id == 1)).all())
        assert len(rows) == 12, f"应重建为 12 段，实际 {len(rows)}"
        covered = set()
        for r in rows:
            assert r.end_hour - r.start_hour == 2, "每段必须是 2 小时"
            covered.update(range(r.start_hour, r.end_hour))
        assert covered == set(range(24)), f"必须覆盖 0-24，实际 {sorted(covered)}"


def test_hour_window_migration_is_idempotent(env):
    """幂等：已是 12 段 0-24 的形态不得被反复重写。"""
    S, _ = env
    from app import share_center_v2 as sc
    from app.models import SCHourWindow

    with S() as db:
        _seed_hour_window(db, version_id=1, start=8, end=24)
        assert sc._upgrade_hour_windows(db, 1) is True
        n1 = len(db.scalars(select(SCHourWindow).where(
            SCHourWindow.plan_version_id == 1)).all())

        assert sc._upgrade_hour_windows(db, 1) is False, "第二次迁移必须返回 False"
        n2 = len(db.scalars(select(SCHourWindow).where(
            SCHourWindow.plan_version_id == 1)).all())
        assert n1 == n2 == 12


def test_user_customized_hour_window_not_migrated(env):
    """用户手工配过的时段/配额不得被迁移覆盖。"""
    S, _ = env
    from app import share_center_v2 as sc
    from app.models import SCHourWindow

    with S() as db:
        # 绑定分类（用户手工配过）
        _seed_hour_window(db, version_id=1, start=8, end=24, category_key="genre:动作")
        assert sc._upgrade_hour_windows(db, 1) is False, "绑定分类的行不得被迁移"
        rows = list(db.scalars(select(SCHourWindow).where(
            SCHourWindow.plan_version_id == 1)).all())
        assert len(rows) == 1 and rows[0].category_key == "genre:动作"

        # 改过配额
        db.query(type(rows[0])).delete()
        db.commit()
        _seed_hour_window(db, version_id=1, start=8, end=24, quota=5)
        assert sc._upgrade_hour_windows(db, 1) is False, "改过配额的行不得被迁移"


def test_category_windows_distribution_is_balanced():
    """分类时段不得再挤在 0-8 点；12-16 黄金段必须有足够分类覆盖。

    真机形态：19 个分类挤在 0-8（用户报 17，量级一致），12-16 只有 2 个覆盖。
    """
    from app import share_center_v2 as sc

    catalog = sc._tmdb_genre_catalog()
    assert len(catalog) == 27, f"TMDB 官方分类应为 27 项，实际 {len(catalog)}"

    prefs = [{"category_key": c["category_key"]} for c in catalog]
    out = sc._assign_category_windows(prefs)

    assert len(out) == len(prefs)

    # 0-8 点（夜间 3 段）承载的分类数不得泛滥
    night_count = sum(1 for (s, e) in out.values() if s < 8)
    assert night_count <= 8, f"0-8 点分类数应 ≤8，实际 {night_count}"

    # 12-16 黄金段必须有多分类覆盖
    golden = {k for k, (s, e) in out.items() if s < 16 and e > 12}
    assert len(golden) >= 6, f"12-16 覆盖分类应 ≥6，实际 {len(golden)}"

    # 每段都不能空转，且不得出现全天窗口
    seg_hits = {}
    for (s, e) in out.values():
        assert (s, e) != (0, 24), "分类时段不得退化为全天窗口"
        assert e - s == 2, f"每分类必须是 2 小时颗粒度，实际 {(s, e)}"
        seg_hits[(s, e)] = seg_hits.get((s, e), 0) + 1
    assert len(seg_hits) >= 10, f"时段段利用率过低：只用到 {len(seg_hits)} 段"


def test_category_window_migration_runs_in_ensure_defaults(env):
    """存量排期必须自动迁移 —— 重排函数要挂在 ensure_defaults 上。

    反证点：此前 _ensure_schedules 只在 3 个 API 端点触发，
    启动路径不跑 → 存量排期永远不会自动收敛。
    """
    import inspect
    from app import share_center_v2 as sc

    src = inspect.getsource(sc.ensure_defaults)
    assert "_migrate_category_windows" in src, \
        "ensure_defaults 必须调用 _migrate_category_windows"
    # 必须排在分类字典种子之后：字典没种下时 windows 为空 → 全部退化成 (0,24)
    seed_pos = src.find("SCCategory")
    sched_pos = src.find("_migrate_category_windows")
    assert seed_pos != -1 and sched_pos != -1, "分类字典种子与排期重排都要在"
    assert seed_pos < sched_pos, "_migrate_category_windows 必须排在分类字典种子之后"


def test_migrate_category_windows_only_reorders_existing_rows(env):
    """端到端：存量排期行被重排到新口径；**不新建行**、不动用户手工行。

    反证点（v2.1.9 全量门禁抓到的真实副作用）：若在 ensure_defaults 里直接调
    _ensure_schedules，它会为尚未存在的分类**新建**启用排期行——
      ① 新库每个分类被限死在 2 小时窗口内 → plan_engine 整轮 items 归零；
      ② 端点侧再插同一 category_key 撞 UNIQUE 约束。
    排期行「存在与否」是用户侧语义，系统只重排已存在的行。
    """
    S, _ = env
    from app import share_center_v2 as sc
    from app.models import SCCategorySchedule

    with S() as db:
        sc.ensure_defaults(db)
        version = sc._active_version(db)
        assert version is not None

        # 库里有 0 行排期 → 迁移后仍是 0 行（系统不替用户造行）
        sc._migrate_category_windows(db, version.id)
        db.commit()
        assert db.scalar(select(SCCategorySchedule.id)) is None, \
            "迁移函数不得新建排期行"

        # 造一条「旧口径」的存量行：8-24（系统曾分配过的形态）
        db.add(SCCategorySchedule(plan_version_id=version.id,
                                  category_key="genre:剧情",
                                  window_start=8, window_end=24,
                                  hourly_limit=3, daily_limit=20,
                                  enabled=True, window_auto=True))
        # 造一条用户手工行：window_auto=False
        db.add(SCCategorySchedule(plan_version_id=version.id,
                                  category_key="genre:动作冒险",
                                  window_start=20, window_end=22,
                                  hourly_limit=2, daily_limit=9,
                                  enabled=True, window_auto=False))
        db.commit()

        changed = sc._migrate_category_windows(db, version.id)
        assert changed == 1, f"只应重排 1 条系统托管行，实际 {changed}"
        db.commit()

        rows = {r.category_key: r for r in db.scalars(select(SCCategorySchedule)).all()}
        assert len(rows) == 2, "迁移不得增删行"
        moved = rows["genre:剧情"]
        ws, we = int(moved.window_start or 0), int(moved.window_end or 0)
        assert (ws, we) != (8, 24), "系统托管行的旧时段必须被重排"
        assert we - ws == 2, f"重排后应是 2 小时颗粒度，实际 {(ws, we)}"
        assert (moved.hourly_limit, moved.daily_limit) == (3, 20), \
            "迁移只改时段，不得动用户可见的上限"
        manual = rows["genre:动作冒险"]
        assert (int(manual.window_start or 0), int(manual.window_end or 0)) == (20, 22), \
            "用户手工行的时段不得被覆盖"


# ===========================================================================
# D. copy_file 127 退避重试
# ===========================================================================

def test_copy_file_with_backoff_retries_127_with_exponential_delay():
    """127 限流必须指数退避重试（1/2/4/8/8），成功即止。"""
    from app import guangya, mirror

    calls = {"n": 0}

    class API:
        async def copy_file(self, fids, parent):
            calls["n"] += 1
            if calls["n"] <= 2:
                raise guangya.WebFileError("copy 失败 code=127 msg=操作过于频繁", 127)
            return "TASK_OK"

    delays = []
    real_sleep = asyncio.sleep

    async def fake_sleep(d):
        delays.append(d)
        await real_sleep(0)

    with patch.object(mirror.asyncio, "sleep", fake_sleep):
        task_id, err = asyncio.run(mirror._copy_file_with_backoff(API(), ["F1"], "P1"))

    assert task_id == "TASK_OK" and err == ""
    assert calls["n"] == 3, "两次 127 后第 3 次成功"
    assert delays == [1.0, 2.0], f"退避序列应为 1→2 秒，实际 {delays}"


def test_copy_file_with_backoff_caps_and_marks_rate_limit():
    """退避上限 8 秒；耗尽后带限流标记（供主循环区分 pending vs failed）。"""
    from app import guangya, mirror

    class API:
        async def copy_file(self, fids, parent):
            raise guangya.WebFileError("操作过于频繁 code=127", 127)

    delays = []
    real_sleep = asyncio.sleep

    async def fake_sleep(d):
        delays.append(d)
        await real_sleep(0)

    with patch.object(mirror.asyncio, "sleep", fake_sleep):
        task_id, err = asyncio.run(mirror._copy_file_with_backoff(API(), ["F1"], "P1"))

    assert task_id == ""
    assert err.startswith(mirror.RENAME_RATE_LIMIT_MARK), "耗尽必须带限流标记"
    assert delays == [1.0, 2.0, 4.0, 8.0, 8.0], f"退避序列应为 1/2/4/8/8，实际 {delays}"
    assert max(delays) == mirror.COPY_RATE_LIMIT_BACKOFF_CAP


def test_copy_file_with_backoff_does_not_retry_real_failure():
    """真实失败立即返回，不重试、不加限流标记。"""
    from app import mirror

    calls = {"n": 0}

    class API:
        async def copy_file(self, fids, parent):
            calls["n"] += 1
            raise RuntimeError("目标目录不存在")

    task_id, err = asyncio.run(mirror._copy_file_with_backoff(API(), ["F1"], "P1"))
    assert task_id == "" and calls["n"] == 1, "真实失败不重试"
    assert mirror.RENAME_RATE_LIMIT_MARK not in err


def test_all_copy_file_call_sites_use_backoff():
    """所有 copy_file 调用点（常规批量 / 定向重试）都必须走退避封装。

    反证点：任一处仍直接调 api.copy_file，遇 127 就直接写 failed、整批永不重试。
    v2.1.13：严格顺序引擎已移除，原来的「strict 批量」调用点随之消失，
    因此这里的调用点计数从 3 降为 2 —— 但**约束本身不变**：只要还有调用点，
    就必须经 `_copy_file_with_backoff`。
    """
    import re
    src = (BACKEND / "app" / "mirror.py").read_text(encoding="utf-8")

    direct = [m for m in re.finditer(r"(?<!def )\bapi\.copy_file\(", src)]
    # 只允许在 _copy_file_with_backoff 内部出现一次直接调用
    helper_start = src.find("async def _copy_file_with_backoff")
    helper_end = src.find("def _is_rate_limit_error", helper_start)
    helper = src[helper_start:helper_end]
    outside = [m for m in direct if not (helper_start <= m.start() < helper_end)]
    assert not outside, f"仍有 {len(outside)} 处 copy_file 未走退避封装"

    # 现存两处调用点（作品内分桶批量 / 失败文件定向重试）都必须调用封装
    assert src.count("_copy_file_with_backoff(api") >= 2, \
        "常规批量 / 定向重试两处都要接入退避封装"


# ===========================================================================
# E. 作品级异常隔离 + 待重试作品优先
# ===========================================================================

def test_works_loop_has_exception_isolation():
    """作品循环必须整体包 try/except —— 单部失败不中断整轮。

    反证点：整轮崩溃 → rule.last_run 永不更新 → 看起来像定时任务停摆，
    后续作品全不处理、上一轮遗留 pending 永远卡住。
    """
    import ast
    src = (BACKEND / "app" / "mirror.py").read_text(encoding="utf-8")
    tree = ast.parse(src)

    target = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "sync_rule":
            target = node
            break
    assert target is not None, "找不到 sync_rule"

    # v2.1.41：作品主循环改成遍历统一去重后的 run_queue（增量 → 修补 → 存量三段拼装）。
    # 兼容旧形态 `for work, cats in priority_works + batch:` 的写法，两种都认。
    work_loops = []
    for node in ast.walk(target):
        if not isinstance(node, ast.For):
            continue
        if isinstance(node.iter, ast.Name) and node.iter.id == "run_queue":
            work_loops.append(node)
        elif isinstance(node.iter, ast.BinOp):
            op = node.iter
            if isinstance(op.op, ast.Add) and getattr(op.left, "id", "") in (
                    "priority_works", "incr_works"):
                work_loops.append(node)
    assert work_loops, "找不到作品主循环"
    loop = work_loops[0]

    # 循环体必须被 try 包住，且 try 带 handler
    guarded = False
    for stmt in loop.body:
        if isinstance(stmt, ast.Try) and stmt.handlers:
            guarded = True
            break
    assert guarded, "作品主循环必须整体包 try/except，否则单部异常会带崩整轮"

    # handler 里必须有 continue（跳过这一部，继续下一部）
    has_continue = any(isinstance(n, ast.Continue)
                       for stmt in loop.body if isinstance(stmt, ast.Try)
                       for h in stmt.handlers for n in ast.walk(h))
    assert has_continue, "异常处理后必须 continue 跳过当前作品"


def test_sync_rule_batch_variable_assigned_before_use():
    """回归：batch 必须在被读取前赋值。

    真机级风险：待重试优先逻辑若写在切批之前，batch 未赋值 →
    UnboundLocalError → 整轮直接崩（反而制造出要修的「整轮停摆」）。
    """
    import ast
    src = (BACKEND / "app" / "mirror.py").read_text(encoding="utf-8")
    tree = ast.parse(src)

    target = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "sync_rule":
            target = node
            break
    assert target is not None

    stores, loads = [], []
    for n in ast.walk(target):
        if isinstance(n, ast.Name) and n.id == "batch":
            (stores if isinstance(n.ctx, ast.Store) else loads).append(n.lineno)
    assert stores, "sync_rule 里必须给 batch 赋值"
    first_store = min(stores)
    early_loads = [l for l in loads if l < first_store]
    assert not early_loads, f"batch 在赋值前被读取（行 {early_loads}）→ UnboundLocalError"


def test_works_with_pending_files_collects_retryable_works(env):
    """待重试作品归集：pending/copying/failed/unknown 的文件都要算进来。"""
    S, _ = env
    from app import mirror
    from app.models import MirrorFile, MirrorMedia, MirrorRule

    with S() as db:
        r = MirrorRule(source_root_id="A", target_root_id="B", enabled=True)
        db.add(r)
        db.commit()

        m1 = MirrorMedia(rule_id=r.id, work_key="work:待重试剧",
                         title_standard="待重试剧", media_type="tv")
        m2 = MirrorMedia(rule_id=r.id, work_key="work:已同步剧",
                         title_standard="已同步剧", media_type="tv")
        db.add(m1)
        db.add(m2)
        db.commit()

        db.add(MirrorFile(media_id=m1.id, rule_id=r.id, identity="i1",
                          source_file_id="F1", file_name="a.mkv",
                          status="pending"))
        db.add(MirrorFile(media_id=m2.id, rule_id=r.id, identity="i2",
                          source_file_id="F2", file_name="b.mkv",
                          status="synced"))
        db.commit()

        got = mirror._works_with_pending_files(db, r.id)
        assert "work:待重试剧" in got, f"待重试作品应被归集，实际 {got}"
        assert "work:已同步剧" not in got, "已同步作品不应被归集"


def test_retry_pending_works_counted_in_stats():
    """待重试作品优先必须计入 stats 并追加到优先队列。

    v2.1.51：sync_rule 改调 `retry_works_order`（同谓词 + 最久未处理优先 + 上限），
    归集口径本身仍由 `_works_with_pending_files` 提供（轻量扫描的强制下探还在用它）。
    """
    import inspect
    from app import mirror
    src = inspect.getsource(mirror.sync_rule)
    assert "retry_works_order" in src, "sync_rule 必须调待重试归集（v2.1.51 起带上限）"
    assert "retry_pending_works" in src, "必须有 retry_pending_works 计数"
    assert "priority_works = priority_works + retry_works" in src, \
        "待重试作品必须追加到优先队列"


def test_work_errors_counted_and_reported():
    """作品级异常必须计数并回显到 last_message（用户能看到有几部被跳过）。"""
    import inspect
    from app import mirror
    src = inspect.getsource(mirror.sync_rule)
    assert 'stats["work_errors"]' in src, "必须有 work_errors 计数"
    assert "异常跳过" in src, "last_message 必须回显异常跳过部数"
    assert '"work_errors": 0' in src, "stats 初始化必须含 work_errors"


# ===========================================================================
# F. 前端契约
# ===========================================================================

def test_frontend_shows_pending_retry_label():
    """前端必须把「待重试」显示成中文标签，且不露后端状态码。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "rate_limited: '待重试'" in app, "identifyLabel 缺 rate_limited 中文标签"
    assert '<el-option label="待重试" value="rate_limited"/>' in app, \
        "识别状态筛选下拉缺「待重试」选项"


def test_frontend_does_not_leak_backend_identify_states():
    """红线：页面不得把后端识别状态码直接当文案回显。"""
    app = APP_VUE.read_text(encoding="utf-8")
    # 待重试是唯一允许出现的字面量状态（作为 el-option 的 value）
    for code in ("identify_status === 'failed'", "识别状态：failed"):
        assert code not in app, f"前端不应直接回显后端状态码：{code}"


def test_frontend_identify_label_covers_all_backend_states():
    """标签映射必须覆盖后端会下发的所有识别状态。"""
    app = APP_VUE.read_text(encoding="utf-8")
    for code in ("identified", "ambiguous", "failed", "none", "unidentified",
                 "rate_limited"):
        assert f"{code}:" in app, f"identifyLabel 缺 {code} 的中文标签"
