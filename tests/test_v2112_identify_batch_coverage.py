"""v2.1.12：识别批次选取门禁（按作品分散 + 轮次推进）。

修的是真机报障「手动操作按目录识别未生效」。

【根因不是识别算法，是批次选取】
用户实测反证（可信）：
  - 库里 1275 条资源，目录线索（path_hint / source_title）空值均为 0；
  - 最新 60 条全部正确识别出「按文件名/目录中的 TMDB ID 精确匹配（剧集）」；
    → 目录名优先、{tmdb-} 标记提取、A 库映射三件套算法完全正常。
  - 三次执行审计 `scanned` 恒为 50，且全部落在 id 1226–1275（全是同一部剧「封神英雄榜」）；
  - id 1–1215 的 1215 条 `unidentified` 从未被这个按钮碰到过。
  → 根因：`order_by(Resource.id.desc()).limit(50)` 是**固定窗口**，
    且 force=true 时连状态过滤都不加 → 已识别行永远霸占窗口，每轮都在同一部剧上打转。

【修法】
  ① 按作品分散：一批里每部作品先出 1 条，作品内部再逐轮加
     → 窗口不再被单部剧的集数吃满。
  ② 轮次推进：`Resource.identify_round` 记累计参与轮次，候选取「轮次少」的优先
     → 连点几次依次覆盖全库，而不是永远重复最新那批。

覆盖：
  A. 批次选取：按作品分散、跨轮推进到不同 id 段
  B. 端点契约：identify-all 返回真实口径（覆盖作品数 / 剩余待识别数）
  C. 自动管线：_post_scan_identify 同样按作品分散，不再只扫最新 500 条
  D. 限流语义不变：限流标「待重试」、中止本轮、不写死失败
  E. 迁移：老库自动补列
"""
import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, inspect, select

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
    d = tempfile.mkdtemp(prefix="v2112id_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'fix.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    from sqlalchemy.orm import sessionmaker
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _seed(db, *, works, per_work, start_id=1, prefix="W"):
    """造资源：works 部作品，每部 per_work 集，目录线索 = 作品目录名。

    目录线索必须真实存在——真机上 1275 条的 path_hint 空值均为 0，
    识别算法本身是好的（用户反证），这里要还原同样的数据形态。
    """
    from app.models import Resource
    made = []
    n = start_id
    for w in range(works):
        for e in range(per_work):
            db.add(Resource(file_id=f"{prefix}{w}_{e}", name=f"{prefix}{w} 第{e + 1}集",
                            path_hint=f"{prefix}{w}", identify_status="unidentified"))
            made.append(n)
            n += 1
    db.commit()
    return made


# ===========================================================================
# A. 批次选取：按作品分散 + 轮次推进
# ===========================================================================

def test_batch_selection_spreads_across_works(env):
    """一批 N 条必须覆盖多部作品，不能被单部剧的集数吃满。

    反证点：旧实现 `order_by(id.desc()).limit(50)` 在真机上取到的 50 条
    **全是同一部剧**（封神英雄榜）——窗口被单部剧的集数吃满。
    """
    S, _ = env
    from app.identify_batch import select_identify_batch
    from app.models import Resource

    with S() as db:
        _seed(db, works=10, per_work=20)
        rows, info = select_identify_batch(db, limit=50, force=True, prefer_new=False)

        assert len(rows) == 50, f"应取满 50 条，实际 {len(rows)}"
        works = {r.path_hint for r in rows}
        assert len(works) == 10, \
            f"一批 50 条必须覆盖全部 10 部作品，实际只覆盖 {len(works)} 部：{sorted(works)}"
        # 每部作品至少 1 条、且不出现「某部占了 50 条」这种退化分布
        counts = {}
        for r in rows:
            counts[r.path_hint] = counts.get(r.path_hint, 0) + 1
        assert max(counts.values()) <= 10, \
            f"单部作品不得超过 10 条（分层轮转应均摊），实际 {counts}"


def test_batch_selection_advances_to_uncovered_ids_across_rounds(env):
    """连点几轮必须依次覆盖不同的 id 段，而不是永远重复最新那批。

    反证点：旧实现每次都取 id 最大的 50 条 → 1215 条从未被覆盖。
    """
    S, _ = env
    from app.identify_batch import mark_attempted, select_identify_batch

    with S() as db:
        _seed(db, works=5, per_work=20)  # 100 条
        seen_first_round = None
        all_picked = []
        for _round in range(4):
            rows, info = select_identify_batch(db, limit=25, force=True, prefer_new=False)
            assert rows, "每轮都应取到候选"
            if seen_first_round is None:
                seen_first_round = {r.id for r in rows}
            for r in rows:
                mark_attempted(r)
            all_picked.append({r.id for r in rows})
            db.commit()

        # 第 2 轮起不得与第 1 轮完全重合
        assert all_picked[1] != all_picked[0], "第 2 轮必须换一批资源"
        assert all_picked[2] != all_picked[1], "第 3 轮必须换一批资源"
        # 4 轮 × 25 条 = 100 条 = 全库 → 必须覆盖到最早的 id
        picked_all = set().union(*all_picked)
        assert len(picked_all) == 100, f"4 轮应覆盖全库 100 条，实际 {len(picked_all)}"
        assert min(picked_all) <= 5, \
            f"必须覆盖到最早的几条（老的 1215 条才是真机痛点），实际最小 id {min(picked_all)}"


def test_batch_selection_prefers_unidentified_when_not_forced(env):
    """非 force 模式只取待识别项；force 模式才带上已识别项（用于修正误配）。"""
    S, _ = env
    from app.identify_batch import select_identify_batch
    from app.models import Resource

    with S() as db:
        _seed(db, works=3, per_work=4)
        db.add(Resource(file_id="DONE", name="已识别集", path_hint="W0",
                        identify_status="identified"))
        db.commit()

        rows, _ = select_identify_batch(db, limit=50, force=False, prefer_new=False)
        assert all(r.identify_status != "identified" for r in rows), \
            "非 force 模式不得把已识别项取进来"

        rows, _ = select_identify_batch(db, limit=50, force=True, prefer_new=False)
        assert any(r.identify_status == "identified" for r in rows), \
            "force 模式必须能把已识别项取进来（修正误配）"


def test_batch_selection_empty_library_returns_empty(env):
    """空库不得抛异常。"""
    S, _ = env
    from app.identify_batch import select_identify_batch

    with S() as db:
        rows, info = select_identify_batch(db, limit=50, force=True, prefer_new=False)
        assert rows == []
        assert int(info.get("works") or 0) == 0


def test_mark_attempted_increments_round(env):
    """参与轮次只增不减，且从 0 起。"""
    S, _ = env
    from app.identify_batch import mark_attempted
    from app.models import Resource

    with S() as db:
        db.add(Resource(file_id="R1", name="作品", path_hint="W", identify_status="unidentified"))
        db.commit()
        row = db.scalar(select(Resource))
        assert int(row.identify_round or 0) == 0
        mark_attempted(row)
        assert int(row.identify_round or 0) == 1
        mark_attempted(row)
        assert int(row.identify_round or 0) == 2


def test_count_pending_reports_real_backlog(env):
    """待识别存量必须能被如实统计（前端要显示「库里还有多少项没认出来」）。"""
    S, _ = env
    from app.identify_batch import count_pending
    from app.models import Resource

    with S() as db:
        _seed(db, works=2, per_work=3)  # 6 条未识别
        db.add(Resource(file_id="OK", name="已识别", path_hint="W0",
                        identify_status="identified"))
        db.commit()
        assert count_pending(db) == 6, "待识别存量必须只算未识别/待重试的"


# ===========================================================================
# A2. 后台补跑全库（一次点击覆盖整个资源库）
# ===========================================================================

def test_background_identify_all_covers_whole_library(env):
    """后台补跑必须一直跑到存量清零 —— 这正是真机「快速补跑全库」的诉求。

    反证点：单批 200 条要跑完 1215 条得让用户点 7 次；后台任务一次跑完。
    """
    S, _ = env
    from app import main
    from app.models import Resource

    with S() as db:
        _seed(db, works=6, per_work=10)  # 60 条

    async def fake_identify(name, x, db_):
        return {"ok": True, "best": {"tmdb_id": 1, "media_type": "tv",
                                     "title": name, "overview": "",
                                     "poster_path": "", "vote_average": 0,
                                     "genres": [], "origin_country": []},
                "source": "tmdb", "reason": "", "status": "identified"}

    # 后台任务自己开 Session（SessionLocal 指向全局库），测试里改成同一个引擎
    with patch("app.main.SessionLocal", S), \
         patch("app.main._identify_with_source_hints", fake_identify):
        asyncio.run(main._run_identify_all_async(force=True))

    st = main._identify_all_state
    assert st["scanned"] >= 60, f"后台补跑必须覆盖全库 60 条，实际 {st['scanned']}"
    assert st["identified"] >= 60, f"识别数应覆盖全库，实际 {st['identified']}"
    assert st["remaining"] == 0, f"跑完后不应还有未识别项，实际 {st['remaining']}"
    assert not st["running"]
    assert not st["error"], f"后台任务不得报错：{st['error']}"

    with S() as db:
        rows = list(db.scalars(select(Resource)).all())
        works = {r.path_hint for r in rows if r.identify_status == "identified"}
        assert len(works) == 6, f"应覆盖 6 部作品，实际 {len(works)}"


def test_background_identify_all_stops_on_rate_limit_without_losing_progress(env):
    """限流必须停下并保住已完成进度，不得把剩余资源打成失败。"""
    S, _ = env
    from app import main
    from app.models import Resource

    with S() as db:
        _seed(db, works=4, per_work=5)  # 20 条

    async def fake_identify(name, x, db_):
        from app.tmdb import TmdbRateLimited
        raise TmdbRateLimited("调用过于频繁")

    with patch("app.main.SessionLocal", S), \
         patch("app.main._identify_with_source_hints", fake_identify), \
         patch("app.main.tmdb.rate_limit_wait_seconds", return_value=0.0):
        asyncio.run(main._run_identify_all_async(force=True))

    st = main._identify_all_state
    assert not st["error"], f"限流不算失败：{st['error']}"
    with S() as db:
        rows = list(db.scalars(select(Resource)).all())
        assert all(r.identify_status in ("unidentified", "rate_limited") for r in rows), \
            "限流不得把资源打成失败"
        assert not any(r.identify_status == "failed" for r in rows), \
            "限流不得写死失败（会永久放弃重试）"


def test_background_identify_all_endpoints_contract(env):
    """后台补跑端点契约：启动立即返回、状态可查。"""
    S, _ = env
    from app import main

    assert hasattr(main, "identify_all_async")
    assert hasattr(main, "identify_all_async_status")
    src = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")
    assert '@app.post("/api/resources/identify-all/async"' in src
    assert '@app.get("/api/resources/identify-all/async"' in src
    # 状态字段一个不能少（前端进度条与结果提示依赖）
    for key in ("running", "rounds", "scanned", "identified", "pending",
                "remaining", "stage", "error"):
        assert f'"{key}"' in src, f"状态字段 {key} 不能缺"


# ===========================================================================
# B. 端点契约：identify-all 返回真实口径
# ===========================================================================

def test_identify_all_endpoint_returns_real_scope(env):
    """端点必须回报「覆盖了几部作品 / 库里还剩多少」，别让用户误以为全库重识别了。"""
    S, _ = env
    from app import main

    with S() as db:
        _seed(db, works=4, per_work=5)  # 20 条

        async def fake_identify(name, x, db_):
            return {"ok": True, "best": {"tmdb_id": 1, "media_type": "tv",
                                         "title": name, "overview": "",
                                         "poster_path": "", "vote_average": 0,
                                         "genres": [], "origin_country": []},
                    "source": "tmdb", "reason": "", "status": "identified"}

        with patch("app.main._identify_with_source_hints", fake_identify):
            out = asyncio.run(main.identify_all(force=True, limit=10, db=db))

        assert out["ok"] is True
        assert out["scanned"] == 10
        assert out["identified"] == 10
        # 真实口径字段
        assert "works" in out and "remaining" in out and "deferred" in out, \
            f"端点必须回报真实口径字段，实际返回键 {sorted(out)}"
        assert out["works"] == 4, f"10 条应覆盖 4 部作品，实际 {out['works']}"
        assert out["remaining"] == 10, \
            f"识别成功后待识别存量应剩 10，实际 {out['remaining']}"


def test_identify_all_endpoint_keeps_legacy_keys(env):
    """老返回键必须保留（前端其它地方/历史脚本可能依赖）。"""
    S, _ = env
    from app import main

    with S() as db:
        _seed(db, works=1, per_work=2)

        async def fake_identify(name, x, db_):
            return {"ok": False, "best": None, "source": "none",
                    "reason": "未找到候选", "status": "none"}

        with patch("app.main._identify_with_source_hints", fake_identify):
            out = asyncio.run(main.identify_all(force=True, limit=5, db=db))

        for key in ("ok", "scanned", "identified", "pending", "queued"):
            assert key in out, f"老返回键 {key} 不能丢"


def test_identify_all_endpoint_stops_on_rate_limit(env):
    """限流必须**中止本轮**（剩余保持原状），不得把整批打成待重试。

    反证点：旧实现是 `continue` —— 配额已耗尽还继续发请求，
    后面每一条都被判限流，真机上 40 条资源被一次性标成「待重试」，
    看起来像「这批剧全都识别不出来」。
    """
    S, _ = env
    from app import main
    from app.models import Resource

    with S() as db:
        _seed(db, works=3, per_work=4)  # 12 条

        async def fake_identify(name, x, db_):
            from app.tmdb import TmdbRateLimited
            raise TmdbRateLimited("调用过于频繁")

        with patch("app.main._identify_with_source_hints", fake_identify):
            out = asyncio.run(main.identify_all(force=True, limit=12, db=db))
        db.commit()

        db.expire_all()
        rows = list(db.scalars(select(Resource)).all())
        limited = [r for r in rows if r.identify_status == "rate_limited"]
        untouched = [r for r in rows if r.identify_status == "unidentified"]
        assert len(limited) == 1, \
            f"限流后只应标 1 条待重试，实际 {len(limited)} 条被污染"
        assert len(untouched) == 11, \
            f"其余 11 条必须保持未识别（下一轮再试），实际 {len(untouched)}"
        assert out["deferred"] == 11, f"必须如实回报推迟条数，实际 {out['deferred']}"


# ===========================================================================
# C. 自动管线：_post_scan_identify 同样按作品分散
# ===========================================================================

def test_post_scan_identify_also_spreads_across_works(env):
    """自动识别管线也必须按作品分散，不能只扫最新 500 条。

    用户报障②：「资源库只自动识别了手动重试的这部剧，之前出来的剧没有自动识别」
    —— 根因就是 `_post_scan_identify` 同样是 `id.desc()` 固定窗口。
    """
    S, _ = env
    from app import main
    from app.models import Resource

    with S() as db:
        _seed(db, works=6, per_work=4)  # 24 条

        async def fake_identify(name, x, db_):
            return {"ok": True, "best": {"tmdb_id": 1, "media_type": "tv",
                                         "title": name, "overview": "",
                                         "poster_path": "", "vote_average": 0,
                                         "genres": [], "origin_country": []},
                    "source": "tmdb", "reason": ""}

        with patch("app.main.identify_with_a_source_fallback", fake_identify):
            asyncio.run(main._post_scan_identify(db, publish_after_identify=False, limit=12))
        db.commit()

        db.expire_all()
        rows = list(db.scalars(select(Resource)).all())
        works = {r.path_hint for r in rows if r.identify_status == "identified"}
        assert len(works) == 6, \
            f"12 条必须覆盖全部 6 部作品，实际只覆盖 {len(works)} 部"


def test_post_scan_identify_keeps_stats_keys(env):
    """既有 stats 键一个不能少（进度看板/诊断面板依赖）。"""
    S, _ = env
    from app import main

    with S() as db:
        _seed(db, works=1, per_work=2)

        async def fake_identify(name, x, db_):
            return {"ok": True, "best": {"tmdb_id": 1, "media_type": "tv",
                                         "title": name, "overview": "",
                                         "poster_path": "", "vote_average": 0,
                                         "genres": [], "origin_country": []},
                    "source": "tmdb", "reason": ""}

        with patch("app.main.identify_with_a_source_fallback", fake_identify):
            stats = asyncio.run(main._post_scan_identify(db, publish_after_identify=False, limit=10))

        for key in ("identified", "failed", "queued", "finish_backfilled",
                    "rate_limited", "enqueue_diag", "work_stats"):
            assert key in stats, f"stats 键 {key} 不能丢"


def test_post_scan_identify_still_marks_rate_limited_not_failed(env):
    """限流语义不变：标「待重试」，绝不写死失败。"""
    S, _ = env
    from app import main
    from app.models import Resource

    with S() as db:
        _seed(db, works=1, per_work=3)

        async def fake_identify(name, x, db_):
            from app.tmdb import TmdbRateLimited
            raise TmdbRateLimited("调用过于频繁")

        with patch("app.main.identify_with_a_source_fallback", fake_identify):
            stats = asyncio.run(main._post_scan_identify(db, publish_after_identify=False, limit=10))
        db.commit()

        db.expire_all()
        rows = list(db.scalars(select(Resource)).all())
        limited = [r for r in rows if r.identify_status == "rate_limited"]
        assert len(limited) == 1
        assert limited[0].identify_status != "failed"
        assert int(stats.get("rate_limited") or 0) >= 1


# ===========================================================================
# D. 迁移：老库自动补列
# ===========================================================================

def test_identify_round_column_migrates_on_old_db(env):
    """老库（无该列）启动时必须自动补列，不能因缺列报错。

    走 `db._COLUMNS` 的补列机制本身（migrate() 作用于全局 engine，
    测试库里改列不便直接验证），因此这里断言两件事：
      ① `_COLUMNS` 里登记了该列（DDL 完整）；
      ② 该 DDL 能被 SQLite 接受（语法有效）。
    真机老库由 `migrate()` 的 ALTER 分支自动补齐。
    """
    S, eng = env
    from sqlalchemy import text
    from app.db import _COLUMNS

    ddl = _COLUMNS["resources"].get("identify_round")
    assert ddl, "_COLUMNS 必须登记识别轮次列（否则老库启动不补列）"

    # DDL 语法有效性：建一张只有该列的表，若 DDL 非法这里会直接抛错
    with eng.begin() as conn:
        conn.execute(text(f"CREATE TABLE _t_round (identify_round {ddl})"))
        conn.execute(text("ALTER TABLE _t_round ADD COLUMN identify_round2 " + ddl))
        conn.execute(text("DROP TABLE _t_round"))


# ===========================================================================
# E. 前端契约：如实展示批次范围
# ===========================================================================

def test_frontend_identify_button_not_hardcoded_limit_50():
    """前端不得再把批次硬编码成 50 条。

    红线关联：真机 1215 条资源从未被覆盖，根因之一就是这条硬编码。
    """
    app = APP_VUE.read_text(encoding="utf-8")
    assert "identify-all?force=true&limit=50" not in app, \
        "前端不得把重识别批次硬编码成 50 条"
    assert "identify-all" in app, "「按目录重识别」按钮不能删"


def test_frontend_uses_background_identify_all():
    """前端必须走后台补跑（一次点击覆盖整个资源库），不得只跑单批就结束。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "/resources/identify-all/async" in app, \
        "前端必须走后台批量重识别（轮询进度），否则用户要点几十次"
    # 单批同步端点不应再被前端直接调用
    assert "identify-all?force=true&limit=" not in app, \
        "前端不得再直接调用单批同步端点"


def test_frontend_shows_real_scope_not_all_library_claim():
    """前端提示必须说清「本次处理 N 条」，不得让用户误以为是全库重识别。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "本次处理" in app, "前端提示必须写清本批真实处理量"
    # 不得出现「已全库」这类夸大口径（注释里也不许出现，避免文案歧义）
    for bad in ("已全库", "全部资源重新识别完毕", "全库识别完成"):
        assert bad not in app, f"前端不得出现夸大口径：{bad}"


def test_frontend_does_not_leak_backend_identify_round_field():
    """红线：页面不得体现后端代码字段名。"""
    app = APP_VUE.read_text(encoding="utf-8")
    for code in ("identify_round", "identify_stamp"):
        assert code not in app, f"前端不得出现后端字段名：{code}"
