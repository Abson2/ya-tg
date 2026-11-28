"""v2.1.67 内存优化：热路径投影化 + 堆回收兜底。

真机背景（2026-11-29，容器 v2.1.66 跑 17 分钟）：RSS 1.33 GB / VmHWM 1.36 GB /
RssAnon 1.28 GB（RssFile 仅 28 MB，说明全是 Python 堆）/ CPU 38%，
日志里 `_job_share_center` 每 5 秒都是 "maximum number of running instances reached"。

容器内实测定位到三个叠加来源：
  ① `sync_library_versions` 每 5 秒整表实例化 2.59 万行 `Resource` 实体
     —— 单次常驻 134.8 MiB / 峰值 147.9 MiB，且连跑 8 轮 RSS 每轮 +2.2 MiB
     单调上涨（投影版同样计算：14.8 MiB，8 轮零增长，快一倍）；
  ② SQLite 连接池每条连接 9.5 MiB page cache（`cache_size=-8192`），
     job 池 8+8 / API 池 10+20 最多 46 条，常态约 171 MiB 且归还后不释放；
  ③ allocator 碎片不还 OS —— `gc.collect()` 只回 11.6 MiB，
     `malloc_trim(0)` 一次回 45.7 MiB。

这里守住三件事，任何一条回退都会让内存重新爬到 1 GB 以上：
  A. 热路径必须用窄投影，不许退回 `db.scalars(select(Resource)).all()`；
  B. `cache_size` 不许再被调大；
  C. `malloc_trim` 必须接在调度周期上，且在没有它的平台（macOS/musl）静默降级。
"""
import ast

import pytest

from app import mem_trim as MEM

from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
APP = REPO / "backend" / "app"


def _src(name: str) -> str:
    return (APP / name).read_text(encoding="utf-8")


def _func_src(name: str, func: str) -> str:
    """取某个函数/方法的源码文本（用 AST 定位，避免正则被同名串骗到）。"""
    src = _src(name)
    tree = ast.parse(src)
    lines = src.splitlines()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == func:
            return "\n".join(lines[node.lineno - 1:node.end_lineno])
    raise AssertionError(f"{name} 里找不到 {func}")


# ---------- A. 热路径投影化 ----------
def test_sync_library_versions_uses_projection():
    """5 秒周期的入口绝不能再整表实体化——这是 RSS 1.3 GB 的主因。"""
    body = _func_src("share_center_v2.py", "sync_library_versions")
    assert "_SYNC_VERSION_COLS" in body
    assert "scalars(select(Resource))" not in body


def test_sync_version_cols_cover_every_consumer():
    """投影列必须覆盖所有下游读取点，少一列就是 AttributeError 或数据漂移。"""
    # 分组键 6 列（utils.work_group_key）
    need = {"file_id", "path_hint", "share_dir_name", "media_title",
            "tmdb_id", "media_type"}
    # 稳定序
    need |= {"size", "utime"}
    # _primary_category
    need |= {"genres"}
    # utils.work_display_meta
    need |= {"parent_id", "poster_path", "share_dir_id"}
    # 写快照 resource_ids_json + recompute 的兜底标题
    need |= {"id", "name"}

    src = _src("share_center_v2.py")
    tree = ast.parse(src)
    cols = None
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
                getattr(t, "id", "") == "_SYNC_VERSION_COLS" for t in node.targets):
            cols = {e.value for e in node.value.elts}
            break
    assert cols is not None, "找不到 _SYNC_VERSION_COLS 常量"
    missing = need - cols
    assert not missing, f"投影缺少下游必读列：{sorted(missing)}"


def test_recompute_work_categories_uses_projection():
    body = _func_src("share_center_v2.py", "recompute_work_categories")
    assert "_SYNC_VERSION_COLS" in body
    assert "scalars(select(Resource))" not in body


def test_rep_resolver_keeps_entities_but_skips_big_text():
    """代表资源解析器**不改数据结构**（下游读 20+ 列），只排掉确定不读的大文本。"""
    body = _func_src("share_center_v2.py", "_build")
    assert "load_only" in body
    src = _src("share_center_v2.py")
    assert '_LIGHT_SKIP = ("dir_chain_json", "thumbnail", "identify_note")' in src
    # 关键：overview / genres / origin_country 被 publisher 读过，绝不能排掉
    for must_keep in ("overview", "genres", "origin_country", "share_url"):
        assert must_keep not in src.split("_LIGHT_SKIP =")[1].split("\n")[0], \
            f"{must_keep} 被误排除，publisher 会懒加载或报错"


def test_publisher_episode_stats_uses_projection():
    """每条推送都为一部剧拉全表 → 单次 148 MiB 峰值。改成投影，筛选逻辑不变。"""
    body = _func_src("publisher.py", "_build_message")
    assert "_EPISODE_STAT_COLS" in body
    assert "scalars(select(Resource))" not in body
    # 判据仍是按 work_group_key 精确比对，不能退化成「目录名相同就算同作品」
    assert "work_group_key(s) == key" in body


# ---------- B. 连接池 / 堆回收 ----------
def test_sqlite_cache_size_back_to_default():
    """v2.1.64 把它调到 8 MiB，实测每条连接吃 9.5 MiB 且归还后不释放。"""
    src = _src("db.py")
    assert '"PRAGMA cache_size=-2048"' in src
    assert "cache_size=-8192" not in src


def test_pool_sizes_not_shrunk():
    """池容量故意不动：缩池只省约 30 MiB，却会让前端 23 个并发请求撞 5 秒超时。"""
    src = _src("db.py")
    assert "POOL_KW_JOB = dict(pool_size=8, max_overflow=8, pool_timeout=15)" in src
    assert "POOL_KW_API = dict(pool_size=10, max_overflow=20, pool_timeout=5)" in src


def test_malloc_trim_never_raises_and_reports_bool():
    """macOS / musl 没有 malloc_trim：必须静默返回 False，不能把主流程带崩。"""
    for _ in range(3):
        assert isinstance(MEM.malloc_trim(), bool)


def test_trim_every_throttles(monkeypatch):
    calls = []
    monkeypatch.setattr(MEM, "malloc_trim", lambda: calls.append(1) or True)
    monkeypatch.setattr(MEM, "_counter", 0)
    for _ in range(10):
        MEM.trim_every(3)
    assert len(calls) == 3, "每 3 次才 trim 一次（第 3/6/9 次）"


def test_trim_every_runs_every_time_when_n_is_one(monkeypatch):
    calls = []
    monkeypatch.setattr(MEM, "malloc_trim", lambda: calls.append(1) or True)
    monkeypatch.setattr(MEM, "_counter", 0)
    for _ in range(4):
        MEM.trim_every(1)
    assert len(calls) == 4


def test_trim_hooked_into_cycle_finally():
    """必须挂在 finally 上：超时/异常路径也要回收，否则卡死那轮的内存再也回不来。"""
    body = _func_src("main.py", "_run_share_center_v2_cycle_in_thread")
    assert "mem_trim.trim_every(" in body
    assert "finally:" in body
    # trim 要在 finally 里，不能只在成功分支
    finally_part = body.split("finally:")[-1]
    assert "mem_trim.trim_every(" in finally_part


def test_gc_freeze_after_startup():
    body = _func_src("main.py", "lifespan")
    assert "gc.freeze()" in body
    # 必须在 yield 之前（启动完成后冻结，而不是退出时）
    assert body.index("gc.freeze()") < body.index("yield")


# ---------- C. 等价性：换的只是取数方式，结果必须逐项相同 ----------
def _compute(rows):
    """复刻 `sync_library_versions` 的读侧全部计算，返回可比对的纯数据。"""
    from app import share_center_v2 as sc2, utils

    grouped = {}
    for res in rows:
        grouped.setdefault(utils.work_group_key(res), []).append(res)
    out = {}
    for wk, mem in grouped.items():
        ordered = sorted(mem, key=lambda r: (str(r.file_id or ""),
                                             int(r.size or 0), int(r.utime or 0)))
        category_key, media_type = sc2._work_category(ordered)
        fingerprint = __import__("hashlib").sha256(
            "|".join(sorted(f"{r.file_id}:{r.size or 0}" for r in mem)).encode("utf-8")
        ).hexdigest()
        meta = utils.work_display_meta(ordered)
        out[wk] = {"ids": [r.id for r in ordered], "cat": category_key,
                   "media_type": media_type, "fp": fingerprint,
                   "meta": {k: meta[k] for k in
                            ("title", "share_dir_id", "share_dir_name",
                             "media_type", "tmdb_id", "poster_path")}}
    return out


def _seed(db):
    """造两组作品：一组已识别有海报（正常路径），一组未识别空壳（易错路径）。"""
    from app.models import Resource

    db.add_all([
        Resource(file_id="f1", name="谷雨.S01E01.mkv", ext=".mkv", size=100, utime=1,
                 share_dir_name="谷雨 (2026)", path_hint="剧/谷雨 (2026)/Season 1",
                 parent_id="season1", share_dir_id="show1", media_type="tv",
                 media_title="谷雨", tmdb_id=111, poster_path="/p.jpg",
                 genres='["剧情"]', identify_status="identified",
                 season=1, episode=1, total_episodes=24, finish_status="returning"),
        Resource(file_id="f2", name="谷雨.S01E02.mkv", ext=".mkv", size=120, utime=2,
                 share_dir_name="谷雨 (2026)", path_hint="剧/谷雨 (2026)/Season 1",
                 parent_id="season1", share_dir_id="show1", media_type="tv",
                 media_title="谷雨", tmdb_id=111, poster_path="/p.jpg",
                 genres='["剧情"]', identify_status="identified",
                 season=1, episode=2, total_episodes=24, finish_status="returning"),
        # 未识别空壳：media_title 空、指向季目录，最能暴露「抄首行」类错误
        Resource(file_id="f3", name="谷雨.S01E03.mkv", ext=".mkv", size=130, utime=3,
                 share_dir_name="谷雨 (2026)", path_hint="剧/谷雨 (2026)/Season 1",
                 parent_id="season1", share_dir_id="season1", media_type="tv",
                 media_title="", tmdb_id=None, poster_path="", genres="",
                 identify_status="failed"),
        # 同名不同剧：靠 tmdb_id 区分，验证分组不会串
        Resource(file_id="f4", name="谷雨.S01E01.mkv", ext=".mkv", size=90, utime=4,
                 share_dir_name="谷雨 (2026)", path_hint="剧2/谷雨 (2026)",
                 parent_id="show2", share_dir_id="show2", media_type="tv",
                 media_title="谷雨·另一部", tmdb_id=222, poster_path="/q.jpg",
                 genres='["纪录片"]', identify_status="identified"),
    ])
    db.commit()


def test_projection_equals_entity_on_same_rows(tmp_path):
    """核心保证：投影版与实体版在**同一批行**上必须算出完全一样的结果。"""
    from sqlalchemy import create_engine, select
    from sqlalchemy.orm import Session as OrmSession

    from app.models import Resource

    eng = create_engine(f"sqlite:///{tmp_path}/eq.db")
    Resource.__table__.create(eng)
    cols = None
    src = _src("share_center_v2.py")
    for node in ast.parse(src).body:
        if isinstance(node, ast.Assign) and any(
                getattr(t, "id", "") == "_SYNC_VERSION_COLS" for t in node.targets):
            cols = [e.value for e in node.value.elts]
    assert cols

    with OrmSession(eng) as db:
        _seed(db)
        ent = list(db.scalars(select(Resource)).all())
        proj = db.execute(select(*[getattr(Resource, c) for c in cols])).all()

        assert len(ent) == len(proj)
        a, b = _compute(ent), _compute(proj)
        assert a == b, "投影与实体算出的作品级结果不一致"

        # 兄弟行筛选（publisher 的整剧统计）也必须一致
        from app import utils
        for tv in [r for r in ent if r.media_type == "tv"]:
            key = utils.work_group_key(tv)
            assert sorted(s.id for s in ent if utils.work_group_key(s) == key) == \
                   sorted(s.id for s in proj if utils.work_group_key(s) == key)
