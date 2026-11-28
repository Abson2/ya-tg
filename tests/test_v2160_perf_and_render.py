"""v2.1.60 回归测试：进页面数据加载慢（接口耗时 + 前端渲染）四项收口。

真机背景（2026-11-28，容器内实测，v2.1.59 线上）：
- `/api/mirror/status`  **2845 ms**
- `/api/library/series`  **1258 ms / 返回体 1168 KB**
- 资源库卡片墙 678 部作品一次性挂载（约 5,400 个 Element Plus 组件）
- `load()` 同时发 23 个 GET，之后还串行 await 6 个模块

四项收口：
① `_invalid_media_rows` 逐部 COUNT（678 条 SQL）→ 一条 DISTINCT 取集合。
② `mirror_status` 两张清单的 `files` 逐部 COUNT（200+N 条）→ 一条 GROUP BY。
③ `library_series` 去掉从不渲染的 `overview` 列（返回体 -465 KB）+ 作品名/分类记忆化。
④ 前端卡片墙分批渲染（首屏 60）+ `load()` 先发首屏关键 8 项、其余空闲补。

每条都带反证：把口径改回去必须变红。
"""
from pathlib import Path

import pytest

from app import tmdb
from app.db import SessionLocal, migrate
from app.main import (library_series, _file_counts_by_media, _genre_pair,
                      _invalid_media_rows, _synced_media_ids)
from app.models import MirrorFile, MirrorMedia, MirrorRule, Resource

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
MAIN = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
UTILS = (ROOT / "backend/app/utils.py").read_text(encoding="utf-8")

RULE_NAME = "v2160-镜像"


def _clean():
    db = SessionLocal()
    rules = db.query(MirrorRule).filter(MirrorRule.name == RULE_NAME).all()
    for r in rules:
        db.query(MirrorFile).filter(MirrorFile.rule_id == r.id).delete(
            synchronize_session=False)
        db.query(MirrorMedia).filter(MirrorMedia.rule_id == r.id).delete(
            synchronize_session=False)
        db.delete(r)
    db.query(Resource).filter(Resource.file_id.like("v2160-%")).delete(
        synchronize_session=False)
    db.commit()
    db.close()


@pytest.fixture()
def env():
    migrate()
    _clean()
    db = SessionLocal()
    rule = MirrorRule(name=RULE_NAME, source_root_id="A_ROOT",
                      target_root_id="B_ROOT", enabled=True)
    db.add(rule)
    db.flush()
    # ① 有 synced 文件 → 有效映射
    m_synced = MirrorMedia(rule_id=rule.id, work_key="v2160-synced",
                           title_standard="有文件 (2026)", title_mirror="有文件 (2026)",
                           source_dir_id="A1", target_dir_id="B1",
                           category_path="影视/电视剧", status="synced")
    # ② 无任何文件、非进行中 → 历史无效映射
    m_zombie = MirrorMedia(rule_id=rule.id, work_key="v2160-zombie",
                           title_standard="僵尸行 (2026)", title_mirror="僵尸行 (2026)",
                           source_dir_id="A2", target_dir_id="B2",
                           category_path="影视/电视剧", status="blocked")
    # ③ 无文件但在进行中 → 绝不许判成无效（会误删正在同步的作品）
    m_busy = MirrorMedia(rule_id=rule.id, work_key="v2160-busy",
                         title_standard="在同步 (2026)", title_mirror="在同步 (2026)",
                         source_dir_id="A3", target_dir_id="B3",
                         category_path="影视/电视剧", status="copying")
    db.add_all([m_synced, m_zombie, m_busy])
    db.flush()
    db.add_all([
        MirrorFile(media_id=m_synced.id, rule_id=rule.id, identity="v2160-e1",
                   source_file_id="A_F1", file_name="A.S01E01.mkv",
                   target_file_id="B_F1", fingerprint="1:1", status="synced"),
        MirrorFile(media_id=m_synced.id, rule_id=rule.id, identity="v2160-e2",
                   source_file_id="A_F2", file_name="A.S01E02.mkv",
                   target_file_id="B_F2", fingerprint="1:2", status="failed"),
    ])
    db.commit()
    yield {"db": db, "rule": rule, "synced": m_synced,
           "zombie": m_zombie, "busy": m_busy}
    db.close()
    _clean()


# ---------- ① _invalid_media_rows 不再逐部 COUNT ----------
def test_invalid_media_rows_uses_one_distinct_query():
    seg = MAIN[MAIN.index("def _invalid_media_rows("):]
    seg = seg[:seg.index("def _synced_media_ids(")]
    # 反证：逐部 COUNT 的写法不许回来
    assert '.filter(\n            MirrorFile.media_id == m.id, MirrorFile.status == "synced").count()' not in seg
    assert "MirrorFile.status == \"synced\").count()" not in seg
    # 必须走集合判据
    assert "synced_ids = _synced_media_ids(db)" in seg
    assert "int(m.id) not in synced_ids" in seg


def test_synced_media_ids_semantics(env):
    db = env["db"]
    got = _synced_media_ids(db)
    assert env["synced"].id in got
    # 只有 failed 文件的不算「有落地文件」
    assert env["zombie"].id not in got
    assert env["busy"].id not in got


def test_invalid_media_rows_matches_old_judgement(env):
    """判据等价：无 synced 文件 + 非进行中 → 无效映射。"""
    db = env["db"]
    ids = [m.id for m in _invalid_media_rows(db, env["rule"].id)]
    assert env["zombie"].id in ids
    assert env["synced"].id not in ids, "有 synced 文件的映射不许被判成无效"
    assert env["busy"].id not in ids, "进行中的映射不许被判成无效（会误删正在同步的作品）"


# ---------- ② mirror_status 的 files 计数一次 GROUP BY ----------
def test_file_counts_by_media_matches_per_row_count(env):
    db = env["db"]
    counts = _file_counts_by_media(db, [env["synced"].id, env["zombie"].id])
    real_synced = db.query(MirrorFile).filter(
        MirrorFile.media_id == env["synced"].id).count()
    assert counts.get(env["synced"].id, 0) == real_synced == 2
    # 没有文件的行不出现在字典里，调用方按 0 取（与逐部 count 一致）
    assert counts.get(env["zombie"].id, 0) == 0


def test_mirror_status_no_per_row_file_count():
    seg = MAIN[MAIN.index("@app.get(\"/api/mirror/status\""):]
    seg = seg[:seg.index("@app.get(\"/api/mirror/batch/status\"")]
    # 反证：两张清单里的逐部 count 不许回来
    assert "db.query(MirrorFile).filter(MirrorFile.media_id == m.id).count()" not in seg
    assert "db.query(MirrorFile).filter(\n                               MirrorFile.media_id == m.id).count()" not in seg
    # 一次聚合 + 集合判定
    assert "_file_counts = _file_counts_by_media(db)" in seg
    assert "_invalid_ids = {int(m.id) for m in invalid_media}" in seg
    assert '"files": _file_counts.get(int(m.id), 0)' in seg
    assert "int(m.id) in _invalid_ids" in seg


# ---------- ③ library_series：去掉 overview、作品名/分类记忆化 ----------
def test_library_series_projection_drops_overview():
    seg = MAIN[MAIN.index("@app.get(\"/api/library/series\""):]
    seg = seg[:seg.index("@app.get(\"/api/library/finish-check\"")]
    # 反证：简介列不许回到投影里（24,096 行 × 约 700 字节 = 多读 16 MB）
    # 注释里允许提这个名字，所以只断言真正的代码写法
    assert "Resource.poster_path, Resource.overview" not in seg
    assert '"overview": x.overview' not in seg
    assert 'g["overview"] = x.overview' not in seg
    # 仍必须是列投影，不许退回全量 ORM 实体（v2.1.59 红线）
    assert "select(*_cols)" in seg
    assert "select(Resource).order_by(Resource.id.desc())" not in seg


def test_library_series_no_overview_in_response_keeps_ids(env):
    db = env["db"]
    db.add_all([
        Resource(file_id="v2160-r1", name="谷雨.S01E01.mkv", ext=".mkv", size=1, utime=1,
                 media_title="谷雨", media_type="tv", tmdb_id=991060,
                 identify_status="identified", genres='["剧情"]',
                 overview="这是一段很长的简介正文", season=1, episode=1),
        Resource(file_id="v2160-r2", name="谷雨.S01E02.mkv", ext=".mkv", size=1, utime=1,
                 media_title="谷雨", media_type="tv", tmdb_id=991060,
                 identify_status="identified", genres='["剧情"]',
                 overview="这是一段很长的简介正文", season=1, episode=2),
    ])
    db.commit()
    rows = library_series(q="", media_type="", db=db)
    g = next(x for x in rows if x.get("tmdb_id") == 991060)
    assert "overview" not in g, "简介已从返回体裁掉（卡片从不渲染它）"
    assert len(g["ids"]) == 2, "成员 id 列表必须保留（前端全选/批量操作用）"
    assert g["count"] == 2 and g["season_count"] == 1


def test_genre_pair_keeps_tmdb_as_single_source():
    """分类归一仍只走 tmdb.genre_name_cn（v2.1.54 红线）。"""
    raw = "Sci-Fi & Fantasy"
    primary, names = _genre_pair(f'["{raw}", "剧情"]')
    assert primary == tmdb.genre_name_cn(raw)
    assert "剧情" in names
    # 记忆化不许改变结果：同一串两次调用必须一致
    assert _genre_pair(f'["{raw}", "剧情"]') == (primary, names)
    # 空串不猜分类
    assert _genre_pair("") == ("", ())


def test_normalize_work_name_memoized_and_same_result():
    from app.utils import normalize_work_name
    a = normalize_work_name("大·醉·侠 (2002) {tmdb-135646}")
    b = normalize_work_name("大·醉·侠 (2002) {tmdb-135646}")
    assert a == b
    assert normalize_work_name.cache_info().hits >= 1, "必须真的命中记忆化"
    # 反证：装饰器不许被摘掉
    assert "@lru_cache(maxsize=8192)\ndef normalize_work_name" in UTILS


# ---------- ④ 前端：卡片墙分批渲染 ----------
def test_series_wall_renders_in_batches():
    assert "const SERIES_BATCH = 60" in APP
    assert "function visibleSeriesItems(items)" in APP
    # 两个视图的 v-for 都必须走可见集合
    assert 'v-for="g in visibleSeriesItems(grp.items)"' in APP
    assert 'v-for="g in visibleSeriesItems(filteredSeries)"' in APP
    # 反证：直出全部卡片的两处写法都不许回来
    assert 'v-for="g in grp.items"' not in APP
    assert 'v-for="g in filteredSeries"' not in APP
    # 滚到底自动追加 + 可点的兜底
    assert 'ref="seriesSentinel"' in APP
    assert "IntersectionObserver" in APP
    # 所有按钮一个不少（用户红线）：四个操作入口仍在卡片里
    assert ">推送</el-button>" in APP
    assert ">识别</el-button>" in APP
    assert ">手动修正</el-button>" in APP


def test_load_sends_critical_first_then_idle():
    seg = APP[APP.index("async function load()"):]
    seg = seg[:seg.index("async function loadLogs()")]
    assert "const LOAD_CRITICAL = 8" in APP, "首屏关键接口必须先发"
    assert "await Promise.allSettled(keys.slice(0, LOAD_CRITICAL).map((k, i) => fire(k, i)))" in seg
    assert "whenIdle(() => {" in seg
    # 反证：6 个模块串行 await 的旧写法不许回来
    assert "try { await loadSc() } catch (e) {}" not in seg
    # 完成语义不变：仍然逐条回填
    assert ".then(v => applyLoadItem(i, v)).catch(() => applyLoadItem(i, null))" in seg


def test_version_locked_to_current():
    """版本锁跟着当前版本走（本文件其余用例锁的仍是 v2.1.60 的行为）。"""
    assert 'app = FastAPI(title="Ya-TG", version="2.1.61"' in MAIN
    assert '"version": "2.1.61"' in MAIN
    assert "v2.1.61 · Asia/Shanghai" in APP
