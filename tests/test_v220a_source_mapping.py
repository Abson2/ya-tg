"""v2.1.10 资源库「映射回 A 库」+ 类型锁定防串台专项验收。

用户口径（原话）：
  1. 「资源库识别和展示都是要用映射的A库正确的影视名和tmdb影视号
     （包含手动修正的输入窗口展示等），别用映射后的B库等名称等可能会识别不出来」
  2. 「电影、电视剧要做判断识别，以免相同影号的影视识别串了」

事实基础：
  - B 库改过名：单集名是英文译名、影视主目录带 `·` 标记 → 拿去 TMDB 搜几乎必然零命中；
  - A 库作品目录名带 `{tmdb-135646}` 标记，但**只有数字没有类型**；
  - **TMDB 的 movie / tv 是两套完全独立的 ID 命名空间**：
      movie/283995 → 《银河护卫队2》   tv/283995 → 印度剧《Indrani》
    同一个数字在两个空间里是完全不同的作品（#247 的串台根因）；
  - A 库类型线索（按可靠度）：
      ① MirrorFile.identity 第二段（`tmdb:{id}:{type}:S..:E..`，同步时按
         A 库原文件名 + A 库完整路径判定，已含季集号/分类目录/名称关键词）；
      ② A 库分类目录链 MirrorMedia.category_path（如「电影/国产」「剧集/欧美剧」）；
      ③ MirrorMedia.media_type。

覆盖：
  1. 模型与迁移：Resource 增 4+1 列（A 库作品名/原文件名/影视号/类型/分类目录链）；
  2. 回填：按 Resource.file_id == MirrorFile.target_file_id 关联回填；
  3. 回填：类型按 identity → 分类目录 → media_type 三级判定，判不出**留空不猜**；
  4. 识别线索：source_query_hint 用 A 库原名 + A 库作品名 + A 库分类目录链；
  5. 类型锁定：tmdb.identify(forced_tmdb_id/forced_type) 在 **ID 阶段与名称阶段**
     都锁定同一个命名空间，绝不跨命名空间试探；
  6. 串台反证：movie/283995 与 tv/283995 同时存在时，锁 movie 只取 movie；
  7. 识别入口：4 个入口全部走 A 库线索，手动/force 绕过缓存；
  8. 展示：library_series 标题/类型用 A 库口径，回传映射字段；
  9. 前端：手动修正窗口用 A 库剧名 + 影视号预填，且不暴露后端字段名。
"""
import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = ROOT / "frontend" / "src" / "App.vue"


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v220a_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'a.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    from sqlalchemy.orm import sessionmaker
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _seed_mirror(db, *, title, tmdb_id=135646, category="电影/国产",
                 identity="tmdb:135646:tv:S01:E03", file_name="大军师司马懿.S01E03.mkv",
                 target_file_id="bf_1", media_type=""):
    """造一条 A→B 映射（MirrorRule + MirrorMedia + MirrorFile）与一条资源库行。"""
    from app.models import MirrorFile, MirrorMedia, MirrorRule, Resource
    rule = db.scalar(__import__("sqlalchemy").select(MirrorRule).limit(1))
    if rule is None:
        rule = MirrorRule(name="默认镜像", source_root_id="a_root", target_root_id="b_root")
        db.add(rule)
        db.flush()
    media = MirrorMedia(rule_id=rule.id, work_key="work:dstsmy",
                         title_standard=title,
                         title_mirror=title,
                         tmdb_id=tmdb_id,
                         media_type=media_type,
                         category_path=category)
    db.add(media)
    db.flush()
    mf = MirrorFile(media_id=media.id, rule_id=rule.id,
                    source_file_id=f"af_{target_file_id}",
                    target_file_id=target_file_id, file_name=file_name,
                    identity=identity, status="synced")
    db.add(mf)
    res = Resource(file_id=target_file_id, name="The.Silent.Storm.S01E03.mkv",
                   path_hint="The·Silent·Storm/Season 1",
                   parent_id="p1", size=1024, ext=".mkv")
    db.add(res)
    db.flush()
    return res, mf, media


# ---------------------------------------------------------------------------
# 1. 模型与迁移
# ---------------------------------------------------------------------------

def test_resource_has_source_mapping_columns(env):
    """Resource 增 A 库映射列。"""
    S, _ = env
    db = S()
    from app.models import Resource
    cols = {c.name for c in Resource.__table__.columns}
    for name in ("source_title", "source_name", "source_tmdb_id",
                 "source_media_type", "source_category"):
        assert name in cols, f"Resource 缺列：{name}"
    db.close()


def test_db_migration_declares_source_columns():
    """老库 ALTER 补列的 DDL 必须齐全。"""
    from app.db import _COLUMNS
    cols = _COLUMNS["resources"]
    for name in ("source_title", "source_name", "source_tmdb_id",
                 "source_media_type", "source_category"):
        assert name in cols, f"迁移 DDL 缺列：{name}"


# ---------------------------------------------------------------------------
# 2~3. 回填
# ---------------------------------------------------------------------------

def test_backfill_fills_a_source_mapping(env):
    """回填：作品名去标记留年份 + 原文件名 + 影视号 + 类型。"""
    S, _ = env
    db = S()
    res, _mf, _media = _seed_mirror(
        db, title="大军师司马懿 (2017) {tmdb-135646}",
        identity="tmdb:135646:tv:S01:E03")
    from app.main import backfill_source_titles
    n = backfill_source_titles(db)
    assert n == 1, "应回填 1 行"
    db.refresh(res)
    assert res.source_title == "大军师司马懿 (2017)", \
        "A 库作品名应保留年份、去掉 {tmdb-} 标记"
    assert res.source_name == "大军师司马懿.S01E03.mkv", "应回填 A 库原文件名"
    assert res.source_tmdb_id == 135646, "应回填 A 库 TMDB 影视号"
    assert res.source_media_type == "tv", "类型应取 identity 的第二段"
    db.close()


def test_backfill_type_from_category_when_identity_unknown(env):
    """identity 类型为 unknown 时，用 A 库分类目录链定类型。"""
    S, _ = env
    db = S()
    res, _mf, _media = _seed_mirror(
        db, title="某电影 (2020) {tmdb-999}", category="电影/欧美",
        identity="tmdb:999:unknown:S00:E00")
    from app.main import backfill_source_titles
    backfill_source_titles(db)
    db.refresh(res)
    assert res.source_media_type == "movie", "「电影」分类目录应定为 movie"
    assert res.source_category == "电影/欧美", "应回填 A 库分类目录链"
    db.close()


def test_backfill_type_from_category_tv(env):
    """「剧集」分类目录链 → tv。"""
    S, _ = env
    db = S()
    res, _mf, _media = _seed_mirror(
        db, title="某剧 (2021) {tmdb-888}", category="剧集/欧美剧",
        identity="tmdb:888:unknown:S00:E00")
    from app.main import backfill_source_titles
    backfill_source_titles(db)
    db.refresh(res)
    assert res.source_media_type == "tv", "「剧集」分类目录应定为 tv"
    db.close()


def test_backfill_type_unknown_stays_empty_not_guessed(env):
    """**判不出类型就留空，绝不猜**——猜错类型比不认更糟。"""
    S, _ = env
    db = S()
    res, _mf, _media = _seed_mirror(
        db, title="不明作品 (2022) {tmdb-777}", category="合集/其它",
        identity="tmdb:777:unknown:S00:E00")
    from app.main import backfill_source_titles
    backfill_source_titles(db)
    db.refresh(res)
    assert res.source_media_type == "", "类型判不出必须留空，不能瞎猜"
    assert res.source_tmdb_id == 777, "影视号仍应回填（类型可后续补）"
    db.close()


def test_backfill_is_idempotent_and_only_fills_empty(env):
    """只处理没有映射的行；重复回填不重复写。"""
    S, _ = env
    db = S()
    _seed_mirror(db, title="大军师司马懿 (2017) {tmdb-135646}")
    from app.main import backfill_source_titles
    assert backfill_source_titles(db) == 1
    db.commit()
    assert backfill_source_titles(db) == 0, "已回填的行不应再改"
    db.close()


def test_backfill_no_match_is_noop(env):
    """找不到 A 库映射的行不动（不编造）。"""
    S, _ = env
    db = S()
    from app.models import Resource
    from app.main import backfill_source_titles
    db.add(Resource(file_id="orphan", name="X.mkv", path_hint="X",
                    parent_id="p1", size=1, ext=".mkv"))
    db.flush()
    assert backfill_source_titles(db) == 0
    db.close()


# ---------------------------------------------------------------------------
# 4. 识别线索
# ---------------------------------------------------------------------------

def test_source_query_hint_uses_a_library_clues(env):
    """识别线索用 A 库原名 + A 库作品名 + A 库分类目录链 + 影视号 + 类型。"""
    S, _ = env
    db = S()
    res, _mf, _media = _seed_mirror(
        db, title="大军师司马懿 (2017) {tmdb-135646}",
        identity="tmdb:135646:tv:S01:E03")
    from app.main import backfill_source_titles
    backfill_source_titles(db)
    db.refresh(res)
    from app.utils import source_query_hint
    name, path_hint, tmdb_id, mtype = source_query_hint(res)
    assert name == "大军师司马懿.S01E03.mkv", "文件名应用 A 库原名"
    assert tmdb_id == 135646, "影视号应用 A 库映射的号"
    assert mtype == "tv", "类型应用 A 库映射的类型"
    assert path_hint.endswith("大军师司马懿 (2017)"), \
        "目录线索末尾应是 A 库作品名（parse_media_name 取最后一段作作品名）"
    assert "电影/国产" in path_hint, "目录线索应含 A 库分类目录链"
    db.close()


def test_source_query_hint_falls_back_without_mapping(env):
    """没有 A 库映射时退回资源自身的文件名与目录线索（行为不变）。"""
    S, _ = env
    db = S()
    from app.models import Resource
    r = Resource(file_id="plain", name="Some.Show.S01E01.mkv",
                 path_hint="Some Show/Season 1", parent_id="p1",
                 size=1, ext=".mkv")
    db.add(r)
    db.flush()
    from app.utils import source_query_hint
    name, path_hint, tmdb_id, mtype = source_query_hint(r)
    assert name == "Some.Show.S01E01.mkv"
    assert path_hint == "Some Show/Season 1"
    assert tmdb_id == 0 and mtype == ""
    db.close()


def test_strip_tmdb_tag_keeps_year():
    """去标记保留年份（展示要「作品名 + 影视号」分开）。"""
    from app.utils import strip_tmdb_tag
    assert strip_tmdb_tag("大军师司马懿 (2017) {tmdb-135646}") == "大军师司马懿 (2017)"
    assert strip_tmdb_tag("No Tag Here") == "No Tag Here"


# ---------------------------------------------------------------------------
# 5~6. 类型锁定防串台
# ---------------------------------------------------------------------------

def _patch_tmdb(monkeypatch, *, movie_ok=True, tv_ok=True):
    """把 tmdb.detail / tmdb.search 换成假的命名空间实现，记录查了哪些命名空间。

    detail = 按 ID 精确取详情；search = 按名称搜候选。
    两者都记录被查的命名空间，断言「绝不跨命名空间」时两侧都要看。
    """
    from app import tmdb
    calls: list[tuple[int, str]] = []
    searched: list[tuple[str, str]] = []

    async def fake_detail(tmdb_id, media_type, db=None):
        calls.append((int(tmdb_id), media_type))
        if media_type == "movie" and movie_ok:
            return {"tmdb_id": int(tmdb_id), "media_type": "movie",
                    "title": "银河护卫队2", "name": "银河护卫队2"}
        if media_type == "tv" and tv_ok:
            return {"tmdb_id": int(tmdb_id), "media_type": "tv",
                    "title": "Indrani", "name": "Indrani"}
        raise tmdb.TmdbNotFound("404")

    async def fake_search(q, media_type, db=None):
        searched.append((str(q), media_type))
        return []

    monkeypatch.setattr(tmdb, "detail", fake_detail)
    monkeypatch.setattr(tmdb, "search", fake_search)
    _patch_tmdb.searched = searched
    return calls


def test_identify_lock_type_never_crosses_namespace(monkeypatch):
    """**核心防串台**：锁 movie 时绝不查 tv 命名空间。"""
    from app import tmdb
    calls = _patch_tmdb(monkeypatch, movie_ok=True, tv_ok=True)
    r = asyncio.run(tmdb.identify("银河护卫队2", path_hint="电影/银河护卫队2",
                                   forced_tmdb_id=283995, forced_type="movie"))
    assert r["ok"] and r["best"], "应识别成功"
    assert r["best"]["media_type"] == "movie", "必须落在 movie 命名空间"
    assert r["best"]["title"] == "银河护卫队2", "不能认成 tv 空间的 Indrani"
    queried = {mt for _tid, mt in calls}
    assert "tv" not in queried, f"锁 movie 却查了 tv 命名空间：{calls}"


def test_identify_lock_type_404_does_not_fallback(monkeypatch):
    """锁定的命名空间 404 → 不换类型试探（宁可不认，也不串台）。"""
    from app import tmdb
    calls = _patch_tmdb(monkeypatch, movie_ok=False, tv_ok=True)
    r = asyncio.run(tmdb.identify("银河护卫队2", path_hint="电影/银河护卫队2",
                                   forced_tmdb_id=283995, forced_type="movie"))
    # movie 空间 404 → ID 阶段不命中；名称阶段也锁 movie（type_hint 被覆盖）
    queried = {mt for _tid, mt in calls}
    assert "tv" not in queried, f"锁 movie 却查了 tv 命名空间：{calls}"
    searched_types = {mt for _q, mt in _patch_tmdb.searched}
    assert "tv" not in searched_types, \
        f"锁 movie 却按 tv 搜了候选：{_patch_tmdb.searched}"
    if r.get("ok") and r.get("best"):
        assert r["best"]["media_type"] == "movie", "命中也必须是 movie"


def test_identify_forced_tmdb_id_overrides_parsed_id(monkeypatch):
    """A 库映射的影视号优先于文件名/目录里解析出的号。"""
    from app import tmdb
    calls = _patch_tmdb(monkeypatch, movie_ok=True, tv_ok=True)
    asyncio.run(tmdb.identify("大军师司马懿.S01E03.mkv",
                              path_hint="剧集/大军师司马懿 {tmdb-111111}",
                              forced_tmdb_id=135646, forced_type="tv"))
    queried_ids = {tid for tid, _mt in calls}
    assert 135646 in queried_ids, f"应按 A 库影视号查：{calls}"
    assert 111111 not in queried_ids, f"不应按目录里解析出的号查：{calls}"


def test_identify_lock_type_applies_to_name_stage(monkeypatch):
    """名称搜索阶段也必须锁同一命名空间（防双路搜索串台）。"""
    from app import tmdb
    searched: list[tuple[str, str]] = []

    async def fake_search(q, mt, db=None):
        searched.append((str(q), mt))
        return []

    monkeypatch.setattr(tmdb, "search", fake_search)
    asyncio.run(tmdb.identify("大军师司马懿", path_hint="电影/大军师司马懿",
                              forced_tmdb_id=0, forced_type="tv"))
    for _q, mt in searched:
        assert mt == "tv", f"名称搜索阶段必须锁 tv，实际查了 {mt}"


def test_identify_without_mapping_keeps_legacy_behavior(monkeypatch):
    """没有 A 库映射时行为不变（type_hint 明确仍锁，未知仍双路）。"""
    from app import tmdb
    calls = _patch_tmdb(monkeypatch, movie_ok=True, tv_ok=True)
    # 目录里带 TMDB 标记 + 分类目录「电影」→ type_hint=movie → 锁 movie 命名空间
    r = asyncio.run(tmdb.identify("银河护卫队2",
                                   path_hint="电影/银河护卫队2 {tmdb-283995}"))
    assert r["ok"] and r["best"]
    assert r["best"]["media_type"] == "movie", "目录「电影」应锁 movie"
    queried = {mt for _tid, mt in calls}
    assert "tv" not in queried, f"目录已判定 movie 就不该查 tv：{calls}"
    # 无 A 库映射时不应产生类型锁定标记（保持旧行为）
    assert "type_lock" not in r["hint"], "无映射时不应锁类型"


# ---------------------------------------------------------------------------
# 7. 识别入口
# ---------------------------------------------------------------------------

def test_all_identify_entrypoints_use_source_hints():
    """4 个识别入口必须走 A 库线索，且手动/force 绕过缓存。"""
    app = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")
    # 直接调 tmdb.identify 的入口必须改成 A 库线索版
    assert "tmdb.identify(x.name, db, path_hint=x.path_hint" not in app, \
        "仍有入口直接用 B 库文件名调 tmdb.identify"
    # A 库线索核
    assert "_identify_with_source_hints" in app, "缺少 A 库线索识别核"
    assert "source_query_hint" in app, "未接入 source_query_hint"
    # 手动/force 走的是**绕过缓存**的核
    assert "await _identify_with_source_hints(x.name, x, db)" in app, \
        "手动识别未走绕过缓存的 A 库线索核"


def test_identify_cache_key_includes_tmdb_and_type():
    """识别缓存 key 必须含影视号与类型（否则不同号/类型会互相污染）。"""
    app = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")
    assert "dict[tuple[str, str, int, str], dict]" in app, \
        "识别缓存 key 注解未含影视号与类型"
    assert "int(q_tmdb or 0), str(q_type or \"\")" in app, \
        "缓存 key 未纳入影视号与类型"


def test_backfill_wired_into_pipelines():
    """回填必须接在扫描入库之后、识别之前（顺序错了识别仍用旧线索）。"""
    app = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")
    assert "backfill_source_titles(db)" in app, "回填未接入任何流程"
    # 镜像后管线：回填在 _post_scan_identify 之前
    seg = app[app.index("async def _mirror_post_sync_pipeline"):]
    seg = seg[:seg.index("async def _scheduled_mirror")]
    assert "ident = await _post_scan_identify(" in seg, "镜像管线未调识别"
    assert seg.index("backfill_source_titles") < seg.index("ident = await _post_scan_identify("), \
        "镜像管线里回填必须在识别之前"


def test_startup_backfill_registered():
    """启动时要回填老库存量。"""
    app = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")
    assert "_job_source_map_backfill" in app, "未注册定时回填"
    assert "await asyncio.to_thread(_backfill_source_titles_in_thread)" in app, \
        "启动时未回填老库存量"


# ---------------------------------------------------------------------------
# 8. 展示
# ---------------------------------------------------------------------------

def test_library_series_title_and_type_use_a_source(env):
    """作品聚合：未识别时标题/类型用 A 库口径，并回传映射字段。"""
    S, _ = env
    db = S()
    res, _mf, _media = _seed_mirror(
        db, title="大军师司马懿 (2017) {tmdb-135646}",
        identity="tmdb:135646:tv:S01:E03")
    from app.main import backfill_source_titles
    backfill_source_titles(db)
    db.flush()
    from app.main import library_series
    rows = library_series(q="", media_type="", finish="",
                          identify_status="", share_status="", db=db)
    row = next(r for r in rows if r["representative_id"] == res.id)
    assert row["title"] == "大军师司马懿 (2017)", \
        f"未识别作品的标题应用 A 库正确剧名，实际：{row['title']}"
    assert row["media_type"] == "tv", "类型应用 A 库映射的类型"
    assert row["source_tmdb_id"] == 135646, "应回传 A 库影视号"
    assert row["source_media_type"] == "tv", "应回传 A 库类型"
    db.close()


def test_library_series_identified_title_wins(env):
    """已识别成功的 TMDB 正名优先于 A 库名。"""
    S, _ = env
    db = S()
    res, _mf, _media = _seed_mirror(
        db, title="大军师司马懿 (2017) {tmdb-135646}",
        identity="tmdb:135646:tv:S01:E03")
    from app.main import backfill_source_titles
    backfill_source_titles(db)
    res.media_title = "大军师司马懿"
    res.media_type = "tv"
    db.flush()
    from app.main import library_series
    rows = library_series(q="", media_type="", finish="",
                          identify_status="", share_status="", db=db)
    row = next(r for r in rows if r["representative_id"] == res.id)
    assert row["title"] == "大军师司马懿", "识别成功的正名应优先"
    db.close()


def test_resources_endpoint_exposes_source_fields(env):
    """/api/resources 回传 A 库映射字段（前端手动修正窗口要用）。"""
    S, _ = env
    db = S()
    _seed_mirror(db, title="大军师司马懿 (2017) {tmdb-135646}")
    from app.main import backfill_source_titles
    backfill_source_titles(db)
    db.flush()
    from app.main import resources
    rows = resources(q="", limit=10, media_type="", identify_status="", db=db)
    row = next(r for r in rows if r["file_id"] == "bf_1")
    for k in ("source_title", "source_name", "source_tmdb_id", "source_media_type"):
        assert k in row, f"/api/resources 未回传 {k}"
    assert row["source_title"] == "大军师司马懿 (2017)"
    db.close()


# ---------------------------------------------------------------------------
# 9. 前端
# ---------------------------------------------------------------------------

def test_frontend_match_window_uses_a_source_prefill():
    """手动修正窗口用 A 库剧名 + 影视号预填。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "function sourceInfo(" in app, "缺少 A 库映射读取函数"
    assert "src.title || dirTitle(row.path_hint)" in app, \
        "搜索词未优先用 A 库正确剧名"
    assert "src.type" in app, "类型未优先用 A 库映射类型"
    assert "source_tmdb_id" in app, "未使用 A 库影视号"


def test_frontend_match_window_shows_a_source_info():
    """手动修正窗口展示 A 库剧名与影视号。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "原库剧名" in app, "手动修正窗口未展示 A 库剧名"
    assert "影视号" in app, "手动修正窗口未展示影视号"


def test_frontend_no_backend_field_leak_in_match_window():
    """页面不得出现后端内部字段名（只展示中性文案）。"""
    app = APP_VUE.read_text(encoding="utf-8")
    start = app.index('v-model="matchVisible"')
    end = app.index("</el-dialog>", start)
    block = app[start:end]
    for word in ("source_title", "source_name", "source_tmdb_id",
                 "source_media_type", "source_category", "work_key",
                 "category_key", "chat_id", "target_file_id", "identify_status"):
        assert word not in block, f"手动修正窗口直出后端字段：{word}"
