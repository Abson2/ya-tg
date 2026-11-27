"""v2.1.54 分类归一 / 归类重算 / 脏键迁移专项验收。

## 真机事实（2026-11-27 线上只读）

  · 678 部作品里 **586 部卡在「未分类」**，其中 504 部的资源其实带着类型；
  · `sc_categories` 里躺着一条 `genre:scifi&fantasy`，name 是「未分类」；
  · 25657 条资源、26 个唯一 genre 名，**只有两个是英文**：
      `Sci-Fi & Fantasy` 2438 条、`War & Politics` 1408 条（TMDB 的 zh-CN 漏翻）；
  · 同一部作品在三个地方显示成三个分类：
      资源库「Sci-Fi & Fantasy」/ 分享中心「未分类」/ 排期配置「科幻奇幻」。

## 根因两处

  ① `sync_library_versions` 把归类写成「只写一次」：
     `if exists: continue`（指纹没变就整段跳过）+ `if not ownership`（归属只在首轮写）。
     资源后来识别回填了 genres，归属永久不更新。
  ② genre 名没有唯一归一入口：`_primary_category` 直出原文拼键，
     `main.py::library_series` 也直出原文 → 两份实现，两份结果。

## 本文件锁住的行为

  1. `genre_name_cn` 是全项目唯一归一入口，英文/大小写/脏键都落到官方中文名；
  2. `_primary_category` 不再产出 `genre:scifi&fantasy` 这种脏键；
  3. `_work_category` 扫**全部资源**，不再被代表资源一条的 genres 决定生死；
  4. `sync_library_versions` **每轮重算**归属（补了 genres 之后再跑必须纠正）；
  5. `migrate_legacy_category_keys` 三表迁移 + 幂等（跑第二次不再动）；
  6. `recompute_work_categories(dry_run=True)` 一个字都不写库；
  7. 资源库端点返回的 primary_genre 是中文，与排期配置同一份判据。
"""
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v2154cat_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'cat.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _res(db, *, file_id, name, genres, media_type="tv", size=1024, tmdb_id=1):
    from app.models import Resource
    r = Resource(
        file_id=file_id, name=name, path_hint=name, parent_id="p1",
        size=size, ext=".mkv", tmdb_id=tmdb_id, media_type=media_type,
        media_title=name, poster_path="/p.jpg",
        genres=json.dumps(genres, ensure_ascii=False) if genres else "",
        identify_status="identified", share_status="not_created", vote_average=7.5,
    )
    db.add(r)
    db.flush()
    return r


# ===========================================================================
# 一、唯一归一入口
# ===========================================================================
def test_genre_name_cn_is_the_single_normalizer():
    """英文原名 / 大小写 / 脏键 / 中文名 → 同一个官方中文名。"""
    from app.tmdb import genre_name_cn

    # 线上实测的两个英文名
    assert genre_name_cn("Sci-Fi & Fantasy") == "科幻奇幻"
    assert genre_name_cn("War & Politics") == "战争政治"
    # 大小写、有无连字符、多余空格都不影响
    assert genre_name_cn("sci-fi & fantasy") == "科幻奇幻"
    assert genre_name_cn("SciFi & Fantasy") == "科幻奇幻"
    assert genre_name_cn("  War  &  Politics  ") == "战争政治"
    # 已经是中文的原样返回（幂等）
    assert genre_name_cn("科幻奇幻") == "科幻奇幻"
    assert genre_name_cn("剧情") == "剧情"
    # 归一函数本身可以反复套，结果不变
    once = genre_name_cn("Sci-Fi & Fantasy")
    assert genre_name_cn(once) == once


def test_genre_name_cn_does_not_guess_unknown_names():
    """认不出来的名字原样返回 —— 绝不猜成某个官方分类（猜错=塞进错库存）。"""
    from app.tmdb import genre_name_cn
    assert genre_name_cn("自造分类名") == "自造分类名"
    assert genre_name_cn("Something Else") == "Something Else"
    assert genre_name_cn("") == ""
    assert genre_name_cn(None) == ""


def test_every_official_cn_name_is_in_the_english_table():
    """27 项官方中文名必须都能从英文表反向命中 —— 表漏了等于该分类又会出脏键。"""
    from app.tmdb import TMDB_GENRES, TMDB_GENRES_EN, genre_name_cn
    for cn in TMDB_GENRES.values():
        assert genre_name_cn(cn) == cn, f"中文名 {cn} 自身必须归一到自己"
    assert set(TMDB_GENRES_EN.values()) == set(TMDB_GENRES.values()), (
        "英文表的中文目标必须与官方字典完全一致，多一个少一个都会造成两套分类")


# ===========================================================================
# 二、主分类键不再出脏键
# ===========================================================================
def test_primary_category_returns_chinese_key_for_english_genre(env):
    """英文 genre 作为第一个类型 → 键是 genre:科幻奇幻，不是 genre:scifi&fantasy。"""
    from app import share_center_v2 as sc2
    S, _ = env
    db = S()
    r = _res(db, file_id="f1", name="怪奇物语",
             genres=["Sci-Fi & Fantasy", "剧情"])
    key, mt = sc2._primary_category(r)
    assert key == "genre:科幻奇幻", f"英文名必须归一到中文键，实际 {key}"
    assert mt == "tv"
    db.close()


def test_primary_category_keeps_strict_first_element(env):
    """仍然严格取第一个类型 —— 归一不改变「以第一个为准」的口径。"""
    from app import share_center_v2 as sc2
    S, _ = env
    db = S()
    r = _res(db, file_id="f1", name="红色", genres=["剧情", "War & Politics"])
    key, _ = sc2._primary_category(r)
    assert key == "genre:剧情", "第二个类型不参与主分类（一部作品只有一个主分类）"
    db.close()


def test_primary_category_empty_genres_is_unknown(env):
    from app import share_center_v2 as sc2
    S, _ = env
    db = S()
    r = _res(db, file_id="f1", name="没识别的", genres=[])
    key, _ = sc2._primary_category(r)
    assert key == "genre:unknown"
    db.close()


# ===========================================================================
# 三、归类看全部资源，不只看代表那一条
# ===========================================================================
def test_work_category_scans_all_resources(env):
    """代表资源 genres 空、后面某条有类型 → 仍必须判出真实分类。

    真机上「红色」有 48 个资源、35 个带 genres，代表那条恰好空 → 整部作品被
    判成未分类。这条用例锁的是「不许再只看第一条」。
    """
    from app import share_center_v2 as sc2
    S, _ = env
    db = S()
    a = _res(db, file_id="a_empty", name="红色", genres=[])
    b = _res(db, file_id="b_has", name="红色", genres=["剧情", "War & Politics"])
    key, mt = sc2._work_category([a, b])
    assert key == "genre:剧情", f"应当从第二条资源判出剧情，实际 {key}"
    assert mt == "tv"
    db.close()


def test_work_category_all_empty_is_unknown(env):
    from app import share_center_v2 as sc2
    S, _ = env
    db = S()
    a = _res(db, file_id="a", name="X", genres=[])
    b = _res(db, file_id="b", name="X", genres=[])
    key, _ = sc2._work_category([a, b])
    assert key == "genre:unknown"
    db.close()


def test_work_category_result_is_stable(env):
    """同一批资源扫几遍结果一样 —— 顺序由调用方给的稳定序决定。"""
    from app import share_center_v2 as sc2
    S, _ = env
    db = S()
    rows = [
        _res(db, file_id="a", name="X", genres=[]),
        _res(db, file_id="b", name="X", genres=["悬疑"]),
        _res(db, file_id="c", name="X", genres=["喜剧"]),
    ]
    first = sc2._work_category(rows)
    for _ in range(3):
        assert sc2._work_category(rows) == first
    assert first[0] == "genre:悬疑"
    db.close()


# ===========================================================================
# 四、sync_library_versions 每轮重算（这是「只写一次」bug 的反证）
# ===========================================================================
def test_sync_versions_recomputes_category_after_genres_backfilled(env):
    """先归类成 unknown，资源补上 genres 后再跑 → 归属必须被纠正。

    这是 v2.1.54 修的那个 bug 的直接反证：
      · 旧代码：`if exists: continue` + `if not ownership` → 永远停在 unknown；
      · 新代码：每轮重算 → 第二次跑就变成「剧情」。
    """
    from app import share_center_v2 as sc2
    from app.models import SCWorkCategory
    from sqlalchemy import select
    S, _ = env
    db = S()
    r = _res(db, file_id="f1", name="朱雀堂", genres=[])
    out1 = sc2.sync_library_versions(db)
    row = db.scalar(select(SCWorkCategory).where(SCWorkCategory.work_key))
    rows = db.scalars(select(SCWorkCategory)).all()
    assert len(rows) == 1
    assert rows[0].category_key == "genre:unknown", f"第一轮应判 unknown，实际 {rows[0].category_key}"
    assert out1["categories_created"] == 1

    # 识别回填 genres —— 资源内容没变（file_id/size 不变），指纹也不变
    r.genres = json.dumps(["悬疑"], ensure_ascii=False)
    db.flush()
    out2 = sc2.sync_library_versions(db)
    rows = db.scalars(select(SCWorkCategory)).all()
    assert len(rows) == 1, "归属表一部作品只许一行"
    assert rows[0].category_key == "genre:悬疑", (
        f"指纹没变也要重算归属，实际 {rows[0].category_key}")
    assert out2["categories_created"] == 0, "不是新建，是纠正"
    assert out2["categories_updated"] == 1
    db.close()


def test_sync_versions_does_not_touch_correct_ownership(env):
    """已经判对的行不刷新 assigned_at —— 归属时间要能追溯。"""
    from app import share_center_v2 as sc2
    from app.models import SCWorkCategory
    from sqlalchemy import select
    S, _ = env
    db = S()
    _res(db, file_id="f1", name="A", genres=["剧情"])
    sc2.sync_library_versions(db)
    row = db.scalars(select(SCWorkCategory)).all()[0]
    stamp = row.assigned_at
    sc2.sync_library_versions(db)
    row2 = db.scalars(select(SCWorkCategory)).all()[0]
    assert row2.category_key == "genre:剧情"
    assert row2.assigned_at == stamp, "判对的行不该被反复改写归属时间"
    db.close()


def test_sync_versions_uses_chinese_key_for_english_genre(env):
    """端到端：英文 genre 的作品落库后的归属键是中文。"""
    from app import share_center_v2 as sc2
    from app.models import SCWorkCategory
    from sqlalchemy import select
    S, _ = env
    db = S()
    _res(db, file_id="f1", name="怪奇物语", genres=["Sci-Fi & Fantasy"])
    sc2.sync_library_versions(db)
    row = db.scalars(select(SCWorkCategory)).all()[0]
    assert row.category_key == "genre:科幻奇幻", (
        f"英文名必须落成中文键，实际 {row.category_key}")
    db.close()


# ===========================================================================
# 五、脏键迁移：三张表 + 幂等
# ===========================================================================
def _seed_legacy(db):
    from app.models import SCCategory, SCCategorySchedule, SCWorkCategory
    db.add(SCCategory(category_key="genre:scifi&fantasy", name="未分类",
                      source="tmdb", media_type="tv", enabled=True, priority=0))
    db.add(SCCategory(category_key="genre:科幻奇幻", name="科幻奇幻",
                      source="tmdb", media_type="tv", enabled=True, priority=-1))
    db.add(SCCategory(category_key="genre:剧情", name="剧情",
                      source="tmdb", media_type="movie", enabled=True, priority=0))
    db.add(SCCategorySchedule(plan_version_id=1, category_key="genre:scifi&fantasy",
                              window_start=10, window_end=12, enabled=True))
    db.add(SCCategorySchedule(plan_version_id=1, category_key="genre:剧情",
                              window_start=8, window_end=10, enabled=True))
    db.add(SCWorkCategory(work_key="work:x", category_key="genre:scifi&fantasy",
                          media_type="tv"))
    db.add(SCWorkCategory(work_key="work:y", category_key="genre:剧情",
                          media_type="movie"))
    db.commit()


def test_migrate_moves_dirty_keys_on_all_three_tables(env):
    from app import share_center_v2 as sc2
    from app.models import SCCategory, SCCategorySchedule, SCWorkCategory
    from sqlalchemy import select
    S, _ = env
    db = S()
    _seed_legacy(db)
    out = sc2.migrate_legacy_category_keys(db)
    assert out["moved"] >= 3, out

    cats = {c.category_key for c in db.scalars(select(SCCategory)).all()}
    assert "genre:scifi&fantasy" not in cats, "脏键必须从分类字典消失"
    assert "genre:科幻奇幻" in cats

    scheds = {s.category_key for s in db.scalars(select(SCCategorySchedule)).all()}
    assert "genre:scifi&fantasy" not in scheds, "脏键必须从排期表消失"
    assert "genre:剧情" in scheds, "正常行不许被顺手删掉"

    owners = {w.category_key for w in db.scalars(select(SCWorkCategory)).all()}
    assert "genre:scifi&fantasy" not in owners
    assert "genre:科幻奇幻" in owners
    db.close()


def test_migrate_keeps_one_row_when_target_already_exists(env):
    """目标键已存在时保留目标行、删影子行 —— 否则撞 (版本,键) 唯一约束。"""
    from app import share_center_v2 as sc2
    from app.models import SCCategorySchedule
    from sqlalchemy import select
    S, _ = env
    db = S()
    _seed_legacy(db)
    # 目标键也来一行排期，模拟「两个键都在」的线上状态
    db.add(SCCategorySchedule(plan_version_id=1, category_key="genre:科幻奇幻",
                              window_start=20, window_end=22, enabled=True))
    db.commit()
    sc2.migrate_legacy_category_keys(db)
    rows = db.scalars(select(SCCategorySchedule).where(
        SCCategorySchedule.category_key == "genre:科幻奇幻")).all()
    assert len(rows) == 1, "同一版本同一分类只许一行"
    assert (rows[0].window_start, rows[0].window_end) == (20, 22), (
        "保留的应是目标键那一行（页面在用的配置），不是脏键影子行")
    db.close()


def test_migrate_is_idempotent(env):
    from app import share_center_v2 as sc2
    S, _ = env
    db = S()
    _seed_legacy(db)
    first = sc2.migrate_legacy_category_keys(db)
    second = sc2.migrate_legacy_category_keys(db)
    assert first["moved"] >= 3
    assert second["moved"] == 0, "第二次不该再迁移任何东西"
    db.close()


def test_migrate_dry_run_writes_nothing(env):
    from app import share_center_v2 as sc2
    from app.models import SCCategory, SCWorkCategory
    from sqlalchemy import select
    S, _ = env
    db = S()
    _seed_legacy(db)
    out = sc2.migrate_legacy_category_keys(db, dry_run=True)
    assert out["moved"] >= 3, "dry_run 也要算出会迁多少"
    cats = {c.category_key for c in db.scalars(select(SCCategory)).all()}
    assert "genre:scifi&fantasy" in cats, "dry_run 一个字都不许写"
    owners = {w.category_key for w in db.scalars(select(SCWorkCategory)).all()}
    assert "genre:scifi&fantasy" in owners
    db.close()


# ===========================================================================
# 六、回填：dry_run 不写库，实跑纠正
# ===========================================================================
def test_recompute_dry_run_reports_without_writing(env):
    from app import share_center_v2 as sc2
    from app.models import SCWorkCategory
    from sqlalchemy import select
    S, _ = env
    db = S()
    _res(db, file_id="f1", name="红色", genres=[])
    sc2.sync_library_versions(db)          # 先落成 unknown
    r = db.scalars(select(SCWorkCategory)).all()[0]
    assert r.category_key == "genre:unknown"
    # 补 genres（模拟识别回填），但不重跑 sync
    from app.models import Resource
    res = db.scalar(select(Resource))
    res.genres = json.dumps(["剧情"], ensure_ascii=False)
    db.commit()

    out = sc2.recompute_work_categories(db, dry_run=True)
    assert out["changed"] == 1, out
    assert out["items"][0]["to"] == "genre:剧情"
    assert out["target_stock"].get("genre:剧情") == 1, "dry_run 要给出迁移后的库存分布"
    still = db.scalars(select(SCWorkCategory)).all()[0]
    assert still.category_key == "genre:unknown", "dry_run 不许写库"
    db.close()


def test_recompute_writes_when_not_dry_run(env):
    from app import share_center_v2 as sc2
    from app.models import SCWorkCategory, Resource
    from sqlalchemy import select
    S, _ = env
    db = S()
    _res(db, file_id="f1", name="红色", genres=[])
    sc2.sync_library_versions(db)
    res = db.scalar(select(Resource))
    res.genres = json.dumps(["剧情"], ensure_ascii=False)
    db.commit()

    out = sc2.recompute_work_categories(db)
    assert out["changed"] == 1
    row = db.scalars(select(SCWorkCategory)).all()[0]
    assert row.category_key == "genre:剧情"
    db.close()


def test_recompute_does_not_create_library_versions(env):
    """回填只动归属表 —— 绝不许顺手造出「更新」版本（那是追更推送的触发源）。"""
    from app import share_center_v2 as sc2
    from app.models import SCLibraryVersion, Resource
    from sqlalchemy import select
    S, _ = env
    db = S()
    _res(db, file_id="f1", name="红色", genres=["剧情"])
    sc2.sync_library_versions(db)
    before = len(db.scalars(select(SCLibraryVersion)).all())
    res = db.scalar(select(Resource))
    res.genres = json.dumps(["悬疑"], ensure_ascii=False)
    db.commit()
    sc2.recompute_work_categories(db)
    after = len(db.scalars(select(SCLibraryVersion)).all())
    assert after == before, "回填归类不许生成新版本快照"
    db.close()


# ===========================================================================
# 七、资源库端点与排期配置同一份判据
# ===========================================================================
def test_library_series_returns_chinese_primary_genre(env):
    """资源库列表的主分类必须是中文，与「分类-时间排期配置」显示一致。"""
    from app.main import library_series
    S, _ = env
    db = S()
    _res(db, file_id="f1", name="怪奇物语", genres=["Sci-Fi & Fantasy", "剧情"])
    _res(db, file_id="f2", name="红雀", genres=["War & Politics"])
    rows = library_series(q="", media_type="", finish="", identify_status="",
                          share_status="", db=db)
    by_title = {r["title"]: r for r in rows}
    assert by_title["怪奇物语"]["primary_genre"] == "科幻奇幻", (
        f"资源库不许再显示英文，实际 {by_title['怪奇物语']['primary_genre']}")
    assert "科幻奇幻" in by_title["怪奇物语"]["genre_names"], "归一名必须能进分组"
    assert by_title["红雀"]["primary_genre"] == "战争政治"
    db.close()


def test_backend_has_no_second_genre_normalizer():
    """判据只许一份：这两个模块都不许再自己维护英文→中文的对照。

    查的是**字符串字面量**（带引号），不是注释里提到分类名 —— 注释该写还得写，
    但代码里一旦出现 `"Sci-Fi & Fantasy"` 这种东西，就是又长出了一份对照表。
    """
    root = Path(__file__).resolve().parents[1] / "backend" / "app"
    banned = ('"Sci-Fi & Fantasy"', "'Sci-Fi & Fantasy'",
              '"War & Politics"', "'War & Politics'")
    for name in ("share_center_v2.py", "main.py"):
        text = (root / name).read_text(encoding="utf-8")
        for lit in banned:
            assert lit not in text, (
                f"{name} 又写了一份英文分类名对照（{lit}）—— "
                f"归一必须只在 tmdb.genre_name_cn")
