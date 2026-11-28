"""v2.1.64 验收：镜像身份治本 + 幽灵目录纳入覆盖。

真机根因（2026-11-28 只读实测）：
  A 库 718 部、已映射 681、未镜像 37。37 部全是同名重拍剧——
  normalize_work_name 剥掉年份/TMDB → 英雄(2001)/(2002) 都归一到 work:英雄，
  老唯一约束 uq_mirror_rule_work(rule_id, work_key) 让后到的版本永远建不进映射。

治本（用户拍板「镜像源目录身份治本」+「纳入镜像覆盖」）：
  ① 全局 work_key 语义不动；MirrorMedia 唯一身份改为 (rule_id, source_dir_id)。
  ② 旧库一次性幂等重建表（数据保留、子表 mirror_files 不受影响）。
  ③ sync_rule 建映射查重锚点 work_key → source_dir_id。
  ④ plan_work_batch 身份判据可选按 source_dir_id（游标仍用 work_key，不动）。
  ⑤ run_queue 三段去重锚点 work_key → source_dir_id（同名版本不互相跳过）。

覆盖：
  - 迁移：旧唯一约束 → 新；幂等；重复 source_dir_id 守卫；数据/子表保留
  - 模型：MirrorMedia 唯一约束是 (rule_id, source_dir_id)
  - plan_work_batch：同名不同源各自算未镜像；mapped_source_ids 与旧 mapped_keys 隔离
  - 源码级：建映射/去重/选批锚点都已改，游标记号仍在
"""
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

MIRROR_SRC = (BACKEND / "app" / "mirror.py").read_text(encoding="utf-8")
MODELS_SRC = (BACKEND / "app" / "models.py").read_text(encoding="utf-8")
DB_SRC = (BACKEND / "app" / "db.py").read_text(encoding="utf-8")
MAIN_SRC = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# ① 迁移纯函数：建一张老约束表，跑 _rebuild_mirror_media_unique
# ---------------------------------------------------------------------------

def _old_style_db(tmp):
    """造一张老版 mirror_media（唯一约束 (rule_id, work_key)）+ mirror_files 子表。"""
    # 与生产 db.py 的 PRAGMA 一致：不开 foreign_keys（SQLite 默认 OFF）。
    # 这样 DROP 父表不会级联动子表，重建后 mirror_files 行原样保留、id 引用仍有效。
    eng = create_engine(f"sqlite:///{os.path.join(tmp, 'old.db')}",
                        connect_args={"check_same_thread": False})

    with eng.begin() as c:
        c.execute(text("""
            CREATE TABLE mirror_media (
                id INTEGER NOT NULL PRIMARY KEY,
                rule_id INTEGER NOT NULL,
                work_key VARCHAR(256) DEFAULT '' NOT NULL,
                title_standard VARCHAR(512) DEFAULT '' NOT NULL,
                title_mirror VARCHAR(512) DEFAULT '' NOT NULL,
                tmdb_id INTEGER,
                media_type VARCHAR(16) DEFAULT '' NOT NULL,
                source_dir_id VARCHAR(64) DEFAULT '' NOT NULL,
                target_dir_id VARCHAR(64) DEFAULT '' NOT NULL,
                category_path VARCHAR(512) DEFAULT '' NOT NULL,
                english_title VARCHAR(512) DEFAULT '' NOT NULL,
                english_source VARCHAR(24) DEFAULT '' NOT NULL,
                status VARCHAR(24) DEFAULT 'pending' NOT NULL,
                last_synced_at DATETIME,
                created_at DATETIME NOT NULL,
                UNIQUE (rule_id, work_key)
            )
        """))
        c.execute(text("CREATE INDEX ix_mirror_media_work_key ON mirror_media (work_key)"))
        c.execute(text("CREATE INDEX ix_mirror_media_rule_id ON mirror_media (rule_id)"))
        c.execute(text("""
            CREATE TABLE mirror_files (
                id INTEGER NOT NULL PRIMARY KEY,
                media_id INTEGER NOT NULL,
                rule_id INTEGER NOT NULL,
                source_file_id VARCHAR(64) DEFAULT '' NOT NULL,
                FOREIGN KEY(media_id) REFERENCES mirror_media(id)
            )
        """))
        # 两部同名不同源的老库：老约束下只能有一部（后到的被吞）——这正复现病根。
        c.execute(text("""
            INSERT INTO mirror_media
            (id, rule_id, work_key, title_standard, title_mirror, media_type,
             source_dir_id, target_dir_id, category_path, english_title,
             english_source, status, created_at)
            VALUES (1, 1, 'work:英雄', '英雄', '英雄·B', 'tv',
                    'SRC-2001', 'DST-2001', '', '', '', 'synced', '2026-11-28 00:00:00')
        """))
        c.execute(text("""
            INSERT INTO mirror_files (id, media_id, rule_id, source_file_id)
            VALUES (10, 1, 1, 'F-001')
        """))
    return eng


def _unique_cols(eng, table):
    with eng.connect() as c:
        for r in c.execute(text(f"PRAGMA index_list({table})")).all():
            if r[2] and r[3] == "u":
                return {ir[2] for ir in c.execute(
                    text(f'PRAGMA index_info("{r[1]}")')).all()}
    return set()


def test_migration_old_unique_to_source(tmp_path):
    """旧唯一 (rule_id, work_key) → 新 (rule_id, source_dir_id)，数据保留。"""
    from app.db import _rebuild_mirror_media_unique
    eng = _old_style_db(str(tmp_path))
    assert _unique_cols(eng, "mirror_media") == {"rule_id", "work_key"}
    with eng.begin() as c:
        n = _rebuild_mirror_media_unique(c)
    assert n == 1
    assert _unique_cols(eng, "mirror_media") == {"rule_id", "source_dir_id"}
    with eng.connect() as c:
        row = c.execute(text("SELECT id, source_dir_id, work_key FROM mirror_media")).one()
        assert (row[0], row[1], row[2]) == (1, "SRC-2001", "work:英雄")
        # 普通索引仍在
        idx = {r[1] for r in c.execute(text("PRAGMA index_list(mirror_media)")).all()}
        assert "ix_mirror_media_work_key" in idx and "ix_mirror_media_rule_id" in idx


def test_migration_idempotent(tmp_path):
    """第二次跑：已是新约束 → 返回 0，不重建、不报错。"""
    from app.db import _rebuild_mirror_media_unique
    eng = _old_style_db(str(tmp_path))
    with eng.begin() as c:
        _rebuild_mirror_media_unique(c)
    with eng.begin() as c:
        assert _rebuild_mirror_media_unique(c) == 0
    assert _unique_cols(eng, "mirror_media") == {"rule_id", "source_dir_id"}


def test_migration_keeps_child_rows(tmp_path):
    """子表 mirror_files 不受重建影响（media_id 引用未变）。"""
    from app.db import _rebuild_mirror_media_unique
    eng = _old_style_db(str(tmp_path))
    with eng.begin() as c:
        _rebuild_mirror_media_unique(c)
    with eng.connect() as c:
        assert c.execute(text("SELECT COUNT(*) FROM mirror_files")).scalar() == 1
        fid = c.execute(text("SELECT media_id FROM mirror_files")).scalar()
        assert fid == 1
        assert c.execute(text("SELECT COUNT(*) FROM mirror_media WHERE id=1")).scalar() == 1


def test_migration_guard_on_duplicate_source(tmp_path):
    """重复 (rule_id, source_dir_id) → 跳过迁移返回 0，绝不猜/改存量。"""
    from app.db import _rebuild_mirror_media_unique
    eng = _old_style_db(str(tmp_path))
    with eng.begin() as c:
        # 再插一条同 rule 同 source（work_key 不同，老约束允许）
        c.execute(text("""
            INSERT INTO mirror_media
            (id, rule_id, work_key, title_standard, title_mirror, media_type,
             source_dir_id, target_dir_id, category_path, english_title,
             english_source, status, created_at)
            VALUES (2, 1, 'work:另一部', '另一部', '', 'tv',
                    'SRC-2001', 'DST-2', '', '', '', 'synced', '2026-11-28 00:00:00')
        """))
    with eng.begin() as c:
        assert _rebuild_mirror_media_unique(c) == 0
    # 旧约束原样保留
    assert _unique_cols(eng, "mirror_media") == {"rule_id", "work_key"}


def test_migration_allows_same_workkey_different_source(tmp_path):
    """核心：迁移后同名不同源可共存（老约束下这条会被吞）。"""
    from app.db import _rebuild_mirror_media_unique
    eng = _old_style_db(str(tmp_path))
    with eng.begin() as c:
        _rebuild_mirror_media_unique(c)
    with eng.begin() as c:
        c.execute(text("""
            INSERT INTO mirror_media
            (id, rule_id, work_key, title_standard, title_mirror, media_type,
             source_dir_id, target_dir_id, category_path, english_title,
             english_source, status, created_at)
            VALUES (2, 1, 'work:英雄', '英雄', '英雄2002·B', 'tv',
                    'SRC-2002', 'DST-2002', '', '', '', 'synced', '2026-11-28 00:00:00')
        """))
    with eng.connect() as c:
        assert c.execute(text(
            "SELECT COUNT(*) FROM mirror_media WHERE work_key='work:英雄'"
        )).scalar() == 2


# ---------------------------------------------------------------------------
# ② 模型约束
# ---------------------------------------------------------------------------

def test_model_unique_is_by_source():
    assert 'UniqueConstraint("rule_id", "source_dir_id"' in MODELS_SRC
    assert 'name="uq_mirror_rule_source"' in MODELS_SRC
    assert 'uq_mirror_rule_work' not in MODELS_SRC


# ---------------------------------------------------------------------------
# ③ plan_work_batch 身份判据按 source_dir_id，与旧 mapped_keys 隔离
# ---------------------------------------------------------------------------

def _works_same_key():
    # 两部同名重拍剧：work_key 相同，source_dir_id 不同
    return [
        ({"id": "SRC-2001", "name": "英雄 (2001)", "files": [], "children": []}, []),
        ({"id": "SRC-2002", "name": "英雄 (2002)", "files": [], "children": []}, []),
        ({"id": "SRC-3", "name": "择日飞升", "files": [], "children": []}, []),
    ]


def test_batch_identity_uses_source_ids():
    from app import mirror
    works = _works_same_key()
    # 老口径反证：work:英雄 已在 mapped_keys → 两部英雄都被当已镜像，
    # 未镜像只有择日飞升；名额=3 时补位才可能带上英雄，名额=1 时英雄进不来。
    old_keys = {mirror.work_key_of(works[0][0])}  # work:英雄
    batch_old, _, _ = mirror.plan_work_batch(works, 1, "", mapped_keys=old_keys)
    assert "择日飞升" in {w["name"] for w, _ in batch_old}, \
        "老口径下未镜像集合不含两部英雄，第一部必是择日飞升"
    # 新口径：只 SRC-2001 已镜像，SRC-2002 仍算未镜像 → 应被优先选中
    new_ids = {"SRC-2001"}
    batch_new, _, _ = mirror.plan_work_batch(
        works, 1, "", mapped_keys=set(), mapped_source_ids=new_ids)
    picked_ids = {w["id"] for w, _ in batch_new}
    assert "SRC-2002" in picked_ids, "同名第二版必须按 source_dir_id 判为未镜像并入选"


def test_batch_cursor_still_work_key_with_source_identity():
    """游标仍返回 work_key（不动），但身份判据用的是 source_dir_id。"""
    from app import mirror
    works = _works_same_key()
    batch, cursor, _ = mirror.plan_work_batch(
        works, 2, "", mapped_keys=set(), mapped_source_ids={"SRC-2001", "SRC-2002"})
    assert cursor.startswith("work:"), "持久化游标语义不变"


# ---------------------------------------------------------------------------
# ④ 源码级反证：锚点全部改掉，别回退
# ---------------------------------------------------------------------------

def test_source_anchor_in_media_lookup():
    assert "MirrorMedia.source_dir_id == str(work[\"id\"])" in MIRROR_SRC


def test_source_anchor_in_run_queue_dedup():
    assert '_ident = str((_pair[0] or {}).get("id") or "")' in MIRROR_SRC


def test_mapped_source_ids_wired():
    assert "mapped_source_ids=mapped_source_ids" in MIRROR_SRC
    assert "select(MirrorMedia.source_dir_id)" in MIRROR_SRC


def test_works_cursor_semantics_preserved():
    assert "keys.index(cursor) + 1" in MIRROR_SRC
    assert "return batch, keys[picked[-1]], wrapped" in MIRROR_SRC
    # 迁移在 migrate() 中、任何同步前调用
    assert "_migrate_mirror_media_unique_by_source()" in DB_SRC


# ---------------------------------------------------------------------------
# ⑤ 问题3：内存优化——投影范式 + SQLite cache_size，判据零变化
# ---------------------------------------------------------------------------

def test_resource_light_projection_wired():
    """推送链路(_push_gate_passed/_enqueue_ready)改走投影，不再全实体加载。"""
    assert "_resource_light_rows(db)" in MAIN_SRC
    assert "members = [s for s in _resource_light_rows(db) if _work_key(s) == key]" in MAIN_SRC
    assert "all_members = [s.id for s in _resource_light_rows(db)" in MAIN_SRC
    # 这两处旧的全实体写法必须消失
    assert 'members = [s for s in db.scalars(select(Resource)).all() if _work_key(s) == key]' not in MAIN_SRC
    assert 'all_members = [s.id for s in db.scalars(select(Resource)).all()' not in MAIN_SRC


def test_resource_light_cols_cover_all_gate_fields():
    """投影列必须覆盖 work_group_key + _member_items + find_mirror_media 的全部判据字段。"""
    # 分组键字段
    for col in ("Resource.file_id", "Resource.path_hint", "Resource.share_dir_name",
                "Resource.media_title", "Resource.media_type", "Resource.tmdb_id"):
        assert col in MAIN_SRC
    # 门禁三字段 + 镜像兜底
    for col in ("Resource.identify_status", "Resource.poster_path",
                "Resource.original_title"):
        assert col in MAIN_SRC
    # 大 Text 字段不得混进投影（否则内存优化失效）
    block = MAIN_SRC[MAIN_SRC.index("_RESOURCE_LIGHT_COLS = ("):
                     MAIN_SRC.index("_RESOURCE_LIGHT_COLS = (") + 600]
    for bad in ("Resource.overview", "Resource.genres", "Resource.dir_chain_json",
                "Resource.origin_country", "Resource.identify_note", "Resource.thumbnail"):
        assert bad not in block, f"{bad} 不能进轻量投影列"


def test_sqlite_cache_size_set():
    """v2.1.64 曾设 8 MiB；v2.1.67 内存优化回退到 2 MiB（口径反转，见 test_v2167_memory_projection）。

    实测（2026-11-29）：8 MiB/连接实际吃 9.5 MiB RSS 且连接归还后只回 2.9 MiB，
    46 条连接常态约 171 MiB 常驻；库文件仅 139 MB、OS page cache 已兜住读放大。
    历史背景保留在此测试，但断言必须跟随当前口径 -2048，否则全量假红。
    """
    assert "PRAGMA cache_size=-2048" in DB_SRC
    assert "cache_size=-8192" not in DB_SRC
    # cache_size 必须在两个池共用的 pragma 函数里（对所有连接生效）
    pragma_fn = DB_SRC[DB_SRC.index("def _sqlite_pragmas"):
                       DB_SRC.index("def _sqlite_pragmas") + 1800]
    assert "cache_size=-2048" in pragma_fn


def test_migration_runs_after_alter_columns():
    """迁移重建 SELECT 引用 category_path/english_title，必须在 ALTER 补列之后。"""
    alter_pos = DB_SRC.index('conn.execute(text(f"ALTER TABLE {table} ADD COLUMN')
    migrate_pos = DB_SRC.index("_migrate_mirror_media_unique_by_source()\n    # Bot token")
    assert alter_pos < migrate_pos, "必须先 ALTER 补列、再重建表，否则缺列时重建失败"
