"""v2.1.65 验收：推送代表资源优先「已识别 + 有海报」+ 快照元数据补海报。

真机事故（2026-11-28 线上只读探针）：
  李卫当官 (2002) 推送无海报、无 TMDB、无简介，只有标题和文件名/大小。
  62 条资源全部 identified + poster_path 有值，但快照 resource_ids_json 第一个
  id（10669）恰好是未识别空壳行（identify_status=unidentified / poster_path 空 /
  media_title 空）。_RepResolver.resolve() 取它当代表 → publisher 按空 poster_path
  退化成 sendMessage 纯文字。全库 1186 部里 365 部代表命中空壳行，89 部组内可救。
  另：快照 metadata_json 有 886/1186 部 poster_path 空——recompute_version_metadata
  的回填字段列表漏了 poster_path，识别回填的海报永远进不了快照。

收口：
  ① 新增 _pick_representative：优先级 已识别+海报 > 已识别 > 有海报 > 原顺序第一条。
  ② resolve() 快照命中一次查出全部成员再挑代表（顺带去 N+1）。
  ③ _build() 实时回退也用同一口径。
  ④ recompute_version_metadata 字段列表补 poster_path（判不出/空值不写，不造版本）。
"""
import json
import os
import sys
from pathlib import Path

import pytest
from sqlalchemy import delete

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

SC2_SRC = (BACKEND / "app" / "share_center_v2.py").read_text(encoding="utf-8")


@pytest.fixture(autouse=True)
def _fresh():
    """与 v2157 同范式：先 migrate 建表，再逐用例清空，避免跨用例串扰。"""
    from app.db import SessionLocal, migrate
    from app.models import Resource, SCLibraryVersion
    migrate()
    for _ in range(2):
        with SessionLocal() as db:
            db.execute(delete(SCLibraryVersion))
            db.execute(delete(Resource))
            db.commit()
    yield


class R:
    """轻量资源行：只需 resolve/_pick 读到的属性。"""
    def __init__(self, id, *, identified=False, poster="", file_id=""):
        self.id = id
        self.identify_status = "identified" if identified else "unidentified"
        self.poster_path = poster
        self.file_id = file_id or f"F{id}"
        self.size = 100
        self.utime = 1


# ---------------------------------------------------------------------------
# ① _pick_representative 优先级
# ---------------------------------------------------------------------------

def test_pick_prefers_identified_with_poster():
    from app.share_center_v2 import _pick_representative
    shell = R(1)  # 快照第一条：未识别空壳
    good = R(2, identified=True, poster="/p.jpg")
    assert _pick_representative([shell, good]) is good


def test_pick_prefers_identified_over_poster_only():
    from app.share_center_v2 import _pick_representative
    poster_only = R(1, poster="/p.jpg")          # 有海报但未识别
    identified = R(2, identified=True)           # 已识别但无海报
    assert _pick_representative([poster_only, identified]) is identified


def test_pick_poster_only_over_empty_shell():
    from app.share_center_v2 import _pick_representative
    shell = R(1)
    poster_only = R(2, poster="/p.jpg")
    assert _pick_representative([shell, poster_only]) is poster_only


def test_pick_all_empty_falls_back_to_first():
    from app.share_center_v2 import _pick_representative
    a, b = R(1), R(2)
    assert _pick_representative([a, b]) is a  # 老行为，绝不返回 None


def test_pick_empty_list_returns_none():
    from app.share_center_v2 import _pick_representative
    assert _pick_representative([]) is None


# ---------------------------------------------------------------------------
# ② resolve() 快照命中：李卫当官场景
# ---------------------------------------------------------------------------

def test_resolve_snapshot_picks_identified_poster_not_first():
    """快照第一个 id 是未识别空壳 → 必须选后面已识别+有海报那条。"""
    from app.models import SCLibraryVersion
    from app.share_center_v2 import _RepResolver
    from app.db import SessionLocal

    with SessionLocal() as db:
        from app.models import Resource
        shell = Resource(file_id="v2165-shell", name="shell.mp4", size=1, utime=1,
                         identify_status="unidentified", poster_path="", media_title="")
        good = Resource(file_id="v2165-good", name="good.mp4", size=2, utime=2,
                        identify_status="identified", poster_path="/p.jpg",
                        media_title="李卫当官")
        db.add_all([shell, good]); db.flush()
        lv = SCLibraryVersion(work_key="work:李卫当官", version_no=1, fingerprint="fp",
                              media_type="tv", change_type="created",
                              resource_ids_json=json.dumps([shell.id, good.id]),
                              metadata_json=json.dumps({"poster_path": ""}))
        db.add(lv); db.commit()
        resolver = _RepResolver(db)
        rep, err = resolver.resolve(lv)
        assert err == ""
        assert rep.id == good.id, "代表资源不能是未识别空壳行"
        assert rep.poster_path == "/p.jpg"


# ---------------------------------------------------------------------------
# ③ 源码级反证：三处口径都改了，别回退
# ---------------------------------------------------------------------------

def test_pick_wired_into_resolve_and_build():
    assert "return _pick_representative(rows), \"\"" in SC2_SRC
    assert "table[wk] = _pick_representative(ordered)" in SC2_SRC
    # resolve 不再「第一个存在就 return」
    assert "if res is not None:\n                return res, \"\"" not in SC2_SRC


def test_recompute_wires_poster_path():
    block = SC2_SRC[SC2_SRC.index('for field in ("title", "share_dir_id"'):
                    SC2_SRC.index('for field in ("title", "share_dir_id"') + 300]
    assert '"poster_path"' in block


# ---------------------------------------------------------------------------
# ④ recompute_version_metadata 补 poster_path
# ---------------------------------------------------------------------------

def test_recompute_fills_poster_path_without_new_version():
    from app.db import SessionLocal
    from app.models import Resource, SCLibraryVersion
    from app import share_center_v2 as sc2

    with SessionLocal() as db:
        ids = []
        for i in range(3):
            r = Resource(file_id=f"v2165-m{i}", name=f"剧.S01E0{i+1}.mkv", ext=".mkv",
                         size=100, utime=1, identify_status="identified", media_type="tv",
                         tmdb_id=87590, media_title="李卫当官",
                         share_dir_name="李·卫·当·官 (2002) {tmdb-87590}",
                         share_scope="series_dir", share_dir_id="dir-work",
                         parent_id="dir-season",
                         path_hint="剧集/李·卫·当·官 (2002)/Season 1",
                         poster_path="/48Q7HqaOOrq2SV4BnBNguPzcbh0.jpg")
            db.add(r); db.flush(); ids.append(r.id)
        v = SCLibraryVersion(work_key="work:李卫当官", version_no=1, fingerprint="fp-x",
                             media_type="tv", change_type="created",
                             resource_ids_json=json.dumps(ids),
                             metadata_json=json.dumps({"title": "李卫当官.2002.S03E21.mp4",
                                                       "poster_path": "", "tmdb_id": 0,
                                                       "share_dir_id": "dir-season"}))
        db.add(v); db.commit()
        before = db.query(SCLibraryVersion).count()
        out = sc2.recompute_version_metadata(db)
        assert out["fixed"] == 1, out
        db.expire_all()
        v2 = db.get(SCLibraryVersion, v.id)
        meta = json.loads(v2.metadata_json)
        assert meta["poster_path"] == "/48Q7HqaOOrq2SV4BnBNguPzcbh0.jpg", meta
        # 红线：不造新版本、不动指纹/版本号/变更类型
        assert db.query(SCLibraryVersion).count() == before
        assert v2.fingerprint == "fp-x" and int(v2.version_no) == 1
        assert v2.change_type == "created"
        # 幂等
        assert sc2.recompute_version_metadata(db)["fixed"] == 0


def test_recompute_does_not_erase_existing_poster():
    """判不出（组内无海报）时保留旧海报值，绝不抹掉、绝不猜。"""
    from app.db import SessionLocal
    from app.models import Resource, SCLibraryVersion
    from app import share_center_v2 as sc2

    with SessionLocal() as db:
        ids = []
        for i in range(2):
            r = Resource(file_id=f"v2165-np{i}", name=f"剧.S01E0{i+1}.mkv", ext=".mkv",
                         size=100, utime=1, identify_status="identified", media_type="tv",
                         tmdb_id=87590, media_title="李卫当官",
                         share_dir_name="李·卫·当·官 (2002)", share_scope="series_dir",
                         share_dir_id="dir-work", parent_id="dir-season",
                         path_hint="剧集/李·卫·当·官 (2002)/Season 1", poster_path="")
            db.add(r); db.flush(); ids.append(r.id)
        v = SCLibraryVersion(work_key="work:李卫当官", version_no=1, fingerprint="fp",
                             media_type="tv", change_type="created",
                             resource_ids_json=json.dumps(ids),
                             metadata_json=json.dumps({"title": "李卫当官",
                                                       "poster_path": "/old.jpg"}))
        db.add(v); db.commit()
        sc2.recompute_version_metadata(db)
        db.expire_all()
        meta = json.loads(db.get(SCLibraryVersion, v.id).metadata_json)
        assert meta["poster_path"] == "/old.jpg", meta
