"""v2.1.59 回归测试：网页体验三项优化 + library_series 内存投影。

真机背景（2026-11-28）：
- 刷新显示一会「正在恢复会话」才进；登录输密码后不立即响应；进页面数据全空很久。
- 内存冲到 1G：主进程 953MB，主因 `/api/library/series` 全表 `select(Resource)`
  把 25,663 行全实体化（单实体约 5KiB → 一次 +120MiB），而前端每次刷新都打它。

三项收口：
① 启动：有 token **立即放行**（不再 await 全量 load），数据后台补。
② 登录：token 落盘立即放行 + 弹提示，load 后台跑（不再 await 完才进）。
③ 加载：`load()` 每条接口 fulfilled 即回填自己模块（applyLoadItem），不等全量。
④ 内存：library_series 改列投影 + SimpleNamespace，禁止退回全量 ORM 实体。

每条都有反证：把口径改回去必须变红。
"""
from pathlib import Path

from app.db import SessionLocal, migrate
from app.main import library_series
from app.models import Resource

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
MAIN = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")


def _clean():
    db = SessionLocal()
    db.query(Resource).filter(Resource.file_id.like("v2159-%")).delete(synchronize_session=False)
    db.commit()
    db.close()


def _res(seq, name, **kw):
    return Resource(file_id=f"v2159-{seq}", name=name, ext=".mkv", size=1, utime=1, **kw)


# ---------- 一、启动立即放行（不再 await 全量 load） ----------
def test_boot_releases_immediately_not_after_load():
    seg = APP[APP.index("if (sessionToken.value) {"):]
    seg = seg[:seg.index("sessionPollTimer = setInterval")]
    # 立即放行必须在调用 load() 之前（v2.1.58 是 await 完才落 authed）
    i_true = seg.index("authed.value = true")
    i_load = seg.index("load().catch")
    assert i_true < i_load, "放行必须早于后台加载，否则就是旧的「等全量」"
    # 再压一道：不许回到 await 总闸阻塞放行
    assert "await withTimeout(loadP, BOOT_TIMEOUT_MS, '首次加载')" not in seg
    # 总闸仍在（后台收敛用）
    assert "withTimeout(loadP, BOOT_TIMEOUT_MS, '首次加载')" in seg


# ---------- 二、登录立即响应（不再 await 完 load 才进） ----------
def test_login_releases_before_background_load():
    seg = APP[APP.index("async function login()"):]
    seg = seg[:seg.index("async function logout()")]
    # 登录成功必须先把 authed 落 true + 弹提示，再把 load 丢后台
    assert "authed.value = true" in seg
    i_true = seg.index("authed.value = true")
    i_load = seg.index("load().catch")
    assert i_true < i_load, "登录放行必须早于后台加载"
    # 反证：旧的 `await load()` 后置放行不许回来
    assert "await load()\n    authed.value = true" not in seg
    # 后台加载带总闸兜底
    assert "withTimeout(loadP, BOOT_TIMEOUT_MS, '登录加载')" in seg


# ---------- 三、load 逐接口回填（谁先回来谁先显示） ----------
def test_load_applies_each_item_as_it_arrives():
    # 必须存在按索引回填的分派函数
    assert "function applyLoadItem(i, v)" in APP
    # 每个请求 fulfilled 立即回填，而不是 allSettled 后统一赋值
    assert ".then(v => applyLoadItem(i, v))" in APP
    # 反证：旧的「先 await allSettled 再 const v = i => settled... 统一赋」不许回来
    assert "const settled = await Promise.allSettled(keys.map(k => api('/' + k)))" not in APP
    # 完成语义仍在：仍 await 全部落定，供总闸收敛
    assert "await Promise.allSettled(keys.map((k, i) =>" in APP


# ---------- 四、library_series 内存投影（不许全量 ORM） ----------
def test_library_series_uses_column_projection():
    seg = MAIN[MAIN.index("@app.get(\"/api/library/series\""):]
    seg = seg[:seg.index("@app.get(\"/api/library/finish-check\"")]
    # 反证：全量实体化 select(Resource) 不许回来
    assert "select(Resource).order_by(Resource.id.desc())" not in seg
    assert "db.scalars(stmt).all()" not in seg
    # 必须是显式列投影 + SimpleNamespace
    assert "select(*_cols)" in seg
    assert "from types import SimpleNamespace" in seg
    assert "rows = [SimpleNamespace(**dict(r._mapping)) for r in db.execute(stmt)]" in seg
    # 另外两处分享查询也不许全实体化
    assert "select(_SR.work_key, _SR.link_state)" in seg
    # SCPublishTask 投影（代码里跨行书写，按列名断言）
    assert "_SPT.work_key, _SPT.share_url, _SPT.link_state, _SPT.status" in seg


def test_library_series_projection_still_aggregates():
    """投影后聚合语义不变：跨集取标题/分类/识别状态/集数仍正确。"""
    migrate(); _clean()
    db = SessionLocal()
    db.add(_res("s1", "Show.S01E01.mkv", identify_status="identified",
                media_title="谷雨", media_type="tv", tmdb_id=991001,
                genres='["剧情"]', season=1, episode=1))
    db.add(_res("s2", "Show.S02E05.mkv", identify_status="failed",
                media_title="谷雨", media_type="tv", tmdb_id=991001,
                genres='["剧情"]', season=2, episode=5))
    # 同一作品：至少一集 identified 即作品级 identified（v2.1.58 粘性）
    db.add(_res("s3", "Show.S02E06.mkv", identify_status="identified",
                media_title="谷雨", media_type="tv", tmdb_id=991001,
                genres='["剧情"]', season=2, episode=6))
    db.commit()
    groups = library_series(q="", media_type="", db=db)
    db.close()
    g = next(x for x in groups if x.get("tmdb_id") == 991001)
    assert g["identify_status"] == "identified", g["identify_status"]
    assert g["count"] == 3, g["count"]
    assert g["identified_count"] == 2, g["identified_count"]
    # seasons 内部 set 被 pop 成 season_count 返回（不是 seasons 列表）
    assert g["season_count"] == 2, g["season_count"]
    assert g["media_type"] == "tv"
    _clean()
