"""v2.1.54：假死复盘后的两处「少烧一点」优化专项验收。

背景（2026-11-27 线上只读复盘）：
  整站假死的直接结构原因是连接池不隔离（已由同版双池修复）。但复盘时还看到两个
  **放大器**，它们不制造假死，却让假死来得更快、更难自愈：

  ① 前端进度轮询是**固定 2 秒、24 小时不停**，注释却写着「空闲降为 5 秒」——
     代码里根本没有降频逻辑。池一满，每次请求还要干等 30 秒才失败，
     几十条这样的请求叠起来就是连接与内存一起涨。
  ② `mirror_status` 和明细接口为了给几十行补作品名，执行
     `select(MirrorMedia).all()` 把**整张映射表的 ORM 实体**建出来。
     2 秒一次的接口，每 2 秒白建一遍全表对象。

本文件锁两件事：
  · 轮询改成自排程（运行中 2 秒 / 空闲 10 秒 / 后台页不发请求 / 切回来补一次）；
  · 作品名查询改两列投影，**且名字必须照样取得到**（功能不许退化）。
"""
from pathlib import Path

import pytest
from sqlalchemy import select

from app.db import SessionLocal, migrate
from app.main import MirrorRule, mirror_status
from app.models import MirrorFile, MirrorMedia

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = ROOT / "frontend" / "src" / "App.vue"
MAIN_PY = ROOT / "backend" / "app" / "main.py"

A_ROOT, B_ROOT = "A_ROOT", "B_ROOT"
WORK = "侠客行 (2001) {tmdb-135646}"


# ---------------------------------------------------------------- 运行环境

def _clean():
    with SessionLocal() as db:
        db.query(MirrorFile).delete()
        db.query(MirrorMedia).delete()
        db.query(MirrorRule).delete()
        db.commit()


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    _clean()
    yield
    _clean()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as session:
        try:
            yield session
        finally:
            session.rollback()


def _seed(db, *, status="failed", rename_status=""):
    rule = MirrorRule(name="v2154", source_root_id=A_ROOT, target_root_id=B_ROOT,
                      enabled=True, file_idle_seconds=0.5, auto_identify=False)
    db.add(rule)
    db.flush()
    media = MirrorMedia(rule_id=rule.id, work_key="work:xiakexing",
                        title_standard=WORK, title_mirror="侠客行 (2001)",
                        source_dir_id="A_DIR", target_dir_id="B_DIR",
                        english_title="Ode to Gallantry", status="synced")
    db.add(media)
    db.flush()
    row = MirrorFile(media_id=media.id, rule_id=rule.id, identity="tmdb:135646:tv:1:1",
                     source_file_id="A_F1", target_file_id="B_F1",
                     file_name="侠客行.S01E01.mkv", size=1024, utime=100,
                     fingerprint="1024:100", status=status,
                     rename_status=rename_status, error="复制失败：网络中断")
    db.add(row)
    db.commit()
    return rule, media, row


# ---------------------------------------------------------------- 行为：名字照样取得到

def test_file_errors_still_resolve_work_name(db):
    """投影改造后，失败文件明细里的作品名不能变成空串。"""
    _seed(db, status="failed")
    st = mirror_status(db=db)
    rows = st["file_errors"]
    assert rows, "失败文件明细不能为空"
    assert rows[0]["work"] == WORK


def test_retryable_rows_still_resolve_work_name(db):
    """同一份名字映射还服务待复制明细（计数与明细必须同源同谓词）。"""
    _seed(db, status="pending")
    st = mirror_status(db=db)
    rows = st["retryable_files"]
    assert rows, "待复制明细不能为空"
    assert rows[0]["work"] == WORK


def test_pending_items_still_resolve_work_name(db):
    """待办清单同样要带作品名 —— 三个消费方共用同一份映射。"""
    _seed(db, status="failed")
    st = mirror_status(db=db)
    items = [x for x in st["pending_items"] if x.get("kind") == "文件待复制"]
    assert items, "待办清单里应有待复制项"
    assert items[0]["work"] == WORK


def test_unknown_media_id_yields_empty_not_crash(db):
    """映射行被删过的孤儿文件：名字取不到就给空串，接口不许 500。"""
    rule, media, row = _seed(db, status="failed")
    media_id = media.id
    db.query(MirrorMedia).filter(MirrorMedia.id == media_id).delete()
    db.commit()
    st = mirror_status(db=db)
    assert st["file_errors"][0]["work"] == ""


# ---------------------------------------------------------------- 源码：不许回到整表实体

def test_status_endpoints_use_two_column_projection():
    """两个高频接口都必须用两列投影，不许再实例化整表实体。"""
    src = MAIN_PY.read_text(encoding="utf-8")
    assert "for m in db.scalars(select(MirrorMedia)).all()" not in src, \
        "仍有用整表 ORM 实体只为查名字的写法"
    assert src.count("select(MirrorMedia.id, MirrorMedia.title_standard)") >= 2


# ---------------------------------------------------------------- 前端：自排程轮询

def test_frontend_poll_is_self_scheduled_not_fixed_interval():
    app = APP_VUE.read_text(encoding="utf-8")
    assert "setInterval(pollMirrorStatus" not in app, \
        "固定间隔轮询在慢响应下会叠加请求，必须改自排程"
    assert "function scheduleMirrorPoll(" in app
    assert "clearTimeout(mirrorPollTimer)" in app


def test_frontend_poll_intervals_busy_and_idle():
    app = APP_VUE.read_text(encoding="utf-8")
    assert "const MIRROR_POLL_BUSY = 2000" in app
    assert "const MIRROR_POLL_IDLE = 10000" in app, "空闲必须降频，不许仍是 2 秒"
    assert "mirrorPollBusy() ? MIRROR_POLL_BUSY : MIRROR_POLL_IDLE" in app


def test_frontend_poll_helper_not_shadowing_busy_ref():
    """`mirrorBusy` 是按钮 loading 的 ref，轮询判据必须另起名字，否则编译直接失败。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "function mirrorBusy()" not in app


def test_frontend_poll_detects_running_state():
    """「有没有活」的判据：本轮进度在跑，或批次在跑。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert ".progress?.running" in app
    assert "batchInfo.value?.status" in app


def test_frontend_poll_pauses_when_tab_hidden():
    """切到后台完全不发请求；切回来立刻补一次，并注册/移除监听。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "document.visibilityState !== 'hidden'" in app
    assert "document.addEventListener('visibilitychange', onVisibilityChange)" in app
    assert "document.removeEventListener('visibilitychange', onVisibilityChange)" in app


def test_frontend_unmount_clears_poll_timer():
    app = APP_VUE.read_text(encoding="utf-8")
    line = next((ln for ln in app.splitlines() if ln.startswith("onUnmounted(")), "")
    assert "clearTimeout(mirrorPollTimer)" in line, "卸载必须清掉自排程定时器"


def test_frontend_idle_hint_matches_real_behavior():
    """页面文案不许再宣称「空闲也每 2 秒」——要和真实行为一致。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "空闲每 10 秒刷新" in app
    assert "空闲时每 10 秒刷新" in app
