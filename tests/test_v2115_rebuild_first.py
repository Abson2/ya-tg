"""v2.1.15 验收（v2.1.16 已随名额语义调整）：重建首推的恢复通道。

端点 `POST /sc/plan/rebuild-first` 仍然保留，语义变为「把轮转位置归零 + 立刻跑一轮」。
v2.1.16 起 plan_engine 候选改为**环形轮转**，所以「游标越过历史快照」不再造成永久
卡死（那条反证已迁到 test_v2116_queue_reset.py 的环形轮转用例）。本文件继续锁：

  ① 预演默认 dry_run，且数字与真实执行同源；
  ② 执行只归零游标，**绝不删除**任何清单项 / 任务 / 执行记录；
  ③ 名额仍被有效推送占用时，重建也不会重复首推。
"""
import json
import os
import shutil
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v2115_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'v.db')}")
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    eng.dispose()
    shutil.rmtree(d, ignore_errors=True)


def _resource(db, file_id="f1", name="Show (2024) {tmdb-1}/Show.S01E01.mkv"):
    from app.models import Resource
    r = Resource(
        file_id=file_id, name=name, ext=".mkv", size=100, utime=1,
        identify_status="identified", media_type="tv", tmdb_id=1,
        media_title="Show", path_hint="Show (2024)",
        share_dir_id="dir_show", share_scope="series_dir", share_dir_name="Show",
        genres=json.dumps([{"id": 18, "name": "Drama"}]),
        poster_path="/poster.jpg",
    )
    db.add(r)
    db.commit()
    return r


def _channel(db, chat_id="-1001"):
    from app.models import Bot, Channel
    bot = db.scalar(select(Bot))
    if not bot:
        bot = Bot(name="test", token="enc", enabled=True)
        db.add(bot)
        db.flush()
    ch = Channel(bot_id=bot.id, name="group", chat_id=chat_id, enabled=True, kind="group")
    db.add(ch)
    db.commit()
    return ch


def _work_key(db, res):
    from app import utils
    return utils.work_group_key(res)


def _ready(db):
    """建资源 + 快照 + 渠道 + 整日窗口，让 plan_engine 与运行时刻无关。"""
    from app import share_center_v2 as sc2
    from conftest import ensure_full_day_hour_window
    sc2.ensure_defaults(db)
    res = _resource(db)
    sc2.sync_library_versions(db)
    ch = _channel(db)
    ensure_full_day_hour_window(db, sc2._active_version(db).id)
    return res, ch


def _work_category(db):
    from app.models import SCWorkCategory
    row = db.scalar(select(SCWorkCategory))
    return row.category_key if row else ""


def _published_item(db, res, ch, *, status="published", link_state="active"):
    """造一条首推清单项 + 任务（默认「已推成功且链接有效」）。"""
    from app.models import SCPublishTask, SCPushListItem
    wk = _work_key(db, res)
    it = SCPushListItem(list_id=1, work_key=wk, chat_id=str(ch.chat_id),
                        dedup_key=f"{wk}:{ch.id}:first", source_kind="first",
                        status="planned")
    db.add(it)
    db.flush()
    task = SCPublishTask(list_item_id=it.id, work_key=wk, kind="first",
                         status=status, link_state=link_state,
                         share_url="https://example/s/1" if link_state == "active" else "")
    db.add(task)
    db.commit()
    return it, task


# ---------- ① 有效推送占用名额 → 重建也不重复首推 ----------

def test_published_task_with_live_link_blocks_rebuild(env):
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env
    with S() as db:
        res, ch = _ready(db)
        _published_item(db, res, ch, status="published", link_state="active")
        pre = sc2.preview_first_rebuild(db)
        assert pre["ok"] is True
        assert pre["blocked_pushed"] >= 1, pre
        assert pre["would_create"] == 0 and pre["would_revive"] == 0, pre
        out = sc2.api_rebuild_first(sc2.Payload(payload={"dry_run": False}), db)
        assert out["plan"]["tasks"] == 0, out
        assert len(list(db.scalars(select(SCPublishTask)).all())) == 1, "只应保留原任务"


def test_rebuild_preview_is_dry_by_default(env):
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env
    with S() as db:
        _ready(db)
        out = sc2.api_rebuild_first(sc2.Payload(payload={}), db)
        assert out["ok"] is True and "cursors_reset" not in out
        assert not list(db.scalars(select(SCPublishTask)).all())


# ---------- ② 执行只归零游标，不删任何历史行 ----------

def test_rebuild_keeps_all_history_rows(env):
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask, SCPushListItem
    S, _ = env
    with S() as db:
        res, ch = _ready(db)
        _published_item(db, res, ch, status="cleaned", link_state="cleaned")
        before_items = len(list(db.scalars(select(SCPushListItem)).all()))
        before_tasks = len(list(db.scalars(select(SCPublishTask)).all()))
        sc2.api_rebuild_first(sc2.Payload(payload={"dry_run": False}), db)
        assert len(list(db.scalars(select(SCPushListItem)).all())) >= before_items
        assert len(list(db.scalars(select(SCPublishTask)).all())) >= before_tasks


def test_rebuild_resets_cursor(env):
    from app import share_center_v2 as sc2
    from app.models import SCCategoryCursor
    S, _ = env
    with S() as db:
        _ready(db)
        version = sc2._active_version(db)
        cat = _work_category(db)
        assert cat, "作品必须有分类，否则用例前提不成立"
        db.add(SCCategoryCursor(plan_version_id=version.id, category_key=cat,
                                last_library_version_id=999999))
        db.commit()
        out = sc2.api_rebuild_first(sc2.Payload(payload={"dry_run": False}), db)
        assert out["ok"] and out["cursors_reset"] >= 1, out
        cursors = list(db.scalars(select(SCCategoryCursor)).all())
        assert all(c.last_library_version_id < 999999 for c in cursors), \
            [c.last_library_version_id for c in cursors]


# ---------- ③ 前端入口 ----------

def test_frontend_has_rebuild_button_and_dry_run_first():
    assert "重建首推任务" in APP
    assert "scRebuildFirst" in APP
    assert "/sc/plan/rebuild-first" in APP
    assert "dry_run: true" in APP and "dry_run: false" in APP, "必须先预演再执行"
    assert "已推送且分享链接仍有效而跳过" in APP, "确认框必须如实报出被跳过的条数"


def test_frontend_does_not_leak_backend_terms():
    block_start = APP.index("async function scRebuildFirst")
    block = APP[block_start:block_start + 2600]
    for term in ("last_library_version_id", "first_dedup_plan", "SCPushListItem",
                 "SCPublishTask", "work_key"):
        assert term not in block, f"页面文案不得体现后端名词：{term}"
