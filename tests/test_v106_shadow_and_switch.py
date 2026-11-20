"""四模块重构：分享中心登记 + 主链开关护栏。

覆盖：
  - v1.1 主链默认开启 is_enabled=True；false/no 可回退旧链排障；
  - _enqueue_ready 入队后登记 ShareTask（first），QueueItem.share_task_id 回填；
  - 登记异常不影响主入队链（注入 ensure_task 抛错，QueueItem 仍写入）；
  - process_change_events 更新事件登记 update 任务，ev.share_task_id 回填；
  - review（auto_publish=False）项不登记分享任务，绕过审核闸门属违规。
"""
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
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
    from app import runtime_config
    d = tempfile.mkdtemp(prefix="v106b_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'b.db')}")
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng, runtime_config
    shutil.rmtree(d, ignore_errors=True)


def _seed(db):
    from app.models import Bot, Channel, Resource, Rule
    bot = Bot(name="b", token="enc", enabled=True)
    db.add(bot); db.flush()
    ch = Channel(bot_id=bot.id, name="c", chat_id="-1", enabled=True)
    db.add(ch); db.flush()
    rule = Rule(name="r", channel_id=ch.id, enabled=True, auto_publish=True)
    db.add(rule); db.flush()
    res = Resource(file_id="f-shadow", name="Show.S01E01.mkv", ext=".mkv", size=1, utime=1,
                   identify_status="identified", media_type="tv", tmdb_id=7,
                   media_title="Show", poster_path="/p.jpg", path_hint="Show (2020)/Season 1",
                   share_dir_id="dir_s", share_scope="series_dir", share_dir_name="Show")
    db.add(res); db.commit()
    return res, rule, ch


def test_switch_defaults_on(env):
    from app import share_center as sc
    S, _, rc = env
    with S() as db:
        # v1.1：分享中心默认接管主链
        assert sc.is_enabled(db) is True
        rc.set_many(db, {"share_center_enabled": "true"})
        assert sc.is_enabled(db) is True
        rc.set_many(db, {"share_center_enabled": "1"})
        assert sc.is_enabled(db) is True
        rc.set_many(db, {"share_center_enabled": "no"})
        assert sc.is_enabled(db) is False
        rc.set_many(db, {"share_center_enabled": "false"})
        assert sc.is_enabled(db) is False


def test_enqueue_ready_shadow_register(env):
    from app import main, share_center as sc
    from app.models import QueueItem, ShareTask
    S, _, _ = env
    with S() as db:
        res, rule, ch = _seed(db)
        assert main._enqueue_ready(db, res, rule) is True
        q = db.scalar(select(QueueItem).where(QueueItem.resource_id == res.id))
        assert q is not None and q.status == "pending"  # 旧链行为不变
        assert q.share_task_id > 0
        assert q.push_reason == "first"
        st = db.get(ShareTask, q.share_task_id)
        assert st is not None and st.kind == "first"
        assert st.work_key == sc.task_work_key(res)


def test_shadow_failure_does_not_break_enqueue(env, monkeypatch):
    from app import main, share_center as sc
    from app.models import QueueItem
    S, _, _ = env
    def boom(*a, **kw):
        raise RuntimeError("shadow down")
    monkeypatch.setattr(sc, "shadow_register", boom)
    with S() as db:
        res, rule, ch = _seed(db)
        assert main._enqueue_ready(db, res, rule) is True
        assert db.scalar(select(QueueItem).where(QueueItem.resource_id == res.id)) is not None


def test_review_item_not_registered(env):
    """auto_publish=False 的 review 项不得登记分享任务，避免绕过人工审核闸门。"""
    from app import main
    from app.models import QueueItem, Rule, ShareTask
    S, _, _ = env
    with S() as db:
        res, _, _ = _seed(db)
        rule = db.scalars(select(Rule).limit(1)).first()
        rule.auto_publish = False
        db.commit()
        assert main._enqueue_ready(db, res, rule) is True
        q = db.scalar(select(QueueItem).where(QueueItem.resource_id == res.id))
        assert q is not None and q.status == "review"
        assert not q.share_task_id
        assert db.scalar(select(ShareTask).where(ShareTask.resource_id == res.id)) is None


def test_change_event_registers_update_task(env):
    from app import main, share_center as sc
    from app.models import DistributionEvent
    S, _, _ = env
    with S() as db:
        res, rule, ch = _seed(db)
        main._enqueue_ready(db, res, rule)  # 先有 first + 队列项
        db.commit()
        ev = DistributionEvent(work_key=sc.task_work_key(res), event_type="updated",
                               fingerprint="fp1", changed_files=1)
        db.add(ev); db.commit()
        stats = main.process_change_events(db, publish=True)
        assert stats["events"] >= 1
        # 同一 session 内对象即被 process_change_events 就地修改（该函数不 commit，由调用方提交）
        assert ev.share_task_id > 0
        st = db.get(sc.ShareTask, ev.share_task_id)
        assert st is not None and st.kind == "update"
        assert st.parent_event_id == ev.id
