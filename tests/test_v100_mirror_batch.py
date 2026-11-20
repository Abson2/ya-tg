"""阶段二路线B：批次控制状态机与序列持久化底座单测。

覆盖：
  - 批次开启/收尾、控制指令幂等与边界；
  - 序列任务落库、进度统计、未终结查询；
  - 重启续跑定位（取第一个未终结任务）；
  - 旧 task_status='' 历史任务不参与（TERMINAL 判定明确）。
用独立临时库，避免污染全局测试库。
"""
import shutil
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app import db as dbmod
from app import mirror_batch
from app import models  # noqa: F401  注册 metadata


@pytest.fixture
def env():
    root = Path(tempfile.mkdtemp(prefix="v100_batch_", dir="/tmp"))
    eng = create_engine(f"sqlite:///{root / 'b.db'}")
    dbmod.Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False, autocommit=False)
    with S() as db:
        rule = models.MirrorRule(name="t", source_root_id="A", target_root_id="B")
        db.add(rule)
        db.commit()
        db.refresh(rule)
        yield db, rule
    eng.dispose()
    shutil.rmtree(root, ignore_errors=True)


def test_new_batch_id_unique():
    a = mirror_batch.new_batch_id(7)
    b = mirror_batch.new_batch_id(7)
    assert a.startswith("b7-") and a != b


def test_start_and_finish_batch(env):
    db, rule = env
    bid = mirror_batch.start_batch(db, rule)
    assert bid == rule.batch_id and rule.batch_status == "running"
    assert rule.cursor_sequence == 0
    mirror_batch.request_control(db, rule, "pause")
    an = mirror_batch.request_control(db, rule, "pause")  # pause 幂等（running 时重复置同值）
    assert an["status"] == "running"  # 指令已记录，但状态要到执行器边界才改
    mirror_batch.finish_batch(db, rule)
    assert rule.batch_status == "idle" and rule.control_action == ""


def test_control_transitions(env):
    db, rule = env
    mirror_batch.start_batch(db, rule)
    # resume 在 running 下清任何指令
    mirror_batch.request_control(db, rule, "skip")
    st = mirror_batch.request_control(db, rule, "resume")
    assert st["status"] == "running" and rule.control_action == ""
    # pause 指令可被消费
    mirror_batch.request_control(db, rule, "pause")
    assert mirror_batch.consume_control(db, rule) == "pause"
    assert mirror_batch.consume_control(db, rule) == ""  # 一次性
    # 未知指令拒绝
    with pytest.raises(ValueError):
        mirror_batch.request_control(db, rule, "explode")


def test_sequence_tasks_progress_and_resume(env):
    db, rule = env
    bid = mirror_batch.start_batch(db, rule)
    t1 = mirror_batch.add_sequence_task(db, rule_id=rule.id, media_id=1,
                                        batch_id=bid, sequence_id=1, parent_sequence_id=0)
    t2 = mirror_batch.add_sequence_task(db, rule_id=rule.id, media_id=1,
                                        batch_id=bid, sequence_id=2, parent_sequence_id=1)
    t3 = mirror_batch.add_sequence_task(db, rule_id=rule.id, media_id=1,
                                        batch_id=bid, sequence_id=3, parent_sequence_id=1)
    assert [t.sequence_id for t in mirror_batch.sequence_tasks(db, bid)] == [1, 2, 3]
    assert len(mirror_batch.pending_sequences(db, bid)) == 3
    # 完成 1、跳过 2 → 续跑定位到 3
    t1.task_status = "completed"
    t2.task_status = "skipped"
    db.commit()
    assert mirror_batch.resume_sequence(db, rule).sequence_id == 3
    prog = mirror_batch.batch_progress(db, bid)
    assert prog["total"] == 3 and prog["done"] == 2 and prog["pending"] == 1
    assert prog["percent"] == 66 and prog["current_sequence"] == 3
    # 全部终结 → 无续跑任务
    t3.task_status = "failed"
    db.commit()
    assert mirror_batch.resume_sequence(db, rule) is None
    assert mirror_batch.batch_progress(db, bid)["percent"] == 100


def test_legacy_empty_task_status_counts_as_pending(env):
    """旧任务 task_status='' 未列入 TERMINAL，仍视为待处理（不会被误判完成）。"""
    db, rule = env
    bid = mirror_batch.start_batch(db, rule)
    t = mirror_batch.add_sequence_task(db, rule_id=rule.id, media_id=1,
                                       batch_id=bid, sequence_id=1, parent_sequence_id=0)
    t.task_status = ""  # 模拟旧数据
    db.commit()
    assert len(mirror_batch.pending_sequences(db, bid)) == 1
