"""v2.1.51：稳定性三件事（用户口径「我只要系统越稳定越好」）。

背景（2026-11-25 22:05 真机看板）：`作品级·镜像失败` 挂着 1 部「完美世界」，
而文件明细里 284/284 全部 `synced`。v2.1.50 已经把「作品状态由谁定」收成唯一判据，
但**重算只在作品被扫到/被轮询/被重试时发生** —— 那部作品因为历史清单损坏一直不进
本轮 works 清单，于是没有任何路径去重算它，状态就那么永久挂着。

本文件锁死三件事：
  ① **状态自愈巡检** `reconcile_stale_media_status`：不依赖作品是否进 works 清单，
     用一条 SQL 找出「有文件行、且不存在非终态行、状态却不是 synced」的作品纠正回来；
     无文件行的作品（目录都没建出来）绝不能被翻成 synced。
  ② **轮询失败先核对 B 库**：光鸭 task_status != 2 可能是「轮询次数用完/超时」而文件
     其实已落库；旧写法一律判失败，白耗一次自动尝试次数，还要等下一轮全局 backfill 才救。
  ③ **待重试作品批设上限 + 轮转**：这类作品是追加进队列、不占名额的（v2.1.9 设计），
     失败作品一多单轮时长就没有上限；按「最久没被处理过」取前 N 部，不按目录顺序硬截
     （那样排在后面的作品永远轮不到，是更坏的回归）。
"""
import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

from app import mirror                                   # noqa: E402
from app.models import (                                  # noqa: E402
    AppConfig, MirrorFile, MirrorMedia, MirrorRule, MirrorTask,
)
from app.runtime_config import DEFAULTS                   # noqa: E402

MIRROR_SRC = (BACKEND / "app" / "mirror.py").read_text(encoding="utf-8")
MAIN_SRC = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")

A_WORK, B_WORK = "A_W1", "B_W1"
WORK = "完美世界 (2021) {tmdb-124003}"
SIZE, FP = 1000, 111


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v2151_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'b.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng, d
    shutil.rmtree(d, ignore_errors=True)


def _rule(db, *, max_works=0, name="m51"):
    r = MirrorRule(name=name, source_root_id="A-ROOT", target_root_id="B-ROOT",
                   interval_minutes=60, enabled=True, file_idle_seconds=0.01,
                   max_works_per_run=max_works)
    db.add(r)
    db.commit()
    db.refresh(r)
    return r


def _media(db, rule, *, key=WORK, status="failed", with_files=True):
    m = MirrorMedia(rule_id=rule.id, work_key="work:" + key, title_standard=key,
                    media_type="tv", source_dir_id=A_WORK, target_dir_id=B_WORK,
                    status=status)
    db.add(m)
    db.commit()
    if with_files:
        db.add(MirrorFile(rule_id=rule.id, media_id=m.id, identity="i1",
                          source_file_id="F1", file_name="S01E01.mkv",
                          size=SIZE, utime=FP, status="synced"))
        db.add(MirrorFile(rule_id=rule.id, media_id=m.id, identity="i2",
                          source_file_id="F2", file_name="S01E02.mkv",
                          size=SIZE, utime=FP, status="synced"))
        db.commit()
    db.refresh(m)
    return m


# ---------------------------------------------------------------------------
# ① 状态自愈巡检：不依赖作品是否在本轮 works 清单里
# ---------------------------------------------------------------------------
def test_heal_flips_stale_failed_media(env):
    """真机那一条：284 行全 synced、作品还是 failed → 直接纠正。"""
    S, _, _ = env
    with S() as db:
        r = _rule(db)
        m = _media(db, r)
        assert m.status == "failed"

        changed = mirror.reconcile_stale_media_status(db, r.id)
        assert changed == [(m.id, "synced")], changed
        db.expire_all()
        assert db.get(MirrorMedia, m.id).status == "synced"


def test_heal_skips_media_with_open_rows(env):
    """还有待复制/失败行：不许因为「不在 works 清单」就被抹平。"""
    S, _, _ = env
    with S() as db:
        r = _rule(db)
        m = _media(db, r)
        row = db.query(MirrorFile).filter(MirrorFile.media_id == m.id).first()
        row.status = "pending"
        db.commit()

        assert mirror.reconcile_stale_media_status(db, r.id) == []
        db.expire_all()
        assert db.get(MirrorMedia, m.id).status == "failed"


def test_heal_skips_media_without_any_file_row(env):
    """目录都没建出来、一条文件行都没有的 failed 作品：翻成 synced 就是骗人。"""
    S, _, _ = env
    with S() as db:
        r = _rule(db)
        m = _media(db, r, key="只有目录失败的作品", with_files=False)
        assert mirror.reconcile_stale_media_status(db, r.id) == []
        db.expire_all()
        assert db.get(MirrorMedia, m.id).status == "failed"


def test_heal_covers_stuck_syncing(env):
    """卡在 syncing（行全完成、状态没落回终态）的也要纠正 —— status != synced 都算候选。"""
    S, _, _ = env
    with S() as db:
        r = _rule(db)
        m = _media(db, r, status="syncing")
        assert mirror.reconcile_stale_media_status(db, r.id) == [(m.id, "synced")]


def test_heal_is_rule_scoped(env):
    """只修本规则的作品，别去动别的规则（多规则共用一张表）。"""
    S, _, _ = env
    with S() as db:
        r1 = _rule(db, name="m51-a")
        r2 = _rule(db, name="m51-b")
        m2 = _media(db, r2, key="别的规则的作品")
        assert mirror.reconcile_stale_media_status(db, r1.id) == []
        db.expire_all()
        assert db.get(MirrorMedia, m2.id).status == "failed"


def test_heal_no_candidate_returns_empty(env):
    S, _, _ = env
    with S() as db:
        r = _rule(db)
        _media(db, r, status="synced")
        assert mirror.reconcile_stale_media_status(db, r.id) == []


def test_heal_uses_the_single_predicate():
    """判据只许有一份：自愈巡检不许自己硬写 status = 'synced'。"""
    import inspect
    body = inspect.getsource(mirror.reconcile_stale_media_status)
    assert 'media.status = "synced"' not in body, "巡检另写了一份判据"
    assert "reconcile_media_status(" in body, "写回必须走唯一写入口"
    assert "MEDIA_TERMINAL_FILE_STATUS" in body


def test_heal_wired_into_both_round_paths():
    """两条收尾路径都要跑：同步收尾 + 轮询收尾（后者不经过 sync_rule）。"""
    assert MAIN_SRC.count("reconcile_stale_media_status(") >= 2
    assert "status_healed" in MAIN_SRC


# ---------------------------------------------------------------------------
# ② 轮询失败：先核对 B 库，别把「其实已经落库」的文件判死
# ---------------------------------------------------------------------------
class FakeAPI:
    b_files = []
    task_status = 3

    def __init__(self, db):
        self.db = db

    async def list_files(self, parent_id, page=0, page_size=100):
        if page > 0:
            return {"list": [], "total": 0}
        if parent_id == B_WORK:
            return {"list": [dict(f) for f in self.b_files],
                    "total": len(self.b_files)}
        return {"list": [], "total": 0}

    async def get_task_status(self, task_id):
        return {"status": self.task_status, "raw": {"status": self.task_status}}


def _poll_case(db, *, status="copying"):
    r = _rule(db)
    m = MirrorMedia(rule_id=r.id, title_standard=WORK, media_type="tv",
                    source_dir_id=A_WORK, target_dir_id=B_WORK, status="syncing")
    db.add(m)
    db.commit()
    row = MirrorFile(rule_id=r.id, media_id=m.id, identity="i1",
                     source_file_id="F1", file_name="S01E01.mkv", size=SIZE,
                     utime=FP, status=status, task_id="TF")
    db.add(row)
    db.add(MirrorTask(rule_id=r.id, media_id=m.id, task_id="TF", status="running"))
    db.commit()
    return r, m.id, row.id


def test_poll_failed_task_backfills_when_file_actually_landed(env):
    """任务报失败，但 B 库确实有那个文件（超时/轮询用完）→ 必须判成功。"""
    S, _, _ = env
    with S() as db:
        r, mid, rid = _poll_case(db)
        FakeAPI.b_files = [{"fileId": "BF1", "resType": 1, "fileName": "S01E01.mkv",
                            "fileSize": SIZE, "parentId": B_WORK}]
        FakeAPI.task_status = 3
        with patch("app.mirror.GuangYaWebFiles", FakeAPI):
            out = asyncio.run(mirror.poll_pending_tasks(db, r, max_polls=1, interval=0.0))
        db.expire_all()
        row = db.get(MirrorFile, rid)
        assert row.status == "synced", (row.status, row.error)
        assert str(row.target_file_id or "") == "BF1"
        assert int(row.attempts or 0) == 0, \
            "文件其实已经在 B 库，不该消耗一次自动尝试次数（否则提前触发「已停止自动重试」）"
        assert db.get(MirrorMedia, mid).status == "synced"
        assert out["failed"] == 1, "任务本身失败是事实，统计口径不变"


def test_poll_failed_task_still_marks_failed_when_file_absent(env):
    """B 库确实没有 → 照旧判失败并累加次数（不许因为新逻辑就不算失败）。"""
    S, _, _ = env
    with S() as db:
        r, mid, rid = _poll_case(db)
        FakeAPI.b_files = []
        FakeAPI.task_status = 3
        with patch("app.mirror.GuangYaWebFiles", FakeAPI):
            asyncio.run(mirror.poll_pending_tasks(db, r, max_polls=1, interval=0.0))
        db.expire_all()
        row = db.get(MirrorFile, rid)
        assert row.status == "failed"
        assert int(row.attempts or 0) == 1
        assert db.get(MirrorMedia, mid).status == "failed"


def test_poll_failed_branch_checks_b_library_first():
    """源码守门：失败分支要先回填再判死，顺序不许颠倒。"""
    seg = MIRROR_SRC[MIRROR_SRC.index("async def poll_pending_tasks("):]
    seg = seg[:seg.index("async def retry_failed_dirs(")]
    failed_at = seg.index('elif task.status == "failed":')
    branch = seg[failed_at:]
    backfill_at = branch.index("_backfill_ids_for_media(")
    mark_at = branch.index("mark_file_failed(")
    assert backfill_at < mark_at, "先判死再核对 B 库 —— 「其实已落库」的文件会被白判一次失败"


# ---------------------------------------------------------------------------
# ③ 待重试作品：批上限 + 最久未处理优先（轮转，不饿死尾部）
# ---------------------------------------------------------------------------
def test_limit_defaults_to_max_works_per_run(env):
    """不发明额度：留空就跟单轮名额同一个数（v2.1.44 口径）。"""
    S, _, _ = env
    with S() as db:
        r = _rule(db, max_works=7)
        assert mirror.retry_works_limit(db, r) == 7


def test_limit_config_override_and_zero(env):
    S, _, _ = env
    with S() as db:
        r = _rule(db, max_works=7)
        db.add(AppConfig(key="mirror_retry_works_max", value_enc="2", updated_at=None))
        db.commit()
        assert mirror.retry_works_limit(db, r) == 2
        cfg = db.query(AppConfig).filter(
            AppConfig.key == "mirror_retry_works_max").first()
        cfg.value_enc = "0"
        db.commit()
        assert mirror.retry_works_limit(db, r) == 0, "0 = 不限（旧行为）"


def test_default_key_registered():
    assert DEFAULTS["mirror_retry_works_max"][1] == "", "留空=跟随单轮名额"


def _pending_media(db, rule, i, *, status="pending", attempts=0):
    # v2.1.64：唯一身份 (rule_id, source_dir_id)，循环里每条必须不同源目录 id。
    m = MirrorMedia(rule_id=rule.id, work_key=f"work:待重试{i}", title_standard=f"待重试{i}",
                    media_type="tv", source_dir_id=f"{A_WORK}-{i}",
                    target_dir_id=f"{B_WORK}-{i}", status="failed")
    db.add(m)
    db.commit()
    db.add(MirrorFile(rule_id=rule.id, media_id=m.id, identity=f"i{i}",
                      source_file_id=f"F{i}", file_name=f"S01E{i:02d}.mkv",
                      size=SIZE, utime=FP, status=status, attempts=attempts))
    db.commit()
    return m


def test_order_truncates_to_limit_but_reports_total(env):
    S, _, _ = env
    with S() as db:
        r = _rule(db, max_works=2)
        for i in range(5):
            _pending_media(db, r, i)
        picked, total = mirror.retry_works_order(db, r.id, 2)
        assert len(picked) == 2 and total == 5, (picked, total)


def test_order_zero_means_unlimited(env):
    S, _, _ = env
    with S() as db:
        r = _rule(db)
        for i in range(5):
            _pending_media(db, r, i)
        picked, total = mirror.retry_works_order(db, r.id, 0)
        assert len(picked) == 5 and total == 5


def test_order_rotates_instead_of_starving_the_tail(env):
    """核心反证：处理过的排到后面，下一轮必须轮到别的作品。

    若按目录顺序硬截前 N 部，第二轮会挑到同一批 → 尾部永远轮不到。
    """
    from app.utils import utcnow
    S, _, _ = env
    with S() as db:
        r = _rule(db, max_works=2)
        medias = [_pending_media(db, r, i) for i in range(4)]
        first, total = mirror.retry_works_order(db, r.id, 2)
        assert total == 4 and len(first) == 2

        # 模拟「被选中的这两部本轮处理过了」
        for m in medias:
            if m.work_key in set(first):
                m.last_synced_at = utcnow()
        db.commit()

        second, _ = mirror.retry_works_order(db, r.id, 2)
        assert set(second).isdisjoint(set(first)), (
            f"第二轮还是同一批 {second} —— 尾部作品会被永久饿死")


def test_order_excludes_exhausted_failed_rows(env):
    """v2.1.42 的 attempts 上限口径必须继承：试满的失败行不再拉作品进重试批。"""
    S, _, _ = env
    with S() as db:
        r = _rule(db, max_works=5)
        _pending_media(db, r, 1, status="failed", attempts=3)
        picked, total = mirror.retry_works_order(db, r.id, 5)
        assert picked == [] and total == 0


def test_sync_rule_uses_capped_order():
    """sync_rule 必须走带排序的上限版本，并如实报「因上限本轮没排上」的数量。"""
    import inspect
    src = inspect.getsource(mirror.sync_rule)
    assert "retry_works_order(" in src
    assert "retry_works_limit(" in src
    assert "retry_capped_out" in src
    # 不许退回「取全集 + 目录顺序」的老写法
    assert "pending_work_keys = _works_with_pending_files(db, rule.id)" not in src


# ---------------------------------------------------------------------------
# ④ 「待改名」口径唯一化（真机：看板待改名 2 条，用户核对后那两个文件早已改名）
# ---------------------------------------------------------------------------
def _rs(db, rule, media_id, status, tag):
    from app.models import MirrorFile as MF
    db.add(MF(rule_id=rule.id, media_id=media_id, identity=f"rs-{tag}",
              source_file_id=f"RS-{tag}", file_name="x.mkv",
              size=SIZE + len(tag), utime=FP, status="synced", rename_status=status))
    db.commit()


def test_rename_pending_filter_covers_exactly_three_states(env):
    """还等着改的只有 None / "" / pending_visible / failed；终态一律不算。"""
    S, _, _ = env
    with S() as db:
        r = _rule(db)
        m = _media(db, r, status="synced")
        for i, st in enumerate([None, "", "pending_visible", "failed",
                                "done", "skipped_ascii", "skipped_legacy"]):
            _rs(db, r, m.id, st, f"{i}")
        got = {x.rename_status for x in db.scalars(
            select(MirrorFile).where(mirror.rename_pending_filter())).all()}
        # 列默认值会把 None 收敛成空串（老库里可能真有 NULL，SQL 判据仍保留 is_(None)）；
        # 关键是 done / skipped_* 三种终态一个都不许进来。
        assert got == {"", "pending_visible", "failed"}, got


def test_rename_is_pending_object_matches_sql(env):
    """对象版判据与 SQL 版必须同源（避免又长出第二份口径）。"""

    class _R:
        def __init__(self, st):
            self.rename_status = st

    for st in [None, "", "pending_visible", "failed"]:
        assert mirror.rename_is_pending(_R(st)) is True
    for st in ["done", "skipped_ascii", "skipped_legacy"]:
        assert mirror.rename_is_pending(_R(st)) is False


def test_rename_todo_has_one_source():
    """看板 / 明细 / 复查 / 选批 / remaining 五处都必须走同一份判据。"""
    assert "RENAME_PENDING_STATUSES" in MIRROR_SRC
    assert MIRROR_SRC.count("def rename_pending_filter(") == 1
    # 旧的三段硬写不许再出现在任一文件里
    assert 'MirrorFile.rename_status == "pending_visible"' not in MIRROR_SRC
    assert 'MirrorFile.rename_status == "pending_visible"' not in MAIN_SRC
    # 看板 + 明细 + 复查计数至少三处调用
    assert MAIN_SRC.count("rename_pending_filter()") >= 3, \
        f"main.py 里只有 {MAIN_SRC.count('rename_pending_filter()')} 处用了统一判据"
    # stats["remaining"] 也用它
    import inspect
    assert "rename_pending_filter()" in inspect.getsource(mirror.rename_mirror_files)
