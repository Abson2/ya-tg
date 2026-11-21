"""v1.4 验收：strict 目录级批量复制 + 改名并发提速。

① strict 按 B 库目标目录分桶：同目录多文件一次 copy_file，跨目录分别提交；
② skip/cancel 粒度由单文件升为目录桶（用户拍板）；
③ 改名 idle 与复制 idle 解耦（rename_idle_seconds，默认 0.3，下限 0.2）；
④ 改名并发默认开（rename_concurrency=3），并发只发网络、写库在主协程串行；
⑤ 光鸭无批量 rename/正则替换接口——本地算名 + 并发单文件 rename 是唯一可行提速。
"""
import asyncio
import inspect
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


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v140_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'b.db')}")
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng, d
    eng.dispose()
    shutil.rmtree(d, ignore_errors=True)


# ---------- ① strict 目录级批量：跨目录分桶 ----------

A_ROOT, B_ROOT, A_WORK, B_WORK = "A_ROOT", "B_ROOT", "A_W1", "B_W1"
S1_ID, S2_ID = "BS1", "BS2"


class TwoDirAPI:
    """A 库一部剧两个季目录，各 2 个文件；B 库建季目录；记录每次 copy_file 的 (fids, parent)。"""

    def __init__(self):
        self.copies = []  # [(fids, parent)]
        self.b = {B_WORK: [], S1_ID: [], S2_ID: []}
        self._task = 0

    async def list_files(self, parent_id, page=0, page_size=100):
        if page > 0:
            return {"list": [], "total": 0}
        if parent_id == A_ROOT:
            return {"list": [{"fileId": A_WORK, "resType": 2,
                              "fileName": "侠客行 (2001) {tmdb-135646}"}], "total": 1}
        if parent_id == A_WORK:
            return {"list": [
                {"fileId": "AS1", "resType": 2, "fileName": "Season 1"},
                {"fileId": "AS2", "resType": 2, "fileName": "Season 2"},
            ], "total": 2}
        if parent_id == "AS1":
            return {"list": [
                {"fileId": "F1", "resType": 1, "fileName": "X.S01E01.mkv",
                 "fileSize": 100, "utime": 1, "parentId": "AS1"},
                {"fileId": "F2", "resType": 1, "fileName": "X.S01E02.mkv",
                 "fileSize": 100, "utime": 1, "parentId": "AS1"},
            ], "total": 2}
        if parent_id == "AS2":
            return {"list": [
                {"fileId": "F3", "resType": 1, "fileName": "X.S02E01.mkv",
                 "fileSize": 100, "utime": 1, "parentId": "AS2"},
                {"fileId": "F4", "resType": 1, "fileName": "X.S02E02.mkv",
                 "fileSize": 100, "utime": 1, "parentId": "AS2"},
            ], "total": 2}
        if parent_id == B_ROOT:
            return {"list": [{"fileId": B_WORK, "resType": 2,
                              "fileName": "侠客行 (2001) {tmdb-135646}"}], "total": 1}
        if parent_id == B_WORK:
            return {"list": [
                {"fileId": S1_ID, "resType": 2, "fileName": "Season 1"},
                {"fileId": S2_ID, "resType": 2, "fileName": "Season 2"},
            ], "total": 2}
        if parent_id in (S1_ID, S2_ID):
            items = self.b[parent_id]
            return {"list": list(items), "total": len(items)}
        return {"list": [], "total": 0}

    async def create_dir(self, parent_id, dir_name):
        fid = S1_ID if dir_name == "Season 1" else S2_ID
        return {"file_id": fid, "name": dir_name, "parent_id": parent_id}

    async def copy_file(self, file_ids, parent_id):
        self.copies.append((list(file_ids), parent_id))
        self._task += 1
        for fid in file_ids:
            self.b[parent_id].append({"fileId": f"BF{fid}", "resType": 1,
                                      "fileName": fid + ".mkv", "fileSize": 100,
                                      "utime": 1, "parentId": parent_id})
        return f"TASK{self._task}"

    async def get_task_status(self, task_id):
        return {"status": 2, "detail": {}}


def test_strict_buckets_by_target_dir(env):
    """两个季目录 → 两次 copy_file（每目录一次批量 2 文件），而非四次逐文件。"""
    from app import mirror, models
    S, _, _ = env
    with S() as db:
        rule = models.MirrorRule(name="t", source_root_id=A_ROOT, target_root_id=B_ROOT,
                                 interval_minutes=60, enabled=True, file_idle_seconds=0.0)
        db.add(rule)
        db.commit()
        db.refresh(rule)
        api = TwoDirAPI()
        with patch("app.mirror.GuangYaWebFiles", lambda d: api):
            stats = asyncio.run(mirror.sync_rule(db, rule, strict=True,
                                                 max_polls=5, poll_interval=0.0))
        assert stats["copy_batches"] == 2, stats
        assert len(api.copies) == 2, api.copies
        # 每批正好是该季目录的 2 个文件，parent 是对应 B 库季目录
        by_parent = {parent: fids for fids, parent in api.copies}
        assert set(by_parent) == {S1_ID, S2_ID}, by_parent
        assert sorted(by_parent[S1_ID]) == ["F1", "F2"]
        assert sorted(by_parent[S2_ID]) == ["F3", "F4"]
        assert all(r.status == "synced" for r in db.scalars(select(models.MirrorFile)).all())


def test_strict_skip_is_dir_level(env):
    """目录级 skip：跳过整桶文件，不逐个跳。"""
    from app import mirror, mirror_batch, models
    S, _, _ = env
    with S() as db:
        rule = models.MirrorRule(name="t", source_root_id=A_ROOT, target_root_id=B_ROOT,
                                 interval_minutes=60, enabled=True, file_idle_seconds=0.0)
        db.add(rule)
        db.commit()
        db.refresh(rule)
        # sync_rule 内部会 start_batch 清空 control，用 patch 让首桶带 skip 指令进入
        def start_with_skip(d, r, batch_id=None):
            bid = mirror_batch.new_batch_id(r.id)
            r.batch_id, r.batch_status, r.cursor_sequence = bid, "running", 0
            r.control_action = "skip"
            d.commit()
            return bid

        api = TwoDirAPI()
        with patch("app.mirror.mirror_batch.start_batch", side_effect=start_with_skip), \
             patch("app.mirror.GuangYaWebFiles", lambda d: api):
            stats = asyncio.run(mirror.sync_rule(db, rule, strict=True,
                                                 max_polls=5, poll_interval=0.0))
        assert stats["skipped"] >= 2, stats  # 首桶整桶 2 文件被跳过
        skipped_tasks = [t for t in db.scalars(select(models.MirrorTask)).all()
                         if t.task_status == "skipped"]
        assert len(skipped_tasks) >= 2, "整桶文件都须落 skipped 终态"


# ---------- ② 改名并发 helper ----------

def test_rename_concurrent_runs_in_parallel_and_isolates_errors():
    """_rename_concurrent 并发发起网络，单文件异常就地转 failed 不影响同批其它。"""
    from app import mirror

    state = {"active": 0, "peak": 0}

    class FakeAPI:
        async def rename_file(self, fid, final):
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
            await asyncio.sleep(0.01)
            state["active"] -= 1
            if fid == "BAD":
                raise RuntimeError("rename 429 风控")

    api = FakeAPI()
    jobs = [("F1", "a"), ("F2", "b"), ("BAD", "c"), ("F3", "d")]
    outcomes = asyncio.run(mirror._rename_concurrent(api, jobs))
    assert len(outcomes) == 4
    assert [ok for ok, _ in outcomes] == [True, True, False, True]
    assert "429" in outcomes[2][1]
    assert state["peak"] >= 2, "并发窗口内应有多请求同时在进行"


# ---------- ③ 配置与默认值 ----------

def test_rename_config_defaults_and_bounds():
    from app import mirror, runtime_config
    # v1.4.1：默认 idle 从 0.3 上调到 0.5（风控余量），并发仍保持 3
    assert runtime_config.DEFAULTS["rename_idle_seconds"][1] == "0.5"
    assert runtime_config.DEFAULTS["rename_concurrency"][1] == "3"
    assert mirror.MIN_RENAME_IDLE_SECONDS == 0.2
    assert mirror.MAX_RENAME_CONCURRENCY == 8


def test_rename_idle_decoupled_from_copy_idle(env, monkeypatch):
    """改名 idle 读 rename_idle_seconds，不再用 rule.file_idle_seconds。"""
    from app import mirror, models
    S, _, _ = env
    with S() as db:
        rule = models.MirrorRule(name="t", source_root_id="A", target_root_id="B",
                                 interval_minutes=60, enabled=True, file_idle_seconds=5.0)
        db.add(rule)
        db.commit()
        db.refresh(rule)
        seen = {}

        async def fake_resolve(*a, **k):
            return "West Out", "tmdb"

        async def fake_visible(*a, **k):
            return {}, False, []

        # 截获 rename_idle：通过 monkeypatch runtime_config.get 控制配置
        real_get = mirror.runtime_config.get

        def cfg_get(d, key):
            if key == "rename_idle_seconds":
                return "0.3"
            if key == "rename_concurrency":
                return "3"
            return real_get(d, key)

        class NoopAPI:
            pass

        monkeypatch.setattr(mirror, "GuangYaWebFiles", lambda d: NoopAPI())
        monkeypatch.setattr(mirror.runtime_config, "get", cfg_get)
        monkeypatch.setattr(mirror, "_resolve_english_title", fake_resolve)
        monkeypatch.setattr(mirror, "_visible_b_candidates", fake_visible)
        stats = asyncio.run(mirror.rename_mirror_files(db, rule))
        assert stats["rename_idle_seconds"] == 0.3, stats
        assert stats["rename_concurrency"] == 3, stats
        # file_idle_seconds=5.0 未影响改名 idle
        assert stats["rename_idle_seconds"] != float(rule.file_idle_seconds)


# ---------- ④ 源码护栏：写库不跨 await / 用并发 helper ----------

def test_rename_write_stays_in_main_coroutine():
    """护栏：内层改名循环调用 _rename_concurrent，网络协程不碰 db。

    v1.4.1：网络调用下沉到 _rename_one_with_backoff（含 127 退避重试），
    护栏随之检查该 helper；_rename_concurrent 只做 gather 编排。
    """
    from app import mirror
    src = inspect.getsource(mirror._rename_concurrent)
    # 网络 helper 不得出现 db 写操作
    assert "db.commit" not in src
    assert "db.rollback" not in src
    net_src = inspect.getsource(mirror._rename_one_with_backoff)
    assert "api.rename_file" in net_src
    assert "db.commit" not in net_src
    assert "db.rollback" not in net_src

    outer = inspect.getsource(mirror.rename_mirror_files)
    assert "_rename_concurrent" in outer, "改名主循环应走并发 helper"


def test_no_batch_rename_api_claimed():
    """护栏：不得虚构光鸭批量 rename接口；rename_file 仍是单 fileId 单 newName。"""
    from app import guangya
    src = inspect.getsource(guangya.GuangYaWebFiles.rename_file)
    assert "fileId" in src and "newName" in src
    assert "fileIds" not in src, "光鸭 rename 是单文件接口，不得写成批量"
