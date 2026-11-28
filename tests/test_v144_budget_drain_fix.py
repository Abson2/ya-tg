"""v1.4.4 验收：改名按部（目录级）收敛 + 断流 P0 根治。

真机现象（v1.4.3）：死锁五轮根治后翻译正常、英文名命中缓存，但开封府 58 集
改名落盘恒为 0。根因不是锁/限流/翻译，而是两处叠加：

  A) 断流：阶段一准备 jobs 到 `attempts + len(jobs) >= file_budget` 时设
     `stop_reason="file_budget"; break`；阶段二旧代码 `if stop_reason: break`
     把已收集 jobs 全部作废，一个 rename_file 都不发。后台 max_files=10 → 0 改名。
  B) 预算单位错：改名用「单集数」预算，与用户口径不符。用户 2026-11-21 明确：
     「单轮最多整理整部剧/电影（部）」max_works_per_run 对复制、翻译都生效，
     按部的目录级收敛，选中的每部所有单集文件必须全部处理完。

修复：
  1. 改名按部收敛：每轮最多 max_works 部（读 rule.max_works_per_run），选中部
     全部单集一次改完，部内不受单集 max_files / 时间预算截断；改完的部不再
     pending，下一轮自动选下一批（天然轮转，不碰复制共用的 works_cursor）。
  2. 断流修复：阶段二不再被「预算类」stop_reason 阻断，先把已收集 jobs 发完
     再停；只有 db_error（事务已坏）才立即中断。

覆盖：
  1. 一部剧 20 集 + max_works=1 → 首轮 20 集全改完（不是单集预算 10）；
  2. 两部剧 + max_works=1 → 首轮只改第 1 部全部集，第 2 部不动；第二轮改第 2 部；
  3. max_works 未显式传时从 rule.max_works_per_run 读取；
  4. max_works<=0 退回旧单集预算（向后兼容）；
  5. db_error 仍立即中断阶段二（止损护栏）；
  6. 源码护栏 + 版本一致性 1.4.4。
"""
import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

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
    d = tempfile.mkdtemp(prefix="v144_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'b.db')}")
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng, d
    eng.dispose()
    shutil.rmtree(d, ignore_errors=True)


ENG = "The Legend of Kaifeng"


def _seed_works(db, models, spec):
    """spec: [(标题, 集数), ...]，每部一部 MirrorMedia + N 个 synced 待改 MirrorFile。"""
    rule = models.MirrorRule(name="v144", source_root_id="A", target_root_id="B",
                             enabled=True, rename_files=True, file_idle_seconds=0.0)
    db.add(rule)
    db.flush()
    works = []
    for wi, (title, n) in enumerate(spec):
        media = models.MirrorMedia(rule_id=rule.id, work_key=f"work:w{wi}",
                                   title_standard=f"{title} (2017) {{tmdb-{1000 + wi}}}",
                                   title_mirror=title, source_dir_id=f"A{wi}",
                                   target_dir_id=f"B{wi}", status="synced")
        db.add(media)
        db.flush()
        files = []
        for i in range(1, n + 1):
            fid = f"BF{wi}_{i}"
            row = models.MirrorFile(
                media_id=media.id, rule_id=rule.id,
                identity=f"tmdb:{1000 + wi}:tv:1:{i}",
                source_file_id=f"AF{wi}_{i}", target_file_id=fid,
                file_name=f"{title}.S01E{i:02d}.mkv", size=1000 + i, utime=100,
                fingerprint=f"{1000 + i}:100", status="synced")
            db.add(row)
            files.append((row, fid))
        works.append((media, files))
    db.commit()
    return rule, works


class FakeAPI:
    def __init__(self):
        self.calls = []

    async def rename_file(self, fid, new_name):
        self.calls.append(fid)


async def _noop_sleep(_d):
    return None


def _run_rename(env, spec, *, max_works=None, api=None, rule_cap=None):
    from app import mirror, models
    S, _, _ = env
    api = api or FakeAPI()
    with S() as db:
        rule, works = _seed_works(db, models, spec)
        if rule_cap is not None:
            rule.max_works_per_run = rule_cap
            db.commit()

        async def fake_resolve(*a, **k):
            return (ENG, "tmdb")

        async def fake_visible(_api, _media, rows):
            from app.utils import normalize_work_name
            candidates = {}
            for r in rows:
                key = normalize_work_name(str(r.file_name))
                candidates[key] = str(r.target_file_id)
                candidates[f"{key}:{int(r.size or 0)}"] = str(r.target_file_id)
            return candidates, True, True

        real_get = mirror.runtime_config.get

        def cfg_get(d, key):
            if key == "rename_idle_seconds":
                return "0.2"
            if key == "rename_concurrency":
                return "3"
            return real_get(d, key)

        kwargs = dict(only_pending=True)
        if max_works is not None:
            kwargs["max_works"] = max_works
        with patch("app.mirror.GuangYaWebFiles", lambda d: api), \
             patch.object(mirror, "_resolve_english_title", fake_resolve), \
             patch.object(mirror, "_visible_b_candidates", fake_visible), \
             patch.object(mirror.asyncio, "sleep", new=_noop_sleep), \
             patch.object(mirror.runtime_config, "get", cfg_get):
            stats = asyncio.run(mirror.rename_mirror_files(db, rule, **kwargs))
        db.expire_all()
        with S() as chk:
            per_work = [
                [chk.get(models.MirrorFile, r.id).rename_status for r, _ in files]
                for _media, files in works
            ]
        return stats, api, per_work


# ---------- ① 按部收敛：选中部全部单集改完 ----------

def test_one_work_all_episodes_renamed_not_single_file_cap(env):
    """一部剧 20 集 + max_works=1 → 首轮 20 集全改完（旧版单集预算只改 10 或 0）。"""
    stats, api, per_work = _run_rename(env, [("开封府", 20)], max_works=1)
    assert stats["renamed"] == 20, stats
    assert stats["failed"] == 0, stats
    assert len(api.calls) == 20
    assert per_work[0].count("done") == 20, per_work


def test_only_selected_works_processed_then_next_round(env):
    """两部剧 + max_works=1：同一 DB 连续两轮。首轮改第 1 部全部集，第 2 部不动；
    第 2 轮自动选到第 2 部并全部改完（验证按部轮转续改闭环）。"""
    from app import mirror, models
    S, _, _ = env
    api = FakeAPI()

    async def fake_resolve(*a, **k):
        return (ENG, "tmdb")

    async def fake_visible(_api, _media, rows):
        from app.utils import normalize_work_name
        candidates = {}
        for r in rows:
            if r.rename_status == "done":
                continue
            key = normalize_work_name(str(r.file_name))
            candidates[key] = str(r.target_file_id)
            candidates[f"{key}:{int(r.size or 0)}"] = str(r.target_file_id)
        return candidates, True, True

    real_get = mirror.runtime_config.get

    def cfg_get(d, key):
        if key == "rename_idle_seconds":
            return "0.2"
        if key == "rename_concurrency":
            return "3"
        return real_get(d, key)

    with S() as db:
        rule, works = _seed_works(db, models, [("剧甲", 6), ("剧乙", 7)])
        with patch("app.mirror.GuangYaWebFiles", lambda d: api), \
             patch.object(mirror, "_resolve_english_title", fake_resolve), \
             patch.object(mirror, "_visible_b_candidates", fake_visible), \
             patch.object(mirror.asyncio, "sleep", new=_noop_sleep), \
             patch.object(mirror.runtime_config, "get", cfg_get):
            s1 = asyncio.run(mirror.rename_mirror_files(db, rule, max_works=1, only_pending=True))
            db.expire_all()
            with S() as chk:
                w1 = [[chk.get(models.MirrorFile, r.id).rename_status for r, _ in files]
                      for _m, files in works]
            s2 = asyncio.run(mirror.rename_mirror_files(db, rule, max_works=1, only_pending=True))
            db.expire_all()
            with S() as chk:
                w2 = [[chk.get(models.MirrorFile, r.id).rename_status for r, _ in files]
                      for _m, files in works]

    assert s1["renamed"] == 6, s1
    assert w1[0].count("done") == 6 and all(s == "" for s in w1[1]), w1
    assert s2["renamed"] == 7, s2
    assert w2[0].count("done") == 6 and w2[1].count("done") == 7, w2


def test_max_works_read_from_rule_config(env):
    """未显式传 max_works 时从 rule.max_works_per_run 读取。"""
    stats, _api, per_work = _run_rename(
        env, [("剧甲", 5), ("剧乙", 5), ("剧丙", 5)], rule_cap=2)
    assert stats["max_works"] == 2 and stats["per_work_mode"] is True, stats
    assert stats["renamed"] == 10, stats  # 两部 × 5 集
    done_works = sum(1 for s in per_work if s.count("done") == 5)
    assert done_works == 2, per_work


# ---------- ② 向后兼容：max_works<=0 退回旧单集预算 ----------

def test_max_works_zero_falls_back_to_single_file_budget(env):
    """max_works=0 时退回旧单集预算口径（向后兼容手动 force 全量）。"""
    from app import mirror, models
    S, _, _ = env
    api = FakeAPI()
    with S() as db:
        rule, works = _seed_works(db, models, [("开封府", 20)])

        async def fake_resolve(*a, **k):
            return (ENG, "tmdb")

        async def fake_visible(_api, _media, rows):
            from app.utils import normalize_work_name
            candidates = {}
            for r in rows:
                key = normalize_work_name(str(r.file_name))
                candidates[key] = str(r.target_file_id)
                candidates[f"{key}:{int(r.size or 0)}"] = str(r.target_file_id)
            return candidates, True, True

        real_get = mirror.runtime_config.get

        def cfg_get(d, key):
            if key == "rename_idle_seconds":
                return "0.2"
            if key == "rename_concurrency":
                return "3"
            return real_get(d, key)

        with patch("app.mirror.GuangYaWebFiles", lambda d: api), \
             patch.object(mirror, "_resolve_english_title", fake_resolve), \
             patch.object(mirror, "_visible_b_candidates", fake_visible), \
             patch.object(mirror.asyncio, "sleep", new=_noop_sleep), \
             patch.object(mirror.runtime_config, "get", cfg_get):
            stats = asyncio.run(mirror.rename_mirror_files(
                db, rule, max_works=0, max_files=6, only_pending=True))
    assert stats["per_work_mode"] is False, stats
    assert stats["renamed"] == 6, stats  # 旧口径：单集预算 6


# ---------- ③ 止损护栏 ----------

def test_db_error_still_halts_stage_two():
    """护栏：阶段二仍以 db_error 为唯一立即中断条件（不能为修断流取消止损）。"""
    import inspect
    from app import mirror
    src = inspect.getsource(mirror.rename_mirror_files)
    assert 'stop_reason == "db_error"' in src, "db_error 须仍可立即中断阶段二"
    stage2 = src.split("# 阶段二：按窗口并发发网络", 1)[-1]
    stage2 = stage2.split('stats["works"] += 1', 1)[0]
    assert "if stop_reason:\n                break" not in stage2, "裸 stop_reason break 须移除"


# ---------- ④ 版本一致性 ----------

def test_version_is_144_everywhere():
    import json
    root = Path(__file__).resolve().parents[1]
    main_src = (root / "backend/app/main.py").read_text(encoding="utf-8")
    assert 'version="2.1.59"' in main_src
    assert '"version": "2.1.59"' in main_src
    assert "v2.1.59 · Asia/Shanghai" in (root / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert json.loads((root / "frontend/package.json").read_text(encoding="utf-8"))["version"] == "2.1.59"
    assert "当前版本：v2.1.59" in (root / "README.md").read_text(encoding="utf-8")
