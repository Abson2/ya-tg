"""阶段四验收（分享中心生命周期）：原子领取 + 建分享适配 + 跨季一剧一链接 + 9态。

对齐 RESOURCE_SHARE_REDESIGN_PLAN.md 阶段四：
  1. claim_tasks 真原子（条件 UPDATE 抢占，旧版 select-then-modify 多进程会重复）；
  2. 光鸭建分享解析收敛到 GuangYaShareClient.create_parsed（三处口径一致，含 link 兜底）；
  3. 跨季 S01/S02 归并同一 work_group_key + 整目录分享一链接（决策3，验证现有语义）；
  4. 9 态状态机定义完整，ST_PUBLISHING 作为活跃态被对账删除引用。
"""
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

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
    d = tempfile.mkdtemp(prefix="v147_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'b.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng, d
    shutil.rmtree(d, ignore_errors=True)


def _queue_item(db, rid, ch_id=1, status_ext="submitted", status="pending", task_id=None):
    from app.models import QueueItem
    q = QueueItem(resource_id=rid, channel_id=ch_id, status=status,
                  status_ext=status_ext, share_task_id=task_id)
    db.add(q)
    db.commit()
    db.refresh(q)
    return q


# ---------- 1. claim_tasks 真原子 ----------

def test_claim_tasks_returns_only_won_rows(env):
    """条件 UPDATE 抢占：只有一个 worker 领到；第二个领不到。"""
    from app import share_center as sc
    S, eng, d = env
    db = S()
    _queue_item(db, 1)
    _queue_item(db, 2)
    got1 = sc.claim_tasks(db, "worker-A", limit=10)
    got2 = sc.claim_tasks(db, "worker-B", limit=10)
    assert len(got1) == 2
    assert got2 == [], "第二个 worker 不得重复领取"


def test_claim_tasks_partial_overlap_only_wins_own(env):
    """候选集部分重叠时，只返回本 worker 真正抢到的（rowcount 无法区分的漏洞已修）。"""
    from app import share_center as sc
    from app.models import QueueItem
    S, eng, d = env
    db = S()
    q1 = _queue_item(db, 1)
    q2 = _queue_item(db, 2)
    # 模拟：另一个 worker 已抢走 q1（status_ext=accepted），q2 仍 submitted
    q1.status_ext = sc.ST_ACCEPTED
    q1.accept_worker = "other"
    db.commit()
    got = sc.claim_tasks(db, "worker-A", limit=10)
    assert len(got) == 1, "只应领到仍 submitted 的 q2"
    db.refresh(q1)
    db.refresh(q2)
    assert q1.accept_worker == "other"  # q1 不属于 worker-A，未被抢
    assert q2.accept_worker == "worker-A"


def test_claim_tasks_uses_conditional_update():
    """源码护栏：claim_tasks 必须用条件 UPDATE（真原子），不得纯 select-then-modify。"""
    import inspect
    from app import share_center as sc
    src = inspect.getsource(sc.claim_tasks)
    assert "update(QueueItem)" in src, "必须用条件 UPDATE 抢占"
    assert "status_ext == ST_SUBMITTED" in src, "UPDATE 必须带 submitted 条件"
    assert "accept_worker == worker" in src, "必须按 worker 标记回查归属"


# ---------- 2. 建分享解析收敛 ----------

@pytest.mark.parametrize("result,expected", [
    ({"data": {"shareId": "S1", "shareUrl": "http://u"}}, ("S1", "http://u")),
    ({"data": {"share_id": "S2", "share_url": "http://v"}}, ("S2", "http://v")),
    ({"data": {"id": "S3", "link": "http://w"}}, ("S3", "http://w")),
    ({"data": {}}, ("", "")),
])
def test_parse_share_result_field_variants(result, expected):
    """统一解析兼容三处历史字段名（含 link 兜底）。"""
    from app.guangya import GuangYaShareClient
    assert GuangYaShareClient.parse_share_result(result) == expected


def test_three_call_sites_use_adapter():
    """源码护栏：publisher/share_center/main 三处建分享都走统一适配器 create_share_parsed。"""
    import inspect
    from app import publisher, share_center, main
    for mod in (publisher, share_center, main):
        src = inspect.getsource(mod)
        assert "create_share_parsed(" in src, f"{mod.__name__} 未走统一建分享适配器"


def test_adapter_falls_back_for_legacy_client():
    """统一适配器对只实现 create() 的 client（测试替身/旧实现）优雅回退。"""
    import asyncio
    from app.guangya import create_share_parsed

    class LegacyClient:
        async def create(self, file_id, title, duration):
            return {"data": {"shareId": "S legacy", "shareUrl": "http://legacy"}}

    got = asyncio.run(create_share_parsed(LegacyClient(), "F1", "标题", 7))
    assert got == ("S legacy", "http://legacy")


# ---------- 3. 跨季一剧一链接（决策3） ----------

def test_cross_season_maps_to_one_work_key():
    """跨季 S01/S02 归同一 work_group_key（剧名母目录，不取季目录名）。"""
    from app.utils import work_group_key

    class R:
        def __init__(self, path, parent=""):
            # 真实识别后 share_dir_name=剧名母目录（series_dir_info 写入）
            self.share_dir_name = "开封府"
            self.path_hint = path
            self.parent_id = parent
            self.media_title = "开封府"
            self.tmdb_id = None
            self.media_type = "tv"
            self.file_id = ""

    s1 = R("/影视/国产剧/开封府/Season 1/开封府 S01E01.mkv")
    s2 = R("/影视/国产剧/开封府/Season 2/开封府 S02E02.mkv")
    assert work_group_key(s1) == work_group_key(s2) == "work:开封府"


def test_cross_season_adapter_reuses_link(env):
    """跨季追更：current_share 复用同一链接（一部剧一条当前分享）。"""
    from app import share_center as sc
    from app.models import Resource, ShareRecord
    from app.utils import utcnow
    S, eng, d = env
    db = S()
    r1 = Resource(id=1, file_id="F1", name="开封府 S01E01.mkv",
                  identify_status="identified", media_title="开封府",
                  share_dir_id="DIR-SERIES", share_scope="series_dir")
    r2 = Resource(id=2, file_id="F2", name="开封府 S02E01.mkv",
                  identify_status="identified", media_title="开封府",
                  share_dir_id="DIR-SERIES", share_scope="series_dir")
    db.add_all([r1, r2])
    rec = ShareRecord(work_key=sc.task_work_key(r1), resource_id=1,
                      share_id="SID", share_url="http://same-link",
                      share_status="active", current_flag=1, created_at=utcnow())
    db.add(rec)
    db.commit()
    # 两集同 work_key、同 share_dir_id
    assert sc.task_work_key(r1) == sc.task_work_key(r2)
    assert sc.current_share(db, sc.task_work_key(r2)).share_url == "http://same-link"


# ---------- 4. 9 态状态机 ----------

def test_nine_states_defined():
    from app import share_center as sc
    states = [sc.ST_PLANNED, sc.ST_BUILDING, sc.ST_READY, sc.ST_SUBMITTED,
              sc.ST_ACCEPTED, sc.ST_PUBLISHING, sc.ST_PUBLISHED,
              sc.ST_FAILED, sc.ST_CANCELLED]
    assert len(set(states)) == 9
    assert sc.ST_PUBLISHING in sc.ACTIVE_STATUS  # 对账删除按活跃态过滤，保留
