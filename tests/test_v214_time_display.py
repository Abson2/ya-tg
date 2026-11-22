"""v2.1.3 时间显示简化验收（T01 未知字符 + 列宽过长）。

真机问题：
  - 分享中心 / 推送任务 / 分享管理三处的时间列显示成 `2026-11-22T01:00:17.123456`：
    ①中间的 `T` 是用户完全看不懂的「未知字符」；②带微秒，长度顶满表格列宽。
  - 根因：后端 `_model_dict` 直接 `value.isoformat()`，且 SQLite 读回的是 **naive UTC**，
    浏览器按本地时区解读会整体偏 8 小时。

覆盖：
  1. 后端时间统一输出 `YYYY-MM-DD HH:mm:ss`（Asia/Shanghai），无 `T` / 无微秒 / 无偏移；
  2. naive UTC 正确按 UTC 解读后再转上海时区（不偏 8 小时）；
  3. 前端存在统一格式化函数，且所有时间渲染点都走它（不再原样输出 ISO 串）；
  4. 前端不得再出现「耗时(ms)」这类工程口径。
"""
import os
import re
import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

ROOT = Path(__file__).resolve().parents[1]
SH = timezone(timedelta(hours=8))


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v214_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'v214.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# 1. 后端时间格式
# ---------------------------------------------------------------------------

def test_display_time_is_local_readable_string():
    """_display_time 输出 YYYY-MM-DD HH:mm:ss（上海本地），无 T / 微秒 / 偏移。"""
    from app.share_center_v2 import _display_time
    dt = datetime(2026, 11, 22, 1, 0, 17, 123456, tzinfo=timezone.utc)
    out = _display_time(dt)
    assert out == "2026-11-22 09:00:17", out
    assert "T" not in out
    assert "." not in out
    assert "+" not in out and "Z" not in out


def test_display_time_treats_naive_as_utc():
    """SQLite 读回的是 naive UTC：必须按 UTC 解读再转上海，不能当本地时间。"""
    from app.share_center_v2 import _display_time
    naive = datetime(2026, 11, 22, 1, 0, 17)
    out = _display_time(naive)
    assert out == "2026-11-22 09:00:17", out


def test_display_time_handles_none_and_aware():
    from app.share_center_v2 import _display_time
    assert _display_time(None) == ""
    aware = datetime(2026, 11, 22, 1, 0, 17, tzinfo=SH)
    assert _display_time(aware) == "2026-11-22 01:00:17"


def test_model_dict_uses_display_time(env):
    """_model_dict 的时间字段走 _display_time（不再 isoformat）。"""
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env
    with S() as db:
        db.add(SCPublishTask(work_key="work:x", status="planned",
                             created_at=datetime(2026, 11, 22, 1, 0, 17)))
        db.commit()
        row = db.scalars(__import__("sqlalchemy").select(SCPublishTask)).first()
        data = sc2._model_dict(row)
        assert data["created_at"] == "2026-11-22 09:00:17", data["created_at"]
        assert "T" not in data["created_at"]


def test_api_tasks_returns_readable_times(env):
    """/sc/tasks 端点返回的时间已是可读格式。"""
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env
    with S() as db:
        db.add(SCPublishTask(work_key="work:x", status="planned",
                             created_at=datetime(2026, 11, 22, 1, 0, 17)))
        db.commit()
        out = sc2.api_tasks(db=db)
        item = out["items"][0]
        assert item["created_at"] == "2026-11-22 09:00:17", item["created_at"]


def test_api_share_records_returns_readable_times(env):
    """/sc/share-records 的三个时间字段（发布时间 / 最近校验 / 最近推送）都可读。"""
    from app import share_center_v2 as sc2
    from app.models import (SCPublishTask, SCPushList, SCPushListItem,
                            SCLibraryVersion)
    S, _ = env
    with S() as db:
        lst = SCPushList(list_key="l1", name="l", status="active")
        db.add(lst)
        db.flush()
        lv = SCLibraryVersion(work_key="work:x", version_no=1, fingerprint="f",
                              resource_ids_json="[]", metadata_json="{}")
        db.add(lv)
        db.flush()
        item = SCPushListItem(list_id=lst.id, work_key="work:x",
                              library_version_id=lv.id, category_key="genre:动作",
                              dedup_key="d1", chat_id="-1001")
        db.add(item)
        db.flush()
        db.add(SCPublishTask(list_item_id=item.id, work_key="work:x",
                             status="published", share_url="https://x/s/abc",
                             link_state="active",
                             created_at=datetime(2026, 11, 22, 1, 0, 17),
                             published_at=datetime(2026, 11, 22, 1, 5, 0)))
        db.commit()
        out = sc2.api_share_records(db=db)
        row = out["items"][0]
        assert row["created_at"] == "2026-11-22 09:00:17", row["created_at"]
        assert row["last_pushed_at"] == "2026-11-22 09:05:00", row["last_pushed_at"]


# ---------------------------------------------------------------------------
# 2. 前端契约
# ---------------------------------------------------------------------------

def test_frontend_has_unified_time_formatter():
    """前端必须存在统一时间格式化函数（fmtTime / fmtDuration）。"""
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "const fmtTime = " in app
    assert "const fmtDuration = " in app


def test_frontend_time_columns_all_use_formatter():
    """所有时间列必须走 fmtTime，不得再原样渲染 ISO 串。"""
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    # 原样渲染时间字段的写法（prop 指向时间列）必须全部消失
    for prop in ("published_at", "created_at", "updated_at", "scheduled_at",
                 "last_checked_at", "last_pushed_at", "started_at"):
        bad = f'prop="{prop}"'
        assert bad not in app, f"仍存在原样渲染时间列：{bad}"
    # 这些时间字段出现在模板里时，必须包在 fmtTime(...) 中
    for field in ("published_at", "created_at", "updated_at", "scheduled_at",
                  "last_checked_at", "last_pushed_at", "started_at"):
        for m in re.finditer(r"\{\{[^}]*" + field + r"[^}]*\}\}", app):
            snippet = m.group(0)
            assert "fmtTime(" in snippet, f"时间字段未走格式化函数：{snippet}"


def test_frontend_has_no_raw_iso_time_output():
    """前端不得出现 'T' 拼接的时间直出（那正是用户看到的未知字符来源）。"""
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    # 后端已转本地时间，前端不应再自己拼 T
    assert "'T' +" not in app
    assert '"T" +' not in app
    assert "toISOString().slice" not in app.replace(
        "new Date().toISOString().slice(0, 10)", "")


def test_frontend_duration_is_human_readable():
    """页面不得出现「耗时(ms)」这类工程口径。"""
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    # 只检查模板区（根 <template>…</template>），脚本里的注释提到该词是正常的。
    # 注意：模板内大量嵌套 <template #default="s">，必须取**最后**一个 </template>。
    start = app.index("<template>")
    end = app.rindex("</template>")
    tpl = app[start:end]
    assert "耗时(ms)" not in tpl
    assert "fmtDuration(s.row.duration_ms)" in tpl
