"""v2.1.2 分享中心打磨验收：资源名可读化 + 分类名安全 + 清理归档 + 批量采纳。

覆盖本轮 5 项优化反馈：
  1. 资源名称剥离 `work:` 内部键前缀（展示层不得暴露后端键形态）；
  2. 分类名安全展示（`genre:unknown` 等后端 key 不得外泄到界面）；
  3. 推送任务「清理无效任务」（归档语义，不物理删除）；
  4. 分享管理「清理无效链接」（归档语义，不物理删除）；
  5. 分类时间排期「批量采纳 AI 建议」。

另含前端契约：详情改隐藏式弹窗、两页表格不挤压、批量采纳入口。
"""
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
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
    d = tempfile.mkdtemp(prefix="v212_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'v212.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _resource(db, file_id="f1", name="Show (2024) {tmdb-1}/Show.S01E01.mkv",
              media_type="tv", title="剧集名", tmdb_id=1, share_dir_name="Show"):
    from app.models import Resource
    r = Resource(
        file_id=file_id, name=name, ext=".mkv", size=100, utime=1,
        identify_status="identified", media_type=media_type, tmdb_id=tmdb_id,
        media_title=title, path_hint=share_dir_name,
        share_dir_id="dir_show", share_scope="series_dir", share_dir_name=share_dir_name,
        genres='[{"id": 18, "name": "Drama"}]', poster_path="/poster.jpg",
    )
    db.add(r)
    db.commit()
    return r


def _version(db, work_key, title, version_no=1):
    from app.models import SCLibraryVersion
    row = SCLibraryVersion(work_key=work_key, version_no=version_no,
                            fingerprint=f"fp-{work_key}-{version_no}",
                            media_type="tv", resource_ids_json="[]",
                            metadata_json=json.dumps({"title": title}, ensure_ascii=False),
                            change_type="created")
    db.add(row)
    db.commit()
    return row


def _push_list(db, work_key, category_key="genre:unknown"):
    from app.models import SCPushList, SCPushListItem
    pl = SCPushList(list_key=f"lk-{work_key}", name="测试清单", status="active",
                    source="plan", plan_version_id=1)
    db.add(pl)
    db.flush()
    item = SCPushListItem(list_id=pl.id, work_key=work_key, library_version_id=0,
                           category_key=category_key, target_type="channel",
                           bot_id=0, chat_id="-1001", dedup_key=f"dk-{work_key}",
                           source_kind="first", status="planned")
    db.add(item)
    db.commit()
    return item


def _task(db, item, work_key, status="failed", link_state="unknown",
          error="", share_url=""):
    from app.models import SCPublishTask
    t = SCPublishTask(list_item_id=item.id, work_key=work_key, plan_version_id=1,
                      kind="first", status=status, priority=0,
                      share_url=share_url, link_state=link_state, error=error)
    db.add(t)
    db.commit()
    return t


# ---------------------------------------------------------------------------
# 1. 资源名称可读化
# ---------------------------------------------------------------------------

def test_work_titles_strips_work_prefix(env):
    """`work:` 前缀必须被剥离，且优先用资源库版本表登记的真实标题。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        _version(db, "work:剑雨", "剑雨")
        titles = sc2._work_titles(db, ["work:剑雨"])
        assert titles["work:剑雨"] == "剑雨"
        assert "work:" not in titles["work:剑雨"]


def test_work_titles_prefers_library_version_title(env):
    """同一 work_key 取版本号最大的那条 metadata_json.title。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        _version(db, "work:theoffice", "办公室", version_no=1)
        _version(db, "work:theoffice", "办公室（美版）", version_no=2)
        titles = sc2._work_titles(db, ["work:theoffice"])
        assert titles["work:theoffice"] == "办公室（美版）"


def test_work_titles_falls_back_to_resource_lookup(env):
    """tmdb:/file: 形态回查资源库取真实作品名。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        _resource(db, file_id="fx", title="星爷", tmdb_id=99, share_dir_name="星爷")
        titles = sc2._work_titles(db, ["tmdb:tv:99", "file:fx"])
        assert titles["tmdb:tv:99"] == "星爷"
        assert titles["file:fx"] == "星爷"


def test_work_titles_unknown_key_neutral(env):
    """查不到作品名时给中性占位，不暴露内部键形态。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        titles = sc2._work_titles(db, ["tmdb:tv:424242"])
        assert titles["tmdb:tv:424242"] == "未知作品"
        assert "tmdb:" not in titles["tmdb:tv:424242"]


def test_aggregate_views_and_tasks_expose_title(env):
    """总览三表与任务列表返回 title 字段，且不含 work: 前缀。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        item = _push_list(db, "work:九品芝麻官")
        _version(db, "work:九品芝麻官", "九品芝麻官")
        t = _task(db, item, "work:九品芝麻官", status="published")
        views = sc2._aggregate_views(db)
        for table in ("history", "queue", "failed"):
            for row in views.get(table, []):
                assert "title" in row
                assert not str(row["title"]).startswith("work:")
        tasks = sc2.api_tasks("", 200, False, db)
        items = tasks.get("items") if isinstance(tasks, dict) else tasks
        assert items, "至少要有一条任务"
        for row in items:
            assert "title" in row
            assert not str(row["title"]).startswith("work:")
        detail = sc2.api_task_detail(t.id, db)
        assert detail["item"]["title"] == "九品芝麻官"


def test_share_records_expose_title(env):
    """分享记录返回 share_title（可读），不是内部键。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        item = _push_list(db, "work:剑雨")
        _version(db, "work:剑雨", "剑雨")
        _task(db, item, "work:剑雨", status="published", share_url="https://x/abc")
        res = sc2.api_share_records(db, 200, False)
        items = res.get("items") if isinstance(res, dict) else res
        assert items, "至少要有一条分享记录"
        for row in items:
            assert "work:" not in str(row.get("share_title") or "")


# ---------------------------------------------------------------------------
# 2. 分类名安全展示
# ---------------------------------------------------------------------------

def test_category_display_name_never_exposes_key(env):
    """`genre:unknown` 等后端 key 不得出现在展示名里。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        assert sc2._category_display_name("genre:unknown") == "未分类"
        assert sc2._category_display_name("genre:动作") == "动作"
        assert sc2._category_display_name("genre:不存在的分类") == "未分类"
        assert sc2._category_display_name("") == "未分类"


def test_category_safe_name_rejects_key_form(env):
    """字典行 name 若是 key 形态，必须回落成可读名。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        assert sc2._category_safe_name("genre:动作", {}) == "动作"
        assert sc2._category_safe_name("genre:动作", {"genre:动作": "genre:动作"}) == "动作"
        assert sc2._category_safe_name("genre:动作", {"genre:动作": "我的动作片"}) == "我的动作片"


def test_share_records_category_name_safe(env):
    """分享记录的分类名不得回显 genre: 形态。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        item = _push_list(db, "work:剑雨", category_key="genre:unknown")
        _version(db, "work:剑雨", "剑雨")
        _task(db, item, "work:剑雨", status="published", share_url="https://x/abc")
        res = sc2.api_share_records(db, 200, False)
        items = res.get("items") if isinstance(res, dict) else res
        for row in items:
            assert "genre:" not in str(row.get("category_name") or "")


def test_category_schedules_name_safe(env):
    """排期列表的分类名不得回显 genre: 形态。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        res = sc2.api_category_schedules(0, db)
        for row in res.get("items", []):
            assert "genre:" not in str(row.get("name") or "")


# ---------------------------------------------------------------------------
# 3. 清理无效任务（归档，不物理删除）
# ---------------------------------------------------------------------------

def test_cleanup_tasks_archives_invalid_rows(env):
    """无效任务被归档为 cleaned，历史行保留、默认列表不再展示。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        item = _push_list(db, "work:剑雨")
        _resource(db, file_id="keep", title="保留作品", tmdb_id=1, share_dir_name="保留作品")
        # ① 已取消
        cancelled = _task(db, item, "work:剑雨", status="cancelled")
        # ② 永久性失败（资源已从资源库移除）
        permanent = _task(db, item, "work:剑雨", status="failed",
                          error="作品资源已从资源库移除：剑雨")
        # ③ 作品已不在资源库
        gone = _task(db, item, "work:不存在作品", status="failed", error="推送超时")
        # ④ 有效任务（资源还在、失败原因可重试）→ 不得清理
        alive = _task(db, item, "work:保留作品", status="failed", error="网络超时")

        res = sc2.api_cleanup_tasks(sc2.Payload(payload={}), db)
        assert res["ok"] is True
        assert res["cleaned"] == 3, f"应清理 3 条无效任务，实得 {res['cleaned']}"
        db.expire_all()
        assert db.get(type(cancelled), cancelled.id).status == sc2.CLEANED_TASK_STATUS
        assert db.get(type(permanent), permanent.id).status == sc2.CLEANED_TASK_STATUS
        assert db.get(type(gone), gone.id).status == sc2.CLEANED_TASK_STATUS
        assert db.get(type(alive), alive.id).status == "failed", "有效任务不得被清理"

        # 默认列表排除已归档
        listed = sc2.api_tasks("", 200, False, db)
        items = listed.get("items") if isinstance(listed, dict) else listed
        ids = {int(r["id"]) for r in (items or [])}
        assert alive.id in ids
        assert cancelled.id not in ids, "已归档任务不得出现在默认列表"

        # include_archived=true 可查回
        with_arch = sc2.api_tasks("", 200, True, db)
        arch_items = with_arch.get("items") if isinstance(with_arch, dict) else with_arch
        arch_ids = {int(r["id"]) for r in (arch_items or [])}
        assert cancelled.id in arch_ids, "归档任务必须仍可查回（历史不丢）"


def test_cleanup_tasks_does_not_delete_rows(env):
    """归档不得物理删除行：执行记录与统计口径仍可追溯。"""
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        item = _push_list(db, "work:剑雨")
        t = _task(db, item, "work:剑雨", status="cancelled")
        sc2.api_cleanup_tasks(sc2.Payload(payload={}), db)
        db.expire_all()
        still = db.scalar(select(SCPublishTask).where(SCPublishTask.id == t.id))
        assert still is not None, "归档只是状态变更，行必须还在"


def test_cleanup_tasks_by_ids_only(env):
    """带 ids 时只清理指定任务。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        item = _push_list(db, "work:剑雨")
        a = _task(db, item, "work:剑雨", status="cancelled")
        b = _task(db, item, "work:九品芝麻官", status="cancelled")
        res = sc2.api_cleanup_tasks(sc2.Payload(payload={"ids": [a.id]}), db)
        assert res["cleaned"] == 1
        db.expire_all()
        assert db.get(type(a), a.id).status == sc2.CLEANED_TASK_STATUS
        assert db.get(type(b), b.id).status == "cancelled"


# ---------------------------------------------------------------------------
# 4. 清理无效链接（归档，不物理删除）
# ---------------------------------------------------------------------------

def test_cleanup_share_records_archives_invalid_links(env):
    """无效链接归档为 cleaned；不删 B 库资源、不删远端分享、不删历史行。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        _resource(db, file_id="keep", title="保留作品", tmdb_id=1, share_dir_name="保留作品")
        item = _push_list(db, "work:剑雨")
        bad = _task(db, item, "work:剑雨", status="published",
                    link_state="inactive", share_url="https://x/bad")
        alive = _task(db, item, "work:保留作品", status="published",
                      link_state="active", share_url="https://x/good")

        res = sc2.api_cleanup_share_records(sc2.Payload(payload={}), db)
        assert res["ok"] is True
        assert res["cleaned"] == 1, f"应清理 1 条无效链接，实得 {res['cleaned']}"
        db.expire_all()
        assert db.get(type(bad), bad.id).link_state == sc2.CLEANED_LINK_STATE
        assert db.get(type(alive), alive.id).link_state == "active", "有效链接不得被清理"
        assert db.get(type(bad), bad.id).share_url == "https://x/bad", \
            "归档不得抹掉历史分享地址"

        # 默认列表排除已归档
        listed = sc2.api_share_records(db, 200, False)
        rows = listed.get("items") if isinstance(listed, dict) else listed
        urls = {str(r.get("share_url") or "") for r in (rows or [])}
        assert "https://x/bad" not in urls

        # include_archived=true 可查回
        with_arch = sc2.api_share_records(db, 200, True)
        arch = with_arch.get("items") if isinstance(with_arch, dict) else with_arch
        arch_urls = {str(r.get("share_url") or "") for r in (arch or [])}
        assert "https://x/bad" in arch_urls


def test_cleanup_share_records_removes_from_stats(env):
    """归档后统计口径同步排除。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        _resource(db, file_id="keep", title="保留作品", tmdb_id=1, share_dir_name="保留作品")
        item = _push_list(db, "work:剑雨")
        bad = _task(db, item, "work:剑雨", status="published",
                    link_state="inactive", share_url="https://x/bad")
        alive = _task(db, item, "work:保留作品", status="published",
                      link_state="active", share_url="https://x/good")
        before = sc2._aggregate_share_stats(db)
        sc2.api_cleanup_share_records(sc2.Payload(payload={}), db)
        after = sc2._aggregate_share_stats(db)
        assert int(after.get("total") or 0) == int(before.get("total") or 0) - 1, \
            "归档后统计总数必须同步减少"


# ---------------------------------------------------------------------------
# 5. 批量采纳 AI 建议
# ---------------------------------------------------------------------------

def test_adopt_ai_all_marks_habit_fallback_rows(env):
    """无真实历史数据的分类：v2.1.6 起用**习惯基线**兜底，不再跳过。

    口径变化的原因：v2.1.4 的周维度建议按「有无实测样本」置灰采纳按钮，
    导致大部分分类按钮永远点不了（用户实测反馈「有的按钮是灰色的」）。
    v2.1.6 改为单一时间段 + 每分类必有建议，并**用来源字段区分**：
    "history"=实测 / "habit"=习惯基线 —— 兜底不冒充实测，不编造历史数据。
    """
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        res = sc2.api_adopt_schedule_ai_all(sc2.Payload(payload={}), db)
        assert res["ok"] is True
        assert res["adopted"] > 0, "习惯基线兜底后应有分类被采纳"
        assert res["adopted"] + res["skipped"] == res["total"]
        # 每个分类的建议来源都必须是合法值：
        # "history"=实测 / "habit"=习惯基线 / ""=无建议。
        # 关键红线：**有实测就必须是 history**；无实测时可以是 habit（兜底），
        # 但绝不能出现「无来源却给出时段」或 habit 冒充实测的情况。
        rows = sc2.api_category_schedules(sc2._active_version(db).id, db)["items"]
        for r in rows:
            assert r.get("suggest_source") in ("history", "habit", ""), \
                f"分类 {r.get('name')} 的建议来源标记非法：{r.get('suggest_source')}"
            if r.get("suggest_source") == "habit":
                assert r.get("suggest_start", 0) < r.get("suggest_end", 0), \
                    f"分类 {r.get('name')} 的习惯基线时段必须非空"
            if r.get("suggest_source") == "":
                assert r.get("suggest_start", 0) == 0 and r.get("suggest_end", 0) == 0, \
                    "无建议时不得给出时段"


def test_adopt_ai_all_writes_real_suggestion(env):
    """有真实发布历史时批量写入建议时段，并脱离系统托管。"""
    from app import share_center_v2 as sc2
    from app.models import SCPublishTask
    S, _ = env
    with S() as db:
        sc2.ensure_defaults(db)
        version = sc2._active_version(db)
        item = _push_list(db, "work:剑雨", category_key="genre:动作")
        t = _task(db, item, "work:剑雨", status="published")
        # SQLite 丢 tzinfo → 必须写 UTC 数值（真实写入路径是 utcnow()），
        # 直接塞上海 aware 时间会变成「上海数值被当成 UTC」，读回偏 8 小时。
        t.published_at = datetime(2026, 1, 1, 20, 30, tzinfo=SH).astimezone(timezone.utc)
        db.commit()
        # 让该分类在偏好排名里靠前，保证有排期行
        res = sc2.api_adopt_schedule_ai_all(sc2.Payload(payload={}), db)
        assert res["ok"] is True
        assert res["adopted"] >= 1
        db.expire_all()
        from app.models import SCCategorySchedule
        row = db.scalar(select(SCCategorySchedule).where(
            SCCategorySchedule.plan_version_id == version.id,
            SCCategorySchedule.category_key == "genre:动作"))
        assert row is not None
        assert (row.window_start, row.window_end) == (20, 22), \
            "AI 建议应写成该分类真实发布小时所在的连续时段"
        assert row.ai_adopted is True
        assert row.window_auto is False, "采纳 AI 建议是用户手工动作"


# ---------------------------------------------------------------------------
# 6. 前端契约
# ---------------------------------------------------------------------------

def test_frontend_titles_not_work_key():
    """三表与任务表渲染 title，不是内部键字段。"""
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert 'prop="work_key"' not in app, "界面不得直接渲染内部作品键"
    assert 'prop="title"' in app


def test_frontend_detail_panels_are_hidden():
    """详情改为隐藏式弹窗，常驻侧栏必须删除。"""
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert ".sc-share-aside" not in app, "分享管理常驻侧栏必须删除"
    assert ".sc-task-aside" not in app, "推送任务常驻侧栏必须删除"
    assert "scOpenShareDetail" in app
    assert "scOpenTaskDetail" in app
    assert "scShareDetailVisible" in app


def test_frontend_cleanup_buttons_exist():
    """两个清理按钮 + 对应端点都在。"""
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "清理无效任务" in app
    assert "/sc/tasks/cleanup" in app
    assert "清理无效链接" in app
    assert "/sc/share-records/cleanup" in app


def test_frontend_batch_adopt_ai_button():
    """批量采纳 AI 建议按钮存在。"""
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "批量采纳 AI 建议" in app
    assert "scAdoptAllAi" in app
    assert "/sc/category-schedules/adopt-ai-all" in app


def test_frontend_layout_not_two_column_grid():
    """推送任务/分享管理两页不得再用两列 Grid 挤压表格。"""
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert ".sc-share-layout,.sc-task-layout{display:block}" in app
    assert "clamp(260px,22vw,360px)" not in app
