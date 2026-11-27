"""v2.1.56：TMDB「新上映」判据 —— `returning` 不再永不过期。

真机事故（用户 2026-11-28 排查记录）：
  《王保长后传》(tmdb 225764, 2005) 在库里被永久标成「新上映」，
  实际数据 `finish_status=returning`、`last_air_date=2005-06-11`、
  `release_date` 也是 2005 年。判据 `_new_release_state` 的 `returning` 分支
  当时只判「上映日是不是未来」，等于无条件信任 TMDB 的 status 字段，
  于是这部 **21 年前**就播完的老剧一直占着「新上映」的插队通道。
  同一库里《王保长歪传》因为 TMDB 标的是 `ended`，走完结分支，
  老播出日直接转存量 —— 两部的唯一差别就是 TMDB 这个字段。

本文件锁死：
  ① `returning` + 最后一集播出日距今超过 `RETURNING_STALE_DAYS` → 转存量；
  ② `returning` + 播出日在窗口内 → 仍是新上映（**不许误伤真在更新的剧**）；
  ③ `returning` + 拿不到 `last_air_date` → 保持原行为「算新上映」，绝不猜；
  ④ 边界：正好 30 天算在映，31 天才转存量；
  ⑤ `ended` 分支与电影窗口口径**一个字都不能变**；
  ⑥ 判据只有一份实现：`RETURNING_STALE_DAYS` 的数值判定只在
     `_new_release_state` 里，`_instant_candidates` 的 SQL 预筛不重复写阈值；
  ⑦ 端到端：那部 21 年前的老剧在标签与插队候选里都落成存量，同库在更的真剧不受影响。
"""
import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete

from app import share_center_v2 as sc2
from app.db import SessionLocal, migrate
from app.models import (
    AppConfig,
    AuditLog,
    Bot,
    Channel,
    QueueItem,
    Resource,
    SCCategory,
    SCCategoryCursor,
    SCCategorySchedule,
    SCHourWindow,
    SCLibraryVersion,
    SCPublishTask,
    SCPushList,
    SCPushListItem,
    SCSharePlan,
    SCSharePlanVersion,
    SCWorkCategory,
    ShareTask,
)

WIPED = (SCPublishTask, SCPushListItem, SCPushList, SCLibraryVersion,
         SCCategoryCursor, SCCategorySchedule, SCWorkCategory, SCCategory,
         SCSharePlanVersion, SCSharePlan, ShareTask, QueueItem, Resource,
         Channel, Bot, AppConfig, AuditLog, SCHourWindow)


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    for _ in range(2):
        with SessionLocal() as db:
            for model in WIPED:
                db.execute(delete(model))
            db.commit()
    yield


def _now() -> datetime:
    """固定「今天」= 2026-11-28 12:00 Asia/Shanghai（与真机排查同一天）。"""
    return datetime(2026, 11, 28, 12, 0, tzinfo=sc2.shanghai_tz())


def _d(days_ago: int) -> str:
    return str((_now() - timedelta(days=days_ago)).date())


def _state(*, release_date="2005-05-01", last_air_date="2005-06-11",
           finish_status="returning", media_type="tv", now_sh=None):
    return sc2._new_release_state(
        release_date=release_date, last_air_date=last_air_date,
        finish_status=finish_status, media_type=media_type,
        now_sh=now_sh or _now())


# ---------------------------------------------------------------------------
# ① 事故现场：21 年前的老剧不能再算新上映
# ---------------------------------------------------------------------------
def test_returning_with_ancient_last_air_date_is_backlog():
    """《王保长后传》原样数据：returning + 2005-06-11 → 存量。"""
    assert _state() is False


def test_returning_is_not_a_free_pass_for_any_old_show():
    """只要最后一集播出日够老，不管上映日填成什么，都不能算新上映。"""
    for rel in ("2005-05-01", "", "1998-01-01"):
        assert _state(release_date=rel) is False, rel


# ---------------------------------------------------------------------------
# ② 不许误伤：真的还在更新的剧必须继续是新上映
# ---------------------------------------------------------------------------
def test_returning_within_window_is_still_new_release():
    """最后一集在窗口内（哪怕首播是很多年前）→ 仍是新上映。"""
    # 首播 2019 年、上周还在更新 → 连载中，必须算新上映
    assert _state(release_date="2019-04-14", last_air_date=_d(7)) is True
    assert _state(release_date="2019-04-14", last_air_date=_d(0)) is True


def test_returning_boundary_exactly_30_days_stays_new_release():
    """边界：正好 `RETURNING_STALE_DAYS` 天算在映，多一天才转存量。"""
    n = sc2.RETURNING_STALE_DAYS
    assert _state(last_air_date=_d(n)) is True, f"{n} 天不该转存量"
    assert _state(last_air_date=_d(n + 1)) is False, f"{n + 1} 天必须转存量"


# ---------------------------------------------------------------------------
# ③ 拿不到播出日 → 不猜（保持原行为）
# ---------------------------------------------------------------------------
def test_returning_without_last_air_date_is_not_guessed():
    """`last_air_date` 为空：无从判断，按原行为算新上映，绝不编一个日期。"""
    assert _state(last_air_date="") is True
    assert _state(last_air_date="   ") is True
    # 脏数据（解析不出日期）同样按「拿不到」处理
    assert _state(last_air_date="2005-13-45") is True
    assert _state(last_air_date="unknown") is True


# ---------------------------------------------------------------------------
# ④ 还没上映的永远不算新上映（原有约束不许被兜底绕过）
# ---------------------------------------------------------------------------
def test_returning_future_release_is_not_new_release():
    """上映日在未来 → 不算新上映，哪怕 TMDB 说是 returning、刚播过一集。"""
    future = str((_now() + timedelta(days=30)).date())
    assert _state(release_date=future, last_air_date=_d(1)) is False


# ---------------------------------------------------------------------------
# ⑤ 其他分支一个字都不能变
# ---------------------------------------------------------------------------
def test_ended_branch_unchanged():
    """ended：播出日次日 0 点转存量；拿不到播出日直接判过期（v2.1.23 口径）。"""
    today = str(_now().date())
    assert _state(finish_status="ended", last_air_date=today) is True
    assert _state(finish_status="ended", last_air_date=_d(1)) is False
    assert _state(finish_status="ended", last_air_date="") is False
    # 真机对照：《王保长歪传》ended + 2005 → 存量（改动前就是这个结果）
    assert _state(finish_status="ended") is False


def test_movie_branch_unchanged():
    """电影没有连载概念 → 沿用 30 天上映窗口，与 `returning` 兜底无关。"""
    n = sc2.NEW_RELEASE_WINDOW_DAYS
    assert _state(media_type="movie", release_date=_d(5),
                  last_air_date="2005-06-11", finish_status="") is True
    assert _state(media_type="movie", release_date=_d(n + 5),
                  last_air_date="", finish_status="") is False


def test_unknown_status_unchanged():
    """剧集状态未知 → 退回上映窗口；没有任何日期 → None（不猜）。"""
    assert _state(finish_status="", release_date=_d(5),
                  last_air_date="") is True
    assert _state(finish_status="", release_date="",
                  last_air_date="") is None


# ---------------------------------------------------------------------------
# ⑥ 判据只有一份实现（红线：不许第二套阈值）
# ---------------------------------------------------------------------------
def test_threshold_declared_once_and_used_once():
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1]
           / "backend/app/share_center_v2.py").read_text(encoding="utf-8")
    assert src.count("RETURNING_STALE_DAYS = ") == 1, "阈值只能定义一次"
    assert src.count("(today - air).days > RETURNING_STALE_DAYS") == 1, (
        "判据只能在 `_new_release_state` 里写一次")
    # 常量值与用户建议口径一致（30 天）
    assert sc2.RETURNING_STALE_DAYS == 30


def test_instant_candidates_sql_does_not_duplicate_the_threshold():
    """`_instant_candidates` 的 SQL 预筛故意不按 `last_air_date` 收紧。

    一旦在 SQL 里再写一遍「距今 N 天」，就有两套判据了 —— 改一处忘一处
    会出现「标签说存量、插队说新上映」的分裂。预筛保持宽松，
    精确判定只由 `_new_release_state` 做。
    """
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1]
           / "backend/app/share_center_v2.py").read_text(encoding="utf-8")
    start = src.index("def _instant_candidates(")
    end = src.index("def plan_engine(")
    seg = src[start:end]
    assert "RETURNING_STALE_DAYS" not in seg, "SQL 预筛不许重复写停更阈值"
    # 但仍然必须把 returning 的行捞出来交给判据（宽松预筛，不能漏）
    assert 'Resource.finish_status == "returning"' in seg


# ---------------------------------------------------------------------------
# ⑦ 端到端：真机那两部 + 一部在更的真剧
# ---------------------------------------------------------------------------
def _tv(db, *, title, file_id, release_date, last_air_date, finish_status,
        entry_days_ago=200):
    res = Resource(
        file_id=file_id, name=f"{title}.S01E01.mkv", ext=".mkv", size=100,
        utime=1, identify_status="identified", media_type="tv",
        tmdb_id=abs(hash(file_id)) % 100000, media_title=title,
        share_dir_name=title, path_hint=f"影视/电视剧/国产剧/{title}/Season 1",
        release_date=release_date, last_air_date=last_air_date,
        finish_status=finish_status)
    db.add(res)
    db.flush()
    work_key = f"work:{title}"
    db.add(SCLibraryVersion(
        work_key=work_key, version_no=1, fingerprint=f"fp-{file_id}",
        media_type="tv", resource_ids_json=json.dumps([res.id]),
        metadata_json=json.dumps({"title": title}),
        change_type="created",
        created_at=datetime.now(timezone.utc) - timedelta(days=entry_days_ago),
    ))
    db.flush()
    return work_key


def test_end_to_end_tags_and_instant_candidates():
    with SessionLocal() as db:
        old = _tv(db, title="王保长后传", file_id="v2156-old",
                  release_date="2005-05-01", last_air_date="2005-06-11",
                  finish_status="returning")
        live = _tv(db, title="在更剧", file_id="v2156-live",
                   release_date="2024-09-01", last_air_date=_d(3),
                   finish_status="returning")
        db.commit()

        tags = sc2._work_push_tags(db, [old, live])
        assert [t["key"] for t in tags[old]] == ["backlog"], tags[old]
        assert [t["key"] for t in tags[live]] == ["new_release"], tags[live]

        # 插队候选：只有真在更的那部进 new_release 通道
        cands = sc2._instant_candidates(db, "prefer_new", now_sh=_now())
        keys = [k for k, _lv, _lane in cands]
        assert live in keys, cands
        assert old not in keys, "21 年前的老剧不许再占新上映插队位"
