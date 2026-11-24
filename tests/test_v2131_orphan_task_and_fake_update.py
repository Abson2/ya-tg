"""v2.1.31：地道战「假追更」+「看着没推成功」两处根因的门禁。

现场（2026-11-24 只读诊断，数据已核）：地道战 6 条推送任务里，
  · 2 条永远停在 `submitted`（页面显示「已提交」），看起来像「没推成功」；
  · 其余被标成「追更」，但作品其实没更新过内容。

两条根因都不是「推送失败」，都断在**记录口径**上：

【问题一】队列行只能记住一条任务 → 前一条永远等不到回执
  `push_queue` 按 `(resource_id, channel_id)` 唯一（`uq_resource_channel`），
  一行同时只能存一个 `q.event_id`。同一作品在同一渠道既有首推又有追更时，
  后提交的那条把 `q.event_id` 顶掉，前一条任务就**永远等不到 mark_result**。
  真机：115 带 event_id=804、116 带 event_id=656 → 655 / 802 双双卡死。
  修法：`mark_result(success=True)` 收口同组 `submitted` 兄弟任务为 `skipped`
  （语义同 v2.1.6：已在本渠道发出，不重复投递）；存量由 `repair_cycle` 扫出来
  一并归档（`_orphan_submitted_tasks` / `_close_orphan_submitted`）。

【问题二】mirror 指纹含 utime → utime 一抖就重拷 → B 库 fileId 变 → 假追更
  scanner（v2.1.25）与作品快照（v2.1.6）早已按「内容身份 = file_id + size」判，
  但 `mirror._fingerprint` 仍是 `size:utime`，`(fileId, 指纹)` 命中不了就把文件
  当 A 库新内容重拷一遍，B 库 fileId 跟着变，快照指纹变，于是新版本 → 假追更。
  这是**绕开前两道防线的第三个独立入口**。
  修法：`_fingerprint` 格式**保持 `size:utime` 不动**（库里存量行就是它，改格式
  会让全部行对不上、整库重拷），另加 `_fp_size()` 只比 size 段，三个比较点统一
  改用它。

本文件锁死：
  A. 成功收口兄弟任务（含失败不动的反证、范围只限「同作品 × 同渠道」）
  B. 孤儿存量回收的三条判据（每条各配一条反证）
  C. mirror 指纹口径（含「格式不许改」的反证）
  D. 前端把新数字摊给用户看，且用中文叫法、不露枚举名
"""
import itertools
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import delete, func, select

from app import share_center_v2 as sc2
from app.db import SessionLocal, migrate
from app.models import (
    SCPublishExecution,
    SCPublishTask,
    SCPushList,
    SCPushListItem,
    utcnow,
)

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = ROOT / "frontend" / "src" / "App.vue"
MIRROR_PY = ROOT / "backend" / "app" / "mirror.py"

# 只清本文件用到的表：这几个用例不建计划版本、不碰资源库，
# 清多了反而容易与其它文件抢表。
WIPED = (SCPublishExecution, SCPublishTask, SCPushListItem, SCPushList)

_SEQ = itertools.count(1)


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    for _ in range(2):
        with SessionLocal() as db:
            for model in WIPED:
                db.execute(delete(model))
            db.commit()
    yield


# --------------------------------------------------------------------------
# 造数据
# --------------------------------------------------------------------------
def _item(db, *, work_key, chat_id="-1001234567890"):
    """一条清单项。同一「作品 × 渠道」可以有**多条**清单项（dedup_key 带序号），
    这正是真机形态：首推与追更各占一条清单项，但 chat_id 相同。"""
    n = next(_SEQ)
    lst = SCPushList(list_key=f"l-{n}", name="t", status="active")
    db.add(lst)
    db.flush()
    item = SCPushListItem(list_id=lst.id, work_key=work_key, chat_id=str(chat_id),
                          dedup_key=f"{work_key}:{chat_id}:{n}", status="planned")
    db.add(item)
    db.flush()
    return item


def _task(db, *, work_key, chat_id="-1001234567890", kind="first",
          status="submitted", created_at=None, published_at=None):
    item = _item(db, work_key=work_key, chat_id=chat_id)
    task = SCPublishTask(list_item_id=item.id, work_key=work_key, kind=kind,
                         status=status, created_at=created_at or utcnow(),
                         published_at=published_at)
    db.add(task)
    db.commit()
    return task


def _status(task_id):
    with SessionLocal() as db:
        return db.get(SCPublishTask, task_id).status


def _executions(task_id):
    with SessionLocal() as db:
        return list(db.scalars(
            select(SCPublishExecution)
            .where(SCPublishExecution.task_id == task_id)
            .order_by(SCPublishExecution.id)).all())


# ==========================================================================
# A. 成功时收口「被队列行顶掉」的兄弟任务
# ==========================================================================
def test_success_closes_sibling_submitted():
    """首推成功 → 同作品同渠道还停在「已提交」的追更任务归档为「已跳过投递」。

    这就是地道战 655 / 802 卡死的正解：消息**其实发出去了**，那条任务不该再投，
    也不该显示成「已提交」让人以为没推成功。
    """
    with SessionLocal() as db:
        first = _task(db, work_key="work-a", kind="first")
        upd = _task(db, work_key="work-a", kind="update")
        first_id, upd_id = first.id, upd.id
        out = sc2.mark_result(db, first_id, True)
    assert out["ok"] is True
    assert out["status"] == "published"
    assert out["siblings_closed"] == 1
    assert _status(first_id) == "published"
    assert _status(upd_id) == "skipped", "被顶掉的兄弟任务必须收口，不能永留已提交"
    rows = _executions(upd_id)
    assert len(rows) == 1, "归档必须留一条执行记录，否则事后查不出是谁归档的"
    assert rows[0].status == "skipped"
    payload = json.loads(rows[0].response_json or "{}")
    assert payload.get("reason") == "superseded_by"
    assert payload.get("task_id") == first_id


def test_failure_keeps_sibling_waiting():
    """反证：失败**不得**动兄弟任务 —— 队列行会原地重试，兄弟要留着等结果。

    如果失败也收口，真失败的任务会被一条不相干的成功记录掩盖掉。
    """
    with SessionLocal() as db:
        first = _task(db, work_key="work-a", kind="first")
        upd = _task(db, work_key="work-a", kind="update")
        first_id, upd_id = first.id, upd.id
        out = sc2.mark_result(db, first_id, False, error="网络抖动")
    assert out["siblings_closed"] == 0
    assert _status(first_id) == "failed"
    assert _status(upd_id) == "submitted", "失败时兄弟任务必须原样留着等重试"
    assert _executions(upd_id) == []


def test_sibling_scope_is_same_work_and_same_channel():
    """收口范围只限「同作品 × 同渠道」。

    同作品换个渠道（另一个群/频道）是**另一次真实投递**，同渠道换作品更无关，
    两者都必须原样留着，不能被顺手收掉。
    """
    with SessionLocal() as db:
        first = _task(db, work_key="work-a", chat_id="-1001111111111")
        other_chat = _task(db, work_key="work-a", chat_id="-1002222222222")
        other_work = _task(db, work_key="work-b", chat_id="-1001111111111")
        first_id, chat_id_, work_id = first.id, other_chat.id, other_work.id
        out = sc2.mark_result(db, first_id, True)
    assert out["siblings_closed"] == 0
    assert _status(chat_id_) == "submitted", "换渠道是另一次投递，不许收口"
    assert _status(work_id) == "submitted", "换作品与本次投递无关，不许收口"


def test_second_success_call_closes_nothing():
    """幂等：同一条任务被重复回执时只收口一次。

    兄弟任务已经是 `skipped`，再调一次不该再数一遍，也不该重复写执行记录
    （否则「归档重复投递 N 个」这个数字会随重放次数虚涨）。
    """
    with SessionLocal() as db:
        first = _task(db, work_key="work-a", kind="first")
        upd = _task(db, work_key="work-a", kind="update")
        first_id, upd_id = first.id, upd.id
        sc2.mark_result(db, first_id, True)
        out = sc2.mark_result(db, first_id, True)
    assert out["siblings_closed"] == 0
    assert len(_executions(upd_id)) == 1


# ==========================================================================
# B. 存量孤儿：三条判据同时成立才回收
# ==========================================================================
def _orphans():
    with SessionLocal() as db:
        return [t.id for t in sc2._orphan_submitted_tasks(db)]


def test_orphan_found_when_published_after_created():
    """三条判据全中：还停在已提交 / 同渠道有成功兄弟 / 兄弟是之后才投出去的。"""
    t0 = datetime(2026, 11, 24, 1, 0, tzinfo=timezone.utc)
    with SessionLocal() as db:
        orphan = _task(db, work_key="work-a", created_at=t0)
        _task(db, work_key="work-a", status="published", created_at=t0,
              published_at=t0 + timedelta(minutes=3))
        orphan_id = orphan.id
    assert _orphans() == [orphan_id]


def test_orphan_not_found_when_sibling_published_before_created():
    """反证：兄弟任务是**本任务之前**投的，说明本任务是一次新的、还没结束的投递。

    这种绝不能归档，否则会把真的在途任务误杀成「已跳过投递」。
    """
    t0 = datetime(2026, 11, 24, 1, 0, tzinfo=timezone.utc)
    with SessionLocal() as db:
        _task(db, work_key="work-a", created_at=t0)
        _task(db, work_key="work-a", status="published", created_at=t0 - timedelta(hours=2),
              published_at=t0 - timedelta(hours=1))
    assert _orphans() == []


def test_orphan_not_found_without_published_sibling():
    """反证：同渠道没有成功兄弟 → 它可能真的还在等回执，不许动。"""
    with SessionLocal() as db:
        _task(db, work_key="work-a", status="submitted")
        _task(db, work_key="work-a", status="failed")
    assert _orphans() == []


def test_orphan_ignores_tasks_not_in_submitted():
    """反证：只捞 `submitted`。

    `planned` / `ready` 是还没提交的，`failed` 是要重试的，`published` 是已成的
    —— 哪一种都不该被这条回收链碰到。
    """
    t0 = datetime(2026, 11, 24, 1, 0, tzinfo=timezone.utc)
    with SessionLocal() as db:
        for st in ("planned", "ready", "failed"):
            _task(db, work_key=f"work-{st}", status=st, created_at=t0)
            _task(db, work_key=f"work-{st}", status="published", created_at=t0,
                  published_at=t0 + timedelta(minutes=1))
    assert _orphans() == []


def test_repair_cycle_dry_run_counts_orphans_without_writing():
    """体检必须报出这个数字（用户才知道「那些已提交」是有原因的），且零写入。"""
    t0 = datetime(2026, 11, 24, 1, 0, tzinfo=timezone.utc)
    with SessionLocal() as db:
        orphan = _task(db, work_key="work-a", created_at=t0)
        _task(db, work_key="work-a", status="published", created_at=t0,
              published_at=t0 + timedelta(minutes=2))
        orphan_id = orphan.id
    import asyncio
    with SessionLocal() as db:
        out = asyncio.run(sc2.repair_cycle(db, dry_run=True))
    assert out["ok"] is True and out["dry_run"] is True
    assert out["tasks_orphan"] == 1
    assert "orphans_closed" not in out, "体检不该给出「已归档」数字，那是执行结果"
    assert _status(orphan_id) == "submitted", "体检不得改任务状态"


def test_repair_cycle_closes_orphans_and_keeps_rows():
    """执行时归档：改状态 + 写执行记录，**绝不删行**（红线：清理一律归档）。"""
    t0 = datetime(2026, 11, 24, 1, 0, tzinfo=timezone.utc)
    with SessionLocal() as db:
        orphan = _task(db, work_key="work-a", created_at=t0)
        _task(db, work_key="work-a", status="published", created_at=t0,
              published_at=t0 + timedelta(minutes=2))
        orphan_id = orphan.id
    import asyncio
    with SessionLocal() as db:
        out = asyncio.run(sc2.repair_cycle(db, dry_run=False))
    assert out["orphans_closed"] == 1
    assert _status(orphan_id) == "skipped"
    rows = _executions(orphan_id)
    assert len(rows) == 1
    assert rows[0].status == "skipped"
    payload = json.loads(rows[0].response_json or "{}")
    assert payload.get("reason") == "orphan_submitted"
    with SessionLocal() as db:
        assert db.scalar(select(func.count(SCPublishTask.id))) == 2, "归档不是删除"


# ==========================================================================
# C. mirror 指纹：只比 size 段，格式本身不许改
# ==========================================================================
def test_fp_size_strips_utime():
    from app import mirror
    assert mirror._fp_size("100:200") == "100"
    assert mirror._fp_size("100") == "100", "兼容老格式与新口径"
    assert mirror._fp_size("") == ""


def test_fingerprint_format_is_still_size_colon_utime():
    """反证：`_fingerprint` 的**输出格式不许改**。

    库里 `mirror_files.fingerprint` 存的就是 `size:utime`。一旦把 utime 摘掉，
    全部存量行都会与新值对不上 → 每个文件都被当「A 库新内容」整库重拷一遍。
    剔除 utime 只能发生在**比较时**（`_fp_size`），不能发生在**落库格式**上。
    """
    from app import mirror
    assert mirror._fingerprint({"fileSize": 100, "utime": 5}) == "100:5"
    assert mirror._fingerprint({}) == "0:0"


def test_utime_only_change_is_not_a_content_change():
    """utime 单独跳动不再算「内容变了」；size 变了才算。"""
    from app import mirror
    a = mirror._fingerprint({"fileSize": 100, "utime": 1})
    b = mirror._fingerprint({"fileSize": 100, "utime": 999})
    c = mirror._fingerprint({"fileSize": 101, "utime": 1})
    assert mirror._fp_size(a) == mirror._fp_size(b)
    assert mirror._fp_size(a) != mirror._fp_size(c)


def test_mirror_comparison_sites_use_size_segment():
    """三个比较点都必须走 `_fp_size`，且不得留下拿整指纹做键的老写法。"""
    src = MIRROR_PY.read_text(encoding="utf-8")
    # ① 变化计数（changed_fp）
    assert '!= _fp_size(str(r.fingerprint or ""))' in src
    # ② 已同步 / 待重试两张索引表的键
    assert "{(m.source_file_id, _fp_size(m.fingerprint)): m" in src
    # ③ 主循环里的命中判定
    assert "if (fid, fp_size) in synced_rows:" in src
    assert "retry_rows.pop((fid, fp_size), None)" in src
    # 反证：整指纹比较一律不许再出现
    assert "!= str(r.fingerprint" not in src
    assert "(fid, fp) in synced_rows" not in src
    assert "(fid, fp) in retry_rows" not in src
    assert "(m.source_file_id, m.fingerprint)" not in src


# ==========================================================================
# D. 前端：把新数字摊给用户看，中文叫法、不露枚举名
# ==========================================================================
def test_frontend_surfaces_orphan_counts_in_chinese():
    src = APP_VUE.read_text(encoding="utf-8")
    assert "pre.tasks_orphan" in src, "体检报告要报出卡住的条数"
    assert "d.orphans_closed" in src, "执行结果要报出归档的条数"
    assert "「已提交」等不到回执" in src
    assert "归档重复投递" in src
    # 状态中文名沿用 v2.1.6 的既有措辞，不新造第五种「跳过」
    assert "skipped: '已跳过投递'" in src
    # 面向用户的那两句文案里不得夹带枚举名（`submitted` / `superseded_by`）
    for line in src.splitlines():
        if "tasks_orphan" in line or "orphans_closed" in line:
            assert "submitted" not in line and "superseded" not in line
