"""v2.1.14 验收：映射清单分页 + 首推 dedup 死锁修复 + TgtoDrive 401 文案。

真机报障（2026-11-23，只读诊断已定位，本文件锁代码契约）：
  ① 目录映射 / 影视映射清单没有翻页，清单过长难用 → 默认每页 5 条、可切换条数；
  ② 资源库入库完毕、分享中心设了「新入库优先」，却**一个推送任务都不生成**。
     根因不是那个开关（它只管已生成任务的排序，v2.1.6 起只作用于排序），
     而是 plan_engine 的两道 dedup 都**不看任务状态**：清理只把 SCPublishTask
     标 cleaned，SCPushListItem 仍 planned → 首推名额被僵尸项永久占死。
     真机实测 227 个最新作品快照被挡、492 条任务 100% cleaned。
  ③ TgtoDrive 401 的根因是 Token **错配**而非撤销，文案须给出可操作的指引。
"""
import shutil
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app import db as dbmod
from app import models
from app import share_center_v2 as scv2

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
TGTO = (ROOT / "backend/app/tgto.py").read_text(encoding="utf-8")

WORK, CHAT = "work:a", "-100123"


@pytest.fixture
def db():
    root = Path(tempfile.mkdtemp(prefix="v2114_", dir="/tmp"))
    eng = create_engine(f"sqlite:///{root / 't.db'}")
    dbmod.Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False, autocommit=False)
    with S() as s:
        yield s
    eng.dispose()
    shutil.rmtree(root, ignore_errors=True)


def _item(s, *, work=WORK, chat=CHAT, dedup="work:a:1:first", status="planned", kind="first"):
    it = models.SCPushListItem(list_id=1, work_key=work, chat_id=chat,
                               dedup_key=dedup, source_kind=kind, status=status)
    s.add(it)
    s.flush()
    return it


def _task(s, item, *, status="planned", work=WORK, kind="first"):
    t = models.SCPublishTask(list_item_id=item.id, work_key=work, kind=kind, status=status)
    s.add(t)
    s.flush()
    return t


def _plan(s, work=WORK, chat=CHAT, dedup="work:a:1:first"):
    return scv2.first_dedup_plan(s, work_key=work, chat_id=chat, dedup=dedup)


# ---------- ① 首推 dedup：cleaned 不再占死名额 ----------

def test_no_item_creates(db):
    action, reuse = _plan(db)
    assert action == "create" and reuse is None


def test_active_first_item_still_blocks(db):
    it = _item(db)
    _task(db, it, status="planned")
    assert _plan(db) == ("skip", None), "在途首推必须继续去重，不能重复建"


def test_cleaned_first_item_revives_instead_of_blocking(db):
    """核心反证：清理后的首推项不得再挡住新任务，且必须**复活同一行**。"""
    it = _item(db)
    _task(db, it, status="cleaned")
    action, reuse = _plan(db)
    assert action == "revive", action
    assert reuse is not None and reuse.id == it.id


def test_revive_reuses_row_so_unique_dedup_key_is_not_violated(db):
    """dedup_key 是唯一约束：复活不能新增行，否则整轮计划会撞唯一键。"""
    it = _item(db)
    _task(db, it, status="cleaned")
    action, reuse = _plan(db)
    assert action == "revive"
    before = len(list(db.scalars(select(models.SCPushListItem)).all()))
    reuse.status = "planned"
    reuse.library_version_id = 999
    db.flush()
    after = list(db.scalars(select(models.SCPushListItem)).all())
    assert len(after) == before == 1
    assert after[0].dedup_key == "work:a:1:first", "复活不得改写 dedup_key"


def test_mixed_cleaned_and_active_still_blocks(db):
    it = _item(db)
    _task(db, it, status="cleaned")
    _task(db, it, status="planned")
    assert _plan(db) == ("skip", None), "只要还有未清理的任务就不能重建"


def test_failed_but_not_cleaned_still_blocks(db):
    """失败但未清理（仍可重试）的首推不在本次修复范围内，行为不变。"""
    it = _item(db)
    _task(db, it, status="failed")
    assert _plan(db) == ("skip", None)


def test_item_without_tasks_blocks_conservatively(db):
    _item(db)
    assert _plan(db) == ("skip", None), "无任务的残留项保守视为占用，避免重复首推"


def test_legacy_dedup_format_revives_legacy_row(db):
    """老格式 dedup（work:fingerprint:channel）同样按任务状态判定。"""
    it = _item(db, dedup="work:a:deadbeef:1")
    _task(db, it, status="cleaned")
    action, reuse = _plan(db)
    assert action == "revive"
    assert reuse.id == it.id
    assert reuse.dedup_key == "work:a:deadbeef:1"


def test_ever_published_never_rebuilds_first(db):
    """防重复闸：即便旧项已清理，只要曾真正推送成功就不再排首推。"""
    it = _item(db)
    _task(db, it, status="published")
    _task(db, it, status="cleaned")
    assert _plan(db) == ("skip", None)


def test_other_channel_is_independent(db):
    it = _item(db)
    _task(db, it, status="cleaned")
    action, reuse = _plan(db, chat="-100999", dedup="work:a:2:first")
    assert action == "create" and reuse is None, "别的渠道不受本渠道清理影响"


def test_other_work_is_independent(db):
    it = _item(db)
    _task(db, it, status="cleaned")
    action, reuse = _plan(db, work="work:b", dedup="work:b:1:first")
    assert action == "create" and reuse is None


# ---------- ② 清理任务时，清单项状态跟着走 ----------

def _cleanup(s, ids=None):
    body = scv2.Payload(payload={"ids": ids or []})
    return scv2.api_cleanup_tasks(body, s)


def test_cleanup_marks_orphan_list_item_cleaned(db):
    it = _item(db)
    t = _task(db, it, status="cancelled")
    out = _cleanup(db, ids=[t.id])
    assert out["cleaned"] == 1, out
    db.refresh(t)
    db.refresh(it)
    assert t.status == "cleaned"
    assert it.status == "cleaned", "清单项不应再显示为待推送"
    # 清理后首推可重建（复活同一行）
    action, reuse = _plan(db)
    assert action == "revive" and reuse.id == it.id


def test_cleanup_does_not_touch_item_with_live_task(db):
    it = _item(db)
    t = _task(db, it, status="cancelled")
    _task(db, it, status="planned")
    out = _cleanup(db, ids=[t.id])
    assert out["cleaned"] == 1
    db.refresh(it)
    assert it.status == "planned", "还有在途任务的清单项不能被标清理"
    assert _plan(db) == ("skip", None)


def test_cleanup_never_physically_deletes_rows(db):
    it = _item(db)
    t = _task(db, it, status="cancelled")
    _cleanup(db, ids=[t.id])
    assert len(list(db.scalars(select(models.SCPublishTask)).all())) == 1
    assert len(list(db.scalars(select(models.SCPushListItem)).all())) == 1


# ---------- ③ 前端：两张映射清单都有分页，默认 5 条 ----------

def test_frontend_pager_helper_exists():
    assert "useLocalPager" in APP
    assert "defaultSize = 5" in APP, "默认每页 5 条"
    assert "dirPager" in APP and "mediaPager" in APP


def test_frontend_tables_use_paged_rows():
    assert ':data="dirPager.paged.value"' in APP, "目录映射清单必须吃分页后的行"
    assert ':data="mediaPager.paged.value"' in APP, "影视映射清单必须吃分页后的行"
    assert ':data="mirrorStatus.dirs || []"' not in APP
    assert ':data="mirrorStatus.media || []"' not in APP


def test_frontend_two_pagination_bars_with_size_options():
    assert APP.count("<el-pagination") >= 2, "两张清单各要一条分页条"
    assert ":page-sizes" in APP, "必须可自定义每页显示条数"
    assert "sizes" in APP and "jumper" in APP
    assert "dirPager.pageSize.value" in APP and "mediaPager.pageSize.value" in APP


def test_frontend_page_resets_on_size_change():
    assert "watch(pageSize, () => { page.value = 1 })" in APP
    assert "watch(pageCount" in APP, "数据变少时页码要夹回有效范围，不留空白页"


# ---------- ④ TgtoDrive 401 文案给出可操作指引 ----------

def test_tgto_401_message_is_actionable():
    assert "TOKEN_401_HINT" in TGTO
    assert TGTO.count("raise TgtoError(TOKEN_401_HINT)") == 2, "读取与回写两条链路都要覆盖"
    assert "重新生成" in TGTO and "测试连接" in TGTO
    assert "不一致" in TGTO, "须点出根因是 Token 错配而非笼统「无效」"
