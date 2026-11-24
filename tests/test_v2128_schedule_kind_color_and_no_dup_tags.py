"""v2.1.28：三表「资源名称」不再挂三类标签 + 「排期类型」里新入库绿色高亮。

用户口径（原话）：
  「资源名称中加了新入库和后面排期类型重复，去掉资源名称前面新入库等标识，
    在排期类型中将新入库字体设置为绿色高亮」

现场：推送队列每行是 `新入库 杨… | 分次 | 新入库 | 待排期` —— 同一信息一行里出现两次，
标签还挤掉了剧名（截断成「杨…」）。

本文件锁死：
  ① 三表（历史 / 队列 / 失败）的「资源名称」列**不再渲染三类标签**（反证：不得回归）；
  ② 标签功能没删 —— 仍保留在「推送任务」列表里（别为了去重把功能一起砍掉）；
  ③ 排期类型给 `schedule_kind_key`（new_release / new_library / backlog / 空），
     前端**按 key 映射颜色**，不比对中文文案（文案一改颜色就静默失效）；
  ④ 「新入库」用主题绿色变量（`--yg-tone-green-fg`），另两档不上色。
"""
import json

import pytest
from sqlalchemy import delete, select

from app import share_center_v2 as sc2
from app import utils
from app.db import SessionLocal, migrate
from app.models import (
    AppConfig,
    Bot,
    Channel,
    DistributionEvent,
    QueueItem,
    Resource,
    ResourceVersion,
    Rule,
    SCCategory,
    SCLibraryVersion,
    SCPublishTask,
    SCPushList,
    SCPushListItem,
    SCWorkCategory,
    ShareTask,
)
from datetime import datetime, timedelta, timezone

WIPED = (DistributionEvent, ResourceVersion, SCPublishTask, SCPushListItem,
         SCPushList, SCLibraryVersion, SCWorkCategory, SCCategory,
         QueueItem, ShareTask, Rule, Resource, Channel, Bot, AppConfig)


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    for _ in range(2):
        with SessionLocal() as db:
            for model in WIPED:
                db.execute(delete(model))
            db.commit()
    yield


def _now():
    return datetime.now(timezone.utc)


def _app_source() -> str:
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    return (root / "frontend/src/App.vue").read_text(encoding="utf-8")


# ==========================================================================
# ① 后端：(key, label) 一对，key 供上色
# ==========================================================================
def _tags(*keys):
    return [{"key": k, "label": sc2.PUSH_TAG_LABELS[k]} for k in keys]


def test_schedule_kind_pair_returns_key_and_label():
    assert sc2._schedule_kind_pair(_tags("new_library")) == ("new_library", "新入库")
    assert sc2._schedule_kind_pair(_tags("new_release")) == ("new_release", "新上映")
    assert sc2._schedule_kind_pair(_tags("backlog")) == ("backlog", "存量排期")
    # 优先级与「今日推送排期」归列同序：新上映 > 新入库 > 存量
    assert sc2._schedule_kind_pair(_tags("backlog", "new_library")) == ("new_library", "新入库")
    assert sc2._schedule_kind_pair(_tags("new_library", "new_release")) == ("new_release", "新上映")
    # 判不出 → 空 key + 空标签（页面显示「—」，不猜成存量）
    assert sc2._schedule_kind_pair([]) == ("", "")
    assert sc2._schedule_kind_pair(None) == ("", "")
    # `_schedule_kind_of` 仍等价于取标签那一半（老调用方不受影响）
    assert sc2._schedule_kind_of(_tags("new_library")) == "新入库"


def test_views_rows_carry_schedule_kind_key():
    """三视图每行都要给 key —— 前端上色只认它。"""
    with SessionLocal() as db:
        res = Resource(file_id="f-k1", name="新片.mkv", ext=".mkv", size=1, utime=1,
                       identify_status="identified", media_type="movie",
                       media_title="今天刚入库", release_date="",
                       share_url="https://share/x")
        db.add(res)
        db.flush()
        wk = utils.work_group_key(res)
        db.add(SCLibraryVersion(
            work_key=wk, version_no=1, fingerprint="fp-k1", media_type="movie",
            resource_ids_json=json.dumps([res.id]),
            metadata_json=json.dumps({"title": "今天刚入库"}, ensure_ascii=False),
            created_at=_now(), change_type="created"))
        db.add(SCWorkCategory(work_key=wk, category_key="genre:动作"))
        db.add(SCCategory(category_key="genre:动作", name="动作", enabled=True, priority=0))
        lst = SCPushList(list_key="l-k1", name="t", status="active")
        db.add(lst)
        db.flush()
        item = SCPushListItem(list_id=lst.id, work_key=wk, chat_id="-100",
                              dedup_key=f"{wk}:k1", category_key="genre:动作")
        db.add(item)
        db.flush()
        db.add(SCPublishTask(list_item_id=item.id, work_key=wk, kind="first",
                             status="published", published_at=_now()))
        db.commit()
        views = sc2._aggregate_views(db)
    rows = views["history"]
    assert rows, "今天入库并推送过的作品应进推送历史"
    row = rows[0]
    assert row["schedule_kind"] == "新入库"
    assert row["schedule_kind_key"] == "new_library", (
        "只给中文标签的话，前端只能拿文案比色，改文案就静默失效")


# ==========================================================================
# ② 前端：资源名称不挂标签
# ==========================================================================
def test_three_tables_name_column_has_no_tags():
    app = _app_source()
    for anchor, name in (('scOverview.views?.history || []', "推送历史"),
                         ('<el-table :data="scQueueRows"', "推送队列"),
                         ('scOverview.views?.failed || []', "推送失败")):
        seg = app[app.index(anchor):]
        seg = seg[:seg.index("</el-table>")]
        assert "sc-rec-tags" not in seg, (
            f"{name}的资源名称列又挂回了三类标签 —— 与排期类型列重复")
        assert "s.row.tags" not in seg, f"{name}不该再读 tags"


def test_tags_still_rendered_somewhere():
    """反证：去重不等于把功能砍掉 —— 推送任务列表仍要显示三类标签。"""
    app = _app_source()
    assert "sc-rec-tags" in app, "三类标签在整个页面都消失了，属于功能删减"
    assert 'class="sc-title-row"' in app, "标签仍在用的地方要保持「剧名左侧」结构"


# ==========================================================================
# ③ 前端：排期类型按 key 上色
# ==========================================================================
def test_schedule_kind_tone_uses_key_not_chinese():
    app = _app_source()
    assert "function scScheduleKindTone" in app or "const scScheduleKindTone" in app
    seg = app[app.index("scScheduleKindTone = (row)"):]
    seg = seg[:seg.index("\n")]
    assert "schedule_kind_key" in seg, "配色要按 key 走"
    assert "new_library" in seg
    assert "新入库" not in seg, "不许用中文文案比色（改文案就静默失效，且等于第二套口径）"


def test_new_library_is_green_in_schedule_kind_column():
    app = _app_source()
    # 三处「排期类型」列都走同一个 class 绑定
    assert app.count("scScheduleKindTone(s.row)") == 3, (
        "三张卡的排期类型列都要上色，漏一处就会出现两种观感")
    assert ".sc-kind.is-new" in app
    seg = app[app.index(".sc-kind.is-new"):]
    seg = seg[:seg.index("\n")]
    assert "--yg-tone-green-fg" in seg, "「新入库」要用主题绿色变量，别写死色值（双主题会崩）"


def test_plan_rows_pass_schedule_kind_key():
    """待排期行同样要透传 key，否则队列里那批永远不上色。"""
    app = _app_source()
    assert "schedule_kind_key: w.schedule_kind_key || ''" in app
