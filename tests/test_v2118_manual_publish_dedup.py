"""v2.1.18：资源库「手动推送」500 —— 同一请求内重复入队撞唯一键。

真机现象（线上容器日志）：
    POST /api/resources/1372/publish → 500
    sqlalchemy.exc.IntegrityError:
      UNIQUE constraint failed: push_queue.resource_id, push_queue.channel_id

根因（已用探针在真实代码路径上复现，不是推断）：
    `publish_resource` 有两个入队分支 —— 目录路由（routes）+ 启用规则（rules）。
    Session 是 `autoflush=False`，routes 分支 `db.add` 之后**没有 flush**，
    rules 分支的 `db.scalar` 因此看不见刚 add 的行 → 同一对
    (resource_id, channel_id) 被 add 两次 → 函数末尾统一 flush 时撞唯一键。

实测触发条件（变体反证）：
    · 目录路由(频道+群) + 规则指向某个路由渠道 → 撞键（线上正是这套配置）
    · 只有路由、无规则                        → 正常（入队 2）
    · 规则指向路由之外的第三渠道              → 正常（入队 3）
    · 两条启用规则指向同一渠道                → 也撞键（rules 循环内部同样漏查）

修法：一个跨分支、跨循环的 `seen_channels` 去重集合 + 统一 helper
`_ensure_pending_q()`，两个分支都走它。只补一处 flush 治不干净 ——
rules 循环内部的重复它也挡不住。

本文件只验证「不撞键 + 去重不丢渠道 + 计数如实」这三件事。
"""
import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest

from app import main, runtime_config
from app.db import SessionLocal, migrate
from app.models import AppConfig, Bot, Channel, QueueItem, Resource, Rule
from sqlalchemy import select

TABLES = (QueueItem, Resource, Channel, Bot, Rule, AppConfig)

CH1 = "-1003749901817"   # 频道
CH2 = "-1003708133864"   # 群
CH3 = "-1009999999999"   # 第三条独立渠道


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as s:
        for m in TABLES:
            s.query(m).delete()
        s.commit()
    yield


def _seed(*, routes=None, rules=()):
    """建 Bot + 三个渠道 + 一条资源。

    routes: [{dir_id, channel_id, group_id}]，None 表示不写路由配置。
    rules:  [(名称, chat_id)]，按 chat_id 找渠道建启用规则。
    """
    with SessionLocal() as s:
        bot = Bot(name="bot", token="x", enabled=True)
        s.add(bot)
        s.flush()
        chans = {}
        for name, chat, kind in (("频道", CH1, "channel"),
                                 ("群", CH2, "group"),
                                 ("频道3", CH3, "channel")):
            ch = Channel(bot_id=bot.id, name=name, chat_id=chat, kind=kind, enabled=True)
            s.add(ch)
            s.flush()
            chans[chat] = ch.id
        res = Resource(file_id="f-1", name="同一部剧.S01E01.mkv",
                       parent_id="d-root", dir_chain_json='["d-root"]')
        s.add(res)
        s.flush()
        for rname, chat in rules:
            s.add(Rule(name=rname, channel_id=chans[chat], enabled=True))
        runtime_config.set_many(s, {
            "push_routes_json": json.dumps(routes if routes is not None else [],
                                           ensure_ascii=False)})
        s.commit()
        return res.id, chans


def _publish(rid):
    """跑一次手动推送，返回 (结果 dict | None, 异常 | None, 队列行 list)。"""
    with SessionLocal() as s:
        try:
            with patch.object(main, "process_queue",
                              new=AsyncMock(return_value={"ok": True})):
                out = asyncio.run(main.publish_resource(rid, s))
            err = None
        except Exception as exc:               # noqa: BLE001 —— 这里就是要抓异常
            out, err = None, exc
    with SessionLocal() as s2:
        rows = list(s2.scalars(select(QueueItem).where(
            QueueItem.resource_id == rid).order_by(QueueItem.id)).all())
        snap = [(r.id, r.channel_id, r.status) for r in rows]
    return out, err, snap


ONE_ROUTE_BOTH = [{"dir_id": "", "channel_id": CH1, "group_id": CH2}]


# ---------- 核心：线上现场必须不再 500 ----------
def test_route_and_rule_same_channel_no_conflict():
    """线上配置：一条整库路由（频道+群）+ 规则「影视」指向频道。

    修复前：IntegrityError → 手动推送 500。
    修复后：200，且频道与群各一条，一个都不少。
    """
    rid, chans = _seed(routes=ONE_ROUTE_BOTH, rules=[("影视", CH1)])
    out, err, rows = _publish(rid)
    assert err is None, f"仍然撞唯一键：{err}"
    assert out["ok"] is True
    assert out["queued"] == 2, "频道 + 群应各入队一条"
    assert sorted(r[1] for r in rows) == sorted([chans[CH1], chans[CH2]])
    assert all(r[2] == "pending" for r in rows)


def test_two_rules_same_channel_no_conflict():
    """两条启用规则指向同一渠道（无路由）——rules 循环内部同样会漏查。

    修复前撞键，修复后只应入队 1 条。
    """
    rid, chans = _seed(routes=[], rules=[("规则1", CH1), ("规则2", CH1)])
    out, err, rows = _publish(rid)
    assert err is None
    assert out["queued"] == 1
    assert [r[1] for r in rows] == [chans[CH1]]


def test_route_and_rule_disjoint_keeps_all_targets():
    """规则指向路由之外的第三渠道 —— 去重不能把不重复的渠道吃掉。"""
    rid, chans = _seed(routes=ONE_ROUTE_BOTH, rules=[("影视", CH3)])
    out, err, rows = _publish(rid)
    assert err is None
    assert out["queued"] == 3
    assert sorted(r[1] for r in rows) == sorted([chans[CH1], chans[CH2], chans[CH3]])


def test_routes_only_still_queues_all():
    """只有路由、没有规则——v0.9.11 的「群也要推」语义不能退回去。"""
    rid, chans = _seed(routes=ONE_ROUTE_BOTH, rules=[])
    out, err, rows = _publish(rid)
    assert err is None
    assert out["queued"] == 2
    assert sorted(r[1] for r in rows) == sorted([chans[CH1], chans[CH2]])


def test_many_rules_same_channel_dedup_to_one():
    """五条规则全指同一渠道：只入队 1 条，不撞键。"""
    rid, chans = _seed(routes=[], rules=[(f"规则{i}", CH1) for i in range(5)])
    out, err, rows = _publish(rid)
    assert err is None
    assert out["queued"] == 1
    assert len(rows) == 1


# ---------- 已有行 / 归档行：复位而不是新增 ----------
def test_existing_pending_row_is_reset_not_duplicated():
    """库里已有 (资源, 渠道) 行时，手动推送应复位它，而不是再 INSERT 一条。"""
    rid, chans = _seed(routes=ONE_ROUTE_BOTH, rules=[("影视", CH1)])
    with SessionLocal() as s:
        s.add(QueueItem(resource_id=rid, channel_id=chans[CH1], status="sent",
                        error="旧的失败原因", attempts=3))
        s.commit()
    out, err, rows = _publish(rid)
    assert err is None
    assert out["queued"] == 2
    assert len(rows) == 2, "不应多出一行"
    ch1_row = [r for r in rows if r[1] == chans[CH1]][0]
    assert ch1_row[2] == "pending", "已推过的行要复位为待推送（手动强制重推）"


def test_archived_row_revived_not_duplicated():
    """归档行仍占唯一键 —— 命中应复活复用，不能再 INSERT。"""
    rid, chans = _seed(routes=[], rules=[("影视", CH1)])
    with SessionLocal() as s:
        s.add(QueueItem(resource_id=rid, channel_id=chans[CH1], status="cleaned",
                        clean_state="cleaned", clean_reason="历史清理"))
        s.commit()
    out, err, rows = _publish(rid)
    assert err is None
    assert out["queued"] == 1
    assert len(rows) == 1, "必须复用归档行，不能新增一行"
    with SessionLocal() as s:
        row = s.scalar(select(QueueItem).where(QueueItem.resource_id == rid))
        assert row.status == "pending"
        assert row.clean_state == "", "复活后要清掉归档标记，否则普通查询看不见它"


# ---------- 计数契约 ----------
def test_queued_count_equals_unique_channels():
    """queued 是「去重后的目标渠道数」，前端提示靠它，不能虚报。"""
    rid, chans = _seed(routes=ONE_ROUTE_BOTH, rules=[("影视", CH1), ("复制", CH2)])
    out, err, rows = _publish(rid)
    assert err is None
    assert out["queued"] == 2 == len(rows)


def test_legacy_only_rules_without_routes_keeps_old_behavior():
    """没有路由配置时，行为与 v2.1.17 一致：按规则逐渠道入队。"""
    rid, chans = _seed(routes=[], rules=[("影视", CH1), ("群推", CH2)])
    out, err, rows = _publish(rid)
    assert err is None
    assert out["queued"] == 2
    assert sorted(r[1] for r in rows) == sorted([chans[CH1], chans[CH2]])
