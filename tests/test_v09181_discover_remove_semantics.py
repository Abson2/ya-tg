"""v0.9.18.1：发现列表「删除」语义修正 + 批量删除按钮。

用户重申的红线：
  - 删除 = 仅从本列表移除，不影响推送白名单、不影响 Bot 已加入的群/频道；
    删除后仍可手动/刷新重新加入。
  - 白名单按钮 = 只对「本项目推送是否加入白名单」生效（逻辑不变）。
  - 批量删除按钮要加上。

覆盖：
  1 删除频道不被白名单兜底复活（截图 -1003749901817 删不掉根因）
  2 删除不动白名单路由/Channel
  3 同 chat_id 多行（不同 Bot）一起移除
  4 手动重新加入恢复显示
  5 批量删除
  6 白名单开关仍独立生效
  7 前端批量删除按钮 + 文案
"""
from pathlib import Path

import pytest
from sqlalchemy import select

from app import bot_listener as bl
from app import main as main_mod
from app.db import SessionLocal, migrate
from app.models import Bot, Channel, DiscoveredChat

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as s:
        s.query(DiscoveredChat).delete()
        s.query(Channel).delete()
        s.query(Bot).delete()
        s.commit()
    yield
    with SessionLocal() as s:
        s.query(DiscoveredChat).delete()
        s.query(Channel).delete()
        s.query(Bot).delete()
        s.commit()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as s:
        yield s


def _seed_bot(db):
    b = Bot(name="Ya-TG Bot", token="enc:fake", enabled=True)
    db.add(b); db.commit()
    return b


def _whitelist_on(db, row):
    main_mod.discover_set_whitelist(row.id, main_mod.DiscoverWhitelistInput(in_whitelist=True), db)
    return row


def test_delete_channel_not_resurrected_by_whitelist_fallback(db):
    """核心回归：白名单里的频道删除后，discover_chats 兜底不再把它加回来。"""
    _seed_bot(db)
    bl.record_discovered_chat(db, {"id": -1003749901817, "type": "channel", "title": "资源频道"},
                              source="manual")
    row = db.scalar(select(DiscoveredChat))
    _whitelist_on(db, row)
    main_mod.discover_delete_chat(row.id, db)
    # 连续多次读取列表（每次都会跑白名单兜底），频道都不该复活
    for _ in range(3):
        out = main_mod.discover_chats(db)
        assert all(c["chat_id"] != "-1003749901817" for c in out["chats"])
    # 白名单配置仍在（推送不受影响）
    assert any(t["chat_id"] == "-1003749901817"
               for r in bl.push_routes_raw(db) for t in r["targets"])


def test_delete_keeps_whitelist_and_channel(db):
    """删除不动白名单路由、不动 Channel（原逻辑错误地移除了）。"""
    _seed_bot(db)
    bl.record_discovered_chat(db, {"id": -1001, "type": "group", "title": "群"})
    row = db.scalar(select(DiscoveredChat))
    _whitelist_on(db, row)
    main_mod.discover_delete_chat(row.id, db)
    ch = db.scalar(select(Channel).where(Channel.chat_id == "-1001"))
    assert ch is not None and ch.enabled is True
    assert any(t["chat_id"] == "-1001" for r in bl.push_routes_raw(db) for t in r["targets"])


def test_delete_all_duplicate_rows_same_chat_id(db):
    """同 chat_id 多 Bot 两行，删除一行时全部隐藏（截图两条相同 ID）。"""
    b1 = _seed_bot(db)
    b2 = Bot(name="Bot2", token="enc:fake2", enabled=True)
    db.add(b2); db.commit()
    bl.record_discovered_chat(db, {"id": -1002, "type": "channel", "title": "频道A"},
                              source="manual", bot_id_override=b1.id)
    bl.record_discovered_chat(db, {"id": -1002, "type": "channel", "title": "频道B"},
                              source="manual", bot_id_override=b2.id)
    rows = db.scalars(select(DiscoveredChat).where(DiscoveredChat.chat_id == "-1002")).all()
    assert len(rows) == 2
    main_mod.discover_delete_chat(rows[0].id, db)
    after = db.scalars(select(DiscoveredChat).where(DiscoveredChat.chat_id == "-1002")).all()
    assert len(after) == 2 and all(r.hidden is True for r in after)


def test_manual_readd_restores_visibility(db):
    """删除后手动重新加入 → 恢复显示。"""
    _seed_bot(db)
    bl.record_discovered_chat(db, {"id": -1003, "type": "channel", "title": "频道"}, source="manual")
    row = db.scalar(select(DiscoveredChat))
    main_mod.discover_delete_chat(row.id, db)
    assert db.scalar(select(DiscoveredChat)).hidden is True
    # 模拟手动重新加入（discover_refresh 的 else 分支核心动作）
    row = db.scalar(select(DiscoveredChat))
    row.hidden = False
    db.commit()
    out = main_mod.discover_chats(db)
    assert any(c["chat_id"] == "-1003" for c in out["chats"])


def test_batch_delete(db):
    """批量删除：全部隐藏、不动白名单。"""
    _seed_bot(db)
    for cid, title in [(-2001, "A"), (-2002, "B"), (-2003, "C")]:
        bl.record_discovered_chat(db, {"id": cid, "type": "group", "title": title}, source="manual")
    rows = db.scalars(select(DiscoveredChat).order_by(DiscoveredChat.id)).all()
    _whitelist_on(db, rows[0])
    ids = [r.id for r in rows]
    out = main_mod.discover_batch_delete(main_mod.DiscoverBatchDeleteInput(ids=ids), db)
    assert out["removed"] == 3
    assert all(r.hidden for r in db.scalars(select(DiscoveredChat)).all())
    out_chats = main_mod.discover_chats(db)["chats"]
    assert all(c["chat_id"] not in ("-2001", "-2002", "-2003") for c in out_chats)


def test_whitelist_toggle_still_independent(db):
    """白名单开关仍独立生效（不受删除语义改造影响）。"""
    _seed_bot(db)
    bl.record_discovered_chat(db, {"id": -3001, "type": "group", "title": "群"})
    row = db.scalar(select(DiscoveredChat))
    main_mod.discover_set_whitelist(row.id, main_mod.DiscoverWhitelistInput(in_whitelist=True), db)
    assert row.in_whitelist is True
    ch = db.scalar(select(Channel).where(Channel.chat_id == "-3001"))
    assert ch is not None and ch.enabled is True
    main_mod.discover_set_whitelist(row.id, main_mod.DiscoverWhitelistInput(in_whitelist=False), db)
    assert row.in_whitelist is False
    ch = db.scalar(select(Channel).where(Channel.chat_id == "-3001"))
    assert ch is None or ch.enabled is False


def test_frontend_batch_delete_button_and_copy():
    seg = APP.split("批量移出白名单", 1)[1].split("</div>", 1)[0]
    assert "batchDeleteDiscover" in seg
    assert "批量从列表移除" in seg
    assert "function batchDeleteDiscover" in APP
    assert "/discover/chats/batch-delete" in APP
    # 删除确认文案不再说「同时移出白名单」
    assert "不影响推送白名单" in APP
    assert "若已加入白名单会同时移出" not in APP
