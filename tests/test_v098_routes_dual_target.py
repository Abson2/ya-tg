"""v0.9.8：推送白名单一条路由的频道 / 群 ID 分别填写（可其一或都填）。"""
import json
from pathlib import Path

from app import bot_listener, runtime_config
from app.db import SessionLocal, migrate
from app.main import PushRouteInput, PushRoutesInput, push_routes_get, push_routes_save

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


def setup_function():
    migrate()


def _db():
    return SessionLocal()


def test_one_route_two_targets_expands_to_channel_and_group():
    with _db() as db:
        runtime_config.set_many(db, {"push_routes_json": json.dumps([
            {"dir_id": "D1", "channel_id": "-100CH", "group_id": "-100GR", "note": "国产"}])})
        raw = bot_listener.push_routes_raw(db)
        assert len(raw) == 1 and raw[0]["channel_id"] == "-100CH" and raw[0]["group_id"] == "-100GR"
        expanded = bot_listener.push_routes(db)
        assert [(r["kind"], r["chat_id"]) for r in expanded] == [
            ("channel", "-100CH"), ("group", "-100GR")]


def test_one_target_only():
    with _db() as db:
        runtime_config.set_many(db, {"push_routes_json": json.dumps([
            {"dir_id": "", "channel_id": "", "group_id": "-100G", "note": ""}])})
        assert [(r["kind"], r["chat_id"]) for r in bot_listener.push_routes(db)] == [("group", "-100G")]


def test_legacy_kind_chat_id_still_works():
    with _db() as db:
        runtime_config.set_many(db, {"push_routes_json": json.dumps([
            {"dir_id": "D2", "kind": "group", "chat_id": "-100OLD", "note": "老数据"}])})
        raw = bot_listener.push_routes_raw(db)
        assert raw[0]["group_id"] == "-100OLD" and raw[0]["channel_id"] == ""
        assert bot_listener.push_routes(db)[0]["chat_id"] == "-100OLD"


def test_get_returns_raw_not_expanded():
    with _db() as db:
        db.query(__import__("app.models", fromlist=["Bot"]).Bot).delete()
        db.commit()
        runtime_config.set_many(db, {"push_routes_json": json.dumps([
            {"dir_id": "D1", "channel_id": "-100C", "group_id": "-100G", "note": ""}])})
        out = push_routes_get(db=db)
    assert len(out["routes"]) == 1  # 管理页回填仍是一条，不是两条
    assert out["routes"][0]["channel_id"] == "-100C"


def test_save_persists_dual_fields():
    with _db() as db:
        p = PushRoutesInput(routes=[
            PushRouteInput(dir_id="D3", channel_id="-100CH2", group_id="-100GR2", note="双目标")])
        # 无 Bot 时 Channel 同步会拦截，先跳过 Channel 部分仅验证解析：塞一个 Bot
        from app.models import Bot
        from app.security import encrypt_secret
        bot = Bot(name="b", token=encrypt_secret("fake-token"), enabled=True)
        db.add(bot)
        db.flush()
        runtime_config.set_many(db, {"interaction_bot_id": str(bot.id)})
        db.commit()
    with _db() as db:
        out = push_routes_save(p, db=db)
        assert out["ok"] is True and out["count"] == 1
        saved = json.loads(runtime_config.get(db, "push_routes_json"))
        assert saved[0]["channel_id"] == "-100CH2" and saved[0]["group_id"] == "-100GR2"
        assert bot_listener.push_routes(db)[0]["chat_id"] in ("-100CH2", "-100GR2")


def test_frontend_dual_target_ui():
    assert "channel_id: '', group_id: ''" in APP
    assert 'v-model="r.channel_id"' in APP
    assert 'v-model="r.group_id"' in APP
    assert "测试频道" in APP and "测试群" in APP
    assert "r.channel_id || r.group_id" in APP
    assert 'v-model="r.kind"' not in APP  # 旧的单选目标类型已移除
