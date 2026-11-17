"""v0.6.0：Bot 验证、频道测试推送，以及 Bot/频道/规则删除与级联清理。"""
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import select

from app.db import SessionLocal, migrate
from app.main import app
from app.models import Bot, Channel, QueueItem, Resource, Rule
from app.security import encrypt_secret

H = {"X-Admin-Password": "testpass"}


class FakeResponse:
    def __init__(self, data, status_code=200, headers=None):
        self._data, self.status_code, self.text = data, status_code, str(data)
        self.headers = headers or {}

    def json(self):
        return self._data


def _cleanup():
    db = SessionLocal()
    res = db.scalar(select(Resource).where(Resource.file_id == "v060-mgmt-res"))
    if res:
        db.query(QueueItem).filter(QueueItem.resource_id == res.id).delete(synchronize_session=False)
        db.delete(res)
    db.query(Rule).filter(Rule.name.like("v060-%")).delete(synchronize_session=False)
    db.query(Channel).filter(Channel.name.like("v060-%")).delete(synchronize_session=False)
    db.query(Bot).filter(Bot.name.like("v060-%")).delete(synchronize_session=False)
    db.commit()
    db.close()


def _seed():
    migrate()
    _cleanup()
    db = SessionLocal()
    b = Bot(name="v060-bot", token=encrypt_secret("123:FAKE"), enabled=True)
    db.add(b)
    db.flush()
    c = Channel(name="v060-channel", bot_id=b.id, chat_id="-100123", enabled=True)
    db.add(c)
    db.flush()
    r = Rule(name="v060-rule", channel_id=c.id, auto_publish=False, enabled=True)
    db.add(r)
    res = Resource(file_id="v060-mgmt-res", name="x.mkv", ext=".mkv", size=1, utime=1)
    db.add(res)
    db.flush()
    q = QueueItem(resource_id=res.id, channel_id=c.id, status="pending")
    db.add(q)
    db.commit()
    ids = (b.id, c.id, r.id, res.id, q.id)
    db.close()
    return ids


def test_bot_verify_and_channel_test_push():
    bid, cid, *_ = _seed()
    calls = []

    async def fake_post(self, url, **kwargs):
        calls.append((url, kwargs.get("json") or {}))
        if url.endswith("/getMe"):
            return FakeResponse({"ok": True, "result": {"username": "ya_tg_test_bot",
                                                         "first_name": "YaTG"}})
        return FakeResponse({"ok": True, "result": {"message_id": 42}})

    c = TestClient(app)
    with patch("httpx.AsyncClient.post", fake_post):
        b = c.post(f"/api/bots/{bid}/test", headers=H)
        ch = c.post(f"/api/channels/{cid}/test", headers=H)
    assert b.status_code == 200 and b.json()["ok"] is True
    assert b.json()["username"] == "ya_tg_test_bot"
    assert ch.status_code == 200 and ch.json()["ok"] is True
    assert ch.json()["message_id"] == "42"
    assert any(u.endswith("/getMe") for u, _ in calls)
    send = [p for u, p in calls if u.endswith("/sendMessage")]
    assert len(send) == 1 and "Ya-TG 测试推送" in send[0]["text"]
    assert "v060-channel" in send[0]["text"] and send[0]["chat_id"] == "-100123"
    _cleanup()


def test_channel_test_push_reports_auth_failure_as_actionable_hint():
    _, cid, *_ = _seed()

    async def fake_post(self, url, **kwargs):
        return FakeResponse({"ok": False, "description": "Bad Request: chat not found"}, 400)

    c = TestClient(app)
    with patch("httpx.AsyncClient.post", fake_post):
        r = c.post(f"/api/channels/{cid}/test", headers=H)
    assert r.status_code == 200 and r.json()["ok"] is False
    assert "频道 ID" in r.json()["message"]
    _cleanup()


def test_delete_channel_cascades_rules_and_queue_but_keeps_resource():
    _, cid, rid, resid, qid = _seed()
    c = TestClient(app)
    d = c.delete(f"/api/channels/{cid}", headers=H)
    assert d.status_code == 200, d.text
    assert d.json()["removed_rules"] == 1 and d.json()["removed_queue"] == 1
    db = SessionLocal()
    assert db.get(Channel, cid) is None
    assert db.get(Rule, rid) is None
    assert db.get(QueueItem, qid) is None
    assert db.get(Resource, resid) is not None
    db.close()
    _cleanup()


def test_delete_bot_cascades_channels_rules_and_queue():
    bid, cid, rid, _, qid = _seed()
    c = TestClient(app)
    d = c.delete(f"/api/bots/{bid}", headers=H)
    assert d.status_code == 200, d.text
    assert d.json()["removed_channels"] == 1
    assert d.json()["removed_rules"] == 1 and d.json()["removed_queue"] == 1
    db = SessionLocal()
    assert db.get(Bot, bid) is None and db.get(Channel, cid) is None
    assert db.get(Rule, rid) is None and db.get(QueueItem, qid) is None
    db.close()
    _cleanup()


def test_delete_rule_endpoint():
    _, _, rid, _, _ = _seed()
    c = TestClient(app)
    d = c.delete(f"/api/rules/{rid}", headers=H)
    assert d.status_code == 200 and d.json()["ok"] is True
    db = SessionLocal()
    assert db.get(Rule, rid) is None
    db.close()
    _cleanup()
