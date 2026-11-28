"""v2.1.58：存量分享重建脚本（scripts/rebuild_work_shares.py）的判据与落库。

脚本本身改的是线上网盘与本地库，所以这里只测**纯逻辑**：
  · 一条老链接算不算「已经是作品级」→ `decide()` 的动作判定
  · 没有作品级链接时必须走新建，且旧链全部列入待删
  · `apply_plan()` 的回写顺序：先回写库、后删旧链（不许出现两头都空）
  · 分享 id 的两种形态（update/delete 用数字段，access_token 用完整尾部）
"""
import asyncio
import importlib.util
from pathlib import Path

from app.db import SessionLocal, migrate
from app.models import Resource, SCPublishTask

ROOT = Path(__file__).resolve().parents[1]


def _load_script():
    spec = importlib.util.spec_from_file_location(
        "rebuild_work_shares", ROOT / "scripts" / "rebuild_work_shares.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


RWS = _load_script()
URL_A = "https://www.guangyapan.com/s/111_aey8CVbCzY9joZbY"
URL_B = "https://www.guangyapan.com/s/222_aey8CVbCzY9joZbY"


def _clean():
    db = SessionLocal()
    db.query(SCPublishTask).filter(SCPublishTask.work_key.like("work:v2158r%")) \
        .delete(synchronize_session=False)
    db.query(Resource).filter(Resource.file_id.like("v2158r-%")) \
        .delete(synchronize_session=False)
    db.commit()
    db.close()


def _item(urls, action="probe"):
    return {"work_key": "work:v2158r-x", "urls": list(urls), "action": action,
            "dir_id": "999", "title": "某剧"}


# ---------- 分享 id 两种形态 ----------
def test_share_id_forms():
    assert RWS.share_id_of(URL_A) == "111"
    assert RWS.share_tail_of(URL_A) == "111_aey8CVbCzY9joZbY"


# ---------- 动作判定 ----------
def test_decide_keeps_the_work_level_share():
    plan = [_item([URL_A, URL_B])]
    RWS.decide(plan, {URL_A: "other", URL_B: "work_level"})
    assert plan[0]["action"] == "keep"
    assert plan[0]["keep"] == URL_B
    assert plan[0]["drop"] == [URL_A]


def test_decide_creates_when_nothing_is_work_level():
    """真机三部失效作品就落在这条：全 dead → 必须新建，旧链全删。"""
    plan = [_item([URL_A])]
    RWS.decide(plan, {URL_A: "dead"})
    assert plan[0]["action"] == "create"
    assert plan[0]["keep"] == ""
    assert plan[0]["drop"] == [URL_A]


def test_decide_prefers_earliest_work_level():
    plan = [_item([URL_A, URL_B])]
    RWS.decide(plan, {URL_A: "work_level", URL_B: "work_level"})
    assert plan[0]["keep"] == URL_A
    assert plan[0]["drop"] == [URL_B]


def test_decide_skips_untouched_items():
    plan = [{"work_key": "work:v2158r-y", "urls": [], "action": "skip",
             "reason": "没有实时资源行（作品已不在库里）", "dir_id": "", "title": ""}]
    RWS.decide(plan, {})
    assert plan[0]["action"] == "skip"


# ---------- 落库顺序 ----------
def test_apply_creates_then_repoints_then_deletes(monkeypatch):
    migrate(); _clean()
    db = SessionLocal()
    db.add(Resource(file_id="v2158r-e1", name="E01.mkv", ext=".mkv", size=1, utime=1,
                    media_title="某剧", media_type="tv", tmdb_id=777001,
                    share_dir_id="999", share_url=URL_A, share_id="111",
                    share_status="active"))
    db.add(SCPublishTask(work_key="work:v2158r-x", kind="publish", status="published",
                         share_url=URL_A, link_state="active"))
    db.commit()

    calls = {"deleted": [], "created": []}

    class FakeClient:
        def __init__(self, _db):
            pass

        async def update(self, sid, title, duration):
            calls.setdefault("renamed", []).append((sid, title))

        async def delete(self, sid):
            calls["deleted"].append(sid)

    async def fake_create(client, file_id, title, duration):
        calls["created"].append((file_id, title))
        return "333", "https://www.guangyan.com/s/333_tok"

    monkeypatch.setattr(RWS, "GuangYaShareClient", FakeClient)
    monkeypatch.setattr(RWS, "create_share_parsed", fake_create)

    plan = [{"work_key": "work:v2158r-x", "urls": [URL_A], "action": "create",
             "dir_id": "999", "title": "某剧", "keep": "", "drop": [URL_A]}]
    stats = asyncio.run(RWS.apply_plan(db, plan, sleep=0))

    assert calls["created"] == [("999", "某剧")]     # 建在**作品目录**上，不是单集
    assert calls["deleted"] == ["111"]
    assert stats["created"] == 1 and stats["deleted"] == 1

    task = db.query(SCPublishTask).filter(SCPublishTask.work_key == "work:v2158r-x").one()
    assert task.share_url == "https://www.guangyan.com/s/333_tok"
    res = db.query(Resource).filter(Resource.file_id == "v2158r-e1").one()
    assert res.share_url == "https://www.guangyan.com/s/333_tok"
    assert res.share_id == "333" and res.share_status == "active"
    db.close()
    _clean()


def test_apply_does_not_delete_when_create_fails(monkeypatch):
    """建失败必须原样保留旧链——不许出现「旧的删了、新的没建出来」。"""
    migrate(); _clean()
    db = SessionLocal()
    db.add(Resource(file_id="v2158r-f1", name="E01.mkv", ext=".mkv", size=1, utime=1,
                    media_title="某剧", media_type="tv", tmdb_id=777002,
                    share_dir_id="999", share_url=URL_A, share_id="111",
                    share_status="active"))
    db.add(SCPublishTask(work_key="work:v2158r-x", kind="publish", status="published",
                         share_url=URL_A, link_state="active"))
    db.commit()

    calls = {"deleted": []}

    class FakeClient:
        def __init__(self, _db):
            pass

        async def delete(self, sid):
            calls["deleted"].append(sid)

    async def boom(*a, **k):
        raise RuntimeError("网盘抽风")

    monkeypatch.setattr(RWS, "GuangYaShareClient", FakeClient)
    monkeypatch.setattr(RWS, "create_share_parsed", boom)

    plan = [{"work_key": "work:v2158r-x", "urls": [URL_A], "action": "create",
             "dir_id": "999", "title": "某剧", "keep": "", "drop": [URL_A]}]
    stats = asyncio.run(RWS.apply_plan(db, plan, sleep=0))

    assert calls["deleted"] == []
    assert stats["created"] == 0 and stats["errors"]
    res = db.query(Resource).filter(Resource.file_id == "v2158r-f1").one()
    assert res.share_url == URL_A          # 旧链完好
    db.close()
    _clean()
