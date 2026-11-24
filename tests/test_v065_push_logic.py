"""v0.6.5 回归测试：识别后才推送、整剧单条、作品归一、整剧大小、模板迁移、节流。

1. 扫描只入库不入队；识别成功且有海报才入队；无海报不入队
2. 同一部剧 N 个单集只入一条队列（聚合键一致）
3. 作品归一：'谷雨' 与 '谷雨 (2026)(1)' 归为同一作品，不显示两张卡
4. episode_label 单集显示已识别 E，不显示 E25-E25
5. _build_message 大小取整剧所有单集之和
6. 模板特征迁移：旧模板自动升级、自定义模板不动
7. 逐条发送节流配置读写
"""
import asyncio
from unittest.mock import AsyncMock, patch

from sqlalchemy import select

from app.db import (DEFAULT_TEMPLATE, SessionLocal, _converge_legacy_queue,
                    _is_legacy_default, _needs_template_upgrade, migrate)
from app.main import (_enqueue_after_identify, _enqueue_existing, _enqueue_ready, _series_group_key,
                      clear_library, library_series, queue_schedule_get, queue_schedule_save,
                      QueueScheduleInput)
from app.models import Bot, Channel, QueueItem, Resource, Rule, ScanCursor
from app.publisher import _build_message
from app.security import encrypt_secret


def _clean():
    db = SessionLocal()
    db.query(QueueItem).delete(synchronize_session=False)
    db.query(Rule).delete(synchronize_session=False)
    db.query(Channel).delete(synchronize_session=False)
    db.query(Bot).delete(synchronize_session=False)
    db.query(Resource).filter(Resource.file_id.like("v065-%")).delete(synchronize_session=False)
    db.commit()
    db.close()


def _res(fid, name, **kw):
    kw.setdefault("size", 1)
    return Resource(file_id=fid, name=name, ext=".mkv", utime=1, **kw)


def _setup_rule():
    db = SessionLocal()
    b = Bot(name="v065-bot", token=encrypt_secret("1:FAKE"), enabled=True)
    db.add(b); db.flush()
    c = Channel(name="v065-ch", bot_id=b.id, chat_id="-1001", enabled=True)
    db.add(c); db.flush()
    db.add(Rule(name="v065-rule", channel_id=c.id, auto_publish=True, enabled=True))
    db.commit(); db.close()


# ---------- 1. 识别后才入队 ----------
def test_enqueue_only_after_identified_with_poster():
    migrate(); _clean(); _setup_rule()
    db = SessionLocal()
    r = _res("v065-no-id", "Movie.2024.mkv")
    r.identify_status = "unidentified"
    db.add(r); db.commit()
    assert _enqueue_after_identify(db, r) == 0  # 未识别不入队
    r.identify_status = "identified"; r.media_type = "movie"; r.media_title = "Movie"
    r.poster_path = ""  # 无海报
    db.commit()
    assert _enqueue_after_identify(db, r) == 0  # 无海报不入队
    r.poster_path = "/poster.jpg"; db.commit()
    assert _enqueue_after_identify(db, r) == 1  # 有海报才入队
    db.close(); _clean()


# ---------- 2. 同剧只入一条 ----------
def test_same_series_enqueues_only_once():
    migrate(); _clean(); _setup_rule()
    db = SessionLocal()
    a = _res("v065-ep1", "Show.S01E01.mkv", identify_status="identified", media_type="tv",
             media_title="谷雨", poster_path="/p.jpg", share_dir_id="dir-show", share_dir_name="谷雨")
    b = _res("v065-ep2", "Show.S01E02.mkv", identify_status="identified", media_type="tv",
             media_title="谷雨", poster_path="/p.jpg", share_dir_id="dir-show", share_dir_name="谷雨")
    db.add_all([a, b]); db.commit()
    q1 = _enqueue_after_identify(db, a)
    q2 = _enqueue_after_identify(db, b)
    db.commit()
    assert q1 == 1 and q2 == 0, (q1, q2)  # 第二集不再入队
    assert db.query(QueueItem).count() == 1
    db.close(); _clean()


# ---------- 3. 作品归一不裂卡 ----------
def test_work_key_normalization_merges_cards():
    db = SessionLocal()
    a = _res("v065-a", "E01.mkv", path_hint="发布组/谷雨 (2026)(1)/Season 1", media_title="谷雨",
             media_type="tv", share_dir_name="谷雨")
    b = _res("v065-b", "E02.mkv", path_hint="发布组/谷雨/Season 1", media_title="谷雨", media_type="tv")
    ka, kb = _series_group_key(a), _series_group_key(b)
    db.close()
    assert ka == kb, (ka, kb)


def test_library_series_single_episode_label():
    migrate(); _clean()
    db = SessionLocal()
    db.add(_res("v065-one", "Show.S01E25.mkv", media_title="谷雨", media_type="tv", tmdb_id=279155,
                season=1, episode=25, identify_status="identified", path_hint="发布组/谷雨"))
    db.commit(); db.close()
    db = SessionLocal(); groups = library_series(q="", media_type="", db=db); db.close()
    g = next((x for x in groups if x.get("tmdb_id") == 279155), None)
    assert g, groups
    assert "E25-E25" not in g["episode_label"]
    assert "E25" in g["episode_label"]
    _clean()


# ---------- 4. 整剧大小求和 ----------
def test_build_message_uses_whole_series_size():
    migrate(); _clean()
    db = SessionLocal()
    dir_id = "v065-dir"
    for i in (1, 2, 3):
        db.add(_res(f"v065-size{i}", f"Show.S01E0{i}.mkv", size=5 * 1024 ** 3,
                    media_title="谷雨", media_type="tv", tmdb_id=279155, season=1,
                    episode=i, identify_status="identified", poster_path="/p.jpg", share_dir_id=dir_id,
                    vote_average=9.0, path_hint="发布组/谷雨"))
    db.commit()
    rep = db.scalar(select(Resource).where(Resource.file_id == "v065-size1"))
    text, _ = _build_message(db, rep, "https://share/x")
    assert "15.0GB" in text, text  # 3 集 × 5GB
    # v2.1.22：默认图文用「📡 分集: 更新至 S01E03 · 已收录 3 集」表达整剧集数与进度
    assert "已收录 3 集" in text, text
    assert ".mkv" not in text, text  # 消息不含单集文件名
    db.close(); _clean()


# ---------- 5. 模板特征迁移 ----------
def test_legacy_template_detection():
    assert _is_legacy_default("资源：{{filename}}\n{{share_url}}") is True
    assert _is_legacy_default("<b>{{title}}</b>\ntype={{type}} · genres={{genres}}") is True
    assert _is_legacy_default(DEFAULT_TEMPLATE) is False
    assert _is_legacy_default("<b>我的自定义模板 {{title}}</b>\n{{overview}}") is False


def test_v064_template_upgrades_for_episode_count():
    """v0.6.4 整剧模板含 🎬 但缺集数行，也需升级——解决「已升级模板没变」。"""
    v064 = ("<b>🎬 {{title}}</b>\n{{#if season_label}}📺 {{season_label}}\n{{/if}}"
            "{{#if version}}🎞 版本：{{version}}\n{{/if}}{{#if size}}💾 大小：{{size}}\n{{/if}}{{share_url}}")
    assert _needs_template_upgrade(v064) is True
    assert _needs_template_upgrade("资源：{{filename}}\n{{share_url}}") is True
    assert _needs_template_upgrade(DEFAULT_TEMPLATE) is False  # 已含 episode_count
    assert _needs_template_upgrade("<b>我的自定义 {{title}}</b>") is False


def test_converge_legacy_queue_keeps_one_per_work():
    """升级前同剧逐集排队的旧残留，收敛后同频道同作品只留最早一条。"""
    migrate(); _clean()
    db = SessionLocal()
    from app.security import encrypt_secret
    b = Bot(name="v065-bot3", token=encrypt_secret("1:FAKE"), enabled=True)
    db.add(b); db.flush()
    c = Channel(name="v065-ch3", bot_id=b.id, chat_id="-1003", enabled=True)
    db.add(c); db.flush()
    eps = []
    for i in range(1, 4):
        r = _res(f"v065-c{i}", f"Show.S01E0{i}.mkv", media_type="tv",
                 media_title="剧", share_dir_name="剧", share_dir_id="dir-conv")
        db.add(r); db.flush()
        eps.append(r)
    for r in eps:
        db.add(QueueItem(resource_id=r.id, channel_id=c.id, status="pending"))
    db.commit()
    removed = _converge_legacy_queue(db)
    db.commit()
    assert removed == 2, removed
    assert db.query(QueueItem).count() == 1
    db.close(); _clean()


def test_process_queue_skips_duplicate_sent_work():
    """运行期兜底：同频道同作品已 sent 时，残留的 pending 单集不再推送。"""
    import app.publisher as pub
    migrate(); _clean()
    db = SessionLocal()
    from app.security import encrypt_secret
    b = Bot(name="v065-bot4", token=encrypt_secret("1:FAKE"), enabled=True)
    db.add(b); db.flush()
    c = Channel(name="v065-ch4", bot_id=b.id, chat_id="-1004", enabled=True)
    db.add(c); db.flush()
    r1 = _res("v065-p1", "Show.S01E01.mkv", media_type="tv", media_title="剧",
              share_dir_id="dir-push", share_url="https://share/x", poster_path="/p.jpg")
    r2 = _res("v065-p2", "Show.S01E02.mkv", media_type="tv", media_title="剧",
              share_dir_id="dir-push", share_url="https://share/x", poster_path="/p.jpg")
    db.add_all([r1, r2]); db.commit()
    id1, id2, cid = r1.id, r2.id, c.id
    db.add(QueueItem(resource_id=id1, channel_id=cid, status="sent", telegram_message_id="old"))
    db.add(QueueItem(resource_id=id2, channel_id=cid, status="pending"))
    db.commit(); db.close()

    async def fake_send(*a, **k):
        return True, "new-msg", "", None

    with patch.object(pub, "ensure_share", new=AsyncMock(return_value="https://share/x")), \
         patch.object(pub, "_send_telegram", new=AsyncMock(side_effect=fake_send)):
        result = asyncio.run(pub.process_queue(limit=20))
    assert result["sent"] == 0, result  # 未真正发送
    db = SessionLocal()
    q2 = db.scalar(select(QueueItem).where(QueueItem.resource_id == id2, QueueItem.channel_id == cid))
    assert q2.status == "sent" and "去重" in (q2.error or ""), (q2.status, q2.error)
    db.close(); _clean()


def test_manual_share_uses_series_dir_not_single_file():
    """手动「分享」必须按剧集目录建链，并把链接同步给同目录所有单集。"""
    from app.main import create_share
    import app.main as m
    migrate(); _clean()
    db = SessionLocal()
    dir_id = "dir-manual-share"
    eps = []
    for i in (1, 2):
        r = _res(f"v065-ms{i}", f"Show.S01E0{i}.mkv", media_type="tv", media_title="剧",
                 season=1, share_dir_id=dir_id, share_scope="series_dir")
        db.add(r); db.flush(); eps.append(r)
    db.commit()
    rid = eps[0].id

    class FakeClient:
        def __init__(self, *a, **k): pass
        async def create(self, target_id, title, duration):
            assert target_id == dir_id, f"分享 target 应为剧集目录，实际 {target_id}"
            assert "第1季" in title, title
            return {"data": {"shareId": "sid-manual", "shareUrl": "https://share/dir"}}

    with patch.object(m, "GuangYaShareClient", FakeClient):
        result = asyncio.run(create_share(rid, 0, db=db))
    assert result["scope"] == "series_dir" and result["members"] == 2, result
    db = SessionLocal()
    for e in eps:
        row = db.get(Resource, e.id)
        assert row.share_url == "https://share/dir"  # 同目录所有单集共用一条链接
        assert row.share_id == "sid-manual"
    db.close(); _clean()


def test_enqueue_repairs_missing_share_dir():
    """旧剧集缺 share_dir_id 时，入队前现场补算目录，保证走整剧分享。"""
    migrate(); _clean(); _setup_rule()
    db = SessionLocal()
    r = _res("v065-nodir", "Show.S01E01.mkv", identify_status="identified", media_type="tv",
             media_title="剧", poster_path="/p.jpg", parent_id="parent-dir-fid",
             dir_chain_json="[]", path_hint="发布组/剧/Season 1")
    db.add(r); db.commit()
    assert not (r.share_dir_id or "").strip()  # 确认旧数据缺失
    from app.models import Rule as _Rule
    rule = db.scalar(select(_Rule))
    assert _enqueue_ready(db, r, rule) is True
    assert r.share_dir_id == "parent-dir-fid"  # 用 parent_id 兜底补成目录
    assert r.share_scope == "series_dir"
    db.close(); _clean()


def test_enqueue_existing_only_ready_resources():
    """加规则回溯时，未识别/无海报/同剧重复资源不入队。"""
    migrate(); _clean()
    db = SessionLocal()
    from app.security import encrypt_secret
    b = Bot(name="v065-bot2", token=encrypt_secret("1:FAKE"), enabled=True)
    db.add(b); db.flush()
    c = Channel(name="v065-ch2", bot_id=b.id, chat_id="-1002", enabled=True)
    db.add(c); db.flush()
    rule = Rule(name="v065-rule2", channel_id=c.id, auto_publish=True, enabled=True)
    db.add(rule); db.flush()
    # 未识别
    db.add(_res("v065-x1", "A.mkv", identify_status="unidentified"))
    # 已识别但无海报
    db.add(_res("v065-x2", "B.mkv", identify_status="identified", media_type="movie", media_title="B"))
    # 已识别有海报的同一部剧两集
    for i in (1, 2):
        db.add(_res(f"v065-x{i+2}", f"Show.S01E0{i}.mkv", identify_status="identified",
                    media_type="tv", media_title="剧", poster_path="/p.jpg",
                    share_dir_name="剧", share_dir_id="dir-x"))
    db.commit()
    added = _enqueue_existing(db, rule)
    db.commit()
    assert added == 1, added  # 只有已识别有海报的剧入一条
    assert db.query(QueueItem).count() == 1
    db.close(); _clean()


# ---------- 6. 节流配置 ----------
def test_send_delay_config():
    migrate()
    db = SessionLocal()
    d = queue_schedule_save(QueueScheduleInput(interval_seconds=30, cron="", send_delay=8), db=db)
    assert d["send_delay"] == 8
    db.close()
    db = SessionLocal()
    cfg = queue_schedule_get(db=db)
    assert cfg["send_delay"] == 8
    queue_schedule_save(QueueScheduleInput(interval_seconds=30, cron="", send_delay=2), db=db)
    db.close()
