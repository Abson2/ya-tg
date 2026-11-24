"""v0.6.4 回归测试：作品聚合视图、清空扫描结果、取消分享、整剧模板。

1. library_series 把同剧 N 个单集聚合成一张作品卡
2. clear_library 清空资源/队列/断点但保留 Bot/频道/规则/模板
3. delete_share 取消后清空 share_url/share_id，同分享兄弟同步
4. 默认模板整剧展示、不暴露单集文件名，version 变量提取技术规格

直接调用端点函数而非 TestClient，避免 lifespan 的后台 worker 与用例争抢同一 SQLite。
"""
import asyncio
from unittest.mock import patch

from sqlalchemy import select

from app.db import DEFAULT_TEMPLATE, SessionLocal, migrate
from app.main import (QueueScheduleInput, clear_library, delete_share, library_series,
                      queue_schedule_get, queue_schedule_save, _queue_trigger)
from app.models import Bot, Channel, MessageTemplate, QueueItem, Resource, Rule, ScanCursor
from app.publisher import _build_message
from app.security import encrypt_secret
from app.utils import version_spec


def _clean():
    db = SessionLocal()
    db.query(QueueItem).delete(synchronize_session=False)
    db.query(Rule).delete(synchronize_session=False)
    db.query(Channel).delete(synchronize_session=False)
    db.query(Bot).delete(synchronize_session=False)
    db.query(Resource).filter(Resource.file_id.like("v064-%")).delete(synchronize_session=False)
    db.commit()
    db.close()


def _res(fid, name, **kw):
    return Resource(file_id=fid, name=name, ext=".mkv", size=1, utime=1, **kw)


# ---------- 1. 作品聚合 ----------
def test_library_series_groups_episodes_into_one_work():
    migrate()
    _clean()
    db = SessionLocal()
    db.add_all([
        _res("v064-e1", "Show.S01E01.mkv", media_title="谷雨", media_type="tv",
             tmdb_id=279155, season=1, episode=1, identify_status="identified",
             share_url="https://share/x", share_status="active"),
        _res("v064-e2", "Show.S01E02.mkv", media_title="谷雨", media_type="tv",
             tmdb_id=279155, season=1, episode=2, identify_status="identified"),
        _res("v064-movie", "Movie.2024.mkv", media_title="某片", media_type="movie",
             tmdb_id=999, identify_status="identified"),
    ])
    db.commit()
    db.close()

    db = SessionLocal()
    groups = library_series(q="", media_type="", db=db)
    db.close()
    tv = next((g for g in groups if g.get("tmdb_id") == 279155), None)
    assert tv, groups
    assert tv["count"] == 2 and len(tv["ids"]) == 2
    assert tv["share_url"] == "https://share/x" and tv["share_status"] == "active"
    assert "E01" in tv["episode_label"] and "E02" in tv["episode_label"]
    assert tv["identified_count"] == 2
    assert any(g.get("tmdb_id") == 999 and g["count"] == 1 for g in groups)
    _clean()


# ---------- 2. 清空扫描结果 ----------
def test_clear_library_keeps_config_but_empties_resources():
    migrate()
    _clean()
    db = SessionLocal()
    b = Bot(name="v064-bot", token=encrypt_secret("1:FAKE"), enabled=True)
    db.add(b); db.flush()
    c = Channel(name="v064-ch", bot_id=b.id, chat_id="-1001", enabled=True)
    db.add(c); db.flush()
    db.add(Rule(name="v064-rule", channel_id=c.id, enabled=True))
    res = _res("v064-clear", "E01.mkv")
    db.add(res); db.flush()
    db.add(QueueItem(resource_id=res.id, channel_id=c.id, status="retry", error="旧错误"))
    db.add(ScanCursor(status="failed", error="旧断点"))
    db.commit()
    rid = res.id
    db.close()

    db = SessionLocal()
    d = clear_library(db=db)
    db.close()
    assert d["resources"] >= 1 and d["queue"] >= 1 and d["cursors"] >= 1

    db = SessionLocal()
    assert db.query(Resource).count() == 0
    assert db.query(QueueItem).count() == 0
    assert db.query(ScanCursor).count() == 0
    assert db.query(Bot).count() >= 1 and db.query(Rule).count() >= 1
    assert db.query(MessageTemplate).count() >= 1
    db.close()


# ---------- 3. 取消分享 ----------
def test_cancel_share_clears_url_and_siblings():
    migrate()
    _clean()
    db = SessionLocal()
    a = _res("v064-share-a", "E01.mkv", share_id="share-v064", share_url="https://share/a",
             share_status="active")
    b = _res("v064-share-b", "E02.mkv", share_id="share-v064", share_url="https://share/a",
             share_status="active")
    db.add_all([a, b]); db.commit()
    rid, bid = a.id, b.id
    db.close()

    async def ok_delete(self, share_id):
        return {"data": {}}

    db = SessionLocal()
    with patch("app.guangya.GuangYaShareClient.delete", ok_delete):
        result = asyncio.run(delete_share(rid=rid, db=db))
    db.close()
    assert result["ok"] is True

    db = SessionLocal()
    for rid2 in (rid, bid):
        r = db.get(Resource, rid2)
        assert r.share_url == "" and r.share_id == "" and r.share_status == "cancelled"
    db.close()
    _clean()


# ---------- 4. 模板整剧展示 ----------
def test_default_template_is_series_oriented():
    # v2.1.24：默认图文版式（🎬 标题 / 📺 TMDB / ⭐ 评分 / 📽 影视信息（折叠）/
    # 📡 剧集更新（折叠）/ 📖 简介（折叠）/ 🏷 标签 / 分享链接）
    assert "🏷 标签: {{tags}}" in DEFAULT_TEMPLATE
    assert "资源：" not in DEFAULT_TEMPLATE and "{{filename}}" not in DEFAULT_TEMPLATE
    # 技术规格与大小已合并成 spec_line 折叠块（不再有独立的 {{quality}} 行），
    # 但「整剧展示 + 季集标签」这条 v0.6.4 的原始口径没变
    assert "{{spec_line}}" in DEFAULT_TEMPLATE
    assert "{{episode_status}}" in DEFAULT_TEMPLATE


def test_version_spec_extracts_full_spec():
    spec = version_spec("The.Silent.Storm.S01E04.2026.2160p.WEB-DL.H265.10bit.HDR10.DoVi.DDP5.1.Atmos.mkv")
    assert "2160p" in spec and "WEB-DL" in spec and "H.265" in spec
    assert "10bit" in spec and "DoVi" in spec and "Atmos" in spec


def test_build_message_hides_single_episode_filename():
    migrate()
    _clean()
    db = SessionLocal()
    r = _res("v064-tpl", "The.Silent.Storm.S01E04.2026.2160p.WEB-DL.mkv",
             media_title="谷雨", media_type="tv", tmdb_id=279155, season=1,
             vote_average=9.0, identify_status="identified")
    db.add(r); db.commit()
    text, _ = _build_message(db, r, "https://share/x")
    # v0.9.0：剧集状态会显示「更新至 S01E04 · 已收录 1 集」这类进度标签，
    # 这是用户明确要的季集进度，不再是「单集文件名泄漏」。真正要防的是
    # 完整文件名 / 扩展名进入推送文案。
    assert ".mkv" not in text
    assert "The.Silent.Storm.S01E04" not in text
    # v2.1.22：默认图文里标题不再带 🖼️ 后缀，评分行改为「⭐ TMDB评分: N/10」
    assert "S01" in text and "2026" in text
    assert "⭐ TMDB评分: 9/10" in text and "https://share/x" in text
    db.close()
    _clean()


# ---------- 5. 推送调度配置 ----------
def test_queue_schedule_interval_and_cron():
    migrate()
    db = SessionLocal()
    # 默认间隔
    d = queue_schedule_save(QueueScheduleInput(interval_seconds=45, cron=""), db=db)
    assert d["ok"] and d["interval_seconds"] == 45 and d["mode"] == "interval"
    trg = _queue_trigger(db)
    assert isinstance(trg, tuple) and trg[1]["seconds"] == 45
    db.close()

    db = SessionLocal()
    cfg = queue_schedule_get(db=db)
    assert cfg["interval_seconds"] == 45 and cfg["cron"] == ""
    db.close()

    # cron 模式
    from fastapi import HTTPException
    db = SessionLocal()
    d = queue_schedule_save(QueueScheduleInput(interval_seconds=30, cron="*/1 * * * *"), db=db)
    assert d["mode"] == "cron" and d["cron"] == "*/1 * * * *"
    trg = _queue_trigger(db)
    assert not isinstance(trg, tuple)  # CronTrigger 实例
    db.close()

    # 非法 cron 被拒
    db = SessionLocal()
    try:
        queue_schedule_save(QueueScheduleInput(interval_seconds=30, cron="bad cron"), db=db)
        assert False, "非法 cron 应抛 HTTPException"
    except HTTPException as e:
        assert e.status_code == 400
    finally:
        db.close()

    # 恢复默认，避免污染其它测试
    db = SessionLocal()
    queue_schedule_save(QueueScheduleInput(interval_seconds=30, cron=""), db=db)
    db.close()
