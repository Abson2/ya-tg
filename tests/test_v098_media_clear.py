"""v0.9.8：影视映射清单无效项清理。"""
from pathlib import Path

import pytest

from app import main
from app.db import SessionLocal, migrate
from app.models import MirrorFile, MirrorMedia, MirrorRule

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


@pytest.fixture
def session():
    migrate()
    with SessionLocal() as s:
        for t in (MirrorFile, MirrorMedia, MirrorRule):
            s.query(t).delete()
        s.commit()
        try:
            yield s
        finally:
            for t in (MirrorFile, MirrorMedia, MirrorRule):
                s.query(t).delete()
            s.commit()


def _media(s, title, status, with_synced=False):
    r = MirrorRule(name="r", source_root_id="A", target_root_id="B", enabled=True)
    s.add(r)
    s.flush()
    m = MirrorMedia(rule_id=r.id, title_standard=title, status=status)
    s.add(m)
    s.flush()
    if with_synced:
        s.add(MirrorFile(media_id=m.id, rule_id=r.id, identity="a", file_name="a.mkv", status="synced"))
    else:
        s.add(MirrorFile(media_id=m.id, rule_id=r.id, identity="b", file_name="b.mkv", status="failed"))
    s.flush()
    return m


def test_invalid_media_detection(session):
    bad = _media(session, "历史失败剧", "failed")
    blocked = _media(session, "历史阻断剧", "blocked")
    good = _media(session, "正常剧", "failed", with_synced=True)  # failed 但有 synced 文件 → 不算无效
    invalid = main._invalid_media_rows(session)
    ids = {m.id for m in invalid}
    assert bad.id in ids and blocked.id in ids
    assert good.id not in ids


def test_clear_invalid_media_only_removes_bad(session):
    bad = _media(session, "历史失败剧", "failed")
    good = _media(session, "正常剧", "synced", with_synced=True)
    out = main.mirror_media_clear_invalid(db=session)
    assert out["removed_media"] == 1
    assert session.get(MirrorMedia, bad.id) is None
    assert session.get(MirrorMedia, good.id) is not None
    # 子 MirrorFile 一并清理，正常剧的文件不受影响
    assert session.query(MirrorFile).filter(MirrorFile.media_id == bad.id).count() == 0
    assert session.query(MirrorFile).filter(MirrorFile.media_id == good.id).count() == 1


def test_status_exposes_invalid_media(session):
    _media(session, "历史失败剧", "failed")
    st = main.mirror_status(db=session)
    assert st["counts"]["media_invalid"] == 1
    assert st["invalid_media"][0]["title"] == "历史失败剧"
    assert any(m["invalid"] for m in st["media"])


def test_frontend_clear_invalid_media_controls():
    assert "async function clearInvalidMedia" in APP
    # v2.1.11：清理动作统一走 /mirror/cleanup（scope=media），旧端点
    # /mirror/media/clear-invalid 在后端保留为薄封装（端点不删，只换实现）。
    assert "/mirror/cleanup" in APP
    assert "清理无效影视映射" in APP
    # v0.9.10：清理口径改为联网核对 B 库，前端不再展示本地 media_invalid 数字 / 「无效待清理」标签
    assert "核对 B 库" in APP
    # 红线：页面不得体现后端代码名词——media_invalid 是后端 counts 的键名，
    # 前端只能用后端给的中文口径别名（invalid_mappings）。
    assert "media_invalid" not in APP
    assert "无效待清理" not in APP
