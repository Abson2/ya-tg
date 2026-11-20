"""阶段五 B-04：手动扫描状态持久化 + 重启恢复。

_async_scan 纯内存态重启丢失，前端会看到 running 卡住/状态凭空清空。本批把状态
落 app_config，启动时把遗留 running 标为「已中断」（asyncio.Task 无法跨进程恢复，
不谎称在跑）。
"""
from datetime import timedelta

from app import async_scan_state
from app.db import SessionLocal, migrate
from app.models import AppConfig, utcnow

migrate()


def _cleanup():
    with SessionLocal() as db:
        row = db.get(AppConfig, async_scan_state.STATE_KEY)
        if row:
            db.delete(row)
        db.commit()


def test_save_load_roundtrip():
    _cleanup()
    state = {"running": True, "started_at": utcnow().isoformat(), "finished_at": None,
             "mode": "rescan", "error": "", "result": {"files": 12},
             "stage": "重新扫描库"}
    with SessionLocal() as db:
        async_scan_state.save(db, state)
        db.commit()
        out = async_scan_state.load(db)
    assert out["running"] is True
    assert out["mode"] == "rescan"
    assert out["result"] == {"files": 12}
    assert out["stage"] == "重新扫描库"
    _cleanup()


def test_load_returns_default_when_missing():
    _cleanup()
    with SessionLocal() as db:
        out = async_scan_state.load(db)
    assert out["running"] is False
    assert out["stage"] == ""


def test_recover_stale_running_marks_interrupted():
    _cleanup()
    state = {"running": True,
             "started_at": (utcnow() - timedelta(hours=3)).isoformat(),
             "finished_at": None, "mode": "scan", "error": "", "result": {},
             "stage": "扫描中"}
    with SessionLocal() as db:
        async_scan_state.save(db, state)
        db.commit()
        out = async_scan_state.recover_on_startup(db)
    assert out["running"] is False
    assert "中断" in out["stage"]
    assert "重启" in out["error"]
    assert out["finished_at"]
    # 持久化已生效：重新 load 拿到恢复后的状态
    with SessionLocal() as db:
        assert async_scan_state.load(db)["running"] is False
    _cleanup()


def test_recover_keeps_terminal_state():
    _cleanup()
    state = {"running": False, "started_at": utcnow().isoformat(),
             "finished_at": utcnow().isoformat(), "mode": "scan", "error": "",
             "result": {"files": 30}, "stage": "完成"}
    with SessionLocal() as db:
        async_scan_state.save(db, state)
        db.commit()
        out = async_scan_state.recover_on_startup(db)
    assert out["stage"] == "完成"
    assert out["result"] == {"files": 30}
    assert out["running"] is False
    _cleanup()


def test_corrupted_json_falls_back_to_default():
    _cleanup()
    with SessionLocal() as db:
        db.add(AppConfig(key=async_scan_state.STATE_KEY, value_enc="{bad json"))
        db.commit()
        out = async_scan_state.load(db)
    assert out["running"] is False
    _cleanup()
