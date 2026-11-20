"""阶段五 B-04：手动扫描状态持久化 + 重启恢复。

背景：`main._async_scan` 是进程内 dict，容器重启/进程崩溃后丢失——前端轮询会看到
running 卡住或状态凭空清空，用户不知道上次扫描是被腰斩还是完成了。本模块把同一份
状态以 JSON 落在 app_config（专用 key，明文 JSON 不含凭证），启动时把遗留的 running
标为「已中断」（asyncio.Task 无法跨进程恢复，绝不谎称继续在跑），供前端如实展示。

不 import main，避免循环依赖；main 负责把本模块的读写挂到现有流程。
"""
from __future__ import annotations

import json
from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import AppConfig, utcnow
from .utils import as_utc

STATE_KEY = "async_scan_state_v1"
_DEFAULT = {"running": False, "started_at": None, "finished_at": None,
            "mode": "", "error": "", "result": {}, "stage": ""}
# 超过该时长仍 running 视为重启残留（正常扫描 minutes 级，1 小时足够保守）
STALE_RUNNING_HOURS = 1


def load(db: Session) -> dict:
    row = db.get(AppConfig, STATE_KEY)
    if not row or not row.value_enc:
        return dict(_DEFAULT)
    try:
        data = json.loads(row.value_enc)
        if isinstance(data, dict):
            out = dict(_DEFAULT)
            out.update({k: v for k, v in data.items() if k in _DEFAULT})
            return out
    except (ValueError, TypeError):
        pass
    return dict(_DEFAULT)


def save(db: Session, state: dict) -> None:
    payload = {k: state.get(k, _DEFAULT[k]) for k in _DEFAULT}
    text = json.dumps(payload, ensure_ascii=False)[:8000]
    row = db.get(AppConfig, STATE_KEY)
    if row is None:
        row = AppConfig(key=STATE_KEY, value_enc=text, updated_at=utcnow())
        db.add(row)
    else:
        row.value_enc = text
        row.updated_at = utcnow()
    db.flush()


def clear(db: Session) -> None:
    row = db.get(AppConfig, STATE_KEY)
    if row is not None:
        db.delete(row)
        db.flush()


def recover_on_startup(db: Session) -> dict:
    """启动时调用：把遗留 running（必然是被重启腰斩的）标为 interrupted。

    返回恢复后的状态 dict。已终结状态保持不动（供前端看到上次成功/失败结果）。
    """
    state = load(db)
    if not state.get("running"):
        return state
    started = as_utc(_parse_ts(state.get("started_at")))
    cutoff = utcnow() - timedelta(hours=STALE_RUNNING_HOURS)
    # 无开始时间或过期的 running 一律判残留；近期 running 也可能是上次未优雅停止，
    # 进程已重启则 task 必然不存在，同样判残留（不谎称在跑）。
    state["running"] = False
    state["stage"] = "已中断（进程重启，扫描未完成）"
    state["error"] = (state.get("error") or "") + ("；" if state.get("error") else "") + \
        "进程重启导致扫描中断，请重新发起"
    state["finished_at"] = utcnow().isoformat()
    state["error"] = state["error"][:1000]
    save(db, state)
    db.commit()
    return state


def _parse_ts(value):
    if not value or not isinstance(value, str):
        return None
    try:
        from datetime import datetime
        return datetime.fromisoformat(value)
    except ValueError:
        return None
