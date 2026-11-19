"""v0.9.4 回归：交互 Bot / 推送目标 / 白名单去重 + 融合配置。

锁死以下红线：
  - effective_token 优先用 interaction_bot_id 指向的 Bot token（解密），未启用/缺失回退旧 contribution_bot_token。
  - push_whitelist 直接读 channels 表 enabled 记录，按 kind 分频道/群聊；channels 为空时回退旧 runtime_config 白名单。
  - is_push_allowed 在 whitelist 模式下按 channels 的 chat_id 放行/拦截。
  - interaction/config 回显 interaction_bot_id 与 bot_options（不含 token 明文）；保存可写入/清空 bot id。
  - add_channel 归一化 kind（非 group 即 channel），channels 回显带 kind。
"""
import pytest

from app import bot_listener, runtime_config
from app.db import SessionLocal, migrate
from app.models import AppConfig, Bot, Channel
from app.security import encrypt_secret


def _set(db, key, value):
    row = db.get(AppConfig, key)
    if row is None:
        db.add(AppConfig(key=key, value_enc=encrypt_secret(str(value))))
    else:
        row.value_enc = encrypt_secret(str(value))
    db.flush()


def _del(db, key):
    row = db.get(AppConfig, key)
    if row is not None:
        db.delete(row)
        db.flush()


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    with SessionLocal() as db:
        db.query(Channel).delete()
        db.query(Bot).delete()
        for k in ("interaction_bot_id", "contribution_bot_token", "bot_gate_mode",
                  "bot_push_channels_json", "bot_push_groups_json"):
            _del(db, k)
        db.commit()
    bot_listener._restrict_cache.clear()


def _add_bot(db, name="bot", token="123456:ABCDEF", enabled=True):
    b = Bot(name=name, token=encrypt_secret(token), enabled=enabled)
    db.add(b)
    db.flush()
    return b


def _add_channel(db, bot_id, chat_id, kind="channel", enabled=True):
    c = Channel(bot_id=bot_id, name="c", chat_id=chat_id, kind=kind, enabled=enabled)
    db.add(c)
    db.flush()
    return c


# ---------- token 去重 ----------
def test_effective_token_uses_bot_when_selected():
    with SessionLocal() as db:
        b = _add_bot(db, token="111:TOKEN_BOT")
        _set(db, "interaction_bot_id", str(b.id))
        _set(db, "contribution_bot_token", "999:OLD_TOKEN")
        db.commit()
        assert bot_listener.effective_token(db) == "111:TOKEN_BOT"


def test_effective_token_falls_back_when_no_selection():
    with SessionLocal() as db:
        _set(db, "contribution_bot_token", "999:OLD_TOKEN")
        db.commit()
        assert bot_listener.effective_token(db) == "999:OLD_TOKEN"


def test_effective_token_empty_when_selected_bot_disabled():
    with SessionLocal() as db:
        b = _add_bot(db, token="111:TOKEN_BOT", enabled=False)
        _set(db, "interaction_bot_id", str(b.id))
        _set(db, "contribution_bot_token", "999:OLD_TOKEN")
        db.commit()
        # 明确选了停用的 bot：返回空（不静默回退旧 token，避免用错身份）
        assert bot_listener.effective_token(db) == ""


# ---------- 白名单去重 ----------
def test_push_whitelist_reads_channels_by_kind():
    with SessionLocal() as db:
        b = _add_bot(db)
        _add_channel(db, b.id, "-100111", kind="channel")
        _add_channel(db, b.id, "-100222", kind="group")
        _add_channel(db, b.id, "-100333", kind="group", enabled=False)  # 停用不计
        db.commit()
        chans, groups = bot_listener.push_whitelist(db)
        assert chans == ["-100111"]
        assert groups == ["-100222"]


def test_push_whitelist_falls_back_when_no_channels():
    with SessionLocal() as db:
        _set(db, "bot_push_channels_json", '["-100777"]')
        _set(db, "bot_push_groups_json", '["-100888"]')
        db.commit()
        chans, groups = bot_listener.push_whitelist(db)
        assert chans == ["-100777"] and groups == ["-100888"]


def test_is_push_allowed_uses_channels_in_whitelist_mode():
    with SessionLocal() as db:
        b = _add_bot(db)
        _add_channel(db, b.id, "-100111", kind="channel")
        _set(db, "bot_gate_mode", "whitelist")
        db.commit()
        assert bot_listener.is_push_allowed(db, "-100111")[0] is True
        assert bot_listener.is_push_allowed(db, "-100999")[0] is False


def test_is_push_allowed_skips_in_membership_mode():
    with SessionLocal() as db:
        b = _add_bot(db)
        _add_channel(db, b.id, "-100111", kind="channel")
        _set(db, "bot_gate_mode", "membership")
        db.commit()
        assert bot_listener.is_push_allowed(db, "-100999")[0] is True


# ---------- 配置回显/保存 ----------
def test_config_get_exposes_interaction_bot_id_and_bot_options():
    from app.main import interaction_config_get
    with SessionLocal() as db:
        b = _add_bot(db, name="emby", token="111:SECRET")
        _set(db, "interaction_bot_id", str(b.id))
        db.commit()
        out = interaction_config_get(db)
        assert out["interaction_bot_id"] == str(b.id)
        opts = out["bot_options"]
        assert len(opts) == 1 and opts[0]["id"] == b.id
        # 不泄露 token 明文
        assert "111:SECRET" not in str(opts)


def test_config_save_writes_and_clears_interaction_bot_id():
    from app.main import InteractionConfigInput, interaction_config_save
    with SessionLocal() as db:
        b = _add_bot(db)
        interaction_config_save(InteractionConfigInput(interaction_bot_id=str(b.id)), db)
        assert runtime_config.get(db, "interaction_bot_id") == str(b.id)
        interaction_config_save(InteractionConfigInput(interaction_bot_id=""), db)
        assert runtime_config.get(db, "interaction_bot_id") == ""


def test_config_save_rejects_unknown_bot_id():
    from fastapi import HTTPException
    from app.main import InteractionConfigInput, interaction_config_save
    with SessionLocal() as db:
        with pytest.raises(HTTPException):
            interaction_config_save(InteractionConfigInput(interaction_bot_id="99999"), db)


def test_add_channel_normalizes_kind_and_echoes():
    from app.main import ChannelInput, add_channel, channels
    with SessionLocal() as db:
        b = _add_bot(db)
        add_channel(ChannelInput(bot_id=b.id, name="群", chat_id="-100222", kind="whatever"), db)
        rows = channels(db)
        assert len(rows) == 1 and rows[0]["kind"] == "channel"  # 非 group 归一为 channel
