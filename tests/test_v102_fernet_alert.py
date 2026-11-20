"""阶段四 B-11（后半）：Fernet 解密失败必须有告警日志，但保留明文回退（功能一致）。

存量 v0.1.0 明文数据靠 InvalidToken 回退兼容，不能因加告警而破坏；APP_SECRET
变更/密文损坏时此前完全静默会把密文当明文用，现在必须有 warning 可排查。
"""
import logging

from app import security


def test_plaintext_fallback_preserved(caplog):
    """明文回退行为不变：解密失败返回原值（不抛异常、不改写）。"""
    plaintext = "https://www.guangyapan.com/s/legacy_plain_token"
    with caplog.at_level(logging.WARNING, logger="app.security"):
        out = security.decrypt_secret(plaintext)
    assert out == plaintext


def test_invalid_token_emits_warning_without_leaking_value(caplog):
    """InvalidToken 触发 warning，且日志不含凭证内容（只记长度）。"""
    # 用合法 Fernet 密文结构但错误密钥制造 InvalidToken
    value = security.encrypt_secret("secret-value-abc")
    with caplog.at_level(logging.WARNING, logger="app.security"):
        # 换一个错误密文（同样 is-ascii 但非本密钥签发）→ InvalidToken
        bad = value[:-4] + "AAAA"
        out = security.decrypt_secret(bad)
    warnings = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
    assert any("解密失败" in w for w in warnings), "解密失败必须有 warning 告警"
    assert "secret-value-abc" not in " ".join(warnings), "日志不得泄露凭证内容"
