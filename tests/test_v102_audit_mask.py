"""阶段四 B-11：审计日志脱敏。

异常 str(exc) 可能带 token / password / 签名 URL 片段。验证 add_audit 落库前
统一脱敏敏感 kv 与 Bearer token，同时保留非敏感诊断上下文。
"""
from app import main
from app.db import SessionLocal, migrate
from app.models import AuditLog

migrate()


def _last_detail():
    with SessionLocal() as db:
        row = db.query(AuditLog).order_by(AuditLog.id.desc()).first()
        return row.detail if row else ""


def test_token_and_password_masked():
    with SessionLocal() as db:
        main.add_audit(db, "test_mask", detail="失败 token=abc123XYZ password=hunter2 Bearer eyJabc.def")
        db.commit()
    d = _last_detail()
    assert "abc123XYZ" not in d
    assert "hunter2" not in d
    assert "eyJabc" not in d
    assert "***" in d
    assert "失败" in d  # 非敏感上下文保留


def test_authorized_kv_variants_masked():
    with SessionLocal() as db:
        main.add_audit(db, "test_mask", detail="access_token: S47access refresh-token=RR99 api_key=AK1")
        db.commit()
    d = _last_detail()
    assert "S47access" not in d and "RR99" not in d and "AK1" not in d


def test_normal_detail_unchanged():
    with SessionLocal() as db:
        main.add_audit(db, "test_mask", detail="普通操作成功 fileId=12345")
        db.commit()
    assert _last_detail() == "普通操作成功 fileId=12345"
