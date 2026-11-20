"""v0.9.18：投稿分发子系统（审核态 / 分发态彻底解耦）。

设计红线：
  - 审核通过只负责：把投稿 status 置 approved + 幂等建立一条分发任务；
    分发成/败绝不回写投稿审核状态。
  - 一条投稿至多一条 ContributionDispatch（contribution_id 唯一），
    重复点「通过」不新建、已 submitted 不自动重跑、失败可手动重试。
  - 不再使用「交互 Bot → TGtoDrive Bot 私聊」：Telegram 直接拒绝
    USER_BOT_TO_BOT_DISABLED。改为两条通道：
      relay   （默认）：交互 Bot 发消息到「专用中转群」，群成员是 TGtoDrive
               已登录的个人账号，由其万能转发规则把链接交给 TGtoDrive Bot；
               消息只含唯一投稿编号 + 资源名 + 分享链接 + 提取码，不含投稿人隐私。
      openapi ：调 TGtoDrive OpenAPI POST /api/media/resources/transfer 提交
               share_url（字段以实际规范为准；该接口在导出的 OpenAPI 中标注
               inferred，未真机验证，故默认不走，配置开关打开才启用）。
  - 光鸭分享链接强校验：非 guangyapan.com 分享链接不进入投稿后台。

通道细节 / 返回结构不可猜测的部分一律以 runtime_config 配置 + 真实响应为准，
失败保留原始响应（截断脱敏）到 last_response，绝不假装成功。
"""
from __future__ import annotations

import re
from datetime import timedelta

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from . import bot_listener, runtime_config, tgto
from .contribution_links import extract_guangya_share_url, is_guangya_share_url, share_code
from .models import ContributionDispatch, UserContribution, utcnow

DISPATCH_MODES = ("relay", "openapi", "off")
DISPATCH_STATUSES = ("pending", "sending", "submitted", "failed", "retry_wait")


# ---------------- 配置 ----------------
def dispatch_mode(db: Session) -> str:
    mode = (runtime_config.get(db, "contribution_dispatch_mode") or "relay").strip().lower()
    return mode if mode in DISPATCH_MODES else "relay"


def _dispatch_ref(contribution_id: int) -> str:
    """中转消息携带的唯一投稿编号，便于追踪（不含投稿人隐私）。"""
    return f"YATG-C-{int(contribution_id)}"


# ---------------- 任务建立（幂等） ----------------
def ensure_dispatch(db: Session, contribution_id: int, *, force: bool = False) -> tuple[ContributionDispatch, bool]:
    """幂等建立/取回一条分发任务。返回 (任务, 是否新建)。

    已存在时：
      - force=False：原样返回（重复点通过不新建、submitted 不重跑）；
      - force=True（管理员手动重试）：重置为 pending 以便后台/即时执行。
    """
    row = db.scalar(select(ContributionDispatch).where(
        ContributionDispatch.contribution_id == contribution_id).limit(1))
    if row is not None:
        if force:
            row.status = "retry_wait"
            row.error = ""
            row.updated_at = utcnow()
            db.flush()
        return row, False
    row = ContributionDispatch(
        contribution_id=contribution_id, status="pending",
        mode=dispatch_mode(db), attempts=0, ref=_dispatch_ref(contribution_id),
        error="", last_response="", created_at=utcnow())
    db.add(row)
    db.flush()  # autoflush=False：写完即取主键必须显式 flush
    return row, True


def dispatch_for(db: Session, contribution_id: int) -> ContributionDispatch | None:
    return db.scalar(select(ContributionDispatch).where(
        ContributionDispatch.contribution_id == contribution_id).limit(1))


# ---------------- 分发执行 ----------------
def _relay_template(db: Session) -> str:
    tpl = (runtime_config.get(db, "contribution_relay_template") or "").strip()
    if not tpl:
        tpl = ("【Ya-TG 审核通过】\n"
               "投稿编号：{ref}\n"
               "资源名称：{title}\n"
               "光鸭分享链接：{share_url}\n"
               "提取码：{code}\n"
               "来源：用户投稿\n"
               "审核状态：已通过")
    return tpl


async def _send_relay(db: Session, c: UserContribution, row: ContributionDispatch) -> tuple[bool, str, str]:
    """中转群通道：交互 Bot → 专用中转群（不是 Bot 私聊）。"""
    chat_id = (runtime_config.get(db, "contribution_relay_chat_id") or "").strip()
    if not chat_id:
        return False, "未配置投稿分发中转群 chat_id", ""
    token = bot_listener.effective_token(db)
    if not token:
        return False, "未配置交互 Bot Token，无法发送到中转群", ""
    text = _relay_template(db).format(
        ref=row.ref or _dispatch_ref(c.id), title=(c.title or "")[:200],
        share_url=c.share_url or "", code=share_code(c.share_url))
    data = await bot_listener._api(db, token, "sendMessage", {
        "chat_id": chat_id, "text": text[:4000], "disable_web_page_preview": False})
    if isinstance(data, dict) and not data.get("ok"):
        desc = str(data.get("description") or "")[:200]
        hint = ""
        if "BOT" in desc.upper() or "chat not found" in desc.lower():
            hint = "；中转群 chat_id 必须是群，且需把 Bot 加进群；不要填 TGtoDrive Bot 的私聊"
        return False, f"中转群发送被 Telegram 拒绝：{desc}{hint}", json_safe(data)
    return True, "已发送到中转群（等待 TGtoDrive 个人账号万能转发处理）", json_safe(data)


async def _send_openapi(db: Session, c: UserContribution, row: ContributionDispatch) -> tuple[bool, str, str, str]:
    """OpenAPI 通道：POST {base}/api/media/resources/transfer。

    字段严格取自本地导出的 tgto-openapi.json（inferred，未真机验证）：
      share_url（光鸭分享链接）、source（资源来源标识）、provider（转存目标）。
    不传 candidate_item_id / slug（那是「已检索媒体资源」专用，投稿场景没有）。
    返回 (ok, info, task_id, raw_response)。
    """
    base = (runtime_config.get(db, "tgto_base_url") or "").rstrip("/")
    token = runtime_config.get(db, "tgto_api_key") or ""
    if not base:
        return False, "未配置 TGtoDrive 地址（tgto_base_url）", "", ""
    if not token:
        return False, "未配置 TGtoDrive OpenAPI Token（tgto_api_key）", "", ""
    share_url = (c.share_url or "").strip()
    if not is_guangya_share_url(share_url):
        return False, "投稿链接非光鸭分享链接，OpenAPI 不提交", "", ""
    payload = {"share_url": share_url, "source": "guangya"}
    try:
        async with httpx.AsyncClient(timeout=25) as client:
            r = await client.post(
                f"{base}/api/media/resources/transfer",
                headers=tgto._headers(token), json=payload)
        raw = r.text[:1000]
        ok = 200 <= r.status_code < 300
        task_id = ""
        try:
            body = r.json()
            if isinstance(body, dict):
                # 不猜字段名：只在常见键出现时记录，找不到就留空
                for k in ("task_id", "taskId", "id", "job_id"):
                    if body.get(k) not in (None, ""):
                        task_id = str(body[k])[:128]
                        break
                # 业务层 success=false 也算失败（HTTP 200 但业务拒绝）
                if body.get("success") is False or body.get("ok") is False:
                    ok = False
        except Exception:
            pass
        info = (f"OpenAPI 提交成功（HTTP {r.status_code}"
                + (f"，任务 {task_id}" if task_id else "") + "）") if ok \
            else f"OpenAPI 提交失败（HTTP {r.status_code}）"
        return ok, info, task_id, sanitize_raw(raw, token)
    except Exception as exc:
        return False, f"OpenAPI 请求异常：{type(exc).__name__}: {str(exc)[:160]}", "", ""


def sanitize_raw(raw: str, token: str = "") -> str:
    """原始响应截断 + 抹掉 Bearer token，避免凭证入日志/数据库。"""
    s = str(raw or "")[:1000]
    if token:
        s = s.replace(token, "***")
    s = re.sub(r"(Bearer\s+)[A-Za-z0-9._\-]+", r"\1***", s, flags=re.I)
    return s


def json_safe(data) -> str:
    try:
        return sanitize_raw(json_dumps(data))
    except Exception:
        return ""


def json_dumps(value) -> str:
    import json
    return json.dumps(value, ensure_ascii=False)


async def execute_dispatch(db: Session, row: ContributionDispatch) -> tuple[bool, str]:
    """执行一条分发任务，更新其状态；返回 (ok, info)。绝不改投稿审核状态。"""
    c = db.get(UserContribution, row.contribution_id)
    if not c:
        row.status, row.error, row.updated_at = "failed", "投稿记录已不存在", utcnow()
        db.flush()
        return False, "投稿记录已不存在"

    mode = row.mode or dispatch_mode(db)
    row.status = "sending"
    row.attempts = int(row.attempts or 0) + 1
    row.updated_at = utcnow()
    db.flush()

    raw = ""
    if mode == "openapi":
        ok, info, task_id, raw = await _send_openapi(db, c, row)
        if task_id:
            row.ref = task_id
    elif mode == "relay":
        ok, info, raw = await _send_relay(db, c, row)
    else:  # off / 未知
        ok, info = False, f"分发通道 {mode!r} 不执行；请在配置里选 relay 或 openapi"

    if ok:
        row.status, row.error, row.dispatched_at = "submitted", "", utcnow()
    else:
        row.status, row.error = "failed", info[:500]
    row.last_response = raw[:1000]
    row.updated_at = utcnow()
    db.flush()
    return ok, info


async def run_dispatch_for_contribution(db: Session, contribution_id: int, *, force: bool = False) -> tuple[bool, str, ContributionDispatch | None]:
    """审核通过后调用：幂等建任务并立即执行一次（失败保留任务供重试）。"""
    row, _created = ensure_dispatch(db, contribution_id, force=force)
    if row.status == "submitted" and not force:
        return True, "已提交过，跳过自动重复分发", row
    ok, info = await execute_dispatch(db, row)
    return ok, info, row


async def retry_dispatch(db: Session, contribution_id: int) -> tuple[bool, str, ContributionDispatch | None]:
    """管理员手动重试：仅对失败/待重试任务生效，submitted 不自动重跑。"""
    row = dispatch_for(db, contribution_id)
    if not row:
        return False, "没有可重试的分发任务", None
    if row.status == "submitted":
        return False, "该投稿已提交成功，如需再次分发请先在配置里切换通道", row
    row.status = "retry_wait"
    row.error = ""
    db.flush()
    ok, info = await execute_dispatch(db, row)
    return ok, info, row


def pick_pending(db: Session, limit: int = 20) -> list[ContributionDispatch]:
    """后台 job 扫描未完成任务（pending / retry_wait），重启后可续处理。"""
    return list(db.scalars(select(ContributionDispatch).where(
        ContributionDispatch.status.in_(("pending", "retry_wait"))).order_by(
        ContributionDispatch.id).limit(limit)).all())


def status_label(status: str) -> str:
    return {"pending": "待发送", "sending": "发送中", "submitted": "已提交",
            "failed": "发送失败", "retry_wait": "待重试"}.get(status or "", status or "待发送")


def mode_label(mode: str) -> str:
    return {"relay": "中转群", "openapi": "OpenAPI", "off": "未启用"}.get(mode or "", mode or "中转群")
