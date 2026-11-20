"""v0.9.19：用户投稿 → 光鸭原生转存（审核态 / 转存态分离）。

替代 v0.9.18 的 contribution_dispatch（TGtoDrive 中转群 / OpenAPI 分发通道）：
  - 不再经过 TGtoDrive，不再 Bot→Bot 私聊（USER_BOT_TO_BOT_DISABLED 根因）。
  - 审核通过只把投稿 status 置 approved 并 commit；转存成/败只写本模块任务表，
    绝不回写审核态。
  - 一条投稿至多一条 ContributionRestore（contribution_id 唯一），
    重复点通过不新建、success 不自动重跑、失败可手动重试。
  - 光鸭真实接口契约见 docs/光鸭接口取证.md 与 GuangYaWebFiles.restore_share_to_dir。
  - 转存后必须列目标目录按「名称 + 大小」核对；只凭接口返回成功不标记 success。
"""
from __future__ import annotations

import logging
import re
from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from . import runtime_config
from .contribution_links import is_guangya_share_url
from .guangya import GuangYaWebFiles, WebFileError
from .models import ContributionRestore, UserContribution, utcnow
from .utils import as_utc

RESTORE_STATUSES = ("pending", "restoring", "success", "failed", "retry_wait")
logger = logging.getLogger(__name__)
# 阶段五 C-02：restoring 是「await 前已 commit」的中间态。进程被 SIGKILL /
# 容器强停后该行永久悬挂，后台 pick_pending 只扫 pending/retry_wait 永远捞不到。
# 转存正常几秒完成，30 分钟阈值足够保守（与镜像僵尸自愈同量级），不会误伤在跑任务。
RESTORING_STALE_MINUTES = 30


# ---------------- 配置 ----------------
def enabled(db: Session) -> bool:
    return (runtime_config.get(db, "contribution_restore_enabled") or "").strip().lower() == "true"


def target_dir(db: Session) -> str:
    return (runtime_config.get(db, "contribution_restore_target_dir_id") or "").strip()


def config_error(db: Session) -> str:
    if not enabled(db):
        return "投稿转存未启用；请在「投稿转存」模块打开开关"
    if not target_dir(db):
        return "未配置投稿转存目标目录 ID；请填写光鸭目录 fileId"
    return ""


def status_label(status: str) -> str:
    return {"pending": "待转存", "restoring": "转存中", "success": "已转存",
            "failed": "转存失败", "retry_wait": "待重试"}.get(status or "", status or "未转存")


# ---------------- 任务建立（幂等） ----------------
def ensure_restore(db: Session, contribution_id: int, *, force: bool = False) -> tuple[ContributionRestore, bool]:
    """幂等建立/取回一条转存任务。返回 (任务, 是否新建)。

    force=True（管理员手动重试）时把 failed/retry_wait 重置为 pending；
    success 任务由调用方显式决定是否重跑，默认不重跑。
    """
    row = db.scalar(select(ContributionRestore).where(
        ContributionRestore.contribution_id == contribution_id).limit(1))
    if row is not None:
        if force and row.status != "success":
            row.status, row.error, row.updated_at = "retry_wait", "", utcnow()
            db.flush()
        return row, False
    row = ContributionRestore(
        contribution_id=contribution_id, status="pending", attempts=0,
        error="", last_response="", verify_note="", created_at=utcnow())
    db.add(row)
    db.flush()  # autoflush=False：写完即取主键必须显式 flush
    return row, True


def restore_for(db: Session, contribution_id: int) -> ContributionRestore | None:
    return db.scalar(select(ContributionRestore).where(
        ContributionRestore.contribution_id == contribution_id).limit(1))


def restore_map_for(db: Session, contribution_ids: list[int]) -> dict[int, ContributionRestore]:
    """阶段三 A-01：批量取转存任务，消除投稿列表的 N+1 查询。"""
    if not contribution_ids:
        return {}
    rows = db.scalars(select(ContributionRestore).where(
        ContributionRestore.contribution_id.in_(contribution_ids))).all()
    return {r.contribution_id: r for r in rows}


def reap_stale_restoring(db: Session, *, stale_minutes: int | None = None) -> dict:
    """C-02：把崩溃后永久悬挂的 restoring 任务重置为 retry_wait，供后台续跑。

    只在持有 _restore_lock 的后台 job 调用，避免与正在 await 的真实转存并发。
    时间比较统一走 as_utc()（SQLite 读回 naive datetime，不能直接与 aware 比）。
    """
    minutes = RESTORING_STALE_MINUTES if stale_minutes is None else stale_minutes
    cutoff = utcnow() - timedelta(minutes=max(1, int(minutes)))
    rows = list(db.scalars(select(ContributionRestore).where(
        ContributionRestore.status == "restoring")).all())
    reaped = 0
    for r in rows:
        stamp = as_utc(r.updated_at) or as_utc(r.created_at)
        if stamp is not None and stamp < cutoff:
            r.status = "retry_wait"
            r.error = (f"后台自愈：转存任务超过 {minutes} 分钟仍停在 restoring，"
                       "判定进程中断残留，重置为待重试")[ :300]
            r.updated_at = utcnow()
            reaped += 1
    if reaped:
        db.commit()
        logger.warning("[contribution-restore] 僵尸 restoring 自愈：重置为 retry_wait %s 条（阈值 %s 分钟）",
                       reaped, minutes)
    return {"scanned": len(rows), "reaped": reaped}


def pick_pending(db: Session, limit: int = 20) -> list[ContributionRestore]:
    """后台 job 扫描未完成任务（pending / retry_wait），重启后可续处理。

    C-02：扫描前先把超时的 restoring 残留重置为 retry_wait，否则 SIGKILL 后
    停在 restoring 的任务永远不会被本函数捞到（永久悬挂）。
    """
    reap_stale_restoring(db)
    return list(db.scalars(select(ContributionRestore).where(
        ContributionRestore.status.in_(("pending", "retry_wait"))).order_by(
        ContributionRestore.id).limit(limit)).all())


# ---------------- 响应脱敏 / 核对落库 ----------------
def _safe_raw(value, token: str = "") -> str:
    s = str(value or "")[:1000]
    if token:
        s = s.replace(token, "***")
    s = re.sub(r'"accessToken"\s*:\s*"[^"]*"', '"accessToken":"***"', s)
    s = re.sub(r"(Bearer\s+)[A-Za-z0-9._\-]+", r"\1***", s, flags=re.I)
    return s


async def list_dir_all(gw: GuangYaWebFiles, parent_id: str, page_size: int = 100,
                       max_items: int = 500) -> list[dict]:
    out: list[dict] = []
    for page in range(0, max_items // page_size + 1):
        data = await gw.list_files(parent_id=parent_id, page=page, page_size=page_size)
        batch = data.get("list") or []
        for it in batch:
            out.append({
                "name": str(it.get("fileName") or ""),
                "size": int(it.get("fileSize") or 0),
                "is_dir": it.get("isDir") in (True, 1, "1"),
            })
        if len(out) >= max_items:
            break
        total = int(data.get("total") or 0)
        if not batch or (total and len(out) >= total):
            break
    return out


def _match_key(item: dict) -> tuple[str, int]:
    return (item.get("name", "").strip(), int(item.get("size") or 0))


async def verify_in_target(gw: GuangYaWebFiles, share_url: str,
                           target_dir_id: str) -> tuple[bool, str, str]:
    """核对分享内容是否已落入目标目录。返回 (all_found, note, raw_for_log)。

    先列分享根文件拿「名称 + 大小」，再分页列目标目录比对；目录项只比名称。
    任何异常（链接失效 / 登录态失效）都作为「未核对到」返回，由上层决定是否转存，
    异常信息本身保留在 note 中，绝不静默吞掉。
    """
    from .contribution_links import parse_share_reference

    sid, code = parse_share_reference(share_url)
    if not sid:
        return False, "分享链接无法解析，跳过核对", ""
    try:
        token = await gw.share_access_token(sid, code)
        shared = await gw.share_root_files(token)
        target = await list_dir_all(gw, target_dir_id)
    except WebFileError as exc:
        return False, f"核对目标目录失败：{exc}", _safe_raw(str(exc))
    except Exception as exc:
        return False, f"核对目标目录异常：{type(exc).__name__}: {str(exc)[:120]}", ""

    target_keys = {(_k[0], _k[1]) for _k in (_match_key(x) for x in target)}
    target_names = {x["name"].strip() for x in target if x.get("is_dir")}
    missing = []
    for f in shared:
        key = _match_key(f)
        if f.get("is_dir"):
            if key[0] not in target_names:
                missing.append(key[0])
        elif key not in target_keys:
            missing.append(f"{key[0]}（{key[1]} 字节）")
    total = len(shared)
    if total and not missing:
        return True, f"目标目录已存在全部 {total} 项（按名称+大小核对）", ""
    note = f"目标目录缺少 {len(missing)}/{total} 项：{'、'.join(missing[:5])}"[:500]
    return False, note, ""


# ---------------- 执行 ----------------
async def execute_restore(db: Session, row: ContributionRestore) -> tuple[bool, str]:
    """执行一条转存任务，只更新任务表，绝不改投稿审核状态。"""
    c = db.get(UserContribution, row.contribution_id)
    if not c:
        row.status, row.error, row.updated_at = "failed", "投稿记录已不存在", utcnow()
        db.flush()
        return False, "投稿记录已不存在"

    cfg_err = config_error(db)
    if cfg_err:
        row.status, row.error, row.updated_at = "failed", cfg_err, utcnow()
        db.flush()
        return False, cfg_err
    share_url = (c.share_url or "").strip()
    if not is_guangya_share_url(share_url):
        msg = "投稿链接非光鸭 /s/ 分享链接，拒绝转存"
        row.status, row.error, row.updated_at = "failed", msg, utcnow()
        db.flush()
        return False, msg

    row.status = "restoring"
    row.attempts = int(row.attempts or 0) + 1
    row.updated_at = utcnow()
    # 阶段四 A-06：await 前先 commit 落盘，避免跨网络 await 持 SQLite 写事务
    # （同仓库 publisher/scanner 已有此范式）。attempts 只在此处累加一次。
    db.commit()

    target = target_dir(db)
    gw = GuangYaWebFiles(db)
    raw = ""
    try:
        # 幂等核对：若上一次其实已转存成功（如响应丢失后重试），不重复提交。
        ok, note, raw = await verify_in_target(gw, share_url, target)
        if ok:
            row.status, row.error, row.verify_note = "success", "", note
            row.restored_at = utcnow()
            row.last_response = _safe_raw(raw)[:1000]
            row.updated_at = utcnow()
            db.flush()
            return True, note

        result = await gw.restore_share_to_dir(share_url, target)
        raw = _safe_raw(result.get("raw"))[:1000]
        # 提交成功 ≠ 落库成功：再列目标目录按名称+大小核对
        ok2, note2, _ = await verify_in_target(gw, share_url, target)
        if ok2:
            row.status, row.error, row.verify_note = "success", "", note2
            row.restored_at = utcnow()
        else:
            row.status = "failed"
            row.error = f"转存接口已提交，但目标目录核对未通过：{note2}"[:500]
            row.verify_note = note2
    except WebFileError as exc:
        row.status = "failed"
        if int(exc.code or 0) == 204:
            row.error = "SHARE_SELF_RESTORE：该分享由当前账号创建，不能自转存（retryable=false，重试无用）"
        elif int(exc.code or 0) in (200, 201, 202):
            row.error = f"分享已失效（code={exc.code}），请让用户重新生成分享链接后再投稿"
        elif int(exc.code or 0) in (203, 209):
            row.error = f"提取码错误（code={exc.code}），请核对分享链接的 ?code= 参数"
        else:
            row.error = f"光鸭转存失败：{exc}"[:500]
        row.last_response = _safe_raw(str(exc))[:1000]
    except Exception as exc:
        row.status = "failed"
        row.error = f"转存执行异常：{type(exc).__name__}: {str(exc)[:200]}"[:500]
    row.last_response = (row.last_response or raw)[:1000]
    row.updated_at = utcnow()
    db.flush()
    return row.status == "success", row.error or row.verify_note or "转存完成"


async def run_restore_for_contribution(db: Session, contribution_id: int, *,
                                       force: bool = False) -> tuple[bool, str, ContributionRestore | None]:
    """审核通过后调用：幂等建任务并即时执行一次（失败留任务供重试）。"""
    row, _created = ensure_restore(db, contribution_id, force=force)
    if row.status == "success" and not force:
        return True, "已转存成功，跳过自动重复转存", row
    ok, info = await execute_restore(db, row)
    return ok, info, row


async def retry_restore(db: Session, contribution_id: int) -> tuple[bool, str, ContributionRestore | None]:
    """管理员手动重试：success 不自动重跑（需 force 显式重来）；其余失败可重试。"""
    row = restore_for(db, contribution_id)
    if not row:
        return False, "没有可重试的转存任务", None
    if row.status == "success":
        return False, "该投稿已转存成功，不自动重复转存", row
    row.status, row.error, row.updated_at = "retry_wait", "", utcnow()
    db.flush()
    ok, info = await execute_restore(db, row)
    return ok, info, row
