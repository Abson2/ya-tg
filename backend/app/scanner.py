import hashlib
import json
import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from .db import SessionLocal, json_dumps
from .guangya import GuangYaOpenAPI, GuangYaWebFiles
from .models import (AuditLog, AuthToken, DistributionEvent, Resource, ResourceScanBatch,
                     ResourceVersion, ScanCursor, ScanTask, utcnow)
from .utils import work_group_key

logger = logging.getLogger(__name__)


def _cursor(db: Session) -> ScanCursor:
    c = db.get(ScanCursor, 1)
    if not c:
        c = ScanCursor(id=1)
        db.add(c)
        db.flush()
    return c


def _save_cursor(c: ScanCursor, visited: list[str], stack: list[str], page: int,
                 files: int, dirs: int, status: str = "running", error: str = "",
                 dir_paths: dict | None = None, dir_chains: dict | None = None) -> None:
    c.visited_json = json_dumps(visited)
    c.stack_json = json_dumps(stack)
    if dir_paths is not None:
        c.dir_paths_json = json_dumps(dir_paths)
    if dir_chains is not None:
        c.dir_chains_json = json_dumps(dir_chains)
    c.next_page = page
    c.files_count = files
    c.dirs_count = dirs
    c.status = status
    c.error = error[:1500]
    c.updated_at = utcnow()
    if status == "running" and not c.started_at:
        c.started_at = utcnow()
    if status in ("done", "failed"):
        c.finished_at = utcnow()


def _dir_name(item: dict) -> str:
    """网页端与官方接口的目录名可能落在不同字段，统一兼容。"""
    for key in ("fileName", "name", "file_name", "folderName"):
        value = item.get(key)
        if value:
            return str(value).strip()
    return ""


def _upsert_resource(db: Session, item: dict, parent: str,
                     path_hint: str = "", dir_chain: list[str] | None = None
                     ) -> tuple[Resource, bool, dict | None]:
    """返回 (资源, 是否新增, 变化信息)。同一 fileId 只保留一行；utime/size 变化记入 revision。

    v0.8.0：内容变化时返回指纹变更信息，交给 _record_change_events 按作品合并成
    一次「内容更新」事件。这里**不重建分享、不入推送队列**——已分享影视内容变化
    只同步文件，分享链接保持不变。

    v0.6.1：path_hint 保存「作品目录/Season 1」这类目录显示名链。剧集单集文件名
    往往只是英文译名（The.Silent.Storm.S01E04），真正的作品名在父目录（谷雨 (2026)），
    识别必须拿得到这层线索，否则会误命中同名英文作品。
    v0.6.3：dir_chain 保存从直接父目录到根的 fileId 链，用于定位剧集目录层级。
    v0.6.5：扫描只入库，不再按规则入队；推送改由「识别成功且有 TMDB 海报」触发。
    """
    fid = str(item.get("fileId", ""))
    name = item.get("fileName", "")
    ext = (item.get("ext") or ("." + name.rsplit(".", 1)[-1] if "." in name else "")).lower()
    utime = int(item.get("utime", 0) or 0)
    size = int(item.get("fileSize", 0) or 0)
    row = db.scalar(select(Resource).where(Resource.file_id == fid))
    raw_parent = str(item.get("parentId") or "")
    if not raw_parent or raw_parent == "*":
        raw_parent = parent
    values = dict(
        parent_id=raw_parent,
        path_hint=path_hint,
        dir_chain_json=json_dumps(dir_chain or []),
        name=name, size=size,
        file_type=int(item.get("fileType", 0) or 0),
        res_type=1, ext=ext,
        ctime=int(item.get("ctime", 0) or 0),
        utime=utime,
        thumbnail=item.get("thumbnail") or "",
        scanned_at=utcnow(),
    )
    created = row is None
    change = None
    if created:
        row = Resource(file_id=fid, **values)
        db.add(row)
    else:
        old_fp = f"{int(row.size or 0)}:{int(row.utime or 0)}"
        new_fp = f"{size}:{utime}"
        for k, v in values.items():
            setattr(row, k, v)
        if old_fp != new_fp:
            row.revision += 1
            db.add(AuditLog(action="resource_updated", target_type="resource", target_id=str(row.id),
                            detail=json_dumps({"file_id": fid, "utime": utime, "size": size})))
            change = {"resource_id": int(row.id), "file_id": fid,
                      "work_key": "", "old": old_fp, "new": new_fp,
                      "size": size, "utime": utime}
    db.flush()
    if change:
        # 作品键在 flush 后计算：取更新后的目录链，未识别单集也能正确归到整部剧
        change["work_key"] = work_group_key(row)
    return row, created, change


def _record_change_events(db: Session, changes: list[dict]) -> int:
    """按作品合并同一批文件变化，生成唯一一条「内容更新」分发事件。

    同一 (work_key, 变化指纹) 只落一次：一批 36 个文件同时被替换也只触发一次推送，
    而不是逐集 36 条。share_reused=True 表示不重建分享（需求3-5）。
    """
    if not changes:
        return 0
    grouped: dict[str, list[dict]] = {}
    for c in changes:
        db.add(ResourceVersion(resource_id=c["resource_id"], file_id=c["file_id"],
                               work_key=c["work_key"], fingerprint=c["new"],
                               old_fingerprint=c["old"], size=c["size"], utime=c["utime"],
                               change_type="updated"))
        grouped.setdefault(c["work_key"], []).append(c)
    created = 0
    for wk, group in grouped.items():
        digest = hashlib.md5("|".join(sorted(
            f"{c['file_id']}:{c['new']}" for c in group)).encode("utf-8")).hexdigest()[:32]
        exists = db.scalar(select(DistributionEvent).where(
            DistributionEvent.work_key == wk, DistributionEvent.fingerprint == digest))
        if exists:
            continue
        db.add(DistributionEvent(work_key=wk, event_type="updated", fingerprint=digest,
                                 changed_files=len(group), share_reused=True))
        # autoflush=False：必须显式 flush，否则同一 Session 内后续查重看不到刚落的事件
        db.flush()
        created += 1
    return created


async def run_scan(root_id: str = "", resume: bool = True, db: Session | None = None) -> dict:
    """返回 {"files","directories","resumed","seen_file_ids"}。

    v0.7.1：额外返回本轮真实见到的 fileId 集合，供「重新扫描库」做对账——
    B 库已不存在（本轮没扫到）的影视要移除看板并取消分享（需求3-1）。
    断点续扫时 seen 不完整，调用方必须跳过对账，避免误删。
    """
    own_db = db is None
    db = db or SessionLocal()
    seen_file_ids: set[str] = set()
    changes: list[dict] = []
    batch: ResourceScanBatch | None = None
    batch_id = 0
    # V2.0 第 12.2 节「扫描批次保护」：只有本轮全部分页读取成功，才允许清理缺失资源。
    # 接口在 total 未读完时提前返回空页，属于不完整批次，绝不能据此对账删数据。
    scan_complete = True
    # 短信登录用户走网页端文件接口（无需开放平台 client_id，真正零配置）；
    # 仅当使用官方 Device/PKCE 授权时才走官方 OpenAPI 业务接口。
    t = db.get(AuthToken, 1)
    if t and t.flow in ("sms", "tgto"):
        api = GuangYaWebFiles(db)
    else:
        api = GuangYaOpenAPI(db)
    # 目录 fileId -> 从根到该目录的显示名路径；根目录在网页端是 "*"，官方是 ""
    dir_paths: dict[str, str] = {"": "", "*": ""}
    # v0.6.3：目录 fileId -> 从直接父目录往上到根的 fileId 链（用于剧集目录级分享）
    dir_chains: dict[str, list[str]] = {"": [], "*": []}
    try:
        c = _cursor(db)
        can_resume = resume and c.status == "running" and (not root_id or c.root_id == root_id)
        if can_resume:
            visited = list(json.loads(c.visited_json or "[]"))
            stack = list(json.loads(c.stack_json or "[]"))
            page = int(c.next_page or 0)
            files, dirs = int(c.files_count or 0), int(c.dirs_count or 0)
            try:
                saved = json.loads(c.dir_paths_json or "{}")
                if isinstance(saved, dict):
                    dir_paths.update({str(k): str(v) for k, v in saved.items()})
            except Exception:
                pass
            try:
                saved_chains = json.loads(c.dir_chains_json or "{}")
                if isinstance(saved_chains, dict):
                    dir_chains.update({str(k): [str(x) for x in v]
                                       for k, v in saved_chains.items() if isinstance(v, list)})
            except Exception:
                pass
        else:
            visited, stack, page, files, dirs = [], [str(root_id or "")], 0, 0, 0
            c.root_id = str(root_id or "")
            c.dir_paths_json = json_dumps(dir_paths)
            c.dir_chains_json = json_dumps(dir_chains)
        c.status = "running"
        db.commit()
        # v0.8.0：每轮扫描落一条批次记录，作为「是否允许对账删除」的唯一权威信号
        batch = ResourceScanBatch(root_id=str(root_id or ""), status="running",
                                  resumed=bool(can_resume))
        db.add(batch)
        db.flush()
        batch_id = int(batch.id)
        # v0.8.8：必须在这里就提交。下面第一个 `await api.list_files()` 是网络请求，
        # 若让这个写事务跨 await 持有，其它后台 job 只要写库就会撞
        # `sqlite3.OperationalError: database is locked`（用户实测的失败重试循环）。
        db.commit()

        # v0.6.5：扫描只入库，不再读取规则入队；推送由识别成功触发
        while stack:
            parent = stack.pop()
            if parent in visited:
                continue
            visited.append(parent)
            dirs += 1
            fetched = 0  # 本目录实际读到的条目数，用于与 total 对账
            while True:
                result = await api.list_files(parent, page, 100)
                items = result.get("list") or []
                fetched += len(items)
                for item in items:
                    fid = str(item.get("fileId", ""))
                    if not fid:
                        continue
                    if int(item.get("resType", 1) or 1) == 2:
                        dir_name = _dir_name(item)
                        if dir_name:
                            base = dir_paths.get(parent, "")
                            dir_paths[fid] = (base + "/" + dir_name).strip("/") if base else dir_name
                        # v0.6.3：目录 fileId -> [自身, 父, 祖父, ...] 到根的链
                        parent_chain = dir_chains.get(parent, [])
                        dir_chains[fid] = [fid] + list(parent_chain)
                        if fid not in visited:
                            stack.append(fid)
                        continue
                    _, _, changed = _upsert_resource(db, item, parent, dir_paths.get(parent, ""),
                                                     dir_chains.get(parent, []))
                    if changed:
                        changes.append(changed)
                    seen_file_ids.add(fid)
                    files += 1
                db.commit()
                total = int(result.get("total", 0) or 0)
                page += 1
                if not items or page * 100 >= total:
                    # V2.0 12.2：只要实际抓取条数少于 total，就是不完整批次。
                    # 覆盖「total=150、第 2 页提前返回空页」这类 page*100 >= total
                    # 但数据其实缺失的情况，绝不能据此对账删资源。
                    if total and fetched < total:
                        scan_complete = False
                    page = 0
                    break
                _save_cursor(c, visited, stack, page, files, dirs, dir_paths=dir_paths, dir_chains=dir_chains)
                db.commit()
            _save_cursor(c, visited, stack, 0, files, dirs, dir_paths=dir_paths, dir_chains=dir_chains)
            db.commit()

        _save_cursor(c, visited, stack, 0, files, dirs, status="done", dir_paths=dir_paths, dir_chains=dir_chains)
        task = db.scalar(select(ScanTask).limit(1))
        if task:
            task.last_run = utcnow()
            task.last_status = "success"
            task.last_message = f"扫描完成：{files} 个文件，遍历 {dirs} 个目录"
        # v0.8.0：同批文件变化按作品合并成一次「内容更新」事件，不逐集触发推送
        events = _record_change_events(db, changes)
        seen = sorted(seen_file_ids)
        if batch:
            batch.status = "success"
            batch.files, batch.directories = files, dirs
            batch.seen_count = len(seen)
            batch.complete = bool(scan_complete)
            batch.reconcile_allowed = bool(scan_complete and not can_resume and seen)
            batch.seen_json = json_dumps(seen)
            batch.finished_at = utcnow()
        db.add(AuditLog(action="scan_done", target_type="scan", target_id=str(c.id),
                        detail=json_dumps({"files": files, "directories": dirs,
                                           "resumed": can_resume, "complete": scan_complete,
                                           "change_events": events})))
        db.commit()
        return {"files": files, "directories": dirs, "resumed": can_resume,
                "complete": scan_complete, "seen_file_ids": seen,
                "batch_id": batch_id, "change_events": events}
    except Exception as exc:
        db.rollback()
        # v0.8.8：写冲突必须给可操作提示，否则用户只看到一串 SQL 与参数，
        # 在「失败 → 重试 → 又失败」里空转（真实环境反馈）。
        raw = str(exc)
        if "database is locked" in raw.lower():
            detail = ("数据库写冲突（database is locked）：请确认只运行一个容器实例、"
                      "没有两个 compose 项目共用同一个 ./db 目录；"
                      "v0.8.8 已启用 WAL 与 30 秒写等待，稍后重试即可。原始错误：" + raw)
            logger.warning("[scan] %s", detail)
        else:
            detail = raw
        try:
            if batch_id:
                b = db.get(ResourceScanBatch, batch_id)
                if b:
                    b.status, b.error, b.finished_at = "failed", detail[:1000], utcnow()
                    db.commit()
            c = _cursor(db)
            _save_cursor(c, json.loads(c.visited_json or "[]"), json.loads(c.stack_json or "[]"),
                         int(c.next_page or 0), int(c.files_count or 0), int(c.dirs_count or 0),
                         status="failed", error=detail, dir_paths=dir_paths, dir_chains=dir_chains)
            task = db.scalar(select(ScanTask).limit(1))
            if task:
                task.last_run = utcnow()
                task.last_status = "failed"
                task.last_message = detail[:1500]
            db.commit()
        except Exception:
            db.rollback()
        raise
    finally:
        if own_db:
            db.close()
