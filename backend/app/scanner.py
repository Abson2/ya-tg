import json
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from .db import SessionLocal, json_dumps
from .guangya import GuangYaOpenAPI, GuangYaWebFiles
from .models import AuditLog, AuthToken, Resource, ScanCursor, ScanTask, utcnow


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
                     path_hint: str = "", dir_chain: list[str] | None = None) -> tuple[Resource, bool]:
    """返回 (资源, 是否新增)。同一 fileId 只保留一行；utime/size 变化记入 revision。

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
    if created:
        row = Resource(file_id=fid, **values)
        db.add(row)
    else:
        changed = row.utime != utime or row.size != size
        for k, v in values.items():
            setattr(row, k, v)
        if changed:
            row.revision += 1
            db.add(AuditLog(action="resource_updated", target_type="resource", target_id=str(row.id),
                            detail=json_dumps({"file_id": fid, "utime": utime, "size": size})))
    db.flush()

    return row, created


async def run_scan(root_id: str = "", resume: bool = True, db: Session | None = None) -> dict:
    own_db = db is None
    db = db or SessionLocal()
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

        # v0.6.5：扫描只入库，不再读取规则入队；推送由识别成功触发
        while stack:
            parent = stack.pop()
            if parent in visited:
                continue
            visited.append(parent)
            dirs += 1
            while True:
                result = await api.list_files(parent, page, 100)
                items = result.get("list") or []
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
                    _upsert_resource(db, item, parent, dir_paths.get(parent, ""),
                                     dir_chains.get(parent, []))
                    files += 1
                db.commit()
                total = int(result.get("total", 0) or 0)
                page += 1
                if not items or page * 100 >= total:
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
        db.add(AuditLog(action="scan_done", target_type="scan", target_id=str(c.id),
                        detail=json_dumps({"files": files, "directories": dirs, "resumed": can_resume})))
        db.commit()
        return {"files": files, "directories": dirs, "resumed": can_resume}
    except Exception as exc:
        db.rollback()
        try:
            c = _cursor(db)
            _save_cursor(c, json.loads(c.visited_json or "[]"), json.loads(c.stack_json or "[]"),
                         int(c.next_page or 0), int(c.files_count or 0), int(c.dirs_count or 0),
                         status="failed", error=str(exc), dir_paths=dir_paths, dir_chains=dir_chains)
            task = db.scalar(select(ScanTask).limit(1))
            if task:
                task.last_run = utcnow()
                task.last_status = "failed"
                task.last_message = str(exc)[:1500]
            db.commit()
        except Exception:
            db.rollback()
        raise
    finally:
        if own_db:
            db.close()
