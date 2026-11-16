import json
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from .db import SessionLocal, json_dumps
from .guangya import GuangYaOpenAPI
from .models import AuditLog, QueueItem, Resource, Rule, ScanCursor, ScanTask, utcnow


def _cursor(db: Session) -> ScanCursor:
    c = db.get(ScanCursor, 1)
    if not c:
        c = ScanCursor(id=1)
        db.add(c)
        db.flush()
    return c


def _save_cursor(c: ScanCursor, visited: list[str], stack: list[str], page: int,
                 files: int, dirs: int, status: str = "running", error: str = "") -> None:
    c.visited_json = json_dumps(visited)
    c.stack_json = json_dumps(stack)
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


def _upsert_resource(db: Session, item: dict, parent: str, rules: list[Rule]) -> tuple[Resource, bool]:
    """返回 (资源, 是否新增)。同一 fileId 只保留一行；utime/size 变化记入 revision。"""
    fid = str(item.get("fileId", ""))
    name = item.get("fileName", "")
    ext = (item.get("ext") or ("." + name.rsplit(".", 1)[-1] if "." in name else "")).lower()
    utime = int(item.get("utime", 0) or 0)
    size = int(item.get("fileSize", 0) or 0)
    row = db.scalar(select(Resource).where(Resource.file_id == fid))
    values = dict(
        parent_id=str(item.get("parentId", parent) or parent),
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

    for rule in rules:
        match_dir = not rule.directory_id or rule.directory_id == row.parent_id
        match_key = not rule.keyword or rule.keyword.lower() in row.name.lower()
        if not (match_dir and match_key):
            continue
        exists = db.scalar(select(QueueItem).where(
            QueueItem.resource_id == row.id, QueueItem.channel_id == rule.channel_id))
        if not exists:
            db.add(QueueItem(resource_id=row.id, channel_id=rule.channel_id,
                             status="pending" if rule.auto_publish else "review"))
    return row, created


async def run_scan(root_id: str = "", resume: bool = True, db: Session | None = None) -> dict:
    own_db = db is None
    db = db or SessionLocal()
    api = GuangYaOpenAPI(db)
    try:
        c = _cursor(db)
        can_resume = resume and c.status == "running" and (not root_id or c.root_id == root_id)
        if can_resume:
            visited = list(json.loads(c.visited_json or "[]"))
            stack = list(json.loads(c.stack_json or "[]"))
            page = int(c.next_page or 0)
            files, dirs = int(c.files_count or 0), int(c.dirs_count or 0)
        else:
            visited, stack, page, files, dirs = [], [str(root_id or "")], 0, 0, 0
            c.root_id = str(root_id or "")
        c.status = "running"
        db.commit()

        rules = list(db.scalars(select(Rule).where(Rule.enabled.is_(True))).all())
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
                        if fid not in visited:
                            stack.append(fid)
                        continue
                    _upsert_resource(db, item, parent, rules)
                    files += 1
                db.commit()
                total = int(result.get("total", 0) or 0)
                page += 1
                if not items or page * 100 >= total:
                    page = 0
                    break
                _save_cursor(c, visited, stack, page, files, dirs)
                db.commit()
            _save_cursor(c, visited, stack, 0, files, dirs)
            db.commit()

        _save_cursor(c, visited, stack, 0, files, dirs, status="done")
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
                         status="failed", error=str(exc))
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
