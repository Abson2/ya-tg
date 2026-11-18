"""v0.8.0：A 源头库 → B 镜像库 增量同步。

设计红线（来自《tg-gy 最终版项目开发需求文档》）：
  1. A 源头库**只读**，任何情况下不修改 A 的文件/目录。
  2. B 镜像库才进入扫描、资源库、分享与 Telegram 分发；A/B fileId 不同，
     必须通过 mirror_files 映射，禁止把 A 的 dir_chain / share_dir_id 搬到 B。
  3. 身份优先「TMDB ID + 类型 + 季 + 集」；身份不明确只允许新增，禁止自动替换。
  4. 本模块**绝不写 push_queue、绝不调用 _enqueue_* / process_queue**。镜像完成
     不做 Telegram 推送；B 库扫描后的自动识别需由调用方显式传
     publish_after_identify=False。
  5. 禁止「先删除旧文件再复制新文件」。内容变化只新增复制，旧 B 文件保留并标记
     superseded（删除接口在 8.6 待验证，未实现）。

已确认接口（文档第八章），按原样实现，未推测参数：
    POST /userres/v1/file/copy_file      {"fileIds":[...],"parentId":"..."} → data.taskId
    POST /userres/v1/get_task_status    {"taskId":"..."}                  → data.status
    status=2 表示完成；其它取值含义未验证，只记录 raw_status，不猜测成功/失败。

仍待验证、因此**未实现**的接口（8.6）：
"""
import asyncio
import re

from sqlalchemy import select
from sqlalchemy.orm import Session

from .guangya import GuangYaWebFiles, WebFileError
from .models import MirrorFile, MirrorMedia, MirrorRule, MirrorTask
from .utils import (VIDEO_EXTS, media_identity, normalize_work_name, parse_media_name,
                    standard_media_name, star_media_name, utcnow)

UNVERIFIED_ENDPOINTS = [
    "创建文件夹",
    "重命名文件夹（B 库主目录加 *）",
    "删除 B 库文件",
    "删除目录 / 批量删除",
    "投稿分享转存",
]

_TMDB_TAG = re.compile(r"\{?\s*tmdb[-_:：]?(\d+)\s*\}?", re.I)


def _ext(name: str) -> str:
    return "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""


def _is_video(name: str) -> bool:
    return _ext(name) in VIDEO_EXTS


def _fingerprint(item: dict) -> str:
    return f"{int(item.get('fileSize', 0) or 0)}:{int(item.get('utime', 0) or 0)}"


def _dir_name(item: dict) -> str:
    for key in ("fileName", "name", "file_name", "folderName"):
        v = item.get(key)
        if v:
            return str(v).strip()
    return ""


async def _list_dir(api: GuangYaWebFiles, parent_id: str) -> tuple[list[dict], bool]:
    """读取单目录全部分页，返回 (条目, 是否完整)。不完整批次绝不用于同步决策。"""
    items: list[dict] = []
    page = 0
    fetched = 0
    while page < 50:  # 单目录最多 5000 条，防接口异常造成死循环
        data = await api.list_files(parent_id, page, 100)
        batch = list(data.get("list") or [])
        fetched += len(batch)
        items.extend(batch)
        total = int(data.get("total", 0) or 0)
        page += 1
        if not batch or page * 100 >= total:
            return items, (not total or fetched >= total)
    return items, False


async def _walk_source(api: GuangYaWebFiles, root_id: str,
                       max_dirs: int = 400) -> tuple[dict[str, dict], bool]:
    """只读遍历 A 库目录树，返回 {dirId: 节点} 与批次完整性。

    节点含 id / name / parent / path / files(直属文件) / children。
    不写任何数据库表，不触碰 Resource —— 扫描 A 库绝不能污染 B 库资源库。
    """
    root = str(root_id or "*")
    nodes: dict[str, dict] = {}
    stack = [(root, "", "", "")]
    complete = True
    while stack and len(nodes) < max_dirs:
        did, name, path, parent = stack.pop()
        if did in nodes:
            continue
        display = (path + "/" + name).strip("/") if name else path
        items, ok = await _list_dir(api, did)
        complete = complete and ok
        node = {"id": did, "name": name, "parent": parent, "path": display,
                "files": [], "children": []}
        nodes[did] = node
        for it in items:
            fid = str(it.get("fileId") or "")
            if not fid:
                continue
            if int(it.get("resType", 1) or 1) == 2:
                node["children"].append(fid)
                stack.append((fid, _dir_name(it), display, did))
            else:
                node["files"].append(it)
    return nodes, complete


async def _target_children(api: GuangYaWebFiles, parent_id: str) -> dict[str, str]:
    """B 库某目录的直属子目录 → {归一化名: fileId}。带 * 的目录名归一化后与 A 对齐。"""
    items, ok = await _list_dir(api, parent_id)
    if not ok:
        raise WebFileError("B 库目录分页不完整，已中止本次镜像")
    out: dict[str, str] = {}
    for it in items:
        if int(it.get("resType", 1) or 1) != 2:
            continue
        fid = str(it.get("fileId") or "")
        name = _dir_name(it)
        key = normalize_work_name(name)
        if fid and key:
            out[key] = fid
    return out


def _work_dir_of(node: dict, nodes: dict[str, dict], root: str) -> dict | None:
    """从文件所在目录往上找「A 根目录的直接子目录」= 影视主目录。

    兼容两种层级：未整理（剧名/单集）与已整理（剧名/Season N/单集）。
    """
    cur = node
    seen = set()
    while cur and cur.get("parent") and cur["parent"] != root and cur["id"] not in seen:
        seen.add(cur["id"])
        cur = nodes.get(cur["parent"])
    if not cur or cur["id"] == root or cur.get("parent") != root:
        return None
    return cur


def _media_files(work: dict, nodes: dict[str, dict]) -> list[tuple[dict, dict]]:
    """从影视主目录出发 BFS，收集所有 (所属目录节点, 文件条目)，兼容未整理/Season 两种层级。"""
    out: list[tuple[dict, dict]] = []
    stack = [work["id"]]
    seen = set()
    while stack:
        did = stack.pop()
        if did in seen:
            continue
        seen.add(did)
        node = nodes.get(did)
        if not node:
            continue
        for it in node["files"]:
            out.append((node, it))
        stack.extend(node["children"])
    return out


async def _poll_task(api: GuangYaWebFiles, task: MirrorTask, *,
                     max_polls: int = 8, interval: float = 2.0) -> bool:
    """轮询复制任务。只把 status=2 当完成；其它取值原样记录为 unknown，不猜含义。"""
    for i in range(max_polls):
        if i:
            await asyncio.sleep(interval)
        try:
            r = await api.get_task_status(task.task_id)
        except Exception as exc:
            task.error = str(exc)[:300]
            task.polls = i + 1
            continue
        raw = r.get("status")
        task.raw_status = "" if raw is None else str(raw)
        task.polls = i + 1
        if raw == 2 or str(raw) == "2":
            task.status = "done"
            task.finished_at = utcnow()
            return True
    # 其它状态码含义未验证：既不判成功也不判失败，保留 taskId 供后续继续轮询
    task.status = "unknown"
    return False


async def sync_rule(db: Session, rule: MirrorRule, *, max_polls: int = 8,
                    poll_interval: float = 2.0, max_media: int = 100) -> dict:
    """执行一次 A→B 增量同步。逐部影视完成后立即提交，不等整库结束。

    返回统计；任何一步的不完整批次都会中止对应影视，不影响已完成的其它影视。
    """
    stats = {"media": 0, "new": 0, "changed": 0, "unchanged": 0, "skipped": 0,
             "tasks": 0, "done": 0, "unknown": 0, "errors": []}
    if not str(rule.source_root_id or "").strip() or not str(rule.target_root_id or "").strip():
        raise WebFileError("请先配置 A 源头库目录 ID 与 B 镜像库目录 ID")
    api = GuangYaWebFiles(db)
    nodes, complete = await _walk_source(api, rule.source_root_id)
    if not complete:
        raise WebFileError("A 库分页不完整，已中止本次镜像（禁止用不完整批次做变更）")
    root = str(rule.source_root_id or "*")
    try:
        top_children = await _target_children(api, rule.target_root_id)
    except Exception as exc:
        raise WebFileError(f"B 库根目录读取失败：{exc}")

    # 按影视主目录分组
    works: dict[str, dict] = {}
    for node in nodes.values():
        if node["parent"] == root and node["id"] != root:
            works[node["id"]] = node

    for work in list(works.values())[:max_media]:
        wkey = "work:" + normalize_work_name(work["name"])
        if wkey == "work:":
            stats["skipped"] += 1
            continue
        title = standard_media_name(work["name"])
        target_dir_id = top_children.get(normalize_work_name(work["name"])) or ""
        media = db.scalar(select(MirrorMedia).where(
            MirrorMedia.rule_id == rule.id, MirrorMedia.work_key == wkey))
        if not media:
            media = MirrorMedia(rule_id=rule.id, work_key=wkey, title_standard=title,
                                title_mirror=star_media_name(title) if rule.star_naming else title,
                                source_dir_id=work["id"], target_dir_id=target_dir_id)
            db.add(media)
            db.flush()
        else:
            media.title_standard = title
            media.title_mirror = star_media_name(title) if rule.star_naming else title
            media.source_dir_id = work["id"]
            if target_dir_id:
                media.target_dir_id = target_dir_id
        if not media.target_dir_id:
            # 创建目录接口未验证：不自动建，明确提示手工创建（带 * 命名也会被归一化识别）
            media.status = "failed"
            stats["skipped"] += 1
            stats["errors"].append(f"{title}：B 库缺少对应影视目录，请在光鸭网页端手工创建后重试")
            db.commit()
            continue
        # B 库季/子目录映射（只读，不自动创建）
        try:
            sub_dirs = await _target_children(api, media.target_dir_id)
        except Exception:
            sub_dirs = {}
        tm = _TMDB_TAG.search(work["name"])
        tmdb_id = int(tm.group(1)) if tm else None
        existing = {(m.source_file_id, m.fingerprint): m for m in db.scalars(
            select(MirrorFile).where(MirrorFile.media_id == media.id)).all()}
        synced_identities = {m.identity for m in db.scalars(
            select(MirrorFile).where(MirrorFile.media_id == media.id,
                                     MirrorFile.status == "synced")).all()}
        by_parent: dict[str, list[tuple[MirrorFile, str]]] = {}
        for container, item in _media_files(work, nodes):
            fid = str(item.get("fileId") or "")
            fname = _dir_name(item)
            if not fid or not _is_video(fname):
                continue
            fp = _fingerprint(item)
            old = existing.get((fid, fp))
            if old:
                stats["unchanged"] += 1
                continue
            hint = parse_media_name(fname, container["path"])
            # 类型硬约束：有季集线索才判 tv；目录里写了「电视剧/TV」也判 tv；
            # 其余不臆断成 movie，交给 media_identity 标记为身份不明确。
            mtype = str(hint.get("type_hint") or "")
            if hint.get("season") or hint.get("episode") or hint.get("is_tv_hint"):
                mtype = "tv"
            identity, confident = media_identity(
                tmdb_id, mtype, hint.get("season"), hint.get("episode"), fname)
            # 身份不明确且已有同身份同步记录：禁止自动替换
            if not confident and identity in synced_identities:
                stats["skipped"] += 1
                stats["errors"].append(f"{title}/{fname}：身份不明确，禁止自动替换")
                continue
            # 旧版本只标记 superseded，不删除 B 库文件（删除接口未验证）
            for (old_fid, _), row in list(existing.items()):
                if row.identity == identity and row.status == "synced":
                    row.status = "superseded"
                    stats["changed"] += 1
            target_parent = media.target_dir_id
            if container["id"] != work["id"]:
                sub = sub_dirs.get(normalize_work_name(container["name"])) or ""
                if not sub:
                    stats["skipped"] += 1
                    stats["errors"].append(
                        f"{title}：B 库缺少子目录「{standard_media_name(container['name'])}」，请手工创建")
                    continue
                target_parent = sub
            row = MirrorFile(media_id=media.id, rule_id=rule.id, identity=identity,
                             source_file_id=fid, target_file_id="", file_name=fname,
                             size=int(item.get("fileSize", 0) or 0),
                             utime=int(item.get("utime", 0) or 0),
                             fingerprint=fp, status="copying")
            db.add(row)
            db.flush()
            by_parent.setdefault(target_parent, []).append((row, fid))
            stats["new"] += 1
        if not by_parent:
            media.status = "synced"
            media.last_synced_at = utcnow()
            stats["media"] += 1
            db.commit()
            continue
        for parent_id, pairs in by_parent.items():
            try:
                task_id = await api.copy_file([fid for _, fid in pairs], parent_id)
            except Exception as exc:
                for row, _ in pairs:
                    row.status, row.error = "failed", str(exc)[:300]
                stats["errors"].append(f"{title}：复制请求失败 {str(exc)[:120]}")
                db.commit()
                continue
            task = MirrorTask(rule_id=rule.id, media_id=media.id, task_id=task_id,
                              status="running")
            db.add(task)
            db.flush()
            for row, _ in pairs:
                row.task_id = task_id
            stats["tasks"] += 1
            done = await _poll_task(api, task, max_polls=max_polls, interval=poll_interval)
            if done:
                for row, _ in pairs:
                    row.status = "synced"
                stats["done"] += 1
            else:
                stats["unknown"] += 1
                stats["errors"].append(
                    f"{title}：复制任务 {task_id} 未返回 status=2（最后状态 {task.raw_status or '未知'}），"
                    "已保留任务供继续轮询，不判定失败")
            # 单部影视完成即入库：每个 parent 批次结束立即提交
            media.status = "synced" if done else "syncing"
            media.last_synced_at = utcnow()
            db.commit()
        stats["media"] += 1

    rule.last_run = utcnow()
    rule.last_status = "success" if not stats["errors"] else "partial"
    rule.last_message = (f"影视 {stats['media']} 部 · 新增 {stats['new']} · 变化 {stats['changed']} · "
                         f"未变 {stats['unchanged']} · 任务完成 {stats['done']} · "
                         f"待确认 {stats['unknown']} · 跳过 {stats['skipped']}")
    db.commit()
    stats["ok"] = True
    return stats


async def backfill_target_file_ids(db: Session, rule: MirrorRule) -> int:
    """复制接口只返回 taskId、不返回 B fileId；镜像后用只读列目录补齐映射。

    只按「目标目录 + 文件名 + 大小」匹配，不写 Resource、不建分享、不入推送队列。
    """
    api = GuangYaWebFiles(db)
    updated = 0
    medias = list(db.scalars(select(MirrorMedia).where(
        MirrorMedia.rule_id == rule.id, MirrorMedia.target_dir_id != "")).all())
    for media in medias:
        pending = list(db.scalars(select(MirrorFile).where(
            MirrorFile.media_id == media.id, MirrorFile.target_file_id == "",
            MirrorFile.status.in_(["synced", "copying"]))).all())
        if not pending:
            continue
        try:
            direct, _ = await _list_dir(api, media.target_dir_id)
            sub_ids = [str(i.get("fileId") or "") for i in direct
                       if int(i.get("resType", 1) or 1) == 2 and i.get("fileId")]
        except Exception:
            continue
        candidates: dict[str, str] = {}
        for parent in [media.target_dir_id] + sub_ids:
            try:
                items, _ = await _list_dir(api, parent)
            except Exception:
                continue
            for it in items:
                if int(it.get("resType", 1) or 1) == 2:
                    continue
                fid = str(it.get("fileId") or "")
                name = _dir_name(it)
                size = int(it.get("fileSize", 0) or 0)
                if fid:
                    candidates[f"{normalize_work_name(name)}:{size}".rstrip(":")] = fid
                    candidates[normalize_work_name(name)] = fid
        for row in pending:
            hit = candidates.get(f"{normalize_work_name(row.file_name)}:{row.size}") or \
                  candidates.get(normalize_work_name(row.file_name))
            if hit:
                row.target_file_id = hit
                row.updated_at = utcnow()
                updated += 1
        db.commit()
    return updated


async def poll_pending_tasks(db: Session, rule: MirrorRule, *,
                             max_polls: int = 5, interval: float = 2.0) -> dict:
    """继续轮询此前 status 未确认的复制任务（其它 status 取值含义未验证，不猜）。"""
    api = GuangYaWebFiles(db)
    tasks = list(db.scalars(select(MirrorTask).where(
        MirrorTask.rule_id == rule.id, MirrorTask.status.in_(["pending", "running", "unknown"]))).all())
    done = 0
    for task in tasks:
        task.status = "running"
        if await _poll_task(api, task, max_polls=max_polls, interval=interval):
            done += 1
            for row in db.scalars(select(MirrorFile).where(MirrorFile.task_id == task.task_id)).all():
                row.status = "synced"
            if task.media_id:
                media = db.get(MirrorMedia, task.media_id)
                if media:
                    media.status = "synced"
                    media.last_synced_at = utcnow()
    db.commit()
    return {"tasks": len(tasks), "done": done, "still_unknown": len(tasks) - done}
