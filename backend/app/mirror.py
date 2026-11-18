"""v0.8.0 起：A 源头库 → B 镜像库 增量同步；v0.8.1 增加分类目录 1:1 镜像与风控节流。

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
from .models import MirrorDir, MirrorFile, MirrorMedia, MirrorRule, MirrorTask
from .utils import (VIDEO_EXTS, media_identity, normalize_work_name, parse_media_name,
                    standard_media_name, star_media_name, utcnow)

# v0.8.1：w.har 已取证 create_dir / rename / delete_file（含批量）。
# 下面只保留仍未取证的转存能力；前端 JS 里虽出现 move_file / recycle_file 等路径，
# 但没有请求体证据，仍然不实现、不猜参数。
UNVERIFIED_ENDPOINTS = [
    "投稿分享转存（TgtoDrive 侧接口，未抓包）",
]
CONFIRMED_WRITE_ENDPOINTS = [
    "POST /userres/v1/file/create_dir {parentId,dirName} → data.fileId",
    "POST /userres/v1/file/rename {fileId,newName}",
    "POST /userres/v1/file/delete_file {fileIds:[...]} → data.taskId",
    "POST /userres/v1/file/copy_file {fileIds,parentId} → data.taskId",
    "POST /userres/v1/get_task_status {taskId}（status=2 完成）",
]

# v0.8.1：官方 OpenAPI v1.3 §10 写接口限频 2 次/秒 → 文件操作之间至少 0.5 秒。
# 这是硬下限，管理页只能调高，不能调低（前端 min 与后端校验双重约束）。
MIN_FILE_IDLE_SECONDS = 0.5
# get_task_status 限频 5 次/秒 → 批量轮询时每次查询之间至少 0.2 秒。
TASK_STATUS_MIN_INTERVAL = 0.2

_TMDB_TAG = re.compile(r"\{?\s*tmdb[-_:：]?(\d+)\s*\}?", re.I)
# 影视主目录强特征：{tmdb-xxx} / (2021) 这类标记
_MEDIA_MARK = re.compile(r"(?:tmdb|imdb)[-_:：]?\d+|[\[(（【{]\s*(?:19|20)\d{2}\s*[\])）】}]", re.I)
# 分类目录兜底特征：动画电影 / 国产电影 / 剧集合集 等
_CATEGORY_HINT = re.compile(r"(电影|电视剧|剧集|分类|合集|综艺|纪录片)", re.I)


# ---------- v0.8.1：分类目录识别与 1:1 路径镜像 ----------
def _has_video(node: dict) -> bool:
    return any(_is_video(_dir_name(f)) for f in node.get("files") or [])


def _subtree_video(did: str, nodes: dict, cache: dict) -> bool:
    """该目录子树里是否存在视频文件（用于跳过空目录/纯字幕目录）。"""
    if did in cache:
        return cache[did]
    cache[did] = False  # 防环
    node = nodes.get(did)
    if not node:
        return False
    ok = _has_video(node) or any(_subtree_video(c, nodes, cache)
                                 for c in node.get("children") or [])
    cache[did] = ok
    return ok


def _looks_like_work(name: str) -> bool:
    return bool(_MEDIA_MARK.search(str(name or "")))


def _discover_works(nodes: dict, root: str) -> list[tuple[dict, list[tuple[str, str]]]]:
    """从 A 根目录往下找到「真正的影视主目录」，返回 (节点, 分类目录链)。

    分类目录（自身无视频、子树有视频、名字像分类）在 B 库保持原名、不加 *；
    只有影视主目录才按 star_naming 加 *。支持多级分类与未整理剧集
    （剧名目录下直接放单集）两种常见结构。
    """
    cache: dict[str, bool] = {}
    out: list[tuple[dict, list[tuple[str, str]]]] = []
    root_node = nodes.get(root) or {"children": []}
    stack = [(cid, []) for cid in root_node.get("children") or []]
    while stack:
        did, cats = stack.pop()
        node = nodes.get(did)
        if not node or not _subtree_video(did, nodes, cache):
            continue
        hint = bool(_CATEGORY_HINT.search(node["name"])) and not _looks_like_work(node["name"])
        if (hint and node.get("children")) or (not _has_video(node) and not _looks_like_work(node["name"])):
            for child in node.get("children") or []:
                stack.append((child, cats + [(node["name"], did)]))
            continue
        out.append((node, cats))
    return out


def _relative_dir_names(node: dict, work: dict, nodes: dict) -> list[str]:
    """从影视主目录到文件所在目录之间的子目录名（Season 1 等），B 库同名映射。"""
    names: list[str] = []
    cur, seen = node, set()
    while cur and cur.get("id") != work["id"] and cur["id"] not in seen:
        seen.add(cur["id"])
        names.append(cur.get("name") or "")
        cur = nodes.get(cur.get("parent"))
    return list(reversed([n for n in names if n]))


async def _target_children_cached(api: GuangYaWebFiles, parent_id: str,
                                  cache: dict) -> dict[str, str]:
    if parent_id not in cache:
        try:
            cache[parent_id] = await _target_children(api, parent_id)
        except Exception:
            cache[parent_id] = {}
    return cache[parent_id]


async def _resolve_b_dir(api: GuangYaWebFiles, b_root: str,
                         cats: list[tuple[str, str]], work_name: str,
                         star_naming: bool, cache: dict):
    """逐级把 A 的目录路径映射到 B，返回 (目标目录ID, 缺失清单, 已解析链)。

    分类目录按原名匹配（B 库不改名）；影视主目录按归一化名匹配（B 库带 * 也能命中）。
    创建目录接口未验证，缺失时只登记清单，绝不猜测接口参数去自动创建。
    """
    parent = str(b_root)
    missing: list[dict] = []
    chain: list[tuple[str, str, str, str]] = []  # (A目录ID, 名称, kind, B目录ID)
    for cat_name, cat_id in cats:
        children = await _target_children_cached(api, parent, cache)
        fid = children.get(normalize_work_name(cat_name)) or ""
        if not fid:
            missing.append({"parent_id": parent, "name": standard_media_name(cat_name),
                            "kind": "分类目录", "star": False,
                            "tip": "在 B 库建立同名目录，名称保持不变，不要加 *"})
            return "", missing, chain
        chain.append((cat_id, cat_name, "category", fid))
        parent = fid
    children = await _target_children_cached(api, parent, cache)
    fid = children.get(normalize_work_name(work_name)) or ""
    expected = (star_media_name(standard_media_name(work_name)) if star_naming
                else standard_media_name(work_name))
    if not fid:
        missing.append({"parent_id": parent, "name": expected,
                        "kind": "影视主目录", "star": bool(star_naming),
                        "tip": "这是影视主目录，按 * 约定命名" if star_naming else "保持标准影视名"})
        return "", missing, chain
    chain.append(("", work_name, "work", fid))
    return fid, missing, chain


def _record_dir(db: Session, rule_id: int, source_dir_id: str, parent_target_id: str,
                name: str, kind: str, cat_path: str, target_dir_id: str, status: str) -> None:
    row = db.scalar(select(MirrorDir).where(MirrorDir.rule_id == rule_id,
                                            MirrorDir.source_dir_id == source_dir_id))
    if not row:
        row = MirrorDir(rule_id=rule_id, source_dir_id=source_dir_id)
        db.add(row)
    row.parent_target_id = parent_target_id
    row.name_standard = name
    row.kind = kind
    row.category_path = cat_path
    row.target_dir_id = target_dir_id
    row.status = status
    row.updated_at = utcnow()
    db.flush()


def _purge_legacy_failed(db: Session, rule_id: int) -> int:
    """清掉 v0.8.0 把分类目录误当成影视而留下的空 failed 记录。

    v0.8.1 起每条 media 都会写 category_path（无分类写 __root__），因此
    category_path 为空且无文件的 failed 记录只可能是旧版误判产物。
    """
    rows = list(db.scalars(select(MirrorMedia).where(
        MirrorMedia.rule_id == rule_id, MirrorMedia.status == "failed",
        MirrorMedia.category_path == "")).all())
    n = 0
    for m in rows:
        if db.query(MirrorFile).filter(MirrorFile.media_id == m.id).count():
            continue
        db.delete(m)
        n += 1
    if n:
        db.commit()
    return n


async def _poll_tasks_batch(api: GuangYaWebFiles, tasks: list[tuple[MirrorTask, list]],
                            *, max_polls: int = 8, interval: float = 2.0) -> None:
    """统一轮询本轮全部复制任务，避免逐任务串行等待导致总耗时随文件数线性增长。"""
    pending = [t for t, _ in tasks]
    for i in range(max_polls):
        if i:
            await asyncio.sleep(interval)
        still = []
        for task in pending:
            try:
                r = await api.get_task_status(task.task_id)
            except Exception as exc:
                task.error = str(exc)[:300]
                task.polls = i + 1
                still.append(task)
                continue
            await asyncio.sleep(TASK_STATUS_MIN_INTERVAL)
            raw = r.get("status")
            task.raw_status = "" if raw is None else str(raw)
            task.polls = i + 1
            if raw == 2 or str(raw) == "2":
                task.status = "done"
                task.finished_at = utcnow()
            else:
                still.append(task)
        pending = still
        if not pending:
            break
    for task in pending:
        task.status = "unknown"


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

    v0.8.1：
      - 分类目录 1:1 镜像到 B 库，名称不修改；只有影视主目录按 star_naming 加 *。
      - B 库缺目录时自动 create_dir（抓包已确认，可用 auto_create_dirs 关闭）。
      - 每次文件复制之间留可配置空闲时间，硬下限 MIN_FILE_IDLE_SECONDS 不可调低。
      - 本模块绝不写 push_queue、绝不调用 _enqueue_* / process_queue。
    """
    idle = max(float(getattr(rule, "file_idle_seconds", 1.0) or 1.0), MIN_FILE_IDLE_SECONDS)
    stats = {"media": 0, "new": 0, "changed": 0, "unchanged": 0, "skipped": 0,
             "tasks": 0, "done": 0, "unknown": 0, "blocked": 0,
             "dirs_created": 0, "dirs_mapped": 0, "missing_dirs": [],
             "idle_seconds": idle, "errors": []}
    if not str(rule.source_root_id or "").strip() or not str(rule.target_root_id or "").strip():
        raise WebFileError("请先配置 A 源头库目录 ID 与 B 镜像库目录 ID")
    api = GuangYaWebFiles(db)
    stats["purged_legacy"] = _purge_legacy_failed(db, rule.id)
    nodes, complete = await _walk_source(api, rule.source_root_id)
    if not complete:
        raise WebFileError("A 库分页不完整，已中止本次镜像（禁止用不完整批次做变更）")
    root = str(rule.source_root_id or "*")
    b_root = str(rule.target_root_id)
    cache: dict[str, dict[str, str]] = {}
    auto_dirs = bool(getattr(rule, "auto_create_dirs", True))

    async def _ensure_child(parent_id: str, name: str, kind: str,
                            source_id: str, cat_path: str) -> tuple[str, dict | None]:
        """在 B 库找同名子目录；开启 auto_create_dirs 时自动创建。返回 (fileId, missing)。"""
        children = await _target_children_cached(api, parent_id, cache)
        key = normalize_work_name(name)
        fid = children.get(key) or ""
        if fid:
            _record_dir(db, rule.id, source_id, parent_id, name, kind, cat_path, fid, "mapped")
            stats["dirs_mapped"] += 1
            return fid, None
        tip = ("在 B 库建立同名分类目录，名称保持不变，不要加 *"
               if kind == "category" else
               "按 * 约定命名（括号年份保持原样）" if (kind == "work" and rule.star_naming)
               else "保持标准影视名/子目录名")
        missing = {"parent_id": parent_id, "name": name, "kind": kind,
                   "star": kind == "work" and bool(rule.star_naming), "tip": tip}
        if not auto_dirs:
            _record_dir(db, rule.id, source_id, parent_id, name, kind, cat_path, "", "missing")
            return "", missing
        # 建目录同样是文件操作，受同一风控间隔约束
        await asyncio.sleep(idle)
        try:
            made = await api.create_dir(parent_id, name)
        except Exception as exc:
            missing["tip"] = f"自动创建失败：{str(exc)[:160]}"
            _record_dir(db, rule.id, source_id, parent_id, name, kind, cat_path, "", "missing")
            return "", missing
        fid = made["file_id"]
        cache.setdefault(parent_id, {})[key] = fid
        _record_dir(db, rule.id, source_id, parent_id, name, kind, cat_path, fid, "created")
        stats["dirs_created"] += 1
        return fid, None

    works = _discover_works(nodes, root)
    for work, cats in works[:max_media]:
        cat_path = "/".join(n for n, _ in cats) or "__root__"
        title = standard_media_name(work["name"])
        expected = star_media_name(title) if rule.star_naming else title
        wkey = "work:" + normalize_work_name(work["name"])
        if wkey == "work:":
            stats["skipped"] += 1
            continue

        # 1:1 解析/创建 B 库分类目录（原名），再解析/创建影视主目录（带 *）
        target_dir_id = b_root
        missing_all: list[dict] = []
        for cat_name, cat_id in cats:
            fid, miss = await _ensure_child(target_dir_id, standard_media_name(cat_name),
                                            "category", cat_id, cat_path)
            if miss:
                missing_all.append(miss)
                break
            target_dir_id = fid
        if not missing_all:
            fid, miss = await _ensure_child(target_dir_id, expected, "work",
                                            work["id"], cat_path)
            if miss:
                missing_all.append(miss)
            else:
                target_dir_id = fid

        media = db.scalar(select(MirrorMedia).where(
            MirrorMedia.rule_id == rule.id, MirrorMedia.work_key == wkey))
        if not media:
            media = MirrorMedia(rule_id=rule.id, work_key=wkey, title_standard=title,
                                title_mirror=expected, source_dir_id=work["id"],
                                target_dir_id=target_dir_id if not missing_all else "",
                                category_path=cat_path)
            db.add(media)
            db.flush()
        else:
            media.title_standard = title
            media.title_mirror = expected
            media.source_dir_id = work["id"]
            media.category_path = cat_path
            if target_dir_id and not missing_all:
                media.target_dir_id = target_dir_id
        if missing_all or not media.target_dir_id:
            media.status = "blocked"
            stats["blocked"] += 1
            stats["skipped"] += 1
            stats["missing_dirs"].extend(missing_all)
            db.commit()
            continue

        tm = _TMDB_TAG.search(work["name"])
        tmdb_id = int(tm.group(1)) if tm else None
        existing = {(m.source_file_id, m.fingerprint): m for m in db.scalars(
            select(MirrorFile).where(MirrorFile.media_id == media.id)).all()}
        synced_identities = {m.identity for m in db.scalars(
            select(MirrorFile).where(MirrorFile.media_id == media.id,
                                     MirrorFile.status == "synced")).all()}
        sub_cache: dict[str, str] = {}
        copy_jobs: list[tuple[MirrorFile, str, str]] = []
        for container, item in _media_files(work, nodes):
            fid = str(item.get("fileId") or "")
            fname = _dir_name(item)
            if not fid or not _is_video(fname):
                continue
            fp = _fingerprint(item)
            if (fid, fp) in existing:
                stats["unchanged"] += 1
                continue
            hint = parse_media_name(fname, container["path"])
            mtype = str(hint.get("type_hint") or "")
            if hint.get("season") or hint.get("episode") or hint.get("is_tv_hint"):
                mtype = "tv"
            identity, confident = media_identity(
                tmdb_id, mtype, hint.get("season"), hint.get("episode"), fname)
            if not confident and identity in synced_identities:
                stats["skipped"] += 1
                stats["errors"].append(f"{title}/{fname}：身份不明确，禁止自动替换")
                continue
            for row in list(existing.values()):
                if row.identity == identity and row.status == "synced":
                    row.status = "superseded"
                    stats["changed"] += 1
            # 作品目录下的 Season/子目录在 B 库同名镜像（不加 *）
            target_parent = media.target_dir_id
            for sub_name in _relative_dir_names(container, work, nodes):
                key = f"{target_parent}|{normalize_work_name(sub_name)}"
                if key in sub_cache:
                    target_parent = sub_cache[key]
                    continue
                sub_id, miss = await _ensure_child(target_parent, standard_media_name(sub_name),
                                                    "season", container["id"], cat_path)
                if miss:
                    stats["missing_dirs"].append(miss)
                    stats["blocked"] += 1
                    target_parent = ""
                    break
                sub_cache[key] = sub_id
                target_parent = sub_id
            if not target_parent:
                stats["skipped"] += 1
                continue
            row = MirrorFile(media_id=media.id, rule_id=rule.id, identity=identity,
                             source_file_id=fid, target_file_id="", file_name=fname,
                             size=int(item.get("fileSize", 0) or 0),
                             utime=int(item.get("utime", 0) or 0),
                             fingerprint=fp, status="copying")
            db.add(row)
            db.flush()
            copy_jobs.append((row, fid, target_parent))
            stats["new"] += 1

        if not copy_jobs:
            media.status = "synced"
            media.last_synced_at = utcnow()
            stats["media"] += 1
            db.commit()
            continue

        # 逐文件复制，每次之间留 idle 秒；统一批量轮询，避免总耗时随文件数线性增长
        tasks: list[tuple[MirrorTask, list[MirrorFile]]] = []
        for idx, (row, source_fid, target_parent) in enumerate(copy_jobs):
            if idx:
                await asyncio.sleep(idle)
            try:
                task_id = await api.copy_file([source_fid], target_parent)
            except Exception as exc:
                row.status, row.error = "failed", str(exc)[:300]
                stats["errors"].append(f"{title}/{row.file_name}：复制请求失败 {str(exc)[:120]}")
                db.commit()
                continue
            task = MirrorTask(rule_id=rule.id, media_id=media.id, task_id=task_id,
                              status="running")
            db.add(task)
            db.flush()
            row.task_id = task_id
            tasks.append((task, [row]))
            stats["tasks"] += 1
        await _poll_tasks_batch(api, tasks, max_polls=max_polls, interval=poll_interval)
        for task, rows in tasks:
            if task.status == "done":
                for r_ in rows:
                    r_.status = "synced"
                stats["done"] += 1
            else:
                stats["unknown"] += 1
                stats["errors"].append(
                    f"{title}：复制任务 {task.task_id} 未返回 status=2"
                    f"（最后状态 {task.raw_status or '未知'}），已保留任务供继续轮询")
        media.status = "synced" if tasks and all(t.status == "done" for t, _ in tasks) else "syncing"
        media.last_synced_at = utcnow()
        db.commit()
        stats["media"] += 1

    rule.last_run = utcnow()
    if stats["errors"]:
        rule.last_status = "partial"
    elif stats["missing_dirs"] or stats["blocked"]:
        rule.last_status = "blocked"
    else:
        rule.last_status = "success"
    rule.last_message = (
        f"影视 {stats['media']} 部 · 新增 {stats['new']} · 变化 {stats['changed']} · "
        f"未变 {stats['unchanged']} · 任务完成 {stats['done']} · 待确认 {stats['unknown']} · "
        f"跳过 {stats['skipped']} · 建目录 {stats['dirs_created']} · "
        f"待建目录 {len(stats['missing_dirs'])} · 文件间隔 {idle}s")
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
