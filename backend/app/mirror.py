"""v0.8.0 起：A 源头库 → B 镜像库 增量同步；v0.8.1 分类目录 1:1 镜像与风控节流；v0.8.2 建目录失败可诊断与自动降级；v0.8.4 文件复制可重试、可诊断；v0.8.6 B 库单集/电影文件按英文剧名改名且保持 A→B 映射。

设计红线（来自《tg-gy 最终版项目开发需求文档》）：
  1. A 源头库**只读**，任何情况下不修改 A 的文件/目录。
  2. B 镜像库才进入扫描、资源库、分享与 Telegram 分发；A/B fileId 不同，
     必须通过 mirror_files 映射，禁止把 A 的 dir_chain / share_dir_id 搬到 B。
  3. 身份优先「TMDB ID + 类型 + 季 + 集」；身份不明确只允许新增，禁止自动替换。
  4. 本模块**绝不写 push_queue、绝不调用 _enqueue_* / process_queue**。推送统一由
     上层 main.py 的镜像后管线负责：v0.8.5 起同步完成会自动扫描 B 库、识别，并把
     本轮识别成功且有海报的影视按分发规则入队（用户明确要求，反转了 v0.8.0~v0.8.4
     写死的「镜像完成不自动推 TG」红线）。但**镜像完成仍不是群推送**：该管线不调用
     process_change_events，不批量补推历史内容变化事件，也不回溯推送历史资源。
  5. 禁止「先删除旧文件再复制新文件」。内容变化只新增复制，旧 B 文件保留并标记
     superseded（删除接口在 8.6 待验证，未实现）。
  6. v0.8.6：**只对 B 库单集/单电影文件改名**（只换中文片名段），A 库与 B 库
     分类目录/影视主目录/季目录名一律不变。`MirrorFile.file_name` 永远是 A 原名，
     改名结果另存 `target_name`；增量比对仍以 `source_file_id + fingerprint` 为准，
     改名不得影响「后期对比变量更新」。
  7. v0.8.7：**以 B 库实际内容为准**。`sync_rule()` 不能只信 `MirrorFile.status`，
     每部作品只读列一次 B 库核对已同步记录；只有列目录完整可信且确实找不到时，
     才把 `synced` 重置为 `pending` 复用同一行重新复制。B 库文件数不少于已同步
     记录数时绝不重置（可能只是网盘改名 / 重名加后缀），避免重复复制整部作品。

v0.8.4 接口取证口径（务必区分三类，勿再混为一谈）：
  - w.har 抓到真实请求+响应：create_dir / rename / delete_file / get_file_list / get_task_status。
    get_task_status 响应 {"msg":"success","data":{"status":2}}；前端 JS 轮询函数 p5
    另给出 status=3 与 detail.code≠0 都是失败的语义。
  - copy_file **只有前端 JS 调用契约**，没有真实请求/响应抓包：
    onCopyTo → POST /userres/v1/file/copy_file {fileIds:[...], parentId} → data.taskId，
    见 index.4998cfbcbd.js。w.har 里 54 条请求中没有任何一次复制操作。
    本模块按该契约实现，但真实返回码/鉴权细节仍需用户补抓一次「复制到」HAR 复核。
  - 仍未实现：投稿分享转存（TgtoDrive 侧，未抓包）。
"""
import asyncio
import logging
import re

from sqlalchemy import select
from sqlalchemy.orm import Session

from . import ai_assist, tmdb
from .file_naming import (english_file_name, sanitize_english_title,
                          with_conflict_suffix, work_core_title)
from .guangya import GuangYaWebFiles, WebFileError
from .models import MirrorDir, MirrorFile, MirrorMedia, MirrorRule, MirrorTask, Resource
from .utils import (NAME_MARK, VIDEO_EXTS, media_identity, normalize_work_name,
                    parse_media_name, standard_media_name, star_media_name, utcnow)

logger = logging.getLogger(__name__)

# v0.8.1：w.har 已取证 create_dir / rename / delete_file（含批量）。
# 下面只保留仍未取证的转存能力；前端 JS 里虽出现 move_file / recycle_file 等路径，
# 但没有请求体证据，仍然不实现、不猜参数。
UNVERIFIED_ENDPOINTS = [
    "投稿分享转存（TgtoDrive 侧接口，未抓包）",
    "copy_file 真实请求/响应：w.har 未发生「复制到」操作，目前只有前端 JS 契约，"
    "建议在网页端手动复制一个文件并抓包复核（重点看 data.taskId 与错误码 157）",
]
CONFIRMED_WRITE_ENDPOINTS = [
    "POST /userres/v1/file/create_dir {parentId,dirName} → data.fileId（w.har）",
    "POST /userres/v1/file/rename {fileId,newName}（w.har；v0.8.6 起仅用于 B 库单集/电影文件改名）",
    "POST /userres/v1/file/delete_file {fileIds:[...]} → data.taskId（w.har）",
    "POST /userres/v1/file/copy_file {fileIds,parentId} → data.taskId（仅前端 JS 契约，待 HAR 复核）",
    "POST /userres/v1/get_task_status {taskId} → data.status（w.har；status=2 完成、3 失败，detail.code≠0 失败）",
]

# v0.8.1：官方 OpenAPI v1.3 §10 写接口限频 2 次/秒 → 文件操作之间至少 0.5 秒。
# 这是硬下限，管理页只能调高，不能调低（前端 min 与后端校验双重约束）。
MIN_FILE_IDLE_SECONDS = 0.5
# get_task_status 限频 5 次/秒 → 批量轮询时每次查询之间至少 0.2 秒。
TASK_STATUS_MIN_INTERVAL = 0.2
# v0.8.4：与 w.har 中网页端 get_file_list 的 pageSize 一致。
# 旧版用 100 且以 page*100>=total 判完整，实际每页不足 100 时会误判为
# 「分页不完整」直接中止整轮，或漏掉文件层条目。
LIST_PAGE_SIZE = 50
# v0.8.4：只有 synced 才表示「文件确实到了 B 库」。这些状态必须重试，
# 绝不能和 synced 一起塞进 existing 判 unchanged（否则永久卡死）。
RETRYABLE_FILE_STATUS = {"pending", "copying", "failed", "unknown"}

_TMDB_TAG = re.compile(r"\{?\s*tmdb[-_:：]?(\d+)\s*\}?", re.I)
# 影视主目录强特征：{tmdb-xxx} / (2021) 这类标记
_MEDIA_MARK = re.compile(r"(?:tmdb|imdb)[-_:：]?\d+|[\[(（【{]\s*(?:19|20)\d{2}\s*[\])）】}]", re.I)
# 分类目录兜底特征：动画电影 / 国产电影 / 剧集合集 等
_CATEGORY_HINT = re.compile(r"(电影|电视剧|剧集|分类|合集|综艺|纪录片)", re.I)
# v0.8.7：季目录特征。作品目录名不带年份/{tmdb-} 且下挂 Season N 时，
# 它仍是作品主目录，不能继续下探（否则 Season 1 会被当成作品、分类链多一层）。
_SEASON_DIR = re.compile(
    r"^(?:season\s*\d+|s\d{1,2}|第\s*[0-9一二三四五六七八九十]+\s*季|specials?|extras?)$",
    re.I)


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

    分类目录（自身无视频、子树有视频、名字像分类）在 B 库保持原名、不加标记；
    只有影视主目录才按 star_naming 加 ·。支持多级分类与未整理剧集
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
        cat_hint = bool(_CATEGORY_HINT.search(node["name"]))
        hint = cat_hint and not _looks_like_work(node["name"])
        # v0.8.7：目录名不像分类、不带年份标记，但直属子目录是 Season N 时，
        # 该目录就是作品主目录（常见写法：影视/国产剧/侠客行/Season 1/E01.mkv）。
        has_season_child = any(
            bool(_SEASON_DIR.search(str(nodes.get(cid, {}).get("name") or "")))
            for cid in (node.get("children") or []))
        if (not cat_hint and not _looks_like_work(node["name"])
                and has_season_child):
            out.append((node, cats))
            continue
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
    """v0.8.2：不再吞异常。B 库访问失败 ≠ B 库没有该目录，必须让上层明确中止。

    旧版把「根目录不可访问」当成空目录，接着对每部作品都调一次 create_dir 并
    静默记成 missing，界面只显示「请手工建立」，真实原因完全丢失。
    """
    if parent_id not in cache:
        cache[parent_id] = await _target_children(api, parent_id)
    return cache[parent_id]


async def _resolve_b_dir(api: GuangYaWebFiles, b_root: str,
                         cats: list[tuple[str, str]], work_name: str,
                         star_naming: bool, cache: dict):
    """逐级把 A 的目录路径映射到 B，返回 (目标目录ID, 缺失清单, 已解析链)。

    分类目录按原名匹配（B 库不改名）；影视主目录按归一化名匹配（带 · / 无标记都能命中）。
    本函数只供旧映射回查；实际建目录走 sync_rule._ensure_child（create_dir 已由 HAR 取证）。
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
                            "tip": "在 B 库建立同名目录，名称保持不变，不要加 ·"})
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
                        "tip": "这是影视主目录，按 · 约定命名" if star_naming else "保持标准影视名"})
        return "", missing, chain
    chain.append(("", work_name, "work", fid))
    return fid, missing, chain


def _record_dir(db: Session, rule_id: int, source_dir_id: str, parent_target_id: str,
                name: str, kind: str, cat_path: str, target_dir_id: str, status: str,
                error: str = "") -> None:
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
    row.error = str(error or "")[:500]
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


def _task_outcome(raw, detail) -> tuple[str, str]:
    """按网页端 JS 轮询函数 p5 的真实语义判定复制任务结果。

    JS 原文（index.4998cfbcbd.js）：
        if ([2,3].includes(status) && detail.code && detail.code !== 0) throw 任务失败
        if (status === 2) return success
        if (status === 3) throw 任务失败
    即 status=2 且 detail.code 非 0、或 status=3 都是**明确失败**（例如 157 空间不足）。
    v0.8.3 只认 status=2 为完成、把失败一律记成 unknown，文件其实没过去，
    MirrorFile 却永远停在 copying 不再重试。
    """
    detail = detail or {}
    try:
        code = int(detail.get("code", 0) or 0)
    except (TypeError, ValueError):
        code = 0
    msg = str(detail.get("msg") or "").strip()
    s = "" if raw is None else str(raw)
    if s in ("2", "3") and code and code != 0:
        return "failed", msg or f"任务失败 detail.code={code}"
    if s == "2":
        return "done", ""
    if s == "3":
        return "failed", msg or "任务 status=3（失败）"
    return "pending", ""


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
            outcome, err = _task_outcome(raw, r.get("detail") or {})
            if outcome == "done":
                task.status, task.error = "done", ""
                task.finished_at = utcnow()
            elif outcome == "failed":
                task.status, task.error = "failed", err[:300]
                task.finished_at = utcnow()
                logger.warning("[mirror] 复制任务 %s 失败：%s", task.task_id, task.error)
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


def _b_names(row: MirrorFile) -> list[str]:
    """v0.8.6：B 库里这个文件可能的名字。

    `file_name` 永远是 A 库原名（增量比对锚点），`target_name` 才是 B 库改名后的
    实际名字。两个都查，兼容「存量未改名 / 改名尚未生效 / 改名失败」三种情况。
    """
    out: list[str] = []
    for n in (str(getattr(row, "target_name", "") or "").strip(),
              str(row.file_name or "").strip()):
        if n and n not in out:
            out.append(n)
    return out


def _lookup_b_file(candidates: dict[str, str], row: MirrorFile) -> str:
    """按 B 库实际文件名（+大小）反查 fileId。

    v0.8.6：改名后必须优先查 target_name，否则已改名的文件会被误判「不存在」，
    下一轮同步重新复制一份——这正是「保持 A→B 映射」要防的事。
    """
    size = int(row.size or 0)
    for name in _b_names(row):
        key = normalize_work_name(name)
        hit = candidates.get(f"{key}:{size}") or candidates.get(key)
        if hit:
            return str(hit)
    return ""


def _dir_name(item: dict) -> str:
    for key in ("fileName", "name", "file_name", "folderName"):
        v = item.get(key)
        if v:
            return str(v).strip()
    return ""


async def _list_dir(api: GuangYaWebFiles, parent_id: str) -> tuple[list[dict], bool]:
    """读取单目录全部分页，返回 (条目, 是否完整)。不完整批次绝不用于同步决策。

    v0.8.4：按抓包对齐每页 50 条，用「累计条数 vs total」判定，不再用
    page*100>=total 这种写死页大小的判断——后者在接口实际每页返回 50 条时
    会提前结束并把完整目录误判成不完整，整轮镜像直接中止。
    """
    items: list[dict] = []
    page = 0
    fetched = 0
    while page < 100:  # 单目录最多 5000 条，防接口异常造成死循环
        data = await api.list_files(parent_id, page, LIST_PAGE_SIZE)
        batch = list(data.get("list") or [])
        fetched += len(batch)
        items.extend(batch)
        total = int(data.get("total", 0) or 0)
        page += 1
        if not batch:
            return items, (not total or fetched >= total)
        if total and fetched >= total:
            return items, True
        # 短页但 total 显示还有更多：接口提前截断，禁止拿这批数据做同步决策
        if len(batch) < LIST_PAGE_SIZE:
            return items, False
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
    """B 库某目录的直属子目录 → {归一化名: fileId}。带 · / 无标记目录名归一化后与 A 对齐。"""
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
    """轮询复制任务。v0.8.4：按 _task_outcome 区分 done / failed / pending。"""
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
        outcome, err = _task_outcome(raw, r.get("detail") or {})
        if outcome == "done":
            task.status, task.error = "done", ""
            task.finished_at = utcnow()
            return True
        if outcome == "failed":
            task.status, task.error = "failed", err[:300]
            task.finished_at = utcnow()
            return False
    # 其它状态码含义未验证：既不判成功也不判失败，保留 taskId 供后续继续轮询
    task.status = "unknown"
    return False


async def sync_rule(db: Session, rule: MirrorRule, *, max_polls: int = 8,
                    poll_interval: float = 2.0, max_media: int = 100) -> dict:
    """执行一次 A→B 增量同步。逐部影视完成后立即提交，不等整库结束。

    v0.8.1：
      - 分类目录 1:1 镜像到 B 库，名称不修改；只有影视主目录按 star_naming 加 ·。
      - B 库缺目录时自动 create_dir（抓包已确认，可用 auto_create_dirs 关闭）。
      - 每次文件复制之间留可配置空闲时间，硬下限 MIN_FILE_IDLE_SECONDS 不可调低。
      - 本模块绝不写 push_queue、绝不调用 _enqueue_* / process_queue。
    """
    idle = max(float(getattr(rule, "file_idle_seconds", 1.0) or 1.0), MIN_FILE_IDLE_SECONDS)
    stats = {"media": 0, "new": 0, "changed": 0, "unchanged": 0, "skipped": 0,
             "tasks": 0, "done": 0, "unknown": 0, "blocked": 0,
             "retries": 0, "failed": 0,
             # v0.8.7：DB 标记 synced 但 B 库实际没有 → 自动补复制的计数
             "missing_in_b": 0, "name_mismatch": 0,
             "dirs_created": 0, "dirs_mapped": 0, "dirs_fallback": 0,
             "dir_errors": [], "missing_dirs": [], "file_errors": [],
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
    # v0.8.2：防御异常数据。列本身是 NOT NULL，但 bool(None) 会把它当成「关闭」，
    # 让整批作品静默变成 missing；NULL 一律按默认开启处理。
    _raw_auto = getattr(rule, "auto_create_dirs", True)
    auto_dirs = True if _raw_auto is None else bool(_raw_auto)
    # v0.8.2：先确认 B 库根目录真的可读。旧版把「访问失败」吞成「空目录」，
    # 会连着对每部作品调用 create_dir 全部失败，界面却只显示「请手工建立」。
    try:
        cache[b_root] = await _target_children(api, b_root)
    except Exception as exc:
        raise WebFileError(
            f"B 镜像库根目录不可访问（{b_root}）：{exc}。"
            "请核对「镜像同步」里填的 B 库目录 ID、确认目录存在且光鸭登录态有效"
        ) from exc
    # v0.8.3：网盘拒绝 *（code=141），标记已改为 ·；只有连 · 也拒绝时整轮才用无标记标准名
    plain_mode = {"on": False, "tried": False}

    async def _ensure_child(parent_id: str, name: str, kind: str,
                            source_id: str, cat_path: str) -> tuple[str, dict | None, str]:
        """在 B 库找同名子目录；开启 auto_create_dirs 时自动创建。

        返回 (fileId, missing, 实际使用的目录名)。失败原因写进 MirrorDir.error 与
        stats["dir_errors"]，不再被上层的通用文案覆盖。
        """
        children = await _target_children_cached(api, parent_id, cache)
        key = normalize_work_name(name)
        fid = children.get(key) or ""
        if fid:
            _record_dir(db, rule.id, source_id, parent_id, name, kind, cat_path, fid, "mapped")
            stats["dirs_mapped"] += 1
            return fid, None, name
        tip = ("在 B 库建立同名分类目录，名称保持不变，不要加 ·"
               if kind == "category" else
               "按 · 约定命名（括号年份保持原样）" if (kind == "work" and rule.star_naming)
               else "保持标准影视名/子目录名")
        missing = {"parent_id": parent_id, "name": name, "kind": kind,
                   "star": kind == "work" and bool(rule.star_naming), "tip": tip}
        if not auto_dirs:
            err = "自动创建目录已关闭：请在「镜像同步 → B 库缺目录自动创建」开启后重新同步"
            logger.warning("[mirror] %s（%s）", err, name)
            missing["error"] = err
            missing["tip"] = err
            _record_dir(db, rule.id, source_id, parent_id, name, kind, cat_path,
                        "", "missing", err)
            if len(stats["dir_errors"]) < 5:
                stats["dir_errors"].append(f"{name}：{err}")
            return "", missing, name

        # v0.8.3：真实网盘 create_dir 拒绝 *（code=141「文件夹名包含非法字符: *」），
        # 影视主目录标记统一改为 ·；旧数据/旧配置里残留的 * 就地换成 · 再建目录。
        # 分类目录与季目录直传 A 库原名，绝不替换字符（原名里的中点不是标记）。
        effective = name.replace("*", NAME_MARK) if kind == "work" else name
        # 本轮已确认网盘连 · 也拒绝时，后续作品直接用无标记标准名，避免逐部重试
        if kind == "work" and plain_mode["on"]:
            effective = standard_media_name(effective)
        await asyncio.sleep(idle)
        fallback_note = ""
        try:
            made = await api.create_dir(parent_id, effective)
        except Exception as exc:
            first = str(exc)[:300]
            plain = standard_media_name(effective)
            if (kind == "work" and NAME_MARK in effective and plain != effective
                    and not plain_mode["tried"]):
                plain_mode["tried"] = True
                await asyncio.sleep(idle)
                try:
                    made = await api.create_dir(parent_id, plain)
                    plain_mode["on"] = True
                    effective = plain
                    fallback_note = (f"网盘拒绝带 · 的目录名（{first[:120]}），"
                                     "已自动降级为不带标记的标准影视名")
                    stats["dirs_fallback"] += 1
                    logger.warning("[mirror] %s：%s", name, fallback_note)
                except Exception as exc2:
                    err = f"create_dir 失败：{first}；降级标准名仍失败：{str(exc2)[:200]}"
            else:
                err = f"create_dir 失败：{first}"
            if not fallback_note:
                logger.warning("[mirror] 建目录失败 %s：%s", name, err)
                missing["error"] = err
                missing["tip"] = err
                _record_dir(db, rule.id, source_id, parent_id, name, kind, cat_path,
                            "", "missing", err)
                if len(stats["dir_errors"]) < 5:
                    stats["dir_errors"].append(f"{name}：{err}")
                return "", missing, name
        fid = str(made.get("file_id") or "")
        if not fid:
            err = f"create_dir 未返回 fileId（{effective}）"
            missing["error"] = err
            missing["tip"] = err
            _record_dir(db, rule.id, source_id, parent_id, effective, kind, cat_path,
                        "", "missing", err)
            if len(stats["dir_errors"]) < 5:
                stats["dir_errors"].append(f"{name}：{err}")
            return "", missing, effective
        cache.setdefault(parent_id, {})[key] = fid
        status = "created_fallback" if fallback_note else "created"
        _record_dir(db, rule.id, source_id, parent_id, effective, kind, cat_path,
                    fid, status, fallback_note)
        stats["dirs_created"] += 1
        return fid, None, effective

    works = _discover_works(nodes, root)
    for work, cats in works[:max_media]:
        cat_path = "/".join(n for n, _ in cats) or "__root__"
        title = standard_media_name(work["name"])
        expected = star_media_name(title) if rule.star_naming else title
        wkey = "work:" + normalize_work_name(work["name"])
        if wkey == "work:":
            stats["skipped"] += 1
            continue

        # 1:1 解析/创建 B 库分类目录（原名），再解析/创建影视主目录（带 ·）
        target_dir_id = b_root
        missing_all: list[dict] = []
        for cat_name, cat_id in cats:
            fid, miss, _actual = await _ensure_child(target_dir_id, cat_name,
                                                    "category", cat_id, cat_path)
            if miss:
                missing_all.append(miss)
                break
            target_dir_id = fid
        if not missing_all:
            fid, miss, actual = await _ensure_child(target_dir_id, expected, "work",
                                                    work["id"], cat_path)
            if miss:
                missing_all.append(miss)
            else:
                target_dir_id = fid
                # v0.8.3：降级为不带标记名称时，展示与实际网盘目录保持一致
                expected = actual

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
        all_rows = list(db.scalars(
            select(MirrorFile).where(MirrorFile.media_id == media.id)).all())
        # v0.8.4 关键修复：只有 status=synced 才代表「文件确实到了 B 库」。
        # 旧版把 copying / failed / unknown 也一起塞进 existing 判 unchanged，
        # 上一轮复制没成功的文件会被永久跳过、再点同步也没用——
        # 表现就是「B 库目录建好了、命名也对，里面却是空的」。
        synced_rows = {(m.source_file_id, m.fingerprint): m
                       for m in all_rows if m.status == "synced"}
        synced_identities = {m.identity for m in all_rows if m.status == "synced"}
        retry_rows = {(m.source_file_id, m.fingerprint): m
                      for m in all_rows if m.status in RETRYABLE_FILE_STATUS}
        # v0.8.7 补上另一半：DB 标记 synced **也不等于** B 库真的有文件。
        # 用户实测「B 库影视文件夹下为空目录」反复出现，根因是文件被删 /
        # 换了 B 库目录 / 复制任务没落地后，记录仍停在 synced，永远判 unchanged。
        # 每部作品先只读列一次 B 库，核对已同步记录是否真实存在。
        if synced_rows or retry_rows:
            candidates, b_verified = await _b_file_candidates(api, media)
        else:
            candidates, b_verified = {}, False
        # 只在「列目录完整可信」时判定缺失；接口抖动 / 分页不全一律不动 synced。
        b_file_count = len({v for k, v in candidates.items() if ":" in k})
        if b_verified and synced_rows:
            if b_file_count < len(synced_rows):
                for key, row in list(synced_rows.items()):
                    if _lookup_b_file(candidates, row):
                        continue
                    # 复用同一行重新复制，不新建行、不碰 A 库、不改任何目录名
                    row.status, row.error = "pending", "B 库未找到已同步文件，自动重新复制"
                    row.target_file_id = ""
                    del synced_rows[key]
                    retry_rows[key] = row
                    stats["missing_in_b"] += 1
                    stats["file_errors"].append(
                        f"{title}/{row.file_name}：B 库未找到已同步文件，已重新入复制队列")
            else:
                # B 库文件数不少于已同步记录数：多半是网盘改名 / 重名加了后缀，
                # 名字对不上而已。此时**绝不重置**，否则会重复复制整部作品。
                for row in synced_rows.values():
                    if not _lookup_b_file(candidates, row):
                        stats["name_mismatch"] += 1
        # 重试前先核对 B 库：任务其实已成功、只是轮询超时的，回填成 synced 后
        # 不再重发复制，避免产生重复文件（需求禁止「先删旧再复制」）。
        if retry_rows:
            for key, old in list(retry_rows.items()):
                # v0.8.6：改名后按 target_name 优先，A 原名 file_name 兜底
                hit = _lookup_b_file(candidates, old)
                if hit:
                    old.target_file_id = hit
                    old.status, old.error, old.task_id = "synced", "", old.task_id
                    stats["unchanged"] += 1
                    del retry_rows[key]
                    synced_rows[key] = old
            db.flush()
        sub_cache: dict[str, str] = {}
        copy_jobs: list[tuple[MirrorFile, str, str]] = []
        for container, item in _media_files(work, nodes):
            fid = str(item.get("fileId") or "")
            fname = _dir_name(item)
            if not fid or not _is_video(fname):
                continue
            fp = _fingerprint(item)
            if (fid, fp) in synced_rows:
                stats["unchanged"] += 1
                continue
            row = retry_rows.pop((fid, fp), None)
            if row is not None:
                # 复用旧记录重新复制，不新建行：避免唯一键冲突与历史留痕膨胀
                identity = row.identity
                row.status, row.error, row.task_id = "copying", "", ""
            else:
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
                for old in all_rows:
                    if old.identity == identity and old.status == "synced":
                        old.status = "superseded"
                        stats["changed"] += 1
            # 作品目录下的 Season/子目录在 B 库同名镜像（不加标记）
            target_parent = media.target_dir_id
            for sub_name in _relative_dir_names(container, work, nodes):
                key = f"{target_parent}|{normalize_work_name(sub_name)}"
                if key in sub_cache:
                    target_parent = sub_cache[key]
                    continue
                sub_id, miss, _actual = await _ensure_child(target_parent, standard_media_name(sub_name),
                                                            "season", container["id"], cat_path)
                if miss:
                    stats["missing_dirs"].append(miss)
                    stats["blocked"] += 1
                    target_parent = ""
                    break
                sub_cache[key] = sub_id
                target_parent = sub_id
            if not target_parent:
                if row is not None:
                    row.status, row.error = "failed", "B 库子目录未就绪，本次未发起复制"
                    stats["file_errors"].append(f"{title}/{fname}：B 库子目录未就绪")
                stats["skipped"] += 1
                continue
            if row is None:
                row = MirrorFile(media_id=media.id, rule_id=rule.id, identity=identity,
                                 source_file_id=fid, target_file_id="", file_name=fname,
                                 size=int(item.get("fileSize", 0) or 0),
                                 utime=int(item.get("utime", 0) or 0),
                                 fingerprint=fp, status="copying")
                db.add(row)
                db.flush()
                stats["new"] += 1
            else:
                stats["retries"] += 1
            copy_jobs.append((row, fid, target_parent))

        if not copy_jobs:
            # v0.8.4：还有未成功的文件记录时绝不标 synced，
            # 否则界面显示「已同步」而 B 库其实是空目录。
            pending_left = any(r.status != "synced" and r.status != "superseded"
                               for r in all_rows)
            media.status = "failed" if pending_left else "synced"
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
                msg = str(exc)[:300]
                row.status, row.error = "failed", msg
                stats["failed"] += 1
                stats["errors"].append(f"{title}/{row.file_name}：复制请求失败 {msg[:160]}")
                stats["file_errors"].append(f"{title}/{row.file_name}：{msg[:240]}")
                logger.warning("[mirror] copy_file 失败 %s/%s：%s", title, row.file_name, msg)
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
                    r_.status, r_.error = "synced", ""
                stats["done"] += 1
            elif task.status == "failed":
                for r_ in rows:
                    r_.status, r_.error = "failed", task.error
                stats["failed"] += 1
                msg = (f"{title}：复制任务 {task.task_id} 失败"
                       f"（{task.error or 'status=' + (task.raw_status or '未知')}）")
                stats["errors"].append(msg)
                stats["file_errors"].append(msg[:240])
            else:
                stats["unknown"] += 1
                msg = (f"{title}：复制任务 {task.task_id} 未返回 status=2"
                       f"（最后状态 {task.raw_status or '未知'}），已保留任务供继续轮询")
                stats["errors"].append(msg)
                stats["file_errors"].append(msg[:240])
        any_failed = any(r.status == "failed" for r, _, _ in copy_jobs)
        media.status = ("failed" if any_failed else
                        "synced" if tasks and all(t.status == "done" for t, _ in tasks)
                        else "syncing")
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
        f"影视 {stats['media']} 部 · 新增 {stats['new']} · 重试 {stats['retries']} · "
        f"变化 {stats['changed']} · 未变 {stats['unchanged']} · "
        f"任务完成 {stats['done']} · 复制失败 {stats['failed']} · 待确认 {stats['unknown']} · "
        f"跳过 {stats['skipped']} · B库缺失补复制 {stats['missing_in_b']} · "
        f"名称不符 {stats['name_mismatch']} · 建目录 {stats['dirs_created']} · "
        f"待建目录 {len(stats['missing_dirs'])} · 文件间隔 {idle}s")
    if stats["dir_errors"]:
        rule.last_message += " · 首个建目录错误：" + stats["dir_errors"][0][:300]
    # v0.8.4：文件级错误必须落到 last_message，刷新页面后仍看得到，
    # 不能只留在本次 POST /api/mirror/run 的响应里。
    if stats["file_errors"]:
        rule.last_message += " · 首个文件错误：" + stats["file_errors"][0][:300]
    if stats["dirs_fallback"]:
        rule.last_message += f" · 已降级标准名 {stats['dirs_fallback']} 次"
    rule.last_message = rule.last_message[:1000]
    db.commit()
    stats["ok"] = True
    return stats


async def _b_file_candidates(api: GuangYaWebFiles, media: MirrorMedia) -> tuple[dict[str, str], bool]:
    """只读列 B 库该作品目录（含直属子目录），返回 (候选映射, 列目录是否完整可信)。

    v0.8.4：既供 backfill 补齐映射，也供 sync_rule 重试前核对。
    v0.8.7：**必须区分「B 库确实空」与「没读到」**。`verified=False`（接口异常 /
    分页不完整）时调用方绝不能据此判断文件缺失，否则接口抖动会把已同步文件
    误判成丢失并重复复制。
    """
    # v0.8.7：改为**递归遍历**（旧版只列主目录 + 一层直属子目录）。剧集常在
    # `作品/Season 1/1080p/` 多层结构里，只列一层会把「文件其实在更深层」
    # 误判成「B 库空」，从而重复复制整部作品。
    listing: list[tuple[list[dict], bool]] = []
    stack = [str(media.target_dir_id or "")]
    visited: set[str] = set()
    truncated = False
    while stack:
        pid = stack.pop()
        if not pid or pid in visited:
            continue
        if len(visited) >= 60:  # 防止接口风暴；到上限即视为「不完整」
            truncated = True
            break
        visited.add(pid)
        try:
            items, ok = await _list_dir(api, pid)
        except Exception as exc:
            logger.debug("[mirror] B 库列目录失败 %s：%s", pid, exc)
            listing.append(([], False))
            continue
        listing.append((items, ok))
        for it in items:
            if int(it.get("resType", 1) or 1) == 2 and it.get("fileId"):
                stack.append(str(it["fileId"]))
    verified = bool(listing) and not truncated and all(o for _, o in listing)
    candidates: dict[str, str] = {}
    for items, _ in listing:
        for it in items:
            if int(it.get("resType", 1) or 1) == 2:
                continue
            fid = str(it.get("fileId") or "")
            name = _dir_name(it)
            size = int(it.get("fileSize", 0) or 0)
            if fid:
                candidates[f"{normalize_work_name(name)}:{size}".rstrip(":")] = fid
                candidates[normalize_work_name(name)] = fid
    return candidates, verified


async def backfill_target_file_ids(db: Session, rule: MirrorRule) -> int:
    """复制接口只返回 taskId、不返回 B fileId；镜像后用只读列目录补齐映射。

    只按「目标目录 + 文件名 + 大小」匹配，不写 Resource、不建分享、不入推送队列。
    v0.8.4：失败/未确认的记录若在 B 库实际存在，也一并回填并纠正为 synced，
    避免下一轮把已经复制好的文件误当成失败再发一次。
    """
    api = GuangYaWebFiles(db)
    updated = 0
    medias = list(db.scalars(select(MirrorMedia).where(
        MirrorMedia.rule_id == rule.id, MirrorMedia.target_dir_id != "")).all())
    for media in medias:
        pending = list(db.scalars(select(MirrorFile).where(
            MirrorFile.media_id == media.id, MirrorFile.target_file_id == "",
            MirrorFile.status.in_(["synced", "copying", "failed", "unknown", "pending"]))).all())
        if not pending:
            continue
        candidates, _verified = await _b_file_candidates(api, media)
        if not candidates:
            continue
        for row in pending:
            # v0.8.6：B 库文件可能已改成英文名，优先按 target_name 反查
            hit = _lookup_b_file(candidates, row)
            if hit:
                row.target_file_id = hit
                row.status, row.error = "synced", ""
                row.updated_at = utcnow()
                updated += 1
        db.commit()
    return updated


# ---------- v0.8.6：B 库单集/单电影文件按英文剧名改名 ----------
async def _resolve_english_title(db: Session, media: MirrorMedia, *,
                                 force: bool = False) -> tuple[str, str]:
    """每部作品只解析一次英文剧名，缓存到 `MirrorMedia.english_title`。

    优先级：TMDB en-US 官方英文片名 → 已识别 `Resource.original_title`（纯 ASCII）
    → AI 翻译中文剧名。都没有就返回空串，由调用方列入「英文名待处理」清单，
    **绝不瞎猜、绝不把中文原名当英文剧名写进文件名**。
    """
    cached = str(getattr(media, "english_title", "") or "").strip()
    if cached and not force:
        return cached, str(getattr(media, "english_source", "") or "cached")
    core, year = work_core_title(media.title_standard)
    eng, source = "", ""
    if not core:
        media.english_title, media.english_source = "", ""
        db.flush()
        return "", ""

    # 1) 收集可复用的识别结果：A 目录里的 {tmdb-} 标记 + 已回填 B fileId 的 Resource
    tm = _TMDB_TAG.search(str(media.title_standard or ""))
    tid = int(tm.group(1)) if tm else None
    mtype = str(media.media_type or "").strip()
    b_ids = [str(x) for x in db.scalars(select(MirrorFile.target_file_id).where(
        MirrorFile.media_id == media.id,
        MirrorFile.target_file_id != "").limit(5)).all()]
    res = None
    if b_ids:
        res = db.scalar(select(Resource).where(
            Resource.file_id.in_(b_ids), Resource.tmdb_id.isnot(None)).limit(1))
    if res:
        tid = tid or int(res.tmdb_id)
        mtype = mtype or str(res.media_type or "")
        orig = sanitize_english_title(str(res.original_title or ""))
        if orig:
            eng, source = orig, "resource"

    # 2) TMDB en-US 优先：官方英文片名比 AI 直译稳定，且每部只请求一次
    if tid and not eng:
        order = [mtype] if mtype in ("movie", "tv") else ["movie", "tv"]
        for mt in order:
            try:
                v = sanitize_english_title(await tmdb.english_title(tid, mt, db))
            except Exception as exc:
                logger.debug("[mirror] TMDB 英文片名 %s/%s 不可用：%s", tid, mt, exc)
                continue
            if v:
                eng, source = v, "tmdb"
                break

    # 3) AI 翻译兜底（未配置时静默降级，不阻断同步）
    if not eng:
        try:
            tr = await ai_assist.translate_title(core, year, db)
            titles = tr.get("titles") or {}
            cand = str(titles.get("en") or (tr.get("queries") or [""])[0])
            v = sanitize_english_title(cand)
            if v:
                eng, source = v, "ai"
        except Exception as exc:
            logger.debug("[mirror] AI 翻译英文剧名失败：%s", exc)

    media.english_title, media.english_source = eng, source
    db.flush()
    return eng, source


async def rename_mirror_files(db: Session, rule: MirrorRule, *,
                              force: bool = False, max_media: int = 100) -> dict:
    """把 B 库已同步的单集 / 单电影文件按英文剧名改名（只换中文片名段）。

    边界：
      - 只对 **B 库 fileId** 调 `rename_file`；A 源头库全程只读；
      - 不动 B 库分类目录 / 影视主目录 / 季目录的名字（保持 `·` 约定）；
      - 不重新复制文件、不写 Resource、不建分享、不入推送队列；
      - `MirrorFile.file_name` 保持 A 原名，改名结果写 `target_name`；
        增量比对仍走 `source_file_id + fingerprint(size:utime)`，
        后期 A 库文件变量更新照常能识别出来并触发更新。
    """
    idle = max(float(getattr(rule, "file_idle_seconds", 1.0) or 1.0), MIN_FILE_IDLE_SECONDS)
    stats = {"works": 0, "renamed": 0, "skipped": 0, "ascii": 0,
             "untranslated": 0, "failed": 0, "errors": [],
             "untranslated_works": [], "idle_seconds": idle}
    api = GuangYaWebFiles(db)
    medias = list(db.scalars(select(MirrorMedia).where(
        MirrorMedia.rule_id == rule.id, MirrorMedia.target_dir_id != "")).all())
    for media in medias[:max_media]:
        eng, _source = await _resolve_english_title(db, media, force=force)
        if not eng:
            stats["untranslated"] += 1
            if len(stats["untranslated_works"]) < 50:
                stats["untranslated_works"].append({
                    "work": media.title_standard,
                    "reason": "TMDB en-US 与 AI 翻译都没给出可用英文剧名，"
                              "请检查 TMDB 密钥 / AI 配置，或等该作品被识别后重跑"})
            continue
        core, _year = work_core_title(media.title_standard)
        rows = list(db.scalars(select(MirrorFile).where(
            MirrorFile.media_id == media.id,
            MirrorFile.status == "synced")).all())
        if not rows:
            continue
        # 只读列一次 B 库：既定位 fileId，也避免 A 库内容更新后新旧版本同名冲突
        candidates, _verified = await _b_file_candidates(api, media)
        name_fids = {k: v for k, v in candidates.items() if ":" not in k}
        first = True
        for row in rows:
            target = english_file_name(row.file_name, core, eng)
            if not target:
                stats["ascii"] += 1
                row.rename_status = "skipped_ascii"
                continue
            if row.target_name == target and row.rename_status == "done":
                stats["skipped"] += 1
                continue
            fid = str(row.target_file_id or "") or _lookup_b_file(candidates, row)
            if not fid:
                row.rename_status, row.rename_error = "failed", "B 库未找到对应 fileId，未改名"
                stats["failed"] += 1
                stats["errors"].append(
                    f"{media.title_standard}/{row.file_name}：B 库未找到对应 fileId")
                continue
            final, n = target, 1
            while (normalize_work_name(final) in name_fids
                   and name_fids[normalize_work_name(final)] != fid and n < 20):
                n += 1
                final = with_conflict_suffix(target, n)
            if not first:
                await asyncio.sleep(idle)  # 与复制/建目录共用同一风控间隔
            first = False
            try:
                await api.rename_file(fid, final)
            except Exception as exc:
                msg = str(exc)[:300]
                row.rename_status, row.rename_error = "failed", msg
                stats["failed"] += 1
                stats["errors"].append(
                    f"{media.title_standard}/{row.file_name}：改名失败 {msg[:160]}")
                logger.warning("[mirror] rename 失败 %s/%s：%s",
                               media.title_standard, row.file_name, msg)
                continue
            row.target_file_id = fid
            row.target_name = final
            row.rename_status, row.rename_error = "done", ""
            row.updated_at = utcnow()
            name_fids[normalize_work_name(final)] = fid
            stats["renamed"] += 1
            db.flush()
        stats["works"] += 1
        db.commit()
    return stats


async def poll_pending_tasks(db: Session, rule: MirrorRule, *,
                             max_polls: int = 5, interval: float = 2.0) -> dict:
    """继续轮询此前 status 未确认的复制任务（其它 status 取值含义未验证，不猜）。"""
    api = GuangYaWebFiles(db)
    tasks = list(db.scalars(select(MirrorTask).where(
        MirrorTask.rule_id == rule.id, MirrorTask.status.in_(["pending", "running", "unknown"]))).all())
    done = 0
    failed = 0
    for task in tasks:
        task.status = "running"
        rows = list(db.scalars(select(MirrorFile).where(
            MirrorFile.task_id == task.task_id)).all())
        if await _poll_task(api, task, max_polls=max_polls, interval=interval):
            done += 1
            for row in rows:
                row.status, row.error = "synced", ""
            if task.media_id:
                media = db.get(MirrorMedia, task.media_id)
                if media:
                    media.status = "synced"
                    media.last_synced_at = utcnow()
        elif task.status == "failed":
            # v0.8.4：明确失败的任务把文件标 failed，下一轮同步会重新发起复制
            failed += 1
            for row in rows:
                row.status, row.error = "failed", task.error
            if task.media_id:
                media = db.get(MirrorMedia, task.media_id)
                if media:
                    media.status = "failed"
    db.commit()
    return {"tasks": len(tasks), "done": done, "failed": failed,
            "still_unknown": len(tasks) - done - failed}
