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
  8. v0.8.9：**绝不跨 `await` 持有 SQLite 写事务**。本模块所有写操作一律用
     `db.commit()`，不保留只 flush 不提交的写法：复制/改名循环里每个文件之间都有
     `await asyncio.sleep(idle)`、`await api.copy_file/rename_file`、
     `await _poll_tasks_batch()`，这些网络等待动辄几十秒。SQLite 只有一个写者，
     只要事务没提交，管理页所有要写库的请求就会排到 30 秒 busy_timeout，
     表现就是「整理剧集改名时后台页面卡死打不开」。v0.8.8 只在 scanner.run_scan()
     修过同类问题，本模块漏了；此处统一收紧，勿改回 flush。

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
import time

# v0.9.21：镜像锁进度心跳钩子。默认 no-op；main.py 在 lifespan 注入
# `_mirror_lock.heartbeat`，让长复制/改名轮询每轮证明「还在进展」。这里不 import
# main，避免循环依赖；钩子失败绝不影响复制主链路。
_progress_heartbeat = None


def set_progress_heartbeat(fn) -> None:
    global _progress_heartbeat
    _progress_heartbeat = fn


def _beat() -> None:
    if _progress_heartbeat is not None:
        try:
            _progress_heartbeat()
        except Exception:
            pass  # 心跳是辅助诊断，绝不阻断同步

from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from . import ai_assist, mirror_batch, tmdb
from .db import SessionLocal
from .file_naming import (english_file_name, pinyin_title, sanitize_english_title,
                          with_conflict_suffix, work_core_title)
from .guangya import GuangYaWebFiles, WebFileError
from .models import MirrorDir, MirrorFile, MirrorMedia, MirrorRule, MirrorTask, Resource
from .utils import (NAME_MARK, VIDEO_EXTS, media_identity, normalize_work_name,
                    parse_media_name, standard_media_name, star_media_name, utcnow)

logger = logging.getLogger(__name__)

# v0.8.9：长剧集分批改名。《中国通史》这类百集作品一轮连发上百次 rename，
# 每个文件间隔 idle 秒，网盘偶发 429/5xx 时 _post_web 还会退避重试三次
# （1+2+4 秒），单轮轻松超过前端 300 秒超时 → 界面报「改名失败」但其实
# 已改了一部分。每轮只处理有限文件 / 有限时长，剩下的下一轮自动续改：
# 每个文件改名后立即 commit，未处理的行 rename_status 仍为空，下轮会再被选中。
RENAME_MAX_FILES = 50
RENAME_MAX_SECONDS = 90.0
RENAME_MAX_CONSECUTIVE_FAILURES = 5

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
# v1.2 需求①：单次 copy_file 的 fileIds 上限。光鸭网页端多选文件复制就是把
# 所有选中条目的 fileId 塞进一个数组（前端 JS 契约 index.4998cfbcbd.js），
# 但一次请求体不宜过大，按目录分组后每批最多 50 个文件，超出再分片。
# 注意：光鸭 copy_file 不支持「复制整个目录」（无 copy_dir 接口、复制也不回传
# 副本 fileId），所以「最大单元」在 API 能力内的落地＝同一目标目录下多文件
# 一次批量提交，把「逐文件 N 次请求」降为「按目录 M 次请求」（M<<N）。
COPY_BATCH_SIZE = 50
# v0.8.4：只有 synced 才表示「文件确实到了 B 库」。这些状态必须重试，
# 绝不能和 synced 一起塞进 existing 判 unchanged（否则永久卡死）。
RETRYABLE_FILE_STATUS = {"pending", "copying", "failed", "unknown"}
# v0.9.6：prune 已清理的目录映射标记。带此标记的 missing 行是「B 库不存在、映射已清、
# 下轮同步从 A 库重建」的僵尸行，不需用户手工建目录；展示层据此从「待创建目录」排除，
# 并在用户确认后由 /api/mirror/mappings/clear-pruned 真正删除（不违反自动 prune 不删行的红线）。
PRUNED_DIR_MARK = "[pruned]"
# v0.9.6.1：v0.8.9 起 prune 写入的旧文案没有 [pruned] 标记。历史库里这类行
# 仍会混进「待创建目录」、也无法被清空端点识别。这里保留旧文案特征，运行期
# 与启动迁移都按同一口径识别，不要求用户先升级到某一版再等下一轮 prune。
PRUNED_DIR_LEGACY_TEXT = "B 库该目录已不存在，映射已清理，下轮同步自动重建"
PRUNED_DIR_TEXT_HINT = "B 库该目录已不存在，映射已清理"


def is_pruned_dir_error(error: object) -> bool:
    """判断一条 missing 目录错误是否属于「已清理、下轮自动重建」僵尸行。

    两种都认：v0.9.6 新写的 `[pruned]` 前缀，和 v0.8.9 无标记的历史文案。
    只用精确业务文案判断，不按「missing」宽泛猜——真正 create_dir 失败的
    行仍要留在待创建目录里让用户处理。
    """
    text = str(error or "")
    if text.startswith(PRUNED_DIR_MARK):
        return True
    return PRUNED_DIR_TEXT_HINT in text or PRUNED_DIR_LEGACY_TEXT in text

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
def natural_key(name: str) -> tuple:
    """阶段二路线B C2：确定性自然排序键。S02 排在 S10 前、E9 排在 E10 前。

    返回由 (类型标记, 数值, 文本) 三元组组成的元组，保证任意片段可比较
    （纯 int/str 混排在 Python3 会 TypeError）。仅用于决定遍历/处理顺序，
    不参与身份、指纹、唯一键或映射语义。
    """
    parts = re.split(r"(\d+)", str(name or ""))
    key: list[tuple[int, int, str]] = []
    for p in parts:
        if not p:
            continue
        if p.isdigit():
            key.append((1, int(p), ""))
        else:
            key.append((0, 0, p.lower()))
    return tuple(key)


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
    # 阶段二路线B C2：根子目录按自然序反序入栈，stack.pop() 得到稳定正序。
    root_children = sorted(root_node.get("children") or [],
                           key=lambda cid: natural_key(nodes.get(cid, {}).get("name", "")))
    stack = [(cid, []) for cid in reversed(root_children)]
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
            # C2：分类目录下探同样按自然序反序入栈，保持 DFS 稳定
            kids = sorted(node.get("children") or [],
                          key=lambda cid: natural_key(nodes.get(cid, {}).get("name", "")))
            for child in reversed(kids):
                stack.append((child, cats + [(node["name"], did)]))
            continue
        out.append((node, cats))
    return out


def work_key_of(work: dict) -> str:
    """作品稳定键，与 sync_rule / MirrorMedia.work_key 同口径（供轮转游标持久化）。"""
    return "work:" + normalize_work_name(work.get("name") or "")


def plan_work_batch(works: list, max_works: int, cursor: str = "",
                    hard_max: int = 100) -> tuple[list, str, bool]:
    """v0.9.0：把一次镜像要处理的作品按「单轮上限 + 轮转游标」切成一批。

    - max_works<=0：不限量，返回全部（仍受 hard_max 防御性上限约束，保持旧行为）。
    - max_works>0：从 cursor 之后接着取，取满 max_works 部为止；到末尾则从头回绕。
      返回 (本批作品, 新的游标=本批最后一部作品的 key, 是否发生回绕)。
    - 游标是「上一轮最后处理完的作品 key」，因此同一部剧绝不会被拆到两轮中间：
      sync_rule 对每部作品是原子处理（文件复制 + 批量轮询完成才进入下一部），
      正好满足用户「第一部剧不要间断，整理完再休息」。
    - 到上限即停，剩余作品留给下一次定时同步接着做，不铺满一屏、不瞬时压满网盘。
    """
    if not works:
        return [], "", False
    if max_works <= 0:
        return works[:hard_max], "", False
    keys = [work_key_of(w) for w, _ in works]
    start = 0
    if cursor:
        try:
            start = keys.index(cursor) + 1
        except ValueError:
            start = 0  # 游标指向的作品已不在 A 库，从头开始
        if start >= len(works):
            start = 0  # 上一轮已到末尾，回绕
    end = start + max_works
    batch = works[start:end]
    wrapped = end > len(works)
    if wrapped and len(batch) < max_works:
        # 尾部不足，用头部补齐，凑满一整批
        batch = batch + works[:max_works - len(batch)]
    new_cursor = work_key_of(batch[-1][0]) if batch else ""
    return batch, new_cursor, wrapped


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
    db.commit()


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
        _beat()  # v0.9.21：每轮轮询打点，长剧复制持续推进不会被误判为卡死
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


async def _wait_one_task(api: "GuangYaWebFiles", task: MirrorTask, *,
                         max_polls: int, interval: float) -> str:
    """严格顺序：轮询单个复制任务直到终态，返回 done/failed/unknown。

    复用 _task_outcome 的真实语义（status=2 完成、3 或 detail.code≠0 失败）。
    unknown 不猜含义，保留 status 供 poll_pending_tasks 续跑。
    """
    for i in range(max_polls):
        if i:
            await asyncio.sleep(interval)
        _beat()
        try:
            r = await api.get_task_status(task.task_id)
        except Exception as exc:
            task.error = str(exc)[:300]
            task.polls = i + 1
            continue
        await asyncio.sleep(TASK_STATUS_MIN_INTERVAL)
        raw = r.get("status")
        task.raw_status = "" if raw is None else str(raw)
        task.polls = i + 1
        outcome, err = _task_outcome(raw, r.get("detail") or {})
        if outcome == "done":
            task.status, task.error, task.task_status = "done", "", "completed"
            task.finished_at = utcnow()
            return "done"
        if outcome == "failed":
            task.status, task.error, task.task_status = "failed", err[:300], "failed"
            task.finished_at = utcnow()
            return "failed"
    task.status, task.task_status = "unknown", "pending"  # 待下轮续跑
    return "unknown"


async def _copy_jobs_strict(db: Session, api: "GuangYaWebFiles", rule: MirrorRule,
                            media: MirrorMedia, title: str,
                            copy_jobs: list[tuple["MirrorFile", str, str]],
                            stats: dict, *, idle: float, max_polls: int,
                            poll_interval: float, batch_id: str,
                            seq_start: int = 0) -> int:
    """阶段二路线B C3：严格顺序复制执行核心。

    语义：对 copy_jobs（已按自然序）逐个「远端核验 → 提交 copy_file →
    轮询到终态 → 更新 MirrorFile」，当前文件确认完成（或明确失败/人工跳过）
    后才推进下一个。每个写操作后立即 commit，绝不跨 await 持写事务。

    控制指令在"任务边界"检查（不打断正在进行的 copy_file/轮询）：
      pause → 置批次 paused 并立即停止推进（余下保持 pending，resume 后续跑）；
      skip  → 当前任务标 skipped 并推进；
      cancel→ 余下任务全部标 cancelled 并停止。

    返回本轮推进到的最后一个 sequence_id（供游标）。
    """
    seq = seq_start
    for row, source_fid, target_parent in copy_jobs:
        seq += 1
        # ---- 任务边界：检查批次控制指令 ----
        action = mirror_batch.consume_control(db, rule)
        if action == "pause":
            rule.batch_status = "paused"
            db.commit()
            stats["paused"] = True
            return seq - 1
        if action == "cancel":
            rule.batch_status = "cancelled"
            # 阶段二 C5：余下未终结序列任务全部落 cancelled（docstring 承诺兑现）
            try:
                n = mirror_batch.cancel_remaining(db, batch_id)
                stats["cancelled_tasks"] = n
            except Exception:
                pass
            db.commit()
            stats["cancelled"] = True
            return seq - 1
        if action == "skip":
            # 阶段二 C5：当前任务落 skipped 终态（即使尚未 add_sequence_task 也补占位行）
            try:
                mirror_batch.mark_sequence_status(db, batch_id, seq, "skipped",
                                                  error="skip by control")
            except Exception:
                pass
            stats["skipped"] += 1
            db.commit()
            continue
        if rule.batch_status in ("paused", "cancelled"):
            stats["paused"] = True
            return seq - 1

        if seq > 1:
            await asyncio.sleep(idle)
        _beat()

        # ---- 远端核验：防止重复复制（复用已验证的按名+大小回填）----
        try:
            candidates, verified = await _b_file_candidates(api, media)
        except Exception:
            candidates, verified = {}, False
        if verified and _lookup_b_file(candidates, row):
            row.status, row.error, row.target_file_id = "synced", "", _lookup_b_file(candidates, row)
            db.commit()
            stats["unchanged"] += 1
            continue

        # ---- 占位行：先落序列任务，再调 copy_file（修 B-09 重启窗口）----
        task = mirror_batch.add_sequence_task(
            db, rule_id=rule.id, media_id=media.id, batch_id=batch_id,
            sequence_id=seq, parent_sequence_id=seq_start, note=f"{title}/{row.file_name}")
        task.task_status = "submitting"
        db.commit()
        try:
            task_id = await api.copy_file([source_fid], target_parent)
        except Exception as exc:
            msg = str(exc)[:300]
            row.status, row.error = "failed", msg
            task.status, task.task_status, task.error = "failed", "failed", msg
            task.finished_at = utcnow()
            stats["failed"] += 1
            stats["errors"].append(f"{title}/{row.file_name}：复制请求失败 {msg[:160]}")
            stats["file_errors"].append(f"{title}/{row.file_name}：{msg[:240]}")
            logger.warning("[mirror] strict copy_file 失败 %s/%s：%s", title, row.file_name, msg)
            db.commit()
            continue
        task.task_id = task_id
        task.task_status = "submitted"
        row.task_id = task_id
        db.commit()
        stats["tasks"] += 1

        # ---- 严格顺序核心：等本任务终结才推进下一个 ----
        task.task_status = "verifying"
        db.commit()
        outcome = await _wait_one_task(api, task, max_polls=max_polls, interval=poll_interval)
        if outcome == "done":
            row.status, row.error = "synced", ""
            stats["done"] += 1
        elif outcome == "failed":
            row.status, row.error = "failed", task.error
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
        db.commit()
        rule.cursor_sequence = seq
        db.commit()
    return seq


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
                       max_dirs: int = 400,
                       stats: dict | None = None) -> tuple[dict[str, dict], bool]:
    """只读遍历 A 库目录树，返回 {dirId: 节点} 与批次完整性。

    节点含 id / name / parent / path / files(直属文件) / children。
    不写任何数据库表，不触碰 Resource —— 扫描 A 库绝不能污染 B 库资源库。

    v0.9.8 #196：stats 可选，用于回显「本轮列了几个目录、发了几次列目录请求、
    看到多少 A 库文件」——让用户确认慢在列目录（必要开销）而非全量复制。
    """
    root = str(root_id or "*")
    nodes: dict[str, dict] = {}
    stack = [(root, "", "", "")]
    complete = True
    a_files = 0
    list_calls = 0
    while stack and len(nodes) < max_dirs:
        did, name, path, parent = stack.pop()
        if did in nodes:
            continue
        display = (path + "/" + name).strip("/") if name else path
        items, ok = await _list_dir(api, did)
        list_calls += 1
        complete = complete and ok
        node = {"id": did, "name": name, "parent": parent, "path": display,
                "files": [], "children": []}
        nodes[did] = node
        # 阶段二路线B C2：确定性排序。先按 natural_key 排序再反序入栈，
        # 使 stack.pop() 按自然序访问；files 同理保持自然序。仅为顺序，不改语义。
        ordered = sorted(items, key=lambda it: natural_key(_dir_name(it)))
        dirs_to_push: list[tuple[str, str, str, str]] = []
        for it in ordered:
            fid = str(it.get("fileId") or "")
            if not fid:
                continue
            if int(it.get("resType", 1) or 1) == 2:
                node["children"].append(fid)
                dirs_to_push.append((fid, _dir_name(it), display, did))
            else:
                node["files"].append(it)
                a_files += 1
        stack.extend(reversed(dirs_to_push))
    if stats is not None:
        stats["walk_dirs"] = len(nodes)
        stats["walk_list_calls"] = list_calls
        stats["walk_a_files"] = a_files
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
    seen: set[str] = set()
    # 阶段二路线B C2：确定性排序——父目录先、子目录按自然序，文件按自然序。
    # 用递归 DFS 保证"目录整体先于其子树"的稳定顺序，避免栈序受网盘返回顺序影响。
    def visit(did: str) -> None:
        if did in seen:
            return
        seen.add(did)
        node = nodes.get(did)
        if not node:
            return
        for it in sorted(node["files"], key=lambda f: natural_key(_dir_name(f))):
            out.append((node, it))
        for cid in sorted(node["children"],
                          key=lambda x: natural_key(nodes.get(x, {}).get("name", ""))):
            visit(cid)

    visit(work["id"])
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
                    poll_interval: float = 2.0, max_media: int = 100,
                    max_works: int | None = None,
                    strict: bool = False) -> dict:
    """执行一次 A→B 增量同步。逐部影视完成后立即提交，不等整库结束。

    strict=False（默认，旧路径）：作品内批量提交 copy_file + 批量轮询。
    strict=True（阶段二路线B C3）：严格顺序——一个文件复制并确认完成后才提交
    下一个，带批次序列持久化与暂停/恢复/跳过/取消控制点。其余逻辑完全复用。

    v0.8.1：
      - 分类目录 1:1 镜像到 B 库，名称不修改；只有影视主目录按 star_naming 加 ·。
      - B 库缺目录时自动 create_dir（抓包已确认，可用 auto_create_dirs 关闭）。
      - 每次文件复制之间留可配置空闲时间，硬下限 MIN_FILE_IDLE_SECONDS 不可调低。
      - 本模块绝不写 push_queue、绝不调用 _enqueue_* / process_queue。
    v0.9.0：
      - max_works 单轮整部作品上限；None 时取 rule.max_works_per_run。
        由 plan_work_batch 按轮转游标切批，做到「一次不铺太多、下次接着做、
        同一部剧不拆两轮」。
    """
    idle = max(float(getattr(rule, "file_idle_seconds", 1.0) or 1.0), MIN_FILE_IDLE_SECONDS)
    if max_works is None:
        try:
            max_works = max(0, int(getattr(rule, "max_works_per_run", 0) or 0))
        except (TypeError, ValueError):
            max_works = 0
    stats = {"media": 0, "new": 0, "changed": 0, "unchanged": 0, "skipped": 0,
             "tasks": 0, "done": 0, "unknown": 0, "blocked": 0,
             "retries": 0, "failed": 0,
             # v0.9.0：单轮作品上限与轮转游标回显
             "max_works": max_works, "batch_size": 0, "works_cursor": "", "wrapped": False,
             # v0.8.7：DB 标记 synced 但 B 库实际没有 → 自动补复制的计数
             "missing_in_b": 0, "name_mismatch": 0,
             "dirs_created": 0, "dirs_mapped": 0, "dirs_fallback": 0,
             "dir_errors": [], "missing_dirs": [], "file_errors": [],
             "idle_seconds": idle, "errors": [],
             # v0.9.8 #196：列目录开销诊断（慢在列目录而非全量复制）
             "walk_dirs": 0, "walk_list_calls": 0, "walk_a_files": 0}
    if not str(rule.source_root_id or "").strip() or not str(rule.target_root_id or "").strip():
        raise WebFileError("请先配置 A 源头库目录 ID 与 B 镜像库目录 ID")
    api = GuangYaWebFiles(db)
    # 阶段二路线B C3：严格模式开启批次，全批次共享 sequence 游标
    batch_id = ""
    seq_cursor = 0
    if strict:
        batch_id = mirror_batch.start_batch(db, rule)
        stats["batch_id"] = batch_id
    stats["purged_legacy"] = _purge_legacy_failed(db, rule.id)
    nodes, complete = await _walk_source(api, rule.source_root_id, stats=stats)
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
    # v0.9.0：按单轮上限 + 轮转游标切批。max_works<=0 时退化为旧行为（全量，
    # 仍受 max_media 防御上限约束）；>0 时本批最多 max_works 部，剩余下轮继续。
    if max_works > 0:
        batch, new_cursor, wrapped = plan_work_batch(
            works, max_works, str(getattr(rule, "works_cursor", "") or ""),
            hard_max=max_media)
    else:
        batch, new_cursor, wrapped = works[:max_media], "", False
    stats["batch_size"] = len(batch)
    stats["works_cursor"] = new_cursor
    stats["wrapped"] = wrapped
    stats["total_works"] = len(works)
    for work, cats in batch:
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
            db.commit()
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
                    # v0.8.8：补复制回来的是 **A 库中文原名**，旧改名状态必须作废。
                    # 否则 rename_mirror_files() 看到 rename_status=done 会整批跳过，
                    # 表现就是用户反馈的「现在文件是有了，不过汉字名称没有翻译」。
                    row.target_name = ""
                    row.rename_status = ""
                    row.rename_error = ""
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
            db.commit()
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
                db.commit()
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

        if strict:
            # 阶段二路线B C3：严格顺序——逐文件「核验→提交→确认完成→下一个」
            seq_cursor = await _copy_jobs_strict(
                db, api, rule, media, title, copy_jobs, stats,
                idle=idle, max_polls=max_polls, poll_interval=poll_interval,
                batch_id=batch_id, seq_start=seq_cursor)
            any_failed = any(r.status == "failed" for r, _, _ in copy_jobs)
            any_unknown = any(r.task_id and r.status not in ("synced", "failed", "superseded")
                              for r, _, _ in copy_jobs)
            media.status = ("failed" if any_failed else
                            "synced" if not any_unknown else "syncing")
            media.last_synced_at = utcnow()
            db.commit()
            stats["media"] += 1
            # 暂停/取消：批次边界立即停止推进后续作品
            if stats.get("paused") or stats.get("cancelled"):
                break
        else:
            # v1.2 需求①：先扫描文件树（_walk_source/_b_file_candidates 已完成），
            # 以 B 库目标目录为「最大单元」分组批量复制：同一 parent 下待复制的
            # 多文件合并成一次 copy_file(fids[], parent)，把「逐文件 N 次请求
            # + N 次 idle」降为「按目录 M 次请求 + M 次 idle」(M<<N)，显著提速。
            # 光鸭 copy_file 契约只支持「多文件→同一 parent」，不支持复制整个目录，
            # 故目录骨架仍由上面的 _ensure_child 逐层建好、副本 ID 由 backfill 回填。
            buckets: dict[str, list[tuple[MirrorFile, str]]] = {}
            for row, source_fid, target_parent in copy_jobs:
                buckets.setdefault(target_parent, []).append((row, source_fid))
            stats["copy_batches"] = len(buckets)
            tasks: list[tuple[MirrorTask, list[MirrorFile]]] = []
            batch_no = 0
            for target_parent, members in buckets.items():
                # 单目录文件数超上限再分片，避免单次请求体过大
                for start in range(0, len(members), COPY_BATCH_SIZE):
                    chunk = members[start:start + COPY_BATCH_SIZE]
                    if batch_no:
                        await asyncio.sleep(idle)
                    _beat()  # v0.9.21：每批复制推进打点
                    fids = [fid for _row, fid in chunk]
                    try:
                        task_id = await api.copy_file(fids, target_parent)
                    except Exception as exc:
                        msg = str(exc)[:300]
                        stats["failed"] += len(chunk)
                        for row, _fid in chunk:
                            row.status, row.error = "failed", msg
                        stats["errors"].append(
                            f"{title}/{len(chunk)} 个文件批量复制失败 {msg[:160]}")
                        stats["file_errors"].extend(
                            f"{title}/{row.file_name}：{msg[:240]}" for row, _fid in chunk)
                        logger.warning("[mirror] 批量 copy_file 失败 %s/%s 文件：%s",
                                       title, len(chunk), msg)
                        db.commit()
                        batch_no += 1
                        continue
                    task = MirrorTask(rule_id=rule.id, media_id=media.id, task_id=task_id,
                                      status="running")
                    db.add(task)
                    db.commit()
                    for row, _fid in chunk:
                        row.task_id = task_id
                    tasks.append((task, [row for row, _fid in chunk]))
                    stats["tasks"] += 1
                    batch_no += 1
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

    # v0.9.0：轮转游标落库，下一次定时同步从本批最后一部之后接着做。
    # 仅在配置了单轮上限时写；0（不限量）时清空，避免升级后残留旧游标。
    if max_works > 0 and batch:
        rule.works_cursor = new_cursor
    elif max_works <= 0:
        rule.works_cursor = ""
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
    # v0.9.8 #196：明确「慢在列目录、复制是增量的」——用户担心全量跑任务时据此判断
    rule.last_message += (f" · 列目录 {stats['walk_dirs']} 个/{stats['walk_list_calls']} 次请求"
                          f" · A库文件 {stats['walk_a_files']} · 未变跳过 {stats['unchanged']}")
    if max_works > 0:
        _left = max(0, stats.get("total_works", 0) - stats["batch_size"])
        rule.last_message += (f" · 本轮限额 {max_works} 部 / 实处理 {stats['batch_size']} 部"
                              f"（A 库共 {stats.get('total_works', 0)} 部，余 {_left} 部下轮继续）")
    if stats["dir_errors"]:
        rule.last_message += " · 首个建目录错误：" + stats["dir_errors"][0][:300]
    # v0.8.4：文件级错误必须落到 last_message，刷新页面后仍看得到，
    # 不能只留在本次 POST /api/mirror/run 的响应里。
    if stats["file_errors"]:
        rule.last_message += " · 首个文件错误：" + stats["file_errors"][0][:300]
    if stats["dirs_fallback"]:
        rule.last_message += f" · 已降级标准名 {stats['dirs_fallback']} 次"
    if strict and batch_id:
        stats["seq_cursor"] = seq_cursor
        stats["batch_status"] = rule.batch_status
        # paused 保留状态供 resume 续跑；cancelled/idle 正常收尾
        if rule.batch_status not in ("paused", "cancelled"):
            mirror_batch.finish_batch(db, rule, status="idle")
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
# v0.8.9：英文名解析的每个外部调用都必须有硬上限。TMDB 详情 httpx 超时 20 秒、
# AI 模型可能更久；旧版对 media_type 未知的作品还会串行查 movie + tv 再等 AI，
# 一部剧就能把整轮同步卡几十秒，管理页直接打不开。
ENGLISH_TITLE_TMDB_TIMEOUT = 6.0
ENGLISH_TITLE_AI_TIMEOUT = 10.0
# v0.9.2：复制任务成功后，B 库文件可见可能有秒级延迟。改名只认「列目录真的看到」，
# 最多再确认 2 次（只读接口，不增加写请求），避免用过期 fileId 改名造成断层/误改。
RENAME_VISIBILITY_RECHECKS = 2
RENAME_VISIBILITY_SETTLE_SECONDS = 2.0


async def _bounded(coro, timeout: float, label: str):
    """硬超时 + 异常一律当作「没查到」，让调用方立刻走直译，绝不拖住主链路。"""
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except Exception as exc:
        logger.debug("[mirror] %s 跳过（%s: %s）", label, type(exc).__name__, str(exc)[:120])
        return None


async def _visible_b_candidates(api: "GuangYaWebFiles", media: MirrorMedia,
                                rows: list) -> tuple[dict, bool, bool]:
    """v0.9.2：改名前的可见性闸门。

    返回 (candidates, verified, all_visible)。只读列目录最多重试
    RENAME_VISIBILITY_RECHECKS 次，期间不调用任何写接口。只有所有待改文件
    都能在 B 库当前列表里命中，才允许后续 rename_file；否则整部作品本轮跳过，
    行状态标记 pending_visible，由后台短任务下轮再试。这样即使复制任务刚返回
    done、文件还没在网盘可见，也不会拿旧 fileId 发出无效改名请求。
    """
    candidates: dict[str, str] = {}
    verified = False
    for attempt in range(RENAME_VISIBILITY_RECHECKS + 1):
        candidates, verified = await _b_file_candidates(api, media)
        if verified and rows and all(_lookup_b_file(candidates, row) for row in rows):
            return candidates, True, True
        if attempt < RENAME_VISIBILITY_RECHECKS:
            await asyncio.sleep(RENAME_VISIBILITY_SETTLE_SECONDS)
    return candidates, verified, False


async def _resolve_english_title(db: Session, media: MirrorMedia, *,
                                 force: bool = False) -> tuple[str, str]:
    """每部作品只解析一次英文剧名，缓存到 `MirrorMedia.english_title`。

    v0.8.9 用户口径：「在 tmdb 上找不到英文名称的剧集卡死了，找不到就直接直译」。
    优先级：本地缓存 → 已识别 `Resource.original_title` → TMDB en-US（**只查一次**、
    6 秒硬超时）→ **AI 直译中文剧名**（10 秒硬超时）→ **本地拼音直译**（零网络）。
    最后一层保证永远有可用 ASCII 剧名，绝不会因为某部剧查不到英文名而卡住
    复制 / 入库 / 分享 / 推送主链路。
    """
    media_id = int(getattr(media, "id", 0) or 0)
    cached = str(getattr(media, "english_title", "") or "").strip()
    src_existing = str(getattr(media, "english_source", "") or "").strip()
    # v0.9.6：用户点「清空待处理清单」后会置 english_source=ignored。非强制解析时
    # 尊重该标记、直接跳过（否则每轮同步又把历史垃圾解析一遍，清单永远清不干净）；
    # 只有 force=True（点「重新解析英文名并改名」）才清掉标记、重新走解析链路。
    if src_existing == "ignored" and not force:
        return "", "ignored"
    if cached and not force:
        return cached, src_existing or "cached"
    core, year = work_core_title(media.title_standard)
    eng, source = "", ""
    if not core:
        _persist_english_title(db, media_id, "", "")
        return "", ""

    # ===== 事务分层（v1.1 修 database is locked）=====
    # 根因：本函数在 SQLite 上先 SELECT 拿读快照，再 await TMDB/AI 网络 6~16s，
    # 期间其它并发 job 提交写，最后 UPDATE 想把「读事务升级为写事务」时快照已过期，
    # 抛 SQLITE_BUSY_SNAPSHOT——这种锁冲突 busy_timeout=30s 不生效（不是等待能解决的）。
    # 修法：①所有只读 SELECT 集中在解析前做完并立即 commit 释放读锁；
    #      ②TMDB/AI 网络 await 期间不持有任何事务；
    #      ③只在最后用独立短事务做一次 UPDATE，带写锁重试。

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

    # v0.8.9.1 修复「改名能力丢了」：MirrorMedia 在镜像阶段创建时**不写 media_type**
    # （默认空串，只有后面扫描入库才回填 Resource）。v0.8.9 把类型判断写成
    # `else "movie"`，于是所有电视剧都用 /movie/{id} 查 en-US 官方英文名，必然查不到
    # → 只剩拼音兜底 → 用户在「只配 TMDB、没配 AI」的现场看到文件名仍是中文。
    # v0.8.8  movie + tv 都试才有英文名。这里恢复双查，但**每次仍带 6 秒硬超时**：
    # 已知类型优先查、只补一次另一类型，最坏 12 秒且绝不重试，卡死红线不破。
    # 类型来源优先级：显式 media_type → MirrorFile.identity(:tv:/:movie:) → 默认 movie。
    if not mtype:
        ident_tv = db.scalar(select(MirrorFile.identity).where(
            MirrorFile.media_id == media.id,
            MirrorFile.identity.like("%:tv:%")).limit(1))
        if ident_tv:
            mtype = "tv"
        else:
            ident_movie = db.scalar(select(MirrorFile.identity).where(
                MirrorFile.media_id == media.id,
                MirrorFile.identity.like("%:movie:%")).limit(1))
            if ident_movie:
                mtype = "movie"

    # v0.8.9.1：A 库目录名未必带 {tmdb-} 标记；镜像阶段 identity 里已写入 TMDB ID
    # （sync_rule 用 parse_media_name + media_identity 生成，如 tmdb:135646:tv:...）。
    # 只配了 TMDB、目录名又没标记时，旧逻辑 tid=None 会**整个跳过 TMDB 查询**，
    # 直接落到未配置的 AI / 拼音，用户看到的就是「文件名还是中文」。这里从 identity 补 tid。
    if not tid:
        ident_row = db.scalar(select(MirrorFile.identity).where(
            MirrorFile.media_id == media.id,
            MirrorFile.identity.like("tmdb:%")).limit(1))
        ident_m = re.match(r"tmdb:(\d+):", str(ident_row or ""))
        if ident_m:
            tid = int(ident_m.group(1))

    # 只读阶段结束：立即提交，释放读快照，绝不让读事务跨越下面的网络 await。
    try:
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.warning("[mirror] 英文名解析前释放读事务失败 media=%s：%s", media_id, exc)

    # 2) TMDB en-US：已知/推断类型优先，未知或被拒时补查另一类型。每次都过 _bounded()
    #    硬超时，双查最坏 2×6 秒；绝不像旧版那样无上限串行 + 每类再等 AI。
    if tid and not eng:
        primary = mtype if mtype in ("movie", "tv") else "movie"
        order = [primary] + (["tv" if primary == "movie" else "movie"]
                             if mtype not in ("movie", "tv") else [])
        for mt in order:
            raw = await _bounded(tmdb.english_title(tid, mt, db),
                                 ENGLISH_TITLE_TMDB_TIMEOUT, f"TMDB en-US {tid}/{mt}")
            v = sanitize_english_title(str(raw or ""))
            if v:
                eng, source = v, "tmdb"
                break

    # 3) 直译：TMDB 无英文名（或超时 / 密钥无效）时直接用 AI 翻译中文剧名
    if not eng:
        tr = await _bounded(ai_assist.translate_title(core, year, db),
                            ENGLISH_TITLE_AI_TIMEOUT, f"AI 直译 {core}")
        if isinstance(tr, dict):
            titles = tr.get("titles") or {}
            cand = str(titles.get("en") or (tr.get("queries") or [""])[0])
            v = sanitize_english_title(cand)
            if v:
                eng, source = v, "ai"

    # 4) 零网络兜底：AI 未配置 / 超时 / 模型不给结果时本地拼音直译，保证必有名字
    if not eng:
        v = pinyin_title(core)
        if v:
            eng, source = v, "pinyin"

    _persist_english_title(db, media_id, eng, source)
    return eng, source


def _persist_english_title(db: Session, media_id: int, eng: str, source: str) -> None:
    """把解析出的英文名写回 MirrorMedia：独立短 Session + Core UPDATE + 写锁重试。

    真机 v1.0.0 `OperationalError: database is locked [UPDATE mirror_media SET
    english_title=?, english_source=?]` 的根因：调用方 `_resolve_english_title`
    在**同一个 db Session** 上先 SELECT 建读快照 → await TMDB/AI 网络 6~16s（其间
    tmdb/ai_assist 内部又用该 db 读配置，autobegin 新的读事务）→ 再 UPDATE 想升级
    成写事务时快照已过期，抛 SQLITE_BUSY_SNAPSHOT。这种情况 busy_timeout=30s 无效
    （不是等待能解决的），必须换连接。

    修法：用**全新的独立 Session** 做这一条 UPDATE，绝不复用被网络调用污染过的
    读快照；Core UPDATE 不走 ORM 对象避免 identity 问题；再叠加写锁退避重试兜底。
    独立 Session 自己在 finally 关闭，异常一律吞掉，绝不因一次写冲突拖垮主链路。
    """
    if not media_id:
        return
    last_exc: Exception | None = None
    for attempt in range(8):
        try:
            with SessionLocal() as write_db:
                write_db.execute(update(MirrorMedia).where(MirrorMedia.id == media_id).values(
                    english_title=eng, english_source=source))
                write_db.commit()
            # 同步刷新调用方 Session 中的对象（若仍被引用），避免陈旧值误导后续逻辑
            try:
                obj = db.get(MirrorMedia, media_id)
                if obj is not None:
                    obj.english_title, obj.english_source = eng, source
            except Exception:
                pass
            return
        except OperationalError as exc:
            last_exc = exc
            if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                logger.warning("[mirror] 英文名回写失败（非锁冲突）media=%s：%s", media_id, exc)
                return
            if attempt == 7:
                logger.warning("[mirror] 英文名回写重试 8 次仍锁，放弃 media=%s：%s",
                               media_id, last_exc)
                return
            time.sleep(0.05 * (2 ** attempt))
        except Exception as exc:
            logger.warning("[mirror] 英文名回写异常 media=%s：%s", media_id, exc)
            return


async def rename_mirror_files(db: Session, rule: MirrorRule, *,
                              force: bool = False, max_media: int = 100,
                              max_files: int | None = None,
                              max_seconds: float | None = None,
                              only_pending: bool = False) -> dict:
    """把 B 库已同步的单集 / 单电影文件按英文剧名改名（只换中文片名段）。

    边界：
      - 只对 **B 库 fileId** 调 `rename_file`；A 源头库全程只读；
      - 不动 B 库分类目录 / 影视主目录 / 季目录的名字（保持 `·` 约定）；
      - 不重新复制文件、不写 Resource、不建分享、不入推送队列；
      - `MirrorFile.file_name` 保持 A 原名，改名结果写 `target_name`；
        增量比对仍走 `source_file_id + fingerprint(size:utime)`，
        后期 A 库文件变量更新照常能识别出来并触发更新。
      - v0.9.6.1：`rename_status='skipped_legacy'` 的历史行默认不回刷。用户口径：
        旧版只改文件夹、不改单集；新版单集英文名改名只作用于升级后新同步的文件。
        只有 force=True 才清掉标记、重新处理历史行。
      - only_pending=True：后台复查专用，只看仍未改名的文件，并卡紧时长预算，
        避免后台任务长期持有全局镜像锁导致「立即同步」报 409。
    """
    idle = max(float(getattr(rule, "file_idle_seconds", 1.0) or 1.0), MIN_FILE_IDLE_SECONDS)
    file_budget = int(max_files if max_files is not None else RENAME_MAX_FILES)
    time_budget = float(max_seconds if max_seconds is not None else RENAME_MAX_SECONDS)
    stats = {"works": 0, "renamed": 0, "skipped": 0, "ascii": 0,
             "untranslated": 0, "failed": 0, "no_rows": 0, "errors": [],
             "waiting_visible": 0,
             # v0.8.9：分批续改是正常进度，不是错误，单独放 notices 前端才不会误报 warning
             "notices": [],
             "literal_ai": 0, "literal_pinyin": 0,
             "remaining": 0, "truncated_reason": "", "attempts": 0,
             "skipped_legacy": 0,
             "untranslated_works": [], "idle_seconds": idle,
             "file_budget": file_budget, "time_budget": time_budget}
    started_all = time.monotonic()
    attempts = 0
    consecutive_failures = 0
    stop_reason = ""
    api = GuangYaWebFiles(db)
    media_q = select(MirrorMedia).where(
        MirrorMedia.rule_id == rule.id, MirrorMedia.target_dir_id != "")
    if only_pending:
        # v0.9.6.1：后台复查只看还有活要干的作品，别为已经 done 的整库
        # 白跑 TMDB/AI 解析与列目录——那正是后台任务持锁 134 秒的主因。
        media_q = media_q.where(MirrorMedia.id.in_(
            select(MirrorFile.media_id).where(
                MirrorFile.status == "synced",
                or_(MirrorFile.rename_status.is_(None),
                    MirrorFile.rename_status.in_(["", "pending_visible", "failed"])))))
    medias = list(db.scalars(media_q).all())
    for media in medias[:max_media]:
        # v0.9.6.1：预算在「每部作品开始前」也要查。旧版只在内层逐文件查，
        # 一部作品的 TMDB(6s)+AI(10s)+递归列目录 就能把后台锁拖到两分钟。
        if stop_reason or time.monotonic() - started_all >= time_budget:
            stop_reason = stop_reason or "time_budget"
            break
        eng, source = await _resolve_english_title(db, media, force=force)
        # v0.8.9：让用户看得到「TMDB 查不到 → 直接用直译」有没有生效
        if source == "ai":
            stats["literal_ai"] += 1
        elif source == "pinyin":
            stats["literal_pinyin"] += 1
        if not eng:
            # 有拼音兜底后几乎不会走到这里；真走到也只跳过改名，主链路照常
            stats["untranslated"] += 1
            if len(stats["untranslated_works"]) < 50:
                stats["untranslated_works"].append({
                    "work": media.title_standard,
                    "reason": "TMDB / AI 直译 / 本地拼音都没给出可用剧名，已跳过改名；"
                              "复制、入库、分享与推送不受影响，可点「重新解析英文名并改名」重试"})
            continue
        core, _year = work_core_title(media.title_standard)
        rows = list(db.scalars(select(MirrorFile).where(
            MirrorFile.media_id == media.id,
            MirrorFile.status == "synced")).all())
        legacy_rows = [r for r in rows
                       if str(getattr(r, "rename_status", "") or "") == "skipped_legacy"]
        if legacy_rows and force:
            # 用户显式点「重新解析英文名并改名」：只对这一批历史行解除回刷保护。
            for row in legacy_rows:
                row.rename_status, row.rename_error = "", ""
            db.commit()
        elif legacy_rows:
            stats["skipped_legacy"] += len(legacy_rows)
            rows = [r for r in rows
                    if str(getattr(r, "rename_status", "") or "") != "skipped_legacy"]
        if only_pending:
            rows = [r for r in rows
                    if str(getattr(r, "rename_status", "") or "") in ("", "pending_visible", "failed")]
        if not rows:
            stats["no_rows"] += 1
            continue
        # v0.9.6.1：再查一次预算，避免刚解析完英文名/列目录已超时还继续发改名请求。
        if time.monotonic() - started_all >= time_budget:
            stop_reason = "time_budget"
            break
        # v0.9.2：复制任务返回 done 后，B 库文件可见可能有秒级延迟。
        # 先用只读列目录确认 fileId 真的可见，再发 rename_file；
        # 不拿 DB 里可能过期的 fileId 冒险，避免「文件有了但没改名」断层。
        candidates, b_verified, _all_visible = await _visible_b_candidates(api, media, rows)
        name_fids = {k: v for k, v in candidates.items() if ":" not in k}
        # v0.9.6.1：从「整部作品全可见才改名」改为「逐文件按可见 fileId 改名」。
        # 旧口径只要有一集暂时不可见，整部剧全被标 pending_visible，用户看到
        # 「文件复制进来了但中文名没换」；现在 B 库列目录完整可信时，可见的先改，
        # 不可见的单独等复查，不拿未知 fileId 冒险。verified=False 仍然一个都不动。
        if not b_verified:
            for row in rows:
                row.rename_status = "pending_visible"
                row.rename_error = "B 库列目录不完整，未据此改名，等待后台复查"
            db.commit()
            stats["waiting_visible"] += len(rows)
            continue
        visible_rows = [r for r in rows if _lookup_b_file(candidates, r)]
        invisible_rows = [r for r in rows if not _lookup_b_file(candidates, r)]
        for row in invisible_rows:
            row.rename_status = "pending_visible"
            row.rename_error = "B 库列目录暂未命中该文件（可能尚未可见），下一轮自动重试"
        if invisible_rows:
            db.commit()
            stats["waiting_visible"] += len(invisible_rows)
        rows = visible_rows
        if not rows:
            continue
        fid_to_norm: dict[str, str] = {}
        for k, v in name_fids.items():
            fid_to_norm.setdefault(str(v), k)
        first = True
        for idx, row in enumerate(rows):
            _beat()  # v0.9.21：逐个文件改名推进打点，长剧批量改名不被误判卡死
            fname = str(row.file_name or "")
            # v0.8.9：预算与熔断。已达标跳过不消耗「尝试次数」，保证下轮能
            # 快速跳过已改名的文件、把预算留给真正还没改的那部分。
            if time.monotonic() - started_all >= time_budget:
                stop_reason = "time_budget"
                break
            if attempts >= file_budget:
                stop_reason = "file_budget"
                break
            if consecutive_failures >= RENAME_MAX_CONSECUTIVE_FAILURES:
                stop_reason = "rate_limited"
                break
            try:
                target = english_file_name(fname, core, eng)
                if not target:
                    stats["ascii"] += 1
                    row.rename_status = "skipped_ascii"
                    db.commit()
                    continue
                # v0.8.8：优先用 B 库列目录命中的 fileId；DB 里的 target_file_id
                # 可能是补复制前旧文件的 ID，列目录失败时才回退到 DB 值。
                fid = _lookup_b_file(candidates, row) or str(row.target_file_id or "")
                if not fid:
                    # 列目录没覆盖到 ≠ 文件不存在。标「待可见」而非永久失败，
                    # 下一轮自动重试，不让百集剧的后半段被一次性判死。
                    row.rename_status = "pending_visible"
                    row.rename_error = "B 库列目录暂未找到该文件（网盘可能尚未可见），下一轮自动重试"
                    db.commit()
                    attempts += 1
                    continue
                final, n = target, 1
                while (normalize_work_name(final) in name_fids
                       and name_fids[normalize_work_name(final)] != fid and n < 20):
                    n += 1
                    final = with_conflict_suffix(target, n)
                cur = fid_to_norm.get(str(fid), "")
                if cur and cur == normalize_work_name(final):
                    # B 库当前确实已是目标英文名：只纠正 DB 状态，不重复调接口
                    row.target_file_id = fid
                    row.target_name = final
                    row.rename_status, row.rename_error = "done", ""
                    stats["skipped"] += 1
                    db.commit()
                    continue
                # 列目录不完整时不信任实际名字，退回 DB 幂等判断，防接口抖动误改
                if not b_verified and row.target_name == final and row.rename_status == "done":
                    stats["skipped"] += 1
                    continue
                if not first:
                    await asyncio.sleep(idle)  # 与复制/建目录共用同一风控间隔
                first = False
                attempts += 1
                stats["attempts"] += 1
                try:
                    await api.rename_file(fid, final)
                except Exception as exc:
                    msg = str(exc)[:300]
                    row.rename_status, row.rename_error = "failed", msg
                    stats["failed"] += 1
                    consecutive_failures += 1
                    stats["errors"].append(
                        f"{media.title_standard}/{fname}：改名失败 {msg[:160]}")
                    logger.warning("[mirror] rename 失败 %s/%s：%s",
                                   media.title_standard, fname, msg)
                    db.commit()
                    continue
                consecutive_failures = 0
                row.target_file_id = fid
                row.target_name = final
                row.rename_status, row.rename_error = "done", ""
                row.updated_at = utcnow()
                name_fids[normalize_work_name(final)] = fid
                stats["renamed"] += 1
                db.commit()
            except Exception as exc:
                # v0.8.9：任何单文件异常（含 db.commit 写冲突）只影响当前文件，
                # 绝不中断整部剧 —— 这正是「长剧集只改了一部分」的旧病灶。
                msg = f"{type(exc).__name__}: {exc}"[:300]
                logger.warning("[mirror] 改名单文件异常 %s/%s：%s",
                               media.title_standard, fname, msg)
                stats["errors"].append(f"{media.title_standard}/{fname}：{msg[:160]}")
                try:
                    db.rollback()
                    row.rename_status, row.rename_error = "failed", msg
                    db.commit()
                    stats["failed"] += 1
                    consecutive_failures += 1
                except Exception:
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    stop_reason = "db_error"
                    break
        stats["works"] += 1
        db.commit()
    if stop_reason:
        # v0.8.9：一次性统计全局剩余（含本轮完全没触及的后续作品），只给一条
        # 明确提示。旧写法逐部累加还会在空 rows 时误用上一轮的 idx。
        stats["truncated_reason"] = stop_reason
        remaining = db.scalar(select(func.count()).select_from(MirrorFile).where(
            MirrorFile.rule_id == rule.id,
            MirrorFile.status == "synced",
            or_(MirrorFile.rename_status.is_(None),
                MirrorFile.rename_status != "done"))) or 0
        stats["remaining"] = int(remaining)
        stats["notices"].append(
            f"本轮改名到上限（{stop_reason}），剩余 {stats['remaining']} 个文件"
            "下一轮自动续改，不是失败；可多点几次「立即同步」推进进度")
    return stats


async def prune_stale_mappings(db: Session, rule: MirrorRule, *,
                               dry_run: bool = False,
                               max_media: int = 200) -> dict:
    """v0.8.9：只读核对 B 库实际内容，清理「B 库已经不存在」的 A→B 映射。

    用户反馈「目录映射清单没有清理无效的按钮」。这里给出可预览、可执行的清理，
    但绝不按 DB 状态猜、绝不在列目录不可信时动手。

    红线：
      1. 只有 `_b_file_candidates()` / `_list_dir()` 返回完整可信且确实读到内容时
         才清理；接口异常、分页截断、整部作品一个文件都没有 → 一律跳过，交给
         `sync_rule()` 自动补复制，绝不让一次接口抖动清掉整部剧的映射。
      2. 只重置映射字段，**不删行**：MirrorFile 保留 A 库锚点
         (`file_name` / `source_file_id` / `fingerprint`)，把 status 置 pending、
         清 target_file_id / target_name / rename_status，下轮同步自动补复制并改名。
      3. 不动 Resource / share_url / QueueItem / DistributionEvent，也不删 B 库文件。
      4. 每次写立即 `db.commit()`（设计红线 8：绝不跨 await 持有 SQLite 写事务）。
    """
    api = GuangYaWebFiles(db)
    stats = {"ok": True, "dry_run": bool(dry_run), "media_checked": 0,
             "media_skipped_unverified": 0, "media_skipped_empty": 0,
             "files_pruned": 0, "dirs_checked": 0, "dirs_pruned": 0,
             "dirs_skipped_unverified": 0, "samples": [], "errors": []}
    medias = list(db.scalars(select(MirrorMedia).where(
        MirrorMedia.rule_id == rule.id, MirrorMedia.target_dir_id != "")).all())
    for media in medias[:max_media]:
        stats["media_checked"] += 1
        try:
            candidates, verified = await _b_file_candidates(api, media)
        except Exception as exc:
            stats["media_skipped_unverified"] += 1
            stats["errors"].append(
                f"{media.title_standard}：列 B 库失败 {str(exc)[:120]}")
            continue
        visible = {str(v) for v in candidates.values()}
        if not verified:
            stats["media_skipped_unverified"] += 1
            continue
        if not visible:
            # 整部作品 B 库无文件：不清映射，交给同步补复制（v0.8.7 同一口径）
            stats["media_skipped_empty"] += 1
            continue
        rows = list(db.scalars(select(MirrorFile).where(
            MirrorFile.media_id == media.id,
            MirrorFile.target_file_id != "",
            MirrorFile.status.in_(["synced", "copying", "failed", "unknown"]))).all())
        for row in rows:
            if str(row.target_file_id) in visible:
                continue
            stats["files_pruned"] += 1
            if len(stats["samples"]) < 20:
                stats["samples"].append(f"{media.title_standard}/{row.file_name}")
            if dry_run:
                continue
            row.status = "pending"
            row.target_file_id = ""
            row.target_name = ""
            row.rename_status = ""
            row.rename_error = ""
            row.task_id = ""
            row.error = "B 库实际文件已不存在，映射已清理，等待下轮同步补复制"
            row.updated_at = utcnow()
            db.commit()

    # 目录映射：按父目录分组，只读列一次即可核对同组所有兄弟，避免接口风暴
    dirs = list(db.scalars(select(MirrorDir).where(
        MirrorDir.rule_id == rule.id,
        MirrorDir.target_dir_id != "",
        MirrorDir.status != "missing")).all())
    groups: dict[str, list[MirrorDir]] = {}
    for d in dirs:
        groups.setdefault(str(d.parent_target_id or ""), []).append(d)
    for pid, group in groups.items():
        if not pid:
            stats["dirs_skipped_unverified"] += len(group)
            continue
        try:
            items, ok = await _list_dir(api, pid)
        except Exception:
            items, ok = [], False
        if not ok:
            stats["dirs_skipped_unverified"] += len(group)
            continue
        alive = {str(it.get("fileId") or "") for it in items
                 if int(it.get("resType", 1) or 1) == 2}
        stats["dirs_checked"] += len(group)
        for d in group:
            if str(d.target_dir_id) in alive:
                continue
            stats["dirs_pruned"] += 1
            if len(stats["samples"]) < 20:
                stats["samples"].append(f"目录 {d.category_path or d.name_standard}")
            if dry_run:
                continue
            d.target_dir_id = ""
            d.status = "missing"
            # v0.9.6：加 [pruned] 标记区分「已清理待重建的僵尸行」与「真正建目录失败」。
            # 前者不需要用户手工介入（下轮同步会从 A 库重建），从「待创建目录」清单里
            # 排除；管理页另给「清空已清理映射」按钮让用户在确认后彻底删除这些行。
            d.error = PRUNED_DIR_MARK + PRUNED_DIR_LEGACY_TEXT
            d.updated_at = utcnow()
            db.commit()
    return stats


async def prune_missing_media_rows(db: Session, rule: MirrorRule, *,
                                   dry_run: bool = False,
                                   max_media: int = 200) -> dict:
    """v0.9.10：影视映射清单「清理无效影视映射」——只读核对 B 库，删除整部已不存在的映射行。

    用户口径（2026-11-19 确认）：影视映射清单要清的是「B 库网盘文件已删除」的历史遗留行，
    不是按本地 MirrorFile.status 猜。此前只按「无 synced 文件」判定，synced+有文件的
    僵尸行一条都清不掉，用户看到「按钮点了没反应」。

    红线：
      1. 逐部递归列 B 库该作品目录（复用 `_b_file_candidates`）；只有 `verified=True`
         （列目录完整可信、未截断）且整部确实一个文件都没有时，才判为「B 库已删除」。
         接口异常 / 分页截断 / 有任一文件 → 一律跳过，绝不误删。
      2. 只删本地 MirrorMedia 行及其子 MirrorFile 行；不动 Resource / share_url /
         QueueItem / DistributionEvent，不删 B 库文件。
      3. 每次删立即 `db.commit()`，绝不跨 await 持有写事务（设计红线 8）。
      4. dry_run 只返回清单不写库，供预览确认。
    """
    api = GuangYaWebFiles(db)
    stats = {"ok": True, "dry_run": bool(dry_run), "media_checked": 0,
             "media_deleted": 0, "skipped_verified_has_files": 0,
             "skipped_unverified": 0, "files_deleted": 0,
             "samples": [], "errors": []}
    medias = list(db.scalars(select(MirrorMedia).where(
        MirrorMedia.rule_id == rule.id).order_by(MirrorMedia.id.desc())).all())
    for media in medias[:max_media]:
        stats["media_checked"] += 1
        try:
            candidates, verified = await _b_file_candidates(api, media)
        except Exception as exc:
            stats["skipped_unverified"] += 1
            stats["errors"].append(f"{media.title_standard}：列 B 库失败 {str(exc)[:120]}")
            continue
        if not verified:
            stats["skipped_unverified"] += 1
            continue
        visible = {str(v) for v in candidates.values() if str(v)}
        if visible:
            # B 库仍有文件：这不是「已删除」的僵尸行，保留
            stats["skipped_verified_has_files"] += 1
            continue
        # verified 且整部为空 → B 库确实已无文件，判定为可清理的历史遗留映射
        if len(stats["samples"]) < 20:
            stats["samples"].append(media.title_standard)
        if dry_run:
            stats["media_deleted"] += 1
            continue
        file_count = db.query(MirrorFile).filter(MirrorFile.media_id == media.id).count()
        db.query(MirrorFile).filter(MirrorFile.media_id == media.id).delete(
            synchronize_session=False)
        db.delete(media)
        stats["media_deleted"] += 1
        stats["files_deleted"] += file_count
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


# ---------------- v0.9.17.2：失败任务定向批量重试（不扫全库） ----------------
# 用户真机痛点：失败任务一直挂着，唯一入口「立即同步」会 _walk_source 全库扫描，
# A 源头库很大时任务长时间不结束。这里只针对用户勾选的失败行定向重发：
#   - 失败目录：MirrorDir 自带 parent_target_id + name_standard，直接 create_dir 重建；
#   - 失败文件：MirrorFile 没存目标父目录，只 walk **该行所属作品**的 A 目录
#     （_walk_source(root=media.source_dir_id)，几十个文件），按季目录相对链映射到 B 库，
#     重发前先用 _b_file_candidates 核对 B 库已存在的直接回填 synced（绝不重复复制）。
# 全程不调用 sync_rule、不扫 A 库根、不写 push_queue；mark 只改被选中的行。

async def retry_failed_dirs(db: Session, rule: MirrorRule, dir_ids: list[int], *,
                            idle: float = MIN_FILE_IDLE_SECONDS) -> dict:
    """定向重建勾选的 missing 目录。复用 _ensure_child 同款 · 规则与标准名降级。"""
    ids = [int(x) for x in (dir_ids or [])]
    out = {"selected": len(ids), "recreated": 0, "already_ok": 0,
           "skipped": 0, "failed": 0, "details": []}
    if not ids:
        return out
    api = GuangYaWebFiles(db)
    # 只允许本规则、确实 missing 的行；其它状态（mapped/created）不重复建
    rows = list(db.scalars(select(MirrorDir).where(
        MirrorDir.rule_id == rule.id, MirrorDir.id.in_(ids))).all())
    by_id = {r.id: r for r in rows}
    plain_mode = {"on": False, "tried": False}
    for did in ids:
        d = by_id.get(did)
        if not d:
            out["skipped"] += 1
            out["details"].append(f"目录#{did}：不属于本规则或不存在，跳过")
            continue
        if d.status != "missing":
            out["already_ok"] += 1
            out["details"].append(f"{d.name_standard}：当前状态 {d.status}，无需重建")
            continue
        parent = str(d.parent_target_id or "")
        base = str(d.name_standard or "")
        if not parent or not base:
            d.error = "[定向重试] 缺少父目录 ID 或目录名，无法重建"
            d.status = "missing"
            out["failed"] += 1
            out["details"].append(f"{base or '#'+str(did)}：{d.error}")
            db.commit()
            continue
        effective = base.replace("*", NAME_MARK) if d.kind == "work" else base
        if d.kind == "work" and plain_mode["on"]:
            effective = standard_media_name(effective)
        await asyncio.sleep(max(float(idle), MIN_FILE_IDLE_SECONDS))
        try:
            made = await api.create_dir(parent, effective)
        except Exception as exc:
            first = str(exc)[:200]
            plain = standard_media_name(effective)
            made = None
            if (d.kind == "work" and NAME_MARK in effective and plain != effective
                    and not plain_mode["tried"]):
                plain_mode["tried"] = True
                await asyncio.sleep(max(float(idle), MIN_FILE_IDLE_SECONDS))
                try:
                    made = await api.create_dir(parent, plain)
                    plain_mode["on"] = True
                    effective = plain
                except Exception as exc2:
                    err = f"[定向重试] create_dir 失败：{first}；降级标准名仍失败：{str(exc2)[:160]}"
            else:
                err = f"[定向重试] create_dir 失败：{first}"
            if made is None:
                d.error = err
                d.status = "missing"
                out["failed"] += 1
                out["details"].append(f"{base}：{err}")
                db.commit()
                continue
        fid = str(made.get("file_id") or "")
        if not fid:
            d.error = "[定向重试] create_dir 未返回 fileId，不标记完成"
            d.status = "missing"
            out["failed"] += 1
            out["details"].append(f"{base}：{d.error}")
            db.commit()
            continue
        d.target_dir_id = fid
        d.status = "created_fallback" if plain_mode["on"] and NAME_MARK not in effective else "created"
        d.error = ""
        d.updated_at = utcnow()
        out["recreated"] += 1
        out["details"].append(f"{base}：已重建（B 目录 ID {fid}）")
        # 同父目录下、等待本目录的子目录下次同步会自动 mapped；不在这里递归
        db.commit()
    return out


async def retry_failed_files(db: Session, rule: MirrorRule, file_ids: list[int], *,
                             idle: float = MIN_FILE_IDLE_SECONDS,
                             max_polls: int = 8, poll_interval: float = 2.0) -> dict:
    """定向重发勾选的失败文件。先核对 B 库已存在则回填，不重复复制。"""
    ids = [int(x) for x in (file_ids or [])]
    out = {"selected": len(ids), "reused": 0, "queued": 0, "done": 0,
           "failed": 0, "skipped": 0, "details": []}
    if not ids:
        return out
    api = GuangYaWebFiles(db)
    rows = list(db.scalars(select(MirrorFile).where(
        MirrorFile.rule_id == rule.id, MirrorFile.id.in_(ids))).all())
    by_id = {r.id: r for r in rows}
    # 按作品分组：一部作品只 walk 一次 A 目录 + 列一次 B 库候选
    groups: dict[int, list] = {}
    for fid in ids:
        r = by_id.get(fid)
        if not r:
            out["skipped"] += 1
            out["details"].append(f"文件#{fid}：不属于本规则或不存在，跳过")
            continue
        groups.setdefault(r.media_id, []).append(r)

    for media_id, group in groups.items():
        media = db.get(MirrorMedia, media_id)
        if not media or not str(media.source_dir_id or "").strip() \
                or not str(media.target_dir_id or "").strip():
            for r in group:
                r.status = "failed"
                r.error = "[定向重试] 作品 A/B 目录映射缺失，无法定位复制目标"
                out["failed"] += 1
                out["details"].append(f"{r.file_name}：{r.error}")
            db.commit()
            continue
        # 只 walk 单部作品（非全库），拿到 A 季目录相对链
        nodes, complete = await _walk_source(api, str(media.source_dir_id), max_dirs=80)
        if not complete:
            for r in group:
                r.status = "failed"
                r.error = "[定向重试] A 库该作品目录分页不完整，已中止（禁止用不完整数据重发）"
                out["failed"] += 1
                out["details"].append(f"{r.file_name}：{r.error}")
            db.commit()
            continue
        root_node = nodes.get(str(media.source_dir_id))
        # A source_file_id -> 所在目录节点
        owner: dict[str, dict] = {}
        for node in nodes.values():
            for it in node.get("files", []):
                sid = str(it.get("fileId") or "")
                if sid:
                    owner[sid] = node
        candidates, verified = await _b_file_candidates(api, media)
        cache: dict[str, dict[str, str]] = {}
        sub_cache: dict[str, str] = {}
        pending_tasks: list[tuple[MirrorTask, list]] = []

        for r in group:
            # 先核对：B 库其实已经有（如轮询超时/已复制成功）→ 回填，不重发
            hit = _lookup_b_file(candidates, r) if verified else ""
            if hit:
                r.target_file_id = hit
                r.status, r.error = "synced", ""
                r.updated_at = utcnow()
                out["reused"] += 1
                out["details"].append(f"{r.file_name}：B 库已存在，回填为已同步")
                # 立即提交：一组若全是回填（无 copy task），末尾的 commit 不会执行，
                # 这里不落库会导致「提示已回填、刷新后仍是 failed」（v0.9.17.2 实测）。
                db.commit()
                continue
            container = owner.get(str(r.source_file_id or ""))
            if not root_node or container is None:
                r.status = "failed"
                r.error = "[定向重试] A 库该作品目录下未找到源文件，可能已被移动/删除"
                out["failed"] += 1
                out["details"].append(f"{r.file_name}：{r.error}")
                db.commit()
                continue
            # 计算季目录相对链并映射到 B 库
            rel = _relative_dir_names(container, root_node, nodes)
            target_parent = str(media.target_dir_id)
            sub_ok = True
            for sub_name in rel:
                key = f"{target_parent}|{normalize_work_name(sub_name)}"
                if key in sub_cache:
                    target_parent = sub_cache[key]
                    continue
                try:
                    children = await _target_children_cached(api, target_parent, cache)
                    sid = children.get(normalize_work_name(sub_name)) or ""
                    if not sid:
                        await asyncio.sleep(max(float(idle), MIN_FILE_IDLE_SECONDS))
                        made = await api.create_dir(target_parent, standard_media_name(sub_name))
                        sid = str(made.get("file_id") or "")
                    if not sid:
                        raise WebFileError("子目录创建后未返回 fileId")
                    sub_cache[key] = sid
                    target_parent = sid
                except Exception as exc:
                    r.status = "failed"
                    r.error = f"[定向重试] B 库子目录「{sub_name}」未就绪：{str(exc)[:160]}"
                    out["failed"] += 1
                    out["details"].append(f"{r.file_name}：{r.error}")
                    sub_ok = False
                    break
            if not sub_ok:
                db.commit()
                continue
            # 重发复制：清目标 ID / 改名状态，复用同一行（禁止先删旧再复制）
            r.status = "copying"
            r.error = ""
            r.target_file_id = ""
            r.target_name = ""
            r.rename_status = ""
            r.rename_error = ""
            try:
                await asyncio.sleep(max(float(idle), MIN_FILE_IDLE_SECONDS))
                task_id = await api.copy_file([str(r.source_file_id)], target_parent)
            except Exception as exc:
                r.status = "failed"
                r.error = f"[定向重试] copy_file 失败：{str(exc)[:200]}"
                out["failed"] += 1
                out["details"].append(f"{r.file_name}：{r.error}")
                db.commit()
                continue
            task = MirrorTask(rule_id=rule.id, media_id=media.id, task_id=task_id,
                              status="running")
            db.add(task)
            db.commit()
            r.task_id = task_id
            pending_tasks.append((task, [r]))
            out["queued"] += 1

        if pending_tasks:
            await _poll_tasks_batch(api, pending_tasks, max_polls=max_polls,
                                    interval=poll_interval)
            for task, task_rows in pending_tasks:
                for rr in task_rows:
                    rr.updated_at = utcnow()
                    if task.status == "done":
                        rr.status, rr.error = "synced", ""
                        out["done"] += 1
                    elif task.status == "failed":
                        rr.status, rr.error = "failed", f"[定向重试] 复制任务失败：{task.error[:160]}"
                        out["failed"] += 1
                    else:
                        rr.status = "unknown"
                        rr.error = f"[定向重试] 任务 {task.task_id} 未确认（{task.raw_status or '未知'}），保留轮询"
                        out["failed"] += 1
                    out["details"].append(f"{rr.file_name}：{rr.status}" +
                                          (f"（{rr.error}）" if rr.error else ""))
            media_row = db.get(MirrorMedia, media_id)
            if media_row:
                media_row.last_synced_at = utcnow()
            db.commit()
    return out
