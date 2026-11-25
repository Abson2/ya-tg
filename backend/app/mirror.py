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
import json
import logging
import re
import time
from datetime import timedelta

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

from . import ai_assist, mirror_batch, runtime_config, tmdb
from .db import SessionLocal
from .file_naming import (english_file_name, pinyin_title, sanitize_english_title,
                          with_conflict_suffix, work_core_title)
from .guangya import (GuangYaWebFiles, TRANSIENT_FAULT_MARK, WebFileError,
                      is_auth_expired_guangya, is_transient_guangya_fault)
from .models import (MirrorDir, MirrorDirScan, MirrorFile, MirrorMedia, MirrorRule,
                     MirrorTask, Resource)
from .utils import (NAME_MARK, VIDEO_EXTS, as_utc, looks_like_name_exists,
                   media_identity, normalize_work_name,
                    parse_media_name, scrub_upstream_internals,
                    standard_media_name, star_media_name, strip_tmdb_tag,
                    utcnow)

logger = logging.getLogger(__name__)

# v0.8.9：长剧集分批改名。《中国通史》这类百集作品一轮连发上百次 rename，
# 每个文件间隔 idle 秒，网盘偶发 429/5xx 时 _post_web 还会退避重试三次
# （1+2+4 秒），单轮轻松超过前端 300 秒超时 → 界面报「改名失败」但其实
# 已改了一部分。每轮只处理有限文件 / 有限时长，剩下的下一轮自动续改：
# 每个文件改名后立即 commit，未处理的行 rename_status 仍为空，下轮会再被选中。
RENAME_MAX_FILES = 50
RENAME_MAX_SECONDS = 90.0
RENAME_MAX_CONSECUTIVE_FAILURES = 5
# v1.4 改名提速：改名 idle 与复制 idle 解耦，硬下限 0.2 秒（防御性风控）。
# 低于下限一律抬到下限，避免用户在配置页填 0 导致瞬时并发触发光鸭风控。
MIN_RENAME_IDLE_SECONDS = 0.2
# v1.4：改名并发度上限。光鸭 rename 是单文件接口，提速靠并发窗口而非批量 API；
# 保守默认 3，最大 8。配置填 1 即退回串行，便于真机遇风控时快速降级。
MAX_RENAME_CONCURRENCY = 8
# v1.4.1：光鸭 code=127「操作过于频繁」写接口风控限流根治。
# 127 是可恢复的瞬时限流，必须与「真实失败」区别对待：指数退避 + 同集自动重试，
# 不因它触发「连续 5 次熔断」，也不让它干等下一轮 120s job。
GUANGYA_RATE_LIMIT_CODE = 127
RENAME_RATE_LIMIT_MAX_RETRIES = 5          # 单文件内最多退避重试 5 次（1/2/4/8/8）
RENAME_RATE_LIMIT_BACKOFF_BASE = 1.0       # 退避基数秒
RENAME_RATE_LIMIT_BACKOFF_CAP = 8.0        # 退避上限秒，防止单窗口阻塞过久
RENAME_RATE_LIMIT_MARK = "@RATE_LIMIT_127@"  # 内层失败结果的限流标记，供主循环识别
# v2.1.9：光鸭 code=127 限流同样适用于**复制**接口（copy_file 是写接口）。
# 此前只有改名路径（_rename_one_with_backoff）做了指数退避重试，
# 复制路径两处（strict 批量提交 / 常规批量复制）遇 127 直接写 failed —— 
# 真机上一次批量复制触发 127，整批 50 个文件瞬间全标 failed 且**永不重试**。
# 这里补齐同口径的退避重试。
COPY_RATE_LIMIT_MAX_RETRIES = 5          # 单次 copy_file 最多退避重试 5 次（1/2/4/8/8）
COPY_RATE_LIMIT_BACKOFF_BASE = 1.0       # 退避基数秒
COPY_RATE_LIMIT_BACKOFF_CAP = 8.0        # 退避上限秒

# v2.1.12 #295：上游**瞬时故障**（服务端 RPC 超时 / 5xx / 429 / 网络抖动）独立标记。
#
# 真机报障（2026-11-23）：网页端 /userres/v1/file/rename 失败
#   msg=rpc timeout: timeout=6s, to=nd.svcuserres.s, method=CheckSafeBoxAccessToken
#
# 这条是**上游服务内部调用超时**（服务端 6s RPC 超时），不是登录态失效。
# 旧代码在鉴权启发式里用裸子串 "token" 判掉登录 → 命中上游方法名
# CheckSafeBoxAccessToken → 直接放弃、零重试 → 单次失败即算「真实失败」，
# 5 次连续失败就把整轮改名熔断（这正是 #295 的病灶）。
#
# 现在与 127（风控限流）、160（名已存）严格区分成三类：
#   127 = 网盘主动限流 → 退避重试，耗尽后落 failed（真·限流）；
#   160 = 目标名已存在 → 不可重试，靠幂等收敛；
#   瞬时 = 上游服务抖动 → **不计熔断**、不计真失败，落「待处理」等下一轮自动重试。
#
# v2.1.12 #295：瞬时标记改为**显式**打标（guangya.TRANSIENT_FAULT_MARK），
# 不再靠错误文案里的关键词猜测 —— 上游一句没特征的 msg 会让真失败被误判成
# 可重试（反之亦然），一次抖动即把整轮改名熔断。
RENAME_TRANSIENT_MARK = TRANSIENT_FAULT_MARK  # 内层失败结果的瞬时故障标记
# v2.1.12 #295：瞬时故障在**改名层**的额外重试次数。
# 底层 guangya._post_web 已按官方 retryable 语义退避重试 3 次（1+2+4 秒），
# 覆盖了「上游短暂抖动」的绝大多数情形；改名层只再给 1 次机会，
# 覆盖「刚好在最后一次底层重试之后恢复」的小概率窗口。
# 这里**刻意不放大**：底层 3 次 × 改名层 2 次 = 6 次请求 × 每次 6 秒 ≈ 36 秒，
# 单文件就能吃掉一整轮改名预算；放大重试并不让成功率线性提升，只让整轮更慢。
RENAME_TRANSIENT_MAX_RETRIES = 1
# 改名层瞬时故障的退避基数（底层已退避过，这里只等一小会儿）。
RENAME_TRANSIENT_BACKOFF_BASE = 1.0
RENAME_TRANSIENT_BACKOFF_CAP = 2.0
# 瞬时故障的中性说明文案（页面与 DB 共用同一句，不含任何上游内部标识）。
RENAME_TRANSIENT_LABEL = "网盘服务暂时不可用（上游响应超时，已自动退避重试，下一轮自动继续）"


def _is_transient_fault(exc: BaseException) -> bool:
    """识别上游瞬时故障（服务端 RPC 超时 / 5xx / 429 / 网络抖动）。

    与 `_is_rate_limit_error`（127）、`_is_name_exists_error`（160）严格分开：
    这三类的处置完全不同（退避重试 / 幂等收敛 / 不计失败等下一轮）。
    判定只认**明确的**服务端超时与网络故障特征，绝不把普通文案里的
    "timeout" 一律当瞬时故障——宁可多重试一次，也别把真失败无限重试。
    """
    code = getattr(exc, "code", None)
    if code in (GUANGYA_RATE_LIMIT_CODE, GUANGYA_NAME_EXISTS_CODE):
        return False
    return is_transient_guangya_fault(str(exc))


async def _copy_file_with_backoff(api: "GuangYaWebFiles", fids: list[str],
                                  target_parent: str) -> tuple[str, str]:
    """复制一批文件到目标目录，遇光鸭 code=127 限流则指数退避 + 自动重试。

    v2.1.9：与改名路径（_rename_one_with_backoff）同口径。
      - 仅对 127 限流重试（1/2/4/8/8 秒封顶），真实失败立即返回不重试；
      - 纯网络，绝不碰 db；
      - 返回 (task_id, err)：成功时 err 为空；失败时 task_id 为空、err 为原因。
        限流重试耗尽时 err 带 RATE_LIMIT_MARK 标记，主循环据此区分
        「限流待重试」与「真实失败」。
    """
    delay = COPY_RATE_LIMIT_BACKOFF_BASE
    last_err = ""
    for attempt in range(COPY_RATE_LIMIT_MAX_RETRIES + 1):
        try:
            task_id = await api.copy_file(fids, target_parent)
            return task_id, ""
        except Exception as exc:
            last_err = str(exc)[:300]
            if not _is_rate_limit_error(exc) or attempt >= COPY_RATE_LIMIT_MAX_RETRIES:
                if _is_rate_limit_error(exc):
                    last_err = f"{RENAME_RATE_LIMIT_MARK}{last_err}"
                return "", last_err
            await asyncio.sleep(delay)
            delay = min(delay * 2, COPY_RATE_LIMIT_BACKOFF_CAP)
    return "", f"{RENAME_RATE_LIMIT_MARK}{last_err or 'copy 限流重试耗尽'}"


def _is_rate_limit_error(exc: BaseException) -> bool:
    """识别光鸭 code=127 限流：结构化 code 优先，错误文本兜底（兼容旧版 code=None）。"""
    if getattr(exc, "code", None) == GUANGYA_RATE_LIMIT_CODE:
        return True
    msg = str(exc).lower()
    return ("code=127" in msg or "操作过于频繁" in str(exc)
            or "too many requests" in msg or "rate limit" in msg)


# v2.1.10：光鸭 code=160「文件名已存」——**不是真错误，是「重复改名」**。
# 真机取证（2026-11-23）：63 条 code160 集中在 3 部剧（大唐双龙传 25 / 大军师司马懿 22 /
# 大唐迷雾 16），全是中文名→英文名改名。同一部剧「间隔式」成败（部分集 done、部分集 160），
# 说明是分批/多轮改名中，**改成功的集 DB 状态没跟上**，后续轮次又拿旧中文名去改了一遍，
# 光鸭发现目标英文名已被占用 → 报 160。
# 与 127 限流是两码事：160 **不可重试**（重试还是占用），但**必须被识别出来**，
# 否则统一走失败分支标 failed，把「其实已经改名成功」误报成永久失败。
GUANGYA_NAME_EXISTS_CODE = 160
RENAME_NAME_EXISTS_MARK = "@NAME_EXISTS_160@"  # 内层失败结果的「名已存在」标记


def _is_name_exists_error(exc: BaseException) -> bool:
    """识别光鸭 code=160「文件名已存」：结构化 code 优先，错误文本兜底。

    与 `_is_rate_limit_error`（127）严格分开：
      - 127 = 瞬时限流 → 指数退避重试；
      - 160 = 目标名已存在 → **重试无用**，只能靠幂等收敛（改 DB 状态，不重复发请求）。
    """
    if getattr(exc, "code", None) == GUANGYA_NAME_EXISTS_CODE:
        return True
    msg = str(exc).lower()
    return ("code=160" in msg or "文件名已存" in str(exc)
            or "文件名已存在" in str(exc) or "name already exists" in msg)

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

def rename_error_label(text: str) -> str:
    """把改名的原始报错翻成中性中文标签（页面只展示标签，不展示原始 code=xxx）。

    红线：页面不得体现后端代码名词。原始报错里带「code=160」「code=127」这类
    后端口径，直接透给用户等于泄露内部契约，而且「160 是什么意思」用户根本看不懂。
    这里统一翻成人话；真正的原始文本仍留在数据库里供排查，页面只显示标签。

    v2.1.12 #295：新增瞬时故障分支，并**屏蔽上游内部标识**。
    真机原文（rpc timeout）带 `nd.svcuserres.s` / `CheckSafeBoxAccessToken` /
    `/userres/v1/file/rename` —— 这些是上游服务内部契约，用户看不懂也不该看到，
    统一走 utils.scrub_upstream_internals 洗掉。
    """
    raw = str(text or "")
    low = raw.lower()
    # v2.1.12 #295：显式标记优先判定。
    # 打标后正文可能**不含任何可识别关键词**（如上游只回 `msg=internal error`），
    # 靠 is_transient_guangya_fault 的文案匹配会漏判 → 掉进兜底分支被洗成残句。
    # 标记是权威信号，先剥标记再判。
    if raw.startswith(RENAME_TRANSIENT_MARK):
        return RENAME_TRANSIENT_LABEL
    if looks_like_name_exists(raw):
        return "目标英文名已被占用（多为重复改名，系统会自动核对并重试）"
    if ("127" in low or "操作过于频繁" in raw or "too many requests" in low
            or "rate limit" in low):
        return "网盘请求过于频繁（已自动退避重试）"
    if is_transient_guangya_fault(raw):
        # v2.1.12 #295：上游服务抖动（服务端 RPC 超时 / 5xx / 网络抖动）。
        # 这类故障下一轮会自动重试，不是「改名失败」，页面不该把它算成失败。
        return RENAME_TRANSIENT_LABEL
    if "locked" in low or "busy" in low:
        return "存储繁忙（已自动重试）"
    if not raw.strip():
        return "未记录原因"
    cleaned = re.sub(r"code\s*=\s*\d+", "", raw, flags=re.I)
    # v2.1.12 #295：屏蔽上游内部标识（服务名 / RPC 方法名 / 接口路径 / 业务码）。
    cleaned = scrub_upstream_internals(cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    return (cleaned[:120] or "未记录原因")


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
# v2.1.38（真机反馈）：用户**确认源文件已被删除**后可手动清理归档的终态。
# 刻意**不放进** RETRYABLE_FILE_STATUS —— 归档后：不再计入「待处理」、不再每轮被
# 优先排进队列重算、也不再参与自动补复制。行本身保留（项目红线：清理用归档，
# 不物理删除，历史行与执行记录必须留），源文件放回 A 库后下一轮同步会按同一行
# 复活重新复制（见 sync_rule 里 archived_rows 的分支）。
ARCHIVED_FILE_STATUS = "archived"
# v2.1.42（真机反馈）：单个文件**自动**复制尝试上限的兜底值（真值走运行时配置
# `mirror_max_file_attempts`，缺省即此值）。达上限后：
#   · 不再进 `retry_rows`（不会被自动补复制）；
#   · 不再让所属作品进 `_works_with_pending_files`（不会被每轮无条件排到队首）；
#   · 状态仍是 `failed`、原因里写清「已停止自动重试，需人工处理」——
#     所以它依然出现在「待重试文件」清单里，人工可「批量重试选中」（清零重算）
#     或「清理选中」（归档）。
MAX_FILE_ATTEMPTS_FALLBACK = 3


def max_file_attempts(db: Session) -> int:
    """当前生效的「自动复制最多试几次」。只读运行时配置，缺省/非法值回落 3。"""
    try:
        raw = runtime_config.get(db, "mirror_max_file_attempts")
        val = int(str(raw).strip() or MAX_FILE_ATTEMPTS_FALLBACK)
    except (TypeError, ValueError):
        val = MAX_FILE_ATTEMPTS_FALLBACK
    return max(1, val)


def file_attempts_exhausted(row, limit: int) -> bool:
    """这一行是否已经「自动试满、该停手了」。

    只对 `failed` 生效：`pending` / `copying` / `unknown` 一律继续自动重试 ——
    限流打回的 pending 不是真失败，不该吃掉重试额度。
    """
    return (str(getattr(row, "status", "") or "") == "failed"
            and int(getattr(row, "attempts", 0) or 0) >= int(limit or 1))


# 达上限后追加在原因后面的说明。必须让用户看懂「为什么不再自动跑了、我该点哪里」。
AUTO_STOP_NOTE = ("。已自动尝试 {n} 次仍未成功，已停止自动重试，需人工处理："
                  "勾选这一行点「批量重试选中」会重新给 {n} 次机会，"
                  "或点「清理选中」把它归档（归档不删文件，源文件放回来会自动恢复）")


def mark_file_failed(row, msg: str, limit: int) -> int:
    """把一个文件行标记为复制失败，并累加「已自动尝试次数」。

    返回累加后的次数。达到上限时在原因后追加 `AUTO_STOP_NOTE`，让页面直接告诉
    用户「已停止自动处理、下一步该点哪里」——不再只是把失败原因重复写一遍。
    """
    n = int(getattr(row, "attempts", 0) or 0) + 1
    row.attempts = n
    row.status = "failed"
    text = str(msg or "").strip() or "复制失败（未记录原因）"
    if n >= int(limit or 1):
        text = text + AUTO_STOP_NOTE.format(n=int(limit or 1))
    row.error = text[:500]
    row.updated_at = utcnow()
    return n


# ---------------- v2.1.50：作品级状态只有一套判据 ----------------
# 真机现象（2026-11-25 20:46 实测）：定向重试把某部作品 284 行全部改成 synced、
# target_name 也全有值，作品在页面上仍显示「失败」。
# 根因不是某一行算错，而是「作品状态由谁定」有三套实现、口径各不同：
#   ① 扫描轮（sync_rule 尾部）：看本作品所有文件行 —— 唯一正确的那套；
#   ② 任务轮询（poll_pending_tasks）：按「最后轮询到的那个任务」直接定生死，
#      同一部作品只要有第二个任务，状态就可能被翻转；
#   ③ 定向重试（retry_failed_files）：**根本不重算**，只更新 last_synced_at。
# 所以「重试成功了但界面还是失败」不是显示层的问题，是状态压根没被重算过。
# 收敛办法：判据只留 media_status_from，写入只留 reconcile_media_status，
# 三个调用点全部改走它 —— 以后新增任何改文件状态的路径也只需要调这一个。
MEDIA_TERMINAL_FILE_STATUS = ("synced", "superseded")


def media_status_from(rows) -> str:
    """一部作品的 status：只要还有文件行留着活要干就是 failed，否则 synced。

    唯一判据。`superseded`（被新内容取代）与 `synced` 一样算「这行没有活要干」；
    其余一切状态都算没干完 —— 包括 `archived`：归档行若被放回 A 库要复活复制，
    在复活完成前作品仍然不该显示「已同步」。
    """
    for r in rows:
        if str(getattr(r, "status", "") or "") not in MEDIA_TERMINAL_FILE_STATUS:
            return "failed"
    return "synced"


def reconcile_media_status(db: Session, media_id: int, *,
                           touch_synced_at: bool = False) -> str:
    """按 `media_status_from` 重算一部作品的状态并落库，返回新状态。

    作品不存在返回空串（调用方据此跳过）。内部是一次 select，会触发 autoflush，
    所以调用前刚在内存里改过的文件行状态一定会被算进去（不需要显式 flush）。
    """
    media = db.get(MirrorMedia, media_id)
    if not media:
        return ""
    rows = list(db.scalars(
        select(MirrorFile).where(MirrorFile.media_id == media_id)).all())
    media.status = media_status_from(rows)
    if touch_synced_at:
        media.last_synced_at = utcnow()
    return media.status


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
    """确定性自然排序键。S02 排在 S10 前、E9 排在 E10 前。

    返回由 (类型标记, 数值, 文本) 三元组组成的元组，保证任意片段可比较
    （纯 int/str 混排在 Python3 会 TypeError）。

    v2.1.12 #296：镜像处理顺序改为**完全按光鸭网盘自身的返回顺序**
    （请求侧已固定 orderBy=3 / sortType=1，即网页端的文件名升序）。
    本函数不再参与遍历/处理顺序，只保留给仍需要自然序的调用方（如展示层排序）。
    理由：用户口径是「顺序固定按网盘自身的文件名排序升序」——本地再排一次，
    等于用我们自己的规则覆盖网盘顺序，两边一旦不一致就变成「看到的和处理的不是一个序」。
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
    files = node.get("files") or []
    if any(_is_video(_dir_name(f)) for f in files):
        return True
    # v2.1.48：清单**看不全**时不能判「没有视频」。
    # 老代码只看 `files`：清单被按字符硬截成非法 JSON 后读回空列表，`_has_video`
    # 判 False → `_subtree_video` 判该目录没有视频 → 作品被 `_discover_works` 跳过，
    # 整部作品从 A 库消失（真机：完美世界(2021) 184 集）。
    # 现在只要「有文件但这个目录的清单不可信」就按「有视频」处理：宁可多算一部，
    # 也不能把作品判没 —— 真正的空目录（n_files=0）仍然判空，分类目录照常下探。
    if node.get("files_broken") or int(node.get("n_files") or 0) > len(files):
        return int(node.get("n_files") or 0) > 0
    return False


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
    # v2.1.12 #296：不再本地重排。栈按 children 原序反序入栈，
    # stack.pop() 得到的就是网盘返回的顺序（与网页端看到的顺序一致）。
    stack = [(cid, []) for cid in reversed(root_node.get("children") or [])]
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
            # v2.1.12 #296：分类目录下探同样按 children 原序反序入栈，不本地重排。
            for child in reversed(node.get("children") or []):
                stack.append((child, cats + [(node["name"], did)]))
            continue
        out.append((node, cats))
    return out


def work_key_of(work: dict) -> str:
    """作品稳定键，与 sync_rule / MirrorMedia.work_key 同口径（供轮转游标持久化）。"""
    return "work:" + normalize_work_name(work.get("name") or "")


def _works_with_pending_files(db: Session, rule_id: int) -> set[str]:
    """查出「有文件待复制/待重试」的作品键集合。

    v2.1.9：真机上 89 条 MirrorFile 卡在 pending（40 条「B库未找到已同步文件」
    + 49 条整部电影），原因是这类作品要等轮转游标转到才有机会被处理，
    而任何一轮异常中断都会让它们永远等下去。
    本函数让这些作品**无条件**排进本轮处理队列。

    口径：MirrorFile.status ∈ RETRYABLE_FILE_STATUS（pending/copying/failed/unknown）
    的行，按其所属 MirrorMedia.work_key 归集。**只读，不改任何状态**。

    v2.1.42（真机反馈）：**自动试满上限的失败行不再算「待处理」**。这类文件是
    永久性失败（源文件被删、B 库拒收、重名冲突），每轮排它到队首只会让那部作品
    每轮重做一遍、汇总里的「重试 N」永远不归零。现在它们归人工处理：行还在
    「待重试文件」清单里，但不再自动排队。pending / copying / unknown 不受影响
    （限流打回的 pending 不是真失败，不该吃掉额度）。
    """
    limit = max_file_attempts(db)
    rows = db.execute(
        select(MirrorMedia.work_key)
        .join(MirrorFile, MirrorFile.media_id == MirrorMedia.id)
        .where(MirrorMedia.rule_id == rule_id,
               MirrorFile.status.in_(sorted(RETRYABLE_FILE_STATUS)),
               or_(MirrorFile.status != "failed",
                   func.coalesce(MirrorFile.attempts, 0) < limit))
    ).all()
    return {str(k or "") for (k,) in rows if str(k or "").strip()}


def detect_increments(db: Session, rule: MirrorRule, works: list,
                      nodes: dict) -> tuple[list[str], dict]:
    """v2.1.41（用户口径）：每轮任务开始时，先查「已复制的作品有没有增量或变化」。

    用户的原话顺序是：① 按顺序把 A 库全量镜像过去；② **每轮开始时优先检查已复制
    作品是否有增量/变化，有则优先处理，没有才继续复制存量**；③ 完全新进来的作品按
    目录原有既定顺序排队。

    本函数只负责 ② 的「发现」部分，判定**纯内存 + 一次行查询、零接口调用**：
      - A 树里出现映射清单里没有的 fileId → 新增（含「已手动清理」后源文件回归）；
      - 同一 fileId 但 size 段变了          → 内容变化；
      - 只比 size 段（`_fp_size`），不看 utime —— 否则网盘侧元数据抖动会造成假增量。

    口径边界：
      - **只算「已经镜像过的作品」**。完全没建过映射的新作品不算增量，它们走存量
        轮转、按目录既定顺序进入（口径 ③）；
      - `superseded` 与 `archived` 的行不计入「已知」，所以源文件回归会自然变成新增；
      - 判定结果交给 `sync_rule` 排到队列最前面，处理完这一轮就不再是增量。

    返回 (work_key 列表, stats)。
    """
    out = {"works": 0, "new_files": 0, "changed_files": 0}
    rows = db.execute(
        select(MirrorFile.media_id, MirrorFile.source_file_id,
               MirrorFile.fingerprint, MirrorFile.status)
        .where(MirrorFile.rule_id == rule.id)).all()
    known: dict[int, dict[str, str]] = {}
    for mid, fid, fp, status in rows:
        sid = str(fid or "")
        st = str(status or "")
        if not sid or st in ("superseded", ARCHIVED_FILE_STATUS):
            continue
        known.setdefault(int(mid), {})[sid] = _fp_size(str(fp or ""))

    by_key: dict[str, MirrorMedia] = {}
    for m in db.scalars(select(MirrorMedia).where(
            MirrorMedia.rule_id == rule.id)).all():
        k = str(m.work_key or "")
        if not k:
            continue
        by_key[k] = m
        if not k.startswith("work:"):
            by_key["work:" + k] = m

    hits: list[str] = []
    for work, _cats in works:
        wk = work_key_of(work)
        media = by_key.get(wk)
        if media is None:
            continue                      # 没镜像过 → 新作品，走存量轮转
        # v2.1.41：映射清单里有、但一行「已知文件」都没有（例如整部都被手动清理
        # 归档了）→ 不能当成「从没镜像过」跳过，否则源文件回来了也要干等轮转。
        # 这种情况 A 树里的视频一律算新增。
        old = known.get(media.id) or {}
        n_new = n_chg = 0
        for _node, item in _media_files(work, nodes):
            fid = str(item.get("fileId") or "")
            if not fid or not _is_video(_dir_name(item)):
                continue
            size = _fp_size(_fingerprint(item))
            if fid not in old:
                n_new += 1
            elif old[fid] != size:
                n_chg += 1
        if n_new or n_chg:
            hits.append(wk)
            out["works"] += 1
            out["new_files"] += n_new
            out["changed_files"] += n_chg
    return hits, out


def plan_work_batch(works: list, max_works: int, cursor: str = "",
                    hard_max: int = 100,
                    mapped_keys: set[str] | None = None) -> tuple[list, str, bool]:
    """v0.9.0：把一次镜像要处理的作品按「单轮上限 + 轮转游标」切成一批。

    - max_works<=0：不限量，返回全部（仍受 hard_max 防御性上限约束，保持旧行为）。
    - max_works>0：从 cursor 之后接着取，取满 max_works 部为止；到末尾则从头回绕。
      返回 (本批作品, 新的游标=本批最后一部作品的 key, 是否发生回绕)。
    - 游标是「上一轮最后处理完的作品 key」，因此同一部剧绝不会被拆到两轮中间：
      sync_rule 对每部作品是原子处理（文件复制 + 批量轮询完成才进入下一部），
      正好满足用户「第一部剧不要间断，整理完再休息」。
    - 到上限即停，剩余作品留给下一次定时同步接着做，不铺满一屏、不瞬时压满网盘。

    v2.1.44（用户口径，两条硬要求）：
      ① **每轮必须接着上一轮的游标往后走，不能每轮从目录头部重扫**；
      ② **单轮名额就是配置的 `max_works_per_run`，一部不多**——不另开「不占名额」
         的旁路批（那会让单轮实际变成 2 倍配置量）。

    传入 `mapped_keys`（已镜像作品的 work_key 集合）时启用新口径——**分两层**：
      - **第一层：全新作品占名额**。从游标之后按 A 库目录既定顺序排未处理作品，
        「排到全新作品的占用本轮名额」。未镜像作品被复制后自然变成已镜像，
        下一轮就不会再排在前面——口径自洽，不需要额外游标，也不会出现
        「某部卡住就永远卡在队首」的头阻塞。
      - **第二层：名额没被新作品填满时，按游标顺序用剩余作品补满**（存量仍要推进，
        否则 C/B 类判定会长期不覆盖它们），总长仍严格等于 max_works。
      - 游标取**本轮批里最后处理完的那一部**（与 v0.9.0 同义），下一轮从它之后
        接着走：既不重复扫头部，也不会永远跳过任何一部（目录末尾会自然回绕）。
      - 注：**已镜像作品的作品内变化（增量）不在本函数范围内**——它们由 sync_rule
        的 `incr_works` 排在队首优先处理，且**不占本轮名额**（见 sync_rule 注释）。
      - 传 None（默认）时**完全保持 v0.9.0 旧语义**，其他调用点零影响。
    """
    if not works:
        return [], "", False
    if max_works <= 0:
        # 不限量（配置为 0）：沿用旧行为，一次给到 hard_max 防御上限
        return works[:hard_max], "", False
    keys = [work_key_of(w) for w, _ in works]
    n = len(keys)
    start = 0
    if cursor:
        try:
            start = keys.index(cursor) + 1
        except ValueError:
            start = 0  # 游标指向的作品已不在 A 库，从头开始
        if start >= n:
            start = 0  # 上一轮已到末尾，回绕
    if mapped_keys is None:
        # ---- v0.9.0 旧口径：纯按目录顺序切，末尾不足从头部补齐 ----
        end = start + max_works
        batch = works[start:end]
        wrapped = end > n
        if wrapped and len(batch) < max_works:
            batch = batch + works[:max_works - len(batch)]
        new_cursor = work_key_of(batch[-1][0]) if batch else ""
        return batch, new_cursor, wrapped
    # ---- v2.1.44：游标之后未镜像优先，取不满用已镜像存量补位，名额不变 ----
    ring = list(range(start, n)) + list(range(0, start))
    picked: list[int] = [i for i in ring if keys[i] not in mapped_keys][:max_works]
    if len(picked) < max_works:
        _got = set(picked)
        for i in ring:
            if i in _got:
                continue
            picked.append(i)
            if len(picked) >= max_works:
                break
    if not picked:
        return [], "", False
    batch = [works[i] for i in picked]
    # 游标 = 本轮批里**最后处理完的那一部**（与 v0.9.0 同义），下一轮从它之后接着走。
    # 用 picked[-1] 而不是「环形位置最大者」：批的实际处理顺序是 picked 的顺序
    #（未镜像段在前、存量补位段在后），最后落地的就是 picked[-1]，语义与旧口径一致。
    wrapped = max((i - start) % n for i in picked) >= n - 1
    return batch, keys[picked[-1]], wrapped


# ============================================================================
# 镜像1.1（2026-11-21）：单轮开始的 A树/B树/映射清单三方对比预扫描（查漏补缺）。
# 需求 a/b/c/d 见 MIRROR_1.1_DESIGN.md。设计红线：
#   - complete=False（A 树不完整）跳过 c/b 判定，绝不误删；
#   - 镜像模式 mirror_mode：full（默认，全镜像）= C 类 A 已删 → 删 B 库对应文件+清清单；
#     half（半镜像）= C 类 B 文件与映射清单都保留、完全不动。边界：A 库只读；
#   - 零跨 await 持写事务：网络 await 前先 commit；
#   - 性能护栏 max_prepass_media 限制单轮核对部数。
# ============================================================================

def _build_a_facts(nodes: dict, works: list) -> tuple[set[str], dict[str, str], set[str]]:
    """从 A 全树构建事实集（纯内存零网络）：文件ID集、{fileId:指纹}、作品键集。"""
    a_file_ids: set[str] = set()
    a_source_fps: dict[str, str] = {}
    for node in nodes.values():
        for item in node.get("files") or []:
            fid = str(item.get("fileId") or "")
            if not fid:
                continue
            a_file_ids.add(fid)
            a_source_fps[fid] = _fingerprint(item)
    a_work_keys = {work_key_of(w) for w, _ in works}
    return a_file_ids, a_source_fps, a_work_keys


def _clear_media_mappings(db: Session, media: MirrorMedia, *, mark_retired: bool = True,
                          status: str = "pending", reason: str = "") -> int:
    """清一部作品的无效映射清单（C 类默认动作）：重置 MirrorFile 字段，不物理删行。

    复用 prune_stale_mappings 的"重置 pending"语义（mirror.py:2216-2224），
    让这些行不参与后续核对；mark_retired 时把 MirrorMedia.status 标 failed 留痕。
    返回清理的文件行数。不删 B 库文件、不删 DB 行（物理删走手动端点）。

    v2.1.36（真机反馈）：C 类（A 库已无该作品，源文件被删）默认仍重置为 `pending`，
    可 A 库已经没有源文件了，补复制永远不可能成功 —— 这些行会一直挂在「待复制」，
    问题诊断的「N 项待处理」就永远清不掉。所以加了 `status` / `reason` 两个参数：
    C 类传 `failed` + 明确原因，让用户看得到「为什么、该怎么办」。
    """
    n = 0
    for row in db.scalars(select(MirrorFile).where(
            MirrorFile.media_id == media.id,
            # v2.1.38：已由用户手动归档的行**不动**。否则整部作品从 A 库消失时
            # C 类每轮都会把 archived 翻回 failed，用户清理完又冒出来，等于白清。
            MirrorFile.status.notin_(("pending", ARCHIVED_FILE_STATUS)))).all():
        row.status = status
        if reason:
            row.error = reason
        elif status != "pending":
            row.error = ""
        row.target_file_id = ""
        row.target_name = ""
        row.rename_status = ""
        row.rename_error = ""
        row.task_id = ""
        n += 1
    if mark_retired:
        media.status = "failed"
    db.commit()
    return n


def archive_retryable_files(db: Session, rule: MirrorRule, ids: list[int],
                            *, dry_run: bool = False) -> dict:
    """v2.1.38：把「用户已确认源文件被删除」的待处理文件行**归档**（不删行）。

    背景（真机反馈）：A 库源文件被用户删掉后，补复制客观上不可能成功 —— v2.1.37 已把
    这类行收敛成「复制失败 + 明确原因」，但用户明明知道片被删了，只希望把这条记录从
    「问题诊断 → N 项待处理」里清掉、别再每轮重算一遍。此前的清理入口只有
    「清理无效影视映射」—— 那是**整部作品 + 联网核对 B 库**，颗粒太粗、还要联网。

    口径（与项目红线一致：清理用归档，不物理删除）：
      - 只处理 status ∈ RETRYABLE_FILE_STATUS 的行；synced / superseded 一律不动
        （已同步的记录不是「待处理」，绝不能借这个按钮把正常映射清掉）；
      - 归档后 status=archived，`error` **保留原原因**并追加「已确认…手动清理归档」，
        用户事后仍能查到当初为什么没复制成功；
      - 清 target_file_id / target_name / rename_* / task_id（与「重置」同口径）；
      - **不删行、不删 B 库文件、不动资源 / 分享 / 队列**；
      - **可恢复**：源文件放回 A 库后，下一轮同步枚举到同一 (fileId, 指纹) 会按**同一行**
        复活重新复制（不新建行，避免撞 (media_id, source_file_id, fingerprint) 唯一键）；
      - **不联网**：纯 DB 操作，用户点一下就生效，不受光鸭接口抖动影响。

    返回 {"ok", "selected", "archived", "skipped", "dry_run", "ids"}。
    """
    wanted = [int(x) for x in (ids or []) if str(x).strip().lstrip("-").isdigit()]
    stmt = select(MirrorFile).where(
        MirrorFile.rule_id == rule.id,
        MirrorFile.status.in_(sorted(RETRYABLE_FILE_STATUS)))
    if wanted:
        stmt = stmt.where(MirrorFile.id.in_(wanted))
    rows = list(db.scalars(stmt.order_by(MirrorFile.id)).all())
    out = {"ok": True, "selected": len(wanted), "archived": len(rows),
           "skipped": max(0, len(wanted) - len(rows)) if wanted else 0,
           "dry_run": bool(dry_run), "ids": [r.id for r in rows],
           "note": "已归档的记录只是从「待处理」里移除，没有删除任何文件与历史；"
                   "源文件放回 A 库后下一轮同步会自动恢复并重新复制"}
    if dry_run or not rows:
        return out
    suffix = ("（已确认 A 库源文件被删除，用户手动清理归档；"
              "源文件放回 A 库后重新同步会自动恢复）")
    for row in rows:
        old = str(row.error or "").strip()
        row.status = ARCHIVED_FILE_STATUS
        row.error = ((old + " · " + suffix) if old else suffix)[:500]
        row.target_file_id = ""
        row.target_name = ""
        row.rename_status = ""
        row.rename_error = ""
        row.task_id = ""
        row.updated_at = utcnow()
    db.commit()
    return out


def purge_archived_files(db: Session, rule: MirrorRule, ids: list[int],
                         *, dry_run: bool = False) -> dict:
    """v2.1.40（真机反馈）：把「已手动清理」的归档记录**彻底删除**。

    与 `archive_retryable_files` 的分工：
      - 归档（v2.1.38）= 从「待处理」里摘出去，行保留、源文件放回来自动复活；
      - 彻底删除（本函数）= 用户明确表示「这两条记录我不想再看到」时的最终清理。

    约束收到最小面（这是唯一一处会物理删 MirrorFile 行的入口）：
      - **只允许删 status == archived 的行**；synced / pending / copying / failed /
        superseded 一律跳过 —— 绝不借这个入口误删正常映射或待处理项；
      - 指定 ids 时逐条校验：不存在的、非 archived 的都计入 skipped，不报错；
      - 不删任何物理文件、不动 A/B 库、不动资源 / 分享 / 队列 / 执行记录；
      - 行删掉后源文件若放回 A 库，下一轮同步按**新行**重建复制。
        （归档复活机制只在行还存在时生效；行没了就是全新行，不会撞唯一键。）

    返回 {"ok", "selected", "purged", "skipped", "dry_run", "ids"}。
    """
    wanted = [int(x) for x in (ids or []) if str(x).strip().lstrip("-").isdigit()]
    stmt = select(MirrorFile).where(
        MirrorFile.rule_id == rule.id,
        MirrorFile.status == ARCHIVED_FILE_STATUS)
    if wanted:
        stmt = stmt.where(MirrorFile.id.in_(wanted))
    rows = list(db.scalars(stmt.order_by(MirrorFile.id)).all())
    out = {"ok": True, "selected": len(wanted), "purged": len(rows),
           "skipped": max(0, len(wanted) - len(rows)) if wanted else 0,
           "dry_run": bool(dry_run), "ids": [r.id for r in rows],
           "note": "只删除了这些已手动清理的记录本身，没有动任何文件与 A/B 库"}
    if dry_run or not rows:
        return out
    for row in rows:
        db.delete(row)
    db.commit()
    return out


async def reconcile_mappings_prepass(db: Session, rule: MirrorRule, nodes: dict,
                                      works: list, api: GuangYaWebFiles, stats: dict,
                                      *, complete: bool) -> tuple[list[str], list[str]]:
    """镜像1.1：单轮开始三方对比，输出 (B 类重排键, A 类缺集键)。

    a) 单文件变化/缺集：只统计+登记缺集键，绝不在这里改 MirrorFile。
       A 库还在但 B 库逐集反查不到（且 B 可见数 < 应同步数，排除改名后缀造成的
       name_mismatch 误伤）→ 缺集剧键进 missing_keys，交 sync_rule 作为
       **不占 max_works 部数预算**的优先补丁批次处理；补复制仍由主循环 v0.8.7
       （1326-1354）复用同一行完成，预扫描提前清 pending 会丢 missing_in_b 统计；
    b) 整部在 B 缺失（A 还在）：只识别+返回优先重处理键；重置/补复制/计数
       一律交给主循环 v0.8.7——预扫描提前清 pending 会让 synced_rows 变空、
       missing_in_b 丢失，且误伤 name_mismatch 保护场景。B 类重排进本批、
       按用户确认「计入第二轮剧部数」占 max_works 名额；
    c) A 库已无该作品：默认清清单+统计；full 模式且 complete 时真删 B 库文件；
    d) 无变化：不动作。

    任何判定都要求 complete=True（A 树完整），否则跳过 c/b。全程无跨 await 写事务。
    返回 (reprocess_keys, missing_keys)：前者重排进 works 占部数，后者单独优先
    批次、不计「整理整部剧」部数预算（设计文档 MIRROR_1.1_DESIGN.md 2.4 节）。
    """
    prepass = {"enabled": True, "complete": complete, "mirror_mode": "full",
               "c_deleted_a": 0, "c_b_files_removed": 0, "c_mapping_cleared": 0,
               "c_kept_in_half_mode": 0, "c_delete_failed": 0,
               # v2.1.40：C 类命中量 / 异常护栏 / 单轮额度 / B 库反查超额
               "c_candidates": 0, "c_anomaly": 0, "c_deferred": 0, "b_over_budget": 0,
               "b_invalid_reprocess": 0, "a_single_file_fix": 0,
               "a_missing_episodes": 0, "d_unchanged": 0,
               "skipped": 0}
    stats["prepass"] = prepass
    reprocess_keys: list[str] = []
    missing_keys: list[str] = []
    try:
        enabled = str(runtime_config.get(db, "mirror_prepass_enabled") or "true").lower() in (
            "1", "true", "yes", "on")
        if not enabled:
            prepass["enabled"] = False
            return reprocess_keys, missing_keys
        try:
            max_media = max(0, int(runtime_config.get(db, "mirror_prepass_max_media") or 50))
        except (TypeError, ValueError):
            max_media = 50

        a_file_ids, a_source_fps, a_work_keys = _build_a_facts(nodes, works)
        # v2.1.40（真机反馈）：C 类必须覆盖**全库**，不能被 `[:max_media]` 截断。
        #
        # 病根：`mirror_prepass_max_media` 的本意是抑制 B 库反查（api_b_candidates_safe）
        # 的接口风暴 —— 见 runtime_config.py「性能护栏，防接口风暴」注释。而 C 类
        # （A 库已无该作品）判定只用 A 树事实集（a_file_ids / a_work_keys，纯内存 set），
        # 一次接口都不打。此前两者被同一个 `[:max_media]` 切片一起截断，后果是
        # **只有最老入库的 50 部作品有「整部消失」保护，且永不轮转**：线上 260 部 →
        # 覆盖率 19%，被保护的那 50 部恰好是最不可能被删的老片。
        # 真机证据（2026-11-25）：「缅北电诈覆灭纪实」media id=220，A 库目录
        # 1955014968956665857 仍在但已空（list 成功、0 项），作品已不被 _discover_works
        # 识别 → 属 C 类；却因 id 220 在 LIMIT 50 之外从未被扫到，2 条记录挂了整周。
        all_medias = list(db.scalars(select(MirrorMedia).where(
            MirrorMedia.rule_id == rule.id).order_by(MirrorMedia.id)).all())
        # B/A 类要反查 B 库，仍按 max_media 限量（性能护栏语义不变）
        probe_ids = ({m.id for m in all_medias[:max_media]} if max_media > 0
                     else {m.id for m in all_medias})
        # C 类命中量先算一遍（一次聚合查询、零接口调用），用于「批量异常」护栏：
        # 一轮之内命中两三成库 ≈ A 树枚举出错，而不是用户真删了这么多片。此时宁可
        # 漏清一轮也不能动刀 —— full 镜像下 C 类会**真删 B 库文件**。
        src_by_media: dict[int, set[str]] = {}
        for _mid, _sid in db.execute(
                select(MirrorFile.media_id, MirrorFile.source_file_id)
                .where(MirrorFile.rule_id == rule.id)).all():
            _s = str(_sid or "").strip()
            if _s:
                src_by_media.setdefault(int(_mid), set()).add(_s)
        c_candidates = 0
        if complete:
            for _m in all_medias:
                _sids = src_by_media.get(_m.id) or set()
                if not _sids:
                    continue
                _wk = str(_m.work_key or "")
                if _wk and not _wk.startswith("work:"):
                    _wk = "work:" + _wk
                if not (_sids & a_file_ids) and _wk not in a_work_keys:
                    c_candidates += 1
        _c_guard = max(10, int(len(all_medias) * 0.2)) if all_medias else 10
        # 一次命中两三成库 ≈ A 树枚举出错，而不是用户真删了这么多片。
        # 不用「A 库一部作品都没枚举到」单独做条件：那会把「小库被删空」这种
        # 真实且合法的场景也挡掉（此时命中数本来就小，比例护栏不会误伤）。
        c_anomaly = bool(c_candidates) and c_candidates > _c_guard
        prepass["c_candidates"] = c_candidates
        prepass["c_anomaly"] = c_candidates if c_anomaly else 0
        # 单轮最多对这么多部「已无作品」动 B 库文件（清映射不受限，删文件受此节流）
        c_delete_budget = max_media if max_media > 0 else len(all_medias)

        for media in all_medias:
            wk = str(media.work_key or "")
            # 镜像1.1：兼容 legacy 无前缀 work_key。C 类判定与 B 类重排都依赖与
            # work_key_of() 同口径的 "work:" 前缀，否则会把 A 还在的作品误判为已删。
            if wk and not wk.startswith("work:"):
                wk = "work:" + wk
            rows = list(db.scalars(select(MirrorFile).where(
                MirrorFile.media_id == media.id)).all())
            if not rows:
                continue
            src_ids = {str(r.source_file_id or "") for r in rows if r.source_file_id}
            in_a = src_ids & a_file_ids

            # C 类：A 库已无该作品（所有源文件都不在 A 树，且作品键也不在工作集）。
            # 镜像模式（用户口径 2026-11-21，边界=A 库只读）：
            #   full（默认，全镜像）→ 自动删 B 库对应文件 + 清映射清单，A/B 保持一致；
            #   half（半镜像）       → B 库文件与映射清单都原样保留，完全不动（B 是 A 超集）。
            # 只有 A 树完整（complete=True）才进入本分支，绝不误删；删 B 库走已抓包
            # api.delete_files（异步任务），失败只记录不中断；网络 await 前不持写事务。
            if complete and not in_a and wk not in a_work_keys:
                # v2.1.40：命中量异常（或 A 树一部作品都没枚举到）→ 本轮一律不动数据。
                # full 镜像下这里会真删 B 库文件，一次可疑的枚举不能拿来当删除依据。
                if c_anomaly:
                    continue
                # v2.1.40：幂等。上一轮已经处理干净的行（都在终态且 B 库句柄已清）不再
                # 重复计数、重复写库 —— 否则覆盖全库后「A库已无作品 N 部」会每轮虚报，
                # 而且每部作品一次 commit，白耗一轮。
                _live = [r for r in rows
                         if str(r.status or "") not in ("failed", "superseded",
                                                        ARCHIVED_FILE_STATUS)
                         or str(r.target_file_id or "").strip()]
                if not _live:
                    continue
                mode = str(runtime_config.get(db, "mirror_mode") or "full").lower().strip()
                prepass["mirror_mode"] = "half" if mode in (
                    "half", "semi", "partial", "0", "false", "off") else "full"
                full_mirror = prepass["mirror_mode"] == "full"
                if not full_mirror:
                    prepass["c_kept_in_half_mode"] += 1
                    continue  # 半镜像：清单与 B 文件都保留
                # v2.1.40：单轮额度用尽就整部留到下一轮（不半途清空 B 库句柄，
                # 否则 target_file_id 被清就再也拿不到 b_ids，B 库文件永久残留）。
                if c_delete_budget <= 0:
                    prepass["c_deferred"] += 1
                    continue
                c_delete_budget -= 1
                prepass["c_deleted_a"] += 1
                # 必须在清清单前捕获 b_ids：_clear_media_mappings 会清空 ORM 行的
                # target_file_id，rows 与其共享 identity map，事后取不到。
                b_ids: list[str] = [str(r.target_file_id) for r in rows
                                    if r.target_file_id and str(r.target_file_id).strip()]
                # 先清清单（不跨 await），再删 B 库（网络 await 前已 commit）
                #
                # v2.1.36（真机反馈）：A 库已无该作品时，源文件**客观上不存在**，
                # 补复制永远不可能成功。此前这里统一重置为 pending，这些行就一直挂在
                # 「待复制」→ 问题诊断的「N 项待处理」永远清不掉、也没有任何入口能处理。
                # 改成 failed + 明确原因：用户看得到「为什么、该怎么办」，也可用
                # 「清理无效影视映射」一次性收掉；源文件放回 A 库后下一轮会重新建行复制。
                prepass["c_mapping_cleared"] += _clear_media_mappings(
                    db, media, status="failed",
                    reason="A 库已无该作品（源文件已被删除），B 库对应文件已按全镜像清理；"
                           "把源文件放回 A 库后重新同步即可恢复")
                if b_ids:
                    try:
                        await api.delete_files(b_ids)
                        prepass["c_b_files_removed"] += len(b_ids)
                    except Exception as exc:
                        logger.warning("[mirror1.1] C类(full)删B库失败 wk=%s：%s", wk, exc)
                        prepass["c_delete_failed"] = prepass.get("c_delete_failed", 0) + 1
                continue

            # 作品 A 库还在 → B 类整部缺失 / A 类单文件缺集或变化 / D 无变化
            if not complete:
                prepass["skipped"] += 1
                continue
            # v2.1.40：B/A 类要逐部反查 B 库（接口开销），仍按 max_media 限量。
            # 超出预算的作品本轮只享受上面那段零接口开销的 C 类判定，其余留给下一轮。
            if media.id not in probe_ids:
                prepass["b_over_budget"] += 1
                continue
            # v2.1.31：只比 size 段 —— utime 单变不算内容变化（见 `_fp_size` 注释）
            changed_fp = sum(1 for r in rows if r.source_file_id in a_source_fps
                             and _fp_size(a_source_fps[r.source_file_id])
                             != _fp_size(str(r.fingerprint or "")))
            # 只在「该作品本轮会被选」的范围内做 B 库核对，控制接口开销
            candidates, verified = await api_b_candidates_safe(api, media)
            if not verified:
                prepass["skipped"] += 1
                continue
            visible = {str(v) for v in candidates.values()}
            if not visible:
                # B 库整部为空 + A 库还有 → B 类：失效。只登记优先重处理键，
                # 不在这里重置 MirrorFile——重置/补复制/计数由主循环 v0.8.7
                # 统一完成，避免 synced_rows 被提前清空导致 missing_in_b 丢失。
                prepass["b_invalid_reprocess"] += 1
                reprocess_keys.append(wk)
                continue
            # A 类缺集识别（镜像1.1 补齐，真机「我不是大师 S01E23」根因）：
            # 映射清单标 synced 且 A 库仍在的集，若 B 库逐集反查不到就是缺集。
            # 判定口径与主循环 v0.8.7（1336-1360）严格一致：只有 B 可见文件数
            # 少于应同步数才判缺集；B 数 >= synced 数时多为改名加后缀的
            # name_mismatch，绝不重排，避免整部重复复制。
            synced_a_rows = [r for r in rows
                             if r.status == "synced" and r.source_file_id in a_file_ids]
            b_file_count = len({v for k, v in candidates.items() if ":" in k})
            missing_rows = ([r for r in synced_a_rows if not _lookup_b_file(candidates, r)]
                            if b_file_count < len(synced_a_rows) else [])
            if missing_rows:
                prepass["a_missing_episodes"] += len(missing_rows)
                prepass["a_single_file_fix"] += 1
                # 缺集剧优先进入本轮，但作为「补丁批次」单独处理、不占 max_works
                # 部数预算；补复制仍由主循环复用同一行完成，这里绝不改 MirrorFile。
                missing_keys.append(wk)
            elif changed_fp or len(in_a) < len(src_ids):
                # A 有更新 或 A 源文件数 > 已同步源文件集合 → A 类单文件补齐（不计部数）
                prepass["a_single_file_fix"] += 1
            else:
                prepass["d_unchanged"] += 1
        return reprocess_keys, missing_keys
    except Exception as exc:
        logger.warning("[mirror1.1] 三方对比预扫描异常（不阻断同步）：%s", exc)
        stats.setdefault("prepass", prepass)["error"] = str(exc)[:200]
        return [], []


async def api_b_candidates_safe(api: GuangYaWebFiles, media: MirrorMedia) -> tuple[dict, bool]:
    """镜像1.1：复用现有 _b_file_candidates 的 B 目录核对（含 verified 语义）。"""
    return await _b_file_candidates(api, media)


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


def _ext(name: str) -> str:
    return "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""


def _is_video(name: str) -> bool:
    return _ext(name) in VIDEO_EXTS


def _fingerprint(item: dict) -> str:
    """A 库文件的指纹串。

    ⚠️ v2.1.31：**这个格式不能改**（仍是 `size:utime`）——库里 `mirror_files.fingerprint`
    存的就是它，改格式会让全部存量行与新值对不上，被当成「A 库新文件」整库重拷一遍。
    变化判定要剔除 utime 的话，走 `_fp_size()` 只比 size 段，见下面的比较点。
    """
    return f"{int(item.get('fileSize', 0) or 0)}:{int(item.get('utime', 0) or 0)}"


def _fp_size(value: str) -> str:
    """取指纹里的 **size 段**，即「内容身份」。

    v2.1.31（地道战假追更·治触发源 2）：utime 是网盘侧元数据修改时间，任何一次
    重扫/触碰都可能让它单独跳动。它一旦变了，`(fileId, 指纹)` 就命中不了 → 本轮
    把文件当成「A 库新内容」→ 重新复制一份到 B 库 → B 库 fileId 变 → 作品级快照
    指纹（`sync_library_versions` 里的 `file_id:size`）跟着变 → 新版本 → **假追更**。
    这条链路绕过了 scanner 的 size-only 判定，是另一个独立入口。

    与 scanner（v2.1.25）、`sync_library_versions`（v2.1.6）统一为「内容身份 =
    file_id + size」：这里只比 size 段，utime 一律不参与判定。
    兼容两种存量写法：老格式 `size:utime` 与新口径 `size`。
    """
    return str(value or "").split(":", 1)[0]


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
    seen_ids: set[str] = set()
    while page < 100:  # 单目录最多 5000 条，防接口异常造成死循环
        data = await api.list_files(parent_id, page, LIST_PAGE_SIZE)
        batch = list(data.get("list") or [])
        fetched += len(batch)
        # v2.1.12 #296：顺序完全交给网盘，这里必须防「翻页期间顺序变动」造成重复。
        # 之前本地重排会掩盖这个问题（重排后重复项相邻，指纹去重能兜住）；
        # 现在按返回序直用，重复项会变成同一文件被提交两次复制/改名 → 必须先按 fileId 去重。
        for it in batch:
            fid = str(it.get("fileId") or "")
            if fid and fid in seen_ids:
                continue
            if fid:
                seen_ids.add(fid)
            items.append(it)
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


async def _list_dir_retry(api: GuangYaWebFiles, parent_id: str, *,
                         retries: int = 3) -> tuple[list[dict], bool]:
    """v2.1.41：单个目录的列目录，带指数退避重试。

    全量 walk 一次要列上千个目录，任何一次瞬时抖动都不该让整轮白跑；但也绝不能
    把「重试后仍拿不全」当成正常结果 —— 那种情况仍返回 ok=False，由调用方的
    `complete` 拦截（红线：禁止用不完整批次做变更）。

    重试策略按项目既有口径：code=127 / 网络抖动用**间隔 + 退避**根治，
    不靠把并发降到 1。
    """
    last_exc: Exception | None = None
    for i in range(max(1, int(retries))):
        try:
            items, ok = await _list_dir(api, parent_id)
        except Exception as exc:          # 网络 / 限流 / 5xx：退避后重试
            last_exc = exc
            await asyncio.sleep(min(8.0, 0.5 * (2 ** i)))
            continue
        if ok:
            return items, True
        last_exc = None                   # 接口成功但分页不全：也重试
        await asyncio.sleep(min(8.0, 0.5 * (2 ** i)))
    if last_exc is not None:
        raise last_exc
    return [], False


# ---------- v2.1.43：A 库目录清单缓存（轻量扫描的底座） ----------
# 必列的层级：0=源根、1=分类目录、2=作品目录。更深（季 / 集 / 子目录）在 light 模式下
# 只在「缓存 utime 变了 / 该作品有未完成文件 / 到深度兜底周期」时才真的去列。
# 为什么作品层也必须每轮列：新集加在 Season 目录里时，作品目录自身的 utime **不会变**
#（实测「国产演唱会」目录 utime 2026-09-24 06:23，而子目录内容 18:21 才变）——
# 只有列一遍作品目录，才能看到季目录自己的 utime 有没有变。
MANIFEST_ALWAYS_LIST_DEPTH = 2

# ---------------------------------------------------------------------------
# v2.1.48（真机事故）：清单落库**绝不能按字符硬截**
#
# 取证：「完美世界(2021)」184 个文件、`json.dumps(...)` 恰好 200000 字符，老代码把
# 这个字符串切到 20 万字符 —— 直接切在 JSON 中间，落库的是一段**非法 JSON**。
# 下一轮 `load_dir_manifest` 解析失败 → `files=[]`（当成「这个目录没有文件」）→
# `_subtree_video` 判「没有视频」→ 作品目录被 `_discover_works` 直接跳过 ——
# 于是 184 个文件对应的作品**从 A 库发现结果里消失**，184 行复制永远失败、
# 也不会自愈（缓存 utime 没变，轻量扫描一直复用这份坏清单）。
#
# 修法两条一起上：
#   ① 落库改为「预算内**完整装下整条**」：JSON 永远合法，装了多少条另存 `n_files`
#      真实条数（`n_files > len(files)` 即「清单被截」，不新增列也能判断）；
#   ② 解析失败不再当成空目录：标 `files_broken`，并把内存里的 utime 归 0 →
#      本轮强制重列该目录，重列后写回合法 JSON，自愈。
#
# 预算单位是**字节**（老代码是字符，中文按 3 字节算，同样的字符数会更小），所以默认
# 给到 1 MB：184 集这种目录要能一条不落地装下（丢一条 = 少镜像一集）。
MANIFEST_MAX_FILE_BYTES = 1024 * 1024
MANIFEST_MAX_CHILDREN_BYTES = 256 * 1024


def _dump_items_capped(items: list, max_bytes: int) -> tuple[str, int]:
    """把条目列表序列化成**合法** JSON，体积不超过 `max_bytes`，返回 (json, 装下的条数)。

    与老代码的区别只有一句：**按整条累加，绝不切字符**。老写法把 json 字符串整个
    切到 20 万字符，会产出非法 JSON，读取端解析失败后当空目录 —— 一部
    184 集的剧就这样整部消失。宁可少装几条（剩余条目由 `n_files` 标出「被截」），
    也不能产出读不回来的字符串。
    """
    if not items:
        return "[]", 0
    kept: list = []
    total = 2                                   # "[]"
    for it in items:
        piece = json.dumps(it, ensure_ascii=False)
        cost = len(piece.encode("utf-8")) + (1 if kept else 0)
        if total + cost > max_bytes:
            break
        kept.append(it)
        total += cost
    return json.dumps(kept, ensure_ascii=False), len(kept)


def _manifest_max_bytes(db: Session, key: str) -> int:
    """清单单个目录的体积上限（字节）。0 或读不到 → 回落模块默认值。"""
    try:
        kb = int(runtime_config.get(db, key) or 0)
    except (TypeError, ValueError):
        kb = 0
    return max(0, kb) * 1024 if kb > 0 else MANIFEST_MAX_FILE_BYTES



def load_dir_manifest(db: Session, rule_id: int) -> dict[str, dict]:
    """把上次的 A 库目录清单读进内存（一次查询、零接口调用）。

    v2.1.48：解析失败不再静默当成「空目录」——
      · `files_broken=True` 传下去，`_has_video` 据此**不判「没有视频」**，
        作品不会因为它从 A 库发现结果里消失；
      · 内存里的 `utime` 归 0，`_walk_source` 的「缓存可复用」判据必然不成立 →
        本轮强制重列这个目录，重列后写回合法 JSON，坏行自愈。
    """
    out: dict[str, dict] = {}
    try:
        rows = db.scalars(select(MirrorDirScan).where(
            MirrorDirScan.rule_id == rule_id)).all()
    except Exception as exc:      # 老库未迁移等异常：退化成「没有缓存」，走全量
        logger.warning("[mirror] 读取目录清单失败（按无缓存处理）：%s", exc)
        return out
    broken = 0
    for r in rows:
        files_broken = False
        try:
            files = json.loads(r.files_json or "[]")
            if not isinstance(files, list):
                files, files_broken = [], True
        except Exception:
            files, files_broken = [], True
        if files_broken:
            broken += 1
        try:
            children = json.loads(r.children_json or "[]")
            if not isinstance(children, list):
                children = []
        except Exception:
            children = []
        out[str(r.dir_id)] = {
            # 坏行的 utime 归 0：与真实 utime 对不上，轻量扫描必然重列它
            "utime": 0 if files_broken else int(r.utime or 0),
            "files": files, "children": children,
            # v2.1.48：真实文件条数与「清单是否可信」。两者一起判「看不看得全」
            "n_files": int(r.n_files or 0), "files_broken": files_broken,
            "name": r.name or "", "path": r.path or "", "parent": r.parent_id or "",
            "depth": int(r.depth or 0),
            "n_dirs": int(r.n_dirs or 0),
            # v2.1.43：深度兜底判据 —— 深层目录多久没被真列过了
            "last_listed_at": r.last_listed_at,
        }
    if broken:
        logger.warning(
            "[mirror] A 库目录清单有 %d 个目录的文件清单无法解析（多半是老版按字符"
            "硬截留下的坏行）→ 本轮强制重列这些目录，重列后自动修好", broken)
    return out


def save_dir_manifest(db: Session, rule_id: int, nodes: dict[str, dict],
                      listed: set[str]) -> dict:
    """把本轮看到的目录树写回清单。

    `listed` = 本轮**真的列过**的目录 id（缓存复用的不算）：只有它们的 files/children
    是这一轮的新采样，所以只有它们刷新 `last_listed_at`；其余只刷 `last_seen_at`，
    证明确实还在树上、并把新的 utime 记下来供下一轮比较。
    """
    out = {"added": 0, "updated": 0, "listed": len(listed or ()), "truncated": 0}
    try:
        rows = {str(r.dir_id): r for r in db.scalars(select(MirrorDirScan).where(
            MirrorDirScan.rule_id == rule_id)).all()}
    except Exception as exc:
        logger.warning("[mirror] 目录清单不可写（跳过保存）：%s", exc)
        return out
    now = utcnow()
    max_files = _manifest_max_bytes(db, "mirror_manifest_max_files_kb")
    max_children = _manifest_max_bytes(db, "mirror_manifest_max_children_kb")
    for did, node in nodes.items():
        did = str(did)
        row = rows.get(did)
        fresh = did in listed
        if row is None:
            row = MirrorDirScan(rule_id=rule_id, dir_id=did,
                                parent_id=str(node.get("parent") or ""),
                                name=str(node.get("name") or "")[:512],
                                path=str(node.get("path") or "")[:1024],
                                depth=int(node.get("depth") or 0),
                                utime=int(node.get("utime") or 0),
                                ctime=int(node.get("ctime") or 0),
                                files_json="[]", children_json="[]",
                                first_seen_at=now, last_seen_at=now)
            db.add(row)
            rows[did] = row
            out["added"] += 1
        else:
            out["updated"] += 1
            row.parent_id = str(node.get("parent") or "") or row.parent_id
            row.name = (str(node.get("name") or "")[:512]) or row.name
            row.path = (str(node.get("path") or "")[:1024]) or row.path
            row.depth = int(node.get("depth") or 0)
            row.last_seen_at = now
        row.utime = int(node.get("utime") or 0)
        row.ctime = int(node.get("ctime") or 0)
        if fresh:
            files = node.get("files") or []
            children = node.get("children") or []
            # v2.1.48：**按整条装、绝不切字符**（老写法把 json 字符串切到 20 万字符，
            # 会写出非法 JSON，读取端当成空目录 → 整部作品从 A 库消失）。
            row.files_json, kept_files = _dump_items_capped(files, max_files)
            row.children_json, kept_dirs = _dump_items_capped(children, max_children)
            # 真实条数（不是「装下几条」）：两者不一致即「清单被截」，
            # `_has_video` 据此知道「看不到全部文件」，不会误判成没有视频。
            row.n_files = len(files)
            row.n_dirs = len(children)
            row.last_listed_at = now
            if kept_files < len(files) or kept_dirs < len(children):
                logger.warning(
                    "[mirror] 目录 %s 的清单超过体积上限（文件 %d 条存下 %d 条、"
                    "子目录 %d 条存下 %d 条）—— 已如实记为被截，作品不会被判成空",
                    did, len(files), kept_files, len(children), kept_dirs)
                out["truncated"] = int(out.get("truncated", 0)) + 1
    try:
        db.commit()
    except Exception as exc:
        logger.warning("[mirror] 目录清单保存失败（不影响本轮同步）：%s", exc)
        db.rollback()
    return out


def pending_work_dirs(db: Session, rule_id: int) -> set[str]:
    """v2.1.43：有未完成文件（待复制/待重试）的作品目录 id。

    轻量扫描下这些作品必须**强制下探**：缓存里的文件清单是上一轮采样，源文件若被
    替换（同 fileId 不同 size）或者失败需要重取，都必须看实时内容才能判断。整部作品
    的子树一起强制，避免出现「季目录新列了、集目录还在用旧缓存」的半新半旧状态。
    """
    keys = _works_with_pending_files(db, rule_id)
    if not keys:
        return set()
    try:
        rows = db.scalars(select(MirrorMedia.source_dir_id).where(
            MirrorMedia.rule_id == rule_id,
            MirrorMedia.work_key.in_(sorted(keys)))).all()
    except Exception as exc:
        logger.warning("[mirror] 读取待处理作品目录失败（本轮不做强制下探）：%s", exc)
        return set()
    return {str(x) for x in rows if str(x or "").strip()}


def plan_light_scan(db: Session, rule_id: int, manifest: dict[str, dict], *,
                    deep_minutes: int = 0) -> dict:
    """v2.1.43：决定这一轮「轻量扫描」还是「全量建清单」。

    三种情况必须全量（light=False），一种都不许省：
      1. 清单为空 —— 第一次生成镜像同步，还没有可比对的东西；
      2. 到了深度兜底周期 —— 深层目录（季/集/子目录）超过 `deep_minutes` 没被真列过。
         为什么需要兜底：轻量扫描只信目录 utime，万一网盘某次没更新 utime，
         那一支就会一直被缓存挡住，永远看不到新集。周期全量是唯一的自愈通道；
      3. 调用方显式要求（`sync_rule?full=1`，排查用）。
    """
    info = {"light": bool(manifest), "force_dirs": set(), "deep_due": False,
            "reason": "", "deep_minutes": int(deep_minutes or 0)}
    if not manifest:
        info["reason"] = "首次运行，生成 A/B 目录清单"
        return info
    info["force_dirs"] = pending_work_dirs(db, rule_id)
    if deep_minutes and deep_minutes > 0:
        cutoff = utcnow() - timedelta(minutes=int(deep_minutes))
        for c in manifest.values():
            if int(c.get("depth") or 0) <= MANIFEST_ALWAYS_LIST_DEPTH:
                continue
            listed = as_utc(c.get("last_listed_at"))
            if listed is None or listed < cutoff:
                info["deep_due"] = True
                info["light"] = False
                info["reason"] = f"深层目录已 {int(deep_minutes)} 分钟未全量核对，本轮改为全量"
                break
    if info["light"] and not info["reason"]:
        info["reason"] = "轻量扫描：分类与作品层照列，更深目录只下探有变化的"
    return info


async def _walk_source(api: GuangYaWebFiles, root_id: str,
                       max_dirs: int = 0,
                       stats: dict | None = None, *,
                       concurrency: int = 3,
                       retries: int = 3,
                       manifest: dict | None = None,
                       light: bool = False,
                       force_dirs: set[str] | None = None,
                       listed_out: set[str] | None = None) -> tuple[dict[str, dict], bool]:
    """只读遍历 A 库目录树，返回 {dirId: 节点} 与批次完整性。

    节点含 id / name / parent / path / utime / files(直属文件) / children。
    不写任何数据库表，不触碰 Resource —— 扫描 A 库绝不能污染 B 库资源库。

    v0.9.8 #196：stats 可选，用于回显「本轮列了几个目录、发了几次列目录请求、
    看到多少 A 库文件」——让用户确认慢在列目录（必要开销）而非全量复制。

    v2.1.41（真机反馈，**全量镜像的命门**）：老版本 `max_dirs` 默认 **400**，而且
    `complete` 只反映「每次列目录请求成功没有」，**跟树有没有走完毫无关系** ——
    撞到 400 提前退出时只要 400 次请求都成功，`complete` 仍是 True，
    `sync_rule` 的 `if not complete: raise` 根本不会触发，于是**静默截断**：
    线上 A 库真实目录数 ≥1436（只读枚举实测：已列 741 个时 frontier 还剩 695 个），
    每轮只看到约 28%，`last_message` 却写着「A库共 241 部」的假全集。

    现在：
      - `max_dirs=0`（新默认）表示**不限**，把全树走完；
      - 与「请求是否成功」分开记：撞上限只置 `walk_truncated`，不再伪报完整；
      - 按层并发（默认 3）+ 退避重试，全量走完的耗时从串行十几分钟压到分钟级。

    v2.1.43（用户口径：轻量扫描，别每轮整盘全扫）：
      - `light=True` 时，**只到 `MANIFEST_ALWAYS_LIST_DEPTH`（分类 + 作品）必列**，
        更深的目录（季/集/子目录）一律先看缓存：
          · 缓存里 utime 与本轮父目录给出的 utime 一致 → 不下探，直接用缓存内容；
          · utime 变了 / 没有缓存 / 在 `force_dirs`（该作品有未完成文件）→ 照常列；
        `light=False`（深度兜底轮）行为与 v2.1.41 完全一致（整棵列一遍）。
      - 判据依据（2026-11-25 线上只读实测）：网盘目录条目本身带 utime/ctime，
        且「日韩演唱会」目录的 utime 与子项最大 utime 完全一致 → 目录 utime 随
        **直接子项**增删改更新。所以「作品目录 utime 未变」不能证明季目录里没新集，
        这也是为什么作品层必须每轮都列（那一层就是用来发现季目录 utime 变化的）。
      - 缓存复用的目录会记进 `stats["walk_cached_dirs"]`，`last_message` 会回显，
        用户能一眼看出「这轮到底真列了多少、复用多少」。
      - `listed_out`（可选出参）：本轮**真的发过列目录请求**的目录 id 集合。缓存复用的
        不进去 —— 调用方只应拿它去刷新清单的采样时间，不能被缓存冒充成新采样。
    """
    root = str(root_id or "*")
    nodes: dict[str, dict] = {}
    manifest = manifest or {}
    # 复制一份：命中「有未完成文件」的目录会把强制下探**向下传染**给子目录
    force = set(force_dirs or ())
    complete = True
    a_files = 0
    list_calls = 0
    truncated = False
    cached_dirs = 0
    limit = int(max_dirs or 0)
    sem = asyncio.Semaphore(max(1, int(concurrency or 1)))
    # (dirId, name, path, parent, level, utime, ctime)
    frontier: list[tuple] = [(root, "", "", "", 0, 0, 0)]

    async def _fetch(item: tuple):
        async with sem:
            return await _list_dir_retry(api, item[0], retries=retries)

    def _materialize_cached(did: str, level: int) -> int:
        """把缓存里的这棵子树补进 nodes（不发任何请求）。返回补进来的目录数。"""
        n = 0
        stack = [(did, level)]
        while stack:
            cur, lv = stack.pop()
            if cur in nodes:
                continue
            c = manifest.get(cur)
            if c is None:
                continue
            node = {"id": cur, "name": c.get("name") or "",
                    "parent": c.get("parent") or "", "path": c.get("path") or "",
                    "utime": int(c.get("utime") or 0),
                    "depth": int(lv),
                    "files": list(c.get("files") or []),
                    "children": list(c.get("children") or []), "cached": True,
                    # v2.1.48：把「清单可信度」一起带进节点 —— 少了这两项，
                    # 缓存复用的目录会被当成「没有视频」而丢掉整部作品。
                    "n_files": int(c.get("n_files") or 0),
                    "files_broken": bool(c.get("files_broken"))}
            nodes[cur] = node
            n += 1
            for _c in node["children"]:
                if _c not in nodes:
                    stack.append((_c, lv + 1))
        return n

    while frontier:
        # 去重：同一目录只列一次（避免重复计入 a_files / list_calls）
        seen_now: set[str] = set()
        level: list[tuple] = []
        for t in frontier:
            if t[0] in nodes or t[0] in seen_now:
                continue
            seen_now.add(t[0])
            level.append(t)
        if not level:
            break
        # v2.1.43：light 模式下，深层目录先看缓存 —— 未变就不下探
        if light:
            still: list[tuple] = []
            for t in level:
                did, _nm, _pth, _par, lv, ut, _ct = t
                if lv <= MANIFEST_ALWAYS_LIST_DEPTH or did in force:
                    still.append(t)
                    continue
                c = manifest.get(did)
                same = (c is not None and int(c.get("utime") or 0) == int(ut or 0)
                        and c.get("files") is not None
                        # v2.1.48：清单坏行不可复用 —— 坏行的 utime 已被置 0，
                        # 这里再挡一次，保证它一定被重列（否则整部作品一直看不到）
                        and not c.get("files_broken"))
                if same:
                    cached_dirs += _materialize_cached(did, lv)
                else:
                    still.append(t)
            level = still
            if not level:
                frontier = []
                break
        if limit > 0:
            room = limit - len(nodes)
            if room <= 0:
                truncated = True
                break
            if len(level) > room:
                level = level[:room]
                truncated = True
        results = await asyncio.gather(*[_fetch(t) for t in level], return_exceptions=True)
        nxt: list[tuple] = []
        for (did, name, path, parent, lv, ut, ct), res in zip(level, results):
            list_calls += 1
            if isinstance(res, BaseException):
                # 重试后仍失败：沿用老语义「整轮中止」，绝不拿残缺树做变更
                raise res
            items, ok = res
            complete = complete and ok
            display = (path + "/" + name).strip("/") if name else path
            node = {"id": did, "name": name, "parent": parent, "path": display,
                    "utime": int(ut or 0), "ctime": int(ct or 0),
                    "depth": int(lv),
                    "files": [], "children": [], "cached": False,
                    # v2.1.48：刚列过的目录文件清单是完整的，可信
                    "n_files": 0, "files_broken": False}
            nodes[did] = node
            if listed_out is not None:
                listed_out.add(did)
            # v2.1.12 #296：完全按网盘返回顺序写入（不再按 natural_key 重排）。
            for it in items:
                fid = str(it.get("fileId") or "")
                if not fid:
                    continue
                if int(it.get("resType", 1) or 1) == 2:
                    node["children"].append(fid)
                    # v2.1.43：强制下探向下传染 —— 有未完成文件的作品，越深也照列
                    if did in force:
                        force.add(fid)
                    nxt.append((fid, _dir_name(it), display, did, lv + 1,
                                int(it.get("utime") or 0), int(it.get("ctime") or 0)))
                else:
                    node["files"].append(it)
                    a_files += 1
            node["n_files"] = len(node["files"])
        frontier = nxt
    if stats is not None:
        stats["walk_dirs"] = len(nodes)
        stats["walk_list_calls"] = list_calls
        stats["walk_a_files"] = a_files
        # v2.1.41：截断必须可见。为 True 时调用方不得把「作品不在树里」当成「已删除」。
        stats["walk_truncated"] = truncated
        # v2.1.43：这轮有多少目录是「用缓存、没真列」的（轻量扫描的核心指标）
        stats["walk_cached_dirs"] = cached_dirs
        stats["walk_light"] = bool(light)
        # v2.1.48：清单看不全的目录数（坏行 + 被体积上限截断），0 才是「干净」
        stats["walk_manifest_broken"] = sum(
            1 for n in nodes.values()
            if n.get("files_broken") or int(n.get("n_files") or 0) > len(n.get("files") or []))
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
    # v2.1.12 #296：目录先于子树、文件与子目录都按**网盘返回顺序**（不再本地重排）。
    # 递归 DFS 保证「父目录先、其子树随后」，栈内顺序直接沿用 nodes[..]["children"]。
    def visit(did: str) -> None:
        if did in seen:
            return
        seen.add(did)
        node = nodes.get(did)
        if not node:
            return
        for it in node["files"]:
            out.append((node, it))
        for cid in node["children"]:
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

    v2.1.13：**只剩一套复制路径**（作品内按目标目录分桶批量提交 + 批量轮询）。
      - 处理顺序：完全按光鸭网盘返回的文件名升序（v2.1.12 #296 已落地，
        顺序真源是 `guangya.LIST_ORDER_BY/LIST_SORT_TYPE`，本地不再重排）。
      - 批次控制（暂停 / 恢复 / 跳过当前目录桶 / 取消批次）：在**桶边界**消费指令，
        不再依赖 strict。此前只有 strict 才 start_batch/finish_batch，而 strict
        默认关闭 → 生产上 batch_status 永远是 idle、5 个批次按钮等于死按钮。
      - `strict` 参数**已废弃**，仅为兼容既有调用方（含 `?strict=1` 的旧前端）
        而保留，传入后不做任何事。严格顺序引擎（逐文件占位序列 + 续跑游标）已删除。

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
    # v2.1.13：strict 已废弃 —— 显式忽略，保留入参只为兼容旧调用方。
    _ = strict
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
             # v2.1.9：作品级异常被隔离跳过的计数（整轮仍正常跑完并落 last_run）
             "work_errors": 0,
             # v0.9.8 #196：列目录开销诊断（慢在列目录而非全量复制）
             "walk_dirs": 0, "walk_list_calls": 0, "walk_a_files": 0,
             # v2.1.41：A 树是否被单轮目录上限截断（截断 ≠ 全集，禁止据此判删除）
             "walk_truncated": False,
             # v2.1.43：本轮扫描模式（light 轻量 / full 全量）与缓存复用目录数
             "walk_scan_mode": "full", "walk_scan_reason": "", "walk_cached_dirs": 0,
             "walk_force_dirs": 0, "walk_manifest_saved": {},
             # v2.1.48：清单看不全的目录数（坏行 / 被体积上限截断），0 才算干净
             "walk_manifest_broken": 0,
             # v2.1.41：本轮「已复制作品有新集/变化」的增量优先批次
             "increment_works": 0, "increment_new_files": 0,
             "increment_changed_files": 0, "queue_size": 0, "stock_left": 0,
             # v2.1.44：从未镜像过的「新作品」优先批计数与剩余量
             "new_works": 0, "new_works_left": 0}
    if not str(rule.source_root_id or "").strip() or not str(rule.target_root_id or "").strip():
        raise WebFileError("请先配置 A 源头库目录 ID 与 B 镜像库目录 ID")
    api = GuangYaWebFiles(db)
    # v2.1.13：批次控制（暂停/恢复/跳过/取消）改成**默认路径标配**。
    # 此前只有 strict 才 start_batch，而 strict 默认关闭 → 生产上 batch_status 永远是
    # idle，request_control 全部返回「只有运行中的批次才能…」，5 个按钮等于死按钮。
    batch_id = mirror_batch.start_batch(db, rule)
    stats["batch_id"] = batch_id
    # v2.1.13：跳过时按文件落 sequence 行，序号从这里连续推进（供「恢复被跳过项」）。
    seq_no = 0
    stats["purged_legacy"] = _purge_legacy_failed(db, rule.id)
    # v2.1.42（真机反馈）：自动复制最多试几次（运行时配置，缺省 3）。整轮只读一次。
    _max_att = max_file_attempts(db)
    stats["max_file_attempts"] = _max_att
    stats["files_auto_stopped"] = 0   # 本轮结束时按库内实况回填（见 last_message 段）
    # v2.1.41：A 树必须走全量。老代码 `max_dirs` 默认 400 且撞上限不报错 →
    # 线上 1436+ 个目录每轮只看到约 28%，「A库共 N 部」是个残缺视图，
    # 用户要的「按顺序把 A 库全量镜像过去」在数据层面永远达不成。
    try:
        _walk_max = max(0, int(runtime_config.get(db, "mirror_walk_max_dirs") or 0))
    except (TypeError, ValueError):
        _walk_max = 0
    try:
        _walk_conc = max(1, int(runtime_config.get(db, "mirror_walk_concurrency") or 3))
    except (TypeError, ValueError):
        _walk_conc = 3
    # v2.1.43（用户口径：轻量扫描，别每轮整盘全扫触发风控）：
    # 第一次生成 A/B 目录清单并缓存，后续每轮只列「分类 + 作品」两层，更深的
    # 季/集目录按 utime 变化、或该作品有未完成文件时才下探。兜底靠深度周期全量。
    try:
        _deep_min = max(0, int(runtime_config.get(
            db, "mirror_manifest_depth_fallback_minutes") or 0))
    except (TypeError, ValueError):
        _deep_min = 1440
    _manifest = load_dir_manifest(db, rule.id)
    _plan = plan_light_scan(db, rule.id, _manifest, deep_minutes=_deep_min)
    _listed: set[str] = set()
    nodes, complete = await _walk_source(
        api, rule.source_root_id, max_dirs=_walk_max, stats=stats,
        concurrency=_walk_conc, manifest=_manifest, light=_plan["light"],
        force_dirs=_plan["force_dirs"], listed_out=_listed)
    if not complete:
        raise WebFileError("A 库分页不完整，已中止本次镜像（禁止用不完整批次做变更）")
    stats["walk_manifest_saved"] = save_dir_manifest(db, rule.id, nodes, _listed)
    stats["walk_scan_mode"] = "light" if _plan["light"] else "full"
    stats["walk_scan_reason"] = _plan["reason"]
    stats["walk_force_dirs"] = len(_plan["force_dirs"])
    # v2.1.41：撞到目录上限被截断时，A 树**不是全集** —— 于是「作品不在树里」
    # 绝不能当成「作品已被删除」。C 类清清单/删 B 库、以及「A库源文件已删」的
    # 收敛判定全部要停（与分页不全同一套红线：不把「没扫到」当成「已删除」）。
    tree_ok = complete and not bool(stats.get("walk_truncated"))
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
    # v2.1.41（用户口径 ②）：**每轮任务开始时先查「已复制的作品有没有增量或变化」**，
    # 有就排到最前面优先处理，没有才继续推进存量。纯内存 + 一次行查询、零接口调用。
    # 必须在预扫描之前跑：预扫描会重排 works（B类失效提到前部），而增量批次要按
    # 目录既定顺序排，用刚发现出来的原始顺序最稳。
    try:
        incr_keys, incr_stats = detect_increments(db, rule, works, nodes)
    except Exception as exc:
        logger.warning("[mirror] 增量检测失败（不阻断同步）：%s", exc)
        incr_keys, incr_stats = [], {}
    incr_set = set(incr_keys)
    incr_works = [p for p in works if work_key_of(p[0]) in incr_set]
    # 单轮上限：增量再多也最多先做 max_works 部（0=不限），剩余下轮继续。
    # 与「缺集补丁 / 待重试」同口径——**不占存量轮转的部数预算**，所以存量每轮
    # 仍然会推进，不会因为库在活跃更新就永远推不动。
    if max_works > 0 and len(incr_works) > max_works:
        incr_works = incr_works[:max_works]
    stats["increment_works"] = len(incr_works)
    stats["increment_new_files"] = int(incr_stats.get("new_files") or 0)
    stats["increment_changed_files"] = int(incr_stats.get("changed_files") or 0)
    # v2.1.44（用户口径，两条硬要求）：**名额就是配置的单轮作品数、一部不多**，
    # **且每轮必须接着上一轮的游标往后走**（不能每轮从目录头部重扫）。
    # 病根：老口径把名额切给全量作品（含大量已同步老片），老片指纹命中后
    # `unchanged` 跳过、一部不复制，452 部未镜像作品被挤在游标后面永远排不上。
    # 修法：把「已镜像作品键集合」交给 plan_work_batch，让它从游标之后
    # **未镜像优先**取批——名额仍严格等于 max_works（不另开旁路批），
    # 游标仍只有一个字段、每轮接着上一轮的位置往后走。
    try:
        mapped_keys = {str(k or "") for (k,) in db.execute(
            select(MirrorMedia.work_key).where(
                MirrorMedia.rule_id == rule.id)).all()}
        # 兼容 legacy 无前缀 work_key（同 _works_with_pending_files 口径）
        mapped_keys |= {"work:" + k for k in list(mapped_keys)
                        if k and not k.startswith("work:")}
    except Exception as exc:
        # 映射清单查不到时**绝不能**把全部作品当新片重复制 —— 降级为 None，
        # 本轮退回 v0.9.0 纯游标轮转口径，保证故障时不产生重复复制风暴。
        logger.warning("[mirror] 已镜像作品清单查询失败（本轮退回纯游标轮转）：%s", exc)
        mapped_keys = None
    stats["unmapped_total"] = (
        sum(1 for p in works if work_key_of(p[0]) not in mapped_keys)
        if mapped_keys is not None else 0)
    # 镜像1.1：单轮开始三方对比预扫描（A树/B树/映射清单查漏补缺）。
    # reprocess_keys（B类整部失效）重排到 works 前部、占本批部数名额；
    # missing_keys（A类单文件缺集）抽成不占 max_works 的优先补丁批次，本轮先补齐
    # 再走正常轮转——真机「B库缺集、映射仍 synced」由此在下一轮自动补回。
    # C类按 mirror_mode 清清单/删B库；预扫描失败只记录不阻断同步。
    # v2.1.41：判「A 库已无该作品」必须以**完整 A 树**为前提（tree_ok），
    # 目录被截断时只看得到一部分，绝不能据此清清单或删 B 库文件。
    try:
        reprocess_keys, missing_keys = await reconcile_mappings_prepass(
            db, rule, nodes, works, api, stats, complete=tree_ok)
        if reprocess_keys:
            rank = {k: i for i, k in enumerate(reprocess_keys)}
            # works 元素是 (node, cats) 元组，work_key_of 要吃 node（w[0]），
            # 不能直接吃元组——否则 .get() 报 'tuple' object has no attribute 'get'。
            works = sorted(
                works,
                key=lambda pair: (0 if work_key_of(pair[0]) in rank else 1,
                                  rank.get(work_key_of(pair[0]), 0)))
            stats["prepass"]["reprocess_prioritized"] = len(reprocess_keys)
        # A 类缺集剧从轮转池抽出，作为优先补丁批次单独处理。不消耗 max_works
        # 部数预算（设计文档 2.4 节：A 类不计剧部数），但与本批同轮执行。
        priority_works: list = []
        if missing_keys:
            missing_set = set(missing_keys)
            priority_works = [p for p in works if work_key_of(p[0]) in missing_set]
            works = [p for p in works if work_key_of(p[0]) not in missing_set]
        stats["prepass"]["priority_fix_works"] = len(priority_works)
    except Exception as exc:
        logger.warning("[mirror1.1] 预扫描挂钩异常（不阻断同步）：%s", exc)
        priority_works = []
    # v0.9.0：按单轮上限 + 轮转游标切批。max_works<=0 时退化为旧行为（全量，
    # 仍受 max_media 防御上限约束）；>0 时本批**最多 max_works 部**，剩余下轮继续。
    # v2.1.44：把 mapped_keys 传下去 —— 从游标之后未镜像优先取批，
    # 名额不变（仍是配置的单轮作品数），游标仍只有一个、接着上一轮往后走。
    if max_works > 0:
        batch, new_cursor, wrapped = plan_work_batch(
            works, max_works, str(getattr(rule, "works_cursor", "") or ""),
            hard_max=max_media, mapped_keys=mapped_keys)
    else:
        batch, new_cursor, wrapped = works[:max_media], "", False
    # 本轮批里有多少部是「从未镜像过」的（回显 & 余量统计用）
    if mapped_keys is None:
        stats["new_works"] = 0
        stats["new_works_left"] = 0
    else:
        _n_new = sum(1 for p in batch if work_key_of(p[0]) not in mapped_keys)
        stats["new_works"] = _n_new
        stats["new_works_left"] = max(0, int(stats.get("unmapped_total") or 0) - _n_new)
    # v2.1.9：待重试作品优先——有 pending / failed 文件待复制的作品，
    # 本轮**无条件**排进处理队列，不受 max_works_per_run 与轮转游标限制。
    # 此前这类作品要等游标转到它才有机会被处理；一旦某一轮因异常中断
    # （整轮带崩，last_run 不更新），它们就永远停在 pending，
    # 真机上 89 条 MirrorFile 就是这样卡死的。
    # 口径：优先作品**追加**到 priority_works 之后，不占用 max_works 部数预算
    #（与 A 类缺集补丁批次同口径），避免挤掉正常轮转的作品。
    #
    # **必须排在 batch 切批之后**：本段要用 batch 的作品键做去重
    #（同一轮里同一部作品绝不能被处理两次）。若写在切批之前，
    # `batch` 尚未赋值 → UnboundLocalError → 整轮直接崩，
    # 反而制造出本轮要修的「整轮停摆」症状。
    try:
        pending_work_keys = _works_with_pending_files(db, rule.id)
    except Exception as exc:
        logger.warning("[mirror] 待重试作品统计失败（不阻断同步）：%s", exc)
        pending_work_keys = set()
    if pending_work_keys:
        seen_keys = {work_key_of(p[0]) for p in priority_works} | {
            work_key_of(p[0]) for p in incr_works}
        # 已进 priority_works 的不重复加入；batch 里的由后续去重逻辑处理
        batch_keys = {work_key_of(p[0]) for p in batch}
        retry_works = [p for p in works
                       if work_key_of(p[0]) in pending_work_keys
                       and work_key_of(p[0]) not in seen_keys
                       and work_key_of(p[0]) not in batch_keys]
        if retry_works:
            priority_works = priority_works + retry_works
            stats["retry_pending_works"] = len(retry_works)
    stats["batch_size"] = len(batch)
    # v2.1.41：增量批次里没被本批轮转覆盖到的那部分，本轮也会被处理，
    # 算「余多少部下轮继续」时要扣掉，否则数字虚高、用户会觉得永远做不完。
    # v2.1.44：批已经统一受 max_works 约束（新作品也在这批里），
    # 余量直接由 len(works) - len(batch) 推出，不再有旁路批要扣。
    _batch_keys_now = {work_key_of(p[0]) for p in batch}
    stats["stock_left"] = max(0, len(works) - len(batch) - sum(
        1 for p in incr_works if work_key_of(p[0]) not in _batch_keys_now))
    stats["works_cursor"] = new_cursor
    stats["wrapped"] = wrapped
    stats["total_works"] = len(works)
    # 执行顺序就是用户口径的顺序（**占不占名额**是关键分野）：
    #   ① 【增量】已镜像作品内部的新增/变化 —— 队首优先，**不占本轮名额**
    #      （它们是「已处理作品的作品内变化」，不该挤掉新作品的位置）
    #   ② 【修补】缺集补丁 / 待重试 —— 同属已处理作品的修补，**不占本轮名额**
    #   ③ 【全新 + 剩余】本批：从上一轮游标之后按目录顺序排未处理作品，**占名额**；
    #      名额没被新作品填满时按游标用剩余作品补满 —— 严格受配置的单轮名额限制
    #      （v2.1.44：新作品就在这批里，不另开旁路批）
    # 三段之间用 seen 去重：同一部作品绝不允许在一轮里被处理两次
    #（A 侧新增集 + B 侧缺集 + 新作品完全可能同时命中同一部剧）。
    run_queue: list = []
    _seen_wk: set[str] = set()
    for _pair in incr_works + priority_works + batch:
        _wk = work_key_of(_pair[0])
        if _wk in _seen_wk:
            continue
        _seen_wk.add(_wk)
        run_queue.append(_pair)
    stats["queue_size"] = len(run_queue)
    for work, cats in run_queue:
        # v2.1.9：作品级异常隔离——单部作品处理失败（光鸭接口抖动、建目录失败、
        # B 库核对异常等）**只跳过这一部**，绝不中断整轮同步。
        # 此前整个循环体没有任何 try/except：任何一步抛异常都会让 sync_rule
        # 直接抛出 → rule.last_run 永不更新（看起来像定时任务停摆）→
        # 后续作品全部不处理 → 上一轮遗留的 pending 文件永远卡在队列里。
        # 真机上 89 条 MirrorFile 卡在 pending、last_run 停在 9.5 小时前，
        # 根因就是这一部作品把整轮带崩了。
        try:
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
            # v2.1.31：索引键改用「size 段」，老库存量 `size:utime` 与新值也能对上，
            # 不会因为 utime 抖动被当成新文件重拷（详见 `_fp_size`）。
            synced_rows = {(m.source_file_id, _fp_size(m.fingerprint)): m
                           for m in all_rows if m.status == "synced"}
            synced_identities = {m.identity for m in all_rows if m.status == "synced"}
            # v2.1.42（真机反馈）：自动试满上限的失败行**不再进自动重试**。
            # 永久性失败（源文件被删 / B 库拒收 / 重名冲突）每轮被拉起来重做一遍，
            # 只会让那部作品每轮空转、汇总里的「重试 N」永远不归零。这类行留给人工：
            # 仍在「待重试文件」清单里，可「批量重试选中」或「清理选中」。
            # 注意只排 `failed` 且次数达上限的行 —— pending / copying / unknown 照常重试。
            retry_rows = {(m.source_file_id, _fp_size(m.fingerprint)): m
                          for m in all_rows
                          if m.status in RETRYABLE_FILE_STATUS
                          and not file_attempts_exhausted(m, _max_att)}
            # v2.1.38（真机反馈）：用户手动归档过的行，源文件回到 A 库后按**同一行**复活。
            # 不复用就必须新建行，而 (media_id, source_file_id, fingerprint) 是唯一键，
            # 归档行还留在库里 → 新插入直接撞唯一约束。所以复活走这里，不新建。
            archived_rows = {(m.source_file_id, _fp_size(m.fingerprint)): m
                             for m in all_rows if m.status == ARCHIVED_FILE_STATUS}
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
            # v2.1.4：本轮是否在 A 库发现了「指纹未命中的新文件」。
            # 用于在作品级解除 english_source='ignored' 标记（见下方解除逻辑）。
            has_new_source_file = False
            # v2.1.36（真机反馈）：记录本轮 A 库该作品枚举到的 fileId → 指纹集合。
            # 用于在循环结束后判定「补复制行为什么一直等不到源文件」：
            # fileId 压根不在 → 源文件被删/移走；fileId 在但指纹变了 → 内容已变化。
            seen_sizes: dict[str, set[str]] = {}
            for container, item in _media_files(work, nodes):
                fid = str(item.get("fileId") or "")
                fname = _dir_name(item)
                if not fid or not _is_video(fname):
                    continue
                fp = _fingerprint(item)
                fp_size = _fp_size(fp)
                seen_sizes.setdefault(fid, set()).add(fp_size)
                if (fid, fp_size) in synced_rows:
                    stats["unchanged"] += 1
                    continue
                # v2.1.4：A 库出现了本轮的新内容（指纹未命中）——即便这部作品曾被
                # 「清空待处理清单」标记为 ignored，也必须解除标记、重新解析英文名，
                # 否则它会永远停留在「永不翻译」状态，新集数也永远改不成英文名。
                has_new_source_file = True
                row = retry_rows.pop((fid, fp_size), None)
                if row is None:
                    # v2.1.38：手动归档过的行，源文件回来了就复活（不新建行）
                    row = archived_rows.pop((fid, fp_size), None)
                    if row is not None:
                        stats["restored"] = int(stats.get("restored", 0) or 0) + 1
                        stats["file_errors"].append(
                            f"{title}/{fname}：已归档的记录随源文件回归，自动恢复复制")
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
                        # v2.1.42：走统一失败出口，累加自动尝试次数并在达上限时改人工
                        mark_file_failed(row, "B 库子目录未就绪，本次未发起复制", _max_att)
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

            # v2.1.36（真机反馈）：A 库源文件被删 → 补复制**客观上不可能成功**，
            # 但自动路径此前会把它永远挂在 pending。
            #
            # 机制：`retry_rows` 的键是 (A 库 fileId, 指纹)，只有在 A 库**再次枚举到**
            # 这个文件时才会被 pop 出来重新入队（上面那句 `retry_rows.pop`）。源文件
            # 被删/移走后它永远匹配不上，循环结束就没人管它了——既不报错、也不再发
            # 复制任务，于是问题诊断里就是「永远 N 项待处理」，而且点重试也没用。
            #
            # 对照：手动「定向重试」路径（`retry_failed_files`）早就有这个兜底，找不到
            # 源文件就明确标 failed 并写原因。这里把同一条兜底补到自动路径。
            #
            # 只在 A 库枚举**完整**时才判定（`complete`，分页取不全时一律不动，
            # 避免把「没扫到」误判成「源文件不存在」——与上面 B 库核对同一套保护）。
            # 标成 failed 不是终点：源文件放回来，下一轮同步会按新文件重新建行并复制。
            # v2.1.41：判「源文件已删」同样要求 A 树是全集（tree_ok）——
            # 目录被截断时「没枚举到」只是没看到，不是不存在。
            if tree_ok and retry_rows:
                for (_fid, _fps), row in list(retry_rows.items()):
                    if _fid not in seen_sizes:
                        why = ("A 库未找到源文件（可能已被移动或删除），无法补复制；"
                               "源文件放回 A 库后重新同步即可恢复")
                        stats["source_missing"] = int(stats.get("source_missing", 0) or 0) + 1
                    else:
                        why = ("A 库该文件内容已变化，旧记录不再补复制（已按新文件重新排队）")
                        stats["source_changed"] = int(stats.get("source_changed", 0) or 0) + 1
                    # v2.1.42：统一失败出口。这两类都是「自动补复制客观上做不成」，
                    # 一直挂着只会每轮重算一遍；累加次数到达上限后转人工处理。
                    mark_file_failed(row, why, _max_att)
                    row.target_file_id = ""
                    stats["file_errors"].append(f"{title}/{row.file_name}：{why}")
                    del retry_rows[(_fid, _fps)]
                db.commit()

            # v2.1.4：本轮 A 库有新文件 → 解除该作品的 ignored 标记，恢复英文名解析。
            #
            # 根因（用户真机反馈）：「清空待处理清单」把英文名为空的作品批量置
            # english_source='ignored'，而 _resolve_english_title 对 ignored 直接短路
            # （force=False 时 return "", "ignored"），导致这些作品**永远不再翻译**——
            # 即使 A 库后来又出了新集，改名链路也不会再启动，且 push_gate 的
            # 「文件名已翻译为英文」门禁永远不通过 → 该作品再也进不了推送队列。
            #
            # 修法：ignored 的语义收敛为「**本轮没有新内容时**跳过解析」，
            # 而不是「永久拉黑」。只要 A 库又出现新文件，就解除标记、重新解析。
            # 用 Core UPDATE + 立即 commit（不污染 ORM 对象，避免 flush 时把读快照
            # 升级为写事务——这正是 mirror.py 里反复出现 database is locked 的根因）。
            if has_new_source_file and str(getattr(media, "english_source", "") or "") == "ignored":
                try:
                    db.execute(update(MirrorMedia).where(MirrorMedia.id == media.id)
                              .values(english_source=""))
                    db.commit()
                    media.english_source = ""
                    stats["unignored"] = int(stats.get("unignored", 0) or 0) + 1
                    logger.info("[mirror] 作品 %s 出现新文件，解除忽略标记、重新解析英文名",
                                title)
                except Exception as exc:
                    db.rollback()
                    logger.warning("[mirror] 解除忽略标记失败 media=%s：%s", media.id, exc)

            if not copy_jobs:
                # v0.8.4：还有未成功的文件记录时绝不标 synced，
                # 否则界面显示「已同步」而 B 库其实是空目录。
                # v2.1.50：判据收敛到 media_status_from，不再在这里另写一份 any(...)。
                media.status = media_status_from(all_rows)
                media.last_synced_at = utcnow()
                stats["media"] += 1
                db.commit()
                continue

            # v2.1.13：只保留一套复制路径 —— 严格顺序引擎已移除。
            # 顺序由网盘自身的 orderBy/sortType 决定（#296 已落地），逐文件
            # 「确认完再提交下一个」不再带来任何收益；批次控制（暂停/跳过/取消）
            # 已下沉到下面 bucket 循环的桶边界，与旧 strict 同语义。
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
                # ---- v2.1.13 桶边界：消费批次控制指令（语义与旧严格引擎一致）----
                # 只在「桶与桶之间」看指令，绝不打断在途的 copy_file / 轮询。
                action = mirror_batch.consume_control(db, rule)
                if action == "pause":
                    rule.batch_status = "paused"
                    db.commit()
                    stats["paused"] = True
                    break
                if action == "cancel":
                    rule.batch_status = "cancelled"
                    try:
                        stats["cancelled_tasks"] = mirror_batch.cancel_remaining(db, batch_id)
                    except Exception:
                        pass
                    db.commit()
                    stats["cancelled"] = True
                    break
                if action == "skip":
                    # 跳过当前目录桶：整桶不复制，行保持 pending → 下一轮自动重试。
                    # 同时按文件落一条 skipped 序列行，让「恢复被跳过项」有据可依
                    # （否则它会永远回「本批次没有被跳过的任务」，又变成一个死按钮）。
                    for _row, _fid in members:
                        seq_no += 1
                        try:
                            mirror_batch.mark_sequence_status(
                                db, batch_id, seq_no, "skipped",
                                error="skip by control (dir-level)")
                        except Exception:
                            pass
                    rule.cursor_sequence = seq_no
                    stats["skipped"] += len(members)
                    logger.info("[mirror] 批次控制跳过当前目录桶 %s（%d 个文件）",
                                target_parent, len(members))
                    db.commit()
                    continue
                if rule.batch_status in ("paused", "cancelled"):
                    stats["paused"] = True
                    break
                # 单目录文件数超上限再分片，避免单次请求体过大
                for start in range(0, len(members), COPY_BATCH_SIZE):
                    chunk = members[start:start + COPY_BATCH_SIZE]
                    if batch_no:
                        await asyncio.sleep(idle)
                    _beat()  # v0.9.21：每批复制推进打点
                    fids = [fid for _row, fid in chunk]
                    # v2.1.9：复制路径补 127 限流退避重试（此前直接 failed，整批永不重试）。
                    task_id, copy_err = await _copy_file_with_backoff(api, fids, target_parent)
                    if copy_err:
                        msg = copy_err
                        stats["failed"] += len(chunk)
                        for row, _fid in chunk:
                            # v2.1.9：限流耗尽的行标记为 pending（待重试），不是 failed ——
                            # 限流是瞬时状态，下一轮有额度就该再试；真失败才写 failed。
                            # v2.1.42：只有真失败才累加自动尝试次数（限流的 pending 不吃额度，
                            # 否则三次限流就把一个完全健康的文件判成「需人工处理」）。
                            if RENAME_RATE_LIMIT_MARK in msg:
                                row.status, row.error = "pending", msg
                            else:
                                mark_file_failed(row, msg, _max_att)
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
                        # v2.1.42：复制任务真失败 → 累加自动尝试次数，达上限转人工
                        mark_file_failed(r_, task.error, _max_att)
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
            # v2.1.50：终态只由 media_status_from 决定；「还有任务在跑」是唯一的中间态。
            # 旧写法只看本轮 copy_jobs 里有没有 failed，本作品其它历史失败行被无视，
            # 于是会出现「面板已同步、实际还有失败行」。
            if "failed" in {str(r.status or "") for r in all_rows}:
                media.status = "failed"
            elif tasks and not all(t.status == "done" for t, _ in tasks):
                media.status = "syncing"
            else:
                media.status = media_status_from(all_rows)
            media.last_synced_at = utcnow()
            db.commit()
            stats["media"] += 1
            # v2.1.13：暂停/取消 → 立即停止推进后续作品（本轮到此为止，下次接着做）
            if stats.get("paused") or stats.get("cancelled"):
                break

        except Exception as exc:
            # 单部作品失败：记录后继续下一部，绝不让整轮停摆。
            db.rollback()
            title_ = standard_media_name(work.get("name") or "")
            stats["errors"].append(f"{title_}：本轮处理异常已跳过：{str(exc)[:200]}")
            stats["file_errors"].append(f"{title_}：{str(exc)[:240]}")
            stats["work_errors"] = int(stats.get("work_errors") or 0) + 1
            logger.warning("[mirror] 作品 %s 处理异常（已跳过，不中断整轮）：%s",
                           title_, exc, exc_info=True)
            continue
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
    # v2.1.9：作品级异常隔离的可见回显——让用户知道「有几部被跳过、为什么」，
    # 而不是只看到一个笼统的「部分失败」。
    if int(stats.get("work_errors") or 0) > 0:
        rule.last_message += f" · 异常跳过 {stats['work_errors']} 部"
    # v2.1.36（真机反馈）：补复制等不到源文件的行会被收敛成失败并写清原因，
    # 这里如实回显——此前它们静默挂在 pending，「待处理 N 项」永远清不掉，
    # 用户既看不到原因也不知道该做什么。
    if int(stats.get("source_missing") or 0) > 0:
        rule.last_message += f" · A库源文件已删 {stats['source_missing']} 项"
    if int(stats.get("source_changed") or 0) > 0:
        rule.last_message += f" · A库源文件已变 {stats['source_changed']} 项"
    # v2.1.38：手动归档的记录随源文件回归自动复活，如实回显（用户清完又恢复要看得到）
    if int(stats.get("restored") or 0) > 0:
        rule.last_message += f" · 已归档恢复 {stats['restored']} 项"
    # v2.1.36：A 库整部作品消失（源文件被删）——同样如实回显，别让用户只看到
    # 「待处理 N 项」却不知道是「片被删了」。
    _c_del = int((stats.get("prepass") or {}).get("c_deleted_a", 0) or 0)
    if _c_del:
        rule.last_message += f" · A库已无作品 {_c_del} 部"
    # v2.1.40：C 类覆盖全库后必须把「有多少、暂缓几部、是否疑似枚举异常」讲清楚，
    # 否则用户只看到清理数字变小，不知道背后还有一批在排队或被护栏挡住。
    _c_cand = int((stats.get("prepass") or {}).get("c_candidates", 0) or 0)
    _c_defer = int((stats.get("prepass") or {}).get("c_deferred", 0) or 0)
    if _c_defer:
        rule.last_message += (f" · 另有 {_c_defer} 部已无作品待清理"
                              "（本轮清理额度已满，下轮继续）")
    if int((stats.get("prepass") or {}).get("c_anomaly", 0) or 0):
        rule.last_message += (f" · 疑似 A 库枚举异常：有 {_c_cand} 部作品查不到任何源文件，"
                              "本轮已暂缓清理，请核实 A 库后再同步")
    # v0.9.8 #196：明确「慢在列目录、复制是增量的」——用户担心全量跑任务时据此判断
    rule.last_message += (f" · 列目录 {stats['walk_dirs']} 个/{stats['walk_list_calls']} 次请求"
                          f" · A库文件 {stats['walk_a_files']} · 未变跳过 {stats['unchanged']}")
    # v2.1.43（用户口径）：把「这轮到底真查了多少、多少沿用上次清单」写清楚。
    # 用户提出轻量扫描就是为了少打网盘接口，所以这行必须能被核对。
    _cached = int(stats.get("walk_cached_dirs") or 0)
    if stats.get("walk_scan_mode") == "light":
        rule.last_message += (f" · 本轮轻量扫描：实查 {stats['walk_list_calls']} 个目录，"
                              f"另 {_cached} 个沿用上次清单")
        _forced = int(stats.get("walk_force_dirs") or 0)
        if _forced:
            rule.last_message += f"（{_forced} 部有未完成文件的作品已强制重查）"
    elif _manifest:
        rule.last_message += f" · 本轮全量核对：{stats['walk_scan_reason'] or '按周期全量核对'}"
        if _cached:
            rule.last_message += f"，其中 {_cached} 个沿用清单"
    # v2.1.41：A 树被单轮上限截断时必须明示——否则用户看到的「A库共 N 部」
    # 又是残缺视图，跟改之前那个 400 目录的假全集是同一个坑。
    if stats.get("walk_truncated"):
        rule.last_message += (" · ⚠ A 库目录数超过单轮上限，本轮只扫了一部分，"
                             "「已无作品」的清理判定已暂停（不要把没扫到当成已删除）")
    # v2.1.48：清单看不全的目录必须明示 —— 这是「作品可能漏采」的唯一线索。
    # 真机事故：一个目录的清单被按字符硬截成非法 JSON，读回空列表，整部作品
    # 从 A 库发现结果里消失，而页面上一个字都没提。
    _broken = int(stats.get("walk_manifest_broken") or 0)
    if _broken:
        rule.last_message += (f" · ⚠ {_broken} 个目录的清单看不全（老版按字符硬截留下的"
                              "坏行本轮已强制重列）；若下轮仍出现，把清单体积上限调大")
    # v2.1.41：每轮开始时的增量预检结果如实回显——用户要看到「优先处理了什么」。
    _incr = int(stats.get("increment_works") or 0)
    if _incr:
        rule.last_message += (
            f" · 增量优先 {_incr} 部（新增 {int(stats.get('increment_new_files') or 0)} 项"
            f" · 变化 {int(stats.get('increment_changed_files') or 0)} 项）")
    # v2.1.44：新作品回显——用户此前看到「处理 20 部只复制 2 部」，现在要能直接
    # 看到「本批里有多少部是从未镜像过的」以及还剩多少未镜像，才能确认全量在推进。
    # 注意：新作品**在名额之内**（与存量同批），不占额外预算。
    _nw = int(stats.get("new_works") or 0)
    if _nw:
        rule.last_message += (
            f" · 其中新作品 {_nw} 部（余 "
            f"{int(stats.get('new_works_left') or 0)} 部未镜像，按目录顺序接着上一轮往后做）")
    # v2.1.42（真机反馈）：有文件自动试满次数已被停手时，必须让用户看到「有几项卡住、
    # 归谁处理」——否则它们只是从数字里消失，用户以为已经自愈。
    try:
        _stopped = int(db.scalar(select(func.count(MirrorFile.id)).where(
            MirrorFile.rule_id == rule.id,
            MirrorFile.status == "failed",
            func.coalesce(MirrorFile.attempts, 0) >= _max_att)) or 0)
    except Exception:
        _stopped = int(stats.get("files_auto_stopped") or 0)
    stats["files_auto_stopped"] = _stopped
    if _stopped:
        rule.last_message += (f" · 已停止自动重试 {_stopped} 项（自动试满 {_max_att} 次，"
                             "等人工处理：在「待重试文件」里批量重试或清理选中）")
    if max_works > 0:
        _left = int(stats.get("stock_left", max(0, stats.get("total_works", 0)
                                                - stats["batch_size"])) or 0)
        rule.last_message += (f" · 本轮限额 {max_works} 部 / 实处理 {stats['batch_size']} 部"
                              f"（A 库共 {stats.get('total_works', 0)} 部，余 {_left} 部下轮继续）")
        _pf = int((stats.get("prepass") or {}).get("priority_fix_works", 0) or 0)
        if _pf:
            rule.last_message += f" · 另优先补齐 {_pf} 部缺集（不占限额）"
    if stats["dir_errors"]:
        rule.last_message += " · 首个建目录错误：" + stats["dir_errors"][0][:300]
    # v0.8.4：文件级错误必须落到 last_message，刷新页面后仍看得到，
    # 不能只留在本次 POST /api/mirror/run 的响应里。
    if stats["file_errors"]:
        rule.last_message += " · 首个文件错误：" + stats["file_errors"][0][:300]
    if stats["dirs_fallback"]:
        rule.last_message += f" · 已降级标准名 {stats['dirs_fallback']} 次"
    # v2.1.13：批次收尾对所有路径生效。此前只有 strict 才 finish_batch，
    # 默认路径的 batch_status 永远停在 idle → request_control 一律判「非运行中」，
    # 5 个批次按钮在生产上等于死按钮。
    if batch_id:
        stats["batch_status"] = rule.batch_status
        # paused 保留状态供 resume 续跑；cancelled 保留终态供页面展示
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


# ---------- v2.1.10：资源库「映射回 A 库」 ----------

# A 库作品目录名里带 {tmdb-135646} 标记；MirrorFile.identity 形如
# tmdb:135646:tv:S01:E03（类型在第二段）。两者都是 A 库口径，可直接关联到资源库。
_IDENTITY_TMDB = re.compile(r"^tmdb:(\d{2,12}):(movie|tv|unknown):", re.I)


def tmdb_tag_id(text: str) -> int:
    """从文本里的 {tmdb-135646} 标记取影视号（取不到返回 0）。

    v2.1.10：公开给 main.py 的 A 库映射回填用。
    """
    m = _TMDB_TAG.search(str(text or ""))
    return int(m.group(1)) if m else 0


def identity_tmdb(identity: str) -> tuple[int, str]:
    """从 MirrorFile.identity 解析 (TMDB 号, 类型)。unknown 类型不算类型。

    v2.1.10：公开给 main.py 的 A 库映射回填用（同一判定口径，两处不重复实现）。
    """
    m = _IDENTITY_TMDB.match(str(identity or "").strip())
    if not m:
        return 0, ""
    tid = int(m.group(1))
    mtype = m.group(2).lower()
    return (tid, "" if mtype == "unknown" else mtype)


def source_media_type(identity: str, media, cat_path: str) -> str:
    """判定 A 库作品的类型（movie/tv）——**必须与影视号一起用**。

    v2.1.10 用户口径：「电影、电视剧要做判断识别，以免相同影号的影视识别串了」。
    TMDB 的 movie/tv 是两套独立 ID 空间，同一个数字在两个空间里是完全不同的
    作品（movie/283995 是《银河护卫队2》，tv/283995 是印度剧《Indrani》）。
    只带号不带类型 → 识别时必须跨命名空间试探 → 串台。

    判定优先级（越靠前越可靠）：
      1. **MirrorFile.identity 的第二段**——同步时由
         `parse_media_name(A库原文件名, A库完整路径)` 得出，已同时吃进
         季集号（S01E03 → 必是剧集）、分类目录与作品名关键词，
         是**文件级最完整**的类型证据。
         注：identity 的类型也可能来自分类目录（若文件名无季集线索），
         此时与第 2 条同源，不冲突。
      2. **A 库分类目录链**（如「电影/国产」「剧集/欧美剧」）——用户自己分的类，
         兜底 identity 判不出（unknown）时使用。
         分类目录里出现「电影/影片/剧场版」→ movie；
         出现「剧集/电视剧/连续剧/动漫/综艺/追更」→ tv。
      3. MirrorMedia.media_type（镜像阶段多数为空）。
    判不出就返回空串——**宁可不锁类型，也不猜错类型**：
    猜错类型会把作品整个认错（比不认更糟）；不锁类型则由识别引擎按名称搜索兜底。
    """
    _tid, ident_type = identity_tmdb(str(identity or ""))
    if ident_type:
        return ident_type
    cat = str(cat_path or "")
    if cat:
        if re.search(r"电影|影片|剧场版", cat):
            return "movie"
        if re.search(r"剧集|电视剧|连续剧|动漫|番剧|综艺|追更|季番", cat):
            return "tv"
    mtype = str(getattr(media, "media_type", "") or "").strip().lower()
    if mtype in ("movie", "tv"):
        return mtype
    return ""





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
    # v2.1.4：ignored 的语义收敛为「**没有新内容时**跳过」，不是永久拉黑——
    # 同步侧一旦发现 A 库又出现新文件，会先解除该标记（见 sync_rule 的
    # has_new_source_file 分支），此处只是兜底：即便标记还在，也只在
    # 「该作品本轮确实没有新文件」时跳过。
    if src_existing == "ignored" and not force:
        return "", "ignored"
    if cached and not force:
        return cached, src_existing or "cached"
    core, year = work_core_title(media.title_standard)
    eng, source = "", ""
    if not core:
        await _persist_english_title_async(db, media_id, "", "")
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

    await _persist_english_title_async(db, media_id, eng, source)
    return eng, source


def _write_english_title_once(media_id: int, eng: str, source: str) -> Exception | None:
    """单次英文名写回：独立短 Session + Core UPDATE，返回异常或 None。

    真机 v1.0.0 `OperationalError: database is locked [UPDATE mirror_media SET
    english_title=?, english_source=?]` 的根因：调用方在同一 db Session 先 SELECT
    建读快照 → await TMDB/AI 网络 6~16s → 再 UPDATE 升级写事务时快照已过期
    （SQLITE_BUSY_SNAPSHOT，busy_timeout=30s 无效），必须换连接。

    这里用**全新独立 Session** 做这一条 Core UPDATE，绝不复用被网络调用污染的
    读快照。**关键（v1.3）**：绝不回刷调用方 Session 的 ORM 对象——旧版回刷会把
    调用方对象标脏，后续 db.commit() flush 时再次把读快照升级为写事务，等于把
    刚修好的锁问题重新引入。缓存值由下一轮读取 DB 获得，不需要回刷内存对象。
    """
    if not media_id:
        return None
    try:
        with SessionLocal() as write_db:
            write_db.execute(update(MirrorMedia).where(MirrorMedia.id == media_id).values(
                english_title=eng, english_source=source))
            write_db.commit()
        return None
    except Exception as exc:
        return exc


async def _persist_english_title_async(db: Session, media_id: int,
                                       eng: str, source: str) -> None:
    """异步主链写回：用 await asyncio.sleep 退避，绝不阻塞事件循环。

    v1.2 之前用同步 `time.sleep(0.05*2**attempt)`（8 次最坏 6.35 秒）跑在 asyncio
    事件循环线程上，写锁冲突时冻结所有 HTTP 请求——用户感知「系统卡死」。改 async
    退避后事件循环在等待期间照常服务管理页 / 健康检查 / 其它任务。
    """
    if not media_id:
        return
    for attempt in range(8):
        exc = await asyncio.to_thread(_write_english_title_once, media_id, eng, source)
        if exc is None:
            return
        if not isinstance(exc, OperationalError) or (
                "locked" not in str(exc).lower() and "busy" not in str(exc).lower()):
            logger.warning("[mirror] 英文名回写失败（非锁冲突）media=%s：%s", media_id, exc)
            return
        if attempt == 7:
            logger.warning("[mirror] 英文名回写重试 8 次仍锁，放弃 media=%s：%s",
                           media_id, exc)
            return
        await asyncio.sleep(0.05 * (2 ** attempt))


def _persist_english_title(db: Session, media_id: int, eng: str, source: str) -> None:
    """同步兼容入口（供单测 / 非 async 调用）：与 async 版共用同一单次写函数。

    同步场景无事件循环，退避用 time.sleep；生产 async 路径走
    `_persist_english_title_async`，不会阻塞事件循环。
    """
    if not media_id:
        return
    for attempt in range(8):
        exc = _write_english_title_once(media_id, eng, source)
        if exc is None:
            return
        if not isinstance(exc, OperationalError) or (
                "locked" not in str(exc).lower() and "busy" not in str(exc).lower()):
            logger.warning("[mirror] 英文名回写失败（非锁冲突）media=%s：%s", media_id, exc)
            return
        if attempt == 7:
            logger.warning("[mirror] 英文名回写重试 8 次仍锁，放弃 media=%s：%s",
                           media_id, exc)
            return
        time.sleep(0.05 * (2 ** attempt))


async def _rename_one_with_backoff(api: "GuangYaWebFiles", fid: str,
                                   final: str) -> tuple[bool, str]:
    """v1.4.1 + v2.1.12 #295：单文件改名，遇可恢复故障则退避重试。

    三类故障严格分开（v2.1.12 #295）：
      - **127 限流**：指数退避重试（1/2/4/8/8 秒封顶），耗尽打 RATE_LIMIT_MARK，
        主循环据此不计入熔断、本轮尾部再排一次队列；
      - **160 名已存**：**不可重试**（重试仍是占用），打 NAME_EXISTS_MARK，
        主循环据此做幂等收敛；
      - **瞬时故障**（服务端 RPC 超时 / 5xx / 429 / 网络抖动）：退避重试，
        耗尽打 TRANSIENT_MARK，主循环据此**不计熔断、不计真失败**，
        落「待处理」等下一轮自动重试。

    旧版只认 127，瞬时故障直接返回失败 → 单次抖动即算「真实失败」，
    5 次连续就把整轮改名熔断（这正是 #295 的病灶）。
    """
    delay = RENAME_RATE_LIMIT_BACKOFF_BASE
    t_delay = RENAME_TRANSIENT_BACKOFF_BASE
    for attempt in range(RENAME_RATE_LIMIT_MAX_RETRIES + 1):
        try:
            await api.rename_file(fid, final)
            return True, ""
        except Exception as exc:
            msg = str(exc)[:300]
            # v2.1.10：160「文件名已存」打独立标记 —— 它不是限流（不可退避重试，
            # 重试仍是占用），但必须能被主循环识别出来做幂等收敛，
            # 否则会被当成真实失败标 failed，把「其实已改名成功」误报成永久失败。
            if _is_name_exists_error(exc):
                return False, f"{RENAME_NAME_EXISTS_MARK}{msg}"
            # v2.1.12 #295：上游瞬时故障（服务端 RPC 超时 / 5xx / 网络抖动）——
            # 可重试，退避后再来一次；耗尽则打 TRANSIENT_MARK 交主循环处理。
            if _is_transient_fault(exc):
                if attempt < RENAME_TRANSIENT_MAX_RETRIES:
                    await asyncio.sleep(t_delay)
                    t_delay = min(t_delay * 2, RENAME_TRANSIENT_BACKOFF_CAP)
                    continue
                return False, f"{RENAME_TRANSIENT_MARK}{msg}"
            if not _is_rate_limit_error(exc) or attempt >= RENAME_RATE_LIMIT_MAX_RETRIES:
                if _is_rate_limit_error(exc):
                    msg = f"{RENAME_RATE_LIMIT_MARK}{msg}"
                return False, msg
            await asyncio.sleep(delay)
            delay = min(delay * 2, RENAME_RATE_LIMIT_BACKOFF_CAP)
    return False, f"{RENAME_RATE_LIMIT_MARK}rename 限流重试耗尽"


async def _rename_concurrent(api: "GuangYaWebFiles",
                             jobs: list[tuple[str, str]]) -> list[tuple[bool, str]]:
    """v1.4 B3 + v1.4.1：并发执行一批 rename_file（纯网络，绝不碰 db）。

    光鸭 rename 是单文件接口（技能包 operations[] 也只是把多个单项打包，
    服务端仍逐个执行，没有批量/正则替换），提速只能靠并发网络窗口。
    返回与 jobs 等长的 [(ok, err)]，顺序一一对应；单文件异常就地转成
    failed 结果，不中断同批其它文件。code=127 限流在 _rename_one_with_backoff
    内指数退避自动重试；db 事务完全留给调用方在主协程串行写。
    """
    return await asyncio.gather(
        *(_rename_one_with_backoff(api, fid, final) for fid, final in jobs))


def _mark_rename_done(row: MirrorFile, fid: str, final: str) -> None:
    """统一「B 库改名已完成」的落库字段（v2.1.10）。

    真机取证（2026-11-23）：历史上这段状态有 4 处各写各的（只改
    `rename_status` / `rename_error`，忘了清 `error`），于是出现
    「`rename_status=done` 却带着上一次的 code=160 错误文案」这种自相矛盾状态
    ——推送门禁只看 `rename_status`，管理页却把错误文案摆出来，用户以为文件坏了。

    这里收敛成唯一入口：状态、目标名、两个错误位一次性对齐，
    任何「判定为已完成」的路径都必须走它，杜绝同类矛盾状态再出现。
    """
    row.target_file_id = fid
    row.target_name = final
    row.rename_status, row.rename_error = "done", ""
    # `error` 是复制错误位。文件已在 B 库且改名成功时它必须为空，
    # 否则会出现「已完成 + 带错误」的矛盾组合。
    row.error = ""
    row.updated_at = utcnow()


async def rename_mirror_files(db: Session, rule: MirrorRule, *,
                              force: bool = False, max_media: int = 100,
                              max_files: int | None = None,
                              max_seconds: float | None = None,
                              max_works: int | None = None,
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
      - only_pending=True：后台复查专用，只看仍未改名的文件。
      - v1.4.4 按部收敛：预算单位是「部」（目录级），与复制阶段的
        `max_works_per_run` 同口径。每轮最多处理 max_works 部作品，选中的每一部
        其全部单集一次改完（部内不再按单集数 / 时间截断）——用户 2026-11-21 口径：
        「单轮最多整理整部剧/电影（部）对复制、翻译都生效，附带的单文件全部处理完」。
        改完的部不再 pending，下一轮自动选下一批，天然轮转、无需额外游标。
        max_files / max_seconds 仅作向后兼容的防御参数保留：max_works 生效时部内
        不触发单集截断；max_works<=0（配置未设）才退回旧的全局单集 / 时间预算。
    """
    # v1.4 B2：改名 idle 与复制 idle 解耦，管理页可配（默认 0.3，下限 0.2）。
    # 旧版直接复用 file_idle_seconds（默认 1.0），40 集纯等待就 39 秒。
    try:
        rename_idle = max(float(runtime_config.get(db, "rename_idle_seconds") or 0.3),
                          MIN_RENAME_IDLE_SECONDS)
    except (TypeError, ValueError):
        rename_idle = max(0.3, MIN_RENAME_IDLE_SECONDS)
    # v1.4 B3：并发度默认 3（用户拍板默认开）；填 1 退回串行，上限 8。
    try:
        rename_concurrency = max(1, min(MAX_RENAME_CONCURRENCY,
                                        int(runtime_config.get(db, "rename_concurrency") or 3)))
    except (TypeError, ValueError):
        rename_concurrency = 3
    idle = rename_idle  # 函数体内所有 await asyncio.sleep(idle) 现指改名专用间隔
    # v1.4.4 按部收敛：优先用显式 max_works，否则读 rule.max_works_per_run。
    # >0 时按部选批、部内全改；<=0 时退回旧的单集/时间预算（向后兼容手动 force 全量）。
    if max_works is None:
        try:
            max_works = int(getattr(rule, "max_works_per_run", 0) or 0)
        except (TypeError, ValueError):
            max_works = 0
    max_works = max(0, int(max_works or 0))
    per_work_mode = max_works > 0
    file_budget = int(max_files if max_files is not None else RENAME_MAX_FILES)
    time_budget = float(max_seconds if max_seconds is not None else RENAME_MAX_SECONDS)
    stats = {"works": 0, "renamed": 0, "skipped": 0, "ascii": 0,
             "untranslated": 0, "failed": 0, "no_rows": 0, "errors": [],
             "waiting_visible": 0,
             # v1.4：改名提速参数回显，便于管理页/测试核对
             "rename_idle_seconds": rename_idle, "rename_concurrency": rename_concurrency,
             # v1.4.1：127 限流统计
             "rate_limited": 0, "rate_limit_recovered": 0, "rate_limit_retries_queued": 0,
             # v2.1.10：160「名已存」统计。recovered = 核对后确认「其实已改名成功」，
             # 纠回完成态（不再算失败）；conflict = 目标名真被别的文件占用，下轮加序号重试。
             "name_exists_recovered": 0, "name_exists_conflict": 0,
             # v2.1.12 #295：上游瞬时故障（RPC 超时 / 5xx / 网络抖动）统计。
             # 瞬时故障**不是改名失败** —— 不计 failed、不计熔断、不进失败清单，
             # 只把文件退回「待处理」，下一轮自动重试。
             "transient": 0,
             # v0.8.9：分批续改是正常进度，不是错误，单独放 notices 前端才不会误报 warning
             "notices": [],
             "literal_ai": 0, "literal_pinyin": 0,
             "remaining": 0, "truncated_reason": "", "attempts": 0,
             "skipped_legacy": 0,
             "untranslated_works": [], "idle_seconds": idle,
             "file_budget": file_budget, "time_budget": time_budget,
             # v1.4.4：按部收敛回显
             "max_works": max_works, "per_work_mode": per_work_mode}
    started_all = time.monotonic()
    attempts = 0
    consecutive_failures = 0
    stop_reason = ""
    api = GuangYaWebFiles(db)
    # v1.4.1：代码=127 限流重试耗尽的文件，本轮尾部再排一次队列（不干等下一轮 120s job）
    requeue_rate_limited: list[tuple[MirrorFile, str, str, str]] = []
    # v2.1.10：记录「本轮改名没成功的作品」，供上层把门禁收敛到作品级
    # （一部剧改名失败不该把同轮其它作品一起拦下）。
    failed_works: set[str] = set()
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
    # v1.4.4 按部收敛：每轮最多 max_works 部，改完的部不再 pending，下一轮自动
    # 选下一批（天然轮转，无需游标，也不与复制阶段共用的 works_cursor 冲突）。
    # 部内全改，不受单集 max_files / 时间预算截断。max_works<=0 时退回旧口径。
    # per_work_mode 下部数完全由 max_works 决定（不再叠加 max_media 的防御截断）；
    # 旧模式仍用 max_media 作全库防御上限。
    selected = medias[:max_works] if per_work_mode else medias[:max_media]
    if per_work_mode:
        stats["selected_works"] = len(selected)
    for media in selected:
        # v1.4.4：按部模式下预算类（file_budget/time_budget）不再在这里阻断，
        # 部内全改；但真实止损（连续失败熔断 rate_limited / 事务损坏 db_error）
        # 仍须停止后续作品。旧版在这里无条件 `if stop_reason` 提前 break，叠加
        # 内层单集预算 break，正是「准备满预算→阶段二全作废→0 改名」的断流根因。
        if per_work_mode:
            if stop_reason in ("rate_limited", "db_error"):
                break
        else:
            if stop_reason or time.monotonic() - started_all >= time_budget:
                stop_reason = stop_reason or "time_budget"
                break
        # v1.3 借鉴 TgtoDrive v8.8.25「已入库的名称继续沿用、不重复解析」：
        # 名称沿用由 _resolve_english_title 的 english_title DB 缓存实现——缓存
        # 命中即零网络返回（force 才强制重解析）。这里**不**按 DB rename_status
        # 整段跳过作品：合法场景「DB 标 done 但 B 库文件被回滚/补复制成中文」
        # 仍须列目录核对后重新改名（v088/v097 回归锁定），冒进跳过会漏改。
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
        # v1.4.4：按部模式下已选中的部不中途放弃，部内单集全部改完。
        if not per_work_mode and time.monotonic() - started_all >= time_budget:
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
        # v1.4 B3：改名并发。两阶段——
        #   阶段一（主协程串行，纯本地）：算目标名/冲突后缀、做幂等与跳过判断，
        #     把真正要发 rename_file 的收集成 jobs；冲突后缀按顺序在 name_fids
        #     占位，保证并发发请求时不会有两个文件抢同一目标名。
        #   阶段二（按 rename_concurrency 分窗口）：窗口前 sleep 一次 idle
        #     （旧版每文件一次，N 次→N/并发次数），并发发网络，主协程串行写库。
        # 红线：任何 db 写都在主协程完成，网络协程绝不碰 db（防 SQLite 写锁/快照升级）。
        jobs: list[tuple[MirrorFile, str, str, str]] = []  # (row, fid, final, fname)
        for idx, row in enumerate(rows):
            fname = str(row.file_name or "")
            # v1.4.4：按部模式下部内全改——单集数 / 时间预算不在这里截断，
            # 已选中的部所有可见单集全部收集进 jobs（用户口径「单文件全部处理完」）。
            # 非按部模式（max_works<=0）保留旧的单集 / 时间预算。
            if not per_work_mode:
                if time.monotonic() - started_all >= time_budget:
                    stop_reason = "time_budget"
                    break
                if attempts + len(jobs) >= file_budget:
                    stop_reason = "file_budget"
                    break
            # 连续失败熔断是真实止损（真实失败/网盘异常），任何模式都保留。
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
                    _mark_rename_done(row, fid, final)
                    stats["skipped"] += 1
                    db.commit()
                    continue
                # v2.1.10：查不到该 fileId 的**实际名**（本次列目录里没命中它），
                # 而 DB 记的目标名已经是本轮算出的最终名 → **不发重复请求**。
                # 旧版这里的兜底判断挂在 `not b_verified` 上，而 b_verified 在上方
                # 已经恒为 True（不完整时整部跳过），整段是死代码 —— 于是「DB 状态
                # 没跟上」的行每轮都会重新发一次 rename，光鸭发现目标英文名已被
                # 占用就回 code=160（真机 63 条）。
                # 注意只在**查不到实际名**时才兜底：实际名已知且 ≠ 目标名时仍要改名
                # （v088/v097 回归：DB 标 done 但 B 库文件被回滚成中文，必须重改）。
                if (not cur
                        and str(getattr(row, "target_name", "") or "").strip()
                        and normalize_work_name(str(row.target_name))
                        == normalize_work_name(final)
                        and str(getattr(row, "rename_status", "") or "") == "done"):
                    stats["skipped"] += 1
                    continue
                # 冲突名占位提前到准备阶段：并发发请求时同一目标名只会出现一次。
                name_fids[normalize_work_name(final)] = fid
                fid_to_norm[str(fid)] = normalize_work_name(final)
                jobs.append((row, fid, final, fname))
            except Exception as exc:
                # v0.8.9：任何单文件异常（含 db.commit 写冲突）只影响当前文件，
                # 绝不中断整部剧 —— 这正是「长剧集只改了一部分」的旧病灶。
                msg = f"{type(exc).__name__}: {exc}"[:300]
                logger.warning("[mirror] 改名单文件准备异常 %s/%s：%s",
                               media.title_standard, fname, msg)
                stats["errors"].append(f"{media.title_standard}/{fname}：{msg[:160]}")
                try:
                    db.rollback()
                    row.rename_status, row.rename_error = "failed", msg
                    db.commit()
                    stats["failed"] += 1
                    failed_works.add(str(media.title_standard or ""))
                    consecutive_failures += 1
                except Exception:
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    stop_reason = "db_error"
                    break

        # 阶段二：按窗口并发发网络 + 主协程串行写库。
        # v1.4.4 P0 断流修复：准备阶段因 file_budget / time_budget / rate_limited
        # 提前 break 时会设 stop_reason，但此时 jobs 里已收集了待发任务。旧版这里
        # `if stop_reason: break` 把这些 jobs 全部作废——单部 58 集、后台预算 10 时
        # 准备到第 10 个就 break，执行阶段一个 rename_file 都不发，renamed 恒为 0。
        # 修法：预算/限流类 stop_reason 不再阻断本轮已收集 jobs 的发送（发完才算停）；
        # 只有 db_error（事务已坏、必须立即止损）才在这里中断。
        # 预算仍然生效：发完 jobs 后外层作品循环因 stop_reason 停止，不会越界多改。
        # v2.1.10：160 判定用的「B 库当前实际名」按作品缓存（None = 未取过）。
        actual_by_fid_cache: dict[str, str] | None = None
        first = True
        for start in range(0, len(jobs), rename_concurrency):
            if stop_reason == "db_error":
                break
            window = jobs[start:start + rename_concurrency]
            if not first:
                await asyncio.sleep(idle)  # 窗口间留一次风控间隔
            first = False
            for row, _fid, _final, _fname in window:
                _beat()
                attempts += 1
                stats["attempts"] += 1
            outcomes = await _rename_concurrent(
                api, [(fid, final) for _row, fid, final, _fname in window])
            # v2.1.10：本窗口若有 160「名已存」，**一次列目录批量判定**该 fileId 当前名，
            # 避免逐条回查（63 条 160 → 63 次递归列目录，纯浪费）。
            # 缓存按作品维度持有：只要本窗口有改名成功，B 库状态就变了，缓存必须作废
            # （否则会把「刚改成功的文件」当成没改名，误判成真冲突）。
            if any(ok for ok, _errmsg in outcomes):
                actual_by_fid_cache = None
            if any((not ok) and errmsg.startswith(RENAME_NAME_EXISTS_MARK)
                   for ok, errmsg in outcomes):
                if actual_by_fid_cache is None:
                    actual_by_fid_cache = {}
                    try:
                        cands, _v = await _b_file_candidates(api, media)
                        for k, v in cands.items():
                            if ":" not in k:
                                actual_by_fid_cache.setdefault(str(v), k)
                    except Exception:
                        actual_by_fid_cache = {}
                actual_by_fid = actual_by_fid_cache
            else:
                actual_by_fid = {}
            for (row, fid, final, fname), (ok, errmsg) in zip(window, outcomes):
                if not ok:
                    # v2.1.10：光鸭 code=160「文件名已存」**不是真错误**，是「重复改名」。
                    # 真机取证：63 条 160 集中在 3 部剧，同一部剧间隔式成败 —— 改成功的集
                    # DB 状态没跟上，后续轮次又拿旧中文名去改了一遍，光鸭发现目标英文名
                    # 已被占用就报 160。**重试无用**（重试仍是占用），必须做幂等收敛：
                    # 看 B 库该 fileId 的当前名，若已是目标名 → 认定改名其实已成功，
                    # 只纠正 DB 状态（done + 清 error），不标 failed、不计入熔断。
                    if errmsg.startswith(RENAME_NAME_EXISTS_MARK):
                        clean_160 = errmsg[len(RENAME_NAME_EXISTS_MARK):]
                        current = actual_by_fid.get(str(fid), "")
                        # 判定「这个文件其实已经改名成功了」有两种依据：
                        #   ① 当前名 == 本轮算出的目标名（上一轮就改成了这个名字）；
                        #   ② 当前名 == DB 里已记录的目标名（上一轮改成了带冲突后缀的
                        #      名字，如 "-v2"，本轮算出的目标名与它不同）。
                        # 两种都要收敛成 done，否则「其实已成功」会被永久记成失败。
                        converged_name = ""
                        if current:
                            if current == normalize_work_name(final):
                                converged_name = final
                            elif (str(getattr(row, "target_name", "") or "").strip()
                                  and current == normalize_work_name(str(row.target_name))):
                                converged_name = str(row.target_name)
                        if converged_name:
                            # 改名其实早已生效 → 只纠正 DB 状态（done + 清两个错误位），
                            # 不标 failed、不计入熔断、不再发请求。
                            _mark_rename_done(row, fid, converged_name)
                            stats["renamed"] += 1
                            stats["name_exists_recovered"] = (
                                stats.get("name_exists_recovered", 0) + 1)
                            logger.info(
                                "[mirror] rename 160 收敛为已改名 %s/%s → %s（DB 状态已纠正）",
                                media.title_standard, fname, converged_name)
                            try:
                                db.commit()
                            except Exception:
                                try:
                                    db.rollback()
                                except Exception:
                                    pass
                                stop_reason = "db_error"
                                break
                            continue
                        # 目标名确实被**别的**文件占用（或查不到）→ 这是真冲突，走冲突后缀重试，
                        # 不标 failed（本轮不算失败，下轮用带后缀的名字再试）。
                        stats["name_exists_conflict"] = (
                            stats.get("name_exists_conflict", 0) + 1)
                        failed_works.add(str(media.title_standard or ""))
                        stats["errors"].append(
                            f"{media.title_standard}/{fname}：目标英文名已被其他文件占用，"
                            "下轮将改用带序号的名字")
                        logger.warning(
                            "[mirror] rename 160 真冲突 %s/%s：目标名 %s 已被占用",
                            media.title_standard, fname, final)
                        try:
                            db.rollback()
                            row.rename_status = "pending_visible"
                            row.rename_error = (
                                "目标英文名已被其他文件占用，下轮改用带序号的名字重试")
                            db.commit()
                        except Exception:
                            try:
                                db.rollback()
                            except Exception:
                                pass
                            stop_reason = "db_error"
                            break
                        continue
                    # v2.1.12 #295：瞬时故障（上游 RPC 超时 / 5xx / 网络抖动）**单列一路**。
                    # 真机报障原文：rename 接口 6 秒 RPC 超时，被旧逻辑当成「鉴权失效」→ 零重试，
                    # 连续 5 次即熔断整轮，用户看到的就是「整轮改名全废、看板数字一动不动」。
                    # 这类故障不是改名失败：不计 failed、不计熔断、不进失败清单，
                    # 只把文件退回「待处理」（rename_status=""），下一轮自动重试。
                    transient = errmsg.startswith(RENAME_TRANSIENT_MARK)
                    if transient:
                        stats["transient"] += 1
                        logger.warning(
                            "[mirror] rename 上游瞬时故障，本轮退回待处理 %s/%s：%s",
                            media.title_standard, fname,
                            errmsg[len(RENAME_TRANSIENT_MARK):][:200])
                        try:
                            db.rollback()
                            row.rename_status = ""
                            row.rename_error = RENAME_TRANSIENT_LABEL
                            db.commit()
                        except Exception:
                            try:
                                db.rollback()
                            except Exception:
                                pass
                            stop_reason = "db_error"
                            break
                        continue
                    # v1.4.1：区分光鸭 127 限流与真实失败。限流不是「连续失败」，
                    # 不计入熔断阈值（否则一次风控就把整轮熔断、误伤后续正常文件）；
                    # 标 failed 可追溯，同时收集到 requeue 在本轮尾部再排一次。
                    rate_limited = errmsg.startswith(RENAME_RATE_LIMIT_MARK)
                    clean_err = errmsg[len(RENAME_RATE_LIMIT_MARK):] if rate_limited else errmsg
                    stats["failed"] += 1
                    failed_works.add(str(media.title_standard or ""))
                    if rate_limited:
                        stats["rate_limited"] += 1
                        requeue_rate_limited.append((row, fid, final, fname))
                    else:
                        consecutive_failures += 1
                    # v2.1.12 #295：`errors` 会一路透到前端 toast 与 last_message，
                    # 因此这里就换成中性标签，绝不把上游内部标识（服务名 / RPC 方法 /
                    # 接口路径 / 业务码）带到页面。原始文本仍写进 DB 的 rename_error 供排查。
                    err_label = rename_error_label(errmsg)
                    stats["errors"].append(
                        f"{media.title_standard}/{fname}：改名失败 {err_label}")
                    logger.warning("[mirror] rename 失败 %s/%s：%s",
                                   media.title_standard, fname, clean_err)
                    try:
                        db.rollback()
                        row.rename_status, row.rename_error = "failed", clean_err
                        db.commit()
                    except Exception:
                        try:
                            db.rollback()
                        except Exception:
                            pass
                        stop_reason = "db_error"
                        break
                    continue
                consecutive_failures = 0
                _mark_rename_done(row, fid, final)
                stats["renamed"] += 1
                try:
                    db.commit()
                except Exception:
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    stop_reason = "db_error"
                    break
        stats["works"] += 1
        db.commit()
    # v1.4.1：本轮尾部二次排队——code=127 限流重试耗尽的集，不干等下一轮 120s job，
    # 在预算允许的范围内本轮再发一轮（函数内同样带退避）。成功则把 failed 计回 renamed。
    # 红线：网络协程不碰 db，写库全部在主协程串行；撞到 time_budget/file_budget 即止。
    if requeue_rate_limited and not stop_reason:
        stats["rate_limit_retries_queued"] = len(requeue_rate_limited)
        for qstart in range(0, len(requeue_rate_limited), rename_concurrency):
            # v1.4.4：按部模式下部内已全改，127 二次排队也不受单集/时间预算阻断。
            if stop_reason or (not per_work_mode
                               and time.monotonic() - started_all >= time_budget):
                stop_reason = stop_reason or "time_budget"
                break
            if not per_work_mode and attempts >= file_budget:
                stop_reason = "file_budget"
                break
            qwin = requeue_rate_limited[qstart:qstart + rename_concurrency]
            for _row, _fid, _final, _fname in qwin:
                _beat()
                attempts += 1
                stats["attempts"] += 1
            qout = await _rename_concurrent(
                api, [(fid, final) for _row, fid, final, _fname in qwin])
            for (row, fid, final, fname), (ok, errmsg) in zip(qwin, qout):
                limited_again = errmsg.startswith(RENAME_RATE_LIMIT_MARK)
                clean_err = errmsg[len(RENAME_RATE_LIMIT_MARK):] if limited_again else errmsg
                if ok:
                    stats["failed"] = max(0, stats["failed"] - 1)
                    stats["renamed"] += 1
                    stats["rate_limit_recovered"] += 1
                    _mark_rename_done(row, fid, final)
                elif errmsg.startswith(RENAME_TRANSIENT_MARK):
                    # v2.1.12 #295：二次排队时撞上瞬时故障 —— 与主循环同口径，
                    # 把先前计入的 failed 退回来，退回「待处理」等下一轮。
                    stats["failed"] = max(0, stats["failed"] - 1)
                    stats["transient"] += 1
                    row.rename_status = ""
                    row.rename_error = RENAME_TRANSIENT_LABEL
                else:
                    # v2.1.12 #295：同上，errors 会透到页面 → 走中性标签，不漏接口路径。
                    stats["errors"].append(
                        f"{fname}：限流二次排队仍失败 {rename_error_label(errmsg)}")
                    row.rename_status, row.rename_error = "failed", clean_err
                    failed_works.add(str(media.title_standard or ""))
                try:
                    db.commit()
                except Exception:
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    stop_reason = "db_error"
                    break
    # v2.1.10：本轮改名没成功的作品清单（供上层把门禁收敛到作品级）。
    stats["failed_works"] = sorted(w for w in failed_works if w)[:50]
    # v2.1.12 #295：瞬时故障走 notices（不是错误），前端才不会把它当失败报 warning。
    # 用一条聚合提示，逐文件刷屏没有意义 —— 用户只需要知道「这不是失败，下一轮会继续」。
    if stats["transient"]:
        stats["notices"].append(
            f"本轮有 {stats['transient']} 个文件遇到网盘服务临时不可用，"
            "已退回待处理，下一轮会自动重试，不计为改名失败")
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
                               scope: str = "all",
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

    v2.1.11：加 `scope` 拆分文件段与目录段。
      此前「清理无效映射」一个端点同时清文件与目录，导致管理页三个清理按钮
      各自含义重叠、又都只能全表操作。现在 scope=file 只处理文件映射、
      scope=dir 只处理目录映射、scope=all 保持原行为（两个老端点仍走 all，兼容）。
    """
    scope = (scope or "all").strip().lower()
    if scope not in ("all", "file", "dir"):
        scope = "all"
    do_file = scope in ("all", "file")
    do_dir = scope in ("all", "dir")
    api = GuangYaWebFiles(db)
    stats = {"ok": True, "dry_run": bool(dry_run), "scope": scope,
             "media_checked": 0,
             "media_skipped_unverified": 0, "media_skipped_empty": 0,
             "files_pruned": 0, "dirs_checked": 0, "dirs_pruned": 0,
             "dirs_skipped_unverified": 0, "samples": [], "errors": []}
    medias = list(db.scalars(select(MirrorMedia).where(
        MirrorMedia.rule_id == rule.id, MirrorMedia.target_dir_id != "")).all())
    for media in medias[:max_media]:
        if not do_file:
            break
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
        MirrorDir.status != "missing")).all()) if do_dir else []
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
    # v2.1.42：这条路径也是自动重试（补轮询历史任务），失败同样要累加尝试次数，
    # 否则一个永远失败的任务能被反复轮询、永远挂在「状态待确认」。
    _lim = max_file_attempts(db)
    for task in tasks:
        task.status = "running"
        rows = list(db.scalars(select(MirrorFile).where(
            MirrorFile.task_id == task.task_id)).all())
        if await _poll_task(api, task, max_polls=max_polls, interval=interval):
            done += 1
            for row in rows:
                row.status, row.error = "synced", ""
            if task.media_id:
                # v2.1.50：不再「这个任务成功 → 整部作品算成功」。同一部作品可能还有
                # 别的失败行（一次定向重试会并发多个任务），按唯一判据重算才不会
                # 把还剩的失败掩盖成已同步。
                reconcile_media_status(db, task.media_id, touch_synced_at=True)
        elif task.status == "failed":
            # v0.8.4：明确失败的任务把文件标 failed，下一轮同步会重新发起复制
            failed += 1
            for row in rows:
                mark_file_failed(row, task.error, _lim)
            if task.media_id:
                reconcile_media_status(db, task.media_id)
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


async def _backfill_ids_for_media(api, db: Session, media: MirrorMedia,
                                  rows: list) -> int:
    """列一次该作品的 B 库目录，把缺 `target_file_id` 的行按文件名+大小回填。

    v2.1.50：光鸭 `copy_file` 只回 taskId、不回副本 fileId —— 这个缺口正常同步靠
    `backfill_target_file_ids` 补，定向重试以前根本没有对应步骤，于是刚复制成功的行
    拿着空 `target_file_id` 停在原地（改名能靠 B 库目录反查自救，但映射行一直是空的）。
    判据与 `backfill_target_file_ids` 完全同一个 `_lookup_b_file`，只是范围限定在
    **这一部作品**：重试结束立刻补齐，不必等下一轮全库回填。
    """
    need = [r for r in rows
            if not str(getattr(r, "target_file_id", "") or "").strip()
            and str(getattr(r, "status", "") or "")
            in ("synced", "copying", "failed", "unknown", "pending")]
    if not need:
        return 0
    candidates, _verified = await _b_file_candidates(api, media)
    if not candidates:
        return 0
    n = 0
    for r in need:
        hit = _lookup_b_file(candidates, r)
        if not hit:
            continue
        r.target_file_id = hit
        # 与 v0.8.4 同一口径：B 库实际存在就纠正为 synced，别让下一轮再发一次。
        if str(getattr(r, "status", "") or "") != "synced":
            r.status, r.error = "synced", ""
        r.updated_at = utcnow()
        n += 1
    return n


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
    # v2.1.42（人工介入入口）：手动「批量重试选中」= 用户明确要求再试一次 →
    # **清零已自动尝试次数**，重新给满额度。否则自动路径刚试满的文件，人工点完
    # 一次失败后仍会被判「已停止自动重试」，自动侧再也不接手，用户会觉得点了没用。
    for r in rows:
        if int(getattr(r, "attempts", 0) or 0):
            r.attempts = 0
    db.commit()
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
            # v2.1.50：中止路径同样要收口作品状态，否则这条作品会一直停在旧状态。
            reconcile_media_status(db, media_id)
            db.commit()
            continue
        # 只 walk 单部作品（非全库），拿到 A 季目录相对链
        # v2.1.41：撞上限被截断与「分页不全」同样不可用于判定源文件是否存在，
        # 一起中止（此前截断是静默的，会把「没扫到」当成「已删除」）。
        _one_stats: dict = {}
        nodes, complete = await _walk_source(api, str(media.source_dir_id), max_dirs=80,
                                             stats=_one_stats)
        if not complete or _one_stats.get("walk_truncated"):
            for r in group:
                r.status = "failed"
                r.error = "[定向重试] A 库该作品目录分页不完整，已中止（禁止用不完整数据重发）"
                out["failed"] += 1
                out["details"].append(f"{r.file_name}：{r.error}")
            # v2.1.50：中止路径同样要收口作品状态。
            reconcile_media_status(db, media_id)
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
                # v2.1.9：定向重试同样补 127 限流退避（单文件版）。
                task_id, copy_err = await _copy_file_with_backoff(
                    api, [str(r.source_file_id)], target_parent)
                if copy_err:
                    raise Exception(copy_err)
            except Exception as exc:
                msg = str(exc)[:300]
                # v2.1.9：限流耗尽 → pending（待重试），真失败 → failed。
                if RENAME_RATE_LIMIT_MARK in msg:
                    r.status = "pending"
                else:
                    r.status = "failed"
                r.error = f"[定向重试] copy_file 失败：{msg[:200]}"
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
            db.commit()
        # v2.1.50（根治「重试成功仍显示失败」）：复制接口只回 taskId，定向重试到这儿
        # 就结束了 —— 文件行拿不到 target_file_id，作品级状态也没人重算。补上与正常
        # 同步同一套收尾：① 列一次 B 库回填 fileId；② 按唯一判据重算作品状态。
        # 放在 if 外面：全部走「B 库已存在 → 回填」这条路径时同样要收尾。
        filled = await _backfill_ids_for_media(api, db, media, group)
        if filled:
            out["backfilled"] = int(out.get("backfilled", 0)) + filled
            out["details"].append(
                f"作品《{media.title_standard}》补回 {filled} 个 B 库文件 ID")
        reconcile_media_status(db, media_id, touch_synced_at=True)
        db.commit()
    return out
