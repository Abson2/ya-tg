"""推送门禁（v2.1.3）：进入推送队列前必须同时满足的五项前置条件。

真机事故（2026-11-22）：「黑豹2 (2022) {tmdb-505642}」在**未取到英文名、未改名**
的状态下被推送到频道与群聊。根因不是口径分歧，而是两条入队路径各写各的、且都
不完整：

  - `share_center_v2.dispatch_ready_tasks`：plan_engine 排进清单就直接建分享并提交
    队列，既不看海报/元数据/识别状态，也不看目录是否按 `·` 约定整理、单文件是否
    已翻译成英文名；
  - 旧链 `main._enqueue_ready`：只校验「识别成功 + 有海报」两项。

于是任何一个条件缺失的作品都能直接进队列推送——黑豹2 只是被抽到的那一个。

本模块把门禁收敛成**一处判定**，供 v2 dispatch 与旧链入队共用，五项缺一不可：

  1. poster        已获取到 TMDB 海报
  2. metadata      已获取到影片资料（标题）
  3. identified    识别成功
  4. dir_marked    B 库影视主目录已按 `·` 约定整理（镜像链路作品）
  5. file_english  作品内单文件名称已翻译为英文（镜像链路作品）

判定口径：**作品级**。推送的粒度是「一部作品一条推送」，因此前三项按「作品内
存在一个已识别、且有海报、且有标题的成员」判定——同一部剧的其它集是否识别成功
不影响这一部能否推。后两项只对**镜像链路作品**（mirror_media 有对应记录）生效：
这类作品的目录名与文件名由镜像同步统一整理，没整理完就不该推出去。非镜像链路
作品（用户自行放入 B 库、没有镜像记录）不存在「影视目录 / 单文件改名」这回事，
按前三项判定即可。

判定只依据库里已有的事实字段，不臆造、不猜测。门禁未过时给出**中文业务原因**，
写进任务备注供前端展示（页面不出现任何后端字段名）。
"""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import MirrorDir, MirrorFile, MirrorMedia
from .utils import NAME_MARK, normalize_work_name

# 五项门禁的固定顺序（展示与判定顺序一致）。
GATE_ITEMS = ("poster", "metadata", "identified", "dir_marked", "file_english")

# 门禁项 → 面向用户的中文原因（**不得**出现后端字段名/状态码）。
GATE_LABELS: dict[str, str] = {
    "poster": "缺少影片海报",
    "metadata": "缺少影片资料",
    "identified": "影片尚未识别完成",
    "dir_marked": "影片目录尚未按命名约定整理",
    "file_english": "影片文件名尚未翻译为英文",
}

# 单文件改名状态中「视为已具备英文名」的三种：
#   done          已改成英文名（target_name 非空）
#   skipped_ascii 原文件名本就是英文/ASCII，无需改
#   skipped_legacy 历史条目不回刷（用户既有口径，不阻塞推送）
_RENAME_OK_STATUS = ("done", "skipped_ascii", "skipped_legacy")


def _text(value) -> str:
    return str(value or "").strip()


def _norm(value) -> str:
    return normalize_work_name(_text(value))


def _member_items(resources) -> dict[str, bool]:
    """作品成员的资源级三项：海报 / 资料 / 识别成功。

    按「作品内存在一个满足条件的成员」判定：推送粒度是作品，一部剧里只要有
    一集识别成功并带回海报与标题，这一部的推送前置就齐了。反之（全部成员都
    没识别成功 / 都没有海报）才判定不通过。

    「资料」用标题（media_title / original_title）而不是 TMDB id：识别成功必然
    带回标题，而 TMDB id 在人工识别、部分离线识别路径下可能为空。强行要求
    TMDB id 会把「已识别且有海报」的正常作品误挡在门外，属于把门禁做成误杀。
    """
    items = {"poster": False, "metadata": False, "identified": False}
    for res in resources or ():
        if _text(getattr(res, "identify_status", "")) != "identified":
            continue
        items["identified"] = True
        if _text(getattr(res, "poster_path", "")):
            items["poster"] = True
        if _text(getattr(res, "media_title", "")) or _text(getattr(res, "original_title", "")):
            items["metadata"] = True
    return items


def find_mirror_media(db: Session, work_key: str = "", resources=None) -> list[MirrorMedia]:
    """定位作品对应的镜像记录（mirror_media）。

    依次尝试：作品聚合键 → TMDB id → 归一化标题。任一命中即返回（可能多规则
    多条）。作品键命中是主路径；后两者是作品键口径漂移时的兜底。
    """
    key = _text(work_key)
    if key.startswith("work:"):
        rows = list(db.scalars(
            select(MirrorMedia).where(MirrorMedia.work_key == key)).all())
        if rows:
            return rows
    # 作品键口径漂移兜底：用作品成员的 TMDB id 反查。
    tmdb_ids: set[int] = set()
    for res in resources or ():
        tid = getattr(res, "tmdb_id", None)
        if tid:
            try:
                tmdb_ids.add(int(tid))
            except (TypeError, ValueError):
                continue
    if tmdb_ids:
        rows = list(db.scalars(
            select(MirrorMedia).where(MirrorMedia.tmdb_id.in_(list(tmdb_ids)))).all())
        if rows:
            return rows
    # 最后兜底：归一化标题匹配。
    names = {_norm(getattr(r, "media_title", "")) for r in (resources or ())}
    names.discard("")
    if names:
        rows = []
        for m in db.scalars(select(MirrorMedia).where(MirrorMedia.title_standard != "")).all():
            if _norm(m.title_standard) in names:
                rows.append(m)
        if rows:
            return rows
    return []


def _dir_marked_ok(db: Session, media: MirrorMedia) -> bool:
    """B 库影视主目录是否已按 `·` 约定整理。

    网盘明确拒绝带 `·` 的目录名时，镜像会降级成标准名并在 mirror_dir 记
    `created_fallback`——这是网盘侧限制而非整理遗漏，不应阻塞推送。
    """
    if NAME_MARK in _text(media.title_mirror):
        return True
    row = db.scalar(select(MirrorDir).where(
        MirrorDir.rule_id == media.rule_id,
        MirrorDir.source_dir_id == _text(media.source_dir_id),
        MirrorDir.kind == "work").limit(1))
    return bool(row is not None and _text(row.status) == "created_fallback")


def _files_english_ok(db: Session, media: MirrorMedia) -> bool:
    """作品内单文件名称是否已全部翻译为英文。

    - 英文名尚未解析（`english_title` 空）→ 不通过；
    - 没有已同步到 B 库的文件 → 不通过（文件都还没过去，谈不上改名）；
    - 任一已同步文件的改名状态不是 done/skipped_ascii/skipped_legacy → 不通过；
    - 状态是 done 但改出来的目标名为空 → 不通过。
    """
    if not _text(media.english_title):
        return False
    rows = list(db.scalars(
        select(MirrorFile).where(MirrorFile.media_id == media.id)).all())
    synced = [r for r in rows if _text(r.status) == "synced"]
    if not synced:
        return False
    for row in synced:
        status = _text(row.rename_status)
        if status == "done":
            if not _text(row.target_name):
                return False
            continue
        if status in ("skipped_ascii", "skipped_legacy"):
            continue
        return False
    return True


def evaluate_push_gate(db: Session, work_key: str, resources,
                       media_rows=None) -> dict:
    """判定一部作品是否满足推送门禁。

    参数：
      db          会话
      work_key    作品聚合键（`work:` / `tmdb:` / `file:`），用于关联镜像记录
      resources   作品成员（Resource 对象列表）。为空则资源级三项一律不通过。
      media_rows  已查出的镜像记录；为 None 时按 work_key 现场查。

    返回：
      {"passed": bool, "blocked": [门禁项], "reason": "中文原因",
       "items": {门禁项: bool}}
    """
    items = _member_items(resources)
    media_list = media_rows if media_rows is not None else find_mirror_media(
        db, work_key=work_key, resources=resources)
    if media_list:
        items["dir_marked"] = any(_dir_marked_ok(db, m) for m in media_list)
        items["file_english"] = any(_files_english_ok(db, m) for m in media_list)
    else:
        # 非镜像链路作品：不存在「影视目录 / 单文件改名」这两项，按前三项判定。
        items["dir_marked"] = True
        items["file_english"] = True
    blocked = [key for key in GATE_ITEMS if not items.get(key, False)]
    reason = "；".join(GATE_LABELS[key] for key in blocked)
    return {"passed": not blocked, "blocked": blocked, "reason": reason, "items": items}
