"""识别批次选取：按作品分散 + 轮次推进。

v2.1.12 新增。修的是真机报障「资源库按目录重识别看起来没生效」。

**根因不是识别算法，是批次选取。**
用户实测反证：库里 1275 条资源，目录线索（path_hint / source_title）空值均为 0，
最新 60 条全部正确识别出「按文件名/目录中的 TMDB ID 精确匹配（剧集）」——
目录名优先、{tmdb-} 标记提取、A 库映射这三件套都是好的。
真正的问题在选哪一批：

  ① ``order_by(Resource.id.desc()).limit(N)`` 是**固定窗口**——永远取 id 最大的 N 条。
     真机 1275 条里 id 1226–1275 恰好全属同一部剧（封神英雄榜），
     于是这个窗口每次都被同一部剧的集数占满，其余 1215 条从未被覆盖。
  ② force=True 时**连状态过滤都不加**——已识别的行永远占着窗口，
     连点几次都只在同一部剧上打转。

本模块把批次选取改成两件事：

  ① **按作品分散**：候选用 ``work_group_key`` 聚成「作品」，一批里每部作品先出 1 条，
     作品内部再逐轮加（第 2 轮每部作品再出 1 条……）。
     窗口不再被单部剧的集数吃满，一批就能同时覆盖多部作品。
  ② **轮次推进**：``Resource.identify_round`` 记该资源**累计参与识别的轮次**
     （0 = 从未参与）。候选取「参与轮次少」的优先 → 连点几次就能依次覆盖全库，
     而不是永远重复最新那批；一轮处理完的自动排到后面，下一轮才回来。

轮次是**每条资源自己的计数**（不是全局批次号），所以天然公平：
哪条被尝试得少，下次优先补哪条；被尝试过的自动让位给还没轮到过的。
限流待重试的资源仍在待识别集合里，只是排到本作品队尾。

口径要点：
  - 手动「按目录重识别」：作品内先补**没认出来的**，再回头重算已识别项（修正误配）。
  - 自动识别管线：作品内**新内容优先**（刚扫进来的要先认出来才能推送）。
"""
from __future__ import annotations

from sqlalchemy import func, select

from .models import Resource
from .utils import work_group_key

# 待识别状态集合。v2.1.9 口径：限流是「待重试」，必须留在候选集里下一轮再试，
# 不能写死 failed（那等于永久放弃重试）。
PENDING_IDENTIFY_STATUSES = (
    "unidentified", "ambiguous", "failed", "none", "", "rate_limited")

# 候选集上限：分组要全量才能算出「每部作品」的分布，但库可能很大，取个上限兜底。
# 取候选的排序保证这个上限不会切掉「刚扫进来的新内容」（见 select_identify_batch）。
CANDIDATE_CAP = 5000


def count_pending(db) -> int:
    """库里当前「待识别」总量（含限流待重试）。

    供前端如实展示「库中还有多少项没认出来」——用户反馈的核心痛点之一
    就是「按钮只处理了一小部分，看起来像没生效」，必须让存量可见。
    """
    stmt = select(func.count()).select_from(Resource).where(
        Resource.identify_status.in_(PENDING_IDENTIFY_STATUSES))
    return int(db.scalar(stmt) or 0)


def _round_of(row: Resource) -> int:
    """该资源累计参与识别的轮次（0 = 从未参与）。"""
    return int(row.identify_round or 0)


def _within_work_key(row: Resource, prefer_new: bool):
    """作品内部的排序键。

    prefer_new=True（自动识别管线）：(轮次, id 倒序) → 同一部作品里**新内容先认**，
    老剧的新增集数不会被旧积压挡住；参与轮次少的也先补。
    prefer_new=False（手动批量重识别）：(待识别优先, 轮次, id 正序)
      → 先把「还没认出来的」补齐，再回头重算已识别项（修正误配）。
    """
    rid = int(row.id or 0)
    rnd = _round_of(row)
    if prefer_new:
        return (rnd, -rid)
    pending_first = 0 if (row.identify_status or "") in PENDING_IDENTIFY_STATUSES else 1
    return (pending_first, rnd, rid)


def select_identify_batch(db, *, limit, force: bool = False,
                          prefer_new: bool = False,
                          cap: int = CANDIDATE_CAP):
    """挑一批待识别资源：按作品分散 + 轮次推进。

    返回 ``(rows, info)``：
      - ``rows`` 选中的资源，已按「先补未识别、再按作品轮转」排好序；
      - ``info``  诊断信息（覆盖作品数、候选数、库里作品总数）。

    force=True 时候选包含**已识别**的资源——「按目录重识别」按钮的既定语义是
    连已识别项一起重算，用于修正误配；只是不再由它们霸占固定窗口。
    """
    limit = max(1, int(limit or 1))
    stmt = select(Resource)
    if not force:
        stmt = stmt.where(Resource.identify_status.in_(PENDING_IDENTIFY_STATUSES))
    # 取候选：参与轮次少的优先 → 跨轮自动推进到没覆盖过的那批。
    # 同一轮次内：手动按 id 正序（旧的先补，覆盖真机那 1215 条老资源），
    # 自动按 id 倒序（刚扫进来的新内容先认，否则会推不出去）。
    order_id = Resource.id.desc() if prefer_new else Resource.id.asc()
    stmt = stmt.order_by(Resource.identify_round.asc(), order_id)
    rows = list(db.scalars(stmt.limit(cap)).all())
    if not rows:
        return [], {"works": 0, "candidates": 0, "works_total": 0}

    # 按作品分组（work_group_key 只用扫描阶段就有的字段，未识别单集也能正确归组）。
    groups: dict[str, list[Resource]] = {}
    for r in rows:
        groups.setdefault(work_group_key(r), []).append(r)
    for items in groups.values():
        items.sort(key=lambda r: _within_work_key(r, prefer_new))

    def _work_sort_key(item):
        _key, items = item
        ids = [int(x.id or 0) for x in items]
        min_round = min(_round_of(x) for x in items)
        if prefer_new:
            # 自动管线：先「还没轮到过的作品」，同组内新内容优先（id 倒序）。
            return (min_round, -max(ids))
        # 手动批量：先「还有没认出来的作品」，其次「还没轮到过的作品」，
        # 最后按最老 id 稳定排序。
        has_pending = 0 if any(
            (x.identify_status or "") in PENDING_IDENTIFY_STATUSES for x in items) else 1
        return (has_pending, min_round, min(ids))

    ordered = sorted(groups.items(), key=_work_sort_key)

    # 分层轮转：第 1 遍每部作品出 1 条，第 2 遍每部作品再出 1 条……
    # 这样一批 limit 条能同时覆盖多部作品，窗口不会被单部剧的集数吃满。
    picked: list[Resource] = []
    depth = 0
    while len(picked) < limit:
        progressed = False
        for _key, items in ordered:
            if depth < len(items):
                picked.append(items[depth])
                progressed = True
                if len(picked) >= limit:
                    break
        if not progressed:
            break
        depth += 1

    info = {
        "works": len({work_group_key(r) for r in picked}),
        "candidates": len(rows),
        "works_total": len(ordered),
    }
    return picked, info


def mark_attempted(row: Resource) -> None:
    """记一次「参与过识别批次」——该资源的轮次 +1。

    只在**真正尝试**识别之后调用（成功/失败/限流都算），
    配额不足直接中止本轮时，未尝试的条目保持原轮次，下一轮照样排前面。
    """
    row.identify_round = _round_of(row) + 1
