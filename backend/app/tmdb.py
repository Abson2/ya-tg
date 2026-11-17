"""TMDB 凭证、搜索、详情与媒体识别决策。

v0.6.0 识别策略（借鉴 TgtoDrive「TMDB 识别作品与季集 / ID 精确优先 / 名称+年份兜底 /
AI 回退识别」的思路，见其 README 与 /api/organize-history/recognize-test）：

1. 文件名或目录里带 tmdb-xxxxx → 直接按 ID 取详情，最可靠
2. 季集号（S01E04 / 第4集 / Season 2）或目录分类 → 硬约束 tv，绝不回退电影
3. 类型未知 → tv / movie 双路搜索，按标题相似度 + 年份 + 热度评分
4. 分数接近或无结果 → 标记 ambiguous/failed，交人工手动匹配（不猜一个电影糊上去）
5. 可选 OpenAI 兼容 AI：解析文件名、提供备用片名、给候选消歧；失败静默降级
"""
import html
import json
import re
from difflib import SequenceMatcher

import httpx
from sqlalchemy.orm import Session

from . import runtime_config
from .config import settings
from .utils import parse_media_name

BASE = "https://api.themoviedb.org/3"
IMAGE_BASE = "https://image.tmdb.org/t/p"

_V3_KEY = re.compile(r"^[0-9a-fA-F]{32}$")
_V4_TOKEN = re.compile(r"^eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$")

# TMDB 官方 genre id -> 中文名（search 结果只有 genre_ids，detail 才有 genres[].name）
TMDB_GENRES = {
    28: "动作", 12: "冒险", 16: "动画", 35: "喜剧", 80: "犯罪", 99: "纪录", 18: "剧情",
    10751: "家庭", 14: "奇幻", 36: "历史", 27: "恐怖", 10402: "音乐", 9648: "悬疑",
    10749: "爱情", 878: "科幻", 10770: "电视电影", 53: "惊悚", 10752: "战争", 37: "西部",
    10759: "动作冒险", 10762: "儿童", 10763: "新闻", 10764: "真人秀", 10765: "科幻奇幻",
    10766: "肥皂剧", 10767: "访谈", 10768: "战争政治",
}
COUNTRY_NAMES = {
    "US": "美国", "GB": "英国", "CN": "中国", "HK": "中国香港", "TW": "中国台湾",
    "MO": "中国澳门", "JP": "日本", "KR": "韩国", "FR": "法国", "DE": "德国",
    "IT": "意大利", "ES": "西班牙", "CA": "加拿大", "AU": "澳大利亚", "IN": "印度",
    "TH": "泰国", "SG": "新加坡", "RU": "俄罗斯", "NZ": "新西兰", "NL": "荷兰",
    "SE": "瑞典", "DK": "丹麦", "NO": "挪威", "FI": "芬兰", "BR": "巴西",
    "MX": "墨西哥", "AR": "阿根廷", "TR": "土耳其", "PL": "波兰", "IE": "爱尔兰",
    "CH": "瑞士", "AT": "奥地利", "BE": "比利时", "PT": "葡萄牙", "GR": "希腊",
    "IL": "以色列", "ID": "印度尼西亚", "MY": "马来西亚", "PH": "菲律宾",
    "VN": "越南", "ZA": "南非", "EG": "埃及", "CZ": "捷克", "HU": "匈牙利",
}

_CN = re.compile(r"[\u4e00-\u9fff]")


def _prefer_chinese(*values) -> str:
    """剧名优先中文：从多个标题字段里挑第一个含中文的；都没有则取第一个非空。"""
    for v in values:
        s = str(v or "").strip()
        if s and _CN.search(s):
            return s
    for v in values:
        s = str(v or "").strip()
        if s:
            return s
    return ""


class TmdbAuthError(RuntimeError):
    """TMDB 凭证缺失或无效，携带可直接展示给用户的中文提示。"""


def _looks_v4(value: str) -> bool:
    return bool(_V4_TOKEN.match(value or ""))


def _looks_v3(value: str) -> bool:
    return bool(_V3_KEY.match(value or ""))


def credentials(db: Session) -> dict:
    """读取 TMDB 凭证（数据库优先、.env 兜底），并按值本身特征自动识别认证方式。"""
    if db is None:
        raw_bearer, raw_key = settings.tmdb_bearer_token, settings.tmdb_api_key
        language = settings.tmdb_language
    else:
        raw_bearer = runtime_config.get(db, "tmdb_bearer_token")
        raw_key = runtime_config.get(db, "tmdb_api_key")
        language = runtime_config.get(db, "tmdb_language") or settings.tmdb_language
    raw_bearer = (raw_bearer or "").strip()
    raw_key = (raw_key or "").strip()

    bearer, key = "", ""
    for origin, value in (("bearer", raw_bearer), ("key", raw_key)):
        if not value:
            continue
        if _looks_v4(value):
            bearer = value
        elif _looks_v3(value):
            key = value
        elif origin == "bearer" and not bearer:
            bearer = value
        elif not key:
            key = value
    return {"bearer": bearer, "key": key, "language": language or "zh-CN"}


def configured(db: Session | None = None) -> bool:
    c = credentials(db)
    return bool(c["bearer"] or c["key"])


def auth_status(db: Session | None = None) -> dict:
    """给管理页展示：是否已配置、识别到的类型、是否有明显格式问题。"""
    c = credentials(db)
    has_any = bool(c["bearer"] or c["key"])
    if db is None:
        raw_b, raw_k = (settings.tmdb_bearer_token or "").strip(), (settings.tmdb_api_key or "").strip()
    else:
        raw_b = (runtime_config.get(db, "tmdb_bearer_token") or "").strip()
        raw_k = (runtime_config.get(db, "tmdb_api_key") or "").strip()
    suspicious = bool((raw_b and not _looks_v4(raw_b) and not _looks_v3(raw_b)) or
                      (raw_k and not _looks_v4(raw_k) and not _looks_v3(raw_k)))
    return {
        "configured": has_any,
        "mode": "v4-bearer" if c["bearer"] else ("v3-key" if c["key"] else "none"),
        "language": c["language"],
        "format_warning": suspicious,
        "masked": (("Bearer " + _mask(c["bearer"])) if c["bearer"] else
                   ("API Key " + _mask(c["key"])) if c["key"] else ""),
    }


def _mask(value: str) -> str:
    if len(value) <= 8:
        return "****"
    return value[:4] + "…" + value[-4:]


async def _get(path: str, params: dict, db: Session | None = None) -> dict:
    c = credentials(db)
    if not (c["bearer"] or c["key"]):
        raise TmdbAuthError("尚未配置 TMDB 密钥。请到「TMDB 设置」页填入 v3 API Key 或 v4 Read Access Token。")
    # 剧名优先中文：所有端点统一带 language，避免漏传时 TMDB 返回英文原名
    params = {**params, "language": params.get("language") or c["language"] or "zh-CN"}
    headers = {"accept": "application/json"}
    if c["bearer"]:
        headers["Authorization"] = f"Bearer {c['bearer']}"
    else:
        params = {**params, "api_key": c["key"]}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(f"{BASE}{path}", params=params, headers=headers)
    if r.status_code in (401, 403):
        mode = "v4 Read Access Token" if c["bearer"] else "v3 API Key"
        raise TmdbAuthError(
            f"TMDB 密钥无效或权限不足（HTTP {r.status_code}，当前按 {mode} 方式调用）。"
            "请确认：v3 API Key 是 32 位十六进制字符串；v4 Read Access Token 以 eyJ 开头。"
            "建议到「TMDB 设置」页重新粘贴正确的密钥并点「测试连接」。")
    if r.status_code >= 400:
        raise RuntimeError(f"TMDB 请求失败 HTTP {r.status_code}：{r.text[:200]}")
    return r.json()


def type_label(media_type: str) -> str:
    return {"movie": "电影", "tv": "剧集"}.get(media_type or "", "未知")


def _as_list(value) -> list[str]:
    """兼容 DB 里的 JSON 字符串、逗号串与 Python list。"""
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        return [str(x).strip() for x in value if x]
    text = str(value).strip()
    if text.startswith("["):
        try:
            return [str(x).strip() for x in json.loads(text) if x]
        except Exception:
            pass
    return [x.strip() for x in text.split(",") if x.strip()]


as_list = _as_list  # 公开别名，供 publisher 等模块复用


def genre_label(value) -> str:
    return "、".join(_as_list(value))


def country_label(value) -> str:
    out = []
    for code in _as_list(value):
        out.append(COUNTRY_NAMES.get(code.upper(), code))
    return "、".join(dict.fromkeys(out))


def _normalize(item: dict, media_type: str) -> dict:
    localized = item.get("name") or item.get("title") or ""
    original = item.get("original_title") or item.get("original_name") or ""
    # zh-CN 下 name 通常已是中文译名；若接口仍给英文，取其它字段里含中文的值兜底
    title = _prefer_chinese(localized, original) or localized
    date = item.get("release_date") or item.get("first_air_date") or ""
    genres: list[str] = []
    for g in item.get("genres") or []:
        if isinstance(g, dict):
            name = str(g.get("name") or "")
        else:
            name = str(g or "")
        if name:
            genres.append(name)
    if not genres:
        for gid in item.get("genre_ids") or []:
            name = TMDB_GENRES.get(int(gid)) if str(gid).isdigit() else ""
            if name:
                genres.append(name)
    countries: list[str] = []
    for c in item.get("origin_country") or item.get("production_countries") or []:
        code = str((c.get("iso_3166_1") or c.get("name") or "") if isinstance(c, dict) else c).strip()
        if code:
            countries.append(code)
    return {
        "tmdb_id": item.get("id"),
        "media_type": media_type,
        "type": media_type,
        "type_label": type_label(media_type),
        "title": title,
        "original_title": original,
        "year": date[:4],
        "poster_path": item.get("poster_path") or "",
        "backdrop_path": item.get("backdrop_path") or "",
        "overview": item.get("overview") or "",
        "vote_average": item.get("vote_average") or 0,
        "popularity": item.get("popularity") or 0,
        "genres": genres,
        "origin_country": countries,
        "score": 0.0,
    }


async def search(title: str, media_type: str | None = None, db: Session | None = None) -> list[dict]:
    title = (title or "").strip()
    if not title:
        return []
    language = credentials(db)["language"]
    params = {"query": title, "language": language, "page": 1}
    path = "/search/multi" if media_type not in ("movie", "tv") else f"/search/{media_type}"
    data = await _get(path, params, db)
    out = []
    for x in data.get("results", []):
        mt = media_type or (x.get("media_type") or ("tv" if "first_air_date" in x else "movie"))
        if mt not in ("movie", "tv"):
            continue
        out.append(_normalize(x, mt))
    return out[:10]


async def search_both(title: str, db: Session | None = None) -> list[dict]:
    """类型未知时同时查剧集和电影，供评分决策。"""
    out: list[dict] = []
    for mt in ("tv", "movie"):
        try:
            out.extend(await search(title, mt, db))
        except TmdbAuthError:
            raise
        except Exception:
            continue
    return out


async def detail(tmdb_id: int, media_type: str, db: Session | None = None) -> dict:
    language = credentials(db)["language"]
    data = await _get(f"/{media_type}/{int(tmdb_id)}", {"language": language}, db)
    return _normalize(data, media_type)


async def detail_by_id_any(tmdb_id: int, preferred: str = "movie",
                           db: Session | None = None) -> tuple[dict | None, str]:
    """ID 精确匹配：先试首选类型，再试另一类型（有些标记不带类型）。"""
    order = [preferred, "tv" if preferred == "movie" else "movie"]
    for mt in order:
        try:
            d = await detail(tmdb_id, mt, db)
            if d and d.get("tmdb_id"):
                return d, mt
        except TmdbAuthError:
            raise
        except Exception:
            continue
    return None, ""


def _norm_text(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"\(?(?:19|20)\d{2}\)?", " ", s)
    s = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if a in b or b in a:
        return 0.85
    return SequenceMatcher(None, a, b).ratio()


def rank(candidates: list[dict], hint: dict) -> list[dict]:
    """标题相似度 + 年份 + 热度打分；剧集单集用「作品名」而不是整串文件名比对。"""
    q = _norm_text(hint.get("clean_title") or hint.get("title") or "")
    year = str(hint.get("year") or "")
    out = []
    for it in candidates:
        names = [it.get("title") or "", it.get("original_title") or ""]
        norm_names = [_norm_text(n) for n in names if n]
        sim = max((_similarity(q, n) for n in norm_names), default=0.0)
        score = sim
        if q and q in norm_names:
            score += 0.30
        cy = str(it.get("year") or "")
        if year and cy:
            if cy == year:
                score += 0.18
            elif abs(int(cy) - int(year)) <= 1:
                score += 0.05
            else:
                score -= 0.15
        score += min(float(it.get("popularity") or 0) / 200.0, 0.05)
        d = dict(it)
        d["score"] = round(max(0.0, score), 4)
        out.append(d)
    return sorted(out, key=lambda x: (-x["score"], -(x.get("popularity") or 0)))


async def _search_with_retry(query: str, media_type: str, alt_query: str,
                             db: Session | None) -> list[dict]:
    c = await search(query, media_type, db)
    if not c and alt_query and alt_query.strip() != query:
        c = await search(alt_query.strip(), media_type, db)
    if not c and " " in query:
        short = " ".join(query.split()[:2])
        if short and short != query:
            c = await search(short, media_type, db)
    return c


def _localize_best(d: dict, hint: dict) -> dict:
    """剧名优先中文：TMDB 仍返回英文标题时，用中文目录名作为展示名。"""
    out = dict(d)
    if not _CN.search(str(out.get("title") or "")) and hint.get("has_chinese"):
        zh = str(hint.get("dir_title") or hint.get("primary_title") or "")
        if zh and _CN.search(zh):
            out["title"] = zh
            out["title_source"] = "directory"
    return out


def _manual_required(hint: dict, ai_info, ranked: list, query: str, english_alt: str) -> dict:
    """中文剧名没搜到、只有英文文件名候选时，绝不自动套一个相近的英文作品。"""
    return {"ok": False, "status": "ambiguous", "source": "tmdb",
            "reason": (f"按目录剧名「{query}」未命中；英文文件名「{english_alt}」找到 {len(ranked)} 个候选。"
                       "为避免把剧集单集认成其它作品，请点「手动匹配」按海报确认，或直接搜中文剧名。"),
            "hint": hint, "ai": ai_info, "candidates": ranked, "best": None}


async def identify(name: str, db: Session | None = None, path_hint: str = "",
                   use_ai: bool = True) -> dict:
    """识别决策，返回结构化结果；不直接写库。

    status: identified（可自动应用）/ ambiguous（匹配存疑，转人工）/ failed（无候选）
    """
    hint = parse_media_name(name, path_hint)
    ai_info = None

    # 1) ID 精确匹配（LitePan / TgtoDrive 目录里的 (tmdb-206484) 标记）
    if hint.get("tmdb_id"):
        preferred = "tv" if hint["type_hint"] == "tv" else "movie"
        d, actual = await detail_by_id_any(hint["tmdb_id"], preferred, db)
        if d:
            d["score"] = 1.0
            return {"ok": True, "status": "identified", "source": "id",
                    "reason": f"按文件名/目录中的 TMDB ID {hint['tmdb_id']} 精确匹配（{type_label(actual)}）",
                    "hint": hint, "ai": ai_info, "candidates": [d], "best": d}

    # 2) AI 辅助解析：只作为回退（类型未知时给类型，主搜索失败时给备用片名）
    alt_query = ""
    ai_on = False
    if use_ai:
        from . import ai_assist
        ai_on = ai_assist.enabled(db)
        if ai_on:
            try:
                ai_info = await ai_assist.parse_filename(name, db)
            except Exception as exc:
                ai_info = {"ok": False, "message": str(exc)}
            if ai_info.get("ok"):
                if hint["type_hint"] == "unknown" and ai_info.get("type") in ("movie", "tv"):
                    hint["type_hint"] = ai_info["type"]
                    hint["type_source"] = "ai"
                if not hint.get("year") and ai_info.get("year"):
                    hint["year"] = str(ai_info["year"])
                alt_query = str(ai_info.get("title") or "")

    # v0.6.1：主查询用目录里的作品名（通常中文），英文单集文件名只作备用，
    # 且备用命中绝不自动落库——这正是「谷雨/Season 1/The.Silent.Storm.S01E04」
    # 被误认成「寂静之噪」的根因。
    query = hint.get("primary_title") or hint.get("clean_title") or hint.get("title") or ""
    english_alt = str(hint.get("alt_title") or "").strip()
    label = type_label(hint["type_hint"]) if hint["type_hint"] != "unknown" else "影视"

    # 中文剧名先自动翻译成英/日/韩标题再搜 TMDB：TMDB 中文索引不全，
    # 用作品官方外语名比拿英文单集文件名去搜可靠得多。
    translated_queries: list[str] = []
    translated_used = ""
    if use_ai and hint.get("has_chinese"):
        try:
            from . import ai_assist
            tr = await ai_assist.translate_title(query, str(hint.get("year") or ""), db)
            translated_queries = list(tr.get("queries") or [])[:3]
        except Exception:
            translated_queries = []

    # 3) 类型明确：只搜该类型。剧集绝不回退电影（修复 S01E04 被识别成电影）
    if hint["type_hint"] in ("tv", "movie"):
        mt = hint["type_hint"]
        candidates = await _search_with_retry(query, mt, alt_query, db)
        if not candidates and translated_queries:
            for tq in translated_queries:
                candidates = await _search_with_retry(tq, mt, "", db)
                if candidates:
                    translated_used = tq
                    break
        fallback_used = False
        if not candidates and english_alt:
            candidates = await _search_with_retry(english_alt, mt, "", db)
            fallback_used = bool(candidates)
        if not candidates:
            return {"ok": False, "status": "failed", "source": "none",
                    "reason": f"按{label}搜索「{query}」没有结果，请用「手动匹配」指定片名或 TMDB ID",
                    "hint": hint, "ai": ai_info, "candidates": [], "best": None}
        rank_hint = dict(hint)
        if fallback_used:
            rank_hint["clean_title"] = english_alt
        elif translated_used:
            rank_hint["clean_title"] = translated_used
        ranked = rank(candidates, rank_hint)
        if fallback_used:
            return _manual_required(hint, ai_info, ranked, query, english_alt)
        if (not translated_used and hint.get("has_chinese")
                and hint.get("title_source") == "directory"):
            # TMDB 已按中文剧名语义命中；中英标题字符串相似度天然接近 0，
            # 不能因此判成「匹配存疑」。年份一致给更高信任分。
            y = str(hint.get("year") or "")
            for it in ranked:
                boost = 0.60 if (y and str(it.get("year") or "") == y) else 0.42
                it["score"] = max(float(it["score"] or 0), boost)
            ranked.sort(key=lambda x: (-x["score"], -(x.get("popularity") or 0)))
        ranked = [_localize_best(x, hint) for x in ranked]
        best = ranked[0]
        if best["score"] < (0.45 if translated_used else 0.30):
            return {"ok": False, "status": "ambiguous", "source": "tmdb",
                    "reason": f"按{label}找到 {len(ranked)} 个候选，但匹配度偏低，建议手动确认",
                    "hint": hint, "ai": ai_info, "candidates": ranked, "best": None}
        why = f"（中文剧名译为「{translated_used}」）" if translated_used else ""
        return {"ok": True, "status": "identified",
                "source": "ai" if translated_used or hint["type_source"] == "ai" else "tmdb",
                "reason": f"按{label}匹配「{best['title']}」{why}（相似度 {best['score']:.2f}）",
                "hint": hint, "ai": ai_info, "candidates": ranked, "best": best}

    # 4) 类型未知：双路搜索 + 评分，分数接近就转人工，绝不乱猜
    candidates = await search_both(query, db)
    fallback_used = False
    if not candidates and alt_query and alt_query.strip() != query:
        candidates = await search_both(alt_query.strip(), db)
    if not candidates and translated_queries:
        for tq in translated_queries:
            candidates = await search_both(tq, db)
            if candidates:
                translated_used = tq
                break
    if not candidates and english_alt:
        candidates = await search_both(english_alt, db)
        fallback_used = bool(candidates)
    if not candidates:
        return {"ok": False, "status": "failed", "source": "none",
                "reason": f"电影/剧集双路搜索「{query}」都没有结果，请用「手动匹配」",
                "hint": hint, "ai": ai_info, "candidates": [], "best": None}
    rank_hint = dict(hint)
    if fallback_used:
        rank_hint["clean_title"] = english_alt
    elif translated_used:
        rank_hint["clean_title"] = translated_used
    ranked = rank(candidates, rank_hint)
    if fallback_used:
        return _manual_required(hint, ai_info, ranked, query, english_alt)
    ranked = [_localize_best(x, hint) for x in ranked]
    top1 = ranked[0]
    top2 = ranked[1] if len(ranked) > 1 else None
    gap = top1["score"] - (top2["score"] if top2 else 0.0)
    same_type = bool(top2 and top2["media_type"] == top1["media_type"])
    if top1["score"] >= 0.60 and (gap >= 0.08 or same_type):
        return {"ok": True, "status": "identified", "source": "tmdb",
                "reason": f"双路搜索评分最高「{top1['title']}」（{type_label(top1['media_type'])}，{top1['score']:.2f}）",
                "hint": hint, "ai": ai_info, "candidates": ranked, "best": top1}

    picked = None
    if ai_on:
        from . import ai_assist
        try:
            picked = await ai_assist.disambiguate(name, ranked[:6], db)
        except Exception:
            picked = None
        if picked and picked.get("tmdb_id"):
            for it in ranked:
                if int(it.get("tmdb_id") or 0) == int(picked["tmdb_id"]):
                    return {"ok": True, "status": "identified", "source": "ai",
                            "reason": "候选接近，由 AI 辅助选定：" + str(picked.get("reason") or it["title"]),
                            "hint": hint, "ai": ai_info, "candidates": ranked, "best": it}

    return {"ok": False, "status": "ambiguous", "source": "tmdb",
            "reason": "电影/剧集候选接近，无法自动判定类型。请点「手动匹配」按海报和类型选择",
            "hint": hint, "ai": ai_info, "candidates": ranked, "best": None}


async def identify_by_filename(name: str, db: Session | None = None,
                               path_hint: str = "", use_ai: bool = True) -> list[dict]:
    """兼容旧调用：返回排序后的候选列表（不再自动回退 movie）。"""
    try:
        result = await identify(name, db, path_hint, use_ai)
        return result["candidates"]
    except TmdbAuthError:
        raise


async def test_connection(db: Session) -> dict:
    """用配置连接 TMDB 做一次真实请求，返回是否可用与识别到的模式。"""
    st = auth_status(db)
    if not st["configured"]:
        return {"ok": False, "mode": st["mode"], "message": "尚未配置 TMDB 密钥"}
    try:
        await _get("/search/movie", {"query": "test", "page": 1}, db)
        return {"ok": True, "mode": st["mode"], "message": "连接成功，密钥有效", "masked": st["masked"]}
    except Exception as exc:
        return {"ok": False, "mode": st["mode"], "message": str(exc), "masked": st["masked"]}


def poster_url(path: str, size: str = "w500") -> str:
    if not path:
        return ""
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return f"{IMAGE_BASE}/{size}{path}"


def tmdb_url(media_type: str, tmdb_id: int) -> str:
    return f"https://www.themoviedb.org/{media_type}/{tmdb_id}"


async def search_multilingual(title: str, media_type: str | None = None, year: str = "",
                              translations: list[str] | None = None,
                              db: Session | None = None) -> dict:
    """手动识别：中文标题 + 自动翻译出的英/日/韩标题分别搜 TMDB，合并去重复选。

    返回带 poster_url 的候选（电影/剧集海报墙）、翻译出的外语标题与实际尝试过的查询词。
    AI 未配置时仍用中文原名直搜，不影响手动匹配可用性。
    """
    from . import ai_assist

    q0 = (title or "").strip()
    if not q0:
        return {"ok": False, "query": "", "translated": [], "tried": [], "candidates": [],
                "message": "请输入中文片名或 TMDB ID"}
    trans: list[str] = []
    note = ""
    if _CN.search(q0):
        try:
            tr = await ai_assist.translate_title(q0, year, db)
            trans = list(tr.get("queries") or [])
            note = str(tr.get("message") or "")
        except Exception:
            trans = []
    queries: list[tuple[str, str]] = [(q0, "中文")]
    for q in trans + list(translations or []):
        q = (q or "").strip()
        if q and q != q0 and q not in [x[0] for x in queries]:
            queries.append((q, "外语"))
    merged: dict[tuple, dict] = {}
    tried: list[dict] = []
    for q, lang in queries[:5]:
        try:
            rows = (await search(q, media_type, db) if media_type in ("movie", "tv")
                    else await search_both(q, db))
        except TmdbAuthError:
            raise
        except Exception:
            rows = []
        tried.append({"query": q, "language": lang, "count": len(rows)})
        for it in rows:
            key = (it.get("media_type"), int(it.get("tmdb_id") or 0))
            if key in merged:
                continue
            d = dict(it)
            d["poster_url"] = poster_url(d.get("poster_path") or "")
            d["matched_by"] = q
            merged[key] = d
    candidates = list(merged.values())[:20]
    tried_text = "、".join(dict.fromkeys(x["query"] for x in tried))
    message = (f"已用「{tried_text}」搜索，找到 {len(candidates)} 个候选"
               + (f"；{note}" if note else "")) if candidates else \
              "中文标题及外语译名均未找到结果，请检查片名或直接输入 TMDB ID"
    return {"ok": bool(candidates), "query": q0, "translated": trans,
            "tried": tried, "candidates": candidates, "message": message}


def escape(value) -> str:
    return html.escape(str(value or ""), quote=False)
