import html
import re
import httpx
from sqlalchemy.orm import Session

from . import runtime_config
from .config import settings
from .utils import parse_media_name

BASE = "https://api.themoviedb.org/3"
IMAGE_BASE = "https://image.tmdb.org/t/p"

_V3_KEY = re.compile(r"^[0-9a-fA-F]{32}$")
_V4_TOKEN = re.compile(r"^eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$")


class TmdbAuthError(RuntimeError):
    """TMDB 凭证缺失或无效，携带可直接展示给用户的中文提示。"""


def _looks_v4(value: str) -> bool:
    return bool(_V4_TOKEN.match(value or ""))


def _looks_v3(value: str) -> bool:
    return bool(_V3_KEY.match(value or ""))


def credentials(db: Session) -> dict:
    """读取 TMDB 凭证（数据库优先、.env 兜底），并按值本身特征自动识别认证方式。

    用户常把 v4 token 填进 TMDB_API_KEY、或把 v3 key 填进 TMDB_BEARER_TOKEN，
    这里按格式纠正，避免 "Invalid API key" 这类难懂的 401。
    """
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
    # 按位置判断来源，避免两个字段填了相同值时误判
    for origin, value in (("bearer", raw_bearer), ("key", raw_key)):
        if not value:
            continue
        if _looks_v4(value):
            bearer = value
        elif _looks_v3(value):
            key = value
        elif origin == "bearer" and not bearer:
            # 格式不认识：按填写字段的原意处理，交给服务端校验后给出明确错误
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
    # 有填值但既不像 v3 也不像 v4 → 提示格式可疑
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


def _normalize(item: dict, media_type: str) -> dict:
    title = item.get("title") or item.get("name") or ""
    date = item.get("release_date") or item.get("first_air_date") or ""
    return {
        "tmdb_id": item.get("id"),
        "media_type": media_type,
        "title": title,
        "year": date[:4],
        "poster_path": item.get("poster_path") or "",
        "overview": item.get("overview") or "",
        "vote_average": item.get("vote_average"),
        "popularity": item.get("popularity"),
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
        mt = x.get("media_type") or ("tv" if "first_air_date" in x else "movie")
        if mt not in ("movie", "tv"):
            continue
        if media_type and mt != media_type:
            continue
        out.append(_normalize(x, mt))
    return out[:10]


async def detail(tmdb_id: int, media_type: str, db: Session | None = None) -> dict:
    language = credentials(db)["language"]
    data = await _get(f"/{media_type}/{int(tmdb_id)}", {"language": language}, db)
    return _normalize(data, media_type)


async def identify_by_filename(name: str, db: Session | None = None) -> list[dict]:
    """按文件名启发式优先搜索 movie/tv，再按 TMDB 结果补全。"""
    hint = parse_media_name(name)
    query = hint["title"]
    preferred = "tv" if hint["is_tv_hint"] else None
    results = await search(query, preferred, db)
    if not results and preferred:
        results = await search(query, None, db)
    return results


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


def escape(value) -> str:
    return html.escape(str(value or ""), quote=False)
