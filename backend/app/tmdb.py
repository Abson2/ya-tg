import html
import httpx

from .config import settings
from .utils import parse_media_name

BASE = "https://api.themoviedb.org/3"
IMAGE_BASE = "https://image.tmdb.org/t/p"


def configured() -> bool:
    return bool(settings.tmdb_bearer_token or settings.tmdb_api_key)


async def _get(path: str, params: dict) -> dict:
    if not configured():
        raise RuntimeError("未配置 TMDB_API_KEY 或 TMDB_BEARER_TOKEN")
    headers = {"accept": "application/json"}
    if settings.tmdb_bearer_token:
        headers["Authorization"] = f"Bearer {settings.tmdb_bearer_token}"
    else:
        params = {**params, "api_key": settings.tmdb_api_key}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(f"{BASE}{path}", params=params, headers=headers)
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


async def search(title: str, media_type: str | None = None) -> list[dict]:
    title = (title or "").strip()
    if not title:
        return []
    params = {"query": title, "language": settings.tmdb_language, "page": 1}
    path = "/search/multi" if media_type not in ("movie", "tv") else f"/search/{media_type}"
    data = await _get(path, params)
    out = []
    for x in data.get("results", []):
        mt = x.get("media_type") or ("tv" if "first_air_date" in x else "movie")
        if mt not in ("movie", "tv"):
            continue
        if media_type and mt != media_type:
            continue
        out.append(_normalize(x, mt))
    return out[:10]


async def detail(tmdb_id: int, media_type: str) -> dict:
    data = await _get(f"/{media_type}/{int(tmdb_id)}", {"language": settings.tmdb_language})
    return _normalize(data, media_type)


async def identify_by_filename(name: str) -> list[dict]:
    """按文件名启发式优先搜索 movie/tv，再按 TMDB 结果补全。"""
    hint = parse_media_name(name)
    query = hint["title"]
    preferred = "tv" if hint["is_tv_hint"] else None
    results = await search(query, preferred)
    if not results and preferred:
        results = await search(query)
    return results


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
