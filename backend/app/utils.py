import asyncio
import re
import secrets
import time
from datetime import datetime, timedelta, timezone

from .config import settings


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def as_utc(value: datetime | None) -> datetime | None:
    """把数据库读回的时间统一成带 UTC 时区的 datetime。

    SQLite 的 DateTime(timezone=True) 不会保存 tzinfo，读回来是 naive；
    直接与 aware 的 utcnow() 比较会抛 'can't compare offset-naive and
    offset-aware datetimes'。所有时间比较都必须先经过本函数。
    """
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def traceparent() -> str:
    return f"00-{secrets.token_hex(16)}-{secrets.token_hex(8)}-01"


def generate_pkce() -> tuple[str, str]:
    import base64
    import hashlib
    import os

    verifier = base64.urlsafe_b64encode(os.urandom(32)).rstrip(b"=").decode("ascii")
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode("ascii")).digest()
    ).rstrip(b"=").decode("ascii")
    return verifier, challenge


def device_sign(client_id: str, timestamp: str, sign_secret: str) -> str:
    """MD5(client_id={client_id}&timestamp={timestamp}&secret={sign_secret}) — OpenAPI v1.3 §4.2"""
    import hashlib

    raw = f"client_id={client_id}&timestamp={timestamp}&secret={sign_secret}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


_RELEASE_NOISE = re.compile(
    r"[\[\(【(]?(?:2160p|1080p|720p|480p|4K|HDR|DV|HEVC|x265|H\.?264|AAC|AC3|EAC3|"
    r"WEB-?DL|BLURAY|BluRay|BDRip|REMUX|HDTV|CAM|TS|TC|DVDScr|Limited|"
    r"中字|国配|双语|特效|压制|小组|首发|独占|完结|合集|国语|粤语|简繁|"
    r"DDP5\.?1|DDP\s*5\s*1|DTS-?HD|TrueHD|Atmos|10bit|8bit|HDR10\+?|DoVi|"
    r"[A-Za-z0-9]+小组|BY-[A-Za-z0-9]+)[)\]】)]?",
    re.I,
)
_YEAR = re.compile(r"(?<!\d)(19|20)\d{2}(?!\d)")
_RES = re.compile(r"(2160p|1080p|720p|480p|4K)", re.I)

# 作品 ID 标记：(tmdb-206484) / {tmdb=206484} / [tmdb_206484] / tmdb:206484
_TMDB_TAG = re.compile(r"[\[\(\{]?\s*tmdb\s*[-_:= ]?\s*(?P<id>\d{2,12})\s*[\]\)\}]?", re.I)

# 季集线索：S01E04 / S1.E4 / 1x04 / 第04集 / EP04
_EP_PATTERNS = [
    re.compile(r"[Ss](?P<s>\d{1,2})\s*[.\-_]?\s*[Ee](?P<e>\d{1,3})(?:\s*[-~&]\s*[Ee]?\d{1,3})?", re.I),
    re.compile(r"(?<!\d)(?P<s>\d{1,2})[xX](?P<e>\d{2,3})(?!\d)"),
    re.compile(r"(?:第)?(?P<cs>\d{1,3})\s*[集期话](?!\d)"),
    re.compile(r"\b(?:EP|Episode|E)[ ._-]?(?P<e2>\d{1,3})\b", re.I),
]
_SEASON_ONLY = re.compile(r"\b(?:Season|Series|S)[ ._-]?(?P<s2>\d{1,2})\b|第(?P<cs2>\d{1,2})[季部]", re.I)
_EP_NOISE = re.compile(
    r"(?:\bS\d{1,2}[ ._-]?E\d{1,3}(?:[ ._-]?E?\d{1,3})?\b|\b\d{1,2}[xX]\d{2,3}\b|"
    r"(?:第)?\d{1,3}\s*[集期话]|\b(?:EP|Episode|E)[ ._-]?\d{1,3}\b|"
    r"\bSeason\s*\d{1,2}\b|第\d{1,2}[季部]|\bS\d{1,2}\b)", re.I)

_TV_DIR = re.compile(r"电视剧|剧集|连续剧|动漫剧|动画剧|番剧|追更|季番|\bTV\b|\bTVShows?\b|\bSeries\b|\bShows?\b", re.I)
_MOVIE_DIR = re.compile(r"电影|影片|剧场版|\bMovies?\b|\bFilms?\b", re.I)
_MOVIE_WORD = re.compile(r"剧场版|\bMovie\b|\bOVA\b|\bOAD\b", re.I)


def parse_media_name(name: str, path_hint: str = "") -> dict:
    """从文件名（可附父目录/路径）抽取识别线索。

    v0.6.0：显式输出 type_hint / clean_title / tmdb_id / type_source，把「剧集单集」
    与「电影」分开。判断顺序：季集 > 目录分类 > 剧集关键词 > 未知（交给双路搜索评分）。
    旧字段 title/year/season/episode/resolution/is_tv_hint 保持不变，调用方无需改动。
    """
    raw = (name or "").strip().replace("\\", "/").rsplit("/", 1)[-1]
    stem = re.sub(r"\.[A-Za-z0-9]{1,5}$", "", raw)
    resolution = ""
    m = _RES.search(stem)
    if m:
        resolution = m.group(1).upper()

    tmdb_id = None
    m = _TMDB_TAG.search(stem)
    if m:
        tmdb_id = int(m.group("id"))
        stem = _TMDB_TAG.sub(" ", stem)
    elif path_hint:
        m = _TMDB_TAG.search(path_hint)
        if m:
            tmdb_id = int(m.group("id"))

    year = ""
    m = re.search(r"[\[\(\{]\s*((?:19|20)\d{2})\s*[\]\)\}]", stem)
    if m:
        year = m.group(1)
    else:
        m = _YEAR.search(stem)
        if m:
            year = m.group(0)

    season = episode = None
    for pat in _EP_PATTERNS:
        m = pat.search(stem)
        if not m:
            continue
        gd = m.groupdict()
        if gd.get("s") and gd.get("e"):
            season, episode = int(gd["s"]), int(gd["e"])
        elif gd.get("e2"):
            season, episode = 1, int(gd["e2"])
        elif gd.get("cs"):
            season, episode = 1, int(gd["cs"])
        if season is not None:
            break
    if season is None:
        m = _SEASON_ONLY.search(stem)
        if m:
            gd = m.groupdict()
            season = int(gd.get("s2") or gd.get("cs2") or 1)

    clean = _RELEASE_NOISE.sub(" ", stem)
    clean = _EP_NOISE.sub(" ", clean)
    clean = re.sub(r"[._]+", " ", clean)
    clean = re.sub(r"[\(\)\[\]\{\}]", " ", clean)
    clean = re.sub(r"\s+", " ", clean).strip(" -–_")
    if year:
        clean = re.sub(rf"\s*{year}\b", " ", clean).strip(" -–_")
    clean = re.sub(r"\s+", " ", clean).strip(" -–_")

    type_hint, type_source = "unknown", "none"
    if season is not None or episode is not None:
        type_hint, type_source = "tv", "episode"
    elif _TV_DIR.search(path_hint or ""):
        type_hint, type_source = "tv", "directory"
    elif _MOVIE_WORD.search(stem):
        type_hint, type_source = "movie", "keyword"
    elif _MOVIE_DIR.search(path_hint or ""):
        type_hint, type_source = "movie", "directory"
    elif " season " in f" {clean.lower()} ":
        type_hint, type_source = "tv", "keyword"

    return {
        "title": clean or stem,
        "clean_title": clean or stem,
        "original": raw,
        "year": year,
        "season": season,
        "episode": episode,
        "resolution": resolution,
        "is_tv_hint": type_hint == "tv",
        "is_movie_hint": type_hint == "movie",
        "type_hint": type_hint,
        "type_source": type_source,
        "tmdb_id": tmdb_id,
    }


def backoff_seconds(attempts: int, base: int | None = None, cap: int | None = None) -> int:
    """第 attempts 次失败后的等待秒数，带少量抖动防止惊群。"""
    base = base or settings.queue_backoff_base
    cap = cap or settings.queue_backoff_max
    exp = min(cap, base * (2 ** max(0, attempts - 1)))
    jittered = int(exp * (0.8 + (time.time() % 1) * 0.4))
    return max(1, min(cap, jittered))


_IF_OPEN = re.compile(r"\{\{#if\s+(\w+)\}\}")
_IF_CLOSE = "{{/if}}"


def _render_blocks(text: str, ctx: dict) -> str:
    """递归处理条件块，支持嵌套；缺闭合标签时原样保留，避免吞掉模板内容。"""
    out: list[str] = []
    pos = 0
    while True:
        m = _IF_OPEN.search(text, pos)
        if not m:
            out.append(text[pos:])
            break
        out.append(text[pos:m.start()])
        depth, i, close_at = 1, m.end(), -1
        while depth > 0:
            nxt_open = _IF_OPEN.search(text, i)
            nxt_close = text.find(_IF_CLOSE, i)
            if nxt_close < 0:
                break
            if nxt_open and nxt_open.start() < nxt_close:
                depth += 1
                i = nxt_open.end()
            else:
                depth -= 1
                close_at = nxt_close
                i = nxt_close + len(_IF_CLOSE)
        if depth > 0:
            out.append(text[m.start():])
            break
        if ctx.get(m.group(1)):
            out.append(_render_blocks(text[m.end():close_at], ctx))
        pos = i
    return "".join(out)


def render_template(template: str, ctx: dict) -> str:
    """支持 {{key}} 与可嵌套的 {{#if key}}...{{/if}}；调用方负责文本 HTML 转义。"""
    text = _render_blocks(template or "", ctx)
    return re.sub(r"\{\{\s*(\w+)\s*\}\}", lambda m: str(ctx.get(m.group(1), "")), text)


class RateLimiter:
    """按接口维度的简单令牌间隔限速（OpenAPI v1.3 §10）。"""

    def __init__(self) -> None:
        self._last: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def wait(self, key: str, min_interval: float) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = self._last.get(key, 0.0) + min_interval - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last[key] = time.monotonic()


def retry_after_seconds(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return max(1, min(300, int(float(value))))
    except ValueError:
        return None


def plus_seconds(dt: datetime | None, seconds: int) -> datetime:
    base = dt or utcnow()
    return base + timedelta(seconds=seconds)
