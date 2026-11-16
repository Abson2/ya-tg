import asyncio
import re
import secrets
import time
from datetime import datetime, timedelta, timezone

from .config import settings


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


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
    r"[A-Za-z0-9]+小组|BY-[A-Za-z0-9]+)[)\]】)]?",
    re.I,
)
_SEason_EP = re.compile(r"[Ss](?P<s>\d{1,2})\s*[Ee](?P<e>\d{1,3})|(?:第)?(?P<cs>\d{1,2})[集期]")
_YEAR = re.compile(r"(?<!\d)(19|20)\d{2}(?!\d)")
_RES = re.compile(r"(2160p|1080p|720p|480p|4K)", re.I)


def parse_media_name(name: str) -> dict:
    """从文件名抽取识别线索。只做启发式，最终以 TMDB 结果/人工确认为准。"""
    stem = re.sub(r"\.[A-Za-z0-9]{1,5}$", "", (name or "").strip())
    resolution = ""
    m = _RES.search(stem)
    if m:
        resolution = m.group(1).upper()
    year = ""
    m = _YEAR.search(stem)
    if m:
        year = m.group(0)
    season = episode = None
    m = _SEason_EP.search(stem)
    if m:
        if m.group("s"):
            season, episode = int(m.group("s")), int(m.group("e"))
        elif m.group("cs"):
            season, episode = 1, int(m.group("cs"))
    clean = _RELEASE_NOISE.sub(" ", stem)
    clean = re.sub(r"[._]+", " ", clean)
    clean = re.sub(r"\s+", " ", clean).strip(" -")
    if year:
        clean = re.sub(rf"\s*{year}\b", " ", clean).strip(" -")
    return {
        "title": clean or stem,
        "year": year,
        "season": season,
        "episode": episode,
        "resolution": resolution,
        "is_tv_hint": season is not None or " season " in f" {clean.lower()} ",
    }


def backoff_seconds(attempts: int, base: int | None = None, cap: int | None = None) -> int:
    """第 attempts 次失败后的等待秒数，带少量抖动防止惊群。"""
    base = base or settings.queue_backoff_base
    cap = cap or settings.queue_backoff_max
    exp = min(cap, base * (2 ** max(0, attempts - 1)))
    jittered = int(exp * (0.8 + (time.time() % 1) * 0.4))
    return max(1, min(cap, jittered))


def render_template(template: str, ctx: dict) -> str:
    """支持 {{key}} 与 {{#if key}}...{{/if}}；调用方负责文本 HTML 转义。"""
    text = template or ""

    def _if(m: re.Match) -> str:
        key, body = m.group(1), m.group(2)
        value = ctx.get(key)
        return body if value else ""

    text = re.sub(r"\{\{#if\s+(\w+)\}\}(.*?)\{\{/if\}\}", _if, text, flags=re.S)
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
