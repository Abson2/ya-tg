"""v0.9.18：光鸭分享链接强校验（独立模块，避免 bot_listener / dispatch 循环依赖）。

仅接受 guangyapan.com 的 /s/ 分享链接；其它链接一律不进入投稿后台。
"""
from __future__ import annotations

import re

# https://www.guangyapan.com/s/<code>（允许 www./m./裸域，允许尾部查询串）
_GY_SHARE_RE = re.compile(
    r"https?://(?:www\.|m\.)?guangyapan\.com/s/[A-Za-z0-9]+(?:/?\?[^\s]*)?",
    re.I,
)
_ANY_URL_RE = re.compile(r"https?://[^\s]+", re.I)

GY_HINT = "仅接受 guangyapan.com 的 /s/ 分享链接（如 https://www.guangyapan.com/s/xxxx）"


def is_guangya_share_url(url: str) -> bool:
    return bool(_GY_SHARE_RE.fullmatch(str(url or "").strip()))


def extract_guangya_share_url(text: str) -> tuple[str, str]:
    """从文本提取光鸭分享链接，返回 (share_url, error)。

    - 命中合法光鸭 /s/ 链接 → (最后一个, "")
    - 完全没有 http(s) 链接 → ("", "")  （视作「只给了剧名」，走候选流程）
    - 有链接但都不是光鸭 → ("", 非光鸭提示)
    """
    raw = str(text or "").strip()
    if not raw:
        return "", ""
    gy = _GY_SHARE_RE.findall(raw)
    if gy:
        return gy[-1].split("#")[0], ""
    if _ANY_URL_RE.findall(raw):
        return "", f"非光鸭分享链接：{GY_HINT}"
    return "", ""


def share_code(url: str) -> str:
    m = re.search(r"/s/([A-Za-z0-9]+)", str(url or ""), re.I)
    return m.group(1) if m else ""
