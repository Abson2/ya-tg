"""v0.9.18：光鸭分享链接强校验（独立模块，避免 bot_listener / dispatch 循环依赖）。

仅接受 guangyapan.com 的 /s/ 分享链接；其它链接一律不进入投稿后台。
"""
from __future__ import annotations

import re

# https://www.guangyapan.com/s/<shareId>[_后缀]（允许 www./m./裸域，允许尾部查询串）
# v0.9.19：真实分享链接形如 /s/1970558251383148604_aey8CVbCzY9joZbY，
# shareId 含下划线后缀；旧正则只认纯字母数字会把真实链接误判为非法。
_GY_SHARE_RE = re.compile(
    r"https?://(?:www\.|m\.)?guangyapan\.com/s/[A-Za-z0-9_]+(?:/?\?[^\s]*)?",
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
    """提取真实提取码（?code=xxx）。/s/ 后的串是 shareId，不是提取码。"""
    try:
        from urllib.parse import parse_qs, urlsplit
        return (parse_qs(urlsplit(str(url or "").strip()).query).get("code") or [""])[0]
    except Exception:
        return ""


def parse_share_reference(url: str) -> tuple[str, str]:
    """把光鸭分享链接解析为 (shareId, code)。

    契约来自光鸭技能 protocol.mjs::parseShareReference：
      /userres/v1/get_share_access_token 需要 {shareId, code}
      shareId = URL path 中 /s/ 后的整段（含下划线后缀，如 1970558251383148604_aey8...）
      code    = ?code= 查询参数（无则留空，公开分享可能不需要提取码）
    返回 ("", "") 表示不是可解析的光鸭 /s/ 链接。
    """
    raw = str(url or "").strip()
    m = re.search(r"/s/([A-Za-z0-9_]+)", raw, re.I)
    if not m:
        return "", ""
    return m.group(1), share_code(raw)
