"""OpenAI 兼容的 AI 辅助识别（可选功能）。

设计借鉴 TgtoDrive 的「AI 回退识别」：在「TMDB 设置」页配置任意 OpenAI 兼容服务
（OpenAI / DeepSeek / Ollama / 各类中转），用户可自定义 base_url、model 与附加提示词。

AI 只做三件事：① 解析文件名得到规范片名与类型 ② 主搜索失败时提供备用片名
③ 电影/剧集候选接近时消歧。**任何 AI 异常都静默降级**到本地启发式 + TMDB，
绝不阻断扫描、建分享、推送主链路。
"""
from __future__ import annotations

import json

import httpx
from sqlalchemy.orm import Session

from . import runtime_config


class AiError(RuntimeError):
    """AI 配置缺失或调用失败，消息可直接展示给用户。"""


DEFAULT_SYSTEM = (
    "你是影视文件名解析助手。只输出一个 JSON 对象，不要解释、不要 Markdown。"
    '格式：{"title":"规范化作品名","year":"四位年份或空字符串","type":"movie|tv|unknown",'
    '"season":整数或null,"episode":整数或null,"confidence":0到1的小数,"reason":"简短中文理由"}。'
    "判定规则：出现 S01E04、S1E4、第4集、EP04、Season 2 等季集信息时 type 必须是 tv；"
    "出现剧场版、Movie、OVA 且没有季集信息时 type 为 movie；其他情况输出 unknown。"
    "title 必须去掉分辨率、编码、音轨、压制组、季集号、年份等噪声，只保留作品本名，不要带集号。"
)
DISAMBIGUATE_SYSTEM = (
    "你是影视元数据匹配助手。根据文件名从候选里选最匹配的一条。只输出一个 JSON 对象："
    '{"index":候选序号整数,"reason":"简短中文理由"}；如果都无法确定，输出 {"index":null,"reason":"原因"}。'
    "注意：文件名带季集信息时只应选 tv 类型候选。"
)
_TRUE = {"1", "true", "yes", "on", "enabled"}


def _cfg(db: Session | None) -> dict:
    def g(key: str) -> str:
        return (runtime_config.get(db, key) if db is not None else
                str(getattr(runtime_config.settings, key, ""))).strip()

    base = g("ai_base_url") or "https://api.openai.com/v1"
    timeout_raw = g("ai_timeout") or "30"
    try:
        timeout = max(5.0, min(120.0, float(timeout_raw)))
    except ValueError:
        timeout = 30.0
    return {
        "enabled": g("ai_enabled").lower() in _TRUE,
        "base_url": base,
        "api_key": g("ai_api_key"),
        "model": g("ai_model") or "gpt-4o-mini",
        "custom_prompt": g("ai_prompt"),
        "timeout": timeout,
    }


def enabled(db: Session | None) -> bool:
    c = _cfg(db)
    return bool(c["enabled"] and c["api_key"])


def _mask(value: str) -> str:
    if len(value) <= 8:
        return "****"
    return value[:4] + "…" + value[-4:]


def status(db: Session | None) -> dict:
    c = _cfg(db)
    return {
        "enabled": c["enabled"],
        "configured": bool(c["api_key"]),
        "active": enabled(db),
        "base_url": c["base_url"],
        "model": c["model"],
        "masked": _mask(c["api_key"]) if c["api_key"] else "",
        "timeout": c["timeout"],
        "custom_prompt": c["custom_prompt"],
    }


async def _chat(messages: list[dict], db: Session | None, max_tokens: int = 300) -> str:
    c = _cfg(db)
    if not c["api_key"]:
        raise AiError("尚未配置 AI API Key，请到「TMDB 设置 → AI 辅助识别」填写")
    url = c["base_url"].rstrip("/")
    if not url.endswith("/chat/completions"):
        url += "/chat/completions"
    payload = {"model": c["model"], "messages": messages,
               "temperature": 0.1, "max_tokens": max_tokens}
    headers = {"Authorization": f"Bearer {c['api_key']}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=c["timeout"]) as client:
        r = await client.post(url, json=payload, headers=headers)
    if r.status_code in (401, 403):
        raise AiError(f"AI 接口鉴权失败（HTTP {r.status_code}）：请检查 API Key 与 base_url")
    if r.status_code >= 400:
        raise AiError(f"AI 接口失败 HTTP {r.status_code}：{r.text[:200]}")
    try:
        data = r.json()
    except Exception:
        raise AiError("AI 接口返回的不是 JSON，已降级到本地识别")
    choices = data.get("choices") or []
    if not choices:
        raise AiError(f"AI 接口未返回 choices：{str(data)[:200]}")
    return str((choices[0].get("message") or {}).get("content") or "")


def _extract_json(text: str) -> dict:
    text = (text or "").strip()
    fenced = text.find("```")
    if fenced >= 0:
        end = text.find("```", fenced + 3)
        if end > 0:
            inner = text[fenced + 3:end].strip()
            if inner.lower().startswith("json"):
                inner = inner[4:].strip()
            text = inner
    start = text.find("{")
    if start < 0:
        raise AiError("AI 未返回 JSON 结果，已降级到本地识别")
    try:
        obj, _ = json.JSONDecoder().raw_decode(text[start:])
    except Exception:
        raise AiError("AI 返回内容不是合法 JSON，已降级到本地识别")
    return obj if isinstance(obj, dict) else {}


async def parse_filename(name: str, db: Session | None = None) -> dict:
    """让 AI 从文件名推断作品名/类型/年份。失败抛 AiError，由 tmdb.identify 吞掉降级。"""
    c = _cfg(db)
    user = f"文件名：{name}\n"
    if c["custom_prompt"]:
        user += f"补充要求：{c['custom_prompt']}\n"
    content = await _chat([{"role": "system", "content": DEFAULT_SYSTEM},
                           {"role": "user", "content": user}], db)
    d = _extract_json(content)
    media_type = str(d.get("type") or "unknown").lower()
    if media_type not in ("movie", "tv", "unknown"):
        media_type = "unknown"
    year = str(d.get("year") or "").strip()
    if not (len(year) == 4 and year.isdigit()):
        year = ""
    try:
        confidence = max(0.0, min(1.0, float(d.get("confidence") or 0.5)))
    except (TypeError, ValueError):
        confidence = 0.5
    def _int_or_none(key):
        v = d.get(key)
        try:
            return int(v) if v is not None else None
        except (TypeError, ValueError):
            return None
    return {
        "ok": True,
        "title": str(d.get("title") or "").strip(),
        "year": year,
        "type": media_type,
        "season": _int_or_none("season"),
        "episode": _int_or_none("episode"),
        "confidence": confidence,
        "reason": str(d.get("reason") or "")[:200],
    }


async def disambiguate(name: str, candidates: list[dict], db: Session | None = None) -> dict | None:
    """候选接近时让 AI 选一条；返回 {tmdb_id, media_type, reason} 或 None。"""
    if not candidates:
        return None
    brief = []
    for i, it in enumerate(candidates[:8], 1):
        brief.append({
            "index": i,
            "tmdb_id": it.get("tmdb_id"),
            "type": it.get("media_type"),
            "title": it.get("title") or "",
            "original_title": it.get("original_title") or "",
            "year": it.get("year") or "",
            "overview": str(it.get("overview") or "")[:80],
        })
    content = await _chat([
        {"role": "system", "content": DISAMBIGUATE_SYSTEM},
        {"role": "user", "content": f"文件名：{name}\n候选列表：{json.dumps(brief, ensure_ascii=False)}"},
    ], db, max_tokens=180)
    d = _extract_json(content)
    idx = d.get("index")
    if isinstance(idx, str) and idx.isdigit():
        idx = int(idx)
    if not isinstance(idx, int) or idx < 1 or idx > len(brief):
        return None
    chosen = candidates[idx - 1]
    return {"tmdb_id": chosen.get("tmdb_id"), "media_type": chosen.get("media_type"),
            "reason": str(d.get("reason") or "")[:200]}


async def test_connection(db: Session | None) -> dict:
    """测试 AI 接口连通性，不抛异常。"""
    st = status(db)
    if not st["enabled"]:
        return {"ok": False, "message": "AI 辅助识别未启用（打开开关后才生效）", **st}
    if not st["configured"]:
        return {"ok": False, "message": "尚未配置 AI API Key", **st}
    try:
        content = await _chat([{"role": "user", "content": '只回复 JSON：{"ok":true}'}], db,
                              max_tokens=40)
        return {"ok": True, "message": "连接成功，模型可用", "reply": content[:80], **st}
    except Exception as exc:
        return {"ok": False, "message": str(exc), **st}
