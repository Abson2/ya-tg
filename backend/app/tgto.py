"""v0.8.0：TgtoDrive OpenAPI 关键词规则对接（独立外部系统边界）。

边界（最终版需求文档）：
  - tg-gy 不集成 TgtoDrive 源码、不部署其容器、不直接读写它的数据库。
  - 只通过 OpenAPI 读取 → 内存合并 → 回写 → 回读验证。
  - **绝不覆盖**原有关键词、管理员手动规则、其它项目规则：
    只按幂等 id `kw_<tmdb_id>_<season>` 新增或更新「来源标记为 ya-tg」的规则；
    若该 id 已存在且非本项目来源，直接返回冲突，不改任何内容。

接口结构来自本地 tgto-openapi.json 与 guangya_visual_filter.json 实证：
  GET  {base}/api/visual-filter/config?scene=ENV_FILTER_GUANGYA
       → {"success":true,"scene":...,"current":{...},"scenes":{"<scene>":{...,"rules":[...]}}}
  POST {base}/api/visual-filter/config
       body {"scene": "<scene>", 规则场景对象的全部字段（含 rules/raw_regex/...）}
  鉴权：Authorization: Bearer <OpenAPI Token>
"""
from __future__ import annotations

import re

import httpx
from sqlalchemy.orm import Session

from . import runtime_config
from .config import settings

RULE_SOURCE = "ya-tg"


class TgtoError(RuntimeError):
    pass


def _config(db: Session) -> tuple[str, str, str]:
    base = (runtime_config.get(db, "tgto_base_url") or "").rstrip("/")
    token = runtime_config.get(db, "tgto_api_key") or ""
    scene = runtime_config.get(db, "tgto_scene") or "ENV_FILTER_GUANGYA"
    if not base:
        raise TgtoError("尚未配置 TgtoDrive 地址，请先在互动设置里填写")
    if not token:
        raise TgtoError("尚未配置 TgtoDrive OpenAPI Token")
    return base, scene, token


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json",
            "Accept": "application/json"}


def _scene_obj(data: dict, scene: str) -> dict | None:
    scenes = data.get("scenes")
    if isinstance(scenes, dict) and isinstance(scenes.get(scene), dict):
        return scenes[scene]
    cur = data.get("current")
    if isinstance(cur, dict) and (not cur.get("key") or cur.get("key") == scene):
        return cur
    return None


def _rules_of(scene: dict) -> list[dict]:
    rules = scene.get("rules")
    return rules if isinstance(rules, list) else []


def keyword_id(tmdb_id: int | None, season: int | None, title: str = "") -> str:
    if tmdb_id:
        return f"kw_{int(tmdb_id)}_{int(season or 1)}"
    # 无 TMDB ID 时用标题做稳定后缀，避免与其它项目规则撞 id
    slug = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff]+", "", title)[:24] or "custom"
    return f"kw_ya-tg_{slug}"


def build_rule(*, title: str, tmdb_id: int | None = None, media_type: str = "tv",
               season: int | None = None, year: str = "", resolution: str = "") -> dict:
    """构造一条结构化关键词规则，字段对齐 TgtoDrive 现有规则，来源标记为 ya-tg。"""
    display = title.strip()
    if season and media_type == "tv":
        display = f"{title.strip()} S{int(season)}"
    return {
        "id": keyword_id(tmdb_id, season, title),
        "aliases": [display],
        "media_name": display,
        "preferred_name": display,
        "title": display,
        "tmdb_id": str(tmdb_id) if tmdb_id else "",
        "media_type": media_type or "tv",
        "year": str(year or ""),
        "keywords": [display],
        "keyword_groups_any": [[display]],
        "exclude_keywords": [],
        "required_keyword": "",
        "include_required_keyword": False,
        "include_year": bool(year),
        "effect": "all",
        "resolution": resolution or "",
        "rule_type": "structured",
        "source": RULE_SOURCE,
        "sharer": "",
        "custom_regex": "",
        "original_title": "",
        "overview": "",
        "poster_path": "",
        "poster_url": "",
        "backdrop_path": "",
        "backdrop_url": "",
    }


async def fetch_config(db: Session) -> tuple[dict, str]:
    """GET 全量配置。返回 (原始 JSON, scene)；原样保留，供回写时不动其它字段。"""
    base, scene, token = _config(db)
    async with httpx.AsyncClient(timeout=settings.guangya_timeout) as client:
        r = await client.get(f"{base}/api/visual-filter/config",
                             params={"scene": scene}, headers=_headers(token))
    if r.status_code == 401:
        raise TgtoError("TgtoDrive OpenAPI Token 无效（401）")
    if r.status_code >= 400:
        raise TgtoError(f"读取 TgtoDrive 配置失败 HTTP {r.status_code}：{r.text[:200]}")
    try:
        data = r.json()
    except Exception:
        raise TgtoError("TgtoDrive 返回的不是 JSON，已中止写入")
    if not isinstance(data, dict):
        raise TgtoError("TgtoDrive 配置结构异常，已中止写入")
    return data, scene


async def save_config(db: Session, data: dict, scene: str, scene_obj: dict) -> None:
    """POST 回写：只替换目标场景对象，请求体其余字段沿用 GET 原样。"""
    base, _, token = _config(db)
    body = {"scene": scene}
    body.update({k: v for k, v in scene_obj.items()})
    async with httpx.AsyncClient(timeout=settings.guangya_timeout) as client:
        r = await client.post(f"{base}/api/visual-filter/config",
                              json=body, headers=_headers(token))
    if r.status_code == 401:
        raise TgtoError("TgtoDrive OpenAPI Token 无效（401）")
    if r.status_code >= 400:
        raise TgtoError(f"写回 TgtoDrive 配置失败 HTTP {r.status_code}：{r.text[:200]}")


async def upsert_keyword(db: Session, *, title: str, tmdb_id: int | None = None,
                         media_type: str = "tv", season: int | None = None,
                         year: str = "", resolution: str = "") -> dict:
    """合并一条「我想看」关键词，并回读验证。任何异常都不改动其它规则。"""
    data, scene = await fetch_config(db)
    obj = _scene_obj(data, scene)
    if obj is None:
        raise TgtoError(f"TgtoDrive 未返回场景 {scene} 的配置，已中止写入")
    rules = _rules_of(obj)
    before = len(rules)
    rule = build_rule(title=title, tmdb_id=tmdb_id, media_type=media_type,
                      season=season, year=year, resolution=resolution)
    rid = rule["id"]
    hit = next((x for x in rules if isinstance(x, dict) and str(x.get("id")) == rid), None)
    if hit is not None:
        if str(hit.get("source") or "") != RULE_SOURCE:
            return {"ok": False, "status": "conflict", "rule_id": rid,
                    "message": "该规则 id 已存在且不是本项目创建，已保留原规则不做覆盖"}
        # 只更新本项目自己创建的规则，且仅覆盖关键词相关字段
        for k in ("keywords", "keyword_groups_any", "aliases", "media_name",
                  "preferred_name", "title", "tmdb_id", "media_type", "year",
                  "resolution", "source"):
            hit[k] = rule[k]
        action = "updated"
    else:
        rules.append(rule)
        action = "created"
    obj["rules"] = rules
    # 同步 current 与 scenes 中同一场景，避免回写时其它字段不一致
    if isinstance(data.get("current"), dict) and (
            not data["current"].get("key") or data["current"].get("key") == scene):
        data["current"]["rules"] = rules
    await save_config(db, data, scene, obj)
    # 回读验证：确认规则真的落库，且其它规则数量没有减少
    again, scene2 = await fetch_config(db)
    obj2 = _scene_obj(again, scene2) or {}
    after_rules = _rules_of(obj2)
    present = any(isinstance(x, dict) and str(x.get("id")) == rid for x in after_rules)
    if not present:
        raise TgtoError("回写后回读未发现新规则，关键词未生效，请检查 TgtoDrive 配置")
    if len(after_rules) < before:
        raise TgtoError("回写后规则数量异常减少，已中止（可能覆盖失败）")
    return {"ok": True, "status": action, "rule_id": rid,
            "rules_before": before, "rules_after": len(after_rules)}


async def test_connection(db: Session) -> dict:
    data, scene = await fetch_config(db)
    obj = _scene_obj(data, scene)
    if obj is None:
        raise TgtoError(f"未找到场景 {scene}，请检查场景名")
    return {"ok": True, "scene": scene, "rules": len(_rules_of(obj)),
            "message": "TgtoDrive OpenAPI 连接正常，仅读取未写入"}
