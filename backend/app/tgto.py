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
import time

import httpx
from sqlalchemy.orm import Session

from . import runtime_config
from .config import settings

RULE_SOURCE = "ya-tg"

# v2.1.14：401 文案改成「可操作的排查指引」，不再只丢一句「Token 无效」。
# 真机根因（2026-11-23 两容器交叉核对）：不是 Token 被撤销，而是**错配**——
# 本机保存的 Token 与 TgtoDrive 端当前有效的凭据不是同一个值（前缀与 sha256 均不同），
# 服务端对「不存在的凭据」和「随便编的假 Token」返回的是同一个 401。
# 用户侧看到的现象是「TgtoDrive 里凭据明明有效，却一直 401」。
TOKEN_401_HINT = (
    "TgtoDrive OpenAPI Token 无效（401）：本机保存的 Token 与 TgtoDrive 端当前有效的"
    "凭据不一致（凭据在 TgtoDrive 端不存在，常见于对方重新生成过 Token）。"
    "请到 TgtoDrive「实用工具 → OpenAPI」复制该凭据的**最新完整 Token**，"
    "回填到本页 TgtoDrive 配置并保存，再点「测试连接」验证；"
    "若刚升级过本系统，请同时确认升级没有覆盖已保存的配置。"
    # v2.1.16：配置页已改为凭证零回显。这里提前说明「存完就空」，否则用户看到空（或
    # 旧版本的星号）会误以为没存上再存一次——那正是掩码被当凭证写库、越存越 401 的成因。
    "另：保存成功后输入框会重新变空属正常，凭证不回显；不要在看不到 Token 时重复保存。"
)

# 阶段五 B-07：求片白名单采纳时 _tmdb_meta 每次都请求 TMDB（走 SCAN 限流），同一
# 作品被重复采纳/刷新会重复消耗配额。加进程内 TTL 缓存，只覆盖这一条链路，不动
# 识别/英文名解析的 TMDB 调用。失败/未配置结果不缓存，避免暂时故障被固化。
_TMDB_META_TTL = 24 * 3600
_TMDB_META_MAX = 500
_tmdb_meta_cache: dict[str, tuple[float, dict]] = {}


def clear_tmdb_meta_cache() -> None:
    """TMDB 配置变更时调用，让新语言/密钥立即生效。"""
    _tmdb_meta_cache.clear()


def _tmdb_meta_cache_get(key: str) -> dict | None:
    hit = _tmdb_meta_cache.get(key)
    if hit and time.time() - hit[0] < _TMDB_META_TTL:
        return dict(hit[1])
    if hit:
        _tmdb_meta_cache.pop(key, None)
    return None


def _tmdb_meta_cache_put(key: str, data: dict) -> None:
    if len(_tmdb_meta_cache) >= _TMDB_META_MAX:
        # 删最早的 1/5，避免每次到上限都 O(n) 全清理
        oldest = sorted(_tmdb_meta_cache, key=lambda k: _tmdb_meta_cache[k][0])
        for k in oldest[:max(1, _TMDB_META_MAX // 5)]:
            _tmdb_meta_cache.pop(k, None)
    _tmdb_meta_cache[key] = (time.time(), dict(data))


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


# v0.9.13 #254：下拉值 → TgtoDrive 真机字段值。真机样本(gugangya_visual_filter.json)
# 里 resolution 取 "4k"/"all"、effect 取 "all"，其余为页面下拉候选；统一小写英文串。
_RESOLUTION_MAP = {
    "4k": "4k", "2160p": "4k", "1080p": "1080p", "720p": "720p",
    "all": "all", "全部": "all", "": "all",
}
_EFFECT_MAP = {
    "all": "all", "全部效果": "all", "": "all",
    "dolby": "dolby", "dolby vision": "dolby", "杜比视界": "dolby", "杜比": "dolby",
    "hdr": "hdr", "hdr10": "hdr",
    "sdr": "sdr",
}


def norm_resolution(value: str) -> str:
    v = str(value or "").strip().lower()
    return _RESOLUTION_MAP.get(v, v or "all")


def norm_effect(value: str) -> str:
    v = str(value or "").strip().lower()
    return _EFFECT_MAP.get(v, v or "all")


def keyword_id(tmdb_id: int | None, season: int | None, title: str = "") -> str:
    if tmdb_id:
        return f"kw_{int(tmdb_id)}_{int(season or 1)}"
    # 无 TMDB ID 时用标题做稳定后缀，避免与其它项目规则撞 id
    slug = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff]+", "", title)[:24] or "custom"
    return f"kw_ya-tg_{slug}"


def build_rule(*, title: str, tmdb_id: int | None = None, media_type: str = "tv",
               season: int | None = None, year: str = "", resolution: str = "",
               effect: str = "", required_keyword: str = "",
               original_title: str = "", overview: str = "",
               poster_path: str = "", backdrop_path: str = "",
               poster_url: str = "", backdrop_url: str = "") -> dict:
    """构造一条结构化关键词规则，字段对齐 TgtoDrive 现有规则，来源标记为 ya-tg。

    v0.9.13 #254：新增 effect / required_keyword 两个真机字段。必含关键词必须同时
    写值（required_keyword）并打开开关（include_required_keyword），否则后端不编译进正则。
    v0.9.15 #268：新增 TMDB 元数据字段（海报/背景/简介/原名），由 upsert_keyword 在
    写入前查 tmdb.detail() 补全；TgtoDrive 前端「正在匹配 TMDB」取决于这些字段是否为空。
    默认值全空串，保持旧的直接调用（如测试）完全兼容。
    """
    display = title.strip()
    if season and media_type == "tv":
        display = f"{title.strip()} S{int(season)}"
    req = str(required_keyword or "").strip()
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
        "required_keyword": req,
        "include_required_keyword": bool(req),
        "include_year": bool(year),
        "effect": norm_effect(effect),
        "resolution": norm_resolution(resolution) if resolution else "",
        "rule_type": "structured",
        "source": RULE_SOURCE,
        "sharer": "",
        "custom_regex": "",
        "original_title": str(original_title or ""),
        "overview": str(overview or ""),
        "poster_path": str(poster_path or ""),
        "poster_url": str(poster_url or ""),
        "backdrop_path": str(backdrop_path or ""),
        "backdrop_url": str(backdrop_url or ""),
    }


async def _tmdb_meta(db: Session, tmdb_id: int | None, media_type: str) -> dict:
    """v0.9.15 #268：查 TMDB 详情补全规则元数据，供 TgtoDrive 前端「已匹配 TMDB」。

    走 SCAN 场景限流（tmdb.detail 默认 SCAN）。任何失败都安全降级为空串，
    绝不因为元数据缺失阻断求片审核把关键词写入白名单。
    """
    empty = {"original_title": "", "overview": "", "poster_path": "",
             "poster_url": "", "backdrop_path": "", "backdrop_url": "", "year": ""}
    if not tmdb_id:
        return empty
    # B-07：命中缓存直接返回（不消耗 TMDB 限流配额、不发网络）。配置变更时由
    # clear_tmdb_meta_cache() 清空，语言/密钥变化立即生效。
    cache_key = f"{int(tmdb_id)}:{(media_type or 'tv').lower()}"
    cached = _tmdb_meta_cache_get(cache_key)
    if cached is not None:
        return dict(cached)
    try:
        from . import tmdb
        if not tmdb.configured(db):  # 未配密钥直接降级，不消耗限流配额也不发网络请求
            return empty
        d = await tmdb.detail(int(tmdb_id), media_type or "tv", db)
    except Exception:
        return empty
    poster = str(d.get("poster_path") or "")
    backdrop = str(d.get("backdrop_path") or "")
    result = {
        "original_title": str(d.get("original_title") or ""),
        "overview": str(d.get("overview") or ""),
        "poster_path": poster,
        "poster_url": tmdb.poster_url(poster, "w500") if poster else "",
        "backdrop_path": backdrop,
        "backdrop_url": tmdb.poster_url(backdrop, "w780") if backdrop else "",
        # v0.9.17 #280：命令式 /求片 不带年份，从 TMDB 详情补 year，供 build_rule 写
        # include_year / year；取不到仍为空串，安全降级不阻断写入。
        "year": str(d.get("year") or ""),
    }
    # B-07：只缓存成功取到数据的结果；全空结果不缓存（可能是 TMDB 临时抖动，
    # 不应固化 24h），下次采纳还可重试补全海报/简介。
    if any(result.values()):
        _tmdb_meta_cache_put(cache_key, result)
    return result


async def fetch_config(db: Session) -> tuple[dict, str]:
    """GET 全量配置。返回 (原始 JSON, scene)；原样保留，供回写时不动其它字段。"""
    base, scene, token = _config(db)
    async with httpx.AsyncClient(timeout=settings.guangya_timeout) as client:
        r = await client.get(f"{base}/api/visual-filter/config",
                             params={"scene": scene}, headers=_headers(token))
    if r.status_code == 401:
        raise TgtoError(TOKEN_401_HINT)
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
        raise TgtoError(TOKEN_401_HINT)
    if r.status_code >= 400:
        raise TgtoError(f"写回 TgtoDrive 配置失败 HTTP {r.status_code}：{r.text[:200]}")


async def upsert_keyword(db: Session, *, title: str, tmdb_id: int | None = None,
                         media_type: str = "tv", season: int | None = None,
                         year: str = "", resolution: str = "", effect: str = "",
                         required_keyword: str = "") -> dict:
    """合并一条「我想看」关键词，并回读验证。任何异常都不改动其它规则。

    v0.9.13 #254：a/b/c 三字段未显式传入时从管理页配置读取（审核通过路径调用时
    只传影视信息，字段由这里统一注入），保证前端配置真正生效。
    """
    if not resolution:
        resolution = runtime_config.get(db, "tgto_rule_resolution") or "4k"
    if not effect:
        effect = runtime_config.get(db, "tgto_rule_effect") or "all"
    if not required_keyword:
        required_keyword = runtime_config.get(db, "tgto_rule_required_keyword") or ""
    data, scene = await fetch_config(db)
    obj = _scene_obj(data, scene)
    if obj is None:
        raise TgtoError(f"TgtoDrive 未返回场景 {scene} 的配置，已中止写入")
    rules = _rules_of(obj)
    before = len(rules)
    # v0.9.15 #268：先查 TMDB 详情补全海报/简介/原名，TgtoDrive 前端才显示「已匹配」
    meta = await _tmdb_meta(db, tmdb_id, media_type)
    # v0.9.17 #280：命令式 /求片 登记时 year 为空；用 TMDB 详情补上，确保 include_year
    # 与 year 真机字段有值（用户反馈"参数没传进 TgtoDrive"的主因之一）。
    resolved_year = str(year or "").strip() or str(meta.get("year") or "").strip()
    rule = build_rule(title=title, tmdb_id=tmdb_id, media_type=media_type,
                      season=season, year=resolved_year, resolution=resolution,
                      effect=effect, required_keyword=required_keyword,
                      original_title=meta.get("original_title", ""),
                      overview=meta.get("overview", ""),
                      poster_path=meta.get("poster_path", ""),
                      poster_url=meta.get("poster_url", ""),
                      backdrop_path=meta.get("backdrop_path", ""),
                      backdrop_url=meta.get("backdrop_url", ""))
    rid = rule["id"]
    hit = next((x for x in rules if isinstance(x, dict) and str(x.get("id")) == rid), None)
    # 诊断快照：审核回执里带回本次"真正写入"的参数，杜绝"前端说成功、用户以为没传"的黑洞
    applied = {"title": title, "tmdb_id": tmdb_id, "media_type": media_type,
               "season": season, "year": resolved_year, "resolution": resolution,
               "effect": effect, "required_keyword": required_keyword}
    if hit is not None:
        if str(hit.get("source") or "") != RULE_SOURCE:
            return {"ok": False, "status": "conflict", "rule_id": rid,
                    "message": "该规则 id 已存在且不是本项目创建，已保留原规则不做覆盖",
                    "detail": f"TgtoDrive 现有规则 source={hit.get('source') or '空'}，id={rid}；为防覆盖他人规则未写入。请确认是否同一作品，或换用其它 TMDB ID/季。",
                    "applied": applied}
        # 只更新本项目自己创建的规则，且仅覆盖关键词相关字段
        for k in ("keywords", "keyword_groups_any", "aliases", "media_name",
                  "preferred_name", "title", "tmdb_id", "media_type", "year",
                  "resolution", "effect", "required_keyword",
                  "include_required_keyword", "include_year", "source",
                  "original_title", "overview", "poster_path", "poster_url",
                  "backdrop_path", "backdrop_url"):
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
            "rules_before": before, "rules_after": len(after_rules),
            "applied": applied}


async def remove_keyword(db: Session, *, tmdb_id: int | None,
                         season: int | None = None, title: str = "") -> dict:
    """v0.9.13 #254：删除求片时联动移除本项目写入的白名单规则（增量、精准）。

    安全护栏（对齐 embyhub remove_rules 的实测经验，绝不误删他人规则）：
      1. 只匹配幂等 id kw_<tmdb>_<season> 且 source==ya-tg 的规则；
      2. GET 失败 / 结构未知 / 场景缺失 → 中止，绝不 POST（避免写出空规则列表）；
      3. 未命中 → 零净改动，不 POST；
      4. 同步 current 副本，保留所有其它规则原顺序。
    """
    rid = keyword_id(tmdb_id, season, title)
    data, scene = await fetch_config(db)
    obj = _scene_obj(data, scene)
    if obj is None:
        return {"ok": False, "changed": False, "message": "场景配置缺失，已中止删除"}
    rules = _rules_of(obj)
    kept, removed = [], []
    for r in rules:
        is_target = (isinstance(r, dict) and str(r.get("id")) == rid
                     and str(r.get("source") or "") == RULE_SOURCE)
        if is_target:
            removed.append(rid)
        else:
            kept.append(r)
    if not removed:
        return {"ok": True, "changed": False, "removed": [], "rule_id": rid,
                "message": "未找到本项目规则，无需删除"}
    obj["rules"] = kept
    if isinstance(data.get("current"), dict) and (
            not data["current"].get("key") or data["current"].get("key") == scene):
        data["current"]["rules"] = kept
    await save_config(db, data, scene, obj)
    return {"ok": True, "changed": True, "removed": removed, "rule_id": rid,
            "kept_count": len(kept)}


async def test_connection(db: Session) -> dict:
    data, scene = await fetch_config(db)
    obj = _scene_obj(data, scene)
    if obj is None:
        raise TgtoError(f"未找到场景 {scene}，请检查场景名")
    return {"ok": True, "scene": scene, "rules": len(_rules_of(obj)),
            "message": "TgtoDrive OpenAPI 连接正常，仅读取未写入"}
