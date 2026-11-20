# Ya-TG ↔ TgtoDrive 交互联动功能说明（复用版）

> 本文自包含，供加入资料库后独立复用。基于 Ya-TG v0.9.15 源码整理，2026-11-20。
> 覆盖：对接边界、OpenAPI 契约、幂等规则、字段映射、两条联动链路、安全护栏、配置项、代码地图、真机状态。

---

## 一、对接边界（先定边界，再谈实现）

TgtoDrive 是**独立外部系统**。Ya-TG 的边界是：

- 不集成 TgtoDrive 源码、不部署它的容器、不读写它的数据库。
- 只通过它暴露的 **OpenAPI** 做「读取 → 内存合并 → 回写 → 回读验证」。
- 绝不覆盖对方原有关键词、管理员手动规则、其它项目规则。

> 来源：`backend/app/tgto.py` 模块 docstring；`docs/配置说明.md` 第三节。
> 参考案例：embyhub 仓库插件中心的 tgto 关键词联动（场景 key 与字段契约与之对齐）。

**"光鸭频道关键词监控白名单"的真实所指**：即 TgtoDrive 的 `visual-filter` 场景规则，不是光鸭网盘自身的功能。场景 key 为 `ENV_FILTER_GUANGYA`。这是最容易踩的概念坑。

---

## 二、OpenAPI 契约

### 端点

```http
GET  {base}/api/visual-filter/config?scene=ENV_FILTER_GUANGYA
POST {base}/api/visual-filter/config
```

- 鉴权：`Authorization: Bearer <OpenAPI Token>`，另带 `Content-Type/Accept: application/json`。
- 超时：复用 `settings.guangya_timeout`。
- GET 响应结构：
  ```json
  {"success": true, "scene": "...", "current": {...},
   "scenes": {"ENV_FILTER_GUANGYA": {..., "rules": [...]}}}
  ```
- POST body：`{"scene": "<scene>", ...目标场景对象的全部字段}`（含 `rules`/`raw_regex` 等，其余字段沿用 GET 原样，不动）。

### 关键约束

该接口**没有单条增删**，只能全量覆盖。因此写入必须走固定四步：

```
1) GET 全量 → 2) 内存只合并目标规则 → 3) POST 回写 → 4) GET 回读校验
```

契约来源：`tgto-openapi.json`、`guangya_visual_filter.json`（真机样本），以及 `backend/app/tgto.py:174-267`。

---

## 三、幂等规则与来源隔离（防覆盖的核心）

- **规则 id**：`kw_<tmdb_id>_<season>`；无 TMDB ID 时用 `kw_ya-tg_<标题slug>`（`tgto.keyword_id()`）。
- **来源标记**：`source = "ya-tg"`（`tgto.RULE_SOURCE`）。
- **写入规则**（`upsert_keyword`）：
  - id 不存在 → append（`created`）。
  - id 存在且 `source=="ya-tg"` → 只覆盖本项目字段（`updated`）。
  - id 存在但 `source!="ya-tg"` → 直接返回 `conflict`，**一次都不 POST**，绝不覆盖他人规则。
- **回读校验**：回写后重新 GET，确认新规则存在且规则总数未减少；异常即抛错回滚提示。
- **删除规则**（`remove_keyword`）：只删 `id` 匹配 **且** `source=="ya-tg"` 的规则。GET 失败 / 结构未知 / 场景缺失 / 未命中 → 一律不 POST（零净改动）。

> 来源：`backend/app/tgto.py:89-94, 207-303`。护栏思想对齐 embyhub `remove_rules` 真机经验。

---

## 四、规则字段映射（`build_rule`）

| 字段 | 值/来源 | 说明 |
| --- | --- | --- |
| `id` | `kw_<tmdb>_<season>` | 幂等键 |
| `aliases` / `keywords` / `keyword_groups_any` | `[display]` / `[[display]]` | `display`=剧名，剧集追加 ` S<季>` |
| `media_name` / `preferred_name` / `title` | `display` | 展示名 |
| `tmdb_id` / `media_type` / `year` | 入参 | `media_type` 默认 `tv` |
| `required_keyword` + `include_required_keyword` | 必含词 + `bool(req)` | **必须同时写值并开开关**，否则编译不进正则 |
| `include_year` | `bool(year)` | 年份开关 |
| `resolution` | 4k/1080p/720p/all | 经 `norm_resolution()` 归一化 |
| `effect` | all/dolby/hdr/sdr | 经 `norm_effect()` 归一化 |
| `rule_type` | `structured` | 结构化规则 |
| `source` | `ya-tg` | 来源隔离锚点 |
| `original_title`/`overview`/`poster_path`/`poster_url`/`backdrop_path`/`backdrop_url` | TMDB 详情补全 | 决定前端是否显示「已匹配 TMDB」，见第六节 |

> 来源：`backend/app/tgto.py:97-142`。下拉值归一化映射见 `_RESOLUTION_MAP`/`_EFFECT_MAP`。

---

## 五、两条业务联动链路

### A. 求片（关键词监控白名单）

```
用户在 Bot 提交求片（/求片 或 TMDB 候选点选）
  → MediaRequest 落库，status=pending, review_status=pending
  → 【不写白名单】(v0.9.13 起由"提交即写"改为"审核通过才写")
管理员后台审核：
  - 采纳 adopted → tgto.upsert_keyword() 写入/更新规则；status=synced
  - 拒绝 rejected 且此前 synced → tgto.remove_keyword() 联动移除
  - 删除求片记录 且 synced → tgto.remove_keyword() 联动移除
```

关键：**审核态 `review_status` 与同步态 `status` 解耦**，列表不再把 `failed` 当业务态。
> 来源：`bot_listener.py:1013-1016,1320-1323`；`main.py:4653-4730`。

### B. 投稿（分享链接转发给监控 Bot 转存）

```
用户在 Bot 投稿（TMDB 候选 + 光鸭链接）
  → UserContribution 落库，status=pending
管理员审核通过 approved
  → bot_listener.forward_contribution_to_tgto() 把分享链接发到配置的
    tgto_contribute_bot_chat_id（用交互 Bot token 发 Telegram 消息）
  → 由该监控 Bot 自行转存；Ya-TG 不直接调转存接口
```

转发失败只写后台 note，不影响审核主流程与用户通知。
> 来源：`main.py:4555-4586`；`bot_listener.py:834-857`。
> 注意：投稿转存 OpenAPI 仅静态推导、**未真机验证**，不要在用户侧提示"已自动入库"。

### C. 测试连接

`POST /api/tgto/test` → `tgto.test_connection()`：只 GET 探测，返回场景与规则数，**不写入**。
> 来源：`main.py:4523-4528`；`tgto.py:306-312`。

---

## 六、"正在匹配 TMDB"问题的根因与修复（易复发）

- 现象：求片审核写入的规则在 TgtoDrive 前端显示"正在匹配 TMDB"，手动添加的显示"已匹配"。
- 根因：`build_rule()` 把 `poster_path/poster_url/backdrop_path/backdrop_url/overview/original_title` 写死空串；TgtoDrive 前端据这些字段是否为空判定匹配状态。
- 修复（v0.9.15 #268）：`upsert_keyword()` 写入前调 `_tmdb_meta()`，用 `tmdb.detail(tmdb_id, media_type, db)` 补全，并拼好 `poster_url`(w500)/`backdrop_url`(w780)；走 SCAN 场景限流。
- 降级：TMDB 未配密钥或查询失败 → 元数据空串，**不阻断**求片审核写入。
- 存量规则需重新采纳一次（或 TgtoDrive 后台补跑匹配）才会补上元数据。

> 来源：`backend/app/tgto.py:145-171,228-246`；`CHANGELOG.md` v0.9.15 第二节。

---

## 七、配置项清单（管理页「互动中心 c 板块」）

| 配置键 | 说明 | 默认 |
| --- | --- | --- |
| `tgto_base_url` | 服务根地址，末尾不带 `/` | 空 |
| `tgto_api_key` | OpenAPI Token（加密存储，回显掩码） | 空 |
| `tgto_scene` | 场景 key | `ENV_FILTER_GUANGYA` |
| `tgto_rule_resolution` | 分辨率 4k/all | `4k` |
| `tgto_rule_effect` | 效果 all/dolby/hdr/sdr | `all` |
| `tgto_rule_required_keyword` | 必含关键词（小写英文串） | `265` |
| `tgto_contribute_bot_chat_id` | 投稿转存监控 Bot 的 chat_id | 空 |

a/b/c 三字段在 `upsert_keyword` 未显式传值时从 runtime_config 读取统一注入，保证前端配置真正生效。所有键均已登记 `runtime_config.DEFAULTS`。
> 来源：`runtime_config.py:60-70`；`main.py:3805-3812,3873-3881`。

---

## 八、代码地图（复用时快速定位）

| 能力 | 文件/函数 |
| --- | --- |
| 对接边界/错误类型 | `backend/app/tgto.py` `TgtoError`, `_config`, `_headers` |
| 场景/规则解析 | `tgto._scene_obj`, `_rules_of` |
| 字段归一化 | `tgto.norm_resolution`, `norm_effect` |
| 幂等 id | `tgto.keyword_id` |
| 规则构造 | `tgto.build_rule` |
| TMDB 元数据补全 | `tgto._tmdb_meta` |
| GET/POST | `tgto.fetch_config`, `save_config` |
| 增改（含回读校验） | `tgto.upsert_keyword` |
| 精准删除（四道护栏） | `tgto.remove_keyword` |
| 只读测试 | `tgto.test_connection` |
| 求片审核/删除联动 | `main.handle_media_request`, `delete_media_request` |
| 投稿审核转发 | `main.handle_contribution` → `bot_listener.forward_contribution_to_tgto` |
| 配置读写 | `main.InteractionConfigInput`, `interaction_config_get/save` |

---

## 九、可复用模式（提炼给其它系统集成参考）

1. **外部系统只走 OpenAPI**：不碰源码/容器/数据库，边界写进模块 docstring。
2. **无单条增删接口时**：固定「GET 全量 → 内存合并 → POST 全量 → GET 回读校验」四步，回读校验规则数不减少。
3. **幂等键 + 来源标记**双锚点：`kw_<业务id>` + `source=<项目名>`，实现"只动自己的、他人零改动"。
4. **冲突即停**：目标 id 属他人 → 返回 conflict 且不发起写请求，绝不静默覆盖。
5. **删除零净改动护栏**：GET 失败/结构未知/未命中 → 不 POST，避免写出空规则列表。
6. **审核态与同步态解耦**：业务审核状态独立于外部系统同步状态，外部失败只写后台 note，不伪装成功、不惊扰用户。
7. **外部依赖失败安全降级**：元数据查询失败时用空值写入，不阻断主业务；但要把"待补元数据"状态显性化。
8. **配置统一注入**：分散字段（a/b/c）在底层函数按配置读取统一注入，避免调用点遗漏导致配置"保存了不生效"。

---

## 十、真机验证状态与已知缺口

- ✅ 代码+契约测试通过：`tests/test_v0913_front_fixes_tgto_link.py`、`tests/test_v0915_batch_fixes.py`（共 23 项，覆盖 upsert/remove/元数据/护栏）。全量 pytest 530 passed。
- 🟡 待真实环境验证：
  - `POST /api/visual-filter/config` 全量回写在真机的字段兼容性与回读一致性。
  - 规则元数据补全后，TgtoDrive 前端是否确实从"正在匹配 TMDB"变为"已匹配"。
  - 投稿转存链路（转发给监控 Bot 后是否自动入库）——OpenAPI 转存仅静态推导未验证。
  - 必含关键词/分辨率/效果字段在真机编译进正则的实际过滤效果。
- 排障入口：规则 id 用 `keyword_id()` 反算；`source=ya-tg` 过滤只看本项目规则；回读数量异常优先怀疑全量覆盖写坏了其它场景。

> 关联文档：`docs/配置说明.md` 第三节、`docs/需求追踪矩阵.md` V63–V65 / V77、`CHANGELOG.md` v0.9.13 / v0.9.15。
