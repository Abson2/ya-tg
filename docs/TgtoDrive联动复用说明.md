# Ya-TG ↔ TgtoDrive 联动模块说明（最新复用版）

> 自包含，供上传资料库后独立复用。基于 Ya-TG 整改全部完成版源码（HEAD `1437b95`，阶段一~七已完、未发版，tag 仍 v0.9.20），2026-11-20 更新。
> 相对上一版（v0.9.15）的**重要变更**：①投稿转存链路**已彻底移除**（v0.9.19 起改走光鸭原生 API，不再经 TgtoDrive）；②求片元数据加本地缓存层（阶段五 B-07）；③年份补全与 conflict/applied 诊断回执（v0.9.17）。复用时请以本文为准，旧文第五节 B「投稿转发」、配置项 `tgto_contribute_bot_chat_id` 均已作废。

---

## 一、对接边界（先定边界，再谈实现）

TgtoDrive 是**独立外部系统**。Ya-TG 的边界：

- 不集成其源码、不部署其容器、不读写其数据库；只通过它暴露的 **OpenAPI** 做「读取 → 内存合并 → 回写 → 回读验证」。
- 绝不覆盖对方原有关键词、管理员手动规则、其它项目规则。
- **只保留一条联动链路：求片关键词监控白名单**（采纳写入 / 拒绝或删除联动移除）。投稿相关已全部剥离。
- 实测确认（2026-11-20 源码核对）：`bot_listener.py:1018` 注释明确旧 `forward_contribution_to_tgto` 已彻底删除；`runtime_config`/前端均无 `tgto_contribute_bot_chat_id` 配置项。

> 来源：`backend/app/tgto.py` docstring、`backend/app/bot_listener.py:1018`、旧文边界节。

**"光鸭频道关键词监控白名单"的真实所指**：即 TgtoDrive 的 `visual-filter` 场景规则（`ENV_FILTER_GUANGYA`），**不是光鸭网盘自身的功能**。这是最容易踩的概念坑。TgtoDrive 的 Telegram 频道监控在 `TG_monitor-guangya.db` 消费同一套 visual-filter 规则做过滤，所以写入该场景规则等价于给"频道监控"加白名单。

---

## 二、OpenAPI 契约

```http
GET  {base}/api/visual-filter/config?scene=ENV_FILTER_GUANGYA
POST {base}/api/visual-filter/config
```

- 鉴权：`Authorization: Bearer <OpenAPI Token>` + `Content-Type/Accept: application/json`；超时复用 `settings.guangya_timeout`。
- GET 响应：`{"success":true,"scene":"...","current":{...},"scenes":{"ENV_FILTER_GUANGYA":{...,"rules":[...]}}}`。
- POST body：`{"scene":"<scene>", ...目标场景对象全部字段}`（含 `rules`/`raw_regex` 等，其余沿用 GET 原样）。
- **无单条增删接口，只能全量覆盖** → 固定四步：`GET 全量 → 内存合并目标规则 → POST 回写 → GET 回读校验`。
- `401` 抛 `TgtoError("Token 无效")`；HTTP≥400 / 非 JSON / 非 dict 均中止写入，不吞错。

> 来源：`tgto.py:219-250`（`fetch_config`/`save_config`）、真机样本 `guangya_visual_filter.json`。

---

## 三、幂等与来源隔离（防覆盖核心）

- **幂等 id**：`kw_<tmdb_id>_<season>`；无 TMDB ID 用 `kw_ya-tg_<标题slug>`（`tgto.keyword_id()`）。
- **来源标记**：`source="ya-tg"`（`tgto.RULE_SOURCE`）。
- **`upsert_keyword`**：id 不存在→append(created)；存在且 `source=ya-tg`→只覆盖本项目字段(updated)；**存在但 `source!=ya-tg`→返回 conflict，一次都不 POST**，绝不覆盖他人规则。
- **回读校验**：回写后重新 GET，确认新规则存在且总数未减少（`rules_after < before` 抛错中止）。
- **`remove_keyword` 四道护栏**：只删 id 匹配且 `source=ya-tg`；GET 失败/结构未知/场景缺失→中止不 POST；未命中→零净改动不 POST；同步 `current` 副本、保留其它规则顺序。

> 来源：`tgto.py:252-364`。

---

## 四、规则字段映射（`build_rule`）

| 字段 | 值/来源 | 说明 |
| --- | --- | --- |
| `id` | `kw_<tmdb>_<season>` | 幂等键 |
| `aliases`/`keywords`/`keyword_groups_any` | `[display]`/`[[display]]` | `display`=剧名，剧集追加 ` S<季>` |
| `media_name`/`preferred_name`/`title` | `display` | 展示名 |
| `tmdb_id`/`media_type`/`year` | 入参（year 可由 TMDB 补） | `media_type` 默认 `tv` |
| `required_keyword` + `include_required_keyword` | 必含词 + `bool(req)` | **值+开关必须同写**，否则编译不进正则 |
| `include_year` | `bool(year)` | 年份开关 |
| `resolution` | 4k/1080p/720p/all | `norm_resolution()` 归一 |
| `effect` | all/dolby/hdr/sdr | `norm_effect()` 归一 |
| `rule_type` | `structured` | 结构化规则 |
| `source` | `ya-tg` | 来源隔离锚点 |
| `original_title`/`overview`/`poster_*`/`backdrop_*` | TMDB 详情补全 | 决定前端是否显示「已匹配」，见第六节 |

> 来源：`tgto.py:128` 起 `build_rule`。

---

## 五、唯一业务链路：求片关键词监控白名单

```
用户在 Bot 提交求片（/求片 或 TMDB 候选点选）
  → MediaRequest 落库 status=pending, review_status=pending
  → 【提交阶段不写白名单】(v0.9.13 起由"提交即写"改为"审核通过才写")
管理员后台审核（POST /api/media-requests/{rid}/handle）：
  - 先 commit 审核态 review_status，再做外部网络调用（绝不跨 await 持写事务）
  - 采纳 adopted → tgto.upsert_keyword()；status=synced/failed
      回执带 sync_ok / sync / sync_note / applied（本次真实写入的参数快照）
  - 拒绝 rejected 且此前 synced → tgto.remove_keyword() 联动移除
  - 删除求片记录 且 synced → remove_keyword() 联动移除（失败只记日志不阻断）
用户侧通知只在审核动作后发，携带真实同步结果，绝不假装成功
```

关键点：
- **审核态 `review_status` 与同步态 `status` 解耦**，列表不把 `failed` 当业务态。
- **参数黑洞已修**（v0.9.17 #280）：命令式 `/求片` 不带 year → `_tmdb_meta` 从 TMDB 详情补 year；`upsert_keyword` 回执 `applied` 带分辨率/效果/必含/年份实际值，前端按 `sync_ok` 分别展示成功参数或失败原因。
- `conflict`（他人规则占 id）：回执带 `detail` 说明 source 与 id，不写、不覆盖。

> 来源：`main.py:5354-5469`（add/handle/delete media-request）、`bot_listener.py` 求片提交路径。

---

## 六、"正在匹配 TMDB"问题根因与修复（易复发）

- 现象：求片审核写入的规则在 TgtoDrive 前端显示"正在匹配 TMDB"，手动添加的显示"已匹配"。
- 根因：`build_rule` 曾把 `poster_path/poster_url/backdrop_*/overview/original_title` 写死空串；TgtoDrive 前端据这些字段是否为空判定匹配状态。
- 修复（v0.9.15 #268）：`upsert_keyword` 写入前调 `_tmdb_meta()`，用 `tmdb.detail(tmdb_id, media_type, db)` 补全并拼 `poster_url`(w500)/`backdrop_url`(w780)，走 SCAN 场景限流。
- **缓存层（阶段五 B-07）**：`_tmdb_meta` 加进程内 TTL 24h（`_TMDB_META_TTL`）+ 硬上限 500（`_TMDB_META_MAX`，淘汰 1/5）缓存；只缓存成功取到数据的结果，全空结果不缓存（避免固化临时抖动）；TMDB 配置变更时 `clear_tmdb_meta_cache()` 立即失效。命中缓存不消耗限流配额、不发网络。
- 降级：TMDB 未配密钥/查询失败 → 元数据空串，**不阻断**求片审核写入。
- 存量规则需重新采纳一次（或 TgtoDrive 后台补跑匹配）才会补上元数据。

> 来源：`tgto.py:33-58,176-216`；`tests/test_v103_tmdb_meta_cache.py`；`CHANGELOG.md` v0.9.15 / 阶段五。

---

## 七、配置项清单（管理页「互动中心 c 板块」）

| 配置键 | 说明 | 默认 |
| --- | --- | --- |
| `tgto_base_url` | 服务根地址，末尾不带 `/` | 空 |
| `tgto_api_key` | OpenAPI Token（加密存储、回显掩码） | 空 |
| `tgto_scene` | 场景 key | `ENV_FILTER_GUANGYA` |
| `tgto_rule_resolution` | 分辨率 4k/1080p/720p/all | `4k` |
| `tgto_rule_effect` | 效果 all/dolby/hdr/sdr | `all` |
| `tgto_rule_required_keyword` | 必含关键词（小写英文串） | `265` |

a/b/c 三字段在 `upsert_keyword` 未显式传值时从 runtime_config 读取统一注入，保证前端配置真正生效（v0.9.13 #254）。所有键均登记 `runtime_config.DEFAULTS:61-68`。**`tgto_contribute_bot_chat_id` 已删除，勿再配置。**

> 来源：`runtime_config.py:61-68`、`tgto.py:261-266`。

---

## 八、代码地图（复用快速定位）

| 能力 | 文件/函数 |
| --- | --- |
| 对接边界/错误类型 | `tgto.py` `TgtoError`,`_config`,`_headers` |
| 场景/规则解析 | `tgto._scene_obj`,`_rules_of` |
| 字段归一化 / 幂等 id | `tgto.norm_resolution`,`norm_effect`,`keyword_id` |
| 规则构造 | `tgto.build_rule` |
| TMDB 元数据 + 缓存 | `tgto._tmdb_meta`,`_tmdb_meta_cache_get/put`,`clear_tmdb_meta_cache` |
| GET/POST | `tgto.fetch_config`,`save_config` |
| 增改（回读校验 + conflict/applied） | `tgto.upsert_keyword` |
| 精准删除（四道护栏） | `tgto.remove_keyword` |
| 只读测试 | `tgto.test_connection` |
| 求片审核/删除联动 | `main.add_media_request`,`handle_media_request`,`delete_media_request` |
| 配置读写 | `main.InteractionConfigInput`,`interaction_config_get/save` |

---

## 九、可复用模式（提炼给其它系统）

1. **外部系统只走 OpenAPI**：不碰源码/容器/数据库，边界写进模块 docstring。
2. **无单条增删接口**：固定「GET 全量→内存合并→POST 全量→GET 回读校验」四步，回读校验规则数不减少。
3. **幂等键 + 来源标记双锚点**：`kw_<业务id>` + `source=<项目名>`，只动自己、他人零改动。
4. **冲突即停**：目标 id 属他人→返回 conflict 且不发写请求，绝不静默覆盖。
5. **删除零净改动**：GET 失败/结构未知/未命中→不 POST，避免写出空规则列表。
6. **审核态与同步态解耦**：审核先落盘再调外部网络（不跨 await 持写事务），外部失败只写后台 note，不伪装成功、不惊扰用户。
7. **回执带 applied 参数快照**：杜绝"前端说成功、用户以为没传"的诊断黑洞。
8. **外部依赖失败安全降级 + 缓存**：元数据失败用空值不阻断主业务；成功结果才缓存、配置变更即失效；"待补元数据"状态显性化。
9. **配置统一注入**：分散字段在底层函数按配置读取注入，避免"保存了不生效"。

---

## 十、真机验证状态与已知缺口（如实标注）

- ✅ 代码+契约测试通过：`test_v0913_front_fixes_tgto_link.py`、`test_v0915_batch_fixes.py`、`test_v103_tmdb_meta_cache.py`、`test_v0916_bot_message_guard_and_discover.py` 等；整改后全量 pytest **741 passed×2 稳定**。
- 🟡 **待真实环境验证**（沙箱连不上用户 TgtoDrive，未实测，勿标 ✅）：
  - `POST /api/visual-filter/config` 全量回写的真机字段兼容性与回读一致性。
  - 规则元数据补全后，前端是否确实从"正在匹配 TMDB"变为"已匹配"。
  - 分辨率/效果/必含词/年份在真机编译进正则后的实际过滤效果。
  - TgtoDrive 白名单真机写入（矩阵 V64/V65 原标 🟡）。
- ⛔ **已废弃，勿复用**：投稿转存经 TgtoDrive 的所有链路（v0.9.18 中转群 / `forward_contribution_to_tgto` / `tgto_contribute_bot_chat_id`）。v0.9.19 起投稿改走光鸭原生 API（get_share_access_token→get_share_page_files_list→restore_share，204=自转存拒绝），与本模块无关。
- 排障入口：规则 id 用 `keyword_id()` 反算；`source=ya-tg` 只看本项目规则；回读数量异常优先怀疑全量覆盖写坏其它场景；规则显示"正在匹配 TMDB"优先查 `_tmdb_meta` 是否取到海报/简介。

> 关联文档：`docs/配置说明.md` 第三节、`docs/需求追踪矩阵.md`、`CHANGELOG.md` v0.9.13/v0.9.15/v0.9.17/v0.9.19、阶段五/七交付文档。
