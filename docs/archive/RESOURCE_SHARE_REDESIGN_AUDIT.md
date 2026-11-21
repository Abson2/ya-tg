# Ya-TG 资源库优化与分享中心重新设计 · 第一阶段源码审计报告

> 审计基线：v1.4.4（HEAD `3f7b60c`，tag `v1.4.4`，2026-11-21）
> 审计范围：backend/app 全量（重点 scanner / tmdb / share_center / publisher / events / guangya / models / db / runtime_config / main）+ frontend/src/App.vue
> 审计方式：四路并行只读审计，所有结论带源码文件+行号；严格区分 `[事实]` / `[推断]+推理链` / `[缺口]` / `[假设]+不成立应对`
> 本阶段**不改任何业务代码**。本文只交付审计结论与改造方案依据。
> 说明：已有 `AUDIT_REPORT.md` / `FOUR_MODULE_REFACTOR_AUDIT.md` 基于 v0.9.20（HEAD `1437b95`），中间经历 v1.0~v1.4.4 大量改造（share_center 实体化、事件总线、MirrorEvent/ResourceEvent 表、strict 引擎、按部收敛等），**已过时**，本报告为当前基线重新审计。

---

## 0. 结论先行

**改造可行性：可做，且已有约 60% 基础设施可复用——但不能推倒重做，必须走「复用现有实体 → 补生命周期维度 → 解耦触链 → 前端拆页」的增量路线。** [推断]+ 依据：v1.1 起已建立 ShareTask/ShareRecord 双表真相源、事件总线、分享中心→分发中心交接协议（submit/claim/mark_result）、can_publish 门禁；用户建议书中「分享中心独立于资源库」「按 resource_id 关联」「事件持久化幂等」「统一门禁」等目标，**现状已有骨架**。真正缺口集中在三块：①链接有效性生命周期（校验/失效复查/清理）全无；②资源事件只有 3 类闭环、6 类死常量；③识别成功仍同步硬触发分享入队、Resource.share_* 缓存与真相源双向写。

**最关键的产品约束（必须让用户先确认，否则方案会做错方向）**：用户建议书要求「分享链接校验、失效复查、清理」，但 `[事实]` 光鸭官方 OpenAPI 通道**零分享接口**，分享能力全在网页端 `api.guangyapan.com/userres/v1/*` 通道；源码中**只有 create/update/delete 三个分享接口，没有任何「查询分享状态/有效性」接口，也没有分享访问统计接口**。`[推断]` 真正可靠的「主动校验链接有效性」在当前 API 能力下**做不到**——这是全项目最大的方案风险，不能虚构。详见第 5 节与第 11 节待确认项。

### 四模块职责漂移总览

| 模块 | 现状承载 | 与目标边界差异 | 置信度 |
|---|---|---|---|
| 镜像同步 | `mirror.py`/`mirror_batch.py`/`main.py:612-642` | **基本符合**：明确零分享/零推送调用；但 MIRROR 事件只写不读，恢复靠周期重扫非事件 | 高 |
| 资源库 | `scanner.py`/`main.py:2180-2283,2567-2679`/`models.Resource` | **职责外溢**：识别成功同步调 `_enqueue_after_identify`（5 处）；`Resource.share_*` 与 ShareRecord 真相源双向写 | 高 |
| 分享中心 | `share_center.py`（836 行）/ShareTask/ShareRecord | **已有骨架但生命周期不全**：任务状态机完整、幂等完整；缺链接有效性维度、缺主动校验/清理、can_publish 只查本地元数据 | 高 |
| 分发与互动 | `publisher.py`/`bot_listener.py`/`contribution_restore.py`/`tgto.py` | **须保持不动**：发送核心与互动逻辑完整自洽；分享中心只通过 4 个交接接口耦合 | 高 |

---

## 1. 现有功能清单（按模块）

### 1.1 资源库已有功能 `[事实]`
- B 库扫描：`scanner.py:151` `run_scan`，显式栈 DFS + 每目录 100 条分页（`:218-253`），断点续扫（`:180-198`），批次落 `resource_scan_batches`（`:207-215`）。
- 完整性保护：`scanner.py:257-258`（`total>fetched → scan_complete=False`）、`guangya.py:233-261`（超时/5xx/429 重试 3 次+退避）、异常置 batch failed 后 raise（`:291-323`）。**一次超时不会误删**。
- 资源组织：`Resource.file_id` 唯一（单文件逐行存），`work_group_key`（`utils.py:197-224`）聚合成「整剧」展示；`season/episode`（`models.py:43-44`）。
- TMDB/AI 识别：`tmdb.identify`（`tmdb.py:461-634`）目录名优先、ID 精确、AI 静默降级；人工匹配 `/api/resources/{rid}/identify`（`main.py:3306-3327`）。
- 海报墙：后端 `/api/library/series`（`main.py:2567-2679`），前端 media-card（`App.vue:2754-2786`），已有分享状态角标 + 识别/完结角标 + 筛选。
- 对账删除闸门：`_reconcile_library`（`main.py:2180-2283`），「complete + 非续扫 + seen 非空 + batch.reconcile_allowed」四重保护。
- 删除/恢复：出库时取消分享+清队列+取消 ShareTask+发 RESOURCE_REMOVED+删 Resource（`:2239-2276`）；恢复靠 cancelled first 任务复活（`:3079-3085`）。

### 1.2 分享中心已有功能 `[事实]`
- 双表真相源：`ShareTask`（`models.py:561-597`）+ `ShareRecord`（`:600-624`，部分唯一索引保证一作品仅一条 current、历史多条保留 `:620-624`）。
- 状态机：`planned/building/ready/submitted/accepted/publishing/published/failed/cancelled`（`share_center.py:32-44`），真实流转 `planned→ready→submitted→accepted→published/failed`。
- 幂等：`dedup_key` 唯一（`:203-293`）、cancelled first 任务复活（`:240-255`）、build_share 四层复用（`:301-341`）、事件 consumed_at。
- 调度：`run_cycle`（`:499-593`）按 priority+id 排序、failed 退避（attempts<8）、`release_gap` 释放间隔（`:512,534-542`）、三类日配额（`:186-199`）。
- 统一门禁 `can_publish`（`:171-200`）：资源存在、identified+海报（force 绕过）、update/restore 需 current 链接 active、配额。
- 交接协议：`submit_to_dispatch`（`:414-443`）/`claim_tasks`（`:446-467`）/`share_url_for_queue_item`（`:470-481`）/`mark_result`（`:484-496`）。
- 事件消费：`consume_resource_events`（`:629-697`）处理 removed/identified/restored，毒事件内存态计数隔离（v1.4.2 死锁根治）。
- 前端：section `App.vue:2823-2901`，7 个纵向区块（总览卡/分享任务/推送队列/发布节奏/追更管理/分享记录），radio 三队列筛选。

### 1.3 分发与互动中心已有功能 `[事实]`
- TG 发送：`publisher.py` `_send_telegram`（`:301-372`）sendPhoto 回退、429 retry_after、错误分级；`_build_message`（`:93-298`）；逐条 2s 节流（`:390-392`）；AuditLog（`:538-540`）。
- 白名单：`bot_listener.is_push_allowed`（`:732-747`），全库仅 `publisher.py:490` 一处调用。
- 多 Bot/多目标：Channel 所属 Bot 发送；push_routes 一路由多目标（`bot_listener.py:349-453`）。
- 互动：求片/投稿会话内存态（`:975-986`）、ProcessedUpdate 幂等（`:1773-1774`）；投稿转存（`contribution_restore.py:58-128,205-274`）；TgtoDrive 白名单联动（`tgto.py:331-364`）。

---

## 2. 现有代码结构与调用链

### 2.1 完整数据流 `[事实]`
```
[A库] --mirror.py(sync_rule/按部收敛)--> [B库]
   └─ mirror_post_sync_pipeline → _resolve_english_title → rename_mirror_files（v1.4.4 按部收敛）
[B库] --scanner.run_scan--> Resource 表（单文件行 + work_group_key 聚合）
   ├─ 识别成功 → _enqueue_after_identify（main:2138/3318/3352/3390/2340）[耦合点]
   │     → _enqueue_ready（main:3052 写 QueueItem + 3061 shadow_register + 3083 emit_resource_event）
   ├─ 内容变化 → DistributionEvent（scanner:120-148）
   └─ 对账删除 → _reconcile_library（取消分享+删资源+emit REMOVED）
[事件] resource_events → consume_resource_events → ensure_task → run_cycle
   → build_share（光鸭 create）→ can_publish → submit_to_dispatch（写 QueueItem status_ext=submitted）
[分发] process_queue → claim_tasks（submitted→accepted）→ _send_telegram → mark_result
```

### 2.2 识别→分享的 5 个硬耦合点 `[事实]`
`_enqueue_after_identify` 调用：`main.py:2138`（扫描后识别）、`:3318`（手动 identify）、`:3352`（auto_identify）、`:3390`（identify_all）、`:2340`（更新事件兜底）。
`[推断]` 这是「识别与发布解耦」要解决的核心：识别入口应只写资源库 + 发 RESOURCE_IDENTIFIED 事件，由分享中心决定是否订阅入队。

### 2.3 光鸭调用散点 `[事实]`
建分享：`publisher.py:36-80`、`share_center.py:296-383`、`main.py:2879-2924`（三处最终都走 `GuangYaShareClient.create`）；另有 `main.py:2852`（re_share）、`2839/2934`（update）。取消/删除：`main.py:2242/2772/2950`（`GuangYaShareClient.delete`）。`[推断]` 七处散点应收敛到统一发布引擎的光鸭适配器。

### 2.4 作品键三套口径 `[事实]`
`utils.work_group_key`（`utils.py:197-224`，`work:{作品名}`）、`main._work_key`、`publisher._work_group_key`（`dir:/tmdb:/file:`）三套实现。`[推断]` 是「一张卡 = 一条推送」的潜在裂缝，应收敛为单一权威作品键服务。

---

## 3. 数据库结构

### 3.1 25 张表模块归属 `[事实]`
- 资源库：`resources`、`resource_versions`、`resource_scan_batches`、`scan_tasks`、`scan_cursors`
- 分享中心：`share_tasks`、`share_records`、`distribution_events`
- 镜像：`mirror_rules`、`mirror_dirs`、`mirror_media`、`mirror_files`、`mirror_tasks`、`mirror_events`
- 分发互动：`bots`、`channels`、`discovered_chats`、`processed_updates`、`rules`、`push_queue`、`message_templates`、`user_contributions`、`contribution_restores`、`media_requests`
- 系统：`auth_tokens`、`auth_sessions`、`app_config`、`audit_logs`

### 3.2 关键模型字段缺口 `[事实]`
- `ShareRecord`（`:600-624`）：有 `share_id/share_url/share_status/current_flag/cancelled_at/last_verified_at`；**无 `share_code`（提取码）、无校验结果字段、无清理状态字段**。
- `last_verified_at` 唯一写入点 `share_center.py:120` 是「登记/刷新时间」，**非有效性校验结果**。
- `Resource.share_scope/share_dir_id/share_url/share_status`（`models.py:23-25,58-61`）与 ShareRecord 真相源双向写（`share_center.py:372,755`、`publisher.py:68-79`）——解耦的主要数据摩擦点。
- `ResourceEvent`（`models.py:655-669`）幂等键 `type|work_key|resource_id`（`events.py:69`），**不含 fingerprint/序号**，无法表达同一资源同一类型的多次变化。

### 3.3 迁移机制 `[事实]`
`db.migrate()`（`db.py:444-501`）：`create_all` + `_COLUMNS` 幂等 `ALTER TABLE ADD COLUMN`（`:56-215,452-455`）+ AppConfig marker 一次性数据修复。**只有增列，无删列/改列/回滚/版本号管理**。`[推断]` 重构应避免删列；新增字段一律带 DEFAULT 幂等增列，符合「可回滚」要求（回滚=恢复旧列值，不改结构）。

---

## 4. 事件机制现状（最薄弱环节）

### 4.1 emit/consume 配对 `[事实]`
| 事件 | emit | consume | 结论 |
|---|---|---|---|
| RESOURCE_NEW(identified) | main:3083 | share_center:654-660 | 闭环 |
| RESOURCE_RESTORED | main:3082-3083 | share_center:654-660 | 闭环 |
| RESOURCE_REMOVED | main:2270-2272 | share_center:648-653 | 闭环 |
| MIRROR_COMPLETED | main:1143 | 无 | 只写不读 |
| RESOURCE_EPISODE_ADDED / RESOURCE_FILE_UPDATED | 无 | 无 | 死常量 |
| MIRROR_{UPDATED/RESTORED/DELETED/FAILED} | 无 | 无 | 死常量 |
| created/file_changed/renamed/moved/deleted | 未定义 | 未定义 | 缺失 |

`consume_mirror_events` 函数**根本不存在**（仅注释提及）。`[推断]` 镜像→资源库→分享中心的闭环实际靠「定时全量镜像→全量扫 B 库→识别→入队」周期流程驱动，非事件驱动。

### 4.2 与用户九类事件的差距 `[缺口]`
用户要求 `resource_created/updated/episode_added/file_changed/renamed/moved/deleted/restored/identified` 九类；现状仅 5 个常量，其中 2 个死常量；缺 5 类定义。且现有幂等键无法支撑「逐集/逐文件多次变化」——需扩展 `ResourceEvent` 幂等键（加 fingerprint/序号）+ `payload_json` 变化摘要。

---

## 5. 光鸭 API 能力边界（关键，用户强制要求先取证）

### 5.1 三个通道 `[事实]`（`guangya.py` / `config.py:22-36`）
- **官方 OpenAPI**（`openapi.guangyapan.com`，x-client-id+Bearer）：仅 3 方法——`list_files`、`get_file_detail`、`get_user_info`（`guangya.py:86-100`）。`_RATE` 中其他路径只是限流配置，无实现。**零分享接口**。
- **网页端文件接口**（`api.guangyapan.com/userres/v1/*`，Bearer+did+dt）：列文件/建目录/单文件 rename/批量删文件/复制/任务状态/转存（`guangya.py:122-496`）。rename 确认单文件 `{fileId,newName}`，与用户抓包一致，**无批量 rename/正则替换**。
- **网页端分享接口**（同 userres 通道，`GuangYaShareClient`，`guangya.py:499-614`）：**create**（`share_file`，`:592-596`）、**update**（`update_share`，`:598-603`）、**delete**（`delete_share`，`:605-606`）。

### 5.2 能力对照表 `[事实]`
| 用户想要 | 源码是否真实存在 | 说明 |
|---|---|---|
| 创建分享 | ✅ 存在 | `share_file`，网页端通道；显式 `enableShareCode=False`，无提取码 |
| 更新分享（标题/效期） | ✅ 存在 | `update_share`，已被 `main.py:2839/2934` 使用 |
| 取消/删除分享 | ✅ 存在 | `delete_share`，光鸭侧真删除；`main.py:2242/2772/2950` 已用 |
| **查询分享状态/有效性** | ❌ 不存在 | 仅投稿转存 `get_share_access_token` 业务码 200/201/202 间接反映失效；自建分享触发 204 自转存拒绝，不能直接当校验器 |
| **分享链接访问统计** | ❌ 不存在 | 全库无浏览/下载统计方法 |
| 删除分享记录（光鸭侧） | ❌ 不存在 | 仅本地 `retire_share_record` |
| 提取码/share_code | ❌ 不存在 | 建分享显式关闭；仅投稿 URL 解析支持 `?code=` |
| 批量 rename/正则替换 | ❌ 不存在 | 已由用户 HAR 抓包坐实 |

### 5.3 结论 `[推断]+推理链`
用户建议书第 5 章「分享链接校验/失效复查/清理」中的**主动有效性校验**，在当前 API 能力下无可直接调用的官方接口。可行替代只有三类：①复用投稿转存 `get_share_access_token` 的失效业务码（需实测自建分享是否可调通，且 204 自转存拒绝是障碍）；②纯本地元数据 + 事件驱动（B 库删除→取消），无主动探测；③新增抓包取证网页端是否有「检查分享有效性」接口（如分享页访问接口）。**必须先让用户确认，不得虚构校验成功结果。**

---

## 6. 可复用 / 需改造 / 缺失 三清单

**可复用（不推倒重做）**
1. 扫描批次闸门 + 完整性保护（`scanner.py:164-282`、`guangya.py:233-261`、`main.py:2200-2230`）
2. TMDB/AI 识别决策与静默降级（`tmdb.py:461-634`、`ai_assist.py`）
3. `work_group_key` 作品聚合（`utils.py:197-224`），建议收敛为单一权威
4. ShareTask/ShareRecord 双表 + 部分唯一索引 + dedup_key 幂等（`models.py:561-624`、`share_center.py:203-341`）
5. can_publish 门禁框架 + 配额 + release_gap 节奏（`share_center.py:171-200,499-593`）
6. 交接协议四接口（`share_center.py:414-496`）
7. 事件落库/读取原语（`events.py:39-100`）
8. 分发中心发送核心 14 处（第 7 节清单，**绝对不动**）
9. 前端 navGroups + 海报墙 media-card + 5 类分享后端 API
10. `_COLUMNS` 幂等增列迁移 + runtime_config DB>env 优先级

**需改造（保留功能、解耦/收敛）**
1. 识别→入队 5 处硬耦合 → 改为「识别只写资源库 + 发事件」
2. `Resource.share_*` 与真相源双向写 → 明确真相源，前端改读 ShareRecord
3. `share_status` 字符串 → 9 态生命周期枚举
4. `last_verified_at` 语义修正 + 补校验结果/原因字段
5. 三套建分享逻辑（publisher/share_center/main）→ 统一发布引擎的光鸭适配器
6. `ResourceEvent` 幂等键扩展 + 补 emit/consume
7. `claim_tasks` 补 `WHERE status_ext=submitted` 条件（兑现多进程原子领取）
8. 前端分享中心 section 拆 5 页；资源卡加推送状态/链接有效性角标

**缺失（需新增）**
1. 链接有效性生命周期 8 态 + 校验/失效复查/清理逻辑（受第 5 节 API 限制约束）
2. A 库原名回溯 TMDB 识别入口（当前仅用 B 库 name/path_hint）
3. 识别结果持久化缓存（当前每次打 TMDB）
4. `consume_mirror_events` + MIRROR_RESTORED emit（镜像事件消费端）
5. 5 类资源事件定义与全链路
6. 独立资源详情页（当前仅「重新匹配元数据」弹窗）
7. 前端 share_center_enabled 开关、链接校验周期配置项

---

## 7. 分发中心不可动核心清单（重构红线）

以下为发送与互动核心，分享中心**只调用对外接口、不得改内部**：
- `publisher.py:36-80` ensure_share；`:93-298` _build_message；`:301-372` _send_telegram；`:385-575` _process_queue_impl；`:578-589` process_queue+锁
- `bot_listener.py:330-346` push_whitelist；`:349-453` push_routes/match_push_routes；`:475-515` route_target_channel/effective_token/bot_id；`:716-747` gate_mode/is_push_allowed；`:832-840` _normalize_tme_link；`:1703+` poll_once
- `contribution_restore.py:93-128,205-274` 转存自愈与执行
- `tgto.py:331-364` remove_keyword
- `models.py:217-237` QueueItem 唯一约束；`:600-624` ShareRecord 真相源

---

## 8. 潜在风险清单（按优先级）

| 级别 | 风险 | 依据 | 缓解 |
|---|---|---|---|
| **P0-决策** | 用户要的「链接有效性主动校验」光鸭无官方接口 | 第 5 节能力表 | 先让用户确认可接受「本地元数据+事件驱动」或授权抓包取证替代接口；不得虚构 |
| P0 | share_* 缓存与真相源双向写，解耦时状态不一致 | models.py:23-25,58-61 + share_center.py:372,755 | 单一真相源 + 前端改读 ShareRecord，双写期以 ShareRecord 为准 |
| P0 | 作品键三套口径导致重复/漏推 | utils/main/publisher 三套 | 收敛单一权威键服务 + 回归测试 |
| P1 | 事件幂等键无法表达重复变化；镜像事件无消费端 | events.py:69 + 第 4 节 | 扩展幂等键 + 补 consume_mirror_events |
| P1 | claim_tasks 非真正原子（UPDATE 无 WHERE 条件） | share_center.py:446-467 | 补条件 UPDATE + worker_id |
| P1 | 前端分享中心 key/label 错位（key=推送队列） | App.vue:549-550 | 拆页时统一迁移并保留兼容断言 |
| P2 | 迁移无删列/回滚/版本号 | db.py:444-501 | 只用幂等增列；回滚走备份恢复（ROLLBACK.md） |
| P2 | 106 个测试中有源码结构断言 | tests/ | 改造时同步更新，保持全量门禁 |

---

## 9. 分阶段实施建议（对齐用户建议书第十二节）

> 原则：每阶段独立可测、可回滚、不动分发核心；阶段 3 前必须先解决第 5 节 API 决策。

- **阶段一（本次）**：源码审计（本报告完成）。
- **阶段二 资源库解耦**：识别入口改「写资源库+发 RESOURCE_IDENTIFIED 事件」；Resource.share_* 只读缓存真相源收敛；A 库原名回溯识别入口；识别结果缓存。验收：识别正常、海报墙正常、分享由事件驱动创建。
- **阶段三 事件体系补齐**：ResourceEvent 幂等键扩展；补 5 类事件 emit/consume；consume_mirror_events + MIRROR_RESTORED。验收：逐集/逐文件变化可追溯、重启不丢事件、幂等。
- **阶段四 分享中心生命周期**：share_status 9 态；share_code/校验结果/清理字段；统一发布引擎（存量/全新/追更）收敛三套建分享逻辑；can_publish 增强。**前置：第 5 节 API 决策**。验收：整目录分享、追更复用链接、历史多记录。
- **阶段五 链接校验与清理**：按第 5 节决策实现（本地事件驱动 / 抓包替代接口 / 投稿转存业务码复用）；失效复查、清理、日志。验收：网络异常不误判失效、清理不删 B 库资源与历史。
- **阶段六 前端拆页**：分享中心 5 页；资源卡加推送状态/链接有效性；统一状态定义。验收：状态两处一致、按钮走真实后端。
- **阶段七 完整测试与真机验证**：全量 pytest（当前 814）+ 真机清单；启用自动发布/自动清理前必须验收通过。

---

## 10. 待用户确认项（阻塞后续设计）

1. **【最高优先】链接有效性校验方案**：光鸭无官方分享查询接口，是否接受「本地元数据 + B库删除事件驱动」作为「校验」的替代语义？还是授权本地抓包取证网页端是否有可用的有效性检查接口？（不得虚构接口）
2. 提取码（share_code）：当前建分享显式关闭。是否需要开启？若需要，需确认光鸭 `share_file` 的 `enableShareCode=true` 真实返回结构（需抓包）。
3. 跨季（S01/S02 分目录）是否算「一部剧」分享一次——现状 `series_dir_info` 取剧名目录为整剧目录，倾向一次。
4. 前端分享中心拆 5 页时，是否保留旧 key `推送队列`（兼容既有测试断言）还是统一迁移为新 key。
5. 存量大批量发布的默认节奏参数（每日部数、释放间隔）——现状默认 0（不限），建议给出保守默认值避免一次性压满网盘。

---

> 附注：本报告全部 `[事实]` 取自 v1.4.4（HEAD `3f7b60c`）实际源码快照；`[推断]`/`[缺口]`/`[假设]` 已标注推理链与不成立应对。未修改任何业务代码。下一步应先就第 10 节第 1 项拍板，再进入阶段二。
