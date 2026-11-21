# Ya-TG 分享中心全面重构 — 第一阶段现状审计报告

## 0. 审计基线与边界

- 审计对象：`ya-tg` 当前工作树，HEAD `19de549`，tag `v1.5.2`，线上最近发布版本 v1.5.2。
- 审计方式：只读审计。未修改业务代码，未执行生产数据清理，未运行发版流程。
- 审计范围：数据库模型与迁移、分享中心/分发链路、资源库/镜像/TMDB、调度器/队列/锁、前端/文档/测试五路。
- 计划依据：本次以用户最新明确需求中的《分享中心重构开发计划书》要点为准；仓库内 `RESOURCE_SHARE_REDESIGN_AUDIT.md`、`RESOURCE_SHARE_REDESIGN_PLAN.md`、`FOUR_MODULE_REFACTOR_DESIGN.md` 仅作为历史基线，不替代新计划。
- 用户新增口径（2026-11-21 15:53）：**旧库数据不用管，新模块上线后全部使用新数据**；**消息推送时间间隔必须按频道、群分别可调，且最低值不得低于 Telegram 风控要求**。
- 标注纪律：`事实` 为源码可验证；`推断` 为基于事实的设计判断；`待核实` 为源码无法证明或需用户/真机确认。

---

## 1. 结论先行

**判断：必须重构，且不能把现有分享中心当作新分享中心直接延续。** 现有模块本质上只是“`share_tasks` 状态机 + `share_records` 真相源 + 分发交接字段 + 5 秒全量轮询 `run_cycle`”的旧发布生命周期模块。新计划要求的“推送清单、三业务视图、TMDB 分类唯一主归属、08:00–24:00 小时计划与日/周/月四级计划、分类独立游标、资源版本防重复、10 分钟更新合并、分享状态核验、任务锁与幂等恢复、OpenAI 周度优化”大部分在源码中不存在或只是近似能力。

**优先级：P0，继续推进第二阶段。** 但推进方式不是推倒所有底层能力，而是：

- **保留底层可信能力**：TG 发送核心、事件幂等、队列原子领取、防死锁线程化、链接 8 态核验、跨季作品键、光鸭目录分享解析。
- **推翻旧业务边界**：三套建分享、三套作品键、旧链影子登记、固定秒级轮询、资源库算目标/手动直入队、前端 5 个子标签但无清单/计划/游标/AI 的信息架构。
- **新增新中枢能力**：推送清单、分类归属、四级计划与版本、分类游标、资源版本快照、统一执行记录、AI 调度审计，以及频道/群独立可调的推送节奏与 Telegram 风控下限。

**一句话边界**：资源库给“资源事实”，镜像同步给“A/B 存在与映射事实”，TMDB 给“元数据与分类事实”，分享中心给“发布清单、计划、状态与调度事实”，分发与互动中心给“消息发送事实与发送节奏”。当前代码在“谁决定推给谁、谁建分享、谁算作品键、谁消费事件”四处越界，是最重要的改造缝。

**新增口径影响**：旧库不再需要兼容迁移，第二阶段直接以新表、新字段、新数据为准；频道与群不得再共用同一个全局发送间隔，必须在分发中心落地“按目标类型的可调间隔 + 官方下限夹逼 + 429 退避”三层控制。

---

## 2. 现有架构事实

### 2.1 数据库与迁移

**事实：核心表已经具备一部分底座，但没有新计划的中枢表。**

核心表与位置：

| 领域 | 表/模型 | 关键事实 |
|---|---|---|
| 资源事实 | `resources`（`backend/app/models.py:11-63`） | `file_id` 唯一；含 `path_hint/dir_chain_json/parent_id/share_dir_id/share_scope`、`tmdb_id/media_type/media_title/genres/origin_country/season/episode/poster_path/share_*` |
| 资源版本 | `resource_versions`（`models.py:443-455`） | 记录 `resource_id/work_key/fingerprint/old_fingerprint/change_type/size/utime`，当前是文件级 `size:utime` 版本，不是作品级聚合版本 |
| 扫描断点 | `resource_scan_batches`、`scan_cursors`（`models.py:278-293`、`78-96`） | 扫描批次与断点持久化，支持重入 |
| 镜像映射 | `mirror_rules/mirror_dirs/mirror_media/mirror_files/mirror_tasks/mirror_events`（`models.py:296-440`、`644-662`） | A/B 目录与文件映射，含 `category_path/work_key/identity/fingerprint/status/rename_status/batch_id/sequence_id` |
| 分享任务 | `share_tasks`（`models.py:561-601`） | 9 态、4 类 `kind`、`dedup_key` 唯一（`models.py:601`）、`channel_targets`、`queue_type`、`building_at` |
| 分享真相源 | `share_records`（`models.py:604-634`） | 当前记录用部分唯一索引 `uq_share_record_current` 保证一个 `work_key` 只有一个 `current_flag=1`（`models.py:632-634`）；含 `link_state/verified_result/verify_fail_count/clean_state` |
| 推送队列 | `push_queue`（`models.py:217-237`） | `resource_id+channel_id` 唯一；含 `status/status_ext/attempts/next_retry_at/accept_worker/accepted_at/share_task_id` |
| 分发事件 | `distribution_events`（`models.py:458-472`） | `work_key+fingerprint` 唯一；含 `share_task_id/push_task_ids`，`queue_id=0` 仍被当待处理标记使用 |
| 运行配置 | `app_config`（`models.py:250-255`） | 所有运行时配置以密文字符串存储，`runtime_config.DEFAULTS` 白名单 88 项 |

**事实：迁移机制是 `create_all + ALTER 补列 + marker 数据迁移`，不是完整迁移框架。**

- `db.py:452-453`：`Base.metadata.create_all(bind=engine)` 只新建不存在的表和索引。
- `db.py:56` 的 `_COLUMNS` 逐表补列，`db.py:456-463` 用 `inspect()` 检查后执行 `ALTER TABLE ... ADD COLUMN`。
- 数据迁移用 `AppConfig` marker 保护，如 `db.py:404-449`。
- 源码中无 alembic、无 schema version、无 downgrade。
- SQLite 未开启 `PRAGMA foreign_keys`；外键多为声明性约束。

**推断：新增索引/唯一约束对老库不会自动生效。** 如果第二阶段在既有表上新增唯一键或索引，必须在 `migrate()` 中补显式索引创建；不能只改模型。

**关键默认配置事实：**

- `runtime_config.py:120`：`share_center_enabled=true`
- `runtime_config.py:125`：`resource_decoupled_enabled=false`
- `runtime_config.py:128`：`publish_paused=false`
- `runtime_config.py:132`：`share_building_timeout_minutes=5`
- `runtime_config.py:153-154`：TG HTTP 8s、单条硬上限 12s
- `runtime_config.py:173`：`mirror_mode=full`

### 2.2 分享中心现状

**事实：状态机存在，但 `publishing` 没有真实写入点。**

- 状态常量：`share_center.py:33-41`，9 态为 `planned/building/ready/submitted/accepted/publishing/published/failed/cancelled`。
- 优先级：`share_center.py:43`，`force=30 > first=20 > update=10 > restore=5`。
- `ST_PUBLISHING` 只在 `share_center.py:38` 定义，并在 `main.py:2282` 作为对账活跃态使用；源码中没有写入该状态的业务点。
- `status_ext` 实际写入态为空、`submitted`、`accepted`、`cancelled`；模型注释中的 `publishing/done` 没有落地。

**事实：评论/更新/删除/重试的幂等方式已成型。**

- `_dedup_key`：`share_center.py:221-227`。first 按 `work_key`、update/restore 按 `kind:event_id`、force 不去重。
- `ensure_task`：`share_center.py:230-311`，first 命中 cancelled 可复活，避免重建重复任务。
- `build_share`：`share_center.py:314-401`，建分享前先提交 building 态，避免跨 await 持写事务。
- `submit_to_dispatch`：`share_center.py:432-461`，写 `QueueItem.status=pending, status_ext=submitted`。
- `claim_tasks`：`share_center.py:464-514`，用条件 UPDATE 原子领取，按 `accept_worker` 回查归属。
- `mark_result`：`share_center.py:531-543`，回写 published/failed。
- `run_cycle`：`share_center.py:546-684`，building 超时自愈、候选扫描、退避、建分享、门禁、节奏顺延、提交分发。

**事实：链接核验是本地事件推断，不是光鸭实时状态接口。**

- 8 态：`share_link.py:32-45`。
- 删除事件 → suspicious：`share_link.py:79-102`，由 `share_center.py:739-755` 调用。
- 二次确认：失败次数与间隔满足才 inactive（`share_link.py:105-133`）。
- 清理：`share_link.py:150-168`，只置 `cleaned` / `current_flag=0`，不删 B 库。
- 发布门禁：`share_center.py:184-218`，非 force 要求 identified + 海报，update/restore 必须有 active 链接且 `link_state` 可用。

**事实：跨季一剧一链接依赖作品键和目录层级。**

- `utils.work_group_key`：`utils.py:197-224`，季目录/通用目录名会被丢弃，改取剧名母目录，刻意把 S01/S02 聚合到同一 `work:xxx`。
- `utils.series_dir_info`：`utils.py:525-583`，`剧名/Season N/单集` 时取剧名目录作为分享粒度。
- 分享链接复用：`share_center.py:331-360` 先查 current share，再按 share_dir 找兄弟任务/资源复用。

**事实：当前 API 只是旧分享中心管理面。**

已存在：`/api/share/run`、`/center/status`、`/tasks`、`/tasks/{id}/cancel|rebuild`、`/schedule`、`/records`、`/followups`、`/publish-overview`、`/rules`、`/link/{work_key}`、`/link/review`、`/link/cleanup`。对应 `main.py:4254-4411`、`:4323-4361`。这些接口没有计划/清单/游标/版本/AI 语义。

### 2.3 资源库、镜像与 TMDB

**事实：资源库是 B 库扫描事实来源，A 库只读。**

- 扫描编排：`main.py:2377-2406` `_refresh_library_from_b`，顺序是 `run_scan → _reconcile_library → _post_scan_identify → process_change_events`。
- 单文件 upsert：`scanner.py:57-117`，`size:utime` 变化则 `revision+=1`，写 `AuditLog`。
- 内容变化：`scanner.py:120-148`，按 `work_key` 合并落 `ResourceVersion` + `DistributionEvent`。
- 识别落库：`main.py:3007-3051` 把 TMDB 元数据写入 `resources`。
- TMDB 元数据：`tmdb.py:256-314` 产出中文标题、genres、origin_country、finish_status、海报、总集数。
- 海报：`tmdb.py:659-664` + `models.py:39`，无海报是当前入队硬闸门之一。
- 英文标题：`tmdb.py:355-367`，`mirror.py:1751-1874` 串起缓存 → Resource → TMDB en-US → AI → pinyin。

**事实：镜像 1.1 负责 A/B 存在与映射，不写队列。**

- 红线：`mirror.py:1-40`，A 只读、B 才处理、禁先删旧再复制、mirror 不写 push_queue。
- full/half：`mirror.py:308-311`、`417-439`；full 在 A 缺失且 complete 时清映射并删 B，half 保留 B 文件与清单。
- prepass：`mirror.py:353-485`，B 类整部缺失进 `reprocess_keys`，缺集进 `missing_keys`。
- 缺集优先补丁：`mirror.py:1255-1282`，`missing_keys` 不占 `max_works`。
- 批量目录复制：`mirror.py:688-884` 严格引擎按目标目录分桶，`COPY_BATCH_SIZE=50`；非严格路径在 `mirror.py:1505-1548`。
- 批量改名：`mirror.py:1989-2384`，按部收敛，写 `target_name/rename_status`。

**推断：可复用 `MirrorMedia.category_path` 做分类映射，但不能把它当 TMDB 分类真相。** 它是 B 库目录路径，适合作为分享路由/目录映射维度；新计划的“TMDB 分类与唯一主归属”应从 TMDB 元数据生成稳定分类键。

### 2.4 调度器、队列与锁

**事实：调度器已是 Asia/Shanghai，但只有固定秒级/粗粒度 cron。**

- `main.py:44`：`AsyncIOScheduler(timezone="Asia/Shanghai")`。
- `main.py:641-642`：`share_center` 每 5 秒跑一轮。
- `main.py:639`：`queue_worker` 默认 30 秒，可切 cron。
- 其他 job：global_scan、mirror_sync、token_refresh、bot_poll、contribution_restore、rename_pending、mirror_task_reap、mirror_task_poll、log_prune，均集中在 `main.py:587-666`。
- `docker-compose.yml` 已有 `restart: unless-stopped`、`TZ: Asia/Shanghai`、`./db:/app/db` 数据卷。

**事实：七次死锁后形成的防死锁底座是有效资产，必须保留。**

- `_TimedMirrorLock`：`main.py:47-165`，带心跳、超时接管、fencing token。
- `_queue_lock`：`publisher.py:647`，非阻塞锁冲突返回独立 `queue_busy`，不再污染 `skipped` 数组契约（`publisher.py:650-659`）。
- `to_thread` 防写锁冻结：`main.py:581/584`、`publisher.py:456`、`main.py:276/374/422`。
- 跨 await 前提交：`share_center.py:326-329`、`publisher.py:504-510/553-556/576/637-638`。
- building 超时自愈：`share_center.py:560-578`。
- 队头扩大取样：`publisher.py:470-486`，`sample=max(limit*10,200)`，可消费优先。
- TG 单条硬上限：`publisher.py:579-586`，默认 12 秒；HTTP 默认 8 秒。

**事实：当前发送节奏是全局单值，不是频道/群分型可调。**

- `runtime_config.py:51`：`telegram_send_delay=2`，所有目标共用；`publisher.py:423` 每条发送前统一 sleep。
- 源码中没有按 `channels.kind`、`chat_id` 正负或 Bot 维度设置发送间隔的表、配置项或 API。
- Telegram 官方 Bot FAQ（https://core.telegram.org/bots/faq ，2026-11-21 检索）给出的免费 Bot 风控底线是：单个 chat 避免超过 1 条/秒；群聊不超过 20 条/分钟；批量广播约 30 条/秒，超出会收到 429。付费广播提升上限需额外条件，本项目不使用。
- 因此当前 `2s` 全局值对频道/单聊尚可留出余量，但对群聊（20 条/分钟＝3s/条下限）仍偏快，新模块必须按目标类型设下限并允许调大。

**事实：队列交接是 DB 条件 UPDATE，不是进程内锁。**

- `claim_tasks`：`share_center.py:464-514`，`status_ext=submitted → accepted`，`accept_worker` 回查。
- `publish_resource` 手动推送直接写 `status_ext=submitted`（`main.py:4500-4510`），再调用 `process_queue(limit=50)`。
- 当前没有 accepted 超时未 sent 的定时自愈，只靠手动重试/重发端点重置。

**推断：新四级调度可以复用 `AsyncIOScheduler + 条件 UPDATE + 超时自愈` 范式，但不能继续用“固定 5 秒全扫候选”当执行核心。** 否则分类游标、计划版本、小时窗口和 AI 建议都无法形成稳定语义。

### 2.5 前端、文档与测试

**事实：前端没有 Mock 与业务态 localStorage，分享中心 10 个按钮均接真实后端。**

- 分享中心实际是导航 key `推送队列`、label `分享中心`：`App.vue:555`。
- 页面 section：`App.vue:2887` 起；5 个子标签 `overview/queue/manage/push/rules`：`App.vue:48`、`2890-3017`。
- 按钮 API：`share/run`、`share/tasks/{id}/rebuild|cancel`、`share/link/*`、`queue/{id}/retry`、`share/rules`、`share/schedule`。
- `localStorage` 只用于会话 token、tab、theme、清理旧密码；没有分享业务状态。
- 定时器只用于时钟、会话倒计时、镜像进度轮询、扫码授权、API 超时；没有前端调度业务。

**事实：前端当前不具备新计划的信息架构。**

- 没有推送清单页、分类管理页、小时排期页、日/周/月计划页、AI 智能调度页。
- 现有“日/周/月”是 `/push-stats` 报表统计维度，不是计划。
- 现有 AI 配置只服务 TMDB 识别/翻译兜底，不参与调度。
- 分享中心无服务端分页；仅固定 `limit`（tasks 200、records 100）+ 前端 computed 筛选。
- 分享中心无批量勾选/批量操作；批量能力在资源库 `/api/library/batch`。

**事实：测试资产厚，但以静态字符串守卫为主。**

- 分享中心核心测试：`test_v106_share_center.py`、`test_v106_dispatch_handoff.py`、`test_v106_share_api.py`、`test_v106_share_schema.py`、`test_v110_four_module_mainchain.py`、`test_v147_share_lifecycle.py`、`test_v152_link_lifecycle.py`、`test_v153_publish_center_frontend.py`。
- `test_v153_publish_center_frontend.py:188-202` 的“5 页守卫”只是字符串断言，前端改名不会触发业务失败。
- 没有浏览器 E2E；新计划要求前后端真实联调，必须补接口级集成测试与 Docker 真机验证。

---

## 3. 新计划能力差距矩阵

| 新计划能力 | 当前源码事实 | 判定 | 进入第二阶段动作 |
|---|---|---|---|
| 推送清单是基础 | `push_queue` + `share_tasks.channel_targets` + `distribution_events.push_task_ids` 是执行/交接态，不是不可变清单 | **缺位** | 新增推送清单模型，区分“清单项/执行项/结果项”；清单由计划版本生成 |
| 首次分享、更新推送、已发布三个业务视图 | 现有 5 个子标签按旧生命周期划分；有 `kind=first/update` 与 `published` 状态 | **近似可用但需重做** | 前端三视图重排；后端提供统一聚合查询，不再让前端拼多接口 |
| TMDB 分类与唯一主归属 | `Resource.genres/origin_country/media_type` 存在；`MirrorMedia.category_path` 是 B 目录 | **缺位** | 定义稳定主分类算法；新增作品分类归属表或字段，确保一作品一主归属 |
| 08:00–24:00 小时计划 | 仅有 `share_release_interval_seconds` 与 queue interval/cron | **缺位** | 新增小时计划表、窗口校验、序列化执行规则 |
| 日/周/月四级计划 | 无计划表；前端日/周/月只是报表 | **缺位** | 新增统一计划表，以计划类型 + 周期表达式 + 版本管理 |
| 分类独立游标 | `mirror_rules.works_cursor/cursor_sequence` 属镜像；分享中心无分类游标 | **缺位** | 新增 `category_cursor`，按计划版本独立推进 |
| 资源版本与防重复推送 | `ResourceVersion` 是文件级；`DistributionEvent` 用 `work_key+fingerprint` 去重 | **部分可复用** | 新增作品级资源版本；清单项绑定版本，已推送版本不得重复 |
| 更新事件合并与 10 分钟窗口 | 现有事件按 `idem_key` 去重，update 按 event_id 建任务；无 10 分钟合并窗口 | **缺位** | 新增更新合并窗口与聚合事件；不得复用链接失效的 30 分钟确认逻辑 |
| 分享状态核验 | `share_link.py` 8 态 + 删除事件本地推断，已可用 | **复用** | 接入新分享状态机；保留“不虚构光鸭状态接口”红线 |
| 任务状态机、任务锁、幂等、重启恢复 | 9 态 + dedup + 条件 UPDATE + to_thread + 超时自愈成熟 | **复用并扩展** | 增加计划版本、执行记录、租约与 accepted 超时自愈 |
| OpenAI 周度优化、审核、自动应用、本地降级 | `ai_assist.py` 仅识别/翻译辅助；无调度优化、审核、自动应用 | **缺位** | 第五阶段新增 AI 建议表、审核接口、自动应用开关、本地降级 |
| 前后端真实联调与 Docker 验证 | 当前无 E2E；Docker 基础能力具备 | **待补** | 第六阶段补接口集成测试、Docker 重启恢复验证 |
| 频道/群推送间隔可调并守住 TG 风控底线 | `telegram_send_delay=2` 是全局单值（`runtime_config.py:51`），无按目标类型配置；群聊官方底线约 3s/条 | **缺位且当前群聊偏快** | 新增 `push_pacing_rules`：频道默认 2s/下限 1s，群默认 4s/下限 3s；支持按 Bot/chat 覆盖；429 retry_after 优先级高于静态间隔 |
| 禁止 Mock/localStorage/前端定时器/AI 直接推送 | 当前无这些问题 | **保持** | 新代码必须继续禁止；AI 只能产出建议，不得直接调用推送端点 |

---

## 4. 模块边界重划基线

| 模块 | 新职责 | 当前必须停止的越界 |
|---|---|---|
| 资源库 | B 库扫描、识别、资源事实、作品级版本、资源事件；保留手动推送入口但不再直写新清单以外的执行态 | 停止把 `resources.share_*` 当真相源；停止在资源库侧计算并固化推送目标 |
| 镜像同步 | A/B 映射、full/half 删除语义、prepass 增量/缺集/失效、目录级复制与批量改名；完成事件只表达资源存在变化 | 不写 push_queue，不决定分享计划，不做推送目标选择 |
| 分享中心 | 推送清单、分类归属、计划版本、四级计划、分类游标、任务状态机、链接核验、发布调度、执行记录 | 停止固定 5 秒全扫当“计划”；停止依赖分发中心反驱 `run_cycle`；停止三套建分享 |
| 分发与互动中心 | 白名单、路由、消息模板、QueueItem 发送、TG 发送核心、按频道/群可调发送间隔、TG 风控下限、429 退避、互动指令；保持发送核心稳定 | 不再反向调用分享中心生产逻辑；不接管清单与计划；不得用单一全局 delay 覆盖群聊节奏 |
| AI 优化层 | 周度统计摘要、计划参数建议、人工审核、自动应用开关、本地降级；输出建议与审计记录 | 不得直接执行建分享/入队/发送；不得把 API Key 返回前端或写普通日志 |
| 前端 | 管理入口与只读可视化；所有状态来自后端 | 不用 localStorage/定时器承载业务计划；不做前端排程 |

---

## 5. 复用 / 扩展 / 新增 / 废弃清单

### 5.1 直接复用

1. TG 发送核心：`publisher.py:92-377`、`418-660`。不得改动 `_send_telegram/_build_message/process_queue` 的业务语义。
2. 交接四接口：`submit_to_dispatch`、`claim_tasks`、`share_url_for_queue_item`、`mark_result`。
3. 幂等底座：`events.emit_resource_event` 的 `idem_key`、`share_tasks.dedup_key`、`DistributionEvent.work_key+fingerprint`、`ProcessedUpdate`。
4. 防死锁底座：`to_thread` + 独立 Session、跨 await 前 commit、`_queue_lock` 非阻塞冲突、TG 双层超时、building 超时自愈、毒事件内存计数。
5. 链接 8 态核验：`share_link.py:32-180`。
6. 跨季作品键：`utils.work_group_key` 与 `utils.series_dir_info`。
7. 光鸭分享解析：`create_share_parsed` / `parse_share_result`。
8. 调度时区与 Docker 持久化：`AsyncIOScheduler(timezone="Asia/Shanghai")`、`restart: unless-stopped`、db 卷。

### 5.2 扩展

1. `consume_resource_events`：扩展处理 `RESOURCE_CREATED/FILE_CHANGED/RENAMED/MOVED/DELETED`，当前只真正处理 identified/restored/removed/deleted。
2. `can_publish`：扩展计划版本、分类状态、资源版本校验，而不仅是 identified/海报/link_state/配额。
3. `claim_tasks`：扩展执行记录、租约、accepted 超时自愈。
4. `publish_overview`：扩展为推送清单/计划/游标/分类的后端聚合总览。
5. `ai_assist.py`：从识别辅助扩展为调度建议能力，但不得进入发送路径。
6. 迁移机制：旧库兼容不再是目标，但新表必须在首次 `create_all` 时一次建对唯一约束；如后续新增索引，按新数据口径补显式创建。
7. `publisher` 发送循环：保留发送核心，但把单一 `telegram_send_delay` 替换为“按目标类型取间隔 → 官方下限夹逼 → 429 retry_after 覆盖 → 成功/失败后记录最后发送时间”的节奏服务。

### 5.3 必须新增

1. 推送清单表及清单项表。
2. 分类归属表与唯一主分类规则。
3. 四级计划表、小时窗口表、计划版本表。
4. 分类游标表。
5. 作品级资源版本表或资源事实快照表。
6. 更新合并窗口记录表。
7. 统一执行记录表/任务租约表。
8. AI 调度建议表、审核记录表、自动应用审计。
9. `push_pacing_rules`：频道/群可分别配置默认间隔、最小间隔、最大发送速率、抖动、`retry_after` 策略，并支持按 Bot/chat 覆盖。
10. 新 API：清单查询/生成/确认，分类管理，计划 CRUD/启停，游标查询，AI 建议列表/审核/应用，任务详情与批量操作，推送节奏规则查询/更新。
11. 新测试：接口集成、幂等、计划版本回滚、分类游标、10 分钟合并、AI 降级、Docker 重启恢复、频道/群间隔下限与 429 退避。

### 5.4 废弃或降级

1. 三套建分享：`main.create_share`、`publisher.ensure_share`、`share_center.build_share` 并存；新中心应统一到一个发布引擎，旧入口只保留兼容兜底。
2. 三套作品键：`publisher._work_group_key` 自造 `dir:/tmdb:/file:` 键废弃，统一 `utils.work_group_key`。
3. `resources.share_*` 作为真相源降级为缓存；`ShareRecord` 是链接真相源。
4. 旧链 `status_ext=""` 与影子登记 `shadow_register` 在完成切换后废弃。
5. 固定 5 秒 `share_center` 全量轮询降级为兜底，不再承担计划语义。
6. `ST_PUBLISHING` 要么补真实写入与超时，要么明确只是对账保护常量，不能继续半实现。

---

## 6. 已发现的关键风险

1. **依赖倒置（P0）**：`publisher._process_queue_impl` 内调用 `_run_share_center_in_thread`，消费者反驱生产者。新调度必须由分享中心自己的调度 job 驱动，分发中心只消费已 accepted 的队列项。
2. **三套建分享/三套作品键（P0）**：会导致同一作品在清单、队列、消息统计中口径不一致，防重复推送无法可靠建立。
3. **迁移机制缺口（P0）**：旧库兼容已不是目标，但新表若首次建表时约束设计错，后续 `migrate()` 仍不会自动补索引/唯一约束；第二阶段必须一次建对并加迁移测试。
4. **无计划版本与分类游标（P0）**：无法回答“这一轮按哪个计划、哪个版本、哪个分类游标执行”，也无法安全回滚。
5. **10 分钟更新合并缺失（P1）**：现有 fingerprint 去重是事件级，不是时间窗聚合；大量连续更新可能产生多次 update 任务。
6. **accepted 无超时自愈（P1）**：领取后卡住只能人工重置；新任务锁必须带租约和超时回收。
7. **前端静态守卫不足（P1）**：当前测试只验证字符串存在，页面重构不会触发业务回归；需补 API 契约与 E2E。
8. **AI 边界风险（P1）**：现有 AI 仅识别辅助；新 AI 调度若直接写发布/队列，会击穿“AI 只是优化工具”的红线。必须只写建议表，经审核或显式自动应用开关后才影响计划。
9. **发送节奏风险（P0）**：当前全局 `2s` 对群聊低于 Telegram 官方约 `3s/条` 的免费 Bot 底线；新模块若只做“可调”但允许用户把间隔降到 0，或只按频道不按群限流，会继续触发 429。必须把官方下限写进后端强校验，不允许前端或配置项绕过。

---

## 7. 进入第二阶段的前置决策与建议默认值

以下不是源码事实，是进入数据库设计前需要固化的设计口径。为保证推进速度，给出建议默认值；若用户后续明确改口径，以用户口径为准。

1. **分类真相源**
   - 建议：TMDB `genres[0]` 归一后作为唯一主分类；`media_type` 作为一级维度（电影/剧集），`origin_country` 作为标签而非主分类；`MirrorMedia.category_path` 只做目录映射。
   - 理由：可稳定复现，避免 B 目录改名或人工移动影响分类归属。
   - 待核实：用户计划中的“分类管理”是否指自定义分类实体，而非 TMDB genre。

2. **推送清单粒度**
   - 建议：清单项 = `work_key + channel_target + resource_version_scope + list_version`。计划生成的是不可变清单快照；执行队列项引用清单项。
   - 理由：能同时支持“同剧同渠道不重复”“资源更新后生成新版本”“已发布视图可追溯”。

3. **计划版本**
   - 建议：每次启用/保存计划生成不可变 `plan_version`；执行记录绑定版本；回滚只切回旧版本，不改写历史执行。

4. **分类游标**
   - 建议：游标按 `plan_id + plan_version + category_key` 独立保存，支持 `last_work_key/last_resource_version/last_success_at`。

5. **更新合并窗口**
   - 建议：严格按新计划 10 分钟；窗口内同 `work_key` 的多个变化合并为一个 update 意图， fingerprint 取最新且保留原始事件列表。

6. **AI 应用边界**
   - 建议：AI 只生成周度建议；默认只进审核队列；自动应用默认关闭；本地降级用纯统计规则，不调用任何推送端点。

7. **旧数据处理（用户已明确，不再是建议）**
   - 用户口径：旧库数据不用管，新模块上线后全部使用新数据。
   - 结论：第二阶段不写旧数据迁移脚本、不做旧 `ShareTask/ShareRecord/QueueItem` 归档兼容；只新增新表与新服务。旧表暂不物理删除，避免影响当前线上运行，但新模块不得读取或依赖它们。
   - 注意：这不等于允许删生产库；用户只是确认“新模块用新数据”，任何物理删除仍需单独授权。

8. **频道/群推送节奏（用户新增要求）**
   - 事实依据：Telegram Bot FAQ（https://core.telegram.org/bots/faq ，2026-11-21 检索）：单 chat 避免超过 1 条/秒；群聊不超过 20 条/分钟；批量广播约 30 条/秒，超出返回 429。
   - 建议默认：频道 `default_interval_seconds=2`、`min_interval_seconds=1`；群 `default_interval_seconds=4`、`min_interval_seconds=3`；其他私聊按单 chat 底线 `min=1`。
   - 强约束：用户可把间隔调大，不得调到官方下限以下；`retry_after` 必须覆盖静态间隔；全局并发仍需保证总发送速率不超过约 30 条/秒。
   - 配置粒度：全局默认值 + 按 Bot/chat 覆盖；前端只展示与提交配置，真正限流在分发中心执行。

---

## 8. 第一阶段完成判定

本次已完成计划书第十五章第一步“现有代码审计”，产出：

- 五路只读审计：数据库、分享/分发链路、资源/镜像/TMDB、调度/队列/锁、前端/文档/测试。
- 当前架构事实清单与行号证据。
- 新旧能力差距矩阵。
- 模块边界重划基线。
- 复用 / 扩展 / 新增 / 废弃清单。
- 进入第二阶段的设计口径建议。
- 用户新增口径：旧库不兼容、新模块用新数据；频道/群推送间隔分型可调并设 Telegram 风控下限。

**未完成且不得在本次声称完成的项：**

- 未新增数据库表与迁移。
- 未实现推送清单、四级计划、分类游标、AI 调度。
- 未改前端页面。
- 未跑发版。
- 未动生产数据。

下一步应进入第二阶段：先做数据库设计与幂等迁移，再做业务服务与 API；前端不得先于后端开工。
