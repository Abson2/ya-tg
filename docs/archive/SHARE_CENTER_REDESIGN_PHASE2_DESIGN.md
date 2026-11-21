# 分享中心全面重构 — 第二阶段设计（新数据体系）

## 0. 用户口径与设计边界

### 0.1 已固化口径

1. **旧库数据不用管**：新模块上线后全部使用新数据。
2. **消息推送间隔必须可调**：频道、群分别配置。
3. **最低底线是 Telegram 风控要求**：用户可调大，不得调低于官方限制。
4. 资源库、镜像同步、分发与互动中心继续各司其职；分享中心只负责发布清单、计划、调度、状态与结果。
5. AI 只做优化建议，不直接建分享、不入队、不发送。

### 0.2 本阶段不做

- 不写旧 `ShareTask/ShareRecord/QueueItem/DistributionEvent` 数据迁移脚本。
- 不做旧数据归档、兼容读取、双写旧表。
- 不清理生产数据，不物理删除任何现有表。
- 不改 TG 发送核心 `_send_telegram/_build_message/process_queue` 的业务语义，只在其外层增加节奏控制服务。
- 不做前端页面。

### 0.3 Telegram 官方限流依据

来源：Telegram Bots FAQ，https://core.telegram.org/bots/faq ，2026-11-21 检索。

官方口径：

- 单个 chat：避免超过 1 条/秒；短时突发可能被允许，最终会收到 429。
- 群聊：Bot 不超过 20 条/分钟。
- 批量广播：免费额度约 30 条/秒，超出返回 429。
- 付费广播可提高上限，但本项目不使用，不纳入设计默认值。

换算为本项目下限：

| 目标类型 | 官方底线 | 本项目硬下限 | 本项目默认间隔 |
|---|---:|---:|---:|
| 频道/单聊 `channel/private` | 1 条/秒 | `1.0s` | `2.0s` |
| 群聊 `group/supergroup` | 20 条/分钟 | `3.0s` | `4.0s` |
| 全局广播聚合 | 约 30 条/秒 | 并发发送必须保证不超 | 发送串行或受控并发 |

说明：频道在 Telegram 侧属于单 chat 发送语义，按 1 条/秒设硬下限；项目若发现某类频道有更严风控，可按 chat 覆盖调大，但不得调小到硬下限以下。

---

## 1. 新数据体系总览

新模块采用全新表，表名统一加 `sc_` 前缀，避免与旧表混淆。

| 域 | 新表 | 用途 |
|---|---|---|
| 分类 | `sc_categories` | TMDB 分类字典与启用状态 |
| 分类 | `sc_work_categories` | 作品唯一主归属 |
| 资源版本 | `sc_library_versions` | 作品级资源版本快照 |
| 清单 | `sc_push_lists` | 推送清单快照 |
| 清单 | `sc_push_list_items` | 清单项：作品 + 资源版本 + 推送目标 |
| 计划 | `sc_share_plans` | 小时/日/周/月计划定义 |
| 计划 | `sc_share_plan_versions` | 不可变计划版本 |
| 计划 | `sc_hour_windows` | 08:00–24:00 小时窗口配置 |
| 游标 | `sc_category_cursors` | 计划版本 × 分类独立游标 |
| 更新合并 | `sc_update_windows` | 10 分钟更新合并窗口 |
| 任务 | `sc_publish_tasks` | 发布任务状态机与租约 |
| 执行 | `sc_publish_executions` | 每阶段执行记录 |
| AI | `sc_ai_suggestions` | 周度优化建议 |
| AI | `sc_ai_apply_logs` | 审核/自动应用日志 |
| 节奏 | `sc_push_pacing_rules` | 频道/群/Bot/chat 分级间隔配置 |
| 节奏 | `sc_push_pacing_state` | 每目标最后发送时间与 429 状态 |

---

## 2. 表结构定义

### 2.1 分类字典 `sc_categories`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | Integer | PK | 主键 |
| `category_key` | String(128) | NOT NULL, UNIQUE | 稳定分类键，如 `genre:action`、`media:tv` |
| `name` | String(128) | NOT NULL | 中文展示名 |
| `source` | String(32) | NOT NULL, default=`tmdb` | `tmdb/manual/system` |
| `media_type` | String(16) | default=`` | 适用 `movie/tv`，空为通用 |
| `enabled` | Boolean | default=True | 是否参与计划 |
| `priority` | Integer | default=0 | 同级排序权重 |
| `created_at` / `updated_at` | DateTime | NOT NULL | 时间戳 |

唯一键：`uq_sc_categories_key(category_key)`。

### 2.2 作品唯一主归属 `sc_work_categories`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | Integer | PK | 主键 |
| `work_key` | String(255) | NOT NULL, UNIQUE | `utils.work_group_key` 产出的作品键 |
| `category_key` | String(128) | NOT NULL | 唯一主分类 |
| `media_type` | String(16) | default=`` | `movie/tv` |
| `source` | String(32) | default=`tmdb` | `tmdb/manual/ai/fallback` |
| `confidence` | Float | default=1.0 | 归属置信度 |
| `assigned_at` | DateTime | NOT NULL | 分配时间 |

唯一键：`uq_sc_work_category_work(work_key)`，保证一作品一主归属。

分类算法：

1. `media_type` 作为一级维度进入 `category_key` 前缀或独立筛选维度。
2. TMDB `genres` 非空时取归一化后的首个 genre 作为主分类。
3. `genres` 为空时使用 `category_key=genre:unknown`。
4. 人工指定优先于 TMDB；AI 建议必须经审核后才可写入。
5. B 库 `category_path` 只作为目录映射，不参与主归属计算。

### 2.3 作品级资源版本 `sc_library_versions`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | Integer | PK | 主键 |
| `work_key` | String(255) | NOT NULL | 作品键 |
| `version_no` | Integer | NOT NULL | 作品内递增版本号 |
| `fingerprint` | String(128) | NOT NULL | 作品级聚合指纹 |
| `media_type` | String(16) | default=`` | 类型 |
| `resource_ids_json` | Text | NOT NULL | 该版本包含的资源 ID 列表 |
| `metadata_json` | Text | NOT NULL | 标题/季集/TMDB/海报等快照 |
| `change_type` | String(32) | default=`created` | `created/updated/removed/restored` |
| `created_at` | DateTime | NOT NULL | 创建时间 |

唯一键：`uq_sc_library_version(work_key, version_no)`、`uq_sc_library_fingerprint(work_key, fingerprint)`。

指纹规则：对该作品下所有资源按 `file_id` 排序，聚合 `file_id:size:utime` 后取 SHA256；只表达 B 库资源事实，不依赖旧 `resource_versions`。

### 2.4 推送清单 `sc_push_lists`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | Integer | PK | 主键 |
| `list_key` | String(128) | NOT NULL, UNIQUE | 清单唯一键 |
| `name` | String(255) | NOT NULL | 名称 |
| `status` | String(32) | default=`draft` | `draft/active/archived` |
| `source` | String(32) | default=`plan` | `plan/manual/ai_reviewed` |
| `plan_version_id` | Integer | nullable | 来源计划版本 |
| `created_at` / `activated_at` | DateTime | nullable | 时间 |

### 2.5 清单项 `sc_push_list_items`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | Integer | PK | 主键 |
| `list_id` | Integer | NOT NULL, FK→`sc_push_lists.id` | 所属清单 |
| `work_key` | String(255) | NOT NULL | 作品键 |
| `library_version_id` | Integer | NOT NULL, FK→`sc_library_versions.id` | 绑定资源版本 |
| `category_key` | String(128) | NOT NULL | 主分类 |
| `target_type` | String(16) | NOT NULL | `channel/group/private` |
| `bot_id` | Integer | nullable | 目标 Bot |
| `chat_id` | String(64) | nullable | 目标 chat |
| `dedup_key` | String(255) | NOT NULL, UNIQUE | `work_key:version:target` |
| `source_kind` | String(16) | default=`first` | `first/update/force/restore` |
| `status` | String(32) | default=`planned` | `planned/skipped/published/failed/cancelled` |
| `created_at` | DateTime | NOT NULL | 创建时间 |

唯一键：`uq_sc_list_item_dedup(dedup_key)`；建议索引：`(list_id, category_key, id)`、`(work_key, target_type)`。

### 2.6 计划 `sc_share_plans`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | Integer | PK | 主键 |
| `name` | String(255) | NOT NULL | 计划名 |
| `plan_type` | String(16) | NOT NULL | `hourly/daily/weekly/monthly` |
| `enabled` | Boolean | default=False | 启用开关 |
| `timezone` | String(64) | default=`Asia/Shanghai` | 时区 |
| `created_at` / `updated_at` | DateTime | NOT NULL | 时间 |

### 2.7 计划版本 `sc_share_plan_versions`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | Integer | PK | 主键 |
| `plan_id` | Integer | NOT NULL, FK→`sc_share_plans.id` | 所属计划 |
| `version_no` | Integer | NOT NULL | 版本号 |
| `status` | String(16) | default=`draft` | `draft/active/archived` |
| `config_json` | Text | NOT NULL | 计划配置快照 |
| `effective_from` / `effective_to` | DateTime | nullable | 生效区间 |
| `created_at` | DateTime | NOT NULL | 创建时间 |

唯一键：`uq_sc_plan_version(plan_id, version_no)`；部分唯一索引：同一 `plan_id` 只能有一个 `status=active`。

### 2.8 小时窗口 `sc_hour_windows`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | Integer | PK | 主键 |
| `plan_version_id` | Integer | NOT NULL, FK→`sc_share_plan_versions.id` | 计划版本 |
| `start_hour` | Integer | NOT NULL | 0–23 |
| `end_hour` | Integer | NOT NULL | 1–24，左闭右开 |
| `quota` | Integer | default=0 | 窗口内最大处理数，0 不限 |
| `enabled` | Boolean | default=True | 是否启用 |
| `created_at` | DateTime | NOT NULL | 创建时间 |

约束：`0 <= start_hour < end_hour <= 24`；默认对外展示 08:00–24:00，但数据层不禁止 00:00–08:00 的显式配置。

### 2.9 分类游标 `sc_category_cursors`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | Integer | PK | 主键 |
| `plan_version_id` | Integer | NOT NULL | 计划版本 |
| `category_key` | String(128) | NOT NULL | 分类 |
| `last_work_key` | String(255) | default=`` | 上次处理作品 |
| `last_library_version_id` | Integer | nullable | 上次处理资源版本 |
| `last_success_at` | DateTime | nullable | 上次成功时间 |
| `status` | String(32) | default=`idle` | `idle/running/paused/error` |
| `updated_at` | DateTime | NOT NULL | 更新时间 |

唯一键：`uq_sc_cursor(plan_version_id, category_key)`。

### 2.10 更新合并窗口 `sc_update_windows`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | Integer | PK | 主键 |
| `work_key` | String(255) | NOT NULL | 作品键 |
| `window_start` | DateTime | NOT NULL | 窗口开始 |
| `window_end` | DateTime | NOT NULL | 窗口结束 |
| `event_keys_json` | Text | NOT NULL | 合并的原始事件幂等键 |
| `fingerprint` | String(128) | NOT NULL | 窗口内最新资源指纹 |
| `status` | String(16) | default=`open` | `open/merged/cancelled` |
| `created_at` | DateTime | NOT NULL | 创建时间 |

唯一键：`uq_sc_update_window(work_key, window_start)`；窗口长度固定 10 分钟，由服务层保证 `window_end = window_start + 600s`。

### 2.11 发布任务 `sc_publish_tasks`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | Integer | PK | 主键 |
| `list_item_id` | Integer | NOT NULL, FK→`sc_push_list_items.id` | 来源清单项 |
| `work_key` | String(255) | NOT NULL | 作品键 |
| `plan_version_id` | Integer | nullable | 计划版本 |
| `kind` | String(16) | default=`first` | `first/update/force/restore` |
| `status` | String(32) | default=`planned` | `planned/building/ready/submitted/accepted/publishing/published/failed/cancelled` |
| `priority` | Integer | default=0 | 排序权重 |
| `scheduled_at` | DateTime | nullable | 计划执行时间 |
| `lease_owner` | String(64) | default=`` | 当前租约持有者 |
| `lease_expires_at` | DateTime | nullable | 租约到期时间 |
| `attempts` | Integer | default=0 | 尝试次数 |
| `next_retry_at` | DateTime | nullable | 下次重试 |
| `share_url` | String(1024) | default=`` | 分享链接 |
| `link_state` | String(32) | default=`unknown` | 链接状态 |
| `error` | Text | default=`` | 错误信息 |
| `created_at` / `updated_at` / `published_at` | DateTime | nullable | 时间 |

约束：领取使用条件 UPDATE，必须带 `status=submitted AND (lease_expires_at IS NULL OR lease_expires_at < now)`；租约到期任务由调度器回收。

### 2.12 执行记录 `sc_publish_executions`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | Integer | PK | 主键 |
| `task_id` | Integer | NOT NULL, FK→`sc_publish_tasks.id` | 任务 |
| `stage` | String(32) | NOT NULL | `build/verify/submit/send/result/retry` |
| `status` | String(16) | NOT NULL | `running/success/failed/skipped` |
| `request_json` / `response_json` | Text | default=`` | 输入输出摘要，禁止写密钥 |
| `error` | Text | default=`` | 错误 |
| `started_at` / `finished_at` | DateTime | nullable | 时间 |
| `duration_ms` | Integer | default=0 | 耗时 |

### 2.13 AI 建议 `sc_ai_suggestions`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | Integer | PK | 主键 |
| `week_start` | Date | NOT NULL | 周起始日 |
| `suggestion_type` | String(32) | NOT NULL | `plan_weights/category_priority/interval` |
| `payload_json` | Text | NOT NULL | 建议内容 |
| `reason` | Text | default=`` | 原因摘要 |
| `confidence` | Float | default=0.0 | 置信度 |
| `status` | String(16) | default=`pending` | `pending/approved/rejected/applied/failed` |
| `reviewed_by` | String(64) | default=`` | 审核人 |
| `reviewed_at` / `applied_at` / `created_at` | DateTime | nullable | 时间 |

### 2.14 AI 应用日志 `sc_ai_apply_logs`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | Integer | PK | 主键 |
| `suggestion_id` | Integer | NOT NULL | 建议 ID |
| `action` | String(32) | NOT NULL | `apply/rollback/reject` |
| `before_json` / `after_json` | Text | default=`` | 变更前后摘要 |
| `result` | String(16) | default=`success` | `success/failed` |
| `error` | Text | default=`` | 错误 |
| `created_at` | DateTime | NOT NULL | 时间 |

### 2.15 推送节奏规则 `sc_push_pacing_rules`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | Integer | PK | 主键 |
| `scope_key` | String(128) | NOT NULL, UNIQUE | `global:channel/global:group/bot:<id>:group/chat:<chat_id>` |
| `target_type` | String(16) | NOT NULL | `channel/group/private` |
| `bot_id` | Integer | nullable | 指定 Bot |
| `chat_id` | String(64) | nullable | 指定 chat |
| `default_interval_seconds` | Float | NOT NULL | 用户可调默认间隔 |
| `min_interval_seconds` | Float | NOT NULL | 后端硬下限 |
| `max_per_minute` | Integer | default=0 | 0 表示只按间隔控制 |
| `jitter_seconds` | Float | default=0 | 随机抖动，默认 0 |
| `enabled` | Boolean | default=True | 是否启用 |
| `updated_by` | String(64) | default=`admin` | 更新者 |
| `created_at` / `updated_at` | DateTime | NOT NULL | 时间 |

初始化数据：

| scope_key | target_type | default_interval_seconds | min_interval_seconds |
|---|---|---:|---:|
| `global:channel` | `channel` | `2.0` | `1.0` |
| `global:group` | `group` | `4.0` | `3.0` |
| `global:private` | `private` | `2.0` | `1.0` |

强校验：

- `default_interval_seconds >= min_interval_seconds`。
- 修改 `min_interval_seconds` 不得低于目标类型官方下限。
- 若同时配置 `max_per_minute`，有效间隔不得小于 `60 / max_per_minute`。
- 前端可提交更大的间隔；后端夹逼并拒绝低于下限的保存请求。

### 2.16 推送节奏状态 `sc_push_pacing_state`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | Integer | PK | 主键 |
| `target_key` | String(128) | NOT NULL, UNIQUE | `bot:<id>:chat:<chat_id>` |
| `target_type` | String(16) | NOT NULL | `channel/group/private` |
| `last_sent_at` | DateTime | nullable | 最近一次成功发送 |
| `last_attempt_at` | DateTime | nullable | 最近一次尝试 |
| `retry_after_seconds` | Float | default=0 | 最近一次 429 返回的等待秒数 |
| `retry_after_until` | DateTime | nullable | 429 冷却截止 |
| `consecutive_429` | Integer | default=0 | 连续 429 次数 |
| `updated_at` | DateTime | NOT NULL | 更新时间 |

用途：

- 进程重启后仍知道每个 chat 的最后发送时间，避免重启导致间隔归零。
- 429 `retry_after` 持久化，重启后仍继续冷却。
- 节奏状态只由分发中心写，分享中心只读展示。

---

## 3. 实际间隔计算

分发中心发送前执行：

1. 查规则优先级：`chat:<chat_id>` > `bot:<bot_id>:<target_type>` > `global:<target_type>`。
2. 取 `configured = default_interval_seconds`。
3. 夹逼下限：`effective = max(configured, min_interval_seconds)`。
4. 若配置 `max_per_minute > 0`，再取 `effective = max(effective, 60 / max_per_minute)`。
5. 若当前时间早于 `retry_after_until`，本轮跳过并退避，不发送。
6. 若 `last_sent_at` 存在，`wait = effective - (now - last_sent_at)`；`wait > 0` 则等待或本轮跳过。
7. 加入 `jitter_seconds` 只能增加等待，不能减少。
8. 发送成功后写 `last_sent_at`；收到 429 后写 `retry_after_until` 并递增 `consecutive_429`。
9. 全局聚合仍需保证不超过约 30 条/秒；若未来启用并发发送，必须先做全局令牌桶，不能只靠 per-chat sleep。

**红线**：任何配置、AI 建议或前端请求都不能把 `effective` 降到官方下限以下。

---

## 4. 迁移策略

1. 只通过 `Base.metadata.create_all()` 创建上述 `sc_*` 新表。
2. 不写旧表 `ALTER`，不迁移旧数据，不读取旧表。
3. 初始化三条全局节奏规则：按 `scope_key` 幂等 `INSERT ... WHERE NOT EXISTS`。
4. 新增 `share_center_v2_enabled` 配置，默认 `false`；新模块上线时由用户显式打开。
5. 旧模块继续运行直到新模块验收通过；新模块不得写旧表。
6. 回滚策略：关闭 `share_center_v2_enabled`，旧模块继续服务；新表保留不删，避免不可逆操作。

---

## 5. 第二阶段 API 契约草案

| 方法 | 路径 | 用途 |
|---|---|---|
| GET/POST | `/api/sc/categories` | 分类列表/启停 |
| POST | `/api/sc/categories/reassign` | 重算作品主归属 |
| GET | `/api/sc/library-versions?work_key=` | 查询作品版本 |
| GET/POST | `/api/sc/push-lists` | 清单列表/生成 |
| GET | `/api/sc/push-lists/{id}/items` | 清单项分页查询 |
| GET/POST | `/api/sc/plans` | 计划列表/创建 |
| POST | `/api/sc/plans/{id}/versions` | 生成计划版本 |
| POST | `/api/sc/plans/{id}/activate` | 激活版本 |
| GET/POST | `/api/sc/hour-windows` | 小时窗口配置 |
| GET | `/api/sc/cursors` | 分类游标查询 |
| GET | `/api/sc/tasks` | 发布任务查询（三视图复用） |
| GET | `/api/sc/tasks/{id}` | 任务详情与执行记录 |
| POST | `/api/sc/tasks/{id}/retry\|cancel` | 任务重试/取消 |
| GET/POST | `/api/sc/pacing-rules` | 推送节奏规则查询/更新 |
| GET | `/api/sc/pacing-state` | 节奏状态只读查询 |
| GET/POST | `/api/sc/ai/suggestions` | AI 建议列表/提交审核 |
| POST | `/api/sc/ai/suggestions/{id}/apply` | 审核通过后应用 |

---

## 6. 测试清单

1. 新表 `create_all` 后字段、唯一键、索引存在。
2. 新模块不写旧表的静态守卫。
3. `share_center_v2_enabled=false` 时新 API 不参与旧链。
4. 一作品一主归属唯一约束。
5. 同一作品同指纹不重复生成资源版本。
6. 清单项 `dedup_key` 唯一。
7. 计划版本不可变；同一计划只允许一个 active 版本。
8. 分类游标按计划版本隔离。
9. 10 分钟更新窗口合并多次事件。
10. 任务领取条件 UPDATE + 租约超时回收。
11. 频道规则默认 2s/下限 1s；群规则默认 4s/下限 3s。
12. 保存低于官方下限的配置被拒绝。
13. `max_per_minute` 换算下限生效。
14. 429 `retry_after` 覆盖静态间隔并在重启后保留。
15. 全局聚合发送速率不超过约 30 条/秒。

---

## 7. 完成判定

第二阶段完成必须满足：

- 上述 `sc_*` 表全部创建并有迁移测试。
- 旧数据不迁移、新模块不读旧表的静态守卫存在。
- 推送节奏规则与状态表可读写，频道/群间隔可调且下限不可突破。
- 第二阶段 API 至少完成清单、计划、任务、节奏规则四类。
- 全量测试通过，前端 build 不受影响。
- 不升版本、不发版，待后续阶段完成后统一发版。
