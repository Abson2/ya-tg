# Ya-TG 资源库优化与分享中心重新设计 · 第二阶段实施方案

> 方案基线：v1.4.4（HEAD `3f7b60c`，tag `v1.4.4`，2026-11-21）
> 上游依据：`RESOURCE_SHARE_REDESIGN_AUDIT.md`（第一阶段源码审计，四路并行只读，结论带源码行号）
> 本文定位：**把审计结论 + 用户 5 项决策固化为可执行、可测、可回滚的落地方案**。仍不改业务代码，代码改造从阶段二开始。
> 标注纪律：`[事实]` 源码/API 取证；`[推断]+推理链` 有依据的推断；`[假设]+不成立应对` 需验证的前提；`[决策]` 用户已拍板；`[建议]` 本方案给出、待真机校准的默认值。

---

## 0. 结论先行

**追，走增量路线，不推倒重做。** 目标架构是四个边界清晰、通过事件与交接协议耦合的模块：资源库（资源唯一管理入口）、分享中心（发布唯一管理入口）、分发与互动中心（消息发送唯一管理入口，发送核心绝不动）、镜像同步（A→B 唯一入口）。`[事实]` 审计确认约 60% 基础设施已存在（ShareTask/ShareRecord 双表真相源、事件总线、交接协议四接口、can_publish 门禁、扫描批次完整性保护），真正缺口集中在三块：链接有效性生命周期全无、资源事件只有 3 类闭环、识别成功仍同步硬触发入队。

**最高约束已在审计中确认且用户已拍板**：光鸭无「查询分享有效性」官方接口，不虚构校验结果。本方案据此采用「本地元数据 + B 库删除事件驱动」为有效性判定的**唯一权威语义**，抓包取证仅作兜底增强，且**不声称主动探测成功**。

**优先级排序**（按依赖与风险）：
1. **P0 决策固化**（本文）→ 2. 阶段二资源库解耦 → 3. 阶段三事件体系补齐 → 4. 阶段四分享中心生命周期（含 9 态 + 跨季一次 + 统一发布引擎）→ 5. 阶段五链接校验与清理 → 6. 阶段六前端全重做 → 7. 阶段七全量测试 + 真机验收。
   `[推断]+推理链` 阶段二/三是阶段四的事件依赖前置；阶段四的 can_publish 增强依赖阶段五的校验语义；前端（六）必须在后端状态稳定后做，避免返工；自动发布/自动清理（默认值见第 3 节）默认关闭，真机验收后才开启。

---

## 1. 用户 5 项决策固化表

| # | 决策项 | 用户拍板（`[决策]`） | 方案落地口径 | 不成立应对 |
|---|---|---|---|---|
| 1 | 链接校验方案 | **优先①本地元数据 + B 库删除事件驱动；兜底②本地抓包取证替代接口** | 有效性判定权威 = 本地 `ShareRecord` + `RESOURCE_REMOVED`/镜像删除事件；抓包取证只在本地做，若发现网页端确有有效性接口再评估接入，**未取证前不接入、不虚构** | 若抓包也无接口 → 永久保持事件驱动语义，UI 明确标注「有效性=本地推断，非光鸭实时状态」，不做主动探测 |
| 2 | 提取码 | **不启用提取码** | `share_file` 保持 `enableShareCode=False`；**不新增 `share_code` 列**（审计 3.2 列为缺口，按决策取消）；投稿 URL 的 `?code=` 解析仅用于投稿转存，与自建分享解耦 | 若后续要启用 → 需先抓包确认 `enableShareCode=true` 的真实返回结构，再单独增列，不在本方案范围 |
| 3 | 跨季 S01/S02 | **算一部，分享一次** | 以剧名母目录的 `work_group_key`（`utils.py:197-224`）为分享粒度，一部剧一条 `current_flag` 分享；新季/新集走 `update_share` 复用同一 `share_url`，不新建 | 若剧名母目录不可靠（多版本/多来源混入）→ 回退到 `dir_chain_json` 的第一级整剧目录，仍保证一剧一链接 |
| 4 | 前端分享中心 | **全部重做（对现页面不满意），但必须与现有分发与互动中心深度耦合，避免推送异常** | 前端 5 页全部重写；后端**只调用现有** `share_center` + 分发中心真实接口；**绝不改** `_send_telegram`/`_build_message`/`process_queue`/白名单/路由等发送核心（审计第 7 节清单） | 若新页面需要后端新接口 → 只加只读查询接口或复用现有接口，不动发送链路；状态定义与后端单一来源（第 6.4 节） |
| 5 | 默认值 | **给建议保守值做默认** | 见第 3 节保守默认值表；存量自动发布、自动清理默认关闭；推送/发布有保守日上限；释放间隔对齐 Telegram 2s 节流 | 真机跑稳后由用户逐项放开，不在发版时默认开启 |

---

## 2. 目标架构与真相源定义

### 2.1 四模块边界（改造后）

```
[A 库只读] ──mirror.py(按部收敛/目录级批量)──> [B 库]
                                                   │
                                            scanner.run_scan
                                                   ▼
        ┌────────────────── 资源库 ────────────────────┐
        │ 唯一管理入口：扫描/识别/海报/Resource 表       │
        │ 识别成功只做两件事：写资源库 + 发事件          │
        │ 不直接建分享、不直接入队                       │
        └───────────────────┬───────────────────────────┘
                            │ RESOURCE_IDENTIFIED / *_FILE_CHANGED / *_REMOVED 事件（持久化幂等）
                            ▼
        ┌────────────────── 分享中心 ───────────────────┐
        │ 唯一发布入口：ShareTask 状态机 + ShareRecord   │
        │ 真相源 + can_publish 门禁 + 统一发布引擎        │
        │ + 链接生命周期（校验/失效复查/清理）            │
        └───────────────────┬───────────────────────────┘
                            │ 交接协议四接口（submit/claim/url/mark_result，现有，不改）
                            ▼
        ┌────────────── 分发与互动中心（绝不动发送核心） ─┐
        │ publisher._send_telegram/_build_message/       │
        │ process_queue + bot_listener 白名单/路由/互动   │
        └────────────────────────────────────────────────┘
```

### 2.2 真相源单一化（`[推断]+推理链` 依据审计 1.2/3.2/P0 风险）

| 数据 | 改造前 | 改造后唯一真相源 | 迁移方式 |
|---|---|---|---|
| 作品/文件存在状态 | `resources` 表 | `resources`（B 库扫描真相） | 不变 |
| 分享链接状态 | `Resource.share_*` 与 `ShareRecord` 双向写 | **`ShareRecord`**；`Resource.share_*` 降级为只读缓存 | 双写期先以 ShareRecord 为准，Resource 缓存只由 ShareRecord 变更时刷新（阶段二） |
| 发布任务状态 | `ShareTask.status` | `ShareTask.status`（不变） | 9 态枚举收口（阶段四） |
| 链接有效性 | `last_verified_at`（实为登记时间） | 新增 `link_state` + `verified_result` + `verify_fail_count`，`last_verified_at` 语义修正为「最近一次有效性判定时间」 | 幂等增列，旧值保留（阶段五） |
| 作品键 | 三套 `work_group_key`/`_work_key`/`dir:\|tmdb:\|file:` | **`utils.work_group_key` 单一权威**（其余改为调用它） | 保留旧函数做兼容转发，逐步替换（阶段二/四） |

`[事实]` `db.migrate()`（`db.py:444-501`）只支持 `_COLUMNS` 幂等 `ALTER TABLE ADD COLUMN`，无删列/改列/版本号。故所有新增字段一律带 DEFAULT 幂等增列，**不删列、不改列类型**，回滚=恢复旧配置值 + 停用新逻辑开关，不改表结构。

---

## 3. 保守默认值建议表（`[建议]`，真机校准前不外放）

> 依据：现状多为 `0`（不限），一次性压满网盘/刷屏风险高（审计 10.5）；默认节奏须对齐 Telegram 2s 节流与光鸭风控（v1.4.1 教训）。

| 配置项（runtime_config key） | 现状默认 | 建议保守默认 | 依据 / 调整条件 |
|---|---|---|---|
| `share_center_enabled` | `true` | `true`（保持） | 已是主链，关闭会导致分享不工作；不动 |
| `auto_publish_existing`（新增） | 无 | **`false` 关闭** | 存量一次性全发风险最高；真机确认后手动开启批次 |
| `auto_publish_new`（新增） | 无 | `true`，但受 `publish_paused` 全局暂停开关约束 | 新资源应及时入队；保留一键暂停避免异常扩散 |
| `publish_paused`（新增） | 无 | `false` | 全局急停；前端总览页显著位置 |
| `followup_merge_window_seconds`（新增） | 无 | **1800（30 分钟）** | 同剧多集连续扫描合并为一次分享，避免碎片链接 |
| `share_release_interval_seconds` | `0` | **2** | 对齐 Telegram 2s 节流与光鸭节奏，防风控 |
| `existing_daily_quota` | `0` | **10（部/日）** | 存量大批量保守上限；`0` 仅作高级选项 |
| `push_daily_limit` | `0` | **20（条/日）** | 防频道刷屏；`0` 仅作高级选项 |
| `link_recheck_interval_hours`（新增） | 无 | **24** | 低频复查，避免高频 DB 写（死锁教训） |
| `link_inactive_confirm_count`（新增） | 无 | **2** | 至少 2 次失效信号才判失效，防网络抖动误判 |
| `link_inactive_confirm_gap_minutes`（新增） | 无 | **30** | 两次确认间隔 ≥30 分钟 |
| `auto_cleanup_enabled`（新增） | 无 | **`false` 关闭** | 清理不可逆；仅人工确认后执行 |
| `stock/fresh/followup_daily_quota` | `0` | 存量 10 / 全新 0(不限) / 追更 0(不限) | 只对存量设保守上限，全新与追更量小 |

`[决策]` 第 5 项「给建议保守值做默认」即指上表：所有新增自动化能力默认关闭或保守限量，真机验收通过后再逐项放开。`[假设]` 上述数值为经验值；不成立应对=真机观察光鸭风控与频道体验后调整，不写死在代码语义里（全部走 runtime_config 热更新）。

---

## 4. 分阶段实施设计

> 每阶段独立可测、可回滚、不动分发核心；阶段二/三是事件依赖前置，阶段四前必须完成第 1 节决策固化（本文已完成）。

### 阶段二：资源库解耦（任务 #138）

**目标**：识别成功不再同步硬触达分发，改为「写资源库 + 发事件」；`Resource.share_*` 降级为只读缓存；补 A 库原名回溯识别入口与识别结果持久化缓存。

**改造点**
1. **切断 5 处硬耦合**（`[事实]` `main.py:2138/2340/3318/3352/3390`）：把 `_enqueue_after_identify` 的同步调用替换为 `emit_resource_event(..., type=RESOURCE_IDENTIFIED)`。保留 `_enqueue_after_identify` 函数体但改为**仅由分享中心订阅事件后调用**，或整体迁入分享中心；资源库侧不再直接写 `QueueItem`/`shadow_register`。
2. **`Resource.share_*` 只读化**（`[事实]` `models.py:23-25,58-61`）：新增统一刷新函数 `sync_resource_share_cache(db, work_key)`，只在 `ShareRecord` 变更时由分享中心调用；资源库自身不再写这些列。前端海报墙角标改读 `ShareRecord`（阶段六落地，本阶段只备好接口）。
3. **A 库原名回溯识别**（审计缺失项 2）：新增入口——当 B 库 `name/path_hint` 识别失败时，用 `MirrorFile.source_file_id` 回溯 A 库原名再送 TMDB。仅在识别失败分支调用，不影响主链。
4. **识别结果持久化缓存**（审计缺失项 3）：识别成功写 `ResourceVersion`/识别缓存表，命中则零网络（复用 english_title 缓存思路）；force 才重解析。

**验收**：识别成功、海报墙正常；识别不再直接产生 QueueItem/分享；分享完全由事件驱动创建。
**回滚**：保留 `_enqueue_after_identify` 旧路径函数，用 `resource_decoupled_enabled` 开关切换；出问题关开关回到同步链路。
**风险**：`[P0]` 双写期 Resource 缓存与 ShareRecord 状态不一致 → 以 ShareRecord 为准 + 刷新函数兜底；回归测试覆盖 5 处触发点。

#### 阶段二实际落地记录（2026-11-21，代码完成待真机验收）

`[事实]` 实施前对源码做二次取证，**推翻了上面"字面切断 5 处硬耦合"的做法**——直接删 `_enqueue_after_identify` 会造成全项目零推送，原因：

1. `[事实]` `consume_resource_events` 消费事件时调 `ensure_task(db, res, kind="first")` **不带 channel_targets**（`share_center.py:659`）；而 channel_targets 此前只由旧链 `_enqueue_ready → shadow_register(db, x, channel_ids=[...])` 写进 ShareTask（`:74-75`）。
2. `[推断]+推理链` 若切断旧链、只靠事件：`ensure_task` 拿到空 targets → `submit_to_dispatch` 读 `task.channel_targets=[]` 返回 0 → 不写 QueueItem → 零推送。这正是用户决策 4「避免推送异常」要防的事故。

**实际采用「切决策不切链路 + 灰度开关」**（可逆、零回归）：
1. 新增 `compute_resource_channel_targets(db, x)`（`main.py`）：纯计算、复用 `push_routes/match_push_routes/规则匹配/_ensure_route_channel`，返回 `[{channel_id,...}]`，**不写 QueueItem**。
2. 新增 `_identify_emit_only(db, x)`：只发资源事件，payload 携带 `channel_targets` + `decoupled=True`；若同类事件已存在且无 targets 则刷新其 payload（防开关切换后断推）。
3. 改 `consume_resource_events`：从 payload 读 `channel_targets` 传给 `ensure_task`；无 targets 的存量事件保持旧行为（兼容）。
4. 新增统一分发 `_after_identify_dispatch(db, x)`：按 `resource_decoupled_enabled`（**默认 false**）选择新链/旧链；且要求 `share_center_enabled=true` 才切新链，避免切到无消费者事件导致漏推。5 个识别入口统一改调它，旧链函数原样保留。
5. A 库原名回溯：`MirrorFile.file_name` 恒为 A 原名（本地查表，零网络），通过 `target_file_id==x.file_id` 关联；`identify_with_a_source_fallback()` 首次识别失败时用 A 原名重试一次。
6. 识别缓存：`_IDENTIFY_CACHE` 进程内 dict（key=name+path_hint），只缓存成功结果，失败/存疑不缓存；force/手动 identify 不走本包装函数，天然重新解析。测试 conftest 加 autouse fixture 每用例清理（防跨用例串扰，v085 两用例曾因此假失败后已修）。

**验收结果**：新增 `tests/test_v145_resource_decouple.py` 13 项全过；全量 pytest 通过（见发版记录）；未改前端；未升版本号、未发版（按计划阶段七统一发版）。
**关键护栏**：默认开关关闭 = 旧链零回归；解耦能力已就绪但默认不启用，符合保守默认值决策（决策 5）。真机需验证「开关开启后新链推送等价」才具备开启条件。

### 阶段三：事件体系补齐（任务 #139）

**目标**：让「逐集/逐文件变化」可追溯、重启不丢事件、幂等；补镜像事件消费端。

**改造点**
1. **ResourceEvent 幂等键扩展**（`[事实]` `events.py:69` 现为 `type|work_key|resource_id`）：加 `fingerprint`/序号维度，payload_json 存变化摘要；保留旧键兼容读。
2. **补齐 5 类事件定义与 emit/consume**：`resource_created/file_changed/renamed/moved/deleted`（+ `episode_added/updated`）。现状 6 类死常量（审计 4.1）要么启用要么删除，不留悬空。
3. **补 `consume_mirror_events()` + `MIRROR_RESTORED` emit**（审计 4.1/缺失项 4）：`[事实]` `MIRROR_COMPLETED` 只写不读（`main.py:1143`），恢复靠周期全量重扫；改为镜像完成/恢复发事件，资源库消费后增量扫描，减少全量开销。

**验收**：逐集/逐文件变化入库且幂等；进程重启未消费事件不丢；镜像恢复走事件而非全量重扫。
**回滚**：事件表新增 consume 逻辑用开关控制，关掉后回到周期全量扫描。
**风险**：`[P1]` 幂等键扩展可能让旧事件重复消费 → 迁移时对存量事件按旧键去重一次。

### 阶段四：分享中心生命周期（含 9 态 + 跨季一次 + 统一发布引擎）

**目标**：`share_status` 收口 9 态；跨季一剧一链接；三套建分享逻辑收敛到统一发布引擎；can_publish 增强。

**改造点**
1. **9 态状态机**（`[事实]` 现 9 常量但真实流转 5 态，`share_center.py:32-44`）：`planned → building → ready → submitted → accepted → published`，异常分支 `failed/cancelled`；`ST_PUBLISHING` 死状态落点或移除（对齐审计待修项）。
2. **跨季一剧一链接**（决策 3）：分享粒度 = `work_group_key` 剧名母目录；追更/新季走 `GuangYaShareClient.update` 复用 `share_url`（`[事实]` `update_share` 已存在，`guangya.py:598-603`），不新建 `ShareRecord`；部分唯一索引（`:620-624`）保证一剧一条 current。
3. **统一发布引擎 + 光鸭适配器**：`[事实]` 三处建分享散点（`publisher.py:36-80`、`share_center.py:296-383`、`main.py:2879-2924`）+ 四处 update/delete（`main.py:2839/2852/2934/2242/2772/2950`）收敛到 `share_center` 的 `PublishEngine`，光鸭调用统一走 `GuangYaShareClient`。
4. **`claim_tasks` 真原子**（`[事实]` `share_center.py:446-467` UPDATE 无 WHERE）：补 `WHERE status_ext='submitted'`，加 `worker_id`，兑现多进程互斥；`_job_share_center` 与 queue_worker 的 run_cycle 加显式互斥（审计待修项）。
5. **can_publish 增强**：在现有门禁（资源存在、identified+海报、current active、配额）基础上，接入阶段五的 `link_state`（失效链接不复用）与跨季聚合判断。

**验收**：整目录分享、跨季追更复用同一链接、历史多记录保留；多进程领取不重复；can_publish 拒绝失效链接。
**回滚**：`publish_engine_enabled` 开关；旧三套建分享逻辑保留到验收后删除。
**风险**：`[P0]` 收敛时误伤发送核心 → 引擎只封装「建/更新/删分享 + 任务状态」，绝不触碰 publisher 发送函数（审计第 7 节红线）。

### 阶段五：链接校验与清理（决策 1 落地，不虚构接口）

**目标**：在不虚构光鸭接口的前提下，建立「有效性判定 → 失效复查 → 清理」的完整可追溯流程。

**核心语义（`[决策]` + `[推断]`）**：
- 有效性权威 = 本地元数据 + 事件驱动，**不是光鸭实时状态**。定义 `link_state` 8 态：`unknown / active / suspicious / inactive / retired / replaced / cleaned / manual_review`。
- **失效信号来源**：`RESOURCE_REMOVED` / 镜像删除事件（B 库文件真没了 → 链接大概率失效）；`[假设]` 投稿转存 `get_share_access_token` 业务码 200/201/202 可间接反映失效——**自建分享 204 自转存拒绝是障碍，接入前必须实测**，实测不通则该来源永久关闭，仅用事件驱动。
- **兜底抓包取证**：仅本地抓包网页端是否有「检查分享有效性」接口（`[事实]` 现无此实现）；未取证到 → 不接入、UI 标注「本地推断」。
- **失效判定**：`verify_fail_count >= link_inactive_confirm_count(2)` 且两次间隔 ≥30 分钟才置 `inactive`；网络异常不计失效信号（防误判）。
- **清理**：`auto_cleanup_enabled=false` 默认；人工确认后才执行；清理只置本地 `link_state=cleaned` + 归档 `ShareRecord`，**绝不删 B 库资源、绝不删历史记录**（审计验收项）。

**改造点**：ShareRecord 幂等增列 `link_state/verified_result/verify_fail_count/last_verified_at 语义修正/cleaned_at/clean_state`；新增 `link_verify_job`（低频 24h）与 `cleanup` 手动端点；`consume_resource_events` 接入失效信号。

**验收**：B 库删文件→链接置 suspicious→二次确认→inactive；网络异常不误判；清理不动 B 库和历史；UI 不显示「光鸭已确认有效」等虚假措辞。
**风险**：`[P1]` 低频 job 高频写 → 走 to_thread + 内存态合并写（死锁教训）。

### 阶段六：前端分享中心全重做（决策 4 落地）

**目标**：分享中心页面 100% 重做；资源卡加分享/推送/链接有效性角标；状态定义与后端单一来源。

**5 页结构**（替代现 `App.vue:2823-2901` 的 7 纵向 panel）：
1. **发布总览**：全局急停 `publish_paused`、今日发布/推送配额进度、待处理事件数、链接有效性分布。
2. **发布队列**：存量 / 全新 / 追更三队列（radio/tab），每条显示 `ShareTask.status`，按钮走现有 `claim_tasks`/`run_cycle` 接口。
3. **分享管理**：`ShareRecord` 列表（当前+历史）、`link_state`、失效复查结果、手动核验入队、手动清理（默认关闭需二次确认）。
4. **推送任务**：分发交接状态（submitted/accepted/published/failed）+ 发送历史，只读为主，复用现有查询接口。
5. **发布规则**：第 3 节所有保守默认值配置 + 开关，热更新。

**耦合红线**：所有按钮只调现有 `share_center`/分发中心真实接口；新增只读查询接口可以，改发送核心不可以。`[事实]` 现 nav key=`推送队列`、label=`分享中心`（`App.vue:549-550`）错位 → 拆页时统一迁移为新 key，保留兼容断言。
**验收**：状态两处（前后端）一致；每个按钮走真实后端、无空壳；推送功能零回归。

### 阶段七：全量测试 + 真机验收

- 全量 pytest（当前 814 passed 基线）+ 前端 build；新增各阶段测试（资源解耦、事件幂等、9 态、跨季、链接生命周期、前端接口）。
- 真机清单：识别→事件→分享→推送全链；跨季复用链接；B 库删文件→链接失效；清理不动 B 库；自动发布/自动清理默认关闭验证。
- **发版门禁**：自动发布/自动清理默认关闭状态下先发版；真机跑稳后由用户逐项放开配置，不在发版时默认开启。

---

## 5. 未决项与观察清单（低置信度，不阻塞开工）

| 项 | 状态 | 提升至可决策所需信息 |
|---|---|---|
| 投稿转存业务码能否作为自建分享校验器 | `[假设]` 单一弱信号，204 自转存拒绝障碍 | 真机实测自建分享调 `get_share_access_token` 返回码 |
| 抓包网页端是否有有效性检查接口 | `[假设]` 未取证 | 本地抓包分享页访问链路（不上传网络） |
| 保守默认值具体数值 | `[建议]` 经验值 | 真机观察光鸭风控阈值与频道推送体验 |

---

## 6. 下一步

本文完成决策固化（任务 #137）。**随后按顺序执行**：任务 #138 阶段二资源库解耦 → #139 阶段三事件体系补齐 → 阶段四~七。每阶段结束独立回归，阶段七统一真机验收后发版。
