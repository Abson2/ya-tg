# Ya-TG 四模块重构 · 功能设计与实施文档

> 前置：《FOUR_MODULE_REFACTOR_AUDIT.md》（第一阶段源码审计，基线 HEAD `1437b95`）
> 本文只做设计，不改代码；所有「现状」引用审计报告行号证据
> 最终边界：镜像同步 / 资源库 / 分享中心 / 分发与互动中心，职责清楚、数据交接可靠、两条锁定业务规则闭环、Telegram 分发与互动保持正常

---

## 1. 设计总原则

1. **单向数据流、事件驱动**：镜像同步 →（A/B 映射就绪事件）→ 资源库 →（资源已识别/已出库事件）→ 分享中心 →（分享已就绪的推送任务）→ 分发与互动中心。禁止反向直接调用，禁止跨模块写对方的表。
2. **分享/推送解耦**：资源库不再入队、不再建分享；`Resource.share_*` 降级为只读缓存，真相源在分享中心任务表。
3. **分享是实体，不是动作**：每次分享有独立任务 ID、生命周期状态、追更关系；「一部剧一个分享、更新复用链接、更新重推」由分享中心状态机保证。
4. **交接有确认**：分享中心提交任务（`submitted`）→ 分发中心原子领取（`accepted` + worker_id）→ 发送 → 回执（`published/failed`）。无确认不发送、不重复发送。
5. **分发与互动中心保持不动**：`publisher._process_queue_impl` 的 TG 发送主体、`bot_listener` 的路由/白名单/互动、投稿转存、求片联动全部保留，只把入队来源从 `main.py` 改为接收分享中心提交。
6. **可回滚**：新表/新列全部幂等创建，不删旧列；切换靠代码路径开关，回退时旧链仍可用。

---

## 2. 四模块职责与边界矩阵

| 模块 | 负责 | 不负责（红线） | 产出事件/任务 | 消费事件 |
|---|---|---|---|---|
| **镜像同步** | A→B 文件一致性、英文改名、strict 顺序、暂停/恢复/跳过/取消、A/B fileId 映射 | 不识别、不分享、不推送、不写 push_queue | `mirror.completed`（本轮同步完成的作品清单，含 B 库 fileId/目录 ID） | 定时/手动触发 |
| **资源库** | 扫描 B 库、TMDB/AI 识别、海报、完结回填、入库/出库、对账 | 不入队、不建分享、不发送 | `resource.identified`、`resource.updated`、`resource.removed`、`resource.restored` | `mirror.completed`、定时扫描、手动刷新 |
| **分享中心**（原推送队列改造前移） | 分享生命周期、发布排序、节奏、追更管理、推送准入、任务调度、交接确认 | 不识别、不发 Telegram、不改 Bot/群/频道配置、不动投稿求片 | `share.ready` → 推送任务（`submitted`） | `resource.*` 事件、手动提交 |
| **分发与互动中心**（保持不动） | Telegram 发送、Bot/群/频道、白名单、投稿/求片/搜索/打赏、TGtoDrive 联动、光鸭转存 | 不建分享、不决定发布顺序/节奏、不重复消费 | 发送回执 `push.result` | 分享中心 `submitted` 任务、用户互动消息 |

**模块间只允许通过「内部事件表 + 任务表」通信，不直接 import 对方写库函数**（镜像模块现有红线 `mirror.py:8` 继续保持）。

---

## 3. 镜像同步模块设计

### 3.1 现状（审计报告 §2、§3.3）
- 引擎：`mirror.py sync_rule(strict=False/True)`，strict 走 natural_key + DFS + batch/sequence 持久化；`mirror_batch.py` 状态机。
- 触发：`/api/mirror/run?strict=true`、`mirror_retry_files`、定时（默认 60min，`main.py:535`）。
- 完成钩子：`_do_mirror_run` → 按 `auto_identify` 走 post_identify（当前直接识别+入队）。

### 3.2 改造点
- **保留**引擎与批次状态机 100% 不动（整改版已验证 741 passed，strict 真机待验证）。
- **改完成钩子**：`auto_identify=True` 时不再直接调 `_post_scan_identify → _enqueue_after_identify`，改为落一条 `mirror.completed` 事件（含本轮新到位作品的 B 库 fileId / share_dir_id 线索），由资源库消费。
- **新增** `mirror.completed → 资源库` 的交接：资源库收到后执行它自己的扫描+识别（保持「以 B 库真实文件为基础」），镜像模块不参与识别结果判断。
- **规则 2 的镜像侧**：B 库整部剧删除后，镜像下一轮自动从 A 库补齐（A 只读），补齐后发 `mirror.completed`（scope=restored），驱动资源库重新入库。

### 3.3 接口
```
POST /api/mirror/run?strict=true        # 保留
POST /api/mirror/batch/control?action=pause|resume|skip|cancel  # 保留
GET  /api/mirror/batch/status           # 保留
内部：mirror_events(id, work_key, b_file_ids, dir_hints, scope=new|restored, created_at)
```

---

## 4. 资源库模块设计

### 4.1 现状（审计报告 §2、§7.2）
- 扫描 `scanner.py` / `run_scan`，识别 `_post_scan_identify`，海报 TMDB/AI，完结回填；`Resource` 表带 `share_*` 字段（`models.py:58-62`）。
- 5 处识别成功后直接 `_enqueue_after_identify`（`main.py:2043/3154/3188/3226/2210`）——**必须改**。

### 4.2 改造点
- **删除_resource → 分享的直连**：5 处入口改为只更新识别状态，并落 `resource.identified` 事件；是否入队、何时入队由分享中心决定。
- **分享字段降级**：`Resource.share_url/share_id/share_status/share_duration` 保留列但不再由资源库写（分享中心双写缓存供旧前端/导出用），识别/重识别不得清空有效 share_url（除非资源出库）。
- **入库/出库事件**：
  - 入库/识别成功 → `resource.identified`（含 work_key、share_dir_id、B 库 fileId、海报、TMDB）。
  - 内容变化（收集集数变化）→ `resource.updated`（复用现有 DistributionEvent 的 `work_key+fingerprint` 去重，`scanner.py:139-143`）。
  - 对账删除 → `resource.removed`（含旧 share_id/share_url，供分享中心取消分享）。
  - 镜像补齐重新出现 → `resource.restored`。
- **作品键统一**：资源库提供单一权威 `work_key` 计算（收敛 `utils.work_group_key` / `main._work_key` / `publisher._work_group_key` 三套，审计报告 §5.2 风险），分享中心与分发中心统一调用，避免「一张卡=一条推送」裂缝。

### 4.3 接口
```
GET  /api/resources                      # 保留（展示用聚合）
POST /api/resources/{rid}/identify|auto-identify|match-none   # 保留，去掉入队副作用
POST /api/resources/identify-all         # 保留，去掉入队副作用
POST /api/scan-now | /api/rescan         # 保留，触发资源库扫描+对账
内部：resource_events(id, type, work_key, resource_ids, payload_json, created_at, consumed_at)
```

---

## 5. 分享中心模块设计（本次重构核心）

### 5.1 定位
由原「推送队列」改造并前移：它不再只是待发队列，而是**分享生命周期 + 发布调度中心**。前端从「分发与互动」组上移到资源组，改名「分享中心」。

### 5.2 数据模型（新增表）

`share_tasks`（分享任务 = 一次分享/推送的完整生命周期）：
| 字段 | 说明 |
|---|---|
| `id` | 独立任务 ID（强制项 3：每次推送独立 ID） |
| `work_key` | 作品键（与资源库统一口径） |
| `kind` | `first`(首次) / `update`(追更) / `force`(手动强推) / `restore`(删除后重建) |
| `resource_id` | 代表单集 Resource（用于建分享、消息构造） |
| `share_dir_id` / `share_scope` | B 库目录 ID 与粒度（强制项 2） |
| `share_id` / `share_url` / `share_status` | 分享结果（建好后写入，真相源） |
| `parent_task_id` | 追更任务指向首次任务（复用链接可追溯） |
| `parent_event_id` | 关联 resource_events / distribution_events（强制项 3 事件 ID） |
| `status` | `planned→building→ready→submitted→accepted→publishing→published/failed/cancelled` |
| `priority` / `scheduled_at` | 发布排序与节奏 |
| `attempts` / `next_retry_at` / `error` | 建分享重试 |
| `channel_targets` | JSON：本任务要推的频道/群（来自分发中心路由真相源） |
| `created_at` / `published_at` | 时间戳 |

`push_tasks`（或直接复用 `push_queue` 加列，倾向复用）：`share_task_id`→share_tasks.id、`status=submitted/accepted/publishing/done`、`accept_worker`、`accepted_at`、原 QueueItem 字段（`channel_id`,`telegram_message_id`,`attempts`,`next_retry_at`,`error`）。

### 5.3 状态机与两条锁定规则的实现

**规则 1（一部剧只分享一次整目录、更新复用链接再推）**：
1. 收到 `resource.identified` → 查 `share_tasks` 是否已有同 `work_key` 的 `published/ready` 任务。
2. 无 → 建 `kind=first` 任务，以 `share_dir_id`（B 库目录 ID，`utils.series_dir_info` 计算）建分享，写回 share_url。
3. 有 → 不重复建分享；若为 `resource.updated`（新增集数），建 `kind=update` 任务，`parent_task_id=首次任务`、`share_url` 直接复用，**独立任务 ID 再次推送**（强制项 3）。
4. 同 `work_key` 的 update 任务在 TG 侧仍受分发中心去重约束？——不：分享中心已保证 update 是新任务，分发中心按 task_id 去重（非按作品），确保「有更新就再推一次」。

**规则 2（B 库删除→出库→取消分享；A 库仍在→镜像补齐→重入库→重新分享推送）**：
1. `resource.removed` → 分享中心把该 work_key 的 active 分享任务标 `cancelled`、调光鸭取消分享（复用现有 `GuangYaShareClient.delete`，`main.py:2147`）、清推送任务。
2. 镜像下一轮补回 B 库 → `mirror.completed(scope=restored)` → 资源库 `resource.restored` → 分享中心建 `kind=restore` 任务，重新建分享、重新推送。
3. 全链路事件可查（resource_events + share_tasks.parent 链），替代现状的「时序巧合」（审计报告 §6）。

### 5.4 发布排序与节奏
- 排序：`priority`（force > first > update > restore）+ `scheduled_at` + 创建时间，FIFO 兜底。
- 节奏：分享中心按 `scheduled_at` 释放任务；TG 逐条 2s 节流仍在分发中心（`publisher.py:388`，保持不动）。两者叠加，不冲突。
- 追更管理：`kind=update` 任务带 `parent_task_id`，前端可展示「某剧已推 N 次更新」。

### 5.5 交接确认（强制项 4）
- 分享中心把 `ready` 任务写为 `submitted`（一次性写 channel_targets）。
- 分发中心 worker 领取时执行原子更新：`UPDATE push_tasks SET status='accepted', accept_worker=:w WHERE status='submitted' AND id IN (...) RETURNING id`（SQLite 用事务+条件 UPDATE 模拟），仅领取成功的任务进入发送。
- 发送回执写 `published/failed`，分享中心据此更新 share_tasks 状态与前端进度。
- 单进程时 `_queue_lock`（`publisher.py:533`）保留作为二次保险；多实例时靠原子领取，无需共享内存锁。

### 5.6 接口
```
POST /api/share/tasks                    # 手动提交（供资源库/前端调用，内部受控）
GET  /api/share/tasks?status=&work_key=  # 分享中心页面列表
POST /api/share/tasks/{id}/cancel        # 取消
POST /api/share/tasks/{id}/rebuild       # 重建分享
POST /api/share/schedule                 # 排序/节奏配置（可选）
内部：submit(task_id) → push_tasks(submitted)；consume() → 分发中心领取
```

### 5.7 前端
- 导航：「推送队列」→「分享中心」，从「分发与互动」组上移到「光鸭资源」组（在资源库之下）。
- 页面：分享任务列表（work_key / kind / 分享链接 / 状态 / 推送目标 / 更新时间）、追更关系、发布节奏配置、取消/重建操作。
- 任务中心（`App.vue:571-640`）：`push` 卡片改指向「分享中心」，数据来源改 share_tasks 聚合；保持纯聚合零新增 API 原则。

---

## 6. 分发与互动中心模块设计（保持不动）

### 6.1 保留清单（逐项不动）
- `publisher._process_queue_impl`（`publisher.py:383-527`）：消息构造、白名单闸门、建分享容错、TG 发送、错误分级（401/403 永久失败、429 退避）、节流 2s、AuditLog。
- `publisher._send_telegram` / `_build_message`：统一「由 Bot 推送」口径、sendPhoto parse_mode=HTML。
- `bot_listener`：push_routes / match_push_routes / is_push_allowed / route_target_channel、求片/投稿/搜索/打赏会话、目录路由真相源。
- `tgto.py`（求片白名单联动）、`contribution_restore.py`（光鸭原生转存）。
- 前端「分发与互动中心」页面（Bot/白名单/路由/投稿/求片/打赏）。

### 6.2 唯一改动：任务来源
- 消费端从「扫 `push_queue` 的 pending」改为「扫 `push_tasks` 中 `status=accepted`（由分享中心提交并经原子领取）」。
- `ensure_share` 调用从发送循环移除：分发中心只读取任务里已建好的 `share_url`，**不再自己建分享**（分享中心已 ready 阶段建好）。若 share_url 为空按分享失败处理（消息无链接），不反建。
- 白名单闸门、目标选择仍以分发中心的路由/白名单为准——分享中心的 `channel_targets` 是**提交时快照**，分发中心发送前再过一次 `is_push_allowed`（`publisher.py:454-467`），双保险。

### 6.3 回执
- 发送成功/失败写 `push.result` → share_tasks 状态更新 + 前端进度；永久失败（401/403）由分享中心标记 failed 并保留诊断（沿用现有文案分级，`publisher.py:505-516`）。

---

## 7. 事件模型与数据交接契约

| 事件 | 生产者 | 消费者 | 负载要点 | 幂等键 |
|---|---|---|---|---|
| `mirror.completed` | 镜像同步 | 资源库 | B 库 fileId 清单、目录线索、scope=new/restored | rule_id+batch_id+work_key |
| `resource.identified` | 资源库 | 分享中心 | work_key、share_dir_id、B fileId、海报、TMDB | resource_id+identify_fingerprint |
| `resource.updated` | 资源库(scanner) | 分享中心 | work_key、fingerprint、changed_files | work_key+fingerprint（沿用 `uq_event_work_fingerprint`） |
| `resource.removed` | 资源库(对账) | 分享中心 | work_key、旧 share_id/url | work_key+batch_id |
| `resource.restored` | 资源库 | 分享中心 | work_key、新 resource_id、B 目录 ID | work_key+restore_batch |
| `share.ready`→`submitted` | 分享中心 | 分发中心 | share_task_id、channel_targets、share_url | share_task_id（唯一） |
| `push.result` | 分发中心 | 分享中心 | share_task_id、status、message_id、error | share_task_id+attempt |

**交接纪律**：生产者在自己事务内落事件/任务并 commit（`autoflush=False` 注意显式 flush，见 `main.py:2897-2924` 教训）；消费者幂等处理（`UNIQUE` 约束 + consumed_at）；失败可重放，不丢事件。

---

## 8. 数据库变更与迁移顺序

1. `CREATE TABLE IF NOT EXISTS share_tasks / push_tasks(或改 push_queue) / resource_events / mirror_events`；加索引 `work_key,status,created_at`。
2. `ALTER TABLE push_queue ADD COLUMN share_task_id / status 扩展 / accept_worker / accepted_at`（走 `db.py:_COLUMNS` 幂等迁移模式）。
3. `distribution_events` 加 `share_task_id / push_task_ids`（可选，追更溯源）。
4. `resources.share_*` 保留为缓存，不删列。
5. **迁移顺序**：建表 → 双写期（旧链继续跑，新链影子写）→ 灰度切换触发点（A1-A7 逐个改指向分享中心）→ 全量切换 → 观察期后可选清理旧列（默认不清，留回滚）。
6. 回滚：代码开关回退到旧 `_enqueue_after_identify` 路径；新表保留不删，数据不丢。

---

## 9. 分阶段实施与验收用例

| 阶段 | 内容 | 验收用例（可执行） |
|---|---|---|
| A 建新 | 新表 + 分享中心模块 + 单测 | 建 first 任务按 B 库 share_dir_id 建分享；同 work_key 不重复建；update 复用链接且独立 ID |
| B 接触发 | A1-A7 改指向分享中心；镜像 post_identify 改事件 | 扫描识别后资源库无 QueueItem 直写、share_tasks 有对应任务；手动强推 force 语义保留 |
| C 交接分发 | submitted→accepted 原子领取；分发中心只读 share_url | 并发两 worker 只一方领取；单进程 _queue_lock 仍生效；share_url 为空不反建分享 |
| D 规则闭环 | 两条锁定规则显式事件 | 剧集新增集→原分享链接不变+再次推送；B 库删整部→取消分享+出库；A 库补齐→重入库+重分享推送 |
| E 前端 | 分享中心改名上移、任务中心更新 | 9 原入口语义保留、分享中心页可见任务/追更、移动端与深色样式不回归 |
| F 验收 | 全量回归+真机+文档 | ≥741 passed 稳定 ×2；`VERIFICATION_CHECKLIST.md` P0 全过；部署/回滚/API 文档齐备 |

### 关键回归断言（防破坏现有 Bot/互动）
- 现有 9 tab + 任务中心入口仍在（`tests/test_v104_*` 系列）。
- 投稿转存仍走光鸭原生 API、不经 TGtoDrive（`tests` 投稿相关）。
- 求片白名单 GET→合并→POST→回读幂等不变（`tests/test_v0915*` / tgto 相关）。
- strict 镜像暂停/恢复/跳过/取消四按钮与状态机不变（`tests/test_v105_fault_injection.py`）。
- 白名单闸门、401/403 分级、sendPhoto parse_mode=HTML 不变。

---

## 10. 已知问题与后续优化（本重构不解决，列清单）

- 三套作品键口径收敛需同时改 scanner/main/publisher，属高风险，建议阶段 A 单独提测。
- 分享中心「发布节奏」参数未提供，先按 FIFO + existing 2s 节流，后续再做 cron 级节奏。
- 多实例部署若成为目标，需把 `submitted→accepted` 领取做成数据库原子操作并压测，当前按单进程设计但预留字段。
- `Resource.share_*` 缓存与 share_tasks 真相源双写的一致性问题：需定义「以 share_tasks 为准」的对账任务，防止旧前端读到过期缓存。

---

> 结论：本设计以「分享中心前移 + 分发中心保持不动 + 事件化交接确认」为主线，四条强制审计项均落到具体表/状态机/接口；两条锁定业务规则由分享中心状态机 + 事件闭环直接实现。所有改动以审计报告 `1437b95` 基线为准，实施时逐阶段加回归门禁，避免破坏已有 Bot/群/频道/互动功能。
