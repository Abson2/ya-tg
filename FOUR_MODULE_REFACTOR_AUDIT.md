# Ya-TG 四模块重构 · 第一阶段源码审计报告

> 审计基线：HEAD `1437b95`（tag `v0.9.20-17-g1437b95`，整改阶段一~七已完成但**未发版**）
> 源码快照时间：2026-11-20　审计范围：backend/app 全量 + frontend/src/App.vue + 测试与文档
> 验收依据：锁定的实际源码（文件+行号）、真实改动记录、真实测试结果；不以 README / 自评为准
> 本阶段**不改任何业务代码**，只交付调用链、数据库关系、四强制审计项结论、风险与迁移计划

---

## 0. 结论先行

**重构可行性：可做，但必须走「先建新模块、后迁触发点、最后断旧链」的三步走，不能一次性重写。** [推断]+ 依据：分享/推送目前高度集中在 `main.py` 的 5 个识别/扫描入口 + `publisher.py` 单一消费函数，职责边界清晰但耦合点固定（共 9 处入队/消费锚点，见第 3 节），只要把锚点逐个改指向新「分享中心」，Bot 发送链路（`publisher._process_queue_impl`）可原样保留。

四个强制审计项结论（详见第 4 节）：

| # | 强制审计项 | 结论 | 置信度 |
|---|---|---|---|
| 1 | 自动分享触发点彻底梳理 | 已锁定 **9 个触发/消费锚点**，无隐藏入口；镜像模块确认零入队 | 高（≥2 处源码+注释+测试互证） |
| 2 | 分享对象必须用 B 库目录 ID | **现状已满足**：`share_dir_id` 来自 B 库扫描的 `dir_chain/parent_id`，`ensure_share` 按 `series_dir` 用目录 ID；风险在电影散放分类目录时的 file 级降级 | 高 |
| 3 | 更新推送要有独立事件 ID | **现状部分满足**：`DistributionEvent` 有独立 id + `work_key/fingerprint` 去重键，但它是「更新事件」不是「推送任务」，且队列项复用导致一次事件可能与首次推送共用同一 QueueItem | 高 |
| 4 | 分享中心与分发中心任务接收确认 | **现状缺失**：两中心尚未分离，无显式接收确认；`process_queue` 靠进程锁 + QueueItem 状态机隐式确认，多实例下有重复消费窗口 | 高 |

**最大的重构风险不是识别或 TG 发送，而是「分享生命周期被压缩在发送瞬间」**——`ensure_share()` 在 `publisher.py:474` 发送前才惰性建分享，分享没有独立的排序/节奏/追更状态。要前移成「分享中心」，必须先把「分享」从 `process_queue` 里剥离成独立可调度任务，这是本次重构的核心工作量。[推断]+ 依据：`publisher.py:34-78` + `:471-485`。

---

## 1. 审计基线核对

- [事实](来源:`git log -1`,时间:2026-11) HEAD = `1437b950a776711eba35471ffddeab935182cd16`，提交说明「docs: 真机验证清单（发版前逐项P0/P1验证，用户选先验证后发版）[skip ci]」。
- [事实](来源:`git describe --tags`,时间:2026-11) 最近 tag = `v0.9.20-17-g1437b95`，即 HEAD 在 v0.9.20 之后 17 个 commit，**整改版尚未升版打 tag**。
- [事实](来源:`git status`,时间:2026-11) 工作区仅 `docs/TgtoDrive联动复用说明.md` 一处未提交改动，与本次四模块重构无关。
- [事实](来源:`ls backend/app`,时间:2026-11) 后端 26 个 py 文件；`main.py` 282KB（单文件,含全部路由+调度+入队逻辑）、`bot_listener.py` 95KB、`mirror.py` 111KB、`publisher.py` 31KB、`models.py` 36KB。

---

## 2. 现有功能清单与四模块职责漂移

| 模块（目标） | 现状承载文件 | 现状职责 | 与目标边界的差异 |
|---|---|---|---|
| 镜像同步 | `mirror.py` / `mirror_batch.py` / `main.py:1257-1403` | A→B 复制、英文改名、strict 顺序引擎、暂停/恢复/跳过/取消 | **基本符合**；仅 `auto_identify` 钩子会跨入识别+推送（`_do_mirror_run`），需改为「通知资源库」而非直接调识别 |
| 资源库 | `scanner.py` / `main.py:2010-2400` / `models.Resource` | 扫描 B 库、TMDB/AI 识别、海报、完结回填、对账出库 | **职责外溢**：识别成功后**直接调 `_enqueue_after_identify` 入队**（main.py:2043/3154/3188/3226），且 `share_url/share_status` 字段也写在 Resource 表上，分享与资源混在同一行 |
| 推送队列→分享中心 | `main.py:2888-3030`（入队）/ `publisher.py:34-78,383-541`（建分享+发送） | 入队去重、建分享、消息构造、白名单闸门、Telegram 发送、重试分级 | **职责混杂**：分享生成、队列调度、TG 发送三者同处一个消费循环；要拆成「分享中心（前移）」+「分发中心（保留发送）」 |
| 分发与互动中心 | `bot_listener.py` / `main.py` 互动端点 / `tgto.py` / `contribution_restore.py` | Bot 求片/投稿/搜索/打赏、白名单、目录路由、TGtoDrive 联动、光鸭转存 | **须保持不动**；但它是当前「推送目标真相源」（`push_routes` / `is_push_allowed`），分享中心必须向它取目标而非另建一套 |

前端导航现状 [事实](来源:`frontend/src/App.vue:523-540`,时间:2026-11)：4 组 10 个入口——工作台（概览/任务中心）、光鸭资源（光鸭授权/镜像同步/资源库）、分发与互动（**分发与互动中心**/**推送队列**）、系统（TMDB 设置/消息模板/操作审计）。「推送队列」位于「分发与互动」组内且排位靠后，与用户要求「改名分享中心、位置上移」一致——改造点是**改名 + 上移到「光鸭资源」组**，并把分享生命周期页面从镜像同步页剥离。

---

## 3. 现有数据流全景与调用链（强制审计项 1）

### 3.1 触发→入队→消费 全链路（带行号）

```
[A 源头库]  --mirror.py(sync_rule strict/批量)-->  [B 镜像库]
                                                      │
                              ┌───────────────────────┘
                              ▼ run_scan(B库) → scanner.py
                     [Resource 表：识别/海报/TMDB]
                              │
          ┌───────────────────┼────────────────────┐
          ▼                   ▼                    ▼
   ①识别成功入队        ③内容变化事件         ④对账出库
 _enqueue_after_identify  DistributionEvent      _reconcile_library
 (main.py:2043/3154/      (scanner.py:139-143)    (main.py:2085-2161)
  3188/3226/2210)              │
          │               process_change_events     取消分享+清队列+删Resource
          ▼                   (main.py:2164)              (main.py:2147/2151/2153)
   _enqueue_ready ─────────────┘
   (main.py:2893-2924)
          │  db.add(QueueItem) @2921
          ▼
   [push_queue 表：pending/review/retry/sent/failed]
          │
          ▼ process_queue (publisher.py:536) → _process_queue_impl (:383)
   ┌──────┴───────┐
   ▼              ▼
ensure_share    白名单闸门 is_push_allowed (bot_listener)
(:34-78,建分享)  (:454-467)
   │              │
   └──────┬───────┘
          ▼ _send_telegram(:486) → [频道/群聊] + AuditLog
```

### 3.2 入队/分享/消费锚点全清单（9 个，无遗漏）

| # | 锚点 | 文件:行 | 触发场景 | 是否尊重海报闸门 | 备注 |
|---|---|---|---|---|---|
| A1 | `_enqueue_after_identify` | `main.py:3154` | 手动「应用匹配」identify | 是 | 强制 |
| A2 | `_enqueue_after_identify` | `main.py:3188` | 单资源 auto-identify | 是 | 强制 |
| A3 | `_enqueue_after_identify` | `main.py:3226` | 批量 identify-all | 是 | 强制 |
| A4 | `_enqueue_after_identify` | `main.py:2043` | 扫描后识别 `_post_scan_identify` | 是（受 `publish_after_identify` 控制） | 镜像管线也经此 |
| A5 | `_enqueue_after_identify` | `main.py:2210` | 更新事件无历史队列项时兜底 | 是 | `process_change_events` |
| A6 | `publish_resource` 直接 add(QueueItem) | `main.py:3901/3929` | 手动「立即推送」 | **否**（强制推送绕过） | ANY 状态重置 pending，含已 sent |
| A7 | `_enqueue_existing` | `main.py:3816-3832` | 保存规则后回溯已入库资源 | 是 | 遵守同作品同频道去重 |
| C1 | `process_queue` 定时 worker | `main.py:466-473` | scheduler 每 30s/cron | — | `max_instances=1, coalesce=True` |
| C2 | `process_queue` 手动触发 | `main.py:3867`（`/api/queue/run`）/ `main.py:3932`（publish 后立即） | 后台按钮 | — | 与 C1 靠 `_queue_lock` 互斥 |

[事实](来源:`main.py:2921`,时间:2026-11) 除 A6 外，所有入队最终汇聚到 `_enqueue_ready()` 的唯一 `db.add(QueueItem(...))`，去重口径为「`_work_key(x)` 相同 + 同 channel 已有任意队列项则跳过」，`db.flush()` 显式落库以对抗 `autoflush=False`（`main.py:2897-2924`）。

[事实](来源:`main.py:3888-3902`,时间:2026-11) `publish_resource` 是唯一绕过 `_enqueue_ready` 的入口：手动强制动作绕过海报闸门、无视去重，直接在路由目标与规则目标上 upsert QueueItem 并立即 `process_queue(limit=50)`。这是「手动推」与「自动推」语义分叉点，迁移时须保留其「强制重推」语义，但应改为向分享中心提交 force 任务。

### 3.3 镜像模块的触发边界（重要）

[事实](来源:`mirror.py:8` 与 `:799` 注释,时间:2026-11) 镜像模块明确「绝不写 push_queue、绝不调用 `_enqueue_*`/`process_queue`，推送统一由 post_identify 管线负责」。
[事实](来源:`main.py:1257-1270` `_do_mirror_run`,时间:2026-11) 镜像同步完成后按 `auto_identify` 开关进入 post_identify 管线，最终经 A4（`:2043`）入队。
[推断]+ 重构后应把这条「镜像→识别→入队」链路改为「镜像完成 → 发事件给资源库 → 资源库识别 → 发任务给分享中心」，镜像模块本身继续零推送调用。

### 3.4 定时与周期任务

- [事实](来源:`main.py:466-473`,时间:2026-11) 队列 worker：`process_queue`，trigger 来自 `_queue_trigger`（`main.py:460`，默认 30s，可 cron），`replace_existing` 热更新。
- [事实](来源:`main.py:476-495`,时间:2026-11) 定时扫描：`run_scan(B库)` → `_post_scan_identify(publish=True)` → `process_change_events(publish=True)`；**不做对账删除**（避免抖动误删）。
- [事实](来源:`main.py:2222-2251` `_refresh_library_from_b`,时间:2026-11) 三个手动刷新入口统一：run_scan → 对账 → 识别 → `process_change_events`；定时/镜像后不走它（不删数据、不批量群推）。

---

## 4. 四个强制审计项逐条结论

### 4.1 强制项 1：原自动分享触发点必须彻底梳理

**结论：已彻底梳理，共 9 个锚点（7 入队 + 2 消费），另有 1 个手动建分享端点、1 个出库入口。** 置信度：高（源码行号 + 注释 + 测试三方互证）。

- [事实](来源:第 3.2 节全表,时间:2026-11) 入队 7 处（A1-A7）、消费 2 处（C1-C2）。
- [事实](来源:`main.py:2750-2791` `/api/resources/{rid}/create_share`,时间:2026-11) 存在手动建分享端点：同目录复用兄弟 share_url，否则调光鸭创建；**只建分享不入队**。
- [事实](来源:`main.py:2818-2825`,时间:2026-11) 删除分享时清空 `share_url/share_id`，防止 `ensure_share` 复用已删链接。
- [假设] 用户担心「隐藏的自动分享入口（如 TMDB 回调、Bot 收到消息直接推）」。若不成立应对：已全量 grep `_enqueue_*`/`QueueItem(`/`create_share`/`ensure_share`/`process_queue`，Bot 侧（`bot_listener.py:829-831`）只用 `publisher._build_message` **构造文本**（搜索卡片 sendPhoto），不写 QueueItem、不建分享，故无隐藏入口。

### 4.2 强制项 2：分享对象必须使用 B 库目录 ID

**结论：现状已满足，剧集/电影都按 B 库目录 ID 分享，file 级降级仅在无目录线索时兜底。** 置信度：高。

- [事实](来源:`utils.py:525-583` `series_dir_info`,时间:2026-11) `dir_id` 取自 `dir_chain[0]`（未整理剧集/电影主目录）、`dir_chain[1]`（已整理剧名目录）或 `parent_id` 兜底；`dir_chain` 与 `parent_id` 均由 B 库 `run_scan` 写入（`scanner.py`），**不是 A 库 ID**。
- [事实](来源:`main.py:2881-2885` + `:2904-2908`,时间:2026-11) `_apply_share_scope` 与 `_enqueue_ready` 都现场补算 `share_dir_id`，旧数据缺目录 ID 也会补齐，保证「推送走整目录分享而非单文件」。
- [事实](来源:`publisher.py:45-63` `ensure_share`,时间:2026-11) `scope == "series_dir" and dir_id` → `target_id = dir_id`（目录 ID）；同目录兄弟资源复用 share_url（`:47-56`），每目录仅一条分享。
- [风险] [事实](来源:`utils.py:551-558`,时间:2026-11) 电影散放在「分类目录」（如 `/国产电影/xxx.mkv`）时 `scope="file"`，按单文件分享——此时分享对象是文件 ID 而非目录 ID。这是合理降级（避免把整类当一部作品），但与「一律目录 ID」的字面要求有偏差，重构时应作为**显式规则**写进分享中心，而非隐式行为。

### 4.3 强制项 3：更新推送要有独立事件 ID

**结论：现状有独立「更新事件」记录，但没有独立「推送任务」ID；一次内容更新可能与首次推送共用同一 QueueItem，追更语义被压缩。** 置信度：高。

- [事实](来源:`models.py:450-461` `DistributionEvent`,时间:2026-11) 有独立主键 `id`、`work_key`、`fingerprint`（唯一约束 `uq_event_work_fingerprint`）、`event_type`、`queue_id`、`share_reused`。这是**作品级变化事件**，不是推送任务。
- [事实](来源:`scanner.py:123-144`,时间:2026-11) 同一 `(work_key, 变化指纹)` 只落一条事件，一批 36 集同时变化只触发一次；注释明确「一批多集同时被替换也只触发一次推送」。
- [事实](来源:`main.py:2200-2216`,时间:2026-11) `process_change_events` 处理事件时：**优先复用已存在的 QueueItem**（`q.status="pending"` 重置，`ev.queue_id=q.id`），只有无历史队列项才走 A5 新建。即「更新推送」与「首次推送」共用同一队列行。
- [推断]+ 缺口：现有模型无法回答「同一作品第 2 次追更推送是哪条任务、复用链接是否仍有效、上次推送时间」。用户要求「更新推送要有独立事件 ID」，应新增 `share_task` / `push_task` 独立任务表，让每次推送（首次/追更/手动强制）都有独立 ID，并 `parent_event_id` 关联 `DistributionEvent`。
- [假设] 若产品确认「更新只需重置原队列项即可」，则不成立应对：至少要在 QueueItem 上补 `push_reason`（first/update/force）与 `event_id` 字段，使追更可追溯——但这样队列会膨胀，不如独立任务表干净（见设计文档 §4）。

### 4.4 强制项 4：分享中心与分发中心要有清晰的任务接收确认

**结论：现状两中心未分离，无显式接收确认；仅靠进程锁 + QueueItem 状态机隐式确认，多实例/并发下有重复消费窗口。** 置信度：高。

- [事实](来源:`publisher.py:530-541`,时间:2026-11) `process_queue` 用模块级 `_queue_lock = asyncio.Lock()` 非阻塞拿锁，锁占用时返回 `skipped=True`。**这是进程内锁**，仅保证单进程内定时 worker 与手动触发互斥。
- [事实](来源:`main.py:469-473`,时间:2026-11) scheduler 侧另用 `max_instances=1, coalesce=True` 防同一 job 重入——与 `_queue_lock` 是两套独立机制。
- [事实](来源:`publisher.py:396-401`,时间:2026-11) 消费端直接 `select(QueueItem).where(status in pending/retry, next_retry_at<=now)`，**没有「已领取/processing」中间态**，一条 pending 在被 `_send_telegram` await 期间（`publisher.py:486`）对其他并发调用仍可见。
- [推断]+ 缺口：跨进程（多 worker / 重启）或未来拆成两服务时，分享中心「已提交任务」与分发中心「已接收任务」之间没有握手，可能重复推送。要建 `share_task` 的 `status=submitted→accepted→sending→done` 状态机 + 分发中心领取时原子 `accepted`（`UPDATE ... WHERE status=submitted RETURNING` 语义），才能真正满足「清晰的任务接收确认」。
- [假设] 若部署永远是单进程 + 现有两把锁已足够，则不成立应对：重构后分享中心与分发中心同进程但代码分离，仍需保留接收确认字段，否则未来水平扩展必然踩坑；成本仅是几个字段 + 一次原子更新。

---

## 5. 数据库表关系报告

### 5.1 表清单与模块归属

| 表（model 类） | 定义行 | 归属模块（现状→目标） | 关键字段 | 与其他表关系 |
|---|---|---|---|---|
| `resources` (Resource) | `models.py:8-63` | 资源库（保留） | `file_id`(B库)、`share_dir_id/share_scope/share_url/share_status`(**分享字段外溢**)、`identify_*`、`finish_*`、`tmdb_id` | 被 push_queue 引用；`file_id` 与 MirrorFile.target_file_id 逻辑对应 |
| `push_queue` (QueueItem) | `models.py:217-229` | 推送队列→**分享中心** | `resource_id`,`channel_id`,`status`,`attempts`,`error`,`next_retry_at`,`telegram_message_id`,`sent_at` | FK→resources.id、channels.id；唯一约束 `uq_resource_channel` |
| `message_templates` | `models.py:232-239` | 分发（保留） | `content`,`is_default`,`enabled` | 被 publisher._build_message 读取 |
| `channels` (Channel) | `models.py`（Bot 段） | 分发与互动（保留） | `bot_id`,`chat_id`,`kind`,`enabled` | FK→bots；被 QueueItem/规则引用 |
| `bots` (Bot) | `models.py`（Bot 段） | 分发与互动（保留） | `token_enc`,`username`,`enabled` | 被 Channel 引用 |
| `rules` (Rule) | `models.py`（Rule 段） | 分发（保留，旧兼容） | `channel_id`,`directory_id`,`keyword`,`auto_publish`,`enabled` | 旧分发真相源，v0.9.5 后 `push_routes` 优先 |
| `distribution_events` | `models.py:450-461` | 推送队列/分享中心 | `work_key`,`fingerprint`,`event_type`,`queue_id`,`share_reused` | `queue_id`→push_queue.id（弱关联，无 FK）；唯一 `uq_event_work_fingerprint` |
| `resource_scan_batches` | `models.py:270-281` | 资源库 | `status`,`complete`,`resumed`,`reconcile_allowed`,`seen_json` | 对账删除唯一权威闸门 |
| `scan_tasks` / `scan_cursors` | `models.py:66-96` | 资源库 | `cron`,`root_id`,`visited_json`,`dir_chains_json` | 扫描断点 |
| `mirror_rules` / `mirror_files` / `mirror_media` / `mirror_tasks` | `models.py:350-437` | 镜像同步（保留） | `batch_id`,`sequence_id`,`work_key`,`target_file_id`,`task_status` | `work_key` 与资源库作品键同口径；`MirrorFile` 隔离 A/B fileId |
| `user_contributions` | `models.py:464-487` | 分发与互动（保留） | `status`,`dedup_key`,`share_url`,`tmdb_id` | 投稿去重 |
| `contribution_restores` | `models.py:490+` | 分发与互动（保留） | `contribution_id`,`status`(restoring/success/failed) | 1:1 user_contributions |
| `media_requests` | `models.py`（MediaRequest） | 分发与互动（保留） | `title`,`tmdb_id`,`status`,`season` | 求片；联动 tgto 白名单 |
| `app_config` / `auth_tokens` / `auth_sessions` / `audit_logs` | `models.py:99-258` | 系统 | 凭证/会话/审计 | 横切 |

### 5.2 关键关系与数据交接点

1. **B 库文件身份链**：`MirrorFile.target_file_id`（B 镜像 fileId）→ 被 `run_scan` 扫描成 `Resource.file_id` → `series_dir_info` 由 B 库目录链算出 `Resource.share_dir_id` → `ensure_share` 用 `share_dir_id` 建分享。**全程 B 库 ID，A 库 ID 只在 MirrorFile 映射表内。** [事实](来源:`models.py:355-375` + `utils.py:525-583` + `publisher.py:45-63`,时间:2026-11)
2. **作品聚合键统一口径**：`utils.work_group_key()`（`utils.py:197-224`）被 scanner（变化事件）、main（展示/入队去重 `_work_key`）、publisher（运行时去重 `_work_group_key`）三处共用，但**实现有两套**：`utils.work_group_key` 用 `work:{作品名}`，`main._work_key` 走 `_series_group_key`，`publisher._work_group_key` 用 `dir:/tmdb:/file:`。[风险] 三套键口径不完全一致，是「一张卡=一条推送」的潜在裂缝，重构时应收敛为**一个权威作品键服务**。
3. **分享时机**：`Resource.share_url/share_id/share_status` 写在资源行上，`ensure_share` 惰性回填（`publisher.py:67-75`）。**分享不是独立实体**——这是与「分享中心负责分享生命周期」目标最大的结构差异。
4. **出库闭环现状**：`_reconcile_library` 对账删除时取消光鸭分享 + 清 QueueItem + 删 Resource（`main.py:2141-2157`），但**不触发镜像补齐**（镜像是独立定时任务，B 库删除后 A 库仍在则由下一轮 mirror 自动补回，但需要下一轮识别/入队重新走一遍）。用户锁定的规则 2「B 库删除→出库→镜像补齐→重新入库→重新分享推送」目前是**半自动**（依赖镜像周期 + 刷新入口），需设计文档补成显式事件闭环。

---

## 6. 两条锁定业务规则的现状落点

**规则 1：一部电视剧只分享一次整部剧目录；更新复用原链接但再次推送。**
- [事实] 「只分享一次」：现状通过 `ensure_share` 同目录兄弟复用（`publisher.py:47-56`）+ `_enqueue_ready` 同作品同频道去重（`main.py:2913-2920`）+ publisher 运行时 `sent` 作品键去重（`publisher.py:436-446`）三重保证。
- [事实] 「更新再次推送」：`process_change_events` 重置原 QueueItem 为 pending 重推（`main.py:2203-2208`），`ev.share_reused=True`（`:2197-2199`）表示复用链接。
- [缺口] 「整部剧目录」粒度：`series_dir_info` 对已整理剧集取 `dir_chain[1]`（剧名目录）为整剧目录，未整理取直接父目录；**但季目录场景（Season 01）当前分享的是季目录上上层的剧名目录**（`utils.py:568-571`），符合「整部剧只分享一次」。规则已基本落实，缺的是**显式化**与独立任务 ID（见强制项 3）。

**规则 2：B 库整部剧删除→资源库出库、分享中心取消原分享；A 库仍在则镜像补齐→重新入库→重新分享推送。**
- [事实] 「出库+取消分享」：`_reconcile_library` 已实现（`main.py:2138-2157`），但只在手动刷新/重新扫描入口触发（`_refresh_library_from_b`，`:2222-2251`），定时任务不做对账（`:476-495`）。
- [事实] 「镜像补齐」：镜像定时任务独立运行，B 库缺失文件会被下一轮 mirror 重新复制（A 库只读保留）；`main.py:2311-2322` 镜像后有识别+入队路径。
- [缺口] 「重新分享推送」：补齐后新 Resource 是新行，`share_url` 为空，会重新建分享——符合预期；但整个闭环**没有一个统一事件串起来**，靠多个定时任务 + 刷新入口的时序巧合。设计文档需定义 `resource_removed` / `resource_restored` 事件，让镜像→资源库→分享中心的闭环可观测、可重试。

---

## 7. 可复用代码与必须修改代码

### 7.1 可原样复用（不动或仅挪位置）
- `publisher._process_queue_impl` 的 TG 发送主体：`_send_telegram`、`_build_message`、白名单闸门、错误分级、节流/退避、AuditLog（`publisher.py:91-527`）——**分发与互动中心保持不动的核心**。
- `bot_listener`：push_routes / match_push_routes / is_push_allowed / route_target_channel / 求片投稿会话——目标真相源与互动逻辑，全部保留。
- `mirror.py` / `mirror_batch.py`：镜像引擎与严格顺序状态机，保留。
- `utils.series_dir_info` / `work_group_key`：目录 ID 计算与作品键，保留（建议收敛为单一权威实现）。
- `tgto.py` / `contribution_restore.py`：TgtoDrive 联动与光鸭转存，保留。

### 7.2 必须修改
- `main.py:2043/3154/3188/3226/2210`：5 处 `_enqueue_after_identify` → 改为向**分享中心**提交任务（资源库只发「已识别」事件）。
- `main.py:2888-3030`（`_enqueue_ready` / `_enqueue_after_identify`）：入队去重逻辑上移到分享中心。
- `main.py:3816-3832`（`_enqueue_existing`）：规则回溯→改为分享中心的「补排」。
- `main.py:3870-3940`（`publish_resource`）：手动强推→改为向分享中心提交 force 任务，TG 发送仍交分发中心。
- `publisher.py:34-78`（`ensure_share`）+ `:471-485`：分享生成从发送循环剥离，改由分享中心调度。
- `publisher.py:383-541`：消费端加「接收确认」状态机，只消费分享中心 accepted 的任务。
- `models.py`：新增分享中心任务/事件表；`Resource` 分享字段迁移或保留为缓存（见设计文档 §5）。
- `frontend/src/App.vue:523-540`：`推送队列` 改名 `分享中心` 并上移；剥离分享生命周期页面；任务中心（`:571-640`）改指向新模块。

---

## 8. 数据库变更方案（建议，待设计文档细化）

1. 新增 `share_tasks`（分享中心任务表）：`id`,`work_key`,`resource_id`,`scope`(first/update/force),`share_dir_id`,`share_id`,`share_url`,`share_status`,`status`(planned/accepted/sending/published/failed/cancelled),`parent_event_id`,`attempts`,`next_retry_at`,`created_at`,`published_at`。
2. 新增 `push_tasks` 或复用 `push_queue` 加字段：`task_id`→share_tasks.id、`push_reason`、`accepted_at`、`accept_worker`、`event_id`。**倾向复用 push_queue 加列**（改动小、兼容存量），分享中心→分发中心的交接列就是 `status=submitted→accepted`。
3. `distribution_events` 增 `share_task_id`、`push_task_ids`（JSON）追踪一次变化事件对应的分享/推送任务。
4. `resources` 的 `share_*` 字段保留为**只读缓存**（兼容旧前端与导出），真相源迁到 share_tasks；双写期以 share_tasks 为准。
5. 迁移脚本：新表 `CREATE TABLE IF NOT EXISTS`，新列走 `db.py:_COLUMNS` 的 `ALTER TABLE ... ADD COLUMN` 幂等模式（`db.py` 已有此机制），不删旧列，支持回滚。

---

## 9. 风险清单（按优先级）

| 级别 | 风险 | 依据 | 缓解 |
|---|---|---|---|
| P0 | 分享从发送循环剥离后，TG 发送拿不到 share_url 或重复建分享 | `publisher.py:471-485` 依赖 ensure_share 先于发送 | 分享中心须在 accepted 前完成建分享并把 url 写入任务，分发中心只读不建 |
| P0 | 入队去重三套键口径不一致导致重复推送/漏推 | `utils.work_group_key` / `main._work_key` / `publisher._work_group_key` 三套实现 | 统一为单一作品键服务，迁移期三处对齐并加回归测试 |
| P0 | 手动强推 A6 绕过闸门，改造后语义丢失 | `main.py:3888-3902` | 分享中心保留 force 类型，显式记录 push_reason=force |
| P1 | 分享中心与分发中心交接无原子确认，多实例重复消费 | `publisher.py:533` 进程锁局限 | 任务表 submitted→accepted 原子领取 + worker_id |
| P1 | 追更事件与首次推送共用 QueueItem，无法追溯 | `main.py:2203-2208` | 独立 share_task ID + parent_event_id |
| P1 | B 库删除→镜像补齐闭环靠时序巧合，不可观测 | `main.py:2138-2157` + 独立镜像定时 | 显式 resource_removed/restored 事件 |
| P1 | 前端 10 入口改名/上移破坏既有 9 tab 与任务中心聚合 | `App.vue:523-540,571-640` | 保留 9 tab 语义，新增分享中心页面，任务中心 ref 指向更新 |
| P2 | 现有 741 测试中断言源码结构（如 process_queue 包装器）需同步更新 | summary: B-01 测试兼容处理 | 迁移时同步更新测试，保持全量门禁 |

---

## 10. 分阶段迁移计划（建议顺序）

0. **基线冻结**：以 HEAD `1437b95` 为起点，本阶段审计通过后不再往旧链加功能；整改版发版决策（先真机验证）与本重构并行排队，避免两次大改叠加。
1. **阶段 A（建新）**：新增 share_tasks 表 + 分享中心模块（建分享/排序/节奏/追更状态机），暂不接触发点，用单测覆盖。
2. **阶段 B（接触发）**：5 个自动识别入口（A1-A5）+ `_enqueue_existing`（A7）改指向分享中心；镜像 post_identify 改为事件通知。
3. **阶段 C（交接分发）**：分享中心→分发中心 submitted→accepted 交接；`publisher` 消费 approved 任务；保留原 TG 发送主体不动。
4. **阶段 D（规则闭环）**：实现两条锁定业务规则的显式事件（整剧单分享+更新重推、B库删除→镜像补齐→重入库→重分享）。
5. **阶段 E（前端）**：推送队列改名分享中心并上移，剥离分享生命周期页，任务中心更新指向。
6. **阶段 F（验收）**：全量回归 ≥741 passed、真机验证清单、部署/回滚文档、内部 API 文档。

---

## 11. 待核实项（需用户/真机确认）

- [待核实] 「一部电视剧只分享一次整部剧目录」中，跨季（S01/S02 分目录存放）是否算「一部剧」 sharing 一次，还是每季一次？现状 `series_dir_info` 取剧名目录（上上层）为整剧目录，倾向一次。
- [待核实] 分享中心是否需要「发布节奏」（如每天 N 部、错峰），任务书提到但未给具体参数；现状仅有 TG 逐条 2s 节流（`publisher.py:388`）。
- [待核实] B 库删除后的镜像补齐周期（当前 `interval_minutes` 默认 60，`main.py:535`），是否满足规则 2 的「自动补齐」时效要求。
- [待核实] 多实例部署是否为目标场景，决定强制项 4 是否必须做跨进程原子领取（若永远单进程可降级为字段留痕）。

---

> 附注：本报告所有 `[事实]` 均带源码文件+行号，取自基线 `1437b95` 的实际源码快照；`[推断]`/`[假设]` 已标注推理链条与不成立应对。第二阶段设计文档《FOUR_MODULE_REFACTOR_DESIGN.md》将基于本报告展开四模块接口、事件模型、迁移顺序与验收用例。本阶段未修改任何业务代码。
