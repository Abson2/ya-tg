# 阶段二路线 B：严格顺序镜像引擎 设计 + 迁移方案

> 基线：`d91c4c10`（审计锁定）+ `2828d24`（C-01 修复）。
> 依据：`AUDIT_REPORT.md` 第 5 节（假设 vs 真实代码逐条核对）、`FEATURE_PRESERVATION.md` 功能红线。
> 用户决策：路线 B（功能一致、效率更高、耦合更好）；新增暂停/恢复/跳过/取消；隔离测试；全部完成统一发版。
> 本文是实施蓝图，非最终结论；每个 commit 后以真实测试结果校正。

---

## 0. 红线（改动不得触碰）

以下均为用户长期维护形成、审计确认正确的机制，**路线 B 只改"复制调度层"，不改这些**：

| 红线 | 真实位置 | 保护方式 |
| --- | --- | --- |
| 每文件立即 commit（无跨 await 写事务） | `mirror.py:942/974/979/1008` | 顺序执行器仍遵守：写状态→commit→await 网络→写结果→commit |
| 按名+大小回填 fileId | `mirror.py:434-446/838-881/1052-1143` | `_lookup_b_file` / `_b_file_candidates` 原样复用 |
| 英文名四层兜底解析 | `mirror.py:1174-1285` | 完全不改 |
| token 每请求重读 DB + 117 刷新 | `guangya.py:161-192/247/284` | 不重复实现、不回退 |
| 先核验远端再决定复制（防重复） | `mirror.py:838-881` | 顺序执行器每步仍走 `_lookup_b_file` |
| 目录 1:1 / `*`→`·` / plain_mode 降级 | `mirror.py:699-753` | `_ensure_child` 原样复用 |
| works_cursor 作品级轮转 | `mirror.py:755-766/1011-1016` | 保留为"批次选取"单位 |
| 镜像后不写队列、不群推 | `mirror.py` 全文 | 本模块不碰 push_queue |
| API URL / 前端入口 / 端口 / 数据目录 | 全局 | 全部保持不变 |

---

## 1. 现状真实执行链路（审计证据）

```
sync_rule (mirror.py:609)
  ├─ _walk_source          :484  DFS 栈 stack.pop()，目录/文件按网盘返回顺序（无排序）
  ├─ _discover_works       :172  DFS 栈 stack.pop()，作品顺序不确定（无排序）
  ├─ plan_work_batch       :212  works_cursor 轮转切批（保留）
  └─ for work in batch:          :768  逐部原子处理
       ├─ _ensure_child×N        建/映射 B 库目录（父失败 break）
       ├─ _b_file_candidates     列 B 库目录，按名+大小回填（:838-881）
       ├─ 构建 copy_jobs          :883-946（作品内全部待复制文件，按网盘顺序）
       ├─ for job: copy_file()    :961-982  批量提交（全部发完）
       ├─ _poll_tasks_batch       :983      批量轮询（统一等所有任务）
       └─ 逐个判定 done/failed/unknown，更新 MirrorFile.status
```

**差距**（任务书要求但当前不存在）：确定性自然排序、DFS 固定任务清单、sequence_id 持久化、严格"前一个完成才提交下一个"、父目录失败不进子目录的任务级依赖、暂停/恢复/跳过/取消、受控并发开关。

---

## 2. 目标架构

### 2.1 分层（降低耦合）

保留 `sync_rule` 作为编排入口，内部拆为三阶段（不新增独立文件，避免大范围 import 冲击测试）：

```
sync_rule
  ├─ [规划] _plan_strict_batch()   → 生成本批固定任务清单（含排序、DFS 序号、父子依赖），落库
  ├─ [执行] _execute_strict_batch()→ 严格顺序执行器（单任务推进 + 控制指令响应）
  └─ [收尾] _finalize_batch()      → 批次状态、统计文案、游标推进（复用现有文案逻辑）
```

### 2.2 确定性自然排序

新增纯函数 `natural_key(name)`：`re.split(r'(\d+)')` 后数字段转 int、非数字段 lower，实现 `S02` 在 `S10` 前、`E9` 在 `E10` 前。

排序注入点（**只排序，不改唯一键/映射/指纹**）：
- `_walk_source`：`children` 与 `files` 收集后按 `natural_key(_dir_name)` 排序再入栈（保证 DFS 稳定）
- `_discover_works`：根目录 `children` 按 `natural_key(name)` 排序
- `_media_files`：目录 `children`/`files` 按 `natural_key` 排序
- `plan_work_batch`：入参 works 已按 discoveries 顺序（确定）

### 2.3 任务清单与序列持久化（DB 扩展）

新增列（**全部带默认值，旧库可 ALTER，旧数据不迁移语义**）：

`mirror_rules`：
- `batch_id TEXT DEFAULT ''` — 当前/最近批次 ID
- `batch_status TEXT DEFAULT 'idle'` — idle/running/paused/cancelling/cancelled
- `control_action TEXT DEFAULT ''` — pause/resume/skip/cancel 指令（执行器轮询消费后置空）
- `cursor_sequence INTEGER DEFAULT 0` — 上次完成的 sequence（恢复点）

`mirror_tasks`（扩展现有表）：
- `batch_id TEXT DEFAULT ''`
- `sequence_id INTEGER DEFAULT 0` — 批次内严格序号（DFS 顺序）
- `parent_sequence_id INTEGER DEFAULT 0` — 父目录任务序号（0=无依赖）
- `task_status TEXT DEFAULT ''` — 空(旧数据)/pending/submitting/submitted/verifying/completed/failed/skipped/cancelled
- `retry_count INTEGER DEFAULT 0`

**旧数据兼容**：`task_status=''` 的旧 MirrorTask 一律按"历史记录"看待，不参与新执行器；`status` 旧值（pending/running/done/failed/unknown）原样保留，前端现有展示不变。

### 2.4 严格顺序执行器

```
for task in sorted(pending_tasks, key=sequence_id):
    # 1) 控制指令
    if control_action == pause:  batch_status='paused'; return
    if control_action == cancel: 标记余下 cancelled; return
    if control_action == skip:  当前任务标 skipped; 消费指令; continue

    # 2) 父依赖
    if parent 未完成(非 completed/skipped/failed): 跳过本任务本轮（保持 pending）

    # 3) 远端核验（防重复）
    if _lookup_b_file(candidates, row): row→synced; task→completed; continue

    # 4) 提交（B-09 占位行：先写 MirrorTask 再 copy_file）
    task.task_status='submitting'; commit
    task_id = await api.copy_file(...)
    task.task_id=task_id; task.task_status='submitted'; commit

    # 5) 轮询到终结（复用 _task_outcome，unknown 先核验远端）
    outcome = await _wait_one(task)
    if done: MirrorFile→synced; task→completed
    if failed: MirrorFile→failed; task→failed
    if unknown: 核验 B 库，命中→completed，否则保留 submitted 待下轮
    commit
    # 6) 只有当前任务终结，才推进下一个（严格顺序核心）
```

**受控并发**：默认 1（严格顺序）；`concurrency` 参数 >1 时按 sequence 分组并行，但同 `parent_sequence_id` 子树仍串行，且写接口间隔 `file_idle_seconds`（≥MIN_FILE_IDLE_SECONDS）不变，不突破 2 次/秒红线。首版只实现并发=1，>1 作为后继开关（避免一次改动过大）。

### 2.5 暂停/恢复/跳过/取消（状态机）

- `POST /api/mirror/batch/pause` → `control_action='pause'`（执行器在下一个任务边界停下，不中途腰斩复制）
- `POST /api/mirror/batch/resume` → `batch_status='running'`, `control_action=''`
- `POST /api/mirror/batch/skip` → `control_action='skip'`（跳过当前任务）
- `POST /api/mirror/batch/cancel` → `control_action='cancel'`（余下任务标 cancelled）
- `GET /api/mirror/batch/status` → 返回 batch_id/status/进度/当前 sequence
- 所有操作二次确认由前端负责；后端幂等（重复 pause/resume 不改状态）。

### 2.6 重启恢复

新 `sync_rule` 入口先查 `mirror_rules.batch_status in ('running','paused')` 且 `batch_id` 非空：
- 若存在未完成 MirrorTask（task_status != completed/failed/skipped/cancelled），从最小 sequence 继续（跳过已完成）
- 否则按 works_cursor 规划新批次
- 定时 job 与手动 `/api/mirror/run` 共用此入口，重启后定时 job 自动续跑（修复现状"taskId 需手动 poll"缺口）

### 2.7 增量进下一批

批次规划冻结：进入执行阶段时，任务清单已固定落库；本轮 `_walk_source` 发现的新文件**不插入当前批次**，留待下一轮（符合任务书 6.12）。works_cursor 在 `_finalize_batch` 推进。

---

## 3. 迁移方案（兼容旧库）

1. `models.py`：在 `MirrorRule`/`MirrorTask` 增加上述列（带 default，nullable 友好）。
2. `db.py:_COLUMNS`：为 `mirror_rules`/`mirror_tasks` 增加 ALTER 定义，旧库启动自动补列。
3. **不改**任何现有列的类型/约束/唯一键；不新增唯一约束（避免旧数据冲突）。
4. 迁移幂等：沿用 `migrate()` 的 `inspect` + `ALTER TABLE ADD COLUMN` 范式。
5. 验证：拷贝一份旧库（无新列）跑 `migrate()`，断言启动成功、旧 MirrorTask 可读、`task_status=''`。

---

## 4. 功能一致性对照（重构前后逐项）

| 功能 | 重构前 | 重构后 | 一致 |
| --- | --- | --- | --- |
| 目录 1:1 / 自动建目录 / 降级 | `_ensure_child` | 原样复用 | ✓ |
| B 库缺文件补复制 | `:844-862` | 规划阶段原样复用 | ✓ |
| 按名+大小回填防重复 | `_lookup_b_file` | 执行阶段每步复用 | ✓ |
| 每文件立即 commit | 是 | 是 | ✓ |
| 英文名解析/改名 | 独立函数 | 不改 | ✓ |
| works_cursor 轮转 | 有 | 保留 | ✓ |
| 统计文案 last_message | 长串拼接 | 原样复用 | ✓ |
| 镜像后不推送 | 是 | 是 | ✓ |
| 复制调度 | 批量提交+批量轮询 | **严格顺序单任务推进** | 变更（目标） |
| 排序 | 无 | **确定性自然排序** | 变更（目标） |
| 序列持久化 | 无 | **batch_id/sequence_id** | 新增（目标） |
| 暂停/恢复/跳过/取消 | 无 | **新增** | 新增（目标） |

---

## 5. 分 commit 实施计划

1. **C1 设计与迁移骨架**：本文 + `models.py` 新列 + `db.py:_COLUMNS` + 旧库迁移测试。
2. **C2 确定性排序**：`natural_key` + 三个注入点 + 单测（不改调度）。
3. **C3 严格顺序执行器**：规划/执行/收尾三阶段 + 序列落库 + 远端核验 + 重启恢复；保留旧 `sync_rule` 行为开关（默认新引擎，异常可回退）。
4. **C4 批次控制状态机与 API**：pause/resume/skip/cancel/status 端点 + 前端按钮 + 二次确认。
5. **C5 测试与隔离性能基线**：自然排序/DFS/序列依赖/故障注入单测 + 100/1000/10000 mock 性能测试。
6. **C6 全量回归**：614+ 例全绿 + 功能对照表逐项验证。

每步 `[skip ci]` 源码验证 commit，全部完成后统一发版（用户决策 4）。

---

## 6. 待实施时确认的细节

- `batch_id` 生成：用 `f"{rule_id}-{utcnowstamp}"` 还是递增整数？倾向前者（可读、无并发冲突）。
- 执行器与定时 job 的并发互斥：复用 v0.9.21 `_TimedMirrorLock`（已加心跳），严格顺序执行器全程持锁。
- 受控并发 >1：首版不实现，接口预留参数但强制钳制为 1，避免范围失控。

---

## 7. 实施进度（阶段二路线B，实际结果）

| Commit | 内容 | 测试 | 状态 |
| --- | --- | --- | --- |
| `2828d24` | C-01 SPA 路径穿越修复（realpath 归属校验） | 618 passed | ✅ |
| `30807f7` | C1 迁移骨架（models+db._COLUMNS+旧库兼容） | 621 passed | ✅ |
| `b41c9aa` | C2 确定性自然排序（natural_key+三处 DFS 注入） | 627 passed | ✅ |
| `f281e67` | C3 严格顺序执行核心（序列落库+远端核验+控制边界） | 637 passed | ✅ |
| `915b600` | C4 批次控制 API + 前端开关/按钮 | 642 passed + build ok | ✅ |
| （本批） | C5 定时续跑 job + 隔离性能基线 | 待全量回归 | 进行中 |

### 实现与设计的偏差（如实记录）
- 严格模式通过 `sync_rule(strict=True)` 开关启用，默认 `False` 保留旧批量路径，**功能完全一致**（FEATURE_PRESERVATION 红线全守住）；未删除旧引擎，作为回退对照。
- 批次 `sequence_id` 为全批次递增（跨作品），`parent_sequence_id` 当前置为作品起点序列（同作品内任务视为同父）；真正的"父目录失败不进子目录"依赖现有 `_ensure_child` 的 `break`（目录阶段已串行阻断），任务级子树依赖首版未做细粒度展开。
- 受控并发 >1 首版未实现（设计文档 2.4 已声明）。
- 重启续跑通过每 60s 的 `_job_mirror_task_poll` 复用现有 `poll_pending_tasks` 覆盖 submitted/verifying 遗留任务，而非重新发起复制。

### C5 隔离性能基线（mock，100/1000/10000 文件，不碰生产网盘）
| 规模 | natural_key 排序 | _media_files DFS | plan_work_batch |
| --- | --- | --- | --- |
| 100 | 0.10 ms | 0.08 ms | 0.10 ms |
| 1000 | 0.75 ms | 0.73 ms | <0.01 ms |
| 10000 | 7.96 ms | 6.96 ms | <0.01 ms |

结论：确定性排序开销可忽略（10000 文件 <8ms），镜像耗时主因仍是网盘列目录（与审计 A-01 一致），路线B未引入显著 CPU 开销。基线脚本：`tests/perf_strict_baseline.py`。
