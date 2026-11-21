# strict 目录级批量 + 改名提速方案

> 版本基线：v1.3.0（HEAD 见 CHANGELOG）
> 取证时间：2026-11-21
> 触发需求：把严格顺序引擎升级为目录级批量；评估光鸭批量重命名接口与正则替换提速的可行性

## 结论先行

1. **strict 目录级批量可做，收益最大**：保留 sequence 顺序记录、批次控制、续跑语义，只把网络层从「逐文件」换成「按目录批量提交 + 批量轮询 + 按名回填」。40 集剧请求量从 400+ 降到十几次。
2. **光鸭没有「批量重命名」服务端接口，也没有「正则替换」接口**：技能包 `rename` 是单文件单目标（`remote_id` + `new_name`），`operations[]` 只是把多个单文件操作打包进一个请求，服务端仍逐个执行；项目后端走的是 `POST /userres/v1/file/rename {fileId,newName}`，同样是单文件。所谓「批量正则替换」只能本地算好新名后逐个 rename。
3. **改名的真实提速空间不在「批量 API」，而在「减少串行 idle + 保守并发 + 缓存复用」**。不存在「一次调用改 N 个文件」的银弹。

---

## 一、取证：光鸭到底有没有批量重命名 / 正则替换

| 取证对象 | 真实能力 | 证据 |
|---|---|---|
| 技能包 `rename` 操作 | 单文件：`remote_id`、`new_name` 两个必填字符串 | 光鸭技能包 `references/operations.md:76` |
| 技能包 `operations[]` | 一个请求里放多个操作，**每个 rename 仍是独立单文件操作**，服务端逐个执行 | 同上 `:108-115`；无 batch-rename / regex / replace 字段 |
| 技能包能力清单 | 整理类只有 `move`/`copy`/`rename`/`create_folder`，**未列批量改名或正则替换** | `references/skill.md:93` |
| 项目后端 WebFiles | `rename_file(file_id, new_name)` → `POST /userres/v1/file/rename`，单文件 | `backend/app/guangya.py:318-328`，HAR 取证 |
| 项目改名执行器 | 逐文件 `await api.rename_file(fid, final)`，文件间 `await asyncio.sleep(idle)` | `backend/app/mirror.py:1757-1832` |
| 本地正则/规则 | 项目用 `english_file_name()` 本地拼名（纯字符串、零网络），非调用光鸭 | `backend/app/mirror.py:1772` |

**判定**：用户记忆中的「批量重命名」= 技能包 `operations[]` 打包多个单文件 rename；它不是服务端批量接口，且属于 Node CLI / Agent 操作体系（需实例绑定、登录、UUID 任务），与本项目后端 Python 直连 WebFiles API 是两条链路。把后端改成调 CLI 是大改且引入运行依赖，不采用。「正则替换」光鸭无能力，只能本地实现。

---

## 二、方案 A：strict 引擎升级为目录级批量（strict-fast）

### 2.1 设计原则

**顺序语义保留，网络层批量化。** strict 的「严格顺序」约束的是三件事，与「逐文件发网络请求」无关：

- sequence 顺序记录（`sequence_id` 递增、`cursor_sequence` 持久化）；
- 批次控制（pause / resume / skip / cancel 在**任务边界**生效）；
- 崩溃续跑（done/failed/unknown 落终态，下次从游标继续）。

这三点全部保留；只把「核验→提交→轮询→落库」从逐文件改为按目录批次。

### 2.2 改动点（`backend/app/mirror.py` `_copy_jobs_strict`，476-593 行）

| 步骤 | 现状（逐文件） | 升级后（目录级批量） |
|---|---|---|
| 分组 | 无，直接遍历 `copy_jobs` | 复用非 strict 的 `buckets`（1185-1194 行），按 `target_parent` 分组 |
| sequence 占位 | 每文件 `add_sequence_task` 后提交 | 每文件仍占一个 `sequence_id`，批量落占位行（零网络） |
| 远端核验 | 每文件一次 `_b_file_candidates` | 每桶一次，按「名+大小」匹配整桶 |
| 复制提交 | 每文件 `copy_file([fid], parent)` | 每桶 `copy_file(fids, parent)`，`COPY_BATCH_SIZE=50` 分片 |
| 轮询 | 每文件 `_wait_one_task`（最坏 8 次/文件） | `_poll_tasks_batch` 统一轮询（最坏 8 次/桶） |
| 回填/落库 | 每文件单独 commit | 轮询 done 后按名回填整桶，再逐行落终态 |
| idle | 每文件 `asyncio.sleep(idle)`（N-1 次） | 每桶一次（M-1 次，M<<N） |
| 控制边界 | 每文件检查 pause/skip/cancel | 每桶检查（pause 停在本桶边界；skip/cancel 落整桶终态） |
| 续跑游标 | `cursor_sequence` = 最后完成文件 | `cursor_sequence` = 最后完成文件（桶内按序推进，粒度不变） |

### 2.3 顺序语义如何保证

- 桶内文件按 `copy_jobs` 原始自然序（A 库文件树顺序）提交和落 sequence，顺序记录不变；
- `skip` 落在「当前桶」而非「当前文件」——这是唯一语义变化，**控制粒度由文件级变目录级**；若必须精确到单集，见 2.5 折中版；
- `pause` 在本桶提交并轮询完成后生效，不打断在途 `copy_file`（与现状一致）；
- `unknown` 任务保留供下轮续跑，游标停在最后一个已终结文件。

### 2.4 收益（40 集剧，单目录）

| 指标 | 现状 strict | strict-fast |
|---|---|---|
| `copy_file` 请求 | 40 | 1 |
| 列目录 | 40 | 1 |
| `get_task_status` | 最坏 320 | 最坏 8 |
| idle 等待 | 39 次 | 0~1 次 |
| 量级 | 分钟级 | 十几秒级 |

### 2.5 折中版（如需单集 skip）

批量提交 + 单集跳过标记：一次 `copy_file` 提交整桶，但每个文件仍单独落 sequence 与终态；`skip` 只标记该行不再推进，回滚/重跑时按标记过滤。代价是「跳过」只能阻止后续对该文件的操作，无法撤回已在途的整桶复制。推荐默认用 2.2 目录级方案，折中版仅在真机要求单集 skip 时启用。

### 2.6 落地清单

- 改 `_copy_jobs_strict`，新增 `bulk` 分支（或直接替换，按用户「不做兼容」口径直接替换）；
- `stats` 增 `copy_batches`，`seq_cursor` 语义不变；
- `mirror_batch` 的 `mark_sequence_status` / `cancel_remaining` 支持按桶批量落终态；
- 测试：同目录 N 文件 → 1 次 `copy_file`；pause/skip/cancel 边界；续跑游标；列目录失败不重复复制；写锁不回归；
- 测试通过发版 v1.4.0。

---

## 三、方案 B：改名提速（rename-fast，无批量 API 前提下的最优解）

### 3.1 现状耗时拆解（`rename_mirror_files`，1626-1840 行）

每个待改文件 = 1 次 `rename_file` HTTP + 1 次 `asyncio.sleep(idle)`。默认 `file_idle_seconds=1.0`，硬下限 `MIN_FILE_IDLE_SECONDS=0.5`（`mirror.py:106`、`models.py:309`）。40 集 ≈ 40 次 HTTP + 39 秒纯等待。英文名解析本身每剧一次且已缓存（v1.3），不是瓶颈。

### 3.2 三层提速（按风险从低到高）

**B1 — 缓存复用 + 已改名跳过（零风险，已有）**
- `_resolve_english_title` 缓存命中零网络；
- B 库列目录已命中目标英文名时只纠正 DB 状态、不调接口（`mirror.py:1794-1802`）。
- 保持不动，作为底座。

**B2 — 降低串行 idle（低风险）**
- 新增运行期配置 `rename_idle_seconds`（默认 0.3，下限 0.2），与复制 idle 解耦；
- 40 集等待从 39 秒降到约 12 秒；
- 保留 `RENAME_MAX_CONSECUTIVE_FAILURES=5` 熔断，遇风控自动停。

**B3 — 保守并发（中风险，需真机验证）**
- 用 `asyncio.Semaphore(rename_concurrency)`（默认 3，可配 1~5），把 1757 行的串行循环改为 `gather` 并发；
- **写库隔离**：每个改名结果用独立 Session + Core UPDATE（复用 v1.3 `_persist_english_title_async` 模式），不跨 await 持写事务，避免 SQLite 单写者冲突；
- 并发下失败计数用锁保护，熔断阈值不放大；
- 风险：光鸭风控（code 117 / 429）。保守并发 3 + 失败退避 + 熔断，真机观察到限流立即降回 1。
- 40 集等待从 39 秒降到约 4~12 秒（取决于并发度）。

### 3.3 「批量正则替换」的可落地形态

光鸭无此接口，但可在本项目侧实现等价用户体验：

- 新增本地规则引擎 `batch_rename_rules`：`{work_key, match_regex, replace_template}`；
- 对 B 库列目录结果在本地批量算最终名（纯字符串，零网络，可预览）；
- 预览确认后交给 B3 并发执行器逐个 `rename_file`；
- 这**不是**服务端批量，但用户侧表现为「一次配置规则、批量预览、批量执行」，是正则替换诉求的真实可行形态。
- 默认规则沿用现有 `english_file_name`（中文片名段 → 英文名 + S/E 编号），自定义正则为可选增强。

### 3.4 红线

- 不改 B 库目录名（分类/作品/季目录是映射锚点，改了破坏续跑与 fileId 映射）；
- 不跨 await 持写事务；写库统一独立 Session + Core UPDATE + 重试；
- force 仍强制重解析英文名，不受缓存跳过影响；
- 不用 DB `rename_status` 整段跳过作品（v1.3 教训：会漏掉「DB 标 done 但 B 库回滚需重改」的合法场景）。

---

## 四、推荐组合与实施顺序

1. **第一步（必做）**：方案 A — strict 目录级批量。收益最大（复制请求降一个数量级），风险可控，不动数据模型。
2. **第二步（必做）**：方案 B2 — 改名 idle 解耦 + 降到 0.3。零风险，立竿见影。
3. **第三步（可选，真机决定）**：方案 B3 — 保守并发 + 规则化批量改名。先真机观察光鸭风控，再决定是否默认开启。
4. 每步独立测试，全部通过后发版 v1.4.0（或按步发 v1.4.0 / v1.4.1）。

## 五、需要你拍板的两点

1. **strict 的 skip 粒度**：默认按目录级（方案 A），还是保留单集级（方案 A 折中版）？
2. **改名并发是否默认开**：默认开（并发 3，快但需真机验风控），还是默认关（仅降 idle，稳）？

若不回复，按推荐执行：skip 目录级 + 改名并发默认关（B2 先行，B3 真机验证后再开），测试通过直接发版。
