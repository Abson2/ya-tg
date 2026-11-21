# 镜像同步 1.0 → 1.1 设计方案：查漏补缺（A/B 目录树 + 映射清单三方对比）

> 需求基线：用户 2026-11-21 原文（已固化到工作日志）
> 代码基线：v1.4.4 + 阶段二~四（HEAD `38bfbe4`）
> 命名约定：「镜像 1.0」= 当前已跑通的镜像同步复制+翻译能力；「镜像 1.1」= 本次查漏补缺迭代。**均为用户自定义版本标注，与 ya-tg 的 vX.Y.Z 项目版本无关**。
> 本文先出设计、经确认后编码；不改 TG/分享中心/前端，不动发送核心。

---

## 0. 需求原文与判定

> 「单轮任务开始时优先做 A、B 目录树和映射清单的对比，清理无效映射清单的同时优先对 A 库的增量、变量目录、文件进行处理：
> a、已在映射清单中的影视单文件变化时处理单文件（B 库个别集被删除或 A 库有更新，做补齐），不计入第二轮任务剧部数数量；
> b、已在映射清单中的整部剧在 B 库缺失，判断为失效，只要 A 库文件还在，重新按目录级复制、翻译重新处理，同时计入第二轮处理剧部数数量；
> c、已在映射清单，A 库文件没有了，则对 B 库做删除并清理无效清单；
> d、两者均无变化时，顺序处理第二轮任务。」

**判定**：这是真功能缺口，不是文档游戏。[事实] 1.0 已有单文件补齐（mirror.py:1152-1172）与轮转切批（plan_work_batch 269-301），但**没有单轮开始的三方对比预扫描**，也没有 a/b/c/d 的分类计数与"整部失效重处理"分支。

---

## 1. 现状取证结论（1.0 能力边界）

| 需求项 | 1.0 现状 | 证据 |
|---|---|---|
| A 库目录树 | 单轮开始全量读，`complete` 完整性闸门 | `_walk_source` mirror.py:774-820；`sync_rule` 956-958 |
| B 库目录树 | **无全树对比**，仅根目录 + 逐部 `_b_file_candidates` 局部核对 | mirror.py:969-974、1148-1151、1407-1452 |
| 映射清单 prune | **仅手动端点**，单轮不自动执行 | prune_stale 2162-2264 / prune_missing 2267-2321；调用 main.py:1611/1655 |
| a 单文件补齐 | 已有（B 缺集重置 pending、A 更新按 fingerprint/identity 补复制） | mirror.py:1152-1172、1194-1256 |
| b 整部缺失重处理 | **无**：只有手动 prune 删本地行，无"按目录级重处理 + 计入部数" | prune_missing 2303-2320 为手动 |
| c A 没了删 B | **无反向检测**；`delete_files` 接口已抓包+有单测，但业务链路从未调用 | guangya.py:336-350；测试 test_v081_mirror_dirs.py:255 |
| d 无变化顺序处理 | 有轮转切批与下一轮，但无显式"第二轮任务"概念 | plan_work_batch 269-301、works_cursor 1360-1365 |

**关键可复用原语**：`_b_file_candidates`（含 verified 语义）、`_lookup_b_file`、`_list_dir`、`_fingerprint`、`work_key_of`、`prune_stale_mappings` / `prune_missing_media_rows` 的核对逻辑。

---

## 2. 1.1 设计：单轮开始三方对比预扫描

### 2.1 落点

`sync_rule` 的 `mirror.py:1065`（`works = _discover_works(nodes, root)`）之后、选部切批（`1068`）之前插入一次预扫描。此时 `nodes`（A 全树）、`works`（全量作品）、`api`、`db`、`rule`、`stats`、`cache[b_root]` 均就绪，尚未进入逐部写操作。

### 2.2 新增函数 `reconcile_mappings_prepass(db, rule, nodes, works, api, stats, complete)`

**入参**：A 全树 nodes、全量作品 works、光鸭 api、统计 stats、A 树完整性 complete。
**动作**：

1. **建 A 库事实集**（纯内存，零网络）：
   - `a_file_ids` = 所有 `node["files"]` 的 fileId 集合
   - `a_source_fps` = `{source_file_id: fingerprint}`（用 `_fingerprint`）
   - `a_work_keys` = `{work_key_of(w) for w in works}`

2. **遍历该 rule 下 MirrorMedia + MirrorFile**（只读），按作品分类：
   - **C 类（A 已删除）**：该作品所有 MirrorFile 的 `source_file_id` 均不在 `a_file_ids`，且该作品 work_key 不在 `a_work_keys` → A 库已无此作品。
     - 保守默认（`mirror_delete_b_on_a_missing=false`）：清映射清单（删 MirrorFile/MirrorMedia 行或标 retired）+ 统计，**不删 B 库文件**。
     - 开关开启：调 `api.delete_files(该作品 MirrorFile.target_file_id 集合)` 删 B 库文件（需轮询 task），再清清单。**仅 `complete=True` 时才允许真删**。
   - **B 类（整部在 B 缺失）**：作品 work_key 在 `a_work_keys`（A 还在），且复用 `_b_file_candidates` 判 B 库整部为空（verified=True 且 visible 空）→ 判失效。
     - **预扫描只登记、不重置**：把该作品 work_key 记入 `reprocess_keys`，由 `sync_rule` 重排 works 优先进入本批。**不在预扫描里清 MirrorFile 为 pending**——v0.8.7 主循环（mirror.py:1323-1339）已有成熟的"B 库空→重置 pending→补复制→missing_in_b 计数"逻辑，且带 name_mismatch 保护，只重置真正缺失的行。预扫描提前清 pending 会让主循环 `synced_rows` 变空、`missing_in_b` 统计丢失（v087/v088 回归根因），还会误伤"改名后名字对不上"场景造成整部重复复制。
     - **计入第二轮剧部数**：重排进本批后走正常主循环，`stats["media"]` 自增（与正常复制同口径）。
   - **A 类（单文件变化）**：个别 MirrorFile 的 source_file_id 在 A 但 fingerprint 变化，或 A 有新增单集 → 交给现有主循环 `1194-1256` 处理，**不计入剧部数**（1.0 已具备，预扫描只统计不新增动作）。
   - **D 类（无变化）**：默认，走正常轮转。

3. **优先处理排序**：B 类 `reprocess_keys` 对应的 work 排到 `works` 列表前部（在 `plan_work_batch` 选批之前重排），使"整部缺失需重处理"的作品优先进入本批。这样 B 类既"计入剧部数"又"优先处理"。
   - 实现注意：`_discover_works()` 返回的 `works` 元素是 `(node, cats)` 元组，重排 key 必须取 `work_key_of(pair[0])`，直接对 pair 调 `.get()` 会报 `'tuple' object has no attribute 'get'`（全量回归 v087/v088 曾踩坑）。`reprocess_keys` 存 `work:` 前缀键，与 `work_key_of()` 同口径；预扫描入口对 legacy 无前缀 `MirrorMedia.work_key` 统一补前缀，避免把 A 还在的作品误判成 C 类已删。

4. **统计回写**：`stats["prepass"] = {"c_deleted_a": n, "c_b_files_removed": n, "b_invalid_reprocess": n, "a_single_file_fix": n, "d_unchanged": n}`。

### 2.3 护栏（红线）

1. **完整性闸门**：`complete=False`（A 树分页不完整）时**整轮跳过 C/B 判定**，只做 A/D 统计——避免 A 树漏读导致误判"A 没了"而误删 B 库。
2. **删除默认关闭**：`mirror_delete_b_on_a_missing` 默认 `false`；开启仍需 `complete=True` + verified + target_file_id 非空三重条件。删除走已取证 `api.delete_files()`，失败只记录不中断同步。
3. **不删映射行物理数据**（保守）：C 类清理优先用状态标记 + 清字段（复用 prune 的"重置 pending"语义），物理删行仍走现有手动端点，避免不可逆。真删 B 库是可配置的高级选项。
4. **零跨 await 写事务**：预扫描的 DB 写（重置 MirrorFile）在调用网络 API 前 commit（项目红线 mirror.py 禁跨 await 持写事务）；B 类核对的 `_b_file_candidates` 是网络 await，之前先 commit。
5. **性能护栏**：预扫描对每部作品做 `_b_file_candidates` 会增加接口开销；用 `max_prepass_media` 上限（默认 = max_works × 2 或 50），避免单轮预扫描打爆接口。B 类判定可只在"该作品本轮会被选中或已被标记"时做，不全量做。

### 2.4 与复制/翻译按部预算的关系

- **A 类单文件补齐**：不计入 `stats["media"]`（剧部数）——现有单文件补齐本就不自增 media，符合 a。
- **B 类整部重处理**：排到批次前部，走正常复制主循环，`stats["media"]` 自增（现有 1265/1282/1358 三处）——符合 b"计入剧部数"。
- **C/D 类**：不进本轮复制，C 类清清单，D 类顺序轮转——符合 c/d。

---

## 3. 配置项（runtime_config，保守默认）

| key | 默认 | 用途 |
|---|---|---|
| `mirror_prepass_enabled` | `true` | 单轮开始三方对比预扫描总开关（默认开，因为只是核对+统计；真删另控） |
| `mirror_delete_b_on_a_missing` | `false` | C 类是否真删 B 库文件（默认关，只清清单+统计） |
| `mirror_prepass_max_media` | `50` | 预扫描单轮最多核对多少部作品（性能护栏） |

---

## 4. 分阶段实施

1. **阶段 1.1-a**：`reconcile_mappings_prepass` 函数 + A 事实集构建 + A/D 统计（零网络、零写，最安全），挂到 `sync_rule` 插入点，加开关 `mirror_prepass_enabled`（默认 true）。
2. **阶段 1.1-b**：B 类判定（复用 `_b_file_candidates`）+ MirrorFile 重置 pending + 优先重排 works +计入剧部数。
3. **阶段 1.1-c**：C 类判定（A 反向检测）+ 清清单（默认）；`api.delete_files` 真删路径（开关 `mirror_delete_b_on_a_missing` 默认关）。
4. **阶段 1.1-d**：测试 + 全量回归 + 文档，真机验证后再考虑是否放开删除开关。

## 5. 验收

- A 树完整时：a 单文件补齐不计部数、b 整部缺失重处理计部数且优先、c A 删除触发清清单（真删需开关）、d 无变化走轮转。
- A 树不完整时：跳过 c/b 判定，零误删。
- 全量 pytest 零回归；镜像主链复制/翻译不受影响。

## 6. 用户决策（2026-11-21 已拍板）

1. **C 类删除**：`[决策]` **默认只清清单+统计，真删靠开关** `mirror_delete_b_on_a_missing`（默认 false）。需求原文 c 说"对 B 库做删除"，但删除不可逆、历史红线是"禁先删旧再复制"，真机验证后再放开开关。
2. **B 类部数**：`[决策]` **优先排进本批 + 自增 stats.media**（与正常复制同一口径），即"计入第二轮处理剧部数"。
